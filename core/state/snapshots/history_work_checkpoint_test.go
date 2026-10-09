package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type checkpointReadAt struct {
	data    []byte
	lengths []int
}

func (r *checkpointReadAt) ReadAt(p []byte, off int64) (int, error) {
	r.lengths = append(r.lengths, len(p))
	return bytes.NewReader(r.data).ReadAt(p, off)
}

type checkpointWrite struct {
	lengths []int
	bytes.Buffer
}

func (w *checkpointWrite) Write(p []byte) (int, error) {
	w.lengths = append(w.lengths, len(p))
	return w.Buffer.Write(p)
}

func TestHistoryWorkCheckpointIOBoundaries(t *testing.T) {
	data := bytes.Repeat([]byte{0x7a}, 3*workCheckpointIOChunk+11)
	var charged uint64
	ctx := maintenance.WithWorkCheckpoint(context.Background(), func(n uint64) error { charged += n; return nil })
	r := &checkpointReadAt{data: data}
	got := make([]byte, len(data))
	if n, err := (contextReaderAt{ctx: ctx, r: r}).ReadAt(got, 0); err != nil || n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("read %d/%v", n, err)
	}
	if len(r.lengths) != 4 || charged != uint64(len(data)) {
		t.Fatalf("reads %v charge %d", r.lengths, charged)
	}
	for _, n := range r.lengths {
		if n > workCheckpointIOChunk {
			t.Fatal("unbounded read", n)
		}
	}
	w := new(checkpointWrite)
	if n, err := (contextWriter{ctx: ctx, w: w}).Write(data); err != nil || n != len(data) || !bytes.Equal(w.Bytes(), data) {
		t.Fatalf("write %d/%v", n, err)
	}
	if len(w.lengths) != 4 {
		t.Fatal(w.lengths)
	}
	stop := errors.New("stop checkpoint")
	calls := 0
	ctx = maintenance.WithWorkCheckpoint(context.Background(), func(uint64) error {
		calls++
		if calls == 2 {
			return stop
		}
		return nil
	})
	r = &checkpointReadAt{data: data}
	if n, err := (contextReaderAt{ctx: ctx, r: r}).ReadAt(got, 0); n != workCheckpointIOChunk || !errors.Is(err, stop) || len(r.lengths) != 1 {
		t.Fatalf("cancel read %d/%v lengths%v", n, err, r.lengths)
	}
	w = new(checkpointWrite)
	calls = 0
	if n, err := (contextWriter{ctx: ctx, w: w}).Write(data); n != workCheckpointIOChunk || !errors.Is(err, stop) || len(w.lengths) != 1 {
		t.Fatalf("cancel write %d/%v lengths%v", n, err, w.lengths)
	}
	r = &checkpointReadAt{data: data}
	if _, err := (contextReaderAt{ctx: context.Background(), r: r}).ReadAt(got, 0); err != nil || len(r.lengths) != 1 {
		t.Fatalf("nil hook altered original read %v/%v", r.lengths, err)
	}
	h := sha256.New()
	charged = 0
	ctx = maintenance.WithWorkCheckpoint(context.Background(), func(n uint64) error { charged += n; return nil })
	if _, err := io.Copy(h, contextReader{ctx: ctx, r: bytes.NewReader(data)}); err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(data)
	if !bytes.Equal(h.Sum(nil), want[:]) || charged < uint64(len(data)) {
		t.Fatalf("stream SHA or checkpoint charge changed %d", charged)
	}
}

func TestHistoryWorkCheckpointSemanticDigestAndCancellation(t *testing.T) {
	_, _, blocks := historyStagingProofFixture(t)
	rows := make([][32]byte, 17000)
	for i := range rows {
		rows[i] = sha256.Sum256([]byte{byte(i), byte(i >> 8)})
	}
	plain := append([][32]byte(nil), rows...)
	wantRange, want := historyStagingSemanticDigest(blocks, plain)
	calls := 0
	ctx := maintenance.WithWorkCheckpoint(context.Background(), func(uint64) error { calls++; return nil })
	gotRange, got, err := historyStagingSemanticDigestContext(ctx, blocks, rows)
	if err != nil || wantRange != gotRange || want != got || !reflect.DeepEqual(rows, plain) || calls < 10 {
		t.Fatalf("digest/order/checkpoints %v calls%d", err, calls)
	}
	// All-equal records perform no heap swaps; heap construction must still
	// checkpoint before reaching extraction or digest hashing.
	equal := make([][32]byte, 17000)
	stopBuild := errors.New("cancel heap construction")
	buildCalls := 0
	equalCtx := maintenance.WithWorkCheckpoint(context.Background(), func(uint64) error { buildCalls++; return stopBuild })
	if err := sortHistoryStagingRecordsContext(equalCtx, equal); !errors.Is(err, stopBuild) || buildCalls != 1 {
		t.Fatalf("equal heap build cancellation %v calls%d", err, buildCalls)
	}
	stop := errors.New("sort cancelled")
	rows = append([][32]byte(nil), plain...)
	ctx = maintenance.WithWorkCheckpoint(context.Background(), func(uint64) error { return stop })
	if _, _, err := historyStagingSemanticDigestContext(ctx, blocks, rows); !errors.Is(err, stop) {
		t.Fatalf("sort accepted cancellation %v", err)
	}
}

// Both directions are necessary: a cooperative owner must not create a global
// flight, and a cooperative waiter must not wait for a noncooperative owner.
func TestHistoryWorkCheckpointAuthenticationDoesNotShareFlights(t *testing.T) {
	for _, receipt := range []bool{false, true} {
		for _, cooperativeOwner := range []bool{false, true} {
			name := "full"
			if receipt {
				name = "receipt"
			}
			if cooperativeOwner {
				name += "/cooperative-owner"
			} else {
				name += "/cooperative-waiter"
			}
			t.Run(name, func(t *testing.T) {
				dir, manifest, _ := historyStagingProofFixture(t)
				refs := stagingReceiptRefs(t, dir, manifest)
				id, err := historyStagingTrioID(refs)
				if err != nil {
					t.Fatal(err)
				}
				started, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				ownerAuth := func(context.Context, [3]SegmentRef) error { close(started); <-release; return nil }
				ownerCtx, waiterCtx := context.Background(), context.Background()
				hook := func(uint64) error { return nil }
				if cooperativeOwner {
					ownerCtx = maintenance.WithWorkCheckpoint(ownerCtx, hook)
				} else {
					waiterCtx = maintenance.WithWorkCheckpoint(waiterCtx, hook)
				}
				var owner, waiter func(context.Context) error
				if receipt {
					a := historyStagingReceiptAuthenticator{dir: dir, manifest: manifest, testAuth: ownerAuth}
					b := historyStagingReceiptAuthenticator{dir: dir, manifest: manifest, testAuth: func(context.Context, [3]SegmentRef) error { return nil }}
					owner = func(ctx context.Context) error { return a.authenticate(ctx, refs, id) }
					waiter = func(ctx context.Context) error { return b.authenticate(ctx, refs, id) }
				} else {
					a, e := NewHistoryStagingColdProver(dir, manifest)
					if e != nil {
						t.Fatal(e)
					}
					defer a.Close()
					a.testAuth = ownerAuth
					b, e := NewHistoryStagingColdProver(dir, manifest)
					if e != nil {
						t.Fatal(e)
					}
					defer b.Close()
					b.testAuth = func(context.Context, [3]SegmentRef) error { return nil }
					owner = func(ctx context.Context) error { return a.authenticate(ctx, refs, id) }
					waiter = func(ctx context.Context) error { return b.authenticate(ctx, refs, id) }
				}
				ownerDone := make(chan error, 1)
				go func() { ownerDone <- owner(ownerCtx) }()
				<-started
				waiterDone := make(chan error, 1)
				go func() { waiterDone <- waiter(waiterCtx) }()
				select {
				case err := <-waiterDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("waiter depends on paused owner flight")
				}
				once.Do(func() { close(release) })
				if err := <-ownerDone; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestHistoryWorkCheckpointProofFormatsAndRejectedContent(t *testing.T) {
	for _, format := range []string{"v5", "v6", "reference"} {
		t.Run(format, func(t *testing.T) {
			dir, manifest, blocks := stagingDenseColdFixture(t, format, 3)
			mask := make([]bool, len(blocks))
			for i := range mask {
				mask[i] = true
			}
			calls := 0
			ctx := maintenance.WithWorkCheckpoint(context.Background(), func(uint64) error { calls++; return nil })
			p, err := NewHistoryStagingColdProver(dir, manifest)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			got, err := p.Build(ctx, blocks, mask)
			if err != nil || calls < 100 {
				t.Fatalf("checkpoint proof %v calls%d", err, calls)
			}
			plain, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, mask)
			if err != nil || !reflect.DeepEqual(got, plain) {
				t.Fatalf("changed span %v", err)
			}
			binding := rawdb.HistoryStagingColdBinding{Bucket: 1, Spans: got}
			binding.Spans = append([]rawdb.HistoryStagingColdSpan(nil), got...)
			binding.Spans[0].SemanticHash[0] ^= 1
			if err := p.VerifyBinding(ctx, binding, blocks); err == nil {
				t.Fatal("accepted semantic mismatch")
			}
			refs := stagingReceiptRefs(t, dir, manifest)
			id, err := historyStagingTrioID(refs)
			if err != nil {
				t.Fatal(err)
			}
			receipt := historyStagingReceiptAuthenticator{dir: dir, manifest: manifest}
			if err := receipt.authenticate(ctx, refs, id); err != nil {
				t.Fatal(err)
			}
			// In-place, same length, restored mtime must still invalidate the ctime/file
			// fingerprint and force strong checksum rejection.
			ref := manifest.Segments[0]
			path := filepath.Join(dir, ref.Path)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			off := int64(ref.Size - 1)
			var b [1]byte
			if _, err = f.ReadAt(b[:], off); err != nil {
				t.Fatal(err)
			}
			b[0] ^= 0x80
			if _, err = f.WriteAt(b[:], off); err != nil {
				t.Fatal(err)
			}
			if err = f.Close(); err != nil {
				t.Fatal(err)
			}
			if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
				t.Fatal(err)
			}
			if err := receipt.authenticate(ctx, refs, id); err == nil {
				t.Fatal("receipt accepted same-size same-mtime corruption")
			}
			fresh, err := NewHistoryStagingColdProver(dir, manifest)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			if _, err := fresh.Build(ctx, blocks, mask); err == nil {
				t.Fatal("accepted same-size same-mtime corrupt trio")
			}
			if err = os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if _, err := fresh.Build(ctx, blocks, mask); err == nil {
				t.Fatal("accepted missing cold file")
			}
		})
	}
}
