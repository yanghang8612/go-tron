package freezer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func cooperativeFixture(t testing.TB) (*Freezer, func(string, uint64) ([]byte, error)) {
	t.Helper()
	f, err := NewFreezer(t.TempDir(), "", false, 2049, map[string]TableConfig{"bodies": {Prunable: true}, "tx_infos": {Prunable: true}, "state_roots": {Prunable: true, NoSnappy: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f, func(kind string, n uint64) ([]byte, error) { return []byte(fmt.Sprintf("%s-record-%d", kind, n)), nil }
}
func cooperativeOptions(count uint64, frame uint32, source func(string, uint64) ([]byte, error)) V2MigrationOptions {
	return V2MigrationOptions{Tables: []string{"bodies", "tx_infos", "state_roots"}, SegmentBlocks: count, FrameBlocks: frame, MaxSegments: 1, Online: true, SourceHead: count, Source: source}
}
func TestCooperativeDirectV2QuiescentWindowsAndAllRows(t *testing.T) {
	for _, tc := range []struct {
		frame   uint32
		count   uint64
		workers int
	}{{1, 4096, 1}, {64, 5120, 8}, {4096, 8192, 8}} {
		t.Run(fmt.Sprintf("frame%d/workers%d", tc.frame, tc.workers), func(t *testing.T) {
			f, source := cooperativeFixture(t)
			var active atomic.Int64
			reads := map[string]int{}
			checks := 0
			options := cooperativeOptions(tc.count, tc.frame, func(kind string, n uint64) ([]byte, error) {
				active.Add(1)
				defer active.Add(-1)
				reads[kind]++
				return source(kind, n)
			})
			options.PreparationWorkers = tc.workers
			options.CompressionWorkers = 4
			options.Transform = func(_ string, _ uint64, b, _ []byte) ([]byte, error) {
				active.Add(1)
				defer active.Add(-1)
				return bytes.Clone(b), nil
			}
			options.CooperativeYield = &V2CooperativeYield{Due: func() bool { return true }, Yield: func(ctx context.Context) error {
				checks++
				if active.Load() != 0 {
					t.Fatal("source/transform still running during yield")
				}
				if f.V1Tail() != 0 {
					t.Fatal("unpublished head became visible")
				}
				before := f.mutationGeneration.Load()
				if _, err := f.TruncateTail(0); err != nil {
					t.Fatal(err)
				}
				if before != f.mutationGeneration.Load() {
					t.Fatal("no-op tail invalidated job")
				}
				if coverage := f.V2Coverage(); coverage != 0 {
					t.Fatalf("partial coverage %d", coverage)
				}
				return ctx.Err()
			}}
			got, err := f.MigrateV2(options)
			if err != nil || got.End != tc.count {
				t.Fatalf("migrate=%+v err=%v", got, err)
			}
			if checks < 3 {
				t.Fatalf("no useful checkpoints: %d", checks)
			}
			for kind := range f.tables {
				for n := uint64(0); n < tc.count; n++ {
					want, _ := source(kind, n)
					got, err := f.Ancient(kind, n)
					if err != nil || !bytes.Equal(want, got) {
						t.Fatalf("%s/%d err=%v", kind, n, err)
					}
				}
			}
			// Read-ahead is joined at each window, never canceled/replayed. Only bodies
			// add dictionary samples, transform receipt body reads and verifier samples.
			if reads["tx_infos"] != int(tc.count)+3 || reads["state_roots"] != int(tc.count)+3 {
				t.Fatalf("window caused rereads: %+v", reads)
			}
		})
	}
}
func TestCooperativeDirectV2MutationAndCancelNeverPublish(t *testing.T) {
	for _, mode := range []string{"mutation", "rollbackABA", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			f, source := cooperativeFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			o := cooperativeOptions(64, 8, source)
			o.Context = ctx
			injected := errors.New("injected rollback")
			called := false
			o.CooperativeYield = &V2CooperativeYield{Due: func() bool { return !called }, Yield: func(context.Context) error {
				called = true
				if mode == "cancel" {
					cancel()
					return ctx.Err()
				}
				_, err := f.ModifyAncients(func(op AncientWriteOp) error {
					for kind := range f.tables {
						if err := op.AppendRaw(kind, 0, []byte("different")); err != nil {
							return err
						}
					}
					if mode == "rollbackABA" {
						return injected
					}
					return nil
				})
				if mode == "rollbackABA" && !errors.Is(err, injected) {
					t.Fatalf("rollback: %v", err)
				}
				return nil
			}}
			_, err := f.MigrateV2(o)
			if mode == "cancel" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, errV2LayoutChanged) {
				t.Fatal(err)
			}
			if f.V2Coverage() != 0 {
				t.Fatal("failed build published coverage")
			}
			paths, _ := filepath.Glob(filepath.Join(f.datadir, "v2", "manifests", "*.json"))
			if len(paths) > 0 {
				t.Fatal(paths)
			}
			for _, kind := range o.Tables {
				entries, err := os.ReadDir(filepath.Join(f.datadir, "v2", kind))
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				if len(entries) > 0 {
					t.Fatalf("unpublished %s files remain: %v", kind, entries)
				}
			}
			// Close must be possible after cancellation/error; all dynamic locks and
			// producer workers have been released. A retry remains possible after ABA.
			if mode != "mutation" {
				o.Context = context.Background()
				o.CooperativeYield = nil
				if _, err := f.MigrateV2(o); err != nil {
					t.Fatal(err)
				}
			}
			filepath.WalkDir(filepath.Join(f.datadir, "v2"), func(path string, d os.DirEntry, err error) error {
				if err == nil && !d.IsDir() && filepath.Ext(path) == ".tmp" {
					t.Errorf("temporary file %s", path)
				}
				return nil
			})
		})
	}
}

func TestCooperativeDirectV2MultipleSegments(t *testing.T) {
	f, source := cooperativeFixture(t)
	o := cooperativeOptions(64, 8, source)
	o.SourceHead = 128
	o.MaxSegments = 2
	seen := map[uint64]int{}
	o.CooperativeYield = &V2CooperativeYield{Due: func() bool { return true }, Yield: func(ctx context.Context) error { seen[f.V2Coverage()]++; return ctx.Err() }}
	result, err := f.MigrateV2(o)
	if err != nil || result.Segments != 2 || result.End != 128 || seen[0] == 0 || seen[64] == 0 {
		t.Fatalf("result=%+v seen=%v err=%v", result, seen, err)
	}
	for kind := range f.tables {
		for n := uint64(0); n < 128; n++ {
			want, _ := source(kind, n)
			got, err := f.Ancient(kind, n)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s/%d: %v", kind, n, err)
			}
		}
	}
}

func TestCooperativeDirectV2LateWindowCancelCleansAndJoins(t *testing.T) {
	f, source := cooperativeFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reads atomic.Uint64
	var active atomic.Int64
	o := cooperativeOptions(4096, 64, func(kind string, n uint64) ([]byte, error) {
		active.Add(1)
		defer active.Add(-1)
		reads.Add(1)
		return source(kind, n)
	})
	o.Context = ctx
	o.PreparationWorkers = 8
	o.CompressionWorkers = 8
	o.Transform = func(_ string, _ uint64, b, _ []byte) ([]byte, error) { return bytes.Clone(b), nil }
	o.CooperativeYield = &V2CooperativeYield{Due: func() bool { return reads.Load() > 2300 }, Yield: func(context.Context) error {
		if active.Load() != 0 {
			t.Fatal("producer not joined")
		}
		cancel()
		return ctx.Err()
	}}
	if _, err := f.MigrateV2(o); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if reads.Load() < 2300 || f.V2Coverage() != 0 {
		t.Fatal("did not reach private written window")
	}
	for _, kind := range o.Tables {
		entries, err := os.ReadDir(filepath.Join(f.datadir, "v2", kind))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if len(entries) > 0 {
			t.Fatal(entries)
		}
	}
	done := make(chan error, 1)
	go func() { done <- f.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked after cancellation")
	}
}
func BenchmarkCooperativeDirectV2Migration(b *testing.B) {
	rows := v2PipelineRecords(8192, 1024)
	for _, mode := range []string{"original", "window_no_contention", "yield_noop"} {
		b.Run(mode, func(b *testing.B) {
			count := 0
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				f, err := NewFreezer(b.TempDir(), "", false, 2049, map[string]TableConfig{"bodies": {Prunable: true}, "tx_infos": {Prunable: true}, "state_roots": {Prunable: true}})
				if err != nil {
					b.Fatal(err)
				}
				o := cooperativeOptions(8192, 64, func(_ string, n uint64) ([]byte, error) { return rows[n], nil })
				o.PreparationWorkers = 4
				o.CompressionWorkers = 4
				o.Transform = func(_ string, _ uint64, data, _ []byte) ([]byte, error) { return data, nil }
				if mode != "original" {
					o.CooperativeYield = &V2CooperativeYield{Due: func() bool { return mode == "yield_noop" }, Yield: func(context.Context) error { count++; return nil }}
				}
				b.StartTimer()
				_, err = f.MigrateV2(o)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if f.V2Coverage() != 8192 {
					b.Fatal("incomplete coverage")
				}
				for _, kind := range o.Tables {
					for _, n := range []uint64{0, 2047, 2048, 8191} {
						got, err := f.Ancient(kind, n)
						if err != nil || !bytes.Equal(got, rows[n]) {
							b.Fatal("oracle", err)
						}
					}
				}
				if err := f.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(count)/float64(b.N), "yields/op")
		})
	}
}

func TestCooperativeWindowPreservesExactEncodedTableBytes(t *testing.T) {
	rows := v2PipelineRecords(512, 2048)
	for _, codec := range []uint32{v2CodecDefault, v2CodecBodiesRawDict} {
		dir := t.TempDir()
		plain, cooperative := filepath.Join(dir, "plain.seg"), filepath.Join(dir, "cooperative.seg")
		read := func(n uint64) ([]byte, error) { return rows[n], nil }
		if err := writeV2SegmentProfileWithWorkers(plain, 0, 512, 8, codec, zstd.SpeedDefault, read, nil, 4); err != nil {
			t.Fatal(err)
		}
		checkpoints := 0
		if err := writeV2SegmentProfileWithCheckpoint(cooperative, 0, 512, 8, codec, zstd.SpeedDefault, read, nil, 4, 64, func() error { checkpoints++; return nil }); err != nil {
			t.Fatal(err)
		}
		a, _ := os.ReadFile(plain)
		b, _ := os.ReadFile(cooperative)
		if checkpoints == 0 || !bytes.Equal(a, b) {
			t.Fatalf("codec%d checkpoints%d changed table bytes", codec, checkpoints)
		}
	}
}
