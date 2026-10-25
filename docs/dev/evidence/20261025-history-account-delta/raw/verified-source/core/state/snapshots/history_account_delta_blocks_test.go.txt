package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAccountDeltaOrderedColumns(t *testing.T) {
	for seed := int64(1); seed < 12; seed++ {
		raw, notes := accountDeltaFixture(seed)
		for _, delta := range []bool{false, true} {
			for _, limit := range []uint8{1, 4, 8} {
				enc, s, e := encodeAccountDeltaColumnsOrder(raw, notes, limit, delta, true)
				if e != nil {
					t.Fatal(e)
				}
				got, ds, e := decodeAccountDeltaColumns(enc, uint64(len(raw)), nil)
				if e != nil || !bytes.Equal(raw, got) || s != ds || ds.MaxDepth > limit {
					t.Fatalf("ordered seed%d limit%d %v", seed, limit, e)
				}
				bad := bytes.Clone(enc)
				bad[len(bad)-1] ^= 1
				if _, _, e = decodeAccountDeltaColumns(bad, uint64(len(raw)), nil); e == nil {
					t.Fatal("accepted corrupt ordered page")
				}
			}
		}
	}
}

func TestAccountDeltaPhysicalStream(t *testing.T) {
	dir := t.TempDir()
	w, e := newAccountDeltaStreamWriter(dir, 128<<10, 128, 4)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	raw := bytes.Repeat([]byte{0x55}, 128)
	if _, e = w.Write(raw); e != nil {
		t.Fatal(e)
	}
	value := bytes.Repeat([]byte{3}, 90)
	for i := 0; i < 14000; i++ {
		header := binary.BigEndian.AppendUint64(nil, uint64(i))
		raw = append(raw, header...)
		if _, e = w.Write(header); e != nil {
			t.Fatal(e)
		}
		value = bytes.Clone(value)
		value[20] = byte(i)
		if e = w.NoteAccountValue(uint32(i%11), uint64(len(raw)), uint32(len(value))); e != nil {
			t.Fatal(e)
		}
		raw = append(raw, value...)
		if _, e = w.Write(value); e != nil {
			t.Fatal(e)
		}
	}
	path := filepath.Join(dir, "delta.seg")
	m, e := w.FinishWithMetadata(path)
	if e != nil {
		t.Fatal(e)
	}
	blob, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !accountDeltaFooterVersion(blob[:48]) || m.size != uint64(len(blob)) || m.checksum != sha256.Sum256(blob) {
		t.Fatal("physical metadata")
	}
	got, e := decompressBlockBlob(blob)
	if e != nil || !bytes.Equal(got, raw) {
		t.Fatalf("whole stream %v", e)
	}
	r, e := openCompressedBlockReader(path)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	var wg sync.WaitGroup
	for seed := int64(0); seed < 4; seed++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 80; i++ {
				off := rng.Intn(len(raw))
				n := min(2000, len(raw)-off)
				b := make([]byte, n)
				nr, e := r.ReadAt(b, int64(off))
				if e != nil || nr != n || !bytes.Equal(b, raw[off:off+n]) {
					t.Errorf("random read %d: %v", off, e)
					return
				}
			}
		}(seed)
	}
	wg.Wait()
	b := make([]byte, 10)
	if n, e := r.ReadAt(b, int64(len(raw)-3)); n != 3 || !errors.Is(e, io.EOF) {
		t.Fatalf("EOF %d %v", n, e)
	}
}
func TestAccountDeltaStreamResetAndCancellation(t *testing.T) {
	dir := t.TempDir()
	w, e := newAccountDeltaStreamWriter(dir, 128<<10, 128, 4)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	if e = w.NoteAccountValue(1, 130, 50); e != nil {
		t.Fatal(e)
	}
	if _, e = w.Write(make([]byte, 1000)); e != nil {
		t.Fatal(e)
	}
	if e = w.Reset(); e != nil {
		t.Fatal(e)
	}
	if len(w.accountDeltas.values) != 0 || !w.body.accountDelta {
		t.Fatal("reset state")
	}
	if _, e = w.Write([]byte("bounded")); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(dir, "cancel.seg")
	if _, e = w.FinishWithMetadataContext(ctx, path); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancel %v", e)
	}
	if _, e = os.Stat(path); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("published canceled output")
	}
}
func FuzzAccountDeltaColumnsDecoder(f *testing.F) {
	raw, notes := accountDeltaFixture(77)
	for _, ordered := range []bool{false, true} {
		enc, _, _ := encodeAccountDeltaColumnsOrder(raw, notes, 4, true, ordered)
		f.Add(enc, uint32(len(raw)))
	}
	f.Fuzz(func(t *testing.T, p []byte, n uint32) {
		if len(p) > 3*accountDeltaPageMax+100 {
			return
		}
		got, s, e := decodeAccountDeltaColumns(p, uint64(n), nil)
		if e == nil && (len(got) != int(n) || s.MaxDepth > accountDeltaDepthMax) {
			t.Fatal("invalid decoder success")
		}
	})
}

func TestAccountDeltaHistoryTrioCanonicalIdentity(t *testing.T) {
	dir := t.TempDir()
	var changes []*rawdb.StateDomainChange
	for i := uint64(1); i <= 4000; i++ {
		value := bytes.Repeat([]byte{byte(i % 7)}, 90)
		changes = append(changes, &rawdb.StateDomainChange{BlockNum: i, TxNum: i, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0: 0x41, 20: byte(i % 7)}, PrevExists: true, Prev: value})
	}
	refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 4000, changes)
	before := make(map[string][]byte)
	for _, r := range refs {
		b, e := os.ReadFile(filepath.Join(dir, r.Path))
		if e != nil {
			t.Fatal(e)
		}
		before[r.Path] = b
	}
	result, s, e := ReencodeHistoryAccountDeltaTrioContext(context.Background(), dir, refs)
	if e != nil {
		t.Fatal(e)
	}
	if !s.Converted || s.AfterBytes >= s.BeforeBytes || s.AccountRecords != 4000 {
		t.Fatal("conversion stats", s)
	}
	for i, r := range refs {
		b, e := os.ReadFile(filepath.Join(dir, r.Path))
		if e != nil || !bytes.Equal(b, before[r.Path]) {
			t.Fatal("source changed", e)
		}
		if r.Kind != SegmentHistory {
			b, e = os.ReadFile(filepath.Join(dir, result[i].Path))
			if e != nil || !bytes.Equal(b, before[r.Path]) {
				t.Fatal("companion changed", e)
			}
		} else {
			v, _, _, e := openHistorySegmentForRead(dir, result[i])
			if e != nil {
				t.Fatal(e)
			}
			got, e := io.ReadAll(io.NewSectionReader(v, 0, int64(s.LogicalBytes)))
			v.Close()
			if e != nil || !bytes.Equal(got, before[r.Path]) {
				t.Fatal("virtual stream changed", e)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, e = ReencodeHistoryAccountDeltaTrioContext(ctx, dir, refs); !errors.Is(e, context.Canceled) {
		t.Fatal("cancellation", e)
	}
	// Every strict source checksum remains a prerequisite.
	bad := append([]SegmentRef(nil), refs...)
	bad[0].Checksum = badSnapshotChecksum()
	if _, _, e = ReencodeHistoryAccountDeltaTrioContext(context.Background(), dir, bad); e == nil {
		t.Fatal("accepted bad source checksum")
	}
}

func TestAccountDeltaHistoryCompatibility(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "4")
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"range-key", TestCompressedHistorySegmentReaderEquivalence}, {"full-check", TestCompressedHistorySegmentFullReadAndCheck}, {"readers", TestCompressedHistorySegmentProductionReadPaths}, {"compaction", TestCompactionMergesCompressedSources}, {"inspect", TestInspectHistorySpaceProfilesCompressedV6Trio},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestAccountDeltaPreservesDecodedCacheByteBudget(t *testing.T) {
	dir := t.TempDir()
	w, e := newAccountDeltaStreamWriter(dir, accountDeltaPageMax, 128, 4)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	raw := bytes.Repeat([]byte("cache bytes remain bounded"), 200000)
	if _, e = w.Write(raw); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "large-pages.seg")
	if e = w.Finish(path); e != nil {
		t.Fatal(e)
	}
	for _, limit := range []int{1, 4, 16} {
		r, e := openCompressedBlockReaderWithCacheLimit(path, limit)
		if e != nil {
			t.Fatal(e)
		}
		for _, b := range r.table {
			got := make([]byte, 1)
			if _, e = r.ReadAt(got, int64(b.uncompressedStart)); e != nil || got[0] != raw[b.uncompressedStart] {
				t.Fatal("read", e)
			}
		}
		var retained int
		for _, entry := range r.cache {
			retained += cap(entry.bytes)
		}
		budget := max(accountDeltaPageMax, limit*historyCompressChunkSize)
		if retained > budget || r.cacheLimit > limit {
			t.Fatalf("cache %d retained%d budget%d", limit, retained, budget)
		}
		r.Close()
	}
}
