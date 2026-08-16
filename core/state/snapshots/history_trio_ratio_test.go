package snapshots

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// compressedSize block-compresses blob and returns the resulting file size.
func compressedSize(t *testing.T, dir, name string, blob []byte) int64 {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := compressBlobToFile(dir, p, blob, historyCompressChunkSize); err != nil {
		t.Fatalf("compress %s: %v", name, err)
	}
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

// TestHistoryTrioCompressionRatio reports the HONEST archive-unit number: the
// whole .seg+.idx+.kv trio, not just the .seg. v3/v4/v5/v6 accessors
// deliberately stay raw because their fixed-width exact table is a random-read
// index; legacy v2 accessors were compressed. The .idx remains raw because it
// is tiny and serves random seeks.
func TestHistoryTrioCompressionRatio(t *testing.T) {
	changes := buildHistoryStructs(400, 50)
	from, to := uint64(9_000_000), uint64(9_000_000+399)
	normalized := normalizeStateDomainChangesForBinary(changes)
	segmentData, index, accessor, err := encodeStateDomainChangeBinarySegment(from, to, normalized)
	if err != nil {
		t.Fatal(err)
	}
	indexData, err := encodeStateDomainChangeBinaryIndex(from, to, index)
	if err != nil {
		t.Fatal(err)
	}
	accessorData, err := encodeStateDomainChangeBinaryAccessor(from, to, accessor)
	if err != nil {
		t.Fatal(err)
	}
	v4AccessorData, err := encodeStateDomainChangeBinaryAccessorV4(from, to, accessor)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	segRaw, idxRaw, kvRaw := len(segmentData), len(indexData), len(accessorData)
	segC := compressedSize(t, dir, "seg.cb", segmentData)
	for _, chunkSize := range []int{32 << 10, 64 << 10, 128 << 10, 256 << 10} {
		name := fmt.Sprintf("seg-%d.cb", chunkSize)
		chunkCompressed := compressedSizeWithChunk(t, dir, name, segmentData, chunkSize)
		t.Logf("  seg chunk %6d KiB = %8d  (%.2fx; %.2f%% vs current %dKiB)", chunkSize>>10, chunkCompressed, float64(segRaw)/float64(chunkCompressed), 100*(1-float64(chunkCompressed)/float64(segC)), historyCompressChunkSize>>10)
	}
	idxC := compressedSize(t, dir, "idx.cb", indexData)
	kvC := compressedSize(t, dir, "kv.cb", accessorData)

	trioRaw := int64(segRaw + idxRaw + kvRaw)
	accessorWired := kvC
	accessorMode := "compressed"
	if binary.BigEndian.Uint32(accessorData[8:12]) >= stateDomainChangeBinaryVersionV3 {
		accessorWired = int64(kvRaw)
		accessorMode = "raw random-read index"
	}
	trioWired := segC + int64(idxRaw) + accessorWired
	v4TrioWired := segC + int64(idxRaw) + int64(len(v4AccessorData))
	if trioWired >= v4TrioWired {
		t.Fatalf("v5 production trio size %d did not improve v4 size %d", trioWired, v4TrioWired)
	}

	t.Logf("records=%d", len(normalized))
	t.Logf("  seg: %8d -> %8d  (%.2fx)", segRaw, segC, float64(segRaw)/float64(segC))
	t.Logf("  idx: %8d -> %8d  (%.2fx)", idxRaw, idxC, float64(idxRaw)/float64(idxC))
	t.Logf("  kv : %8d -> %8d  (%.2fx)", kvRaw, kvC, float64(kvRaw)/float64(kvC))
	t.Logf("  trio raw            = %8d", trioRaw)
	t.Logf("  trio production v4  = %8d", v4TrioWired)
	t.Logf("  trio production     = %8d  (%.2fx)  <- compressed .seg + %s .kv; raw .idx", trioWired, float64(trioRaw)/float64(trioWired), accessorMode)
	t.Logf("  trio saving vs v4   = %.2f%%", 100*(1-float64(trioWired)/float64(v4TrioWired)))
	t.Logf("  kv share of trio    = %.0f%%", 100*float64(kvRaw)/float64(trioRaw))
	koSegment, _, koAccessorData, err := encodeStateDomainChangeBinarySegmentV6(from, to, normalized)
	if err != nil {
		t.Fatal(err)
	}
	koSegmentCompressed := compressedSize(t, dir, "seg-key-oriented.cb", koSegment)
	koTrio := koSegmentCompressed + int64(idxRaw) + int64(len(koAccessorData))
	t.Logf("  key-oriented unique = %8d (seg=%d kv=%d; %.2f%% vs v5)", koTrio, koSegmentCompressed, len(koAccessorData), 100*(1-float64(koTrio)/float64(trioWired)))

	repeated := make([]*rawdb.StateDomainChange, len(normalized))
	for i, change := range normalized {
		clone := cloneStateDomainChangeForSegment(change)
		source := normalized[i%256]
		clone.Owner, clone.Generation, clone.Domain = source.Owner, source.Generation, source.Domain
		clone.Key = append([]byte(nil), source.Key...)
		repeated[i] = clone
	}
	repeatedSegment, _, repeatedAccessorEntries, err := encodeStateDomainChangeBinarySegment(from, to, repeated)
	if err != nil {
		t.Fatal(err)
	}
	repeatedAccessor, err := encodeStateDomainChangeBinaryAccessor(from, to, repeatedAccessorEntries)
	if err != nil {
		t.Fatal(err)
	}
	repeatedV5 := compressedSize(t, dir, "seg-repeated-v5.cb", repeatedSegment) + int64(idxRaw) + int64(len(repeatedAccessor))
	koRepeatedSegment, _, koRepeatedAccessorData, err := encodeStateDomainChangeBinarySegmentV6(from, to, repeated)
	if err != nil {
		t.Fatal(err)
	}
	koRepeatedSegmentCompressed := compressedSize(t, dir, "seg-repeated-key-oriented.cb", koRepeatedSegment)
	koRepeated := koRepeatedSegmentCompressed + int64(idxRaw) + int64(len(koRepeatedAccessorData))
	t.Logf("  key-oriented repeat = %8d (seg=%d kv=%d; %.2f%% vs repeated v5 %d)", koRepeated, koRepeatedSegmentCompressed, len(koRepeatedAccessorData), 100*(1-float64(koRepeated)/float64(repeatedV5)), repeatedV5)
}

func compressedSizeWithChunk(t *testing.T, dir, name string, blob []byte, chunkSize int) int64 {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := compressBlobToFile(dir, p, blob, chunkSize); err != nil {
		t.Fatalf("compress %s: %v", name, err)
	}
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}
