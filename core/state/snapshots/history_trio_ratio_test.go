package snapshots

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
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
// whole .seg+.idx+.kv trio, not just the .seg. v3/v4 accessors deliberately stay
// raw because their fixed-width exact table is a random-read index; legacy v2
// accessors were compressed. The .idx remains raw because it is tiny and serves
// random seeks.
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

	dir := t.TempDir()
	segRaw, idxRaw, kvRaw := len(segmentData), len(indexData), len(accessorData)
	segC := compressedSize(t, dir, "seg.cb", segmentData)
	idxC := compressedSize(t, dir, "idx.cb", indexData)
	kvC := compressedSize(t, dir, "kv.cb", accessorData)

	trioRaw := int64(segRaw + idxRaw + kvRaw)
	accessorWired := kvC
	accessorMode := "compressed"
	if binary.BigEndian.Uint32(accessorData[8:12]) >= stateDomainChangeBinaryVersionV3 {
		accessorWired = int64(kvRaw)
		accessorMode = "raw v3/v4 random-read index"
	}
	trioWired := segC + int64(idxRaw) + accessorWired

	t.Logf("records=%d", len(normalized))
	t.Logf("  seg: %8d -> %8d  (%.2fx)", segRaw, segC, float64(segRaw)/float64(segC))
	t.Logf("  idx: %8d -> %8d  (%.2fx)", idxRaw, idxC, float64(idxRaw)/float64(idxC))
	t.Logf("  kv : %8d -> %8d  (%.2fx)", kvRaw, kvC, float64(kvRaw)/float64(kvC))
	t.Logf("  trio raw            = %8d", trioRaw)
	t.Logf("  trio production     = %8d  (%.2fx)  <- compressed .seg + %s .kv; raw .idx", trioWired, float64(trioRaw)/float64(trioWired), accessorMode)
	t.Logf("  kv share of trio    = %.0f%%", 100*float64(kvRaw)/float64(trioRaw))
}
