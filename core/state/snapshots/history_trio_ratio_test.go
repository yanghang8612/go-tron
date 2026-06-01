package snapshots

import (
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
// whole .seg+.idx+.kv trio, not just the .seg. The .kv duplicates each record's
// full key (incompressible for keccak-distributed storage slots), so seg-only
// compression overstates the DB-size win. This measures each companion's own
// ratio and the trio totals for seg-only vs all-compressed, to decide whether
// compressing .kv/.idx is worth wiring.
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
	trioSegOnly := segC + int64(idxRaw) + int64(kvRaw)
	trioAll := segC + idxC + kvC

	t.Logf("records=%d", len(normalized))
	t.Logf("  seg: %8d -> %8d  (%.2fx)", segRaw, segC, float64(segRaw)/float64(segC))
	t.Logf("  idx: %8d -> %8d  (%.2fx)", idxRaw, idxC, float64(idxRaw)/float64(idxC))
	t.Logf("  kv : %8d -> %8d  (%.2fx)", kvRaw, kvC, float64(kvRaw)/float64(kvC))
	t.Logf("  trio raw            = %8d", trioRaw)
	t.Logf("  trio seg-only-compr = %8d  (%.2fx)  <- what's wired today", trioSegOnly, float64(trioRaw)/float64(trioSegOnly))
	t.Logf("  trio all-compressed = %8d  (%.2fx)  <- if .idx/.kv compressed too", trioAll, float64(trioRaw)/float64(trioAll))
	t.Logf("  kv share of trio    = %.0f%%", 100*float64(kvRaw)/float64(trioRaw))
}
