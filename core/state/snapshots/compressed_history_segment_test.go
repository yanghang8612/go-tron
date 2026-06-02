package snapshots

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// buildHistoryStructs builds realistic StateDomainChange records grouped into
// blocks (shared BlockNum/BlockHash, sequential Seq), for the compressed-segment
// integration test.
func buildHistoryStructs(blocks, perBlock int) []*rawdb.StateDomainChange {
	rng := rand.New(rand.NewSource(7))
	owners := make([]common.Address, 32)
	for i := range owners {
		_, _ = rng.Read(owners[i][:])
		owners[i][0] = 0x41
	}
	out := make([]*rawdb.StateDomainChange, 0, blocks*perBlock)
	for b := 0; b < blocks; b++ {
		var bh common.Hash
		_, _ = rng.Read(bh[:])
		bn := uint64(9_000_000 + b)
		owner := owners[rng.Intn(len(owners))]
		for s := 0; s < perBlock; s++ {
			if s%8 == 0 {
				owner = owners[rng.Intn(len(owners))]
			}
			key := make([]byte, 32)
			_, _ = rng.Read(key)
			ch := &rawdb.StateDomainChange{
				BlockNum:   bn,
				BlockHash:  bh,
				TxNum:      bn,
				Seq:        uint64(s),
				FlatDomain: rawdb.StateFlatDomainKVLatest,
				Owner:      owner,
				Generation: 0,
				Domain:     kvdomains.ContractStorage,
				Key:        key,
				NextExists: true,
				Next:       mixedValue(rng),
			}
			if s%2 == 0 {
				ch.PrevExists = true
				ch.Prev = mixedValue(rng)
			}
			out = append(out, ch)
		}
	}
	return out
}

// TestCompressedHistorySegmentReaderEquivalence is the integration proof for
// wiring compression into cold history: it block-compresses a real segment's
// serialized bytes, then reads every record back at its accessor offset THROUGH
// THE EXISTING production reader (readStateDomainChangeBinaryRecordAtBounded) over
// the codec's ReadAt — and asserts byte-identical results vs reading the raw,
// uncompressed segment. This proves the offset-addressed read path needs no
// change beyond swapping in the codec-backed io.ReaderAt; the on-disk segment is
// smaller. (The remaining work is routing each read site through that swap.)
func TestCompressedHistorySegmentReaderEquivalence(t *testing.T) {
	changes := buildHistoryStructs(300, 50)
	fromTx, toTx := uint64(9_000_000), uint64(9_000_000+300-1)
	normalized := normalizeStateDomainChangesForBinary(changes)
	segmentData, index, accessor, err := encodeStateDomainChangeBinarySegment(fromTx, toTx, normalized)
	if err != nil {
		t.Fatalf("encode segment: %v", err)
	}

	dir := t.TempDir()
	segPath := filepath.Join(dir, "history.cseg")
	if err := compressBlobToFile(dir, segPath, segmentData, 16384); err != nil {
		t.Fatalf("compressBlobToFile: %v", err)
	}

	r, err := openCompressedBlockReader(segPath)
	if err != nil {
		t.Fatalf("open compressed: %v", err)
	}
	defer r.Close()
	if r.UncompressedSize() != uint64(len(segmentData)) {
		t.Fatalf("uncompressed size = %d, want %d", r.UncompressedSize(), len(segmentData))
	}

	raw := bytes.NewReader(segmentData)
	rawSize := uint64(len(segmentData))

	// Header reads identically over the codec ReaderAt.
	hRaw, err := readStateDomainChangeBinaryHeaderAt(raw, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		t.Fatalf("raw header: %v", err)
	}
	hComp, err := readStateDomainChangeBinaryHeaderAt(r, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		t.Fatalf("compressed header: %v", err)
	}
	if hRaw != hComp {
		t.Fatalf("header mismatch raw=%+v comp=%+v", hRaw, hComp)
	}
	if hComp.count != uint64(len(normalized)) {
		t.Fatalf("count = %d, want %d", hComp.count, len(normalized))
	}

	// Every accessor offset: the production record reader returns byte-identical
	// records from the compressed segment and the raw segment.
	for i, entry := range accessor {
		wantChange, _, err := readStateDomainChangeBinaryRecordAtBounded(raw, entry.offset, rawSize)
		if err != nil {
			t.Fatalf("raw record %d @%d: %v", i, entry.offset, err)
		}
		gotChange, _, err := readStateDomainChangeBinaryRecordAtBounded(r, entry.offset, r.UncompressedSize())
		if err != nil {
			t.Fatalf("compressed record %d @%d: %v", i, entry.offset, err)
		}
		wantEnc, _ := encodeStateDomainChangeRecord(wantChange)
		gotEnc, _ := encodeStateDomainChangeRecord(gotChange)
		if !bytes.Equal(wantEnc, gotEnc) {
			t.Fatalf("record %d @%d differs between raw and compressed read", i, entry.offset)
		}
	}

	// And the index offsets (tx-range entry points) resolve identically too.
	for i, e := range index {
		wantChange, _, err := readStateDomainChangeBinaryRecordAtBounded(raw, e.offset, rawSize)
		if err != nil {
			t.Fatalf("raw index record %d: %v", i, err)
		}
		gotChange, _, err := readStateDomainChangeBinaryRecordAtBounded(r, e.offset, r.UncompressedSize())
		if err != nil {
			t.Fatalf("compressed index record %d: %v", i, err)
		}
		if gotChange.TxNum != wantChange.TxNum || gotChange.TxNum != e.txNum {
			t.Fatalf("index record %d txNum mismatch", i)
		}
	}

	// The point of it all: the compressed segment is smaller on disk.
	st, err := os.Stat(segPath)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() >= int64(len(segmentData)) {
		t.Fatalf("compressed seg %d not smaller than raw %d", st.Size(), len(segmentData))
	}
	t.Logf("history segment %d -> %d bytes (%.2fx), %d records",
		len(segmentData), st.Size(), float64(len(segmentData))/float64(st.Size()), len(accessor))
}

// TestCompressedHistorySegmentFullReadAndCheck proves the whole-segment readers
// and the integrity checkers accept a compressed segment: the full read
// decompresses + decodes byte-identically, and both the seg and .kv checkers pass
// (checksum over physical bytes, structure over the logical view).
func TestCompressedHistorySegmentFullReadAndCheck(t *testing.T) {
	changes := buildHistoryStructs(200, 40)
	from, to := uint64(9_000_000), uint64(9_000_000+199)
	baseRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: from,
		ToTxNum:   to,
		Path:      "sdc.seg",
	}
	dir := t.TempDir()
	segC, idxC, accC, err := writeStateDomainChangeBinaryCompressedSegmentFiles(dir, baseRef, changes)
	if err != nil {
		t.Fatalf("write compressed: %v", err)
	}

	got, err := readStateDomainChangeBinarySegment(dir, segC)
	if err != nil {
		t.Fatalf("full read of compressed seg: %v", err)
	}
	normalized := normalizeStateDomainChangesForBinary(changes)
	if len(got) != len(normalized) {
		t.Fatalf("full read count %d, want %d", len(got), len(normalized))
	}
	for i := range got {
		wEnc, _ := encodeStateDomainChangeRecord(normalized[i])
		gEnc, _ := encodeStateDomainChangeRecord(got[i])
		if !bytes.Equal(wEnc, gEnc) {
			t.Fatalf("full read record %d mismatch", i)
		}
	}

	if err := checkStateDomainChangeBinarySegment(dir, segC); err != nil {
		t.Fatalf("seg checker rejected compressed segment: %v", err)
	}
	if err := checkStateDomainChangeBinaryAccessor(dir, accC); err != nil {
		t.Fatalf("accessor checker rejected compressed .kv: %v", err)
	}
	_ = idxC
}

// TestCompressedHistorySegmentProductionReadPaths writes a real cold segment both
// uncompressed and compressed, then drives the ACTUAL production read functions
// (range via the .idx, keyed via the .kv) over each — asserting the compressed
// segment yields byte-identical records through the routed openHistorySegmentForRead
// seam, while being smaller on disk.
func TestCompressedHistorySegmentProductionReadPaths(t *testing.T) {
	changes := buildHistoryStructs(300, 50)
	from, to := uint64(9_000_000), uint64(9_000_000+299)
	baseRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: from,
		ToTxNum:   to,
		Path:      "sdc.seg",
	}

	dirU, dirC := t.TempDir(), t.TempDir()
	segU, idxU, accU, err := writeStateDomainChangeBinaryFilesWithAccessor(dirU, baseRef, changes)
	if err != nil {
		t.Fatalf("write uncompressed: %v", err)
	}
	segC, idxC, accC, err := writeStateDomainChangeBinaryCompressedSegmentFiles(dirC, baseRef, changes)
	if err != nil {
		t.Fatalf("write compressed: %v", err)
	}
	if segC.Size >= segU.Size {
		t.Fatalf("compressed seg %d not smaller than uncompressed %d", segC.Size, segU.Size)
	}
	t.Logf("cold seg uncompressed %d -> compressed %d (%.2fx)", segU.Size, segC.Size, float64(segU.Size)/float64(segC.Size))
	// The honest archive number is the whole trio (.seg+.idx+.kv); the writer now
	// compresses .kv too, so this should be well above the seg-only 1.67x.
	trioU := segU.Size + idxU.Size + accU.Size
	trioC := segC.Size + idxC.Size + accC.Size
	if trioC >= trioU {
		t.Fatalf("compressed trio %d not smaller than uncompressed %d", trioC, trioU)
	}
	t.Logf("cold trio (seg+idx+kv) uncompressed %d -> compressed %d (%.2fx)", trioU, trioC, float64(trioU)/float64(trioC))

	collectRange := func(dir string, seg, idx SegmentRef) [][]byte {
		var got [][]byte
		if err := iterateStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, seg, idx, from, to, func(ch *rawdb.StateDomainChange) (bool, error) {
			enc, _ := encodeStateDomainChangeRecord(ch)
			got = append(got, enc)
			return true, nil
		}); err != nil {
			t.Fatalf("range read: %v", err)
		}
		return got
	}
	gotU, gotC := collectRange(dirU, segU, idxU), collectRange(dirC, segC, idxC)
	if len(gotU) != len(gotC) || len(gotU) != len(changes) {
		t.Fatalf("range counts u=%d c=%d want %d", len(gotU), len(gotC), len(changes))
	}
	for i := range gotU {
		if !bytes.Equal(gotU[i], gotC[i]) {
			t.Fatalf("range record %d differs between compressed and uncompressed", i)
		}
	}

	normalized := normalizeStateDomainChangesForBinary(changes)
	lookupKey := stateDomainChangeBinaryAccessorKey(normalized[len(normalized)/2])
	collectKeyed := func(dir string, seg, acc SegmentRef) [][]byte {
		var got [][]byte
		if err := iterateStateDomainChangeBinarySegmentByAccessorFile(dir, seg, acc, lookupKey, from, to, func(ch *rawdb.StateDomainChange) (bool, error) {
			enc, _ := encodeStateDomainChangeRecord(ch)
			got = append(got, enc)
			return true, nil
		}); err != nil {
			t.Fatalf("keyed read: %v", err)
		}
		return got
	}
	kU, kC := collectKeyed(dirU, segU, accU), collectKeyed(dirC, segC, accC)
	if len(kU) == 0 {
		t.Fatal("keyed read returned nothing for a present key")
	}
	if len(kU) != len(kC) {
		t.Fatalf("keyed counts u=%d c=%d", len(kU), len(kC))
	}
	for i := range kU {
		if !bytes.Equal(kU[i], kC[i]) {
			t.Fatalf("keyed record %d differs between compressed and uncompressed", i)
		}
	}
}
