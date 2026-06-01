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
