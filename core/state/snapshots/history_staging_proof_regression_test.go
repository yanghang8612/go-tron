package snapshots

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// This fixture puts several changes in each of 1024 distinct transaction
// ranges. Re-reading the header and binary-searching the table per record is
// measurably different from advancing one range cursor per transaction.
func stagingDenseColdFixture(tb testing.TB, format string, rowsPerBlock int) (string, *Manifest, []rawdb.HistoryStagingBlockProof) {
	tb.Helper()
	dir := tb.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	changes := make([]*rawdb.StateDomainChange, 0, len(blocks)*rowsPerBlock)
	for i := range blocks {
		number := uint64(i + 1024)
		hash := common.Hash{byte(number >> 8), byte(number)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash, BeginTxNum: number, EndTxNum: number}
		ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash, BeginTxNum: number, EndTxNum: number}
		for row := 0; row < rowsPerBlock; row++ {
			change := binaryStateDomainChange(number, number, uint64(row+1), fmt.Sprintf("dense-%04d-%03d", i, row))
			change.BlockHash = hash
			changes = append(changes, change)
		}
	}
	var refs []SegmentRef
	switch format {
	case "v5":
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum,
			Path: stateDomainChangeHistorySegmentPath(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum)}
		history, index, accessor, err := writeHistorySegmentFiles(dir, ref, changes, ranges)
		if err != nil {
			tb.Fatal(err)
		}
		refs = []SegmentRef{history, index, accessor}
	case "v6", "reference":
		refs = writeV6StateDomainHistorySegmentForTest(tb, dir, blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum, changes)
		if format == "reference" {
			var err error
			refs, _, err = ReencodeHistoryReferenceTrioContext(context.Background(), dir, refs)
			if err != nil {
				tb.Fatal(err)
			}
		}
	default:
		tb.Fatalf("unknown format %q", format)
	}
	manifest := NewManifestForChain(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum,
		refs, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	if err := manifest.ValidateProduction(); err != nil {
		tb.Fatal(err)
	}
	return dir, manifest, blocks
}

type stagingReadCounter struct {
	historySegmentReader
	reads       uint64
	cancel      context.CancelFunc
	cancelAfter uint64
}

func (r *stagingReadCounter) ReadAt(p []byte, off int64) (int, error) {
	r.reads++
	if r.cancel != nil && r.reads == r.cancelAfter {
		r.cancel()
	}
	return r.historySegmentReader.ReadAt(p, off)
}

// The reference decoder is the pre-fix path: a cancellable wrapper around a
// contextual reader, passed to the per-record header/range lookup API.
func stagingLegacyWrappedRecords(ctx context.Context, open *historyStagingOpenTrio) ([][32]byte, error) {
	history := contextReaderAt{ctx: ctx, r: open.history}
	index := contextReaderAt{ctx: ctx, r: open.index}
	var records [][32]byte
	var previousTx, ordinal uint64
	var havePrevious bool
	for i := uint64(0); i < open.indexHeader.count; i++ {
		entry, err := readStateDomainChangeBinaryIndexEntryAt(index, i)
		if err != nil {
			return nil, err
		}
		offset := entry.offset
		for j := uint64(0); j < entry.count; j++ {
			change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(history, offset, open.historySize, entry.recordIndex+j)
			if err != nil {
				return nil, err
			}
			if change.TxNum != entry.txNum {
				return nil, errors.New("legacy reference index mismatch")
			}
			if !havePrevious || change.TxNum != previousTx {
				ordinal = 0
			} else {
				ordinal++
			}
			previousTx, havePrevious = change.TxNum, true
			records = append(records, historyStagingRecordDigest(change, ordinal))
			offset = next
		}
	}
	return records, nil
}

func stagingCountedProver(tb testing.TB, format string, rowsPerBlock int) (*HistoryStagingColdProver, SegmentRef, [32]byte, []rawdb.HistoryStagingBlockProof, *stagingReadCounter) {
	tb.Helper()
	dir, manifest, blocks := stagingDenseColdFixture(tb, format, rowsPerBlock)
	p, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		tb.Fatal(err)
	}
	p.EnableReaderReuse()
	needed := make([]bool, len(blocks))
	for i := range needed {
		needed[i] = true
	}
	if _, err := p.Build(context.Background(), blocks, needed); err != nil {
		_ = p.Close()
		tb.Fatal(err)
	}
	ref := p.open.refs[0]
	id := p.open.id
	reader, ok := p.open.history.(*stateDomainChangeHistoryReader)
	if !ok {
		_ = p.Close()
		tb.Fatalf("history reader type %T", p.open.history)
	}
	counter := &stagingReadCounter{historySegmentReader: reader.historySegmentReader}
	reader.historySegmentReader = counter
	return p, ref, id, blocks, counter
}

func TestHistoryStagingColdProofCursorMatchesLegacyWrappedDecoder(t *testing.T) {
	for _, format := range []string{"v5", "v6", "reference"} {
		t.Run(format, func(t *testing.T) {
			p, ref, id, blocks, counter := stagingCountedProver(t, format, 8)
			defer p.Close()
			fast, err := p.collectSpanRecords(context.Background(), ref, id, blocks)
			if err != nil {
				t.Fatal(err)
			}
			fastReads := counter.reads
			counter.reads = 0
			legacy, err := stagingLegacyWrappedRecords(context.Background(), p.open)
			if err != nil {
				t.Fatal(err)
			}
			legacyReads := counter.reads
			if len(fast) != len(legacy) || len(fast) != len(blocks)*8 {
				t.Fatalf("row counts fast=%d legacy=%d", len(fast), len(legacy))
			}
			_, got := historyStagingSemanticDigest(blocks, fast)
			_, want := historyStagingSemanticDigest(blocks, legacy)
			if got != want {
				t.Fatal("cursor changed V5/V6/reference semantic digest")
			}
			if legacyReads <= fastReads*2 {
				t.Fatalf("wrapped decoder reads=%d, cursor proof reads=%d: missing per-record regression signal", legacyReads, fastReads)
			}
			t.Logf("format=%s cursor history ReadAt=%d legacy wrapped ReadAt=%d rows=%d", format, fastReads, legacyReads, len(fast))
		})
	}
}

func TestHistoryStagingColdProofCursorCancelsDuringRead(t *testing.T) {
	p, ref, id, blocks, counter := stagingCountedProver(t, "reference", 4)
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	counter.reads = 0
	counter.cancel, counter.cancelAfter = cancel, 100
	if _, err := p.collectSpanRecords(ctx, ref, id, blocks); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-scan cancellation = %v", err)
	}
}

func TestHistoryStagingFullAuthIndexCoverageUsesForwardRangeCursor(t *testing.T) {
	for _, format := range []string{"v5", "v6"} {
		t.Run(format, func(t *testing.T) {
			p, _, _, blocks, counter := stagingCountedProver(t, format, 8)
			defer p.Close()
			open := p.open
			history := contextReaderAt{ctx: context.Background(), r: open.history}
			index := contextReaderAt{ctx: context.Background(), r: open.index}
			recordOffset, err := validateStateDomainChangeBinaryTxRangeTableAt(history, open.historySize, open.refs[0], open.header)
			if err != nil {
				t.Fatal(err)
			}
			var verified [][32]byte
			var priorTx, ordinal uint64
			var havePrior bool
			counter.reads = 0
			err = verifyStateDomainChangeBinaryIndexCoverageWithVisitor(open.refs[0], open.refs[1], history,
				open.historySize, recordOffset, open.header, index, open.indexHeader.count,
				func(change *rawdb.StateDomainChange, _, _ uint64) error {
					if !havePrior || change.TxNum != priorTx {
						ordinal = 0
					} else {
						ordinal++
					}
					priorTx, havePrior = change.TxNum, true
					verified = append(verified, historyStagingRecordDigest(change, ordinal))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			fastReads := counter.reads
			counter.reads = 0
			legacy, err := stagingLegacyWrappedRecords(context.Background(), open)
			if err != nil {
				t.Fatal(err)
			}
			legacyReads := counter.reads
			if len(verified) != len(blocks)*8 || len(verified) != len(legacy) {
				t.Fatalf("verified=%d legacy=%d", len(verified), len(legacy))
			}
			_, got := historyStagingSemanticDigest(blocks, verified)
			_, want := historyStagingSemanticDigest(blocks, legacy)
			if got != want {
				t.Fatal("index coverage changed hydrated record semantics")
			}
			if legacyReads <= fastReads*2 {
				t.Fatalf("full auth reads=%d vs legacy decoder=%d", fastReads, legacyReads)
			}
			t.Logf("format=%s full auth history ReadAt=%d legacy wrapped ReadAt=%d", format, fastReads, legacyReads)
		})
	}
}

func BenchmarkHistoryStagingColdProofCursorReads(b *testing.B) {
	for _, format := range []string{"v5", "reference"} {
		b.Run(format, func(b *testing.B) {
			p, ref, id, blocks, counter := stagingCountedProver(b, format, 8)
			defer p.Close()
			for _, path := range []string{"cursor", "legacy-wrapped"} {
				b.Run(path, func(b *testing.B) {
					counter.reads = 0
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var err error
						if path == "cursor" {
							_, err = p.collectSpanRecords(context.Background(), ref, id, blocks)
						} else {
							_, err = stagingLegacyWrappedRecords(context.Background(), p.open)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(counter.reads)/float64(b.N), "history-readat/op")
				})
			}
		})
	}
}

var _ io.ReaderAt = (*stagingReadCounter)(nil)
