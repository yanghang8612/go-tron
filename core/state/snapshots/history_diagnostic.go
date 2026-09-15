package snapshots

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// BuildDiagnosticStateHistoryTrioContext uses the complete production bounded
// builder with an explicit compression policy (auto, 2 or 3). It only creates an immutable
// trio; publication, metadata registration, pruning and lifecycle work are absent.
func BuildDiagnosticStateHistoryTrioContext(ctx context.Context, db ethdb.Iteratee, dir string, fromTx, toTx, fromBlock, toBlock uint64, path, format string) ([]SegmentRef, error) {
	return BuildDiagnosticStateHistoryTrioWithETLContext(ctx, db, dir, fromTx, toTx, fromBlock, toBlock, path, format, etl.Options{})
}

// BuildDiagnosticStateHistoryTrioWithETLContext is the same offline-only full
// builder with explicit collector thresholds. BufferLimit applies separately
// to each collector; it is not a heap/RSS limit and excludes codec/key tables.
// A parallel diagnostic must divide its aggregate threshold across workers and
// both key/posting collectors. The ordinary diagnostic defaults are unchanged.
func BuildDiagnosticStateHistoryTrioWithETLContext(ctx context.Context, db ethdb.Iteratee, dir string, fromTx, toTx, fromBlock, toBlock uint64, path, format string, opts etl.Options) ([]SegmentRef, error) {
	return BuildDiagnosticStateHistoryTrioWithReadPipelineContext(ctx, db, dir, fromTx, toTx, fromBlock, toBlock, path, format, opts, false)
}

// BuildDiagnosticStateHistoryTrioWithReadPipelineContext enables only the
// explicit offline shared-block authentication experiment. It preserves both
// ordered passes and one output trio. False retains the normal serial reader;
// true requires an audited concurrent pinned owned view and never falls back
// silently. No production Runner enables this option.
func BuildDiagnosticStateHistoryTrioWithReadPipelineContext(ctx context.Context, db ethdb.Iteratee, dir string, fromTx, toTx, fromBlock, toBlock uint64, path, format string, opts etl.Options, pipeline bool) ([]SegmentRef, error) {
	return BuildDiagnosticStateHistoryTrioWithReadWorkersContext(ctx, db, dir, fromTx, toTx, fromBlock, toBlock, path, format, opts, pipeline, 2)
}

// BuildDiagnosticStateHistoryTrioWithReadWorkersContext explicitly selects
// 2/4/8 offline authentication slots, keeping the shared output budget fixed.
// Pipeline disabled permits only the default two-slot setting (unused).
func BuildDiagnosticStateHistoryTrioWithReadWorkersContext(ctx context.Context, db ethdb.Iteratee, dir string, fromTx, toTx, fromBlock, toBlock uint64, path, format string, opts etl.Options, pipeline bool, workers int) ([]SegmentRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if workers != 2 && workers != 4 && workers != 8 || !pipeline && workers != 2 {
		return nil, rawdb.ErrStateHistoryPipelineWorkers
	}
	if format != "auto" && format != "2" && format != "3" {
		return nil, errors.New("snapshots: diagnostic compression policy must be auto, 2 or 3")
	}
	if !CompressHistorySegments {
		return nil, errors.New("snapshots: diagnostic requires compression enabled")
	}
	if db == nil || toTx < fromTx || toBlock < fromBlock || !isStateDomainChangeBinarySegmentPath(path) {
		return nil, errors.New("snapshots: invalid diagnostic history range or path")
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	changes := cfg.IterateHotHistoryBlockTxBorrowed
	if pipeline {
		changes = func(db ethdb.Iteratee, fromBlock, toBlock, fromTx, toTx uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
			return rawdb.IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(ctx, db, fromBlock, toBlock, fromTx, toTx, workers, fn)
		}
	}
	cfg.IterateHotHistoryBlockTxBorrowed = func(db ethdb.Iteratee, fromBlock, toBlock, fromTx, toTx uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
		return changes(db, fromBlock, toBlock, fromTx, toTx, func(row *rawdb.StateDomainChange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return fn(row)
		})
	}
	ranges := cfg.IterateHotHistoryTxRangeBorrowed
	cfg.IterateHotHistoryTxRangeBorrowed = func(db ethdb.Iteratee, from, to uint64, fn func(*rawdb.StateTxRange) (bool, error)) error {
		return ranges(db, from, to, func(row *rawdb.StateTxRange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return fn(row)
		})
	}
	result, err := buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContextFormat(ctx, db, dir,
		SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: fromTx, ToTxNum: toTx, Path: path},
		cfg, opts, &stateDomainChangeHistoryBlockRange{from: fromBlock, to: toBlock}, format)
	return result.refs, err
}

// StateHistoryDiagnosticDigest describes every archival logical row in order,
// and every complete block/tx range. Next is transient; physical Seq is replaced
// by cold format's stable ordinal. Neither is an archival payload. The row hash
// includes block/hash/tx/domain/owner/generation/key/presence/previous value.
type StateHistoryDiagnosticDigest struct {
	Rows                uint64 `json:"rows"`
	PayloadBytes        uint64 `json:"payload_bytes"`
	PrevBytes           uint64 `json:"prev_bytes"`
	MaxPrevBytes        uint64 `json:"max_prev_bytes"`
	LargePrevRows       uint64 `json:"large_prev_rows_ge_128kib"`
	LargePrevBytes      uint64 `json:"large_prev_bytes_ge_128kib"`
	DelegationRows      uint64 `json:"delegation_rows"`
	DelegationPrevBytes uint64 `json:"delegation_prev_bytes"`
	RowSHA256           string `json:"row_sha256"`
	TxRanges            uint64 `json:"tx_ranges"`
	TxRangeSHA256       string `json:"tx_range_sha256"`
}

func (d *StateHistoryDiagnosticDigest) addRow(row *rawdb.StateDomainChange) {
	previous := uint64(len(row.Prev))
	d.Rows++
	d.PayloadBytes += uint64(len(row.Key)) + previous
	d.PrevBytes += previous
	d.MaxPrevBytes = max(d.MaxPrevBytes, previous)
	if previous >= 128<<10 {
		d.LargePrevRows++
		d.LargePrevBytes += previous
	}
	if row.FlatDomain == rawdb.StateFlatDomainKVLatest && row.Domain == kvdomains.SystemDelegation {
		d.DelegationRows++
		d.DelegationPrevBytes += previous
	}
}

func historyDiagnosticUint(h hash.Hash, n uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	h.Write(b[:])
}
func historyDiagnosticBytes(h hash.Hash, b []byte) {
	historyDiagnosticUint(h, uint64(len(b)))
	h.Write(b)
}
func historyDiagnosticRow(row *rawdb.StateDomainChange) []byte {
	h := sha256.New()
	historyDiagnosticUint(h, row.BlockNum)
	h.Write(row.BlockHash[:])
	historyDiagnosticUint(h, row.TxNum)
	historyDiagnosticUint(h, uint64(row.FlatDomain))
	h.Write(row.Owner[:])
	historyDiagnosticUint(h, row.Generation)
	historyDiagnosticUint(h, uint64(row.Domain))
	historyDiagnosticBytes(h, row.Key)
	if row.PrevExists {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}
	historyDiagnosticBytes(h, row.Prev)
	return h.Sum(nil)
}
func historyDiagnosticRange(h hash.Hash, row *rawdb.StateTxRange) {
	historyDiagnosticUint(h, row.BlockNum)
	h.Write(row.BlockHash[:])
	historyDiagnosticUint(h, row.BeginTxNum)
	historyDiagnosticUint(h, row.EndTxNum)
}

// DigestHotStateHistoryContext authenticates shared packs through the normal
// readers, then uses bounded ETL to preserve the production tx/sequence order.
// Like the production borrowed path it rejects legacy/repair rows rather than
// silently selecting a different reader. It retains no batch of decoded values.
// ETL and one source pack bound memory; scratch scales with the exported input.
func DigestHotStateHistoryContext(ctx context.Context, db ethdb.Iteratee, scratch string, fromTx, toTx, fromBlock, toBlock, maxPayload uint64) (out StateHistoryDiagnosticDigest, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, release()) }()
	collector, err := etl.NewCollector(etl.Options{TempDir: scratch})
	if err != nil {
		return out, err
	}
	defer collector.Close()
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	br := &stateDomainChangeHistoryBlockRange{from: fromBlock, to: toBlock}
	err = iterateStateDomainChangeHistoryChanges(view, cfg, fromTx, toTx, br, func(row *rawdb.StateDomainChange) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if row == nil {
			return false, errors.New("snapshots: nil diagnostic source row")
		}
		payload := uint64(len(row.Key)) + uint64(len(row.Prev))
		if payload > maxPayload-out.PayloadBytes {
			return false, errors.New("snapshots: diagnostic logical payload budget exceeded")
		}
		// Full production sort key includes a unique ordinal, preserving duplicate
		// rows instead of Collector's usual duplicate-key replacement.
		err := collector.PutOwned(stateDomainChangeHistoryRecordETLSortKey(row, out.Rows), historyDiagnosticRow(row))
		out.addRow(row)
		return err == nil, err
	})
	if err != nil {
		return out, err
	}
	h := sha256.New()
	_, err = collector.LoadInterruptible(historyDiagnosticHashWriter{h}, func() bool { return ctx.Err() != nil })
	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	if err != nil {
		return out, err
	}
	out.RowSHA256 = hex.EncodeToString(h.Sum(nil))
	h.Reset()
	err = iterateStateDomainChangeHistoryTxRanges(view, cfg, fromTx, toTx, br, func(row *rawdb.StateTxRange) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		historyDiagnosticRange(h, row)
		out.TxRanges++
		return nil
	})
	if err != nil {
		return out, err
	}
	out.TxRangeSHA256 = hex.EncodeToString(h.Sum(nil))
	return out, ctx.Err()
}

type historyDiagnosticHashWriter struct{ hash.Hash }

func (w historyDiagnosticHashWriter) Put(_, value []byte) error { _, err := w.Write(value); return err }
func (w historyDiagnosticHashWriter) Delete([]byte) error {
	return errors.New("snapshots: deletion in diagnostic digest")
}

// DigestColdStateHistoryContext first authenticates all three files and their
// complete index/accessor coverage, then streams every history row and tx range.
// Sequential history uses a one-block compressed cache, never Open's large slice.
// Its work is deliberately outside the timed trio build.
func DigestColdStateHistoryContext(ctx context.Context, dir string, refs []SegmentRef) (out StateHistoryDiagnosticDigest, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(refs) != 3 {
		return out, errors.New("snapshots: diagnostic requires exactly one trio")
	}
	var ref SegmentRef
	for _, candidate := range refs {
		if candidate.Kind == SegmentHistory {
			if ref.Path != "" {
				return out, errors.New("snapshots: duplicate diagnostic history")
			}
			ref = candidate
		}
	}
	if ref.Path == "" {
		return out, errors.New("snapshots: missing diagnostic history")
	}
	if err := VerifyHistorySegmentWithCompanionsContext(ctx, dir, &Manifest{Segments: refs}, ref); err != nil {
		return out, err
	}
	reader, header, size, err := openStateDomainChangeBinarySegmentSequentialReader(dir, ref)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	r := contextReaderAt{ctx: ctx, r: reader}
	h := sha256.New()
	_, err = iterateStateDomainChangeBinaryTxRangeTableAt(r, size, ref, header, func(row *rawdb.StateTxRange) (bool, error) {
		historyDiagnosticRange(h, row)
		out.TxRanges++
		return true, ctx.Err()
	})
	if err != nil {
		return out, err
	}
	out.TxRangeSHA256 = hex.EncodeToString(h.Sum(nil))
	h.Reset()
	err = iterateStateDomainChangeBinaryRecords(r, size, func(_ uint64, row *rawdb.StateDomainChange) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		h.Write(historyDiagnosticRow(row))
		out.addRow(row)
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("snapshots: diagnostic cold rows: %w", err)
	}
	out.RowSHA256 = hex.EncodeToString(h.Sum(nil))
	return out, ctx.Err()
}
