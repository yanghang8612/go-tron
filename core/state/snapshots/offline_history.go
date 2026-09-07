package snapshots

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// InspectStateDomainHistoryVerificationContext reads only format metadata. The
// returned bytes bound semantic-verification ETL for every complete covered
// trio, including the portion outside an offline hot-prune batch. V7 writes
// 43 bytes per posting plus at most one 8-byte spill header per record. V6
// verifies with random reads and does not materialize an uncompressed file.
// Older formats use different scratch paths and are deliberately unsupported.
func InspectStateDomainHistoryVerificationContext(ctx context.Context, dir string, refs []SegmentRef) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	manifest := &Manifest{Segments: refs}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	var total uint64
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if ref.NormalizedDataset() != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
			continue
		}
		segment, _, header, err := openHistorySegmentForRead(dir, ref)
		if err != nil {
			return 0, err
		}
		if err := segment.Close(); err != nil {
			return 0, err
		}
		accessorRef, ok := cfg.HistoryAccessorRef(manifest, ref)
		if !ok {
			return 0, errors.New("snapshots: offline verification requires an accessor")
		}
		accessor, accessorHeader, _, err := openStateDomainChangeBinaryAccessorReader(dir, accessorRef)
		if err != nil {
			return 0, err
		}
		if err := accessor.Close(); err != nil {
			return 0, err
		}
		if header.version != stateDomainChangeBinaryVersionV6 ||
			(accessorHeader.version != stateDomainChangeBinaryVersionV6 && accessorHeader.version != stateDomainChangeBinaryVersionV7) ||
			header.count != accessorHeader.count || header.fromTxNum != accessorHeader.fromTxNum || header.toTxNum != accessorHeader.toTxNum {
			return 0, errors.New("snapshots: offline verification requires matching V6 history and V6/V7 accessor headers")
		}
		needed := uint64(1 << 20)
		if accessorHeader.version == stateDomainChangeBinaryVersionV7 {
			if header.count > (math.MaxUint64-needed)/51 {
				return 0, errors.New("snapshots: offline verification scratch overflows")
			}
			needed += header.count * 51
		}
		if needed > math.MaxUint64-total {
			return 0, errors.New("snapshots: offline verification scratch overflows")
		}
		total += needed
	}
	return total, nil
}

// VerifyOfflineHistoryBlockRangesContext compares retained canonical block
// ranges to authenticated cold tables, including blocks with zero changes.
// It uses bounded table lookups and never writes or materializes a segment.
func VerifyOfflineHistoryBlockRangesContext(ctx context.Context, dir string, refs []SegmentRef, expected []rawdb.StateTxRange) error {
	if ctx == nil {
		ctx = context.Background()
	}
	verified := make([]bool, len(expected))
	for _, ref := range refs {
		if ref.NormalizedDataset() != SegmentDatasetStateDomainChange || ref.Kind != SegmentHistory {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		segment, size, header, err := openHistorySegmentForRead(dir, ref)
		if err != nil {
			return err
		}
		err = func() error {
			defer segment.Close()
			for i, row := range expected {
				if row.EndTxNum < ref.FromTxNum || row.BeginTxNum > ref.ToTxNum {
					continue
				}
				cold, table, found, err := findStateDomainChangeBinaryTxRangeForBlock(contextReaderAt{ctx: ctx, r: segment}, size, ref, header, row.BlockNum)
				if err != nil {
					return err
				}
				if !table || !found || cold == nil || *cold != row || row.BeginTxNum < ref.FromTxNum || row.EndTxNum > ref.ToTxNum || verified[i] {
					return fmt.Errorf("snapshots: offline cold block range mismatch at block %d", row.BlockNum)
				}
				verified[i] = true
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	for i, ok := range verified {
		if !ok {
			return fmt.Errorf("snapshots: offline cold block range missing at block %d", expected[i].BlockNum)
		}
	}
	return ctx.Err()
}

// BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeContext writes exactly
// one immutable history trio. It does not publish a manifest, alter hot data,
// start a lifecycle, or build any latest/derived dataset. The caller must own
// the stopped datadir exclusively and authenticate the complete output before
// publishing it. Cancellation is observed in both source passes and ETL work.
func BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeContext(ctx context.Context, db ethdb.Iteratee, dir string, fromTxNum, toTxNum, fromBlock, toBlock uint64, relPath string, opts etl.Options) ([]SegmentRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil || toTxNum < fromTxNum || toBlock < fromBlock {
		return nil, errors.New("snapshots: invalid offline history source or range")
	}
	if !isStateDomainChangeBinarySegmentPath(relPath) {
		return nil, errors.New("snapshots: offline history requires the production binary format")
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok || cfg.IterateHotHistoryBlockTxBorrowed == nil || cfg.IterateHotHistoryTxRangeBorrowed == nil {
		return nil, errors.New("snapshots: offline history requires bounded source iterators")
	}
	changes := cfg.IterateHotHistoryBlockTxBorrowed
	cfg.IterateHotHistoryBlockTxBorrowed = func(db ethdb.Iteratee, fromBlock, toBlock, fromTx, toTx uint64, visit func(*rawdb.StateDomainChange) (bool, error)) error {
		return changes(db, fromBlock, toBlock, fromTx, toTx, func(change *rawdb.StateDomainChange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return visit(change)
		})
	}
	ranges := cfg.IterateHotHistoryTxRangeBorrowed
	cfg.IterateHotHistoryTxRangeBorrowed = func(db ethdb.Iteratee, from, to uint64, visit func(*rawdb.StateTxRange) (bool, error)) error {
		return ranges(db, from, to, func(row *rawdb.StateTxRange) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return visit(row)
		})
	}
	result, err := buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContext(ctx, db, dir, SegmentRef{
		Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: fromTxNum, ToTxNum: toTxNum, Path: relPath,
	}, cfg, opts, &stateDomainChangeHistoryBlockRange{from: fromBlock, to: toBlock})
	if err != nil {
		return nil, err
	}
	return result.refs, ctx.Err()
}

// SyncHistorySegmentDirectories makes immutable names durable before manifest
// publication. Syncing only the snapshot root cannot persist nested history/
// renames. An error leaves the output intact; the caller must not publish it.
func SyncHistorySegmentDirectories(dir string, refs []SegmentRef) error {
	seen := make(map[string]struct{})
	for _, ref := range refs {
		if ref.NormalizedDataset() != SegmentDatasetStateDomainChange {
			return fmt.Errorf("snapshots: cannot sync unrelated offline history ref %q", ref.Path)
		}
		if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
			return err
		}
		root := filepath.Clean(dir)
		for parent := filepath.Dir(filepath.Join(dir, ref.Path)); ; parent = filepath.Dir(parent) {
			if _, ok := seen[parent]; !ok {
				if err := syncSnapshotDir(parent); err != nil {
					return err
				}
				seen[parent] = struct{}{}
			}
			if parent == root {
				break
			}
		}
	}
	return syncSnapshotDir(dir)
}
