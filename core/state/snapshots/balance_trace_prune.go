package snapshots

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type PruneHotBalanceTraceResult struct {
	HasRange             bool
	FromBlock            uint64
	ToBlock              uint64
	BlockTracesDeleted   uint64
	AccountTracesDeleted uint64
	ColdTraceSegments    uint64

	hasProcessedToBlock bool
	processedToBlock    uint64
}

type balanceTracePruneBounds struct {
	lastPrunedBlock    uint64
	hasLastPrunedBlock bool
}

// PruneHotBalanceTraces deletes hot account/balance trace rows only after a
// registered cold balance-trace segment has been verified, every block in the
// processed segment range has a cold block trace, and every hot row in the
// segment range has an exact cold match.
func PruneHotBalanceTraces(db ethdb.KeyValueStore, dir string, manifest *Manifest) (*PruneHotBalanceTraceResult, error) {
	return pruneHotBalanceTraces(db, dir, manifest, balanceTracePruneBounds{})
}

// PruneHotBalanceTracesWithProgress prunes covered hot account/balance trace
// rows after the persisted StageSnapshotBalanceTracePrune boundary and advances
// that boundary when a densely covered cold segment is processed.
func PruneHotBalanceTracesWithProgress(db ethdb.KeyValueStore, dir string, manifest *Manifest) (*PruneHotBalanceTraceResult, error) {
	if db == nil {
		return nil, errors.New("snapshots: nil balance trace prune database")
	}
	lastPrunedBlock, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBalanceTracePrune)
	if err != nil {
		return nil, err
	}
	result, err := pruneHotBalanceTraces(db, dir, manifest, balanceTracePruneBounds{
		lastPrunedBlock:    lastPrunedBlock,
		hasLastPrunedBlock: ok,
	})
	if err != nil {
		return nil, err
	}
	if result.hasProcessedToBlock {
		if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotBalanceTracePrune, result.processedToBlock); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func pruneHotBalanceTraces(db ethdb.KeyValueStore, dir string, manifest *Manifest, bounds balanceTracePruneBounds) (*PruneHotBalanceTraceResult, error) {
	if db == nil {
		return nil, errors.New("snapshots: nil balance trace prune database")
	}
	if manifest == nil {
		return nil, errors.New("snapshots: nil manifest")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	result := new(PruneHotBalanceTraceResult)
	for _, ref := range balanceTraceRefsAscending(manifest) {
		if bounds.hasLastPrunedBlock && ref.ToTxNum <= bounds.lastPrunedBlock {
			continue
		}
		if ref.ToTxNum > math.MaxInt64 {
			return nil, fmt.Errorf("snapshots: balance trace prune range [%d,%d] exceeds int64 block number range", ref.FromTxNum, ref.ToTxNum)
		}
		if err := CheckBalanceTraceSegment(dir, ref); err != nil {
			return nil, err
		}
		seg, err := OpenBalanceTraceSegment(dir, ref)
		if err != nil {
			return nil, err
		}
		pruneFromBlock := balanceTracePruneStartBlock(ref, bounds)
		denselyCovered, err := balanceTraceSegmentHasEveryBlock(seg, pruneFromBlock, ref.ToTxNum)
		if err == nil && !denselyCovered {
			err = fmt.Errorf("snapshots: cold balance trace segment %q missing block trace coverage [%d,%d]", ref.Path, pruneFromBlock, ref.ToTxNum)
		}
		var verified balanceTraceCoverageCheck
		if err == nil {
			verified, err = verifyHotBalanceTraceRowsCovered(db, seg, ref, bounds)
		}
		if closeErr := seg.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
		if verified.hasRows {
			seg, err = OpenBalanceTraceSegment(dir, ref)
			if err != nil {
				return nil, err
			}
			pruned, pruneErr := pruneHotBalanceTraceRowsForSegment(db, seg, ref, result, bounds)
			closeErr := seg.Close()
			if pruneErr != nil {
				return nil, pruneErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if pruned {
				result.ColdTraceSegments++
			}
		}
		markBalanceTraceProcessedToBlock(result, ref.ToTxNum)
	}
	return result, nil
}

func balanceTracePruneStartBlock(ref SegmentRef, bounds balanceTracePruneBounds) uint64 {
	if bounds.hasLastPrunedBlock && bounds.lastPrunedBlock >= ref.FromTxNum {
		return bounds.lastPrunedBlock + 1
	}
	return ref.FromTxNum
}

type balanceTraceCoverageCheck struct {
	hasRows bool
}

func verifyHotBalanceTraceRowsCovered(db ethdb.Iteratee, seg *BalanceTraceSegment, ref SegmentRef, bounds balanceTracePruneBounds) (balanceTraceCoverageCheck, error) {
	var out balanceTraceCoverageCheck
	if err := rawdb.IterateBlockBalanceTraceRows(db, int64(ref.FromTxNum), int64(ref.ToTxNum), func(blockNum int64, raw []byte) (bool, error) {
		if blockNum >= 0 && bounds.skipBlock(uint64(blockNum)) {
			return true, nil
		}
		coldRaw, ok, err := seg.blockBalanceTraceRaw(blockNum)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, fmt.Errorf("snapshots: cold balance trace segment %q missing hot block trace %d", ref.Path, blockNum)
		}
		if !bytes.Equal(raw, coldRaw) {
			return false, fmt.Errorf("snapshots: cold balance trace segment %q differs from hot block trace %d", ref.Path, blockNum)
		}
		out.hasRows = true
		return true, nil
	}); err != nil {
		return out, err
	}
	if err := rawdb.IterateAccountTraceRows(db, int64(ref.FromTxNum), int64(ref.ToTxNum), func(owner []byte, blockNum int64, balance int64) (bool, error) {
		if blockNum >= 0 && bounds.skipBlock(uint64(blockNum)) {
			return true, nil
		}
		if len(owner) != common.AddressLength {
			return false, fmt.Errorf("snapshots: account trace owner length %d, want %d", len(owner), common.AddressLength)
		}
		coldBlock, coldBalance, ok, err := seg.AccountTraceAtOrBefore(owner, blockNum)
		if err != nil {
			return false, err
		}
		if !ok || coldBlock != blockNum {
			return false, fmt.Errorf("snapshots: cold balance trace segment %q missing hot account trace owner=%x block=%d", ref.Path, owner, blockNum)
		}
		if coldBalance != balance {
			return false, fmt.Errorf("snapshots: cold balance trace segment %q account trace owner=%x block=%d balance %d, hot %d",
				ref.Path, owner, blockNum, coldBalance, balance)
		}
		out.hasRows = true
		return true, nil
	}); err != nil {
		return out, err
	}
	return out, nil
}

func pruneHotBalanceTraceRowsForSegment(db ethdb.KeyValueWriter, seg *BalanceTraceSegment, ref SegmentRef, result *PruneHotBalanceTraceResult, bounds balanceTracePruneBounds) (bool, error) {
	var pruned bool
	if err := seg.IterateBlockBalanceTraceRows(func(blockNum uint64, _ []byte) error {
		if blockNum < ref.FromTxNum || blockNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: balance trace segment %q block %d outside [%d,%d]", ref.Path, blockNum, ref.FromTxNum, ref.ToTxNum)
		}
		if bounds.skipBlock(blockNum) {
			return nil
		}
		if err := rawdb.DeleteBlockBalanceTrace(db, int64(blockNum)); err != nil {
			return err
		}
		markBalanceTracePrunedRange(result, blockNum)
		result.BlockTracesDeleted++
		pruned = true
		return nil
	}); err != nil {
		return false, err
	}
	if err := seg.IterateAccountTraceRows(func(owner common.Address, blockNum uint64, _ int64) error {
		if blockNum < ref.FromTxNum || blockNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: balance trace segment %q account block %d outside [%d,%d]", ref.Path, blockNum, ref.FromTxNum, ref.ToTxNum)
		}
		if bounds.skipBlock(blockNum) {
			return nil
		}
		if err := rawdb.DeleteAccountTrace(db, owner.Bytes(), int64(blockNum)); err != nil {
			return err
		}
		markBalanceTracePrunedRange(result, blockNum)
		result.AccountTracesDeleted++
		pruned = true
		return nil
	}); err != nil {
		return false, err
	}
	return pruned, nil
}

func (b balanceTracePruneBounds) skipBlock(blockNum uint64) bool {
	return b.hasLastPrunedBlock && blockNum <= b.lastPrunedBlock
}

func markBalanceTracePrunedRange(result *PruneHotBalanceTraceResult, blockNum uint64) {
	if result == nil {
		return
	}
	if !result.HasRange {
		result.HasRange = true
		result.FromBlock = blockNum
		result.ToBlock = blockNum
		return
	}
	if blockNum < result.FromBlock {
		result.FromBlock = blockNum
	}
	if blockNum > result.ToBlock {
		result.ToBlock = blockNum
	}
}

func markBalanceTraceProcessedToBlock(result *PruneHotBalanceTraceResult, blockNum uint64) {
	if result == nil {
		return
	}
	if !result.hasProcessedToBlock || blockNum > result.processedToBlock {
		result.hasProcessedToBlock = true
		result.processedToBlock = blockNum
	}
}

func balanceTraceRefsAscending(manifest *Manifest) []SegmentRef {
	refs := balanceTraceRefs(manifest)
	for i, j := 0, len(refs)-1; i < j; i, j = i+1, j-1 {
		refs[i], refs[j] = refs[j], refs[i]
	}
	return refs
}

func (s *BalanceTraceSegment) IterateBlockBalanceTraceRows(fn func(blockNum uint64, raw []byte) error) error {
	if s == nil || s.file == nil || fn == nil {
		return nil
	}
	for i := uint64(0); i < s.header.blockCount; i++ {
		entry, err := readBalanceTraceBlockIndexEntryAt(s.file, s.header.blockIndexOffset+i*balanceTraceBlockIndexEntrySize)
		if err != nil {
			return err
		}
		if err := validateBalanceTracePayloadBounds(s.header, entry); err != nil {
			return err
		}
		raw, err := readBalanceTracePayloadAt(s.file, entry.offset, entry.length)
		if err != nil {
			return err
		}
		if err := fn(entry.blockNum, raw); err != nil {
			return err
		}
	}
	return nil
}

func (s *BalanceTraceSegment) IterateAccountTraceRows(fn func(owner common.Address, blockNum uint64, balance int64) error) error {
	if s == nil || s.file == nil || fn == nil {
		return nil
	}
	for i := uint64(0); i < s.header.accountCount; i++ {
		entry, err := readBalanceTraceAccountIndexEntryAt(s.file, balanceTraceAccountEntryOffset(s.header, i))
		if err != nil {
			return err
		}
		if err := fn(entry.owner, entry.blockNum, entry.balance); err != nil {
			return err
		}
	}
	return nil
}

func (s *BalanceTraceSegment) blockBalanceTraceRaw(blockNum int64) ([]byte, bool, error) {
	if s == nil || s.file == nil || blockNum < 0 {
		return nil, false, nil
	}
	num := uint64(blockNum)
	if num < s.header.fromBlock || num > s.header.toBlock {
		return nil, false, nil
	}
	lo, hi := uint64(0), s.header.blockCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry, err := readBalanceTraceBlockIndexEntryAt(s.file, s.header.blockIndexOffset+mid*balanceTraceBlockIndexEntrySize)
		if err != nil {
			return nil, false, err
		}
		switch {
		case entry.blockNum == num:
			if err := validateBalanceTracePayloadBounds(s.header, entry); err != nil {
				return nil, false, err
			}
			raw, err := readBalanceTracePayloadAt(s.file, entry.offset, entry.length)
			if err != nil {
				return nil, false, err
			}
			return raw, true, nil
		case entry.blockNum < num:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return nil, false, nil
}
