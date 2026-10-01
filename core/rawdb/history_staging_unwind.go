package rawdb

import (
	"bytes"
	"context"
	"errors"

	"github.com/tronprotocol/go-tron/core/pointread"
)

// IsSourceOwnedUnpruned is a deliberately strict incremental-unwind proof.
// Old heights without atomic per-block completeness receipts return false;
// StateTxRange existence alone is never treated as payload coverage.
func (m *HistoryStagingManager) IsSourceOwnedUnpruned(first, last uint64) (bool, error) {
	if m == nil || first == 0 || last < first {
		return false, errors.New("rawdb: invalid staged unwind range")
	}
	if last-first >= 4096 {
		return false, nil
	}
	if intent, present, err := m.ReadResetIntent(); err != nil {
		return false, err
	} else if present && !intent.Complete {
		return false, ErrHistoryStagingResetting
	}
	epoch, err := m.CurrentEpoch()
	if err != nil {
		return false, err
	}
	snapshot, err := m.hot.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return false, err
	}
	defer snapshot.Close()
	prunedTx, hasPrune, err := ReadStageProgress(snapshot, StageSnapshotHotPrune)
	if err != nil {
		return false, err
	}
	firstRange, hasFirstRange, err := ReadStateTxRange(snapshot, first)
	if err != nil {
		return false, err
	}
	if !hasFirstRange || (hasPrune && firstRange.BeginTxNum <= prunedTx) {
		return false, nil
	}
	for bucket := first / StateHistoryChunkBucketBlocks; bucket <= last/StateHistoryChunkBucketBlocks; bucket++ {
		var route HistoryStagingRoute
		present, err := readHistoryStagingValue(snapshot, historyStagingBucketKey(historyStagingRoutePrefix, bucket), &route)
		if err != nil {
			return false, err
		}
		if !present || route.Version != HistoryStagingFormatVersion || route.Epoch != epoch || route.Owner != HistoryStagingOwnerSource {
			return false, nil
		}
		if _, present, err := readPresentValue(snapshot, historyStagingBucketKey(historyStagingClaimPrefix, bucket), "staging claim"); err != nil {
			return false, err
		} else if present {
			return false, nil
		}
		if binding, present, err := m.ReadColdBindingAt(epoch, bucket); err != nil {
			return false, err
		} else if present && len(binding.Spans) != 0 {
			return false, nil
		}
	}
	for block := first; block <= last; block++ {
		receipt, present, err := ReadHistoryStagingBlockComplete(snapshot, epoch, block)
		if err != nil {
			return false, err
		}
		if !present {
			return false, nil
		}
		tx, present, err := ReadStateTxRange(snapshot, block)
		if err != nil {
			return false, err
		}
		if !present || tx.BlockHash != receipt.BlockHash || tx.BeginTxNum != receipt.BeginTxNum || tx.EndTxNum != receipt.EndTxNum {
			return false, nil
		}
		h := NewHistoryStagingBlockHasher(block)
		it := snapshot.NewIterator(stateChangeSetBlockPrefix(block), nil)
		for it.Next() {
			if !bytes.HasPrefix(it.Key(), stateChangeSetBlockPrefix(block)) || len(it.Key()) != len(stateChangeSetPrefix)+16 {
				it.Release()
				return false, ErrHistoryStagingConflict
			}
			if err := h.Add(it.Key(), it.Value()); err != nil {
				it.Release()
				return false, err
			}
		}
		err = it.Error()
		it.Release()
		if err != nil {
			return false, err
		}
		computed, err := h.Finish(receipt.BlockHash, epoch, receipt.BeginTxNum, receipt.EndTxNum)
		if err != nil {
			return false, err
		}
		if computed.PhysicalRows != receipt.PhysicalRows || computed.PayloadDigest != receipt.PayloadDigest {
			return false, nil
		}
		if block == ^uint64(0) {
			break
		}
	}
	view := &stateHistoryReadView{reader: snapshot, iteratee: snapshot, pinned: true}
	lastRange, present, err := ReadStateTxRange(snapshot, last)
	if err != nil || !present {
		return false, err
	}
	if err := IterateStateHistorySpanBlocks(context.Background(), view, first, last, firstRange.BeginTxNum, lastRange.EndTxNum, func(*StateHistorySpanBlock) (bool, error) {
		return true, nil
	}); err != nil {
		return false, err
	}
	return true, nil
}
