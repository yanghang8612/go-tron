package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// AdoptClaim publishes a target-owned route only after the target receipt is
// durable. The caller MUST keep the index→chain writer guard across its own
// renewed canonical/Finish/index/settled proof and this call. No core callback
// is invoked inside routeMu. A sync error forbids ClearSource until retry.
func (m *HistoryStagingManager) AdoptClaim(ctx context.Context, claim HistoryStagingClaim, proof HistoryStagingProof) (HistoryStagingRoute, error) {
	var zero HistoryStagingRoute
	if m == nil || ctx == nil {
		return zero, errors.New("rawdb: missing staging manager or context")
	}
	digest, err := proof.digest()
	if err != nil || digest != claim.ProofDigest || proof.Bucket != claim.Bucket || proof.Epoch != claim.Epoch {
		return zero, ErrHistoryStagingConflict
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.routeMu.Lock()
	if prior, hasPrior, err := m.ReadRoute(claim.Bucket); err == nil && hasPrior && prior.Owner == HistoryStagingOwnerTarget && prior.Epoch == claim.Epoch {
		receipt, hasReceipt, readErr := m.ReadReceiptAt(claim.Epoch, claim.Bucket)
		receiptDigest, digestErr := historyStagingReceiptDigest(receipt)
		m.routeMu.Unlock()
		if readErr != nil || digestErr != nil || !hasReceipt || receipt.ClaimID != claim.ClaimID || receipt.ProofDigest != digest || prior.ReceiptDigest != receiptDigest {
			return zero, ErrHistoryStagingConflict
		}
		if err := m.syncHot(); err != nil {
			return zero, err
		}
		return prior, nil
	} else if err != nil {
		m.routeMu.Unlock()
		return zero, err
	}
	current, present, err := m.ReadClaim(claim.Bucket)
	if err != nil || !present || current.Cancelled || current.ClaimID != claim.ClaimID || current.ProofDigest != digest || current.WriteVersion != claim.WriteVersion || !current.TargetReady || current.SourceDigest == ([32]byte{}) {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingConflict
	}
	if epoch, err := m.CurrentEpoch(); err != nil || epoch != claim.Epoch {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingConflict
	}
	if intent, hasIntent, err := m.ReadResetIntent(); err != nil || (hasIntent && !intent.Complete) {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingResetting
	}
	previous, hasRoute, err := m.ReadRoute(claim.Bucket)
	if err != nil || (hasRoute && (previous.Epoch != claim.Epoch || previous.Owner != HistoryStagingOwnerSource || previous.WriteVersion+1 != claim.WriteVersion)) {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingConflict
	}
	receipt, hasReceipt, err := m.ReadReceiptAt(claim.Epoch, claim.Bucket)
	if err != nil || !hasReceipt || receipt.ClaimID != claim.ClaimID || receipt.ProofDigest != digest || receipt.DataDigest != current.SourceDigest || receipt.PayloadBytes != current.SourceBytes {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingIncomplete
	}
	receiptDigest, err := historyStagingReceiptDigest(receipt)
	if err != nil {
		m.routeMu.Unlock()
		return zero, err
	}
	route := HistoryStagingRoute{Version: HistoryStagingFormatVersion, Bucket: claim.Bucket, Epoch: claim.Epoch, WriteVersion: claim.WriteVersion, Owner: HistoryStagingOwnerTarget, ReceiptDigest: receiptDigest}
	if hasRoute {
		route.ColdBindingEpoch = previous.ColdBindingEpoch
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	if err := writeHistoryStagingValue(batch, historyStagingBucketKey(historyStagingRoutePrefix, claim.Bucket), route); err != nil {
		m.routeMu.Unlock()
		return zero, err
	}
	// Permanent retired marker prevents later v3 packs from referencing
	// chunks that source clearing is about to remove.
	if err := batch.Put(stateHistoryChunkBucketKey(claim.Bucket), []byte{1, 1}); err != nil {
		m.routeMu.Unlock()
		return zero, err
	}
	if err := batch.Write(); err != nil {
		m.routeMu.Unlock()
		return zero, err
	}
	m.generation++
	m.routeMu.Unlock()
	if err := m.syncHot(); err != nil {
		return zero, err
	}
	return route, nil
}

// ClearSource removes only changeset and chunk payload, never tx-range,
// posting, directory, canonical or stage metadata. Each delete batch is
// bounded and crash-resumable. It first re-syncs the adopted route so a prior
// failed WAL sync cannot be mistaken for durable authorization.
func (m *HistoryStagingManager) ClearSource(ctx context.Context, claim HistoryStagingClaim, limits HistoryStagingLimits) error {
	if m == nil || ctx == nil {
		return errors.New("rawdb: missing staging manager or context")
	}
	if err := limits.validate(); err != nil {
		return err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	if err := m.syncHot(); err != nil {
		return err
	}
	route, present, err := m.ReadRoute(claim.Bucket)
	if err != nil || !present || route.Owner != HistoryStagingOwnerTarget || route.Epoch != claim.Epoch {
		return ErrHistoryStagingConflict
	}
	receipt, hasReceipt, err := m.ReadReceiptAt(claim.Epoch, claim.Bucket)
	if err != nil || !hasReceipt || receipt.ClaimID != claim.ClaimID || receipt.ProofDigest != claim.ProofDigest {
		return ErrHistoryStagingIncomplete
	}
	digest, err := historyStagingReceiptDigest(receipt)
	if err != nil || digest != route.ReceiptDigest {
		return ErrHistoryStagingIncomplete
	}
	if route.SourceCleared {
		return nil
	}
	if intent, hasIntent, err := m.ReadResetIntent(); err != nil || (hasIntent && !intent.Complete) {
		return ErrHistoryStagingResetting
	}
	first, last, err := StateHistoryChunkBucketBounds(claim.Bucket)
	if err != nil {
		return err
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	var pending, work uint64
	flush := func() error {
		if pending == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := limits.checkpoint(0); err != nil {
			return err
		}
		if epoch, err := m.CurrentEpoch(); err != nil || epoch != claim.Epoch {
			return ErrHistoryStagingConflict
		}
		if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
			return ErrHistoryStagingResetting
		}
		if current, present, err := m.ReadRoute(claim.Bucket); err != nil || !present || current != route {
			return ErrHistoryStagingConflict
		}
		free, err := limits.FreeBytes()
		if err != nil || free < limits.MinFreeBytes {
			return errors.New("rawdb: insufficient space for source delete WAL")
		}
		if err := batch.Write(); err != nil {
			return err
		}
		batch.Reset()
		pending = 0
		return nil
	}
	remove := func(it ethdb.Iterator, prefix []byte, check func([]byte) (bool, error)) error {
		defer it.Release()
		for it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := bytes.Clone(it.Key())
			if !bytes.HasPrefix(key, prefix) {
				return ErrHistoryStagingConflict
			}
			include, err := check(key)
			if err != nil {
				return err
			}
			if !include {
				break
			}
			cost := uint64(len(key)) + uint64(len(it.Value()))
			if cost > limits.MaxWorkBytes-work || cost > limits.MaxBatchBytes {
				return errors.New("rawdb: source clear work limit reached; resume with next bounded pass")
			}
			if pending != 0 && uint64(len(key)) > limits.MaxBatchBytes-pending {
				if err := flush(); err != nil {
					return err
				}
			}
			if err := limits.checkpoint(cost); err != nil {
				return err
			}
			if err := batch.Delete(key); err != nil {
				return err
			}
			pending += uint64(len(key))
			work += cost
		}
		return it.Error()
	}
	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], first)
	if err := remove(m.hot.NewIterator(stateChangeSetPrefix, suffix[:]), stateChangeSetPrefix, func(key []byte) (bool, error) {
		if len(key) != len(stateChangeSetPrefix)+16 {
			return false, ErrHistoryStagingConflict
		}
		return binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]) <= last, nil
	}); err != nil {
		return err
	}
	chunkPrefix := stateHistoryChunkBucketPrefix(claim.Bucket)
	if err := remove(m.hot.NewIterator(chunkPrefix, nil), chunkPrefix, func(key []byte) (bool, error) {
		return len(key) == len(chunkPrefix)+32, nil
	}); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if err := m.syncHot(); err != nil {
		return err
	}
	// Fresh committed iterator, never the source snapshot used for copying.
	for _, prefix := range [][]byte{stateChangeSetPrefix, chunkPrefix} {
		start := []byte(nil)
		if bytes.Equal(prefix, stateChangeSetPrefix) {
			start = suffix[:]
		}
		it := m.hot.NewIterator(prefix, start)
		if it.Next() {
			key := bytes.Clone(it.Key())
			it.Release()
			if bytes.Equal(prefix, chunkPrefix) || (len(key) >= len(stateChangeSetPrefix)+8 && binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):]) <= last) {
				return fmt.Errorf("rawdb: source bucket %d still has payload", claim.Bucket)
			}
		} else {
			err := it.Error()
			it.Release()
			if err != nil {
				return err
			}
		}
	}
	m.coldGCMu.Lock()
	defer m.coldGCMu.Unlock()
	m.routeMu.Lock()
	current, ok, err := m.ReadRoute(claim.Bucket)
	if err != nil || !ok || current != route {
		m.routeMu.Unlock()
		return ErrHistoryStagingConflict
	}
	current.SourceCleared = true
	finish := m.hot.NewBatch()
	defer finish.Reset()
	if err := writeHistoryStagingValue(finish, historyStagingBucketKey(historyStagingRoutePrefix, claim.Bucket), current); err != nil {
		m.routeMu.Unlock()
		return err
	}
	if err := finish.Delete(historyStagingBucketKey(historyStagingClaimPrefix, claim.Bucket)); err != nil {
		m.routeMu.Unlock()
		return err
	}
	seenCold := make(map[[32]byte]struct{}, len(claim.Proof.ColdSpans))
	for _, span := range claim.Proof.ColdSpans {
		if _, seen := seenCold[span.ContentID]; seen {
			continue
		}
		seenCold[span.ContentID] = struct{}{}
		if err := finish.Delete(historyStagingClaimColdRefKey(span.ContentID, claim.Epoch, claim.Bucket)); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	for block := first; block <= last; block++ {
		if err := DeleteHistoryStagingBlockComplete(finish, claim.Epoch, block); err != nil {
			m.routeMu.Unlock()
			return err
		}
	}
	m.coldGCUncertain = true
	if err := finish.Write(); err != nil {
		m.routeMu.Unlock()
		return err
	}
	m.generation++
	m.routeMu.Unlock()
	if err := m.syncHot(); err != nil {
		return err
	}
	m.coldGCUncertain = false
	return nil
}
