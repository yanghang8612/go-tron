package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

type HistoryStagingPhysicalStats struct {
	Rows, Bytes, MaxRowBytes uint64
	ChangeRows, ChunkRows    uint64
	TxRangeRows, BucketRows  uint64
	Digest                   [32]byte
}

// CheckHistoryStagingBucketWritable is a short admission check for canonical
// old-height writes, repair and range deletes. The caller must hold its writer
// guard through the actual write; this check alone is not a lock.
func (m *HistoryStagingManager) CheckHistoryStagingBucketWritable(bucket uint64) error {
	if m == nil {
		return errors.New("rawdb: missing history staging manager")
	}
	if intent, present, err := m.ReadResetIntent(); err != nil {
		return err
	} else if present && !intent.Complete {
		return ErrHistoryStagingResetting
	}
	epoch, err := m.CurrentEpoch()
	if err != nil {
		return err
	}
	if claim, present, err := m.ReadClaim(bucket); err != nil {
		return err
	} else if present && claim.Epoch == epoch {
		return ErrHistoryStagingConflict
	}
	route, present, err := m.ReadRoute(bucket)
	if err != nil {
		return err
	}
	if !present || route.Epoch != epoch || route.Owner != HistoryStagingOwnerSource {
		return ErrHistoryStagingConflict
	}
	return nil
}

// BeginClaim persists a sealed bucket claim. Core must call it while holding
// the index→chain writer guard after proving Finish/index/solid/retention and
// settled source. Every source mutation path must check this claim under the
// same guard; otherwise CopyClaim cannot assume a stable source.
func (m *HistoryStagingManager) BeginClaim(ctx context.Context, proof HistoryStagingProof, claimID [32]byte) (HistoryStagingClaim, error) {
	var zero HistoryStagingClaim
	if m == nil || ctx == nil || claimID == ([32]byte{}) {
		return zero, errors.New("rawdb: invalid staging claim")
	}
	digest, err := proof.digest()
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	m.coldGCMu.Lock()
	defer m.coldGCMu.Unlock()
	m.routeMu.Lock()
	if err := m.VerifyIdentity(); err != nil {
		m.routeMu.Unlock()
		return zero, err
	}
	epoch, err := m.CurrentEpoch()
	if err != nil || epoch != proof.Epoch {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingConflict
	}
	if intent, present, err := m.ReadResetIntent(); err != nil || (present && !intent.Complete) {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingResetting
	}
	if existing, present, err := m.ReadClaim(proof.Bucket); err != nil {
		m.routeMu.Unlock()
		return zero, err
	} else if present && existing.Epoch == epoch {
		m.routeMu.Unlock()
		if !existing.Cancelled && existing.ClaimID == claimID && existing.ProofDigest == digest {
			return existing, nil
		}
		return zero, ErrHistoryStagingConflict
	}
	route, present, err := m.ReadRoute(proof.Bucket)
	if err != nil || (present && (route.Epoch != epoch || route.Owner != HistoryStagingOwnerSource)) {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingConflict
	}
	claim := HistoryStagingClaim{Version: HistoryStagingFormatVersion, Bucket: proof.Bucket, Epoch: epoch, WriteVersion: 1, ClaimID: claimID, ProofDigest: digest, Proof: proof}
	if present {
		if route.WriteVersion == ^uint64(0) {
			m.routeMu.Unlock()
			return zero, ErrHistoryStagingConflict
		}
		claim.WriteVersion = route.WriteVersion + 1
	}
	batch := m.hot.NewBatch()
	defer batch.Reset()
	if err := writeHistoryStagingValue(batch, historyStagingBucketKey(historyStagingClaimPrefix, proof.Bucket), claim); err != nil {
		m.routeMu.Unlock()
		return zero, err
	}
	seenCold := make(map[[32]byte]struct{}, len(proof.ColdSpans))
	for _, span := range proof.ColdSpans {
		if _, seen := seenCold[span.ContentID]; seen {
			continue
		}
		seenCold[span.ContentID] = struct{}{}
		if err := batch.Put(historyStagingClaimColdRefKey(span.ContentID, epoch, proof.Bucket), []byte{1}); err != nil {
			m.routeMu.Unlock()
			return zero, err
		}
	}
	m.coldGCUncertain = true
	if err := batch.Write(); err != nil {
		m.routeMu.Unlock()
		return zero, err
	}
	m.routeMu.Unlock()
	if err := m.syncHot(); err != nil {
		return zero, err
	}
	m.coldGCUncertain = false
	return claim, nil
}

type historyStagingPayloadSnapshot struct {
	base  pointread.KeyValueSnapshot
	epoch uint64
}

func (s historyStagingPayloadSnapshot) IsPinnedKeyValueView() bool { return true }
func (s historyStagingPayloadSnapshot) Get(key []byte) ([]byte, error) {
	return s.base.Get(historyStagingPayloadKey(s.epoch, key))
}
func (s historyStagingPayloadSnapshot) Has(key []byte) (bool, error) {
	return s.base.Has(historyStagingPayloadKey(s.epoch, key))
}
func (s historyStagingPayloadSnapshot) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &historyStagingPayloadIterator{Iterator: s.base.NewIterator(historyStagingPayloadKey(s.epoch, prefix), start), epochPrefix: historyStagingPayloadEpochPrefix(s.epoch)}
}

type historyStagingPayloadIterator struct {
	ethdb.Iterator
	epochPrefix []byte
}

func (it *historyStagingPayloadIterator) Key() []byte {
	key := it.Iterator.Key()
	if !bytes.HasPrefix(key, it.epochPrefix) {
		return nil
	}
	return key[len(it.epochPrefix):]
}

func historyStagingHashRow(h hash.Hash, key, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(key)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(key)
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(value)
}

// visitHistoryStagingBucket visits exact persisted rows in deterministic
// family order. Its independent tx-range and logical decode checks detect
// malformed rows even if a byte-for-byte copy would have passed SHA checking.
func visitHistoryStagingBucket(ctx context.Context, db StateHistoryReadView, proof HistoryStagingProof, limits HistoryStagingLimits, visit func(key, value []byte) error) (HistoryStagingPhysicalStats, error) {
	var stats HistoryStagingPhysicalStats
	first, last, err := StateHistoryChunkBucketBounds(proof.Bucket)
	if err != nil {
		return stats, err
	}
	h := sha256.New()
	add := func(key, value []byte, family *uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rowBytes := uint64(len(historyStagingPayloadPrefix)) + 8 + uint64(len(key)) + uint64(len(value))
		if rowBytes > limits.MaxRowBytes || stats.Bytes > limits.MaxBucketBytes || rowBytes > limits.MaxBucketBytes-stats.Bytes || stats.Rows == ^uint64(0) {
			return errors.New("rawdb: staging bucket exceeds row or byte limit")
		}
		if visit != nil {
			if err := visit(key, value); err != nil {
				return err
			}
		}
		historyStagingHashRow(h, key, value)
		stats.Rows++
		stats.Bytes += rowBytes
		if rowBytes > stats.MaxRowBytes {
			stats.MaxRowBytes = rowBytes
		}
		*family++
		return nil
	}
	for i, block := range proof.Blocks {
		if block.Number != first+uint64(i) {
			return stats, ErrHistoryStagingConflict
		}
		row, present, err := ReadStateTxRange(db, block.Number)
		if err != nil || !present || row.BlockHash != block.Hash || row.BeginTxNum != block.BeginTxNum || row.EndTxNum != block.EndTxNum {
			return stats, fmt.Errorf("rawdb: staged tx-range proof mismatch at %d: %w", block.Number, err)
		}
		key := stateTxRangeKey(block.Number)
		value, err := db.Get(key)
		if err != nil {
			return stats, err
		}
		if err := add(key, value, &stats.TxRangeRows); err != nil {
			return stats, err
		}
	}
	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], first)
	it := db.NewIterator(stateChangeSetPrefix, suffix[:])
	for it.Next() {
		key, value := it.Key(), it.Value()
		if len(key) != len(stateChangeSetPrefix)+16 || !bytes.HasPrefix(key, stateChangeSetPrefix) {
			it.Release()
			return stats, ErrHistoryStagingConflict
		}
		block := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):])
		if block > last {
			break
		}
		if block < first {
			it.Release()
			return stats, ErrHistoryStagingConflict
		}
		for _, span := range proof.ColdSpans {
			if block >= span.From && block <= span.To {
				it.Release()
				return stats, errors.New("rawdb: source changeset overlaps certified cold gap")
			}
		}
		if uint64(len(historyStagingPayloadPrefix)+8+len(key)+len(value)) > limits.MaxRowBytes {
			it.Release()
			return stats, errors.New("rawdb: staging changeset exceeds row limit before decode")
		}
		if binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:]) == 0 {
			if isStateHistorySharedPack(value) {
				_, decodedLen, _, _, err := sharedStateHistoryPackHeader(value, block)
				if err != nil || uint64(decodedLen) > limits.MaxDecodedBytes {
					it.Release()
					return stats, errors.New("rawdb: shared staging pack exceeds predecode budget")
				}
			} else {
				_, decodedLen, compressed, err := stateDomainChangeBlockCompressionPayload(value)
				if err != nil || (compressed && uint64(decodedLen) > limits.MaxDecodedBytes) || (!compressed && uint64(len(value)) > limits.MaxDecodedBytes) {
					it.Release()
					return stats, errors.New("rawdb: staging changeset logical size exceeds decode budget")
				}
			}
		}
		if err := add(key, value, &stats.ChangeRows); err != nil {
			it.Release()
			return stats, err
		}
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return stats, err
	}
	chunkPrefix := stateHistoryChunkBucketPrefix(proof.Bucket)
	it = db.NewIterator(chunkPrefix, nil)
	for it.Next() {
		key, value := it.Key(), it.Value()
		if len(key) != len(chunkPrefix)+32 || !bytes.HasPrefix(key, chunkPrefix) {
			it.Release()
			return stats, ErrHistoryStagingConflict
		}
		if len(value) < 3 {
			it.Release()
			return stats, ErrHistoryStagingConflict
		}
		n, used := binary.Uvarint(value[2:])
		if used <= 0 || n == 0 || n > limits.MaxDecodedBytes {
			it.Release()
			return stats, errors.New("rawdb: staged chunk exceeds decode budget")
		}
		var digest [32]byte
		copy(digest[:], key[len(chunkPrefix):])
		if _, err := decodeStateHistorySharedChunk(value, int(n), digest); err != nil {
			it.Release()
			return stats, err
		}
		if err := add(key, value, &stats.ChunkRows); err != nil {
			it.Release()
			return stats, err
		}
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return stats, err
	}
	metaKey := stateHistoryChunkBucketKey(proof.Bucket)
	if value, present, err := readPresentValue(db, metaKey, "staging bucket metadata"); err != nil {
		return stats, err
	} else if present {
		if len(value) != 2 || value[0] != 1 || value[1] > 1 {
			return stats, ErrHistoryStagingConflict
		}
		if err := add(metaKey, value, &stats.BucketRows); err != nil {
			return stats, err
		}
	}
	copy(stats.Digest[:], h.Sum(nil))
	// The span iterator authenticates every pack and repair row without
	// retaining a []*StateDomainChange or expanding all Prev values at once.
	if err := IterateStateHistorySpanBlocks(ctx, db, first, last, proof.Blocks[0].BeginTxNum, proof.Blocks[len(proof.Blocks)-1].EndTxNum, func(block *StateHistorySpanBlock) (bool, error) {
		info := block.Info()
		want := proof.Blocks[info.BlockNum-first]
		if info.BlockHash != want.Hash || info.BeginTxNum != want.BeginTxNum || info.EndTxNum != want.EndTxNum || info.DecodedBytes > limits.MaxDecodedBytes {
			return false, ErrHistoryStagingConflict
		}
		return true, nil
	}); err != nil {
		return stats, err
	}
	return stats, nil
}

// InspectPhysicalBucket is the bounded physical inventory used by the CLI.
// It does not by itself certify cold gaps or the canonical proof.
func (m *HistoryStagingManager) InspectPhysicalBucket(ctx context.Context, proof HistoryStagingProof, limits HistoryStagingLimits) (HistoryStagingPhysicalStats, error) {
	if m == nil || ctx == nil {
		return HistoryStagingPhysicalStats{}, errors.New("rawdb: missing staging manager or context")
	}
	if err := proof.validate(); err != nil {
		return HistoryStagingPhysicalStats{}, err
	}
	if err := limits.validate(); err != nil {
		return HistoryStagingPhysicalStats{}, err
	}
	snapshot, err := m.hot.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return HistoryStagingPhysicalStats{}, err
	}
	defer snapshot.Close()
	view := &stateHistoryReadView{reader: snapshot, iteratee: snapshot, pinned: true}
	return visitHistoryStagingBucket(ctx, view, proof, limits, nil)
}

// InspectHistoryStagingPhysicalBucket is a read-only inventory entrypoint for
// dry-run tools. The caller supplies an already pinned source view; this does
// not open, initialize, or mutate a staging store. Physical absence alone
// cannot prove a legal zero-change block or a cold-pruned gap.
func InspectHistoryStagingPhysicalBucket(ctx context.Context, pinnedHotView StateHistoryReadView, proof HistoryStagingProof, limits HistoryStagingLimits) (HistoryStagingPhysicalStats, error) {
	if ctx == nil || pinnedHotView == nil || !pinnedHotView.IsPinnedKeyValueView() {
		return HistoryStagingPhysicalStats{}, ErrStateHistoryReadViewUnpinned
	}
	if err := proof.validate(); err != nil {
		return HistoryStagingPhysicalStats{}, err
	}
	if err := limits.validate(); err != nil {
		return HistoryStagingPhysicalStats{}, err
	}
	return visitHistoryStagingBucket(ctx, pinnedHotView, proof, limits, nil)
}

// InspectTargetBucket independently replays the bounded physical and logical
// authentication from a new target snapshot. Offline verify compares its
// digest and family counts with both the frozen plan and durable receipt.
func (m *HistoryStagingManager) InspectTargetBucket(ctx context.Context, proof HistoryStagingProof, limits HistoryStagingLimits) (HistoryStagingPhysicalStats, error) {
	if m == nil || ctx == nil {
		return HistoryStagingPhysicalStats{}, ErrHistoryStagingConflict
	}
	if err := proof.validate(); err != nil {
		return HistoryStagingPhysicalStats{}, err
	}
	if err := limits.validate(); err != nil {
		return HistoryStagingPhysicalStats{}, err
	}
	snapshot, err := m.stage.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return HistoryStagingPhysicalStats{}, err
	}
	defer snapshot.Close()
	return visitHistoryStagingBucket(ctx, historyStagingPayloadSnapshot{base: snapshot, epoch: proof.Epoch}, proof, limits, nil)
}

// CopyClaim copies the sealed source snapshot into epoch-private target keys.
// It never changes the authoritative source route. A partial target copy is an
// unreadable orphan until exact target verification and a synced receipt pass.
func (m *HistoryStagingManager) CopyClaim(ctx context.Context, claim HistoryStagingClaim, limits HistoryStagingLimits) (HistoryStagingReceipt, error) {
	var zero HistoryStagingReceipt
	if m == nil || ctx == nil {
		return zero, errors.New("rawdb: missing staging manager or context")
	}
	if err := limits.validate(); err != nil {
		return zero, err
	}
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	if err := m.VerifyIdentity(); err != nil {
		return zero, err
	}
	current, present, err := m.ReadClaim(claim.Bucket)
	if err != nil || !present || current.Cancelled || current.ClaimID != claim.ClaimID || current.ProofDigest != claim.ProofDigest || current.WriteVersion != claim.WriteVersion {
		return zero, ErrHistoryStagingConflict
	}
	claim = current
	if route, hasRoute, err := m.ReadRoute(claim.Bucket); err != nil || (hasRoute && route.Owner != HistoryStagingOwnerSource) {
		return zero, ErrHistoryStagingConflict
	}
	if epoch, err := m.CurrentEpoch(); err != nil || epoch != claim.Epoch {
		return zero, ErrHistoryStagingConflict
	}
	if intent, hasIntent, err := m.ReadResetIntent(); err != nil || (hasIntent && !intent.Complete) {
		return zero, ErrHistoryStagingResetting
	}
	free, err := limits.FreeBytes()
	if err != nil || free < limits.MinFreeBytes || free-limits.MinFreeBytes < limits.MaxBucketBytes {
		return zero, errors.New("rawdb: insufficient reserved space for staging copy")
	}
	source, err := m.hot.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return zero, err
	}
	defer source.Close()
	sourceView := &stateHistoryReadView{reader: source, iteratee: source, pinned: true}
	preflight, err := visitHistoryStagingBucket(ctx, sourceView, claim.Proof, limits, nil)
	if err != nil {
		return zero, err
	}
	if preflight.Bytes > HistoryStagingMaxCopyPhysicalBytes(limits.MaxWorkBytes) {
		return zero, errors.New("rawdb: staging source exceeds total bounded scan/copy/verify work")
	}
	free, err = limits.FreeBytes()
	if err != nil || free < limits.MinFreeBytes || free-limits.MinFreeBytes < preflight.Bytes*2 {
		return zero, errors.New("rawdb: insufficient source-sized target and compaction reservation")
	}
	batch := m.stage.NewBatch()
	defer batch.Reset()
	var pending uint64
	flush := func() error {
		if pending == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		free, err := limits.FreeBytes()
		if err != nil || free < limits.MinFreeBytes {
			return errors.New("rawdb: staging copy crossed free-space floor")
		}
		if err := batch.Write(); err != nil {
			return err
		}
		batch.Reset()
		pending = 0
		return nil
	}
	sourceStats, err := visitHistoryStagingBucket(ctx, sourceView, claim.Proof, limits, func(key, value []byte) error {
		stageKey := historyStagingPayloadKey(claim.Epoch, key)
		rowBytes := uint64(len(stageKey)) + uint64(len(value))
		if rowBytes > limits.MaxBatchBytes {
			return errors.New("rawdb: staging row exceeds batch limit")
		}
		if pending != 0 && rowBytes > limits.MaxBatchBytes-pending {
			if err := flush(); err != nil {
				return err
			}
		}
		if err := batch.Put(stageKey, value); err != nil {
			return err
		}
		pending += rowBytes
		return nil
	})
	if err != nil {
		return zero, err
	}
	if sourceStats != preflight {
		return zero, errors.New("rawdb: sealed staging source changed after preflight")
	}
	if err := flush(); err != nil {
		return zero, err
	}
	if err := m.syncStage(); err != nil {
		return zero, err
	}
	target, err := m.stage.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return zero, err
	}
	defer target.Close()
	targetView := historyStagingPayloadSnapshot{base: target, epoch: claim.Epoch}
	targetStats, err := visitHistoryStagingBucket(ctx, targetView, claim.Proof, limits, nil)
	if err != nil {
		return zero, err
	}
	if sourceStats != targetStats {
		return zero, errors.New("rawdb: staging target physical verification mismatch")
	}
	receipt := HistoryStagingReceipt{Version: HistoryStagingFormatVersion, Bucket: claim.Bucket, Epoch: claim.Epoch, ClaimID: claim.ClaimID, ProofDigest: claim.ProofDigest, DataDigest: sourceStats.Digest, PayloadRows: sourceStats.ChangeRows, PayloadBytes: sourceStats.Bytes, TxRangeRows: sourceStats.TxRangeRows, ChunkRows: sourceStats.ChunkRows}
	if old, hasOld, err := m.ReadReceiptAt(claim.Epoch, claim.Bucket); err != nil {
		return zero, err
	} else if hasOld && old != receipt {
		return zero, ErrHistoryStagingConflict
	}
	if err := writeHistoryStagingValue(m.stage, historyStagingReceiptKey(claim.Epoch, claim.Bucket), receipt); err != nil {
		return zero, err
	}
	if err := m.syncStage(); err != nil {
		return zero, err
	}
	m.routeMu.Lock()
	current, present, err = m.ReadClaim(claim.Bucket)
	if err != nil || !present || current.ClaimID != claim.ClaimID || current.ProofDigest != claim.ProofDigest {
		m.routeMu.Unlock()
		return zero, ErrHistoryStagingConflict
	}
	current.TargetReady = true
	current.SourceDigest = sourceStats.Digest
	current.SourceRows = sourceStats.Rows
	current.SourceBytes = sourceStats.Bytes
	err = writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingClaimPrefix, claim.Bucket), current)
	m.routeMu.Unlock()
	if err != nil {
		return zero, err
	}
	if err := m.syncHot(); err != nil {
		return zero, err
	}
	return receipt, nil
}
