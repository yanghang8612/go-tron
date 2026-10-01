package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// HistoryStagingTargetBucketView exposes only one epoch-private target bucket.
// It is intended for a bounded, streaming comparison with authenticated cold
// rows before a pure-target bucket is retired. It never routes through hot or
// consults a mutable source-owner pointer.
type HistoryStagingTargetBucketView struct {
	snapshot pointread.KeyValueSnapshot
	epoch    uint64
	bucket   uint64
	closed   atomic.Bool
}

func (m *HistoryStagingManager) AcquireTargetBucketView(epoch, bucket uint64) (*HistoryStagingTargetBucketView, error) {
	if m == nil || epoch == 0 {
		return nil, errors.New("rawdb: invalid target bucket view")
	}
	snapshot, err := m.stage.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, ErrHistoryStagingIncomplete
	}
	return &HistoryStagingTargetBucketView{snapshot: snapshot, epoch: epoch, bucket: bucket}, nil
}

func (v *HistoryStagingTargetBucketView) IsPinnedKeyValueView() bool {
	return v != nil && !v.closed.Load()
}

func (v *HistoryStagingTargetBucketView) Close() error {
	if v == nil || !v.closed.CompareAndSwap(false, true) {
		return nil
	}
	return v.snapshot.Close()
}

func (v *HistoryStagingTargetBucketView) checkedKey(key []byte) ([]byte, error) {
	if v == nil || v.closed.Load() {
		return nil, errors.New("rawdb: closed target bucket view")
	}
	bucket, payload, err := historyStagingTargetBucketForKey(key)
	if err != nil || !payload || bucket != v.bucket {
		return nil, ErrHistoryStagingConflict
	}
	return historyStagingPayloadKey(v.epoch, key), nil
}

func (v *HistoryStagingTargetBucketView) Get(key []byte) ([]byte, error) {
	encoded, err := v.checkedKey(key)
	if err != nil {
		return nil, err
	}
	return v.snapshot.Get(encoded)
}

func (v *HistoryStagingTargetBucketView) Has(key []byte) (bool, error) {
	encoded, err := v.checkedKey(key)
	if err != nil {
		return false, err
	}
	return v.snapshot.Has(encoded)
}

func (v *HistoryStagingTargetBucketView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if v == nil || v.closed.Load() || !historyStagingBucketBoundedPrefix(prefix, start, v.bucket) {
		return &stateHistoryErrorIterator{err: ErrHistoryStagingConflict}
	}
	return &historyStagingTargetBucketIterator{
		underlying:  v.snapshot.NewIterator(historyStagingPayloadKey(v.epoch, prefix), start),
		epochPrefix: historyStagingPayloadEpochPrefix(v.epoch),
		bucket:      v.bucket,
	}
}

func historyStagingTargetBucketForKey(key []byte) (uint64, bool, error) {
	if bytes.HasPrefix(key, stateTxRangePrefix) {
		if len(key) != len(stateTxRangePrefix)+8 {
			return 0, true, ErrHistoryStagingConflict
		}
		return binary.BigEndian.Uint64(key[len(stateTxRangePrefix):]) / StateHistoryChunkBucketBlocks, true, nil
	}
	return historyStagingPayloadBucket(key)
}

func historyStagingBucketBoundedPrefix(prefix, start []byte, bucket uint64) bool {
	switch {
	case bytes.Equal(prefix, stateTxRangePrefix), bytes.Equal(prefix, stateChangeSetPrefix):
		return len(start) == 8 && binary.BigEndian.Uint64(start)/StateHistoryChunkBucketBlocks == bucket
	case bytes.HasPrefix(prefix, stateTxRangePrefix) && len(prefix) >= len(stateTxRangePrefix)+8:
		return binary.BigEndian.Uint64(prefix[len(stateTxRangePrefix):])/StateHistoryChunkBucketBlocks == bucket
	case bytes.HasPrefix(prefix, stateChangeSetPrefix) && len(prefix) >= len(stateChangeSetPrefix)+8:
		return binary.BigEndian.Uint64(prefix[len(stateChangeSetPrefix):])/StateHistoryChunkBucketBlocks == bucket
	case bytes.HasPrefix(prefix, stateHistorySharedChunkPrefix) && len(prefix) >= len(stateHistorySharedChunkPrefix)+8:
		return binary.BigEndian.Uint64(prefix[len(stateHistorySharedChunkPrefix):]) == bucket
	case bytes.HasPrefix(prefix, stateHistorySharedBucketPrefix) && len(prefix) >= len(stateHistorySharedBucketPrefix)+8:
		return binary.BigEndian.Uint64(prefix[len(stateHistorySharedBucketPrefix):]) == bucket
	default:
		return false
	}
}

type historyStagingTargetBucketIterator struct {
	underlying  ethdb.Iterator
	epochPrefix []byte
	bucket      uint64
	key         []byte
	err         error
}

func (it *historyStagingTargetBucketIterator) Next() bool {
	if it.err != nil || !it.underlying.Next() {
		return false
	}
	key := it.underlying.Key()
	if !bytes.HasPrefix(key, it.epochPrefix) {
		it.err = ErrHistoryStagingConflict
		return false
	}
	key = key[len(it.epochPrefix):]
	bucket, payload, err := historyStagingTargetBucketForKey(key)
	if err != nil || !payload || bucket < it.bucket {
		it.err = ErrHistoryStagingConflict
		return false
	}
	if bucket > it.bucket {
		return false
	}
	it.key = key
	return true
}

func (it *historyStagingTargetBucketIterator) Error() error {
	return errors.Join(it.err, it.underlying.Error())
}
func (it *historyStagingTargetBucketIterator) Key() []byte   { return it.key }
func (it *historyStagingTargetBucketIterator) Value() []byte { return it.underlying.Value() }
func (it *historyStagingTargetBucketIterator) Release()      { it.underlying.Release() }
