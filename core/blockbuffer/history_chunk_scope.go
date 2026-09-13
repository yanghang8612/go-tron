package blockbuffer

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// StateHistoryChunkWritesAtomic reports whether the active canonical layer can
// stage chunks followed by their referencing pack for one atomic base batch.
// FlushUpTo never splits a layer across batches; ancestor layers are flushed
// before, or in the same batch as, their descendants. The base must also offer
// snapshots so readers can resolve a pack and its chunks through one view.
//
// This is a capability check, not a lock or a transaction. The caller must keep
// the canonical single-writer scope through every chunk and the final pack Put,
// flush to this same batched base, and discard dependent descendant layers when
// an ancestor fails or is rewound. Chunks must be immutable and cannot be deleted
// while a retained pack references them. Generic LayerViews and buffer batches
// deliberately do not advertise this canonical lifetime guarantee.
func (b *Buffer) StateHistoryChunkWritesAtomic() bool {
	if b == nil {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.base.(ethdb.Batcher); !ok {
		return false
	}
	if !pointread.SupportsKeyValueSnapshots(b.base) {
		return false
	}
	l := b.newestInflightLocked()
	return l != nil && l.owner == b && l.state == layerInflight
}

var _ pointread.KeyValueSnapshotter = (*Buffer)(nil)
var _ pointread.KeyValueSnapshot = (*ReadSnapshot)(nil)
var _ pointread.PinnedKeyValueView = (*ReadSnapshot)(nil)

// NewKeyValueSnapshot preserves both the overlay and the durable base. It has
// the same external single-writer capture requirement as NewReadSnapshot; it
// must not be called while holding b.mu or b.flushMu. In particular, pinning
// the topology does not freeze further writes into an already captured layer.
// History callers publish immutable chunks and a complete pack before reading
// them through this view.
func (b *Buffer) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	if b == nil || b.base == nil {
		return nil, errors.Join(ErrReadSnapshotUnsupported, pointread.ErrKeyValueSnapshotUnsupported)
	}
	if _, ok := b.base.(pointread.KeyValueSnapshotter); !ok {
		return nil, errors.Join(ErrReadSnapshotUnsupported, pointread.ErrKeyValueSnapshotUnsupported)
	}
	// Do not translate errors from a supported factory into unsupported: a
	// closed or failing store must not make readers fall back to its live view.
	snapshot, err := b.NewReadSnapshot()
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// IsPinnedKeyValueView identifies an already acquired logical read snapshot.
// Its caller owns the snapshot lifetime and the NewReadSnapshot capture rules.
// The live Buffer deliberately does not implement this marker.
func (*ReadSnapshot) IsPinnedKeyValueView() bool { return true }
