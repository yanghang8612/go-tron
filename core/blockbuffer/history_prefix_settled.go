package blockbuffer

import "github.com/tronprotocol/go-tron/common"

// HistoryPrefixSettled reports whether the buffer has no pending layer at or
// below through. This is a conservative topology check, not a flush or a durable
// stage proof. Nil buffers, missing bases, and malformed layer metadata fail
// closed. Both committed and begun-but-uncommitted layers are checked in full.
//
// FlushUpTo retains its snapshot's layers in b.layers throughout disk I/O and
// removes only the successfully written prefix under b.mu. Thus a layer remains
// visible here even after its batch has reached the base but before Write returns
// or while a failed/ambiguous write can be retried. No flushMu or I/O is needed.
// Detached batch targets cannot rejoin this topology: batch Write/writeFiltered
// reject or discard them, and detached LayerViews cannot become flush eligible.
//
// The caller must independently verify the durable/canonical boundary and hold
// its chain/repair guard from this check through its own delete commit. That
// guard must prevent new layers or old-height repairs at or below through; this
// method alone is not a lease and does not inspect keys in newer layers.
func (b *Buffer) HistoryPrefixSettled(through uint64) bool {
	if b == nil {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.base == nil {
		return false
	}
	for _, l := range b.layers {
		if l == nil || l.owner != b || l.state != layerCommitted || l.blockHash == (common.Hash{}) || l.number <= through {
			return false
		}
	}
	for _, l := range b.inflight {
		if l == nil || l.owner != b || l.state != layerInflight || l.blockHash == (common.Hash{}) || l.number <= through {
			return false
		}
	}
	return true
}
