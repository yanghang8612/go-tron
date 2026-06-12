package downloader

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// BlockPath reserves a single hash per block number for one sync session.
// It prevents different peers from steering the same session across
// conflicting forks while still allowing the reservation to be released once a
// block is popped for canonical import.
type BlockPath map[uint64]tcommon.Hash

// NewBlockPath returns an initialized session block path.
func NewBlockPath() BlockPath {
	return make(BlockPath)
}

// Conflicts reports whether bid conflicts with the hash already reserved for
// its block number.
func (p BlockPath) Conflicts(bid types.BlockID) bool {
	if p == nil {
		return false
	}
	hash, ok := p[bid.Num]
	return ok && hash != bid.Hash
}

// Reserve records bid's hash for its block number unless another hash is
// already reserved for that number. The returned BlockPath may be newly
// allocated when the receiver was nil.
func (p BlockPath) Reserve(bid types.BlockID) (BlockPath, bool) {
	if p.Conflicts(bid) {
		return p, false
	}
	if p == nil {
		p = NewBlockPath()
	}
	p[bid.Num] = bid.Hash
	return p, true
}

// Release removes the reservation for blockNum.
func (p BlockPath) Release(blockNum uint64) {
	delete(p, blockNum)
}
