package downloader

import (
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
)

// BufferedBlock holds an out-of-order sync block awaiting contiguous drain.
// It stores the raw wire bytes plus light metadata rather than the decoded
// *types.Block: a decoded block pins its full proto tree and can balloon the
// GC mark set when many blocks are waiting on a gap.
type BufferedBlock struct {
	Raw  []byte
	Hash tcommon.Hash
	Num  uint64
	Peer *p2p.Peer
}

// NewBufferedBlock returns a buffered sync block with self-owned wire bytes.
func NewBufferedBlock(peer *p2p.Peer, block *types.Block, raw []byte) BufferedBlock {
	return BufferedBlock{
		Raw:  RawBlockBytes(block, raw),
		Hash: block.Hash(),
		Num:  block.Number(),
		Peer: peer,
	}
}

// RawBlockBytes returns a self-owned copy of the block's wire bytes for the
// sync buffer. raw is the exact payload received off the wire; callers without
// it pass nil and the bytes are re-marshaled from the decoded block.
func RawBlockBytes(block *types.Block, raw []byte) []byte {
	if len(raw) == 0 {
		b, _ := block.Marshal()
		return b
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// BufferedBatch is the contiguous raw block run popped for local import.
type BufferedBatch struct {
	Blocks      []*types.Block
	Buffered    []BufferedBlock
	BufferWaits []time.Duration
}

// DecodeBlocks decodes raw buffered entries into Blocks. It preserves the
// successfully decoded prefix and reports the first undecodable entry; callers
// can import the prefix and refetch the dropped suffix.
func (b *BufferedBatch) DecodeBlocks() (BufferedBlock, error) {
	if b == nil {
		return BufferedBlock{}, nil
	}
	b.Blocks = make([]*types.Block, 0, len(b.Buffered))
	for _, buffered := range b.Buffered {
		block, err := types.UnmarshalBlock(buffered.Raw)
		if err != nil {
			return buffered, err
		}
		b.Blocks = append(b.Blocks, block)
	}
	return BufferedBlock{}, nil
}
