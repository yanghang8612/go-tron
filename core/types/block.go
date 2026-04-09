package types

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// BlockID combines a block hash with its number. The first 8 bytes of the hash
// are overwritten with the big-endian block number.
type BlockID struct {
	Hash common.Hash
	Num  uint64
}

func (id BlockID) Number() uint64 {
	return id.Num
}

// Block wraps a protobuf Block message with cached derived fields.
type Block struct {
	pb       *corepb.Block
	hash     common.Hash
	hashOnce sync.Once
}

func NewBlockFromPB(pb *corepb.Block) *Block {
	return &Block{pb: pb}
}

func (b *Block) Proto() *corepb.Block { return b.pb }

func (b *Block) Number() uint64 {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return 0
	}
	return uint64(b.pb.BlockHeader.RawData.Number)
}

func (b *Block) Timestamp() int64 {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return 0
	}
	return b.pb.BlockHeader.RawData.Timestamp
}

func (b *Block) ParentHash() common.Hash {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return common.Hash{}
	}
	return common.BytesToHash(b.pb.BlockHeader.RawData.ParentHash)
}

func (b *Block) WitnessAddress() common.Address {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return common.Address{}
	}
	return common.BytesToAddress(b.pb.BlockHeader.RawData.WitnessAddress)
}

func (b *Block) WitnessSignature() []byte {
	if b.pb.BlockHeader == nil {
		return nil
	}
	return b.pb.BlockHeader.WitnessSignature
}

func (b *Block) Version() int32 {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return 0
	}
	return b.pb.BlockHeader.RawData.Version
}

func (b *Block) Transactions() []*Transaction {
	txs := make([]*Transaction, len(b.pb.Transactions))
	for i, pb := range b.pb.Transactions {
		txs[i] = NewTransactionFromPB(pb)
	}
	return txs
}

// Hash computes SHA-256 of serialized BlockHeader.RawData.
func (b *Block) Hash() common.Hash {
	b.hashOnce.Do(func() {
		if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
			return
		}
		data, err := proto.Marshal(b.pb.BlockHeader.RawData)
		if err != nil {
			return
		}
		b.hash = sha256.Sum256(data)
	})
	return b.hash
}

// ID returns BlockID (hash with block number in first 8 bytes).
func (b *Block) ID() BlockID {
	h := b.Hash()
	num := b.Number()
	binary.BigEndian.PutUint64(h[:8], num)
	return BlockID{Hash: h, Num: num}
}

func (b *Block) Marshal() ([]byte, error) {
	return proto.Marshal(b.pb)
}

func UnmarshalBlock(data []byte) (*Block, error) {
	pb := &corepb.Block{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewBlockFromPB(pb), nil
}
