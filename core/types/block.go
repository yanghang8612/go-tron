package types

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
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
	hashDone bool
	hashMu   sync.Mutex
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

func (b *Block) AccountStateRoot() common.Hash {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return common.Hash{}
	}
	return common.BytesToHash(b.pb.BlockHeader.RawData.AccountStateRoot)
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

// Hash returns the canonical block identifier: SHA-256 of serialized
// BlockHeader.RawData with the first 8 bytes overwritten by the
// big-endian block number. This matches java-tron's `BlockId` format and
// is what `block_header.raw_data.parent_hash` references on the wire.
//
// Use `recoverWitness` / `SignBlock` for the raw SHA256(RawData) bytes
// when verifying / producing witness signatures — those compute the
// pre-overwrite digest directly.
func (b *Block) Hash() common.Hash {
	b.hashMu.Lock()
	defer b.hashMu.Unlock()
	if !b.hashDone {
		if b.pb.BlockHeader != nil && b.pb.BlockHeader.RawData != nil {
			data, err := proto.Marshal(b.pb.BlockHeader.RawData)
			if err != nil {
				panic(fmt.Sprintf("block header marshal failed: %v", err))
			}
			b.hash = sha256.Sum256(data)
			binary.BigEndian.PutUint64(b.hash[:8], uint64(b.pb.BlockHeader.RawData.Number))
		}
		b.hashDone = true
	}
	return b.hash
}

// ID returns BlockID. With Hash() now in BlockId format, this is a thin
// wrapper kept for callers that need the explicit (Hash, Num) pair.
func (b *Block) ID() BlockID {
	return BlockID{Hash: b.Hash(), Num: b.Number()}
}

// SetWitnessSignature sets the witness signature on the block header.
func (b *Block) SetWitnessSignature(sig []byte) {
	if b.pb.BlockHeader == nil {
		b.pb.BlockHeader = &corepb.BlockHeader{}
	}
	b.pb.BlockHeader.WitnessSignature = sig
}

// SetAccountStateRoot sets the account state root in the block header raw data.
func (b *Block) SetAccountStateRoot(root common.Hash) {
	if b.pb.BlockHeader == nil {
		b.pb.BlockHeader = &corepb.BlockHeader{}
	}
	if b.pb.BlockHeader.RawData == nil {
		b.pb.BlockHeader.RawData = &corepb.BlockHeaderRaw{}
	}
	b.pb.BlockHeader.RawData.AccountStateRoot = root.Bytes()
}

// ResetHash clears the cached hash so it will be recomputed on next Hash() call.
func (b *Block) ResetHash() {
	b.hashMu.Lock()
	b.hashDone = false
	b.hashMu.Unlock()
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
