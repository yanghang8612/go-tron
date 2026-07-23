package types

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var ErrInvalidTransactionMerkleRoot = errors.New("block transaction merkle root mismatch")

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

	// txs memoizes the wrapped Transaction slice so the same *Transaction
	// instances are returned on every Transactions() call. This is what lets a
	// parallel signer-recovery pre-pass warm each tx's signers memo and have the
	// serial execution path observe the warm result (it re-fetches via
	// Transactions()). pb.Transactions is never mutated after construction
	// (block_builder builds the full slice before NewBlockFromPB; sync blocks
	// come from UnmarshalBlock), so caching the wrappers is safe.
	txsOnce sync.Once
	txs     []*Transaction

	// witness memoizes the ECDSA recovery of the block's witness signature
	// (recovered address or error), keyed by this block's identity. Header
	// verification reads it through CachedRecoveredWitness so the parallel
	// pre-pass can move the single per-block SR-signature recovery off the
	// serial critical path. SetWitnessSignature / ResetHash clear it.
	witnessMu       sync.Mutex
	witnessDone     bool
	witnessAddr     common.Address
	witnessRecovErr error
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

func (b *Block) TransactionMerkleRoot() common.Hash {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return common.Hash{}
	}
	return common.BytesToHash(b.pb.BlockHeader.RawData.TxTrieRoot)
}

// ValidateTransactionMerkleRoot mirrors java-tron's
// BlockCapsule.validateMerkleRoot. Normal blocks must carry exactly 32 bytes,
// including the all-zero root for an empty transaction list.
func (b *Block) ValidateTransactionMerkleRoot() error {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return fmt.Errorf("%w: missing block header", ErrInvalidTransactionMerkleRoot)
	}
	encoded := b.pb.BlockHeader.RawData.TxTrieRoot
	if len(encoded) != common.HashLength {
		return fmt.Errorf("%w for block %d: root length %d, want %d", ErrInvalidTransactionMerkleRoot, b.Number(), len(encoded), common.HashLength)
	}
	actual, err := TransactionMerkleRoot(b.pb.Transactions)
	if err != nil {
		return fmt.Errorf("%w for block %d: %v", ErrInvalidTransactionMerkleRoot, b.Number(), err)
	}
	expected := common.BytesToHash(encoded)
	if actual != expected {
		return fmt.Errorf("%w for block %d: expected %x, actual %x", ErrInvalidTransactionMerkleRoot, b.Number(), expected, actual)
	}
	return nil
}

func (b *Block) Version() int32 {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return 0
	}
	return b.pb.BlockHeader.RawData.Version
}

func (b *Block) Transactions() []*Transaction {
	b.txsOnce.Do(func() {
		txs := make([]*Transaction, len(b.pb.Transactions))
		for i, pb := range b.pb.Transactions {
			txs[i] = NewTransactionFromPB(pb)
		}
		b.txs = txs
	})
	return b.txs
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

// CachedRecoveredWitness returns the address recovered from this block's witness
// signature, memoizing the result. On a cache miss it calls recover (which owns
// the actual ECDSA recovery, living in the consensus package) exactly once; the
// stored (addr, err) is returned on every subsequent call. Recovery is a pure
// function of the immutable BlockHeader.RawData + WitnessSignature, so the memo
// is identical-by-construction to an inline recompute — a performance memo only.
// SetWitnessSignature / ResetHash clear it.
func (b *Block) CachedRecoveredWitness(recover func(*Block) (common.Address, error)) (common.Address, error) {
	b.witnessMu.Lock()
	defer b.witnessMu.Unlock()
	if !b.witnessDone {
		b.witnessAddr, b.witnessRecovErr = recover(b)
		b.witnessDone = true
	}
	return b.witnessAddr, b.witnessRecovErr
}

// SetWitnessSignature sets the witness signature on the block header. It clears
// the cached witness recovery so a re-signed block re-derives the signer.
func (b *Block) SetWitnessSignature(sig []byte) {
	if b.pb.BlockHeader == nil {
		b.pb.BlockHeader = &corepb.BlockHeader{}
	}
	b.pb.BlockHeader.WitnessSignature = sig
	b.witnessMu.Lock()
	b.witnessDone = false
	b.witnessMu.Unlock()
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
// It also clears the cached witness recovery, since a header-raw change (the
// reason to reset the hash) invalidates the recovered signer.
func (b *Block) ResetHash() {
	b.hashMu.Lock()
	b.hashDone = false
	b.hashMu.Unlock()
	b.witnessMu.Lock()
	b.witnessDone = false
	b.witnessMu.Unlock()
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

// BlockHashFromRaw derives the canonical BlockID directly from bytes produced
// by Block.Marshal. It scans past transaction fields without decoding them,
// extracts BlockHeader.RawData, hashes those exact canonical protobuf bytes and
// reads only the header's number varint for the BlockID prefix. Freezer uses
// this after it has already loaded blockRaw, avoiding a second DB read and a
// full transaction-tree unmarshal.
func BlockHashFromRaw(data []byte) (common.Hash, error) {
	header, ok, err := protobufBytesField(data, 2)
	if err != nil {
		return common.Hash{}, fmt.Errorf("block raw header: %w", err)
	}
	if !ok {
		return common.Hash{}, errors.New("block raw header: missing block_header")
	}
	rawData, ok, err := protobufBytesField(header, 1)
	if err != nil {
		return common.Hash{}, fmt.Errorf("block raw header data: %w", err)
	}
	if !ok {
		return common.Hash{}, errors.New("block raw header data: missing raw_data")
	}
	number, err := protobufInt64Field(rawData, 7)
	if err != nil {
		return common.Hash{}, fmt.Errorf("block raw number: %w", err)
	}
	hash := sha256.Sum256(rawData)
	binary.BigEndian.PutUint64(hash[:8], uint64(number))
	return hash, nil
}

// protobufBytesField returns the last occurrence of a bytes/message field.
// Canonical Block.Marshal output contains one occurrence of both fields used
// here; unrelated fields are skipped without allocating.
func protobufBytesField(data []byte, field protowire.Number) ([]byte, bool, error) {
	var out []byte
	var found bool
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, false, protowire.ParseError(n)
		}
		data = data[n:]
		if number == field {
			if wireType != protowire.BytesType {
				return nil, false, fmt.Errorf("field %d has wire type %d, want bytes", field, wireType)
			}
			value, m := protowire.ConsumeBytes(data)
			if m < 0 {
				return nil, false, protowire.ParseError(m)
			}
			out, found = value, true
			data = data[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(number, wireType, data)
		if m < 0 {
			return nil, false, protowire.ParseError(m)
		}
		data = data[m:]
	}
	return out, found, nil
}

func protobufInt64Field(data []byte, field protowire.Number) (int64, error) {
	var out int64
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		data = data[n:]
		if number == field {
			if wireType != protowire.VarintType {
				return 0, fmt.Errorf("field %d has wire type %d, want varint", field, wireType)
			}
			value, m := protowire.ConsumeVarint(data)
			if m < 0 {
				return 0, protowire.ParseError(m)
			}
			out = int64(value)
			data = data[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(number, wireType, data)
		if m < 0 {
			return 0, protowire.ParseError(m)
		}
		data = data[m:]
	}
	return out, nil
}
