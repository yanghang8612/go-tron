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
	"google.golang.org/protobuf/reflect/protoreflect"
)

var ErrInvalidTransactionMerkleRoot = errors.New("block transaction merkle root mismatch")

var blockDecodeReserveLayoutOK = verifyBlockDecodeReserveLayout()

// decodedWireBlock coallocates the wrapper and the two singular block-header
// messages that every canonical block carries. decodedWireTransaction does the
// same for Transaction.raw_data and reserves the overwhelmingly common single
// signature/result/contract slots inline. The protobuf decoder appends into
// these zero-length slices under Merge mode, preserving generated decode
// semantics while avoiding several tiny heap objects per transaction.
type decodedWireBlock struct {
	block     Block
	pb        corepb.Block
	header    corepb.BlockHeader
	headerRaw corepb.BlockHeaderRaw
}

type decodedWireTransaction struct {
	tx            corepb.Transaction
	raw           corepb.TransactionRaw
	signatureSlot [1][]byte
	resultSlot    [1]*corepb.Transaction_Result
	contractSlot  [1]*corepb.Transaction_Contract
}

type blockTransactionReserve struct {
	rawPresent bool
	signatures int
	results    int
	pqAuthSigs int
	auths      int
	contracts  int
}

func verifyBlockDecodeReserveLayout() bool {
	blockFields := (&corepb.Block{}).ProtoReflect().Descriptor().Fields()
	if blockFields.Len() != 2 || !protoFieldShape(blockFields, 1, protoreflect.MessageKind, true) ||
		!protoFieldShape(blockFields, 2, protoreflect.MessageKind, false) {
		return false
	}
	txFields := (&corepb.Transaction{}).ProtoReflect().Descriptor().Fields()
	if txFields.Len() != 4 || !protoFieldShape(txFields, 1, protoreflect.MessageKind, false) ||
		!protoFieldShape(txFields, 2, protoreflect.BytesKind, true) ||
		!protoFieldShape(txFields, 5, protoreflect.MessageKind, true) ||
		!protoFieldShape(txFields, 6, protoreflect.MessageKind, true) {
		return false
	}
	rawFields := (&corepb.TransactionRaw{}).ProtoReflect().Descriptor().Fields()
	if rawFields.Len() != 10 || !protoFieldShape(rawFields, 9, protoreflect.MessageKind, true) ||
		!protoFieldShape(rawFields, 11, protoreflect.MessageKind, true) {
		return false
	}
	headerFields := (&corepb.BlockHeader{}).ProtoReflect().Descriptor().Fields()
	return headerFields.Len() == 3 && protoFieldShape(headerFields, 1, protoreflect.MessageKind, false)
}

func protoFieldShape(fields protoreflect.FieldDescriptors, number protoreflect.FieldNumber, kind protoreflect.Kind, list bool) bool {
	field := fields.ByNumber(number)
	return field != nil && field.Kind() == kind && field.IsList() == list
}

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

	// marshalScratch is an exclusively-owned wire buffer transferred by the
	// sync pipeline after decode. Commit marshals the protobuf back into this
	// capacity, preserving canonical proto.Marshal output while avoiding a
	// second full-block allocation. It is consumed exactly once.
	marshalScratch []byte

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
		// The pointers stored in txs keep storage's backing allocation alive.
		// Each element therefore owns independent sync.Once-backed caches while
		// all wrappers require only one block-sized heap object.
		storage := make([]Transaction, len(b.pb.Transactions))
		txs := make([]*Transaction, len(b.pb.Transactions))
		for i, pb := range b.pb.Transactions {
			storage[i].pb = pb
			txs[i] = &storage[i]
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

// MarshalReusable is Marshal with a one-shot, exclusively-owned destination
// buffer. Sync blocks already own their received frame bytes after decoding;
// reusing that capacity through MarshalAppend retains canonical protobuf
// encoding (unlike persisting arbitrary raw wire order) without allocating a
// second block-sized slice. The returned bytes belong to the caller.
func (b *Block) MarshalReusable() ([]byte, error) {
	scratch := b.marshalScratch
	b.marshalScratch = nil
	return proto.MarshalOptions{}.MarshalAppend(scratch[:0], b.pb)
}

// AdoptMarshalScratch transfers an immutable wire buffer's capacity to b for
// its next MarshalReusable call. The caller must not access or mutate data
// afterward. It is public only for the network sync package's ownership
// handoff; ordinary callers should use Marshal.
func (b *Block) AdoptMarshalScratch(data []byte) {
	if data == nil {
		b.marshalScratch = nil
		return
	}
	// Clamp capacity to the owned message bytes. Even if a transport handed us
	// a subslice of a larger frame allocation, MarshalAppend must never overwrite
	// adjacent frame storage through spare capacity.
	b.marshalScratch = data[:len(data):len(data)]
}

func UnmarshalBlock(data []byte) (*Block, error) {
	block, err := unmarshalBlockReserved(data)
	if err != nil {
		// Keep the generated decoder authoritative on malformed input. The fast
		// path only changes allocation layout for valid messages; rerunning this
		// cold path preserves its exact error and partial-decode behaviour before
		// the historical pre-PQ compatibility retry below.
		pb := &corepb.Block{}
		err = proto.Unmarshal(data, pb)
		if err == nil {
			return NewBlockFromPB(pb), nil
		}
		// VERSION_4_8_2_PQ1 assigned protobuf fields that older nodes had
		// treated as unknown. Historical mainnet blocks can therefore contain
		// arbitrary length-delimited data at Transaction field 6 (block
		// 10,476,461 is one example). The PQ-aware decoder tries to interpret
		// that legacy value as a nested PQAuthSig and rejects the whole block.
		// Preserve such values in protobuf's unknown-field set for pre-PQ block
		// versions, exactly matching the schema that produced those blocks.
		legacyPB, ok := unmarshalPrePQBlock(data)
		if !ok {
			return nil, err
		}
		return NewBlockFromPB(legacyPB), nil
	}
	return block, nil
}

var blockMergeUnmarshal = proto.UnmarshalOptions{Merge: true}

// unmarshalBlockReserved decodes only Block's two-field envelope directly;
// every nested message still goes through protobuf's generated decoder. This
// lets each transaction seed repeated-slice capacity before decode without
// reimplementing any contract/result/PQ wire semantics. A schema guard makes
// regenerated Block layouts fall back to the generated decoder automatically.
func unmarshalBlockReserved(data []byte) (*Block, error) {
	if !blockDecodeReserveLayoutOK {
		pb := new(corepb.Block)
		if err := proto.Unmarshal(data, pb); err != nil {
			return nil, err
		}
		return NewBlockFromPB(pb), nil
	}
	txCount, ok := countBlockTransactionFields(data)
	if !ok {
		return nil, errors.New("malformed block wire envelope")
	}
	decoded := new(decodedWireBlock)
	decoded.block.pb = &decoded.pb
	if txCount != 0 {
		decoded.pb.Transactions = make([]*corepb.Transaction, 0, txCount)
	}

	var unknown []byte
	for len(data) != 0 {
		fieldData := data
		field, wireType, n := protowire.ConsumeField(fieldData)
		if n < 0 || !field.IsValid() {
			if n >= 0 {
				return nil, errors.New("invalid block field number")
			}
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if wireType != protowire.BytesType || (field != 1 && field != 2) {
			_, _, tagLen := protowire.ConsumeTag(fieldData[:n])
			unknown = protowire.AppendTag(unknown, field, wireType)
			unknown = append(unknown, fieldData[tagLen:n]...)
			continue
		}
		value, ok := bytesFieldValue(fieldData[:n])
		if !ok {
			return nil, errors.New("malformed block bytes field")
		}
		if field == 1 {
			tx, err := unmarshalBlockTransactionReserved(value)
			if err != nil {
				return nil, err
			}
			decoded.pb.Transactions = append(decoded.pb.Transactions, tx)
			continue
		}
		if decoded.pb.BlockHeader == nil {
			decoded.pb.BlockHeader = &decoded.header
		}
		if decoded.header.RawData == nil && hasBytesField(value, 1) {
			decoded.header.RawData = &decoded.headerRaw
		}
		if err := blockMergeUnmarshal.Unmarshal(value, &decoded.header); err != nil {
			return nil, err
		}
	}
	appendProtoUnknown(&decoded.pb, unknown)
	return &decoded.block, nil
}

func countBlockTransactionFields(data []byte) (int, bool) {
	count := 0
	for len(data) != 0 {
		field, wireType, n := protowire.ConsumeField(data)
		if n < 0 || !field.IsValid() {
			return 0, false
		}
		if field == 1 && wireType == protowire.BytesType {
			count++
		}
		data = data[n:]
	}
	return count, true
}

func unmarshalBlockTransactionReserved(data []byte) (*corepb.Transaction, error) {
	reserve, ok := scanBlockTransactionReserve(data)
	if !ok {
		return nil, errors.New("malformed transaction wire envelope")
	}
	decoded := new(decodedWireTransaction)
	if reserve.rawPresent {
		decoded.tx.RawData = &decoded.raw
	}
	if reserve.signatures == 1 {
		decoded.tx.Signature = decoded.signatureSlot[:0]
	} else if reserve.signatures > 1 {
		decoded.tx.Signature = make([][]byte, 0, reserve.signatures)
	}
	if reserve.results == 1 {
		decoded.tx.Ret = decoded.resultSlot[:0]
	} else if reserve.results > 1 {
		decoded.tx.Ret = make([]*corepb.Transaction_Result, 0, reserve.results)
	}
	if reserve.pqAuthSigs != 0 {
		decoded.tx.PqAuthSig = make([]*corepb.PQAuthSig, 0, reserve.pqAuthSigs)
	}
	if reserve.auths != 0 {
		decoded.raw.Auths = make([]*corepb.Authority, 0, reserve.auths)
	}
	if reserve.contracts == 1 {
		decoded.raw.Contract = decoded.contractSlot[:0]
	} else if reserve.contracts > 1 {
		decoded.raw.Contract = make([]*corepb.Transaction_Contract, 0, reserve.contracts)
	}
	if err := blockMergeUnmarshal.Unmarshal(data, &decoded.tx); err != nil {
		return nil, err
	}
	return &decoded.tx, nil
}

func scanBlockTransactionReserve(data []byte) (blockTransactionReserve, bool) {
	var reserve blockTransactionReserve
	for len(data) != 0 {
		fieldData := data
		field, wireType, n := protowire.ConsumeField(fieldData)
		if n < 0 || !field.IsValid() {
			return blockTransactionReserve{}, false
		}
		data = data[n:]
		if wireType != protowire.BytesType {
			continue
		}
		switch field {
		case 1:
			value, ok := bytesFieldValue(fieldData[:n])
			if !ok {
				return blockTransactionReserve{}, false
			}
			reserve.rawPresent = true
			auths, contracts, ok := scanTransactionRawRepeated(value)
			if !ok {
				return blockTransactionReserve{}, false
			}
			reserve.auths += auths
			reserve.contracts += contracts
		case 2:
			reserve.signatures++
		case 5:
			reserve.results++
		case 6:
			reserve.pqAuthSigs++
		}
	}
	return reserve, true
}

func scanTransactionRawRepeated(data []byte) (auths, contracts int, ok bool) {
	for len(data) != 0 {
		field, wireType, n := protowire.ConsumeField(data)
		if n < 0 || !field.IsValid() {
			return 0, 0, false
		}
		if wireType == protowire.BytesType {
			if field == 9 {
				auths++
			} else if field == 11 {
				contracts++
			}
		}
		data = data[n:]
	}
	return auths, contracts, true
}

func hasBytesField(data []byte, want protowire.Number) bool {
	for len(data) != 0 {
		field, wireType, n := protowire.ConsumeField(data)
		if n < 0 || !field.IsValid() {
			return false
		}
		if field == want && wireType == protowire.BytesType {
			return true
		}
		data = data[n:]
	}
	return false
}

func bytesFieldValue(fieldData []byte) ([]byte, bool) {
	field, _, tagLen := protowire.ConsumeTag(fieldData)
	if tagLen < 0 || !field.IsValid() {
		return nil, false
	}
	value, valueLen := protowire.ConsumeBytes(fieldData[tagLen:])
	return value, valueLen >= 0 && tagLen+valueLen == len(fieldData)
}

// firstPQBlockVersion is VERSION_4_8_2_PQ1. Blocks below this version were
// produced with a schema where Transaction field 6 and BlockHeader field 3
// were unknown, so their contents must not be recursively decoded as PQAuthSig.
const firstPQBlockVersion int64 = 37

// unmarshalPrePQBlock retries a block after moving malformed values that
// collide with later PQAuthSig field assignments into protobuf unknown-field
// storage. It returns ok=false unless the block is pre-PQ, contains at least
// one such collision, and is otherwise a valid core.Block protobuf.
func unmarshalPrePQBlock(data []byte) (*corepb.Block, bool) {
	header, ok, err := protobufBytesField(data, 2)
	if err != nil || !ok {
		return nil, false
	}
	rawHeader, ok, err := protobufBytesField(header, 1)
	if err != nil || !ok {
		return nil, false
	}
	version, err := protobufInt64Field(rawHeader, 10)
	if err != nil || version >= firstPQBlockVersion {
		return nil, false
	}

	cleanBlock := make([]byte, 0, len(data))
	var txUnknown [][]byte
	var headerUnknown []byte
	changed := false
	for len(data) > 0 {
		field, wireType, n := protowire.ConsumeField(data)
		if n < 0 {
			return nil, false
		}
		rawField := data[:n]
		data = data[n:]
		if (field != 1 && field != 2) || wireType != protowire.BytesType {
			cleanBlock = append(cleanBlock, rawField...)
			continue
		}
		_, _, tagLen := protowire.ConsumeTag(rawField)
		if tagLen < 0 {
			return nil, false
		}
		value, valueLen := protowire.ConsumeBytes(rawField[tagLen:])
		if valueLen < 0 {
			return nil, false
		}

		pqField := protowire.Number(6)
		if field == 2 {
			pqField = 3
		}
		var message proto.Message = &corepb.Transaction{}
		if field == 2 {
			message = &corepb.BlockHeader{}
		}
		cleanValue, unknown, fieldChanged, err := stripMalformedPQFields(value, pqField, message)
		if err != nil {
			return nil, false
		}
		if field == 1 {
			txUnknown = append(txUnknown, unknown)
		} else {
			headerUnknown = unknown
		}
		if !fieldChanged {
			cleanBlock = append(cleanBlock, rawField...)
			continue
		}
		changed = true
		cleanBlock = protowire.AppendTag(cleanBlock, field, protowire.BytesType)
		cleanBlock = protowire.AppendBytes(cleanBlock, cleanValue)
	}
	if !changed {
		return nil, false
	}

	pb := &corepb.Block{}
	if err := proto.Unmarshal(cleanBlock, pb); err != nil || len(pb.Transactions) != len(txUnknown) {
		return nil, false
	}
	for i, unknown := range txUnknown {
		appendProtoUnknown(pb.Transactions[i], unknown)
	}
	if len(headerUnknown) != 0 {
		if pb.BlockHeader == nil {
			return nil, false
		}
		appendProtoUnknown(pb.BlockHeader, headerUnknown)
	}
	return pb, true
}

// stripMalformedPQFields removes only values that cannot be decoded as the
// newer nested PQAuthSig message. The returned unknown bytes include their
// original field tags and lengths so proto.Marshal preserves block size and
// legacy wire contents.
func stripMalformedPQFields(data []byte, pqField protowire.Number, message proto.Message) (clean, unknown []byte, changed bool, err error) {
	clean = make([]byte, 0, len(data))
	fields := message.ProtoReflect().Descriptor().Fields()
	for len(data) > 0 {
		field, wireType, n := protowire.ConsumeField(data)
		if n < 0 {
			return nil, nil, false, protowire.ParseError(n)
		}
		rawField := data[:n]
		data = data[n:]
		malformedPQ := false
		if field == pqField {
			if wireType != protowire.BytesType {
				malformedPQ = true
			} else {
				_, _, tagLen := protowire.ConsumeTag(rawField)
				if tagLen < 0 {
					return nil, nil, false, errors.New("malformed PQ field envelope")
				}
				value, valueLen := protowire.ConsumeBytes(rawField[tagLen:])
				if valueLen < 0 {
					return nil, nil, false, errors.New("malformed PQ field envelope")
				}
				malformedPQ = proto.Unmarshal(value, &corepb.PQAuthSig{}) != nil
			}
		}
		// Once a malformed PQ collision requires rebuilding this message, keep
		// every unknown field out of the intermediate decode. They are restored
		// together below in their original order; otherwise protobuf would place
		// the removed PQ field after unknown fields that originally followed it.
		if malformedPQ || fields.ByNumber(protoreflect.FieldNumber(field)) == nil {
			unknown = append(unknown, rawField...)
			changed = changed || malformedPQ
			continue
		}
		clean = append(clean, rawField...)
	}
	return clean, unknown, changed, nil
}

func appendProtoUnknown(message proto.Message, unknown []byte) {
	if len(unknown) == 0 {
		return
	}
	reflection := message.ProtoReflect()
	existing := reflection.GetUnknown()
	combined := make([]byte, 0, len(existing)+len(unknown))
	combined = append(combined, existing...)
	combined = append(combined, unknown...)
	reflection.SetUnknown(combined)
}

// UnmarshalBlockOwned is UnmarshalBlock plus ownership transfer of data's
// backing capacity. protobuf decoding does not alias input bytes, so the block
// may later overwrite that buffer while producing its canonical durable
// encoding through MarshalReusable.
func UnmarshalBlockOwned(data []byte) (*Block, error) {
	block, err := UnmarshalBlock(data)
	if err != nil {
		return nil, err
	}
	block.AdoptMarshalScratch(data)
	return block, nil
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
