package types

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ContractTypeNone indicates no contract is present in the transaction.
const ContractTypeNone corepb.Transaction_Contract_ContractType = -1

// triggerDataInlineSize fits the dominant token-call shape: a 4-byte selector
// plus two 32-byte ABI words. Keeping four words inline saved one allocation for
// some larger calls but charged every transaction wrapper another 64 bytes;
// larger calls already retain an exact-sized owned copy on the cold path below.
const triggerDataInlineSize = 68

// Current TRC10 IDs are seven ASCII digits. Keep the dominant replay shape in
// the one owned decode object without charging every transfer for the legacy
// 32-byte asset-name limit; longer names retain an exact standalone copy.
const transferAssetNameInlineSize = 8

// Exchange token IDs use "_" for TRX or the same seven-digit TRC10 ID shape.
const exchangeTokenIDInlineSize = 8

var (
	triggerDecodeReserveLayoutOK       = verifyTriggerDecodeReserveLayout()
	transferDecodeReserveLayoutOK      = verifyTransferDecodeReserveLayout()
	transferAssetDecodeReserveLayoutOK = verifyTransferAssetDecodeReserveLayout()
	exchangeTxDecodeReserveLayoutOK    = verifyExchangeTransactionDecodeReserveLayout()
)

// decodedTransferContract coalesces the message and its two canonical TRON
// addresses into one 144-byte ownership object. A pointer to contract keeps the
// entire wrapper reachable through Transaction.contractMessage.
type decodedTransferContract struct {
	contract     contractpb.TransferContract
	ownerAddress [common.AddressLength]byte
	toAddress    [common.AddressLength]byte
}

// decodedTransferAssetContract coalesces the protobuf message and its three
// byte fields into one ownership object. A pointer to contract keeps the whole
// wrapper reachable through Transaction.contractMessage.
type decodedTransferAssetContract struct {
	contract     contractpb.TransferAssetContract
	assetName    [transferAssetNameInlineSize]byte
	ownerAddress [common.AddressLength]byte
	toAddress    [common.AddressLength]byte
}

type decodedExchangeTransactionContract struct {
	contract     contractpb.ExchangeTransactionContract
	ownerAddress [common.AddressLength]byte
	tokenID      [exchangeTokenIDInlineSize]byte
}

type Transaction struct {
	pb       *corepb.Transaction
	hash     common.Hash
	hashOnce sync.Once

	// contractMessage memoizes Any.UnmarshalNew for the first (and, by TRON
	// envelope rules, only) contract. Envelope validation, bandwidth charging,
	// actuator validation/execution and energy settlement all inspect the same
	// immutable transaction parameter. Keeping one decoded, read-only message
	// avoids rebuilding the protobuf object at every stage. Callers must not
	// mutate the returned message; actuators that modify their local contract
	// representation must continue to decode or clone their own copy.
	contractMessageOnce sync.Once
	contractMessage     proto.Message
	contractMessageErr  error
	// triggerContract owns the dominant mainnet contract type inline. Block
	// wrappers are already coallocated in one []Transaction backing object, so
	// decoding a matching Any into this slot removes one standalone protobuf
	// allocation without extending the lifetime of any additional object.
	triggerContract        contractpb.TriggerSmartContract
	triggerOwnerAddress    [common.AddressLength]byte
	triggerContractAddress [common.AddressLength]byte
	triggerData            [triggerDataInlineSize]byte
	// borrowTriggerData is set only by Block.Transactions. A block owns its
	// immutable protobuf graph (including Any.Value), so oversized calldata can
	// safely alias that storage for the wrapper's equally long lifetime. A
	// standalone transaction retains the established independent-copy behavior.
	borrowTriggerData bool

	// signers memoizes RecoverSigners' ECDSA output (recovered addresses or
	// the first recovery error) so the parallel pre-verification pass in
	// InsertBlocks can warm it off the serial critical path. The result is a
	// pure function of pb.RawData + pb.Signature, both immutable after
	// construction, so the cached value is identical-by-construction to an
	// inline recompute — this is a performance memo, never a semantics change.
	signersOnce sync.Once
	signers     []common.Address
	signersErr  error
	// signerInline owns the overwhelmingly common single-signature result.
	// RecoverSigners caches the returned slice for the wrapper's lifetime, so
	// placing its one element here removes one tiny heap object per ordinary
	// transaction without changing the public []Address result.
	signerInline [1]common.Address
}

func NewTransactionFromPB(pb *corepb.Transaction) *Transaction {
	return &Transaction{pb: pb}
}

func (tx *Transaction) Proto() *corepb.Transaction { return tx.pb }

func (tx *Transaction) Hash() common.Hash {
	tx.hashOnce.Do(func() {
		if tx.pb.RawData == nil {
			return
		}
		hash, err := hashProtoMessage(tx.pb.RawData)
		if err != nil {
			panic(fmt.Sprintf("transaction raw marshal failed: %v", err))
		}
		tx.hash = hash
	})
	return tx.hash
}

func (tx *Transaction) ContractType() corepb.Transaction_Contract_ContractType {
	if tx.pb.RawData == nil || len(tx.pb.RawData.Contract) == 0 {
		return ContractTypeNone
	}
	return tx.pb.RawData.Contract[0].Type
}

func (tx *Transaction) Contract() *corepb.Transaction_Contract {
	if tx.pb.RawData == nil || len(tx.pb.RawData.Contract) == 0 {
		return nil
	}
	return tx.pb.RawData.Contract[0]
}

// DecodedContract returns the first contract parameter decoded as its concrete
// protobuf message. The returned message is owned by tx and must be treated as
// read-only. Both a successful result and an error are memoized.
func (tx *Transaction) DecodedContract() (proto.Message, error) {
	tx.contractMessageOnce.Do(func() {
		contract := tx.Contract()
		if contract == nil {
			tx.contractMessageErr = errors.New("transaction has no contract")
			return
		}
		if contract.Parameter == nil {
			tx.contractMessageErr = errors.New("contract has no parameter")
			return
		}
		if contract.Parameter.MessageIs(&tx.triggerContract) {
			tx.contractMessage = &tx.triggerContract
			if triggerDecodeReserveLayoutOK {
				tx.contractMessageErr = tx.unmarshalTriggerContractInline(contract.Parameter.Value)
				if tx.contractMessageErr == nil {
					return
				}
			}
			// Preserve the generated decoder's exact error on the cold malformed
			// path, and retain the allocation-only R99 fast path if a regenerated
			// TriggerSmartContract schema disables the manual envelope decoder.
			tx.contractMessageErr = contract.Parameter.UnmarshalTo(tx.contractMessage)
			return
		}
		if contract.Type == corepb.Transaction_Contract_TransferContract {
			decoded := new(decodedTransferContract)
			if contract.Parameter.MessageIs(&decoded.contract) {
				tx.contractMessage = &decoded.contract
				if transferDecodeReserveLayoutOK {
					tx.contractMessageErr = decoded.unmarshal(contract.Parameter.Value)
					if tx.contractMessageErr == nil {
						return
					}
				}
				// Preserve generated decoding errors for malformed data and act as
				// the schema-regeneration fallback.
				tx.contractMessageErr = contract.Parameter.UnmarshalTo(tx.contractMessage)
				return
			}
		}
		if contract.Type == corepb.Transaction_Contract_TransferAssetContract {
			decoded := new(decodedTransferAssetContract)
			if contract.Parameter.MessageIs(&decoded.contract) {
				tx.contractMessage = &decoded.contract
				if transferAssetDecodeReserveLayoutOK {
					tx.contractMessageErr = decoded.unmarshal(contract.Parameter.Value)
					if tx.contractMessageErr == nil {
						return
					}
				}
				// Preserve generated decoding errors for malformed data and act as
				// the schema-regeneration fallback.
				tx.contractMessageErr = contract.Parameter.UnmarshalTo(tx.contractMessage)
				return
			}
		}
		if contract.Type == corepb.Transaction_Contract_ExchangeTransactionContract {
			decoded := new(decodedExchangeTransactionContract)
			if contract.Parameter.MessageIs(&decoded.contract) {
				tx.contractMessage = &decoded.contract
				if exchangeTxDecodeReserveLayoutOK {
					tx.contractMessageErr = decoded.unmarshal(contract.Parameter.Value)
					if tx.contractMessageErr == nil {
						return
					}
				}
				// Preserve generated decoding errors for malformed data and act as
				// the schema-regeneration fallback.
				tx.contractMessageErr = contract.Parameter.UnmarshalTo(tx.contractMessage)
				return
			}
		}
		tx.contractMessage, tx.contractMessageErr = contract.Parameter.UnmarshalNew()
	})
	return tx.contractMessage, tx.contractMessageErr
}

func verifyTriggerDecodeReserveLayout() bool {
	fields := (&contractpb.TriggerSmartContract{}).ProtoReflect().Descriptor().Fields()
	return fields.Len() == 6 &&
		protoFieldShape(fields, 1, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 2, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 3, protoreflect.Int64Kind, false) &&
		protoFieldShape(fields, 4, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 5, protoreflect.Int64Kind, false) &&
		protoFieldShape(fields, 6, protoreflect.Int64Kind, false)
}

func verifyTransferDecodeReserveLayout() bool {
	fields := (&contractpb.TransferContract{}).ProtoReflect().Descriptor().Fields()
	return fields.Len() == 3 &&
		protoFieldShape(fields, 1, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 2, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 3, protoreflect.Int64Kind, false)
}

func verifyTransferAssetDecodeReserveLayout() bool {
	fields := (&contractpb.TransferAssetContract{}).ProtoReflect().Descriptor().Fields()
	return fields.Len() == 4 &&
		protoFieldShape(fields, 1, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 2, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 3, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 4, protoreflect.Int64Kind, false)
}

func verifyExchangeTransactionDecodeReserveLayout() bool {
	fields := (&contractpb.ExchangeTransactionContract{}).ProtoReflect().Descriptor().Fields()
	return fields.Len() == 5 &&
		protoFieldShape(fields, 1, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 2, protoreflect.Int64Kind, false) &&
		protoFieldShape(fields, 3, protoreflect.BytesKind, false) &&
		protoFieldShape(fields, 4, protoreflect.Int64Kind, false) &&
		protoFieldShape(fields, 5, protoreflect.Int64Kind, false)
}

func (decoded *decodedTransferContract) unmarshal(data []byte) error {
	decoded.contract = contractpb.TransferContract{}
	var unknown []byte
	for len(data) != 0 {
		fieldData := data
		field, wireType, n := protowire.ConsumeField(fieldData)
		if n < 0 || !field.IsValid() {
			return errors.New("malformed transfer contract wire envelope")
		}
		data = data[n:]
		switch {
		case wireType == protowire.BytesType && (field == 1 || field == 2):
			value, ok := bytesFieldValue(fieldData[:n])
			if !ok {
				return errors.New("malformed transfer contract bytes field")
			}
			if field == 1 {
				decoded.contract.OwnerAddress = copyBytesInto(value, decoded.ownerAddress[:])
			} else {
				decoded.contract.ToAddress = copyBytesInto(value, decoded.toAddress[:])
			}
		case wireType == protowire.VarintType && field == 3:
			value, ok := varintFieldValue(fieldData[:n])
			if !ok {
				return errors.New("malformed transfer contract amount field")
			}
			decoded.contract.Amount = int64(value)
		default:
			unknown = appendCanonicalUnknown(unknown, fieldData[:n], field, wireType)
		}
	}
	appendProtoUnknown(&decoded.contract, unknown)
	return nil
}

func (decoded *decodedTransferAssetContract) unmarshal(data []byte) error {
	decoded.contract = contractpb.TransferAssetContract{}
	var unknown []byte
	for len(data) != 0 {
		fieldData := data
		field, wireType, n := protowire.ConsumeField(fieldData)
		if n < 0 || !field.IsValid() {
			return errors.New("malformed transfer asset contract wire envelope")
		}
		data = data[n:]
		switch {
		case wireType == protowire.BytesType && (field == 1 || field == 2 || field == 3):
			value, ok := bytesFieldValue(fieldData[:n])
			if !ok {
				return errors.New("malformed transfer asset contract bytes field")
			}
			switch field {
			case 1:
				decoded.contract.AssetName = copyBytesInto(value, decoded.assetName[:])
			case 2:
				decoded.contract.OwnerAddress = copyBytesInto(value, decoded.ownerAddress[:])
			case 3:
				decoded.contract.ToAddress = copyBytesInto(value, decoded.toAddress[:])
			}
		case wireType == protowire.VarintType && field == 4:
			value, ok := varintFieldValue(fieldData[:n])
			if !ok {
				return errors.New("malformed transfer asset contract amount field")
			}
			decoded.contract.Amount = int64(value)
		default:
			unknown = appendCanonicalUnknown(unknown, fieldData[:n], field, wireType)
		}
	}
	appendProtoUnknown(&decoded.contract, unknown)
	return nil
}

func (decoded *decodedExchangeTransactionContract) unmarshal(data []byte) error {
	decoded.contract = contractpb.ExchangeTransactionContract{}
	var unknown []byte
	for len(data) != 0 {
		fieldData := data
		field, wireType, n := protowire.ConsumeField(fieldData)
		if n < 0 || !field.IsValid() {
			return errors.New("malformed exchange transaction contract wire envelope")
		}
		data = data[n:]
		switch {
		case wireType == protowire.BytesType && (field == 1 || field == 3):
			value, ok := bytesFieldValue(fieldData[:n])
			if !ok {
				return errors.New("malformed exchange transaction contract bytes field")
			}
			if field == 1 {
				decoded.contract.OwnerAddress = copyBytesInto(value, decoded.ownerAddress[:])
			} else {
				decoded.contract.TokenId = copyBytesInto(value, decoded.tokenID[:])
			}
		case wireType == protowire.VarintType && (field == 2 || field == 4 || field == 5):
			value, ok := varintFieldValue(fieldData[:n])
			if !ok {
				return errors.New("malformed exchange transaction contract varint field")
			}
			switch field {
			case 2:
				decoded.contract.ExchangeId = int64(value)
			case 4:
				decoded.contract.Quant = int64(value)
			case 5:
				decoded.contract.Expected = int64(value)
			}
		default:
			unknown = appendCanonicalUnknown(unknown, fieldData[:n], field, wireType)
		}
	}
	appendProtoUnknown(&decoded.contract, unknown)
	return nil
}

func (tx *Transaction) unmarshalTriggerContractInline(data []byte) error {
	tx.triggerContract = contractpb.TriggerSmartContract{}
	var unknown []byte
	for len(data) != 0 {
		fieldData := data
		field, wireType, n := protowire.ConsumeField(fieldData)
		if n < 0 || !field.IsValid() {
			return errors.New("malformed trigger contract wire envelope")
		}
		data = data[n:]
		switch {
		case wireType == protowire.BytesType && (field == 1 || field == 2 || field == 4):
			value, ok := bytesFieldValue(fieldData[:n])
			if !ok {
				return errors.New("malformed trigger contract bytes field")
			}
			switch field {
			case 1:
				tx.triggerContract.OwnerAddress = copyBytesInto(value, tx.triggerOwnerAddress[:])
			case 2:
				tx.triggerContract.ContractAddress = copyBytesInto(value, tx.triggerContractAddress[:])
			case 4:
				tx.triggerContract.Data = tx.copyTriggerData(value)
			}
		case wireType == protowire.VarintType && (field == 3 || field == 5 || field == 6):
			value, ok := varintFieldValue(fieldData[:n])
			if !ok {
				return errors.New("malformed trigger contract varint field")
			}
			switch field {
			case 3:
				tx.triggerContract.CallValue = int64(value)
			case 5:
				tx.triggerContract.CallTokenValue = int64(value)
			case 6:
				tx.triggerContract.TokenId = int64(value)
			}
		default:
			unknown = appendCanonicalUnknown(unknown, fieldData[:n], field, wireType)
		}
	}
	appendProtoUnknown(&tx.triggerContract, unknown)
	return nil
}

func (tx *Transaction) copyTriggerData(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	if len(value) <= cap(tx.triggerData) {
		out := tx.triggerData[:len(value)]
		copy(out, value)
		return out
	}
	if tx.borrowTriggerData {
		return value[:len(value):len(value)]
	}
	return append([]byte(nil), value...)
}

func copyBytesInto(value, storage []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	if len(value) <= cap(storage) {
		out := storage[:len(value)]
		copy(out, value)
		return out
	}
	return append([]byte(nil), value...)
}

func (tx *Transaction) Timestamp() int64 {
	if tx.pb.RawData == nil {
		return 0
	}
	return tx.pb.RawData.Timestamp
}

func (tx *Transaction) Expiration() int64 {
	if tx.pb.RawData == nil {
		return 0
	}
	return tx.pb.RawData.Expiration
}

func (tx *Transaction) FeeLimit() int64 {
	if tx.pb.RawData == nil {
		return 0
	}
	return tx.pb.RawData.FeeLimit
}

func (tx *Transaction) Signatures() [][]byte {
	return tx.pb.Signature
}

// ErrBadSignatureLength means a tx signature element was shorter than the
// canonical 65 bytes (r ‖ s ‖ v). Returned by RecoverSigners.
var ErrBadSignatureLength = errors.New("transaction: signature length < 65")

// ErrBadSignatureRecoveryID means a tx signature's v/recovery-id byte is outside
// java-tron's accepted range after Rsv.fromSignature normalization.
var ErrBadSignatureRecoveryID = errors.New("transaction: signature recovery id out of range")

func javaSignatureHeader(sig []byte) (byte, error) {
	if len(sig) < 65 {
		return 0, ErrBadSignatureLength
	}
	v := int(sig[64])
	if v < 27 {
		v += 27
	}
	if v < 27 || v > 34 {
		return 0, ErrBadSignatureRecoveryID
	}
	return byte(v), nil
}

func signatureForRecovery(sig []byte) ([]byte, error) {
	header, err := javaSignatureHeader(sig)
	if err != nil {
		return nil, err
	}
	if header >= 31 {
		header -= 4
	}
	recoveryID := header - 27
	// Signatures produced by java-tron and gtron normally already carry the
	// geth recovery id (0/1). Transaction protobufs are immutable after their
	// wrapper is constructed and Ecrecover only reads its input, so the common
	// case can borrow the first 65 bytes directly. Historical Java-style v=27/28
	// (and v=31..34) still need a private normalized copy. Besides the copy this
	// removes one heap allocation per ordinary signature from sync prewarming.
	if sig[64] == recoveryID {
		return sig[:65], nil
	}
	out := make([]byte, 65)
	copy(out, sig[:65])
	out[64] = recoveryID
	return out, nil
}

// CanonicalSignatureKey returns java-tron's pre-VERSION_4_7_1 duplicate-signature
// key for a transaction signature. TransactionCapsule.getBase64FromByteString
// canonicalizes through Rsv.fromSignature and ECDSASignature.toBase64, which is
// v||r||s with v normalized into java's 27..34 header range; bytes after the
// first 65 are ignored.
func CanonicalSignatureKey(sig []byte) (string, error) {
	header, err := javaSignatureHeader(sig)
	if err != nil {
		return "", err
	}
	key := make([]byte, 65)
	key[0] = header
	copy(key[1:33], sig[:32])
	copy(key[33:65], sig[32:64])
	return string(key), nil
}

// RecoverSigners returns the address recovered from each signature in
// tx.Signatures, signing over the tx RawData hash. The order matches the
// signature order; callers that need set semantics (e.g. weight summation
// across distinct keys) must dedupe themselves.
//
// Canonical signatures are at least 65 bytes (r ‖ s ‖ v). java's
// Rsv.fromSignature takes [0:32], [32:64], [64], maps v<27 to v+27, and
// silently ignores anything past byte 65; checkWeight only rejects
// sig.size() < 65 (TransactionCapsule.checkWeight). Historical Nile txs carry
// both 66-byte payloads with trailing bytes and Java-style v=27/28 signatures.
// Match the parity rule: require len(sig) >= 65, normalize v like java-tron,
// then pass a geth-compatible recovery id to crypto.SigToPub.
func (tx *Transaction) RecoverSigners() ([]common.Address, error) {
	tx.signersOnce.Do(func() {
		tx.signers, tx.signersErr = tx.recoverSigners()
	})
	return tx.signers, tx.signersErr
}

// recoverSigners performs the actual per-signature ECDSA recovery. It is a pure
// function of the transaction's immutable raw data and signatures, so its result
// is safe to memoize (see RecoverSigners) and to compute concurrently across
// transactions during pre-verification.
func (tx *Transaction) recoverSigners() ([]common.Address, error) {
	// Warm the hash once, then lend the recovery routine the Transaction-owned
	// bytes directly. Keeping Hash's value return in a local [32]byte makes that
	// copy escape when passed as a slice across the crypto package boundary.
	tx.Hash()
	sigs := tx.Signatures()
	var addrs []common.Address
	switch len(sigs) {
	case 0:
		return nil, nil
	case 1:
		addrs = tx.signerInline[:]
	default:
		addrs = make([]common.Address, len(sigs))
	}
	for i, sig := range sigs {
		recoverySig, err := signatureForRecovery(sig)
		if err != nil {
			return nil, err
		}
		// SigToAddressJavaCompat mirrors java-tron's ECKey.signatureToAddress,
		// including the point-at-infinity quirk where a recovery that lands on
		// the infinity point resolves to keccak256("")[12:] rather than
		// failing (Nile block 18,278,266). Genuine bad signatures still error.
		addr, err := crypto.SigToAddressJavaCompat(tx.hash[:], recoverySig)
		if err != nil {
			return nil, fmt.Errorf("transaction: recover signer: %w", err)
		}
		addrs[i] = addr
	}
	return addrs, nil
}

func (tx *Transaction) Size() int {
	return proto.Size(tx.pb)
}

func (tx *Transaction) Marshal() ([]byte, error) {
	return proto.Marshal(tx.pb)
}

func UnmarshalTransaction(data []byte) (*Transaction, error) {
	pb := &corepb.Transaction{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}
	return NewTransactionFromPB(pb), nil
}
