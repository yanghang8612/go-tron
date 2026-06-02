package types

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// ContractTypeNone indicates no contract is present in the transaction.
const ContractTypeNone corepb.Transaction_Contract_ContractType = -1

type Transaction struct {
	pb       *corepb.Transaction
	hash     common.Hash
	hashOnce sync.Once

	// signers memoizes RecoverSigners' ECDSA output (recovered addresses or
	// the first recovery error) so the parallel pre-verification pass in
	// InsertBlocks can warm it off the serial critical path. The result is a
	// pure function of pb.RawData + pb.Signature, both immutable after
	// construction, so the cached value is identical-by-construction to an
	// inline recompute — this is a performance memo, never a semantics change.
	signersOnce sync.Once
	signers     []common.Address
	signersErr  error
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
		data, err := proto.Marshal(tx.pb.RawData)
		if err != nil {
			panic(fmt.Sprintf("transaction raw marshal failed: %v", err))
		}
		tx.hash = sha256.Sum256(data)
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
	out := make([]byte, 65)
	copy(out, sig[:65])
	out[64] = header - 27
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
	hash := tx.Hash()
	sigs := tx.Signatures()
	addrs := make([]common.Address, 0, len(sigs))
	for _, sig := range sigs {
		recoverySig, err := signatureForRecovery(sig)
		if err != nil {
			return nil, err
		}
		pub, err := crypto.SigToPub(hash[:], recoverySig)
		if err != nil {
			return nil, fmt.Errorf("transaction: recover signer: %w", err)
		}
		addrs = append(addrs, crypto.PubkeyToAddress(pub))
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
