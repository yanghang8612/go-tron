package core

import (
	"errors"
	"fmt"
	"math"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto/pq"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	transactionMaxByteSize     int64 = 500 * 1024
	maximumTimeUntilExpiration int64 = 24 * 60 * 60 * 1000
)

// Tx-envelope validation errors. They're separate from actuator.Validate
// errors so the caller (state_processor / txpool) can distinguish a
// permission/signature failure (always reject, no replay tolerance) from a
// state-precondition failure (insufficient balance, missing token, etc.).
var (
	ErrNoContract                = errors.New("transaction has no contract")
	ErrContractSizeNotEqualToOne = errors.New("transaction contract size should be exactly 1")
	ErrTransactionRetCount       = errors.New("transaction result count exceeds contract count")
	ErrTransactionRetMissing     = errors.New("transaction vm result missing")
	ErrTransactionRetMismatch    = errors.New("transaction vm result mismatch")
	ErrTransactionTooLarge       = errors.New("transaction size exceeds maximum")
	ErrTransactionExpiration     = errors.New("transaction expiration out of range")
	ErrMissingOwnerAddress       = errors.New("contract has no owner address")
	ErrNoSignature               = errors.New("transaction has no signature")
	ErrTooManySignatures         = errors.New("transaction has more signatures than permission keys")
	ErrPermissionNotFound        = errors.New("permission_id not configured on account")
	ErrPermissionForbidsType     = errors.New("permission operations bitmask forbids this contract type")
	ErrInsufficientWeight        = errors.New("signature weight below permission threshold")
	ErrUnauthorizedSigner        = errors.New("signer not in permission key set")
	ErrInvalidTxSignature        = errors.New("invalid transaction signature")
	ErrDuplicateSignature        = errors.New("transaction has duplicate signer")
	ErrSignatureWeightOverflow   = errors.New("signature weight overflow")
	// ErrShieldedUnexpectedSignature mirrors java TransactionCapsule
	// .validateSignature: a transfer FROM a shielded address (no transparent
	// owner) must carry NO transparent ECDSA signatures.
	ErrShieldedUnexpectedSignature = errors.New("shielded transfer must not carry transparent signatures")
	// ErrTransactionResultTooLarge mirrors java BandwidthProcessor.consume's
	// always-on getResultSerializedSize() > MAX_RESULT_SIZE_IN_TX*contractCount
	// guard (TooBigTransactionResultException).
	ErrTransactionResultTooLarge = errors.New("transaction result size exceeds maximum")
)

// ValidateTxEnvelope verifies the signature(s) on a transaction match the
// account permission named by contract[0].Permission_id under java-tron's
// multi-key, weight-based rules. Mirrors
// `TransactionUtil.validateSignature` + `AccountCapsule.getPermissionById`.
//
// The single source of truth for envelope-level rejection: a tx that fails
// here must never be Validate/Execute'd. Both block replay (state_processor
// .ApplyTransaction) and txpool admission (TxPool.Add) call this — keeping
// the rules in one place is the whole point of P0-2.
//
// Shielded transfer (ShieldedTransferContract) with no transparent input
// has no ECDSA signature path; the zk-proof check lives in the shielded
// actuator itself. We detect that case via the absence of a transparent
// owner address and short-circuit. Mixed shielded txs (with transparent
// input) carry an owner_address on `transparent_from_address` and fall
// through to the normal permission check.
//
// `multiSigByAddress` selects the dedup key for repeated signers, mirroring
// java-tron `TransactionCapsule.getCurrentWeight`:
//   - true  (VERSION_4_7_1 passed): dedup by recovered address; the same
//     address signing twice with different signatures collides.
//   - false (pre-VERSION_4_7_1):    dedup by raw signature bytes; the same
//     address may contribute multiple times if the signatures differ.
//
// Either way, a duplicate aborts with ErrDuplicateSignature — java throws
// "has signed twice". We always scan ALL signatures (no early return when
// threshold is met) so a duplicate or unauthorized signer trailing a
// satisfied threshold still rejects the tx.
func ValidateTxEnvelope(tx *types.Transaction, statedb *state.StateDB, multiSigByAddress bool, dynProps ...*state.DynamicProperties) error {
	if err := ValidateContractCount(tx); err != nil {
		return err
	}
	pb := tx.Proto()
	contract := pb.RawData.Contract[0]
	var dp *state.DynamicProperties
	if len(dynProps) != 0 {
		dp = dynProps[0]
	}

	ownerBytes, isShielded, err := extractContractOwner(contract)
	if err != nil {
		return err
	}
	if isShielded && len(ownerBytes) == 0 {
		// Fully shielded transfer: no ECDSA signer; zk-proof check is the
		// actuator's responsibility. But java TransactionCapsule.validateSignature
		// still REJECTS a shielded-source tx that carries transparent signatures
		// ("there should be no signatures ... when transfer from shielded address");
		// gtron previously skipped envelope validation entirely, accepting it.
		if len(tx.Proto().GetSignature()) > 0 || len(tx.Proto().GetPqAuthSig()) > 0 {
			return ErrShieldedUnexpectedSignature
		}
		return nil
	}
	if len(ownerBytes) == 0 {
		return ErrMissingOwnerAddress
	}
	ownerAddr := tcommon.BytesToAddress(ownerBytes)

	sigs := tx.Signatures()
	pqSigs := pb.GetPqAuthSig()
	if len(sigs)+len(pqSigs) == 0 {
		return ErrNoSignature
	}

	permID := contract.PermissionId
	var perm *corepb.Permission
	account := statedb.GetAccount(ownerAddr)
	if account == nil {
		// Account not yet materialized. java-tron's
		// AccountCapsule.getDefaultPermission returns a single-key Owner
		// permission keyed on ownerAddr — works for first-tx onboarding
		// (account creation pays bandwidth out of pocket). Any non-Owner
		// permission_id fails: there's no record of an active permission
		// for a never-seen account.
		switch permID {
		case 0:
			perm = types.MakeDefaultOwnerPermission(ownerAddr)
		case 2:
			if dp == nil {
				return ErrPermissionNotFound
			}
			perm = types.MakeDefaultActivePermission(ownerAddr, dp.ActiveDefaultOperations())
		default:
			return ErrPermissionNotFound
		}
	} else {
		perm = types.PermissionByID(account, permID)
		if perm == nil {
			// Legacy account materialized before the multi-sign fork
			// stored an explicit owner_permission, OR a fresh account
			// created via on-chain side-effect (Transfer-to-new-address)
			// that never ran ApplyDefaultAccountPermissions. java-tron
			// rebuilds the default Owner permission on the fly for these
			// when permission_id == 0. Without this fallback every such
			// historical account becomes unspendable — chain-split shape.
			if permID != 0 {
				return ErrPermissionNotFound
			}
			perm = types.MakeDefaultOwnerPermission(ownerAddr)
		}
	}

	if !types.OperationAllowed(perm, contract.Type) {
		return ErrPermissionForbidsType
	}

	// Cap sig count at len(perm.Keys): if you'd need more than `keys`
	// distinct signers to clear the threshold, the math can't work out and
	// extra signatures only waste bandwidth + dust the recovered-address
	// set. java-tron has the same guard.
	if len(sigs)+len(pqSigs) > len(perm.Keys) {
		return ErrTooManySignatures
	}

	var addrs []tcommon.Address
	if len(sigs) != 0 {
		addrs, err = tx.RecoverSigners()
		if err != nil {
			return ErrInvalidTxSignature
		}
	}

	// Mirrors java-tron TransactionCapsule.getCurrentWeight: scan every
	// signature, reject on duplicate, and never short-circuit once threshold
	// is reached — a later unauthorized or duplicate signer still aborts.
	//
	// Pre-VERSION_4_7_1 java dedups by getBase64FromByteString:
	// Rsv.fromSignature over bytes [0:65] followed by ECDSASignature.toBase64,
	// i.e. canonical v||r||s. This normalizes v=0/1 to 27/28 and ignores
	// trailing bytes past v. A naive string(sigs[i]) key would silently accept
	// duplicates that java rejects.
	var totalWeight int64
	seenAddr := make(map[tcommon.Address]struct{}, len(addrs))
	seenSig := make(map[string]struct{}, len(sigs))
	for i, addr := range addrs {
		sigKey, err := types.CanonicalSignatureKey(sigs[i])
		if err != nil {
			return ErrInvalidTxSignature
		}
		var dup bool
		if multiSigByAddress {
			_, dup = seenAddr[addr]
		} else {
			_, dup = seenSig[sigKey]
		}
		if dup {
			return ErrDuplicateSignature
		}
		seenAddr[addr] = struct{}{}
		seenSig[sigKey] = struct{}{}

		w := types.KeyWeight(perm, addr)
		if w == 0 {
			return ErrUnauthorizedSigner
		}
		if w > 0 && totalWeight > math.MaxInt64-w {
			return ErrSignatureWeightOverflow
		}
		totalWeight += w
	}

	// PQ signatures use the same permission key set and contribute the same
	// weights as ECDSA signatures. Their derived address must be unique across
	// both schemes, so a hybrid transaction cannot count the same permission
	// key twice. java-tron enables each scheme independently by proposal.
	digest := tx.Hash()
	for _, auth := range pqSigs {
		if dp == nil || !pqTxSchemeAllowed(auth.GetScheme(), dp) {
			return ErrInvalidTxSignature
		}
		addr, err := pq.Address(auth.GetScheme(), auth.GetPublicKey())
		if err != nil {
			return ErrInvalidTxSignature
		}
		if _, duplicate := seenAddr[addr]; duplicate {
			return ErrDuplicateSignature
		}
		seenAddr[addr] = struct{}{}
		w := types.KeyWeight(perm, addr)
		if w == 0 {
			return ErrUnauthorizedSigner
		}
		if err := pq.Validate(auth, addr, digest[:]); err != nil {
			return ErrInvalidTxSignature
		}
		if w > 0 && totalWeight > math.MaxInt64-w {
			return ErrSignatureWeightOverflow
		}
		totalWeight += w
	}
	if totalWeight < perm.Threshold {
		return ErrInsufficientWeight
	}
	return nil
}

func pqTxSchemeAllowed(scheme corepb.PQScheme, dp *state.DynamicProperties) bool {
	switch scheme {
	case corepb.PQScheme_FN_DSA_512:
		return dp.AllowFnDsa512()
	case corepb.PQScheme_ML_DSA_44:
		return dp.AllowMlDsa44()
	default:
		return false
	}
}

func ValidateTxRetCount(tx *types.Transaction) error {
	if err := ValidateContractCount(tx); err != nil {
		return err
	}
	if len(tx.Proto().Ret) > len(tx.Proto().RawData.Contract) {
		return ErrTransactionRetCount
	}
	return nil
}

func ValidateTxVMContractRet(tx *types.Transaction, actual corepb.Transaction_ResultContractResult) error {
	if err := ValidateContractCount(tx); err != nil {
		return err
	}
	switch tx.ContractType() {
	case corepb.Transaction_Contract_CreateSmartContract, corepb.Transaction_Contract_TriggerSmartContract:
	default:
		return nil
	}

	ret := tx.Proto().Ret
	if len(ret) == 0 {
		return fmt.Errorf("%w: tx %s", ErrTransactionRetMissing, tx.Hash())
	}
	expected := ret[0].GetContractRet()
	if expected != actual {
		return fmt.Errorf("%w: tx %s expected %s actual %s", ErrTransactionRetMismatch, tx.Hash(), expected, actual)
	}
	return nil
}

func ValidateContractCount(tx *types.Transaction) error {
	if tx == nil {
		return ErrNoContract
	}
	pb := tx.Proto()
	if pb == nil || pb.RawData == nil {
		return ErrNoContract
	}
	switch len(pb.RawData.Contract) {
	case 0:
		return ErrNoContract
	case 1:
		return nil
	default:
		return ErrContractSizeNotEqualToOne
	}
}

func ValidateTxCommon(tx *types.Transaction, headBlockTime int64) error {
	return validateTxCommon(tx, headBlockTime, true)
}

func validateTxCommon(tx *types.Transaction, headBlockTime int64, validateResultSize bool) error {
	if err := ValidateContractCount(tx); err != nil {
		return err
	}
	pb := tx.Proto()
	if validateResultSize {
		withoutRet := proto.Clone(pb).(*corepb.Transaction)
		withoutRet.Ret = nil
		generalBytesSize := int64(proto.Size(withoutRet)) + maxResultSizeInTx + maxResultSizeInTx
		if generalBytesSize > transactionMaxByteSize {
			return ErrTransactionTooLarge
		}
	}
	if int64(tx.Size()) > transactionMaxByteSize {
		return ErrTransactionTooLarge
	}

	expiration := tx.Expiration()
	if expiration <= headBlockTime || expiration > headBlockTime+maximumTimeUntilExpiration {
		return ErrTransactionExpiration
	}
	return nil
}

// extractContractOwner uses proto reflection to read the owner address from a
// Transaction_Contract regardless of its concrete type. Almost every TRON
// contract carries `bytes owner_address`; ShieldedTransferContract uses
// `bytes transparent_from_address` and may legitimately leave it empty.
//
// Returns:
//   - owner: the address bytes (may be empty for fully shielded txs)
//   - isShielded: true iff the contract type is ShieldedTransfer
//   - err: malformed parameter or unmarshal failure
func extractContractOwner(contract *corepb.Transaction_Contract) ([]byte, bool, error) {
	if contract.Parameter == nil {
		return nil, false, fmt.Errorf("contract has no parameter")
	}
	msg, err := contract.Parameter.UnmarshalNew()
	if err != nil {
		return nil, false, fmt.Errorf("unmarshal contract parameter: %w", err)
	}
	mr := msg.ProtoReflect()
	fields := mr.Descriptor().Fields()
	isShielded := contract.Type == corepb.Transaction_Contract_ShieldedTransferContract
	if fd := fields.ByName("owner_address"); fd != nil {
		return mr.Get(fd).Bytes(), isShielded, nil
	}
	if fd := fields.ByName("transparent_from_address"); fd != nil {
		return mr.Get(fd).Bytes(), isShielded, nil
	}
	return nil, isShielded, nil
}
