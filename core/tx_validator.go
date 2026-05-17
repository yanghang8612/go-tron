package core

import (
	"errors"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
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
	ErrNoContract                  = errors.New("transaction has no contract")
	ErrContractSizeNotEqualToOne   = errors.New("transaction contract size should be exactly 1")
	ErrTransactionRetCount         = errors.New("transaction result count exceeds contract count")
	ErrTransactionTooLarge         = errors.New("transaction size exceeds maximum")
	ErrTransactionExpiration       = errors.New("transaction expiration out of range")
	ErrMissingOwnerAddress         = errors.New("contract has no owner address")
	ErrNoSignature                 = errors.New("transaction has no signature")
	ErrTooManySignatures           = errors.New("transaction has more signatures than permission keys")
	ErrPermissionNotFound          = errors.New("permission_id not configured on account")
	ErrPermissionForbidsType       = errors.New("permission operations bitmask forbids this contract type")
	ErrInsufficientWeight          = errors.New("signature weight below permission threshold")
	ErrUnauthorizedSigner          = errors.New("signer not in permission key set")
	ErrAccountPermUpdateNotByOwner = errors.New("AccountPermissionUpdateContract must be signed with Owner permission")
	ErrInvalidTxSignature          = errors.New("invalid transaction signature")
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
func ValidateTxEnvelope(tx *types.Transaction, statedb *state.StateDB) error {
	if err := ValidateContractCount(tx); err != nil {
		return err
	}
	pb := tx.Proto()
	contract := pb.RawData.Contract[0]

	ownerBytes, isShielded, err := extractContractOwner(contract)
	if err != nil {
		return err
	}
	if isShielded && len(ownerBytes) == 0 {
		// Fully shielded transfer: no ECDSA signer; zk-proof check is the
		// actuator's responsibility. Skip envelope validation.
		return nil
	}
	if len(ownerBytes) == 0 {
		return ErrMissingOwnerAddress
	}
	ownerAddr := tcommon.BytesToAddress(ownerBytes)

	sigs := tx.Signatures()
	if len(sigs) == 0 {
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
		if permID != 0 {
			return ErrPermissionNotFound
		}
		perm = types.MakeDefaultOwnerPermission(ownerAddr)
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

	// AccountPermissionUpdate locks the signing-permission choice to Owner,
	// matching java-tron's hard-coded guard. Without this an active key
	// could rotate the owner key set, escalating privilege.
	if contract.Type == corepb.Transaction_Contract_AccountPermissionUpdateContract &&
		perm.Type != corepb.Permission_Owner {
		return ErrAccountPermUpdateNotByOwner
	}

	if !types.OperationAllowed(perm, contract.Type) {
		return ErrPermissionForbidsType
	}

	// Cap sig count at len(perm.Keys): if you'd need more than `keys`
	// distinct signers to clear the threshold, the math can't work out and
	// extra signatures only waste bandwidth + dust the recovered-address
	// set. java-tron has the same guard.
	if len(sigs) > len(perm.Keys) {
		return ErrTooManySignatures
	}

	addrs, err := tx.RecoverSigners()
	if err != nil {
		return ErrInvalidTxSignature
	}

	var totalWeight int64
	seen := make(map[tcommon.Address]struct{}, len(addrs))
	for _, addr := range addrs {
		if _, dup := seen[addr]; dup {
			// Duplicate signer — java-tron counts each address once. Skip.
			continue
		}
		seen[addr] = struct{}{}
		w := types.KeyWeight(perm, addr)
		if w == 0 {
			return ErrUnauthorizedSigner
		}
		totalWeight += w
		if totalWeight >= perm.Threshold {
			return nil
		}
	}
	return ErrInsufficientWeight
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
	if err := ValidateContractCount(tx); err != nil {
		return err
	}
	pb := tx.Proto()
	withoutRet := proto.Clone(pb).(*corepb.Transaction)
	withoutRet.Ret = nil
	generalBytesSize := int64(proto.Size(withoutRet)) + maxResultSizeInTx + maxResultSizeInTx
	if generalBytesSize > transactionMaxByteSize {
		return ErrTransactionTooLarge
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
