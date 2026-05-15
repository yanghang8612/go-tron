package core

import (
	"crypto/ecdsa"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// newValidatorState builds an in-memory StateDB suitable for unit-testing
// ValidateTxEnvelope. A live DynamicProperties is wired so the default
// `active_default_operations` bitmap (matching java-tron mainnet) flows
// through to MakeDefaultActivePermission.
func newValidatorState(t *testing.T) (*state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb, state.NewDynamicProperties()
}

// buildTransferTx assembles a TransferContract tx with the given owner +
// permission_id and signs it once with each provided key. ref_block fields
// are deliberately zero-valued: chain-level tests that go through
// bc.ValidateTransaction (which checks TAPOS) should rebuild via
// buildTransferTxWithRef pointing at a real recent-block ring slot. The
// unit-level ValidateTxEnvelope tests bypass TAPOS and don't care.
func buildTransferTx(t *testing.T, owner, recipient tcommon.Address, amount, permissionID int32, signers ...*ecdsa.PrivateKey) *types.Transaction {
	t.Helper()
	return buildTransferTxWithRef(t, owner, recipient, amount, permissionID, nil, nil, signers...)
}

// buildTransferTxWithRef extends buildTransferTx with caller-supplied
// ref_block_bytes (2B) / ref_block_hash (8B). Pass nil for both to skip
// (default zero-value protobuf encoding).
func buildTransferTxWithRef(t *testing.T, owner, recipient tcommon.Address, amount, permissionID int32, refBytes, refHash []byte, signers ...*ecdsa.PrivateKey) *types.Transaction {
	t.Helper()
	tc := &contractpb.TransferContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    recipient.Bytes(),
		Amount:       int64(amount),
	}
	param, err := anypb.New(tc)
	if err != nil {
		t.Fatal(err)
	}
	pbTx := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			RefBlockBytes: refBytes,
			RefBlockHash:  refHash,
			Contract: []*corepb.Transaction_Contract{{
				Type:         corepb.Transaction_Contract_TransferContract,
				Parameter:    param,
				PermissionId: permissionID,
			}},
		},
	}
	tx := types.NewTransactionFromPB(pbTx)
	hash := tx.Hash()
	for _, k := range signers {
		sig, err := crypto.Sign(hash[:], k)
		if err != nil {
			t.Fatal(err)
		}
		tx.Proto().Signature = append(tx.Proto().Signature, sig)
	}
	return tx
}

func keyAndAddr(t *testing.T) (*ecdsa.PrivateKey, tcommon.Address) {
	t.Helper()
	k, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k, crypto.PubkeyToAddress(&k.PublicKey)
}

// TestValidateTxEnvelope_DefaultPermission_NewAccount: an account that
// doesn't yet exist still validates with the default Owner permission
// (single key = owner_address). This is the "first transaction creates the
// account" path — pool admission can't refuse on `account not found` since
// the very first tx is exactly that case.
func TestValidateTxEnvelope_DefaultPermission_NewAccount(t *testing.T) {
	statedb, _ := newValidatorState(t)
	ownerKey, owner := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	tx := buildTransferTx(t, owner, recipient, 100, 0, ownerKey)
	if err := ValidateTxEnvelope(tx, statedb); err != nil {
		t.Fatalf("expected accept, got %v", err)
	}
}

// TestValidateTxEnvelope_DefaultPermission_WrongKey: same shape, but signed
// by an unrelated key. Recovered address isn't in the default permission's
// key set ⇒ unauthorized.
func TestValidateTxEnvelope_DefaultPermission_WrongKey(t *testing.T) {
	statedb, _ := newValidatorState(t)
	_, owner := keyAndAddr(t)
	_, recipient := keyAndAddr(t)
	wrongKey, _ := keyAndAddr(t)

	tx := buildTransferTx(t, owner, recipient, 100, 0, wrongKey)
	err := ValidateTxEnvelope(tx, statedb)
	if !errors.Is(err, ErrUnauthorizedSigner) {
		t.Fatalf("expected ErrUnauthorizedSigner, got %v", err)
	}
}

// TestValidateTxEnvelope_NoSignature.
func TestValidateTxEnvelope_NoSignature(t *testing.T) {
	statedb, _ := newValidatorState(t)
	_, owner := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	tx := buildTransferTx(t, owner, recipient, 100, 0)
	if err := ValidateTxEnvelope(tx, statedb); !errors.Is(err, ErrNoSignature) {
		t.Fatalf("expected ErrNoSignature, got %v", err)
	}
}

// TestValidateTxEnvelope_ExistingAccount_OwnerPermission: account
// materialized with the default Owner permission; signing with the owner
// key still works (regression of the default-perm path when an account
// exists).
func TestValidateTxEnvelope_ExistingAccount_OwnerPermission(t *testing.T) {
	statedb, dp := newValidatorState(t)
	ownerKey, owner := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.ApplyDefaultAccountPermissions(owner, dp)

	tx := buildTransferTx(t, owner, recipient, 100, 0, ownerKey)
	if err := ValidateTxEnvelope(tx, statedb); err != nil {
		t.Fatalf("expected accept, got %v", err)
	}
}

// TestValidateTxEnvelope_ActivePermission_AllowedOp: a custom Active
// permission with a single delegate key authorizes the contract type via
// its operations bitmask.
func TestValidateTxEnvelope_ActivePermission_AllowedOp(t *testing.T) {
	statedb, dp := newValidatorState(t)
	_, owner := keyAndAddr(t)
	delegateKey, delegate := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	// Bitmask with TransferContract bit set (type id = 1).
	ops := make([]byte, 32)
	const transferOp = int(corepb.Transaction_Contract_TransferContract)
	ops[transferOp/8] |= 1 << uint(transferOp%8)
	active := &corepb.Permission{
		Type:       corepb.Permission_Active,
		Id:         2,
		Threshold:  1,
		Operations: ops,
		Keys:       []*corepb.Key{{Address: delegate.Bytes(), Weight: 1}},
	}
	statedb.SetPermissions(owner,
		types.MakeDefaultOwnerPermission(owner),
		nil,
		[]*corepb.Permission{active},
	)
	_ = dp

	tx := buildTransferTx(t, owner, recipient, 100, 2, delegateKey)
	if err := ValidateTxEnvelope(tx, statedb); err != nil {
		t.Fatalf("expected accept (delegate active perm), got %v", err)
	}
}

// TestValidateTxEnvelope_ActivePermission_ForbiddenOp: same shape but the
// bitmask has the TransferContract bit *cleared*; java-tron rejects with
// "permission denied". Our analog: ErrPermissionForbidsType.
func TestValidateTxEnvelope_ActivePermission_ForbiddenOp(t *testing.T) {
	statedb, _ := newValidatorState(t)
	_, owner := keyAndAddr(t)
	delegateKey, delegate := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ops := make([]byte, 32) // all zeros — no op allowed
	active := &corepb.Permission{
		Type:       corepb.Permission_Active,
		Id:         2,
		Threshold:  1,
		Operations: ops,
		Keys:       []*corepb.Key{{Address: delegate.Bytes(), Weight: 1}},
	}
	statedb.SetPermissions(owner,
		types.MakeDefaultOwnerPermission(owner),
		nil,
		[]*corepb.Permission{active},
	)

	tx := buildTransferTx(t, owner, recipient, 100, 2, delegateKey)
	if err := ValidateTxEnvelope(tx, statedb); !errors.Is(err, ErrPermissionForbidsType) {
		t.Fatalf("expected ErrPermissionForbidsType, got %v", err)
	}
}

// TestValidateTxEnvelope_MultiSig_Pass: 2-of-3 weighted permission with two
// signers reaches threshold; pass.
func TestValidateTxEnvelope_MultiSig_Pass(t *testing.T) {
	statedb, _ := newValidatorState(t)
	_, owner := keyAndAddr(t)
	k1, a1 := keyAndAddr(t)
	k2, a2 := keyAndAddr(t)
	_, a3 := keyAndAddr(t) // third key, unused
	_, recipient := keyAndAddr(t)

	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ops := make([]byte, 32)
	const transferOp = int(corepb.Transaction_Contract_TransferContract)
	ops[transferOp/8] |= 1 << uint(transferOp%8)
	active := &corepb.Permission{
		Type:       corepb.Permission_Active,
		Id:         2,
		Threshold:  2,
		Operations: ops,
		Keys: []*corepb.Key{
			{Address: a1.Bytes(), Weight: 1},
			{Address: a2.Bytes(), Weight: 1},
			{Address: a3.Bytes(), Weight: 1},
		},
	}
	statedb.SetPermissions(owner,
		types.MakeDefaultOwnerPermission(owner),
		nil,
		[]*corepb.Permission{active},
	)

	tx := buildTransferTx(t, owner, recipient, 100, 2, k1, k2)
	if err := ValidateTxEnvelope(tx, statedb); err != nil {
		t.Fatalf("expected accept (2 sigs ≥ threshold 2), got %v", err)
	}
}

// TestValidateTxEnvelope_MultiSig_InsufficientWeight: 2-of-3 threshold but
// only one valid signer ⇒ reject.
func TestValidateTxEnvelope_MultiSig_InsufficientWeight(t *testing.T) {
	statedb, _ := newValidatorState(t)
	_, owner := keyAndAddr(t)
	k1, a1 := keyAndAddr(t)
	_, a2 := keyAndAddr(t)
	_, a3 := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ops := make([]byte, 32)
	const transferOp = int(corepb.Transaction_Contract_TransferContract)
	ops[transferOp/8] |= 1 << uint(transferOp%8)
	active := &corepb.Permission{
		Type:       corepb.Permission_Active,
		Id:         2,
		Threshold:  2,
		Operations: ops,
		Keys: []*corepb.Key{
			{Address: a1.Bytes(), Weight: 1},
			{Address: a2.Bytes(), Weight: 1},
			{Address: a3.Bytes(), Weight: 1},
		},
	}
	statedb.SetPermissions(owner,
		types.MakeDefaultOwnerPermission(owner),
		nil,
		[]*corepb.Permission{active},
	)

	tx := buildTransferTx(t, owner, recipient, 100, 2, k1)
	if err := ValidateTxEnvelope(tx, statedb); !errors.Is(err, ErrInsufficientWeight) {
		t.Fatalf("expected ErrInsufficientWeight, got %v", err)
	}
}

// TestValidateTxEnvelope_DuplicateSigners_NoDoubleCount: same key signing
// twice counts as one weight; threshold 2 with a single duplicated signer
// must fail rather than passing on 1+1.
func TestValidateTxEnvelope_DuplicateSigners_NoDoubleCount(t *testing.T) {
	statedb, _ := newValidatorState(t)
	_, owner := keyAndAddr(t)
	k1, a1 := keyAndAddr(t)
	_, a2 := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ops := make([]byte, 32)
	const transferOp = int(corepb.Transaction_Contract_TransferContract)
	ops[transferOp/8] |= 1 << uint(transferOp%8)
	active := &corepb.Permission{
		Type:       corepb.Permission_Active,
		Id:         2,
		Threshold:  2,
		Operations: ops,
		Keys: []*corepb.Key{
			{Address: a1.Bytes(), Weight: 1},
			{Address: a2.Bytes(), Weight: 1},
		},
	}
	statedb.SetPermissions(owner,
		types.MakeDefaultOwnerPermission(owner),
		nil,
		[]*corepb.Permission{active},
	)

	// Sign with k1 twice. Dedupe must keep weight at 1; threshold=2 fails.
	tx := buildTransferTx(t, owner, recipient, 100, 2, k1, k1)
	if err := ValidateTxEnvelope(tx, statedb); !errors.Is(err, ErrInsufficientWeight) {
		t.Fatalf("expected ErrInsufficientWeight (duplicate signer can't double-count), got %v", err)
	}
}

// TestValidateTxEnvelope_PermissionNotFound: contract names permission_id=2
// but the account has only the default Owner+Active[0] (id=2 IS active[0]).
// Make permission_id=5 to actually miss.
func TestValidateTxEnvelope_PermissionNotFound(t *testing.T) {
	statedb, dp := newValidatorState(t)
	ownerKey, owner := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.ApplyDefaultAccountPermissions(owner, dp)

	tx := buildTransferTx(t, owner, recipient, 100, 5, ownerKey)
	if err := ValidateTxEnvelope(tx, statedb); !errors.Is(err, ErrPermissionNotFound) {
		t.Fatalf("expected ErrPermissionNotFound, got %v", err)
	}
}

// TestValidateTxEnvelope_PerTxInterleaving: java-tron Manager.processBlock
// runs signature/permission validation *inside* the per-tx loop, AFTER
// prior txs in the same block have mutated state. The concrete case: a
// block containing [AccountPermissionUpdate replacing owner keys, Transfer
// signed with the post-rotation key]. ValidateTxEnvelope on the second tx
// MUST see the just-mutated permission set.
//
// Regression of P0-2b's first iteration which validated all txs against
// pre-block state in a single sweep — that shape rejected post-rotation
// signers and would chain-split if mainnet history contained such a block.
func TestValidateTxEnvelope_PerTxInterleaving(t *testing.T) {
	statedb, dp := newValidatorState(t)
	oldKey, owner := keyAndAddr(t)
	newKey, newAddr := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	// Owner starts with the default Owner permission (single key = owner).
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.ApplyDefaultAccountPermissions(owner, dp)

	// Pre-rotation: a transfer signed with the new key must be rejected —
	// recovered address isn't in the current Owner permission.
	txPre := buildTransferTx(t, owner, recipient, 100, 0, newKey)
	if err := ValidateTxEnvelope(txPre, statedb); !errors.Is(err, ErrUnauthorizedSigner) {
		t.Fatalf("pre-rotation: expected ErrUnauthorizedSigner, got %v", err)
	}

	// Simulate AccountPermissionUpdate.Execute by directly rotating the
	// Owner permission to newAddr (single key, threshold 1).
	statedb.SetPermissions(owner,
		&corepb.Permission{
			Type:      corepb.Permission_Owner,
			Id:        0,
			Threshold: 1,
			Keys:      []*corepb.Key{{Address: newAddr.Bytes(), Weight: 1}},
		},
		nil,
		[]*corepb.Permission{types.MakeDefaultActivePermission(owner, dp.ActiveDefaultOperations())},
	)

	// Post-rotation: the same transfer (rebuilt to pick up no state
	// dependency — only the recovered signer matters) signed with newKey
	// must now pass.
	txPost := buildTransferTx(t, owner, recipient, 100, 0, newKey)
	if err := ValidateTxEnvelope(txPost, statedb); err != nil {
		t.Fatalf("post-rotation: expected accept, got %v", err)
	}

	// And the old key, which was just rotated out, must now be rejected.
	txOld := buildTransferTx(t, owner, recipient, 100, 0, oldKey)
	if err := ValidateTxEnvelope(txOld, statedb); !errors.Is(err, ErrUnauthorizedSigner) {
		t.Fatalf("old key after rotation: expected ErrUnauthorizedSigner, got %v", err)
	}
}

// TestValidateTxEnvelope_LegacyAccount_NoOwnerPermission: an account
// materialized before multi-sign existed (or via on-chain side-effect like
// receive-Transfer) has no explicit owner_permission. Java-tron's
// TransactionUtil.validateSignature falls back to the default single-key
// Owner permission for permission_id=0. Without this fallback every such
// historical account becomes unspendable on replay.
func TestValidateTxEnvelope_LegacyAccount_NoOwnerPermission(t *testing.T) {
	statedb, _ := newValidatorState(t)
	ownerKey, owner := keyAndAddr(t)
	_, recipient := keyAndAddr(t)

	// Materialize the account but DO NOT install permissions — emulates a
	// fresh account created by Transfer-to-new-address before multi-sign.
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	tx := buildTransferTx(t, owner, recipient, 100, 0, ownerKey)
	if err := ValidateTxEnvelope(tx, statedb); err != nil {
		t.Fatalf("legacy account fallback: expected accept, got %v", err)
	}
}

// TestValidateTxEnvelope_AccountPermissionUpdate_MustUseOwner: the
// AccountPermissionUpdateContract is privileged — only the Owner permission
// can authorize it. Using an Active permission must be rejected even if all
// other checks pass (signer is in active perm key set, operation bitmask
// allows it, threshold met). Mirrors java-tron's hard-coded guard.
func TestValidateTxEnvelope_AccountPermissionUpdate_MustUseOwner(t *testing.T) {
	statedb, _ := newValidatorState(t)
	_, owner := keyAndAddr(t)
	delegateKey, delegate := keyAndAddr(t)

	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	const apuOp = int(corepb.Transaction_Contract_AccountPermissionUpdateContract)
	ops := make([]byte, 32)
	ops[apuOp/8] |= 1 << uint(apuOp%8) // deliberately permissive
	active := &corepb.Permission{
		Type:       corepb.Permission_Active,
		Id:         2,
		Threshold:  1,
		Operations: ops,
		Keys:       []*corepb.Key{{Address: delegate.Bytes(), Weight: 1}},
	}
	statedb.SetPermissions(owner,
		types.MakeDefaultOwnerPermission(owner),
		nil,
		[]*corepb.Permission{active},
	)

	// Build an AccountPermissionUpdateContract tx and sign with delegate.
	apu := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner.Bytes(),
	}
	param, _ := anypb.New(apu)
	pbTx := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:         corepb.Transaction_Contract_AccountPermissionUpdateContract,
				Parameter:    param,
				PermissionId: 2, // active, NOT owner
			}},
		},
	}
	tx := types.NewTransactionFromPB(pbTx)
	hash := tx.Hash()
	sig, _ := crypto.Sign(hash[:], delegateKey)
	tx.Proto().Signature = [][]byte{sig}

	if err := ValidateTxEnvelope(tx, statedb); !errors.Is(err, ErrAccountPermUpdateNotByOwner) {
		t.Fatalf("expected ErrAccountPermUpdateNotByOwner, got %v", err)
	}
}
