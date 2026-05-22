package actuator

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// M11.5: under AllowMultiSign, every actuator that creates a new account must
// install the default Owner + Active[0] permission, with Active[0].Operations
// loaded from `dp.ActiveDefaultOperations()`. With AllowMultiSign disabled,
// new accounts must NOT have any default permissions installed (preserves
// pre-fork behavior).

// ---- TransferActuator ------------------------------------------------------

func TestTransferExecute_PreFork_NoDefaultPermissions(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	to := makeTestAddr(31)
	seedAccount(statedb, from, 10_000_000)

	tx := makeTransferTx(1, 31, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	// AllowMultiSign defaults to 0 -> false.

	if _, err := (&TransferActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	acct := statedb.GetAccount(to)
	if acct == nil {
		t.Fatal("recipient account should exist")
	}
	if acct.OwnerPermission() != nil {
		t.Errorf("OwnerPermission: want nil pre-fork, got %+v", acct.OwnerPermission())
	}
	if len(acct.ActivePermission()) != 0 {
		t.Errorf("ActivePermission: want empty pre-fork, got %d entries", len(acct.ActivePermission()))
	}
}

func TestTransferExecute_PostFork_LoadsDefaultPermissions(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	to := makeTestAddr(32)
	seedAccount(statedb, from, 10_000_000)

	tx := makeTransferTx(1, 32, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)

	if _, err := (&TransferActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	assertDefaultPermissions(t, statedb, ctx.DynProps.ActiveDefaultOperations(), to)
}

// ---- CreateAccountActuator -------------------------------------------------

func TestCreateAccountExecute_PostFork_LoadsDefaultPermissions(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	newAddr := makeTestAddr(33)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeCreateAccountTx(1, 33)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)

	if _, err := (&CreateAccountActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	assertDefaultPermissions(t, statedb, ctx.DynProps.ActiveDefaultOperations(), newAddr)
}

// ---- TransferAssetActuator -------------------------------------------------

func TestTransferAssetExecute_PostFork_LoadsDefaultPermissions(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(34)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000) // covers create-account fee
	statedb.SetTRC10Balance(owner, tokenID, 1_000_000)

	if err := statedb.WriteAssetIssue(tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 34, tokenID, 500_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.DynProps.SetAllowSameTokenName(true)

	if _, err := (&TransferAssetActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	assertDefaultPermissions(t, statedb, ctx.DynProps.ActiveDefaultOperations(), to)
}

// ---- ShieldedTransferActuator (transparent-out → creates recipient) --------

func TestShieldedTransferExecute_PostFork_LoadsDefaultPermissions(t *testing.T) {
	to := tcommon.Address{0x41, 0x35}
	nullifier := []byte("nullifier_m11_5_default_perm___1")
	c := &contractpb.ShieldedTransferContract{
		SpendDescription:     []*contractpb.SpendDescription{{Nullifier: nullifier}},
		TransparentToAddress: to[:],
		ToAmount:             300_000,
	}
	ctx := setupShieldedCtx(t, c)
	ctx.DynProps.SetAllowMultiSign(true)
	// Pool seeded so the deduction is well-formed.
	ctx.DynProps.AdjustTotalShieldedPoolValue(1_000_000)

	if _, err := (&ShieldedTransferActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	assertDefaultPermissions(t, ctx.State, ctx.DynProps.ActiveDefaultOperations(), to)
}

// ---- Cross-cutting: proposal flip affects only later accounts --------------

func TestDefaultPermissions_ProposalFlipAffectsOnlyLaterAccounts(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	earlyTo := makeTestAddr(40)
	lateTo := makeTestAddr(41)
	seedAccount(statedb, from, 100_000_000)

	// Create early recipient under the default operations bitmap.
	tx1 := makeTransferTx(1, 40, 1_000_000)
	ctx1 := setupContext(t, statedb, tx1)
	ctx1.DynProps.SetAllowMultiSign(true)

	dp := ctx1.DynProps
	preBitmap := append([]byte(nil), dp.ActiveDefaultOperations()...)

	if _, err := (&TransferActuator{}).Execute(ctx1); err != nil {
		t.Fatalf("execute1 failed: %v", err)
	}

	// Simulate a proposal flipping a previously-unset bit (bit 100 — high
	// in the bitmap, far from the default-set first-6-byte block).
	dp.AddSystemContractAndSetPermission(100)
	postBitmap := append([]byte(nil), dp.ActiveDefaultOperations()...)
	if bytes.Equal(preBitmap, postBitmap) {
		t.Fatal("flip did not change bitmap; test is meaningless")
	}

	// Create late recipient under the new bitmap; reuse the same DP instance
	// so the flip is visible.
	tx2 := makeTransferTx(1, 41, 1_000_000)
	ctx2 := setupContext(t, statedb, tx2)
	ctx2.DynProps = dp

	if _, err := (&TransferActuator{}).Execute(ctx2); err != nil {
		t.Fatalf("execute2 failed: %v", err)
	}

	earlyOps := getActiveOperations(t, statedb, earlyTo)
	lateOps := getActiveOperations(t, statedb, lateTo)
	if !bytes.Equal(earlyOps, preBitmap) {
		t.Errorf("earlyTo operations = %x, want preBitmap %x", earlyOps, preBitmap)
	}
	if !bytes.Equal(lateOps, postBitmap) {
		t.Errorf("lateTo operations = %x, want postBitmap %x", lateOps, postBitmap)
	}
	if bytes.Equal(earlyOps, lateOps) {
		t.Error("earlyTo and lateTo should have different operations bitmaps")
	}
}

// ---- helpers ---------------------------------------------------------------

// assertDefaultPermissions checks that addr has the default Owner shape and an
// Active[0] whose Operations equals wantOps (32-byte exact match).
func assertDefaultPermissions(t *testing.T, statedb *state.StateDB, wantOps []byte, addr tcommon.Address) {
	t.Helper()
	acct := statedb.GetAccount(addr)
	if acct == nil {
		t.Fatalf("account %x missing", addr)
	}
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission is nil; want default owner shape")
	}
	if owner.Type != corepb.Permission_Owner {
		t.Errorf("Owner.Type: want Owner, got %v", owner.Type)
	}
	if owner.Id != 0 || owner.PermissionName != "owner" || owner.Threshold != 1 {
		t.Errorf("Owner shape: id=%d name=%q threshold=%d", owner.Id, owner.PermissionName, owner.Threshold)
	}
	if len(owner.Keys) != 1 || string(owner.Keys[0].Address) != string(addr.Bytes()) || owner.Keys[0].Weight != 1 {
		t.Errorf("Owner key mismatch")
	}

	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry, got %d", len(actives))
	}
	active := actives[0]
	if active.Type != corepb.Permission_Active {
		t.Errorf("Active.Type: want Active, got %v", active.Type)
	}
	if active.Id != 2 || active.PermissionName != "active" || active.Threshold != 1 {
		t.Errorf("Active shape: id=%d name=%q threshold=%d", active.Id, active.PermissionName, active.Threshold)
	}
	if len(active.Keys) != 1 || string(active.Keys[0].Address) != string(addr.Bytes()) || active.Keys[0].Weight != 1 {
		t.Errorf("Active key mismatch")
	}
	if !bytes.Equal(active.Operations, wantOps) {
		t.Errorf("Active.Operations = %x, want %x", active.Operations, wantOps)
	}
}

func getActiveOperations(t *testing.T, statedb *state.StateDB, addr tcommon.Address) []byte {
	t.Helper()
	acct := statedb.GetAccount(addr)
	if acct == nil {
		t.Fatalf("account %x missing", addr)
	}
	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry, got %d", len(actives))
	}
	return actives[0].Operations
}
