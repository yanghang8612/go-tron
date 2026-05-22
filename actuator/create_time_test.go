package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// M11.5 slice 2b: every actuator new-account path must stamp
// Account.create_time = ctx.DynProps.LatestBlockHeaderTimestamp(), mirroring
// java-tron's `new AccountCapsule(..., dynamicStore.getLatestBlockHeaderTimestamp(), ...)`
// at:
//   - TransferActuator.java:53-54
//   - TransferAssetActuator.java:66-67
//   - ShieldedTransferActuator.java:142-143
//   - CreateAccountActuator.java:44-45
//
// Per java-tron, create_time is unconditional — set on both withDefaultPermission
// branches (AccountCapsule.java:158-180), independent of AllowMultiSign. Tests
// here assert with AllowMultiSign disabled to lock that property.

const m115b_blockTS = int64(1_704_067_200_321) // 2024-01-01 00:00:00.321 UTC

// ---- TransferActuator ------------------------------------------------------

func TestTransferExecute_StampsCreateTimeFromDynProps(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	to := makeTestAddr(50)
	seedAccount(statedb, from, 10_000_000)

	tx := makeTransferTx(1, 50, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetLatestBlockHeaderTimestamp(m115b_blockTS)
	// AllowMultiSign deliberately false — create_time must NOT be gated on it.

	if _, err := (&TransferActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	got := statedb.GetAccount(to).CreateTime()
	if got != m115b_blockTS {
		t.Errorf("create_time = %d, want %d (= dp.LatestBlockHeaderTimestamp)", got, m115b_blockTS)
	}
}

// ---- CreateAccountActuator -------------------------------------------------

func TestCreateAccountExecute_StampsCreateTimeFromDynProps(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	newAddr := makeTestAddr(51)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeCreateAccountTx(1, 51)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetLatestBlockHeaderTimestamp(m115b_blockTS)

	if _, err := (&CreateAccountActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	got := statedb.GetAccount(newAddr).CreateTime()
	if got != m115b_blockTS {
		t.Errorf("create_time = %d, want %d", got, m115b_blockTS)
	}
}

// ---- TransferAssetActuator -------------------------------------------------

func TestTransferAssetExecute_StampsCreateTimeFromDynProps(t *testing.T) {
	const tokenID = int64(1_000_002)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(52)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000) // covers create-account fee
	statedb.SetTRC10Balance(owner, tokenID, 1_000_000)

	if err := statedb.WriteAssetIssue(tokenID, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	tx := makeTransferAssetTx(1, 52, tokenID, 500_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowSameTokenName(true)
	ctx.DynProps.SetLatestBlockHeaderTimestamp(m115b_blockTS)

	if _, err := (&TransferAssetActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	got := statedb.GetAccount(to).CreateTime()
	if got != m115b_blockTS {
		t.Errorf("create_time = %d, want %d", got, m115b_blockTS)
	}
}

// ---- ShieldedTransferActuator (transparent-out → creates recipient) --------

func TestShieldedTransferExecute_StampsCreateTimeFromDynProps(t *testing.T) {
	to := tcommon.Address{0x41, 0x53}
	nullifier := []byte("nullifier_m11_5_create_time____1")
	c := &contractpb.ShieldedTransferContract{
		SpendDescription:     []*contractpb.SpendDescription{{Nullifier: nullifier}},
		TransparentToAddress: to[:],
		ToAmount:             300_000,
	}
	ctx := setupShieldedCtx(t, c)
	ctx.DynProps.SetLatestBlockHeaderTimestamp(m115b_blockTS)
	ctx.DynProps.AdjustTotalShieldedPoolValue(1_000_000)

	if _, err := (&ShieldedTransferActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	got := ctx.State.GetAccount(to).CreateTime()
	if got != m115b_blockTS {
		t.Errorf("create_time = %d, want %d", got, m115b_blockTS)
	}
}

// ---- Negative: zero DP timestamp leaks zero through (parity, not bug) -----
//
// java-tron and go-tron both set create_time = whatever DP currently holds;
// at genesis or in a fresh DP it's 0. Lock that no surprise default is added.
func TestTransferExecute_ZeroDPTimestamp_StampsZero(t *testing.T) {
	statedb := setupStateDB(t)
	from := makeTestAddr(1)
	to := makeTestAddr(54)
	seedAccount(statedb, from, 10_000_000)

	tx := makeTransferTx(1, 54, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	// LatestBlockHeaderTimestamp left at 0.

	if _, err := (&TransferActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got := statedb.GetAccount(to).CreateTime(); got != 0 {
		t.Errorf("create_time = %d, want 0", got)
	}
}
