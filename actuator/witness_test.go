package actuator

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWitnessCreateTx(ownerByte byte, url string) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	contract := &contractpb.WitnessCreateContract{
		OwnerAddress: owner.Bytes(),
		Url:          []byte(url),
	}
	anyParam, _ := anypb.New(contract)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_WitnessCreateContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestWitnessCreateExecute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 100_000_000_000)

	tx := makeWitnessCreateTx(1, "http://test.com")
	ctx := setupContext(t, statedb, tx)
	act := &WitnessCreateActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	w := statedb.GetWitness(owner)
	if w == nil {
		t.Fatal("witness should exist after creation")
	}
	if w.URL() != "http://test.com" {
		t.Fatalf("expected url http://test.com, got %s", w.URL())
	}
	if w.VoteCount() != 0 {
		t.Fatalf("initial vote count should be 0, got %d", w.VoteCount())
	}
}

func TestWitnessCreateExecute_TotalCreateWitnessCostAccumulates(t *testing.T) {
	statedb := setupStateDB(t)
	owner1 := makeTestAddr(1)
	owner2 := makeTestAddr(2)
	seedAccount(statedb, owner1, 100_000_000_000)
	seedAccount(statedb, owner2, 100_000_000_000)

	ctx1 := setupContext(t, statedb, makeWitnessCreateTx(1, "http://a.com"))
	if before := ctx1.DynProps.TotalCreateWitnessCost(); before != 0 {
		t.Fatalf("initial TotalCreateWitnessCost: want 0, got %d", before)
	}

	fee := ctx1.DynProps.AccountUpgradeCost()
	if _, err := (&WitnessCreateActuator{}).Execute(ctx1); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if got := ctx1.DynProps.TotalCreateWitnessCost(); got != fee {
		t.Errorf("after first witness: want %d, got %d", fee, got)
	}

	ctx2 := setupContext(t, statedb, makeWitnessCreateTx(2, "http://b.com"))
	// Reuse the same DP — production path holds a single DP per block.
	ctx2.DynProps = ctx1.DynProps
	if _, err := (&WitnessCreateActuator{}).Execute(ctx2); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if got := ctx2.DynProps.TotalCreateWitnessCost(); got != 2*fee {
		t.Errorf("after second witness: want %d, got %d", 2*fee, got)
	}
}

func TestWitnessCreateValidate_AlreadyWitness(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 100_000_000_000)

	// Pre-register as witness
	statedb.PutWitness(owner, "http://existing.com")

	tx := makeWitnessCreateTx(1, "http://new.com")
	ctx := setupContext(t, statedb, tx)
	act := &WitnessCreateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject duplicate witness")
	}
}

// ---- M11.5 slice 2a: WitnessCreate permission backfill --------------------

func TestWitnessCreateExecute_PreFork_NoPermissionChange(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 100_000_000_000)

	tx := makeWitnessCreateTx(1, "http://test.com")
	ctx := setupContext(t, statedb, tx)
	// AllowMultiSign defaults to 0 -> false.

	if _, err := (&WitnessCreateActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if !statedb.IsWitness(owner) {
		t.Fatal("is_witness flag should be set")
	}
	if statedb.GetWitness(owner) == nil {
		t.Fatal("witness object should exist")
	}

	acct := statedb.GetAccount(owner)
	if acct == nil {
		t.Fatal("account missing")
	}
	if acct.OwnerPermission() != nil {
		t.Errorf("OwnerPermission: want nil pre-fork, got %+v", acct.OwnerPermission())
	}
	if len(acct.ActivePermission()) != 0 {
		t.Errorf("ActivePermission: want empty pre-fork, got %d entries", len(acct.ActivePermission()))
	}
	if acct.WitnessPermission() != nil {
		t.Errorf("WitnessPermission: want nil pre-fork, got %+v", acct.WitnessPermission())
	}
}

func TestWitnessCreateExecute_PostFork_InstallsDefaultPermissions(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	const startBal = int64(100_000_000_000)
	seedAccount(statedb, owner, startBal)

	tx := makeWitnessCreateTx(1, "http://test.com")
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)
	wantOps := append([]byte(nil), ctx.DynProps.ActiveDefaultOperations()...)
	fee := ctx.DynProps.AccountUpgradeCost()

	if _, err := (&WitnessCreateActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if !statedb.IsWitness(owner) {
		t.Fatal("is_witness flag should be set")
	}
	if statedb.GetWitness(owner) == nil {
		t.Fatal("witness object should exist")
	}
	if got := statedb.GetBalance(owner); got != startBal-fee {
		t.Errorf("balance: want %d (startBal - fee), got %d", startBal-fee, got)
	}

	acct := statedb.GetAccount(owner)
	if acct == nil {
		t.Fatal("account missing")
	}

	ownerP := acct.OwnerPermission()
	if ownerP == nil {
		t.Fatal("OwnerPermission is nil; want default install")
	}
	if ownerP.Type != corepb.Permission_Owner || ownerP.Id != 0 || ownerP.PermissionName != "owner" || ownerP.Threshold != 1 {
		t.Errorf("Owner shape: type=%v id=%d name=%q threshold=%d", ownerP.Type, ownerP.Id, ownerP.PermissionName, ownerP.Threshold)
	}
	if len(ownerP.Keys) != 1 || string(ownerP.Keys[0].Address) != string(owner.Bytes()) || ownerP.Keys[0].Weight != 1 {
		t.Errorf("Owner key mismatch")
	}

	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1, got %d", len(actives))
	}
	a := actives[0]
	if a.Type != corepb.Permission_Active || a.Id != 2 || a.PermissionName != "active" || a.Threshold != 1 {
		t.Errorf("Active shape: type=%v id=%d name=%q threshold=%d", a.Type, a.Id, a.PermissionName, a.Threshold)
	}
	if !bytes.Equal(a.Operations, wantOps) {
		t.Errorf("Active.Operations = %x, want %x", a.Operations, wantOps)
	}

	witP := acct.WitnessPermission()
	if witP == nil {
		t.Fatal("WitnessPermission is nil; want default install")
	}
	if witP.Type != corepb.Permission_Witness || witP.Id != 1 || witP.PermissionName != "witness" || witP.Threshold != 1 {
		t.Errorf("Witness shape: type=%v id=%d name=%q threshold=%d", witP.Type, witP.Id, witP.PermissionName, witP.Threshold)
	}
	if len(witP.Keys) != 1 || string(witP.Keys[0].Address) != string(owner.Bytes()) || witP.Keys[0].Weight != 1 {
		t.Errorf("Witness key mismatch")
	}
	if len(witP.Operations) != 0 {
		t.Errorf("Witness.Operations: want empty, got %d bytes", len(witP.Operations))
	}
}

func TestWitnessCreateExecute_PostFork_PreservesCustomOwner(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	co1 := makeTestAddr(101)
	co2 := makeTestAddr(102)
	seedAccount(statedb, owner, 100_000_000_000)

	customOwner := &corepb.Permission{
		Type:           corepb.Permission_Owner,
		Id:             0,
		PermissionName: "owner-custom",
		Threshold:      2,
		ParentId:       0,
		Keys: []*corepb.Key{
			{Address: co1.Bytes(), Weight: 1},
			{Address: co2.Bytes(), Weight: 1},
		},
	}
	statedb.SetPermissions(owner, customOwner, nil, nil)

	tx := makeWitnessCreateTx(1, "http://test.com")
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowMultiSign(true)

	if _, err := (&WitnessCreateActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	acct := statedb.GetAccount(owner)
	if acct == nil {
		t.Fatal("account missing")
	}

	ownerP := acct.OwnerPermission()
	if ownerP == nil {
		t.Fatal("OwnerPermission cleared; want preserved custom")
	}
	if ownerP.PermissionName != "owner-custom" || ownerP.Threshold != 2 || len(ownerP.Keys) != 2 {
		t.Errorf("custom Owner not preserved: name=%q threshold=%d keys=%d",
			ownerP.PermissionName, ownerP.Threshold, len(ownerP.Keys))
	}

	// Active was empty -> default Active[0] should now be installed.
	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry installed, got %d", len(actives))
	}
	if actives[0].PermissionName != "active" {
		t.Errorf("Active[0] not default: name=%q", actives[0].PermissionName)
	}

	// Witness always set.
	witP := acct.WitnessPermission()
	if witP == nil || witP.PermissionName != "witness" {
		t.Fatal("WitnessPermission missing or wrong shape")
	}
}
