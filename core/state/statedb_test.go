package state

import (
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func newTestStateDB(t *testing.T) *StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	return sdb
}

func testAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestStateDBGetSetBalance(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100_000_000)
	if got := sdb.GetBalance(addr); got != 100_000_000 {
		t.Fatalf("balance: want 100000000, got %d", got)
	}
	if err := sdb.SubBalance(addr, 30_000_000); err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetBalance(addr); got != 70_000_000 {
		t.Fatalf("balance after sub: want 70000000, got %d", got)
	}
}

func TestStateDBSubBalanceInsufficient(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 50)
	err := sdb.SubBalance(addr, 100)
	if err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestStateDBSnapshotRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100)
	snap := sdb.Snapshot()
	sdb.AddBalance(addr, 50)
	if got := sdb.GetBalance(addr); got != 150 {
		t.Fatalf("before revert: want 150, got %d", got)
	}
	sdb.RevertToSnapshot(snap)
	if got := sdb.GetBalance(addr); got != 100 {
		t.Fatalf("after revert: want 100, got %d", got)
	}
}

func TestStateDBSnapshotRevertNewAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	snap := sdb.Snapshot()
	addr := testAddr(2)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 200)
	sdb.RevertToSnapshot(snap)
	acc := sdb.GetAccount(addr)
	if acc != nil {
		t.Fatal("account should not exist after revert")
	}
}

func TestStateDBCommitChangesRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	emptyRoot := ethcommon.Hash(ethtypes.EmptyRootHash)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if ethcommon.Hash(root) == emptyRoot {
		t.Fatal("root should differ from empty root after commit")
	}
}

func TestStateDBCommitDeterministic(t *testing.T) {
	makeState := func() tcommon.Hash {
		diskdb := ethrawdb.NewMemoryDatabase()
		db := NewDatabase(diskdb)
		sdb, _ := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
		addr1 := testAddr(1)
		addr2 := testAddr(2)
		sdb.GetOrCreateAccount(addr1)
		sdb.AddBalance(addr1, 500)
		sdb.GetOrCreateAccount(addr2)
		sdb.AddBalance(addr2, 300)
		root, _ := sdb.Commit()
		return root
	}
	root1 := makeState()
	root2 := makeState()
	if root1 != root2 {
		t.Fatalf("roots differ: %x vs %x", root1, root2)
	}
}

func TestStateDBFreezeV2(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100_000_000)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 50_000_000)
	acc := sdb.GetAccount(addr)
	if acc == nil {
		t.Fatal("account not found")
	}
	if got := acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 50_000_000 {
		t.Fatalf("frozen bandwidth: want 50000000, got %d", got)
	}
}

func TestStateDBCommitThenContinue(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)

	root1, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// StateDB should still be usable after commit
	sdb.AddBalance(addr, 500)
	if got := sdb.GetBalance(addr); got != 1500 {
		t.Fatalf("balance after second add: want 1500, got %d", got)
	}

	root2, err := sdb.Commit()
	if err != nil {
		t.Fatal("second commit failed:", err)
	}

	if root1 == root2 {
		t.Fatal("roots should differ after second commit")
	}
}

func TestStateDBWitness(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	w := sdb.GetWitness(addr)
	if w != nil {
		t.Fatal("witness should not exist")
	}
	sdb.GetOrCreateAccount(addr)
	sdb.PutWitness(addr, "http://example.com")
	w = sdb.GetWitness(addr)
	if w == nil {
		t.Fatal("witness should exist")
	}
	if w.URL() != "http://example.com" {
		t.Fatalf("witness url: want http://example.com, got %s", w.URL())
	}
}

func TestStateDBAccountExists(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)

	if db.AccountExists(addr) {
		t.Fatal("should not exist before creation")
	}

	db.CreateAccount(addr, corepb.AccountType_Normal)
	if !db.AccountExists(addr) {
		t.Fatal("should exist after creation")
	}
}

func TestStateDBFreezeAndUnfreeze(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)
	db.AddBalance(addr, 10_000_000)

	// Freeze
	db.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 5_000_000)
	if got := db.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH); got != 5_000_000 {
		t.Fatalf("frozen: got %d, want 5000000", got)
	}
	if got := db.TotalFrozenV2(addr); got != 5_000_000 {
		t.Fatalf("total frozen: got %d, want 5000000", got)
	}

	// Reduce frozen
	db.ReduceFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000)
	if got := db.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH); got != 3_000_000 {
		t.Fatalf("after reduce: got %d, want 3000000", got)
	}

	// Add unfreeze entry
	db.AddUnfreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000, 100_000)
	if got := db.UnfreezeV2Count(addr); got != 1 {
		t.Fatalf("unfreeze count: got %d, want 1", got)
	}

	// Remove expired
	withdrawn := db.RemoveExpiredUnfreezeV2(addr, 200_000)
	if withdrawn != 2_000_000 {
		t.Fatalf("withdrawn: got %d, want 2000000", withdrawn)
	}
	if got := db.UnfreezeV2Count(addr); got != 0 {
		t.Fatalf("unfreeze count after remove: got %d, want 0", got)
	}
}

func TestStateDBVotes(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)

	votes := []*corepb.Vote{
		{VoteAddress: testAddr(0x10).Bytes(), VoteCount: 100},
		{VoteAddress: testAddr(0x11).Bytes(), VoteCount: 200},
	}
	db.SetVotes(addr, votes)
	got := db.GetVotes(addr)
	if len(got) != 2 {
		t.Fatalf("votes: got %d, want 2", len(got))
	}

	db.ClearVotes(addr)
	got = db.GetVotes(addr)
	if len(got) != 0 {
		t.Fatalf("after clear: got %d votes, want 0", len(got))
	}
}

func TestStateDBAllowanceAndWithdraw(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)

	db.SetAllowance(addr, 1000)
	if got := db.GetAllowance(addr); got != 1000 {
		t.Fatalf("allowance: got %d, want 1000", got)
	}

	db.AddAllowance(addr, 500)
	if got := db.GetAllowance(addr); got != 1500 {
		t.Fatalf("after add: got %d, want 1500", got)
	}

	db.SetLatestWithdrawTime(addr, 999)
	if got := db.GetLatestWithdrawTime(addr); got != 999 {
		t.Fatalf("withdraw time: got %d, want 999", got)
	}
}

func TestStateDBWitnessVoteCount(t *testing.T) {
	db := newTestStateDB(t)
	wAddr := testAddr(0x10)
	db.PutWitness(wAddr, "http://w1.example.com")

	db.AddWitnessVoteCount(wAddr, 100)
	w := db.GetWitness(wAddr)
	if w.VoteCount() != 100 {
		t.Fatalf("vote count: got %d, want 100", w.VoteCount())
	}
	db.AddWitnessVoteCount(wAddr, -30)
	if w.VoteCount() != 70 {
		t.Fatalf("after sub: got %d, want 70", w.VoteCount())
	}
}

func TestStateDB_BandwidthMethods(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x01)
	statedb.GetOrCreateAccount(addr)

	// NetUsage
	if statedb.GetNetUsage(addr) != 0 {
		t.Fatal("initial NetUsage should be 0")
	}
	statedb.SetNetUsage(addr, 500)
	if statedb.GetNetUsage(addr) != 500 {
		t.Fatalf("NetUsage: want 500, got %d", statedb.GetNetUsage(addr))
	}

	// LatestConsumeTime
	statedb.SetLatestConsumeTime(addr, 3000)
	if statedb.GetLatestConsumeTime(addr) != 3000 {
		t.Fatalf("LatestConsumeTime: want 3000, got %d", statedb.GetLatestConsumeTime(addr))
	}

	// FreeNetUsage
	statedb.SetFreeNetUsage(addr, 200)
	if statedb.GetFreeNetUsage(addr) != 200 {
		t.Fatalf("FreeNetUsage: want 200, got %d", statedb.GetFreeNetUsage(addr))
	}

	// LatestConsumeFreeTime
	statedb.SetLatestConsumeFreeTime(addr, 6000)
	if statedb.GetLatestConsumeFreeTime(addr) != 6000 {
		t.Fatalf("LatestConsumeFreeTime: want 6000, got %d", statedb.GetLatestConsumeFreeTime(addr))
	}
}

func TestStateDB_BandwidthRevert(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x01)
	statedb.GetOrCreateAccount(addr)
	statedb.SetNetUsage(addr, 100)

	snap := statedb.Snapshot()
	statedb.SetNetUsage(addr, 999)
	if statedb.GetNetUsage(addr) != 999 {
		t.Fatalf("want 999 after set, got %d", statedb.GetNetUsage(addr))
	}

	statedb.RevertToSnapshot(snap)
	if statedb.GetNetUsage(addr) != 100 {
		t.Fatalf("want 100 after revert, got %d", statedb.GetNetUsage(addr))
	}
}

func TestContractStoragePersistsAcrossCommit(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	contract := testAddr(0x42)
	sdb.CreateAccount(contract, corepb.AccountType_Normal)

	var slot, value tcommon.Hash
	slot[31] = 0x00
	value[31] = 42

	sdb.SetState(contract, slot, value)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Re-open state from committed root (simulates TriggerConstantContract)
	sdb2, err := New(root, db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	got := sdb2.GetState(contract, slot)
	if got != value {
		t.Fatalf("storage after reopen: got %x, want %x", got, value)
	}
}

// ---- M11.5: ApplyDefaultAccountPermissions --------------------------------

func TestApplyDefaultAccountPermissions_PopulatesBoth(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(7)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.ApplyDefaultAccountPermissions(addr, dp)

	acct := sdb.GetAccount(addr)
	if acct == nil {
		t.Fatal("account missing after create")
	}
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission is nil")
	}
	if owner.Type != corepb.Permission_Owner || owner.Id != 0 || owner.PermissionName != "owner" {
		t.Errorf("owner shape mismatch: type=%v id=%d name=%q", owner.Type, owner.Id, owner.PermissionName)
	}
	if len(owner.Keys) != 1 || string(owner.Keys[0].Address) != string(addr.Bytes()) {
		t.Errorf("owner key mismatch")
	}
	if len(owner.Operations) != 0 {
		t.Errorf("owner.Operations: want empty, got %d bytes", len(owner.Operations))
	}

	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry, got %d", len(actives))
	}
	active := actives[0]
	if active.Type != corepb.Permission_Active || active.Id != 2 || active.PermissionName != "active" {
		t.Errorf("active shape mismatch: type=%v id=%d name=%q", active.Type, active.Id, active.PermissionName)
	}
	want := dp.ActiveDefaultOperations()
	if len(active.Operations) != len(want) {
		t.Fatalf("active.Operations length: want %d, got %d", len(want), len(active.Operations))
	}
	for i := range want {
		if active.Operations[i] != want[i] {
			t.Errorf("active.Operations[%d]: want %#x, got %#x", i, want[i], active.Operations[i])
		}
	}
}

func TestApplyDefaultAccountPermissions_NoOpIfMissing(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(8)

	// Account does not exist; helper must not panic and must not create the account.
	sdb.ApplyDefaultAccountPermissions(addr, dp)

	if sdb.AccountExists(addr) {
		t.Fatal("ApplyDefaultAccountPermissions created an account; expected no-op")
	}
}

// ---- M11.5 slice 2a: ApplyWitnessPermissions ------------------------------

func TestApplyWitnessPermissions_NoOpIfMissing(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(9)

	// Account does not exist; helper must not panic and must not create the account.
	sdb.ApplyWitnessPermissions(addr, dp)

	if sdb.AccountExists(addr) {
		t.Fatal("ApplyWitnessPermissions created an account; expected no-op")
	}
}

func TestApplyWitnessPermissions_PopulatesAllOnEmptyAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(10)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.ApplyWitnessPermissions(addr, dp)

	acct := sdb.GetAccount(addr)
	if acct == nil {
		t.Fatal("account missing after create")
	}
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission is nil; want default owner shape")
	}
	if owner.Type != corepb.Permission_Owner || owner.Id != 0 || owner.PermissionName != "owner" || owner.Threshold != 1 {
		t.Errorf("owner shape mismatch: type=%v id=%d name=%q threshold=%d", owner.Type, owner.Id, owner.PermissionName, owner.Threshold)
	}
	if len(owner.Keys) != 1 || string(owner.Keys[0].Address) != string(addr.Bytes()) || owner.Keys[0].Weight != 1 {
		t.Errorf("owner key mismatch")
	}

	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry, got %d", len(actives))
	}
	active := actives[0]
	if active.Type != corepb.Permission_Active || active.Id != 2 || active.PermissionName != "active" || active.Threshold != 1 {
		t.Errorf("active shape mismatch: type=%v id=%d name=%q threshold=%d", active.Type, active.Id, active.PermissionName, active.Threshold)
	}
	want := dp.ActiveDefaultOperations()
	if len(active.Operations) != len(want) {
		t.Fatalf("active.Operations length: want %d, got %d", len(want), len(active.Operations))
	}
	for i := range want {
		if active.Operations[i] != want[i] {
			t.Errorf("active.Operations[%d]: want %#x, got %#x", i, want[i], active.Operations[i])
		}
	}

	witness := acct.WitnessPermission()
	if witness == nil {
		t.Fatal("WitnessPermission is nil; want default witness shape")
	}
	if witness.Type != corepb.Permission_Witness || witness.Id != 1 || witness.PermissionName != "witness" || witness.Threshold != 1 {
		t.Errorf("witness shape mismatch: type=%v id=%d name=%q threshold=%d", witness.Type, witness.Id, witness.PermissionName, witness.Threshold)
	}
	if len(witness.Keys) != 1 || string(witness.Keys[0].Address) != string(addr.Bytes()) || witness.Keys[0].Weight != 1 {
		t.Errorf("witness key mismatch")
	}
	if len(witness.Operations) != 0 {
		t.Errorf("witness.Operations: want empty, got %d bytes", len(witness.Operations))
	}
}

func TestApplyWitnessPermissions_PreservesCustomOwner(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(11)
	co1 := testAddr(101)
	co2 := testAddr(102)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)

	// Install a custom 2-of-2-ish Owner via SetPermissions (Active untouched).
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
	sdb.SetPermissions(addr, customOwner, nil, nil)

	sdb.ApplyWitnessPermissions(addr, dp)

	acct := sdb.GetAccount(addr)
	if acct == nil {
		t.Fatal("account missing")
	}
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission cleared; want preserved custom")
	}
	if owner.PermissionName != "owner-custom" || owner.Threshold != 2 || len(owner.Keys) != 2 {
		t.Errorf("custom owner not preserved: name=%q threshold=%d keys=%d", owner.PermissionName, owner.Threshold, len(owner.Keys))
	}
	if string(owner.Keys[0].Address) != string(co1.Bytes()) || string(owner.Keys[1].Address) != string(co2.Bytes()) {
		t.Errorf("custom owner key addresses changed")
	}

	// Active was empty -> default Active[0] should now be installed.
	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry installed (was empty), got %d", len(actives))
	}
	if actives[0].PermissionName != "active" || actives[0].Threshold != 1 {
		t.Errorf("active not the default shape after install")
	}

	// Witness is always set.
	witness := acct.WitnessPermission()
	if witness == nil || witness.PermissionName != "witness" {
		t.Fatal("WitnessPermission missing or wrong shape")
	}
}

func TestApplyWitnessPermissions_PreservesCustomActives(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(12)
	signer1 := testAddr(201)
	signer2 := testAddr(202)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)

	// Install two custom Active permissions; leave Owner nil.
	customA0 := &corepb.Permission{
		Type:           corepb.Permission_Active,
		Id:             2,
		PermissionName: "active-a",
		Threshold:      1,
		ParentId:       0,
		Operations:     []byte{0x01, 0x02},
		Keys:           []*corepb.Key{{Address: signer1.Bytes(), Weight: 1}},
	}
	customA1 := &corepb.Permission{
		Type:           corepb.Permission_Active,
		Id:             3,
		PermissionName: "active-b",
		Threshold:      1,
		ParentId:       0,
		Operations:     []byte{0x03, 0x04},
		Keys:           []*corepb.Key{{Address: signer2.Bytes(), Weight: 1}},
	}
	sdb.SetPermissions(addr, nil, nil, []*corepb.Permission{customA0, customA1})

	sdb.ApplyWitnessPermissions(addr, dp)

	acct := sdb.GetAccount(addr)
	if acct == nil {
		t.Fatal("account missing")
	}

	// Owner was nil -> default Owner should now be installed.
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission missing; expected default install")
	}
	if owner.PermissionName != "owner" {
		t.Errorf("Owner not default shape: name=%q", owner.PermissionName)
	}

	// Active list (non-empty) preserved verbatim.
	actives := acct.ActivePermission()
	if len(actives) != 2 {
		t.Fatalf("ActivePermission: want 2 entries (preserved), got %d", len(actives))
	}
	if actives[0].PermissionName != "active-a" || actives[1].PermissionName != "active-b" {
		t.Errorf("custom active list mutated: %q, %q", actives[0].PermissionName, actives[1].PermissionName)
	}

	// Witness is always set.
	witness := acct.WitnessPermission()
	if witness == nil || witness.PermissionName != "witness" {
		t.Fatal("WitnessPermission missing or wrong shape")
	}
}
