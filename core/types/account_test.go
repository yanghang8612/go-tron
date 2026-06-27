package types

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func makeTestAccount() *Account {
	addr := common.BytesToAddress([]byte{
		0x41, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
		0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13,
	})
	return NewAccount(addr, corepb.AccountType_Normal)
}

// ---- FreezeV2 ---------------------------------------------------------------

func TestFreezeV2AddAndGet(t *testing.T) {
	a := makeTestAccount()

	if got := a.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}

	a.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 1000)
	if got := a.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 1000 {
		t.Fatalf("expected 1000, got %d", got)
	}

	// Add to existing type.
	a.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 500)
	if got := a.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 1500 {
		t.Fatalf("expected 1500, got %d", got)
	}

	// Different type should be tracked separately.
	a.AddFreezeV2(corepb.ResourceCode_ENERGY, 2000)
	if got := a.GetFrozenV2Amount(corepb.ResourceCode_ENERGY); got != 2000 {
		t.Fatalf("expected 2000, got %d", got)
	}

	// BANDWIDTH unchanged.
	if got := a.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 1500 {
		t.Fatalf("expected BANDWIDTH still 1500, got %d", got)
	}
}

func TestFreezeV2Total(t *testing.T) {
	a := makeTestAccount()
	a.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 300)
	a.AddFreezeV2(corepb.ResourceCode_ENERGY, 700)
	if got := a.TotalFrozenV2(); got != 1000 {
		t.Fatalf("expected total 1000, got %d", got)
	}
}

func TestFreezeV2ReduceAndFloor(t *testing.T) {
	a := makeTestAccount()
	a.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 500)

	a.ReduceFreezeV2(corepb.ResourceCode_BANDWIDTH, 200)
	if got := a.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 300 {
		t.Fatalf("expected 300 after reduce, got %d", got)
	}

	// Reduce below zero — should floor at 0.
	a.ReduceFreezeV2(corepb.ResourceCode_BANDWIDTH, 9999)
	if got := a.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 0 {
		t.Fatalf("expected 0 (floored), got %d", got)
	}
}

func TestFreezeV2ReduceNonExistentType(t *testing.T) {
	a := makeTestAccount()
	// Should not panic on missing type.
	a.ReduceFreezeV2(corepb.ResourceCode_ENERGY, 100)
	if got := a.GetFrozenV2Amount(corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("expected 0 for missing type, got %d", got)
	}
}

func TestFrozenV2Slice(t *testing.T) {
	a := makeTestAccount()
	if len(a.FrozenV2()) != 0 {
		t.Fatal("expected empty FrozenV2 slice initially")
	}
	a.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 100)
	a.AddFreezeV2(corepb.ResourceCode_ENERGY, 200)
	if len(a.FrozenV2()) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(a.FrozenV2()))
	}
}

// ---- UnfreezeV2 -------------------------------------------------------------

func TestUnfreezeV2AddAndCount(t *testing.T) {
	a := makeTestAccount()
	if len(a.UnfrozenV2()) != 0 {
		t.Fatal("expected empty UnfrozenV2 initially")
	}
	a.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 100, 1000)
	a.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 200, 2000)
	if len(a.UnfrozenV2()) != 2 {
		t.Fatalf("expected 2 unfreeze entries, got %d", len(a.UnfrozenV2()))
	}
}

func TestRemoveExpiredUnfreezeV2(t *testing.T) {
	tests := []struct {
		name          string
		entries       []struct{ amount, expire int64 }
		now           int64
		wantWithdrawn int64
		wantRemaining int
	}{
		{
			name: "remove all expired",
			entries: []struct{ amount, expire int64 }{
				{100, 500},
				{200, 700},
			},
			now:           1000,
			wantWithdrawn: 300,
			wantRemaining: 0,
		},
		{
			name: "remove none",
			entries: []struct{ amount, expire int64 }{
				{100, 2000},
				{200, 3000},
			},
			now:           1000,
			wantWithdrawn: 0,
			wantRemaining: 2,
		},
		{
			name: "remove some",
			entries: []struct{ amount, expire int64 }{
				{100, 500},
				{200, 1500},
				{300, 2000},
			},
			now:           1000,
			wantWithdrawn: 100,
			wantRemaining: 2,
		},
		{
			name: "boundary — expire exactly at now",
			entries: []struct{ amount, expire int64 }{
				{50, 1000},
			},
			now:           1000,
			wantWithdrawn: 50,
			wantRemaining: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := makeTestAccount()
			for _, e := range tc.entries {
				a.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, e.amount, e.expire)
			}
			withdrawn := a.RemoveExpiredUnfreezeV2(tc.now)
			if withdrawn != tc.wantWithdrawn {
				t.Errorf("withdrawn: want %d, got %d", tc.wantWithdrawn, withdrawn)
			}
			if len(a.UnfrozenV2()) != tc.wantRemaining {
				t.Errorf("remaining: want %d, got %d", tc.wantRemaining, len(a.UnfrozenV2()))
			}
		})
	}
}

// ---- Votes ------------------------------------------------------------------

func TestVotesSetClear(t *testing.T) {
	a := makeTestAccount()
	if len(a.Votes()) != 0 {
		t.Fatal("expected no votes initially")
	}

	votes := []*corepb.Vote{
		{VoteAddress: []byte{0x01}, VoteCount: 10},
		{VoteAddress: []byte{0x02}, VoteCount: 20},
	}
	a.SetVotes(votes)
	if len(a.Votes()) != 2 {
		t.Fatalf("expected 2 votes, got %d", len(a.Votes()))
	}
	if a.Votes()[0].VoteCount != 10 {
		t.Fatalf("expected vote count 10, got %d", a.Votes()[0].VoteCount)
	}

	a.ClearVotes()
	if len(a.Votes()) != 0 {
		t.Fatalf("expected 0 votes after clear, got %d", len(a.Votes()))
	}
}

// ---- Bandwidth resource tracking --------------------------------------------

func TestBandwidthFields(t *testing.T) {
	a := makeTestAccount()

	a.SetNetUsage(1234)
	if a.NetUsage() != 1234 {
		t.Fatalf("NetUsage: want 1234, got %d", a.NetUsage())
	}

	a.SetLatestConsumeTime(99999)
	if a.LatestConsumeTime() != 99999 {
		t.Fatalf("LatestConsumeTime: want 99999, got %d", a.LatestConsumeTime())
	}

	a.SetFreeNetUsage(500)
	if a.FreeNetUsage() != 500 {
		t.Fatalf("FreeNetUsage: want 500, got %d", a.FreeNetUsage())
	}

	a.SetLatestConsumeFreeTime(88888)
	if a.LatestConsumeFreeTime() != 88888 {
		t.Fatalf("LatestConsumeFreeTime: want 88888, got %d", a.LatestConsumeFreeTime())
	}
}

// ---- Energy resource tracking -----------------------------------------------

func TestEnergyFieldsNilSafe(t *testing.T) {
	a := makeTestAccount()
	// AccountResource is nil initially — should return 0, not panic.
	if a.EnergyUsage() != 0 {
		t.Fatalf("EnergyUsage: want 0 (nil safe), got %d", a.EnergyUsage())
	}
	if a.LatestConsumeTimeForEnergy() != 0 {
		t.Fatalf("LatestConsumeTimeForEnergy: want 0 (nil safe), got %d", a.LatestConsumeTimeForEnergy())
	}
}

func TestEnergyFieldsSetCreatesSubMessage(t *testing.T) {
	a := makeTestAccount()
	a.SetEnergyUsage(7777)
	if a.EnergyUsage() != 7777 {
		t.Fatalf("EnergyUsage: want 7777, got %d", a.EnergyUsage())
	}
	if a.pb.AccountResource == nil {
		t.Fatal("AccountResource should be created by SetEnergyUsage")
	}

	a.SetLatestConsumeTimeForEnergy(55555)
	if a.LatestConsumeTimeForEnergy() != 55555 {
		t.Fatalf("LatestConsumeTimeForEnergy: want 55555, got %d", a.LatestConsumeTimeForEnergy())
	}
}

// ---- Allowance --------------------------------------------------------------

func TestAllowanceFields(t *testing.T) {
	a := makeTestAccount()

	a.SetAllowance(9876)
	if a.Allowance() != 9876 {
		t.Fatalf("Allowance: want 9876, got %d", a.Allowance())
	}

	a.SetLatestWithdrawTime(11111)
	if a.LatestWithdrawTime() != 11111 {
		t.Fatalf("LatestWithdrawTime: want 11111, got %d", a.LatestWithdrawTime())
	}
}

// ---- AccountName ------------------------------------------------------------

func TestAccountName(t *testing.T) {
	a := makeTestAccount()
	a.SetAccountName("MyNode")
	if a.AccountName() != "MyNode" {
		t.Fatalf("AccountName: want MyNode, got %q", a.AccountName())
	}
}

// ---- Marshal / Unmarshal round-trip -----------------------------------------

func TestAccountMarshalRoundTrip(t *testing.T) {
	a := makeTestAccount()
	a.SetBalance(42000)
	a.SetAccountName("RoundTrip")
	a.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 111)
	a.AddFreezeV2(corepb.ResourceCode_ENERGY, 222)
	a.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 50, 9999)
	a.SetVotes([]*corepb.Vote{{VoteAddress: []byte{0x03}, VoteCount: 5}})
	a.SetNetUsage(10)
	a.SetFreeNetUsage(20)
	a.SetLatestConsumeTime(30)
	a.SetLatestConsumeFreeTime(40)
	a.SetEnergyUsage(300)
	a.SetLatestConsumeTimeForEnergy(400)
	a.SetAllowance(500)
	a.SetLatestWithdrawTime(600)

	data, err := a.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	a2, err := UnmarshalAccount(data)
	if err != nil {
		t.Fatalf("UnmarshalAccount: %v", err)
	}

	if a2.Balance() != 42000 {
		t.Errorf("Balance: want 42000, got %d", a2.Balance())
	}
	if a2.AccountName() != "RoundTrip" {
		t.Errorf("AccountName: want RoundTrip, got %q", a2.AccountName())
	}
	if a2.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH) != 111 {
		t.Errorf("FreezeV2 BANDWIDTH: want 111, got %d", a2.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH))
	}
	if a2.GetFrozenV2Amount(corepb.ResourceCode_ENERGY) != 222 {
		t.Errorf("FreezeV2 ENERGY: want 222, got %d", a2.GetFrozenV2Amount(corepb.ResourceCode_ENERGY))
	}
	if len(a2.UnfrozenV2()) != 1 || a2.UnfrozenV2()[0].UnfreezeAmount != 50 {
		t.Errorf("UnfrozenV2 not preserved")
	}
	if len(a2.Votes()) != 1 || a2.Votes()[0].VoteCount != 5 {
		t.Errorf("Votes not preserved")
	}
	if a2.NetUsage() != 10 {
		t.Errorf("NetUsage: want 10, got %d", a2.NetUsage())
	}
	if a2.FreeNetUsage() != 20 {
		t.Errorf("FreeNetUsage: want 20, got %d", a2.FreeNetUsage())
	}
	if a2.LatestConsumeTime() != 30 {
		t.Errorf("LatestConsumeTime: want 30, got %d", a2.LatestConsumeTime())
	}
	if a2.LatestConsumeFreeTime() != 40 {
		t.Errorf("LatestConsumeFreeTime: want 40, got %d", a2.LatestConsumeFreeTime())
	}
	if a2.EnergyUsage() != 300 {
		t.Errorf("EnergyUsage: want 300, got %d", a2.EnergyUsage())
	}
	if a2.LatestConsumeTimeForEnergy() != 400 {
		t.Errorf("LatestConsumeTimeForEnergy: want 400, got %d", a2.LatestConsumeTimeForEnergy())
	}
	if a2.Allowance() != 500 {
		t.Errorf("Allowance: want 500, got %d", a2.Allowance())
	}
	if a2.LatestWithdrawTime() != 600 {
		t.Errorf("LatestWithdrawTime: want 600, got %d", a2.LatestWithdrawTime())
	}
}

// ---- Default permission helpers (M11.5) ------------------------------------

func TestMakeDefaultOwnerPermission(t *testing.T) {
	addr := common.BytesToAddress([]byte{
		0x41, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
		0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13,
	})
	p := MakeDefaultOwnerPermission(addr)
	if p == nil {
		t.Fatal("nil permission")
	}
	if p.Type != corepb.Permission_Owner {
		t.Errorf("Type: want Owner, got %v", p.Type)
	}
	if p.Id != 0 {
		t.Errorf("Id: want 0, got %d", p.Id)
	}
	if p.PermissionName != "owner" {
		t.Errorf("PermissionName: want \"owner\", got %q", p.PermissionName)
	}
	if p.Threshold != 1 {
		t.Errorf("Threshold: want 1, got %d", p.Threshold)
	}
	if p.ParentId != 0 {
		t.Errorf("ParentId: want 0, got %d", p.ParentId)
	}
	if len(p.Operations) != 0 {
		t.Errorf("Operations: want empty, got %d bytes", len(p.Operations))
	}
	if len(p.Keys) != 1 {
		t.Fatalf("Keys: want 1, got %d", len(p.Keys))
	}
	if string(p.Keys[0].Address) != string(addr.Bytes()) {
		t.Errorf("Keys[0].Address mismatch")
	}
	if p.Keys[0].Weight != 1 {
		t.Errorf("Keys[0].Weight: want 1, got %d", p.Keys[0].Weight)
	}
}

func TestMakeDefaultActivePermission(t *testing.T) {
	addr := common.BytesToAddress([]byte{
		0x41, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22,
		0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22,
	})
	ops := []byte{
		0x7f, 0xff, 0x1f, 0xc0, 0x03, 0x3e, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	p := MakeDefaultActivePermission(addr, ops)
	if p == nil {
		t.Fatal("nil permission")
	}
	if p.Type != corepb.Permission_Active {
		t.Errorf("Type: want Active, got %v", p.Type)
	}
	if p.Id != 2 {
		t.Errorf("Id: want 2, got %d", p.Id)
	}
	if p.PermissionName != "active" {
		t.Errorf("PermissionName: want \"active\", got %q", p.PermissionName)
	}
	if p.Threshold != 1 {
		t.Errorf("Threshold: want 1, got %d", p.Threshold)
	}
	if p.ParentId != 0 {
		t.Errorf("ParentId: want 0, got %d", p.ParentId)
	}
	if len(p.Keys) != 1 || string(p.Keys[0].Address) != string(addr.Bytes()) || p.Keys[0].Weight != 1 {
		t.Errorf("Keys mismatch")
	}
	if len(p.Operations) != 32 {
		t.Fatalf("Operations: want 32 bytes, got %d", len(p.Operations))
	}
	for i := range ops {
		if p.Operations[i] != ops[i] {
			t.Errorf("Operations[%d]: want %#x, got %#x", i, ops[i], p.Operations[i])
		}
	}
	// Defensive copy: mutating input must not change permission.
	ops[0] = 0x00
	if p.Operations[0] != 0x7f {
		t.Errorf("Operations not defensively copied: now %#x", p.Operations[0])
	}
}

func TestMakeDefaultWitnessPermission(t *testing.T) {
	addr := common.BytesToAddress([]byte{
		0x41, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33, 0x44,
		0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
	})
	p := MakeDefaultWitnessPermission(addr)
	if p == nil {
		t.Fatal("nil permission")
	}
	if p.Type != corepb.Permission_Witness {
		t.Errorf("Type: want Witness, got %v", p.Type)
	}
	if p.Id != 1 {
		t.Errorf("Id: want 1, got %d", p.Id)
	}
	if p.PermissionName != "witness" {
		t.Errorf("PermissionName: want \"witness\", got %q", p.PermissionName)
	}
	if p.Threshold != 1 {
		t.Errorf("Threshold: want 1, got %d", p.Threshold)
	}
	if p.ParentId != 0 {
		t.Errorf("ParentId: want 0, got %d", p.ParentId)
	}
	if len(p.Operations) != 0 {
		t.Errorf("Operations: want empty (witness has no ops bitmap), got %d bytes", len(p.Operations))
	}
	if len(p.Keys) != 1 {
		t.Fatalf("Keys: want 1, got %d", len(p.Keys))
	}
	if string(p.Keys[0].Address) != string(addr.Bytes()) {
		t.Errorf("Keys[0].Address mismatch")
	}
	if p.Keys[0].Weight != 1 {
		t.Errorf("Keys[0].Weight: want 1, got %d", p.Keys[0].Weight)
	}
}

// TestWitnessPermissionAddress pins java-tron
// AccountCapsule.getWitnessPermissionAddress, which block signature validation
// uses under AllowMultiSign: the first key of the witness permission, or the
// account's own address when no witness permission (or an empty key set) is
// configured. The delegated case is the Nile 45,490,765 stall (SR 417d6fd4
// delegated block signing to 415624c1).
func TestWitnessPermissionAddress(t *testing.T) {
	acc := makeTestAccount()
	own := acc.Address()

	// No witness permission set ⇒ own address.
	if got := acc.WitnessPermissionAddress(); got != own {
		t.Fatalf("no witness permission: want own %x, got %x", own, got)
	}

	// Witness permission with an empty key set ⇒ own address.
	acc.SetWitnessPermission(&corepb.Permission{Type: corepb.Permission_Witness})
	if got := acc.WitnessPermissionAddress(); got != own {
		t.Fatalf("empty-key witness permission: want own %x, got %x", own, got)
	}

	// Witness permission delegating to a separate key ⇒ that key's address.
	deleg := common.BytesToAddress([]byte{
		0x41, 0x56, 0x24, 0xc1, 0x2e, 0x30, 0x8b, 0x03, 0xa1, 0xa6,
		0xb2, 0x1d, 0x9b, 0x86, 0xe3, 0x94, 0x2f, 0xac, 0x1a, 0xb9,
	})
	acc.SetWitnessPermission(&corepb.Permission{
		Type:      corepb.Permission_Witness,
		Threshold: 1,
		Keys:      []*corepb.Key{{Address: deleg.Bytes(), Weight: 1}},
	})
	if got := acc.WitnessPermissionAddress(); got != deleg {
		t.Fatalf("delegated witness permission: want %x, got %x", deleg, got)
	}
}
