package actuator

import (
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func accountPermOps(bits ...int) []byte {
	ops := make([]byte, 32)
	for _, bit := range bits {
		ops[bit/8] |= 1 << (bit % 8)
	}
	return ops
}

func accountOwnerPermission(addr tcommon.Address, threshold int64, keys ...*corepb.Key) *corepb.Permission {
	if len(keys) == 0 {
		keys = []*corepb.Key{{Address: addr[:], Weight: 1}}
	}
	return &corepb.Permission{
		Type:      corepb.Permission_Owner,
		Threshold: threshold,
		Keys:      keys,
	}
}

func accountActivePermission(addr tcommon.Address) *corepb.Permission {
	return &corepb.Permission{
		Type:       corepb.Permission_Active,
		Threshold:  1,
		Keys:       []*corepb.Key{{Address: addr[:], Weight: 1}},
		Operations: accountPermOps(1),
	}
}

func TestAccountPermissionValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner:        accountOwnerPermission(owner, 1),
		Actives:      []*corepb.Permission{accountActivePermission(owner)},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	act := &AccountPermissionUpdateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000) // cover UpdateAccountPermissionFee
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

// TestAccountPermissionNameLengthCountsUTF16 pins java-tron
// AccountPermissionUpdateActuator's `name.length() > 32` check: java
// String.length() counts UTF-16 code units, NOT UTF-8 bytes. A 32-character
// Chinese name (96 UTF-8 bytes) sits exactly on the boundary and must be
// ACCEPTED — go was rejecting it on byte length, stalling the Nile re-sync at
// block 38,418,800 (tx 0, an AccountPermissionUpdate whose active permission was
// named "我的时候了吗丁啉哦…好了叫", 32 runes / 96 bytes).
func TestAccountPermissionNameLengthCountsUTF16(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	validate := func(name string) error {
		active := accountActivePermission(owner)
		active.PermissionName = name
		c := &contractpb.AccountPermissionUpdateContract{
			OwnerAddress: owner[:],
			Owner:        accountOwnerPermission(owner, 1),
			Actives:      []*corepb.Permission{active},
		}
		ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
		ctx.DynProps.SetAllowMultiSign(true)
		ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
		ctx.State.AddBalance(owner, 100_000_000)
		return (&AccountPermissionUpdateActuator{}).Validate(ctx)
	}

	// 32 Chinese chars = 96 UTF-8 bytes = 32 UTF-16 code units → boundary, ACCEPT.
	if err := validate(strings.Repeat("好", 32)); err != nil {
		t.Fatalf("32-char (96-byte) permission name wrongly rejected: %v", err)
	}
	// 33 UTF-16 code units → REJECT.
	if err := validate(strings.Repeat("好", 33)); err == nil {
		t.Fatal("33-char permission name should be rejected")
	}
	// 16 emojis = 16 runes but 32 UTF-16 code units (surrogate pairs) → ACCEPT;
	// 17 emojis = 34 UTF-16 units → REJECT. Pins UTF-16, not plain rune count.
	if err := validate(strings.Repeat("😀", 16)); err != nil {
		t.Fatalf("16-emoji (32 UTF-16) permission name wrongly rejected: %v", err)
	}
	if err := validate(strings.Repeat("😀", 17)); err == nil {
		t.Fatal("17-emoji (34 UTF-16) permission name should be rejected")
	}
}

func TestAccountPermissionValidate_DoesNotRequireUpdateFee(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner:        accountOwnerPermission(owner, 1),
		Actives:      []*corepb.Permission{accountActivePermission(owner)},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should match java-tron and not require update fee balance: %v", err)
	}
	if _, err := act.Execute(ctx); err == nil {
		t.Fatal("execute should still fail when the update fee cannot be paid")
	}
}

func TestAccountPermissionNoOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing owner permission")
	}
}

func TestAccountPermissionThresholdExceedsWeight(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner:        accountOwnerPermission(owner, 10),
		Actives:      []*corepb.Permission{accountActivePermission(owner)},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for threshold > weight")
	}
}

func TestAccountPermissionRejectsUnavailableContractType(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	// Set bit 48 (ClearABIContract) — not in default available_contract_type.
	ops := make([]byte, 32)
	ops[48/8] |= 1 << (48 % 8)
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner:        accountOwnerPermission(owner, 1),
		Actives: []*corepb.Permission{{
			Type:       corepb.Permission_Active,
			Id:         2,
			Threshold:  1,
			Keys:       []*corepb.Key{{Address: owner[:], Weight: 1}},
			Operations: ops,
		}},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)

	act := &AccountPermissionUpdateActuator{}
	err := act.Validate(ctx)
	if err == nil {
		t.Fatal("expected validation error for unavailable contract type 48")
	}
	if err.Error() != "48 isn't a validate ContractType" {
		t.Errorf("unexpected error message: %q", err.Error())
	}
}

func TestAccountPermissionAcceptsAvailableContractType(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	// Set only bit 1 (TransferContract) — in default available_contract_type.
	ops := make([]byte, 32)
	ops[1/8] |= 1 << (1 % 8)
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner:        accountOwnerPermission(owner, 1),
		Actives: []*corepb.Permission{{
			Type:       corepb.Permission_Active,
			Id:         2,
			Threshold:  1,
			Keys:       []*corepb.Key{{Address: owner[:], Weight: 1}},
			Operations: ops,
		}},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)

	act := &AccountPermissionUpdateActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate must accept available contract type: %v", err)
	}
}

func TestAccountPermissionExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	key2 := tcommon.Address{0x41, 0x02}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner: &corepb.Permission{
			Type:      corepb.Permission_Owner,
			Id:        99,
			Threshold: 2,
			Keys: []*corepb.Key{
				{Address: owner[:], Weight: 1},
				{Address: key2[:], Weight: 1},
			},
		},
		Actives: []*corepb.Permission{accountActivePermission(owner)},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000) // cover UpdateAccountPermissionFee

	act := &AccountPermissionUpdateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	acc := ctx.State.GetAccount(owner)
	if acc.OwnerPermission() == nil {
		t.Fatal("owner permission not set")
	}
	if acc.OwnerPermission().Threshold != 2 {
		t.Fatalf("expected threshold 2, got %d", acc.OwnerPermission().Threshold)
	}
	if acc.OwnerPermission().Id != 0 {
		t.Fatalf("expected owner permission id 0, got %d", acc.OwnerPermission().Id)
	}
	if len(acc.ActivePermission()) != 1 {
		t.Fatalf("expected 1 active permission, got %d", len(acc.ActivePermission()))
	}
	if got := acc.ActivePermission()[0].Id; got != 2 {
		t.Fatalf("expected active permission id 2, got %d", got)
	}
	if c.Owner.Id != 99 {
		t.Fatalf("execute mutated input owner permission id: got %d", c.Owner.Id)
	}
	if c.Actives[0].Id != 0 {
		t.Fatalf("execute mutated input active permission id: got %d", c.Actives[0].Id)
	}
}
