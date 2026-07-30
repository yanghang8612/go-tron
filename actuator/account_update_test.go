package actuator

import (
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestAccountUpdateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  []byte("myaccount"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	act := &AccountUpdateActuator{}

	// Owner doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}

	// Set name, then try again
	ctx.State.SetAccountName(owner, "existing")
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for already-set name")
	}
}

func TestAccountUpdateExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  []byte("alice"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountUpdateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1, got %d", result.ContractRet)
	}
	if ctx.State.GetAccountName(owner) != "alice" {
		t.Fatalf("name not set")
	}
	if got := ctx.State.ReadAccountNameIndex([]byte("alice")); string(got) != string(owner[:]) {
		t.Fatalf("account name index not written: got %x", got)
	}
}

func TestAccountUpdateEmptyName(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  []byte{},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountUpdateActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("empty name should be valid like java-tron: %v", err)
	}
}

func TestAccountUpdateNameTooLong(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  make([]byte, 201),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for account name longer than 200 bytes")
	}
}

func TestAccountUpdateDuplicateNameIndex(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	other := tcommon.Address{0x41, 0x02}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  []byte("taken"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := ctx.State.WriteAccountNameIndex([]byte("taken"), other); err != nil {
		t.Fatal(err)
	}

	act := &AccountUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for duplicate account name index")
	}
}

func TestAccountUpdateMalformedNameIndexFailsValidation(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  []byte("taken"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	key := append([]byte{0x01}, []byte("taken")...)
	if err := ctx.State.SystemKVPut(kvdomains.SystemAccountIndex, key, []byte("short")); err != nil {
		t.Fatal(err)
	}

	act := &AccountUpdateActuator{}
	err := act.Validate(ctx)
	if err == nil || !strings.Contains(err.Error(), "read account name index") || !strings.Contains(err.Error(), "malformed length") {
		t.Fatalf("Validate malformed name index error = %v, want index read malformed length", err)
	}
}
