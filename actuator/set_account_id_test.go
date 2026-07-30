package actuator

import (
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestSetAccountIdValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    []byte("myuserid"), // min 8 bytes
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_SetAccountIdContract, c, 0)
	act := &SetAccountIdActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}

	ctx.State.SetAccountId(owner, "existing")
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for already-set id")
	}
}

func TestSetAccountIdDuplicateIndexIsCaseInsensitive(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	other := tcommon.Address{0x41, 0x02}
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    []byte("AliceID1"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_SetAccountIdContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := ctx.State.WriteAccountIdIndex([]byte("aliceid1"), other); err != nil {
		t.Fatal(err)
	}

	act := &SetAccountIdActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected duplicate id error")
	}
}

func TestSetAccountIdMalformedIndexFailsValidation(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    []byte("AliceID1"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_SetAccountIdContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	key := append([]byte{0x02}, []byte("aliceid1")...)
	if err := ctx.State.SystemKVPut(kvdomains.SystemAccountIndex, key, []byte("short")); err != nil {
		t.Fatal(err)
	}

	act := &SetAccountIdActuator{}
	err := act.Validate(ctx)
	if err == nil || !strings.Contains(err.Error(), "read account id index") || !strings.Contains(err.Error(), "malformed length") {
		t.Fatalf("Validate malformed id index error = %v, want index read malformed length", err)
	}
}

func TestSetAccountIdExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    []byte("user1234"), // min 8 bytes
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_SetAccountIdContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &SetAccountIdActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	if ctx.State.GetAccountId(owner) != "user1234" {
		t.Fatal("id not set")
	}
	if got := ctx.State.ReadAccountIdIndex([]byte("USER1234")); string(got) != string(owner[:]) {
		t.Fatalf("account id index not written case-insensitively: got %x", got)
	}
}
