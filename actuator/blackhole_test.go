package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// setBlackholeOpt enables or disables AllowBlackholeOptimization on a Context.
func setBlackholeOpt(ctx *Context, on bool) {
	v := int64(0)
	if on {
		v = 1
	}
	ctx.DynProps.Set("allow_blackhole_optimization", v)
}

// blackholeBalance returns the current balance of the genesis Blackhole account.
func blackholeBalance(ctx *Context) int64 {
	return ctx.State.GetBalance(params.BlackholeAddress)
}

// TestBurnFee_AssetIssue_FlagOn: when optimization active, blackhole gets nothing.
func TestBurnFee_AssetIssue_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	fee := ctx.DynProps.AssetIssueFee()
	ctx.State.AddBalance(owner, fee)

	setBlackholeOpt(ctx, true)

	act := &AssetIssueActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got := ctx.State.GetBalance(owner); got != 0 {
		t.Errorf("owner balance: want 0, got %d", got)
	}
	if got := blackholeBalance(ctx); got != 0 {
		t.Errorf("blackhole balance (flag ON): want 0, got %d", got)
	}
}

// TestBurnFee_AssetIssue_FlagOff: when optimization inactive, fee credited to blackhole.
func TestBurnFee_AssetIssue_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	fee := ctx.DynProps.AssetIssueFee()
	ctx.State.AddBalance(owner, fee)

	setBlackholeOpt(ctx, false)

	act := &AssetIssueActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got := ctx.State.GetBalance(owner); got != 0 {
		t.Errorf("owner balance: want 0, got %d", got)
	}
	if got := blackholeBalance(ctx); got != fee {
		t.Errorf("blackhole balance (flag OFF): want %d, got %d", fee, got)
	}
}

// TestBurnFee_WitnessCreate_FlagOn: witness fee is burned.
func TestBurnFee_WitnessCreate_FlagOn(t *testing.T) {
	owner := makeTestAddr(2)
	c := &contractpb.WitnessCreateContract{
		OwnerAddress: owner[:],
		Url:          []byte("http://mywitness.example.com"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WitnessCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	fee := ctx.DynProps.AccountUpgradeCost()
	ctx.State.AddBalance(owner, fee)

	setBlackholeOpt(ctx, true)

	act := &WitnessCreateActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got := ctx.State.GetBalance(owner); got != 0 {
		t.Errorf("owner balance: want 0, got %d", got)
	}
	if got := blackholeBalance(ctx); got != 0 {
		t.Errorf("blackhole balance (flag ON): want 0, got %d", got)
	}
}

// TestBurnFee_WitnessCreate_FlagOff: witness fee goes to blackhole.
func TestBurnFee_WitnessCreate_FlagOff(t *testing.T) {
	owner := makeTestAddr(2)
	c := &contractpb.WitnessCreateContract{
		OwnerAddress: owner[:],
		Url:          []byte("http://mywitness.example.com"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WitnessCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	fee := ctx.DynProps.AccountUpgradeCost()
	ctx.State.AddBalance(owner, fee)

	setBlackholeOpt(ctx, false)

	act := &WitnessCreateActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if got := ctx.State.GetBalance(owner); got != 0 {
		t.Errorf("owner balance: want 0, got %d", got)
	}
	if got := blackholeBalance(ctx); got != fee {
		t.Errorf("blackhole balance (flag OFF): want %d, got %d", fee, got)
	}
}

// TestBurnFee_Transfer_FlagOff: account-creation fee goes to blackhole when flag off.
func TestBurnFee_Transfer_FlagOff(t *testing.T) {
	owner := makeTestAddr(3)
	to := makeTestAddr(4) // not yet created
	const accountCreationFee = 100_000

	c := &contractpb.TransferContract{
		OwnerAddress: owner[:],
		ToAddress:    to[:],
		Amount:       1,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_TransferContract, c, 0)
	ctx.DynProps.Set("create_new_account_fee_in_system_contract", accountCreationFee)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, accountCreationFee+1) // fee + transfer amount

	setBlackholeOpt(ctx, false)

	act := &TransferActuator{}
	res, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if res.Fee != accountCreationFee {
		t.Errorf("result fee: want %d, got %d", accountCreationFee, res.Fee)
	}
	if got := blackholeBalance(ctx); got != accountCreationFee {
		t.Errorf("blackhole balance (flag OFF): want %d, got %d", accountCreationFee, got)
	}
}

// TestBurnFee_Transfer_FlagOn: account-creation fee is burned when flag on.
func TestBurnFee_Transfer_FlagOn(t *testing.T) {
	owner := makeTestAddr(3)
	to := makeTestAddr(4)
	const accountCreationFee = 100_000

	c := &contractpb.TransferContract{
		OwnerAddress: owner[:],
		ToAddress:    to[:],
		Amount:       1,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_TransferContract, c, 0)
	ctx.DynProps.Set("create_new_account_fee_in_system_contract", accountCreationFee)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, accountCreationFee+1)

	setBlackholeOpt(ctx, true)

	act := &TransferActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if got := blackholeBalance(ctx); got != 0 {
		t.Errorf("blackhole balance (flag ON): want 0, got %d", got)
	}
}
