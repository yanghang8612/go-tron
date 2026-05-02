package actuator

import (
	"math"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func makeAssetIssueContract(ownerByte byte, name string, totalSupply int64) *contractpb.AssetIssueContract {
	owner := makeTestAddr(ownerByte)
	return &contractpb.AssetIssueContract{
		OwnerAddress: owner.Bytes(),
		Name:         []byte(name),
		Abbr:         []byte("TKN"),
		TotalSupply:  totalSupply,
		TrxNum:       1,
		Num:          1,
		StartTime:    1000,
		EndTime:      2000,
		Precision:    0,
	}
}

func TestAssetIssueValidate_Success(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestAssetIssueValidate_DuplicateName(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee()*2)
	if err := rawdb.WriteAssetNameIndex(db, []byte("MYTOKEN"), 999_999); err != nil {
		t.Fatal(err)
	}

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for duplicate name")
	}
}

func TestAssetIssueValidate_AlreadyIssued(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "NEWTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee()*2)
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], 999_999); err != nil {
		t.Fatal(err)
	}

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for already issued token")
	}
}

func TestAssetIssueValidate_InsufficientFee(t *testing.T) {
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	owner := makeTestAddr(1)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	// balance = 0, fee = 1_024_000_000

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient fee")
	}
}

func TestAssetIssueExecute(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	tokenID := int64(1_000_001)
	asset := rawdb.ReadAssetIssue(db, tokenID)
	if asset == nil {
		t.Fatal("asset should be stored in rawdb")
	}
	if string(asset.Name) != "MYTOKEN" {
		t.Fatalf("asset name: want MYTOKEN, got %s", asset.Name)
	}
	if ctx.State.GetTRC10Balance(owner, tokenID) != 1_000_000 {
		t.Fatalf("TRC10 balance: want 1000000, got %d", ctx.State.GetTRC10Balance(owner, tokenID))
	}
	if ctx.State.GetBalance(owner) != 0 {
		t.Fatalf("TRX balance after fee: expected 0, got %d", ctx.State.GetBalance(owner))
	}
	if ctx.DynProps.NextTokenID() != 1_000_002 {
		t.Fatalf("next_token_id: want 1000002, got %d", ctx.DynProps.NextTokenID())
	}
}

func TestAssetIssueValidate_NegativeFrozenAmount(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	c.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: -100_000, FrozenDays: 30},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for negative frozen_amount")
	}
}

func TestAssetIssueExecute_WithFrozenSupply(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "FROZENTOKEN", 1_000_000)
	c.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 200_000, FrozenDays: 30},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	tokenID := int64(1_000_001)
	// Only the 800,000 free tokens are minted; 200,000 are frozen
	if bal := ctx.State.GetTRC10Balance(owner, tokenID); bal != 800_000 {
		t.Fatalf("TRC10 balance: want 800000 (free portion), got %d", bal)
	}
}

func TestAssetIssueValidate_TooManyFrozenEntries(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "BIGTOKEN", 10_000_000)
	// maxFrozenSupplyNumber defaults to 10; add 11 entries
	for i := 0; i < 11; i++ {
		c.FrozenSupply = append(c.FrozenSupply, &contractpb.AssetIssueContract_FrozenSupply{
			FrozenAmount: 100,
			FrozenDays:   1,
		})
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for frozen supply count > max_frozen_supply_number")
	}
}

func makeAssetIssueCtx(t *testing.T, c *contractpb.AssetIssueContract) *Context {
	t.Helper()
	owner := makeTestAddr(1)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())
	return ctx
}

func TestAssetIssueValidate_FreeAssetNetLimitOutOfRange(t *testing.T) {
	act := &AssetIssueActuator{}

	// Negative limit
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	c.FreeAssetNetLimit = -1
	if err := act.Validate(makeAssetIssueCtx(t, c)); err == nil {
		t.Fatal("expected error for negative free_asset_net_limit")
	}

	// At the upper bound (== oneDayNetLimit = 57_600_000_000)
	c2 := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	c2.FreeAssetNetLimit = 57_600_000_000
	if err := act.Validate(makeAssetIssueCtx(t, c2)); err == nil {
		t.Fatal("expected error for free_asset_net_limit >= one_day_net_limit")
	}
}

func TestAssetIssueValidate_PublicFreeAssetNetLimitOutOfRange(t *testing.T) {
	act := &AssetIssueActuator{}

	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	c.PublicFreeAssetNetLimit = 57_600_000_000
	if err := act.Validate(makeAssetIssueCtx(t, c)); err == nil {
		t.Fatal("expected error for public_free_asset_net_limit >= one_day_net_limit")
	}
}

func TestAssetIssueValidate_FrozenDaysOutOfRange(t *testing.T) {
	act := &AssetIssueActuator{}

	// frozen_days = 0 (below minFrozenSupplyTime=1)
	c1 := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	c1.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 100, FrozenDays: 0},
	}
	if err := act.Validate(makeAssetIssueCtx(t, c1)); err == nil {
		t.Fatal("expected error for frozen_days=0 (below min)")
	}

	// frozen_days = 3653 (above maxFrozenSupplyTime=3652)
	c2 := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	c2.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 100, FrozenDays: 3653},
	}
	if err := act.Validate(makeAssetIssueCtx(t, c2)); err == nil {
		t.Fatal("expected error for frozen_days=3653 (above max)")
	}
}

func TestAssetIssueValidate_FrozenSumOverflow(t *testing.T) {
	owner := makeTestAddr(1)
	// Use math.MaxInt64 as total supply so individual amounts pass the per-entry check,
	// but two entries each just over half of MaxInt64 will overflow when summed.
	overflowAmount := int64(math.MaxInt64/2 + 1)
	c := makeAssetIssueContract(1, "OVERFLOWTOKEN", math.MaxInt64)
	c.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: overflowAmount, FrozenDays: 30},
		{FrozenAmount: overflowAmount, FrozenDays: 60},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for frozen supply sum overflow, got nil")
	}
}

// activateV34 seeds the v34 fork bitmap at quorum (80% of 27 SRs = ceil(21.6)
// = 22) so forks.PassVersion(db, 34, ...) returns true. ctx.BlockTime must
// be past the v34 hardForkTime ceiling for the test to exercise the gate.
func activateV34(t *testing.T, ctx *Context) {
	t.Helper()
	stats := make([]byte, 27)
	for i := 0; i < 22; i++ {
		stats[i] = forks.VoteUpgrade
	}
	rawdb.WriteForkStats(ctx.DB, 34, stats)
	ctx.BlockTime = 1_700_000_000_000 // well past 1596780000000
}

// TestAssetIssueValidate_FrozenSupplyOverflow_GatedOff locks down replay
// safety: pre-fork blocks containing combos that would trigger the v34
// expire-time overflow check must still validate. With the v34 bitmap
// untouched, forks.PassVersion returns false and the gate is skipped.
func TestAssetIssueValidate_FrozenSupplyOverflow_GatedOff(t *testing.T) {
	c := makeAssetIssueContract(1, "OVERFLOWTOKEN", 1_000_000)
	c.StartTime = math.MaxInt64 - 1 // would overflow on any positive frozenPeriod
	c.EndTime = math.MaxInt64       // start_time < end_time still holds
	c.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 100, FrozenDays: 3652},
	}
	ctx := makeAssetIssueCtx(t, c)
	// gate stays off — no v34 stats written.

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("pre-fork validate must succeed (replay safety), got: %v", err)
	}
}

// TestAssetIssueValidate_FrozenSupplyOverflow_PostFork mirrors java-tron
// AssetIssueActuator's VERSION_4_8_1 gate: with v34 active, a startTime +
// frozenDays*FROZEN_PERIOD that overflows int64 returns the expire-time
// overflow error.
func TestAssetIssueValidate_FrozenSupplyOverflow_PostFork(t *testing.T) {
	c := makeAssetIssueContract(1, "OVERFLOWTOKEN", 1_000_000)
	c.StartTime = math.MaxInt64 - 1
	c.EndTime = math.MaxInt64
	c.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 100, FrozenDays: 3652},
	}
	ctx := makeAssetIssueCtx(t, c)
	activateV34(t, ctx)

	act := &AssetIssueActuator{}
	err := act.Validate(ctx)
	if err == nil {
		t.Fatal("expected expire-time overflow error, got nil")
	}
	if !strings.Contains(err.Error(), "expire time overflow") {
		t.Fatalf("expected expire-time overflow error, got: %v", err)
	}
}

// TestAssetIssueValidate_FrozenSupplyOverflow_PostForkNormalValues confirms
// the gate does NOT false-positive on realistic combos: a current-epoch
// startTime + max-allowed FrozenDays adds well under MaxInt64.
func TestAssetIssueValidate_FrozenSupplyOverflow_PostForkNormalValues(t *testing.T) {
	c := makeAssetIssueContract(1, "GOODTOKEN", 1_000_000)
	c.StartTime = 1_700_000_000_000              // April 2026, ms epoch
	c.EndTime = c.StartTime + 365*86_400_000     // 1y after start
	c.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 100, FrozenDays: 3652}, // ~10 years
	}
	ctx := makeAssetIssueCtx(t, c)
	activateV34(t, ctx)

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("realistic combo must pass post-fork, got: %v", err)
	}
}

// TestAssetIssueValidate_FrozenSupplyOverflow_BoundaryAtMaxInt64 checks the
// exact threshold: startTime + frozenPeriod == MaxInt64 must succeed
// (MaxInt64 is a valid sum), but startTime + frozenPeriod > MaxInt64 fails.
func TestAssetIssueValidate_FrozenSupplyOverflow_BoundaryAtMaxInt64(t *testing.T) {
	const frozenPeriod = 3652 * 86_400_000 // = 315_532_800_000

	// Just-fits case: startTime + frozenPeriod == MaxInt64.
	c1 := makeAssetIssueContract(1, "BOUNDARY1", 1_000_000)
	c1.StartTime = math.MaxInt64 - frozenPeriod
	c1.EndTime = math.MaxInt64
	c1.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 100, FrozenDays: 3652},
	}
	ctx1 := makeAssetIssueCtx(t, c1)
	activateV34(t, ctx1)
	if err := (&AssetIssueActuator{}).Validate(ctx1); err != nil {
		t.Fatalf("startTime + frozenPeriod == MaxInt64 must pass, got: %v", err)
	}

	// Just-overflows case: startTime + frozenPeriod == MaxInt64 + 1.
	c2 := makeAssetIssueContract(1, "BOUNDARY2", 1_000_000)
	c2.StartTime = math.MaxInt64 - frozenPeriod + 1
	c2.EndTime = math.MaxInt64
	c2.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 100, FrozenDays: 3652},
	}
	ctx2 := makeAssetIssueCtx(t, c2)
	activateV34(t, ctx2)
	err := (&AssetIssueActuator{}).Validate(ctx2)
	if err == nil || !strings.Contains(err.Error(), "expire time overflow") {
		t.Fatalf("startTime + frozenPeriod == MaxInt64+1 must fail with overflow error, got: %v", err)
	}
}
