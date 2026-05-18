package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestProposalCreateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.ActiveWitnesses = []tcommon.Address{owner}
	act := &ProposalCreateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestProposalCreateNotWitness(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = nil // no active witnesses

	act := &ProposalCreateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-active witness")
	}
}

func TestProposalCreateEmptyParams(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	act := &ProposalCreateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for empty parameters")
	}
}

func TestProposalCreateUnknownParameter(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{4000: 1},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")

	act := &ProposalCreateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for unknown proposal parameter")
	}
}

func TestProposalCreateRejectsInvalidParameterValue(t *testing.T) {
	ctx, act := newProposalCreateValidationContext(t, map[int64]int64{
		9: 2, // ALLOW_CREATION_OF_CONTRACTS only accepts 1 in java-tron.
	})
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for invalid proposal parameter value")
	}
}

func TestProposalCreateRejectsMissingParameterDependency(t *testing.T) {
	ctx, act := newProposalCreateValidationContext(t, map[int64]int64{
		18: 1, // ALLOW_TVM_TRANSFER_TRC10 requires ALLOW_SAME_TOKEN_NAME first.
	})
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing proposal dependency")
	}

	ctx.DynProps.SetAllowSameTokenName(true)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate after dependency activation failed: %v", err)
	}
}

func TestProposalCreateRejectsParameterBeforeForkVersion(t *testing.T) {
	ctx, act := newProposalCreateValidationContext(t, map[int64]int64{
		20: 1, // ALLOW_MULTI_SIGN requires VERSION_3_5 fork votes first.
	})
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error before required fork version passes")
	}

	markProposalForkVersionPassed(t, ctx, 7)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate after fork version passed failed: %v", err)
	}
}

func TestProposalCreateValidatesParameterRange(t *testing.T) {
	ctx, act := newProposalCreateValidationContext(t, map[int64]int64{
		0: 3 * 27 * 1000, // MAINTENANCE_TIME_INTERVAL lower bound.
	})
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed for java-tron lower-bound value: %v", err)
	}

	ctx, act = newProposalCreateValidationContext(t, map[int64]int64{0: 3*27*1000 - 1})
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for maintenance interval below java-tron lower bound")
	}
}

func TestProposalCreateAcceptsHistoricalNileShieldedActivation(t *testing.T) {
	ctx, act := newProposalCreateValidationContext(t, map[int64]int64{
		27: 1, // ALLOW_SHIELDED_TRANSACTION, accepted by Nile at block 1,628,391 only.
	})
	ctx.BlockNumber = proposalNileShieldedActivationBlock
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate historical Nile proposal failed: %v", err)
	}

	ctx.BlockNumber = proposalNileShieldedActivationBlock - 1
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected historical shielded proposal to be rejected at a different block")
	}

	ctx, act = newProposalCreateValidationContext(t, map[int64]int64{27: 0})
	ctx.BlockNumber = proposalNileShieldedActivationBlock
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected historical shielded proposal to require value 1")
	}
}

func newProposalCreateValidationContext(t *testing.T, params map[int64]int64) (*Context, *ProposalCreateActuator) {
	t.Helper()
	owner := makeTestAddr(1)
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   params,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")
	return ctx, &ProposalCreateActuator{}
}

func markProposalForkVersionPassed(t *testing.T, ctx *Context, version int32) {
	t.Helper()
	stats := make([]byte, 27)
	for i := range stats {
		stats[i] = 1
	}
	rawdb.WriteForkStats(ctx.DB, version, stats)
}

// TestProposalCreateExpiration_NileProposal1Parity replays Nile-live's
// proposal #1 fixture and asserts gtron's ProposalCreateActuator computes
// the same expiration_time. Inputs are taken directly from
// `nile.trongrid.io/wallet/getproposalbyid?id=1`:
//
//	create_time     = 1572596523000      (proposer's block timestamp)
//	expiration_time = 1572597600000      (Oct 31 2019 14:00:00 UTC)
//	parameters      = {9:1, 10:1}        (allow_creation_of_contracts +
//	                                      remove_the_power_of_the_gr)
//
// The chain values at that block (from `config-nile.conf` and observable
// at Nile h~5k) were:
//
//	proposal_expire_time      = 600000              (10 min)
//	next_maintenance_time     = 1572576000000       (Oct 31 08:00 UTC)
//	maintenance_time_interval = 21600000            (6h)
//
// java-tron ProposalCreateActuator computes:
//
//	now3       = blockTime + proposal_expire_time = 1572597123000
//	round      = (now3 - nextMaintenance) / interval = 0
//	expiration = nextMaintenance + (round+1)*interval = 1572597600000 ✓
//
// Pre-fix gtron defaulted `proposal_expire_time = 259_200_000` (3 days),
// pushing the expiration ~12 maintenance cycles past creation. On the
// gtron soak this materialized as proposal #1 expiration_time =
// 1572868800000 (75h late); by then the active witness set had rotated
// SR-only, so the 27 GR approvers no longer intersected and the proposal
// settled CANCELED while Nile-live had it APPROVED. Diagnosed 2026-05-11.
func TestProposalCreateExpiration_NileProposal1Parity(t *testing.T) {
	const (
		nileBlockTime       int64 = 1572596523000
		nileNextMaintenance int64 = 1572576000000
		nileInterval        int64 = 21600000
		nileProposalExpire  int64 = 600000
		expectedExpiration  int64 = 1572597600000
	)

	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{9: 1, 10: 1},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}
	ctx.BlockTime = nileBlockTime
	ctx.PrevBlockTime = nileBlockTime
	ctx.DynProps.SetNextMaintenanceTime(nileNextMaintenance)
	ctx.DynProps.Set("maintenance_time_interval", nileInterval)
	ctx.DynProps.Set("proposal_expire_time", nileProposalExpire)
	ctx.DB = ethrawdb.NewMemoryDatabase()

	act := &ProposalCreateActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}

	p := rawdb.ReadProposal(ctx.DB, 1)
	if p == nil {
		t.Fatal("proposal #1 not stored")
	}
	if p.CreateTime != nileBlockTime {
		t.Fatalf("create_time: got %d, want %d (Nile-live)", p.CreateTime, nileBlockTime)
	}
	if p.ExpirationTime != expectedExpiration {
		t.Fatalf("expiration_time: got %d, want %d (Nile-live). "+
			"delta=%d ms (%dh); a non-zero delta means proposal_expire_time "+
			"or maintenance grid is off vs Nile.",
			p.ExpirationTime, expectedExpiration,
			p.ExpirationTime-expectedExpiration,
			(p.ExpirationTime-expectedExpiration)/3_600_000)
	}
}

// TestProposalCreate_UsesPrevBlockTime locks in the
// BlockTime-vs-PrevBlockTime semantic gap: java-tron stores
// `create_time = getLatestBlockHeaderTimestamp()` during
// processTransaction, which is the *previous* block's timestamp because
// Manager.applyBlock advances the DP value only after processTransaction
// returns. Pre-fix gtron stored the *current* block's timestamp,
// producing a +3000ms (one Nile block) drift vs Nile-live's proposal #1.
// Asserts both `create_time` and the expiration-computation input
// (`now3 = prevBlockTime + proposal_expire_time`) consume PrevBlockTime
// rather than BlockTime.
func TestProposalCreate_UsesPrevBlockTime(t *testing.T) {
	const (
		prev int64 = 1_572_596_523_000
		curr int64 = 1_572_596_526_000 // prev + 3000ms (one Nile slot)
	)

	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{9: 1},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}
	ctx.BlockTime = curr
	ctx.PrevBlockTime = prev
	// Maintenance grid wide enough that the round math is deterministic
	// and the only signal observable is which timestamp drives it.
	ctx.DynProps.Set("maintenance_time_interval", int64(21_600_000))
	ctx.DynProps.SetNextMaintenanceTime(prev + 1)
	ctx.DynProps.Set("proposal_expire_time", int64(600_000))
	ctx.DB = ethrawdb.NewMemoryDatabase()

	act := &ProposalCreateActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}

	p := rawdb.ReadProposal(ctx.DB, 1)
	if p == nil {
		t.Fatal("proposal not stored")
	}
	if p.CreateTime != prev {
		t.Fatalf("create_time: got %d, want %d (PrevBlockTime); +%d ms drift "+
			"means we accidentally read BlockTime",
			p.CreateTime, prev, p.CreateTime-prev)
	}
	if p.CreateTime == curr {
		t.Fatalf("create_time matched BlockTime=%d; actuator must read PrevBlockTime", curr)
	}
}

func TestProposalCreateExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db

	act := &ProposalCreateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	// java-tron parity: first proposal_id == 1 (pre-increment of latest=0).
	p := rawdb.ReadProposal(db, 1)
	if p == nil {
		t.Fatal("proposal not stored at id=1")
	}
	if p.ID != 1 || p.Proposer != owner || p.State != rawdb.ProposalStatePending {
		t.Fatalf("unexpected proposal: %+v", p)
	}
	if rawdb.ReadProposal(db, 0) != nil {
		t.Fatal("no proposal should be stored at id=0")
	}
	if ctx.DynProps.LatestProposalNum() != 1 {
		t.Fatalf("latest_proposal_num=%d, want 1", ctx.DynProps.LatestProposalNum())
	}

	// Second proposal must get id=2; counter advances to 2.
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("second execute failed: %v", err)
	}
	if p2 := rawdb.ReadProposal(db, 2); p2 == nil || p2.ID != 2 {
		t.Fatalf("second proposal not stored at id=2: %+v", p2)
	}
	if ctx.DynProps.LatestProposalNum() != 2 {
		t.Fatalf("latest_proposal_num=%d, want 2", ctx.DynProps.LatestProposalNum())
	}
}
