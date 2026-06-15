package vm

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// freezeSelf executes the FREEZE/UNFREEZE opcodes (contract.Address). It is
// always funded and created in the harness below, so a self-freeze never hits
// the dead-account surcharge — mirroring java getFreezeCost where the caller
// always exists.
var freezeSelf = tcommon.Address{0x41, 0x02}

// runFreezeProgram runs `PUSH20 <receiver> PUSH3 <1 TRX> PUSH1 <ENERGY> FREEZE
// STOP` through the full interpreter so BOTH the jump-table base cost and any
// in-op surcharge are billed, and returns the energy consumed plus any run
// error. cfg.Freeze must be set by the caller (FREEZE is gated on AllowTvmFreeze).
// `receiver` is created iff createReceiver, toggling the dead-account branch.
func runFreezeProgram(t *testing.T, cfg TVMConfig, energyLimit uint64, receiver tcommon.Address, createReceiver bool) (uint64, error) {
	t.Helper()
	tvm, statedb, _ := newTestTVMForCreate(t, cfg, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(1_000_000)
	})
	statedb.CreateAccount(freezeSelf, corepb.AccountType_Normal)
	statedb.AddBalance(freezeSelf, 10*tvmTRXPrecision)
	if createReceiver {
		statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	}

	// Stack (bottom→top): receiver, amount, resourceType — pushed in that order.
	code := []byte{byte(PUSH20)}
	code = append(code, receiver[1:]...) // 20 bytes; uint256ToAddress re-prepends 0x41
	code = append(code,
		byte(PUSH3), 0x0F, 0x42, 0x40, // amount = 1_000_000 sun = 1 TRX
		byte(PUSH1), 0x01, // resourceType = 1 (ENERGY)
		byte(FREEZE),
		byte(STOP),
	)
	contract := NewContract(tcommon.Address{0x41, 0x01}, freezeSelf, 0, energyLimit)
	contract.SetCode(freezeSelf, code)
	_, err := tvm.interpreter.Run(contract)
	return energyLimit - contract.Energy, err
}

// pushTriple is the energy of PUSH20 + PUSH3 + PUSH1 (all VERY_LOW_TIER).
const pushTriple = 3 * EnergyVeryLow

// TestFreezeNewAccountSurcharge pins go-tron FREEZE energy to java-tron
// EnergyCost.getFreezeCost: FREEZE (20000) for a live receiver, FREEZE +
// NEW_ACCT_CALL (45000) when the receiver argument (stack[size-3]) is a dead
// account. gtron previously charged only the flat 20000 jump-table cost — a
// 25000-energy under-charge that forks state/energy_used on replay.
func TestFreezeNewAccountSurcharge(t *testing.T) {
	deadReceiver := tcommon.Address{0x41, 0xDE, 0xAD}
	liveReceiver := tcommon.Address{0x41, 0xA1, 0x10}
	const limit = 1_000_000

	cases := []struct {
		name           string
		cfg            TVMConfig
		receiver       tcommon.Address
		createReceiver bool
		wantOpCost     uint64
	}{
		{
			name:       "dead delegated receiver pays FREEZE + NEW_ACCT_CALL",
			cfg:        TVMConfig{Freeze: true},
			receiver:   deadReceiver,
			wantOpCost: EnergyFreeze + EnergyCallNewAcct, // 20000 + 25000
		},
		{
			name:           "existing delegated receiver pays only FREEZE",
			cfg:            TVMConfig{Freeze: true},
			receiver:       liveReceiver,
			createReceiver: true,
			wantOpCost:     EnergyFreeze, // 20000
		},
		{
			// receiver == caller: java reads the receiver word unconditionally,
			// but the caller always exists, so no surcharge — confirm we match.
			name:       "self freeze pays only FREEZE (caller always exists)",
			cfg:        TVMConfig{Freeze: true},
			receiver:   freezeSelf,
			wantOpCost: EnergyFreeze,
		},
		{
			// allow_energy_adjustment must not rebase FREEZE or its surcharge.
			name:       "dead receiver under EnergyAdjustment still pays surcharge",
			cfg:        TVMConfig{Freeze: true, EnergyAdjustment: true},
			receiver:   deadReceiver,
			wantOpCost: EnergyFreeze + EnergyCallNewAcct,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := runFreezeProgram(t, tc.cfg, limit, tc.receiver, tc.createReceiver)
			if err != nil {
				t.Fatalf("FREEZE run error: %v", err)
			}
			want := pushTriple + tc.wantOpCost
			if got != want {
				t.Fatalf("FREEZE energy: got %d, want %d (push %d + op %d)",
					got, want, pushTriple, tc.wantOpCost)
			}
		})
	}
}

// TestFreezeNewAccountOutOfEnergy reproduces the exact consensus divergence: a
// FREEZE to a new delegated receiver with only enough energy for the base cost
// but not the +25000 NEW_ACCT_CALL surcharge. java OUT_OF_ENERGYs here; gtron
// used to succeed, forking contractRet/energy_used on replay.
func TestFreezeNewAccountOutOfEnergy(t *testing.T) {
	deadReceiver := tcommon.Address{0x41, 0xDE, 0xAD}
	// Enough for the three PUSHes + FREEZE base, plus part of the surcharge,
	// but strictly less than push + base + NEW_ACCT_CALL.
	limit := uint64(pushTriple) + EnergyFreeze + (EnergyCallNewAcct - 1)

	_, err := runFreezeProgram(t, TVMConfig{Freeze: true}, limit, deadReceiver, false)
	if !errors.Is(err, ErrOutOfEnergy) {
		t.Fatalf("dead-receiver FREEZE under tight limit: got err %v, want ErrOutOfEnergy", err)
	}
}

// TestFreezeSiblings_NoNewAcctSurcharge guards java parity for the sibling ops:
// getUnfreezeCost / getFreezeExpireTimeCost are pure constants with NO
// dead-account term, so UNFREEZE/FREEZEEXPIRETIME to a dead receiver must charge
// only their flat base — never the NEW_ACCT_CALL surcharge.
func TestFreezeSiblings_NoNewAcctSurcharge(t *testing.T) {
	deadReceiver := tcommon.Address{0x41, 0xDE, 0xAD}
	const pushPair = 2 * EnergyVeryLow // PUSH20 + PUSH1

	run := func(op OpCode) uint64 {
		tvm, statedb, _ := newTestTVMForCreate(t, TVMConfig{Freeze: true}, func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_000_000)
		})
		statedb.CreateAccount(freezeSelf, corepb.AccountType_Normal)
		// Stack (bottom→top): receiver, resourceType.
		code := []byte{byte(PUSH20)}
		code = append(code, deadReceiver[1:]...)
		code = append(code, byte(PUSH1), 0x01, byte(op), byte(STOP))
		const limit = 1_000_000
		contract := NewContract(tcommon.Address{0x41, 0x01}, freezeSelf, 0, limit)
		contract.SetCode(freezeSelf, code)
		if _, err := tvm.interpreter.Run(contract); err != nil {
			t.Fatalf("op %v run error: %v", op, err)
		}
		return limit - contract.Energy
	}

	if got, want := run(UNFREEZE), pushPair+EnergyUnfreeze; got != want {
		t.Fatalf("UNFREEZE dead receiver: got %d, want %d (no NEW_ACCT_CALL)", got, want)
	}
	if got, want := run(FREEZEEXPIRETIME), pushPair+EnergyFreezeExpireTime; got != want {
		t.Fatalf("FREEZEEXPIRETIME dead receiver: got %d, want %d (no NEW_ACCT_CALL)", got, want)
	}
}
