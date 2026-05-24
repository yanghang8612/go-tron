package core

import (
	"bytes"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
)

// payBlockReward distributes a per-block reward to a witness, mirroring
// java-tron's MortgageService.payBlockReward → payReward.
//
// Before the change_delegation proposal activates (flat path), the full
// amount goes straight to the witness's allowance — the legacy behavior
// go-tron had prior to M1.5.
//
// Once change_delegation is on (new path), the reward is split by the
// witness's per-cycle brokerage: witness keeps `brokerage%`, the rest
// accumulates into DelegationStore as the voter reward pool for the
// current cycle.
//
// The db parameter remains in the signature for the existing block-processing
// call chain; reward-v2 storage itself is rooted in StateDB's SystemReward
// domain so cycle snapshots rewind with the block state root.
func payBlockReward(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties, witness tcommon.Address, amount int64) {
	if amount <= 0 {
		return
	}
	if !dp.ChangeDelegation() {
		statedb.AddAllowanceFinalReward(witness, amount)
		return
	}
	cycle := dp.CurrentCycleNumber()
	brokerage := statedb.ReadCycleBrokerage(cycle, witness.Bytes())
	payBlockRewardWithBrokerage(db, statedb, dp, witness, amount, cycle, brokerage)
}

func payBlockRewardWithBrokerage(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties, witness tcommon.Address, amount int64, cycle int64, brokerage int) {
	// Mirror java-tron expression tree exactly:
	//   double brokerageRate = (double) brokerage / 100;
	//   long brokerageAmount = (long) (brokerageRate * value);
	brokerageRate := float64(brokerage) / 100.0
	brokerageAmount := int64(brokerageRate * float64(amount))
	voterAmount := amount - brokerageAmount

	if voterAmount > 0 {
		_ = statedb.AddCycleRewardFinal(cycle, witness.Bytes(), voterAmount)
	}
	if brokerageAmount > 0 {
		statedb.AddAllowanceFinalReward(witness, brokerageAmount)
	}
}

// transactionFeePoolPeriod mirrors java-tron Constant.TRANSACTION_FEE_POOL_PERIOD.
// It is currently 1 block, but keep the constant explicit because Manager.payReward
// computes the fee reward through this divisor before subtracting it from the pool.
const transactionFeePoolPeriod int64 = 1

// payTransactionFeeReward pays the current block producer its transaction-fee
// pool share, then subtracts exactly that share from the dynamic-property pool.
// java-tron's Manager.payReward runs this after the normal block reward and
// standby reward whenever supportTransactionFeePool is active.
func payTransactionFeeReward(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties, witness tcommon.Address) {
	if !dp.AllowTransactionFeePool() {
		return
	}
	pool := dp.TransactionFeePool()
	reward := pool / transactionFeePoolPeriod
	if reward != 0 {
		payBlockReward(db, statedb, dp, witness, reward)
	}
	dp.SetTransactionFeePool(pool - reward)
}

// payStandbyWitness distributes the per-block WITNESS_127_PAY_PER_BLOCK
// allowance pro-rata among the top-WitnessStandbyLength witnesses by vote
// count, passing each share through payBlockReward (which applies
// brokerage splitting). Mirrors MortgageService.payStandbyWitness.
//
// Only runs once change_delegation is active — before that, the legacy
// IncentiveManager.reward path at maintenance time handles standby pay.
type standbyWitnessVote struct {
	addr      tcommon.Address
	votes     int64
	brokerage int
}

type standbyWitnessPaySet struct {
	witnesses []standbyWitnessVote
	voteSum   int64
	cycle     int64
}

func buildStandbyWitnessPaySet(db kvReadWriter, statedb *state.StateDB, cycle int64) *standbyWitnessPaySet {
	// Gather all known witnesses from the index, sort by vote count desc,
	// take the top N. Matches java-tron's WitnessStore.getWitnessStandby
	// for the non-optimized path.
	addrs := statedb.ReadWitnessIndex()
	if len(addrs) == 0 {
		return nil
	}
	all := make([]standbyWitnessVote, 0, len(addrs))
	for _, a := range addrs {
		w := statedb.GetWitness(a)
		if w == nil {
			continue
		}
		all = append(all, standbyWitnessVote{addr: a, votes: w.VoteCount()})
	}
	// Descending by vote; stable tiebreak by address to match java-tron's
	// deterministic sort.
	sort.Slice(all, func(i, j int) bool {
		if all[i].votes != all[j].votes {
			return all[i].votes > all[j].votes
		}
		return bytes.Compare(all[i].addr[:], all[j].addr[:]) < 0
	})
	const standbyN = 127
	if len(all) > standbyN {
		all = all[:standbyN]
	}

	var voteSum int64
	for i, v := range all {
		voteSum += v.votes
		all[i].brokerage = statedb.ReadCycleBrokerage(cycle, v.addr.Bytes())
	}
	if voteSum < 1 {
		return nil
	}
	return &standbyWitnessPaySet{witnesses: all, voteSum: voteSum, cycle: cycle}
}

func payStandbyWitness(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties) {
	payStandbyWitnessWithSet(db, statedb, dp, nil)
}

func payStandbyWitnessWithSet(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties, set *standbyWitnessPaySet) {
	if !dp.ChangeDelegation() {
		return
	}
	totalPay := dp.Witness127PayPerBlock()
	if totalPay <= 0 {
		return
	}
	cycle := dp.CurrentCycleNumber()
	if set == nil || set.cycle != cycle {
		set = buildStandbyWitnessPaySet(db, statedb, cycle)
	}
	if set == nil || set.voteSum < 1 {
		return
	}
	eachVotePay := float64(totalPay) / float64(set.voteSum)
	voterDeltas := make(map[tcommon.Address]int64, len(set.witnesses))
	for _, v := range set.witnesses {
		pay := int64(float64(v.votes) * eachVotePay)
		if pay <= 0 {
			continue
		}
		brokerageRate := float64(v.brokerage) / 100.0
		brokerageAmount := int64(brokerageRate * float64(pay))
		voterAmount := pay - brokerageAmount
		if voterAmount > 0 {
			voterDeltas[v.addr] += voterAmount
		}
		if brokerageAmount > 0 {
			statedb.AddAllowanceFinalReward(v.addr, brokerageAmount)
		}
	}
	_ = statedb.AddCycleRewardsFinal(cycle, voterDeltas)
}

// kvReadWriter is retained for the existing block-processing signatures while
// reward-v2 data moves into StateDB. Callers still pass the same DB-like value
// used by older paths, but this file no longer writes reward snapshots there.
type kvReadWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// accumulateWitnessVi rolls the per-cycle reward into the witness VI at a
// maintenance boundary. Mirrors DelegationStore.accumulateWitnessVi.
//
// Formula: VI[cycle] = VI[cycle-1] + (reward × 10^18 / voteCount)
// If reward or voteCount is zero, VI is just forwarded (only persisted
// when the forwarded value is nonzero, matching java-tron's
// "Zero vi will not be record" guard).
func accumulateWitnessVi(statedb *state.StateDB, cycle int64, addr []byte, voteCount int64) {
	preVi := statedb.ReadWitnessVI(cycle-1, addr)
	cycleReward := statedb.ReadCycleReward(cycle, addr)
	if cycleReward == 0 || voteCount == 0 {
		if preVi.Sign() != 0 {
			_ = statedb.WriteWitnessVI(cycle, addr, preVi)
		}
		return
	}
	delta := new(big.Int).Mul(big.NewInt(cycleReward), reward.DecimalOfViReward)
	delta.Quo(delta, big.NewInt(voteCount))
	_ = statedb.WriteWitnessVI(cycle, addr, new(big.Int).Add(preVi, delta))
}

// maintenanceWitnessVotes returns every known witness and its current
// StateDB vote count. The native witness domain is authoritative during
// maintenance because tryRemoveThePowerOfTheGr and pending vote deltas may have
// already mutated witnesses before any legacy flat mirror is flushed.
func maintenanceWitnessVotes(statedb *state.StateDB) []struct {
	addr  tcommon.Address
	votes int64
} {
	witnessAddrs := statedb.ReadWitnessIndex()
	if len(witnessAddrs) == 0 {
		return nil
	}

	ws := make([]struct {
		addr  tcommon.Address
		votes int64
	}, 0, len(witnessAddrs))
	for _, a := range witnessAddrs {
		w := statedb.GetWitness(a)
		if w == nil {
			continue
		}
		ws = append(ws, struct {
			addr  tcommon.Address
			votes int64
		}{a, w.VoteCount()})
	}
	return ws
}

// applyRewardVI mirrors the first reward step in java-tron
// MaintenanceManager.doMaintenance: accumulate VI before VotesStore deltas are
// folded into WitnessStore.
func applyRewardVI(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties) {
	ws := maintenanceWitnessVotes(statedb)
	if len(ws) == 0 {
		return
	}
	curCycle := dp.CurrentCycleNumber()
	if dp.UseNewRewardAlgorithm() {
		for _, w := range ws {
			accumulateWitnessVi(statedb, curCycle, w.addr.Bytes(), w.votes)
		}
	}
}

// applyRewardCycleSnapshot mirrors the final change_delegation step in
// java-tron MaintenanceManager.doMaintenance: after pending vote deltas have
// been applied, advance the cycle and snapshot brokerage/vote counts for the
// new cycle.
func applyRewardCycleSnapshot(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties) {
	if !dp.ChangeDelegation() {
		return
	}
	ws := maintenanceWitnessVotes(statedb)
	if len(ws) == 0 {
		return
	}

	nextCycle := dp.CurrentCycleNumber() + 1
	for _, w := range ws {
		brokerage := statedb.ReadWitnessBrokerage(w.addr)
		_ = statedb.WriteCycleBrokerage(nextCycle, w.addr.Bytes(), int(brokerage))
		_ = statedb.WriteCycleVote(nextCycle, w.addr.Bytes(), w.votes)
	}
	dp.SetCurrentCycleNumber(nextCycle)
}

// applyRewardMaintenance is retained for tests and standalone callers; the
// block maintenance path uses the split helpers so VotesStore deltas can be
// applied between VI accumulation and cycle snapshot, matching java-tron.
func applyRewardMaintenance(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties) {
	applyRewardVI(db, statedb, dp)
	applyRewardCycleSnapshot(db, statedb, dp)
}
