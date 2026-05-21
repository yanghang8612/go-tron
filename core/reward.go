package core

import (
	"bytes"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
// The db parameter is widened to `kvReadWriter` (slice 3 of the fork-rewind
// fix) so applyBlock can pass `bc.buffer`, making the cycle-reward write
// rewindable on switchFork once `change_delegation` is active. The legacy
// flat path does not touch rawdb, so the gate keeps disk traffic unchanged
// on mainnet pre-fork.
func payBlockReward(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties, witness tcommon.Address, amount int64) {
	if amount <= 0 {
		return
	}
	if !dp.ChangeDelegation() {
		statedb.AddAllowanceFinalReward(witness, amount)
		return
	}
	cycle := dp.CurrentCycleNumber()
	brokerage := rawdb.ReadCycleBrokerage(db, cycle, witness.Bytes())
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
		rawdb.AddCycleReward(db, cycle, witness.Bytes(), voterAmount)
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
//
// db is `kvReadWriter` so applyBlock can pass `bc.buffer` (slice 3 of the
// fork-rewind fix); see payBlockReward for the rewind semantics.
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
	addrs := rawdb.ReadWitnessIndex(db)
	if len(addrs) == 0 {
		return nil
	}
	all := make([]standbyWitnessVote, 0, len(addrs))
	for _, a := range addrs {
		w := statedb.GetWitness(a)
		if w == nil {
			wc := rawdb.ReadWitness(db, a)
			if wc == nil {
				continue
			}
			all = append(all, standbyWitnessVote{addr: a, votes: wc.VoteCount()})
		} else {
			all = append(all, standbyWitnessVote{addr: a, votes: w.VoteCount()})
		}
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
		all[i].brokerage = rawdb.ReadCycleBrokerage(db, cycle, v.addr.Bytes())
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
	for _, v := range set.witnesses {
		pay := int64(float64(v.votes) * eachVotePay)
		payBlockRewardWithBrokerage(db, statedb, dp, v.addr, pay, cycle, v.brokerage)
	}
}

// kvReadWriter is the narrow ethdb capability applyRewardMaintenance and
// accumulateWitnessVi need: reads (Read*) and writes (Write*) on per-cycle
// reward keys. Both rawdb.NewMemoryDatabase() (an ethdb.KeyValueStore) and
// blockbuffer.Buffer satisfy this, letting callers route the writes either
// to disk directly or through the fork-rewind buffer (slice 2).
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
func accumulateWitnessVi(db kvReadWriter, cycle int64, addr []byte, voteCount int64) {
	preVi := rawdb.ReadWitnessVI(db, cycle-1, addr)
	cycleReward := rawdb.ReadCycleReward(db, cycle, addr)
	if cycleReward == 0 || voteCount == 0 {
		if preVi.Sign() != 0 {
			rawdb.WriteWitnessVI(db, cycle, addr, preVi)
		}
		return
	}
	delta := new(big.Int).Mul(big.NewInt(cycleReward), reward.DecimalOfViReward)
	delta.Quo(delta, big.NewInt(voteCount))
	rawdb.WriteWitnessVI(db, cycle, addr, new(big.Int).Add(preVi, delta))
}

// maintenanceWitnessVotes returns every known witness and its current
// StateDB vote count. The in-memory StateDB is authoritative during
// maintenance because tryRemoveThePowerOfTheGr and pending vote deltas may
// have already mutated witnesses before the rawdb view is flushed.
func maintenanceWitnessVotes(db kvReadWriter, statedb *state.StateDB) []struct {
	addr  tcommon.Address
	votes int64
} {
	witnessAddrs := rawdb.ReadWitnessIndex(db)
	if len(witnessAddrs) == 0 {
		return nil
	}

	ws := make([]struct {
		addr  tcommon.Address
		votes int64
	}, 0, len(witnessAddrs))
	for _, a := range witnessAddrs {
		w := statedb.GetWitness(a)
		var votes int64
		if w != nil {
			votes = w.VoteCount()
		} else {
			stored := rawdb.ReadWitness(db, a)
			if stored == nil {
				continue
			}
			votes = stored.VoteCount()
		}
		ws = append(ws, struct {
			addr  tcommon.Address
			votes int64
		}{a, votes})
	}
	return ws
}

// applyRewardVI mirrors the first reward step in java-tron
// MaintenanceManager.doMaintenance: accumulate VI before VotesStore deltas are
// folded into WitnessStore.
func applyRewardVI(db kvReadWriter, statedb *state.StateDB, dp *state.DynamicProperties) {
	ws := maintenanceWitnessVotes(db, statedb)
	if len(ws) == 0 {
		return
	}
	curCycle := dp.CurrentCycleNumber()
	if dp.UseNewRewardAlgorithm() {
		for _, w := range ws {
			accumulateWitnessVi(db, curCycle, w.addr.Bytes(), w.votes)
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
	ws := maintenanceWitnessVotes(db, statedb)
	if len(ws) == 0 {
		return
	}

	nextCycle := dp.CurrentCycleNumber() + 1
	for _, w := range ws {
		brokerage := rawdb.ReadWitnessBrokerage(db, w.addr)
		rawdb.WriteCycleBrokerage(db, nextCycle, w.addr.Bytes(), int(brokerage))
		rawdb.WriteCycleVote(db, nextCycle, w.addr.Bytes(), w.votes)
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
