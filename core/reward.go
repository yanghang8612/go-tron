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
func payBlockReward(db ethdb.KeyValueStore, statedb *state.StateDB, dp *state.DynamicProperties, witness tcommon.Address, amount int64) {
	if amount <= 0 {
		return
	}
	if !dp.ChangeDelegation() {
		statedb.AddAllowance(witness, amount)
		return
	}
	cycle := dp.CurrentCycleNumber()
	brokerage := rawdb.ReadCycleBrokerage(db, cycle, witness.Bytes())

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
		statedb.AddAllowance(witness, brokerageAmount)
	}
}

// payStandbyWitness distributes the per-block WITNESS_127_PAY_PER_BLOCK
// allowance pro-rata among the top-WitnessStandbyLength witnesses by vote
// count, passing each share through payBlockReward (which applies
// brokerage splitting). Mirrors MortgageService.payStandbyWitness.
//
// Only runs once change_delegation is active — before that, the legacy
// IncentiveManager.reward path at maintenance time handles standby pay.
func payStandbyWitness(db ethdb.KeyValueStore, statedb *state.StateDB, dp *state.DynamicProperties) {
	if !dp.ChangeDelegation() {
		return
	}
	totalPay := dp.Witness127PayPerBlock()
	if totalPay <= 0 {
		return
	}

	// Gather all known witnesses from the index, sort by vote count desc,
	// take the top N. Matches java-tron's WitnessStore.getWitnessStandby
	// for the non-optimized path.
	addrs := rawdb.ReadWitnessIndex(db)
	if len(addrs) == 0 {
		return
	}
	type vw struct {
		addr  tcommon.Address
		votes int64
	}
	all := make([]vw, 0, len(addrs))
	for _, a := range addrs {
		w := statedb.GetWitness(a)
		if w == nil {
			wc := rawdb.ReadWitness(db, a)
			if wc == nil {
				continue
			}
			all = append(all, vw{a, wc.VoteCount()})
		} else {
			all = append(all, vw{a, w.VoteCount()})
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
	for _, v := range all {
		voteSum += v.votes
	}
	if voteSum < 1 {
		return
	}
	eachVotePay := float64(totalPay) / float64(voteSum)
	for _, v := range all {
		pay := int64(float64(v.votes) * eachVotePay)
		payBlockReward(db, statedb, dp, v.addr, pay)
	}
}

// accumulateWitnessVi rolls the per-cycle reward into the witness VI at a
// maintenance boundary. Mirrors DelegationStore.accumulateWitnessVi.
//
// Formula: VI[cycle] = VI[cycle-1] + (reward × 10^18 / voteCount)
// If reward or voteCount is zero, VI is just forwarded (only persisted
// when the forwarded value is nonzero, matching java-tron's
// "Zero vi will not be record" guard).
func accumulateWitnessVi(db ethdb.KeyValueStore, cycle int64, addr []byte, voteCount int64) {
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

// applyRewardMaintenance runs at the maintenance boundary after
// dpos.DoMaintenance. It performs the two M1.5 pieces that belong to the
// new reward path:
//
//  1. If the chain has entered the new-algorithm window
//     (useNewRewardAlgorithm), accumulate per-cycle VI for every known
//     witness. Mirrors MaintenanceManager.doMaintenance's VI loop.
//  2. If change_delegation is on, increment current_cycle_number and
//     snapshot each witness's current brokerage rate and vote count into
//     DelegationStore under the new cycle number — this is the "current"
//     data voters will read against during their next withdraw.
//
// Called from InsertBlock / BuildBlock under the same maintenance-trigger
// condition as dpos.DoMaintenance.
func applyRewardMaintenance(db ethdb.KeyValueStore, statedb *state.StateDB, dp *state.DynamicProperties) {
	witnessAddrs := rawdb.ReadWitnessIndex(db)
	if len(witnessAddrs) == 0 {
		return
	}

	// Load vote counts from the in-memory statedb first (authoritative
	// during block processing) — fall back to the persisted witness on
	// cold starts.
	type wv struct {
		addr  tcommon.Address
		votes int64
	}
	ws := make([]wv, 0, len(witnessAddrs))
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
		ws = append(ws, wv{a, votes})
	}

	curCycle := dp.CurrentCycleNumber()

	if dp.UseNewRewardAlgorithm() {
		for _, w := range ws {
			accumulateWitnessVi(db, curCycle, w.addr.Bytes(), w.votes)
		}
	}

	if dp.ChangeDelegation() {
		nextCycle := curCycle + 1
		for _, w := range ws {
			brokerage := rawdb.ReadWitnessBrokerage(db, w.addr)
			rawdb.WriteCycleBrokerage(db, nextCycle, w.addr.Bytes(), int(brokerage))
			rawdb.WriteCycleVote(db, nextCycle, w.addr.Bytes(), w.votes)
		}
		dp.SetCurrentCycleNumber(nextCycle)
	}
}

