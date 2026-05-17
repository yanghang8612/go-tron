package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestBlockChainInsertBlock_IsJobsRotationAcrossMaintenance is the
// applyBlock-level regression test for e1b9920 ("fix(witness): set is_jobs
// at genesis and maintenance rotation"). flipWitnessIsJobs is unit-tested in
// blockchain_test.go, but until this test there was no integration coverage
// that actually rotates the active set across a real maintenance-cycle
// boundary inside applyBlock.
//
// Setup: 30 witnesses with distinct, strictly-decreasing vote counts, so the
// vote-ranked top-27 is the well-defined set [0..26]. We deliberately seed
// the *persisted* active set to a different 27-member set — [0..25] ∪ {27} —
// i.e. witness #27 sits where witness #26 belongs. This models a chain whose
// stored active set has drifted from the current vote ranking (e.g. carried
// over from a prior cycle). When block #2 crosses the maintenance boundary,
// applyBlock's DoMaintenance → SelectActiveWitnesses recomputes the set
// strictly from votes, yielding [0..26]: witness #26 rotates IN, witness #27
// rotates OUT. Everyone in [0..25] is unaffected, as is standby witness #28
// and #29.
//
// After the boundary, applyBlock's flipWitnessIsJobs must have:
//
//	(a) the new active set = [0..26] (java-tron updateWitness parity),
//	(b) is_jobs=true on every member of the new active set, including the
//	    freshly-admitted witness #26,
//	(c) is_jobs=false on witness #27 (rotated out),
//	(d) unaffected witnesses keep their prior is_jobs — the [0..25] active
//	    members stay true, the untouched standbys #28/#29 stay false.
//
// is_jobs is read back from the persisted witness records via bc.BufferedDB()
// (flipWitnessIsJobs writes through bc.buffer; with 27 active witnesses and a
// single producer the solidified line never advances, so nothing flushes to
// the bare disk store).
func TestBlockChainInsertBlock_IsJobsRotationAcrossMaintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	const interval = int64(21_600_000) // 6h, java-tron default
	const numWitnesses = 30            // > MaxActiveWitnessNum (27)

	// witnessAddr(i) is testCoreAddr(40+i); i ranges over [0, numWitnesses).
	witnessAddr := func(i int) tcommon.Address { return testCoreAddr(byte(40 + i)) }

	// Distinct, strictly-decreasing vote counts: witness i gets 100000-i*100.
	// Vote-ranked top 27 = witnesses [0..26]; standby = [27,28,29].
	initialVote := func(i int) int64 { return int64(100_000 - i*100) }

	genesisWitnesses := make([]params.GenesisWitness, numWitnesses)
	accounts := []params.GenesisAccount{{Address: testCoreAddr(1), Balance: 100_000_000}}
	for i := 0; i < numWitnesses; i++ {
		genesisWitnesses[i] = params.GenesisWitness{
			Address:   witnessAddr(i),
			VoteCount: initialVote(i),
			URL:       "http://w",
		}
		accounts = append(accounts, params.GenesisAccount{Address: witnessAddr(i), Balance: 1_000_000})
	}

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts:  accounts,
		Witnesses: genesisWitnesses,
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval,
			// Neutralize tryRemoveThePowerOfTheGr — at value 1 it would strip
			// each genesis witness's initial vote and scramble the rotation.
			"remove_the_power_of_the_gr": -1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteVotes(diskdb, witnessAddr(0), &corepb.Votes{
		Address: witnessAddr(0).Bytes(),
		OldVotes: []*corepb.Vote{
			{VoteAddress: witnessAddr(0).Bytes(), VoteCount: 1},
		},
		NewVotes: []*corepb.Vote{
			{VoteAddress: witnessAddr(0).Bytes(), VoteCount: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Seed the persisted active set to [0..25] ∪ {27} — witness #27 occupies
	// the slot that the vote ranking assigns to witness #26. NewBlockChain
	// honours a non-empty persisted active set verbatim (it only derives the
	// set from votes when none is stored), so this is the chain's "old"
	// active set going into the boundary.
	seededActive := make([]tcommon.Address, 0, params.MaxActiveWitnessNum)
	for i := 0; i <= 25; i++ {
		seededActive = append(seededActive, witnessAddr(i))
	}
	seededActive = append(seededActive, witnessAddr(27))
	rawdb.WriteActiveWitnesses(diskdb, seededActive)

	// genesis.go sets is_jobs=true on EVERY genesis witness, including the
	// standby ones. Make the persisted is_jobs flags consistent with the
	// seeded active set: true for [0..25,27], false for [26,28,29]. This way
	// the "incoming witness flips to true" assertion on witness #26 is a
	// discriminating signal (it starts false) rather than trivially
	// pre-satisfied by the genesis blanket-true.
	seededActiveSet := map[tcommon.Address]bool{}
	for _, a := range seededActive {
		seededActiveSet[a] = true
	}
	for i := 0; i < numWitnesses; i++ {
		w := rawdb.ReadWitness(diskdb, witnessAddr(i))
		w.SetIsJobs(seededActiveSet[witnessAddr(i)])
		rawdb.WriteWitness(diskdb, witnessAddr(i), w)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Sanity: NewBlockChain loaded the seeded (drifted) active set verbatim.
	oldActive := map[tcommon.Address]bool{}
	for _, a := range bc.ActiveWitnesses() {
		oldActive[a] = true
	}
	if len(oldActive) != params.MaxActiveWitnessNum {
		t.Fatalf("initial active set size: got %d, want %d", len(oldActive), params.MaxActiveWitnessNum)
	}
	if !oldActive[witnessAddr(27)] || oldActive[witnessAddr(26)] {
		t.Fatalf("seeded active set not loaded: want #27 in / #26 out, got in27=%v in26=%v",
			oldActive[witnessAddr(27)], oldActive[witnessAddr(26)])
	}

	// Producer = witness #0: highest vote, firmly inside the top-27 both
	// before and after rotation.
	producer := witnessAddr(0)

	// Block #1: pre-boundary. java-tron skips doMaintenance on block #1
	// regardless of the boundary flag, so the real rotation must land on
	// block #2+.
	block1 := buildTestBlock(bc, producer, interval/2)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("InsertBlock(block#1): %v", err)
	}
	if bc.NextMaintenanceTime() != interval {
		t.Fatalf("next_maintenance_time should be unchanged before the boundary, got %d", bc.NextMaintenanceTime())
	}

	// Block #2: crosses the maintenance boundary → runs doMaintenance,
	// recomputes the active set strictly from votes, and flips is_jobs.
	block2 := buildTestBlock(bc, producer, interval)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatalf("InsertBlock(block#2): %v", err)
	}

	// (a) New active set = vote-ranked top 27 = [0..26]; #27 dropped.
	wantActive := map[tcommon.Address]bool{}
	for i := 0; i <= 26; i++ {
		wantActive[witnessAddr(i)] = true
	}
	gotActive := map[tcommon.Address]bool{}
	for _, a := range bc.ActiveWitnesses() {
		gotActive[a] = true
	}
	if len(gotActive) != params.MaxActiveWitnessNum {
		t.Fatalf("post-boundary active set size: got %d, want %d", len(gotActive), params.MaxActiveWitnessNum)
	}
	for addr := range wantActive {
		if !gotActive[addr] {
			t.Fatalf("witness %s missing from post-boundary active set", addr.Hex())
		}
	}
	if gotActive[witnessAddr(27)] {
		t.Fatal("witness #27 should have rotated out of the active set")
	}
	// Cross-check the hand-computed expectation against SelectActiveWitnesses
	// applied to the witness votes directly — guards the test against drift
	// from the production selection logic.
	allVotes := make([]dpos.WitnessVote, numWitnesses)
	for i := 0; i < numWitnesses; i++ {
		allVotes[i] = dpos.WitnessVote{Address: witnessAddr(i), Votes: initialVote(i)}
	}
	for _, a := range dpos.SelectActiveWitnesses(allVotes) {
		if !gotActive[a] {
			t.Fatalf("SelectActiveWitnesses picked %s but it is not in bc.ActiveWitnesses()", a.Hex())
		}
	}

	// is_jobs is read from the persisted witness records through the buffered
	// view — flipWitnessIsJobs writes via bc.buffer and with 27 active
	// witnesses + one producer the solidified line never advances.
	isJobs := func(i int) bool {
		w := rawdb.ReadWitness(bc.BufferedDB(), witnessAddr(i))
		if w == nil {
			t.Fatalf("witness #%d missing after maintenance", i)
		}
		return w.IsJobs()
	}

	// (b) Every member of the new active set has is_jobs=true. Witness #26 is
	//     the discriminating case: it was seeded is_jobs=false above, so a
	//     true reading here proves the incoming flip ran.
	for i := 0; i <= 26; i++ {
		if !isJobs(i) {
			t.Errorf("witness #%d in active set: is_jobs=false, want true", i)
		}
	}

	// (c) Witness #27 rotated out → is_jobs cleared.
	if isJobs(27) {
		t.Error("witness #27 (rotated OUT): is_jobs=true, want false")
	}

	// (d) Unaffected witnesses keep their prior is_jobs:
	//     - #28 and #29 were standby before and after → stay false.
	if isJobs(28) {
		t.Error("witness #28 (untouched standby): is_jobs=true, want false")
	}
	if isJobs(29) {
		t.Error("witness #29 (untouched standby): is_jobs=true, want false")
	}
	//     - active members [0..25] were active before and after → stay true
	//       (asserted in (b); the point is they were never incorrectly
	//       cleared-then-reset — covered by the same readings).

	// next_maintenance_time advanced past the boundary, confirming
	// doMaintenance actually ran on block #2.
	if got := bc.NextMaintenanceTime(); got != 2*interval {
		t.Fatalf("next_maintenance_time after boundary: got %d, want %d", got, 2*interval)
	}
}
