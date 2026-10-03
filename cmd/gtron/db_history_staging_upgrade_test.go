package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryStagingUpgradeJournalBindingAndSwitchBoundaries(t *testing.T) {
	paths := historyStagingPaths{Source: "/source", Target: "/target", Cold: "/cold"}
	c := &historyStagingCLIContext{paths: paths, event: historyStagingCLIEvent{JobID: strings.Repeat("a", 32), CandidateSHA256: strings.Repeat("c", 64)}}
	old := historyStagingUpgradeIdentity{JobID: c.event.JobID, CandidateSHA: strings.Repeat("b", 64), Candidate: "/old", SourceCommit: strings.Repeat("e", 40), Source: paths.Source, Target: paths.Target, Cold: paths.Cold}
	newID := old
	newID.CandidateSHA = c.event.CandidateSHA256
	newID.Candidate = "/new"
	oldL := map[string]any{"version": 1, "state": "MIGRATION_IN_PROGRESS", "job_id": old.JobID, "plan_id": strings.Repeat("d", 64), "candidate": old.Candidate, "candidate_sha256": old.CandidateSHA, "source_commit": old.SourceCommit, "source": old.Source, "target": old.Target, "cold": old.Cold}
	oldP := map[string]any{"version": 1, "candidate_sha256": old.CandidateSHA, "source_commit": old.SourceCommit, "source": old.Source, "target": old.Target, "cold": old.Cold}
	newL, newP := map[string]any{}, map[string]any{}
	for k, v := range oldL {
		newL[k] = v
	}
	for k, v := range oldP {
		newP[k] = v
	}
	newL["candidate"] = newID.Candidate
	newL["candidate_sha256"] = newID.CandidateSHA
	newL["plan_producer_sha256"] = old.CandidateSHA
	newL["upgrade_binding"] = historyStagingUpgradeBinding
	newP["candidate_sha256"] = newID.CandidateSHA
	enc := func(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
	j := historyStagingUpgradeJournal{Version: 1, State: "PRECHECK", JobID: old.JobID, PlanID: strings.Repeat("d", 64), ProducerSHA: old.CandidateSHA, Old: old, New: newID, OldLatch: enc(oldL), NewLatch: enc(newL), OldPrepared: enc(oldP), NewPrepared: enc(newP)}
	for _, phase := range []string{"PRECHECK", "AUTHORIZED", "DONE"} {
		j.State = phase
		for _, pair := range []struct {
			l, p  []byte
			valid bool
		}{{j.OldLatch, j.OldPrepared, true}, {j.OldLatch, j.NewPrepared, phase != "PRECHECK"}, {j.NewLatch, j.NewPrepared, phase != "PRECHECK"}, {j.NewLatch, j.OldPrepared, false}} {
			err := validateHistoryStagingUpgradeJournal(j, c, j.PlanID, "upgrade-check", pair.l, pair.p, true)
			if (err == nil) != pair.valid {
				t.Fatalf("%s wrong switch acceptance: %v", phase, err)
			}
		}
		err := validateHistoryStagingUpgradeJournal(j, c, j.PlanID, "resume", j.NewLatch, j.NewPrepared, false)
		if (err == nil) != (phase == "DONE") {
			t.Fatalf("%s ordinary resume acceptance: %v", phase, err)
		}
		if validateHistoryStagingUpgradeJournal(j, c, j.PlanID, "resume", j.NewLatch, j.NewPrepared, true) == nil {
			t.Fatal("pending marker allowed ordinary resume")
		}
	}
	j.State = "DONE"
	for _, mutate := range []func(*historyStagingUpgradeJournal){func(v *historyStagingUpgradeJournal) { v.ProducerSHA = strings.Repeat("e", 64) }, func(v *historyStagingUpgradeJournal) { v.New.CandidateSHA = v.Old.CandidateSHA }, func(v *historyStagingUpgradeJournal) { v.New.Target = "/elsewhere" }, func(v *historyStagingUpgradeJournal) { v.PlanID = strings.Repeat("e", 64) }} {
		other := j
		mutate(&other)
		if validateHistoryStagingUpgradeJournal(other, c, j.PlanID, "resume", j.NewLatch, j.NewPrepared, false) == nil {
			t.Fatal("forged upgrade identity accepted")
		}
	}
	// Activation adds only the lifecycle fields; frozen intent remains fixed.
	active := map[string]any{}
	for k, v := range newL {
		active[k] = v
	}
	active["state"] = "VERIFIED_PENDING_ACTIVATION"
	active["history_window"] = 256
	active["prune_mode"] = "snap"
	if !historyStagingUpgradeActiveLatch(enc(active), j.NewLatch) {
		t.Fatal("matching activation lifecycle refused")
	}
	active["candidate_sha256"] = "wrong"
	if historyStagingUpgradeActiveLatch(enc(active), j.NewLatch) {
		t.Fatal("changed executor allowed")
	}
}
func TestHistoryStagingUpgradeReceiptBeforeReadyCheckpoint(t *testing.T) {
	// CopyClaim persists the receipt before the source claim's TargetReady bit.
	// This legal interruption must retain the same claim and resume without replan.
	proof := rawdb.HistoryStagingProof{Bucket: 1, Epoch: 1, EligibleThrough: 2047, FinishBlock: 2047, IndexBlock: 2047, FinishHash: common.Hash{1}, IndexHash: common.Hash{1}}
	for n := uint64(1024); n <= 2047; n++ {
		proof.Blocks = append(proof.Blocks, rawdb.HistoryStagingBlockProof{Number: n, Hash: common.Hash{1}, BeginTxNum: n, EndTxNum: n})
	}
	row := historyStagingPlanBucket{Proof: proof, Physical: rawdb.HistoryStagingPhysicalStats{ChangeRows: 2, Rows: 1028, Bytes: 4096, TxRangeRows: 1024, ChunkRows: 1, Digest: [32]byte{9}}}
	planID := strings.Repeat("d", 64)
	claimID, err := historyStagingClaimID(planID, proof.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	pd, err := rawdb.HistoryStagingProofDigest(proof)
	if err != nil {
		t.Fatal(err)
	}
	state := rawdb.HistoryStagingBucketState{HasRoute: true, Route: rawdb.HistoryStagingRoute{Bucket: proof.Bucket, Epoch: proof.Epoch, Owner: rawdb.HistoryStagingOwnerSource}, HasClaim: true, Claim: rawdb.HistoryStagingClaim{WriteVersion: 1, Bucket: proof.Bucket, Epoch: proof.Epoch, ClaimID: claimID, Proof: proof, ProofDigest: pd}, HasReceipt: true, Receipt: rawdb.HistoryStagingReceipt{Bucket: proof.Bucket, Epoch: proof.Epoch, ClaimID: claimID, ProofDigest: pd, DataDigest: row.Physical.Digest, PayloadRows: 2, PayloadBytes: 4096, TxRangeRows: 1024, ChunkRows: 1}}
	if err = verifyHistoryStagingUpgradeBucket(state, row, planID); err != nil {
		t.Fatal(err)
	}
	state.Receipt.ChunkRows++
	if verifyHistoryStagingUpgradeBucket(state, row, planID) == nil {
		t.Fatal("mismatched receipt accepted")
	}
}
