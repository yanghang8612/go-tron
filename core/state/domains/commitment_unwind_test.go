package domains

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// acctOp describes one account-latest mutation for applyAccountBlock.
// prevVal == nil means the row did not exist before this block (PrevExists=false).
// nextVal == nil means the row is deleted by this block (NextExists=false).
type acctOp struct {
	owner   common.Address
	prevVal []byte
	nextVal []byte
}

// applyAccountBlock writes one "block" worth of AccountLatest mutations into db
// and store:
//   - Updates (or deletes) the latest-domain row to nextVal.
//   - Writes a StateDomainChange capturing Prev/Next for the unwind path.
//   - Applies the matching commitment Update so the branch keyspace is current.
func applyAccountBlock(t *testing.T, db CommitmentDB, store LatestCommitmentStore, blockNum uint64, ops []acctOp) common.Hash {
	t.Helper()
	var updates []rawdb.StateCommitmentUpdate
	for i, op := range ops {
		// 1. Write/delete the latest-domain row.
		if op.nextVal != nil {
			if err := rawdb.WriteStateAccountLatest(db, op.owner, op.nextVal); err != nil {
				t.Fatalf("block %d op %d WriteStateAccountLatest: %v", blockNum, i, err)
			}
		} else {
			if err := rawdb.DeleteStateAccountLatest(db, op.owner); err != nil {
				t.Fatalf("block %d op %d DeleteStateAccountLatest: %v", blockNum, i, err)
			}
		}

		// 2. Write the StateDomainChange for the unwind path.
		change := &rawdb.StateDomainChange{
			BlockNum:    blockNum,
			Seq:         uint64(i),
			FlatDomain:  rawdb.StateFlatDomainAccountLatest,
			Owner:       op.owner,
			PrevExists:  op.prevVal != nil,
			Prev:        op.prevVal,
			NextExists:  op.nextVal != nil,
			Next:        op.nextVal,
		}
		if err := rawdb.WriteStateDomainChange(db, change); err != nil {
			t.Fatalf("block %d op %d WriteStateDomainChange: %v", blockNum, i, err)
		}

		// 3. Build the matching commitment update.
		key := rawdb.StateAccountLatestCommitmentKey(op.owner)
		if op.nextVal != nil {
			updates = append(updates, rawdb.NewStateCommitmentPut(key, op.nextVal))
		} else {
			updates = append(updates, rawdb.NewStateCommitmentDelete(key))
		}
	}

	root, err := store.Update(rawdb.CoalesceStateCommitmentUpdates(updates))
	if err != nil {
		t.Fatalf("block %d store.Update: %v", blockNum, err)
	}
	return root
}

// snapshotBranches captures the full commitment branch keyspace as a
// map[logicalPrefix → encodedBranchData], copying all bytes.
func snapshotBranches(t *testing.T, db CommitmentDB) map[string][]byte {
	t.Helper()
	snap := make(map[string][]byte)
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		snap[string(prefix)] = append([]byte(nil), encoded...)
		return true, nil
	}); err != nil {
		t.Fatalf("snapshotBranches: %v", err)
	}
	return snap
}

// diffBranches returns a human-readable diff between two branch snapshots,
// or "" when they are byte-identical.
func diffBranches(want, got map[string][]byte) string {
	allKeys := make(map[string]struct{})
	for k := range want {
		allKeys[k] = struct{}{}
	}
	for k := range got {
		allKeys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(allKeys))
	for k := range allKeys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var buf bytes.Buffer
	for _, k := range sorted {
		wv, winWant := want[k]
		gv, inGot := got[k]
		if winWant && inGot && bytes.Equal(wv, gv) {
			continue
		}
		if winWant && !inGot {
			fmt.Fprintf(&buf, "- prefix %x: %x (missing after unwind)\n", []byte(k), wv)
		} else if !winWant && inGot {
			fmt.Fprintf(&buf, "+ prefix %x: %x (spurious after unwind)\n", []byte(k), gv)
		} else {
			fmt.Fprintf(&buf, "~ prefix %x: want %x, got %x\n", []byte(k), wv, gv)
		}
	}
	return buf.String()
}

// seedEarlyBlocks seeds blocks 1..3 (M=3) for the 32-account set into db/store
// and returns the root at block M.  It is called identically from the main phase
// and the from-scratch replay in assertion 3, so the two databases always reflect
// the same state at M.
//
// Accounts seeded: owners {0x41, byte(i)} for i = 1..32, value "acct-<i>-v1".
//   - Block 1: create accounts 1..32 (all 32 at once).
//   - Block 2: update accounts 1..5 to v2 (shifts some first-nibble slots).
//   - Block 3 (=M): update accounts 1..5 to v3; accounts 6..32 unchanged from v1.
//
// The currentVal map is updated in place so callers can continue emitting
// accurate prevVal fields for post-M ops.
func seedEarlyBlocks(t *testing.T, db CommitmentDB, store LatestCommitmentStore, currentVal map[common.Address][]byte) common.Hash {
	t.Helper()

	// Block 1: create all 32 accounts.
	var block1Ops []acctOp
	for i := 1; i <= 32; i++ {
		owner := common.Address{0x41, byte(i)}
		val := []byte(fmt.Sprintf("acct-%02d-v1", i))
		block1Ops = append(block1Ops, acctOp{owner: owner, prevVal: nil, nextVal: val})
		currentVal[owner] = val
	}
	applyAccountBlock(t, db, store, 1, block1Ops)

	// Block 2: overwrite accounts 1..5 to v2.
	var block2Ops []acctOp
	for i := 1; i <= 5; i++ {
		owner := common.Address{0x41, byte(i)}
		prev := currentVal[owner]
		val := []byte(fmt.Sprintf("acct-%02d-v2", i))
		block2Ops = append(block2Ops, acctOp{owner: owner, prevVal: prev, nextVal: val})
		currentVal[owner] = val
	}
	applyAccountBlock(t, db, store, 2, block2Ops)

	// Block 3 (=M): overwrite accounts 1..5 to v3.
	var block3Ops []acctOp
	for i := 1; i <= 5; i++ {
		owner := common.Address{0x41, byte(i)}
		prev := currentVal[owner]
		val := []byte(fmt.Sprintf("acct-%02d-v3", i))
		block3Ops = append(block3Ops, acctOp{owner: owner, prevVal: prev, nextVal: val})
		currentVal[owner] = val
	}
	return applyAccountBlock(t, db, store, 3, block3Ops)
}

// TestUnwindCommitmentMatchesFromScratchRebuild is the property oracle for the
// inverse-delta unwind primitive.  It builds blocks 1..M (M=3) with 32 accounts,
// captures the branch keyspace at M, then applies K=3 more blocks with a change
// mix that exercises all three meaningful invariant cases:
//
//	(a) Keys written at ≤M and overwritten again after M → unwind must restore
//	    earliest Prev (the value at end of M), not a later intermediate value.
//	    Accounts {0x41,1..5}: overwritten in blocks 4 and 5.
//
//	(b) Keys CREATED after M (prevVal=nil) → unwind must delete them; their
//	    branch rows must be entirely removed.
//	    Accounts {0x42,1..5}: created in blocks 4 and 5.
//
//	(c) Keys DELETED after M that existed at ≤M → unwind must restore them; their
//	    branch rows must be re-created.
//	    Accounts {0x41,28..32}: deleted in blocks 5 and 6.
//
//	Untouched: accounts {0x41,10..15} are never touched after M; their rows must
//	survive the unwind unchanged.
//
// Three assertions are verified:
//  1. Root: UnwindCommitment returns rootM.
//  2. Branch byte-identity: branch keyspace is byte-for-byte equal to branchesM.
//     (If this fails we STOP — that is a real inverse-fold-vs-forward-topology
//     finding; do NOT weaken this assertion.)
//  3. Forward-commit equivalence: applying one more block to the rewound store
//     yields the same root as applying the same block to a from-scratch store
//     built only to M.
func TestUnwindCommitmentMatchesFromScratchRebuild(t *testing.T) {
	const M = 3
	const K = 3

	// ------- Phase 1: build blocks 1..M -------
	db := rawdb.NewMemoryDatabase()
	store := newStagedCommitmentStore(db)
	currentVal := make(map[common.Address][]byte) // tracks latest persisted value per address

	rootM := seedEarlyBlocks(t, db, store, currentVal)
	if rootM == (common.Hash{}) {
		t.Fatalf("block M root is zero")
	}
	branchesM := snapshotBranches(t, db)
	if len(branchesM) == 0 {
		t.Fatalf("no branches at block M — trie not built")
	}
	// Guard: the trie must have intermediate branch rows so the test exercises
	// the branch-create/collapse path.  With 32 keccak-spread keys over 16
	// possible first nibbles, pigeonhole guarantees first-nibble collisions,
	// producing depth-1 intermediate rows.  If this fires, the seeding or
	// hashing function changed in a way that nullifies the test.
	if len(branchesM) <= 1 {
		t.Fatalf("trie has no intermediate branch rows (branches@M=%d); test would not exercise inverse-fold branch create/collapse", len(branchesM))
	}
	t.Logf("rootM=%x  branches@M=%d", rootM, len(branchesM))

	// ------- Phase 2: apply blocks M+1..M+K -------
	// Block 4 (M+1):
	//   case (a): overwrite accounts 1..3 (earliest-Prev-wins test).
	//   case (b): create new accounts {0x42,1..3}.
	var block4Ops []acctOp
	for i := 1; i <= 3; i++ {
		owner := common.Address{0x41, byte(i)}
		prev := currentVal[owner]
		val := []byte(fmt.Sprintf("acct-%02d-v4", i))
		block4Ops = append(block4Ops, acctOp{owner: owner, prevVal: prev, nextVal: val})
		currentVal[owner] = val
	}
	for i := 1; i <= 3; i++ {
		owner := common.Address{0x42, byte(i)}
		val := []byte(fmt.Sprintf("new-%02d-v1", i))
		block4Ops = append(block4Ops, acctOp{owner: owner, prevVal: nil, nextVal: val})
		currentVal[owner] = val
	}
	applyAccountBlock(t, db, store, 4, block4Ops)

	// Block 5 (M+2):
	//   case (a): overwrite accounts 1..3 again (second overwrite; tests that unwind
	//             restores v3, not v4) + overwrite accounts 4..5 (first overwrite after M).
	//   case (b): create new accounts {0x42,4..5}.
	//   case (c): delete accounts 28..30.
	var block5Ops []acctOp
	for i := 1; i <= 5; i++ {
		owner := common.Address{0x41, byte(i)}
		prev := currentVal[owner]
		val := []byte(fmt.Sprintf("acct-%02d-v5", i))
		block5Ops = append(block5Ops, acctOp{owner: owner, prevVal: prev, nextVal: val})
		currentVal[owner] = val
	}
	for i := 4; i <= 5; i++ {
		owner := common.Address{0x42, byte(i)}
		val := []byte(fmt.Sprintf("new-%02d-v1", i))
		block5Ops = append(block5Ops, acctOp{owner: owner, prevVal: nil, nextVal: val})
		currentVal[owner] = val
	}
	for i := 28; i <= 30; i++ {
		owner := common.Address{0x41, byte(i)}
		prev := currentVal[owner]
		block5Ops = append(block5Ops, acctOp{owner: owner, prevVal: prev, nextVal: nil})
		delete(currentVal, owner)
	}
	applyAccountBlock(t, db, store, 5, block5Ops)

	// Block 6 (M+3):
	//   case (a): touch account 1 one more time (deepens the earliest-Prev chain).
	//   case (b): update one of the new accounts {0x42,1} (add a second changeset row).
	//   case (c): delete accounts 31..32.
	var block6Ops []acctOp
	{
		owner := common.Address{0x41, 1}
		prev := currentVal[owner]
		val := []byte("acct-01-v6")
		block6Ops = append(block6Ops, acctOp{owner: owner, prevVal: prev, nextVal: val})
		currentVal[owner] = val
	}
	{
		owner := common.Address{0x42, 1}
		prev := currentVal[owner]
		val := []byte("new-01-v2")
		block6Ops = append(block6Ops, acctOp{owner: owner, prevVal: prev, nextVal: val})
		currentVal[owner] = val
	}
	for i := 31; i <= 32; i++ {
		owner := common.Address{0x41, byte(i)}
		prev := currentVal[owner]
		block6Ops = append(block6Ops, acctOp{owner: owner, prevVal: prev, nextVal: nil})
		delete(currentVal, owner)
	}
	applyAccountBlock(t, db, store, 6, block6Ops)

	// ------- Phase 3: unwind back to M -------
	got, err := UnwindCommitment(db, store, uint64(M+K), uint64(M), rootM)
	if err != nil {
		t.Fatalf("UnwindCommitment: %v", err)
	}

	// Assertion 1: root equals rootM.
	if got != rootM {
		t.Fatalf("assertion 1 (root): got %x, want %x", got, rootM)
	}
	t.Logf("assertion 1 PASS: root=%x", got)

	// Assertion 2: branch keyspace is byte-for-byte identical to branchesM.
	branchesAfter := snapshotBranches(t, db)
	if diff := diffBranches(branchesM, branchesAfter); diff != "" {
		// This is a REAL finding (inverse-delta fold ≠ forward-fold topology).
		// Do NOT weaken; STOP and report.
		t.Fatalf("assertion 2 (branch byte-identity) FAILED — inverse-fold topology diverges:\n%s", diff)
	}
	t.Logf("assertion 2 PASS: %d branch rows byte-identical", len(branchesAfter))

	// Assertion 3: forward-commit equivalence.
	// Build a from-scratch store seeded only to M (replay blocks 1..M in a
	// separate DB with the same ops but no post-M changeset rows).
	freshDB := rawdb.NewMemoryDatabase()
	freshStore := newStagedCommitmentStore(freshDB)
	freshVal := make(map[common.Address][]byte)
	freshRootM := seedEarlyBlocks(t, freshDB, freshStore, freshVal)
	if freshRootM != rootM {
		t.Fatalf("fresh replay root %x != original rootM %x — test setup error", freshRootM, rootM)
	}

	// Apply one more identical block (block M+K+1 = 7) to BOTH stores.
	// Use accounts from case-(a) set at their state-at-M values and a couple of
	// the untouched accounts, so both stores must start from an identical leaf set.
	nextOps := []acctOp{
		// accounts 1..2 are in case-(a); their value at M is v3.
		{owner: common.Address{0x41, 1}, prevVal: []byte("acct-01-v3"), nextVal: []byte("acct-01-v7")},
		{owner: common.Address{0x41, 2}, prevVal: []byte("acct-02-v3"), nextVal: []byte("acct-02-v7")},
		// account 10 is untouched; its value is acct-10-v1.
		{owner: common.Address{0x41, 10}, prevVal: []byte("acct-10-v1"), nextVal: []byte("acct-10-v7")},
	}
	nextBlockNum := uint64(M + K + 1)
	r1 := applyAccountBlock(t, db, store, nextBlockNum, nextOps)
	r2 := applyAccountBlock(t, freshDB, freshStore, nextBlockNum, nextOps)
	if r1 != r2 {
		t.Fatalf("assertion 3 (forward-commit equivalence): rewound store root %x != fresh store root %x", r1, r2)
	}
	t.Logf("assertion 3 PASS: forward root=%x", r1)
}
