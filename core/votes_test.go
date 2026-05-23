package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// seedPendingVotesAtGenesis stages pending VotesStore records into the rooted
// WitnessVoteState KV at the chain's genesis state root and re-anchors the
// genesis root pointer to the recommitted root. The pending-vote ledger is
// rooted (it rewinds with the full state root), so a test that needs the
// maintenance drain — or the ListWitnesses JSON-RPC read — to observe a seeded
// vote must put it in the head root, not the flat store. This must be called
// while head is still the genesis block (no blocks inserted yet), which every
// caller satisfies. Genesis block headers carry no state root (java-tron
// parity), so rewriting the pointer is sufficient; applyBlock(parentRoot),
// switchFork(lcaRoot), and HeadStateRoot all read it live.
func seedPendingVotesAtGenesis(t *testing.T, bc *BlockChain, records map[tcommon.Address]*corepb.Votes) {
	t.Helper()
	root := bc.HeadStateRoot()
	sdb, err := state.New(root, bc.stateDB)
	if err != nil {
		t.Fatalf("open statedb at genesis root: %v", err)
	}
	for voter, votes := range records {
		if err := sdb.WriteVotes(voter, votes); err != nil {
			t.Fatalf("seed pending votes for %s: %v", voter.Hex(), err)
		}
	}
	newRoot, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit seeded votes: %v", err)
	}
	rawdb.WriteGenesisStateRoot(bc.db, newRoot)
	if bc.genesisBlock != nil {
		rawdb.WriteBlockStateRoot(bc.db, bc.genesisBlock.Hash(), newRoot)
	}
}

// TestApplyPendingVotes_SameBlockVisibility proves the rooting's central
// invariant: a pending vote written into the WitnessVoteState KV earlier in a
// block (as an actuator / TVM opVote does, via the block's *StateDB) is visible
// to the maintenance drain that runs later in the SAME block, because both hold
// the same statedb overlay. Pre-rooting this coupling was implicit (both hit the
// flat buffer); post-rooting it must hold through the shared trie overlay before
// any Commit. The drain folds the net delta into the witness vote count and
// clears the store.
func TestApplyPendingVotes_SameBlockVisibility(t *testing.T) {
	db := state.NewDatabase(ethrawdb.NewMemoryDatabase())
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	voter := testCoreAddr(1)
	witness := testCoreAddr(2)
	statedb.PutWitness(witness, "http://w") // VoteCount starts at 0.

	// Stage the voter's index into the witness enumeration the drain folds into
	// (mirrors a registered witness). gatherWitnessVotes is not used by the drain
	// directly, but AddWitnessVoteCount must land on a live capsule.
	if got := statedb.GetWitness(witness).VoteCount(); got != 0 {
		t.Fatalf("witness should start at 0 votes, got %d", got)
	}

	// Actuator/TVM write surface: a pending +30 vote, staged on the SAME statedb,
	// with NO intervening Commit.
	if err := statedb.WriteVotes(voter, &corepb.Votes{
		Address:  voter.Bytes(),
		NewVotes: []*corepb.Vote{{VoteAddress: witness.Bytes(), VoteCount: 30}},
	}); err != nil {
		t.Fatal(err)
	}

	// The maintenance drain runs later in the same block on the same statedb.
	if applied := applyPendingVotes(statedb); !applied {
		t.Fatal("drain reported no records — same-block write was not visible to the drain")
	}

	// The delta folded into the witness, and the pending store was cleared.
	if got := statedb.GetWitness(witness).VoteCount(); got != 30 {
		t.Fatalf("witness vote count after drain: got %d, want 30 (same-block vote not settled)", got)
	}
	if got := statedb.ReadVotes(voter); got != nil {
		t.Fatalf("pending vote not cleared by drain: %+v", got)
	}
	if idx := statedb.ReadVotesIndex(); len(idx) != 0 {
		t.Fatalf("voter index not cleared by drain: %v", idx)
	}
}
