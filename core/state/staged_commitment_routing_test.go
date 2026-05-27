package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestStagedCommitmentGateRoutesCommitToStagedEngine proves the StagedCommitment
// config gate selects the Erigon-style staged engine on the live commit path
// (StateDB.Commit -> applyLatestDomainCommitment). With the gate on, branch rows
// appear under the staged keyspace and no legacy binary-radix tree/node rows are
// written. With the gate off, the opposite holds (legacy nodes, no branch rows).
func TestStagedCommitmentGateRoutesCommitToStagedEngine(t *testing.T) {
	commitAddr := func(staged bool) (branchRows, legacyRows int) {
		diskdb := ethrawdb.NewMemoryDatabase()
		db := NewDatabaseWithConfig(diskdb, DatabaseConfig{StagedCommitment: staged})
		sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
		if err != nil {
			t.Fatalf("New(staged=%v): %v", staged, err)
		}
		addr := testAddr(0x21)
		sdb.CreateAccount(addr, corepb.AccountType_Normal)
		sdb.AddBalance(addr, 999)
		if _, err := sdb.Commit(); err != nil {
			t.Fatalf("commit(staged=%v): %v", staged, err)
		}

		index := sdb.accountKVIndex()
		if err := rawdb.IterateCommitmentBranches(index, func(_, _ []byte) (bool, error) {
			branchRows++
			return true, nil
		}); err != nil {
			t.Fatalf("iterate branches: %v", err)
		}
		if err := rawdb.IterateStateCommitmentDomain(index, rawdb.LatestDomainCommitmentNodeLogicalPrefix(), func(_, _ []byte) (bool, error) {
			legacyRows++
			return true, nil
		}); err != nil {
			t.Fatalf("iterate legacy nodes: %v", err)
		}
		return branchRows, legacyRows
	}

	// Gate ON: staged branch rows, zero legacy nodes.
	stagedBranch, stagedLegacy := commitAddr(true)
	if stagedBranch == 0 {
		t.Errorf("staged gate: no branch rows written under staged keyspace; gate did not route to staged engine")
	}
	if stagedLegacy != 0 {
		t.Errorf("staged gate: %d legacy tree/node rows written; staged engine must not touch the legacy keyspace", stagedLegacy)
	}

	// Gate OFF: legacy nodes, zero staged branch rows. Byte-identical to today.
	legacyBranch, legacyLegacy := commitAddr(false)
	if legacyLegacy == 0 {
		t.Errorf("legacy gate: no legacy tree/node rows written; default path changed")
	}
	if legacyBranch != 0 {
		t.Errorf("legacy gate: %d staged branch rows written; legacy path must not touch the staged keyspace", legacyBranch)
	}
}
