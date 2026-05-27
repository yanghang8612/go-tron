package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// TestStagedCommitmentConfigFlag pins the inert StagedCommitment config gate:
// it is readable from a StateDB but defaults to false and (for now) changes no
// behavior. A later task branches the commitment apply path on it.
func TestStagedCommitmentConfigFlag(t *testing.T) {
	emptyRoot := tcommon.Hash(ethtypes.EmptyRootHash)

	// Default config: staged commitment off.
	defDB := NewDatabase(ethrawdb.NewMemoryDatabase())
	defSDB, err := New(emptyRoot, defDB)
	if err != nil {
		t.Fatalf("New(default): %v", err)
	}
	if defDB.StagedCommitment() {
		t.Error("default Database.StagedCommitment() = true, want false")
	}
	if defSDB.stagedCommitment() {
		t.Error("default StateDB.stagedCommitment() = true, want false")
	}

	// Explicit StagedCommitment: true.
	onDB := NewDatabaseWithConfig(ethrawdb.NewMemoryDatabase(), DatabaseConfig{StagedCommitment: true})
	onSDB, err := New(emptyRoot, onDB)
	if err != nil {
		t.Fatalf("New(staged): %v", err)
	}
	if !onDB.StagedCommitment() {
		t.Error("Database.StagedCommitment() = false, want true")
	}
	if !onSDB.stagedCommitment() {
		t.Error("StateDB.stagedCommitment() = false, want true")
	}
}
