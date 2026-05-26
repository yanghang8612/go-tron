package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

// StateRootAdapter is the boundary between go-tron's internal state root and
// java-tron's wire-compatible accountStateRoot.
//
// The internal root is produced by StateDB.Commit/CommitWithStatsOptions and is
// persisted out-of-band through rawdb.WriteBlockStateRoot. The block header's
// accountStateRoot is java-tron's lightweight AccountStateCallBack root and
// must only be computed or validated through this adapter.
type StateRootAdapter struct{}

// JavaAccountStateRoot computes java-tron's lightweight block-header root from
// account-store writes recorded since journalMark.
func (StateRootAdapter) JavaAccountStateRoot(statedb *state.StateDB, parentRoot tcommon.Hash, journalMark int) (tcommon.Hash, error) {
	return statedb.JavaAccountStateRoot(parentRoot, journalMark)
}

// ValidateJavaAccountStateRoot checks a non-empty block-header accountStateRoot
// against the computed java-tron root.
func (StateRootAdapter) ValidateJavaAccountStateRoot(blockRoot, computedRoot tcommon.Hash) error {
	if blockRoot == (tcommon.Hash{}) {
		return nil
	}
	if blockRoot != computedRoot {
		return fmt.Errorf("state root mismatch: block=%x computed=%x", blockRoot, computedRoot)
	}
	return nil
}

var defaultStateRootAdapter StateRootAdapter
