package domains

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// UnwindCommitment rewinds the staged commitment engine + latest-domain tables
// from fromBlock to toBlock via the inverse-delta derived from persisted
// changesets (no full Rebuild scan), then verifies the re-folded root equals
// expectedRoot (toBlock's persisted internal commitment root; pass zero to skip).
// On mismatch it returns an error WITHOUT a Rebuild; the caller runs this inside
// a batch/buffer it can discard, so a mismatch never half-commits.
//
// db must satisfy CommitmentDB (Reader+Writer+Iteratee) — it is also passed to
// rawdb.CollectStateUnwind, which requires rawdb.StateUnwindStore (= StateKVLatestStore
// = Reader+Writer+Iteratee). The interface sets are identical, so no type
// assertion is needed.
func UnwindCommitment(db CommitmentDB, store LatestCommitmentStore, fromBlock, toBlock uint64, expectedRoot common.Hash) (common.Hash, error) {
	if store == nil {
		return common.Hash{}, ErrNilCommitmentStore
	}
	updates, err := rawdb.CollectStateUnwind(db, fromBlock, toBlock)
	if err != nil {
		return common.Hash{}, err
	}
	root, err := store.Update(updates)
	if err != nil {
		return common.Hash{}, err
	}
	if expectedRoot != (common.Hash{}) && root != expectedRoot {
		return common.Hash{}, fmt.Errorf("domains: commitment unwind %d->%d root mismatch: got %x want %x", fromBlock, toBlock, root, expectedRoot)
	}
	return root, nil
}
