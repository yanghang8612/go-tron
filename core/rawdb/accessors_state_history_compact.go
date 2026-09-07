package rawdb

import "fmt"

// StateHistoryBlockRangeBounds returns the half-open changeset key range for
// inclusive block bounds. It does not include the posting or directory index.
func StateHistoryBlockRangeBounds(fromBlock, throughBlock uint64) ([]byte, []byte, error) {
	if fromBlock > throughBlock {
		return nil, nil, fmt.Errorf("state history: first block %d exceeds last block %d", fromBlock, throughBlock)
	}
	start := stateChangeSetKey(fromBlock, 0)
	if throughBlock == ^uint64(0) {
		return start, prefixUpperBound(stateChangeSetPrefix), nil
	}
	return start, stateChangeSetKey(throughBlock+1, 0), nil
}

// StateHistoryKeyspaceBounds returns the half-open key ranges occupied by the
// hot state changesets. Offline maintenance
// tools use these bounds to compact point tombstones left by the live pruner
// without rewriting unrelated latest-state or commitment keyspaces.
func StateHistoryKeyspaceBounds() (changeSetStart, changeSetLimit []byte) {
	return append([]byte(nil), stateChangeSetPrefix...), prefixUpperBound(stateChangeSetPrefix)
}

func StateHistoryPostingKeyspaceBounds() (postingStart, postingLimit, directoryStart, directoryLimit []byte) {
	return append([]byte(nil), stateChangePostingPrefix...), prefixUpperBound(stateChangePostingPrefix),
		append([]byte(nil), stateChangeKeyDirectoryPrefix...), prefixUpperBound(stateChangeKeyDirectoryPrefix)
}
