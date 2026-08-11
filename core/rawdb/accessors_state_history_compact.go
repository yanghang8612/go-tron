package rawdb

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
