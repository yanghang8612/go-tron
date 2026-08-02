package rawdb

// StateHistoryKeyspaceBounds returns the half-open key ranges occupied by the
// hot state changesets and their exact-key inverse index. Offline maintenance
// tools use these bounds to compact point tombstones left by the live pruner
// without rewriting unrelated latest-state or commitment keyspaces.
func StateHistoryKeyspaceBounds() (changeSetStart, changeSetLimit, changeIndexStart, changeIndexLimit []byte) {
	return append([]byte(nil), stateChangeSetPrefix...), prefixUpperBound(stateChangeSetPrefix),
		append([]byte(nil), stateChangeInversePrefix...), prefixUpperBound(stateChangeInversePrefix)
}
