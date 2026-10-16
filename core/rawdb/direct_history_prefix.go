package rawdb

// IsDirectHistoryPrefix reports whether the entire prefix range belongs to the
// history partition. A singleton stage key is a valid point key but cannot name
// a history-only prefix range because its extensions belong to the hot store.
func IsDirectHistoryPrefix(prefix []byte) bool {
	history, exclusive := directHistoryPrefixPartition(prefix)
	return history && exclusive
}
