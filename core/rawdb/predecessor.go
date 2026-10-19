package rawdb

import "github.com/tronprotocol/go-tron/core/pointread"

// Preserve optional reverse seeks through the same wrappers that forward
// snapshots. Embedding ethdb interfaces alone hides backend capabilities.
func (db *ChainDB) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	return pointread.SeekBefore(db.KeyValueStore, prefix, upper)
}
func (db *ChainDBReadView) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	return pointread.SeekBefore(db.iteratee, prefix, upper)
}
func (db *stateHistorySnapshotDatabase) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	return pointread.SeekBefore(db.source, prefix, upper)
}
