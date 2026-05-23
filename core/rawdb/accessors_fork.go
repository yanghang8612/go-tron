package rawdb

import "github.com/ethereum/go-ethereum/ethdb"

// WriteForkStats persists the per-witness vote bitmap for a block version.
// stats[i] == 0x01 means the SR in slot i declared support for `version`
// in its last produced block; 0x00 means downgrade/unknown. Length should
// equal the active witness count.
func WriteForkStats(db ethdb.KeyValueWriter, version int32, stats []byte) {
	db.Put(forkStatsKey(version), stats)
}

// ReadForkStats returns the vote bitmap for `version`, or nil when no
// entry has been written yet. Callers should treat nil as "no votes
// counted" — do not conflate it with a zero-length valid bitmap.
func ReadForkStats(db ethdb.KeyValueReader, version int32) []byte {
	data, err := db.Get(forkStatsKey(version))
	if err != nil {
		return nil
	}
	return data
}

// ForkStatsStateKey returns the legacy logical key used when fork vote
// bitmaps are stored under the rooted SystemForkVote domain.
func ForkStatsStateKey(version int32) []byte {
	return forkStatsKey(version)
}
