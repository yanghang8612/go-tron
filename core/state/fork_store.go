package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// ReadForkStats loads a software-version vote bitmap from the rooted
// SystemForkVote domain.
func (s *StateDB) ReadForkStats(version int32) []byte {
	data, ok, err := s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemForkVote, rawdb.ForkStatsStateKey(version))
	if err != nil || !ok {
		return nil
	}
	return data
}

// ReadForkStatsBatch loads multiple software-version vote bitmaps with a
// single account-KV trie open. ForkController.Update calls this once per block
// for all known versions, which avoids repeated SystemForkVote trie setup.
func (s *StateDB) ReadForkStatsBatch(versions []int32) map[int32][]byte {
	keys := make([][]byte, 0, len(versions))
	for _, version := range versions {
		keys = append(keys, rawdb.ForkStatsStateKey(version))
	}
	values, err := s.GetAccountKVBatch(tcommon.SystemAccountAddress, kvdomains.SystemForkVote, keys)
	if err != nil {
		return nil
	}
	out := make(map[int32][]byte, len(values))
	for i, version := range versions {
		if value, ok := values[string(keys[i])]; ok {
			out[version] = value
		}
	}
	return out
}

// WriteForkStats stages a software-version vote bitmap in the rooted
// SystemForkVote domain.
func (s *StateDB) WriteForkStats(version int32, stats []byte) {
	_ = s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemForkVote, rawdb.ForkStatsStateKey(version), stats)
}

// WriteForkStatsFinal stages a block-final fork vote bitmap without
// transaction-snapshot journaling.
func (s *StateDB) WriteForkStatsFinal(version int32, stats []byte) {
	_ = s.SetAccountKVFinal(tcommon.SystemAccountAddress, kvdomains.SystemForkVote, rawdb.ForkStatsStateKey(version), stats)
}
