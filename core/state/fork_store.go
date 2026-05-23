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

// WriteForkStats stages a software-version vote bitmap in the rooted
// SystemForkVote domain.
func (s *StateDB) WriteForkStats(version int32, stats []byte) {
	_ = s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemForkVote, rawdb.ForkStatsStateKey(version), stats)
}
