package forks

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// PassVersion is a stateless variant of ForkController.Pass for callers
// that hold a KV reader (e.g. actuators via ctx.DB) but not a
// *ForkController. Same activation rules: hardForkTime ceil-aligned to
// the maintenance interval AND >= ceil(rate% * length) slots voting
// VoteUpgrade for versions > Version4_0; legacy versions require every
// slot upgraded.
//
// The bitmap is read directly from rawdb under the same `fv-` prefix
// ForkController writes through; concurrent ForkController.Update
// callers serialise via Pebble's per-key consistency, so this read does
// not need the controller's mutex.
func PassVersion(db ethdb.KeyValueReader, version int32, latestBlockTime, maintenanceIntervalMs int64) bool {
	return PassVersionFromStore(rawDBForkStatsReader{db: db}, version, latestBlockTime, maintenanceIntervalMs)
}

type rawDBForkStatsReader struct {
	db ethdb.KeyValueReader
}

func (s rawDBForkStatsReader) ReadForkStats(version int32) []byte {
	return rawdb.ReadForkStats(s.db, version)
}

// PassVersionFromStore is the typed-store variant of PassVersion.
func PassVersionFromStore(store ForkStatsReader, version int32, latestBlockTime, maintenanceIntervalMs int64) bool {
	vp, ok := lookupVersion(version)
	if !ok {
		return false
	}
	stats := store.ReadForkStats(version)
	return passFromStats(stats, vp, latestBlockTime, maintenanceIntervalMs)
}
