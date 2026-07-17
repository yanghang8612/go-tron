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
// The rawdb reader is the legacy/stateless entry point. Production actuators and
// block processing should use PassVersionFromStore with StateDB so the bitmap is
// read from the rooted SystemForkVote domain.
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

// PassVersionFromStoreWithRate evaluates a wire version with a chain-specific
// quorum rate. Nile assigned different releases to wire versions 33 and 34
// before those enum values were finalised on mainnet, so their rates are also
// reversed (v33=80%, v34=70%). The stored bitmap remains keyed by the raw wire
// version; only the quorum parameter is overridden.
func PassVersionFromStoreWithRate(store ForkStatsReader, version int32, latestBlockTime, maintenanceIntervalMs int64, hardForkRate int) bool {
	vp, ok := lookupVersion(version)
	if !ok {
		return false
	}
	vp.HardForkRate = hardForkRate
	stats := store.ReadForkStats(version)
	return passFromStats(stats, vp, latestBlockTime, maintenanceIntervalMs)
}
