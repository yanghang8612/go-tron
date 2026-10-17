package pebbledb

import (
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

// storagePressureThresholds records the options used when opening Pebble. They
// are immutable once the Database is returned to its caller.
type storagePressureThresholds struct {
	l0Compaction int
	l0StopWrites int
	memTableStop int
}

// MaintenancePressure returns current engine pressure without scanning keys,
// reading SST data, or relying on the asynchronous metrics registry. The
// lifecycle read lease excludes Close while Pebble's scheduling fields are
// collected. Cache and table metrics are deliberately outside this path.
// Pebble's snapshot and the atomic stall counters can advance independently;
// this is a scheduling observation, not an atomic audit of all engine state.
func (d *Database) MaintenancePressure() maintenance.StoragePressure {
	if d == nil {
		return maintenance.StoragePressure{}
	}
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed || d.db == nil {
		return maintenance.StoragePressure{}
	}

	sampledAt := time.Now()
	stalledBefore := d.writeStalled.Load()
	stats := d.db.MaintenanceMetrics()
	return maintenance.StoragePressure{
		Available:                   true,
		SampledAt:                   sampledAt,
		WriteStalled:                stalledBefore || d.writeStalled.Load(),
		StallCount:                  uint64(d.writeDelayCount.Load()),
		StallDuration:               time.Duration(d.writeDelayTime.Load()),
		L0Sublevels:                 int(stats.L0Sublevels),
		L0CompactionThreshold:       d.pressureThresholds.l0Compaction,
		L0StopWritesThreshold:       d.pressureThresholds.l0StopWrites,
		MemTableCount:               stats.MemTableCount,
		MemTableStopWritesThreshold: d.pressureThresholds.memTableStop,
		CompactionDebt:              stats.CompactionDebt,
	}
}
