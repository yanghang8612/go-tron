package pebbledb

import (
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
)

// engineSpaceMetrics publishes the space accounting already collected by the
// meter's pebble.Metrics call. It performs no DB calls, file stats or key scans.
// These are engine estimates/bookkeeping, not filesystem allocated bytes or
// immediately reclaimable space. Snapshots retain logical versions; zombie
// tables/memtables instead remain referenced by iterators (and a memtable may
// also be waiting for recycling).
// The current SST sum follows level metrics, including virtual tables when
// enabled; it is not a physical directory inventory. Obsolete WAL statistics
// include files retained by Pebble's WAL recycler. Metadata covers only the
// OPTIONS/MANIFEST sizes included by Pebble's own DiskSpaceUsage formula.
type engineSpaceMetrics struct {
	sampledAt                 *metrics.Gauge
	diskBytes                 *metrics.Gauge
	sstCurrentFiles           *metrics.Gauge
	sstCurrentBytes           *metrics.Gauge
	sstObsoleteFiles          *metrics.Gauge
	sstObsoleteBytes          *metrics.Gauge
	sstZombieFiles            *metrics.Gauge
	sstZombieBytes            *metrics.Gauge
	walLiveFiles              *metrics.Gauge
	walLiveLogicalBytes       *metrics.Gauge
	walLivePhysicalBytes      *metrics.Gauge
	walObsoleteFiles          *metrics.Gauge
	walObsoletePhysicalBytes  *metrics.Gauge
	snapshotCount             *metrics.Gauge
	snapshotEarliestSeqNum    *metrics.Gauge
	snapshotPinnedKeysTotal   *metrics.Gauge
	snapshotPinnedBytesTotal  *metrics.Gauge
	tombstonesEstimate        *metrics.Gauge
	memtableLiveBytes         *metrics.Gauge
	memtableZombieBytes       *metrics.Gauge
	blockCacheBytes           *metrics.Gauge
	blockCacheClock           engineCacheClockMetrics
	blockCacheCapacityBytes   *metrics.Gauge
	compactionInProgressBytes *metrics.Gauge
	optionsManifestBytes      *metrics.Gauge
}

func newEngineSpaceMetrics(namespace string) *engineSpaceMetrics {
	prefix := namespace + "storage/engine/"
	gauge := func(name string) *metrics.Gauge {
		return metrics.GetOrRegisterGauge(prefix+name, nil)
	}
	return &engineSpaceMetrics{
		sampledAt:                 gauge("sampled_at"),
		diskBytes:                 gauge("disk/bytes"),
		sstCurrentFiles:           gauge("sst/current/files"),
		sstCurrentBytes:           gauge("sst/current/bytes"),
		sstObsoleteFiles:          gauge("sst/obsolete/files"),
		sstObsoleteBytes:          gauge("sst/obsolete/bytes"),
		sstZombieFiles:            gauge("sst/zombie/files"),
		sstZombieBytes:            gauge("sst/zombie/bytes"),
		walLiveFiles:              gauge("wal/live/files"),
		walLiveLogicalBytes:       gauge("wal/live/logical_bytes"),
		walLivePhysicalBytes:      gauge("wal/live/physical_bytes"),
		walObsoleteFiles:          gauge("wal/obsolete/files"),
		walObsoletePhysicalBytes:  gauge("wal/obsolete/physical_bytes"),
		snapshotCount:             gauge("snapshots/count"),
		snapshotEarliestSeqNum:    gauge("snapshots/earliest_sequence_number"),
		snapshotPinnedKeysTotal:   gauge("snapshots/pinned_keys/total"),
		snapshotPinnedBytesTotal:  gauge("snapshots/pinned_bytes/total"),
		tombstonesEstimate:        gauge("keys/tombstones/estimated_count"),
		memtableLiveBytes:         gauge("memtable/live/bytes"),
		memtableZombieBytes:       gauge("memtable/zombie/bytes"),
		blockCacheBytes:           gauge("block_cache/bytes"),
		blockCacheClock:           newEngineCacheClockMetrics(gauge),
		blockCacheCapacityBytes:   gauge("block_cache/configured_capacity_bytes"),
		compactionInProgressBytes: gauge("compaction/in_progress/bytes"),
		optionsManifestBytes:      gauge("metadata/options_manifest/bytes"),
	}
}

func (m *engineSpaceMetrics) update(stats *pebble.Metrics) {
	if m == nil || stats == nil {
		return
	}
	var currentFiles int64
	var currentBytes uint64
	for _, level := range stats.Levels {
		currentFiles += level.NumFiles
		currentBytes += uint64(level.Size)
	}
	m.sstCurrentFiles.Update(currentFiles)
	m.sstCurrentBytes.Update(int64(currentBytes))
	m.sstObsoleteFiles.Update(stats.Table.ObsoleteCount)
	m.sstObsoleteBytes.Update(int64(stats.Table.ObsoleteSize))
	m.sstZombieFiles.Update(stats.Table.ZombieCount)
	m.sstZombieBytes.Update(int64(stats.Table.ZombieSize))
	m.walLiveFiles.Update(stats.WAL.Files)
	m.walLiveLogicalBytes.Update(int64(stats.WAL.Size))
	m.walLivePhysicalBytes.Update(int64(stats.WAL.PhysicalSize))
	m.walObsoleteFiles.Update(stats.WAL.ObsoleteFiles)
	m.walObsoletePhysicalBytes.Update(int64(stats.WAL.ObsoletePhysicalSize))
	m.snapshotCount.Update(int64(stats.Snapshots.Count))
	m.snapshotEarliestSeqNum.Update(int64(stats.Snapshots.EarliestSeqNum))
	// PinnedKeys/PinnedSize are cumulative since DB open, counting entries
	// written by flush/compaction that snapshots prevented from being elided.
	// They may count an entry repeatedly and do not drop when snapshots close.
	// Set the cumulative reading rather than adding it again each meter tick.
	m.snapshotPinnedKeysTotal.Update(int64(stats.Snapshots.PinnedKeys))
	m.snapshotPinnedBytesTotal.Update(int64(stats.Snapshots.PinnedSize))
	m.tombstonesEstimate.Update(int64(stats.Keys.TombstoneCount))
	m.memtableLiveBytes.Update(int64(stats.MemTable.Size))
	m.memtableZombieBytes.Update(int64(stats.MemTable.ZombieSize))
	// Size is Pebble's currently cached block payload, not the configured
	// capacity or the entire native allocation footprint (e.g. cache metadata).
	m.blockCacheBytes.Update(stats.BlockCache.Size)
	m.blockCacheClock.update(stats.BlockCache)
	m.compactionInProgressBytes.Update(stats.Compact.InProgressBytes)

	// Pebble v1.1.5 keeps OPTIONS/MANIFEST sizes private. Its DiskSpaceUsage
	// formula adds exactly these two sizes to the public terms below, so the
	// same immutable Metrics value gives their combined residual without
	// reflection, another DB.Metrics call, or filesystem I/O. Keep this formula
	// aligned with Pebble when upgrading the dependency. Logical WAL bytes,
	// memtables and cumulative snapshot pinned bytes are NOT additive disk
	// components. WAL recycling can make live physical bytes exceed logical
	// bytes; Pebble exposes no obsolete-WAL logical-size metric.
	diskBytes := stats.DiskSpaceUsage()
	publicBytes := currentBytes + stats.WAL.PhysicalSize + stats.WAL.ObsoletePhysicalSize +
		stats.Table.ObsoleteSize + stats.Table.ZombieSize + uint64(stats.Compact.InProgressBytes)
	m.diskBytes.Update(int64(diskBytes))
	m.optionsManifestBytes.Update(int64(diskBytes - publicBytes))
	// Mark completion of this publication pass; separate registry gauges are
	// still not an atomic snapshot for concurrent readers.
	m.sampledAt.Update(time.Now().Unix())
}

// engineCacheClockMetrics publishes the CLOCK-Pro snapshot already included in
// the engine sample. These gauges are non-atomic across cache shards and registry
// publications. Test bytes describe ghost bookkeeping, not resident payload;
// cold target bytes describe the replacement target, not occupied bytes.
type engineCacheClockMetrics struct {
	shards, hotEntries, coldEntries, testEntries    *metrics.Gauge
	hotBytes, coldBytes, testBytes, coldTargetBytes *metrics.Gauge
	coldCountMin, coldCountMax                      *metrics.Gauge
	coldCountBuckets                                [6]*metrics.Gauge
}

func newEngineCacheClockMetrics(gauge func(string) *metrics.Gauge) engineCacheClockMetrics {
	const prefix = "block_cache/clock/"
	m := engineCacheClockMetrics{
		shards:          gauge(prefix + "shards"),
		hotEntries:      gauge(prefix + "hot/entries"),
		coldEntries:     gauge(prefix + "cold/entries"),
		testEntries:     gauge(prefix + "test/entries"),
		hotBytes:        gauge(prefix + "hot/bytes"),
		coldBytes:       gauge(prefix + "cold/bytes"),
		testBytes:       gauge(prefix + "test/ghost_bytes"),
		coldTargetBytes: gauge(prefix + "cold_target_bytes"),
		coldCountMin:    gauge(prefix + "cold_count/min"),
		coldCountMax:    gauge(prefix + "cold_count/max"),
	}
	for i, name := range [6]string{"0", "1", "2_7", "8_63", "64_511", "512_plus"} {
		m.coldCountBuckets[i] = gauge(prefix + "cold_count/shards_" + name)
	}
	return m
}

func (m *engineCacheClockMetrics) update(stats pebble.CacheMetrics) {
	m.shards.Update(stats.Clock.Shards)
	m.hotEntries.Update(stats.Clock.HotCount)
	m.coldEntries.Update(stats.Clock.ColdCount)
	m.testEntries.Update(stats.Clock.TestCount)
	m.hotBytes.Update(stats.Clock.HotBytes)
	m.coldBytes.Update(stats.Clock.ColdBytes)
	m.testBytes.Update(stats.Clock.TestBytes)
	m.coldTargetBytes.Update(stats.Clock.ColdTargetBytes)
	m.coldCountMin.Update(stats.Clock.ColdCountMin)
	m.coldCountMax.Update(stats.Clock.ColdCountMax)
	for i, value := range stats.Clock.ColdCountBuckets {
		m.coldCountBuckets[i].Update(value)
	}
}
