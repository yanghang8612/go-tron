package pebbledb

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/metrics"
)

func TestEngineSpaceMetricsSnapshotTotalsSurviveRelease(t *testing.T) {
	db, err := pebble.Open("engine-space", &pebble.Options{
		FS: vfs.NewMem(), DisableAutomaticCompactions: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	oldValue := bytes.Repeat([]byte("old"), 64)
	if err := db.Set([]byte("key"), oldValue, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot := db.NewSnapshot()
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = snapshot.Close()
		}
	})
	if err := db.Set([]byte("key"), []byte("replacement"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte("a"), []byte("z"), false); err != nil {
		t.Fatal(err)
	}
	value, closer, err := snapshot.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	retained := bytes.Equal(value, oldValue)
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if !retained {
		t.Fatal("snapshot did not retain the overwritten value through compaction")
	}

	namespace := "test/engine-space-snapshot/"
	m := newEngineSpaceMetrics(namespace)
	held := db.Metrics()
	if held.Snapshots.Count != 1 || held.Snapshots.EarliestSeqNum == 0 || held.Snapshots.PinnedKeys == 0 || held.Snapshots.PinnedSize == 0 {
		t.Fatalf("real snapshot did not pin compacted versions: %+v", held.Snapshots)
	}
	m.update(held)
	m.update(held) // Sampling the same cumulative value must not double it.
	if got := m.snapshotPinnedKeysTotal.Snapshot().Value(); got != int64(held.Snapshots.PinnedKeys) {
		t.Fatalf("pinned keys = %d, want cumulative engine reading %d", got, held.Snapshots.PinnedKeys)
	}
	registered := metrics.DefaultRegistry.Get(namespace + "storage/engine/snapshots/pinned_bytes/total")
	if registered != m.snapshotPinnedBytesTotal || m.snapshotPinnedBytesTotal.Snapshot().Value() != int64(held.Snapshots.PinnedSize) {
		t.Fatal("snapshot byte metric must publish the engine tally under an explicit /total name")
	}
	if m.snapshotCount.Snapshot().Value() != 1 || m.snapshotEarliestSeqNum.Snapshot().Value() != int64(held.Snapshots.EarliestSeqNum) {
		t.Fatal("active snapshot count/earliest sequence did not reflect the engine sample")
	}
	if m.optionsManifestBytes.Snapshot().Value() <= 0 {
		t.Fatal("real DB disk accounting did not retain its OPTIONS/MANIFEST component")
	}
	assertEngineDiskComponents(t, m, held.DiskSpaceUsage())

	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if err := db.Compact([]byte("a"), []byte("z"), false); err != nil {
		t.Fatal(err)
	}
	released := db.Metrics()
	m.update(released)
	if m.snapshotCount.Snapshot().Value() != 0 || m.snapshotEarliestSeqNum.Snapshot().Value() != 0 {
		t.Fatal("released snapshot remained in active count/earliest sequence")
	}
	if released.Snapshots.PinnedKeys != held.Snapshots.PinnedKeys || released.Snapshots.PinnedSize != held.Snapshots.PinnedSize {
		t.Fatalf("snapshot release unexpectedly changed cumulative pinned tally: held=%+v released=%+v", held.Snapshots, released.Snapshots)
	}
	if m.snapshotPinnedBytesTotal.Snapshot().Value() != int64(held.Snapshots.PinnedSize) {
		t.Fatal("closing snapshots must not reset cumulative pinned bytes")
	}
	assertEngineDiskComponents(t, m, released.DiskSpaceUsage())
}

func TestEngineSpaceMetricsDiskComponentsExcludeLogicalAndCumulativeBytes(t *testing.T) {
	stats := &pebble.Metrics{}
	stats.Levels[0].Size, stats.Levels[4].Size = 101, 307
	stats.Levels[0].NumFiles, stats.Levels[4].NumFiles = 2, 5
	stats.WAL.Files, stats.WAL.ObsoleteFiles = 1, 2
	stats.WAL.Size, stats.WAL.PhysicalSize, stats.WAL.ObsoletePhysicalSize = 9, 64, 128
	stats.Table.ObsoleteSize, stats.Table.ZombieSize = 17, 23
	stats.Compact.InProgressBytes = 31
	stats.MemTable.Size, stats.MemTable.ZombieSize = 1<<20, 2<<20
	stats.Snapshots.PinnedSize = 8 << 20
	m := newEngineSpaceMetrics("test/engine-space-components/")
	m.update(stats)
	assertEngineDiskComponents(t, m, 671)
	if m.sstCurrentFiles.Snapshot().Value() != 7 || m.sstCurrentBytes.Snapshot().Value() != 408 {
		t.Fatal("current SST metrics must sum current levels only")
	}
	if m.walLiveLogicalBytes.Snapshot().Value() != 9 || m.walLivePhysicalBytes.Snapshot().Value() != 64 || m.walObsoletePhysicalBytes.Snapshot().Value() != 128 {
		t.Fatal("live logical/physical and obsolete WAL bytes were conflated")
	}
	if m.optionsManifestBytes.Snapshot().Value() != 0 {
		t.Fatal("zero-private-metadata fixture must not classify memtables or snapshot totals as disk metadata")
	}
	// A nil sample leaves the latest valid reading intact; update needs no DB.
	m.update(nil)
	(*engineSpaceMetrics)(nil).update(stats)
	if m.diskBytes.Snapshot().Value() != 671 {
		t.Fatal("nil sample replaced a valid engine reading")
	}
}

func assertEngineDiskComponents(t *testing.T, m *engineSpaceMetrics, want uint64) {
	t.Helper()
	components := m.sstCurrentBytes.Snapshot().Value() + m.sstObsoleteBytes.Snapshot().Value() +
		m.sstZombieBytes.Snapshot().Value() + m.walLivePhysicalBytes.Snapshot().Value() +
		m.walObsoletePhysicalBytes.Snapshot().Value() + m.compactionInProgressBytes.Snapshot().Value() +
		m.optionsManifestBytes.Snapshot().Value()
	if components != int64(want) || m.diskBytes.Snapshot().Value() != int64(want) {
		t.Fatalf("disk component sum=%d gauge=%d, want engine DiskSpaceUsage=%d", components, m.diskBytes.Snapshot().Value(), want)
	}
}
