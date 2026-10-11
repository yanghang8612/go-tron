package rawdb

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/pointread"
)

type directPressureDB struct {
	ethdb.KeyValueStore
	pressure maintenance.StoragePressure
}

func (d *directPressureDB) MaintenancePressure() maintenance.StoragePressure { return d.pressure }
func TestDirectHistoryMaintenanceAggregatesBothEngines(t *testing.T) {
	now := time.Now()
	hot := &directPressureDB{KeyValueStore: NewMemoryDatabase(), pressure: maintenance.StoragePressure{Available: true, SampledAt: now, L0Sublevels: 8, L0CompactionThreshold: 16, L0StopWritesThreshold: 32, MemTableCount: 1, MemTableStopWritesThreshold: 8, CompactionDebt: 100, StallCount: 2, StallDuration: time.Second}}
	history := &directPressureDB{KeyValueStore: NewMemoryDatabase(), pressure: maintenance.StoragePressure{Available: true, SampledAt: now, L0Sublevels: 4, L0CompactionThreshold: 2, L0StopWritesThreshold: 32, MemTableCount: 3, MemTableStopWritesThreshold: 4, CompactionDebt: 200, StallCount: 3, StallDuration: 2 * time.Second}}
	db := NewDirectHistoryStore(hot, history)
	defer db.Close()
	p := db.MaintenancePressure()
	if !p.Available || p.CompactionDebt != 300 || p.StallCount != 5 || p.StallDuration != 3*time.Second {
		t.Fatalf("aggregate=%+v", p)
	}
	if p.L0Sublevels != 8 || p.L0CompactionThreshold != 4 || p.L0StopWritesThreshold != 32 {
		t.Fatalf("lost worst L0 soft/hard occupancy: %+v", p)
	}
	if p.MemTableCount != 3 || p.MemTableStopWritesThreshold != 4 || !p.HardLimitReached(now) {
		t.Fatalf("lost history memtable occupancy: %+v", p)
	}
	history.pressure.WriteStalled = true
	if !db.MaintenancePressure().WriteStalled {
		t.Fatal("lost history write stall")
	}
	history.pressure.Available = false
	if got := db.MaintenancePressure(); got != hot.pressure {
		t.Fatalf("unavailable history changed hot sample: %+v", got)
	}
}

type directCompactionGateDB struct {
	ethdb.KeyValueStore
	entered, release chan struct{}
}

func (d *directCompactionGateDB) Compact([]byte, []byte) error {
	close(d.entered)
	<-d.release
	return nil
}
func (d *directCompactionGateDB) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return d.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
}
func TestDirectHistoryCompactionDoesNotBlockWritesOrSnapshots(t *testing.T) {
	dir := t.TempDir()
	hot := &directCompactionGateDB{KeyValueStore: directTestPebble(t, filepath.Join(dir, "hot")), entered: make(chan struct{}), release: make(chan struct{})}
	history := directTestPebble(t, filepath.Join(dir, "history"))
	db := NewDirectHistoryStore(hot, history)
	defer db.Close()
	compacted := make(chan error, 1)
	go func() { compacted <- db.Compact([]byte("a"), []byte("b")) }()
	<-hot.entered
	defer func() {
		close(hot.release)
		if err := <-compacted; err != nil {
			t.Error(err)
		}
	}()
	written := make(chan error, 1)
	go func() { written <- db.Put(headBlockKey, []byte("new")) }()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("foreground write blocked on physical compaction")
	}
	snapshotted := make(chan error, 1)
	go func() {
		snapshot, err := db.NewKeyValueSnapshot()
		if err == nil {
			err = snapshot.Close()
		}
		snapshotted <- err
	}()
	select {
	case err := <-snapshotted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot acquisition blocked on physical compaction")
	}
}
