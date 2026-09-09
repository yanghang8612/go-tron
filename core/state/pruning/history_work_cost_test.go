package pruning

import (
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Delay the real first batch commit, not its construction or metadata path.
// This catches density accounting which accidentally discounts all pruning.
type historyCostCommitStore struct {
	ethdb.KeyValueStore
	entered    chan struct{}
	resume     chan struct{}
	once       sync.Once
	commitWait time.Duration
}

func (db *historyCostCommitStore) NewBatch() ethdb.Batch {
	return &historyCostCommitBatch{Batch: db.KeyValueStore.NewBatch(), db: db}
}

func (db *historyCostCommitStore) NewBatchWithSize(size int) ethdb.Batch {
	return &historyCostCommitBatch{Batch: db.KeyValueStore.NewBatchWithSize(size), db: db}
}

type historyCostCommitBatch struct {
	ethdb.Batch
	db *historyCostCommitStore
}

func (b *historyCostCommitBatch) Write() error {
	b.db.once.Do(func() {
		started := time.Now()
		close(b.db.entered)
		<-b.db.resume
		b.db.commitWait = time.Since(started)
	})
	return b.Batch.Write()
}

func TestHistoryDensityLifecycleRetainsRealPruneCommitCost(t *testing.T) {
	db := &historyCostCommitStore{KeyValueStore: rawdb.NewMemoryDatabase(), entered: make(chan struct{}), resume: make(chan struct{})}
	defer db.Close()
	writeSnapPruningChange(t, db, 1, 10, 12)
	dir := t.TempDir()
	namespace := "test/" + t.Name()
	chain := &fakePruneChain{db: db, solidified: 2, syncRemaining: 100, syncRemainingOK: true}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{Dir: dir, Enabled: true, Interval: time.Hour, HistoryWindow: 1,
			HistoryCatchupMode: snapshots.HistoryCatchupThroughput, MetricsNamespace: namespace},
		Pruner: PrunerConfig{Policy: SnapPolicy(1, 1), Interval: time.Hour, SnapshotDir: dir,
			DeferStateCodePruneWhileSyncing: true},
	})
	type outcome struct {
		result SnapshotLifecyclePass
		err    error
	}
	done := make(chan outcome, 1)
	var resume sync.Once
	received := false
	defer func() {
		resume.Do(func() { close(db.resume) })
		if !received {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("lifecycle did not stop after releasing commit")
			}
		}
	}()
	go func() { result, err := lifecycle.OnePass(); done <- outcome{result, err} }()
	select {
	case <-db.entered:
	case got := <-done:
		received = true
		t.Fatalf("lifecycle never reached prune commit: %+v %v", got.result, got.err)
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle never reached prune commit")
	}
	// A measurable delay with no upper timing assertion tolerates loaded CI.
	time.Sleep(20 * time.Millisecond)
	resume.Do(func() { close(db.resume) })
	var got outcome
	select {
	case got = <-done:
		received = true
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle failed to finish")
	}
	r := got.result.Snapshot
	if got.err != nil || !r.Built || got.result.Prune.DeletedDomainChangeBlocks != 1 {
		t.Fatalf("real build/prune failed: %+v %v", got.result, got.err)
	}
	if got.result.Prune.HistoryMetadataDuration <= 0 || r.BeforeMergeMetadataDuration < got.result.Prune.HistoryMetadataDuration || r.HistoryMetadataDuration <= 0 {
		t.Fatalf("worker/lifecycle metadata timing was not connected: %+v", got.result)
	}
	if r.BeforeMergeDuration-r.BeforeMergeMetadataDuration < db.commitWait || r.HistoryRecoveryCost < db.commitWait {
		t.Fatalf("batch commit was discounted: wait=%s result=%+v", db.commitWait, r)
	}
	want := r.BuildDuration + r.BeforeMergeDuration - r.HistoryMetadataDuration - r.BeforeMergeMetadataDuration
	gauge, ok := metrics.DefaultRegistry.Get(namespace + "/history/budget/density_work").(*metrics.Gauge)
	if !ok || gauge.Snapshot().Value() != int64(want) || want < db.commitWait {
		t.Fatalf("complete lifecycle did not feed retained row cost: gauge=%v want=%s", gauge, want)
	}
	if r.HistoryRecoveryCost < r.BuildDuration+r.BeforeMergeDuration {
		t.Fatalf("metadata disappeared from complete maintenance duty: %+v", r)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("committed hot row was not removed: present=%v err=%v", ok, err)
	}
}
