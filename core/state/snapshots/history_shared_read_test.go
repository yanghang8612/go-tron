package snapshots

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

func TestHistorySharedReadResourceSelection(t *testing.T) {
	old := runtime.GOMAXPROCS(16)
	defer runtime.GOMAXPROCS(old)
	now := time.Now()
	for _, tc := range []struct {
		name             string
		workers          int
		cache, forced    bool
		idle, memory     uint64
		age              time.Duration
		unknown, storage bool
		want             int
		wantCache        bool
		reason           historyReadFallback
	}{
		{name: "default", memory: 8 << 30, idle: 16000, reason: historyReadDisabled},
		{name: "eight", workers: 8, cache: true, memory: 8 << 30, idle: 9000, want: 8, wantCache: true},
		{name: "downshift-four", workers: 8, memory: 8 << 30, idle: 5000, want: 4},
		{name: "downshift-two", workers: 4, memory: 8 << 30, idle: 3000, want: 2},
		{name: "no-idle", workers: 4, memory: 8 << 30, idle: 2999, reason: historyReadCPULow},
		{name: "cache-only", cache: true, memory: (2 << 30) + (64 << 20), idle: 1000, wantCache: true},
		{name: "cache-headroom", cache: true, memory: (2 << 30) + (64 << 20) - 1, idle: 1000, reason: historyReadMemoryLow},
		{name: "pipeline-headroom", workers: 2, memory: (2 << 30) + (256 << 20) - 1, idle: 3000, reason: historyReadMemoryLow},
		{name: "unknown", workers: 4, cache: true, unknown: true, memory: 8 << 30, idle: 16000, reason: historyReadResourcesUnknown},
		{name: "stale", workers: 4, age: 16 * time.Second, memory: 8 << 30, idle: 16000, reason: historyReadResourcesStale},
		{name: "future", workers: 4, age: -2 * time.Second, memory: 8 << 30, idle: 16000, reason: historyReadResourcesStale},
		{name: "storage", workers: 4, storage: true, memory: 8 << 30, idle: 16000, reason: historyReadStoragePressure},
		{name: "forcedbusy-ready", workers: 8, cache: true, forced: true, memory: 8 << 30, idle: 16000, want: 4, wantCache: true},
		{name: "forcedbusy-low", workers: 4, cache: true, forced: true, memory: 8 << 30, idle: 2000, reason: historyReadCPULow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Runner{cfg: Config{HistorySharedReadWorkers: tc.workers, HistorySharedChunkCache: tc.cache, HeavyWorkGate: maintenance.NewHeavyWorkGate(), HistoryReadResourceProbe: func() HistoryReadResources {
				return HistoryReadResources{Available: !tc.unknown, SampledAt: now.Add(-tc.age), IdleCoresMilli: tc.idle, MemoryAvailableBytes: tc.memory}
			}}}
			r.historyLoad.level = 3
			r.historyLoad.sample = healthyHistoryLoad(now)
			if tc.storage {
				r.historyLoad.level = 1
			}
			got, reason := r.selectHistoryReadOptions(tc.forced, now)
			if got.Workers != tc.want || got.ChunkCache != tc.wantCache || reason != tc.reason {
				t.Fatal("incorrect execution plan", got, reason)
			}
			if got.enabled() && got.compressionWorkers() != 1 {
				t.Fatal("enhanced read amplified codec workers")
			}
		})
	}
}

type historySharedSeedWriter struct {
	ethdb.KeyValueStore
	batch ethdb.Batch
}

func (w historySharedSeedWriter) Put(k, v []byte) error               { return w.batch.Put(k, v) }
func (w historySharedSeedWriter) Delete(k []byte) error               { return w.batch.Delete(k) }
func (w historySharedSeedWriter) StateHistoryChunkWritesAtomic() bool { return true }

func newHistorySharedSource(t *testing.T, blockCount ...int) *pebbledb.Database {
	t.Helper()
	db, err := pebbledb.New(t.TempDir(), 16, 16, "test/history-shared/", false, pebbledb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	rawdb.SetStateHistoryCrossBlockDedup(true)
	defer rawdb.SetStateHistoryCrossBlockDedup(false)
	batch := db.NewBatch()
	blocks := 4
	if len(blockCount) != 0 {
		blocks = blockCount[0]
	}
	seedParallelHistoryEvent(t, historySharedSeedWriter{db, batch}, blocks, 2, 1, 256<<10)
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	return db
}

type historyExecutionSource struct {
	ethdb.KeyValueStore
	factory                      pointread.KeyValueSnapshotter
	block                        bool
	closeErr                     error
	opened, closed, active, late atomic.Int32
	first                        atomic.Bool
	entered, release             chan struct{}
	once                         sync.Once
}

func (s *historyExecutionSource) unblock() { s.once.Do(func() { close(s.release) }) }
func (s *historyExecutionSource) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	view, err := s.factory.NewKeyValueSnapshot()
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return &historyExecutionView{KeyValueSnapshot: view, source: s}, nil
}

type historyExecutionView struct {
	pointread.KeyValueSnapshot
	source *historyExecutionSource
}

func (v *historyExecutionView) IsPinnedKeyValueView() bool {
	return v.KeyValueSnapshot.(pointread.PinnedKeyValueView).IsPinnedKeyValueView()
}
func (v *historyExecutionView) GetReturnsOwnedBytes() bool {
	return v.KeyValueSnapshot.(pointread.OwnedKeyValueReader).GetReturnsOwnedBytes()
}
func (v *historyExecutionView) ConcurrentOwnedHistoryReads() bool {
	return v.KeyValueSnapshot.(pointread.ConcurrentOwnedKeyValueView).ConcurrentOwnedHistoryReads()
}
func (v *historyExecutionView) Get(key []byte) ([]byte, error) {
	s := v.source
	if s.closed.Load() != 0 {
		s.late.Add(1)
		return nil, errors.New("read after snapshot close")
	}
	s.active.Add(1)
	defer s.active.Add(-1)
	if s.first.CompareAndSwap(false, true) {
		close(s.entered)
		if s.block {
			<-s.release
		}
	}
	return v.KeyValueSnapshot.Get(key)
}
func (v *historyExecutionView) Close() error {
	s := v.source
	s.closed.Add(1)
	if s.active.Load() != 0 {
		s.late.Add(1)
	}
	return errors.Join(v.KeyValueSnapshot.Close(), s.closeErr)
}

func historySharedRunner(source ethdb.KeyValueStore, dir string) *Runner {
	chain := &parallelHistoryEventChain{&coldBuilderChain{db: source, solidified: 5, syncRemaining: 1000, syncRemainingOK: true}, rawdb.NewChainDB(source, nil)}
	r := parallelHistoryEventRunner(chain, dir, 4, 2)
	r.cfg.HistorySharedReadWorkers = 4
	r.cfg.HistorySharedChunkCache = true
	r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
		return HistoryReadResources{Available: true, SampledAt: time.Now(), IdleCoresMilli: 16000, MemoryAvailableBytes: 8 << 30}
	}
	return r
}

func TestHistorySharedReadRunnerCancelJoinCloseBeforePublication(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	for _, mode := range []string{"cancel", "close-error", "serial-close-error", "success"} {
		t.Run(mode, func(t *testing.T) {
			db := newHistorySharedSource(t)
			sentinel := errors.New("snapshot close sentinel")
			source := &historyExecutionSource{KeyValueStore: db, factory: db, block: mode == "cancel", entered: make(chan struct{}), release: make(chan struct{})}
			if mode == "close-error" || mode == "serial-close-error" {
				source.closeErr = sentinel
			}
			defer source.unblock()
			r := historySharedRunner(source, t.TempDir())
			if mode == "serial-close-error" {
				r.cfg.HistorySharedChunkCache = false
			}
			defer r.cancel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var maintenanceCalls atomic.Int32
			type outcome struct {
				result PassResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				result, err := r.OnePassWithMaintenanceContext(ctx, func(context.Context, PassResult) error { maintenanceCalls.Add(1); return nil })
				done <- outcome{result, err}
			}()
			joined := false
			defer func() {
				source.unblock()
				if !joined {
					<-done
				}
			}()
			if mode == "cancel" {
				waitParallelRead(t, source.entered)
				cancel()
				select {
				case got := <-done:
					joined = true
					t.Fatal("returned before blocked read joined", got)
				case <-time.After(20 * time.Millisecond):
				}
				if release, ok := r.cfg.HeavyWorkGate.TryAcquire(); ok {
					release()
					t.Fatal("lease escaped live worker")
				}
				if source.closed.Load() != 0 || maintenanceCalls.Load() != 0 {
					t.Fatal("snapshot/pruning crossed join boundary")
				}
				if _, err := LoadProductionManifest(r.cfg.Dir); !os.IsNotExist(err) {
					t.Fatal("manifest published during live read", err)
				}
				source.unblock()
			}
			got := <-done
			joined = true
			if source.opened.Load() != 1 || source.closed.Load() != 1 || source.active.Load() != 0 || source.late.Load() != 0 {
				t.Fatal("view lifetime incorrect", source.opened.Load(), source.closed.Load(), source.active.Load(), source.late.Load())
			}
			if got.result.HistorySharedReadWorkers != 4 || got.result.HistorySharedChunkCache != (mode != "serial-close-error") || got.result.HistoryEventParallel {
				t.Fatal("actual topology not recorded/sequential", got.result)
			}
			if r.historyReadMetrics.values["active"].Snapshot().Value() != 0 || r.historyReadMetrics.values["workers"].Snapshot().Value() != 4 || r.historyReadMetrics.values["codec_workers"].Snapshot().Value() != 1 {
				t.Fatal("actual topology metrics missing")
			}
			if mode == "success" {
				if got.err != nil || !got.result.Built || !got.result.EventLogBuilt || maintenanceCalls.Load() != 1 {
					t.Fatal("successful complete publication failed", got)
				}
				if r.historyReadMetrics.values["last_published"].Snapshot().Value() != 1 {
					t.Fatal("publication metric not set")
				}
			} else {
				want := sentinel
				if mode == "cancel" {
					want = context.Canceled
				}
				if !errors.Is(got.err, want) || got.result.Built || maintenanceCalls.Load() != 0 {
					t.Fatal("failed history published or pruned", got)
				}
				if _, err := LoadProductionManifest(r.cfg.Dir); !os.IsNotExist(err) {
					t.Fatal("failed history manifest exists", err)
				}
				if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || ok {
					t.Fatal("failed history advanced stage", ok, err)
				}
				if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
					t.Fatal("failed history pruned input", ok, err)
				}
			}
		})
	}
}

func TestHistorySharedReadRunnerDefaultsAndFallbackKeepFiles(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	var wantRefs []SegmentRef
	var wantFiles map[string][]byte
	for _, mode := range []string{"default", "enhanced", "unknown", "memory-low", "forced-busy-ready", "forced-busy-unknown"} {
		t.Run(mode, func(t *testing.T) {
			db := newHistorySharedSource(t)
			r := historySharedRunner(db, t.TempDir())
			defer r.cancel()
			parallelChecks := 0
			r.cfg.ParallelHistoryEventReady = func() bool {
				parallelChecks++
				return true
			}
			if mode == "default" {
				r.cfg.HistorySharedReadWorkers = 0
				r.cfg.HistorySharedChunkCache = false
			}
			if mode == "unknown" {
				r.cfg.HistoryReadResourceProbe = func() HistoryReadResources { return HistoryReadResources{} }
			}
			if mode == "memory-low" {
				r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
					return HistoryReadResources{Available: true, SampledAt: time.Now(), IdleCoresMilli: 16000, MemoryAvailableBytes: historyReadMinimumHeadroom}
				}
			}
			if mode == "forced-busy-ready" || mode == "forced-busy-unknown" {
				r.cfg.DeferHistoryBuildWhileSyncing = true
				r.cfg.MaxDeferredHistoryBlocks = 1
				r.cfg.MaxBusyDeferredHistoryBlocks = 1
				r.cfg.SyncBuildReady = func() bool { return false }
				if mode == "forced-busy-unknown" {
					r.cfg.HistoryReadResourceProbe = func() HistoryReadResources { return HistoryReadResources{} }
				}
			}
			result, err := r.OnePass()
			if err != nil || !result.Built || !result.EventLogBuilt {
				t.Fatal("fallback must retain progress", result, err)
			}
			if mode == "enhanced" || mode == "forced-busy-ready" {
				if result.HistorySharedReadWorkers != 4 || !result.HistorySharedChunkCache || result.HistoryEventParallel {
					t.Fatal("enhanced topology wrong", result)
				}
			} else if result.HistorySharedReadWorkers != 0 || result.HistorySharedChunkCache {
				t.Fatal("fallback enabled new reader", result)
			}
			wantParallel := mode == "default" || mode == "unknown" || mode == "memory-low" || mode == "forced-busy-unknown"
			if result.HistoryEventParallel != wantParallel {
				t.Fatal("parallel event decision did not follow selected topology", result)
			}
			wantParallelChecks := 0
			if wantParallel {
				wantParallelChecks = 1
			}
			if parallelChecks != wantParallelChecks {
				t.Fatal("parallel readiness gate checks", parallelChecks, "want", wantParallelChecks)
			}
			if mode == "memory-low" && result.HistoryReadFallbackReason != uint8(historyReadMemoryLow) {
				t.Fatal("memory-low fallback reason", result.HistoryReadFallbackReason)
			}
			files := make(map[string][]byte)
			for _, ref := range result.Segments {
				data, err := os.ReadFile(filepath.Join(r.cfg.Dir, ref.Path))
				if err != nil {
					t.Fatal(err)
				}
				files[ref.Path] = data
			}
			if mode == "default" {
				wantRefs = result.Segments
				wantFiles = files
			} else if mode != "forced-busy-ready" && mode != "forced-busy-unknown" && (!reflect.DeepEqual(result.Segments, wantRefs) || !reflect.DeepEqual(files, wantFiles)) {
				t.Fatal("default/enhanced/fallback output differs")
			}
			if (mode == "forced-busy-ready" || mode == "forced-busy-unknown") && !result.HistoryForcedBusy {
				t.Fatal("forcedbusy classification changed", result)
			}
		})
	}
}

func TestHistorySharedReadOldEntryAndContextEntryEqual(t *testing.T) {
	db := newHistorySharedSource(t)
	oldDir, newDir := t.TempDir(), t.TempDir()
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	path := cfg.HistoryPath(1, 8)
	old, err := BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(db, oldDir, 1, 8, 1, 4, path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeReadContext(context.Background(), db, newDir, 1, 8, 1, 4, path, HistoryReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old, got) {
		t.Fatal("default context entry refs differ")
	}
	for _, ref := range old {
		a, _ := os.ReadFile(filepath.Join(oldDir, ref.Path))
		b, _ := os.ReadFile(filepath.Join(newDir, ref.Path))
		if !bytes.Equal(a, b) {
			t.Fatal("default context entry bytes differ")
		}
	}
}

func TestHistorySharedReadGateBeforeResourceProbe(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	db := newHistorySharedSource(t)
	source := &historyExecutionSource{KeyValueStore: db, factory: db, entered: make(chan struct{}), release: make(chan struct{})}
	r := historySharedRunner(source, t.TempDir())
	defer r.cancel()
	calls := 0
	r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
		calls++
		if release, ok := r.cfg.HeavyWorkGate.TryAcquire(); ok {
			release()
			t.Error("resource plan selected without lease")
		}
		return HistoryReadResources{Available: true, SampledAt: time.Now(), IdleCoresMilli: 8000, MemoryAvailableBytes: 8 << 30}
	}
	release, ok := r.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		t.Fatal("fixture gate unavailable")
	}
	result, err := r.OnePass()
	release()
	if err != nil || result.HistoryBuildAttempted || !result.HistoryDeferred || calls != 0 || source.opened.Load() != 0 {
		t.Fatal("gate loser probed/opened source", result, err, calls, source.opened.Load())
	}
	result, err = r.OnePass()
	if err != nil || !result.Built || calls != 1 || source.opened.Load() != 1 || source.closed.Load() != 1 {
		t.Fatal("gate winner plan failed", result, err, calls)
	}
}

func TestHistorySharedReadForcedBusyRetainsCompleteMaintenanceRecovery(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial-fallback", true: "enhanced"}[known], func(t *testing.T) {
			db := newHistorySharedSource(t, 5)
			r := historySharedRunner(db, t.TempDir())
			defer r.cancel()
			r.cfg.DeferHistoryBuildWhileSyncing = true
			r.cfg.MaxDeferredHistoryBlocks = 1
			r.cfg.MaxBusyDeferredHistoryBlocks = 1
			r.cfg.SyncBuildReady = func() bool { return false }
			if !known {
				r.cfg.HistoryReadResourceProbe = func() HistoryReadResources { return HistoryReadResources{} }
			}
			result, err := r.OnePassWithDeferredMaintenanceContext(context.Background(), nil)
			if err != nil || !result.Built || !result.HistoryForcedBusy || !result.historyCompletionPending {
				t.Fatal("forced build failed", result, err)
			}
			if result.HistoryBatchBlocks != 4 || result.HistoryBatchTxNums != 8 || result.HistorySharedChunkCache != known {
				t.Fatal("reader changed batch bounds", result)
			}
			if next, err := r.OnePass(); !errors.Is(err, ErrHistoryMaintenancePending) || next.HistoryBuildAttempted {
				t.Fatal("outer maintenance bypassed", next, err)
			}
			r.CompleteHistoryMaintenance(&result, time.Now().Add(-20*time.Second), nil)
			want := r.throughputRecovery(result.HistoryRecoveryCost, false)
			if result.historyCompletionPending || result.HistoryMaintenanceDuration < 20*time.Second || result.HistoryRecoveryCost < 20*time.Second-result.CompactionDuration || result.HistoryMinRecovery != want {
				t.Fatal("complete maintenance recovery changed", result, want)
			}
			if result.HistoryReadFallbackReason != uint8(map[bool]historyReadFallback{false: historyReadResourcesUnknown, true: historyReadEnabled}[known]) {
				t.Fatal("wrong fallback reason", result)
			}
			deadline := r.historyNotBefore.Load()
			r.cfg.SyncBuildReady = func() bool { return true }
			chain := r.chain.(*parallelHistoryEventChain)
			chain.solidified = 6
			chain.syncRemainingOK = false
			next, err := r.OnePass()
			if err != nil || next.HistoryBuildAttempted || !next.HistoryRateLimited || r.historyNotBefore.Load() != deadline {
				t.Fatal("new topology bypassed existing recovery", next, err)
			}
		})
	}
}
