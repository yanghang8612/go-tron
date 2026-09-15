package snapshots

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func referenceProductionRunner(source *historyExecutionSource, dir string) *Runner {
	r := historySharedRunner(source, dir)
	r.cfg.HistorySharedReadWorkers = 0
	r.cfg.HistoryReferenceContainer = true
	return r
}

func referenceProductionHistoryRefs(refs []SegmentRef) []SegmentRef {
	var out []SegmentRef
	for _, ref := range refs {
		if ref.Dataset == SegmentDatasetStateDomainChange {
			out = append(out, ref)
		}
	}
	return out
}

func TestHistoryReferenceRunnerResourceFallbackPreservesFormat(t *testing.T) {
	old := runtime.GOMAXPROCS(10)
	defer runtime.GOMAXPROCS(old)
	now := time.Now()
	for _, forced := range []bool{false, true} {
		for _, fault := range []string{"none", "cache-off", "no-lease", "unknown", "stale", "memory", "cpu", "storage"} {
			t.Run(fault+map[bool]string{false: "", true: "-forced"}[forced], func(t *testing.T) {
				r := historySharedRunner(nil, t.TempDir())
				defer r.cancel()
				r.cfg.HistorySharedReadWorkers = 0
				r.cfg.HistoryReferenceContainer = true
				r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
					return HistoryReadResources{Available: true, SampledAt: now, IdleCoresMilli: 4000, MemoryAvailableBytes: 8 << 30}
				}
				switch fault {
				case "cache-off":
					r.cfg.HistorySharedChunkCache = false
				case "no-lease":
					r.cfg.HeavyWorkGate = nil
				case "unknown":
					r.cfg.HistoryReadResourceProbe = func() HistoryReadResources { return HistoryReadResources{} }
				case "stale":
					r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
						return HistoryReadResources{Available: true, SampledAt: now.Add(-time.Minute)}
					}
				case "memory":
					r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
						return HistoryReadResources{Available: true, SampledAt: now, IdleCoresMilli: 4000, MemoryAvailableBytes: 1}
					}
				case "cpu":
					r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
						return HistoryReadResources{Available: true, SampledAt: now, IdleCoresMilli: 0, MemoryAvailableBytes: 8 << 30}
					}
				case "storage":
					r.historyLoad.level = 0
				}
				got, _ := r.selectHistoryReadOptions(forced, now)
				if !got.ReferenceContainer || got.Workers != 0 || got.ChunkCache != (fault == "none") || !got.enabled() || got.compressionWorkers() != 0 {
					t.Fatal("resource fallback changed format", got)
				}
			})
		}
	}
	for _, workers := range []int{0, 2, 4, 8} {
		if err := (HistoryReadOptions{Workers: workers, ReferenceContainer: true}).Validate(); err != nil {
			t.Fatal("valid R1 read options rejected", workers, err)
		}
		for _, forced := range []bool{false, true} {
			r := historySharedRunner(nil, t.TempDir())
			defer r.cancel()
			r.cfg.HistorySharedReadWorkers = workers
			r.cfg.HistoryReferenceContainer = true
			r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
				return HistoryReadResources{Available: true, SampledAt: now, IdleCoresMilli: 10000, MemoryAvailableBytes: 8 << 30}
			}
			got, _ := r.selectHistoryReadOptions(forced, now)
			want := workers
			if forced && want > 4 {
				want = 4
			}
			if got.Workers != want || !got.ReferenceContainer || !got.ChunkCache {
				t.Fatal("parallel R1 resource admission differs", got, want)
			}
			r.cfg.HistoryReadResourceProbe = func() HistoryReadResources { return HistoryReadResources{} }
			got, _ = r.selectHistoryReadOptions(forced, now)
			if got.Workers != 0 || got.ChunkCache || !got.ReferenceContainer {
				t.Fatal("parallel resource fallback changed format", got)
			}
		}
	}
	for _, workers := range []int{1, 3, -2} {
		if err := (HistoryReadOptions{Workers: workers, ReferenceContainer: true}).Validate(); err == nil {
			t.Fatal("invalid R1 worker count accepted", workers)
		}
	}
}

func TestHistoryReferenceRunnerPublishesOncePrunesAndReopens(t *testing.T) {
	old := runtime.GOMAXPROCS(10)
	defer runtime.GOMAXPROCS(old)
	var baselineRefs []SegmentRef
	var baselineFiles map[string][]byte
	for _, mode := range []string{"cache", "pipeline-2", "pipeline-4", "pipeline-8", "unknown", "cache-off"} {
		t.Run(mode, func(t *testing.T) {
			db := newHistorySharedSource(t)
			source := &historyExecutionSource{KeyValueStore: db, factory: db, entered: make(chan struct{}), release: make(chan struct{})}
			defer source.unblock()
			dir := t.TempDir()
			r := referenceProductionRunner(source, dir)
			defer r.cancel()
			wantWorkers := map[string]int{"pipeline-2": 2, "pipeline-4": 4, "pipeline-8": 8}[mode]
			r.cfg.HistorySharedReadWorkers = wantWorkers
			wantCache := mode != "unknown" && mode != "cache-off"
			if mode == "unknown" {
				r.cfg.HistoryReadResourceProbe = func() HistoryReadResources { return HistoryReadResources{} }
			}
			if mode == "cache-off" {
				r.cfg.HistorySharedChunkCache = false
			}
			hot, err := DigestHotStateHistoryContext(context.Background(), db, t.TempDir(), 1, 8, 1, 4, 8<<20)
			if err != nil {
				t.Fatal(err)
			}
			var maintenanceCalls int
			var published []SegmentRef
			initial := r.historyReadMetrics.published.Snapshot().Count()
			result, err := r.OnePassWithMaintenanceContext(context.Background(), func(ctx context.Context, result PassResult) error {
				maintenanceCalls++
				if !result.Built || !result.HistoryReferenceContainer || result.HistoryEventParallel || source.closed.Load() != 1 {
					return errors.New("publication crossed source close or format boundary")
				}
				manifest, e := LoadProductionManifest(dir)
				if e != nil {
					return e
				}
				published = referenceProductionHistoryRefs(manifest.Segments)
				if len(published) != 3 {
					return errors.New("missing published trio")
				}
				history, index, accessor, _, e := historyReferenceTranscodeIdentity(published)
				if e != nil {
					return e
				}
				if e = verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, history, index, accessor); e != nil {
					return e
				}
				stage, ok, e := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild)
				if e != nil || !ok || stage != 4 {
					return errors.New("maintenance precedes durable snapshot stage")
				}
				cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
				// Exercise the production hot-block deletion function only after complete
				// published-trio verification. The outer pruning worker's canonical guard
				// is tested in pruning; this fixture has no concurrent writers.
				stats, e := cfg.PruneHotHistory(db, HotHistoryPruneOptions{MaxBlocks: 4, Decide: func(row rawdb.StateTxRange) (HotHistoryPruneDecision, error) {
					if row.BeginTxNum < 1 || row.EndTxNum > 8 {
						return HotHistoryPruneDecision{}, errors.New("prune exceeds verified range")
					}
					return HotHistoryPruneDecision{DeleteHistoryBlock: true}, nil
				}})
				if e != nil {
					return e
				}
				if stats.DeletedHistoryBlocks != 4 || stats.DeletedTxRanges != 0 {
					return errors.New("snap-mode hot deletion differs")
				}
				return nil
			})
			if err != nil || !result.Built || !result.EventLogBuilt || !result.HistoryReferenceContainer || maintenanceCalls != 1 {
				t.Fatal("R1 publication failed", result, err, maintenanceCalls)
			}
			if result.HistorySharedChunkCache != wantCache || result.HistorySharedReadWorkers != wantWorkers || source.opened.Load() != 1 || source.closed.Load() != 1 || source.late.Load() != 0 {
				t.Fatal("incorrect actual topology or source ownership", result)
			}
			if r.historyReadMetrics.published.Snapshot().Count() != initial+1 {
				t.Fatal("expected exactly one publication")
			}
			files := make(map[string][]byte)
			for _, ref := range published {
				b, e := os.ReadFile(filepath.Join(dir, ref.Path))
				if e != nil {
					t.Fatal(e)
				}
				files[ref.Path] = b
				if ref.Kind == SegmentHistory && !bytes.HasPrefix(b, []byte(historyReferenceMagic)) {
					t.Fatal("resource fallback emitted old format")
				}
			}
			if mode == "cache" {
				baselineRefs = published
				baselineFiles = files
			} else if !reflect.DeepEqual(baselineRefs, published) || !reflect.DeepEqual(baselineFiles, files) {
				t.Fatal("cache admission changed logical or physical output")
			}
			for block := uint64(1); block <= 4; block++ {
				if row, ok, e := rawdb.ReadStateDomainChange(db, block, 1); e != nil || ok || row != nil {
					t.Fatal("hot source survived prune", row, ok, e)
				}
			}
			cold, e := DigestColdStateHistoryContext(context.Background(), dir, published)
			if e != nil || cold != hot {
				t.Fatal("cold full rows changed after hot prune", cold, hot, e)
			}
			m, e := OpenManager(dir)
			if e != nil {
				t.Fatal(e)
			}
			count := 0
			if e = m.IterateStateDomainChanges(1, 8, func(row *rawdb.StateDomainChange) (bool, error) { count++; return true, nil }); e != nil || count != 8 {
				t.Fatal("reopened manager cannot recover history", count, e)
			}
			before, e := os.ReadFile(filepath.Join(dir, ManifestFile))
			if e != nil {
				t.Fatal(e)
			}
			next, e := r.OnePass()
			if e != nil || next.Built {
				t.Fatal("unchanged frontier republished", next, e)
			}
			after, e := os.ReadFile(filepath.Join(dir, ManifestFile))
			if e != nil || !bytes.Equal(before, after) || r.historyReadMetrics.published.Snapshot().Count() != initial+1 {
				t.Fatal("second pass changed publication", e)
			}
		})
	}
}

func TestHistoryReferenceRunnerCancellationAndClosePreventPublication(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	for _, mode := range []string{"cancel", "close-error", "cancel-and-close-error", "fallback-close-error", "pipeline-cancel-close-error"} {
		t.Run(mode, func(t *testing.T) {
			db := newHistorySharedSource(t)
			sentinel := errors.New("reference snapshot close sentinel")
			cancelRead := mode == "cancel" || mode == "cancel-and-close-error" || mode == "pipeline-cancel-close-error"
			source := &historyExecutionSource{KeyValueStore: db, factory: db, block: cancelRead, entered: make(chan struct{}), release: make(chan struct{})}
			if mode != "cancel" {
				source.closeErr = sentinel
			}
			defer source.unblock()
			r := referenceProductionRunner(source, t.TempDir())
			defer r.cancel()
			if mode == "pipeline-cancel-close-error" {
				r.cfg.HistorySharedReadWorkers = 4
			}
			if mode == "fallback-close-error" {
				r.cfg.HistoryReadResourceProbe = func() HistoryReadResources { return HistoryReadResources{} }
			}
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
			if cancelRead {
				waitParallelRead(t, source.entered)
				cancel()
				select {
				case got := <-done:
					joined = true
					t.Fatal("R1 returned before source read joined", got)
				case <-time.After(20 * time.Millisecond):
				}
				if source.closed.Load() != 0 || maintenanceCalls.Load() != 0 {
					t.Fatal("cancel closed live view or pruned")
				}
				if release, ok := r.cfg.HeavyWorkGate.TryAcquire(); ok {
					release()
					t.Fatal("cancel released lease before read completion")
				}
				source.unblock()
			}
			got := <-done
			joined = true
			if cancelRead && !errors.Is(got.err, context.Canceled) {
				t.Fatal("cancellation lost", got.err)
			}
			if mode != "cancel" && !errors.Is(got.err, sentinel) {
				t.Fatal("close error lost", got.err)
			}
			if got.err == nil || got.result.Built || !got.result.HistoryReferenceContainer || maintenanceCalls.Load() != 0 {
				t.Fatal("failed reference source published", got)
			}
			if source.opened.Load() != 1 || source.closed.Load() != 1 || source.active.Load() != 0 || source.late.Load() != 0 {
				t.Fatal("reference view lifetime violated")
			}
			if _, e := LoadProductionManifest(r.cfg.Dir); !os.IsNotExist(e) {
				t.Fatal("failed source has published manifest", e)
			}
			if _, ok, e := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); e != nil || ok {
				t.Fatal("failed source advanced stage", ok, e)
			}
			if _, ok, e := rawdb.ReadStateDomainChange(db, 1, 1); e != nil || !ok {
				t.Fatal("failed source lost hot history", ok, e)
			}
		})
	}
}

// A sparse catch-up range may fit the tx budget while exceeding the container's
// fixed block bound. Keep the common admission range bounded before the builder.
func TestHistoryReferenceRunnerCapsSparseBatch(t *testing.T) {
	db := newHistorySharedSource(t, 0)
	hash := common.Hash{1}
	batch := db.NewBatch()
	for block := uint64(1); block <= historyReferenceBlockLimit+1; block++ {
		if err := rawdb.WriteStateTxRange(batch, block, hash, block, block); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	chain := &coldBuilderChain{db: db, solidified: historyReferenceBlockLimit + 2, canonicalHashes: map[uint64]common.Hash{historyReferenceBlockLimit: hash, historyReferenceBlockLimit + 1: hash}}
	r := NewRunner(chain, Config{Dir: t.TempDir(), Enabled: true, HistoryWindow: 1, BatchBlocks: 2 * historyReferenceBlockLimit, BatchTxNums: 4 * historyReferenceBlockLimit, HistoryReferenceContainer: true})
	defer r.cancel()
	result, err := r.OnePass()
	if err != nil || !result.Built || !result.HistoryReferenceContainer || result.FromBlock != 1 || result.ToBlock != historyReferenceBlockLimit || result.HistoryBatchBlocks != historyReferenceBlockLimit {
		t.Fatal("sparse range bypassed R1 cap", result, err)
	}
}
