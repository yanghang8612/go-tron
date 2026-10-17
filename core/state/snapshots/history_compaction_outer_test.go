package snapshots

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestHistoryCompactionOuterPhaseAccounting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newHistoryCompactionOuterMetrics("test/" + t.Name())
		p := m.begin()
		if m.active.Snapshot().Value() != 1 || m.phase.Snapshot().Value() != int64(historyCompactionOuterPrepare) {
			t.Fatal("outer not active")
		}
		time.Sleep(time.Second)
		p.setPhase(historyCompactionOuterManifest)
		time.Sleep(2 * time.Second)
		p.setPhase(historyCompactionOuterSelect)
		time.Sleep(3 * time.Second)
		p.finish()
		if m.active.Snapshot().Value() != 0 || m.phase.Snapshot().Value() != 0 || m.started.Snapshot().Value() != 0 || m.phaseStarted.Snapshot().Value() != 0 || m.lastDuration.Snapshot().Value() != int64(6*time.Second) {
			t.Fatal("outer not cleared/timed")
		}
		var sum int64
		for phase := historyCompactionOuterPrepare; phase < historyCompactionOuterPhaseCount; phase++ {
			sum += m.last[phase].Snapshot().Value()
		}
		if sum != m.lastDuration.Snapshot().Value() || m.total[historyCompactionOuterManifest].Snapshot().Count() != int64(2*time.Second) {
			t.Fatal("phase time overlaps/holes")
		}
		p = m.begin()
		time.Sleep(time.Second)
		p.finish()
		if m.last[historyCompactionOuterManifest].Snapshot().Value() != 0 || m.total[historyCompactionOuterManifest].Snapshot().Count() != int64(2*time.Second) || m.attempts.Snapshot().Count() != 2 {
			t.Fatal("last/total scopes mixed")
		}
		if m.maxDuration.Snapshot().Value() != int64(6*time.Second) || m.totalDuration.Snapshot().Count() != int64(7*time.Second) || m.max[historyCompactionOuterManifest].Snapshot().Value() != int64(2*time.Second) {
			t.Fatal("short defer overwrote cumulative/highwater")
		}
	})
}

func TestHistoryCompactionOuterErrorsAndDeferClear(t *testing.T) {
	for _, kind := range []string{"cancel", "invalid-dir", "absent", "defer", "revalidate-error"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			m := newHistoryCompactionOuterMetrics("test/" + t.Name())
			ctx := context.Background()
			cfg := CompactionConfig{BusyLeafOnly: true, MaxInputRecords: 2, MaxSources: 2, outerMetrics: m}
			cache := newHistoryCompactionCostCache()
			cfg.inputCostCache = cache
			statCalls := 0
			failure := errors.New("selected-stat-failure")
			cache.stat = func(path string) (os.FileInfo, error) {
				statCalls++
				if kind == "revalidate-error" && path != cache.dir && path != dir {
					return nil, failure
				}
				return os.Stat(path)
			}
			switch kind {
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "invalid-dir":
				dir = ""
			case "defer", "revalidate-error":
				refs := costCacheLeaf(t, dir, 1, 1)
				if kind == "revalidate-error" {
					refs = append(refs, costCacheLeaf(t, dir, 2, 1)...)
				}
				end := uint64(1)
				if kind == "revalidate-error" {
					end = 2
				}
				if err := PublishManifest(dir, NewManifest(1, end, refs)); err != nil {
					t.Fatal(err)
				}
			}
			var before []byte
			if kind == "revalidate-error" {
				before, _ = os.ReadFile(filepath.Join(dir, ManifestFile))
			}
			result, err := CompactHistoryDomainContext(ctx, dir, SegmentDatasetStateDomainChange, cfg)
			if m.active.Snapshot().Value() != 0 || m.phase.Snapshot().Value() != 0 || m.started.Snapshot().Value() != 0 || m.phaseStarted.Snapshot().Value() != 0 || m.attempts.Snapshot().Count() != 1 {
				t.Fatal("failure/defer left active telemetry")
			}
			switch kind {
			case "cancel":
				if !errors.Is(err, context.Canceled) || statCalls != 0 {
					t.Fatal(err, statCalls)
				}
			case "invalid-dir":
				if err == nil {
					t.Fatal("expected dir error")
				}
			case "absent":
				if err != nil || result.Merged {
					t.Fatal(result, err)
				}
			case "defer":
				if err != nil || !result.Deferred || statCalls != 1 || m.hintMisses.Snapshot().Count() != 1 || m.hintReads.Snapshot().Count() != 1 || m.hintEntries.Snapshot().Value() != 1 || m.hintBytes.Snapshot().Value() != 512 {
					t.Fatal("defer telemetry or extra IO", result, err, statCalls)
				}
			case "revalidate-error":
				if !errors.Is(err, failure) || m.entered[historyCompactionOuterRevalidate].Snapshot().Count() != 1 || m.entered[historyCompactionOuterInner].Snapshot().Count() != 0 {
					t.Fatal("selected failure bypass", err)
				}
				after, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
				if !bytes.Equal(before, after) {
					t.Fatal("selected failure published")
				}
			}
		})
	}
}

func TestHistoryCompactionOuterSuccessfulPublication(t *testing.T) {
	dir := t.TempDir()
	refs := append(costCacheLeaf(t, dir, 1, 1), costCacheLeaf(t, dir, 2, 1)...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}
	m := newHistoryCompactionOuterMetrics("test/" + t.Name())
	cache := newHistoryCompactionCostCache()
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: true, MaxInputRecords: 2, MaxSources: 2, inputCostCache: cache, outerMetrics: m})
	if err != nil || !result.Merged {
		t.Fatal(result, err)
	}
	for phase := historyCompactionOuterPrepare; phase < historyCompactionOuterPhaseCount; phase++ {
		if m.entered[phase].Snapshot().Count() != 1 {
			t.Fatal("phase missing", phase)
		}
	}
	if m.active.Snapshot().Value() != 0 || m.hintMisses.Snapshot().Count() != 2 || m.hintReads.Snapshot().Count() != 4 || m.hintFresh.Snapshot().Count() != 2 || m.hintRetired.Snapshot().Count() != 2 || m.hintEntries.Snapshot().Value() != 0 || m.hintBytes.Snapshot().Value() != 0 {
		t.Fatal("incorrect successful hint snapshot", cache.snapshot())
	}
	// Existing inner metrics retain a distinct scope/name.
	if m.phase == productionHistoryCompactionMetrics.phase || m.lastDuration == productionHistoryCompactionMetrics.elapsed {
		t.Fatal("outer overwrites inner metrics")
	}
}

func TestHistoryCompactionOuterRunnerNamespaceAndReuse(t *testing.T) {
	ns := "test/" + t.Name() + "/"
	r := NewRunner(nil, Config{Enabled: true, Dir: t.TempDir(), MetricsNamespace: ns, HistoryDataset: SegmentDatasetStateDomainChange, HeavyWorkGate: maintenance.NewHeavyWorkGate()})
	if _, err := r.compactHistory(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	m := r.compactionBudget.outerMetrics
	if m == nil || metrics.DefaultRegistry.Get(ns+"compaction/outer/attempts") != m.attempts {
		t.Fatal("Runner namespace not used")
	}
	if _, err := r.compactHistory(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if r.compactionBudget.outerMetrics != m || m.attempts.Snapshot().Count() != 2 || m.active.Snapshot().Value() != 0 {
		t.Fatal("observer not reused or not cleared")
	}
}
