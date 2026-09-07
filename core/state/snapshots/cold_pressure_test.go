package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func pressureFixture(t *testing.T) *Runner {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	for n := uint64(1); n <= 12; n++ {
		writeColdBuilderChange(t, db, coldBuilderOwner(1), n, n, "previous")
		writeColdBuilderCanonicalBlock(t, db, n)
	}
	return NewRunner(&coldBuilderChain{db: db, solidified: 13, syncRemaining: 1000, syncRemainingOK: true}, Config{
		Dir: t.TempDir(), Enabled: true, HistoryWindow: 1, BatchBlocks: 8, BatchTxNums: 8,
		DeferHistoryBuildWhileSyncing: true, MaxDeferredHistoryBlocks: 100, MaxBusyDeferredHistoryBlocks: 200,
		SyncBuildReady: func() bool { return false }, CatchupBuildMinInterval: time.Minute, CatchupHeavyWorkCooldown: 3 * time.Second,
		HistoryPressureHotBytes: 100, HistoryPressureFreeBytes: 80, MinHistoryBuildFreeBytes: 40,
	})
}

func TestColdPressureUsesAvailableMeasurementsAndKeepsBoundedRecovery(t *testing.T) {
	r := pressureFixture(t)
	p := HistoryPressure{HotHistoryBytes: 100, FreeBytes: 100, HotHistoryBytesAvailable: false, FreeBytesAvailable: false}
	r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) { return p, nil }
	deferred, err := r.OnePass()
	if err != nil || !deferred.HistoryDeferred || deferred.Built {
		t.Fatalf("unknown measurement: %+v %v", deferred, err)
	}
	p.HotHistoryBytesAvailable = true
	built, err := r.OnePass()
	if err != nil {
		t.Fatal(err)
	}
	if !built.Built || !built.HistoryForcedBusy || !built.HistoryPressureActive || built.HistoryBatchBlocks != 2 || built.HistoryBatchTxNums != 2 || built.HistoryMinRecovery < 3*time.Second || built.HistoryMinRecovery > 30*time.Second {
		t.Fatalf("pressure admission: %+v", built)
	}
	limited, err := r.OnePass()
	if err != nil || limited.Built || !limited.HistoryRateLimited || limited.HistoryRetryAfter <= 0 {
		t.Fatalf("pressure retry spin: %+v %v", limited, err)
	}
}

func TestColdPressureKeepsHeavyLeaseAndHardReserve(t *testing.T) {
	r := pressureFixture(t)
	p := HistoryPressure{HotHistoryBytes: 100, FreeBytes: 39, HotHistoryBytesAvailable: true, FreeBytesAvailable: true}
	r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) { return p, nil }
	called := 0
	result, err := r.OnePassWithMaintenanceContext(context.Background(), func(_ context.Context, result PassResult) error {
		called++
		if !result.HistorySpaceDeferred {
			t.Fatal("missing reserve signal")
		}
		return nil
	})
	if err != nil || called != 1 || result.Built || result.HistoryBuildAttempted || !result.HistorySpaceDeferred || result.HistoryRetryAfter <= 0 {
		t.Fatalf("reserve: %+v %v calls%d", result, err, called)
	}
	if _, err := os.Stat(r.cfg.Dir + "/" + ManifestFile); !os.IsNotExist(err) {
		t.Fatalf("reserve created manifest: %v", err)
	}
	p.FreeBytes = 40 // Inclusive admission boundary, still under soft pressure.
	r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGate()
	release, ok := r.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		t.Fatal("lease setup")
	}
	defer release()
	result, err = r.OnePass()
	if err != nil || result.Built || !result.HistoryGateDeferred || result.HistoryBuildAttempted {
		t.Fatalf("pressure bypassed lease: %+v %v", result, err)
	}
}

func TestColdPressureProbeAndMaintenanceCancellationStopLaterWork(t *testing.T) {
	r := pressureFixture(t)
	probeCtx, stopProbe := context.WithCancel(context.Background())
	r.cfg.HistoryPressureProbe = func(ctx context.Context) (HistoryPressure, error) { stopProbe(); return HistoryPressure{}, ctx.Err() }
	called := false
	if _, err := r.OnePassWithMaintenanceContext(probeCtx, func(context.Context, PassResult) error { called = true; return nil }); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("probe cancellation: %v called%v", err, called)
	}
	r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) {
		return HistoryPressure{HotHistoryBytes: 100, HotHistoryBytesAvailable: true}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	result, err := r.OnePassWithMaintenanceContext(ctx, func(_ context.Context, p PassResult) error {
		if !p.Built {
			t.Fatal("maintenance ran before publication")
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || !result.Built || result.Compaction.Merged || result.LatestBuilt || result.HistoryMinRecovery != time.Minute || result.HistoryRetryAfter <= 0 {
		t.Fatalf("failed pressure work recovery: %+v %v", result, err)
	}
	if result.BeforeMergeDuration <= 0 {
		t.Fatal("maintenance work not accounted")
	}
	next, err := r.OnePass()
	if err != nil || next.Built || !next.HistoryRateLimited {
		t.Fatalf("failed pressure retry spun: %+v %v", next, err)
	}
}

func TestPressureHistoryRecoveryClampsAndCountsMaintenance(t *testing.T) {
	r := pressureFixture(t)
	for _, tc := range []struct{ work, want time.Duration }{{0, time.Minute}, {time.Millisecond, 3 * time.Second}, {10 * time.Second, 10 * time.Second}, {time.Hour, 30 * time.Second}} {
		if got := r.pressureHistoryRecovery(tc.work); got != tc.want {
			t.Fatalf("work %v got %v want%v", tc.work, got, tc.want)
		}
	}
	if got := forcedBusyHistoryWorkDuration(PassResult{BuildDuration: time.Second, BeforeMergeDuration: 2 * time.Second, CompactionDuration: 3 * time.Second}); got != 6*time.Second {
		t.Fatal(got)
	}
}

func TestColdCompactionPassLimitYieldsWithReadableCoverage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	dir := t.TempDir()
	var refs []SegmentRef
	for n := uint64(1); n <= 4; n++ {
		writeColdBuilderChange(t, db, coldBuilderOwner(1), n, n, "previous")
		writeColdBuilderCanonicalBlock(t, db, n)
		built, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, n, n, fmt.Sprintf("history/state-domain-change-%d-%d.seg", n, n))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	if err := PublishManifest(dir, NewManifest(1, 4, refs)); err != nil {
		t.Fatal(err)
	}
	r := NewRunner(&coldBuilderChain{db: db, solidified: 5}, Config{Dir: dir, Enabled: true, HistoryWindow: 1, CompactMaxSteps: 2, MaxCompactionPasses: 1})
	result, err := r.OnePass()
	if err != nil || result.Compaction.MergePasses != 1 {
		t.Fatalf("bounded merge: %+v %v", result, err)
	}
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var segments, records int
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentHistory {
			segments++
			if err := cfg.IterateHistoryRange(dir, manifest, ref, 1, 4, func(*rawdb.StateDomainChange) (bool, error) { records++; return true, nil }); err != nil {
				t.Fatal(err)
			}
		}
	}
	if segments != 3 || records != 4 {
		t.Fatalf("limited merge lost coverage or drained: segments=%d records=%d", segments, records)
	}
}
