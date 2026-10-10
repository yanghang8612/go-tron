package snapshots

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type initialTestChain struct {
	*coldBuilderChain
	begin, complete, cleanup int
	beginErr                 error
}

func (c *initialTestChain) BeginInitialCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error) {
	c.begin++
	if c.beginErr != nil {
		return rawdb.CommitmentBranchRotation{}, false, c.beginErr
	}
	return *c.rotation, true, nil
}
func (c *initialTestChain) CompleteInitialCommitmentBranchRotation(ctx context.Context, rotation rawdb.CommitmentBranchRotation, proof *VerifiedCommitmentBranchBase) error {
	c.complete++
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := proof.Recheck(rotation); err != nil {
		return err
	}
	db := c.db.(ethdb.KeyValueStore)
	if err := rawdb.WriteCommitmentBranchBase(db, rawdb.CommitmentBranchBase{Generation: rotation.Generation, SnapshotTxNum: rotation.SnapshotTxNum, Root: rotation.Root, BlockNum: rotation.BlockNum, BlockHash: rotation.BlockHash}); err != nil {
		return err
	}
	if err := rawdb.DeleteCommitmentBranchRotation(db); err != nil {
		return err
	}
	return rawdb.DeleteCommitmentBranches(db)
}
func (c *initialTestChain) CleanupAcceptedInitialCommitmentBranchBase(ctx context.Context, proof *VerifiedCommitmentBranchBase) (bool, error) {
	c.cleanup++
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := proof.Recheck(proof.Rotation()); err != nil {
		return false, err
	}
	return true, rawdb.DeleteCommitmentBranches(c.db.(ethdb.KeyValueStore))
}
func initialTestConfig(dir string) Config {
	return Config{Dir: dir, Enabled: true, CommitmentBaseMode: CommitmentBaseInitialOnly, HistoryCatchupMode: HistoryCatchupThroughput, LatestBuildBlocks: 1, DeferLatestBuildWhileSyncing: true, HeavyWorkGate: maintenance.NewHeavyWorkGate(), MinHistoryBuildFreeBytes: 1 << 30, InitialCommitmentBaseReserveBytes: 1 << 30,
		HistoryLoadProbe: func() maintenance.StoragePressure {
			return maintenance.StoragePressure{Available: true, SampledAt: time.Now(), DeviceAvailable: true, DeviceSampledAt: time.Now(), DeviceAwait: time.Millisecond}
		},
		HistoryReadResourceProbe: func() HistoryReadResources {
			return HistoryReadResources{Available: true, SampledAt: time.Now(), IdleCoresMilli: 2000, MemoryAvailableBytes: 4 << 30}
		},
		HistoryPressureProbe: func(context.Context) (HistoryPressure, error) {
			return HistoryPressure{FreeBytesAvailable: true, FreeBytes: 10 << 30}, nil
		},
	}
}

func TestInitialCommitmentRuntimePrecedesHistoryAndSuppressesLatestAfterRestart(t *testing.T) {
	dir, db, rotation, _ := initialBranchFixture(t)
	chain := &initialTestChain{coldBuilderChain: &coldBuilderChain{db: db, solidified: 10, syncRemaining: 1_000_000, syncRemainingOK: true, rotation: &rotation}}
	cfg := initialTestConfig(dir)
	runner := NewRunner(chain, cfg)
	called := 0
	result, err := runner.OnePassWithMaintenanceContext(context.Background(), func(context.Context, PassResult) error { called++; return nil })
	if err != nil || chain.begin != 1 || chain.complete != 1 || called != 1 || result.HistoryBuildAttempted {
		t.Fatalf("initial result=%+v begin=%d complete=%d callback=%d err=%v", result, chain.begin, chain.complete, called, err)
	}
	if !runner.initialCommitmentDone {
		t.Fatal("initial task did not finish")
	}
	for _, syncing := range []bool{true, false} {
		chain.syncRemainingOK = syncing
		for _, r := range []*Runner{runner, NewRunner(chain, cfg)} {
			built, deferred, err := r.latestPassWithStatusContext(context.Background())
			if built || !deferred || err != nil {
				t.Fatalf("general latest allowed syncing=%v: %v %v %v", syncing, built, deferred, err)
			}
			attempted, _, err := r.initialCommitmentPass(context.Background())
			if attempted || err != nil {
				t.Fatalf("finished initial task restarted: %v %v", attempted, err)
			}
		}
	}
	if chain.begin != 1 || chain.rotationBegin != 0 {
		t.Fatal("second generation started")
	}
}

func TestInitialCommitmentAdmissionRechecksAndPreservesHistoryOpportunity(t *testing.T) {
	for _, fault := range []string{"unknown", "stale", "memory", "space", "after-lease", "busy-gate"} {
		t.Run(fault, func(t *testing.T) {
			dir, db, rotation, _ := initialBranchFixture(t)
			chain := &initialTestChain{coldBuilderChain: &coldBuilderChain{db: db, rotation: &rotation}}
			cfg := initialTestConfig(dir)
			switch fault {
			case "unknown":
				cfg.HistoryReadResourceProbe = func() HistoryReadResources { return HistoryReadResources{} }
			case "stale":
				cfg.HistoryReadResourceProbe = func() HistoryReadResources {
					return HistoryReadResources{Available: true, SampledAt: time.Now().Add(-time.Minute)}
				}
			case "memory":
				cfg.HistoryReadResourceProbe = func() HistoryReadResources {
					return HistoryReadResources{Available: true, SampledAt: time.Now(), IdleCoresMilli: 2000, MemoryAvailableBytes: 1}
				}
			case "space":
				cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) { return HistoryPressure{}, nil }
			case "after-lease":
				calls := 0
				old := cfg.HistoryReadResourceProbe
				cfg.HistoryReadResourceProbe = func() HistoryReadResources {
					calls++
					if calls > 1 {
						return HistoryReadResources{}
					}
					return old()
				}
			case "busy-gate":
				release, ok := cfg.HeavyWorkGate.TryAcquire()
				if !ok {
					t.Fatal("gate")
				}
				defer release()
			}
			runner := NewRunner(chain, cfg)
			attempted, _, err := runner.initialCommitmentPass(context.Background())
			if attempted || err != nil || chain.begin != 0 || !runner.initialCommitmentNotBefore.IsZero() {
				t.Fatalf("rejected work attempted=%v begin=%d err=%v", attempted, chain.begin, err)
			}
			if _, ok, err := rawdb.ReadCommitmentBranchBase(db); err != nil || ok {
				t.Fatal("admission published base")
			}
		})
	}
	// Failed initial work must not install its long recovery on the shared gate
	// or suppress the next ordinary history/maintenance pass.
	dir, db, rotation, _ := initialBranchFixture(t)
	chain := &initialTestChain{coldBuilderChain: &coldBuilderChain{db: db, rotation: &rotation}, beginErr: errors.New("begin injected")}
	runner := NewRunner(chain, initialTestConfig(dir))
	attempted, _, err := runner.initialCommitmentPass(context.Background())
	if !attempted || err == nil || runner.initialCommitmentNotBefore.IsZero() {
		t.Fatal("failed work retry missing")
	}
	if !runner.cfg.HeavyWorkGate.CanTryAcquire() {
		t.Fatal("initial retry blocked shared gate")
	}
	if runner.historyNotBefore.Load() != 0 {
		t.Fatal("initial retry delayed history")
	}
	called := false
	_, err = runner.OnePassWithMaintenanceContext(context.Background(), func(context.Context, PassResult) error { called = true; return nil })
	if err != nil || !called || chain.begin != 1 {
		t.Fatalf("normal maintenance blocked by retry: callback=%v begin=%d err=%v", called, chain.begin, err)
	}
}

func TestInitialCommitmentMonitorCancellationAndJoin(t *testing.T) {
	for _, block := range []bool{false, true} {
		t.Run(map[bool]string{false: "pressure-cancel", true: "stop-blocked-probe"}[block], func(t *testing.T) {
			cfg := initialTestConfig(t.TempDir())
			var calls atomic.Int32
			entered := make(chan struct{})
			cfg.HistoryPressureProbe = func(ctx context.Context) (HistoryPressure, error) {
				if calls.Add(1) == 1 {
					close(entered)
				}
				if block {
					<-ctx.Done()
					return HistoryPressure{}, ctx.Err()
				}
				return HistoryPressure{FreeBytesAvailable: true, FreeBytes: 0}, nil
			}
			runner := NewRunner(nil, cfg)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			join := runner.initialCommitmentMonitor(ctx, cancel, time.Millisecond)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("probe not entered")
			}
			if !block {
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
					t.Fatal("pressure did not cancel")
				}
			}
			joined := make(chan struct{})
			go func() { join(); close(joined) }()
			select {
			case <-joined:
			case <-time.After(time.Second):
				t.Fatal("monitor join blocked")
			}
			if block && ctx.Err() != nil {
				t.Fatal("intentional stop cancelled job")
			}
		})
	}
}

func TestInitialCommitmentConfigCanonicalAndFailClosed(t *testing.T) {
	cfg := initialTestConfig(t.TempDir())
	cfg.CommitmentBaseMode = " INITIAL-ONLY "
	cfg = cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.CommitmentBaseMode != CommitmentBaseInitialOnly {
		t.Fatal("mode not normalized")
	}
	cfg.HistoryLoadProbe = nil
	if err := cfg.validate(); err == nil {
		t.Fatal("missing admission dependency accepted")
	}
	cfg = initialTestConfig(t.TempDir())
	cfg.Enabled = false
	if err := cfg.validate(); err == nil {
		t.Fatal("disabled lifecycle accepted")
	}
	if _, err := ParseCommitmentBaseMode("periodic"); err == nil {
		t.Fatal("unknown policy accepted")
	}
}
