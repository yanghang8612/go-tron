package snapshots

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Publish real history while event sidecars are deferred, then resume the
// manifest with sync-critical event catch-up. This exercises the independent
// event path rather than the event files attached to a new history batch.
func historyEventBudgetFixture(t *testing.T) (*Runner, *coldBuilderChain, []byte, common.Hash) {
	t.Helper()
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x91)
	address, topic := eventLogTestAddress(0x92), common.Hash{0x93}
	for blockNum := uint64(1); blockNum <= 4; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		block, infos := coldBuilderEventLogBlock(t, blockNum, []*corepb.TransactionInfo_Log{{
			Address: address, Topics: [][]byte{topic[:]}, Data: []byte{byte(blockNum)},
		}})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			t.Fatal(err)
		}
	}
	chain := &coldBuilderChain{db: db, solidified: 5, syncRemaining: 1000, syncRemainingOK: true}
	namespace := normalizeColdSnapshotMetricNamespace("test/event-budget/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	cfg := Config{
		Dir: dir, Enabled: true, HistoryWindow: 1, BatchBlocks: 4,
		DeferLatestBuildWhileSyncing: true, DeferDerivedSidecarsWhileSyncing: true,
		BuildEventLogs: true, EventLogVersion: EventLogSegmentV4Version,
		MetricsNamespace: namespace + "bootstrap/",
	}
	bootstrap := NewRunner(chain, cfg)
	t.Cleanup(bootstrap.cancel)
	built, err := bootstrap.OnePass()
	if err != nil || !built.Built || built.ToBlock != 4 || built.EventLogBuilt {
		t.Fatalf("establish history/event gap: %+v %v", built, err)
	}
	cfg.MetricsNamespace = namespace + "runtime/"
	cfg.HistoryCatchupMode = HistoryCatchupThroughput
	cfg.BuildEventLogsWhileSyncing = true
	cfg.SyncEventLogCatchupBlocks = 65536
	cfg.SyncEventLogTargetBlock = func() (uint64, bool, bool) { return 4, true, false }
	cfg.HistoryLoadProbe = func() maintenance.StoragePressure {
		return maintenance.StoragePressure{Available: true, SampledAt: time.Now(),
			L0CompactionThreshold: 8, L0StopWritesThreshold: 64,
			MemTableCount: 1, MemTableStopWritesThreshold: 8}
	}
	runner := NewRunner(chain, cfg)
	t.Cleanup(runner.cancel)
	return runner, chain, address, topic
}

func assertHistoryEventBudgetQuery(t *testing.T, runner *Runner, address []byte, topic common.Hash) {
	t.Helper()
	manager, err := OpenManager(runner.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	covered, err := manager.EventLogIndexedRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("published event range missing: %v %v", covered, err)
	}
	var rows []EventLog
	err = manager.IterateEventLogs(1, 1, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(address)}, Topics: [][]common.Hash{{topic}},
	}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	})
	if err != nil || len(rows) != 1 || rows[0].BlockNum != 1 || !bytes.Equal(rows[0].Log.GetData(), []byte{1}) {
		t.Fatalf("actual event query: %+v %v", rows, err)
	}
}

func assertHistoryEventCounters(t *testing.T, runner *Runner, wantCatchups uint64) {
	t.Helper()
	s := runner.Snapshot()
	if s.DerivedSidecarCatchups != wantCatchups || s.SegmentsBuilt != 0 ||
		s.HistoryAcceleratedBuilds != 0 || s.ForcedBusyAttempts != 0 || s.ForcedBusyBuilds != 0 ||
		s.LastBatchBlocks != 0 || s.LastBatchTxNums != 0 || runner.lastSuccessfulForcedAt.Load() != 0 {
		t.Fatalf("independent event work changed history-build counters: %+v", s)
	}
}

func TestHistoryEventBudgetHardPressureAndDeadline(t *testing.T) {
	runner, _, address, topic := historyEventBudgetFixture(t)
	probe := runner.cfg.HistoryLoadProbe
	blocked := true
	runner.cfg.HistoryLoadProbe = func() maintenance.StoragePressure {
		p := probe()
		p.WriteStalled = blocked
		return p
	}
	result, err := runner.OnePass()
	if err != nil || !result.HistoryLoadDeferred || result.HistoryEventAttempted || result.EventLogBuilt {
		t.Fatalf("event hard-pressure admission: %+v %v", result, err)
	}
	manifest, err := LoadProductionManifest(runner.cfg.Dir)
	if err != nil || len(eventLogRefs(manifest)) != 0 {
		t.Fatalf("hard pressure wrote event files: %+v %v", manifest, err)
	}
	assertHistoryEventCounters(t, runner, 0)
	blocked = false
	deadline := time.Now().Add(time.Minute).UnixNano()
	runner.historyNotBefore.Store(deadline)
	result, err = runner.OnePass()
	if err != nil || !result.HistoryRateLimited || result.HistoryEventAttempted || result.EventLogBuilt || runner.historyNotBefore.Load() != deadline {
		t.Fatalf("event bypassed/renewed existing recovery: %+v %v", result, err)
	}
	assertHistoryEventCounters(t, runner, 0)
	// Expire the deadline deterministically, without sleeping through recovery.
	runner.historyNotBefore.Store(time.Now().Add(-time.Second).UnixNano())
	result, err = runner.OnePass()
	if err != nil || !result.HistoryEventAttempted || !result.EventLogBuilt || result.Built || result.HistoryBuildAttempted || result.historyEventBatchBlocks != 1 {
		t.Fatalf("bounded event recovery: %+v %v", result, err)
	}
	assertHistoryEventCounters(t, runner, 1)
	assertHistoryEventBudgetQuery(t, runner, address, topic)
}

func TestHistoryEventBudgetCompletionSurvivesSyncBecomingIdle(t *testing.T) {
	runner, chain, address, topic := historyEventBudgetFixture(t)
	result, err := runner.OnePassWithDeferredMaintenanceContext(context.Background(), nil)
	if err != nil || !result.EventLogBuilt || !result.HistoryEventAttempted || !result.historyWasSyncing || !result.historyCompletionPending {
		t.Fatalf("deferred event handoff: %+v %v", result, err)
	}
	assertHistoryEventBudgetQuery(t, runner, address, topic)
	chain.syncRemainingOK = false
	// Account an outer operation after the event publication. The earlier
	// provisional deadline alone cannot satisfy this larger final recovery.
	started := time.Now().Add(-4 * time.Second)
	beforeComplete := time.Now()
	runner.CompleteHistoryMaintenance(&result, started, nil)
	if result.historyCompletionPending || runner.pendingMaintenanceID != 0 ||
		result.HistoryMaintenanceDuration < 4*time.Second || result.HistoryMinRecovery < 8*time.Second ||
		result.HistoryRetryDeadline.Before(beforeComplete.Add(8*time.Second)) {
		t.Fatalf("sync-idle transition skipped outer recovery: %+v", result)
	}
	if runner.historyLoad.event.blocks != 1 || runner.historyLoad.event.bytes == 0 || runner.historyLoad.event.work <= 0 {
		t.Fatalf("completed event density was not recorded: %+v", runner.historyLoad.event)
	}
	assertHistoryEventCounters(t, runner, 1)
	// The same recovery also protects the next independent sidecar pass when
	// its currently inactive sync state no longer labels it sync-critical.
	runner.lastLatestBuildBlock.Store(4)
	limited, err := runner.OnePass()
	if err != nil || !limited.HistoryRateLimited || limited.HistoryEventAttempted || limited.EventLogBuilt {
		t.Fatalf("inactive sidecar bypassed existing event recovery: %+v %v", limited, err)
	}
	assertHistoryEventCounters(t, runner, 1)
}

func TestHistoryEventBudgetDeferredCompletionFailure(t *testing.T) {
	runner, _, address, topic := historyEventBudgetFixture(t)
	result, err := runner.OnePassWithDeferredMaintenanceContext(context.Background(), nil)
	if err != nil || !result.HistoryEventAttempted || !result.EventLogBuilt || !result.historyCompletionPending {
		t.Fatalf("deferred event handoff: %+v %v", result, err)
	}
	if _, err := runner.OnePass(); !errors.Is(err, ErrHistoryMaintenancePending) {
		t.Fatalf("another pass entered before outer completion: %v", err)
	}
	beforeComplete := time.Now()
	runner.CompleteHistoryMaintenance(&result, beforeComplete.Add(-2*time.Second), errors.New("injected post-event catalog failure"))
	if result.historyCompletionPending || runner.pendingMaintenanceID != 0 ||
		result.HistoryMinRecovery < time.Minute || result.HistoryRetryDeadline.Before(beforeComplete.Add(time.Minute)) {
		t.Fatalf("post-event failure lacked final backoff: %+v", result)
	}
	// Completed output remains readable and useful to density estimation even
	// though the later outer operation failed.
	assertHistoryEventBudgetQuery(t, runner, address, topic)
	if runner.historyLoad.event.blocks != 1 || runner.historyLoad.event.bytes == 0 {
		t.Fatalf("outer failure discarded event density: %+v", runner.historyLoad.event)
	}
	assertHistoryEventCounters(t, runner, 1)
	deadline := runner.historyNotBefore.Load()
	runner.CompleteHistoryMaintenance(&result, time.Now().Add(-time.Hour), errors.New("duplicate completion"))
	if runner.historyNotBefore.Load() != deadline {
		t.Fatal("duplicate completion changed event recovery")
	}
	limited, err := runner.OnePass()
	if err != nil || !limited.HistoryRateLimited || limited.HistoryEventAttempted || limited.EventLogBuilt {
		t.Fatalf("event retry bypassed outer failure deadline: %+v %v", limited, err)
	}
	assertHistoryEventCounters(t, runner, 1)
}

func TestHistoryEventBudgetIdleAdmissionThenActiveCompletion(t *testing.T) {
	runner, chain, address, topic := historyEventBudgetFixture(t)
	chain.syncRemainingOK = false
	runner.historyLoad.syncSeenAt = time.Now().Add(-2 * time.Minute)
	runner.lastLatestBuildBlock.Store(4)
	result, err := runner.OnePassWithDeferredMaintenanceContext(context.Background(), nil)
	if err != nil || !result.EventLogBuilt || !result.HistoryEventAttempted || result.historyWasSyncing || !result.historyCompletionPending {
		t.Fatalf("idle event did not join outer completion: %+v %v", result, err)
	}
	assertHistoryEventBudgetQuery(t, runner, address, topic)
	chain.syncRemainingOK = true
	beforeComplete := time.Now()
	runner.CompleteHistoryMaintenance(&result, beforeComplete.Add(-4*time.Second), nil)
	if result.historyCompletionPending || runner.pendingMaintenanceID != 0 ||
		result.HistoryMinRecovery < 7*time.Second || result.HistoryRetryDeadline.Before(beforeComplete.Add(7*time.Second)) {
		t.Fatalf("sync resumption skipped event recovery: %+v", result)
	}
	assertHistoryEventCounters(t, runner, 1)
}

func TestHistoryEventBudgetWarmupPublishesOnlyOneBatch(t *testing.T) {
	runner, chain, address, topic := historyEventBudgetFixture(t)
	chain.syncRemainingOK = false
	result, err := runner.OnePassWithDeferredMaintenanceContext(context.Background(), nil)
	if err != nil || !result.HistoryEventAttempted || !result.EventLogBuilt ||
		!result.historyCompletionPending || result.historyEventBatchBlocks != 1 || !result.LatestDeferred {
		t.Fatalf("warmup event handoff: %+v %v", result, err)
	}
	manifest, err := LoadProductionManifest(runner.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	logs := eventLogRefs(manifest)
	if len(logs) != 1 || logs[0].FromTxNum != 1 || logs[0].ToTxNum != 1 {
		t.Fatalf("warmup invoked event construction twice or overbuilt: %+v", logs)
	}
	assertHistoryEventBudgetQuery(t, runner, address, topic)
	runner.CompleteHistoryMaintenance(&result, time.Now(), nil)
	assertHistoryEventCounters(t, runner, 1)
}
