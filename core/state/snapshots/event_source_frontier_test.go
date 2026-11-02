package snapshots

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func independentEventFixture(t *testing.T) (*Runner, *coldBuilderChain) {
	t.Helper()
	db := rawdb.NewMemoryChainDB()
	for n := uint64(1); n <= 4; n++ {
		block, infos := coldBuilderEventLogBlock(t, n, []*corepb.TransactionInfo_Log{{Address: eventLogTestAddress(0x51), Data: []byte{byte(n)}}})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, n, infos); err != nil {
			t.Fatal(err)
		}
	}
	hash, ok, err := rawdb.ReadBlockHashByNumberStrict(db, 4)
	if err != nil || !ok {
		t.Fatal(hash, ok, err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 4, hash); err != nil {
		t.Fatal(err)
	}
	chain := &coldBuilderChain{db: db, solidified: 4, syncRemaining: 1000, syncRemainingOK: true}
	r := NewRunner(chain, Config{Dir: t.TempDir(), Enabled: true, HistoryWindow: 100,
		BuildEventLogs: true, EventLogVersion: EventLogSegmentV4Version,
		DeferDerivedSidecarsWhileSyncing: true, DeferLatestBuildWhileSyncing: true,
		BuildEventLogsWhileSyncing: true, EventLogIndependentFrontier: true,
		SyncEventLogCatchupBlocks: 2, SyncEventLogTargetBlock: func() (uint64, bool, bool) { return 3, true, false },
	})
	t.Cleanup(r.cancel)
	return r, chain
}

func TestIndependentEventFrontierLeadsStateAndHandsOff(t *testing.T) {
	r, chain := independentEventFixture(t)
	for pass := 0; pass < 2; pass++ {
		result, err := r.OnePass()
		if err != nil || !result.EventLogBuilt || result.Built || !result.DerivedSidecarCatchup {
			t.Fatalf("pass %d: %+v %v", pass, result, err)
		}
		if result.EventLogFreezerHandoff != (pass == 1) {
			t.Fatalf("handoff %d: %+v", pass, result)
		}
		if _, ok, err := rawdb.ReadStageProgress(chain.db, rawdb.StageSnapshotBuild); err != nil || ok {
			t.Fatalf("event work manufactured state progress: %v %v", ok, err)
		}
		if result.Manifest.VisibleTxEnd != 0 {
			t.Fatal("event-only manifest changed history coverage")
		}
	}
	manager, err := OpenManager(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var seen uint64
	if err := manager.IterateEventLogs(1, 3, EventLogFilter{}, func(row EventLog) (bool, error) {
		seen++
		if row.BlockNum != seen || len(row.Log.Data) != 1 || row.Log.Data[0] != byte(seen) {
			t.Fatalf("wrong event: %+v", row)
		}
		return true, nil
	}); err != nil || seen != 3 {
		t.Fatal(seen, err)
	}
	if covered, err := manager.EventLogIndexedRangeCovered(1, 4); err != nil || covered {
		t.Fatal("escaped next-segment target", covered, err)
	}
	if err := rawdb.DeleteStageProgress(chain.db.(*rawdb.ChainDB), rawdb.StageSnapshotEventLogBuild); err != nil {
		t.Fatal(err)
	}
	restarted := NewRunner(chain, r.cfg)
	t.Cleanup(restarted.cancel)
	result, err := restarted.OnePass()
	if err != nil || result.EventLogBuilt {
		t.Fatalf("manifest-first recovery rebuilt: %+v %v", result, err)
	}
	if n, ok, err := rawdb.ReadVerifiedStageProgressBlock(chain.db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || n != 3 {
		t.Fatal(n, ok, err)
	}
}

func TestIndependentEventFrontierRequiresVerifiedFinishAndSource(t *testing.T) {
	for _, fault := range []string{"missing-finish", "unbound-finish", "wrong-finish", "missing-receipt", "solid-cap", "finish-cap"} {
		t.Run(fault, func(t *testing.T) {
			r, chain := independentEventFixture(t)
			switch fault {
			case "missing-finish":
				_ = rawdb.DeleteStageProgress(chain.db.(*rawdb.ChainDB), rawdb.StageFinish)
			case "unbound-finish":
				_ = rawdb.WriteStageProgress(chain.db.(*rawdb.ChainDB), rawdb.StageFinish, 4)
			case "wrong-finish":
				_ = rawdb.WriteStageProgressWithHash(chain.db.(*rawdb.ChainDB), rawdb.StageFinish, 4, common.Hash{0x99})
			case "missing-receipt":
				_ = rawdb.DeleteTransactionInfosByBlock(chain.db.(*rawdb.ChainDB), 2)
			case "solid-cap":
				chain.solidified = 1
			case "finish-cap":
				h, _, _ := rawdb.ReadBlockHashByNumberStrict(chain.db, 1)
				_ = rawdb.WriteStageProgressWithHash(chain.db.(*rawdb.ChainDB), rawdb.StageFinish, 1, h)
			}
			result, err := r.OnePass()
			if fault == "solid-cap" || fault == "finish-cap" {
				if err != nil || !result.EventLogBuilt || result.EventLogFreezerHandoff {
					t.Fatalf("cap: %+v %v", result, err)
				}
				if n, _, _ := rawdb.ReadStageProgress(chain.db, rawdb.StageSnapshotEventLogBuild); n != 1 {
					t.Fatal(n)
				}
				return
			}
			if fault != "missing-finish" && err == nil {
				t.Fatalf("accepted %s: %+v", fault, result)
			}
			if _, err := os.Stat(r.cfg.Dir + "/" + ManifestFile); !os.IsNotExist(err) {
				t.Fatal("invalid source published manifest", err)
			}
			if _, ok, _ := rawdb.ReadStageProgress(chain.db, rawdb.StageSnapshotEventLogBuild); ok {
				t.Fatal("invalid source advanced event stage")
			}
		})
	}
}

type eventFrontierMutatingChain struct {
	*coldBuilderChain
	calls int
}

func (c *eventFrontierMutatingChain) CanonicalBlockHashStrict(n uint64) (common.Hash, bool, error) {
	if n == 2 {
		c.calls++
		if c.calls > 1 {
			return common.Hash{0xfe}, true, nil
		}
	}
	return c.coldBuilderChain.CanonicalBlockHashStrict(n)
}
func TestIndependentEventFrontierRejectsChangedPublicationAnchor(t *testing.T) {
	r, chain := independentEventFixture(t)
	r.chain = &eventFrontierMutatingChain{coldBuilderChain: chain}
	result, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "changed") || result.DerivedSidecarCatchup {
		t.Fatalf("source replacement: %+v %v", result, err)
	}
	if _, err := os.Stat(r.cfg.Dir + "/" + ManifestFile); !os.IsNotExist(err) {
		t.Fatal("changed source activated", err)
	}
}

func TestIndependentEventFrontierPreservesGateAndBudget(t *testing.T) {
	r, _ := independentEventFixture(t)
	r.cfg.HistoryCatchupMode = HistoryCatchupThroughput
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure {
		return maintenance.StoragePressure{Available: true, SampledAt: time.Now(), L0CompactionThreshold: 8, L0StopWritesThreshold: 64, MemTableCount: 1, MemTableStopWritesThreshold: 8}
	}
	deadline := time.Now().Add(time.Minute).UnixNano()
	r.historyNotBefore.Store(deadline)
	result, err := r.OnePass()
	if err != nil || !result.HistoryRateLimited || result.EventLogBuilt || r.historyNotBefore.Load() != deadline {
		t.Fatalf("recovery: %+v %v", result, err)
	}
	r.historyNotBefore.Store(0)
	r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGate()
	release, ok := r.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		t.Fatal("test gate")
	}
	result, err = r.OnePass()
	release()
	if err != nil || !result.HistoryGateDeferred || result.EventLogBuilt {
		t.Fatalf("gate: %+v %v", result, err)
	}
	// Failure-density feedback retains its bounded retry even though event
	// authority no longer comes from the history cursor.
	r.historyLoad.eventFailureBlocks = 1
	result, err = r.OnePass()
	if err != nil || !result.EventLogBuilt || result.historyEventBatchBlocks != 1 {
		t.Fatalf("bounded retry: %+v %v", result, err)
	}
}

func TestIndependentEventFutureFilesRequireCanonicalCompleteCut(t *testing.T) {
	r, chain := independentEventFixture(t)
	result, err := r.OnePass()
	if err != nil || !result.EventLogBuilt {
		t.Fatal(result, err)
	}
	manager, err := OpenManager(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetEventLogCanonicalVerifier(NewCanonicalEventLogVerifier(chain))
	// Even a request for the known prefix must not trust an incomplete whole
	// ref: a filtered empty candidate set is not proof that future logs are absent.
	chain.solidified = 1
	filter := EventLogFilter{Addresses: []common.Address{{0xff}}}
	checkUnavailable := func(m *Manager) {
		t.Helper()
		if covered, err := m.EventLogRangeCovered(1, 1); err != nil || covered {
			t.Fatal("future plain coverage", covered, err)
		}
		if covered, err := m.EventLogIndexedRangeCovered(1, 1); err != nil || covered {
			t.Fatal("future indexed coverage", covered, err)
		}
		if covered, err := m.EventLogRangeCoveredForFilter(1, 1, filter); err != nil || covered {
			t.Fatal("future filtered coverage", covered, err)
		}
		if covered, err := m.IterateCoveredEventLogs(1, 1, filter, func(EventLog) (bool, error) { t.Fatal("future row delivered"); return true, nil }); err != nil || covered {
			t.Fatal("future covered iteration", covered, err)
		}
		if err := m.IterateEventLogs(1, 1, filter, func(EventLog) (bool, error) { return true, nil }); err == nil {
			t.Fatal("future empty lookup incorrectly succeeded")
		}
	}
	checkUnavailable(manager)
	pinned, release, err := manager.PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	checkUnavailable(pinned)
	chain.solidified = 4
	if covered, err := manager.EventLogRangeCovered(1, 2); err != nil || !covered {
		t.Fatal("replayed source not activated", covered, err)
	}
	var seen int
	if err := manager.IterateEventLogs(1, 2, EventLogFilter{}, func(EventLog) (bool, error) { seen++; return true, nil }); err != nil || seen != 2 {
		t.Fatal(seen, err)
	}
	// An independently signed file must never be accepted on a different
	// local fork merely because its checksums and range are intact.
	h4, _, _ := rawdb.ReadBlockHashByNumberStrict(chain.db, 4)
	chain.canonicalHashes = map[uint64]common.Hash{4: h4, 2: {0x77}}
	if covered, err := manager.EventLogRangeCovered(1, 1); err == nil || covered {
		t.Fatal("mismatched terminal source trusted", covered, err)
	}
}

func TestIndependentEventInstallCapsProgressAndRetainsAheadFiles(t *testing.T) {
	r, chain := independentEventFixture(t)
	for i := 0; i < 2; i++ {
		if result, err := r.OnePass(); err != nil || !result.EventLogBuilt {
			t.Fatal(result, err)
		}
	}
	manifest, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	// This is the real final-publication stage planner's reader surface,
	// including a restored HEAD held in unpublishedRestore's anchor overlay.
	head := rawdb.NewStateCommitmentHead()
	head.BlockNum = 2
	head.BlockHash, _, _ = rawdb.ReadBlockHashByNumberStrict(chain.db, 2)
	head.TxNum = 7
	if err := rawdb.WriteStateCommitmentHead(chain.db.(*rawdb.ChainDB), head); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.DeleteStageProgress(chain.db.(*rawdb.ChainDB), rawdb.StageSnapshotEventLogBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteSnapshotInstallProgress(chain.db.(*rawdb.ChainDB), manifest); err != nil {
		t.Fatal(err)
	}
	if n, ok, err := rawdb.ReadVerifiedStageProgressBlock(chain.db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || n != 2 {
		t.Fatal("install escaped authenticated HEAD", n, ok, err)
	}
	loaded, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil || len(eventLogRefs(loaded)) != 2 {
		t.Fatal("ahead metadata discarded", err)
	}
	for _, ref := range eventLogRefs(loaded) {
		if _, err := os.Stat(r.cfg.Dir + "/" + ref.Path); err != nil {
			t.Fatal("ahead file removed", err)
		}
	}
	// Source validation is mandatory even when the install cut is known.
	for i := range manifest.Segments {
		if manifest.Segments[i].Kind == SegmentEventLog && manifest.Segments[i].ToTxNum == 2 {
			manifest.Segments[i].SourceBlockHash = common.Hash{0xee}.Hex()
		}
	}
	if _, err := WriteSnapshotInstallProgress(chain.db.(*rawdb.ChainDB), manifest); err == nil {
		t.Fatal("wrong fork event anchor accepted on install")
	}
}

func TestIndependentEventRejectsMalformedOrMissingAnchors(t *testing.T) {
	for _, raw := range []string{"0x", common.Hash{}.Hex(), strings.Repeat("a", 66), "0x" + strings.Repeat("z", 64)} {
		if _, err := parseEventSourceBlockHash(raw); err == nil {
			t.Fatal("accepted source hash", raw)
		}
	}
	r, chain := independentEventFixture(t)
	result, err := r.OnePass()
	if err != nil || !result.EventLogBuilt {
		t.Fatal(result, err)
	}
	manifest := result.Manifest
	for i := range manifest.Segments {
		manifest.Segments[i].SourceBlockHash = ""
	}
	manager, err := OpenPinnedManager(r.cfg.Dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetEventLogCanonicalVerifier(NewCanonicalEventLogVerifier(chain))
	if covered, err := manager.EventLogRangeCovered(1, 1); err != nil || covered {
		t.Fatal("missing source hash grants authority", covered, err)
	}
}

func TestIndependentEventHistoryBatchPreservesPreparedSuffix(t *testing.T) {
	r, chain := independentEventFixture(t)
	for i := 0; i < 2; i++ {
		if result, err := r.OnePass(); err != nil || !result.EventLogBuilt {
			t.Fatal(result, err)
		}
	}
	before, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	for n := uint64(1); n <= 4; n++ {
		hash, _, _ := rawdb.ReadBlockHashByNumberStrict(chain.db, n)
		if err := rawdb.WriteStateTxRange(chain.db.(*rawdb.ChainDB), n, hash, n, n); err != nil {
			t.Fatal(err)
		}
		change := &rawdb.StateDomainChange{BlockNum: n, BlockHash: hash, TxNum: n, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: coldBuilderOwner(0x11), Domain: kvdomains.SystemReward, Key: []byte{byte(n)}, PrevExists: true, Prev: []byte("before")}
		if err := rawdb.WriteStateDomainChangeBlockRows(chain.db.(*rawdb.ChainDB), []*rawdb.StateDomainChange{change}); err != nil {
			t.Fatal(err)
		}
	}
	r.cfg.HistoryWindow = 1
	r.cfg.BatchBlocks = 1
	result, err := r.OnePass()
	if err != nil || !result.Built || result.EventLogBuilt || result.ToBlock != 1 {
		t.Fatalf("short state batch %+v %v", result, err)
	}
	if a, b := eventLogRefs(before), eventLogRefs(result.Manifest); len(a) != len(b) || a[0] != b[0] || a[1] != b[1] {
		t.Fatal("state batch retired prepared event suffix")
	}
	manager, err := OpenManager(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetEventLogCanonicalVerifier(NewCanonicalEventLogVerifier(chain))
	if covered, err := manager.EventLogIndexedRangeCovered(1, 3); err != nil || !covered {
		t.Fatal("prepared suffix became unavailable", covered, err)
	}
}

func TestIndependentEventRepairBeyondDemandRequiresOfflineRebuild(t *testing.T) {
	r, chain := independentEventFixture(t)
	built, err := NewAggregator(r.cfg.Dir).BuildEventLogsWithBuildOptions(chain.db.(*rawdb.ChainDB), 1, 4, EventLogBuildOptions{Version: EventLogSegmentV4Version})
	if err != nil {
		t.Fatal(err)
	}
	manifest := *built.Manifest
	manifest.Segments = eventLogRefs(built.Manifest) // Missing the whole companion.
	if err := PublishManifest(r.cfg.Dir, &manifest); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.DeleteStageProgress(chain.db.(*rawdb.ChainDB), rawdb.StageSnapshotEventLogBuild); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(r.cfg.Dir + "/" + ManifestFile)
	if err != nil {
		t.Fatal(err)
	}
	attempts := eventReplayMetrics.attempts.Snapshot().Count()
	files, err := os.ReadDir(r.cfg.Dir + "/log")
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.OnePass()
	if !errors.Is(err, ErrEventLogCompanionRepairRequired) || !strings.Contains(err.Error(), "requested [1,3]") || !strings.Contains(err.Error(), "repair [1,4]") {
		t.Fatal("unsupported repair did not fail explicitly", result, err)
	}
	if result.EventLogBuilt || result.HistoryEventAttempted || result.DerivedSidecarCatchup || eventReplayMetrics.attempts.Snapshot().Count() != attempts {
		t.Fatal("unsupported repair started a source build", result)
	}
	after, readErr := os.ReadFile(r.cfg.Dir + "/" + ManifestFile)
	if readErr != nil || !bytes.Equal(before, after) {
		t.Fatal("unsupported repair changed manifest", readErr)
	}
	afterFiles, readErr := os.ReadDir(r.cfg.Dir + "/log")
	if readErr != nil || len(afterFiles) != len(files) {
		t.Fatal("unsupported repair wrote files", readErr)
	}
	if n, ok, readErr := rawdb.ReadStageProgress(chain.db, rawdb.StageSnapshotEventLogBuild); readErr != nil || ok {
		t.Fatal("unsupported repair advanced stage", n, ok, readErr)
	}
}
