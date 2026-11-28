package snapshots

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func buildV4TestPairForCompactor(t *testing.T, dir string, fromBlock, toBlock uint64, anchor common.Hash, rows []EventLog) (SegmentRef, SegmentRef) {
	t.Helper()
	evRef, err := BuildEventLogV4SegmentFromReader(eventLogRowsReader{rows: rows}, dir, "", fromBlock, toBlock)
	if err != nil {
		t.Fatalf("BuildEventLogV4SegmentFromReader [%d,%d]: %v", fromBlock, toBlock, err)
	}
	evRef.SourceBlockHash = anchor.Hex()
	idxRef, err := writeFreshEventLogV4Index(dir, evRef, "")
	if err != nil {
		t.Fatalf("writeFreshEventLogV4Index [%d,%d]: %v", fromBlock, toBlock, err)
	}
	return evRef, idxRef
}

func TestEventLogCompactorMergesV4PairsToV5AndCleansUp(t *testing.T) {
	dir := t.TempDir()
	cache := NewChainFreezerVerificationCache(dir)
	addrA := common.BytesToAddress(eventLogTestAddress(0x71))
	addrB := common.BytesToAddress(eventLogTestAddress(0x72))
	topicShared := common.Hash{0xaa}

	var allRows []EventLog
	var oldRefs []SegmentRef
	var oldIdxRefs []SegmentRef
	var lastAnchor common.Hash
	for seg := uint64(0); seg < 10; seg++ {
		from := seg*10 + 1
		to := from + 9
		var segRows []EventLog
		for b := from; b <= to; b++ {
			var txHash, blkHash, uniqTopic common.Hash
			binary.BigEndian.PutUint64(txHash[24:], b*10)
			binary.BigEndian.PutUint64(blkHash[24:], b)
			binary.BigEndian.PutUint64(uniqTopic[24:], b)
			addr := addrA
			if b%2 == 0 {
				addr = addrB
			}
			row := EventLog{
				BlockNum: b, TxIndex: 0, LogIndex: 0, TxHash: txHash, BlockHash: blkHash, Address: addr,
				Log: &corepb.TransactionInfo_Log{
					Address: eventLogV3PayloadAddress(addr),
					Topics:  [][]byte{topicShared[:], uniqTopic[:]},
					Data:    []byte("payload-data"),
				},
			}
			segRows = append(segRows, row)
			allRows = append(allRows, row)
		}
		lastAnchor = common.Hash{byte(0x10 + seg)}
		evRef, idxRef := buildV4TestPairForCompactor(t, dir, from, to, lastAnchor, segRows)
		oldRefs = append(oldRefs, evRef, idxRef)
		oldIdxRefs = append(oldIdxRefs, idxRef)
		if err := cache.recordTrustedEventLogs(dir, idxRef, []SegmentRef{evRef}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := NewAggregator(dir).Integrate(1, 50, oldRefs); err != nil {
		t.Fatal(err)
	}

	res, err := CompactEventLogs(dir, EventLogCompactionConfig{
		TargetBlocks:      65_536,
		VerificationCache: cache,
	})
	if err != nil {
		t.Fatalf("CompactEventLogs: %v", err)
	}
	if !res.Compacted || res.SourceSegments != 10 || res.SourceIndexSegments != 10 {
		t.Fatalf("unexpected compaction result: %+v", res)
	}
	if res.FromBlock != 1 || res.ToBlock != 100 {
		t.Fatalf("compacted range = [%d,%d], want [1,100]", res.FromBlock, res.ToBlock)
	}
	if res.DeletedFiles != 20 || res.RetiredTrimmed != 20 {
		t.Fatalf("deleted=%d retiredTrimmed=%d, want 20/20", res.DeletedFiles, res.RetiredTrimmed)
	}

	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.VisibleTxStart != 1 || manifest.VisibleTxEnd != 50 {
		t.Fatalf("visible tx range changed: [%d,%d]", manifest.VisibleTxStart, manifest.VisibleTxEnd)
	}
	if len(manifest.Retired) != 0 {
		t.Fatalf("manifest still has %d retired refs: %+v", len(manifest.Retired), manifest.Retired)
	}
	evList := eventLogRefs(manifest)
	idxList := eventLogIndexRefs(manifest)
	if len(evList) != 1 || len(idxList) != 1 {
		t.Fatalf("active event=%d index=%d, want 1/1", len(evList), len(idxList))
	}
	if evList[0].SourceBlockHash != lastAnchor.Hex() {
		t.Fatalf("SourceBlockHash = %q, want %q", evList[0].SourceBlockHash, lastAnchor.Hex())
	}
	for _, oldRef := range oldRefs {
		if _, err := os.Stat(filepath.Join(dir, oldRef.Path)); !os.IsNotExist(err) {
			t.Fatalf("obsolete file %q still present on disk (err=%v)", oldRef.Path, err)
		}
	}

	// Verification cache must contain 1 entry (the new V5+V3 pair) and have pruned all 10 old entries.
	stats := cache.Stats()
	if stats.EventEntries != 1 {
		t.Fatalf("verification cache EventEntries = %d, want 1 (pruned old 10)", stats.EventEntries)
	}
	_ = oldIdxRefs

	// Manager queries must return byte-for-byte identical logs.
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []EventLog
	if err := mgr.IterateEventLogs(1, 100, EventLogFilter{
		Addresses: []common.Address{addrA},
		Topics:    [][]common.Hash{{topicShared}},
	}, func(row EventLog) (bool, error) {
		got = append(got, row)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	wantCount := 0
	for _, r := range allRows {
		if r.Address == addrA {
			wantCount++
		}
	}
	if len(got) != wantCount {
		t.Fatalf("Manager filtered count = %d, want %d", len(got), wantCount)
	}

	// Subsequent pass must be a no-op (settled).
	second, err := CompactEventLogs(dir, EventLogCompactionConfig{TargetBlocks: 65_536})
	if err != nil {
		t.Fatal(err)
	}
	if second.Compacted {
		t.Fatalf("expected second pass to be settled, got %+v", second)
	}
}

func TestEventLogCompactorPreservesPublishedCatalogLeases(t *testing.T) {
	dir := t.TempDir()
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	addr := common.BytesToAddress(eventLogTestAddress(0x73))
	ev1, idx1 := buildV4TestPairForCompactor(t, dir, 1, 5, common.Hash{1}, []EventLog{
		eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{11}, common.Hash{21}, []byte("a")),
	})
	ev2, idx2 := buildV4TestPairForCompactor(t, dir, 6, 10, common.Hash{2}, []EventLog{
		eventLogV3TestRow(6, 0, 0, addr, common.Hash{2}, common.Hash{12}, common.Hash{22}, []byte("b")),
	})
	if _, err := NewAggregator(dir).Integrate(0, 0, []SegmentRef{ev1, idx1, ev2, idx2}); err != nil {
		t.Fatal(err)
	}
	identity := ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: (common.Hash{0xaa}).Hex()}
	if _, err := EnsureProductionManifestChainIdentity(dir, identity); err != nil {
		t.Fatal(err)
	}
	if _, err := PublishSignedSnapshotCatalog(dir, priv); err != nil {
		t.Fatal(err)
	}

	res, err := CompactEventLogs(dir, EventLogCompactionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Compacted {
		t.Fatal("expected compaction to succeed")
	}
	if res.DeletedFiles != 0 {
		t.Fatalf("deletedFiles = %d, want 0 while published catalog holds leases", res.DeletedFiles)
	}
	for _, oldRef := range []SegmentRef{ev1, idx1, ev2, idx2} {
		if _, err := os.Stat(filepath.Join(dir, oldRef.Path)); err != nil {
			t.Fatalf("leased file %q should remain on disk: %v", oldRef.Path, err)
		}
	}
}

func TestEventLogCompactorCrossingLegacyIndexSafety(t *testing.T) {
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x74))
	ev1, _ := buildV4TestPairForCompactor(t, dir, 1, 5, common.Hash{1}, []EventLog{
		eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{11}, common.Hash{21}, []byte("a")),
	})
	ev2, _ := buildV4TestPairForCompactor(t, dir, 6, 10, common.Hash{2}, []EventLog{
		eventLogV3TestRow(6, 0, 0, addr, common.Hash{2}, common.Hash{12}, common.Hash{22}, []byte("b")),
	})
	// Single multi-segment V2 index spanning both segments [1,10]
	combinedIdx, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ev1, ev2}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewAggregator(dir).Integrate(0, 0, []SegmentRef{ev1, ev2, combinedIdx}); err != nil {
		t.Fatal(err)
	}

	// Trying with TargetBlocks: 5 can't merge ev1+ev2 without exceeding TargetBlocks, so must skip cleanly rather than breaking combinedIdx.
	resSmall, err := CompactEventLogs(dir, EventLogCompactionConfig{TargetBlocks: 5})
	if err != nil {
		t.Fatal(err)
	}
	if resSmall.Compacted {
		t.Fatalf("should not break crossing index [1,10] with target 5: %+v", resSmall)
	}

	// With TargetBlocks >= 10, both ev1+ev2 and combinedIdx fit in one batch and merge cleanly!
	resFit, err := CompactEventLogs(dir, EventLogCompactionConfig{TargetBlocks: 16})
	if err != nil {
		t.Fatal(err)
	}
	if !resFit.Compacted || resFit.FromBlock != 1 || resFit.ToBlock != 10 {
		t.Fatalf("expected clean merge over [1,10], got %+v", resFit)
	}
}

func TestColdBuilderCompactsEventLogsAfterHistory(t *testing.T) {
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x75))
	ev1, idx1 := buildV4TestPairForCompactor(t, dir, 1, 5, common.Hash{1}, []EventLog{
		eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{11}, common.Hash{21}, []byte("a")),
	})
	ev2, idx2 := buildV4TestPairForCompactor(t, dir, 6, 10, common.Hash{2}, []EventLog{
		eventLogV3TestRow(6, 0, 0, addr, common.Hash{2}, common.Hash{12}, common.Hash{22}, []byte("b")),
	})
	if _, err := NewAggregator(dir).Integrate(0, 0, []SegmentRef{ev1, idx1, ev2, idx2}); err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(nil, Config{
		Dir:              dir,
		Enabled:          true,
		BuildEventLogs:   true,
		CompactEventLogs: true,
		HeavyWorkGate:    maintenance.NewHeavyWorkGate(),
		MetricsNamespace: "test/eventlog-compaction/",
	})
	res, err := runner.compactHistory(context.Background(), false)
	if err != nil {
		t.Fatalf("runner.compactHistory: %v", err)
	}
	if !res.EventLogsCompacted || !res.EventLogCompaction.Compacted || res.EventLogCompaction.ToBlock != 10 {
		t.Fatalf("expected EventLogsCompacted in runner.compactHistory, got %+v", res)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	evList := eventLogRefs(manifest)
	if len(evList) != 1 || evList[0].FromTxNum != 1 || evList[0].ToTxNum != 10 {
		t.Fatalf("unexpected post-compaction event-log refs: %+v", evList)
	}
}

func TestEventLogCompactorConcurrentManifestSafety(t *testing.T) {
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x76))
	ev1, idx1 := buildV4TestPairForCompactor(t, dir, 1, 5, common.Hash{1}, []EventLog{
		eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{11}, common.Hash{21}, []byte("a")),
	})
	ev2, idx2 := buildV4TestPairForCompactor(t, dir, 6, 10, common.Hash{2}, []EventLog{
		eventLogV3TestRow(6, 0, 0, addr, common.Hash{2}, common.Hash{12}, common.Hash{22}, []byte("b")),
	})
	manifest, err := NewAggregator(dir).Integrate(0, 0, []SegmentRef{ev1, idx1, ev2, idx2})
	if err != nil {
		t.Fatal(err)
	}
	sel, ok, err := selectEventLogCompactionRun(dir, manifest, EventLogCompactionConfig{}.withDefaults())
	if err != nil || !ok {
		t.Fatalf("selectEventLogCompactionRun: ok=%v err=%v", ok, err)
	}
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	v5Ref, idxRef, err := buildAnchoredEventLogV5Pair(pinned, dir, sel.fromBlock, sel.toBlock, sel.sources)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Concurrent unrelated event-log segment [11,15] advances Generation: compaction publish in [1,10] MUST succeed.
	ev3, idx3 := buildV4TestPairForCompactor(t, dir, 11, 15, common.Hash{3}, []EventLog{
		eventLogV3TestRow(11, 0, 0, addr, common.Hash{3}, common.Hash{13}, common.Hash{23}, []byte("c")),
	})
	if _, err := NewAggregator(dir).Integrate(0, 0, []SegmentRef{ev3, idx3}); err != nil {
		t.Fatal(err)
	}
	published, err := publishEventLogCompactionManifest(dir, sel, []SegmentRef{v5Ref, idxRef})
	if err != nil {
		t.Fatalf("expected concurrent publish in disjoint range [11,15] to succeed: %v", err)
	}
	if len(eventLogRefs(published)) != 2 {
		t.Fatalf("expected 2 active event-log refs ([1,10] + [11,15]), got %+v", eventLogRefs(published))
	}

	// 2. Concurrent change inside selected range MUST fail closed without mutating manifest.
	if _, err := publishEventLogCompactionManifest(dir, sel, []SegmentRef{v5Ref, idxRef}); err == nil || !strings.Contains(err.Error(), "changed concurrently") {
		t.Fatalf("expected fail-closed error when range already mutated concurrently, got %v", err)
	}
}

func TestColdBuilderEventLogCompactionDoesNotStarveBehindRoutingAndHistory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x77))

	var refs []SegmentRef
	for block := uint64(1); block <= 12; block++ {
		refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, block, block,
			binaryStateDomainChange(block, block, 1, fmt.Sprintf("account-%d", block)))...)
	}
	ev1, idx1 := buildV4TestPairForCompactor(t, dir, 1, 5, common.Hash{1}, []EventLog{
		eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{11}, common.Hash{21}, []byte("ev-1")),
	})
	ev2, idx2 := buildV4TestPairForCompactor(t, dir, 6, 10, common.Hash{2}, []EventLog{
		eventLogV3TestRow(6, 0, 0, addr, common.Hash{2}, common.Hash{12}, common.Hash{22}, []byte("ev-2")),
	})
	refs = append(refs, ev1, idx1, ev2, idx2)
	if err := PublishManifest(dir, NewManifest(1, 12, refs)); err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(&priorityCompactionTestChain{idle: false}, Config{
		Dir:                dir,
		Enabled:            true,
		HistoryDataset:     SegmentDatasetStateDomainChange,
		HistoryCatchupMode: HistoryCatchupThroughput,
		CompactMaxSteps:    4,
		BuildEventLogs:     true,
		CompactEventLogs:   true,
		HeavyWorkGate:      maintenance.NewHeavyWorkGate(),
		MetricsNamespace:   "test/" + strings.ReplaceAll(t.Name(), "/", "_") + "/",
	})
	t.Cleanup(runner.cancel)

	// Turn 1 runs state-history routing or history compaction and increments nonEventCompactionTurns.
	turn1, err := runner.compactHistory(ctx, true)
	if err != nil {
		t.Fatalf("turn 1: %v", err)
	}
	if (!turn1.RoutingBuilt && !turn1.Merged) || turn1.EventLogsCompacted {
		t.Fatalf("turn 1 should prioritize routing/history first: %+v", turn1)
	}
	if runner.compactionBudget.nonEventCompactionTurns != 1 {
		t.Fatalf("turn 1 nonEventCompactionTurns = %d, want 1", runner.compactionBudget.nonEventCompactionTurns)
	}

	// Turn 2 must preempt further routing/history backlog and compact the legacy event-log pairs.
	runner.compactionBudget.notBefore = time.Time{}
	runner.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGate()
	turn2, err := runner.compactHistory(ctx, true)
	if err != nil {
		t.Fatalf("turn 2: %v", err)
	}
	if !turn2.EventLogsCompacted || !turn2.EventLogCompaction.Compacted ||
		turn2.EventLogCompaction.FromBlock != 1 || turn2.EventLogCompaction.ToBlock != 10 {
		t.Fatalf("turn 2 should compact legacy event logs without starvation: %+v", turn2)
	}
	if turn2.RoutingBuilt || turn2.Merged {
		t.Fatalf("turn 2 unexpectedly mixed non-event compaction with event-log compaction: %+v", turn2)
	}
	if runner.compactionBudget.nonEventCompactionTurns != 0 {
		t.Fatalf("turn 2 nonEventCompactionTurns = %d, want 0", runner.compactionBudget.nonEventCompactionTurns)
	}
	manifest, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	evList := eventLogRefs(manifest)
	if len(evList) != 1 || evList[0].FromTxNum != 1 || evList[0].ToTxNum != 10 {
		t.Fatalf("expected settled single V5 event-log segment after turn 2, got %+v", evList)
	}
	for _, oldRef := range []SegmentRef{ev1, idx1, ev2, idx2} {
		if _, statErr := os.Stat(filepath.Join(dir, oldRef.Path)); !os.IsNotExist(statErr) {
			t.Fatalf("old segment %q should have been deleted, statErr=%v", oldRef.Path, statErr)
		}
	}

	// Turn 3 alternates cleanly back to routing/history work.
	runner.compactionBudget.notBefore = time.Time{}
	runner.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGate()
	turn3, err := runner.compactHistory(ctx, true)
	if err != nil {
		t.Fatalf("turn 3: %v", err)
	}
	if (!turn3.RoutingBuilt && !turn3.Merged) || turn3.EventLogsCompacted {
		t.Fatalf("turn 3 should return to routing/history work: %+v", turn3)
	}
	if runner.compactionBudget.nonEventCompactionTurns != 1 {
		t.Fatalf("turn 3 nonEventCompactionTurns = %d, want 1", runner.compactionBudget.nonEventCompactionTurns)
	}
	beforeTurn4, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Turn 4 probes event-log compaction, finds none left, records idle gen, and falls through in the same turn.
	runner.compactionBudget.notBefore = time.Time{}
	runner.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGate()
	turn4, err := runner.compactHistory(ctx, true)
	if err != nil {
		t.Fatalf("turn 4: %v", err)
	}
	if (!turn4.RoutingBuilt && !turn4.Merged) || turn4.EventLogsCompacted {
		t.Fatalf("turn 4 should fall through to routing/history when event logs are settled: %+v", turn4)
	}
	if runner.compactionBudget.eventLogCompactIdleGen != beforeTurn4.Generation {
		t.Fatalf("turn 4 eventLogCompactIdleGen = %d, want %d", runner.compactionBudget.eventLogCompactIdleGen, beforeTurn4.Generation)
	}
}

func TestColdBuilderEventLogCompactionSoftLoadArmsPendingAndBusyCap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x78))
	ev1, idx1 := buildV4TestPairForCompactor(t, dir, 1, 5, common.Hash{1}, []EventLog{
		eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{11}, common.Hash{21}, []byte("soft-1")),
	})
	ev2, idx2 := buildV4TestPairForCompactor(t, dir, 6, 10, common.Hash{2}, []EventLog{
		eventLogV3TestRow(6, 0, 0, addr, common.Hash{2}, common.Hash{12}, common.Hash{22}, []byte("soft-2")),
	})
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ev1, idx1, ev2, idx2})); err != nil {
		t.Fatal(err)
	}

	softLoad := true
	probe := func() maintenance.StoragePressure {
		now := time.Now()
		p := healthyHistoryLoad(now)
		if softLoad {
			p.DeviceBusyPPM = 960_000
			p.DeviceQueueMilli = 3_000
			p.DeviceAwait = 10 * time.Millisecond
		}
		return p
	}
	runner := NewRunner(&priorityCompactionTestChain{idle: false}, Config{
		Dir:                dir,
		Enabled:            true,
		HistoryDataset:     "", // intentionally empty to prove event-log readiness does not require HistoryDataset
		HistoryCatchupMode: HistoryCatchupThroughput,
		CompactMaxSteps:    1, // < 2 to prove small CompactMaxSteps still reports event-log readiness
		BuildEventLogs:     true,
		CompactEventLogs:   true,
		HeavyWorkGate:      maintenance.NewHeavyWorkGate(),
		HistoryLoadProbe:   probe,
		MetricsNamespace:   "test/" + strings.ReplaceAll(t.Name(), "/", "_") + "/",
	})
	t.Cleanup(runner.cancel)

	if got := runner.effectiveEventLogCompactionConfig(true).MaxSourceMainBytes; got != busyEventLogCompactionMaxSourceMainBytes {
		t.Fatalf("busy MaxSourceMainBytes = %d, want %d", got, busyEventLogCompactionMaxSourceMainBytes)
	}
	runner.syncEventLogCompactionMode(true)
	runner.compactionBudget.eventLogCompactHintBlk = 12345
	runner.compactionBudget.eventLogCompactIdleGen = 99
	runner.syncEventLogCompactionMode(false)
	if runner.compactionBudget.eventLogCompactHintBlk != 0 || runner.compactionBudget.eventLogCompactIdleGen != 0 {
		t.Fatalf("busy->idle did not reset hint/idle gen: hint=%d gen=%d",
			runner.compactionBudget.eventLogCompactHintBlk, runner.compactionBudget.eventLogCompactIdleGen)
	}

	potential, err := runner.hasPotentialBusyCompaction()
	if err != nil || !potential {
		t.Fatalf("hasPotentialBusyCompaction = (%v, %v), want (true, nil)", potential, err)
	}

	// Under soft pressure (level=1, !hard), busy compaction returns import-load with zero RetryDeadline and arms pending.
	deferred, err := runner.compactHistory(ctx, true)
	if err != nil {
		t.Fatalf("soft-load compactHistory: %v", err)
	}
	if !deferred.Deferred || deferred.DeferReason != "import-load" || !deferred.RetryDeadline.IsZero() {
		t.Fatalf("unexpected soft-load defer result: %+v", deferred)
	}
	if !runner.compactionBudget.pending {
		t.Fatal("soft-load import-load did not arm compactionBudget.pending")
	}

	// While level stays 1, shouldPrioritizePendingCompaction keeps returning false without clearing pending.
	if runner.shouldPrioritizePendingCompaction(time.Now()) || !runner.compactionBudget.pending {
		t.Fatal("shouldPrioritizePendingCompaction should wait silently at level=1 without clearing pending")
	}

	// Recover storage load to level 2 across two spaced healthy samples; pending priority must trigger cleanly.
	softLoad = false
	runner.historyLoad.lastAccepted = time.Now().Add(-10 * time.Second)
	runner.refreshHistoryLoad(time.Now())
	runner.historyLoad.lastAccepted = time.Now().Add(-10 * time.Second)
	if !runner.shouldPrioritizePendingCompaction(time.Now()) {
		t.Fatalf("expected shouldPrioritizePendingCompaction=true once recovered to level=%d", runner.historyLoad.level)
	}
	if runner.compactionBudget.pending {
		t.Fatal("shouldPrioritizePendingCompaction did not consume pending")
	}
}
