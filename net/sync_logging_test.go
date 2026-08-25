package net

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	gnet "net"

	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
	"github.com/tronprotocol/go-tron/p2p"
)

// TestSync_BatchSummaryReportedOnInterval drives a stream of blocks through
// HandleBlock with StatsReportInterval temporarily shrunk to 50ms, then
// asserts the throttled "Sync import progress" summary line is emitted at
// least once with the expected fields.
func TestSync_BatchSummaryReportedOnInterval(t *testing.T) {
	oldInterval := tsync.StatsReportInterval
	tsync.StatsReportInterval = 50 * time.Millisecond
	defer func() { tsync.StatsReportInterval = oldInterval }()

	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)
	h := gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelDebug)
	gtronlog.SetDefault(gtronlog.NewLogger(h))

	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	// Pipe + peer so HandleBlock's bookkeeping has a peer to record stats
	// against. We don't drive the writer end — the test never causes
	// fetchNextBatch to send, so the pipe stays quiescent.
	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "summary-peer", false, nil)

	now := time.Now()
	ss.stats.InitSession(now)
	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.inflight = 1
	ss.armFetchTimer()
	ss.mu.Unlock()

	// Insert 3 blocks with a 60ms gap before the third so the rolling
	// window definitely exceeds statsReportInterval=50ms and triggers a
	// summary emit at least once. Each block must be pre-registered in
	// ss.pending so HandleBlock's request-dedup gate accepts it.
	parent := bc.CurrentBlock().Hash()
	for i := int64(1); i <= 3; i++ {
		if i == 3 {
			time.Sleep(60 * time.Millisecond)
		}
		blk := stubBlock(i, parent)
		ss.mu.Lock()
		ss.inflight = 1
		if ss.pending == nil {
			ss.pending = make(map[tcommon.Hash]uint64)
		}
		ss.pending[blk.Hash()] = uint64(i)
		ss.mu.Unlock()
		if !ss.HandleBlock(peer, blk, nil) {
			t.Fatalf("HandleBlock(%d) returned false", i)
		}
		parent = blk.Hash()
	}

	out := buf.String()
	var summary, detail string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "Sync import diagnostics") {
			detail = line
		} else if strings.Contains(line, "Sync import progress") {
			summary = line
		}
	}
	if summary == "" {
		t.Fatalf("expected 'Sync import progress' summary line, got:\n%s", out)
	}
	for _, k := range []string{
		"window=",
		"head=",
		"blocks=",
		"txs=",
		"blocksPerSec=",
		"txsPerSec=",
		"txsPerBlock=",
		"energyPerSec=",
		"energyPerBlock=",
		"energyPerTx=",
		"execBusyPct=",
		"bufferWaitPct=",
		"applySamples=",
		"applyCoveragePct=",
		"importMsPerBlock=",
		"applyMsPerBlock=",
		"importOverheadMsPerBlock=",
		"outsideTxMsPerBlock=",
		"executeFixedMsPerBlock=",
		"transactionMsPerTx=",
		"rewardsMsPerBlock=",
		"blockStatsMsPerBlock=",
		"stateCommitMsPerBlock=",
		"persistMsPerBlock=",
		"remaining=",
		"peers=",
		"activePeers=",
		"inflight=",
		"buffered=",
		"requested=",
		"retries=",
	} {
		if !strings.Contains(summary, k) {
			t.Errorf("missing key %q in summary line:\n%s", k, summary)
		}
	}
	for _, k := range []string{
		"target=",
		"progress=",
		"remain=",
		"speedWindow=",
		"avgBlocks/s=",
		"minBlocks/s=",
		"maxBlocks/s=",
		"eta=",
		"execElapsed=",
		"applyElapsed=",
		"statePrefetchEnqueued=",
		"slowPhase=",
		"syncStageComplete=",
		"peer=",
	} {
		if strings.Contains(summary, k) {
			t.Errorf("diagnostic key %q leaked into compact summary:\n%s", k, summary)
		}
	}
	if detail == "" {
		t.Fatalf("expected debug detail line, got:\n%s", out)
	}
	for _, k := range []string{
		"execElapsed=",
		"applyElapsed=",
		"bufferWaitElapsed=",
		"validate=",
		"execute=",
		"transactionExecute=",
		"accountStateRoot=",
		"adaptiveEnergy=",
		"rewards=",
		"shieldedFinalize=",
		"witnessFlush=",
		"blockStatistics=",
		"energy=",
		"energyPerSec=",
		"maintenance=",
		"stateCommit=",
		"stateCommitMeasured=",
		"stateCommitPrepare=",
		"stateCommitFlatWrite=",
		"stateCommitFlatFlush=",
		"stateCommitKVCompute=",
		"stateCommitKVNodes=",
		"stateCommitAccountTrieUpdate=",
		"stateCommitAccountTrieMarshal=",
		"stateCommitAccountTrieGeneration=",
		"stateCommitAccountTrieWrite=",
		"stateCommitFinalize=",
		"stateCommitAccountTrieCommit=",
		"stateCommitTrieNodes=",
		"stateCommitTrieFlush=",
		"stateCommitReopen=",
		"stateCommitAccounts=",
		"stateCommitKVAccounts=",
		"stateCommitKVItems=",
		"stateMutAccountCreates=",
		"stateMutAccountUpdates=",
		"stateMutAccountDeletes=",
		"stateMutCodeUpdates=",
		"stateMutCodeDeletes=",
		"stateMutContractMetaUpdates=",
		"stateMutContractMetaDeletes=",
		"stateMutStoragePuts=",
		"stateMutStorageDeletes=",
		"stateMutStorageNoops=",
		"stateMutKVPuts=",
		"stateMutKVDeletes=",
		"stateMutKVNoops=",
		"stateMutTop=",
		"stateMutKVTop=",
		"dpUpdate=",
		"persist=",
		"hooks=",
		"blockBuffer=",
		"requested=",
		"retryList=",
		"syncStageComplete=",
		"syncStageCompleted=",
		"syncStageScheduled=",
		"peerState=",
		"inflight=",
		"fetchList=",
	} {
		if !strings.Contains(detail, k) {
			t.Errorf("missing key %q in detail line:\n%s", k, detail)
		}
	}
}

func TestReportSegmentInfoIsCompactOperationalStatus(t *testing.T) {
	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)
	h := gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)
	gtronlog.SetDefault(gtronlog.NewLogger(h))

	start := time.Now().Add(-2 * time.Second)
	(&SyncService{stats: tsync.NewStats()}).reportSegment(tsync.Snapshot{
		StartTime:   start,
		TotalStart:  start.Add(-8 * time.Second),
		Blocks:      20,
		Txs:         40,
		ApplyBlocks: 20,
		ApplyTxs:    40,
		TotalBlocks: 80,
		TotalTxs:    160,
		TxKinds:     map[string]int{"TransferContract": 40},
		ApplyStats: core.ApplyStats{EnergyUsageTotal: 6_000_000_000, StateCommitDetail: state.CommitStats{
			Mutations: state.CommitMutationStats{StoragePuts: 12, AccountUpdates: 8},
		}},
	}, syncdl.NewDiagnostics(3, 4, 1, []syncdl.PeerDiagnostics{
		{ID: "active", Inflight: 2},
		{ID: "done", Done: true},
	}), 90, 10, nil)

	out := buf.String()
	var segmentLine string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "Sync import progress") {
			segmentLine = line
		}
	}
	for _, field := range []string{
		"window=", "head=90", "blocks=20", "txs=40", "blocksPerSec=10", "txsPerSec=20", "remaining=10",
		"txsPerBlock=2", "energyPerSec=", "energyPerBlock=300.00M", "energyPerTx=150.00M",
		"execBusyPct=", "bufferWaitPct=", "applySamples=20", "applyCoveragePct=100", "importMsPerBlock=", "applyMsPerBlock=",
		"importOverheadMsPerBlock=", "outsideTxMsPerBlock=", "executeFixedMsPerBlock=", "transactionMsPerTx=",
		"rewardsMsPerBlock=", "blockStatsMsPerBlock=",
		"stateCommitMsPerBlock=", "persistMsPerBlock=", "peers=2", "activePeers=1", "inflight=2",
		"buffered=3", "requested=4", "retries=1",
	} {
		if !strings.Contains(segmentLine, field) {
			t.Errorf("missing real-time field %q:\n%s", field, segmentLine)
		}
	}
	for _, field := range []string{
		"Sync import diagnostics", "execElapsed=", "stateCommit=", "peerState=",
		"energy=", "txTop=", "stateMutTop=", "stateMutKVTop=",
	} {
		if strings.Contains(out, field) {
			t.Errorf("diagnostic field %q emitted at info level:\n%s", field, out)
		}
	}
}

func TestFormatCompactEnergy(t *testing.T) {
	for _, tc := range []struct {
		value float64
		want  string
	}{
		{0, "0"},
		{999.125, "999.13"},
		{1_000, "1.00k"},
		{18_436.725, "18.44k"},
		{999_999.125, "1000.00k"},
		{1_000_000, "1.00M"},
		{18_436_725, "18.44M"},
		{1_000_000_000, "1.00B"},
		{18_436_725_910, "18.44B"},
		{-2_304_014_432.64, "-2.30B"},
	} {
		if got := formatCompactEnergy(tc.value); got != tc.want {
			t.Errorf("formatCompactEnergy(%v) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

func TestSyncEnergyPerSecCalculationAndFormatting(t *testing.T) {
	if got := syncEnergyPerSec(6_000_000_001, 2*time.Second); got != 3_000_000_000.5 {
		t.Fatalf("energyPerSec = %v, want 3000000000.5", got)
	}
	if got := formatSyncEnergyPerSec(6_000_000_001, 2*time.Second); got != "3.00B" {
		t.Fatalf("formatted energyPerSec = %q, want 3.00B", got)
	}
	if got := formatSyncEnergyPerSec(36_873, 2*time.Second); got != "18.44k" {
		t.Fatalf("formatted energyPerSec = %q, want 18.44k", got)
	}
	for _, tc := range []struct {
		total   int64
		elapsed time.Duration
	}{
		{0, time.Second},
		{-1, time.Second},
		{1, 0},
	} {
		if got := syncEnergyPerSec(tc.total, tc.elapsed); got != 0 {
			t.Errorf("syncEnergyPerSec(%d, %s) = %v, want 0", tc.total, tc.elapsed, got)
		}
	}
}

func TestSyncImportWindowObservationSeparatesWorkAndSupply(t *testing.T) {
	s := tsync.Snapshot{
		Blocks:            10,
		Txs:               40,
		ApplyBlocks:       10,
		ApplyTxs:          40,
		ExecElapsed:       800 * time.Millisecond,
		BufferWaitElapsed: 100 * time.Millisecond,
		ApplyStats: core.ApplyStats{
			Validate:           50 * time.Millisecond,
			Execute:            400 * time.Millisecond,
			TransactionExecute: 300 * time.Millisecond,
			AccountStateRoot:   10 * time.Millisecond,
			AdaptiveEnergy:     20 * time.Millisecond,
			Rewards:            30 * time.Millisecond,
			ShieldedFinalize:   5 * time.Millisecond,
			WitnessFlush:       5 * time.Millisecond,
			BlockStatistics:    10 * time.Millisecond,
			EnergyUsageTotal:   8_000_000,
			Maintenance:        10 * time.Millisecond,
			StateCommit:        200 * time.Millisecond,
			DPUpdate:           20 * time.Millisecond,
			Persist:            100 * time.Millisecond,
			Hooks:              20 * time.Millisecond,
		},
	}
	got := newSyncImportWindowObservation(s, 2*time.Second)
	for name, check := range map[string]struct{ got, want float64 }{
		"blocks/s":               {got.BlocksPerSec, 5},
		"txs/s":                  {got.TxsPerSec, 20},
		"txs/block":              {got.TxsPerBlock, 4},
		"energy/s":               {got.EnergyPerSec, 4_000_000},
		"energy/block":           {got.EnergyPerBlock, 800_000},
		"energy/tx":              {got.EnergyPerTx, 200_000},
		"exec busy":              {got.ExecBusyRatio, 0.4},
		"buffer wait":            {got.BufferWaitRatio, 0.05},
		"apply coverage":         {got.ApplyCoverageRatio, 1},
		"import ms/block":        {got.ImportMillisPerBlock, 80},
		"apply ms/block":         {got.ApplyMillisPerBlock, 80},
		"import overhead/block":  {got.ImportOverheadMillisPerBlock, 0},
		"outside tx ms/block":    {got.OutsideTxMillisPerBlock, 50},
		"execute fixed ms/block": {got.ExecuteFixedMillisPerBlock, 10},
		"execute ms/block":       {got.ExecuteMillisPerBlock, 40},
		"transaction ms/block":   {got.TransactionMillisPerBlock, 30},
		"transaction ms/tx":      {got.TransactionMillisPerTx, 7.5},
		"reward ms/block":        {got.RewardsMillisPerBlock, 3},
		"block stats ms/block":   {got.BlockStatsMillisPerBlock, 1},
		"commit ms/block":        {got.StateCommitMillisPerBlock, 20},
		"persist ms/block":       {got.PersistMillisPerBlock, 10},
	} {
		if check.got != check.want {
			t.Errorf("%s = %v, want %v", name, check.got, check.want)
		}
	}
}

func TestSyncImportWindowObservationUsesCompletedApplyCoverage(t *testing.T) {
	s := tsync.Snapshot{
		Blocks:      10,
		Txs:         40,
		ApplyBlocks: 8,
		ApplyTxs:    32,
		ExecElapsed: 800 * time.Millisecond,
		ApplyStats: core.ApplyStats{
			Execute:            320 * time.Millisecond,
			TransactionExecute: 240 * time.Millisecond,
			EnergyUsageTotal:   6_400,
		},
	}
	got := newSyncImportWindowObservation(s, time.Second)
	if got.ApplyCoverageRatio != 0.8 {
		t.Fatalf("apply coverage = %v, want 0.8", got.ApplyCoverageRatio)
	}
	if got.ApplyMillisPerBlock != 40 || got.TransactionMillisPerTx != 7.5 {
		t.Fatalf("coverage-normalized apply/tx = %v/%v, want 40/7.5", got.ApplyMillisPerBlock, got.TransactionMillisPerTx)
	}
	if got.EnergyPerBlock != 800 || got.EnergyPerTx != 200 {
		t.Fatalf("coverage-normalized energy = %v/block %v/tx, want 800/200", got.EnergyPerBlock, got.EnergyPerTx)
	}
	if got.ImportOverheadMillisPerBlock != 0 {
		t.Fatalf("mismatched coverage must suppress import-overhead subtraction, got %v", got.ImportOverheadMillisPerBlock)
	}
}

func TestSyncProgressWindowsAlignToWallClock(t *testing.T) {
	loc := time.FixedZone("UTC+8", 8*60*60)
	now := time.Date(2026, 7, 31, 12, 3, 27, 0, loc)
	wantNext := time.Date(2026, 7, 31, 12, 5, 0, 0, loc)
	if got := nextSyncProgressBoundary(now); !got.Equal(wantNext) {
		t.Fatalf("next boundary = %v, want %v", got, wantNext)
	}

	hour := time.Date(2026, 7, 31, 13, 0, 0, 0, loc)
	windows := dueSyncProgressWindows(hour)
	if len(windows) != 3 || windows[0].label != "5m" || windows[1].label != "30m" || windows[2].label != "1h" {
		t.Fatalf("hour windows = %+v, want 5m/30m/1h", windows)
	}
	halfHour := time.Date(2026, 7, 31, 13, 30, 0, 0, loc)
	windows = dueSyncProgressWindows(halfHour)
	if len(windows) != 2 || windows[0].label != "5m" || windows[1].label != "30m" {
		t.Fatalf("half-hour windows = %+v, want 5m/30m", windows)
	}
	midnight := time.Date(2026, 8, 1, 0, 0, 0, 0, loc)
	windows = dueSyncProgressWindows(midnight)
	if len(windows) != 4 || windows[3].label != "1d" || !windows[3].from.Equal(midnight.AddDate(0, 0, -1)) {
		t.Fatalf("midnight windows = %+v, want natural previous day", windows)
	}
}

func TestReportCalendarProgressEmitsDueWindows(t *testing.T) {
	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)
	h := gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)
	gtronlog.SetDefault(gtronlog.NewLogger(h))

	loc := time.FixedZone("UTC+8", 8*60*60)
	boundary := time.Date(2026, 7, 31, 12, 0, 0, 0, loc)
	stats := tsync.NewStats()
	stats.InitSession(boundary.Add(-2 * time.Hour))
	stats.ObserveSpeed(boundary.Add(-30*time.Minute), 300, 5*time.Minute, time.Hour)
	ss := &SyncService{
		stats:         stats,
		pause:         tsync.NewPauseGate(),
		syncing:       true,
		syncedTipNum:  90,
		targetHeadNum: 100,
	}
	ss.reportCalendarProgress(boundary)

	out := buf.String()
	if got := strings.Count(out, `msg="Sync progress"`); got != 3 {
		t.Fatalf("progress line count = %d, want 3:\n%s", got, out)
	}
	for _, field := range []string{
		"window=5m", "window=30m", "window=1h",
		"from=", "to=", "coveragePct=100", "warming=false", "head=90", "target=100",
		"chainProgressPct=90", "remaining=10", "windowBlocks=", "avgBlocksPerSec=", "minBlocksPerSec=", "maxBlocksPerSec=", "eta=",
	} {
		if !strings.Contains(out, field) {
			t.Errorf("missing progress field %q:\n%s", field, out)
		}
	}
}

func TestReportCalendarProgressSuppressesWarmupETA(t *testing.T) {
	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))

	loc := time.FixedZone("UTC+8", 8*60*60)
	boundary := time.Date(2026, 7, 31, 12, 0, 0, 0, loc)
	stats := tsync.NewStats()
	stats.InitSession(boundary.Add(-5 * time.Minute))
	stats.ObserveSpeed(boundary, 300, 5*time.Minute, time.Hour)
	ss := &SyncService{
		stats:         stats,
		pause:         tsync.NewPauseGate(),
		syncing:       true,
		syncedTipNum:  90,
		targetHeadNum: 100,
	}
	ss.reportCalendarProgress(boundary)

	for _, line := range strings.Split(buf.String(), "\n") {
		if !strings.Contains(line, "window=1h") {
			continue
		}
		if !strings.Contains(line, "warming=true") {
			t.Fatalf("1h warmup line missing warming=true:\n%s", line)
		}
		if strings.Contains(line, " eta=") {
			t.Fatalf("1h warmup line emitted unstable ETA:\n%s", line)
		}
		return
	}
	t.Fatalf("missing 1h progress line:\n%s", buf.String())
}

func TestUnavailableSyncPeerInfoIsRateLimited(t *testing.T) {
	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "range-peer", false, nil)
	ss := &SyncService{}
	ss.reportUnavailableSyncPeer(peer, 100, 1_000, 2_000)
	ss.reportUnavailableSyncPeer(peer, 101, 1_000, 2_000)

	out := buf.String()
	if got := strings.Count(out, `msg="Historical sync peers unavailable"`); got != 1 {
		t.Fatalf("range summary count = %d, want 1:\n%s", got, out)
	}
	for _, field := range []string{"rejectedSinceLastReport=1", "needFrom=100", "samplePeerLowest=1000", "samplePeerHead=2000"} {
		if !strings.Contains(out, field) {
			t.Errorf("missing range summary field %q:\n%s", field, out)
		}
	}
}

func TestUnavailableSyncPeerConcurrentAccounting(t *testing.T) {
	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "range-peer", false, nil)
	ss := &SyncService{}
	ss.lastPeerRangeSummary.Store(time.Now().UnixNano())

	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ss.reportUnavailableSyncPeer(peer, 100, 1_000, 2_000)
		}()
	}
	wg.Wait()
	if got := ss.peerRangeRejects.Load(); got != 64 {
		t.Fatalf("pending rejected peers = %d, want 64", got)
	}
}

func TestSync_StartupRepairSummaryLogged(t *testing.T) {
	var buf bytes.Buffer
	prev := gtronlog.Root()
	defer gtronlog.SetDefault(prev)
	h := gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelDebug)
	gtronlog.SetDefault(gtronlog.NewLogger(h))

	ss := &SyncService{}
	ss.logSyncStartupRepairSummary(syncdl.SessionStartupApplyResult{
		HasSyncPipelineRepair: true,
		SyncPipelineRepairResult: syncdl.SyncPipelineProgressRepairResult{
			Repairs:           []syncdl.SyncStageProgressRepair{{Stage: rawdb.StageSyncImport}, {Stage: rawdb.StageSyncExecution}, {Stage: rawdb.StageSyncCommitment}},
			Kept:              2,
			Deleted:           1,
			HasBlocked:        true,
			FirstBlockedStage: rawdb.StageSyncCommitment,
		},
		HasSyncPipelineOrderRepair: true,
		SyncPipelineOrderRepair: syncdl.SyncPipelineProgressOrderRepairResult{
			Complete: true,
			Deleted:  1,
			Updated:  2,
			Repairs: []syncdl.SyncPipelineProgressOrderRepair{
				{Stage: rawdb.StageSyncBodies},
				{Stage: rawdb.StageSyncImport},
				{Stage: rawdb.StageSyncExecution},
			},
		},
		HasSyncPipelineCursor: true,
		SyncPipelineCursor: syncdl.SyncPipelineProgressCursor{
			StageRows:   4,
			HasLast:     true,
			LastStage:   rawdb.StageSyncExecution,
			LastBlock:   6,
			LastHasHash: true,
			HasNext:     true,
			NextStage:   rawdb.StageSyncCommitment,
		},
		HasStagedBodyRestore: true,
		StagedBodyRestore: syncdl.StagedBodyRestoreResult{
			Restored:         3,
			TargetHead:       9,
			NextExpected:     7,
			NeedPruneTail:    true,
			PruneFrom:        8,
			HaveLastRestored: true,
			LastRestoredNum:  6,
		},
	})

	out := buf.String()
	var summaryLine, diagnosticLine string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "Sync startup repair summary") {
			summaryLine = line
		}
		if strings.Contains(line, "Sync startup repair diagnostics") {
			diagnosticLine = line
		}
	}
	if summaryLine == "" {
		t.Fatalf("expected startup repair summary line, got:\n%s", out)
	}
	for _, field := range []string{"repairComplete=false", "repairRows=3", "kept=2", "deleted=1", "blockedStage=SyncCommitment", "orderDeleted=1", "orderUpdated=2", "cursorRows=4", "cursorLastBlock=6", "cursorNextStage=SyncCommitment", "stagedBodiesRestored=3", "stagedPruneFrom=8"} {
		if !strings.Contains(summaryLine, field) {
			t.Errorf("missing compact summary field %q:\n%s", field, summaryLine)
		}
	}
	if strings.Contains(summaryLine, "syncStartup") {
		t.Fatalf("verbose diagnostic fields leaked into Info summary:\n%s", summaryLine)
	}
	if diagnosticLine == "" {
		t.Fatalf("expected startup repair diagnostics at Debug, got:\n%s", out)
	}
	for _, k := range []string{
		"syncStartupRepairComplete=false",
		"syncStartupRepairKept=2",
		"syncStartupRepairMissing=0",
		"syncStartupRepairDeleted=1",
		"syncStartupRepairHasBlocked=true",
		"syncStartupRepairFirstBlocked=SyncCommitment",
		"syncStartupRepairRows=3",
		"syncStartupPipelineOrderRepairChecked=true",
		"syncStartupPipelineOrderRepairComplete=true",
		"syncStartupPipelineOrderRepairDeleted=1",
		"syncStartupPipelineOrderRepairUpdated=2",
		"syncStartupPipelineOrderRepairRows=3",
		"syncStartupPipelineCursorChecked=true",
		"syncStartupPipelineCursorRows=4",
		"syncStartupPipelineCursorHasLast=true",
		"syncStartupPipelineCursorLastStage=SyncExecution",
		"syncStartupPipelineCursorLastBlock=6",
		"syncStartupPipelineCursorLastHasHash=true",
		"syncStartupPipelineCursorHasNext=true",
		"syncStartupPipelineCursorNextStage=SyncCommitment",
		"syncStartupStagedRestored=3",
		"syncStartupStagedTargetHead=9",
		"syncStartupStagedNextExpected=7",
		"syncStartupStagedNeedPruneTail=true",
		"syncStartupStagedPruneFrom=8",
		"syncStartupStagedHaveLastRestored=true",
		"syncStartupStagedLastRestored=6",
	} {
		if !strings.Contains(out, k) {
			t.Errorf("missing key/value %q in startup summary line:\n%s", k, out)
		}
	}
}

func TestSlowestStateCommitPhasePrefersAccountTrieLeafWhenAvailable(t *testing.T) {
	phase, elapsed := slowestStateCommitPhase(core.ApplyStats{
		StateCommitDetail: state.CommitStats{
			KVCompute:             2 * time.Second,
			AccountTrieUpdate:     10 * time.Second,
			AccountTrieMarshal:    time.Second,
			AccountTrieGeneration: 500 * time.Millisecond,
			AccountTrieWrite:      6 * time.Second,
		},
	})
	if phase != "accountTrieWrite" || elapsed != 6*time.Second {
		t.Fatalf("slowestStateCommitPhase = %s %v, want accountTrieWrite 6s", phase, elapsed)
	}
}

func TestSlowestStateCommitPhaseFallsBackToAccountTrieAggregate(t *testing.T) {
	phase, elapsed := slowestStateCommitPhase(core.ApplyStats{
		StateCommitDetail: state.CommitStats{
			KVCompute:         2 * time.Second,
			AccountTrieUpdate: 10 * time.Second,
		},
	})
	if phase != "accountTrieUpdate" || elapsed != 10*time.Second {
		t.Fatalf("slowestStateCommitPhase = %s %v, want accountTrieUpdate 10s", phase, elapsed)
	}
}
