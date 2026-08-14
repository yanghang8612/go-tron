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
		"peers=2", "activePeers=1", "inflight=2",
		"buffered=3", "requested=4", "retries=1",
	} {
		if !strings.Contains(segmentLine, field) {
			t.Errorf("missing real-time field %q:\n%s", field, segmentLine)
		}
	}
	for _, field := range []string{
		"Sync import diagnostics", "execElapsed=", "stateCommit=", "peerState=",
		"energy=", "energyPerSec=", "txTop=", "stateMutTop=", "stateMutKVTop=",
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
		{999_999.125, "999999.13"},
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
	if !strings.Contains(out, "Sync startup repair summary") {
		t.Fatalf("expected startup repair summary line, got:\n%s", out)
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
