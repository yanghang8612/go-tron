package snapshots

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// seedLatestRows writes the minimum set of hot-DB rows needed for BuildLatest
// to produce at least one segment ref per registered latest dataset.
func seedLatestRows(t *testing.T, db ethdb.KeyValueWriter, owner common.Address, blockNum, txNum uint64) {
	t.Helper()
	root := common.BytesToHash(bytes.Repeat([]byte{0xab}, common.HashLength))
	code := []byte{0x60, 0x00, 0x60, 0x01}
	codeHash := common.Keccak256(code)

	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account-v1")); err != nil {
		t.Fatalf("seed account latest: %v", err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 1); err != nil {
		t.Fatalf("seed kv generation: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 1, kvdomains.ContractStorage, []byte("slot/a"), []byte("storage-v1")); err != nil {
		t.Fatalf("seed kv latest: %v", err)
	}
	if err := rawdb.WriteStateCode(db, codeHash, code); err != nil {
		t.Fatalf("seed code: %v", err)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, root); err != nil {
		t.Fatalf("seed commitment root: %v", err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{
		BlockNum:  blockNum,
		BlockHash: common.Hash{byte(blockNum)},
		Root:      root,
		Scheme:    rawdb.LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("seed commitment checkpoint: %v", err)
	}
	// Seed a StateTxRange so latestBuildWatermark returns txNum > 0.
	if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, txNum, txNum); err != nil {
		t.Fatalf("seed tx range block %d: %v", blockNum, err)
	}
	writeColdBuilderCanonicalBlock(t, db, blockNum)
}

func coldBuilderCatalogIdentity() ChainIdentity {
	return ChainIdentity{
		ChainID:     1,
		NetworkID:   1,
		GenesisHash: strings.Repeat("79", common.HashLength),
	}
}

func TestColdBuilderConfigDefaultsHistoryDataset(t *testing.T) {
	cfg := Config{
		Dir:     t.TempDir(),
		Enabled: true,
	}.applyDefaults()
	if cfg.HistoryDataset != SegmentDatasetStateDomainChange {
		t.Fatalf("history dataset = %s, want %s", cfg.HistoryDataset, SegmentDatasetStateDomainChange)
	}
	if cfg.BatchBlocks != defaultColdSnapshotBatchBlocks || cfg.BatchTxNums != defaultColdSnapshotBatchTxNums {
		t.Fatalf("cold step defaults = %d blocks/%d txNums, want %d/%d", cfg.BatchBlocks, cfg.BatchTxNums, defaultColdSnapshotBatchBlocks, defaultColdSnapshotBatchTxNums)
	}
	if cfg.CompactMaxSteps != defaultCompactionMaxSteps {
		t.Fatalf("compact max steps = %d, want %d", cfg.CompactMaxSteps, defaultCompactionMaxSteps)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate defaulted config: %v", err)
	}
}

func TestColdBuilderConfigRejectsUnknownHistoryDataset(t *testing.T) {
	cfg := Config{
		Dir:            t.TempDir(),
		Enabled:        true,
		HistoryDataset: SegmentDataset("unknown-history"),
	}.applyDefaults()
	if err := cfg.validate(); err == nil {
		t.Fatal("unknown history dataset accepted")
	}
}

func TestColdBuilderConfigRejectsSyncEventCatchupWithoutEventBuilds(t *testing.T) {
	cfg := Config{
		Dir:                        t.TempDir(),
		Enabled:                    true,
		BuildEventLogsWhileSyncing: true,
	}.applyDefaults()
	if err := cfg.validate(); err == nil {
		t.Fatal("sync-time event-log catch-up without event-log builds accepted")
	}
}

func TestColdBuilderOnePassBuildsStateDomainChangeHistoryAndManagerReads(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x71)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderCanonicalBlock(t, db, 1)
	writeColdBuilderChange(t, db, owner, 2, 2, "b")
	writeColdBuilderCanonicalBlock(t, db, 2)
	writeColdBuilderChange(t, db, owner, 3, 3, "c")
	block3Hash := writeColdBuilderCanonicalBlock(t, db, 3)

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager before build: %v", err)
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 4}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("one pass: %v", err)
	}
	if !result.Built || result.FromTxNum != 1 || result.ToTxNum != 3 || result.CutoffBlock != 3 {
		t.Fatalf("result = %+v", result)
	}
	if result.Segment.Dataset != SegmentDatasetStateDomainChange || result.Segment.Kind != SegmentHistory {
		t.Fatalf("segment ref = %+v", result.Segment)
	}
	if len(result.Segments) != 3 {
		t.Fatalf("segment refs = %+v, want history/accessor/index refs", result.Segments)
	}
	if result.HistoryDuration <= 0 || result.PublishDuration <= 0 {
		t.Fatalf("phase durations = history %s publish %s, want both measured", result.HistoryDuration, result.PublishDuration)
	}
	for _, ref := range result.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange {
			t.Fatalf("segment ref dataset = %s, want %s: %+v", ref.Dataset, SegmentDatasetStateDomainChange, ref)
		}
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.VisibleTxStart != 1 || manifest.VisibleTxEnd != 3 || len(manifest.Segments) != 3 {
		t.Fatalf("manifest = %+v", manifest)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || !ok || got != 3 {
		t.Fatalf("snapshot build stage progress = %d ok=%v err=%v, want 3", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block3Hash {
		t.Fatalf("SnapshotBuild row = %+v ok=%v err=%v, want hash %x", row, ok, err, block3Hash)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHistory); err != nil || !ok || got != 3 {
		t.Fatalf("snapshot history stage progress = %d ok=%v err=%v, want 3", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotAccessor); err != nil || !ok || got != 3 {
		t.Fatalf("snapshot accessor stage progress = %d ok=%v err=%v, want 3", got, ok, err)
	}

	var got []string
	if err := mgr.IterateStateDomainChanges(1, 3, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, string(change.Prev))
		return true, nil
	}); err != nil {
		t.Fatalf("iterate state domain changes: %v", err)
	}
	if want := []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Fatalf("changes = %v, want %v", got, want)
	}
}

func TestColdSnapshotPublishedLogReportsOperationalProgress(t *testing.T) {
	var buf bytes.Buffer
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))

	runner := &Runner{cfg: Config{HistoryDataset: SegmentDatasetStateDomainChange}}
	runner.lastLagBlocks.Store(500)
	runner.lastHistoryBuildAt.Store(time.Now().Add(-10 * time.Second).UnixNano())
	result := PassResult{
		Built:                true,
		HistoryAccelerated:   true,
		FromTxNum:            100,
		ToTxNum:              199,
		FromBlock:            10,
		ToBlock:              19,
		PublishedBlock:       19,
		EligibleCutoffBlock:  419,
		HistoryDuration:      2 * time.Second,
		EventLogDuration:     time.Second,
		SectionBloomDuration: 250 * time.Millisecond,
		PublishDuration:      100 * time.Millisecond,
		Segments: []SegmentRef{
			{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, Size: 1_000},
			{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, Size: 2_000},
		},
	}
	logColdSnapshotPublished(runner, result, time.Now().Add(-4*time.Second), 1, 1_000)

	out := buf.String()
	for _, field := range []string{
		`msg="History cold snapshot published"`,
		"txs=100", "blocks=10", "totalBytes=3000", "historyElapsed=2s",
		"eventLogElapsed=1s", "publishElapsed=100ms", "publishedBlock=19",
		"eligibleCutoffBlock=419", "backlogBlocks=400", "accelerated=true",
		"blocksPerSec=", "txsPerSec=", "netCatchupBlocksPerSec=", "eta=",
	} {
		if !strings.Contains(out, field) {
			t.Errorf("missing published log field %q:\n%s", field, out)
		}
	}
}

func TestColdSnapshotETAKeepsSubCentPrecisionAndBoundsOverflow(t *testing.T) {
	if got := coldSnapshotDisplayRate(0.004); got != 0.004 {
		t.Fatalf("display rate = %v, want 0.004", got)
	}
	if eta, ok := coldSnapshotETA(1, 0.004); !ok || eta != 250*time.Second {
		t.Fatalf("eta = %s, %t, want 4m10s, true", eta, ok)
	}
	if eta, ok := coldSnapshotETA(^uint64(0), 1e-12); ok || eta != 0 {
		t.Fatalf("overflow eta = %s, %t, want suppressed", eta, ok)
	}
	if got := smoothColdSnapshotCatchupRate(100, 2, coldSnapshotCatchupRateReset); got != 2 {
		t.Fatalf("stale smoothed rate = %v, want reset to 2", got)
	}
}

func TestColdSnapshotPublishInfoIsSampledDuringAcceleratedCatchup(t *testing.T) {
	runner := &Runner{}
	result := PassResult{HistoryAccelerated: true, PublishedBlock: 50, EligibleCutoffBlock: 100}
	base := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)

	if info, suppressed := coldSnapshotPublishLogDecision(runner, result, base); !info || suppressed != 0 {
		t.Fatalf("first publication = info %t suppressed %d, want true/0", info, suppressed)
	}
	if info, _ := coldSnapshotPublishLogDecision(runner, result, base.Add(time.Second)); info {
		t.Fatal("accelerated publication inside sampling window logged at Info")
	}
	if info, suppressed := coldSnapshotPublishLogDecision(runner, result, base.Add(coldSnapshotPublishLogInterval)); !info || suppressed != 1 {
		t.Fatalf("sampled publication = info %t suppressed %d, want true/1", info, suppressed)
	}

	result.PublishedBlock = result.EligibleCutoffBlock
	if info, suppressed := coldSnapshotPublishLogDecision(runner, result, base.Add(coldSnapshotPublishLogInterval+time.Second)); !info || suppressed != 0 {
		t.Fatalf("final catch-up publication = info %t suppressed %d, want true/0", info, suppressed)
	}
}

func TestColdSnapshotPublishSamplingIsConcurrentSafe(t *testing.T) {
	runner := &Runner{}
	result := PassResult{HistoryAccelerated: true, PublishedBlock: 50, EligibleCutoffBlock: 100}
	base := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	if info, _ := coldSnapshotPublishLogDecision(runner, result, base); !info {
		t.Fatal("initial publication should establish the sampling window")
	}

	var reports atomic.Uint64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if info, _ := coldSnapshotPublishLogDecision(runner, result, base.Add(coldSnapshotPublishLogInterval)); info {
				reports.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := reports.Load(); got != 1 {
		t.Fatalf("concurrent Info reports = %d, want 1", got)
	}
}

func TestColdBuilderSubsequentPassSeeksFromPublishedBlock(t *testing.T) {
	dir := t.TempDir()
	store := &coldBuilderSeekRecordingDB{KeyValueStore: rawdb.NewMemoryDatabase()}
	owner := coldBuilderOwner(0x72)
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeColdBuilderChange(t, store, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, store, blockNum)
	}
	runner := NewRunner(&coldBuilderChain{db: store, solidified: 4}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
		BatchBlocks:   1,
	})
	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !first.Built || first.FromBlock != 1 || first.ToBlock != 1 {
		t.Fatalf("first pass = %+v, want block 1", first)
	}

	store.stateTxRangeStarts = nil
	second, err := runner.OnePass()
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if !second.Built || second.FromBlock != 2 || second.ToBlock != 2 {
		t.Fatalf("second pass = %+v, want block 2", second)
	}
	var want [8]byte
	binary.BigEndian.PutUint64(want[:], 2)
	seekCount := 0
	for _, start := range store.stateTxRangeStarts {
		if bytes.Equal(start, want[:]) {
			seekCount++
		}
	}
	// Boundary discovery, record collation, and the single-pass tx-range
	// emission must all use the bounded source rather than rescanning the prefix.
	if seekCount < 3 {
		t.Fatalf("state tx-range iterator starts = %x, want at least three seek starts %x", store.stateTxRangeStarts, want)
	}
}

func TestColdBuilderCapsBaseStepByTxNumsAtWholeBlockBoundary(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7a)
	for blockNum := uint64(1); blockNum <= 4; blockNum++ {
		endTxNum := blockNum * 2
		writeColdBuilderChange(t, db, owner, blockNum, endTxNum, "previous")
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, endTxNum-1, endTxNum); err != nil {
			t.Fatalf("write two-tx range for block %d: %v", blockNum, err)
		}
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 5}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
		BatchBlocks:   100,
		BatchTxNums:   3,
	})

	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !first.Built || first.FromBlock != 1 || first.ToBlock != 2 || first.FromTxNum != 1 || first.ToTxNum != 4 {
		t.Fatalf("first pass = %+v, want whole blocks [1,2] and tx range [1,4]", first)
	}
	if first.EligibleCutoffBlock != 4 || !first.NeedsCatchup() {
		t.Fatalf("first pass = %+v, want eligible cutoff 4 with immediate catch-up", first)
	}
	for _, ref := range first.Segments {
		if ref.Dataset == SegmentDatasetStateDomainChange && ref.AggregationSteps != 1 {
			t.Fatalf("base step ref = %+v, want one aggregation step", ref)
		}
	}

	second, err := runner.OnePass()
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if !second.Built || second.FromBlock != 3 || second.ToBlock != 4 || second.FromTxNum != 5 || second.ToTxNum != 8 {
		t.Fatalf("second pass = %+v, want whole blocks [3,4] and tx range [5,8]", second)
	}
	if second.NeedsCatchup() {
		t.Fatalf("second pass still reports catch-up: %+v", second)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if got := ContiguousHistoryVisibleTxEnd(manifest, SegmentDatasetStateDomainChange, 1); got != 8 {
		t.Fatalf("contiguous history end = %d, want 8", got)
	}
}

func TestColdBuilderTxNumCapNeverSplitsDenseBlock(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7b)
	writeColdBuilderChange(t, db, owner, 1, 10, "previous")
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 1, 10); err != nil {
		t.Fatalf("write dense block range: %v", err)
	}
	writeColdBuilderCanonicalBlock(t, db, 1)
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
		BatchBlocks:   100,
		BatchTxNums:   3,
	})

	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("one pass: %v", err)
	}
	if !result.Built || result.FromBlock != 1 || result.ToBlock != 1 || result.FromTxNum != 1 || result.ToTxNum != 10 {
		t.Fatalf("result = %+v, want complete dense block [1,10]", result)
	}
}

func BenchmarkColdBuilderTxNumCutoffSeek(b *testing.B) {
	db, err := rawdb.NewPebbleDB(b.TempDir(), 64, 64)
	if err != nil {
		b.Fatalf("open Pebble: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	const (
		blocks     = uint64(5_000)
		txPerBlock = uint64(100)
	)
	for blockNum := uint64(1); blockNum <= blocks; blockNum++ {
		beginTxNum := (blockNum-1)*txPerBlock + 1
		endTxNum := blockNum * txPerBlock
		if writeErr := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, beginTxNum, endTxNum); writeErr != nil {
			b.Fatalf("write range %d: %v", blockNum, writeErr)
		}
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		b.Fatal("state-domain-change config missing")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block, found, err := firstHotHistoryTxRangeBlockAtOrAfterTx(cfg, db, defaultColdSnapshotBatchTxNums, 1, blocks)
		if err != nil || !found || block != 3_907 {
			b.Fatalf("cutoff = %d/%v err=%v, want 3907/true/nil", block, found, err)
		}
	}
}

func TestColdBuilderMetricsExposeBuildLagAndPhaseDurations(t *testing.T) {
	namespace := normalizeColdSnapshotMetricNamespace("test/state/snapshot/cold/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x73)
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 4}, Config{
		Dir:              dir,
		Enabled:          true,
		Interval:         time.Hour,
		HistoryWindow:    1,
		BatchBlocks:      1,
		MetricsNamespace: namespace,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("one pass: %v", err)
	}
	if !result.Built {
		t.Fatalf("result = %+v, want built segment", result)
	}
	stats := runner.Snapshot()
	if stats.PassesCompleted != 1 || stats.PassErrors != 0 || stats.SegmentsBuilt != 1 || stats.BytesBuilt == 0 {
		t.Fatalf("runner counters = %+v", stats)
	}
	if stats.LastEligibleCutoffBlock != 3 || stats.LastCutoffBlock != 1 || stats.LastPublishedBlock != 1 || stats.LastLagBlocks != 2 {
		t.Fatalf("runner progress = %+v", stats)
	}
	if stats.LastPassDuration <= 0 || stats.LastBuildDuration <= 0 || stats.LastCompactionDuration <= 0 || stats.LastLatestDuration <= 0 {
		t.Fatalf("runner durations = %+v", stats)
	}

	assertColdRunnerGauge(t, namespace+"passes", 1)
	assertColdRunnerGauge(t, namespace+"errors", 0)
	assertColdRunnerGauge(t, namespace+"segments/built", 1)
	assertColdRunnerGauge(t, namespace+"compaction/merges", 0)
	assertColdRunnerGauge(t, namespace+"compaction/deferred/catchup", 1)
	assertColdRunnerGauge(t, namespace+"lastpass/compaction/merges", 0)
	assertColdRunnerGauge(t, namespace+"last/eligible_cutoff_block", 3)
	assertColdRunnerGauge(t, namespace+"last/selected_cutoff_block", 1)
	assertColdRunnerGauge(t, namespace+"last/published_block", 1)
	assertColdRunnerGauge(t, namespace+"lag/blocks", 2)
	if got := coldRunnerGaugeValue(t, namespace+"bytes/built"); got <= 0 {
		t.Fatalf("bytes/built = %d, want positive", got)
	}
	for _, suffix := range []string{"lastpass/duration", "lastpass/build/duration", "lastpass/compaction/duration", "lastpass/latest/duration"} {
		if got := coldRunnerGaugeValue(t, namespace+suffix); got <= 0 {
			t.Fatalf("%s = %d, want positive", suffix, got)
		}
	}
}

func TestColdBuilderDefersFullLatestBuildDuringActiveSync(t *testing.T) {
	namespace := normalizeColdSnapshotMetricNamespace("test/state/snapshot/cold/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x76)
	seedLatestRows(t, db, owner, 50, 50)
	chain := &coldBuilderChain{db: db, solidified: 50, syncRemaining: 1_000, syncRemainingOK: true}
	runner := NewRunner(chain, Config{
		Dir:                          dir,
		Enabled:                      true,
		Interval:                     time.Hour,
		HistoryWindow:                100,
		LatestBuildBlocks:            10,
		DeferLatestBuildWhileSyncing: true,
		MetricsNamespace:             namespace,
	})

	deferred, err := runner.OnePass()
	if err != nil {
		t.Fatalf("deferred pass: %v", err)
	}
	if !deferred.LatestDeferred || deferred.LatestBuilt {
		t.Fatalf("deferred pass = %+v, want latest build deferred", deferred)
	}
	if stats := runner.Snapshot(); stats.LatestDeferredSync != 1 {
		t.Fatalf("runner stats = %+v, want one sync deferral", stats)
	}
	assertColdRunnerGauge(t, namespace+"latest/deferred/sync", 1)
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatestBuild); err != nil || ok {
		t.Fatalf("latest build stage exists during sync: ok=%v err=%v", ok, err)
	}

	chain.syncRemainingOK = false
	built, err := runner.OnePass()
	if err != nil {
		t.Fatalf("post-sync pass: %v", err)
	}
	if !built.LatestBuilt || built.LatestDeferred {
		t.Fatalf("post-sync pass = %+v, want latest build", built)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || got != 50 {
		t.Fatalf("latest build stage = %d ok=%v err=%v, want 50", got, ok, err)
	}
}

func TestColdBuilderBuildsOnlyCommitmentBranchBaseDuringActiveSync(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x77)
	seedLatestRows(t, db, owner, 50, 50)
	root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok {
		t.Fatalf("read root ok=%v err=%v", ok, err)
	}
	if err := rawdb.WriteCommitmentBranch(db, nil, []byte("root-branch")); err != nil {
		t.Fatal(err)
	}
	rotation := rawdb.CommitmentBranchRotation{
		Generation: 1, SnapshotTxNum: 50, Root: root, BlockNum: 50, BlockHash: common.Hash{50},
	}
	if err := rawdb.WriteCommitmentBranchRotation(db, rotation); err != nil {
		t.Fatal(err)
	}
	chain := &coldBuilderChain{
		db: db, solidified: 50, syncRemaining: 1_000, syncRemainingOK: true,
		rotation: &rotation, persistRotationBase: true,
	}
	runner := NewRunner(chain, Config{
		Dir: dir, Enabled: true, Interval: time.Hour, HistoryWindow: 100,
		LatestBuildBlocks: 10, DeferLatestBuildWhileSyncing: true,
		BuildCommitmentBranchBaseWhileSyncing: true,
	})
	// Model a recent full latest build: an already-started branch rotation must
	// resume independently of the coarse full-latest cadence.
	runner.lastLatestBuildBlock.Store(50)

	built, deferred, err := runner.latestPassWithStatus()
	if err != nil || !built || deferred {
		t.Fatalf("sync commitment pass built=%v deferred=%v err=%v", built, deferred, err)
	}
	if chain.rotationBegin != 1 || chain.rotationComplete != 1 {
		t.Fatalf("rotation calls begin=%d complete=%d", chain.rotationBegin, chain.rotationComplete)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range manifest.Segments {
		if ref.Dataset != SegmentDatasetCommitmentRoot && ref.Dataset != SegmentDatasetCommitmentBranch {
			t.Fatalf("sync commitment pass published unrelated ref %+v", ref)
		}
	}
	if manifest.Progress == nil || manifest.Progress.LatestBuildTxNum != 0 || manifest.Progress.AccessorBuildTxNum != 0 || manifest.Progress.CommitmentFlushTxNum != 0 {
		t.Fatalf("sync commitment progress = %+v", manifest.Progress)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	if txNum, ok, err := mgr.LatestStateTxNum(); err != nil || ok {
		t.Fatalf("commitment-only manifest advertised general latest state tx=%d ok=%v err=%v", txNum, ok, err)
	}
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatestBuild); err != nil || ok {
		t.Fatalf("full latest build stage exists ok=%v err=%v", ok, err)
	}
	base, ok, err := rawdb.ReadCommitmentBranchBase(db)
	if err != nil || !ok || base.BlockNum != rotation.BlockNum || base.BlockHash != rotation.BlockHash {
		t.Fatalf("persisted base = %+v ok=%v err=%v", base, ok, err)
	}

	// The baseline block, rather than the deferred full-latest watermark, owns
	// the sync-time cadence. A second pass at the same boundary must not rotate.
	built, deferred, err = runner.latestPassWithStatus()
	if err != nil || built || deferred {
		t.Fatalf("cadence pass built=%v deferred=%v err=%v", built, deferred, err)
	}
	if chain.rotationBegin != 1 {
		t.Fatalf("rotation began again inside cadence: %d", chain.rotationBegin)
	}
}

func TestColdBuilderDefersHistoryAndCompactionWhileFarBehind(t *testing.T) {
	namespace := normalizeColdSnapshotMetricNamespace("test/state/snapshot/cold/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x78)
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	chain := &coldBuilderChain{db: db, solidified: 3, syncRemaining: 100, syncRemainingOK: true}
	runner := NewRunner(chain, Config{
		Dir:                           dir,
		Enabled:                       true,
		HistoryWindow:                 1,
		LatestBuildBlocks:             0,
		DeferHistoryBuildWhileSyncing: true,
		MaxDeferredHistoryBlocks:      2,
		MetricsNamespace:              namespace,
	})

	deferred, err := runner.OnePass()
	if err != nil {
		t.Fatalf("deferred history pass: %v", err)
	}
	if !deferred.HistoryDeferred || deferred.Built || deferred.Compaction.Merged {
		t.Fatalf("deferred pass = %+v, want history and compaction deferred", deferred)
	}
	if stats := runner.Snapshot(); stats.HistoryDeferredSync != 1 || stats.SegmentsBuilt != 0 || stats.SegmentsCompacted != 0 {
		t.Fatalf("runner stats = %+v, want one history sync deferral", stats)
	}
	assertColdRunnerGauge(t, namespace+"history/deferred/sync", 1)

	chain.syncRemaining = 1
	built, err := runner.OnePass()
	if err != nil {
		t.Fatalf("near-tip history pass: %v", err)
	}
	if !built.Built || built.HistoryDeferred {
		t.Fatalf("near-tip pass = %+v, want bounded cold build", built)
	}
}

func TestColdBuilderForcesBoundedCatchupPastDeferredBacklogCap(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7b)
	for blockNum := uint64(1); blockNum <= 4; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	runner := NewRunner(&coldBuilderChain{
		db: db, solidified: 4, syncRemaining: 1_000, syncRemainingOK: true,
	}, Config{
		Dir:                           dir,
		Enabled:                       true,
		HistoryWindow:                 1,
		BatchBlocks:                   1,
		DeferHistoryBuildWhileSyncing: true,
		MaxDeferredHistoryBlocks:      2,
	})

	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if !result.Built || result.HistoryDeferred || result.FromBlock != 1 || result.ToBlock != 1 {
		t.Fatalf("over-cap pass = %+v, want one bounded history batch despite active deep sync", result)
	}
}

func TestColdBuilderWaitsForImporterCapacityBetweenSoftAndHardCaps(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7c)
	for blockNum := uint64(1); blockNum <= 4; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	ready := false
	callbackCalls := 0
	runner := NewRunner(&coldBuilderChain{
		db: db, solidified: 4, syncRemaining: 1_000, syncRemainingOK: true,
	}, Config{
		Dir:                           dir,
		Enabled:                       true,
		HistoryWindow:                 1,
		BatchBlocks:                   1,
		DeferHistoryBuildWhileSyncing: true,
		MaxDeferredHistoryBlocks:      2,
		MaxBusyDeferredHistoryBlocks:  4,
		SyncBuildReady: func() bool {
			callbackCalls++
			return ready
		},
	})

	deferred, err := runner.OnePass()
	if err != nil {
		t.Fatalf("busy OnePass: %v", err)
	}
	if !deferred.HistoryDeferred || deferred.Built || callbackCalls != 1 {
		t.Fatalf("busy pass = %+v callbackCalls=%d, want capacity deferral", deferred, callbackCalls)
	}

	ready = true
	built, err := runner.OnePass()
	if err != nil {
		t.Fatalf("ready OnePass: %v", err)
	}
	if !built.Built || built.HistoryDeferred || built.FromBlock != 1 || built.ToBlock != 1 || callbackCalls != 2 {
		t.Fatalf("ready pass = %+v callbackCalls=%d, want one admitted bounded build", built, callbackCalls)
	}
}

func TestColdBuilderHardCapPreventsBusyImporterStarvation(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7d)
	for blockNum := uint64(1); blockNum <= 6; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	callbackCalls := 0
	runner := NewRunner(&coldBuilderChain{
		db: db, solidified: 6, syncRemaining: 1_000, syncRemainingOK: true,
	}, Config{
		Dir:                           dir,
		Enabled:                       true,
		HistoryWindow:                 1,
		BatchBlocks:                   1,
		DeferHistoryBuildWhileSyncing: true,
		MaxDeferredHistoryBlocks:      2,
		MaxBusyDeferredHistoryBlocks:  4,
		SyncBuildReady: func() bool {
			callbackCalls++
			return false
		},
	})

	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if !result.Built || result.HistoryDeferred || result.FromBlock != 1 || result.ToBlock != 1 {
		t.Fatalf("hard-cap pass = %+v, want one forced bounded build", result)
	}
	if callbackCalls != 0 {
		t.Fatalf("hard-cap pass consulted capacity callback %d times, want 0", callbackCalls)
	}
}

func TestColdBuilderDefersDerivedSidecarsWhileFarBehind(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7a)
	writeColdBuilderChange(t, db, owner, 1, 1, "previous")
	writeColdBuilderCanonicalBlock(t, db, 1)
	runner := NewRunner(&coldBuilderChain{
		db: db, solidified: 2, syncRemaining: 1_000, syncRemainingOK: true,
	}, Config{
		Dir:                           dir,
		Enabled:                       true,
		HistoryWindow:                 1,
		DeferHistoryBuildWhileSyncing: true,
		BuildSectionBlooms:            true,
		BuildBalanceTraces:            true,
		BuildEventLogs:                true,
	})

	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("far-behind pass: %v", err)
	}
	if !result.HistoryDeferred || result.Built || result.SectionBloomBuilt || result.BalanceTraceBuilt || result.EventLogBuilt {
		t.Fatalf("far-behind pass = %+v, want all cold and derived builds deferred", result)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read snapshot dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("snapshot dir contains %d entries during deep sync, want none", len(entries))
	}
}

func TestColdBuilderDecouplesAndRestartsDerivedSidecarCatchup(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x7d)
	address := eventLogTestAddress(0x7e)
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		block, infos := coldBuilderEventLogBlock(t, blockNum, []*corepb.TransactionInfo_Log{{
			Address: address,
			Data:    []byte{byte(blockNum)},
		}})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			t.Fatalf("WriteTransactionInfosByBlock %d: %v", blockNum, err)
		}
	}
	chain := &coldBuilderChain{
		db: db, solidified: 4, syncRemaining: 1_000, syncRemainingOK: true,
	}
	cfg := Config{
		Dir:                              dir,
		Enabled:                          true,
		HistoryWindow:                    1,
		BatchBlocks:                      1,
		DeferDerivedSidecarsWhileSyncing: true,
		BuildEventLogs:                   true,
		EventLogVersion:                  EventLogSegmentV4Version,
	}
	runner := NewRunner(chain, cfg)
	for pass := uint64(1); pass <= 2; pass++ {
		result, err := runner.OnePass()
		if err != nil {
			t.Fatalf("sync pass %d: %v", pass, err)
		}
		if !result.Built || result.ToBlock != pass || result.EventLogBuilt || !result.DerivedSidecarsDeferred {
			t.Fatalf("sync pass %d = %+v, want history-only publication", pass, result)
		}
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest during sync: %v", err)
	}
	if logs := eventLogRefs(manifest); len(logs) != 0 {
		t.Fatalf("sync-time event logs = %+v, want none", logs)
	}

	// Finish sync. The last state-history batch may publish its own sidecar,
	// leaving an older hole which the independent cursor must fill safely.
	chain.syncRemainingOK = false
	lastHistory, err := runner.OnePass()
	if err != nil {
		t.Fatalf("last history pass: %v", err)
	}
	if !lastHistory.Built || lastHistory.ToBlock != 3 || !lastHistory.EventLogBuilt {
		t.Fatalf("last history pass = %+v, want block 3 history+event log", lastHistory)
	}
	// Simulate a crash after atomic manifest publication but before the
	// hash-bound DB stage write. The retained StateTxRange mapping must repair
	// the history boundary before sidecar catch-up chooses its upper bound.
	if err := rawdb.DeleteStageProgress(db, rawdb.StageSnapshotBuild); err != nil {
		t.Fatalf("DeleteStageProgress SnapshotBuild: %v", err)
	}

	firstCatchup, err := runner.OnePass()
	if err != nil {
		t.Fatalf("first sidecar catch-up: %v", err)
	}
	if firstCatchup.Built || !firstCatchup.DerivedSidecarCatchup || !firstCatchup.EventLogBuilt ||
		!firstCatchup.DerivedSidecarsPending || !firstCatchup.NeedsCatchup() {
		t.Fatalf("first sidecar catch-up = %+v, want bounded progress with debt", firstCatchup)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || !ok || got != 3 {
		t.Fatalf("repaired StageSnapshotBuild = %d ok=%v err=%v, want 3", got, ok, err)
	}

	// Recreate the runner to prove that active manifest coverage, rather than an
	// in-memory queue, resumes the remaining sidecar debt.
	runner = NewRunner(chain, cfg)
	finalCatchup, err := runner.OnePass()
	if err != nil {
		t.Fatalf("restart sidecar catch-up: %v", err)
	}
	if finalCatchup.Built || !finalCatchup.DerivedSidecarCatchup || !finalCatchup.EventLogBuilt ||
		finalCatchup.DerivedSidecarsPending || finalCatchup.NeedsCatchup() {
		t.Fatalf("restart sidecar catch-up = %+v, want debt fully drained", finalCatchup)
	}
	manifest, err = LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest after catch-up: %v", err)
	}
	if logs, indexes := eventLogRefs(manifest), eventLogIndexRefs(manifest); len(logs) != 3 || len(indexes) != 3 {
		t.Fatalf("event sidecars after catch-up = logs %+v indexes %+v, want three aligned pairs", logs, indexes)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || got != 3 {
		t.Fatalf("StageSnapshotEventLogBuild = %d ok=%v err=%v, want 3", got, ok, err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogIndexedRangeCovered(1, 3)
	if err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered = %v/%v, want true/nil", covered, err)
	}
}

func TestColdBuilderKeepsRequiredEventLogCoverageMovingDuringSync(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x8d)
	address := eventLogTestAddress(0x8e)
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		block, infos := coldBuilderEventLogBlock(t, blockNum, []*corepb.TransactionInfo_Log{{
			Address: address,
			Data:    []byte{byte(blockNum)},
		}})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			t.Fatalf("WriteTransactionInfosByBlock %d: %v", blockNum, err)
		}
	}
	chain := &coldBuilderChain{
		db: db, solidified: 4, syncRemaining: 1_000, syncRemainingOK: true,
	}
	cfg := Config{
		Dir:                              dir,
		Enabled:                          true,
		HistoryWindow:                    1,
		BatchBlocks:                      1,
		DeferHistoryBuildWhileSyncing:    false,
		DeferDerivedSidecarsWhileSyncing: true,
		BuildEventLogs:                   true,
		EventLogVersion:                  EventLogSegmentV4Version,
	}

	// Establish a restart-safe history/event-log gap using the normal sync-time
	// deferral policy.
	runner := NewRunner(chain, cfg)
	for pass := uint64(1); pass <= 3; pass++ {
		result, err := runner.OnePass()
		if err != nil {
			t.Fatalf("deferred sync pass %d: %v", pass, err)
		}
		if !result.Built || result.EventLogBuilt {
			t.Fatalf("deferred sync pass %d = %+v, want history-only publication", pass, result)
		}
	}

	// Direct V2 receipt-log externalization enables only the event-log family
	// during sync. Cap it at the end of the next freezer segment even though the
	// verified history boundary is farther ahead, so one successful build hands
	// the shared heavy-work turn to V2 instead of immediately scheduling itself.
	targetBlock := uint64(2)
	cfg.BuildEventLogsWhileSyncing = true
	cfg.SyncEventLogCatchupBlocks = 2
	cfg.SyncEventLogTargetBlock = func() (uint64, bool) { return targetBlock, true }
	cfg.HeavyWorkGate = maintenance.NewHeavyWorkGateWithCooldown(time.Hour)
	cfg.CatchupHeavyWorkCooldown = time.Hour
	runner = NewRunner(chain, cfg)
	critical, err := runner.OnePass()
	if err != nil {
		t.Fatalf("sync-critical event-log pass: %v", err)
	}
	if critical.Built || !critical.DerivedSidecarCatchup ||
		!critical.EventLogBuilt || !critical.EventLogFreezerHandoff ||
		critical.DerivedSidecarsPending || critical.NeedsCatchup() {
		t.Fatalf("sync-critical pass = %+v, want one bounded event-log segment and freezer handoff", critical)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest after critical pass: %v", err)
	}
	manager, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager after critical pass: %v", err)
	}
	covered, err := manager.EventLogIndexedRangeCovered(1, 2)
	if err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered(1,2) = %v/%v, want true/nil; manifest=%+v", covered, err, manifest)
	}
	covered, err = manager.EventLogIndexedRangeCovered(1, 3)
	if err != nil || covered {
		t.Fatalf("EventLogIndexedRangeCovered(1,3) = %v/%v, want false/nil", covered, err)
	}

	// Re-running before V2 advances must not build the next event-log range.
	waiting, err := runner.OnePass()
	if err != nil {
		t.Fatalf("event-log pass while waiting for freezer: %v", err)
	}
	if waiting.EventLogBuilt || waiting.DerivedSidecarCatchup || waiting.NeedsCatchup() {
		t.Fatalf("waiting pass = %+v, want no event-log work before freezer advances", waiting)
	}

	// Advancing V2 moves the callback target and admits exactly the next range.
	targetBlock = 3
	cfg.HeavyWorkGate = nil
	cfg.CatchupHeavyWorkCooldown = 0
	runner = NewRunner(chain, cfg)
	final, err := runner.OnePass()
	if err != nil {
		t.Fatalf("event-log pass after freezer advance: %v", err)
	}
	if final.Built || !final.DerivedSidecarCatchup || !final.EventLogBuilt ||
		final.DerivedSidecarsPending || final.NeedsCatchup() {
		t.Fatalf("event-log pass after freezer advance = %+v, want exactly the next range", final)
	}
	manager, err = OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager after final pass: %v", err)
	}
	covered, err = manager.EventLogIndexedRangeCovered(1, 3)
	if err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered(1,3) = %v/%v, want true/nil", covered, err)
	}
}

func TestColdBuilderPrepareSeedsPersistedHistoryMetrics(t *testing.T) {
	namespace := normalizeColdSnapshotMetricNamespace("test/state/snapshot/cold/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	db := rawdb.NewMemoryDatabase()
	snapshotHash := writeColdBuilderCanonicalBlock(t, db, 10)
	finishHash := writeColdBuilderCanonicalBlock(t, db, 14)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotBuild, 10, snapshotHash); err != nil {
		t.Fatalf("write snapshot-build stage: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 14, finishHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 20}, Config{
		Dir:              t.TempDir(),
		Enabled:          true,
		HistoryWindow:    5,
		MetricsNamespace: namespace,
	})
	if err := runner.Prepare(); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stats := runner.Snapshot()
	if stats.LastSolidified != 20 || stats.LastEligibleCutoffBlock != 14 ||
		stats.LastCutoffBlock != 14 || stats.LastPublishedBlock != 10 || stats.LastLagBlocks != 4 {
		t.Fatalf("seeded history stats = %+v", stats)
	}
	for suffix, want := range map[string]int64{
		"last/solidified_block":      20,
		"last/eligible_cutoff_block": 14,
		"last/selected_cutoff_block": 14,
		"last/published_block":       10,
		"lag/blocks":                 4,
	} {
		if got := coldRunnerGaugeValue(t, namespace+suffix); got != want {
			t.Fatalf("gauge %s = %d, want %d", suffix, got, want)
		}
	}
}

func TestColdBuilderRateLimitsLargeBacklogDuringSync(t *testing.T) {
	namespace := normalizeColdSnapshotMetricNamespace("test/state/snapshot/cold/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x79)
	for blockNum := uint64(1); blockNum <= 6; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	chain := &coldBuilderChain{db: db, solidified: 7, syncRemaining: 100, syncRemainingOK: true}
	runner := NewRunner(chain, Config{
		Dir:                     dir,
		Enabled:                 true,
		HistoryWindow:           1,
		BatchBlocks:             2,
		CatchupBuildMinInterval: time.Hour,
		MetricsNamespace:        namespace,
	})

	first, err := runner.OnePass()
	if err != nil || !first.Built || !first.NeedsCatchup() {
		t.Fatalf("first bounded pass = %+v err=%v", first, err)
	}
	if lag := first.EligibleCutoffBlock - first.PublishedBlock; lag <= 2 {
		t.Fatalf("first bounded pass lag = %d, want backlog larger than one batch", lag)
	}
	deferred, err := runner.OnePass()
	if err != nil {
		t.Fatalf("rate-limited pass: %v", err)
	}
	if !deferred.HistoryDeferred || !deferred.HistoryRateLimited || deferred.Built {
		t.Fatalf("rate-limited pass = %+v", deferred)
	}
	if stats := runner.Snapshot(); stats.HistoryRateLimitedSync != 1 || stats.HistoryDeferredSync != 0 {
		t.Fatalf("rate-limit stats = %+v", stats)
	}
	assertColdRunnerGauge(t, namespace+"history/deferred/rate_limit", 1)

	chain.syncRemainingOK = false
	final, err := runner.OnePass()
	if err != nil || !final.Built || final.HistoryDeferred {
		t.Fatalf("post-sync pass = %+v err=%v", final, err)
	}
}

func TestColdBuilderAcceleratesDeepBacklogDuringSync(t *testing.T) {
	namespace := normalizeColdSnapshotMetricNamespace("test/state/snapshot/cold/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7b)
	for blockNum := uint64(1); blockNum <= 8; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	chain := &coldBuilderChain{db: db, solidified: 9, syncRemaining: 100, syncRemainingOK: true}
	runner := NewRunner(chain, Config{
		Dir:                         dir,
		Enabled:                     true,
		HistoryWindow:               1,
		BatchBlocks:                 2,
		CatchupBuildMinInterval:     time.Hour,
		CatchupUnthrottledLagBlocks: 4,
		MetricsNamespace:            namespace,
	})

	first, err := runner.OnePass()
	if err != nil || !first.Built || !first.HistoryAccelerated || first.ToBlock != 2 {
		t.Fatalf("first accelerated pass = %+v err=%v", first, err)
	}
	second, err := runner.OnePass()
	if err != nil || !second.Built || !second.HistoryAccelerated || second.ToBlock != 4 {
		t.Fatalf("second accelerated pass = %+v err=%v", second, err)
	}
	deferred, err := runner.OnePass()
	if err != nil {
		t.Fatalf("post-acceleration rate-limited pass: %v", err)
	}
	if !deferred.HistoryDeferred || !deferred.HistoryRateLimited || deferred.HistoryAccelerated || deferred.Built {
		t.Fatalf("post-acceleration pass = %+v", deferred)
	}
	stats := runner.Snapshot()
	if stats.SegmentsBuilt != 2 || stats.HistoryAcceleratedBuilds != 2 || stats.HistoryRateLimitedSync != 1 || stats.LastLagBlocks != 4 {
		t.Fatalf("accelerated stats = %+v", stats)
	}
	assertColdRunnerGauge(t, namespace+"history/accelerated/builds", 2)
	assertColdRunnerGauge(t, namespace+"history/deferred/rate_limit", 1)
}

func TestColdBuilderDefersWhenHeavyWorkGateIsBusy(t *testing.T) {
	namespace := normalizeColdSnapshotMetricNamespace("test/state/snapshot/cold/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7a)
	writeColdBuilderChange(t, db, owner, 1, 1, "previous")
	writeColdBuilderCanonicalBlock(t, db, 1)
	gate := maintenance.NewHeavyWorkGate()
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("hold maintenance gate")
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:              dir,
		Enabled:          true,
		HistoryWindow:    1,
		HeavyWorkGate:    gate,
		MetricsNamespace: namespace,
	})

	deferred, err := runner.OnePass()
	if err != nil {
		t.Fatalf("resource-deferred pass: %v", err)
	}
	if !deferred.HistoryDeferred || !deferred.HistoryGateDeferred || deferred.Built {
		t.Fatalf("resource-deferred pass = %+v", deferred)
	}
	if stats := runner.Snapshot(); stats.HistoryGateDeferred != 1 || stats.HistoryDeferredSync != 0 {
		t.Fatalf("resource-deferred stats = %+v", stats)
	}
	assertColdRunnerGauge(t, namespace+"history/deferred/resource", 1)

	release()
	built, err := runner.OnePass()
	if err != nil || !built.Built {
		t.Fatalf("pass after gate release = %+v err=%v", built, err)
	}
}

func TestColdBuilderAcceleratedBuildUsesShortRecoveryCooldown(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7c)
	for blockNum := uint64(1); blockNum <= 4; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	gate := maintenance.NewHeavyWorkGateWithCooldown(time.Hour)
	runner := NewRunner(&coldBuilderChain{
		db:              db,
		solidified:      5,
		syncRemaining:   100,
		syncRemainingOK: true,
	}, Config{
		Dir:                         dir,
		Enabled:                     true,
		HistoryWindow:               1,
		BatchBlocks:                 1,
		CatchupUnthrottledLagBlocks: 1,
		CatchupHeavyWorkCooldown:    20 * time.Millisecond,
		HeavyWorkGate:               gate,
	})

	first, err := runner.OnePass()
	if err != nil || !first.Built || !first.HistoryAccelerated {
		t.Fatalf("first accelerated pass = %+v err=%v", first, err)
	}
	deferred, err := runner.OnePass()
	if err != nil {
		t.Fatalf("cooldown-deferred pass: %v", err)
	}
	if !deferred.HistoryGateDeferred || deferred.HistoryRetryAfter <= 0 || deferred.HistoryRetryAfter > 20*time.Millisecond {
		t.Fatalf("cooldown-deferred pass = %+v, want retry within 20ms", deferred)
	}
	time.Sleep(deferred.HistoryRetryAfter + 5*time.Millisecond)
	second, err := runner.OnePass()
	if err != nil || !second.Built || !second.HistoryAccelerated {
		t.Fatalf("second accelerated pass = %+v err=%v", second, err)
	}
}

func TestColdBuilderDeferredPassPreservesLastSuccessfulBuildDuration(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x7d)
	for blockNum := uint64(1); blockNum <= 2; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	gate := maintenance.NewHeavyWorkGate()
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 3}, Config{
		Dir:           dir,
		Enabled:       true,
		HistoryWindow: 1,
		BatchBlocks:   1,
		HeavyWorkGate: gate,
	})

	first, err := runner.OnePass()
	if err != nil || !first.Built {
		t.Fatalf("successful pass = %+v err=%v", first, err)
	}
	want := runner.Snapshot().LastBuildDuration
	if want <= 0 {
		t.Fatalf("successful build duration = %s, want positive", want)
	}
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("hold maintenance gate")
	}
	deferred, err := runner.OnePass()
	release()
	if err != nil || !deferred.HistoryGateDeferred || deferred.Built {
		t.Fatalf("deferred pass = %+v err=%v", deferred, err)
	}
	if got := runner.Snapshot().LastBuildDuration; got != want {
		t.Fatalf("last successful build duration = %s after deferred pass, want %s", got, want)
	}
}

func TestColdBuilderLoopDrainsReadyBatchesWithoutIntervalWait(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x74)
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 4}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
		BatchBlocks:   1,
	})
	if err := runner.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		if err := runner.Stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	waitColdBuilderStats(t, runner, func(stats Stats) bool {
		return stats.LastPublishedBlock == 3 && stats.LastLagBlocks == 0
	})
	stats := runner.Snapshot()
	if stats.PassesCompleted != 3 || stats.SegmentsBuilt != 3 {
		t.Fatalf("runner stats = %+v, want three immediately drained batches", stats)
	}
}

func TestColdBuilderLoopStopsCatchupWhenPassMakesNoProgress(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x75)
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		writeColdBuilderCanonicalBlock(t, db, blockNum)
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 4}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
		BatchBlocks:   1,
	})
	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !first.NeedsCatchup() || first.PublishedBlock != 1 || first.EligibleCutoffBlock != 3 {
		t.Fatalf("first pass = %+v, want one built batch with backlog", first)
	}
	// Remove the verified cutoff range after the first publication. The loop's
	// initial pass now makes no progress and must not keep requeueing itself.
	if err := rawdb.DeleteStateTxRange(db, 3); err != nil {
		t.Fatalf("delete cutoff range: %v", err)
	}
	if err := runner.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		if err := runner.Stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	waitColdBuilderStats(t, runner, func(stats Stats) bool { return stats.PassesCompleted >= 2 })
	time.Sleep(50 * time.Millisecond)
	stats := runner.Snapshot()
	if stats.PassesCompleted != 2 || stats.SegmentsBuilt != 1 || stats.LastLagBlocks != 2 {
		t.Fatalf("runner stats = %+v, want one build, one no-progress pass, and no spin", stats)
	}
}

func TestColdBuilderOnePassPublishesSignedCatalog(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x79)
	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderCanonicalBlock(t, db, 1)
	writeColdBuilderChange(t, db, owner, 2, 2, "b")
	writeColdBuilderCanonicalBlock(t, db, 2)
	writeColdBuilderChange(t, db, owner, 3, 3, "c")
	writeColdBuilderCanonicalBlock(t, db, 3)

	identity := coldBuilderCatalogIdentity()
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x79}, ed25519.SeedSize))
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 4}, Config{
		Dir:               dir,
		Enabled:           true,
		Interval:          time.Hour,
		HistoryWindow:     1,
		CatalogSigningKey: privateKey,
		CatalogChain:      &identity,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if !result.Built || result.CatalogPublished {
		t.Fatalf("result = %+v, want history without standalone catalog publication", result)
	}
	published, err := runner.PublishCatalogIfManifestChanged()
	if err != nil || !published {
		t.Fatalf("PublishCatalogIfManifestChanged = %v/%v, want true/nil", published, err)
	}
	if _, report, err := VerifySignedSnapshotCatalog(dir, identity, []ed25519.PublicKey{privateKey.Public().(ed25519.PublicKey)}); err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog: %v", err)
	} else if report == nil || report.ActiveSegments == 0 {
		t.Fatalf("catalog report = %+v, want verified active segments", report)
	}

	idle, err := runner.PublishCatalogIfManifestChanged()
	if err != nil {
		t.Fatalf("idle PublishCatalogIfManifestChanged: %v", err)
	}
	if idle {
		t.Fatal("unchanged manifest published a new catalog")
	}
}

func TestColdBuilderPreflightUpgradesLegacyCatalogBeforeMutation(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize))
	legacy := publishUncheckedSignedSnapshotCatalogForTest(t, dir, privateKey)
	if legacy.ManifestPath != ManifestFile {
		t.Fatalf("legacy manifest path = %q, want %q", legacy.ManifestPath, ManifestFile)
	}
	runner := NewRunner(nil, Config{
		Dir:               dir,
		Enabled:           true,
		CatalogSigningKey: privateKey,
		CatalogChain:      &identity,
	})
	if err := runner.PreflightCatalog(); err != nil {
		t.Fatalf("PreflightCatalog: %v", err)
	}
	upgraded, err := LoadSnapshotCatalog(dir)
	if err != nil {
		t.Fatalf("LoadSnapshotCatalog: %v", err)
	}
	if upgraded.ManifestPath == ManifestFile {
		t.Fatalf("catalog remained mutable: %+v", upgraded)
	}
	if _, err := os.Stat(filepath.Join(dir, upgraded.ManifestPath)); err != nil {
		t.Fatalf("immutable manifest: %v", err)
	}
	if _, _, err := VerifySignedSnapshotCatalog(dir, identity, []ed25519.PublicKey{privateKey.Public().(ed25519.PublicKey)}); err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog(upgraded): %v", err)
	}
}

func TestColdBuilderOnePassCapsCutoffAtVerifiedFinishStage(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x72)

	for n := uint64(1); n <= 5; n++ {
		writeColdBuilderChange(t, db, owner, n, n, string([]byte{'a' + byte(n)}))
		writeColdBuilderCanonicalBlock(t, db, n)
	}
	finishHash := writeColdBuilderCanonicalBlock(t, db, 3)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 3, finishHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 6}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("one pass: %v", err)
	}
	if !result.Built || result.CutoffBlock != 3 || result.FromTxNum != 1 || result.ToTxNum != 3 {
		t.Fatalf("result = %+v, want build through finish stage block/tx 3", result)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || !ok || got != 3 {
		t.Fatalf("StageSnapshotBuild = %d ok=%v err=%v, want 3", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != finishHash {
		t.Fatalf("StageSnapshotBuild row = %+v ok=%v err=%v, want finish hash %x", row, ok, err, finishHash)
	}
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.VisibleTxEnd != 3 {
		t.Fatalf("manifest visible end = %d, want 3", manifest.VisibleTxEnd)
	}
}

func TestColdBuilderOnePassVerifiesFinishStageThroughChainSourceHash(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x74)

	var finishHash common.Hash
	for n := uint64(1); n <= 3; n++ {
		writeColdBuilderChange(t, db, owner, n, n, string([]byte{'a' + byte(n)}))
		hash := writeColdBuilderCanonicalBlock(t, db, n)
		if n == 3 {
			finishHash = hash
		}
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 3, finishHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	if err := rawdb.DeleteFrozenBlockRange(db, 3, 3); err != nil {
		t.Fatalf("delete hot block row: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{
		db:              db,
		solidified:      4,
		canonicalHashes: map[uint64]common.Hash{3: finishHash},
	}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("one pass: %v", err)
	}
	if !result.Built || result.CutoffBlock != 3 || result.ToTxNum != 3 {
		t.Fatalf("result = %+v, want build through chain-source finish stage", result)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != finishHash {
		t.Fatalf("StageSnapshotBuild row = %+v ok=%v err=%v, want chain-source hash %x", row, ok, err, finishHash)
	}
}

func TestColdBuilderOnePassRejectsFinishStageHashMismatch(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x73)

	for n := uint64(1); n <= 3; n++ {
		writeColdBuilderChange(t, db, owner, n, n, string([]byte{'a' + byte(n)}))
		writeColdBuilderCanonicalBlock(t, db, n)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 3, common.Hash{0xee}); err != nil {
		t.Fatalf("write mismatched finish stage: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 4}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	result, err := runner.OnePass()
	if err == nil || !strings.Contains(err.Error(), "finish stage 3 hash") {
		t.Fatalf("one pass result=%+v err=%v, want finish stage hash mismatch", result, err)
	}
	if _, err := LoadManifest(dir); err == nil || !os.IsNotExist(err) {
		t.Fatalf("manifest after rejected pass err=%v, want not exist", err)
	}
}

func TestColdBuilderOnePassPropagatesFinishStageHashLookupError(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x79)

	var finishHash common.Hash
	for n := uint64(1); n <= 3; n++ {
		writeColdBuilderChange(t, db, owner, n, n, string([]byte{'a' + byte(n)}))
		hash := writeColdBuilderCanonicalBlock(t, db, n)
		if n == 3 {
			finishHash = hash
		}
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 3, finishHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{
		db:              db,
		solidified:      4,
		canonicalHashes: map[uint64]common.Hash{3: finishHash},
		canonicalErrs:   map[uint64]error{3: errors.New("canonical hash corrupt")},
	}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	result, err := runner.OnePass()
	if err == nil || !strings.Contains(err.Error(), "finish stage 3 canonical hash lookup") || !strings.Contains(err.Error(), "canonical hash corrupt") {
		t.Fatalf("one pass result=%+v err=%v, want finish stage hash lookup error", result, err)
	}
	if _, err := LoadManifest(dir); err == nil || !os.IsNotExist(err) {
		t.Fatalf("manifest after rejected pass err=%v, want not exist", err)
	}
}

func TestColdBuilderOnePassRejectsMissingSnapshotBuildBoundaryHashBeforePublish(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x78)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	result, err := runner.OnePass()
	if err == nil || !strings.Contains(err.Error(), "missing canonical hash for SnapshotBuild stage block 1") {
		t.Fatalf("one pass result=%+v err=%v, want missing SnapshotBuild boundary hash", result, err)
	}
	if _, err := LoadManifest(dir); err == nil || !os.IsNotExist(err) {
		t.Fatalf("manifest after rejected pass err=%v, want not exist", err)
	}
}

func TestColdBuilderSecondPassNoOpWhenManifestCoversCutoff(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x72)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderCanonicalBlock(t, db, 1)
	writeColdBuilderChange(t, db, owner, 2, 2, "b")
	writeColdBuilderCanonicalBlock(t, db, 2)

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 3}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !first.Built || first.ToTxNum != 2 {
		t.Fatalf("first result = %+v", first)
	}
	second, err := runner.OnePass()
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if second.Built {
		t.Fatalf("second result built unexpectedly: %+v", second)
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.VisibleTxEnd != 2 || len(manifest.Segments) != 3 {
		t.Fatalf("manifest after no-op = %+v", manifest)
	}
}

func TestColdBuilderSecondPassContinuesFromManifestVisibleEnd(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x73)

	for block := uint64(1); block <= 5; block++ {
		writeColdBuilderChange(t, db, owner, block, block, string(rune('a'+block-1)))
		writeColdBuilderCanonicalBlock(t, db, block)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 6}, Config{
		Dir:             dir,
		Enabled:         true,
		Interval:        time.Hour,
		HistoryWindow:   1,
		BatchBlocks:     2,
		CompactMaxSteps: 2,
	})
	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !first.Built || first.FromTxNum != 1 || first.ToTxNum != 2 || first.CutoffBlock != 2 {
		t.Fatalf("first result = %+v", first)
	}
	second, err := runner.OnePass()
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if !second.Built || second.FromTxNum != 3 || second.ToTxNum != 4 || second.CutoffBlock != 4 {
		t.Fatalf("second result = %+v", second)
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.VisibleTxStart != 1 || manifest.VisibleTxEnd != 4 || len(manifest.Segments) != 3 {
		t.Fatalf("manifest after continuation = %+v", manifest)
	}
	for _, ref := range manifest.Segments {
		if ref.AggregationSteps != 2 {
			t.Fatalf("continued history aggregation steps = %d, want 2: %+v", ref.AggregationSteps, ref)
		}
	}
}

func TestColdBuilderCompactsSmallHistorySegments(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x76)
	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderCanonicalBlock(t, db, 1)
	writeColdBuilderChange(t, db, owner, 2, 2, "b")
	writeColdBuilderCanonicalBlock(t, db, 2)
	chain := &coldBuilderChain{db: db, solidified: 2}
	runner := NewRunner(chain, Config{
		Dir:             dir,
		Enabled:         true,
		Interval:        time.Hour,
		HistoryWindow:   1,
		BatchBlocks:     1,
		CompactMaxSteps: 256,
	})

	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !first.Built || first.Compaction.Merged {
		t.Fatalf("first result = %+v", first)
	}
	for _, ref := range first.Segments {
		if ref.AggregationSteps != 1 {
			t.Fatalf("base history aggregation steps = %d, want 1: %+v", ref.AggregationSteps, ref)
		}
	}
	chain.solidified = 3
	second, err := runner.OnePass()
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if !second.Built || !second.Compaction.Merged || second.Compaction.FromTxNum != 1 || second.Compaction.ToTxNum != 2 {
		t.Fatalf("second result = %+v", second)
	}
	if second.Compaction.Dataset != SegmentDatasetStateDomainChange || len(second.Compaction.Segments) != 3 {
		t.Fatalf("second compaction = %+v", second.Compaction)
	}
	if second.Compaction.AggregationSteps != 2 {
		t.Fatalf("second compaction aggregation steps = %d, want 2", second.Compaction.AggregationSteps)
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	var historyRefs, accessorRefs, indexRefs int
	for _, ref := range manifest.Segments {
		if ref.Dataset != SegmentDatasetStateDomainChange {
			continue
		}
		switch ref.Kind {
		case SegmentHistory:
			historyRefs++
			if ref.FromTxNum != 1 || ref.ToTxNum != 2 {
				t.Fatalf("history ref = %+v, want [1,2]", ref)
			}
		case SegmentAccessor:
			accessorRefs++
			if ref.FromTxNum != 1 || ref.ToTxNum != 2 {
				t.Fatalf("accessor ref = %+v, want [1,2]", ref)
			}
		case SegmentInverted:
			indexRefs++
			if ref.FromTxNum != 1 || ref.ToTxNum != 2 {
				t.Fatalf("index ref = %+v, want [1,2]", ref)
			}
		}
	}
	if historyRefs != 1 || accessorRefs != 1 || indexRefs != 1 {
		t.Fatalf("state-domain-change refs history=%d accessor=%d index=%d manifest=%+v", historyRefs, accessorRefs, indexRefs, manifest.Segments)
	}
	got := runner.Snapshot()
	if got.SegmentsBuilt != 2 || got.SegmentsCompacted != 2 {
		t.Fatalf("runner stats = %+v", got)
	}
}

func TestColdBuilderCatchupWritesFullFrozenSpanOnce(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x79)
	for block := uint64(1); block <= 5; block++ {
		writeColdBuilderChange(t, db, owner, block, block, fmt.Sprintf("key-%d", block))
		writeColdBuilderCanonicalBlock(t, db, block)
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 6}, Config{
		Dir:             dir,
		Enabled:         true,
		Interval:        time.Hour,
		HistoryWindow:   1,
		BatchBlocks:     1,
		CompactMaxSteps: 4,
	})

	for pass := 1; pass <= 3; pass++ {
		result, err := runner.OnePass()
		if err != nil {
			t.Fatalf("catch-up pass %d: %v", pass, err)
		}
		if !result.Built || !result.NeedsCatchup() || result.Compaction.Merged {
			t.Fatalf("catch-up pass %d result = %+v, want unmerged leaf", pass, result)
		}
	}
	fourth, err := runner.OnePass()
	if err != nil {
		t.Fatalf("fourth catch-up pass: %v", err)
	}
	if !fourth.NeedsCatchup() || !fourth.Compaction.Merged || fourth.Compaction.MergePasses != 1 ||
		fourth.Compaction.AggregationSteps != 4 || fourth.Compaction.SegmentsMerged != 4 {
		t.Fatalf("fourth result = %+v, want one direct frozen-span merge", fourth)
	}
	fifth, err := runner.OnePass()
	if err != nil {
		t.Fatalf("final catch-up pass: %v", err)
	}
	if !fifth.Built || fifth.NeedsCatchup() || fifth.Compaction.Merged {
		t.Fatalf("final result = %+v, want trailing leaf without rewrite", fifth)
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	var historySteps []uint64
	for _, ref := range manifest.Segments {
		if ref.Dataset == SegmentDatasetStateDomainChange && ref.Kind == SegmentHistory {
			historySteps = append(historySteps, ref.AggregationSteps)
		}
	}
	sort.Slice(historySteps, func(i, j int) bool { return historySteps[i] > historySteps[j] })
	if !reflect.DeepEqual(historySteps, []uint64{4, 1}) {
		t.Fatalf("history shape = %v, want frozen four-step file plus one leaf", historySteps)
	}
	if got := runner.Snapshot(); got.SegmentsBuilt != 5 || got.SegmentsCompacted != 4 ||
		got.CompactionMerges != 1 || got.CompactionCatchupDefers != 3 || got.LastCompactionMerges != 0 {
		t.Fatalf("runner stats = %+v", got)
	}
}

func TestColdBuilderDrainsAllReadyCompactionsWhenCaughtUp(t *testing.T) {
	dir := t.TempDir()
	var refs []SegmentRef
	for txNum := uint64(1); txNum <= 8; txNum++ {
		refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, txNum, txNum,
			binaryStateDomainChange(txNum, txNum, 1, fmt.Sprintf("key-%d", txNum)))...)
	}
	if err := PublishManifest(dir, NewManifest(1, 8, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	runner := NewRunner(&coldBuilderChain{db: rawdb.NewMemoryDatabase()}, Config{
		Dir:             dir,
		Enabled:         true,
		Interval:        time.Hour,
		BatchBlocks:     1,
		CompactMaxSteps: 4,
	})

	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("drain compactions: %v", err)
	}
	if result.Built || !result.Compaction.Merged || result.Compaction.MergePasses != 2 ||
		result.Compaction.SegmentsMerged != 8 || result.Compaction.AggregationSteps != 8 {
		t.Fatalf("drain result = %+v, want two direct four-step merges", result)
	}
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load drained manifest: %v", err)
	}
	var historyRefs int
	for _, ref := range manifest.Segments {
		if ref.Dataset == SegmentDatasetStateDomainChange && ref.Kind == SegmentHistory {
			historyRefs++
			if ref.AggregationSteps != 4 {
				t.Fatalf("drained history ref = %+v, want four steps", ref)
			}
		}
	}
	if historyRefs != 2 {
		t.Fatalf("drained history refs = %d, want 2: %+v", historyRefs, manifest.Segments)
	}
	if got := runner.Snapshot(); got.CompactionMerges != 2 || got.CompactionCatchupDefers != 0 ||
		got.LastCompactionMerges != 2 || got.SegmentsCompacted != 8 {
		t.Fatalf("runner drain stats = %+v", got)
	}
}

func TestColdBuilderCursorIgnoresNonHistoryManifestVisibility(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x75)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderCanonicalBlock(t, db, 1)
	writeColdBuilderChange(t, db, owner, 2, 2, "b")
	writeColdBuilderCanonicalBlock(t, db, 2)
	if err := PublishManifest(dir, NewManifest(1, 2, nil)); err != nil {
		t.Fatalf("publish non-history manifest: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 3}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("one pass: %v", err)
	}
	if !result.Built || result.FromTxNum != 1 || result.ToTxNum != 2 {
		t.Fatalf("result = %+v, want build full state-domain-change history from tx 1..2", result)
	}
}

func TestColdBuilderRejectsHistoryProgressAheadOfCoverage(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x77)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	manifest := NewManifest(1, 1, nil)
	manifest.Progress = &Progress{HistoryBuildTxNum: 1}
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	if _, err := runner.OnePass(); err == nil {
		t.Fatal("history progress ahead of coverage accepted")
	}
}

func TestColdBuilderNoOpWhenCutoffStateTxRangeMissing(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x74)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 3}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("one pass: %v", err)
	}
	if result.Built || result.CutoffBlock != 2 {
		t.Fatalf("result = %+v", result)
	}
	if _, err := LoadManifest(dir); err == nil {
		t.Fatal("manifest published for missing cutoff StateTxRange")
	}
}

func TestRunnerLatestPassIntervalGate(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x88)

	// Seed hot DB rows so BuildLatest produces segments (block 50, txNum 50).
	seedLatestRows(t, db, owner, 50, 50)

	chain := &coldBuilderChain{db: db, solidified: 50}
	runner := NewRunner(chain, Config{
		Dir:               dir,
		Enabled:           true,
		Interval:          time.Hour,
		HistoryWindow:     1,
		LatestBuildBlocks: 10,
	})
	// Do NOT call Start() — we drive latestPass directly to control seeding.
	// lastLatestBuildBlock starts at zero, so the first call must build.

	// Call 1: prevBlock==0 → must build latest segments.
	built1, err := runner.latestPass()
	if err != nil {
		t.Fatalf("latestPass call 1: %v", err)
	}
	if !built1 {
		t.Fatal("latestPass call 1: expected built=true (prevBlock==0), got false")
	}
	// After building, lastLatestBuildBlock should be 50.
	if got := runner.lastLatestBuildBlock.Load(); got != 50 {
		t.Fatalf("lastLatestBuildBlock after call 1 = %d, want 50", got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || !row.HasBlockHash {
		t.Fatalf("StageSnapshotLatestBuild row after call 1 = %+v ok=%v err=%v, want hash-bound row", row, ok, err)
	}
	// Manifest must have latest refs.
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest after call 1: %v", err)
	}
	var latestRefs int
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentLatest {
			latestRefs++
		}
	}
	if latestRefs == 0 {
		t.Fatalf("manifest after call 1 has no SegmentLatest refs: %+v", manifest.Segments)
	}

	// Call 2: still block 50, prevBlock==50 → 50 < 50+10 → must be gated (false).
	built2, err := runner.latestPass()
	if err != nil {
		t.Fatalf("latestPass call 2: %v", err)
	}
	if built2 {
		t.Fatal("latestPass call 2: expected built=false (interval gate), got true")
	}

	// Advance to block 65: 65 >= 50+10 → must build.
	chain.solidified = 65
	// Seed tx range for block 65 so latestBuildWatermark returns txNum > 0.
	if err := rawdb.WriteStateTxRange(db, 65, common.Hash{65}, 65, 65); err != nil {
		t.Fatalf("seed tx range block 65: %v", err)
	}
	writeColdBuilderCanonicalBlock(t, db, 65)
	built3, err := runner.latestPass()
	if err != nil {
		t.Fatalf("latestPass call 3: %v", err)
	}
	if !built3 {
		t.Fatal("latestPass call 3: expected built=true (interval elapsed), got false")
	}
	if got := runner.lastLatestBuildBlock.Load(); got != 65 {
		t.Fatalf("lastLatestBuildBlock after call 3 = %d, want 65", got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || !row.HasBlockHash {
		t.Fatalf("StageSnapshotLatestBuild row after call 3 = %+v ok=%v err=%v, want hash-bound row", row, ok, err)
	}
}

func TestRunnerLatestPassCapsWatermarkAtVerifiedFinishStage(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x87)

	seedLatestRows(t, db, owner, 40, 40)
	finishHash := writeColdBuilderCanonicalBlock(t, db, 40)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 40, finishHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 50, common.Hash{50}, 50, 50); err != nil {
		t.Fatalf("seed tx range block 50: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 50}, Config{
		Dir:               dir,
		Enabled:           true,
		Interval:          time.Hour,
		HistoryWindow:     1,
		LatestBuildBlocks: 10,
	})
	built, err := runner.latestPass()
	if err != nil {
		t.Fatalf("latestPass: %v", err)
	}
	if !built {
		t.Fatal("latestPass expected built=true")
	}
	if got := runner.lastLatestBuildBlock.Load(); got != 40 {
		t.Fatalf("lastLatestBuildBlock = %d, want verified finish stage 40", got)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || got != 40 {
		t.Fatalf("StageSnapshotLatestBuild = %d ok=%v err=%v, want 40", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != finishHash {
		t.Fatalf("StageSnapshotLatestBuild row = %+v ok=%v err=%v, want hash %x", row, ok, err, finishHash)
	}
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.Progress == nil || manifest.Progress.LatestBuildTxNum != 40 {
		t.Fatalf("manifest progress = %+v, want LatestBuildTxNum 40", manifest.Progress)
	}
}

func TestRunnerLatestPassCoordinatesCommitmentBranchRotation(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x88)
	seedLatestRows(t, db, owner, 10, 10)
	frozenRoot, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok {
		t.Fatalf("read frozen root ok=%v err=%v", ok, err)
	}
	rotation := rawdb.CommitmentBranchRotation{
		Generation: 1, SnapshotTxNum: 10, Root: frozenRoot, BlockNum: 10, BlockHash: common.Hash{10},
	}
	if err := rawdb.WriteCommitmentBranchRotation(db, rotation); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentBranch(db, nil, []byte("frozen-root-branch")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, common.Hash{0xcc}); err != nil {
		t.Fatal(err)
	}
	chain := &coldBuilderChain{
		db: db, solidified: 20, rotation: &rotation,
		rotationCompleteErr: ErrCommitmentBranchRotationNotSolidified,
	}
	runner := NewRunner(chain, Config{
		Dir: dir, Enabled: true, Interval: time.Hour, HistoryWindow: 1, LatestBuildBlocks: 10,
	})
	built, deferred, err := runner.latestPassWithStatus()
	if err != nil || !built || !deferred {
		t.Fatalf("deferred rotation pass built=%v deferred=%v err=%v", built, deferred, err)
	}
	if chain.rotationBegin != 1 || chain.rotationComplete != 1 {
		t.Fatalf("rotation calls begin=%d complete=%d", chain.rotationBegin, chain.rotationComplete)
	}
	if chain.rotationSnapshotRoot != frozenRoot {
		t.Fatalf("snapshot root = %x, want frozen %x", chain.rotationSnapshotRoot, frozenRoot)
	}
	if got := runner.lastLatestBuildBlock.Load(); got != 0 {
		t.Fatalf("deferred rotation advanced latest watermark to %d", got)
	}
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatestBuild); err != nil || ok {
		t.Fatalf("deferred rotation wrote latest stage ok=%v err=%v", ok, err)
	}

	chain.rotationCompleteErr = nil
	built, deferred, err = runner.latestPassWithStatus()
	if err != nil || !built || deferred {
		t.Fatalf("completed rotation pass built=%v deferred=%v err=%v", built, deferred, err)
	}
	if got := runner.lastLatestBuildBlock.Load(); got != rotation.BlockNum {
		t.Fatalf("completed rotation watermark = %d, want %d", got, rotation.BlockNum)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || row.BlockHash != rotation.BlockHash {
		t.Fatalf("completed rotation stage = %+v ok=%v err=%v", row, ok, err)
	}
}

// TestRunnerLatestBuildWatermarkSurvivesRestart proves that the latest-build
// watermark is persisted to StageSnapshotLatestBuild and that a fresh Runner
// over the same DB seeds from that persisted value rather than re-seeding to
// the current head.  The test would FAIL under the old behaviour because:
//   - Runner1 builds at block 50, persists stage=50.
//   - The fake chain is advanced to block 55 before Runner2 is constructed.
//   - Old Start(): seed = current head = 55 → lastLatestBuildBlock = 55.
//   - Gate at chain=55: 55 < 55+10=65 → gated. (Still gated, same symptom.)
//   - But the direct Load() assertion "lastLatestBuildBlock==50" would fail
//     because the old path stored 55, not 50.
//   - Additionally under the old path, advancing chain to 65 would build at
//     65 ≥ 55+10=65 (boundary), whereas under new path 65 ≥ 50+10=60 — both
//     build, but the stage value and discriminating Load() catch the difference.
func TestRunnerLatestBuildWatermarkSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x89)

	// Seed hot-DB rows for block 50 so BuildLatest produces segments.
	seedLatestRows(t, db, owner, 50, 50)

	// ── Runner1 at block 50 ──────────────────────────────────────────────────
	chain := &coldBuilderChain{db: db, solidified: 50}
	runner1 := NewRunner(chain, Config{
		Dir:               dir,
		Enabled:           true,
		Interval:          time.Hour,
		HistoryWindow:     1,
		LatestBuildBlocks: 10,
	})
	// Drive latestPass directly (skip Start/loop) so there's no goroutine.
	built1, err := runner1.latestPass()
	if err != nil {
		t.Fatalf("runner1 latestPass: %v", err)
	}
	if !built1 {
		t.Fatal("runner1 latestPass: expected built=true (prevBlock==0)")
	}

	// Manifest must have SegmentLatest refs.
	manifest1, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest after runner1 build: %v", err)
	}
	var latestRefs1 int
	for _, ref := range manifest1.Segments {
		if ref.Kind == SegmentLatest {
			latestRefs1++
		}
	}
	if latestRefs1 == 0 {
		t.Fatalf("manifest after runner1 has no SegmentLatest refs: %+v", manifest1.Segments)
	}

	// Persisted stage must be 50.
	stageBlock1, stageOK1, stageErr1 := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatestBuild)
	if stageErr1 != nil || !stageOK1 || stageBlock1 != 50 {
		t.Fatalf("StageSnapshotLatestBuild after runner1 = %d ok=%v err=%v, want 50", stageBlock1, stageOK1, stageErr1)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || !row.HasBlockHash {
		t.Fatalf("StageSnapshotLatestBuild row after runner1 = %+v ok=%v err=%v, want hash-bound row", row, ok, err)
	}

	// ── Simulate restart: advance chain to 55 BEFORE constructing Runner2 ───
	// This is the discriminator: old Start() would seed to 55 (current head);
	// new Start() seeds from the persisted stage → 50.
	chain.solidified = 55
	if err := rawdb.WriteStateTxRange(db, 55, common.Hash{55}, 55, 55); err != nil {
		t.Fatalf("seed tx range block 55: %v", err)
	}

	runner2 := NewRunner(chain, Config{
		Dir:               dir,
		Enabled:           true,
		Interval:          time.Hour,
		HistoryWindow:     1,
		LatestBuildBlocks: 10,
	})
	// Drive the Start() seed path inline (avoids spawning the loop goroutine).
	if err := runner2.seedLatestBuildBlock(); err != nil {
		t.Fatalf("runner2 seedLatestBuildBlock: %v", err)
	}

	// Assert seeded from persistence (50), NOT from current head (55).
	if got := runner2.lastLatestBuildBlock.Load(); got != 50 {
		t.Fatalf("runner2 lastLatestBuildBlock after restart seed = %d, want 50 (persisted), not 55 (current head)", got)
	}

	// ── Runner2 at chain=55: gate must fire (55 < 50+10=60) ─────────────────
	// Record manifest before the gated pass to verify history is untouched.
	manifestBeforeGated, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest before gated pass: %v", err)
	}

	result2a, err := runner2.OnePass()
	if err != nil {
		t.Fatalf("runner2 OnePass at block 55: %v", err)
	}
	if result2a.LatestBuilt {
		t.Fatal("runner2 OnePass at block 55: expected LatestBuilt=false (gate: 55 < 60)")
	}

	// History coverage must be unchanged by the gated pass.
	manifestAfterGated, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest after gated pass: %v", err)
	}
	if manifestAfterGated.VisibleTxEnd != manifestBeforeGated.VisibleTxEnd {
		t.Fatalf("history VisibleTxEnd changed during gated pass: before=%d after=%d",
			manifestBeforeGated.VisibleTxEnd, manifestAfterGated.VisibleTxEnd)
	}

	// ── Advance to 65: gate must open (65 >= 50+10=60) ──────────────────────
	chain.solidified = 65
	if err := rawdb.WriteStateTxRange(db, 65, common.Hash{65}, 65, 65); err != nil {
		t.Fatalf("seed tx range block 65: %v", err)
	}
	writeColdBuilderCanonicalBlock(t, db, 65)

	result2b, err := runner2.OnePass()
	if err != nil {
		t.Fatalf("runner2 OnePass at block 65: %v", err)
	}
	if !result2b.LatestBuilt {
		t.Fatal("runner2 OnePass at block 65: expected LatestBuilt=true (65 >= 60)")
	}
	if got := runner2.lastLatestBuildBlock.Load(); got != 65 {
		t.Fatalf("runner2 lastLatestBuildBlock after build at 65 = %d, want 65", got)
	}

	// Persisted stage must now be 65.
	stageBlock2, stageOK2, stageErr2 := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatestBuild)
	if stageErr2 != nil || !stageOK2 || stageBlock2 != 65 {
		t.Fatalf("StageSnapshotLatestBuild after runner2 build = %d ok=%v err=%v, want 65", stageBlock2, stageOK2, stageErr2)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || !row.HasBlockHash {
		t.Fatalf("StageSnapshotLatestBuild row after runner2 build = %+v ok=%v err=%v, want hash-bound row", row, ok, err)
	}
}

func TestRunnerLatestBuildWatermarkIgnoresUnboundRestartStage(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x8a)
	seedLatestRows(t, db, owner, 55, 55)
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotLatestBuild, 50); err != nil {
		t.Fatalf("WriteStageProgress SnapshotLatestBuild: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 55}, Config{
		Dir:               dir,
		Enabled:           true,
		Interval:          time.Hour,
		HistoryWindow:     1,
		LatestBuildBlocks: 10,
	})
	if err := runner.seedLatestBuildBlock(); err != nil {
		t.Fatalf("seedLatestBuildBlock: %v", err)
	}
	if got := runner.lastLatestBuildBlock.Load(); got != 0 {
		t.Fatalf("lastLatestBuildBlock after unbound seed = %d, want 0 for immediate repair build", got)
	}
	built, err := runner.latestPass()
	if err != nil {
		t.Fatalf("latestPass: %v", err)
	}
	if !built {
		t.Fatal("latestPass = false, want repair build after unbound restart stage")
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild); err != nil || !ok || row.BlockNum != 55 || !row.HasBlockHash {
		t.Fatalf("SnapshotLatestBuild row after repair = %+v ok=%v err=%v, want block 55 hash-bound", row, ok, err)
	}
}

func TestRunnerLatestBuildWatermarkIgnoresHashMismatchRestartStage(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x8b)
	seedLatestRows(t, db, owner, 50, 50)
	seedLatestRows(t, db, owner, 55, 55)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotLatestBuild, 50, common.Hash{0xee}); err != nil {
		t.Fatalf("WriteStageProgressWithHash SnapshotLatestBuild: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 55}, Config{
		Dir:               dir,
		Enabled:           true,
		Interval:          time.Hour,
		HistoryWindow:     1,
		LatestBuildBlocks: 10,
	})
	if err := runner.seedLatestBuildBlock(); err != nil {
		t.Fatalf("seedLatestBuildBlock: %v", err)
	}
	if got := runner.lastLatestBuildBlock.Load(); got != 0 {
		t.Fatalf("lastLatestBuildBlock after mismatched seed = %d, want 0 for immediate repair build", got)
	}
	built, err := runner.latestPass()
	if err != nil {
		t.Fatalf("latestPass: %v", err)
	}
	if !built {
		t.Fatal("latestPass = false, want repair build after mismatched restart stage")
	}
	row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild)
	if err != nil || !ok || row.BlockNum != 55 || !row.HasBlockHash {
		t.Fatalf("SnapshotLatestBuild row after mismatch repair = %+v ok=%v err=%v, want block 55 hash-bound", row, ok, err)
	}
	if canonical := rawdb.ReadBlockHashByNumber(db, 55); canonical == (common.Hash{}) || row.BlockHash != canonical {
		t.Fatalf("SnapshotLatestBuild hash = %x, want canonical block 55 hash %x", row.BlockHash, canonical)
	}
}

func TestColdBuilderBuildsEventLogsWithHistorySegment(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x42)
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	addr := eventLogTestAddress(0x66)
	topic := common.Hash{0xaa}
	block, infos := coldBuilderEventLogBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x01}},
	})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	etlTemp := filepath.Join(dir, "etl-scratch")
	verificationCache := NewChainFreezerVerificationCache(dir)

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:                        dir,
		Enabled:                    true,
		HistoryWindow:              1,
		BuildEventLogs:             true,
		ColdChainVerificationCache: verificationCache,
		ETL: RestoreETLOptions{
			TempDir:     etlTemp,
			BufferLimit: 1,
		},
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if _, err := os.Stat(etlTemp); !os.IsNotExist(err) {
		t.Fatalf("ordered cold/event-log build unexpectedly used ETL scratch: %v", err)
	}
	if !result.Built || !result.EventLogBuilt || result.FromBlock != 1 || result.ToBlock != 1 {
		t.Fatalf("result = %+v, want history+event-log build over block 1", result)
	}
	haveEventLog := false
	haveEventLogIndex := false
	for _, ref := range result.Segments {
		switch ref.Kind {
		case SegmentEventLog:
			haveEventLog = true
		case SegmentEventLogIndex:
			haveEventLogIndex = true
		}
	}
	if !haveEventLog || !haveEventLogIndex {
		t.Fatalf("segments = %+v, want event-log and event-log-index segments with history", result.Segments)
	}
	stats := verificationCache.Stats()
	if stats.EventTrustedRecorded != 1 || stats.EventFullVerified != 0 || stats.EventEntries != 1 {
		t.Fatalf("trusted cold-builder event stats = %+v", stats)
	}
	eventHead, err := verifiedIndexedEventLogHeadWithCache(dir, result.Manifest, verificationCache)
	if err != nil || eventHead != 2 {
		t.Fatalf("trusted event head = %d/%v, want 2/nil", eventHead, err)
	}
	stats = verificationCache.Stats()
	if stats.EventMemoryHits != 1 || stats.EventFullVerified != 0 {
		t.Fatalf("trusted event reuse stats = %+v", stats)
	}
	restartedCache := NewChainFreezerVerificationCache(dir)
	eventHead, err = verifiedIndexedEventLogHeadWithCache(dir, result.Manifest, restartedCache)
	if err != nil || eventHead != 2 {
		t.Fatalf("restart trusted event head = %d/%v, want 2/nil", eventHead, err)
	}
	stats = restartedCache.Stats()
	if stats.EventPersistentHits != 1 || stats.EventFullVerified != 0 || stats.EventEntries != 1 {
		t.Fatalf("restart trusted event stats = %+v", stats)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || got != 1 {
		t.Fatalf("StageSnapshotEventLogBuild = %d ok=%v err=%v, want 1", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("StageSnapshotEventLogBuild row = %+v ok=%v err=%v, want hash %x", row, ok, err, block.Hash())
	}
	if err := rawdb.DeleteStageProgress(db, rawdb.StageSnapshotEventLogBuild); err != nil {
		t.Fatalf("DeleteStageProgress SnapshotEventLogBuild: %v", err)
	}
	repaired, err := runner.OnePass()
	if err != nil {
		t.Fatalf("repair manifest-first event stage: %v", err)
	}
	if repaired.DerivedSidecarCatchup || repaired.EventLogBuilt {
		t.Fatalf("event stage repair rebuilt immutable sidecars: %+v", repaired)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotEventLogBuild); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("repaired StageSnapshotEventLogBuild row = %+v ok=%v err=%v, want hash %x", row, ok, err, block.Hash())
	}
	mgr, err := OpenManagerWithChainVerificationCache(dir, verificationCache)
	if err != nil {
		t.Fatalf("OpenManagerWithChainVerificationCache: %v", err)
	}
	covered, err := mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered = %v/%v, want true/nil", covered, err)
	}
	stats = verificationCache.Stats()
	if stats.EventMemoryHits != 2 || stats.EventFullVerified != 0 {
		t.Fatalf("manager did not reuse trusted event proof: %+v", stats)
	}
	covered, err = mgr.EventLogIndexedRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered = %v/%v, want true/nil", covered, err)
	}
	var rows []EventLog
	if err := mgr.IterateEventLogs(1, 1, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addr)},
		Topics:    [][]common.Hash{{topic}},
	}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x01}) {
		t.Fatalf("event rows = %+v, want one cold event log", rows)
	}
}

func TestColdBuilderCatchupKeepsEventLogIndexesSegmentLocal(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x4a)
	address := eventLogTestAddress(0x6a)
	topic := common.Hash{0xca}
	for blockNum := uint64(1); blockNum <= 2; blockNum++ {
		writeColdBuilderChange(t, db, owner, blockNum, blockNum, "previous")
		block, infos := coldBuilderEventLogBlock(t, blockNum, []*corepb.TransactionInfo_Log{{
			Address: address,
			Topics:  [][]byte{topic[:]},
			Data:    []byte{byte(blockNum)},
		}})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			t.Fatalf("WriteTransactionInfosByBlock %d: %v", blockNum, err)
		}
	}

	cfg := Config{
		Dir:             dir,
		Enabled:         true,
		Interval:        time.Hour,
		HistoryWindow:   1,
		BatchBlocks:     1,
		BuildEventLogs:  true,
		EventLogVersion: EventLogSegmentV4Version,
	}
	for pass := uint64(1); pass <= 2; pass++ {
		// Recreate the lifecycle on every pass to exercise manifest/stage resume
		// exactly as a node restart during a genesis catch-up would.
		runner := NewRunner(&coldBuilderChain{db: db, solidified: 3}, cfg)
		result, err := runner.OnePass()
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if !result.Built || !result.EventLogBuilt || result.FromBlock != pass || result.ToBlock != pass {
			t.Fatalf("pass %d result = %+v, want one event-log block", pass, result)
		}
	}

	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	indexes := eventLogIndexRefs(manifest)
	if len(indexes) != 2 || indexes[0].FromTxNum != 1 || indexes[0].ToTxNum != 1 || indexes[1].FromTxNum != 2 || indexes[1].ToTxNum != 2 {
		t.Fatalf("catch-up indexes = %+v, want [1,1] and [2,2]", indexes)
	}
	logs := eventLogRefs(manifest)
	if len(logs) != 2 {
		t.Fatalf("catch-up event logs = %+v, want two V3 segments", logs)
	}
	for _, ref := range logs {
		segment, err := OpenEventLogSegment(dir, ref)
		if err != nil {
			t.Fatalf("OpenEventLogSegment(%s): %v", ref.Path, err)
		}
		version := segment.header.version
		_ = segment.Close()
		if version != EventLogSegmentV4Version {
			t.Fatalf("event-log %s version = %d, want V3", ref.Path, version)
		}
	}
	if _, err := VerifyManifestFiles(dir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	var rows []EventLog
	if err := mgr.IterateEventLogs(1, 2, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(address)},
		Topics:    [][]common.Hash{{topic}},
	}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 2 || rows[0].BlockNum != 1 || rows[1].BlockNum != 2 {
		t.Fatalf("catch-up rows = %+v, want blocks 1 and 2", rows)
	}
}

func TestColdBuilderWritesEventLogStageFromAncientBlock(t *testing.T) {
	dir := t.TempDir()
	hot := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x43)
	writeColdBuilderChange(t, hot, owner, 1, 1, "next")
	addr := eventLogTestAddress(0x67)
	topic := common.Hash{0xbb}
	block, infos := coldBuilderEventLogBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x02}},
	})
	if err := rawdb.WriteBlock(hot, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(hot, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	blockRaw, err := block.Marshal()
	if err != nil {
		t.Fatalf("Marshal block: %v", err)
	}
	if err := rawdb.DeleteFrozenBlockRange(hot, 1, 1); err != nil {
		t.Fatalf("DeleteFrozenBlockRange: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(hot, 1, infos); err != nil {
		t.Fatalf("restore hot tx infos for event-log build: %v", err)
	}
	db := rawdb.NewChainDB(hot, sectionBloomPruneAncientBlock(1, blockRaw))

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:            dir,
		Enabled:        true,
		HistoryWindow:  1,
		BuildEventLogs: true,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if !result.Built || !result.EventLogBuilt || result.ToBlock != 1 {
		t.Fatalf("result = %+v, want event-log build through ancient block 1", result)
	}
	row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotEventLogBuild)
	if err != nil || !ok || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("StageSnapshotEventLogBuild row = %+v ok=%v err=%v, want ancient block hash %x", row, ok, err, block.Hash())
	}
}

func TestColdBuilderBuildsBalanceTracesWithHistorySegment(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x45)
	traceOwner := balanceTraceTestAddress(0x46)
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	block, _ := coldBuilderEventLogBlock(t, 1, nil)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	trace := coldBuilderBlockBalanceTrace(block, 10_001)
	trace.TransactionBalanceTrace = []*contractpb.TransactionBalanceTrace{{
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			OperationIdentifier: 0,
			Address:             traceOwner.Bytes(),
			Amount:              777,
		}},
	}}
	if err := rawdb.WriteBlockBalanceTrace(db, 1, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, traceOwner.Bytes(), 1, 777); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}
	etlTemp := filepath.Join(dir, "etl-scratch")

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:                dir,
		Enabled:            true,
		HistoryWindow:      1,
		BuildBalanceTraces: true,
		ETL: RestoreETLOptions{
			TempDir:     etlTemp,
			BufferLimit: 1,
		},
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("ETL temp parent stat: %v", err)
	}
	if !result.Built || !result.BalanceTraceBuilt || result.FromBlock != 1 || result.ToBlock != 1 {
		t.Fatalf("result = %+v, want history+balance-trace build over block 1", result)
	}
	haveBalanceTrace := false
	for _, ref := range result.Segments {
		if ref.Kind == SegmentBalanceTrace {
			haveBalanceTrace = true
		}
	}
	if !haveBalanceTrace {
		t.Fatalf("segments = %+v, want balance-trace segment with history", result.Segments)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	trace, ok, err := mgr.BlockBalanceTrace(1)
	if err != nil || !ok || trace.GetTimestamp() != 10_001 {
		t.Fatalf("BlockBalanceTrace = %+v/%v/%v, want timestamp 10001", trace, ok, err)
	}
	traceBlock, balance, ok, err := mgr.AccountTraceAtOrBefore(traceOwner.Bytes(), 1)
	if err != nil || !ok || traceBlock != 1 || balance != 777 {
		t.Fatalf("AccountTraceAtOrBefore = block %d balance %d ok %v err %v, want 1/777/true/nil", traceBlock, balance, ok, err)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	pruned, err := PruneHotBalanceTracesWithProgress(db, dir, manifest)
	if err != nil {
		t.Fatalf("PruneHotBalanceTracesWithProgress: %v", err)
	}
	if pruned == nil || pruned.BlockTracesDeleted != 1 || pruned.AccountTracesDeleted != 1 || pruned.ColdTraceSegments != 1 {
		t.Fatalf("balance trace prune = %+v, want one block/account trace deleted", pruned)
	}
	if got := rawdb.ReadBlockBalanceTrace(db, 1); got != nil {
		t.Fatalf("hot BlockBalanceTrace survived = %+v, want nil", got)
	}
	if balance, ok := rawdb.ReadAccountTrace(db, traceOwner.Bytes(), 1); ok || balance != 0 {
		t.Fatalf("hot AccountTrace survived = %d/%v, want 0/false", balance, ok)
	}
	db.SetBalanceTraceReader(mgr)
	if got := rawdb.ReadBlockBalanceTrace(db, 1); got == nil || got.GetTimestamp() != 10_001 {
		t.Fatalf("cold ReadBlockBalanceTrace after prune = %+v, want timestamp 10001", got)
	}
	traceBlock, balance, ok, err = rawdb.ReadAccountTraceAtOrBefore(db, traceOwner.Bytes(), 1)
	if err != nil || !ok || traceBlock != 1 || balance != 777 {
		t.Fatalf("cold ReadAccountTraceAtOrBefore = block %d balance %d ok %v err %v, want 1/777/true/nil", traceBlock, balance, ok, err)
	}
}

func TestColdBuilderSkipsBalanceTraceBuildWithoutCompleteCoverage(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x47)
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	block, _ := coldBuilderEventLogBlock(t, 1, nil)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:                dir,
		Enabled:            true,
		HistoryWindow:      1,
		BuildBalanceTraces: true,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if !result.Built {
		t.Fatalf("result = %+v, want state-history build", result)
	}
	if result.BalanceTraceBuilt {
		t.Fatalf("result = %+v, want no balance-trace build without complete coverage", result)
	}
	for _, ref := range result.Segments {
		if ref.Kind == SegmentBalanceTrace {
			t.Fatalf("segments = %+v, want no balance-trace segment without complete coverage", result.Segments)
		}
	}
}

func TestColdBuilderBackfillsBalanceTraceAfterSourceCoverageArrives(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x4b)
	traceOwner := balanceTraceTestAddress(0x4c)
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	block, _ := coldBuilderEventLogBlock(t, 1, nil)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	cfg := Config{
		Dir:                dir,
		Enabled:            true,
		HistoryWindow:      1,
		BuildBalanceTraces: true,
	}
	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, cfg)
	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("history pass: %v", err)
	}
	if !first.Built || first.BalanceTraceBuilt {
		t.Fatalf("history pass = %+v, want history without incomplete balance trace", first)
	}

	trace := coldBuilderBlockBalanceTrace(block, 10_004)
	trace.TransactionBalanceTrace = []*contractpb.TransactionBalanceTrace{{
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			OperationIdentifier: 0,
			Address:             traceOwner.Bytes(),
			Amount:              888,
		}},
	}}
	if err := rawdb.WriteBlockBalanceTrace(db, 1, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, traceOwner.Bytes(), 1, 888); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}

	second, err := runner.OnePass()
	if err != nil {
		t.Fatalf("balance-trace catch-up: %v", err)
	}
	if second.Built || !second.DerivedSidecarCatchup || !second.BalanceTraceBuilt || second.DerivedSidecarsPending {
		t.Fatalf("balance-trace catch-up = %+v, want independent completed build", second)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	traceBlock, balance, ok, err := mgr.AccountTraceAtOrBefore(traceOwner.Bytes(), 1)
	if err != nil || !ok || traceBlock != 1 || balance != 888 {
		t.Fatalf("AccountTraceAtOrBefore = %d/%d/%v/%v, want 1/888/true/nil", traceBlock, balance, ok, err)
	}
}

func TestColdBuilderSkipsBalanceTraceBuildWithoutAccountTraceCoverage(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x49)
	traceOwner := balanceTraceTestAddress(0x4a)
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	block, _ := coldBuilderEventLogBlock(t, 1, nil)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	trace := coldBuilderBlockBalanceTrace(block, 10_003)
	trace.TransactionBalanceTrace = []*contractpb.TransactionBalanceTrace{{
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			OperationIdentifier: 0,
			Address:             traceOwner.Bytes(),
			Amount:              1,
		}},
	}}
	if err := rawdb.WriteBlockBalanceTrace(db, 1, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:                dir,
		Enabled:            true,
		HistoryWindow:      1,
		BuildBalanceTraces: true,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if !result.Built {
		t.Fatalf("result = %+v, want state-history build", result)
	}
	if result.BalanceTraceBuilt {
		t.Fatalf("result = %+v, want no balance-trace build without account trace coverage", result)
	}
	for _, ref := range result.Segments {
		if ref.Kind == SegmentBalanceTrace {
			t.Fatalf("segments = %+v, want no balance-trace segment without account trace coverage", result.Segments)
		}
	}
}

func TestColdBuilderRejectsMismatchedBalanceTraceCoverage(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x48)
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	block, _ := coldBuilderEventLogBlock(t, 1, nil)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	badTrace := coldBuilderBlockBalanceTrace(block, 10_002)
	badTrace.BlockIdentifier.Hash = bytes.Repeat([]byte{0xff}, common.HashLength)
	if err := rawdb.WriteBlockBalanceTrace(db, 1, badTrace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 2}, Config{
		Dir:                dir,
		Enabled:            true,
		HistoryWindow:      1,
		BuildBalanceTraces: true,
	})
	_, err := runner.OnePass()
	if err == nil || !strings.Contains(err.Error(), "balance trace coverage mismatch") {
		t.Fatalf("OnePass error = %v, want balance trace coverage mismatch", err)
	}
	if manifest, err := LoadProductionManifest(dir); err == nil {
		for _, ref := range manifest.Segments {
			if ref.Kind == SegmentBalanceTrace {
				t.Fatalf("manifest segments = %+v, want no balance-trace segment after mismatch", manifest.Segments)
			}
		}
	} else if !os.IsNotExist(err) {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
}

func TestColdBuilderBuildsSectionBloomsOnlyForCompleteSections(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x43)
	sectionEnd := uint64(rawdb.SectionBloomBlockPerSection) - 1
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	if err := rawdb.WriteStateTxRange(db, sectionEnd, common.Hash{0xee}, sectionEnd, sectionEnd); err != nil {
		t.Fatalf("WriteStateTxRange section end: %v", err)
	}
	writeColdBuilderCanonicalBlock(t, db, sectionEnd)
	row := sectionBloomTestEncodedBit(t, 5)
	if err := rawdb.WriteSectionBloom(db, 0, 42, row); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	etlTemp := filepath.Join(dir, "etl-scratch")

	runner := NewRunner(&coldBuilderChain{db: db, solidified: int64(sectionEnd + 1)}, Config{
		Dir:                dir,
		Enabled:            true,
		HistoryWindow:      1,
		BuildSectionBlooms: true,
		ETL: RestoreETLOptions{
			TempDir:     etlTemp,
			BufferLimit: 1,
		},
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("ETL temp parent stat: %v", err)
	}
	if !result.Built || !result.SectionBloomBuilt {
		t.Fatalf("result = %+v, want history+section-bloom build", result)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	cold, ok, err := mgr.SectionBloom(0, 42)
	if err != nil || !ok || !bytes.Equal(cold, row) {
		t.Fatalf("cold SectionBloom = %x/%v/%v, want row", cold, ok, err)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	pruned, err := PruneHotSectionBloomsWithProgress(db, dir, manifest)
	if err != nil {
		t.Fatalf("PruneHotSectionBloomsWithProgress: %v", err)
	}
	if pruned == nil || pruned.RowsDeleted != 1 || pruned.ColdBloomSegments != 1 {
		t.Fatalf("section bloom prune = %+v, want one deleted row", pruned)
	}
	if got := rawdb.ReadSectionBloom(db, 0, 42); got != nil {
		t.Fatalf("hot section bloom survived = %x, want nil", got)
	}
	db.SetSectionBloomReader(mgr)
	bitset, ok, err := rawdb.ReadSectionBloomBitSet(db, 0, 42)
	if err != nil || !ok || !rawdb.SectionBloomBitSetHas(bitset, 5) {
		t.Fatalf("cold ReadSectionBloomBitSet = %x/%v/%v, want bit 5", bitset, ok, err)
	}
}

func TestColdBuilderBackfillsDeferredSectionBloomAfterSync(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x4d)
	sectionEnd := uint64(rawdb.SectionBloomBlockPerSection) - 1
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	if err := rawdb.WriteStateTxRange(db, sectionEnd, common.Hash{0xef}, sectionEnd, sectionEnd); err != nil {
		t.Fatalf("WriteStateTxRange section end: %v", err)
	}
	writeColdBuilderCanonicalBlock(t, db, sectionEnd)
	row := sectionBloomTestEncodedBit(t, 7)
	if err := rawdb.WriteSectionBloom(db, 0, 51, row); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	chain := &coldBuilderChain{
		db: db, solidified: int64(sectionEnd + 1), syncRemaining: 1_000, syncRemainingOK: true,
	}
	cfg := Config{
		Dir:                              dir,
		Enabled:                          true,
		HistoryWindow:                    1,
		BatchBlocks:                      sectionEnd,
		BuildSectionBlooms:               true,
		DeferDerivedSidecarsWhileSyncing: true,
	}
	runner := NewRunner(chain, cfg)
	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("sync history pass: %v", err)
	}
	if !first.Built || first.SectionBloomBuilt || !first.DerivedSidecarsDeferred {
		t.Fatalf("sync history pass = %+v, want section bloom deferred", first)
	}
	chain.syncRemainingOK = false
	second, err := runner.OnePass()
	if err != nil {
		t.Fatalf("section-bloom catch-up: %v", err)
	}
	if second.Built || !second.DerivedSidecarCatchup || !second.SectionBloomBuilt || second.DerivedSidecarsPending {
		t.Fatalf("section-bloom catch-up = %+v, want independent completed build", second)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	cold, ok, err := mgr.SectionBloom(0, 51)
	if err != nil || !ok || !bytes.Equal(cold, row) {
		t.Fatalf("cold SectionBloom = %x/%v/%v, want row", cold, ok, err)
	}
}

func TestColdBuilderSkipsPartialSectionBloomSection(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	owner := coldBuilderOwner(0x44)
	cutoff := uint64(rawdb.SectionBloomBlockPerSection) - 2
	writeColdBuilderChange(t, db, owner, 1, 1, "next")
	if err := rawdb.WriteStateTxRange(db, cutoff, common.Hash{0xdd}, cutoff, cutoff); err != nil {
		t.Fatalf("WriteStateTxRange cutoff: %v", err)
	}
	writeColdBuilderCanonicalBlock(t, db, cutoff)
	if err := rawdb.WriteSectionBloom(db, 0, 42, sectionBloomTestEncodedBit(t, 5)); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: int64(cutoff + 1)}, Config{
		Dir:                dir,
		Enabled:            true,
		HistoryWindow:      1,
		BuildSectionBlooms: true,
	})
	result, err := runner.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result.SectionBloomBuilt {
		t.Fatalf("result = %+v, want no section-bloom build for partial section", result)
	}
	for _, ref := range result.Segments {
		if ref.Kind == SegmentSectionBloom {
			t.Fatalf("segments = %+v, want no section-bloom segment before full section boundary", result.Segments)
		}
	}
	if got := rawdb.ReadSectionBloom(db, 0, 42); got == nil {
		t.Fatal("hot section bloom was removed despite no full-section cold coverage")
	}
}

func TestSectionBloomFullSectionCoverageMatchesReference(t *testing.T) {
	blocksPerSection := uint64(rawdb.SectionBloomBlockPerSection)
	manifest := NewManifest(0, 1, nil)
	for i := uint64(0); i < 128; i++ {
		from := i * blocksPerSection
		if i%3 == 1 {
			from++
		}
		to := (i + 1 + i%7) * blocksPerSection
		if i%4 == 0 {
			to--
		}
		manifest.Segments = append(manifest.Segments, SegmentRef{
			Dataset:   SegmentDatasetSectionBloom,
			Kind:      SegmentSectionBloom,
			FromTxNum: from,
			ToTxNum:   to,
			Path:      fmt.Sprintf("log/coverage-%d.seg", i),
		})
	}
	manifest.Segments = append(manifest.Segments, SegmentRef{
		Dataset:   SegmentDatasetEventLog,
		Kind:      SegmentEventLog,
		FromTxNum: 0,
		ToTxNum:   256 * blocksPerSection,
		Path:      "log/not-a-bloom.seg",
	})

	const maxSection = uint64(255)
	covered, err := sectionBloomFullSectionCoverage(manifest, maxSection)
	if err != nil {
		t.Fatalf("sectionBloomFullSectionCoverage: %v", err)
	}
	if len(covered) != int(maxSection)+1 {
		t.Fatalf("coverage len = %d, want %d", len(covered), maxSection+1)
	}
	for section := uint64(0); section <= maxSection; section++ {
		want := sectionBloomManifestCoversFullSection(manifest, section)
		if covered[section] != want {
			t.Fatalf("section %d coverage = %v, want %v", section, covered[section], want)
		}
	}
}

func BenchmarkSectionBloomFullSectionCoverage(b *testing.B) {
	blocksPerSection := uint64(rawdb.SectionBloomBlockPerSection)
	manifest := NewManifest(0, 1, nil)
	for i := uint64(0); i < 2_000; i++ {
		manifest.Segments = append(manifest.Segments, SegmentRef{
			Dataset:   SegmentDatasetSectionBloom,
			Kind:      SegmentSectionBloom,
			FromTxNum: i * blocksPerSection,
			ToTxNum:   (i+4)*blocksPerSection - 1,
			Path:      fmt.Sprintf("log/benchmark-%d.seg", i),
		})
	}
	const maxSection = uint64(5_999)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		covered, err := sectionBloomFullSectionCoverage(manifest, maxSection)
		if err != nil || len(covered) != int(maxSection)+1 {
			b.Fatalf("coverage len=%d err=%v", len(covered), err)
		}
	}
}

type coldBuilderChain struct {
	db                   AggregatorDB
	solidified           int64
	syncRemaining        uint64
	syncRemainingOK      bool
	canonicalHashes      map[uint64]common.Hash
	canonicalErrs        map[uint64]error
	rotation             *rawdb.CommitmentBranchRotation
	rotationBegin        int
	rotationComplete     int
	rotationCompleteErr  error
	rotationSnapshotRoot common.Hash
	persistRotationBase  bool
}

type coldBuilderSeekRecordingDB struct {
	ethdb.KeyValueStore
	stateTxRangeStarts [][]byte
}

func (db *coldBuilderSeekRecordingDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.Equal(prefix, []byte("state-tx-range-v1-")) {
		db.stateTxRangeStarts = append(db.stateTxRangeStarts, append([]byte(nil), start...))
	}
	return db.KeyValueStore.NewIterator(prefix, start)
}

func (c *coldBuilderChain) DB() AggregatorDB { return c.db }

func (c *coldBuilderChain) LatestSolidifiedBlockNum() int64 { return c.solidified }

func (c *coldBuilderChain) SyncRemainingBlocks() (uint64, bool) {
	return c.syncRemaining, c.syncRemainingOK
}

func (c *coldBuilderChain) BeginCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error) {
	c.rotationBegin++
	if c.rotation == nil {
		return rawdb.CommitmentBranchRotation{}, false, nil
	}
	return *c.rotation, true, nil
}

func (c *coldBuilderChain) CompleteCommitmentBranchRotation(rotation rawdb.CommitmentBranchRotation, mgr *Manager) error {
	c.rotationComplete++
	root, ok, err := mgr.GetCommitmentRoot(rotation.SnapshotTxNum)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("test rotation snapshot root missing")
	}
	c.rotationSnapshotRoot = root
	if c.rotationCompleteErr != nil {
		return c.rotationCompleteErr
	}
	if c.persistRotationBase {
		writer, ok := c.db.(ethdb.KeyValueWriter)
		if !ok {
			return errors.New("test rotation database is not writable")
		}
		if err := rawdb.WriteCommitmentBranchBase(writer, rawdb.CommitmentBranchBase{
			Generation: rotation.Generation, SnapshotTxNum: rotation.SnapshotTxNum, Root: rotation.Root,
			BlockNum: rotation.BlockNum, BlockHash: rotation.BlockHash,
		}); err != nil {
			return err
		}
		if err := rawdb.DeleteCommitmentBranchRotation(writer); err != nil {
			return err
		}
		c.rotation = nil
	}
	return nil
}

func (c *coldBuilderChain) CanonicalBlockHash(blockNum uint64) (common.Hash, bool) {
	hash, ok, err := c.CanonicalBlockHashStrict(blockNum)
	if err != nil {
		return common.Hash{}, false
	}
	return hash, ok
}

func (c *coldBuilderChain) CanonicalBlockHashStrict(blockNum uint64) (common.Hash, bool, error) {
	if err := c.canonicalErrs[blockNum]; err != nil {
		return common.Hash{}, false, err
	}
	if c.canonicalHashes != nil {
		hash, ok := c.canonicalHashes[blockNum]
		return hash, ok, nil
	}
	return rawdb.ReadBlockHashByNumberStrict(c.db, blockNum)
}

func coldBuilderEventLogBlock(t *testing.T, number uint64, logs []*corepb.TransactionInfo_Log) (*coretypes.Block, []*corepb.TransactionInfo) {
	t.Helper()
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  int64(10_000 + number),
			Expiration: int64(20_000 + number),
			Data:       []byte{byte(number)},
		},
	}
	tx := coretypes.NewTransactionFromPB(txPB)
	info := &corepb.TransactionInfo{
		Id:             append([]byte(nil), tx.Hash().Bytes()...),
		BlockNumber:    int64(number),
		BlockTimeStamp: int64(30_000 + number),
		Log:            logs,
	}
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	return block, []*corepb.TransactionInfo{info}
}

func coldBuilderBlockBalanceTrace(block *coretypes.Block, timestamp int64) *contractpb.BlockBalanceTrace {
	if block == nil {
		return &contractpb.BlockBalanceTrace{Timestamp: timestamp}
	}
	return &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   append([]byte(nil), block.Hash().Bytes()...),
			Number: int64(block.Number()),
		},
		Timestamp: timestamp,
	}
}

func coldBuilderOwner(seed byte) common.Address {
	return common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{seed}, common.AccountIDLength)...))
}

func writeColdBuilderChange(t *testing.T, db ethdb.KeyValueWriter, owner common.Address, blockNum, txNum uint64, prev string) {
	t.Helper()
	blockHash := common.Hash{byte(blockNum)}
	if err := rawdb.WriteStateTxRange(db, blockNum, blockHash, txNum, txNum); err != nil {
		t.Fatalf("write tx range block %d: %v", blockNum, err)
	}
	change := &rawdb.StateDomainChange{
		BlockNum:   blockNum,
		BlockHash:  blockHash,
		TxNum:      txNum,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.SystemReward,
		Key:        []byte{byte('k'), byte(blockNum)},
		PrevExists: true,
		Prev:       []byte(prev),
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
		t.Fatalf("write change block %d: %v", blockNum, err)
	}
}

func writeColdBuilderCanonicalBlock(t *testing.T, db ethdb.KeyValueWriter, number uint64) common.Hash {
	t.Helper()
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(number) * 3000,
			},
		},
	})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("write canonical block %d: %v", number, err)
	}
	return block.Hash()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func assertColdRunnerGauge(t *testing.T, name string, want int64) {
	t.Helper()
	if got := coldRunnerGaugeValue(t, name); got != want {
		t.Fatalf("gauge %s = %d, want %d", name, got, want)
	}
}

func waitColdBuilderStats(t *testing.T, runner *Runner, ready func(Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if stats := runner.Snapshot(); ready(stats) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for cold builder stats, last = %+v", runner.Snapshot())
}

func coldRunnerGaugeValue(t *testing.T, name string) int64 {
	t.Helper()
	gauge, ok := metrics.DefaultRegistry.Get(name).(*metrics.Gauge)
	if !ok {
		t.Fatalf("missing gauge %s", name)
	}
	return gauge.Snapshot().Value()
}

func unregisterColdRunnerMetricNamespace(namespace string) {
	for _, suffix := range []string{
		"passes",
		"errors",
		"segments/built",
		"segments/compacted",
		"compaction/merges",
		"compaction/deferred/catchup",
		"bytes/built",
		"last/solidified_block",
		"last/eligible_cutoff_block",
		"last/selected_cutoff_block",
		"last/published_block",
		"lag/blocks",
		"last/visible_tx_end",
		"last/from_tx",
		"last/to_tx",
		"lastpass/duration",
		"lastpass/build/duration",
		"lastpass/compaction/duration",
		"lastpass/compaction/merges",
		"lastpass/latest/duration",
		"latest/deferred/sync",
		"history/deferred/sync",
		"history/deferred/rate_limit",
		"history/accelerated/builds",
		"history/deferred/resource",
		"sidecar/deferred/sync_or_resource",
		"sidecar/catchup/builds",
		"sidecar/event_log/freezer_handoffs",
		"last/latest_build_block",
	} {
		metrics.DefaultRegistry.Unregister(namespace + suffix)
	}
}
