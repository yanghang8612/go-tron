package snapshots

import (
	"bytes"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestColdBuilderConfigDefaultsHistoryDataset(t *testing.T) {
	cfg := Config{
		Dir:     t.TempDir(),
		Enabled: true,
	}.applyDefaults()
	if cfg.HistoryDataset != SegmentDatasetStateDomainChange {
		t.Fatalf("history dataset = %s, want %s", cfg.HistoryDataset, SegmentDatasetStateDomainChange)
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

func TestColdBuilderOnePassBuildsStateDomainChangeHistoryAndManagerReads(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x71)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderChange(t, db, owner, 2, 2, "b")
	writeColdBuilderChange(t, db, owner, 3, 3, "c")

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
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHistory); err != nil || !ok || got != 3 {
		t.Fatalf("snapshot history stage progress = %d ok=%v err=%v, want 3", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotAccessor); err != nil || !ok || got != 3 {
		t.Fatalf("snapshot accessor stage progress = %d ok=%v err=%v, want 3", got, ok, err)
	}

	var got []string
	if err := mgr.IterateStateDomainChanges(1, 3, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, string(change.Next))
		return true, nil
	}); err != nil {
		t.Fatalf("iterate state domain changes: %v", err)
	}
	if want := []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Fatalf("changes = %v, want %v", got, want)
	}
}

func TestColdBuilderSecondPassNoOpWhenManifestCoversCutoff(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x72)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderChange(t, db, owner, 2, 2, "b")

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
	}

	runner := NewRunner(&coldBuilderChain{db: db, solidified: 6}, Config{
		Dir:           dir,
		Enabled:       true,
		Interval:      time.Hour,
		HistoryWindow: 1,
		BatchBlocks:   2,
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
	if manifest.VisibleTxStart != 1 || manifest.VisibleTxEnd != 4 || len(manifest.Segments) != 6 {
		t.Fatalf("manifest after continuation = %+v", manifest)
	}
}

func TestColdBuilderCompactsSmallHistorySegments(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x76)
	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderChange(t, db, owner, 2, 2, "b")
	chain := &coldBuilderChain{db: db, solidified: 2}
	runner := NewRunner(chain, Config{
		Dir:                dir,
		Enabled:            true,
		Interval:           time.Hour,
		HistoryWindow:      1,
		BatchBlocks:        1,
		CompactMinSegments: 2,
		CompactMaxTxSpan:   2,
	})

	first, err := runner.OnePass()
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !first.Built || first.Compaction.Merged {
		t.Fatalf("first result = %+v", first)
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

func TestColdBuilderCursorIgnoresNonHistoryManifestVisibility(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := coldBuilderOwner(0x75)

	writeColdBuilderChange(t, db, owner, 1, 1, "a")
	writeColdBuilderChange(t, db, owner, 2, 2, "b")
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

type coldBuilderChain struct {
	db         AggregatorDB
	solidified int64
}

func (c *coldBuilderChain) DB() AggregatorDB { return c.db }

func (c *coldBuilderChain) LatestSolidifiedBlockNum() int64 { return c.solidified }

func coldBuilderOwner(seed byte) common.Address {
	return common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{seed}, common.AccountIDLength)...))
}

func writeColdBuilderChange(t *testing.T, db ethdb.KeyValueWriter, owner common.Address, blockNum, txNum uint64, next string) {
	t.Helper()
	blockHash := common.Hash{byte(blockNum)}
	if err := rawdb.WriteStateTxRange(db, blockNum, blockHash, txNum, txNum); err != nil {
		t.Fatalf("write tx range block %d: %v", blockNum, err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   blockNum,
		BlockHash:  blockHash,
		TxNum:      txNum,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.SystemReward,
		Key:        []byte{byte('k'), byte(blockNum)},
		PrevExists: true,
		Prev:       []byte("prev"),
		NextExists: true,
		Next:       []byte(next),
	}); err != nil {
		t.Fatalf("write change block %d: %v", blockNum, err)
	}
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
