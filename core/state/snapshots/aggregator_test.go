package snapshots

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestAggregatorBuildsManifestServesLatestAndHistory(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x66}, common.AccountIDLength)...))
	root := common.BytesToHash(bytes.Repeat([]byte{0xab}, common.HashLength))
	code := []byte{0x60, 0x00, 0x60, 0x01}
	codeHash := common.Keccak256(code)

	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 4); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 4, kvdomains.ContractStorage, []byte("slot/a"), []byte("storage-v1")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCode(db, codeHash, code); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, root); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{
		BlockNum:  3,
		BlockHash: common.Hash{0x03},
		Root:      root,
		Scheme:    rawdb.LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 3, common.Hash{0x03}, 10, 20); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   3,
		BlockHash:  common.Hash{0x03},
		TxNum:      15,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 4,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot/a"),
		PrevExists: true,
		Prev:       []byte("storage-v0"),
		NextExists: true,
		Next:       []byte("storage-v1"),
	}); err != nil {
		t.Fatal(err)
	}

	agg := NewAggregator(dir)
	result, err := agg.Build(db, AggregatorBuildOptions{FromTxNum: 10, ToTxNum: 20})
	if err != nil {
		t.Fatalf("build aggregate snapshot: %v", err)
	}
	if result.Manifest == nil || len(result.Segments) != 21 {
		t.Fatalf("aggregate result = %+v", result)
	}
	loaded, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	assertSegmentRef(t, loaded, SegmentDatasetAccountLatest, 0, SegmentLatest)
	assertSegmentRef(t, loaded, SegmentDatasetAccountLatest, 0, SegmentAccessor)
	assertSegmentRef(t, loaded, SegmentDatasetAccountLatest, 0, SegmentBTree)
	assertSegmentRef(t, loaded, SegmentDatasetKVGeneration, 0, SegmentLatest)
	assertSegmentRef(t, loaded, SegmentDatasetKVGeneration, 0, SegmentAccessor)
	assertSegmentRef(t, loaded, SegmentDatasetKVGeneration, 0, SegmentBTree)
	assertSegmentRef(t, loaded, SegmentDatasetCode, 0, SegmentLatest)
	assertSegmentRef(t, loaded, SegmentDatasetCode, 0, SegmentAccessor)
	assertSegmentRef(t, loaded, SegmentDatasetCode, 0, SegmentBTree)
	assertSegmentRef(t, loaded, SegmentDatasetKVLatest, kvdomains.ContractStorage, SegmentLatest)
	assertSegmentRef(t, loaded, SegmentDatasetKVLatest, kvdomains.ContractStorage, SegmentAccessor)
	assertSegmentRef(t, loaded, SegmentDatasetKVLatest, kvdomains.ContractStorage, SegmentBTree)
	assertSegmentRef(t, loaded, SegmentDatasetCommitmentRoot, 0, SegmentLatest)
	assertSegmentRef(t, loaded, SegmentDatasetCommitmentRoot, 0, SegmentAccessor)
	assertSegmentRef(t, loaded, SegmentDatasetCommitmentRoot, 0, SegmentBTree)
	assertSegmentRef(t, loaded, SegmentDatasetCommitmentCheckpoint, 0, SegmentLatest)
	assertSegmentRef(t, loaded, SegmentDatasetCommitmentCheckpoint, 0, SegmentAccessor)
	assertSegmentRef(t, loaded, SegmentDatasetCommitmentCheckpoint, 0, SegmentBTree)
	historyRef := assertSegmentRef(t, loaded, SegmentDatasetStateDomainChange, 0, SegmentHistory)
	assertSegmentRef(t, loaded, SegmentDatasetStateDomainChange, 0, SegmentAccessor)
	assertSegmentRef(t, loaded, SegmentDatasetStateDomainChange, 0, SegmentInverted)

	accountRef := assertSegmentRef(t, loaded, SegmentDatasetAccountLatest, 0, SegmentLatest)
	if !strings.HasSuffix(accountRef.Path, ".seg") {
		t.Fatalf("account latest path = %q, want .seg", accountRef.Path)
	}
	accountSeg, err := OpenLatestSegment(dir, accountRef)
	if err != nil {
		t.Fatalf("open account latest segment: %v", err)
	}
	if got, ok, err := accountSeg.Get(AccountSnapshotKey(owner)); err != nil || !ok || string(got) != "account-v1" {
		t.Fatalf("account latest segment get = %q ok=%v err=%v", got, ok, err)
	}
	historySeg, err := OpenStateDomainChangeSegment(dir, historyRef)
	if err != nil {
		t.Fatalf("open history segment: %v", err)
	}
	if len(historySeg.Changes) != 1 || historySeg.Changes[0].TxNum != 15 || string(historySeg.Changes[0].Next) != "storage-v1" {
		t.Fatalf("history changes = %+v", historySeg.Changes)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	if got, ok, err := mgr.GetAccountLatest(owner, 15); err != nil || !ok || string(got) != "account-v1" {
		t.Fatalf("manager account latest = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetKVLatest(kvdomains.ContractStorage, owner, 4, []byte("slot/a"), 15); err != nil || !ok || string(got) != "storage-v1" {
		t.Fatalf("manager kv latest = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetKVGeneration(owner, 15); err != nil || !ok || got != 4 {
		t.Fatalf("manager kv generation = %d ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetCode(codeHash, 15); err != nil || !ok || !bytes.Equal(got, code) {
		t.Fatalf("manager code = %x ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetCodeAtOrBefore(codeHash, 16); err != nil || !ok || !bytes.Equal(got, code) {
		t.Fatalf("manager code at-or-before = %x ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetCodeAtOrBefore(codeHash, 9); err != nil || ok || got != nil {
		t.Fatalf("manager future code at-or-before = %x ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetCommitmentRoot(15); err != nil || !ok || got != root {
		t.Fatalf("manager root = %x ok=%v err=%v", got, ok, err)
	}
	var historyReads []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChanges(15, 15, func(change *rawdb.StateDomainChange) (bool, error) {
		historyReads = append(historyReads, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate history: %v", err)
	}
	if len(historyReads) != 1 || string(historyReads[0].Prev) != "storage-v0" {
		t.Fatalf("history reads = %+v", historyReads)
	}
	for _, tc := range []struct {
		stage rawdb.StageID
		want  uint64
	}{
		{stage: rawdb.StageSnapshotLatest, want: 20},
		{stage: rawdb.StageSnapshotHistory, want: 20},
		{stage: rawdb.StageSnapshotAccessor, want: 20},
		{stage: rawdb.StageSnapshotCommitmentFlush, want: 20},
	} {
		if got, ok, err := rawdb.ReadStageProgress(db, tc.stage); err != nil || !ok || got != tc.want {
			t.Fatalf("%s progress = %d ok=%v err=%v, want %d", tc.stage, got, ok, err, tc.want)
		}
	}

	restored := rawdb.NewMemoryDatabase()
	if err := mgr.RestoreLatest(restored, 15); err != nil {
		t.Fatalf("restore latest: %v", err)
	}
	if got, ok, err := rawdb.ReadStateAccountLatest(restored, owner); err != nil || !ok || string(got) != "account-v1" {
		t.Fatalf("restored account = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(restored, owner, 4, kvdomains.ContractStorage, []byte("slot/a")); err != nil || !ok || string(got) != "storage-v1" {
		t.Fatalf("restored kv latest = %q ok=%v err=%v", got, ok, err)
	}
	if got := rawdb.ReadStateCode(restored, codeHash); !bytes.Equal(got, code) {
		t.Fatalf("restored code = %x", got)
	}
	if got, ok, err := rawdb.ReadStateCommitmentCheckpoint(restored, 3); err != nil || !ok || got.Root != root {
		t.Fatalf("restored checkpoint = %+v ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := rawdb.ReadLatestStateCommitmentCheckpoint(restored); err != nil || !ok || got.BlockNum != 3 || got.Root != root {
		t.Fatalf("restored latest checkpoint pointer = %+v ok=%v err=%v", got, ok, err)
	}

	newRoot := common.BytesToHash(bytes.Repeat([]byte{0xef}, common.HashLength))
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account-v2")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, newRoot); err != nil {
		t.Fatal(err)
	}
	updated, err := agg.Build(db, AggregatorBuildOptions{FromTxNum: 10, ToTxNum: 20})
	if err != nil {
		t.Fatalf("update aggregate snapshot: %v", err)
	}
	if len(updated.Manifest.Segments) != len(result.Manifest.Segments) {
		t.Fatalf("updated segments = %d, want %d", len(updated.Manifest.Segments), len(result.Manifest.Segments))
	}
	if updated.Manifest.Generation != result.Manifest.Generation+1 {
		t.Fatalf("updated generation = %d, want %d", updated.Manifest.Generation, result.Manifest.Generation+1)
	}
	if len(updated.Manifest.Retired) == 0 {
		t.Fatal("updated manifest did not record retired segments")
	}
	mgr, err = OpenManager(dir)
	if err != nil {
		t.Fatalf("reopen manager: %v", err)
	}
	if got, ok, err := mgr.GetAccountLatest(owner, 15); err != nil || !ok || string(got) != "account-v2" {
		t.Fatalf("updated account latest = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := mgr.GetCommitmentRoot(15); err != nil || !ok || got != newRoot {
		t.Fatalf("updated root = %x ok=%v err=%v", got, ok, err)
	}
}

func TestAggregatorBuildLatestOnly(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x77}, common.AccountIDLength)...))
	root := common.BytesToHash(bytes.Repeat([]byte{0xcd}, common.HashLength))
	code := []byte{0x60, 0x00, 0x60, 0x02}
	codeHash := common.Keccak256(code)

	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 5); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 5, kvdomains.ContractStorage, []byte("slot/b"), []byte("storage-v1")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCode(db, codeHash, code); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, root); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{
		BlockNum:  4,
		BlockHash: common.Hash{0x04},
		Root:      root,
		Scheme:    rawdb.LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 4, common.Hash{0x04}, 10, 20); err != nil {
		t.Fatal(err)
	}
	// Seed a history row to prove BuildLatest ignores available history data.
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   4,
		BlockHash:  common.Hash{0x04},
		TxNum:      15,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 5,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot/b"),
		PrevExists: true,
		Prev:       []byte("storage-v0"),
		NextExists: true,
		Next:       []byte("storage-v1"),
	}); err != nil {
		t.Fatal(err)
	}

	agg := NewAggregator(dir)
	result, err := agg.BuildLatest(db, AggregatorBuildOptions{FromTxNum: 10, ToTxNum: 20})
	if err != nil {
		t.Fatalf("BuildLatest: %v", err)
	}
	if result.Manifest == nil {
		t.Fatal("BuildLatest returned nil manifest")
	}

	// (a) Verify expected latest/accessor/btree refs are present.
	assertSegmentRef(t, result.Manifest, SegmentDatasetAccountLatest, 0, SegmentLatest)
	assertSegmentRef(t, result.Manifest, SegmentDatasetAccountLatest, 0, SegmentAccessor)
	assertSegmentRef(t, result.Manifest, SegmentDatasetAccountLatest, 0, SegmentBTree)
	assertSegmentRef(t, result.Manifest, SegmentDatasetKVGeneration, 0, SegmentLatest)
	assertSegmentRef(t, result.Manifest, SegmentDatasetKVGeneration, 0, SegmentAccessor)
	assertSegmentRef(t, result.Manifest, SegmentDatasetKVGeneration, 0, SegmentBTree)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCode, 0, SegmentLatest)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCode, 0, SegmentAccessor)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCode, 0, SegmentBTree)
	assertSegmentRef(t, result.Manifest, SegmentDatasetKVLatest, kvdomains.ContractStorage, SegmentLatest)
	assertSegmentRef(t, result.Manifest, SegmentDatasetKVLatest, kvdomains.ContractStorage, SegmentAccessor)
	assertSegmentRef(t, result.Manifest, SegmentDatasetKVLatest, kvdomains.ContractStorage, SegmentBTree)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCommitmentRoot, 0, SegmentLatest)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCommitmentRoot, 0, SegmentAccessor)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCommitmentRoot, 0, SegmentBTree)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCommitmentCheckpoint, 0, SegmentLatest)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCommitmentCheckpoint, 0, SegmentAccessor)
	assertSegmentRef(t, result.Manifest, SegmentDatasetCommitmentCheckpoint, 0, SegmentBTree)

	// (b) Assert NO history-kind refs are present.
	for _, ref := range result.Manifest.Segments {
		if ref.Kind == SegmentHistory || ref.Kind == SegmentInverted {
			t.Errorf("BuildLatest produced history ref: %+v", ref)
		}
	}

	// (c) Verify progress values.
	if result.Manifest.Progress == nil {
		t.Fatal("BuildLatest manifest has nil Progress")
	}
	if result.Manifest.Progress.LatestBuildTxNum != 20 {
		t.Errorf("LatestBuildTxNum = %d, want 20", result.Manifest.Progress.LatestBuildTxNum)
	}
	if result.Manifest.Progress.HistoryBuildTxNum != 0 {
		t.Errorf("HistoryBuildTxNum = %d, want 0", result.Manifest.Progress.HistoryBuildTxNum)
	}

	// Verify stage progress was written to rawdb for latest, not for history.
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatest); err != nil || !ok || got != 20 {
		t.Errorf("StageSnapshotLatest progress = %d ok=%v err=%v, want 20", got, ok, err)
	}
	if got, ok, _ := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHistory); ok {
		t.Errorf("StageSnapshotHistory progress = %d, want absent", got)
	}
}

func TestWriteManifestProgressStagesUsesStageProgressStore(t *testing.T) {
	store := &recordingSnapshotStageProgressStore{}
	progress := &Progress{
		LatestBuildTxNum:     11,
		HistoryBuildTxNum:    12,
		AccessorBuildTxNum:   13,
		CommitmentFlushTxNum: 14,
		HotPruneTxNum:        15,
	}
	if err := writeManifestProgressStages(store, progress); err != nil {
		t.Fatalf("write manifest progress stages: %v", err)
	}
	want := []snapshotStageProgressWrite{
		{stage: rawdb.StageSnapshotLatest, blockNum: 11},
		{stage: rawdb.StageSnapshotHistory, blockNum: 12},
		{stage: rawdb.StageSnapshotAccessor, blockNum: 13},
		{stage: rawdb.StageSnapshotCommitmentFlush, blockNum: 14},
		{stage: rawdb.StageSnapshotHotPrune, blockNum: 15},
	}
	if len(store.writes) != len(want) {
		t.Fatalf("writes = %+v, want %+v", store.writes, want)
	}
	for i := range want {
		if store.writes[i] != want[i] {
			t.Fatalf("write %d = %+v, want %+v", i, store.writes[i], want[i])
		}
	}
}

type snapshotStageProgressWrite struct {
	stage    rawdb.StageID
	blockNum uint64
}

type recordingSnapshotStageProgressStore struct {
	writes []snapshotStageProgressWrite
}

func (s *recordingSnapshotStageProgressStore) Write(stage rawdb.StageID, blockNum uint64) error {
	s.writes = append(s.writes, snapshotStageProgressWrite{stage: stage, blockNum: blockNum})
	return nil
}

func (s *recordingSnapshotStageProgressStore) Read(_ rawdb.StageID) (rawdb.StageProgress, bool, error) {
	return rawdb.StageProgress{}, false, nil
}

func assertSegmentRef(t *testing.T, manifest *Manifest, dataset SegmentDataset, domain kvdomains.KVDomain, kind SegmentKind) SegmentRef {
	t.Helper()
	for _, ref := range manifest.Segments {
		if ref.normalizedDataset() == dataset && ref.Domain == domain && ref.Kind == kind {
			return ref
		}
	}
	t.Fatalf("missing %s/%s domain %#04x in %+v", dataset, kind, uint16(domain), manifest.Segments)
	return SegmentRef{}
}
