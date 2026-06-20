package snapshots

import (
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPruneHotChainLookupsKeepsColdReads(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, root+"/src")
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	stateRoot := common.HexToHash("1212121212121212121212121212121212121212121212121212121212121212")
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw, stateRoot: stateRoot.Bytes()},
	})

	snapshotDir := root + "/snapshot"
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	hot := rawdb.NewMemoryDatabase()
	if _, err := RestoreChainFreezerIndexes(hot, snapshotDir, freezerRef); err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	if err := rawdb.WriteBlockStateRoot(hot, block1.Hash(), stateRoot); err != nil {
		t.Fatalf("WriteBlockStateRoot: %v", err)
	}
	chainDB := rawdb.NewChainDB(hot, rawdb.NewFreezerReader(src))
	if num := rawdb.ReadBlockNumber(chainDB, block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("hot ReadBlockNumber = %v, want 1", num)
	}
	if idx := rawdb.ReadTransactionIndex(chainDB, txHash[:]); idx == nil || *idx != 1 {
		t.Fatalf("hot ReadTransactionIndex = %v, want 1", idx)
	}
	assertChainFreezerTxInfo(t, "hot ReadTransactionInfo", rawdb.ReadTransactionInfo(chainDB, txHash[:]), 1)
	if got := rawdb.ReadBlockStateRoot(chainDB, block1.Hash()); got != stateRoot {
		t.Fatalf("hot ReadBlockStateRoot = %x, want %x", got, stateRoot)
	}

	result, err := PruneHotChainLookups(hot, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("PruneHotChainLookups: %v", err)
	}
	if result.ColdIndexSegments != 1 || result.BlockIndexesDeleted != 2 || result.StateRootsDeleted != 2 || result.TxIndexesDeleted != 1 || result.TxInfosDeleted != 1 {
		t.Fatalf("prune result = %+v, want one segment, 2 block/state roots, 1 tx index/info", result)
	}
	hotOnly := rawdb.NewChainDB(hot, rawdb.NoopAncient{})
	if num := rawdb.ReadBlockNumber(hotOnly, block1.Hash()); num != nil {
		t.Fatalf("hot-only ReadBlockNumber after prune = %v, want nil", num)
	}
	if idx := rawdb.ReadTransactionIndex(hotOnly, txHash[:]); idx != nil {
		t.Fatalf("hot-only ReadTransactionIndex after prune = %v, want nil", idx)
	}
	if info := rawdb.ReadTransactionInfo(hotOnly, txHash[:]); info != nil {
		t.Fatalf("hot-only ReadTransactionInfo after prune = %+v, want nil", info)
	}
	if got := rawdb.ReadBlockStateRoot(hotOnly, block1.Hash()); got != (common.Hash{}) {
		t.Fatalf("hot-only ReadBlockStateRoot after prune = %x, want zero", got)
	}

	chainDB.SetChainIndexReader(mgr)
	if num := rawdb.ReadBlockNumber(chainDB, block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("cold ReadBlockNumber = %v, want 1", num)
	}
	if idx := rawdb.ReadTransactionIndex(chainDB, txHash[:]); idx == nil || *idx != 1 {
		t.Fatalf("cold ReadTransactionIndex = %v, want 1", idx)
	}
	assertChainFreezerTxInfo(t, "cold ReadTransactionInfo after hot lookup prune", rawdb.ReadTransactionInfo(chainDB, txHash[:]), 1)
	if got := rawdb.ReadBlockStateRoot(chainDB, block1.Hash()); got != stateRoot {
		t.Fatalf("cold ReadBlockStateRoot = %x, want %x", got, stateRoot)
	}
}

func TestPruneHotChainLookupsWithProgressSkipsProcessedBlocks(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, root+"/src")
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	stateRoot := common.HexToHash("3434343434343434343434343434343434343434343434343434343434343434")
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw, stateRoot: stateRoot.Bytes()},
	})

	snapshotDir := root + "/snapshot"
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	hot := rawdb.NewMemoryDatabase()
	if _, err := RestoreChainFreezerIndexes(hot, snapshotDir, freezerRef); err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	if err := rawdb.WriteBlockStateRoot(hot, block1.Hash(), stateRoot); err != nil {
		t.Fatalf("WriteBlockStateRoot: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageChainFreezer, 1, block1.Hash()); err != nil {
		t.Fatalf("WriteStageProgressWithHash ChainFreezer: %v", err)
	}
	first, err := PruneHotChainLookupsWithProgress(hot, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("first PruneHotChainLookupsWithProgress: %v", err)
	}
	if !first.HasRange || first.FromBlock != 0 || first.ToBlock != 1 || first.BlockIndexesDeleted != 2 || first.TxIndexesDeleted != 1 {
		t.Fatalf("first prune result = %+v, want range 0..1 with block/tx deletes", first)
	}
	if got, ok, err := rawdb.ReadStageProgress(hot, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || got != 1 {
		t.Fatalf("chain lookup prune stage = %d ok=%v err=%v, want 1", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(hot, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block1.Hash() {
		t.Fatalf("chain lookup prune stage row = %+v ok=%v err=%v, want block1 hash", row, ok, err)
	}

	second, err := PruneHotChainLookupsWithProgress(hot, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("second PruneHotChainLookupsWithProgress: %v", err)
	}
	if second.HasRange || second.ColdIndexSegments != 0 || second.BlockIndexesDeleted != 0 || second.TxIndexesDeleted != 0 {
		t.Fatalf("second prune result = %+v, want no repeated work", second)
	}
	if info := rawdb.ReadTransactionInfo(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), txHash[:]); info != nil {
		t.Fatalf("hot tx info after stage-aware prune = %+v, want nil", info)
	}
}

func TestWriteSnapshotChainLookupPruneStageHashRules(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	wantHash := common.Hash{0x11}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 7); err != nil {
		t.Fatalf("WriteStageProgress unbound: %v", err)
	}
	if err := writeSnapshotChainLookupPruneStage(db, 7, wantHash, true); err != nil {
		t.Fatalf("writeSnapshotChainLookupPruneStage upgrade: %v", err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != wantHash {
		t.Fatalf("upgraded prune stage row = %+v ok=%v err=%v, want hash-bound", row, ok, err)
	}

	otherHash := common.Hash{0x22}
	err := writeSnapshotChainLookupPruneStage(db, 7, otherHash, true)
	if err == nil || !strings.Contains(err.Error(), "does not match pruned block hash") {
		t.Fatalf("writeSnapshotChainLookupPruneStage mismatch err = %v, want hash mismatch", err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || row.BlockHash != wantHash {
		t.Fatalf("prune stage after rejected mismatch = %+v ok=%v err=%v, want original hash", row, ok, err)
	}
}

func TestPruneHotChainLookupsWithProgressRejectsUnboundChainFreezerStage(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, root+"/src")
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw},
	})

	snapshotDir := root + "/snapshot"
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})
	hot := rawdb.NewMemoryDatabase()
	if _, err := RestoreChainFreezerIndexes(hot, snapshotDir, freezerRef); err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	if err := rawdb.WriteStageProgress(hot, rawdb.StageChainFreezer, 1); err != nil {
		t.Fatalf("WriteStageProgress ChainFreezer: %v", err)
	}

	_, err = PruneHotChainLookupsWithProgress(hot, snapshotDir, manifest)
	if err == nil || !strings.Contains(err.Error(), "ChainFreezer stage 1 is not hash-bound") {
		t.Fatalf("PruneHotChainLookupsWithProgress err = %v, want unbound ChainFreezer rejection", err)
	}
	if idx := rawdb.ReadTransactionIndex(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), txHash[:]); idx == nil || *idx != 1 {
		t.Fatalf("hot tx lookup after rejected unbound stage = %v, want still present", idx)
	}
}

func TestPruneHotChainLookupsWithProgressRejectsChainFreezerHashMismatchBeforeDelete(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, root+"/src")
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw},
	})

	snapshotDir := root + "/snapshot"
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})
	hot := rawdb.NewMemoryDatabase()
	if _, err := RestoreChainFreezerIndexes(hot, snapshotDir, freezerRef); err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	wrongHash := block1.Hash()
	wrongHash[0] ^= 0xff
	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageChainFreezer, 1, wrongHash); err != nil {
		t.Fatalf("WriteStageProgressWithHash ChainFreezer: %v", err)
	}

	_, err = PruneHotChainLookupsWithProgress(hot, snapshotDir, manifest)
	if err == nil || !strings.Contains(err.Error(), "ChainFreezer stage 1 hash") || !strings.Contains(err.Error(), "does not match chain-freezer segment hash") {
		t.Fatalf("PruneHotChainLookupsWithProgress err = %v, want ChainFreezer hash mismatch", err)
	}
	if num := rawdb.ReadBlockNumber(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), block0.Hash()); num == nil || *num != 0 {
		t.Fatalf("hot block0 lookup after rejected mismatch = %v, want still present", num)
	}
	if idx := rawdb.ReadTransactionIndex(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), txHash[:]); idx == nil || *idx != 1 {
		t.Fatalf("hot tx lookup after rejected mismatch = %v, want still present", idx)
	}
}

func TestPruneHotChainLookupsWithProgressUpgradesUnboundResumeStage(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, root+"/src")
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw},
	})

	snapshotDir := root + "/snapshot"
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})
	hot := rawdb.NewMemoryDatabase()
	if _, err := RestoreChainFreezerIndexes(hot, snapshotDir, freezerRef); err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageChainFreezer, 1, block1.Hash()); err != nil {
		t.Fatalf("WriteStageProgressWithHash ChainFreezer: %v", err)
	}
	if err := rawdb.WriteStageProgress(hot, rawdb.StageSnapshotChainLookupPrune, 1); err != nil {
		t.Fatalf("WriteStageProgress SnapshotChainLookupPrune: %v", err)
	}

	result, err := PruneHotChainLookupsWithProgress(hot, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("PruneHotChainLookupsWithProgress: %v", err)
	}
	if !result.HasRange || result.FromBlock != 0 || result.ToBlock != 1 || result.TxIndexesDeleted != 1 {
		t.Fatalf("prune result = %+v, want reprocessed range 0..1", result)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(hot, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block1.Hash() {
		t.Fatalf("upgraded chain lookup prune row = %+v ok=%v err=%v, want block1 hash", row, ok, err)
	}
	if idx := rawdb.ReadTransactionIndex(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), txHash[:]); idx != nil {
		t.Fatalf("hot tx lookup after upgraded prune = %v, want nil", idx)
	}
}

func TestPruneHotChainLookupsWithProgressWaitsForChainFreezerStage(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, root+"/src")
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw},
	})

	snapshotDir := root + "/snapshot"
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	hot := rawdb.NewMemoryDatabase()
	if _, err := RestoreChainFreezerIndexes(hot, snapshotDir, freezerRef); err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	if result, err := PruneHotChainLookupsWithProgress(hot, snapshotDir, manifest); err != nil || result.HasRange {
		t.Fatalf("prune without ChainFreezer stage = %+v err=%v, want no-op", result, err)
	}
	if num := rawdb.ReadBlockNumber(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("hot block1 lookup without ChainFreezer stage = %v, want 1", num)
	}

	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageChainFreezer, 0, block0.Hash()); err != nil {
		t.Fatalf("WriteStageProgressWithHash ChainFreezer 0: %v", err)
	}
	first, err := PruneHotChainLookupsWithProgress(hot, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("partial PruneHotChainLookupsWithProgress: %v", err)
	}
	if !first.HasRange || first.FromBlock != 0 || first.ToBlock != 0 || first.BlockIndexesDeleted != 1 || first.TxIndexesDeleted != 0 {
		t.Fatalf("partial prune result = %+v, want only block0 lookup", first)
	}
	if num := rawdb.ReadBlockNumber(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), block1.Hash()); num == nil || *num != 1 {
		t.Fatalf("hot block1 lookup after partial prune = %v, want 1", num)
	}
	if got, ok, err := rawdb.ReadStageProgress(hot, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || got != 0 {
		t.Fatalf("chain lookup prune stage after partial = %d ok=%v err=%v, want 0", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(hot, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block0.Hash() {
		t.Fatalf("partial chain lookup prune stage row = %+v ok=%v err=%v, want block0 hash", row, ok, err)
	}

	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageChainFreezer, 1, block1.Hash()); err != nil {
		t.Fatalf("WriteStageProgressWithHash ChainFreezer 1: %v", err)
	}
	second, err := PruneHotChainLookupsWithProgress(hot, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("final PruneHotChainLookupsWithProgress: %v", err)
	}
	if !second.HasRange || second.FromBlock != 1 || second.ToBlock != 1 || second.TxIndexesDeleted != 1 || second.TxInfosDeleted != 1 {
		t.Fatalf("final prune result = %+v, want block1 tx lookups", second)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(hot, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block1.Hash() {
		t.Fatalf("final chain lookup prune stage row = %+v ok=%v err=%v, want block1 hash", row, ok, err)
	}
	if idx := rawdb.ReadTransactionIndex(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), txHash[:]); idx != nil {
		t.Fatalf("hot tx lookup after final prune = %v, want nil", idx)
	}
}

func TestChainLookupPruneLifecycleNoManifestNoop(t *testing.T) {
	lifecycle := NewChainLookupPruneLifecycle(rawdb.NewMemoryDatabase(), ChainLookupPruneLifecycleConfig{Dir: t.TempDir()})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result != nil {
		t.Fatalf("OnePass result = %+v, want nil without manifest", result)
	}
}

func TestChainLookupPruneLifecycleOnePassPrunesCoveredLookups(t *testing.T) {
	root := t.TempDir()
	src := openChainFreezerTestStore(t, root+"/src")
	defer src.Close()
	block0 := canonicalBoundaryTestBlock(t, 0)
	block1, txHash, txInfoRaw := chainFreezerBlockWithTx(t, 1)
	appendChainFreezerRawRows(t, src, []chainFreezerRawTestRow{
		{block: block0},
		{block: block1, txInfosRaw: txInfoRaw},
	})

	snapshotDir := root + "/snapshot"
	freezerRef, err := BuildChainFreezerSegmentFromAncient(src, snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, freezerRef, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{freezerRef, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	hot := rawdb.NewMemoryDatabase()
	if _, err := RestoreChainFreezerIndexes(hot, snapshotDir, freezerRef); err != nil {
		t.Fatalf("RestoreChainFreezerIndexes: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageChainFreezer, 1, block1.Hash()); err != nil {
		t.Fatalf("WriteStageProgressWithHash ChainFreezer: %v", err)
	}
	lifecycle := NewChainLookupPruneLifecycle(hot, ChainLookupPruneLifecycleConfig{Dir: snapshotDir})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result == nil || !result.HasRange || result.ToBlock != 1 || result.TxIndexesDeleted != 1 || result.TxInfosDeleted != 1 {
		t.Fatalf("OnePass result = %+v, want covered tx lookup prune through block 1", result)
	}
	if got, ok, err := rawdb.ReadStageProgress(hot, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || got != 1 {
		t.Fatalf("chain lookup prune stage = %d ok=%v err=%v, want 1", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(hot, rawdb.StageSnapshotChainLookupPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block1.Hash() {
		t.Fatalf("lifecycle chain lookup prune stage row = %+v ok=%v err=%v, want block1 hash", row, ok, err)
	}
	if num := rawdb.ReadBlockNumber(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), block1.Hash()); num != nil {
		t.Fatalf("hot block lookup after lifecycle prune = %v, want nil", num)
	}
	if info := rawdb.ReadTransactionInfo(rawdb.NewChainDB(hot, rawdb.NoopAncient{}), txHash[:]); info != nil {
		t.Fatalf("hot tx info after lifecycle prune = %+v, want nil", info)
	}
}
