package snapshots

import (
	"bytes"
	"os"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestDefaultDomainRegistryDrivesSnapshotFamilies(t *testing.T) {
	registry := DefaultDomainRegistry()

	latest := registry.LatestConfigs()
	if len(latest) != 7 {
		t.Fatalf("latest configs = %d, want 7", len(latest))
	}
	for _, cfg := range latest {
		if cfg.LatestPathStem == "" {
			t.Fatalf("%s missing latest path stem", cfg.Dataset)
		}
		if cfg.BuildLatest == nil {
			t.Fatalf("%s missing latest builder", cfg.Dataset)
		}
		if !cfg.HasLatestAccessor || !cfg.HasLatestBTree {
			t.Fatalf("%s latest companion flags accessor=%v btree=%v", cfg.Dataset, cfg.HasLatestAccessor, cfg.HasLatestBTree)
		}
		ref := SegmentRef{Dataset: cfg.Dataset, Kind: SegmentAccessor, FromTxNum: 1, ToTxNum: 2, Path: cfg.LatestPathBase(kvdomains.ContractStorage) + "-1-2.lidx"}
		if cfg.DomainSpecific {
			ref.Domain = kvdomains.ContractStorage
		}
		if !IsLatestAccessorRef(ref) {
			t.Fatalf("%s accessor ref not recognized: %+v", cfg.Dataset, ref)
		}
		ref.Kind = SegmentBTree
		ref.Path = cfg.LatestPathBase(kvdomains.ContractStorage) + "-1-2.bt"
		if !IsLatestBTreeRef(ref) {
			t.Fatalf("%s btree ref not recognized: %+v", cfg.Dataset, ref)
		}
	}

	history := registry.HistoryConfigs()
	if len(history) != 1 {
		t.Fatalf("history configs = %d, want 1", len(history))
	}
	if history[0].Dataset != SegmentDatasetStateDomainChange || history[0].HistoryPath(10, 20) != "history/state-domain-change-10-20.seg" {
		t.Fatalf("history config = %+v path=%q", history[0], history[0].HistoryPath(10, 20))
	}
	if history[0].BuildHistory == nil || history[0].OpenHistory == nil || history[0].WriteHistory == nil ||
		history[0].ReadHistoryRange == nil || history[0].ReadHistoryByKey == nil ||
		history[0].IterateHistoryRange == nil || history[0].IterateHistoryByKey == nil ||
		history[0].CompactHistory == nil ||
		history[0].WriteHotHistoryRow == nil || history[0].WriteHotHistoryIndex == nil ||
		history[0].ReadHotHistoryTxRange == nil || history[0].IterateHotHistoryTxRanges == nil ||
		history[0].DeleteHotHistoryTxRange == nil ||
		history[0].DeleteHotHistoryBlock == nil ||
		history[0].IterateHotHistoryTxRangeChanges == nil ||
		history[0].IterateHotHistoryBlocks == nil || history[0].IterateHotHistoryChanges == nil ||
		history[0].IterateHotHistoryPrefix == nil || history[0].ReadHotAccountLatestAsOf == nil ||
		history[0].ReadHotKVLatestAsOf == nil || history[0].ReadHotKVGenerationAsOf == nil ||
		history[0].ReadHotAccountKVAsOf == nil ||
		history[0].IterateHotAccountKVPrefixAsOf == nil ||
		!history[0].HasHistoryAccessor || !history[0].HasHistoryInvertedIndex {
		t.Fatalf("history config missing builder/accessors: %+v", history[0])
	}
}

func TestDomainRegistryHotLatestReaders(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x66}
	accountValue := []byte("account-latest")
	if err := rawdb.WriteStateAccountLatest(db, owner, accountValue); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 9); err != nil {
		t.Fatal(err)
	}
	kvKey := []byte("reward/hot-latest-reader")
	kvValue := []byte("kv-value")
	if err := rawdb.WriteStateKVLatest(db, owner, 9, kvdomains.SystemReward, kvKey, kvValue); err != nil {
		t.Fatal(err)
	}
	code := []byte{0x60, 0x01, 0x00}
	codeHash := common.Keccak256(code)
	if err := rawdb.WriteStateCode(db, codeHash, code); err != nil {
		t.Fatal(err)
	}

	registry := DefaultDomainRegistry()
	accountCfg, ok := registry.Dataset(SegmentDatasetAccountLatest)
	if !ok || accountCfg.ReadHotAccountLatest == nil || accountCfg.IterateHotAccountLatest == nil {
		t.Fatalf("account latest hot reader missing: %+v", accountCfg)
	}
	gotAccount, ok, err := accountCfg.ReadHotAccountLatest(db, owner)
	if err != nil || !ok || !bytes.Equal(gotAccount, accountValue) {
		t.Fatalf("account latest = %q ok=%v err=%v", gotAccount, ok, err)
	}
	var accountRows int
	if err := accountCfg.IterateHotAccountLatest(db, nil, func(row rawdb.StateAccountLatestRow) (bool, error) {
		accountRows++
		if row.Owner != owner || !bytes.Equal(row.Value, accountValue) {
			t.Fatalf("account row = %+v", row)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate account latest: %v", err)
	}
	if accountRows != 1 {
		t.Fatalf("account latest rows = %d, want 1", accountRows)
	}
	generationCfg, ok := registry.Dataset(SegmentDatasetKVGeneration)
	if !ok || generationCfg.ReadHotKVGeneration == nil || generationCfg.IterateHotKVGeneration == nil {
		t.Fatalf("generation hot reader missing: %+v", generationCfg)
	}
	gotGeneration, ok, err := generationCfg.ReadHotKVGeneration(db, owner)
	if err != nil || !ok || gotGeneration != 9 {
		t.Fatalf("generation = %d ok=%v err=%v", gotGeneration, ok, err)
	}
	var generationRows int
	if err := generationCfg.IterateHotKVGeneration(db, nil, func(row rawdb.StateKVGenerationRow) (bool, error) {
		generationRows++
		if row.Owner != owner || row.Generation != 9 {
			t.Fatalf("generation row = %+v", row)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate generation: %v", err)
	}
	if generationRows != 1 {
		t.Fatalf("generation rows = %d, want 1", generationRows)
	}
	kvCfg, ok := registry.Dataset(SegmentDatasetKVLatest)
	if !ok || kvCfg.ReadHotKVLatest == nil || kvCfg.IterateHotKVLatestRows == nil {
		t.Fatalf("kv latest hot reader missing: %+v", kvCfg)
	}
	gotKV, ok, err := kvCfg.ReadHotKVLatest(db, owner, 9, kvdomains.SystemReward, kvKey)
	if err != nil || !ok || !bytes.Equal(gotKV, kvValue) {
		t.Fatalf("kv latest = %q ok=%v err=%v", gotKV, ok, err)
	}
	var kvRows int
	if err := kvCfg.IterateHotKVLatestRows(db, func(row rawdb.StateKVLatestRow) (bool, error) {
		kvRows++
		if row.Owner != owner || row.Generation != 9 || row.Domain != kvdomains.SystemReward || !bytes.Equal(row.Key, kvKey) || !bytes.Equal(row.Value, kvValue) {
			t.Fatalf("kv row = %+v", row)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate kv latest: %v", err)
	}
	if kvRows != 1 {
		t.Fatalf("kv rows = %d, want 1", kvRows)
	}
	codeCfg, ok := registry.Dataset(SegmentDatasetCode)
	if !ok || codeCfg.ReadHotCode == nil || codeCfg.IterateHotCode == nil || codeCfg.DeleteHotCode == nil {
		t.Fatalf("code hot reader missing: %+v", codeCfg)
	}
	gotCode, ok, err := codeCfg.ReadHotCode(db, codeHash)
	if err != nil || !ok || !bytes.Equal(gotCode, code) {
		t.Fatalf("code = %x ok=%v err=%v", gotCode, ok, err)
	}
	var codeRows int
	if err := codeCfg.IterateHotCode(db, func(row rawdb.StateCodeRow) (bool, error) {
		codeRows++
		if row.Hash != codeHash || !bytes.Equal(row.Code, code) {
			t.Fatalf("code row = %+v", row)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate code: %v", err)
	}
	if codeRows != 1 {
		t.Fatalf("code rows = %d, want 1", codeRows)
	}
	if err := codeCfg.DeleteHotCode(db, codeHash); err != nil {
		t.Fatalf("delete code through registry: %v", err)
	}
	if gotCode, ok, err = codeCfg.ReadHotCode(db, codeHash); err != nil || ok || len(gotCode) != 0 {
		t.Fatalf("code after registry delete = %x ok=%v err=%v", gotCode, ok, err)
	}
}

func TestDomainRegistryHotCommitmentLifecycle(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	root := common.Hash{0x01}
	nodeKey := append(rawdb.LatestDomainCommitmentNodeLogicalPrefix(), []byte("node")...)
	nodeValue := common.Hash{0x02}
	checkpointHash := common.Hash{0x03}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, root); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCommitmentDomain(db, nodeKey, nodeValue.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{
		BlockNum:  3,
		BlockHash: checkpointHash,
		Root:      root,
		Scheme:    rawdb.LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatal(err)
	}

	registry := DefaultDomainRegistry()
	commitmentCfg, ok := registry.Dataset(SegmentDatasetCommitmentNode)
	if !ok || commitmentCfg.IterateHotCommitmentDomain == nil {
		t.Fatalf("commitment domain lifecycle missing: %+v", commitmentCfg)
	}
	var rootSeen, nodeSeen, checkpointRowSeen, latestPointerSeen bool
	if err := commitmentCfg.IterateHotCommitmentDomain(db, nil, func(logicalKey, value []byte) (bool, error) {
		switch {
		case rawdb.IsLatestDomainCommitmentRootLogicalKey(logicalKey):
			rootSeen = bytes.Equal(value, root.Bytes())
		case bytes.Equal(logicalKey, nodeKey):
			nodeSeen = bytes.Equal(value, nodeValue.Bytes())
		case rawdb.IsLatestStateCommitmentCheckpointLogicalKey(logicalKey):
			latestPointerSeen = true
		case rawdb.IsStateCommitmentCheckpointLogicalKey(logicalKey):
			checkpointRowSeen = true
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate commitment domain: %v", err)
	}
	if !rootSeen || !nodeSeen || !checkpointRowSeen || !latestPointerSeen {
		t.Fatalf("commitment iteration root=%v node=%v checkpoint=%v latestPointer=%v", rootSeen, nodeSeen, checkpointRowSeen, latestPointerSeen)
	}
	checkpointCfg, ok := registry.Dataset(SegmentDatasetCommitmentCheckpoint)
	if !ok || checkpointCfg.WriteHotCommitmentCheckpoint == nil || checkpointCfg.ReadHotLatestCommitmentCheckpoint == nil ||
		checkpointCfg.IterateHotCommitmentCheckpoints == nil || checkpointCfg.DeleteHotCommitmentCheckpoint == nil {
		t.Fatalf("checkpoint lifecycle missing: %+v", checkpointCfg)
	}
	writtenHash := common.Hash{0x04}
	if err := checkpointCfg.WriteHotCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{
		BlockNum:  4,
		BlockHash: writtenHash,
		Root:      root,
		Scheme:    rawdb.LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("write checkpoint through registry: %v", err)
	}
	if latest, ok, err := checkpointCfg.ReadHotLatestCommitmentCheckpoint(db); err != nil || !ok || latest.BlockNum != 4 || latest.BlockHash != writtenHash {
		t.Fatalf("latest checkpoint through registry = %+v ok=%v err=%v", latest, ok, err)
	}
	var checkpoints []uint64
	if err := checkpointCfg.IterateHotCommitmentCheckpoints(db, func(cp *rawdb.StateCommitmentCheckpoint) (bool, error) {
		checkpoints = append(checkpoints, cp.BlockNum)
		switch cp.BlockNum {
		case 3:
			if cp.Root != root || cp.BlockHash != checkpointHash || cp.Scheme != rawdb.LatestDomainCommitmentScheme {
				t.Fatalf("checkpoint 3 = %+v", cp)
			}
		case 4:
			if cp.Root != root || cp.BlockHash != writtenHash || cp.Scheme != rawdb.LatestDomainCommitmentScheme {
				t.Fatalf("checkpoint 4 = %+v", cp)
			}
		default:
			t.Fatalf("unexpected checkpoint = %+v", cp)
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate checkpoints: %v", err)
	}
	if len(checkpoints) != 2 {
		t.Fatalf("checkpoints = %v, want two entries", checkpoints)
	}
	if err := checkpointCfg.DeleteHotCommitmentCheckpoint(db, 3); err != nil {
		t.Fatalf("delete checkpoint through registry: %v", err)
	}
	if got, ok, err := rawdb.ReadStateCommitmentCheckpoint(db, 3); err != nil || ok || got != nil {
		t.Fatalf("checkpoint after registry delete = %+v ok=%v err=%v", got, ok, err)
	}
}

func TestDomainRegistryHotHistoryPruneRunnerOwnsTxRangeMetadata(t *testing.T) {
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain-change history config")
	}
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x55}
	for _, blockNum := range []uint64{1, 2} {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum*10, blockNum*10+2); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
			BlockNum:   blockNum,
			BlockHash:  common.Hash{byte(blockNum)},
			TxNum:      blockNum*10 + 1,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 4,
			Domain:     kvdomains.SystemReward,
			Key:        []byte("reward/prune-runner"),
			PrevExists: true,
			Prev:       []byte("old"),
			NextExists: true,
			Next:       []byte("new"),
		}); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := cfg.PruneHotHistory(db, HotHistoryPruneOptions{
		MaxBlocks: 1,
		Decide: func(row *rawdb.StateTxRange) (HotHistoryPruneDecision, error) {
			return HotHistoryPruneDecision{DeleteTxRange: true, DeleteHistoryBlock: true}, nil
		},
	})
	if err != nil {
		t.Fatalf("prune hot history via registry: %v", err)
	}
	if stats.DeletedTxRanges != 1 || stats.DeletedHistoryBlocks != 1 || stats.MaxDeletedHistoryBlockTx != 12 {
		t.Fatalf("stats = %+v, want one block ending at tx 12", stats)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || ok {
		t.Fatalf("block 1 tx range survived ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 2); err != nil || !ok {
		t.Fatalf("block 2 tx range missing ok=%v err=%v", ok, err)
	}
	txNum, err := cfg.HotHistoryTxNumAtBlockEnd(db, 2)
	if err != nil || txNum != 22 {
		t.Fatalf("hot history tx num = %d err=%v, want 22", txNum, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("block 1 domain change survived ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 2, 1); err != nil || !ok {
		t.Fatalf("block 2 domain change missing ok=%v err=%v", ok, err)
	}
	var blocks []uint64
	if err := cfg.IterateHotHistoryBlocks(db, rawdb.StateFlatDomainKVLatest, owner, 4, kvdomains.SystemReward, []byte("reward/prune-runner"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate after prune: %v", err)
	}
	if len(blocks) != 1 || blocks[0] != 2 {
		t.Fatalf("remaining inverse blocks = %v, want [2]", blocks)
	}
}

func TestDomainRegistryHotHistoryPublicationDispatch(t *testing.T) {
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain-change history config")
	}
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x44}
	for _, row := range []struct {
		blockNum uint64
		txNum    uint64
	}{
		{blockNum: 7, txNum: 42},
		{blockNum: 8, txNum: 43},
		{blockNum: 9, txNum: 44},
	} {
		if err := rawdb.WriteStateTxRange(db, row.blockNum, common.Hash{byte(row.blockNum)}, row.txNum, row.txNum); err != nil {
			t.Fatal(err)
		}
	}
	change := &rawdb.StateDomainChange{
		BlockNum:   7,
		BlockHash:  common.Hash{0x07},
		TxNum:      42,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 3,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/hot-registry"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("new"),
	}
	accountChange := &rawdb.StateDomainChange{
		BlockNum:   8,
		BlockHash:  common.Hash{0x08},
		TxNum:      43,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		PrevExists: true,
		Prev:       []byte("old-account"),
		NextExists: true,
		Next:       []byte("new-account"),
	}
	generationChange := &rawdb.StateDomainChange{
		BlockNum:   9,
		BlockHash:  common.Hash{0x09},
		TxNum:      44,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVGeneration,
		Owner:      owner,
		PrevExists: true,
		Prev:       rawdb.EncodeStateKVGenerationValue(2),
		NextExists: true,
		Next:       rawdb.EncodeStateKVGenerationValue(3),
	}
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("new-account")); err != nil {
		t.Fatalf("write hot account latest: %v", err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 3); err != nil {
		t.Fatalf("write hot generation: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 3, kvdomains.SystemReward, []byte("reward/hot-registry"), []byte("new")); err != nil {
		t.Fatalf("write hot latest: %v", err)
	}
	if err := cfg.WriteHotHistoryRow(db, change); err != nil {
		t.Fatalf("write hot row via registry: %v", err)
	}
	if err := cfg.WriteHotHistoryRow(db, accountChange); err != nil {
		t.Fatalf("write hot account row via registry: %v", err)
	}
	if err := cfg.WriteHotHistoryRow(db, generationChange); err != nil {
		t.Fatalf("write hot generation row via registry: %v", err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 7, 1); err != nil || !ok {
		t.Fatalf("read hot row = ok:%v err:%v", ok, err)
	}
	var blocks []uint64
	if err := cfg.IterateHotHistoryBlocks(db, rawdb.StateFlatDomainKVLatest, owner, 3, kvdomains.SystemReward, []byte("reward/hot-registry"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate before index: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("row-only registry publish created inverse blocks %v", blocks)
	}
	if err := cfg.WriteHotHistoryIndex(db, change); err != nil {
		t.Fatalf("write hot inverse index via registry: %v", err)
	}
	if err := cfg.WriteHotHistoryIndex(db, accountChange); err != nil {
		t.Fatalf("write hot account inverse index via registry: %v", err)
	}
	if err := cfg.WriteHotHistoryIndex(db, generationChange); err != nil {
		t.Fatalf("write hot generation inverse index via registry: %v", err)
	}
	if err := cfg.IterateHotHistoryBlocks(db, rawdb.StateFlatDomainKVLatest, owner, 3, kvdomains.SystemReward, []byte("reward/hot-registry"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate after index: %v", err)
	}
	if len(blocks) != 1 || blocks[0] != 7 {
		t.Fatalf("inverse blocks = %v, want [7]", blocks)
	}
	var changes []*rawdb.StateDomainChange
	if err := cfg.IterateHotHistoryChanges(db, 0, 42, rawdb.StateFlatDomainKVLatest, owner, 3, kvdomains.SystemReward, []byte("reward/hot-registry"), func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate hot changes via registry: %v", err)
	}
	if len(changes) != 1 || changes[0].BlockNum != 7 || string(changes[0].Next) != "new" {
		t.Fatalf("hot changes = %+v", changes)
	}
	var txRangeChanges []*rawdb.StateDomainChange
	if err := cfg.IterateHotHistoryChangesByTxRange(db, 42, 44, func(change *rawdb.StateDomainChange) (bool, error) {
		txRangeChanges = append(txRangeChanges, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate tx-range hot changes via registry: %v", err)
	}
	if len(txRangeChanges) != 3 {
		t.Fatalf("tx-range hot changes = %d, want 3", len(txRangeChanges))
	}
	var prefixChanges []*rawdb.StateDomainChange
	if err := cfg.IterateHotHistoryPrefix(db, 0, 42, owner, 3, kvdomains.SystemReward, []byte("reward/"), func(change *rawdb.StateDomainChange) (bool, error) {
		prefixChanges = append(prefixChanges, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate hot prefix changes via registry: %v", err)
	}
	if len(prefixChanges) != 1 || prefixChanges[0].BlockNum != 7 || string(prefixChanges[0].Key) != "reward/hot-registry" {
		t.Fatalf("hot prefix changes = %+v", prefixChanges)
	}
	value, ok, err := cfg.ReadHotAccountLatestAsOf(db, owner, 0, 43)
	if err != nil || !ok || string(value) != "old-account" {
		t.Fatalf("hot account latest as-of = %q ok=%v err=%v, want old-account", value, ok, err)
	}
	value, ok, err = cfg.ReadHotKVLatestAsOf(db, owner, 3, kvdomains.SystemReward, []byte("reward/hot-registry"), 0, 42)
	if err != nil || !ok || string(value) != "old" {
		t.Fatalf("hot kv latest as-of = %q ok=%v err=%v, want old", value, ok, err)
	}
	generation, ok, err := cfg.ReadHotKVGenerationAsOf(db, owner, 0, 44)
	if err != nil || !ok || generation != 2 {
		t.Fatalf("hot kv generation as-of = %d ok=%v err=%v, want 2", generation, ok, err)
	}
	value, ok, err = cfg.ReadHotAccountKVAsOf(db, owner, kvdomains.SystemReward, []byte("reward/hot-registry"), 0, 42)
	if err != nil || !ok || string(value) != "old" {
		t.Fatalf("hot account kv as-of = %q ok=%v err=%v, want old", value, ok, err)
	}
	values := make(map[string]string)
	if err := cfg.IterateHotAccountKVPrefixAsOf(db, owner, kvdomains.SystemReward, []byte("reward/"), 0, 42, func(key, value []byte) (bool, error) {
		values[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate hot account kv prefix as-of via registry: %v", err)
	}
	if len(values) != 1 || values["reward/hot-registry"] != "old" {
		t.Fatalf("hot account kv prefix as-of = %v, want old pre-value", values)
	}
	if err := cfg.DeleteHotHistoryBlock(db, 7); err != nil {
		t.Fatalf("delete hot history block via registry: %v", err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 7, 1); err != nil || ok {
		t.Fatalf("deleted hot history row ok=%v err=%v", ok, err)
	}
	blocks = blocks[:0]
	if err := cfg.IterateHotHistoryBlocks(db, rawdb.StateFlatDomainKVLatest, owner, 3, kvdomains.SystemReward, []byte("reward/hot-registry"), func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate after delete: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("deleted hot inverse blocks = %v, want none", blocks)
	}
}

func TestDomainRegistryHistoryCompanionsAndCoverage(t *testing.T) {
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain-change history config")
	}

	segRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 10,
		ToTxNum:   12,
		Path:      cfg.HistoryPath(10, 12),
	}
	idxRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentInverted,
		FromTxNum: 10,
		ToTxNum:   12,
		Path:      cfg.HistoryIndexPathFor(segRef.Path),
	}
	accessorRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentAccessor,
		FromTxNum: 10,
		ToTxNum:   12,
		Path:      cfg.HistoryAccessorPathFor(segRef.Path),
	}
	manifest := &Manifest{Segments: []SegmentRef{
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 20, ToTxNum: 22, Path: cfg.HistoryPath(20, 22)},
		segRef,
		idxRef,
		accessorRef,
		{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 13, ToTxNum: 19, Path: cfg.HistoryPath(13, 19)},
	}}

	if !cfg.IsHistoryBinarySegmentPath(segRef.Path) {
		t.Fatalf("history segment path %q not recognized", segRef.Path)
	}
	if !cfg.IsHistoryBinaryCompanionPath(idxRef.Path) || !cfg.IsHistoryBinaryCompanionPath(accessorRef.Path) {
		t.Fatalf("history companion paths not recognized: %q %q", idxRef.Path, accessorRef.Path)
	}
	if got, ok := cfg.HistoryIndexRef(manifest, segRef); !ok || got.Path != idxRef.Path {
		t.Fatalf("history index ref = %+v ok=%v, want %q", got, ok, idxRef.Path)
	}
	if got, ok := cfg.HistoryAccessorRef(manifest, segRef); !ok || got.Path != accessorRef.Path {
		t.Fatalf("history accessor ref = %+v ok=%v, want %q", got, ok, accessorRef.Path)
	}

	ranges := HistoryTxRanges(manifest, SegmentDatasetStateDomainChange)
	if len(ranges) != 3 || ranges[0] != (TxRange{From: 10, To: 12}) || ranges[1] != (TxRange{From: 13, To: 19}) || ranges[2] != (TxRange{From: 20, To: 22}) {
		t.Fatalf("history ranges = %+v", ranges)
	}
	if got := ContiguousHistoryVisibleTxEnd(manifest, SegmentDatasetStateDomainChange, 10); got != 22 {
		t.Fatalf("contiguous visible end = %d, want 22", got)
	}
	if got := ContiguousHistoryVisibleTxEnd(manifest, SegmentDatasetStateDomainChange, 9); got != 0 {
		t.Fatalf("visible end across a gap = %d, want 0", got)
	}
}

func TestDomainRegistryHistoryCodecDispatch(t *testing.T) {
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain-change history config")
	}
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x57}, common.AccountIDLength)...))
	changes := []*rawdb.StateDomainChange{
		{
			BlockNum:   1,
			TxNum:      1,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 7,
			Domain:     kvdomains.ContractStorage,
			Key:        []byte("slot/a"),
			PrevExists: true,
			Prev:       []byte("old-a"),
			NextExists: true,
			Next:       []byte("new-a"),
		},
		{
			BlockNum:   2,
			TxNum:      2,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 7,
			Domain:     kvdomains.ContractStorage,
			Key:        []byte("slot/b"),
			PrevExists: true,
			Prev:       []byte("old-b"),
			NextExists: true,
			Next:       []byte("new-b"),
		},
	}
	segRef, idxRef, accessorRef, err := cfg.WriteHistory(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: 1,
		ToTxNum:   2,
		Path:      cfg.HistoryPath(1, 2),
	}, changes)
	if err != nil {
		t.Fatalf("write history via registry: %v", err)
	}
	manifest := NewManifest(1, 2, []SegmentRef{segRef, accessorRef, idxRef})

	opened, err := cfg.OpenHistory(dir, segRef)
	if err != nil {
		t.Fatalf("open history via registry: %v", err)
	}
	if len(opened) != 2 {
		t.Fatalf("opened changes = %d, want 2", len(opened))
	}
	ranged, err := cfg.ReadHistoryRange(dir, manifest, segRef, 2, 2)
	if err != nil {
		t.Fatalf("range history via registry: %v", err)
	}
	if len(ranged) != 1 || string(ranged[0].Key) != "slot/b" {
		t.Fatalf("range changes = %+v", ranged)
	}
	lookupKey := stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainKVLatest, owner, 7, kvdomains.ContractStorage, []byte("slot/a"))
	keyed, err := cfg.ReadHistoryByKey(dir, manifest, segRef, lookupKey, 1, 2)
	if err != nil {
		t.Fatalf("key history via registry: %v", err)
	}
	if len(keyed) != 1 || string(keyed[0].Key) != "slot/a" {
		t.Fatalf("keyed changes = %+v", keyed)
	}
}

func TestDomainRegistryCheckRegisteredSegmentDispatch(t *testing.T) {
	dir := t.TempDir()
	latestCfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetAccountLatest)
	if !ok {
		t.Fatal("missing account latest config")
	}
	checked, err := CheckRegisteredSegment(dir, SegmentRef{
		Dataset:   SegmentDatasetAccountLatest,
		Kind:      SegmentLatest,
		FromTxNum: 1,
		ToTxNum:   1,
		Path:      latestCfg.LatestPathBase(0) + "-1-1.seg",
	})
	if !checked || !os.IsNotExist(err) {
		t.Fatalf("latest check checked=%v err=%v, want registered missing-file check", checked, err)
	}

	historyCfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain-change config")
	}
	historyPath := historyCfg.HistoryPath(1, 1)
	checked, err = CheckRegisteredSegment(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentAccessor,
		FromTxNum: 1,
		ToTxNum:   1,
		Path:      historyCfg.HistoryAccessorPathFor(historyPath),
	})
	if !checked || !os.IsNotExist(err) {
		t.Fatalf("history accessor check checked=%v err=%v, want registered missing-file check", checked, err)
	}

	checked, err = CheckRegisteredSegment(dir, SegmentRef{
		Dataset:   SegmentDataset("unknown"),
		Kind:      SegmentHistory,
		FromTxNum: 1,
		ToTxNum:   1,
		Path:      "history/unknown-1-1.seg",
	})
	if checked || err != nil {
		t.Fatalf("unknown check checked=%v err=%v, want fallback", checked, err)
	}
}
