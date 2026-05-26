package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestStateCommitmentCheckpointRoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	cp := &StateCommitmentCheckpoint{
		BlockNum:  12,
		BlockHash: common.Hash{0x12},
		Root:      common.Hash{0xaa},
		Scheme:    LatestDomainCommitmentScheme,
	}
	if _, ok, err := ReadStateCommitmentCheckpoint(db, 12); err != nil || ok {
		t.Fatalf("pre-read = ok:%v err:%v", ok, err)
	}
	if err := WriteStateCommitmentCheckpoint(db, cp); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	if ok, err := db.Has(stateCommitmentCheckpointKey(12)); err != nil || ok {
		t.Fatalf("legacy checkpoint prefix has row ok=%v err=%v", ok, err)
	}
	if _, ok, err := ReadStateCommitmentDomain(db, stateCommitmentCheckpointLogicalKey(12)); err != nil || !ok {
		t.Fatalf("checkpoint missing from CommitmentDomain: ok=%v err=%v", ok, err)
	}
	got, ok, err := ReadStateCommitmentCheckpoint(db, 12)
	if err != nil || !ok {
		t.Fatalf("read checkpoint = ok:%v err:%v", ok, err)
	}
	if got.BlockNum != cp.BlockNum || got.BlockHash != cp.BlockHash || got.Root != cp.Root || got.Scheme != cp.Scheme {
		t.Fatalf("checkpoint = %+v, want %+v", got, cp)
	}
	if err := WriteStateCommitmentCheckpoint(db, &StateCommitmentCheckpoint{
		BlockNum: 13,
		Root:     common.Hash{0xbb},
		Scheme:   LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("write second checkpoint: %v", err)
	}
	var blocks []uint64
	if err := IterateStateCommitmentCheckpoints(db, func(cp *StateCommitmentCheckpoint) (bool, error) {
		blocks = append(blocks, cp.BlockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate checkpoints: %v", err)
	}
	if len(blocks) != 2 || blocks[0] != 12 || blocks[1] != 13 {
		t.Fatalf("checkpoint blocks = %v, want [12 13]", blocks)
	}
	if err := DeleteStateCommitmentCheckpoint(db, 12); err != nil {
		t.Fatalf("delete checkpoint: %v", err)
	}
	if _, ok, err := ReadStateCommitmentCheckpoint(db, 12); err != nil || ok {
		t.Fatalf("deleted checkpoint ok=%v err=%v", ok, err)
	}
}

func TestStateCommitmentCheckpointMaintainsLatestPointer(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	checkpoints := []*StateCommitmentCheckpoint{
		{BlockNum: 12, BlockHash: common.Hash{0x12}, Root: common.Hash{0xa1}, Scheme: LatestDomainCommitmentScheme},
		{BlockNum: 11, BlockHash: common.Hash{0x11}, Root: common.Hash{0xa0}, Scheme: LatestDomainCommitmentScheme},
		{BlockNum: 13, BlockHash: common.Hash{0x13}, Root: common.Hash{0xa2}, Scheme: LatestDomainCommitmentScheme},
	}
	for i, checkpoint := range checkpoints {
		if err := WriteStateCommitmentCheckpoint(db, checkpoint); err != nil {
			t.Fatalf("write checkpoint %d: %v", checkpoint.BlockNum, err)
		}
		latest, ok, err := ReadLatestStateCommitmentCheckpoint(db)
		if err != nil || !ok {
			t.Fatalf("latest after write %d = ok:%v err:%v", checkpoint.BlockNum, ok, err)
		}
		wantBlock := uint64(12)
		if i == 2 {
			wantBlock = 13
		}
		if latest.BlockNum != wantBlock {
			t.Fatalf("latest after write %d = block %d, want %d", checkpoint.BlockNum, latest.BlockNum, wantBlock)
		}
	}

	var iterated []uint64
	if err := IterateStateCommitmentCheckpoints(db, func(checkpoint *StateCommitmentCheckpoint) (bool, error) {
		iterated = append(iterated, checkpoint.BlockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate checkpoints: %v", err)
	}
	if len(iterated) != 3 || iterated[0] != 11 || iterated[1] != 12 || iterated[2] != 13 {
		t.Fatalf("iterated checkpoints = %v, want [11 12 13]", iterated)
	}

	if err := DeleteStateCommitmentCheckpoint(db, 13); err != nil {
		t.Fatalf("delete latest checkpoint: %v", err)
	}
	latest, ok, err := ReadLatestStateCommitmentCheckpoint(db)
	if err != nil || !ok || latest.BlockNum != 12 {
		t.Fatalf("latest after deleting 13 = block:%v ok:%v err:%v, want 12,true,nil", latest, ok, err)
	}
	if err := DeleteStateCommitmentCheckpoint(db, 12); err != nil {
		t.Fatalf("delete repaired latest checkpoint: %v", err)
	}
	latest, ok, err = ReadLatestStateCommitmentCheckpoint(db)
	if err != nil || !ok || latest.BlockNum != 11 {
		t.Fatalf("latest after deleting 12 = block:%v ok:%v err:%v, want 11,true,nil", latest, ok, err)
	}
	if err := DeleteStateCommitmentCheckpoint(db, 11); err != nil {
		t.Fatalf("delete final checkpoint: %v", err)
	}
	if _, ok, err := ReadLatestStateCommitmentCheckpoint(db); err != nil || ok {
		t.Fatalf("latest after deleting all checkpoints ok=%v err=%v, want false,nil", ok, err)
	}
}

func TestStateCommitmentCheckpointRepairsMissingLatestPointerOnWrite(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	for _, checkpoint := range []*StateCommitmentCheckpoint{
		{BlockNum: 12, BlockHash: common.Hash{0x12}, Root: common.Hash{0xa1}, Scheme: LatestDomainCommitmentScheme},
		{BlockNum: 13, BlockHash: common.Hash{0x13}, Root: common.Hash{0xa2}, Scheme: LatestDomainCommitmentScheme},
	} {
		if err := WriteStateCommitmentCheckpoint(db, checkpoint); err != nil {
			t.Fatalf("write checkpoint %d: %v", checkpoint.BlockNum, err)
		}
	}
	if err := DeleteStateCommitmentDomain(db, LatestStateCommitmentCheckpointLogicalKey()); err != nil {
		t.Fatalf("delete latest checkpoint pointer: %v", err)
	}
	if err := WriteStateCommitmentCheckpoint(db, &StateCommitmentCheckpoint{
		BlockNum:  11,
		BlockHash: common.Hash{0x11},
		Root:      common.Hash{0xa0},
		Scheme:    LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("write lower checkpoint after pointer loss: %v", err)
	}
	latest, ok, err := ReadLatestStateCommitmentCheckpoint(db)
	if err != nil || !ok || latest.BlockNum != 13 {
		t.Fatalf("latest after low write repairs to block:%v ok:%v err:%v, want 13,true,nil", latest, ok, err)
	}
}

func TestComputeLatestDomainRootDeterministicAndSensitive(t *testing.T) {
	db1 := ethrawdb.NewMemoryDatabase()
	db2 := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	other := common.Address{0x41, 0x02}

	mustWriteStateKVLatest(t, db1, owner, 0, kvdomains.SystemReward, []byte("b"), []byte("2"))
	mustWriteStateKVLatest(t, db1, owner, 0, kvdomains.SystemReward, []byte("a"), []byte("1"))
	if err := WriteStateKVGeneration(db1, owner, 0); err != nil {
		t.Fatal(err)
	}

	if err := WriteStateKVGeneration(db2, owner, 0); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db2, owner, 0, kvdomains.SystemReward, []byte("a"), []byte("1"))
	mustWriteStateKVLatest(t, db2, owner, 0, kvdomains.SystemReward, []byte("b"), []byte("2"))

	root1, err := ComputeLatestDomainRoot(db1)
	if err != nil {
		t.Fatalf("root1: %v", err)
	}
	root2, err := ComputeLatestDomainRoot(db2)
	if err != nil {
		t.Fatalf("root2: %v", err)
	}
	if root1 != root2 {
		t.Fatalf("same latest rows produced different roots: %x vs %x", root1, root2)
	}

	mustWriteStateKVLatest(t, db2, other, 0, kvdomains.SystemReward, []byte("a"), []byte("1"))
	root3, err := ComputeLatestDomainRoot(db2)
	if err != nil {
		t.Fatalf("root3: %v", err)
	}
	if root3 == root2 {
		t.Fatal("commitment root did not change after latest-domain mutation")
	}

	if err := WriteStateAccountLatest(db2, owner, []byte("account-envelope")); err != nil {
		t.Fatal(err)
	}
	root4, err := ComputeLatestDomainRoot(db2)
	if err != nil {
		t.Fatalf("root4: %v", err)
	}
	if root4 == root3 {
		t.Fatal("commitment root did not change after flat account mutation")
	}
	if got, err := ComputeAndWriteLatestDomainRoot(db2); err != nil || got == (common.Hash{}) {
		t.Fatalf("compute/write root = %x err=%v", got, err)
	} else if stored, ok, err := ReadLatestDomainCommitmentRoot(db2); err != nil || !ok || stored != got {
		t.Fatalf("stored commitment root = %x ok=%v err=%v, want %x", stored, ok, err, got)
	}
}

func TestLatestDomainCommitmentIncrementalMatchesRebuild(t *testing.T) {
	owner := common.Address{0x41, 0x11}
	slotKey := []byte("slot")

	inc := ethrawdb.NewMemoryDatabase()
	mustWriteStateKVLatest(t, inc, owner, 0, kvdomains.ContractStorage, slotKey, []byte("v1"))
	if err := WriteStateKVGeneration(inc, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateAccountLatest(inc, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	root1, err := RebuildLatestDomainCommitment(inc)
	if err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	if root1 == (common.Hash{}) {
		t.Fatal("initial rebuild produced zero root")
	}

	mustWriteStateKVLatest(t, inc, owner, 0, kvdomains.ContractStorage, slotKey, []byte("v2"))
	kvKey := StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, slotKey)
	kvValue, err := inc.Get(kvKey)
	if err != nil {
		t.Fatalf("read encoded kv latest: %v", err)
	}
	root2, err := UpdateLatestDomainCommitment(inc, []StateCommitmentUpdate{
		NewStateCommitmentPut(kvKey, kvValue),
	})
	if err != nil {
		t.Fatalf("incremental update: %v", err)
	}

	rebuild := ethrawdb.NewMemoryDatabase()
	mustWriteStateKVLatest(t, rebuild, owner, 0, kvdomains.ContractStorage, slotKey, []byte("v2"))
	if err := WriteStateKVGeneration(rebuild, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateAccountLatest(rebuild, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	want2, err := RebuildLatestDomainCommitment(rebuild)
	if err != nil {
		t.Fatalf("rebuild after update: %v", err)
	}
	if root2 != want2 {
		t.Fatalf("incremental root = %x, rebuild = %x", root2, want2)
	}

	if err := DeleteStateKVLatest(inc, owner, 0, kvdomains.ContractStorage, slotKey); err != nil {
		t.Fatal(err)
	}
	root3, err := UpdateLatestDomainCommitment(inc, []StateCommitmentUpdate{
		NewStateCommitmentDelete(kvKey),
	})
	if err != nil {
		t.Fatalf("incremental delete: %v", err)
	}
	rebuildDeleted := ethrawdb.NewMemoryDatabase()
	if err := WriteStateKVGeneration(rebuildDeleted, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateAccountLatest(rebuildDeleted, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	want3, err := RebuildLatestDomainCommitment(rebuildDeleted)
	if err != nil {
		t.Fatalf("rebuild after delete: %v", err)
	}
	if root3 != want3 {
		t.Fatalf("incremental delete root = %x, rebuild = %x", root3, want3)
	}
}

func TestLatestDomainCommitmentRootRestoresFromNodes(t *testing.T) {
	owner := common.Address{0x41, 0x12}
	slotKey := []byte("slot")

	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateAccountLatest(db, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVGeneration(db, owner, 2); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, db, owner, 2, kvdomains.ContractStorage, slotKey, []byte("value"))
	want, err := RebuildLatestDomainCommitment(db)
	if err != nil {
		t.Fatalf("rebuild commitment: %v", err)
	}
	if err := DeleteStateCommitmentDomain(db, LatestDomainCommitmentRootLogicalKey()); err != nil {
		t.Fatalf("delete root row: %v", err)
	}
	if _, ok, err := ReadLatestDomainCommitmentRoot(db); err != nil || ok {
		t.Fatalf("root before repair ok=%v err=%v", ok, err)
	}

	got, ok, err := RestoreLatestDomainCommitmentRootFromNodes(db)
	if err != nil || !ok {
		t.Fatalf("restore root from nodes ok=%v err=%v", ok, err)
	}
	if got != want {
		t.Fatalf("restored root = %x, want %x", got, want)
	}
	if stored, ok, err := ReadLatestDomainCommitmentRoot(db); err != nil || !ok || stored != want {
		t.Fatalf("stored restored root = %x ok=%v err=%v, want %x", stored, ok, err, want)
	}
}

func TestLatestDomainCommitmentRootNodePresent(t *testing.T) {
	owner := common.Address{0x41, 0x13}
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateKVLatest(db, owner, 0, kvdomains.SystemReward, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	root, err := RebuildLatestDomainCommitment(db)
	if err != nil {
		t.Fatalf("rebuild commitment: %v", err)
	}
	if ok, err := LatestDomainCommitmentRootNodePresent(db, root); err != nil || !ok {
		t.Fatalf("root node present ok=%v err=%v, want true,nil", ok, err)
	}
	if ok, err := LatestDomainCommitmentRootNodePresent(db, common.Hash{0xff}); err != nil || ok {
		t.Fatalf("wrong root node present ok=%v err=%v, want false,nil", ok, err)
	}
	if err := clearLatestDomainCommitmentNodes(db); err != nil {
		t.Fatalf("clear commitment nodes: %v", err)
	}
	if ok, err := LatestDomainCommitmentRootNodePresent(db, root); err != nil || ok {
		t.Fatalf("root node after prune ok=%v err=%v, want false,nil", ok, err)
	}
	if ok, err := LatestDomainCommitmentRootNodePresent(db, common.Hash{}); err != nil || !ok {
		t.Fatalf("zero root node present ok=%v err=%v, want true,nil", ok, err)
	}
}

func TestLatestDomainCommitmentBootstrapsFromUpdates(t *testing.T) {
	owner := common.Address{0x41, 0x21}
	slotKey := []byte("slot")

	inc := ethrawdb.NewMemoryDatabase()
	if err := WriteStateAccountLatest(inc, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVGeneration(inc, owner, 2); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, inc, owner, 2, kvdomains.ContractStorage, slotKey, []byte("value"))
	if _, ok, err := ReadLatestDomainCommitmentRoot(inc); err != nil || ok {
		t.Fatalf("pre-root ok=%v err=%v", ok, err)
	}
	root, err := UpdateLatestDomainCommitment(inc, []StateCommitmentUpdate{
		NewStateCommitmentPut(StateAccountLatestCommitmentKey(owner), []byte("account-v1")),
		NewStateCommitmentPut(StateKVGenerationCommitmentKey(owner), EncodeStateKVGenerationValue(2)),
		NewStateCommitmentPut(StateKVLatestCommitmentKey(owner, 2, kvdomains.ContractStorage, slotKey), EncodeStateKVLatestValue([]byte("value"))),
	})
	if err != nil {
		t.Fatalf("bootstrap commitment from updates: %v", err)
	}

	rebuild := ethrawdb.NewMemoryDatabase()
	if err := WriteStateAccountLatest(rebuild, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVGeneration(rebuild, owner, 2); err != nil {
		t.Fatal(err)
	}
	mustWriteStateKVLatest(t, rebuild, owner, 2, kvdomains.ContractStorage, slotKey, []byte("value"))
	want, err := RebuildLatestDomainCommitment(rebuild)
	if err != nil {
		t.Fatalf("rebuild commitment: %v", err)
	}
	if root != want {
		t.Fatalf("bootstrap root = %x, rebuild = %x", root, want)
	}
}

func TestLatestDomainCommitmentCoalescesDuplicateUpdates(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x22}
	key := StateKVLatestCommitmentKey(owner, 0, kvdomains.SystemReward, []byte("cycle"))

	updates := CoalesceStateCommitmentUpdates([]StateCommitmentUpdate{
		NewStateCommitmentPut(key, EncodeStateKVLatestValue([]byte("old"))),
		NewStateCommitmentDelete(key),
		NewStateCommitmentPut(key, EncodeStateKVLatestValue([]byte("new"))),
	})
	if len(updates) != 1 || updates[0].Delete || string(updates[0].Value) != string(EncodeStateKVLatestValue([]byte("new"))) {
		t.Fatalf("coalesced updates = %+v", updates)
	}

	if err := WriteStateKVLatest(db, owner, 0, kvdomains.SystemReward, []byte("cycle"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	root, err := UpdateLatestDomainCommitment(db, []StateCommitmentUpdate{
		NewStateCommitmentPut(key, EncodeStateKVLatestValue([]byte("old"))),
		NewStateCommitmentDelete(key),
		NewStateCommitmentPut(key, EncodeStateKVLatestValue([]byte("new"))),
	})
	if err != nil {
		t.Fatalf("incremental duplicate updates: %v", err)
	}
	want, err := RebuildLatestDomainCommitment(db)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if root != want {
		t.Fatalf("incremental duplicate root = %x, rebuild = %x", root, want)
	}
}
