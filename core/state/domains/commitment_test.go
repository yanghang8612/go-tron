package domains

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestApplyLatestCommitmentUsesTypedStoreWhenRootExists(t *testing.T) {
	updateRoot := common.Hash{0x22}
	store := &recordingCommitmentStore{
		root:       common.Hash{0x11},
		rootOK:     true,
		rootNodeOK: true,
		updateRoot: updateRoot,
	}
	updates := []rawdb.StateCommitmentUpdate{rawdb.NewStateCommitmentPut([]byte("k"), []byte("v"))}

	root, err := applyLatestCommitment(store, updates)
	if err != nil {
		t.Fatalf("apply latest commitment: %v", err)
	}
	if root != updateRoot {
		t.Fatalf("root = %x, want update root %x", root, updateRoot)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root", "root-node-present", "update"}) {
		t.Fatalf("calls = %v, want [read-root root-node-present update]", store.calls)
	}
	if len(store.updates) != 1 || string(store.updates[0].Key) != "k" {
		t.Fatalf("updates = %+v", store.updates)
	}
}

func TestApplyLatestCommitmentNoopReturnsRootWithoutBranchRead(t *testing.T) {
	root := common.Hash{0x21}
	store := &recordingCommitmentStore{
		root:   root,
		rootOK: true,
	}

	got, err := applyLatestCommitment(store, nil)
	if err != nil {
		t.Fatalf("apply latest commitment noop: %v", err)
	}
	if got != root {
		t.Fatalf("root = %x, want %x", got, root)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root"}) {
		t.Fatalf("calls = %v, want [read-root]", store.calls)
	}
}

func TestApplyLatestCommitmentRestoresMissingRootThroughTypedStore(t *testing.T) {
	restoredRoot := common.Hash{0x33}
	store := &recordingCommitmentStore{
		restoredRoot: restoredRoot,
		restoredOK:   true,
	}

	root, err := applyLatestCommitment(store, nil)
	if err != nil {
		t.Fatalf("apply latest commitment restore: %v", err)
	}
	if root != restoredRoot {
		t.Fatalf("root = %x, want restored root %x", root, restoredRoot)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root", "restore-root"}) {
		t.Fatalf("calls = %v, want [read-root restore-root]", store.calls)
	}
}

func TestApplyLatestCommitmentRepairsMissingRootFromLatestCheckpoint(t *testing.T) {
	checkpointRoot := common.Hash{0x34}
	store := &recordingCommitmentStore{
		latestCheckpoint: &rawdb.StateCommitmentCheckpoint{BlockNum: 8, Root: checkpointRoot, Scheme: rawdb.LatestDomainCommitmentScheme},
	}

	root, err := applyLatestCommitment(store, nil)
	if err != nil {
		t.Fatalf("apply latest commitment checkpoint repair: %v", err)
	}
	if root != checkpointRoot {
		t.Fatalf("root = %x, want checkpoint root %x", root, checkpointRoot)
	}
	if store.writtenRoot != checkpointRoot {
		t.Fatalf("written root = %x, want %x", store.writtenRoot, checkpointRoot)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root", "restore-root", "read-latest-checkpoint", "write-root"}) {
		t.Fatalf("calls = %v, want [read-root restore-root read-latest-checkpoint write-root]", store.calls)
	}
}

func TestApplyLatestCommitmentRebuildsAndUpdatesThroughTypedStore(t *testing.T) {
	rebuildRoot := common.Hash{0x44}
	updateRoot := common.Hash{0x55}
	store := &recordingCommitmentStore{
		rebuildRoot: rebuildRoot,
		updateRoot:  updateRoot,
	}
	updates := []rawdb.StateCommitmentUpdate{rawdb.NewStateCommitmentDelete([]byte("k"))}

	root, err := applyLatestCommitment(store, updates)
	if err != nil {
		t.Fatalf("apply latest commitment rebuild/update: %v", err)
	}
	if root != updateRoot {
		t.Fatalf("root = %x, want update root %x", root, updateRoot)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root", "restore-root", "rebuild", "update"}) {
		t.Fatalf("calls = %v, want [read-root restore-root rebuild update]", store.calls)
	}
	if len(store.updates) != 1 || string(store.updates[0].Key) != "k" {
		t.Fatalf("updates = %+v", store.updates)
	}

	store = &recordingCommitmentStore{rebuildRoot: rebuildRoot}
	root, err = applyLatestCommitment(store, nil)
	if err != nil {
		t.Fatalf("apply latest commitment rebuild without updates: %v", err)
	}
	if root != rebuildRoot {
		t.Fatalf("root = %x, want rebuild root %x", root, rebuildRoot)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root", "restore-root", "read-latest-checkpoint", "rebuild"}) {
		t.Fatalf("calls = %v, want [read-root restore-root read-latest-checkpoint rebuild]", store.calls)
	}
}

func TestApplyLatestCommitmentRebuildsWhenBranchStatePruned(t *testing.T) {
	rebuildRoot := common.Hash{0x66}
	updateRoot := common.Hash{0x77}
	store := &recordingCommitmentStore{
		root:        common.Hash{0x65},
		rootOK:      true,
		rootNodeOK:  false,
		rebuildRoot: rebuildRoot,
		updateRoot:  updateRoot,
	}

	root, err := applyLatestCommitment(store, []rawdb.StateCommitmentUpdate{rawdb.NewStateCommitmentPut([]byte("k"), []byte("v"))})
	if err != nil {
		t.Fatalf("apply latest commitment branch repair: %v", err)
	}
	if root != updateRoot {
		t.Fatalf("root = %x, want update root %x", root, updateRoot)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root", "root-node-present", "rebuild", "update"}) {
		t.Fatalf("calls = %v, want [read-root root-node-present rebuild update]", store.calls)
	}
}

func TestApplyLatestCommitmentRestoresBranchFromSnapshotBeforeUpdate(t *testing.T) {
	root := common.Hash{0x88}
	updateRoot := common.Hash{0x99}
	store := &recordingCommitmentStore{
		root:             root,
		rootOK:           true,
		rootNodeOK:       false,
		snapshotRestored: true,
		updateRoot:       updateRoot,
	}
	updates := []rawdb.StateCommitmentUpdate{rawdb.NewStateCommitmentPut([]byte("k"), []byte("v"))}

	got, err := applyLatestCommitmentWithRepair(store, updates, CommitmentSnapshotRepair{Source: noopCommitmentSnapshotSource{}, TxNum: 42})
	if err != nil {
		t.Fatalf("apply latest commitment snapshot repair: %v", err)
	}
	if got != updateRoot {
		t.Fatalf("root = %x, want %x", got, updateRoot)
	}
	if store.snapshotTxNum != 42 || store.snapshotExpectedRoot != root {
		t.Fatalf("snapshot repair tx/root = %d/%x, want 42/%x", store.snapshotTxNum, store.snapshotExpectedRoot, root)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root", "root-node-present", "restore-nodes-from-snapshot", "update"}) {
		t.Fatalf("calls = %v, want snapshot repair without rebuild", store.calls)
	}
}

func TestApplyLatestCommitmentUsesCheckpointRootForSnapshotRepair(t *testing.T) {
	root := common.Hash{0xaa}
	updateRoot := common.Hash{0xbb}
	store := &recordingCommitmentStore{
		latestCheckpoint: &rawdb.StateCommitmentCheckpoint{BlockNum: 11, Root: root, Scheme: rawdb.LatestDomainCommitmentScheme},
		snapshotRestored: true,
		updateRoot:       updateRoot,
	}
	updates := []rawdb.StateCommitmentUpdate{rawdb.NewStateCommitmentPut([]byte("k"), []byte("v"))}

	got, err := applyLatestCommitmentWithRepair(store, updates, CommitmentSnapshotRepair{Source: noopCommitmentSnapshotSource{}, TxNum: 110})
	if err != nil {
		t.Fatalf("apply latest commitment checkpoint snapshot repair: %v", err)
	}
	if got != updateRoot {
		t.Fatalf("root = %x, want %x", got, updateRoot)
	}
	if store.writtenRoot != root {
		t.Fatalf("written root = %x, want checkpoint root %x", store.writtenRoot, root)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-root", "restore-root", "read-latest-checkpoint", "restore-nodes-from-snapshot", "write-root", "update"}) {
		t.Fatalf("calls = %v, want checkpoint-root snapshot repair without rebuild", store.calls)
	}
}

func TestApplyLatestCommitmentWithPrunedBranchMatchesRebuild(t *testing.T) {
	owner := common.Address{0x41, 0x42}
	key := []byte("slot")
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(db, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := rawdb.RebuildLatestDomainCommitment(db); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	deleteCommitmentNodes(t, db)

	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	commitmentKey := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, key)
	commitmentValue := rawdb.EncodeStateKVLatestValue([]byte("v2"))
	root, err := ApplyLatestCommitment(db, []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(commitmentKey, commitmentValue),
	})
	if err != nil {
		t.Fatalf("apply latest commitment after branch prune: %v", err)
	}

	wantDB := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(wantDB, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(wantDB, owner, 0, kvdomains.ContractStorage, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	want, err := rawdb.RebuildLatestDomainCommitment(wantDB)
	if err != nil {
		t.Fatalf("rebuild expected commitment: %v", err)
	}
	if root != want {
		t.Fatalf("root after pruned-branch repair = %x, want rebuild %x", root, want)
	}
	if stored, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db); err != nil || !ok || stored != want {
		t.Fatalf("stored root = %x ok=%v err=%v, want %x", stored, ok, err, want)
	}
}

func TestApplyLatestCommitmentRestoresPrunedBranchFromSnapshot(t *testing.T) {
	owner := common.Address{0x41, 0x43}
	key := []byte("slot/snapshot")
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(db, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := rawdb.RebuildLatestDomainCommitment(db); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	dir := t.TempDir()
	rootRef, rootAccessorRef, rootBTreeRef, err := snapshots.BuildCommitmentRootSegmentFilesFromDB(db, dir, 10, 10, "commitment/root-10-10.seg")
	if err != nil {
		t.Fatalf("build commitment root snapshot: %v", err)
	}
	nodeRef, nodeAccessorRef, nodeBTreeRef, err := snapshots.BuildCommitmentNodeSegmentFilesFromDB(db, dir, 10, 10, "commitment/nodes-10-10.seg")
	if err != nil {
		t.Fatalf("build commitment node snapshot: %v", err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 10, []snapshots.SegmentRef{
		rootRef, rootAccessorRef, rootBTreeRef,
		nodeRef, nodeAccessorRef, nodeBTreeRef,
	})); err != nil {
		t.Fatalf("publish commitment snapshots: %v", err)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	deleteCommitmentNodes(t, db)

	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	commitmentKey := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, key)
	commitmentValue := rawdb.EncodeStateKVLatestValue([]byte("v2"))
	root, err := ApplyLatestCommitmentWithSnapshotRepair(db, []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(commitmentKey, commitmentValue),
	}, CommitmentSnapshotRepair{Source: mgr, TxNum: 10})
	if err != nil {
		t.Fatalf("apply latest commitment with snapshot branch repair: %v", err)
	}

	wantDB := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(wantDB, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(wantDB, owner, 0, kvdomains.ContractStorage, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	want, err := rawdb.RebuildLatestDomainCommitment(wantDB)
	if err != nil {
		t.Fatalf("rebuild expected commitment: %v", err)
	}
	if root != want {
		t.Fatalf("root after snapshot branch repair = %x, want rebuild %x", root, want)
	}
	if ok, err := rawdb.LatestDomainCommitmentRootNodePresent(db, root); err != nil || !ok {
		t.Fatalf("restored branch root present = %v err=%v", ok, err)
	}
}

func TestSeekLatestCommitmentUsesTypedStoreCheckpoints(t *testing.T) {
	store := &recordingCommitmentStore{
		checkpoints: []*rawdb.StateCommitmentCheckpoint{
			{BlockNum: 3, Root: common.Hash{0x03}},
			{BlockNum: 5, Root: common.Hash{0x05}},
			{BlockNum: 4, Root: common.Hash{0x04}},
		},
	}

	txNum, blockNum, err := seekLatestCommitment(store, func(blockNum uint64) (uint64, error) {
		return blockNum + 100, nil
	})
	if err != nil {
		t.Fatalf("seek latest commitment: %v", err)
	}
	if txNum != 105 || blockNum != 5 {
		t.Fatalf("seek = txNum %d block %d, want 105/5", txNum, blockNum)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-latest-checkpoint", "iterate-checkpoints"}) {
		t.Fatalf("calls = %v, want [read-latest-checkpoint iterate-checkpoints]", store.calls)
	}
}

func TestSeekLatestCommitmentPrefersLatestCheckpointPointer(t *testing.T) {
	store := &recordingCommitmentStore{
		latestCheckpoint: &rawdb.StateCommitmentCheckpoint{BlockNum: 9, Root: common.Hash{0x09}},
		checkpoints: []*rawdb.StateCommitmentCheckpoint{
			{BlockNum: 3, Root: common.Hash{0x03}},
			{BlockNum: 5, Root: common.Hash{0x05}},
		},
	}

	txNum, blockNum, err := seekLatestCommitment(store, func(blockNum uint64) (uint64, error) {
		return blockNum + 100, nil
	})
	if err != nil {
		t.Fatalf("seek latest commitment: %v", err)
	}
	if txNum != 109 || blockNum != 9 {
		t.Fatalf("seek = txNum %d block %d, want 109/9", txNum, blockNum)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-latest-checkpoint"}) {
		t.Fatalf("calls = %v, want [read-latest-checkpoint]", store.calls)
	}
}

func TestSeekLatestCommitmentFallsBackToRootThroughTypedStore(t *testing.T) {
	store := &recordingCommitmentStore{root: common.Hash{0x77}, rootOK: true}

	txNum, blockNum, err := seekLatestCommitment(store, nil)
	if err != nil {
		t.Fatalf("seek latest commitment root fallback: %v", err)
	}
	if txNum != 0 || blockNum != 0 {
		t.Fatalf("seek fallback = txNum %d block %d, want 0/0", txNum, blockNum)
	}
	if !sameCommitmentCalls(store.calls, []string{"read-latest-checkpoint", "iterate-checkpoints", "read-root"}) {
		t.Fatalf("calls = %v, want [read-latest-checkpoint iterate-checkpoints read-root]", store.calls)
	}
}

type recordingCommitmentStore struct {
	root                 common.Hash
	rootOK               bool
	writtenRoot          common.Hash
	rootNodeOK           bool
	restoredRoot         common.Hash
	restoredOK           bool
	rebuildRoot          common.Hash
	updateRoot           common.Hash
	latestCheckpoint     *rawdb.StateCommitmentCheckpoint
	checkpoints          []*rawdb.StateCommitmentCheckpoint
	updates              []rawdb.StateCommitmentUpdate
	snapshotRestored     bool
	snapshotTxNum        uint64
	snapshotExpectedRoot common.Hash
	calls                []string
}

func (s *recordingCommitmentStore) ReadRoot() (common.Hash, bool, error) {
	s.calls = append(s.calls, "read-root")
	return s.root, s.rootOK, nil
}

func (s *recordingCommitmentStore) WriteRoot(root common.Hash) error {
	s.calls = append(s.calls, "write-root")
	s.writtenRoot = root
	return nil
}

func (s *recordingCommitmentStore) RootNodePresent(root common.Hash) (bool, error) {
	s.calls = append(s.calls, "root-node-present")
	return s.rootNodeOK, nil
}

func (s *recordingCommitmentStore) RestoreRootFromNodes() (common.Hash, bool, error) {
	s.calls = append(s.calls, "restore-root")
	return s.restoredRoot, s.restoredOK, nil
}

func (s *recordingCommitmentStore) RestoreNodesFromSnapshot(source CommitmentSnapshotSource, txNum uint64, expectedRoot common.Hash) (bool, error) {
	s.calls = append(s.calls, "restore-nodes-from-snapshot")
	s.snapshotTxNum = txNum
	s.snapshotExpectedRoot = expectedRoot
	return s.snapshotRestored, nil
}

func (s *recordingCommitmentStore) Rebuild() (common.Hash, error) {
	s.calls = append(s.calls, "rebuild")
	return s.rebuildRoot, nil
}

func (s *recordingCommitmentStore) Update(updates []rawdb.StateCommitmentUpdate) (common.Hash, error) {
	s.calls = append(s.calls, "update")
	s.updates = append([]rawdb.StateCommitmentUpdate(nil), updates...)
	return s.updateRoot, nil
}

func (s *recordingCommitmentStore) ReadLatestCheckpoint() (*rawdb.StateCommitmentCheckpoint, bool, error) {
	s.calls = append(s.calls, "read-latest-checkpoint")
	if s.latestCheckpoint == nil {
		return nil, false, nil
	}
	cp := *s.latestCheckpoint
	return &cp, true, nil
}

func (s *recordingCommitmentStore) IterateCheckpoints(fn func(*rawdb.StateCommitmentCheckpoint) (bool, error)) error {
	s.calls = append(s.calls, "iterate-checkpoints")
	for _, checkpoint := range s.checkpoints {
		cp := *checkpoint
		cont, err := fn(&cp)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func sameCommitmentCalls(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

type noopCommitmentSnapshotSource struct{}

func (noopCommitmentSnapshotSource) GetCommitmentRoot(uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, nil
}

func (noopCommitmentSnapshotSource) IterateCommitmentNodes([]byte, uint64, func(logicalKey, value []byte) (bool, error)) error {
	return nil
}

func deleteCommitmentNodes(t *testing.T, db CommitmentDB) {
	t.Helper()
	var keys [][]byte
	if err := rawdb.IterateStateCommitmentDomain(db, rawdb.LatestDomainCommitmentNodeLogicalPrefix(), func(logicalKey, _ []byte) (bool, error) {
		keys = append(keys, append([]byte(nil), logicalKey...))
		return true, nil
	}); err != nil {
		t.Fatalf("iterate commitment nodes: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("no commitment nodes to delete")
	}
	for _, key := range keys {
		if err := rawdb.DeleteStateCommitmentDomain(db, key); err != nil {
			t.Fatalf("delete commitment node %x: %v", key, err)
		}
	}
	if ok, err := rawdb.LatestDomainCommitmentRootNodePresent(db, common.Hash{0xff}); err != nil {
		t.Fatalf("check deleted root node: %v", err)
	} else if ok {
		t.Fatal("commitment root node still present after delete")
	}
}
