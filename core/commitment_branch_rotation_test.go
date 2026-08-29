package core

import (
	"errors"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/params"
)

type commitmentRotationBeginResult struct {
	rotation rawdb.CommitmentBranchRotation
	active   bool
	err      error
}

func assertCommitmentRotationBlocked(t *testing.T, done <-chan struct{}, phase string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s crossed a live InsertSession", phase)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestCommitmentBranchRotationWaitsForInsertSessionLifecycle is the regression
// for a production sync failure where Begin rotated between two calls on one
// BeginSyncInsertSession. chainmu was free at that point, but the session still
// owned a StateDB/CommitScope whose target buffer layer was detached by
// Discard. The next batch then failed with "batch target layer is no longer
// pending". Both marker transitions must wait for the complete session
// lifetime, including at every supported async commit depth.
func TestCommitmentBranchRotationWaitsForInsertSessionLifecycle(t *testing.T) {
	witness := testInsertAddr(0xc2)
	blocks, wantRoots := buildSyncBlockSequence(t, witness, 3)

	for _, tc := range []struct {
		name  string
		async bool
		depth string
	}{
		{name: "async_off"},
		{name: "async_depth_2", async: true, depth: "2"},
		{name: "async_depth_4", async: true, depth: "4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.async {
				t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", tc.depth)
			}
			db := ethrawdb.NewMemoryDatabase()
			bc := newAsyncFlushChainOn(t, db, witness)
			if tc.async {
				bc.SetAsyncCommit(true)
			}

			// Hold the session read side across a fully applied block and the
			// chainmu gap before Finish. Begin must wait at the writer gate; in
			// particular it must not grab chainmu and deadlock Finish.
			session := bc.BeginSyncInsertSession()
			if err := session.Insert(blocks[:1]); err != nil {
				t.Fatalf("first session insert: %v", err)
			}
			// The rotation boundary resolver consumes this recoverable stage row.
			// Write it before releasing the session gate so the waiting Begin sees
			// the exact end-of-block tx boundary after Finish flushes the scope.
			if err := rawdb.WriteStateTxRange(db, blocks[0].Number(), blocks[0].Hash(), 1, 1); err != nil {
				t.Fatalf("write block 1 tx range: %v", err)
			}

			beginStarted := make(chan struct{})
			beginDone := make(chan struct{})
			beginResult := make(chan commitmentRotationBeginResult, 1)
			go func() {
				close(beginStarted)
				rotation, active, err := bc.BeginCommitmentBranchRotation()
				beginResult <- commitmentRotationBeginResult{rotation: rotation, active: active, err: err}
				close(beginDone)
			}()
			<-beginStarted
			assertCommitmentRotationBlocked(t, beginDone, "BeginCommitmentBranchRotation")
			if !bc.chainmu.TryLock() {
				t.Fatal("BeginCommitmentBranchRotation waited while holding chainmu")
			}
			bc.chainmu.Unlock()

			if err := session.Finish(); err != nil {
				t.Fatalf("first session finish: %v", err)
			}
			// Finish owns exactly one RUnlock and returns the saved result on
			// every later call. An Insert after that boundary is rejected.
			if err := session.Finish(); err != nil {
				t.Fatalf("idempotent session finish: %v", err)
			}
			if err := session.Insert(nil); !errors.Is(err, ErrInsertSessionFinished) {
				t.Fatalf("insert after Finish = %v, want %v", err, ErrInsertSessionFinished)
			}

			var begin commitmentRotationBeginResult
			select {
			case begin = <-beginResult:
			case <-time.After(10 * time.Second):
				t.Fatal("BeginCommitmentBranchRotation deadlocked after Finish")
			}
			if begin.err != nil || !begin.active {
				t.Fatalf("begin rotation active=%v err=%v", begin.active, begin.err)
			}
			if begin.rotation.BlockNum != blocks[0].Number() || begin.rotation.BlockHash != blocks[0].Hash() {
				t.Fatalf("rotation boundary = %d/%x, want %d/%x", begin.rotation.BlockNum, begin.rotation.BlockHash, blocks[0].Number(), blocks[0].Hash())
			}
			if got := rawdb.ReadBlockStateRoot(bc.chaindb, blocks[0].Hash()); got != wantRoots[0] {
				t.Fatalf("block 1 root after Begin = %x, want %x", got, wantRoots[0])
			}

			dir := t.TempDir()
			result, err := snapshots.NewAggregator(dir).BuildLatest(db, snapshots.AggregatorBuildOptions{
				FromTxNum: 1, ToTxNum: begin.rotation.SnapshotTxNum,
			})
			if err != nil {
				t.Fatalf("build rotation snapshot: %v", err)
			}
			mgr, err := snapshots.OpenPinnedManager(dir, result.Manifest)
			if err != nil {
				t.Fatalf("open rotation snapshot: %v", err)
			}

			// Complete has the same routing swap + Discard hazard. Exercise a
			// live delta write and prove completion waits for its session scope.
			session = bc.BeginSyncInsertSession()
			if err := session.Insert(blocks[1:2]); err != nil {
				t.Fatalf("second session insert: %v", err)
			}
			completeStarted := make(chan struct{})
			completeDone := make(chan struct{})
			completeResult := make(chan error, 1)
			go func() {
				close(completeStarted)
				completeResult <- bc.CompleteCommitmentBranchRotation(begin.rotation, mgr)
				close(completeDone)
			}()
			<-completeStarted
			assertCommitmentRotationBlocked(t, completeDone, "CompleteCommitmentBranchRotation")
			if !bc.chainmu.TryLock() {
				t.Fatal("CompleteCommitmentBranchRotation waited while holding chainmu")
			}
			bc.chainmu.Unlock()
			if err := session.Finish(); err != nil {
				t.Fatalf("second session finish: %v", err)
			}
			select {
			case err := <-completeResult:
				if err != nil {
					t.Fatalf("complete rotation: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("CompleteCommitmentBranchRotation deadlocked after Finish")
			}
			if got := rawdb.ReadBlockStateRoot(bc.chaindb, blocks[1].Hash()); got != wantRoots[1] {
				t.Fatalf("block 2 root after Complete = %x, want %x", got, wantRoots[1])
			}
			rootBeforeRestart, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
			if err != nil || !ok {
				t.Fatalf("latest root before restart ok=%v err=%v", ok, err)
			}
			if err := bc.Close(); err != nil {
				t.Fatalf("close before restart: %v", err)
			}

			bc, err = NewBlockChain(db, state.NewDatabase(db), params.MainnetChainConfig)
			if err != nil {
				t.Fatalf("restart after completed rotation: %v", err)
			}
			defer bc.Close()
			bc.SetStateCommitmentColdHistory(mgr)
			if got := bc.CurrentBlock().Hash(); got != blocks[1].Hash() {
				t.Fatalf("restart head = %x, want %x", got, blocks[1].Hash())
			}
			rootAfterRestart, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
			if err != nil || !ok || rootAfterRestart != rootBeforeRestart {
				t.Fatalf("latest root after restart = %x ok=%v err=%v, want %x", rootAfterRestart, ok, err, rootBeforeRestart)
			}
			if err := bc.InsertBlock(blocks[2]); err != nil {
				t.Fatalf("post-restart block insert: %v", err)
			}
			bc.WaitForCommitSettled()
			bc.WaitForFlushSettled()
			if got := rawdb.ReadBlockStateRoot(bc.chaindb, blocks[2].Hash()); got != wantRoots[2] {
				t.Fatalf("post-restart block 3 root = %x, want %x", got, wantRoots[2])
			}
		})
	}
}

func TestBlockChainCloseWaitsForInsertSessionLifecycle(t *testing.T) {
	witness := testInsertAddr(0xc3)
	bc := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witness)
	session := bc.BeginSyncInsertSession()

	closeStarted := make(chan struct{})
	closeDone := make(chan struct{})
	closeResult := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeResult <- bc.Close()
		close(closeDone)
	}()
	<-closeStarted
	assertCommitmentRotationBlocked(t, closeDone, "BlockChain.Close")
	if !bc.chainmu.TryLock() {
		t.Fatal("BlockChain.Close waited for InsertSession while holding chainmu")
	}
	bc.chainmu.Unlock()
	if err := session.Finish(); err != nil {
		t.Fatalf("session finish before close: %v", err)
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("close after session finish: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("BlockChain.Close deadlocked after Finish")
	}
}

func TestCommitmentBranchRotationKeepsLiveDeltaAcrossSnapshotPublish(t *testing.T) {
	witness := testInsertAddr(0xc1)
	db := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: witness, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witness, VoteCount: 1, URL: "test"},
		},
		DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1},
	}
	if _, _, err := SetupGenesisBlock(db, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(db, state.NewDatabase(db), params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bc.Close() }()

	block1 := buildTestBlock(bc, witness, 3_000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if err := rawdb.WriteStateTxRange(db, block1.Number(), block1.Hash(), 1, 1); err != nil {
		t.Fatal(err)
	}
	rotation, rotating, err := bc.BeginCommitmentBranchRotation()
	if err != nil || !rotating {
		t.Fatalf("begin rotation rotating=%v err=%v", rotating, err)
	}
	if rotation.BlockNum != block1.Number() || rotation.BlockHash != block1.Hash() {
		t.Fatalf("rotation boundary = %d/%x, want %d/%x", rotation.BlockNum, rotation.BlockHash, block1.Number(), block1.Hash())
	}
	// Restart in the first crash window: the durable rotation marker must be
	// sufficient to recover delta->legacy reads without any snapshot files.
	if err := bc.Close(); err != nil {
		t.Fatal(err)
	}
	bc, err = NewBlockChain(db, state.NewDatabase(db), params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("restart during rotation: %v", err)
	}
	if resumed, ok, err := rawdb.ReadCommitmentBranchRotation(db); err != nil || !ok || resumed != rotation {
		t.Fatalf("resumed rotation = %+v ok=%v err=%v", resumed, ok, err)
	}

	// Import continues while the immutable builder streams the frozen legacy
	// table. Empty blocks still update witness/account state and therefore the
	// staged commitment, exercising real delta writes.
	block2 := buildTestBlock(bc, witness, 6_000)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatal(err)
	}
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	liveRoot, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok {
		t.Fatalf("read live root ok=%v err=%v", ok, err)
	}
	if liveRoot == rotation.Root {
		t.Fatal("post-rotation block did not advance commitment root")
	}

	dir := t.TempDir()
	result, err := snapshots.NewAggregator(dir).BuildLatest(db, snapshots.AggregatorBuildOptions{
		FromTxNum: 1, ToTxNum: rotation.SnapshotTxNum,
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := snapshots.OpenPinnedManager(dir, result.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := bc.CompleteCommitmentBranchRotation(rotation, mgr); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := rawdb.ReadCommitmentBranchRotation(db); err != nil || ok {
		t.Fatalf("rotation marker after publish ok=%v err=%v", ok, err)
	}
	base, ok, err := rawdb.ReadCommitmentBranchBase(db)
	if err != nil || !ok || base.Generation != rotation.Generation || base.Root != rotation.Root {
		t.Fatalf("base after publish = %+v ok=%v err=%v", base, ok, err)
	}
	legacyRows := 0
	if err := rawdb.IterateCommitmentBranches(db, func(_, _ []byte) (bool, error) {
		legacyRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 0 {
		t.Fatalf("legacy rows after publish = %d", legacyRows)
	}
	delta, err := rawdb.NewCommitmentBranchDeltaKeyspace(rotation.Generation)
	if err != nil {
		t.Fatal(err)
	}
	deltaRows := 0
	if err := delta.Iterate(db, func(_, _ []byte) (bool, error) {
		deltaRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if deltaRows == 0 {
		t.Fatal("live updates produced no branch delta rows")
	}
	// Model a crash after the base marker became durable but before legacy
	// cleanup finished. Even a conflicting leftover legacy root must be ignored.
	var staleLegacyRoot statedomains.BranchData
	staleLegacyRoot.SetHashChild(0, rotation.Root)
	if err := rawdb.WriteCommitmentBranch(db, nil, staleLegacyRoot.Encode()); err != nil {
		t.Fatal(err)
	}

	latest, err := statedomains.NewStagedCommitmentStoreWithRepair(db, statedomains.CommitmentSnapshotRepair{Source: mgr}, false)
	if err != nil {
		t.Fatal(err)
	}
	present, err := latest.RootNodePresent(liveRoot)
	if err != nil || !present {
		t.Fatalf("published snapshot+delta root present=%v err=%v", present, err)
	}
	if err := statedomains.CloseLatestCommitmentStore(latest); err != nil {
		t.Fatal(err)
	}
	bc.SetStateCommitmentColdHistory(mgr)
	if err := rawdb.WriteStateTxRange(db, block2.Number(), block2.Hash(), 2, 2); err != nil {
		t.Fatal(err)
	}
	rotation2, active, err := bc.BeginCommitmentBranchRotation()
	if err != nil || !active {
		t.Fatalf("begin base+delta merge active=%v err=%v", active, err)
	}
	if rotation2.Generation != rotation.Generation+1 || rotation2.Root != liveRoot {
		t.Fatalf("second rotation = %+v, prior generation=%d liveRoot=%x", rotation2, rotation.Generation, liveRoot)
	}
	if resumed, active, err := bc.BeginCommitmentBranchRotation(); err != nil || !active || resumed != rotation2 {
		t.Fatalf("resume base+delta merge = %+v active=%v err=%v, want %+v", resumed, active, err, rotation2)
	}
	legacyRows = 0
	if err := rawdb.IterateCommitmentBranches(db, func(_, _ []byte) (bool, error) {
		legacyRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 0 {
		t.Fatalf("resumed base cleanup left %d legacy rows", legacyRows)
	}

	block3 := buildTestBlock(bc, witness, 9_000)
	if err := bc.InsertBlock(block3); err != nil {
		t.Fatal(err)
	}
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	liveRoot2, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok || liveRoot2 == rotation2.Root {
		t.Fatalf("read second live root = %x ok=%v err=%v", liveRoot2, ok, err)
	}
	result2, err := snapshots.NewAggregator(dir).BuildLatest(db, snapshots.AggregatorBuildOptions{
		FromTxNum: 1, ToTxNum: rotation2.SnapshotTxNum,
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr2, err := snapshots.OpenPinnedManager(dir, result2.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := bc.CompleteCommitmentBranchRotation(rotation2, mgr2); err != nil {
		t.Fatal(err)
	}
	base, ok, err = rawdb.ReadCommitmentBranchBase(db)
	if err != nil || !ok || base.Generation != rotation2.Generation || base.Root != rotation2.Root {
		t.Fatalf("second base after publish = %+v ok=%v err=%v", base, ok, err)
	}
	oldDeltaRows := 0
	if err := delta.Iterate(db, func(_, _ []byte) (bool, error) {
		oldDeltaRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if oldDeltaRows != 0 {
		t.Fatalf("covered generation retained %d rows", oldDeltaRows)
	}
	newDelta, err := rawdb.NewCommitmentBranchDeltaKeyspace(rotation2.Generation)
	if err != nil {
		t.Fatal(err)
	}
	newDeltaRows := 0
	if err := newDelta.Iterate(db, func(_, _ []byte) (bool, error) {
		newDeltaRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if newDeltaRows == 0 {
		t.Fatal("second rotation retained no live delta rows")
	}
	latest, err = statedomains.NewStagedCommitmentStoreWithRepair(db, statedomains.CommitmentSnapshotRepair{Source: mgr2}, false)
	if err != nil {
		t.Fatal(err)
	}
	present, err = latest.RootNodePresent(liveRoot2)
	if err != nil || !present {
		t.Fatalf("second snapshot+delta root present=%v err=%v", present, err)
	}
	if err := statedomains.CloseLatestCommitmentStore(latest); err != nil {
		t.Fatal(err)
	}
}
