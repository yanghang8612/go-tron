package domains

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestOrderedCommitmentPipelineMatchesSequentialAcrossInflightBlocks(t *testing.T) {
	seed := buildRandomPuts(rand.New(rand.NewSource(8181)), 2_000)
	referenceDB := rawdb.NewMemoryDatabase()
	pipelineDB := rawdb.NewMemoryDatabase()
	for _, db := range []CommitmentDB{referenceDB, pipelineDB} {
		if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), seed); err != nil {
			t.Fatalf("seed commitment: %v", err)
		}
	}

	buf := blockbuffer.New(pipelineDB)
	buf.SetMaxInflight(4)
	pipeline, err := NewOrderedCommitmentPipeline(buf)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()

	type pendingBlock struct {
		handle blockbuffer.InflightHandle
		result <-chan OrderedCommitmentResult
		want   common.Hash
	}
	pending := make([]pendingBlock, 0, 4)
	for blockNum := uint64(1); blockNum <= 4; blockNum++ {
		updates := make([]rawdb.StateCommitmentUpdate, 0, 129)
		for i := 0; i < 128; i++ {
			seedIndex := (int(blockNum)*197 + i*31) % len(seed)
			value := []byte{byte(blockNum), byte(i), byte(i >> 8)}
			updates = append(updates, rawdb.NewStateCommitmentPut(seed[seedIndex].Key, value))
		}
		if blockNum == 2 {
			updates = append(updates, rawdb.NewStateCommitmentDelete(seed[17].Key))
		}
		want, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(referenceDB), updates)
		if err != nil {
			t.Fatalf("reference block %d: %v", blockNum, err)
		}

		buf.BeginBlock(common.Hash{byte(blockNum)}, blockNum)
		handle, ok := buf.NewestInflight()
		if !ok {
			t.Fatalf("block %d has no inflight layer", blockNum)
		}
		view := buf.ViewLayer(handle)
		pending = append(pending, pendingBlock{
			handle: handle,
			result: pipeline.Submit(view, updates),
			want:   want,
		})
	}

	for i, block := range pending {
		result := <-block.result
		if result.Err != nil {
			t.Fatalf("pipeline block %d: %v", i+1, result.Err)
		}
		if result.Root != block.want {
			t.Fatalf("pipeline block %d root = %x, want %x", i+1, result.Root, block.want)
		}
		if err := buf.CommitInflight(block.handle); err != nil {
			t.Fatalf("commit block %d: %v", i+1, err)
		}
	}

	gotRoot, ok, err := rawdb.ReadLatestDomainCommitmentRoot(buf)
	if err != nil || !ok || gotRoot != pending[len(pending)-1].want {
		t.Fatalf("latest root = %x ok=%v err=%v, want %x", gotRoot, ok, err, pending[len(pending)-1].want)
	}
	wantRows := collectCommitmentRows(t, referenceDB)
	gotRows := collectCommitmentRows(t, buf)
	if len(gotRows) != len(wantRows) {
		t.Fatalf("branch rows = %d, want %d", len(gotRows), len(wantRows))
	}
	for key, want := range wantRows {
		if got := gotRows[key]; !bytes.Equal(got, want) {
			t.Fatalf("branch %x = %x, want %x", key, got, want)
		}
	}
}

func TestOrderedCommitmentPipelineEmptySingletonNoopAndDelete(t *testing.T) {
	referenceDB := rawdb.NewMemoryDatabase()
	pipelineDB := rawdb.NewMemoryDatabase()
	buf := blockbuffer.New(pipelineDB)
	buf.SetMaxInflight(4)
	pipeline, err := NewOrderedCommitmentPipeline(buf)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()

	blocks := [][]rawdb.StateCommitmentUpdate{
		{rawdb.NewStateCommitmentPut([]byte("singleton"), []byte("one"))},
		nil,
		{rawdb.NewStateCommitmentPut([]byte("singleton"), []byte("two"))},
		{rawdb.NewStateCommitmentDelete([]byte("singleton"))},
	}
	type pendingBlock struct {
		handle blockbuffer.InflightHandle
		result <-chan OrderedCommitmentResult
		want   common.Hash
	}
	pending := make([]pendingBlock, 0, len(blocks))
	for i, updates := range blocks {
		want, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(referenceDB), updates)
		if err != nil {
			t.Fatalf("reference block %d: %v", i+1, err)
		}
		number := uint64(i + 1)
		buf.BeginBlock(common.Hash{byte(number)}, number)
		handle, ok := buf.NewestInflight()
		if !ok {
			t.Fatalf("block %d has no inflight layer", number)
		}
		pending = append(pending, pendingBlock{
			handle: handle,
			result: pipeline.Submit(buf.ViewLayer(handle), updates),
			want:   want,
		})
	}

	for i, block := range pending {
		result := <-block.result
		if result.Err != nil {
			t.Fatalf("pipeline block %d: %v", i+1, result.Err)
		}
		if result.Root != block.want {
			t.Fatalf("pipeline block %d root = %x, want %x", i+1, result.Root, block.want)
		}
		if err := buf.CommitInflight(block.handle); err != nil {
			t.Fatalf("commit block %d: %v", i+1, err)
		}
	}
	if root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(buf); err != nil || !ok || root != (common.Hash{}) {
		t.Fatalf("empty latest root = %x ok=%v err=%v", root, ok, err)
	}
	if rows := collectCommitmentRows(t, buf); len(rows) != 0 {
		t.Fatalf("empty trie retained %d branch rows", len(rows))
	}
}

func TestOrderedCommitmentPipelineRejectsMismatchedSeedRoot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), buildRandomPuts(rand.New(rand.NewSource(9191)), 32)); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, common.Hash{0xff}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewOrderedCommitmentPipeline(db); err == nil {
		t.Fatal("NewOrderedCommitmentPipeline accepted mismatched root")
	}
}

func TestOrderedCommitmentPipelineRequiresConcurrentLayer(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), buildRandomPuts(rand.New(rand.NewSource(9292)), 32)); err != nil {
		t.Fatal(err)
	}
	buf := blockbuffer.New(db)
	pipeline, err := NewOrderedCommitmentPipeline(buf)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()
	result := <-pipeline.Submit(db, []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut([]byte("key"), []byte("value")),
	})
	if result.Err == nil {
		t.Fatal("Submit accepted a non-concurrent store")
	}
}

func TestOrderedCommitmentPipelineUsesImmutableBaseDelta(t *testing.T) {
	const (
		txNum      = uint64(500)
		generation = uint64(11)
	)
	seed := buildRandomPuts(rand.New(rand.NewSource(9393)), 512)
	referenceDB := rawdb.NewMemoryDatabase()
	pipelineDB := rawdb.NewMemoryDatabase()
	for _, db := range []CommitmentDB{referenceDB, pipelineDB} {
		if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), seed); err != nil {
			t.Fatalf("seed commitment: %v", err)
		}
	}
	baseRoot, ok, err := rawdb.ReadLatestDomainCommitmentRoot(pipelineDB)
	if err != nil || !ok {
		t.Fatalf("read base root ok=%v err=%v", ok, err)
	}
	mgr := buildManagerWithBranchSnapshot(t, pipelineDB, t.TempDir(), txNum)
	if err := rawdb.WriteCommitmentBranchBase(pipelineDB, rawdb.CommitmentBranchBase{
		Generation: generation, SnapshotTxNum: txNum, Root: baseRoot,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.DeleteCommitmentBranches(pipelineDB); err != nil {
		t.Fatal(err)
	}

	buf := blockbuffer.New(pipelineDB)
	buf.SetMaxInflight(4)
	repair := CommitmentSnapshotRepair{Source: mgr, TxNum: txNum}
	pipeline, err := NewOrderedCommitmentPipelineWithRepair(buf, repair)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()

	type pendingBlock struct {
		handle blockbuffer.InflightHandle
		result <-chan OrderedCommitmentResult
		want   common.Hash
	}
	pending := make([]pendingBlock, 0, 3)
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		updates := make([]rawdb.StateCommitmentUpdate, 0, 65)
		for i := 0; i < 64; i++ {
			seedIndex := (int(blockNum)*83 + i*29) % len(seed)
			updates = append(updates, rawdb.NewStateCommitmentPut(
				seed[seedIndex].Key,
				[]byte{byte(blockNum), byte(i), 0xa5},
			))
		}
		if blockNum == 2 {
			updates = append(updates, rawdb.NewStateCommitmentDelete(seed[7].Key))
		}
		want, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(referenceDB), updates)
		if err != nil {
			t.Fatalf("reference block %d: %v", blockNum, err)
		}
		buf.BeginBlock(common.Hash{byte(blockNum)}, blockNum)
		handle, ok := buf.NewestInflight()
		if !ok {
			t.Fatalf("block %d has no inflight layer", blockNum)
		}
		pending = append(pending, pendingBlock{
			handle: handle,
			result: pipeline.Submit(buf.ViewLayer(handle), updates),
			want:   want,
		})
	}
	for i, block := range pending {
		result := <-block.result
		if result.Err != nil {
			t.Fatalf("pipeline block %d: %v", i+1, result.Err)
		}
		if result.Root != block.want {
			t.Fatalf("pipeline block %d root = %x, want %x", i+1, result.Root, block.want)
		}
		if err := buf.CommitInflight(block.handle); err != nil {
			t.Fatalf("commit block %d: %v", i+1, err)
		}
	}
	if legacyRows := len(collectCommitmentRows(t, buf)); legacyRows != 0 {
		t.Fatalf("ordered pipeline repopulated %d legacy rows", legacyRows)
	}
	delta, err := rawdb.NewCommitmentBranchDeltaKeyspace(generation)
	if err != nil {
		t.Fatal(err)
	}
	deltaRows := 0
	if err := delta.Iterate(buf, func(_, _ []byte) (bool, error) {
		deltaRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if deltaRows == 0 {
		t.Fatal("ordered pipeline wrote no delta rows")
	}
}

func TestOrderedCommitmentPipelineUsesNewDeltaOverFrozenDeltaAndBase(t *testing.T) {
	const (
		baseTx      = uint64(700)
		baseGen     = uint64(31)
		rotationTx  = uint64(800)
		rotationGen = baseGen + 1
	)
	seed := buildRandomPuts(rand.New(rand.NewSource(9494)), 512)
	referenceDB := rawdb.NewMemoryDatabase()
	rotationDB := rawdb.NewMemoryDatabase()
	for _, db := range []CommitmentDB{referenceDB, rotationDB} {
		if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), seed); err != nil {
			t.Fatal(err)
		}
	}
	baseRoot, _, _ := rawdb.ReadLatestDomainCommitmentRoot(rotationDB)
	mgr := buildManagerWithBranchSnapshot(t, rotationDB, t.TempDir(), baseTx)
	if err := rawdb.WriteCommitmentBranchBase(rotationDB, rawdb.CommitmentBranchBase{
		Generation: baseGen, SnapshotTxNum: baseTx, Root: baseRoot,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.DeleteCommitmentBranches(rotationDB); err != nil {
		t.Fatal(err)
	}
	first := []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentDelete(seed[7].Key),
		rawdb.NewStateCommitmentPut(seed[19].Key, []byte("frozen-delta-update")),
	}
	wantRoot, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(referenceDB), first)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStagedCommitmentStoreWithRepair(rotationDB, CommitmentSnapshotRepair{Source: mgr}, false)
	if err != nil {
		t.Fatal(err)
	}
	gotRoot, err := ApplyLatestCommitmentWithStore(store, first)
	if closeErr := CloseLatestCommitmentStore(store); err == nil {
		err = closeErr
	}
	if err != nil || gotRoot != wantRoot {
		t.Fatalf("frozen generation root = %x err=%v, want %x", gotRoot, err, wantRoot)
	}
	if err := rawdb.WriteCommitmentBranchRotation(rotationDB, rawdb.CommitmentBranchRotation{
		Generation: rotationGen, SnapshotTxNum: rotationTx, Root: gotRoot,
		BlockNum: 80, BlockHash: common.Hash{0x80},
	}); err != nil {
		t.Fatal(err)
	}

	buf := blockbuffer.New(rotationDB)
	buf.SetMaxInflight(4)
	pipeline, err := NewOrderedCommitmentPipelineWithRepair(buf, CommitmentSnapshotRepair{Source: mgr})
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()
	second := []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(seed[7].Key, []byte("reinsert-after-frozen-tombstone")),
		rawdb.NewStateCommitmentPut(seed[29].Key, []byte("new-generation-update")),
	}
	wantRoot, err = ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(referenceDB), second)
	if err != nil {
		t.Fatal(err)
	}
	buf.BeginBlock(common.Hash{0x81}, 81)
	handle, _ := buf.NewestInflight()
	result := <-pipeline.Submit(buf.ViewLayer(handle), second)
	if result.Err != nil || result.Root != wantRoot {
		t.Fatalf("rotating pipeline root = %x err=%v, want %x", result.Root, result.Err, wantRoot)
	}
	if err := buf.CommitInflight(handle); err != nil {
		t.Fatal(err)
	}
	newDelta, err := rawdb.NewCommitmentBranchDeltaKeyspace(rotationGen)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	if err := newDelta.Iterate(buf, func(_, _ []byte) (bool, error) {
		rows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if rows == 0 {
		t.Fatal("ordered pipeline wrote no new-generation delta rows")
	}
}

func collectCommitmentRows(t *testing.T, db ethdb.Iteratee) map[string][]byte {
	t.Helper()
	rows := make(map[string][]byte)
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		rows[string(prefix)] = append([]byte(nil), encoded...)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}
