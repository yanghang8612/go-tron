package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Independently rebuild the physical set, without trusting or modifying HEAD.
func physicalMuHashRoot(t *testing.T, db interface{ ethdb.Iteratee }) common.Hash {
	t.Helper()
	acc := muhash3072.NewAccumulator()
	for _, p := range rawdb.StateCommitmentSourcePrefixes() {
		it := db.NewIterator(p, nil)
		for it.Next() {
			element, err := rawdb.EncodeStateCommitmentElement(it.Key(), it.Value())
			if err != nil {
				t.Fatal(err)
			}
			if err = acc.Insert(element); err != nil {
				t.Fatal(err)
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	final, err := acc.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	return common.Hash(final.Digest)
}

func TestMuHashGenesisRejectsUnpublishedResidue(t *testing.T) {
	for _, kind := range []string{"restore-marker", "stray-source", "head", "latest-root", "current-identity"} {
		t.Run(kind, func(t *testing.T) {
			db := ethrawdb.NewMemoryDatabase()
			defer db.Close()
			var err error
			switch kind {
			case "restore-marker":
				err = rawdb.WriteStateCommitmentRestoreMarker(db)
			case "stray-source":
				err = rawdb.WriteStateAccountLatest(db, testInsertAddr(3), []byte("orphan"))
			case "head":
				err = rawdb.WriteStateCommitmentHead(db, rawdb.NewStateCommitmentHead())
			case "latest-root":
				err = rawdb.WriteLatestDomainCommitmentRoot(db, common.Hash{1})
			case "current-identity":
				rawdb.WriteHeadBlockHash(db, common.Hash{1})
			}
			if err != nil {
				t.Fatal(err)
			}
			before := map[string]string{}
			it := db.NewIterator(nil, nil)
			for it.Next() {
				before[string(it.Key())] = string(it.Value())
			}
			it.Release()
			if _, _, err = SetupGenesisBlock(db, &params.Genesis{Config: params.MainnetChainConfig}); err == nil {
				t.Fatal("accepted residual state")
			}
			after := map[string]string{}
			it = db.NewIterator(nil, nil)
			for it.Next() {
				after[string(it.Key())] = string(it.Value())
			}
			it.Release()
			if fmt.Sprint(before) != fmt.Sprint(after) {
				t.Fatal("rejected genesis mutated store")
			}
		})
	}
}

func TestMuHashDeepAsyncDurableCutAndPinnedView(t *testing.T) {
	witness := testInsertAddr(1)
	blocks, roots := buildSyncBlockSequence(t, witness, 20)
	for _, depth := range []int{2, 4, 16} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", fmt.Sprint(depth))
			path := t.TempDir()
			kv, err := rawdb.NewPebbleDB(path, 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			disk := rawdb.WrapKeyValueStore(kv)
			bc := newAsyncFlushChainOn(t, disk, witness)
			bc.SetAsyncCommit(true)
			SetCommitFoldHookForTest(func(uint64) error { time.Sleep(time.Millisecond); return nil })
			defer SetCommitFoldHookForTest(nil)
			if err = bc.InsertBlocks(blocks[:10]); err != nil {
				t.Fatal(err)
			}
			bc.WaitForCommitSettled()
			bc.WaitForFlushSettled()
			pin, head, err := bc.AcquireStateLatestSnapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if head.BlockHash != blocks[9].Hash() || head.Root != roots[9] || physicalMuHashRoot(t, pin) != head.Root {
				t.Fatal("pinned cut mismatch")
			}
			if err = bc.InsertBlocks(blocks[10:]); err != nil {
				t.Fatal(err)
			}
			bc.WaitForCommitSettled()
			bc.WaitForFlushSettled()
			still, present, err := rawdb.ReadStateCommitmentHead(pin)
			if err != nil || !present || still != head {
				t.Fatal("pin advanced")
			}
			if physicalMuHashRoot(t, pin) != head.Root {
				t.Fatal("pinned source advanced")
			}
			if err = pin.Close(); err != nil {
				t.Fatal(err)
			}
			if err = bc.Close(); err != nil {
				t.Fatal(err)
			}
			durable, present, err := rawdb.ReadStateCommitmentHead(kv)
			if err != nil || !present || durable.Root != roots[19] || physicalMuHashRoot(t, kv) != durable.Root {
				t.Fatal("final durable cut mismatch")
			}
			if err = disk.Close(); err != nil {
				t.Fatal(err)
			}
			kv, err = rawdb.NewPebbleDB(path, 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer kv.Close()
			disk = rawdb.WrapKeyValueStore(kv)
			restarted, err := NewBlockChain(disk, state.NewDatabase(disk), params.MainnetChainConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if restarted.CurrentBlock().Hash() != blocks[19].Hash() || restarted.HeadStateRoot() != roots[19] {
				t.Fatal("restart chose wrong source cut")
			}
		})
	}
}

func TestMuHashLateAsyncFailureDoesNotPublishSuffix(t *testing.T) {
	for _, boundary := range []string{"finish", "lookup", "history", "promotion"} {
		t.Run(boundary, func(t *testing.T) {
			t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "16")
			witness := testInsertAddr(1)
			disk := ethrawdb.NewMemoryDatabase()
			defer disk.Close()
			bc := newAsyncFlushChainOn(t, disk, witness)
			cfg := *bc.config
			cfg.HistoryEnabled = true
			bc.config = &cfg
			bc.SetAsyncCommit(true)
			blocks := chainFrom(bc.CurrentBlock(), witness, 12, 0)
			entered, release := make(chan struct{}), make(chan struct{})
			injected := errors.New("late publish failure")
			bc.commitTailTestHook = func(stage string, job *commitJob) error {
				if stage == boundary && job.block.Number() == 4 {
					close(entered)
					<-release
					return injected
				}
				return nil
			}
			var hookMu sync.Mutex
			var hooks []uint64
			bc.AddBlockHook(func(block *types.Block) { hookMu.Lock(); hooks = append(hooks, block.Number()); hookMu.Unlock() })
			done := make(chan error, 1)
			go func() { done <- bc.InsertBlocks(blocks) }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("tail seam not reached")
			}
			deadline := time.Now().Add(5 * time.Second)
			for len(bc.commitQueue) == 0 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if len(bc.commitQueue) == 0 {
				close(release)
				t.Fatal("no speculative suffix queued")
			}
			close(release)
			select {
			case err := <-done:
				if !errors.Is(err, injected) {
					t.Fatalf("insert error=%v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("failed suffix did not drain")
			}
			bc.WaitForCommitSettled()
			bc.WaitForFlushSettled()
			if bc.CurrentBlock().Hash() != blocks[2].Hash() {
				t.Fatal("failed block advanced global head")
			}
			hookMu.Lock()
			if fmt.Sprint(hooks) != "[1 2 3]" {
				t.Fatalf("hooks=%v", hooks)
			}
			hookMu.Unlock()
			if got := bc.cachedDynProps(); got == nil || got.LatestBlockHeaderNumber() != 3 {
				t.Fatal("failed block advanced DP cache")
			}
			if err := bc.buffer.Flush(bc.db); err != nil {
				t.Fatal(err)
			}
			h, ok, err := rawdb.ReadStateCommitmentHead(disk)
			if err != nil || !ok || h.BlockHash != blocks[2].Hash() || physicalMuHashRoot(t, disk) != h.Root {
				t.Fatal("committed prefix source/HEAD mismatch")
			}
			if rawdb.ReadBlockStateRoot(bc.chaindb, blocks[3].Hash()) == (common.Hash{}) {
				t.Fatal("test did not cross early durable metadata write")
			}
			if err := bc.Close(); err == nil {
				t.Fatal("poisoned chain close unexpectedly succeeded")
			}
			restarted, err := NewBlockChain(disk, state.NewDatabase(disk), cfgPtr(cfg))
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if restarted.CurrentBlock().Hash() != blocks[2].Hash() {
				t.Fatal("restart selected ahead metadata")
			}
			failedHash := blocks[3].Hash()
			failedRef := types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{RefBlockBytes: []byte{0, 4}, RefBlockHash: failedHash[8:16]}})
			if err := ValidateTAPOS(failedRef, restarted.buffer); !errors.Is(err, ErrTaposUnknownRef) {
				t.Fatalf("failed block TAPOS survived restart: %v", err)
			}
		})
	}
}
func cfgPtr(c params.ChainConfig) *params.ChainConfig { return &c }

func TestMuHashInterruptedUnwindFailsClosed(t *testing.T) {
	witness := testInsertAddr(1)
	disk := ethrawdb.NewMemoryDatabase()
	defer disk.Close()
	bc := newAsyncFlushChainOn(t, disk, witness)
	cfg := *bc.config
	cfg.HistoryEnabled = true
	bc.config = &cfg
	blocks := chainFrom(bc.CurrentBlock(), witness, 4, 0)
	if err := bc.InsertBlocks(blocks); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("first cleanup after target publication failed")
	bc.unwindCleanupTestHook = func() error { return injected }
	genesis := &params.Genesis{Config: &cfg, Accounts: []params.GenesisAccount{{Address: witness, Balance: 99_000_000_000_000_000}}, Witnesses: []params.GenesisWitness{{Address: witness, VoteCount: 1, URL: "test"}}, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	if err := bc.RestartSyncFromHeight(1, genesis, nil, nil); !errors.Is(err, injected) {
		t.Fatalf("rewind=%v", err)
	}
	pending, err := rawdb.StateCommitmentUnwindCleanupPending(disk)
	if err != nil || !pending {
		t.Fatal("interrupted cleanup missing durable marker")
	}
	h, present, err := rawdb.ReadStateCommitmentHead(disk)
	if err != nil || !present || h.BlockHash != blocks[0].Hash() || physicalMuHashRoot(t, disk) != h.Root {
		t.Fatal("target cut did not publish atomically")
	}
	if err := bc.InsertBlock(chainFrom(blocks[0], witness, 1, 9)[0]); err == nil {
		t.Fatal("poisoned live process accepted alternate fork")
	}
	if _, err := NewBlockChain(disk, state.NewDatabase(disk), &cfg); err == nil {
		t.Fatal("startup accepted unfinished cleanup")
	}
	if err := rawdb.InitializeStateCommitment(disk); err == nil {
		t.Fatal("initializer accepted unfinished cleanup")
	}
	if _, _, err := SetupGenesisBlock(disk, genesis); err == nil {
		t.Fatal("genesis silently repaired unfinished cleanup")
	}
	_ = bc.Close()
}

type muhashAckLossStore struct {
	ethdb.Database
	trip bool
}
type muhashAckLossBatch struct {
	ethdb.Batch
	store  *muhashAckLossStore
	target bool
}

func (s *muhashAckLossStore) NewBatch() ethdb.Batch {
	return &muhashAckLossBatch{Batch: s.Database.NewBatch(), store: s}
}
func (s *muhashAckLossStore) NewBatchWithSize(n int) ethdb.Batch {
	return &muhashAckLossBatch{Batch: s.Database.NewBatchWithSize(n), store: s}
}
func (b *muhashAckLossBatch) Put(k, v []byte) error {
	if rawdb.IsStateCommitmentHeadKey(k) {
		head, err := rawdb.DecodeStateCommitmentHead(v)
		if err != nil {
			return err
		}
		b.target = head.BlockNum == 1
	}
	return b.Batch.Put(k, v)
}
func (b *muhashAckLossBatch) Write() error {
	if err := b.Batch.Write(); err != nil {
		return err
	}
	if b.store.trip && b.target {
		return errors.New("inverse batch acknowledgement lost")
	}
	return nil
}
func TestMuHashUnwindAppliedButUnacknowledgedPoisonsProcess(t *testing.T) {
	witness := testInsertAddr(1)
	disk := &muhashAckLossStore{Database: ethrawdb.NewMemoryDatabase()}
	defer disk.Close()
	bc := newAsyncFlushChainOn(t, disk, witness)
	cfg := *bc.config
	cfg.HistoryEnabled = true
	bc.config = &cfg
	blocks := chainFrom(bc.CurrentBlock(), witness, 3, 0)
	if err := bc.InsertBlocks(blocks); err != nil {
		t.Fatal(err)
	}
	disk.trip = true
	genesis := &params.Genesis{Config: &cfg, Accounts: []params.GenesisAccount{{Address: witness, Balance: 99_000_000_000_000_000}}, Witnesses: []params.GenesisWitness{{Address: witness, VoteCount: 1, URL: "test"}}, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	if err := bc.RestartSyncFromHeight(1, genesis, nil, nil); err == nil {
		t.Fatal("lost ack reported success")
	}
	if bc.commitErr.Load() == nil {
		t.Fatal("uncertain publication did not poison live process")
	}
	head, ok, err := rawdb.ReadStateCommitmentHead(disk)
	if err != nil || !ok || head.BlockHash != blocks[0].Hash() {
		t.Fatal("fixture did not apply inverse cut")
	}
	if pending, err := rawdb.StateCommitmentUnwindCleanupPending(disk); err != nil || !pending {
		t.Fatal("uncertain inverse lacks cleanup marker")
	}
	if _, err := NewBlockChain(disk, state.NewDatabase(disk), &cfg); err == nil {
		t.Fatal("restart accepted uncertain cleanup")
	}
	_ = bc.Close()
}

type muhashCountStore struct {
	ethdb.Database
	sourceReads, headWrites atomic.Uint64
}
type muhashCountBatch struct {
	ethdb.Batch
	store *muhashCountStore
}

func (s *muhashCountStore) Get(k []byte) ([]byte, error) {
	if _, err := rawdb.StateCommitmentElementFamily(k); err == nil {
		s.sourceReads.Add(1)
	}
	return s.Database.Get(k)
}
func (s *muhashCountStore) NewBatch() ethdb.Batch { return &muhashCountBatch{s.Database.NewBatch(), s} }
func (s *muhashCountStore) NewBatchWithSize(n int) ethdb.Batch {
	return &muhashCountBatch{s.Database.NewBatchWithSize(n), s}
}
func (b *muhashCountBatch) Put(k, v []byte) error {
	if rawdb.IsStateCommitmentHeadKey(k) {
		b.store.headWrites.Add(1)
	}
	return b.Batch.Put(k, v)
}
func TestMuHashCanonicalWritesNoRetiredBranches(t *testing.T) {
	disk := &muhashCountStore{Database: ethrawdb.NewMemoryDatabase()}
	defer disk.Close()
	witness := testInsertAddr(1)
	bc := newAsyncFlushChainOn(t, disk, witness)
	reads, writes := disk.sourceReads.Load(), disk.headWrites.Load()
	blocks := chainFrom(bc.CurrentBlock(), witness, 4, 0)
	if err := bc.InsertBlocks(blocks); err != nil {
		t.Fatal(err)
	}
	if err := bc.Close(); err != nil {
		t.Fatal(err)
	}
	sourceCalls, headPuts := disk.sourceReads.Load()-reads, disk.headWrites.Load()-writes
	if sourceCalls == 0 || headPuts == 0 {
		t.Fatal("production source/HEAD instrumentation did not observe work")
	}
	for _, prefix := range []string{rawdb.CommitmentBranchKeyPrefix, rawdb.CommitmentBranchDeltaKeyPrefix, rawdb.CommitmentBranchBaseKey, rawdb.CommitmentBranchRotationKey} {
		it := disk.NewIterator([]byte(prefix), nil)
		has := it.Next()
		err := it.Error()
		it.Release()
		if err != nil || has {
			t.Fatalf("retired branch family %s contains output err=%v", prefix, err)
		}
	}
	if _, present, err := rawdb.ReadCommitmentEngineState(disk); err != nil || present {
		t.Fatal("retired engine-state output")
	}
	h, ok, err := rawdb.ReadStateCommitmentHead(disk)
	if err != nil || !ok || physicalMuHashRoot(t, disk) != h.Root {
		t.Fatal("durable HEAD/source mismatch")
	}
	t.Logf("4 canonical blocks: source Get calls=%d, physical HEAD batch puts=%d; retired branch/base/delta/rotation rows=0", sourceCalls, headPuts)
}

func TestMuHashHistoryDurableBoundaryFork(t *testing.T) {
	witness := testInsertAddr(1)
	kv, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	disk := rawdb.WrapKeyValueStore(kv)
	bc := newAsyncFlushChainOn(t, disk, witness)
	cfg := *bc.config
	cfg.HistoryEnabled = true
	bc.config = &cfg
	a := chainFrom(bc.CurrentBlock(), witness, 4, 0)
	if err = bc.InsertBlocks(a); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()
	pin, old, err := bc.AcquireStateLatestSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	durable, present, err := rawdb.ReadStateCommitmentHead(kv)
	if err != nil || !present || durable.BlockNum != 4 {
		t.Fatalf("fixture did not cover durable LCA gap: %+v %v", durable, err)
	}
	b := chainFrom(a[2], witness, 2, 1)
	if err = bc.InsertBlocks(b); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()
	reference := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witness)
	defer reference.Close()
	refCfg := *reference.config
	refCfg.HistoryEnabled = true
	reference.config = &refCfg
	if err = reference.InsertBlocks(append(append([]*types.Block{}, a[:3]...), b...)); err != nil {
		t.Fatal(err)
	}
	reference.WaitForFlushSettled()
	if bc.CurrentBlock().Hash() != b[1].Hash() || bc.HeadStateRoot() != reference.HeadStateRoot() {
		t.Fatal("owned inverse fork differs from uninterrupted canonical branch")
	}
	still, present, err := rawdb.ReadStateCommitmentHead(pin)
	if err != nil || !present || still != old || physicalMuHashRoot(t, pin) != old.Root {
		t.Fatal("fork changed previously pinned source/HEAD cut")
	}
	if err = bc.Close(); err != nil {
		t.Fatal(err)
	}
	head, present, err := rawdb.ReadStateCommitmentHead(kv)
	if err != nil || !present || physicalMuHashRoot(t, kv) != head.Root {
		t.Fatal("fork durable source/HEAD mismatch")
	}
}

func TestMuHashDurableMaintenanceForkUsesRecipeReplay(t *testing.T) {
	witness := testInsertAddr(1)
	cfg := *params.MainnetChainConfig
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{Config: &cfg, Accounts: []params.GenesisAccount{{Address: witness, Balance: 99_000_000_000_000_000}}, Witnesses: []params.GenesisWitness{{Address: witness, VoteCount: 1, URL: "test"}}, DynamicProperties: map[string]int64{"next_maintenance_time": 30_000, "maintenance_time_interval": 30_000, "change_delegation": 1, "current_cycle_number": 7, "witness_pay_per_block": 1000, "witness_127_pay_per_block": 0}}
	makeChain := func() *BlockChain {
		disk := ethrawdb.NewMemoryDatabase()
		t.Cleanup(func() { disk.Close() })
		if _, _, err := SetupGenesisBlock(disk, genesis); err != nil {
			t.Fatal(err)
		}
		bc, err := NewBlockChain(disk, state.NewDatabase(disk), &cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { bc.Close() })
		return bc
	}
	bc := makeChain()
	var a []*types.Block
	for _, timestamp := range []int64{3000, 6000, 30000, 33000} {
		b := buildTestBlock(bc, witness, timestamp)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatal(err)
		}
		a = append(a, b)
	}
	bc.WaitForFlushSettled()
	if err := bc.buffer.Flush(bc.db); err != nil {
		t.Fatal(err)
	}
	durable, present, err := rawdb.ReadStateCommitmentHead(bc.db)
	if err != nil || !present || durable.BlockNum != 4 {
		t.Fatal("fixture durable cut missing")
	}
	if bc.canIncrementalUnwind(1, 4) {
		t.Fatal("cross-maintenance inverse was incorrectly admitted")
	}
	bc.proposalCache.markTerminal(12345)
	b := chainFrom(a[0], witness, 4, 1)
	if err = bc.InsertBlocks(b); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()
	ref := makeChain()
	if err = ref.InsertBlocks(append([]*types.Block{a[0]}, b...)); err != nil {
		t.Fatal(err)
	}
	ref.WaitForFlushSettled()
	if bc.HeadStateRoot() != ref.HeadStateRoot() {
		t.Fatal("maintenance fork differs from uninterrupted execution")
	}
	if bc.proposalCache.isTerminal(12345) {
		t.Fatal("discarded future proposal cache survived replay")
	}
	ac, ar, ap, ae := rawdb.ReadCycleRewardPending(bc.buffer)
	rc, rr, rp, re := rawdb.ReadCycleRewardPending(ref.buffer)
	if ae != nil || re != nil || ap != rp || ac != rc || !reflect.DeepEqual(ar, rr) {
		t.Fatal("fork pending rewards differ from uninterrupted execution")
	}
	if err = bc.buffer.Flush(bc.db); err != nil {
		t.Fatal(err)
	}
	head, _, err := rawdb.ReadStateCommitmentHead(bc.db)
	if err != nil || physicalMuHashRoot(t, bc.db) != head.Root {
		t.Fatal("replayed physical source/HEAD mismatch")
	}
	t.Logf("durable=%d lca=1 cross-maintenance inverse=false replay target verified", durable.BlockNum)
}

// The direct metadata batch may publish body/index anchors early, but cannot
// publish a TAPOS slot that belongs to an uncommitted source/HEAD cut.
func TestMuHashEarlyMetadataPreservesWrappedTAPOS(t *testing.T) {
	witness := testInsertAddr(1)
	disk := ethrawdb.NewMemoryDatabase()
	defer disk.Close()
	bc := newAsyncFlushChainOn(t, disk, witness)
	defer bc.Close()
	old := common.Hash{8: 0xa1, 15: 0xb2}
	if err := rawdb.WriteTaposRef(disk, 1, old); err != nil {
		t.Fatal(err)
	}
	pb := buildTestBlock(bc, witness, 3000).Proto()
	pb.BlockHeader.RawData.Number = 65537
	block := types.NewBlockFromPB(pb)
	if _, err := bc.writeBlockMetadataBatch(block, nil, common.Hash{1}, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	oldRef := types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{RefBlockBytes: []byte{0, 1}, RefBlockHash: old[8:16]}})
	if err := ValidateTAPOS(oldRef, disk); err != nil {
		t.Fatalf("early metadata damaged wrapped canonical slot: %v", err)
	}
	failed := block.Hash()
	failedRef := types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{RefBlockBytes: []byte{0, 1}, RefBlockHash: failed[8:16]}})
	if err := ValidateTAPOS(failedRef, disk); !errors.Is(err, ErrTaposHashMismatch) {
		t.Fatalf("early metadata published uncommitted wrapped slot: %v", err)
	}
	for _, key := range blockMetadataOverlayKeys(block) {
		if bytes.Equal(key, rawdb.TaposRefStorageKey(block.Number())) {
			t.Fatal("TAPOS incorrectly marked early durable")
		}
	}
}
