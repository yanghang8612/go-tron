package snapshots

import (
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"os"
	"sync"
	"testing"
)

func seedTestMuHashHead(t *testing.T, db interface {
	AggregatorDB
	ethdb.KeyValueWriter
}, block, tx uint64, hash common.Hash) rawdb.StateCommitmentHead {
	t.Helper()
	value, err := RebuildLatestMuHash(db)
	if err != nil {
		t.Fatal(err)
	}
	head := rawdb.NewStateCommitmentHead()
	head.BlockNum = block
	head.BlockHash = hash
	head.TxNum = tx
	head.Root = common.Hash(value.Digest)
	head.State = value.State
	if err := rawdb.WriteStateExecutionProperties(db, rawdb.StateExecutionProperties{BlockNum: int64(block), Timestamp: int64(1000 + block), BlockHash: hash}); err != nil {
		t.Fatal(err)
	}
	if row, present, err := rawdb.ReadStateTxRange(db, block); err != nil {
		t.Fatal(err)
	} else if tx > 0 && (!present || row.BlockHash != hash || row.EndTxNum != tx) {
		if err := rawdb.WriteStateTxRange(db, block, hash, tx, tx); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteBlockNumber(db, hash, block); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCommitmentHead(db, head); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, head.Root); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{BlockNum: block, BlockHash: hash, Root: head.Root, Scheme: head.Scheme}); err != nil {
		t.Fatal(err)
	}
	return head
}

// Tests use an owned copy as their pinned view; production uses real Pebble MVCC.
type ownedTestSnapshot struct{ ethdb.KeyValueStore }

func (*ownedTestSnapshot) IsPinnedKeyValueView() bool { return true }
func (c *coldBuilderChain) AcquireStateLatestSnapshot(ctx context.Context) (pointread.KeyValueSnapshot, rawdb.StateCommitmentHead, error) {
	copy := rawdb.NewMemoryDatabase()
	it := c.db.NewIterator(nil, nil)
	for it.Next() {
		if err := copy.Put(it.Key(), it.Value()); err != nil {
			it.Release()
			return nil, rawdb.StateCommitmentHead{}, err
		}
	}
	err := it.Error()
	it.Release()
	if err != nil {
		return nil, rawdb.StateCommitmentHead{}, err
	}
	head, ok, err := rawdb.ReadStateCommitmentHead(copy)
	if err != nil {
		return nil, head, err
	}
	if !ok {
		return nil, head, rawdb.ErrStateCommitmentHead
	}
	return &ownedTestSnapshot{copy}, head, nil
}

type advancingCutChain struct {
	db       ethdb.KeyValueStore
	solid    int64
	head     rawdb.StateCommitmentHead
	pins     int
	advanced chan struct{}
	finished chan struct{}
}

func (c *advancingCutChain) DB() AggregatorDB                { return c.db }
func (c *advancingCutChain) LatestSolidifiedBlockNum() int64 { return c.solid }
func (c *advancingCutChain) CanonicalBlockHashStrict(n uint64) (common.Hash, bool, error) {
	return c.head.BlockHash, n == c.head.BlockNum, nil
}
func (c *advancingCutChain) AcquireStateLatestSnapshot(ctx context.Context) (pointread.KeyValueSnapshot, rawdb.StateCommitmentHead, error) {
	c.pins++
	snap, err := c.db.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		return nil, c.head, err
	}
	head, _, err := rawdb.ReadStateCommitmentHead(snap)
	if err != nil {
		snap.Close()
		return nil, head, err
	}
	return &advanceOnScanSnapshot{KeyValueSnapshot: snap, advanced: c.advanced, finished: c.finished}, head, nil
}

type advanceOnScanSnapshot struct {
	pointread.KeyValueSnapshot
	advanced chan struct{}
	finished chan struct{}
	once     sync.Once
}

func (s *advanceOnScanSnapshot) NewIterator(prefix, start []byte) ethdb.Iterator {
	s.once.Do(func() { close(s.advanced); <-s.finished })
	return s.KeyValueSnapshot.NewIterator(prefix, start)
}

func TestMuHashColdCutPinsOnceWhileHeadAdvancesAndSolidCatchesUp(t *testing.T) {
	tune := rawdb.DefaultPebbleOptions()
	tune.MemTableSizeBytes = 1 << 20
	db, err := rawdb.NewPebbleDBWithOptions(t.TempDir(), 16, 64, tune)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	owner := coldBuilderOwner(0x37)
	if err := rawdb.WriteStateAccountLatest(db, owner, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(db, owner, 2); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 1, kvdomains.ContractStorage, []byte("slot"), []byte{}); err != nil {
		t.Fatal(err)
	}
	hash := writeColdBuilderCanonicalBlock(t, db, 10)
	head := seedTestMuHashHead(t, db, 10, 100, hash)
	if err := rawdb.WriteStateTxRange(db, 9, common.Hash{9}, 90, 90); err != nil {
		t.Fatal(err)
	}
	chain := &advancingCutChain{db: db, solid: 9, head: head, advanced: make(chan struct{}), finished: make(chan struct{})}
	go func() {
		<-chain.advanced
		defer close(chain.finished)
		acc, err := muhash3072.FromState(head.State)
		if err != nil {
			t.Error(err)
			return
		}
		key := rawdb.StateAccountLatestCommitmentKey(owner)
		old, _ := rawdb.EncodeStateCommitmentElement(key, []byte("old"))
		nextValue, _ := rawdb.EncodeStateCommitmentElement(key, []byte("new"))
		if err := acc.Remove(old); err != nil {
			t.Error(err)
			return
		}
		if err := acc.Insert(nextValue); err != nil {
			t.Error(err)
			return
		}
		finalized, err := acc.Finalize()
		if err != nil {
			t.Error(err)
			return
		}
		next := head
		next.BlockNum = 15
		next.BlockHash = common.Hash{15}
		next.TxNum = 150
		next.Root = common.Hash(finalized.Digest)
		next.State = finalized.State
		batch := db.NewBatch()
		defer batch.Close()
		if err := rawdb.WriteStateAccountLatest(batch, owner, []byte("new")); err != nil {
			t.Error(err)
			return
		}
		if err := rawdb.WriteStateCommitmentHead(batch, next); err != nil {
			t.Error(err)
			return
		}
		if err := rawdb.WriteLatestDomainCommitmentRoot(batch, next.Root); err != nil {
			t.Error(err)
			return
		}
		if err := batch.Write(); err != nil {
			t.Error(err)
		}
	}()

	dir := t.TempDir()
	runner := NewRunner(chain, Config{Enabled: true, Dir: dir, LatestBuildBlocks: 1})
	built, deferred, err := runner.latestPassWithStatus()
	if err != nil || built || !deferred {
		t.Fatalf("waiting cut %v %v %v", built, deferred, err)
	}
	if chain.pins != 1 || runner.pendingLatest == nil {
		t.Fatal("cut was not retained")
	}
	if _, err := LoadManifest(dir); !os.IsNotExist(err) {
		t.Fatalf("unfinalized cut published: %v", err)
	}
	chain.solid = 12
	built, deferred, err = runner.latestPassWithStatus()
	if err != nil || !built || deferred {
		t.Fatalf("publish cut %v %v %v", built, deferred, err)
	}
	if chain.pins != 1 {
		t.Fatalf("chased moving head: %d pins", chain.pins)
	}
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.VisibleTxEnd != 0 || manifest.LatestStateTxNum != 100 || manifest.SourceRows != [3]uint64{1, 1, 1} {
		t.Fatalf("cut/census %+v", manifest)
	}
	mgr, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := mgr.GetAccountLatest(owner, 100)
	if err != nil || !ok || string(got) != "old" {
		t.Fatalf("source moved with current head %q %v", got, err)
	}
	cold, ok, err := mgr.GetCommitmentHead(100)
	if err != nil || !ok || cold != head {
		t.Fatalf("cold HEAD/cut mismatch %+v %v", cold, err)
	}
	// A subsequent history-only integration must retain the exact latest cut.
	next, err := NewAggregator(dir).Integrate(1, 200, nil)
	if err != nil {
		t.Fatal(err)
	}
	if next.VisibleTxEnd != 200 || next.LatestStateTxNum != 100 || next.SourceRows != manifest.SourceRows {
		t.Fatal("history publication relabeled latest cut")
	}
}

type failAnchorStore struct {
	ethdb.KeyValueStore
	failHead bool
}

func (s *failAnchorStore) NewBatch() ethdb.Batch {
	return &failAnchorBatch{Batch: s.KeyValueStore.NewBatch(), owner: s}
}
func (s *failAnchorStore) NewBatchWithSize(n int) ethdb.Batch {
	return &failAnchorBatch{Batch: s.KeyValueStore.NewBatchWithSize(n), owner: s}
}

type failAnchorBatch struct {
	ethdb.Batch
	owner *failAnchorStore
}

func (b *failAnchorBatch) Put(k, v []byte) error {
	if b.owner.failHead && rawdb.IsStateCommitmentHeadKey(k) {
		return errors.New("injected final HEAD write failure")
	}
	return b.Batch.Put(k, v)
}
func TestMuHashFreshRestoreMarkerAndAtomicPublication(t *testing.T) {
	dir, identity, wantRoot, _ := writeVerifiableCommitmentManifest(t)
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("final-failure-%v", fail), func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			target := &failAnchorStore{KeyValueStore: db, failHead: fail}
			_, err := RestoreLatestFromVerifiedManifest(target, dir, identity)
			head, ok, readErr := rawdb.ReadStateCommitmentHead(db)
			if readErr != nil {
				t.Fatal(readErr)
			}
			pending, markerErr := rawdb.StateCommitmentRestoreInProgress(db)
			if markerErr != nil {
				t.Fatal(markerErr)
			}
			if fail {
				if err == nil || ok || !pending {
					t.Fatalf("partial publication err=%v head=%v marker=%v", err, ok, pending)
				}
				if _, err := RestoreLatestFromVerifiedManifest(target, dir, identity); err == nil {
					t.Fatal("failed payload target silently resumed")
				}
				return
			}
			if err != nil || !ok || pending || head.Root != wantRoot {
				t.Fatalf("complete publication err=%v head=%+v marker=%v", err, head, pending)
			}
			if _, err := RestoreLatestFromVerifiedManifest(target, dir, identity); err == nil {
				t.Fatal("existing state overwritten")
			}
		})
	}
	// Residual physical source data is rejected before any marker/publication.
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateAccountLatest(db, coldBuilderOwner(9), []byte{}); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreLatestFromVerifiedManifest(db, dir, identity); err == nil {
		t.Fatal("unknown residual state imported")
	}
	if active, err := rawdb.StateCommitmentRestoreInProgress(db); err != nil || active {
		t.Fatalf("rejected source was mutated: marker=%v %v", active, err)
	}
}

func TestMuHashManagerLatestCutDoesNotUseHistoryOrProgress(t *testing.T) {
	dir, _ := writeVerifiableHeadManifest(t)
	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.LatestStateTxNum <= manifest.VisibleTxEnd {
		t.Fatal("fixture lacks separated latest/history cuts")
	}
	for _, progress := range []*Progress{nil, {LatestBuildTxNum: 1}} {
		copy := *manifest
		copy.Progress = progress
		mgr, err := OpenPinnedManager(dir, &copy)
		if err != nil {
			t.Fatal(err)
		}
		got, present, err := mgr.LatestStateTxNum()
		if err != nil || !present || got != manifest.LatestStateTxNum {
			t.Fatalf("latest cut=%d %v %v, want %d", got, present, err, manifest.LatestStateTxNum)
		}
	}
}
