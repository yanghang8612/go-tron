package state

import (
	"errors"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"reflect"
	"testing"
)

type muhashFailStore struct {
	ethdb.KeyValueStore
	fail bool
}
type muhashFailBatch struct {
	ethdb.Batch
	owner *muhashFailStore
}

func (s *muhashFailStore) NewBatch() ethdb.Batch {
	return &muhashFailBatch{s.KeyValueStore.NewBatch(), s}
}
func (s *muhashFailStore) NewBatchWithSize(n int) ethdb.Batch {
	return &muhashFailBatch{s.KeyValueStore.NewBatchWithSize(n), s}
}
func (b *muhashFailBatch) Write() error {
	if b.owner.fail {
		return errors.New("atomic batch rejected")
	}
	return b.Batch.Write()
}
func physicalRows(t *testing.T, db ethdb.Iteratee) map[string]string {
	t.Helper()
	out := map[string]string{}
	it := db.NewIterator(nil, nil)
	for it.Next() {
		out[string(it.Key())] = string(it.Value())
	}
	err := it.Error()
	it.Release()
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func TestMuHashStandaloneBatchFailureIsAtomic(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "scope"}[scoped], func(t *testing.T) {
			store := &muhashFailStore{KeyValueStore: ethrawdb.NewMemoryDatabase()}
			defer store.Close()
			db := NewDatabase(rawdb.WrapKeyValueStore(store))
			s, err := New(common.Hash{}, db)
			if err != nil {
				t.Fatal(err)
			}
			owner := testAddr(0xa7)
			key := []byte("atomic")
			if err = s.SetAccountKV(owner, kvdomains.SystemReward, key, []byte("initial")); err != nil {
				t.Fatal(err)
			}
			if _, err = s.Commit(); err != nil {
				t.Fatal(err)
			}
			before := physicalRows(t, store)
			if scoped {
				scope := s.NewCommitScope()
				defer scope.Discard()
				for _, value := range []string{"pending-one", "pending-two"} {
					if err = s.SetAccountKV(owner, kvdomains.SystemReward, key, []byte(value)); err != nil {
						t.Fatal(err)
					}
					if _, _, err = s.CommitWithStatsOptionsInScope(scope, CommitOptions{FlushLatestDomain: func() error { return nil }}); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(before, physicalRows(t, store)) {
					t.Fatal("scope published before explicit flush")
				}
				store.fail = true
				if err = scope.FlushLatest(); err == nil {
					t.Fatal("flush succeeded despite injected failure")
				}
			} else {
				if err = s.SetAccountKV(owner, kvdomains.SystemReward, key, []byte("after")); err != nil {
					t.Fatal(err)
				}
				store.fail = true
				if _, err = s.Commit(); err == nil {
					t.Fatal("commit succeeded despite injected failure")
				}
			}
			if !reflect.DeepEqual(before, physicalRows(t, store)) {
				t.Fatal("failed batch partially published source or HEAD")
			}
		})
	}
}
func TestMuHashSeekUsesTxThenBlock(t *testing.T) {
	s := newTestStateDB(t)
	h, ok, err := rawdb.ReadStateCommitmentHead(s.db.DiskDB())
	if err != nil || !ok {
		t.Fatal(err)
	}
	h.BlockNum = 7
	h.TxNum = 19
	if err = rawdb.WriteStateCommitmentHead(s.db.DiskDB(), h); err != nil {
		t.Fatal(err)
	}
	tx, block, err := NewDomainCommitmentState(s).SeekCommitment(nil)
	if err != nil || tx != 19 || block != 7 {
		t.Fatalf("seek=%d,%d,%v", tx, block, err)
	}
}

func TestMuHashStandaloneCapturedOwnerLifetime(t *testing.T) {
	for _, mode := range []string{"wrong-target", "discard-before-fold", "release-without-fold"} {
		t.Run(mode, func(t *testing.T) {
			store := rawdb.NewMemoryDatabase()
			defer store.Close()
			s, err := New(common.Hash{}, NewDatabase(rawdb.WrapKeyValueStore(store)))
			if err != nil {
				t.Fatal(err)
			}
			baseline := physicalRows(t, store)
			scope := s.NewCommitScope()
			defer scope.Discard()
			if err = s.SetAccountKV(testAddr(0xb4), kvdomains.SystemReward, []byte("pending"), []byte("one")); err != nil {
				t.Fatal(err)
			}
			s.SetDeferFold(true)
			if _, _, err = s.CommitWithStatsOptionsInScope(scope, CommitOptions{DeepAsync: true}); err != nil {
				t.Fatal(err)
			}
			captured := s.TakeCapturedFold()
			if captured == nil {
				t.Fatal("no captured fold")
			}
			defer captured.Release()
			if _, _, err = s.CommitWithStatsOptionsInScope(scope, CommitOptions{}); err == nil {
				t.Fatal("pending private fold allowed another commit")
			}
			if err = scope.Close(); err == nil {
				t.Fatal("Close accepted an unjoined captured fold")
			}
			switch mode {
			case "wrong-target":
				other := rawdb.NewMemoryDatabase()
				defer other.Close()
				if err = rawdb.InitializeStateCommitment(other); err != nil {
					t.Fatal(err)
				}
				otherBefore := physicalRows(t, other)
				if _, err = captured.Fold(other); err == nil {
					t.Fatal("captured fold accepted another valid database")
				}
				if !reflect.DeepEqual(otherBefore, physicalRows(t, other)) {
					t.Fatal("wrong target mutated before rejection")
				}
			case "discard-before-fold":
				scope.Discard()
				if _, err = captured.Fold(scope.atomicBuffer); err == nil {
					t.Fatal("closed scope fold succeeded")
				}
			default:
				captured.Release()
			}
			if err = scope.FlushLatest(); err == nil {
				t.Fatal("abandoned capture resurrected scope publication")
			}
			if !reflect.DeepEqual(baseline, physicalRows(t, store)) {
				t.Fatal("abandoned capture published source/HEAD")
			}
		})
	}
}

func TestMuHashStandaloneNextFlushCannotPublishPreviousReadyHead(t *testing.T) {
	store := rawdb.NewMemoryDatabase()
	defer store.Close()
	s, err := New(common.Hash{}, NewDatabase(rawdb.WrapKeyValueStore(store)))
	if err != nil {
		t.Fatal(err)
	}
	baseline := physicalRows(t, store)
	scope := s.NewCommitScope()
	defer scope.Discard()
	owner := testAddr(0xb6)
	key := []byte("same-source")
	if err = s.SetAccountKV(owner, kvdomains.SystemReward, key, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.CommitWithStatsOptionsInScope(scope, CommitOptions{FlushLatestDomain: func() error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if err = s.SetAccountKV(owner, kvdomains.SystemReward, key, []byte("two")); err != nil {
		t.Fatal(err)
	}
	observed := false
	root, _, err := s.CommitWithStatsOptionsInScope(scope, CommitOptions{FlushLatestDomain: func() error {
		if err := scope.FlushLatest(); err != nil {
			return err
		}
		observed = true
		if !reflect.DeepEqual(baseline, physicalRows(t, store)) {
			t.Fatal("next source published with preceding ready HEAD before fold")
		}
		return nil
	}})
	if err != nil || !observed {
		t.Fatalf("second commit: observed=%v err=%v", observed, err)
	}
	head, present, err := rawdb.ReadStateCommitmentHead(store)
	if err != nil || !present || head.Root != root {
		t.Fatal("successful next fold did not publish matching HEAD")
	}
	rebuilt, err := domains.RebuildMuHashCommitment(store)
	if err != nil || rebuilt != root {
		t.Fatal("published source disagrees with HEAD")
	}
}

func TestMuHashImplicitStandaloneDeferredRejectsBeforeTransport(t *testing.T) {
	store := rawdb.NewMemoryDatabase()
	defer store.Close()
	s, err := New(common.Hash{}, NewDatabase(rawdb.WrapKeyValueStore(store)))
	if err != nil {
		t.Fatal(err)
	}
	baseline := physicalRows(t, store)
	index := s.accountKVIndex()
	if err = s.SetAccountKV(testAddr(0xb8), kvdomains.SystemReward, []byte("implicit"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	s.SetDeferFold(true)
	if _, err = s.Commit(); err == nil {
		t.Fatal("implicit scope accepted an unjoined deferred fold")
	}
	if s.accountKVIndex() != index || s.TakeCapturedFold() != nil || !reflect.DeepEqual(baseline, physicalRows(t, store)) {
		t.Fatal("rejected implicit capture left an active transport or durable changes")
	}
	s.SetDeferFold(false)
	if _, err = s.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestMuHashBareStaleParentRejectsSourceAndHistory(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "stale-copy", true: "changed-after-private-fold"}[pending], func(t *testing.T) {
			store := rawdb.NewMemoryDatabase()
			defer store.Close()
			db := NewDatabase(rawdb.WrapKeyValueStore(store))
			original, err := New(common.Hash{}, db)
			if err != nil {
				t.Fatal(err)
			}
			loser, err := original.Copy()
			if err != nil {
				t.Fatal(err)
			}
			if err = loser.SetAccountKV(testAddr(0xc1), kvdomains.SystemReward, []byte("loser"), []byte("unpublished")); err != nil {
				t.Fatal(err)
			}
			var scope *CommitScope
			if pending {
				scope = loser.NewCommitScope()
				defer scope.Discard()
				if _, _, err = loser.CommitWithStatsOptionsInScope(scope, CommitOptions{FlushLatestDomain: func() error { return nil }}); err != nil {
					t.Fatal(err)
				}
			}
			if err = original.SetAccountKV(testAddr(0xc2), kvdomains.SystemReward, []byte("winner"), []byte("durable")); err != nil {
				t.Fatal(err)
			}
			root, err := original.Commit()
			if err != nil {
				t.Fatal(err)
			}
			winning := physicalRows(t, store)
			if pending {
				if err = scope.FlushLatest(); err == nil {
					t.Fatal("private scope published over changed base HEAD")
				}
				if err = scope.FlushLatest(); err == nil {
					t.Fatal("failed parent guard was resurrected")
				}
			} else {
				loser.SetDomainChangeSetWriter(store, 1, common.Hash{0xd1})
				if _, err = loser.Commit(); err == nil {
					t.Fatal("stale copy silently committed against another parent")
				}
			}
			if !reflect.DeepEqual(winning, physicalRows(t, store)) {
				t.Fatal("stale writer changed winning source/HEAD/history")
			}
			fresh, err := New(common.Hash{}, db)
			if err != nil {
				t.Fatal(err)
			}
			if common.Hash(fresh.originRoot) != root {
				t.Fatal("zero constructor did not remember actual MuHash parent")
			}
			rebuilt, err := domains.RebuildMuHashCommitment(store)
			if err != nil || rebuilt != root {
				t.Fatal("winning physical source/HEAD mismatch")
			}
		})
	}
}
