package state

import (
	"errors"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
