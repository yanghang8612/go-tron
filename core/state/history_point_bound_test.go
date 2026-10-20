package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type genesisNegativeCold struct{ calls int }

func (*genesisNegativeCold) IterateStateDomainChanges(uint64, uint64, func(*rawdb.StateDomainChange) (bool, error)) error {
	return nil
}
func (c *genesisNegativeCold) StateTxRangeForBlock(uint64) (*rawdb.StateTxRange, bool, error) {
	c.calls++
	return nil, false, errors.New("unexpected cold tx-range scan")
}
func TestHistoryGenesisCutAvoidsColdNegativeScan(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	cold := &genesisNegativeCold{}
	r := NewPersistentHistoryReaderWithColdHistory(db, nil, 10, cold)
	n, err := r.stateTxNumAtBlockEnd(0)
	if err != nil || n != 0 || cold.calls != 0 {
		t.Fatalf("cut %d err %v calls %d", n, err, cold.calls)
	}
	if err := rawdb.WriteStateTxRange(db, 0, common.Hash{1}, 4, 5); err != nil {
		t.Fatal(err)
	}
	r = NewPersistentHistoryReaderWithColdHistory(db, nil, 10, cold)
	n, err = r.stateTxNumAtBlockEnd(0)
	if err != nil || n != 5 || cold.calls != 0 {
		t.Fatalf("explicit cut %d err %v calls %d", n, err, cold.calls)
	}
}

func TestHistoryColdBoundRetainsEarlierHotSequence(t *testing.T) {
	for _, hotSeq := range []uint64{1, 3} {
		db := rawdb.NewMemoryDatabase()
		owner, key := testAddr(0x56), []byte("same-tx")
		hot := &rawdb.StateDomainChange{BlockNum: 2, TxNum: 22, Seq: hotSeq, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 7, Domain: kvdomains.SystemReward, Key: key, PrevExists: true, Prev: []byte("hot")}
		if err := rawdb.WriteStateDomainChange(db, hot); err != nil {
			t.Fatal(err)
		}
		coldRow := *hot
		coldRow.Seq = 2
		coldRow.Prev = []byte("cold")
		cold := &keyedColdHistoryStub{changes: []*rawdb.StateDomainChange{&coldRow}}
		r := NewPersistentHistoryReaderWithColdHistory(db, nil, 100, cold)
		if err := r.SetHotHistoryBlockRange(1, 100); err != nil {
			t.Fatal(err)
		}
		got, err := r.firstStateDomainChangeByKey(11, 1000, hot.FlatDomain, owner, 7, hot.Domain, key)
		want := "cold"
		if hotSeq == 1 {
			want = "hot"
		}
		if err != nil || got == nil || string(got.Prev) != want {
			t.Fatalf("seq=%d got=%+v err=%v", hotSeq, got, err)
		}
		db.Close()
	}
}

// Cancel during an empty posting seek, before the first tail block is read.
// A callback-only cancellation check cannot handle an absent logical key.
type cancelOnIteratorDB struct {
	readerDB
	cancel context.CancelFunc
	calls  int
}

func (d *cancelOnIteratorDB) IsPinnedKeyValueView() bool { return true }

// NewIterator is defined separately below to keep cancellation at the IO seam.

func (d *cancelOnIteratorDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	d.calls++
	d.cancel()
	return d.readerDB.NewIterator(prefix, start)
}
func TestHistoryPointSeekCancelsInsideAbsentHotLookup(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, 2, common.Hash{2}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &cancelOnIteratorDB{readerDB: db, cancel: cancel}
	r := NewPersistentHistoryReader(d, nil, 100)
	r.SetContext(ctx)
	if err := r.SetHotHistoryBlockRange(1, 100); err != nil {
		t.Fatal(err)
	}
	_, err := r.firstStateDomainChangeByKey(1, 100, rawdb.StateFlatDomainKVLatest, testAddr(0x57), 1, kvdomains.SystemReward, []byte("missing"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v calls=%d", err, d.calls)
	}
}

func TestHistoryStorageMetadataAcrossRecreateHotAndCold(t *testing.T) {
	for _, cold := range []bool{false, true} {
		t.Run(fmt.Sprint(cold), func(t *testing.T) {
			f := newHistoryFixture(t)
			defer f.disk.Close()
			addr, other := testAddr(0x67), testAddr(0x68)
			slot := common.Hash{31: 4}
			meta := func(version int32, seed byte) *contractpb.SmartContract {
				return &contractpb.SmartContract{Version: version, TrxHash: bytes.Repeat([]byte{seed}, 32), ContractAddress: addr.Bytes()}
			}
			f.applyBlock(common.Hash{1}, func(s *StateDB) {
				s.CreateAccount(addr, corepb.AccountType_Contract)
				s.SetContract(addr, meta(0, 1))
				s.SetState(addr, slot, common.Hash{31: 11})
			})
			f.applyBlock(common.Hash{2}, func(s *StateDB) {
				m := meta(0, 1)
				m.Name = "changed metadata"
				s.SetContract(addr, m)
				s.SetState(addr, slot, common.Hash{31: 22})
			})
			f.applyBlock(common.Hash{3}, func(s *StateDB) { s.SelfDestruct(addr) })
			f.applyBlock(common.Hash{4}, func(s *StateDB) {
				s.CreateAccount(addr, corepb.AccountType_Contract)
				s.SetContract(addr, meta(1, 2))
				s.SetState(addr, slot, common.Hash{31: 44})
			})
			f.applyBlock(common.Hash{5}, func(s *StateDB) { s.AddBalance(other, 1) })
			before, ok, err := rawdb.ReadStateCommitmentHead(f.disk)
			if err != nil || !ok {
				t.Fatalf("HEAD: %v", err)
			}
			r := f.reader()
			if cold {
				first, _, err := rawdb.ReadStateTxRange(f.disk, 1)
				if err != nil {
					t.Fatal(err)
				}
				dir := t.TempDir()
				refs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(f.disk, dir, first.BeginTxNum, f.endTxNum, "history/state-domain-change-meta-recreate.seg")
				if err != nil {
					t.Fatal(err)
				}
				if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(first.BeginTxNum, f.endTxNum, refs)); err != nil {
					t.Fatal(err)
				}
				m, err := statesnapshots.OpenManager(dir)
				if err != nil {
					t.Fatal(err)
				}
				f.pruneHotStateDomainHistory()
				r = NewPersistentHistoryReaderWithColdHistory(f.disk, f.state, f.head, m)
			}
			// The fixture is now immutable. Force the unindexed-tail path and
			// query-scoped pinned reuse while checking recreate generations,
			// cold overlap and the independent source-fold MuHash below.
			if err := rawdb.WriteStageProgressWithHash(f.disk, rawdb.StageStateHistoryIndex, 0, common.Hash{}); err != nil {
				t.Fatal(err)
			}
			r.db = rawdb.WithStateHistoryQueryTailCache(&pointReadCounter{readerDB: r.db})
			for height, want := range map[uint64]byte{0: 0, 1: 11, 2: 22, 3: 0, 4: 44} {
				if err := r.SetHotHistoryBlockRange(height, f.head); err != nil {
					t.Fatal(err)
				}
				got, err := r.StorageAt(addr, slot, height)
				if err != nil || got != (common.Hash{31: want}) {
					t.Fatalf("height=%d got=%x want=%d err=%v", height, got, want, err)
				}
			}
			after, ok, err := rawdb.ReadStateCommitmentHead(f.disk)
			if err != nil || !ok || before.Root != after.Root || before.TxNum != after.TxNum {
				t.Fatalf("read mutated HEAD: %v", err)
			}
			oracle := muhash3072.NewAccumulator()
			for _, prefix := range rawdb.StateCommitmentSourcePrefixes() {
				it := f.disk.NewIterator(prefix, nil)
				for it.Next() {
					e, err := rawdb.EncodeStateCommitmentElement(it.Key(), it.Value())
					if err != nil {
						t.Fatal(err)
					}
					if err := oracle.Insert(e); err != nil {
						t.Fatal(err)
					}
				}
				err := it.Error()
				it.Release()
				if err != nil {
					t.Fatal(err)
				}
			}
			digest, err := oracle.Digest()
			if err != nil || common.Hash(digest) != after.Root {
				t.Fatalf("independent MuHash root: got %x want %x err=%v", digest, after.Root, err)
			}
		})
	}
}

type pointColdPruned struct{ keyedColdHistoryStub }

func (*pointColdPruned) HotPrunedThroughBlock() (uint64, bool, error) { return 2, true, nil }

type pointReadCounter struct {
	readerDB
	values int
}

func (d *pointReadCounter) IsPinnedKeyValueView() bool { return true }
func (d *pointReadCounter) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &pointReadIterator{Iterator: d.readerDB.NewIterator(prefix, start), owner: d}
}

type pointReadIterator struct {
	ethdb.Iterator
	owner *pointReadCounter
}

func (i *pointReadIterator) Value() []byte { i.owner.values++; return i.Iterator.Value() }

func TestHistoryColdFirstAvoidsIrrelevantHotTail(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	owner, noise := testAddr(0x71), testAddr(0x72)
	key := []byte("cold-match")
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, 2, common.Hash{2}); err != nil {
		t.Fatal(err)
	}
	for b := uint64(3); b <= 65; b++ {
		if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{{BlockNum: b, TxNum: b, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: noise, Domain: kvdomains.SystemReward, Key: key, Prev: []byte("noise")}}); err != nil {
			t.Fatal(err)
		}
	}
	cold := &pointColdPruned{keyedColdHistoryStub{changes: []*rawdb.StateDomainChange{{BlockNum: 2, TxNum: 2, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.SystemReward, Key: key, PrevExists: true, Prev: []byte("at-target")}}}}
	d := &pointReadCounter{readerDB: db}
	r := NewPersistentHistoryReaderWithColdHistory(d, nil, 65, cold)
	if err := r.SetHotHistoryBlockRange(1, 65); err != nil {
		t.Fatal(err)
	}
	control, err := r.readFirstHotStateDomainChangeByKey(1, 65, rawdb.StateFlatDomainKVLatest, owner, 0, kvdomains.SystemReward, key)
	if err != nil || control != nil || d.values != 63 {
		t.Fatalf("control=%+v reads=%d err=%v", control, d.values, err)
	}
	d.values = 0
	got, err := r.firstStateDomainChangeByKey(1, 65, rawdb.StateFlatDomainKVLatest, owner, 0, kvdomains.SystemReward, key)
	if err != nil || got == nil || string(got.Prev) != "at-target" || d.values != 0 {
		t.Fatalf("got=%+v reads=%d err=%v", got, d.values, err)
	}
}
