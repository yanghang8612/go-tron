package state

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Observe actual physical points independently of the frozen materialization.
type frozenConsensusReader struct {
	disk  ethdb.KeyValueStore
	reads []TransactionAccessKey
}

func (r *frozenConsensusReader) GetLatest(a tcommon.Address, d kvdomains.KVDomain, k []byte) ([]byte, bool, error) {
	return r.KVLatest(a, 0, d, k)
}
func (r *frozenConsensusReader) KVLatest(a tcommon.Address, g uint64, d kvdomains.KVDomain, k []byte) ([]byte, bool, error) {
	r.reads = append(r.reads, TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: a, KVDomain: d, LogicalKey: string(k)})
	return rawdb.ReadStateKVLatest(r.disk, a, g, d, k)
}

func TestFrozenConsensusBandwidthLegalLifecycle(t *testing.T) {
	for _, v1 := range []int64{0, 11} {
		for _, fullBefore := range []bool{false, true} {
			t.Run(fmt.Sprintf("v1=%d/full=%t", v1, fullBefore), func(t *testing.T) {
				base := newTestStateDB(t)
				a, delegator := testAddr(0xc1), testAddr(0xc2)
				base.CreateAccount(a, corepb.AccountType_Normal)
				base.CreateAccount(delegator, corepb.AccountType_Normal)
				if v1 != 0 {
					base.FreezeV1Bandwidth(a, v1, 100)
				}
				base.AddFreezeV2(a, corepb.ResourceCode_BANDWIDTH, 21)
				base.FreezeV1DelegatedBandwidth(delegator, a, 31)
				base.AddAcquiredDelegatedFrozenV2(a, corepb.ResourceCode_BANDWIDTH, 41)
				root, e := base.Commit()
				if e != nil {
					t.Fatal(e)
				}
				var outcomes [][]int64
				var roots []tcommon.Hash
				for _, canonical := range []bool{false, true} {
					disk := rawdb.NewMemoryDatabase()
					it := base.db.DiskDB().NewIterator(nil, nil)
					for it.Next() {
						if e := disk.Put(it.Key(), it.Value()); e != nil {
							t.Fatal(e)
						}
					}
					if e := it.Error(); e != nil {
						t.Fatal(e)
					}
					it.Release()
					database := NewDatabase(rawdb.WrapKeyValueStore(disk))
					t.Cleanup(func() { database.Close() })
					s, e := New(root, database)
					if e != nil {
						t.Fatal(e)
					}
					if fullBefore && s.GetAccount(a) == nil {
						t.Fatal("missing full account")
					}
					var values []int64
					read := func(v *StateDB) int64 {
						var got int64
						var e error
						if canonical {
							got, e = v.GetAccountFrozenBandwidthForConsensus(a)
						} else {
							got, e = v.GetAccountFrozenBandwidth(a)
						}
						if e != nil {
							t.Fatal(e)
						}
						values = append(values, got)
						return got
					}
					if got := read(s); got != v1+93 {
						t.Fatalf("sum=%d want%d", got, v1+93)
					}
					read(s)
					cp, e := s.Copy()
					if e != nil {
						t.Fatal(e)
					}
					read(cp)
					cp.FreezeV1Bandwidth(a, 8, 200)
					read(cp)
					if got := read(s); got != v1+93 {
						t.Fatal("copy mutation leaked")
					}
					mark := s.Snapshot()
					s.FreezeV1Bandwidth(a, 99, 200)
					read(s)
					s.RevertToSnapshot(mark)
					if got := read(s); got != v1+93 {
						t.Fatal("revert lost prior sum")
					}
					mark = s.Snapshot()
					if e = s.ResetAccountKV(a); e != nil {
						t.Fatal(e)
					}
					s.FreezeV1Bandwidth(a, 77, 300)
					s.AddFreezeV2(a, corepb.ResourceCode_BANDWIDTH, 88)
					read(s)
					s.RevertToSnapshot(mark)
					if got := read(s); got != v1+93 {
						t.Fatal("reset revert lost prior sum")
					}
					s.FreezeV1Bandwidth(a, 5, 400)
					read(s)
					next, e := s.Commit()
					if e != nil {
						t.Fatal(e)
					}
					reopened, e := New(next, database)
					if e != nil {
						t.Fatal(e)
					}
					read(reopened)
					outcomes = append(outcomes, values)
					roots = append(roots, next)
				}
				if !reflect.DeepEqual(outcomes[0], outcomes[1]) || roots[0] != roots[1] {
					t.Fatalf("route outcomes differ: %v roots%v", outcomes, roots)
				}
			})
		}
	}
}

func TestFrozenConsensusBandwidthReadDependencies(t *testing.T) {
	base := newTestStateDB(t)
	a := testAddr(0xc3)
	base.CreateAccount(a, corepb.AccountType_Normal)
	base.FreezeV1Bandwidth(a, 11, 100)
	base.AddFreezeV2(a, corepb.ResourceCode_BANDWIDTH, 21)
	root, e := base.Commit()
	if e != nil {
		t.Fatal(e)
	}
	for _, canonical := range []bool{false, true} {
		t.Run(fmt.Sprint(canonical), func(t *testing.T) {
			s, e := New(root, base.db)
			if e != nil {
				t.Fatal(e)
			}
			reader := &frozenConsensusReader{disk: base.db.DiskDB()}
			s.setAccountKVLatestView(reader, nil)
			capture := func() map[TransactionAccessKey]TransactionAccessMode {
				var rec TransactionAccessRecorder
				rec.Reset(1)
				s.SetTransactionAccessRecorder(&rec)
				var got int64
				var e error
				if canonical {
					got, e = s.GetAccountFrozenBandwidthForConsensus(a)
				} else {
					got, e = s.GetAccountFrozenBandwidth(a)
				}
				s.SetTransactionAccessRecorder(nil)
				if e != nil || got != 32 {
					t.Fatalf("read %d/%v", got, e)
				}
				out := map[TransactionAccessKey]TransactionAccessMode{}
				for _, r := range rec.CaptureReadSet().Reads {
					out[r.Key] = r.Mode
				}
				return out
			}
			cold := capture()
			n := len(reader.reads)
			hot := capture()
			if len(reader.reads) != n || !reflect.DeepEqual(cold, hot) {
				t.Fatal("warm dependency replay or point cache differs")
			}
			for _, key := range []TransactionAccessKey{{Kind: TransactionAccessAccountKV, Address: a, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(0))}, {Kind: TransactionAccessAccountKV, Address: a, KVDomain: kvdomains.AccountFrozenV2Aux, LogicalKey: string(accountFrozenV2Key(corepb.ResourceCode_BANDWIDTH))}} {
				if _, ok := cold[key]; !ok {
					t.Fatalf("missing dependency %+v", key)
				}
			}
			second := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: a, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(1))}
			_, hasSecond := cold[second]
			if hasSecond == canonical {
				t.Fatalf("row1 dependency=%t canonical=%t", hasSecond, canonical)
			}
			wantPoints := 3
			if canonical {
				wantPoints = 2
			}
			if n != wantPoints {
				t.Fatalf("actual points%d want%d", n, wantPoints)
			}
		})
	}
}

func TestFrozenConsensusBandwidthGeneralCompatibility(t *testing.T) {
	for _, shape := range []string{"multi", "sparse", "malformed"} {
		for _, general := range []string{"bandwidth", "totals", "full", "mutation"} {
			t.Run(shape+"/"+general, func(t *testing.T) {
				base := newTestStateDB(t)
				a := testAddr(0xc4)
				base.CreateAccount(a, corepb.AccountType_Normal)
				put := func(index uint32, raw []byte) {
					if e := base.SetAccountKV(a, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(index), raw); e != nil {
						t.Fatal(e)
					}
				}
				first, _ := encodeAccountFrozen(&corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 100})
				second, _ := encodeAccountFrozen(&corepb.Account_Frozen{FrozenBalance: 22, ExpireTime: 200})
				if shape != "sparse" {
					put(0, first)
				}
				if shape == "malformed" {
					second = []byte("invalid frozen")
				}
				put(1, second)
				root, e := base.Commit()
				if e != nil {
					t.Fatal(e)
				}
				s, e := New(root, base.db)
				if e != nil {
					t.Fatal(e)
				}
				got, e := s.GetAccountFrozenBandwidthForConsensus(a)
				wantCanonical := int64(11)
				if shape == "sparse" {
					wantCanonical = 0
				}
				if e != nil || got != wantCanonical {
					t.Fatalf("canonical %d/%v", got, e)
				}
				var total int64
				var err error
				switch general {
				case "bandwidth":
					total, err = s.GetAccountFrozenBandwidth(a)
				case "totals":
					total, _, err = s.GetAccountFrozenResourceTotals(a)
				case "full":
					acc := s.GetAccount(a)
					err = s.Error()
					if acc != nil {
						total = acc.TotalFrozenBandwidth()
					}
				case "mutation":
					s.FreezeV1Bandwidth(a, 5, 300)
					err = s.Error()
					if err == nil {
						total, err = s.GetAccountFrozenBandwidth(a)
					}
				}
				if shape == "malformed" {
					if err == nil || s.Error() == nil {
						t.Fatal("general route hid malformed row1")
					}
					return
				}
				want := int64(33)
				if shape == "sparse" {
					want = 22
				}
				if general == "mutation" {
					want += 5
				}
				if err != nil || total != want {
					t.Fatalf("general total%d/%v want%d", total, err, want)
				}
				if general != "mutation" {
					obj := s.getStateObject(a)
					if obj.accountFrozenBandwidthMode != accountFrozenBandwidthComplete {
						t.Fatalf("general loader failed complete upgrade mode%d", obj.accountFrozenBandwidthMode)
					}
				}
				if general == "full" {
					raw, exists, err := s.GetAccountKV(a, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(1))
					if err != nil || !exists || !bytes.Equal(raw, second) {
						t.Fatal("full upgrade changed raw row1")
					}
				}
			})
		}
	}
}

func TestFrozenConsensusBandwidthVersionedFallback(t *testing.T) {
	for _, scenario := range []string{"updated", "deleted-account", "missing-account"} {
		t.Run(scenario, func(t *testing.T) {
			base := newTestStateDB(t)
			a := testAddr(0xc6)
			if scenario != "missing-account" {
				base.CreateAccount(a, corepb.AccountType_Normal)
				base.FreezeV1Bandwidth(a, 11, 100)
				base.AddFreezeV2(a, corepb.ResourceCode_BANDWIDTH, 21)
			}
			root, e := base.Commit()
			if e != nil {
				t.Fatal(e)
			}
			updated, _ := encodeAccountFrozen(&corepb.Account_Frozen{FrozenBalance: 55, ExpireTime: 200})
			reader := testTransactionVersionedReader{{Kind: TransactionAccessAccountKV, Address: a, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(0))}: {{txIndex: 1, value: TransactionWriteValue{Exists: true, Value: updated}}}}
			if scenario == "deleted-account" {
				reader[TransactionAccessKey{Kind: TransactionAccessAccountField, Address: a, AccountField: TransactionAccountFieldExistence}] = []testTransactionVersion{{txIndex: 1, value: TransactionWriteValue{Exists: false}}}
			}
			var values []int64
			var deps []TransactionReadSet
			for _, canonical := range []bool{false, true} {
				s, e := New(root, base.db)
				if e != nil {
					t.Fatal(e)
				}
				// Both routes start with an existing canonical materialization, the exact
				// boundary at which returning a cached canonical row would be stale.
				if _, e = s.GetAccountFrozenBandwidthForConsensus(a); e != nil {
					t.Fatal(e)
				}
				s.SetTransactionVersionedValueReader(reader, 2)
				var rec TransactionAccessRecorder
				rec.Reset(2)
				s.SetTransactionAccessRecorder(&rec)
				var got int64
				if canonical {
					got, e = s.GetAccountFrozenBandwidthForConsensus(a)
				} else {
					got, e = s.GetAccountFrozenBandwidth(a)
				}
				s.SetTransactionAccessRecorder(nil)
				if e != nil {
					t.Fatal(e)
				}
				values = append(values, got)
				deps = append(deps, rec.CaptureReadSet())
			}
			want := int64(76)
			if scenario != "updated" {
				want = 0
			}
			if values[0] != want || values[1] != want {
				t.Fatalf("versioned sums%v want%d", values, want)
			}
			normalize := func(set TransactionReadSet) map[TransactionAccessKey]TransactionRead {
				out := map[TransactionAccessKey]TransactionRead{}
				for _, r := range set.Reads {
					out[r.Key] = r
				}
				return out
			}
			if !reflect.DeepEqual(normalize(deps[0]), normalize(deps[1])) || deps[0].Unsupported != deps[1].Unsupported {
				t.Fatal("versioned fallback changed dependencies")
			}
		})
	}
}
