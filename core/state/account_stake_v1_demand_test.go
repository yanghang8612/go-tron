package state

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type frozenDemandReader struct {
	rows    map[string][]byte
	reads   int
	failure error
}

func (r *frozenDemandReader) GetLatest(_ tcommon.Address, _ kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	r.reads++
	if r.failure != nil {
		return nil, false, r.failure
	}
	v, ok := r.rows[string(key)]
	return bytes.Clone(v), ok, nil
}

func TestFrozenDemandPointCacheLifecycle(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(0xe1)
	s.CreateAccount(addr, corepb.AccountType_Normal)
	root, err := s.Commit()
	if err != nil {
		t.Fatal(err)
	}
	s, err = New(root, s.db)
	if err != nil {
		t.Fatal(err)
	}
	entry := &corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 22}
	entry.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x07})
	encoded, err := encodeAccountFrozen(entry)
	if err != nil {
		t.Fatal(err)
	}
	r := &frozenDemandReader{rows: map[string][]byte{string(accountFrozenBandwidthKey(0)): encoded}}
	s.setAccountKVLatestView(r, nil)
	obj := s.getStateObject(addr)
	read := func(s *StateDB, index uint32) *corepb.Account_Frozen {
		t.Helper()
		row, ok, err := s.accountFrozenBandwidthRowAt(s.getStateObject(addr), index)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return nil
		}
		return row.entry
	}
	if got := read(s, 0); !proto.Equal(got, entry) {
		t.Fatal(got)
	}
	if read(s, 1) != nil {
		t.Fatal("expected missing compatibility row")
	}
	reads := r.reads
	for repeat := 0; repeat < 3; repeat++ {
		got := read(s, 0)
		if !proto.Equal(got, entry) {
			t.Fatal(got)
		}
		got.FrozenBalance = 999
		got.ProtoReflect().SetUnknown(nil)
		if read(s, 1) != nil {
			t.Fatal("cached miss became present")
		}
	}
	if r.reads != reads {
		t.Fatalf("cached point reads reached durable store: %d -> %d", reads, r.reads)
	}
	if obj.accountFrozenBandwidthLoaded {
		t.Fatal("point cache materialized mutable account")
	}
	capture := func() TransactionReadSet {
		var recorder TransactionAccessRecorder
		recorder.Reset(1)
		s.SetTransactionAccessRecorder(&recorder)
		_ = read(s, 0)
		_ = read(s, 1)
		s.SetTransactionAccessRecorder(nil)
		return recorder.CaptureReadSet()
	}
	normalize := func(r TransactionReadSet) map[TransactionAccessKey]TransactionAccessMode {
		out := map[TransactionAccessKey]TransactionAccessMode{}
		for _, v := range r.Reads {
			out[v.Key] = v.Mode
		}
		return out
	}
	obj.accountFrozenBandwidthPoints = nil
	first := capture()
	second := capture()
	reads = r.reads
	if !reflect.DeepEqual(normalize(first), normalize(second)) || first.Unsupported {
		t.Fatalf("point dependency replay: %v %v", first, second)
	}
	for _, key := range []TransactionAccessKey{
		{Kind: TransactionAccessAccountKVGeneration, Address: addr},
		{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(0))},
		{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(1))},
	} {
		found := false
		for _, v := range first.Reads {
			if v.Key == key {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing dependency %+v", key)
		}
	}
	// Installing a materialized account must preserve independently owned point results.
	if err := s.materializeAccountFrozenBandwidthFast(obj); err != nil {
		t.Fatal(err)
	}
	obj.account.Proto().Frozen[0].FrozenBalance = 999
	if got := read(s, 0); !proto.Equal(got, entry) {
		t.Fatal("mutable Account aliased point cache", got)
	}
	if r.reads != reads {
		t.Fatal("materializer discarded compatibility results")
	}
	cp, err := s.Copy()
	if err != nil {
		t.Fatal(err)
	}
	if cp.getStateObject(addr).accountFrozenBandwidthPoints == obj.accountFrozenBandwidthPoints {
		t.Fatal("copy shares mutable cache container")
	}
	if err := cp.DeleteAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(0)); err != nil {
		t.Fatal(err)
	}
	if read(cp, 0) != nil || !proto.Equal(read(s, 0), entry) {
		t.Fatal("copy mutation leaked")
	}
	mark := s.Snapshot()
	replacement, _ := encodeAccountFrozen(&corepb.Account_Frozen{FrozenBalance: 33})
	if err := s.SetAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(1), replacement); err != nil {
		t.Fatal(err)
	}
	if got := read(s, 1); got == nil || got.FrozenBalance != 33 {
		t.Fatal("cached miss survived insertion", got)
	}
	s.RevertToSnapshot(mark)
	if read(s, 1) != nil || !proto.Equal(read(s, 0), entry) {
		t.Fatal("revert retained post-image")
	}
	mark = s.Snapshot()
	if err := s.ResetAccountKV(addr); err != nil {
		t.Fatal(err)
	}
	if read(s, 0) != nil {
		t.Fatal("reset retained old-generation point")
	}
	s.RevertToSnapshot(mark)
	if !proto.Equal(read(s, 0), entry) {
		t.Fatal("reset revert lost original row")
	}
	s.DeleteAccount(addr)
	s.CreateAccount(addr, corepb.AccountType_Normal)
	if read(s, 0) != nil || read(s, 1) != nil {
		t.Fatal("recreation retained old cache")
	}
}

func TestFrozenDemandCanonicalProvenance(t *testing.T) {
	for _, index := range []uint32{0, 1, 7} {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			s := newTestStateDB(t)
			addr := testAddr(0xe2)
			s.CreateAccount(addr, corepb.AccountType_Normal)
			value, _ := encodeAccountFrozen(&corepb.Account_Frozen{FrozenBalance: 44})
			if err := s.SetAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(index), value); err != nil {
				t.Fatal(err)
			}
			root, err := s.Commit()
			if err != nil {
				t.Fatal(err)
			}
			s, err = New(root, s.db)
			if err != nil {
				t.Fatal(err)
			}
			obj := s.getStateObject(addr)
			if err := s.materializeAccountFrozenBandwidthCanonical(obj); err != nil {
				t.Fatal(err)
			}
			if obj.accountFrozenBandwidthMode != accountFrozenBandwidthCanonical {
				t.Fatalf("canonical provenance = %d", obj.accountFrozenBandwidthMode)
			}
			// A canonical hit cannot prove the complete prefix, including index >=2.
			if err := s.materializeAccountFrozenBandwidth(obj); err != nil {
				t.Fatal(err)
			}
			if got := obj.account.TotalFrozenBandwidth(); got != 44 {
				t.Fatalf("complete amount=%d index=%d", got, index)
			}
			if obj.accountFrozenBandwidthMode != accountFrozenBandwidthComplete {
				t.Fatal("missing complete provenance")
			}
			var rec TransactionAccessRecorder
			rec.Reset(2)
			s.SetTransactionAccessRecorder(&rec)
			if err := s.materializeAccountFrozenBandwidthFast(obj); err != nil {
				t.Fatal(err)
			}
			if !rec.Unsupported() {
				t.Fatal("complete cache hit omitted prefix dependency")
			}
			s.SetTransactionAccessRecorder(nil)
			cp, err := s.Copy()
			if err != nil {
				t.Fatal(err)
			}
			if cp.getStateObject(addr).accountFrozenBandwidthMode != accountFrozenBandwidthComplete {
				t.Fatal("copy lost provenance")
			}
		})
	}
}

func TestFrozenDemandPointErrorsAreNotCached(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(0xe3)
	s.CreateAccount(addr, corepb.AccountType_Normal)
	root, err := s.Commit()
	if err != nil {
		t.Fatal(err)
	}
	s, err = New(root, s.db)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("frozen injected failure")
	r := &frozenDemandReader{failure: failure}
	s.setAccountKVLatestView(r, nil)
	obj := s.getStateObject(addr)
	for range 2 {
		if _, _, err := s.accountFrozenBandwidthRowAt(obj, 1); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	if r.reads != 2 || obj.accountFrozenBandwidthPoints != nil {
		t.Fatal("read error cached")
	}
	r.failure = nil
	r.rows = map[string][]byte{string(accountFrozenBandwidthKey(1)): {0xff}}
	for range 2 {
		if _, _, err := s.accountFrozenBandwidthRowAt(obj, 1); err == nil {
			t.Fatal("corrupt row accepted")
		}
	}
	if r.reads != 4 || obj.accountFrozenBandwidthPoints != nil {
		t.Fatal("decode error cached")
	}
}

func TestFrozenDemandVersionedReaderBypassesPointCache(t *testing.T) {
	for _, generation := range []bool{false, true} {
		t.Run(map[bool]string{false: "same_generation", true: "new_generation"}[generation], func(t *testing.T) {
			s := newTestStateDB(t)
			addr := testAddr(0xe4)
			s.CreateAccount(addr, corepb.AccountType_Normal)
			s.FreezeV1Bandwidth(addr, 11, 100)
			root, err := s.Commit()
			if err != nil {
				t.Fatal(err)
			}
			s, err = New(root, s.db)
			if err != nil {
				t.Fatal(err)
			}
			if got := s.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 11 {
				t.Fatal(got)
			}
			value, _ := encodeAccountFrozen(&corepb.Account_Frozen{FrozenBalance: 22, ExpireTime: 100})
			reader := testTransactionVersionedReader{{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(0))}: {{txIndex: 1, value: TransactionWriteValue{Exists: true, Value: value}}}}
			if generation {
				reader[TransactionAccessKey{Kind: TransactionAccessAccountKVGeneration, Address: addr}] = []testTransactionVersion{{txIndex: 1, value: testVersionedInt64(1)}}
			}
			s.SetTransactionVersionedValueReader(reader, 2)
			if got := s.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 22 {
				t.Fatalf("versioned row=%d", got)
			}
			reader[TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(0))}] = []testTransactionVersion{{txIndex: 1, value: TransactionWriteValue{Exists: false}}}
			s.SetTransactionVersionedValueReader(reader, 2)
			if got := s.FrozenV1BandwidthCount(addr); got != 0 {
				t.Fatalf("versioned tombstone count=%d", got)
			}
			// A local write still precedes the versioned tombstone.
			if err := s.SetAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(0), value); err != nil {
				t.Fatal(err)
			}
			if got := s.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 22 {
				t.Fatalf("dirty row lost to versioned tombstone: %d", got)
			}
		})
	}
}

func TestDemandResourceProjectionDependenciesAndErrors(t *testing.T) {
	for _, resource := range []corepb.ResourceCode{corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_ENERGY} {
		t.Run(resource.String(), func(t *testing.T) {
			s := newTestStateDB(t)
			addr := testAddr(0xe5)
			s.CreateAccount(addr, corepb.AccountType_Normal)
			s.FreezeV1Bandwidth(addr, 11, 100)
			s.FreezeV1Energy(addr, 12, 100)
			s.AddFreezeV2(addr, resource, 13)
			root, err := s.Commit()
			if err != nil {
				t.Fatal(err)
			}
			s, err = New(root, s.db)
			if err != nil {
				t.Fatal(err)
			}
			capture := func() (map[TransactionAccessKey]TransactionAccessMode, bool) {
				var rec TransactionAccessRecorder
				rec.Reset(2)
				s.SetTransactionAccessRecorder(&rec)
				if s.AccountDelegationResourceReference(addr, resource) == nil {
					t.Fatal(s.Error())
				}
				s.SetTransactionAccessRecorder(nil)
				out := map[TransactionAccessKey]TransactionAccessMode{}
				for _, v := range rec.CaptureReadSet().Reads {
					out[v.Key] = v.Mode
				}
				return out, rec.Unsupported()
			}
			cold, coldUnsupported := capture()
			hot, hotUnsupported := capture()
			// Cold materialization additionally records a conservative full envelope
			// read through the prefix iterator. All consumed exact cells and the range
			// marker remain required on every warm transaction.
			for _, key := range []TransactionAccessKey{
				{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldExistence},
				{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldFrozenResource},
				{Kind: TransactionAccessAccountKVGeneration, Address: addr},
				{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.AccountFrozenV2Aux, LogicalKey: string(accountFrozenV2Key(resource))},
				{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.AccountResourceAux, LogicalKey: string(accountResourceKey)},
			} {
				if cold[key] != TransactionAccessRead || hot[key] != TransactionAccessRead {
					t.Fatalf("missing consumed dependency %+v cold=%v hot=%v", key, cold, hot)
				}
			}
			if coldUnsupported != (resource == corepb.ResourceCode_BANDWIDTH) || hotUnsupported != coldUnsupported {
				t.Fatal("complete prefix provenance not replayed")
			}
			for key := range hot {
				if key.Kind == TransactionAccessAccountKV && key.KVDomain != kvdomains.AccountFrozenV2Aux && key.KVDomain != kvdomains.AccountResourceAux {
					t.Fatalf("unrelated point domain %+v", key)
				}
			}
			obj := s.getStateObject(addr)
			if obj.accountMapsLoaded || obj.accountPermissionsLoaded || obj.accountVotesLoaded || obj.accountStakeV2Loaded || obj.accountFrozenSupplyLoaded || obj.accountTronPowerLoaded {
				t.Fatal("projection loaded unrelated domain")
			}
		})
	}
	// Relevant row errors retain V1 -> requested V2 -> resource precedence.
	for _, first := range []kvdomains.KVDomain{kvdomains.AccountFrozenBandwidthAux, kvdomains.AccountFrozenV2Aux, kvdomains.AccountResourceAux} {
		t.Run(fmt.Sprintf("domain_%d", first), func(t *testing.T) {
			s := newTestStateDB(t)
			addr := testAddr(0xe6)
			s.CreateAccount(addr, corepb.AccountType_Normal)
			domains := []kvdomains.KVDomain{kvdomains.AccountFrozenBandwidthAux, kvdomains.AccountFrozenV2Aux, kvdomains.AccountResourceAux}
			active := false
			for _, domain := range domains {
				if domain == first {
					active = true
				}
				if !active {
					continue
				}
				key := accountResourceKey
				if domain == kvdomains.AccountFrozenBandwidthAux {
					key = accountFrozenBandwidthKey(0)
				} else if domain == kvdomains.AccountFrozenV2Aux {
					key = accountFrozenV2Key(corepb.ResourceCode_BANDWIDTH)
				}
				if err := s.SetAccountKV(addr, domain, key, []byte{0xff}); err != nil {
					t.Fatal(err)
				}
			}
			root, err := s.Commit()
			if err != nil {
				t.Fatal(err)
			}
			s, err = New(root, s.db)
			if err != nil {
				t.Fatal(err)
			}
			if s.AccountDelegationResourceReference(addr, corepb.ResourceCode_BANDWIDTH) != nil || s.Error() == nil {
				t.Fatal("corruption accepted")
			}
			want := map[kvdomains.KVDomain]string{kvdomains.AccountFrozenBandwidthAux: "frozen bandwidth", kvdomains.AccountFrozenV2Aux: "frozen-v2", kvdomains.AccountResourceAux: "account resource"}[first]
			if !strings.Contains(s.Error().Error(), want) {
				t.Fatalf("wrong first error: %v want %s", s.Error(), want)
			}
		})
	}
}

func TestFrozenDemandPublicMaterializerDependencyReplay(t *testing.T) {
	for _, api := range []string{"count", "total"} {
		t.Run(api, func(t *testing.T) {
			s := newTestStateDB(t)
			addr := testAddr(0xe7)
			s.CreateAccount(addr, corepb.AccountType_Normal)
			s.FreezeV1Bandwidth(addr, 11, 100)
			root, err := s.Commit()
			if err != nil {
				t.Fatal(err)
			}
			s, err = New(root, s.db)
			if err != nil {
				t.Fatal(err)
			}
			capture := func() map[TransactionAccessKey]TransactionAccessMode {
				var rec TransactionAccessRecorder
				rec.Reset(4)
				s.SetTransactionAccessRecorder(&rec)
				if api == "count" {
					if _, err := s.GetFreezeV1BandwidthCount(addr); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := s.GetAccountFrozenBandwidth(addr); err != nil {
						t.Fatal(err)
					}
				}
				s.SetTransactionAccessRecorder(nil)
				out := map[TransactionAccessKey]TransactionAccessMode{}
				for _, v := range rec.CaptureReadSet().Reads {
					out[v.Key] = v.Mode
				}
				return out
			}
			cold, hot := capture(), capture()
			if !reflect.DeepEqual(cold, hot) {
				t.Fatalf("public cold/hot dependencies differ: %v / %v", cold, hot)
			}
		})
	}
}

// A preloaded protobuf has existing upstream visibility after deletion. Keep
// that case separate: only the independent point cache is changed here.
func TestFrozenDemandDeletedVisibilityBoundaries(t *testing.T) {
	fixture := func(t *testing.T, materialized bool) (*StateDB, tcommon.Address) {
		t.Helper()
		s := newTestStateDB(t)
		addr := testAddr(0xe9)
		s.CreateAccount(addr, corepb.AccountType_Normal)
		s.FreezeV1Bandwidth(addr, 11, 100)
		root, err := s.Commit()
		if err != nil {
			t.Fatal(err)
		}
		s, err = New(root, s.db)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 11 {
			t.Fatal(got)
		}
		if materialized {
			if n, err := s.GetFreezeV1BandwidthCount(addr); err != nil || n != 1 {
				t.Fatal(n, err)
			}
		}
		return s, addr
	}
	for _, materialized := range []bool{false, true} {
		for _, consumer := range []string{"count", "balance_expire", "legacy_power"} {
			t.Run(fmt.Sprintf("materialized_%t/%s", materialized, consumer), func(t *testing.T) {
				s, addr := fixture(t, materialized)
				mark := s.Snapshot()
				s.DeleteAccount(addr)
				obj := s.getStateObject(addr)
				before := *obj.accountFrozenBandwidthPoints
				// Deleted direct probes must reach the normal deleted guard and must not
				// fill absence atop the old present mask or expose the old physical row.
				var rec TransactionAccessRecorder
				rec.Reset(4)
				s.SetTransactionAccessRecorder(&rec)
				if _, exists, err := s.accountFrozenBandwidthRowAt(obj, 0); err != nil || exists {
					t.Fatal("deleted point visible", exists, err)
				}
				s.SetTransactionAccessRecorder(nil)
				if *obj.accountFrozenBandwidthPoints != before {
					t.Fatal("deleted probe populated point cache")
				}
				required := map[TransactionAccessKey]bool{
					{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldExistence}:                                               false,
					{Kind: TransactionAccessAccountKVGeneration, Address: addr}:                                                                                        false,
					{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(0))}: false,
				}
				for _, read := range rec.CaptureReadSet().Reads {
					if _, ok := required[read.Key]; ok {
						required[read.Key] = true
					}
				}
				for key, found := range required {
					if !found {
						t.Fatalf("deleted point omitted dependency %+v", key)
					}
				}
				copy, err := s.Copy()
				if err != nil {
					t.Fatal(err)
				}
				for _, view := range []*StateDB{s, copy} {
					switch consumer {
					case "count":
						want := 0
						if materialized {
							want = 1
						}
						n, err := view.GetFreezeV1BandwidthCount(addr)
						if err != nil || n != want {
							t.Fatalf("deleted count=%d err=%v want=%d", n, err, want)
						}
					case "balance_expire":
						wantAmount, wantExpiry := int64(0), int64(0)
						if materialized {
							wantAmount, wantExpiry = 11, 100
						}
						amount, expiry := view.GetFreezeV1BalanceAndExpire(addr, 0)
						if amount != wantAmount || expiry != wantExpiry {
							t.Fatalf("deleted balance/expiry=%d/%d want=%d/%d", amount, expiry, wantAmount, wantExpiry)
						}
					case "legacy_power":
						if got := view.GetLegacyTronPower(addr); got != 0 {
							t.Fatalf("deleted legacy power=%d", got)
						}
					}
				}
				s.RevertToSnapshot(mark)
				if got := s.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 11 {
					t.Fatalf("delete revert amount=%d", got)
				}
				oldGeneration := s.getStateObject(addr).accountKVGeneration
				s.DeleteAccount(addr)
				s.CreateAccount(addr, corepb.AccountType_Normal)
				if s.getStateObject(addr).accountKVGeneration <= oldGeneration {
					t.Fatal("recreation did not advance generation")
				}
				if n, err := s.GetFreezeV1BandwidthCount(addr); err != nil || n != 0 {
					t.Fatal("recreate retained old point", n, err)
				}
				s.FreezeV1Bandwidth(addr, 33, 200)
				if got := s.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 33 {
					t.Fatal(got)
				}
			})
		}
	}
	for _, boundary := range []string{"transaction_finalize", "commit_finalize"} {
		t.Run(boundary, func(t *testing.T) {
			s, addr := fixture(t, false)
			mark := s.Snapshot()
			s.SelfDestruct(addr)
			// SELFDESTRUCT is visible until transaction/commit finalization, exactly as
			// upstream's generic account-KV reader (which checks deleted only).
			if got := s.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 11 {
				t.Fatalf("pre-finalize selfdestruct amount=%d", got)
			}
			if boundary == "transaction_finalize" {
				s.FinalizeTransaction()
				if n, err := s.GetFreezeV1BandwidthCount(addr); err != nil || n != 0 {
					t.Fatal("finalize retained point", n, err)
				}
				s.RevertToSnapshot(mark)
				if got := s.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 11 {
					t.Fatal("finalize revert lost row", got)
				}
			} else {
				if _, err := s.Commit(); err != nil {
					t.Fatal(err)
				}
				if obj := s.getStateObject(addr); obj != nil {
					if _, exists, err := s.accountFrozenBandwidthRowAt(obj, 0); err != nil || exists {
						t.Fatal("commit-finalized point visible", exists, err)
					}
				}
				if n, err := s.GetFreezeV1BandwidthCount(addr); err != nil || n != 0 {
					t.Fatal("commit finalize retained point", n, err)
				}
			}
		})
	}
}
