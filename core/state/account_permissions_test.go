package state

import (
	"bytes"
	"strconv"
	"testing"
	"unsafe"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func splitTestPermission(kind corepb.Permission_PermissionType, id int32, name string, marker byte) *corepb.Permission {
	return &corepb.Permission{
		Type:           kind,
		Id:             id,
		PermissionName: name,
		Threshold:      1,
		Keys: []*corepb.Key{{
			Address: []byte{0x41, marker},
			Weight:  1,
		}},
	}
}

func TestAccountPermissionsPersistOutsideAccountEnvelope(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x92)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	owner := splitTestPermission(corepb.Permission_Owner, 0, "owner", 0x01)
	witness := splitTestPermission(corepb.Permission_Witness, 1, "witness", 0x02)
	active2 := splitTestPermission(corepb.Permission_Active, 2, "active-2", 0x03)
	active3 := splitTestPermission(corepb.Permission_Active, 3, "active-3", 0x04)
	sdb.SetPermissions(addr, owner, witness, []*corepb.Permission{active3, active2})

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	raw, ok, err := rawdb.ReadStateAccountLatest(sdb.db.DiskDB(), addr)
	if err != nil || !ok {
		t.Fatalf("read account latest: ok=%v err=%v", ok, err)
	}
	envelope, err := DecodeStateAccountV3(raw)
	if err != nil {
		t.Fatal(err)
	}
	var stored corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.OwnerPermission != nil || stored.WitnessPermission != nil || len(stored.ActivePermission) != 0 {
		t.Fatalf("split permissions leaked into account envelope: %+v", &stored)
	}

	for _, test := range []struct {
		key  []byte
		want *corepb.Permission
	}{
		{accountOwnerPermissionKey, owner},
		{accountWitnessPermissionKey, witness},
		{accountActivePermissionKey(2), active2},
		{accountActivePermissionKey(3), active3},
	} {
		value, exists, readErr := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountPermissionAux, test.key)
		if readErr != nil || !exists {
			t.Fatalf("read permission row %x: exists=%v err=%v", test.key, exists, readErr)
		}
		var got corepb.Permission
		if err := proto.Unmarshal(value, &got); err != nil {
			t.Fatalf("decode permission row %x: %v", test.key, err)
		}
		if !proto.Equal(&got, test.want) {
			t.Fatalf("permission row %x = %+v, want %+v", test.key, &got, test.want)
		}
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	account := reopened.GetAccount(addr)
	if account == nil || !proto.Equal(account.OwnerPermission(), owner) || !proto.Equal(account.WitnessPermission(), witness) {
		t.Fatalf("materialized singleton permissions = %+v", account)
	}
	actives := account.ActivePermission()
	if len(actives) != 2 || actives[0].GetId() != 2 || actives[1].GetId() != 3 || !proto.Equal(actives[0], active2) || !proto.Equal(actives[1], active3) {
		t.Fatalf("materialized active permissions = %+v", actives)
	}
}

func TestAccountPermissionByIDDoesNotMaterializeSplitAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x96)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	owner := splitTestPermission(corepb.Permission_Owner, 0, "owner", 0x41)
	witness := splitTestPermission(corepb.Permission_Witness, 1, "witness", 0x42)
	active := splitTestPermission(corepb.Permission_Active, 3, "active-3", 0x43)
	sdb.SetPermissions(addr, owner, witness, []*corepb.Permission{active})
	if err := sdb.SetAccountKV(addr, kvdomains.AccountAssetV2, []byte("1000001"), encodeAccountAuxInt64(99)); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		id   int32
		want *corepb.Permission
	}{
		{id: 0, want: owner},
		{id: 1, want: witness},
		{id: 3, want: active},
		{id: 4, want: nil},
	} {
		got, lookupErr := reopened.AccountPermissionByID(addr, test.id)
		if lookupErr != nil {
			t.Fatalf("permission %d: %v", test.id, lookupErr)
		}
		if !proto.Equal(got, test.want) {
			t.Fatalf("permission %d = %+v, want %+v", test.id, got, test.want)
		}
	}
	obj := reopened.getStateObject(addr)
	if obj == nil {
		t.Fatal("account missing after permission lookup")
	}
	if obj.accountPermissionsLoaded || obj.accountMapsLoaded {
		t.Fatalf("point lookup materialized split account: permissions=%v maps=%v", obj.accountPermissionsLoaded, obj.accountMapsLoaded)
	}
	if pb := obj.account.Proto(); pb.OwnerPermission != nil || pb.WitnessPermission != nil || len(pb.ActivePermission) != 0 || len(pb.AssetV2) != 0 {
		t.Fatalf("point lookup populated account proto: %+v", pb)
	}
}

func TestAccountPermissionByIDCachesPointReadAndInvalidates(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x9a)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	owner1 := splitTestPermission(corepb.Permission_Owner, 0, "owner-1", 0x61)
	owner2 := splitTestPermission(corepb.Permission_Owner, 0, "owner-2", 0x62)
	sdb.SetPermissions(addr, owner1, nil, nil)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	index := &countingKVIndexStore{KeyValueStore: reopened.db.DiskDB()}
	reopened.SetAccountKVIndexStore(index)
	first, err := reopened.AccountPermissionByID(addr, 0)
	if err != nil || !proto.Equal(first, owner1) {
		t.Fatalf("first owner lookup = %+v err=%v", first, err)
	}
	second, err := reopened.AccountPermissionByID(addr, 0)
	if err != nil || second != first {
		t.Fatalf("cached owner lookup = %+v err=%v, want same pointer", second, err)
	}
	if got := index.getsByDomain[kvdomains.AccountPermissionAux]; got != 1 {
		t.Fatalf("two owner lookups read permission row %d times, want 1", got)
	}
	if _, err := reopened.Commit(); err != nil {
		t.Fatal(err)
	}
	third, err := reopened.AccountPermissionByID(addr, 0)
	if err != nil || third != first {
		t.Fatalf("adjacent-block cached owner lookup = %+v err=%v, want same pointer", third, err)
	}
	if got := index.getsByDomain[kvdomains.AccountPermissionAux]; got != 1 {
		t.Fatalf("adjacent-block owner lookup read permission row %d times, want 1", got)
	}

	snapshot := reopened.Snapshot()
	reopened.SetPermissions(addr, owner2, nil, nil)
	updated, err := reopened.AccountPermissionByID(addr, 0)
	if err != nil || !proto.Equal(updated, owner2) || updated == first {
		t.Fatalf("owner after permission write = %+v err=%v", updated, err)
	}
	reopened.RevertToSnapshot(snapshot)
	reverted, err := reopened.AccountPermissionByID(addr, 0)
	if err != nil || !proto.Equal(reverted, owner1) || reverted == first {
		t.Fatalf("owner after permission revert = %+v err=%v", reverted, err)
	}

	missing1, err := reopened.AccountPermissionByID(addr, 9)
	if err != nil || missing1 != nil {
		t.Fatalf("first missing lookup = %+v err=%v", missing1, err)
	}
	readsAfterMissing := index.getsByDomain[kvdomains.AccountPermissionAux]
	missing2, err := reopened.AccountPermissionByID(addr, 9)
	if err != nil || missing2 != nil {
		t.Fatalf("cached missing lookup = %+v err=%v", missing2, err)
	}
	if got := index.getsByDomain[kvdomains.AccountPermissionAux]; got != readsAfterMissing {
		t.Fatalf("cached missing lookup added durable read: got %d want %d", got, readsAfterMissing)
	}
}

func TestWitnessPermissionAddressCachesPointReadAndInvalidates(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x98)
	delegated1 := testAddr(0xa1)
	delegated2 := testAddr(0xa2)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	witness1 := splitTestPermission(corepb.Permission_Witness, 1, "witness-1", 0xa1)
	witness1.Keys[0].Address = delegated1.Bytes()
	sdb.SetPermissions(addr, nil, witness1, nil)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	index := &countingKVIndexStore{KeyValueStore: reopened.db.DiskDB()}
	reopened.SetAccountKVIndexStore(index)

	for i := 0; i < 2; i++ {
		if got := reopened.WitnessPermissionAddress(addr); got != delegated1 {
			t.Fatalf("cached signer lookup %d = %x, want %x", i, got, delegated1)
		}
	}
	if got := index.getsByDomain[kvdomains.AccountPermissionAux]; got != 1 {
		t.Fatalf("two signer lookups read permission row %d times, want 1", got)
	}
	obj := reopened.getStateObject(addr)
	if obj == nil || !obj.witnessPermissionSignerLoaded || obj.accountPermissionsLoaded {
		t.Fatalf("point cache state = obj:%v signerLoaded:%v permissionsLoaded:%v", obj != nil, obj != nil && obj.witnessPermissionSignerLoaded, obj != nil && obj.accountPermissionsLoaded)
	}

	snapshot := reopened.Snapshot()
	witness2 := splitTestPermission(corepb.Permission_Witness, 1, "witness-2", 0xa2)
	witness2.Keys[0].Address = delegated2.Bytes()
	reopened.SetPermissions(addr, nil, witness2, nil)
	if got := reopened.WitnessPermissionAddress(addr); got != delegated2 {
		t.Fatalf("signer after permission write = %x, want %x", got, delegated2)
	}
	reopened.RevertToSnapshot(snapshot)
	if got := reopened.WitnessPermissionAddress(addr); got != delegated1 {
		t.Fatalf("signer after permission revert = %x, want %x", got, delegated1)
	}

	resetSnapshot := reopened.Snapshot()
	if err := reopened.ResetAccountKV(addr); err != nil {
		t.Fatal(err)
	}
	if got := reopened.WitnessPermissionAddress(addr); got != addr {
		t.Fatalf("signer after KV generation reset = %x, want fallback %x", got, addr)
	}
	reopened.RevertToSnapshot(resetSnapshot)
	if got := reopened.WitnessPermissionAddress(addr); got != delegated1 {
		t.Fatalf("signer after generation revert = %x, want %x", got, delegated1)
	}
}

func TestAccountPermissionsSnapshotRevertInvalidatesMaterializedCache(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x93)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	owner1 := splitTestPermission(corepb.Permission_Owner, 0, "owner-1", 0x11)
	owner2 := splitTestPermission(corepb.Permission_Owner, 0, "owner-2", 0x12)
	active2 := splitTestPermission(corepb.Permission_Active, 2, "active-2", 0x13)
	active3 := splitTestPermission(corepb.Permission_Active, 3, "active-3", 0x14)
	sdb.SetPermissions(addr, owner1, nil, []*corepb.Permission{active2})
	if got := sdb.GetAccount(addr); got == nil || !proto.Equal(got.OwnerPermission(), owner1) {
		t.Fatalf("initial permissions = %+v", got)
	}

	snapshot := sdb.Snapshot()
	sdb.SetPermissions(addr, owner2, nil, []*corepb.Permission{active3})
	if got := sdb.GetAccount(addr); got == nil || !proto.Equal(got.OwnerPermission(), owner2) || len(got.ActivePermission()) != 1 || got.ActivePermission()[0].GetId() != 3 {
		t.Fatalf("updated permissions = %+v", got)
	}
	sdb.RevertToSnapshot(snapshot)

	got := sdb.GetAccount(addr)
	if got == nil || !proto.Equal(got.OwnerPermission(), owner1) || got.WitnessPermission() != nil {
		t.Fatalf("permissions after revert = %+v", got)
	}
	if actives := got.ActivePermission(); len(actives) != 1 || !proto.Equal(actives[0], active2) {
		t.Fatalf("active permissions after revert = %+v", actives)
	}
}

func TestAccountPermissionsReplaceRemovesStaleActiveRows(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x95)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	owner := splitTestPermission(corepb.Permission_Owner, 0, "owner", 0x31)
	active2 := splitTestPermission(corepb.Permission_Active, 2, "active-2", 0x32)
	active3 := splitTestPermission(corepb.Permission_Active, 3, "active-3", 0x33)
	sdb.SetPermissions(addr, owner, nil, []*corepb.Permission{active2, active3})
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	active3Updated := splitTestPermission(corepb.Permission_Active, 3, "active-3-updated", 0x34)
	reopened.SetPermissions(addr, owner, nil, []*corepb.Permission{active3Updated})
	root2, err := reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if _, exists, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountPermissionAux, accountActivePermissionKey(2)); err != nil || exists {
		t.Fatalf("removed active permission still stored: exists=%v err=%v", exists, err)
	}

	reopenedAgain, err := New(root2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	account := reopenedAgain.GetAccount(addr)
	if account == nil {
		t.Fatal("account missing after permission replacement")
	}
	if actives := account.ActivePermission(); len(actives) != 1 || !proto.Equal(actives[0], active3Updated) {
		t.Fatalf("active permissions after replacement = %+v", actives)
	}
}

func TestApplyDefaultPermissionsCreatedOverwritesDirtyAndReverts(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x9b)
	dp := NewDynamicProperties()
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	owner := splitTestPermission(corepb.Permission_Owner, 0, "custom-owner", 0x71)
	witness := splitTestPermission(corepb.Permission_Witness, 1, "custom-witness", 0x72)
	active2 := splitTestPermission(corepb.Permission_Active, 2, "custom-active-2", 0x73)
	active3 := splitTestPermission(corepb.Permission_Active, 3, "custom-active-3", 0x74)
	sdb.SetPermissions(addr, owner, witness, []*corepb.Permission{active2, active3})
	snapshot := sdb.Snapshot()

	sdb.ApplyDefaultAccountPermissions(addr, dp)
	got := sdb.GetAccount(addr)
	if got == nil || got.OwnerPermission().GetPermissionName() != "owner" || got.WitnessPermission() != nil {
		t.Fatalf("default singleton permissions = %+v", got)
	}
	if actives := got.ActivePermission(); len(actives) != 1 || actives[0].GetId() != 2 || actives[0].GetPermissionName() != "active" {
		t.Fatalf("default active permissions = %+v", actives)
	}

	sdb.RevertToSnapshot(snapshot)
	got = sdb.GetAccount(addr)
	if got == nil || !proto.Equal(got.OwnerPermission(), owner) || !proto.Equal(got.WitnessPermission(), witness) {
		t.Fatalf("reverted singleton permissions = %+v", got)
	}
	if actives := got.ActivePermission(); len(actives) != 2 || !proto.Equal(actives[0], active2) || !proto.Equal(actives[1], active3) {
		t.Fatalf("reverted active permissions = %+v", actives)
	}
}

func TestApplyDefaultPermissionsRecreatedGenerationDoesNotLeak(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x9c)
	dp := NewDynamicProperties()
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	oldOwner := splitTestPermission(corepb.Permission_Owner, 0, "old-owner", 0x81)
	oldActive := splitTestPermission(corepb.Permission_Active, 3, "old-active", 0x82)
	sdb.SetPermissions(addr, oldOwner, nil, []*corepb.Permission{oldActive})
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	reopened.DeleteAccount(addr)
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	reopened.CreateAccount(addr, corepb.AccountType_Normal)
	obj := reopened.getStateObject(addr)
	if obj == nil || !obj.created || obj.accountKVGeneration != 1 {
		t.Fatalf("recreated object = %+v", obj)
	}
	reopened.ApplyDefaultAccountPermissions(addr, dp)
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	got := reopened.GetAccount(addr)
	if got == nil || got.OwnerPermission().GetPermissionName() != "owner" {
		t.Fatalf("recreated singleton permissions = %+v", got)
	}
	if actives := got.ActivePermission(); len(actives) != 1 || actives[0].GetId() != 2 {
		t.Fatalf("old generation leaked into recreated account: %+v", actives)
	}
}

func BenchmarkAccountPermissionLookup(b *testing.B) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		b.Fatal(err)
	}
	addr := testAddr(0x97)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	owner := splitTestPermission(corepb.Permission_Owner, 0, "owner", 0x51)
	witness := splitTestPermission(corepb.Permission_Witness, 1, "witness", 0x52)
	sdb.SetPermissions(addr, owner, witness, nil)
	for i := 0; i < 128; i++ {
		key := []byte(strconv.Itoa(1_000_000 + i))
		if err := sdb.SetAccountKV(addr, kvdomains.AccountAssetV2, key, encodeAccountAuxInt64(int64(i+1))); err != nil {
			b.Fatal(err)
		}
	}
	root, err := sdb.Commit()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("point-read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, err := New(root, db)
			if err != nil {
				b.Fatal(err)
			}
			permission, err := view.AccountPermissionByID(addr, 0)
			if err != nil || permission == nil {
				b.Fatalf("permission lookup: permission=%+v err=%v", permission, err)
			}
		}
	})
	b.Run("full-account", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, err := New(root, db)
			if err != nil {
				b.Fatal(err)
			}
			if account := view.GetAccount(addr); account == nil || account.OwnerPermission() == nil {
				b.Fatal("account permission missing")
			}
		}
	})
	b.Run("cached-witness-signer", func(b *testing.B) {
		view, err := New(root, db)
		if err != nil {
			b.Fatal(err)
		}
		want := tcommon.BytesToAddress(witness.Keys[0].Address)
		if got := view.WitnessPermissionAddress(addr); got != want {
			b.Fatalf("warm signer lookup = %x, want %x", got, want)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if got := view.WitnessPermissionAddress(addr); got != want {
				b.Fatalf("cached signer lookup = %x, want %x", got, want)
			}
		}
	})
	b.Run("cached-point-read", func(b *testing.B) {
		view, err := New(root, db)
		if err != nil {
			b.Fatal(err)
		}
		if permission, err := view.AccountPermissionByID(addr, 0); err != nil || permission == nil {
			b.Fatalf("warm permission lookup: permission=%+v err=%v", permission, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if permission, err := view.AccountPermissionByID(addr, 0); err != nil || permission == nil {
				b.Fatalf("cached permission lookup: permission=%+v err=%v", permission, err)
			}
		}
	})
}

var benchmarkDecodedAccountPermission *corepb.Permission

func BenchmarkDecodeAccountPermissionRow(b *testing.B) {
	for _, fixture := range []struct {
		name       string
		permission *corepb.Permission
	}{
		{name: "owner-one-key", permission: splitTestPermission(corepb.Permission_Owner, 0, "owner", 0x71)},
		{name: "active-one-key", permission: func() *corepb.Permission {
			permission := splitTestPermission(corepb.Permission_Active, 2, "active", 0x72)
			permission.Operations = make([]byte, 32)
			return permission
		}()},
		{name: "active-four-keys", permission: func() *corepb.Permission {
			permission := splitTestPermission(corepb.Permission_Active, 2, "active", 0x73)
			permission.Operations = make([]byte, 32)
			for i := 1; i < 4; i++ {
				permission.Keys = append(permission.Keys, &corepb.Key{Address: []byte{0x41, byte(0x73 + i)}, Weight: 1})
			}
			return permission
		}()},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			value, err := proto.MarshalOptions{Deterministic: true}.Marshal(fixture.permission)
			if err != nil {
				b.Fatal(err)
			}
			key := accountOwnerPermissionKey
			if fixture.permission.Id >= 2 {
				key = accountActivePermissionKey(fixture.permission.Id)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				permission, _, err := decodeAccountPermissionRow(key, value)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkDecodedAccountPermission = permission
			}
		})
	}
}

func TestDecodeAccountPermissionRowOwnsCoalescedBytes(t *testing.T) {
	permission := splitTestPermission(corepb.Permission_Active, 2, "active-arena", 0x84)
	permission.Operations = bytes.Repeat([]byte{0xa5}, 32)
	permission.Keys[0].Address = append([]byte{0x41}, bytes.Repeat([]byte{0x84}, 20)...)
	permission.Keys = append(permission.Keys, &corepb.Key{
		Address: append([]byte{0x41}, bytes.Repeat([]byte{0x85}, 20)...),
		Weight:  2,
	})
	permission.Keys[1].ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 7))
	permission.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 100, protowire.BytesType), []byte("future")))

	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(permission)
	if err != nil {
		t.Fatal(err)
	}
	want := new(corepb.Permission)
	if err := proto.Unmarshal(raw, want); err != nil {
		t.Fatal(err)
	}
	got, _, err := decodeAccountPermissionRow(accountActivePermissionKey(2), raw)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("decoded permission mismatch\n got: %v\nwant: %v", got, want)
	}

	parts := []struct {
		name  string
		start uintptr
		len   int
	}{
		{name: "permission name", start: uintptr(unsafe.Pointer(unsafe.StringData(got.PermissionName))), len: len(got.PermissionName)},
		{name: "operations", start: uintptr(unsafe.Pointer(unsafe.SliceData(got.Operations))), len: len(got.Operations)},
		{name: "first address", start: uintptr(unsafe.Pointer(unsafe.SliceData(got.Keys[0].Address))), len: len(got.Keys[0].Address)},
		{name: "second address", start: uintptr(unsafe.Pointer(unsafe.SliceData(got.Keys[1].Address))), len: len(got.Keys[1].Address)},
	}
	for index := 1; index < len(parts); index++ {
		if parts[index].start != parts[index-1].start+uintptr(parts[index-1].len) {
			t.Fatalf("%s does not immediately follow %s in the owned arena", parts[index].name, parts[index-1].name)
		}
	}
	for _, key := range got.Keys {
		if cap(key.Address) != len(key.Address) {
			t.Fatalf("address capacity %d, want owned span %d", cap(key.Address), len(key.Address))
		}
	}
	if cap(got.Operations) != len(got.Operations) {
		t.Fatalf("operations capacity %d, want owned span %d", cap(got.Operations), len(got.Operations))
	}

	for index := range raw {
		raw[index] ^= 0xff
	}
	if !proto.Equal(got, want) {
		t.Fatal("decoded permission aliases its input wire buffer")
	}
}

func TestDecodeAccountPermissionDuplicateBytesKeepLastValue(t *testing.T) {
	keyRaw := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), []byte("first-address"))
	keyRaw = protowire.AppendBytes(protowire.AppendTag(keyRaw, 1, protowire.BytesType), []byte("last-address"))
	keyRaw = protowire.AppendVarint(protowire.AppendTag(keyRaw, 2, protowire.VarintType), 3)

	raw := protowire.AppendBytes(protowire.AppendTag(nil, 3, protowire.BytesType), []byte("first-name"))
	raw = protowire.AppendBytes(protowire.AppendTag(raw, 6, protowire.BytesType), []byte("first-operations"))
	raw = protowire.AppendBytes(protowire.AppendTag(raw, 3, protowire.BytesType), []byte("last-name"))
	raw = protowire.AppendBytes(protowire.AppendTag(raw, 6, protowire.BytesType), []byte("last-operations"))
	raw = protowire.AppendBytes(protowire.AppendTag(raw, 7, protowire.BytesType), keyRaw)

	want := new(corepb.Permission)
	if err := proto.Unmarshal(raw, want); err != nil {
		t.Fatal(err)
	}
	got, err := decodeAccountPermission(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("duplicate-field mismatch\n got: %v\nwant: %v", got, want)
	}
	if got.PermissionName != "last-name" || string(got.Operations) != "last-operations" || len(got.Keys) != 1 || string(got.Keys[0].Address) != "last-address" {
		t.Fatalf("last values not retained: %v", got)
	}
}

func TestDecodeAccountPermissionMalformedMatchesGeneratedError(t *testing.T) {
	for _, raw := range [][]byte{
		{0x80},
		protowire.AppendBytes(protowire.AppendTag(nil, 3, protowire.BytesType), []byte{0xff}),
		protowire.AppendBytes(protowire.AppendTag(nil, 7, protowire.BytesType), []byte{0x80}),
	} {
		want := new(corepb.Permission)
		wantErr := proto.Unmarshal(raw, want)
		_, gotErr := decodeAccountPermission(raw)
		if wantErr == nil || gotErr == nil {
			t.Fatalf("malformed %x errors: optimized=%v generated=%v", raw, gotErr, wantErr)
		}
		if gotErr.Error() != wantErr.Error() {
			t.Fatalf("malformed %x error mismatch: optimized=%q generated=%q", raw, gotErr, wantErr)
		}
	}
}

func FuzzDecodeAccountPermissionRowEquivalent(f *testing.F) {
	for _, permission := range []*corepb.Permission{
		splitTestPermission(corepb.Permission_Owner, 0, "owner", 0x81),
		func() *corepb.Permission {
			permission := splitTestPermission(corepb.Permission_Active, 2, "active", 0x82)
			permission.Operations = make([]byte, 32)
			permission.Keys = append(permission.Keys, &corepb.Key{Address: []byte{0x41, 0x83}, Weight: 2})
			return permission
		}(),
	} {
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(permission)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add([]byte{0x3a, 0x05, 0x0a})
	f.Add(protowire.AppendBytes(protowire.AppendTag(nil, 99, protowire.BytesType), []byte("future")))
	f.Add([]byte{0x9b, 0x06, 0x08, 0x01, 0x9c, 0x06})

	f.Fuzz(func(t *testing.T, data []byte) {
		want := new(corepb.Permission)
		genericErr := proto.Unmarshal(data, want)
		got, _, directErr := decodeAccountPermissionRow(accountOwnerPermissionKey, data)
		if (genericErr != nil) != (directErr != nil) {
			t.Fatalf("error mismatch: generic=%v direct=%v data=%x", genericErr, directErr, data)
		}
		if genericErr == nil && !proto.Equal(got, want) {
			t.Fatalf("decoded permission mismatch\ngot:  %v\nwant: %v\ndata: %x", got, want, data)
		}
	})
}
