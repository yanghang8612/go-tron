package state

import (
	"strconv"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	sdb.SetPermissions(addr, owner, nil, nil)
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
}
