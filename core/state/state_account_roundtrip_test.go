package state

import (
	"bytes"
	"runtime"
	"strconv"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestAccountSurvivesCommitReopen(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x22)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 9999)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	obj := reopened.getStateObject(addr)
	if obj == nil {
		t.Fatal("account not found after reopen")
	}
	if got := obj.account.Balance(); got != 9999 {
		t.Fatalf("balance = %d, want 9999", got)
	}
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("accountKVRoot = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 0 {
		t.Fatalf("accountKVGeneration = %d, want 0", obj.accountKVGeneration)
	}
}

func TestLoadedAccountProtoReusedThenBoundedAtCommit(t *testing.T) {
	source := newTestStateDB(t)
	addr := testAddr(0x23)
	source.CreateAccount(addr, corepb.AccountType_Normal)
	source.GetStateObject(addr).Proto().AssetV2 = map[string]int64{"1000001": 7}
	root, err := source.Commit()
	if err != nil {
		t.Fatalf("commit source: %v", err)
	}
	raw, ok, err := source.readStateAccountLatest(addr)
	if err != nil || !ok {
		t.Fatalf("read source envelope: ok=%v err=%v", ok, err)
	}
	envelope, err := DecodeStateAccountV2(raw)
	if err != nil {
		t.Fatalf("decode source envelope: %v", err)
	}

	// A first load retains the exact durable proto for a possible same-block
	// journal pre-image. A non-scalar mutation must consume those bytes without
	// re-marshaling them.
	mutated, err := New(root, source.db)
	if err != nil {
		t.Fatal(err)
	}
	obj := mutated.getStateObject(addr)
	if obj == nil {
		t.Fatal("loaded account missing")
	}
	if !obj.accountProtoLoaded || !bytes.Equal(obj.accountProto, envelope.AccountProto) {
		t.Fatalf("loaded proto = loaded:%v bytes:%x, want durable %x", obj.accountProtoLoaded, obj.accountProto, envelope.AccountProto)
	}
	snapshot := mutated.Snapshot()
	mutated.SetAccountName(addr, "changed")
	if obj.accountProtoLoaded || obj.accountProto != nil {
		t.Fatal("account mutation did not consume/invalidate the loaded proto")
	}
	change, ok := mutated.journal.entries[len(mutated.journal.entries)-1].(accountChange)
	if !ok || !bytes.Equal(change.prev, envelope.AccountProto) {
		t.Fatalf("journal pre-image = %x, want durable proto %x", change.prev, envelope.AccountProto)
	}
	if !change.prevProtoLoaded {
		t.Fatal("journal did not remember that the pre-image came from the durable envelope")
	}
	mutated.RevertToSnapshot(snapshot)
	if !obj.accountProtoLoaded || !bytes.Equal(obj.accountProto, envelope.AccountProto) {
		t.Fatal("revert did not restore the bounded loaded-proto marker")
	}
	mutated.SetAccountName(addr, "changed")
	// A read-only load has no journal consumer. Its duplicate encoded form is
	// released at the successful block boundary instead of accumulating for the
	// lifetime of the reused range StateDB.
	readOnly, err := New(root, source.db)
	if err != nil {
		t.Fatal(err)
	}
	readOnlyObj := readOnly.getStateObject(addr)
	if readOnlyObj == nil || !readOnlyObj.accountProtoLoaded {
		t.Fatal("read-only account did not retain its same-block proto")
	}
	if _, err := readOnly.Commit(); err != nil {
		t.Fatalf("commit read-only block: %v", err)
	}
	if readOnlyObj.accountProto != nil || readOnlyObj.accountProtoLoaded {
		t.Fatal("read-only loaded proto survived the commit boundary")
	}
	if len(readOnly.loadedAccountProtoObjects) != 0 {
		t.Fatalf("loaded proto tracker retained %d objects", len(readOnly.loadedAccountProtoObjects))
	}

	if _, err := mutated.Commit(); err != nil {
		t.Fatalf("commit mutation: %v", err)
	}
	if obj.accountProto == nil || obj.accountProtoLoaded {
		t.Fatal("committed post-image should remain cached as a generated proto")
	}
}

func BenchmarkLoadedAccountFirstJournal(b *testing.B) {
	diskdb := ethrawdb.NewMemoryDatabase()
	source, err := New(tcommon.Hash(ethtypes.EmptyRootHash), NewDatabase(diskdb))
	if err != nil {
		b.Fatal(err)
	}
	addr := testAddr(0x24)
	source.CreateAccount(addr, corepb.AccountType_Normal)
	pb := source.GetStateObject(addr).Proto()
	pb.Asset = make(map[string]int64, 256)
	pb.AssetV2 = make(map[string]int64, 256)
	pb.LatestAssetOperationTime = make(map[string]int64, 256)
	pb.FreeAssetNetUsage = make(map[string]int64, 256)
	pb.LatestAssetOperationTimeV2 = make(map[string]int64, 256)
	pb.FreeAssetNetUsageV2 = make(map[string]int64, 256)
	for i := 0; i < 256; i++ {
		key := strconv.Itoa(1_000_000 + i)
		pb.Asset[key] = int64(i)
		pb.AssetV2[key] = int64(i + 1)
		pb.LatestAssetOperationTime[key] = int64(i + 2)
		pb.FreeAssetNetUsage[key] = int64(i + 3)
		pb.LatestAssetOperationTimeV2[key] = int64(i + 4)
		pb.FreeAssetNetUsageV2[key] = int64(i + 5)
	}
	root, err := source.Commit()
	if err != nil {
		b.Fatal(err)
	}

	for _, tc := range []struct {
		name      string
		clearSeed bool
	}{
		{name: "remarshal", clearSeed: true},
		{name: "reuse_envelope"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sdb, err := New(root, source.db)
				if err != nil {
					b.Fatal(err)
				}
				obj := sdb.getStateObject(addr)
				if tc.clearSeed {
					obj.invalidateAccountProto()
				}
				sdb.SetAccountName(addr, "changed")
				runtime.KeepAlive(sdb)
			}
		})
	}
}
