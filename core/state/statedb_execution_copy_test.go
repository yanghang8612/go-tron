package state

import (
	"bytes"
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestStateObjectCopyFieldPolicy is deliberately exhaustive. stateObject is a
// private execution snapshot, so adding a field without deciding whether it is
// copied, independently cloned, recomputed, rebound, or reset can silently
// weaken the canonical-boundary serial oracle.
func TestStateObjectCopyFieldPolicy(t *testing.T) {
	policies := map[string]string{
		"address": "copy", "account": "deep-copy",
		"wrapperEscaped": "reset-private-wrapper", "cacheTouched": "reset-working-set",
		"cacheGeneration": "reset-working-set", "accountProto": "deep-copy-or-reencode",
		"accountProtoLoaded": "reset-owned-by-copy", "dirty": "copy", "accountDirty": "copy",
		"accountMapsLoaded": "copy", "accountPermissionsLoaded": "copy",
		"accountPermissionPoint": "deep-copy", "accountPermissionPointID": "copy",
		"accountPermissionPointLoaded": "copy", "witnessPermissionSigner": "copy",
		"witnessPermissionSignerLoaded": "copy", "accountVotesLoaded": "copy",
		"accountStakeV2Loaded": "copy", "accountFrozenSupplyLoaded": "copy",
		"accountResourceLoaded": "copy", "accountFrozenBandwidthLoaded": "copy",
		"accountTronPowerLoaded": "copy", "trc10PointDomain": "copy",
		"trc10PointKey": "copy-immutable", "trc10PointValue": "copy",
		"trc10PointExists": "copy", "trc10PointLoaded": "copy",
		"accountFrozenBandwidthCanonicalPooled": "reset-deep-copy-not-pooled",
		"accountFrozenV2PointLoaded":            "copy", "accountFrozenV2PointExists": "copy",
		"accountFrozenV2PointAmounts": "copy", "deleted": "copy", "created": "copy",
		"code": "deep-copy", "codeHash": "copy", "codeDirty": "copy",
		"contractMeta": "deep-copy", "contractMetaDirty": "copy",
		"contractRuntime": "copy", "contractRuntimeLoaded": "copy",
		"contractRuntimeExists": "copy", "storageKeyPrefix": "copy",
		"storageKeyLayoutCached": "copy", "storageKeyHashSlot": "copy",
		"storage": "deep-copy", "storageHighWater": "recompute",
		"dirtyStorage": "deep-copy", "dirtyStorageHighWater": "recompute",
		"selfDestructed": "copy", "accountKVRoot": "copy",
		"accountKVGeneration": "copy", "accountKVGenerationDirty": "copy",
		"kvDirty": "deep-copy", "kvDirtyHighWater": "recompute", "dirtySet": "rebind",
	}
	typ := reflect.TypeOf(stateObject{})
	if len(policies) != typ.NumField() {
		t.Fatalf("stateObject copy policy covers %d fields, struct has %d", len(policies), typ.NumField())
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i).Name
		if policies[field] == "" {
			t.Fatalf("stateObject field %q has no copy policy", field)
		}
	}
}

// TestStateDBCopyTopLevelFieldPolicy complements the stateObject policy above.
// Execution-local journals, metrics and mutable external sinks intentionally
// restart; every value that changes consensus reads must be copied or shared
// only under an immutable-reader contract.
func TestStateDBCopyTopLevelFieldPolicy(t *testing.T) {
	policies := map[string]string{
		"db": "share-database", "dbErr": "copy-fail-closed",
		"stateObjects": "deep-copy", "witnesses": "deep-copy",
		"transactionVersionedReader":         "share-immutable-reader",
		"transactionVersionedTxIndex":        "copy",
		"transactionVersionedHydrated":       "deep-copy",
		"transactionVersionedMissing":        "deep-copy",
		"transactionVersionedStorageChecked": "deep-copy",
		"trc10TokenKeyScratch":               "reset-scratch", "assetIDKeyScratch": "reset-scratch",
		"assetBytesKeyScratch": "reset-scratch", "assetBandwidthKeyScratch": "reset-scratch",
		"accountUint32KeyScratch": "reset-scratch", "witnessCapsuleKeys": "reset-cache",
		"accountRowMarshalScratch": "reset-scratch", "delegRewardKeyScratch": "reset-scratch",
		"lastStateObject": "reset-cache", "touchedStateObjects": "reset-working-set",
		"retainedStateObjects": "reset-working-set", "olderRetainedStateObjects": "reset-working-set",
		"oldestRetainedStateObjects": "reset-working-set", "ancientRetainedStateObjects": "reset-working-set",
		"stateObjectWorkingGeneration": "reset-working-set",
		"storageObservability":         "reset-metrics", "oversizedStorageSamples": "reset-metrics",
		"loadedAccountProtoObjects": "reset-owned-copy-protos",
		"dirtyWitnesses":            "deep-copy", "standbyWitnessVersion": "copy",
		"txFinalizeDirty": "deep-copy", "dirtyObjects": "rebuild",
		"accountCommitPlans": "reset-workspace", "kvEntryArena": "reset-owned-copy-values",
		"journal": "fresh", "snapshots": "reset", "balanceTrace": "fresh",
		"balanceTraceSnapshots": "reset", "accountJournalPos": "reset",
		"transientStorage": "deep-copy", "domainChangeNoJournal": "reset-temporal-capture",
		"dynProps":          "share-requires-explicit-copy-before-execution",
		"transactionAccess": "reset-recorder", "originRoot": "copy",
		"deferFold": "reset-commit-control", "capturedFold": "reset-commit-control",
		"accountKVIndexStore": "share-reader-writer", "accountKVLatestReader": "share-immutable-reader",
		"accountKVLatestIterator": "share-immutable-reader", "flatLatestReader": "share-immutable-reader",
		"changeSet": "reset-temporal-capture", "codeStore": "share-code-store",
		"codeColdHistory": "share-immutable-reader", "codeColdTxNum": "copy",
		"commitmentColdHistory": "share-immutable-reader", "commitmentColdTxNum": "copy",
		"cycleRewardSink": "reset-external-mutable-sink",
	}
	typ := reflect.TypeOf(StateDB{})
	if len(policies) != typ.NumField() {
		t.Fatalf("StateDB copy policy covers %d fields, struct has %d", len(policies), typ.NumField())
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i).Name
		if policies[field] == "" {
			t.Fatalf("StateDB field %q has no copy policy", field)
		}
	}
}

func TestStateDBCopiesPreserveMaterializedSnapshotCaches(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x35)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	obj := sdb.getStateObject(addr)
	obj.account.Proto().AccountResource = &corepb.Account_AccountResource{EnergyUsage: 77}
	obj.accountMapsLoaded = true
	obj.accountPermissionsLoaded = true
	obj.accountPermissionPoint = &corepb.Permission{Id: 3, PermissionName: "point", Keys: []*corepb.Key{{Address: []byte{0x41, 0x99}, Weight: 2}}}
	obj.accountPermissionPointID = 3
	obj.accountPermissionPointLoaded = true
	obj.witnessPermissionSigner = testAddr(0x36)
	obj.witnessPermissionSignerLoaded = true
	obj.accountVotesLoaded = true
	obj.accountStakeV2Loaded = true
	obj.accountFrozenSupplyLoaded = true
	obj.accountResourceLoaded = true
	obj.accountFrozenBandwidthLoaded = true
	obj.accountTronPowerLoaded = true
	obj.trc10PointDomain = kvdomains.AccountAssetV2
	obj.trc10PointKey = "1000001"
	obj.trc10PointValue = 123
	obj.trc10PointExists = true
	obj.trc10PointLoaded = true
	obj.accountFrozenV2PointLoaded = 0b111
	obj.accountFrozenV2PointExists = 0b101
	obj.accountFrozenV2PointAmounts = [3]int64{11, 22, 33}
	obj.contractRuntime = ContractRuntimeMetadata{
		OriginAddress: testAddr(0x37), ConsumeUserResourcePercent: 41,
		OriginEnergyLimit: 42, Version: 43,
		storageKeyPrefix: [storageKeyPrefixBytes]byte{0x51}, storageKeyHashSlot: true,
	}
	obj.contractRuntimeLoaded = true
	obj.contractRuntimeExists = true
	// A newly-created object carries a metadata deletion intent. Clear it to
	// model a clean lightweight runtime cache without materializing the full
	// SmartContract protobuf.
	obj.contractMetaDirty = false
	obj.storageKeyPrefix = [storageKeyPrefixBytes]byte{0x61}
	obj.storageKeyLayoutCached = true
	obj.storageKeyHashSlot = true

	// If a copy loses a loaded marker, these deliberately empty shared rows
	// replace the source's exact in-memory snapshot on the first accessor call.
	sdb.setAccountKVLatestView(&countingAccountKVLatestReader{}, nil)

	for name, copyFn := range map[string]func() (*StateDB, error){
		"full":            sdb.Copy,
		"block_execution": sdb.CopyBlockExecutionBase,
	} {
		t.Run(name, func(t *testing.T) {
			cp, err := copyFn()
			if err != nil {
				t.Fatal(err)
			}
			copied := cp.stateObjects[addr]
			if copied == nil {
				t.Fatal("materialized source object was omitted")
			}
			if got := cp.GetEnergyUsage(addr); got != 77 {
				t.Fatalf("copied materialized resource = %d, want 77", got)
			}
			if got, ok := cp.ContractRuntime(addr); !ok || got != obj.contractRuntime {
				t.Fatalf("copied contract runtime = %+v ok=%v, want %+v", got, ok, obj.contractRuntime)
			}
			if !copied.accountMapsLoaded || !copied.accountPermissionsLoaded ||
				!copied.accountPermissionPointLoaded || copied.accountPermissionPointID != 3 ||
				!copied.witnessPermissionSignerLoaded || copied.witnessPermissionSigner != obj.witnessPermissionSigner ||
				!copied.accountVotesLoaded || !copied.accountStakeV2Loaded ||
				!copied.accountFrozenSupplyLoaded || !copied.accountResourceLoaded ||
				!copied.accountFrozenBandwidthLoaded || !copied.accountTronPowerLoaded {
				t.Fatalf("copied materialization markers are incomplete: %+v", copied)
			}
			if copied.trc10PointDomain != obj.trc10PointDomain || copied.trc10PointKey != obj.trc10PointKey ||
				copied.trc10PointValue != obj.trc10PointValue || !copied.trc10PointExists || !copied.trc10PointLoaded {
				t.Fatalf("copied TRC10 point cache = domain:%d key:%q value:%d exists:%v loaded:%v",
					copied.trc10PointDomain, copied.trc10PointKey, copied.trc10PointValue,
					copied.trc10PointExists, copied.trc10PointLoaded)
			}
			if copied.accountFrozenV2PointLoaded != obj.accountFrozenV2PointLoaded ||
				copied.accountFrozenV2PointExists != obj.accountFrozenV2PointExists ||
				copied.accountFrozenV2PointAmounts != obj.accountFrozenV2PointAmounts {
				t.Fatalf("copied frozen-v2 point cache = %#v/%#v/%v, want %#v/%#v/%v",
					copied.accountFrozenV2PointLoaded, copied.accountFrozenV2PointExists, copied.accountFrozenV2PointAmounts,
					obj.accountFrozenV2PointLoaded, obj.accountFrozenV2PointExists, obj.accountFrozenV2PointAmounts)
			}
			if !copied.storageKeyLayoutCached || copied.storageKeyPrefix != obj.storageKeyPrefix || !copied.storageKeyHashSlot {
				t.Fatalf("copied storage-key layout = cached:%v prefix:%x hash:%v",
					copied.storageKeyLayoutCached, copied.storageKeyPrefix, copied.storageKeyHashSlot)
			}
			if copied.accountPermissionPoint == nil || copied.accountPermissionPoint == obj.accountPermissionPoint {
				t.Fatal("permission point cache was omitted or shallow-copied")
			}
			copied.accountPermissionPoint.Keys[0].Address[1] = 0x44
			if obj.accountPermissionPoint.Keys[0].Address[1] != 0x99 {
				t.Fatal("copy mutation changed source permission point cache")
			}
		})
	}
}

func TestStateDBCopiesPreservePendingTransactionFinalization(t *testing.T) {
	sdb := newTestStateDB(t)
	storageAddr := testAddr(0x38)
	destructAddr := testAddr(0x39)
	slot := tcommon.Hash{0x01}
	sdb.CreateAccount(storageAddr, corepb.AccountType_Contract)
	sdb.SetState(storageAddr, slot, tcommon.Hash{0x02})
	sdb.CreateAccount(destructAddr, corepb.AccountType_Contract)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	sdb.SetState(storageAddr, slot, tcommon.Hash{})
	sdb.SelfDestruct(destructAddr)
	if len(sdb.txFinalizeDirty) != 2 {
		t.Fatalf("source finalization set = %d, want 2", len(sdb.txFinalizeDirty))
	}

	for name, copyFn := range map[string]func() (*StateDB, error){
		"full":            sdb.Copy,
		"block_execution": sdb.CopyBlockExecutionBase,
	} {
		t.Run(name, func(t *testing.T) {
			cp, err := copyFn()
			if err != nil {
				t.Fatal(err)
			}
			if len(cp.txFinalizeDirty) != 2 {
				t.Fatalf("copied finalization set = %d, want 2", len(cp.txFinalizeDirty))
			}
			cp.FinalizeTransaction()
			if got := cp.stateObjects[storageAddr].storage[slot]; got.exists || got.value != (tcommon.Hash{}) {
				t.Fatalf("copied zero storage after finalize = %+v, want absent zero", got)
			}
			if obj := cp.stateObjects[destructAddr]; obj == nil || !obj.deleted {
				t.Fatalf("copied selfdestruct after finalize = %+v, want deleted", obj)
			}
		})
	}

	if got := sdb.stateObjects[storageAddr].storage[slot]; !got.exists || got.value != (tcommon.Hash{}) {
		t.Fatalf("copy finalization changed source storage = %+v", got)
	}
	if obj := sdb.stateObjects[destructAddr]; obj == nil || obj.deleted || !obj.selfDestructed {
		t.Fatalf("copy finalization changed source selfdestruct = %+v", obj)
	}
}

func TestStateDBCopiesPreserveTransientAndVersionedViews(t *testing.T) {
	sdb := newTestStateDB(t)
	account := testAddr(0x3a)
	contract := testAddr(0x3b)
	slot := tcommon.Hash{0x01}
	transient := tcommon.Hash{0x77}
	sdb.CreateAccount(account, corepb.AccountType_Normal)
	sdb.AddBalance(account, 10)
	sdb.CreateAccount(contract, corepb.AccountType_Contract)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	sdb.SetTransientState(contract, slot, transient)
	reader := testTransactionVersionedReader{
		{Kind: TransactionAccessAccountField, Address: account, AccountField: TransactionAccountFieldBalance}: {
			{txIndex: 1, value: testVersionedInt64(99)},
		},
	}
	sdb.SetTransactionVersionedValueReader(reader, 2)

	for name, copyFn := range map[string]func() (*StateDB, error){
		"full":            sdb.Copy,
		"block_execution": sdb.CopyBlockExecutionBase,
	} {
		t.Run(name, func(t *testing.T) {
			cp, err := copyFn()
			if err != nil {
				t.Fatal(err)
			}
			if got := cp.GetTransientState(contract, slot); got != transient {
				t.Fatalf("copied transient state = %x, want %x", got, transient)
			}
			cp.SetTransientState(contract, slot, tcommon.Hash{0x88})
			if got := sdb.GetTransientState(contract, slot); got != transient {
				t.Fatalf("copy transient mutation changed source to %x", got)
			}
			if cp.transactionVersionedReader == nil || cp.transactionVersionedTxIndex != 2 {
				t.Fatalf("copied version reader = %T tx=%d", cp.transactionVersionedReader, cp.transactionVersionedTxIndex)
			}
			if got := cp.GetBalance(account); got != 99 {
				t.Fatalf("copied versioned balance = %d, want 99", got)
			}
			if len(cp.transactionVersionedHydrated) != 1 {
				t.Fatalf("copied hydration set = %d, want 1", len(cp.transactionVersionedHydrated))
			}
			if len(sdb.transactionVersionedHydrated) != 0 {
				t.Fatalf("copy hydration changed source set = %d", len(sdb.transactionVersionedHydrated))
			}
		})
	}
}

func TestStateDBCopyBlockExecutionBaseOmitsCleanObjects(t *testing.T) {
	sdb := newTestStateDB(t)
	cleanAddr := testAddr(0x31)
	dirtyAddr := testAddr(0x32)
	storageKey := tcommon.Hash{0x01}
	cleanStorageKey := tcommon.Hash{0x02}
	originalStorage := tcommon.Hash{0x10}
	cleanStorage := tcommon.Hash{0x11}
	pendingStorage := tcommon.Hash{0x20}

	sdb.CreateAccount(cleanAddr, corepb.AccountType_Normal)
	sdb.AddBalance(cleanAddr, 100)
	sdb.CreateAccount(dirtyAddr, corepb.AccountType_Contract)
	sdb.AddBalance(dirtyAddr, 200)
	sdb.SetState(dirtyAddr, storageKey, originalStorage)
	sdb.SetState(dirtyAddr, cleanStorageKey, cleanStorage)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	// Keep both committed accounts in the source's cross-block cache, then make
	// only one of them part of the next block's uncommitted working set.
	if got := sdb.GetBalance(cleanAddr); got != 100 {
		t.Fatalf("clean source balance = %d, want 100", got)
	}
	if got := sdb.GetBalance(dirtyAddr); got != 200 {
		t.Fatalf("dirty source balance = %d, want 200", got)
	}
	if got := sdb.GetState(dirtyAddr, cleanStorageKey); got != cleanStorage {
		t.Fatalf("clean cached source storage = %x, want %x", got, cleanStorage)
	}
	sdb.AddBalance(dirtyAddr, 7)
	sdb.SetState(dirtyAddr, storageKey, pendingStorage)

	cp, err := sdb.CopyBlockExecutionBase()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cp.stateObjects[cleanAddr]; ok {
		t.Fatal("clean cached account was eagerly copied")
	}
	if _, ok := cp.stateObjects[dirtyAddr]; !ok {
		t.Fatal("dirty account was omitted from execution copy")
	}
	if len(cp.stateObjects) != 1 {
		t.Fatalf("initial execution-copy object count = %d, want 1", len(cp.stateObjects))
	}
	if _, copied := cp.stateObjects[dirtyAddr].storage[cleanStorageKey]; copied {
		t.Fatal("clean cached storage slot was eagerly copied")
	}

	// The omitted account rehydrates from the stable latest view. The dirty
	// account must instead expose the source's not-yet-published block writes.
	if got := cp.GetBalance(cleanAddr); got != 100 {
		t.Fatalf("lazy clean balance = %d, want 100", got)
	}
	if got := cp.GetBalance(dirtyAddr); got != 207 {
		t.Fatalf("copied dirty balance = %d, want 207", got)
	}
	if got := cp.GetState(dirtyAddr, storageKey); got != pendingStorage {
		t.Fatalf("copied dirty storage = %x, want %x", got, pendingStorage)
	}
	if got := cp.GetState(dirtyAddr, cleanStorageKey); got != cleanStorage {
		t.Fatalf("lazy clean storage = %x, want %x", got, cleanStorage)
	}
	if len(cp.stateObjects) != 2 {
		t.Fatalf("post-hydration execution-copy object count = %d, want 2", len(cp.stateObjects))
	}

	cp.AddBalance(cleanAddr, 11)
	cp.AddBalance(dirtyAddr, 13)
	cp.SetState(dirtyAddr, storageKey, tcommon.Hash{0x30})
	if got := sdb.GetBalance(cleanAddr); got != 100 {
		t.Fatalf("copy mutation changed clean source balance to %d", got)
	}
	if got := sdb.GetBalance(dirtyAddr); got != 207 {
		t.Fatalf("copy mutation changed dirty source balance to %d", got)
	}
	if got := sdb.GetState(dirtyAddr, storageKey); got != pendingStorage {
		t.Fatalf("copy mutation changed dirty source storage to %x", got)
	}
}

func TestStateDBCopyBlockExecutionBaseRetainsCachedContractCode(t *testing.T) {
	sdb := newTestStateDB(t)
	contract := testAddr(0x33)
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x50, 0x00}

	sdb.CreateAccount(contract, corepb.AccountType_Contract)
	sdb.SetCode(contract, code)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetCode(contract); !bytes.Equal(got, code) {
		t.Fatalf("cached source code = %x, want %x", got, code)
	}

	// Model the live failure shape: the canonical StateDB still owns immutable
	// cached code while the durable hot row has already moved out of the reader
	// visible to a speculative execution copy. Dropping the clean state object
	// must not silently turn a contract call into a successful empty-code call.
	codeHash := tcommon.Keccak256(code)
	if err := rawdb.DeleteStateCode(sdb.db.DiskDB(), codeHash); err != nil {
		t.Fatal(err)
	}
	if got := rawdb.ReadStateCode(sdb.db.DiskDB(), codeHash); len(got) != 0 {
		t.Fatalf("hot code still present: %x", got)
	}

	cp, err := sdb.CopyBlockExecutionBase()
	if err != nil {
		t.Fatal(err)
	}
	got := cp.GetCode(contract)
	if !bytes.Equal(got, code) {
		t.Fatalf("execution-copy code = %x, want cached %x", got, code)
	}
	got[0] ^= 0xff
	if source := sdb.GetCode(contract); !bytes.Equal(source, code) {
		t.Fatalf("execution-copy mutation changed source code: %x", source)
	}
}

func TestStateDBCopiesRetainUnflushedWitnessView(t *testing.T) {
	sdb := newTestStateDB(t)
	witnessAddr := testAddr(0x34)
	sdb.CreateAccount(witnessAddr, corepb.AccountType_Normal)
	if err := sdb.SetWitnessCapsule(types.NewWitness(witnessAddr, "https://old.example")); err != nil {
		t.Fatal(err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	// Witness mutations live in the separate in-memory witness cache until the
	// block-level FlushWitnesses call. Rehydrating a copy from the durable row at
	// this boundary would silently recover the old URL.
	sdb.SetWitnessURL(witnessAddr, "https://new.example")
	if got := sdb.GetWitness(witnessAddr).URL(); got != "https://new.example" {
		t.Fatalf("source witness URL = %q, want updated value", got)
	}

	for name, copyFn := range map[string]func() (*StateDB, error){
		"full":            sdb.Copy,
		"block_execution": sdb.CopyBlockExecutionBase,
	} {
		t.Run(name, func(t *testing.T) {
			cp, err := copyFn()
			if err != nil {
				t.Fatal(err)
			}
			if got := cp.GetWitness(witnessAddr); got == nil || got.URL() != "https://new.example" {
				t.Fatalf("copied witness = %v, want updated URL", got)
			}
			cp.SetWitnessURL(witnessAddr, "https://copy.example")
			if got := sdb.GetWitness(witnessAddr).URL(); got != "https://new.example" {
				t.Fatalf("copy mutation changed source witness URL to %q", got)
			}
		})
	}
}

var stateDBExecutionCopyBenchmarkSink *StateDB

func BenchmarkStateDBBlockExecutionCopy(b *testing.B) {
	sdb := newTestStateDB(b)
	const accounts = 256
	for i := 0; i < accounts; i++ {
		var addr tcommon.Address
		addr[0] = 0x41
		addr[19] = byte(i >> 8)
		addr[20] = byte(i)
		sdb.CreateAccount(addr, corepb.AccountType_Normal)
		sdb.AddBalance(addr, int64(i+1))
	}
	if _, err := sdb.Commit(); err != nil {
		b.Fatal(err)
	}
	// Model writeHistoryBlockHash: one object is dirty at block-copy time while
	// the rest are clean entries in the bounded cross-block account cache.
	sdb.AddBalance(testAddr(0), 1)

	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var err error
			stateDBExecutionCopyBenchmarkSink, err = sdb.Copy()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("block_execution", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var err error
			stateDBExecutionCopyBenchmarkSink, err = sdb.CopyBlockExecutionBase()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
