package state

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func mustReadStateAccountEnvelope(t *testing.T, sdb *StateDB, addr tcommon.Address) *StateAccountV2 {
	t.Helper()
	data, ok, err := rawdb.ReadStateAccountLatest(sdb.db.DiskDB(), addr)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("account latest row missing for %s", addr.Hex())
	}
	env, err := DecodeStateAccountV2(data)
	if err != nil {
		t.Fatal(err)
	}
	return env
}

type recordingStateCodeStore struct {
	codes  map[tcommon.Hash][]byte
	reads  []tcommon.Hash
	writes []tcommon.Hash
}

func newRecordingStateCodeStore() *recordingStateCodeStore {
	return &recordingStateCodeStore{codes: make(map[tcommon.Hash][]byte)}
}

func (s *recordingStateCodeStore) ReadStateCode(hash tcommon.Hash) []byte {
	s.reads = append(s.reads, hash)
	code := s.codes[hash]
	if len(code) == 0 {
		return nil
	}
	return append([]byte(nil), code...)
}

func (s *recordingStateCodeStore) WriteStateCode(hash tcommon.Hash, code []byte) error {
	s.writes = append(s.writes, hash)
	s.codes[hash] = append([]byte(nil), code...)
	return nil
}

func TestStateDBCodeMethods(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x01}

	if code := sdb.GetCode(addr); code != nil {
		t.Fatalf("expected nil code, got %x", code)
	}
	if size := sdb.GetCodeSize(addr); size != 0 {
		t.Fatalf("expected 0 code size, got %d", size)
	}

	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	sdb.SetCode(addr, code)

	if got := sdb.GetCode(addr); string(got) != string(code) {
		t.Fatalf("code mismatch: got %x, want %x", got, code)
	}
	if size := sdb.GetCodeSize(addr); size != len(code) {
		t.Fatalf("code size mismatch: got %d, want %d", size, len(code))
	}
	if hash := sdb.GetCodeHash(addr); hash != tcommon.Keccak256(code) {
		t.Fatalf("code hash: got %x, want %x", hash, tcommon.Keccak256(code))
	}
}

func TestStateDBCodeUsesTypedStoreBoundary(t *testing.T) {
	sdb := newTestStateDB(t)
	store := newRecordingStateCodeStore()
	sdb.codeStore = store
	addr := tcommon.Address{0x41, 0x01, 0x10}
	code := []byte{0x60, 0x0a, 0x60, 0x00, 0xf3}
	hash := tcommon.Keccak256(code)

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, code)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit code through typed store: %v", err)
	}
	if len(store.writes) != 1 || store.writes[0] != hash || !bytes.Equal(store.codes[hash], code) {
		t.Fatalf("typed code writes = %x store=%x, want hash %x code %x", store.writes, store.codes[hash], hash, code)
	}
	if got := rawdb.ReadStateCode(sdb.db.DiskDB(), hash); len(got) != 0 {
		t.Fatalf("rawdb code table was written despite typed store override: %x", got)
	}

	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	reloaded.codeStore = store
	if got := reloaded.GetCode(addr); !bytes.Equal(got, code) {
		t.Fatalf("typed store code reload = %x, want %x", got, code)
	}
	if len(store.reads) != 1 || store.reads[0] != hash {
		t.Fatalf("typed code reads = %x, want [%x]", store.reads, hash)
	}
}

func TestStateDBCodeHashForExistingEmptyAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x01}
	sdb.CreateAccount(addr, corepb.AccountType_Normal)

	if hash := sdb.GetCodeHash(addr); hash != tcommon.Keccak256(nil) {
		t.Fatalf("empty account code hash: got %x, want %x", hash, tcommon.Keccak256(nil))
	}
}

func TestStateDBGetCodeFallsBackToColdCodeDomain(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x01, 0x02}
	code := []byte{0x60, 0x01, 0x60, 0x02}
	hash := tcommon.Keccak256(code)
	sdb.SetCode(addr, code)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	codeRef, accessorRef, btreeRef, err := snapshots.BuildCodeSegmentFilesFromDB(diskdb, dir, 10, 10, "latest/code-10-10.seg")
	if err != nil {
		t.Fatalf("build code snapshot: %v", err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 10, []snapshots.SegmentRef{codeRef, accessorRef, btreeRef})); err != nil {
		t.Fatalf("publish code snapshot: %v", err)
	}
	if err := rawdb.DeleteStateCode(diskdb, hash); err != nil {
		t.Fatalf("delete hot code: %v", err)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	reloaded, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	reloaded.SetCodeColdHistory(mgr, 11)
	copied, err := reloaded.Copy()
	if err != nil {
		t.Fatalf("copy state: %v", err)
	}
	if got := copied.GetCode(addr); !bytes.Equal(got, code) {
		t.Fatalf("copied cold code = %x, want %x", got, code)
	}
	if got := reloaded.GetCode(addr); !bytes.Equal(got, code) {
		t.Fatalf("cold code = %x, want %x", got, code)
	}
}

func TestStateDBStorageMethods(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x02}

	key := tcommon.Hash{0x01}
	val := tcommon.Hash{0x42}

	if got := sdb.GetState(addr, key); got != (tcommon.Hash{}) {
		t.Fatalf("expected empty state, got %x", got)
	}

	sdb.SetState(addr, key, val)
	if got := sdb.GetState(addr, key); got != val {
		t.Fatalf("state mismatch: got %x, want %x", got, val)
	}
}

func TestStateDBStorageUsesJavaRowKeyForLegacyContracts(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x22}
	key := tcommon.Hash{0x01}
	collidingKey := tcommon.Hash{0x02}
	val := tcommon.Hash{0x42}

	sdb.SetState(addr, key, val)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rowKey := javaStorageRowKey(addr, key, nil)
	if raw := rawdb.ReadStorage(sdb.db.DiskDB(), addr, rowKey); len(raw) != 0 {
		t.Fatalf("legacy storage mirror was written: %x", raw)
	}
	env := mustReadStateAccountEnvelope(t, sdb, addr)
	raw, ok, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, env.AccountKVGeneration, kvdomains.ContractStorage, rowKey.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || tcommon.BytesToHash(raw) != val {
		t.Fatalf("ContractStorage latest row = %x ok=%v, want %x", raw, ok, val)
	}

	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.GetState(addr, collidingKey); got != val {
		t.Fatalf("legacy storage row collision: got %x, want %x", got, val)
	}
}

func TestStateDBStorageVersionOneHashesSlotBeforeRowKey(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x23}
	key := tcommon.Hash{0x01}
	otherKey := tcommon.Hash{0x02}
	val := tcommon.Hash{0x42}
	meta := &contractpb.SmartContract{
		ContractAddress: addr.Bytes(),
		Version:         1,
	}

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, meta)
	sdb.SetState(addr, key, val)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	if rowKey := javaStorageRowKey(addr, key, nil); len(rawdb.ReadStorage(sdb.db.DiskDB(), addr, rowKey)) != 0 {
		t.Fatal("version=1 storage must not write legacy slot row key")
	}
	rowKey := javaStorageRowKey(addr, key, meta)
	if raw := rawdb.ReadStorage(sdb.db.DiskDB(), addr, rowKey); len(raw) != 0 {
		t.Fatalf("version=1 storage wrote legacy mirror: %x", raw)
	}
	env := mustReadStateAccountEnvelope(t, sdb, addr)
	raw, ok, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, env.AccountKVGeneration, kvdomains.ContractStorage, rowKey.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || tcommon.BytesToHash(raw) != val {
		t.Fatalf("version=1 ContractStorage latest row = %x ok=%v, want %x", raw, ok, val)
	}

	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.GetState(addr, otherKey); got != (tcommon.Hash{}) {
		t.Fatalf("version=1 storage should not collide on raw slot suffix, got %x", got)
	}
}

func TestStateDBContractMeta(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x03}

	if sdb.IsContract(addr) {
		t.Fatal("should not be contract initially")
	}

	meta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "test",
	}
	sdb.SetContract(addr, meta)
	if !sdb.IsContract(addr) {
		t.Fatal("should be contract after SetContract")
	}
	got := sdb.GetContract(addr)
	if got == nil || got.Name != "test" {
		t.Fatal("contract meta mismatch")
	}
	raw, ok, err := sdb.GetContractMetadataBytes(addr)
	if err != nil || !ok {
		t.Fatalf("GetContractMetadataBytes = ok=%t err=%v", ok, err)
	}
	var decoded contractpb.SmartContract
	if err := proto.Unmarshal(raw, &decoded); err != nil || !proto.Equal(&decoded, meta) {
		t.Fatalf("GetContractMetadataBytes decoded = %v err=%v, want %v", &decoded, err, meta)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok, err = reloaded.GetContractMetadataBytes(addr)
	if err != nil || !ok {
		t.Fatalf("reloaded GetContractMetadataBytes = ok=%t err=%v", ok, err)
	}
	decoded.Reset()
	if err := proto.Unmarshal(raw, &decoded); err != nil || !proto.Equal(&decoded, meta) {
		t.Fatalf("reloaded GetContractMetadataBytes decoded = %v err=%v, want %v", &decoded, err, meta)
	}
	raw, ok, err = ReadCommittedContractMetadataBytes(sdb.db.DiskDB(), addr)
	if err != nil || !ok {
		t.Fatalf("ReadCommittedContractMetadataBytes = ok=%t err=%v", ok, err)
	}
	decoded.Reset()
	if err := proto.Unmarshal(raw, &decoded); err != nil || !proto.Equal(&decoded, meta) {
		t.Fatalf("ReadCommittedContractMetadataBytes decoded = %v err=%v, want %v", &decoded, err, meta)
	}
}

func TestStateDBContractRuntimeStateRoundTrip(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x24}
	cs := types.NewContractState(12)
	cs.SetEnergyFactor(3000)
	cs.AddEnergyUsage(456)

	if got := sdb.ReadContractState(addr); got != nil {
		t.Fatalf("contract state should be absent before write, got %+v", got)
	}
	if err := sdb.WriteContractState(addr, cs); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}

	got := reloaded.ReadContractState(addr)
	if got == nil {
		t.Fatal("contract state missing after reopen")
	}
	if got.UpdateCycle() != 12 || got.EnergyFactor() != 3000 || got.EnergyUsage() != 456 {
		t.Fatalf("contract state = cycle:%d factor:%d usage:%d", got.UpdateCycle(), got.EnergyFactor(), got.EnergyUsage())
	}
}

func TestStateDBContractRuntimeStateIgnoresFutureFlatMirror(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x25}
	cs := types.NewContractState(10)
	cs.SetEnergyFactor(1000)
	if err := sdb.WriteContractState(addr, cs); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	future := types.NewContractState(99)
	future.SetEnergyFactor(9000)
	if err := rawdb.WriteContractState(diskdb, addr, future); err != nil {
		t.Fatal(err)
	}

	reloaded, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	got := reloaded.ReadContractState(addr)
	if got == nil {
		t.Fatal("rooted contract state missing")
	}
	if got.UpdateCycle() != 10 || got.EnergyFactor() != 1000 {
		t.Fatalf("historical root loaded future flat contract state: cycle=%d factor=%d", got.UpdateCycle(), got.EnergyFactor())
	}
}

func TestStateDBContractABIRoundTrip(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x26}
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes()})
	abi := &contractpb.SmartContract_ABI{
		Entrys: []*contractpb.SmartContract_ABI_Entry{{
			Name: "transfer",
			Type: contractpb.SmartContract_ABI_Entry_Function,
		}},
	}

	if got := sdb.ReadContractABI(addr); got != nil {
		t.Fatalf("ABI should be absent before write, got %+v", got)
	}
	if err := sdb.WriteContractABI(addr, abi); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}

	if got := reloaded.ReadContractABI(addr); !proto.Equal(got, abi) {
		t.Fatalf("ABI = %v, want %v", got, abi)
	}
}

func TestStateDBEmptyContractCodeKeepsEmptyCodeHash(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x2a}
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, nil)

	if got, want := sdb.GetCodeHash(addr), tcommon.Keccak256(nil); got != want {
		t.Fatalf("empty contract code hash = %x, want %x", got, want)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := reloaded.GetCodeHash(addr), tcommon.Keccak256(nil); got != want {
		t.Fatalf("persisted empty contract code hash = %x, want %x", got, want)
	}
}

func TestStateDBContractABIIgnoresFutureFlatMirror(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x27}
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes()})
	abi := &contractpb.SmartContract_ABI{
		Entrys: []*contractpb.SmartContract_ABI_Entry{{Name: "rooted"}},
	}
	if err := sdb.WriteContractABI(addr, abi); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	if err := rawdb.WriteContractABI(diskdb, addr.Bytes(), &contractpb.SmartContract_ABI{
		Entrys: []*contractpb.SmartContract_ABI_Entry{{Name: "future"}},
	}); err != nil {
		t.Fatal(err)
	}

	reloaded, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.ReadContractABI(addr); !proto.Equal(got, abi) {
		t.Fatalf("historical root loaded future flat ABI: %v", got)
	}
}

func TestStateDBContractReadsIgnoreFutureFlatMirror(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x33}
	slot := tcommon.BytesToHash([]byte{0x01})
	stale := tcommon.BytesToHash([]byte{0x99})

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	futureMeta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "future",
	}
	metaBytes, err := proto.Marshal(futureMeta)
	if err != nil {
		t.Fatal(err)
	}
	rawdb.WriteCode(diskdb, addr, []byte{0xfe})
	rawdb.WriteContract(diskdb, addr, metaBytes)
	rawdb.WriteStorage(diskdb, addr, javaStorageRowKey(addr, slot, nil), stale.Bytes())

	reloaded, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.GetCode(addr); len(got) != 0 {
		t.Fatalf("historical root loaded future flat code: %x", got)
	}
	if got := reloaded.GetContract(addr); got != nil {
		t.Fatalf("historical root loaded future flat contract metadata: %+v", got)
	}
	if got := reloaded.GetState(addr, slot); got != (tcommon.Hash{}) {
		t.Fatalf("historical root loaded future flat storage: %x", got)
	}
}

func TestStateDBSelfDestruct(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x04}

	sdb.SetCode(addr, []byte{0x00})
	if sdb.HasSelfDestructed(addr) {
		t.Fatal("should not be selfDestructed")
	}

	sdb.SelfDestruct(addr)
	if !sdb.HasSelfDestructed(addr) {
		t.Fatal("should be selfDestructed")
	}
}

func TestStateDBSelfDestructDeletesAccountAtCommit(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x34}
	code := []byte{0x60, 0x2a, 0x60, 0x00, 0x52, 0x00}
	meta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "selfdestructed",
	}

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, code)
	sdb.SetContract(addr, meta)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	sdb.SelfDestruct(addr)
	if !sdb.AccountExists(addr) {
		t.Fatal("selfdestruct should not hide account before commit")
	}
	if got := sdb.GetCode(addr); string(got) != string(code) {
		t.Fatalf("selfdestruct should not hide code before commit: got %x", got)
	}
	if got := sdb.GetContract(addr); got == nil || got.Name != "selfdestructed" {
		t.Fatalf("selfdestruct should not hide contract meta before commit: %+v", got)
	}

	root, err = sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if sdb.AccountExists(addr) {
		t.Fatal("selfdestructed account survived commit")
	}
	if got := sdb.GetCode(addr); len(got) != 0 {
		t.Fatalf("selfdestructed code survived commit: %x", got)
	}
	if got := sdb.GetContract(addr); got != nil {
		t.Fatalf("selfdestructed contract meta survived commit: %+v", got)
	}
}

func TestStateDBFinalizeTransactionDeletesSelfDestructForNextTx(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x44}
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	meta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "finalize-selfdestruct",
	}

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, code)
	sdb.SetContract(addr, meta)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	sdb.SelfDestruct(addr)
	sdb.FinalizeTransaction()
	if sdb.AccountExists(addr) {
		t.Fatal("selfdestructed account should be absent after transaction boundary")
	}
	if got := sdb.GetCodeSize(addr); got != 0 {
		t.Fatalf("code should be hidden after transaction boundary: got size %d", got)
	}
	if sdb.GetContract(addr) != nil {
		t.Fatal("contract metadata should be hidden after transaction boundary")
	}

	snap := sdb.Snapshot()
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, []byte{0x00})
	sdb.RevertToSnapshot(snap)
	if sdb.AccountExists(addr) {
		t.Fatal("reverting a later recreate should restore the pending delete")
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if sdb.AccountExists(addr) {
		t.Fatal("pending delete should survive commit after later recreate revert")
	}
}

func TestDeleteAccountClearsPersistedContractCodeOnRecreate(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x44}
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	meta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "deleted",
	}

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, code)
	sdb.SetContract(addr, meta)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetCode(addr); string(got) != string(code) {
		t.Fatalf("precondition code mismatch: got %x", got)
	}
	if got := sdb.GetContract(addr); got == nil || got.Name != "deleted" {
		t.Fatalf("precondition contract meta mismatch: %+v", got)
	}

	sdb.DeleteAccount(addr)
	sdb.CreateAccountWithTime(addr, corepb.AccountType_Normal, 12345)
	if got := sdb.GetCode(addr); len(got) != 0 {
		t.Fatalf("recreated normal account must not expose stale code before commit: %x", got)
	}
	if got := sdb.GetContract(addr); got != nil {
		t.Fatalf("recreated normal account must not expose stale contract meta before commit: %+v", got)
	}
	root, err = sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetCode(addr); len(got) != 0 {
		t.Fatalf("stale code survived commit: %x", got)
	}
	if got := sdb.GetContract(addr); got != nil {
		t.Fatalf("stale contract meta survived commit: %+v", got)
	}
	if !sdb.AccountExists(addr) {
		t.Fatal("recreated normal account should persist")
	}
	if sdb.GetAccount(addr).Type() != corepb.AccountType_Normal {
		t.Fatalf("account type: got %v, want normal", sdb.GetAccount(addr).Type())
	}

}

func TestStateDBExistEmpty(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x05}

	if sdb.Exist(addr) {
		t.Fatal("should not exist")
	}
	if !sdb.Empty(addr) {
		t.Fatal("should be empty")
	}

	sdb.AddBalance(addr, 100)
	if !sdb.Exist(addr) {
		t.Fatal("should exist after AddBalance")
	}
	if sdb.Empty(addr) {
		t.Fatal("should not be empty with balance")
	}
}

func TestStateDBStorageRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x06}
	key := tcommon.Hash{0x01}

	sdb.SetState(addr, key, tcommon.Hash{0x10})
	snap := sdb.Snapshot()
	sdb.SetState(addr, key, tcommon.Hash{0x20})

	if got := sdb.GetState(addr, key); got != (tcommon.Hash{0x20}) {
		t.Fatalf("expected 0x20, got %x", got)
	}

	sdb.RevertToSnapshot(snap)
	if got := sdb.GetState(addr, key); got != (tcommon.Hash{0x10}) {
		t.Fatalf("expected 0x10 after revert, got %x", got)
	}
}

func TestStateDBCopy(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x07}
	sdb.AddBalance(addr, 1000)
	sdb.SetCode(addr, []byte{0x60, 0x00})
	sdb.SetState(addr, tcommon.Hash{0x01}, tcommon.Hash{0x42})

	cp, err := sdb.Copy()
	if err != nil {
		t.Fatal(err)
	}

	// Verify copy has same data
	if cp.GetBalance(addr) != 1000 {
		t.Fatal("copy balance mismatch")
	}
	if string(cp.GetCode(addr)) != string(sdb.GetCode(addr)) {
		t.Fatal("copy code mismatch")
	}
	if cp.GetState(addr, tcommon.Hash{0x01}) != (tcommon.Hash{0x42}) {
		t.Fatal("copy storage mismatch")
	}

	// Modify copy, original unchanged
	cp.AddBalance(addr, 500)
	if sdb.GetBalance(addr) != 1000 {
		t.Fatal("original should be unchanged")
	}
}
