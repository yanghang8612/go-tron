package state

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestSetContractJournalHistory(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x73)
	f.applyBlock(tcommon.Hash{1}, func(s *StateDB) {
		s.CreateAccount(addr, corepb.AccountType_Contract)
		s.SetContract(addr, &contractpb.SmartContract{Name: "original", ConsumeUserResourcePercent: 10})
	})
	f.applyBlock(tcommon.Hash{2}, func(s *StateDB) {
		meta := s.GetContract(addr)
		meta.Name = "updated"
		meta.ConsumeUserResourcePercent = 99
		s.SetContract(addr, meta)
	})
	for height, want := range map[uint64]string{1: "original", 2: "updated"} {
		got, err := f.reader().ContractAt(addr, height)
		if err != nil || got == nil || got.Name != want {
			t.Fatalf("height %d: got %v err %v want %s", height, got, err, want)
		}
	}
	count := 0
	for _, row := range collectStateDomainChanges(t, f.disk, 2) {
		if row.FlatDomain == rawdb.StateFlatDomainKVLatest && row.Domain == kvdomains.ContractMetadata {
			count++
			var prev contractpb.SmartContract
			if err := statecodec.Unmarshal(row.Prev, &prev); err != nil || prev.Name != "original" {
				t.Fatal("before image", &prev, err)
			}
		}
	}
	if count != 1 {
		t.Fatal("metadata row count", count)
	}
	first, _, err := rawdb.ReadStateTxRange(f.disk, 1)
	if err != nil {
		t.Fatal(err)
	}
	last, _, err := rawdb.ReadStateTxRange(f.disk, 2)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(f.disk, dir, first.BeginTxNum, last.EndTxNum, "history/state-domain-change-contract-capture.seg")
	if err != nil {
		t.Fatal(err)
	}
	m := snapshots.NewManifest(first.BeginTxNum, last.EndTxNum, refs)
	if err = snapshots.PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	filter, built, err := snapshots.BuildMetadataHistoryRoutingContext(context.Background(), dir, m, 256)
	if err != nil || !built {
		t.Fatal(built, err)
	}
	m, err = snapshots.NewAggregator(dir).Integrate(first.BeginTxNum, last.EndTxNum, []snapshots.SegmentRef{filter})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = snapshots.VerifyLoadedManifestFiles(dir, m, snapshots.VerifyManifestOptions{RequireChecksums: true, RequireRegistered: true}); err != nil {
		t.Fatal(err)
	}
	f.pruneHotStateDomainHistory()
	manager, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	cold := NewPersistentHistoryReaderWithColdHistory(f.disk, nil, f.head, manager)
	got, err := cold.ContractAt(addr, 1)
	if err != nil || got == nil || got.Name != "original" {
		t.Fatal("captured metadata after cold export", got, err)
	}
}

func TestSetContractStageTransactionsRollbackAndMuHash(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(0x74)
	s.CreateAccount(addr, corepb.AccountType_Contract)
	s.SetContract(addr, &contractpb.SmartContract{Name: "before"})
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	disk := s.db.DiskDB()
	stage, err := s.BeginDomainChangeStage(disk, &rawdb.StateTxRange{BlockNum: 1, BlockHash: tcommon.Hash{1}, BeginTxNum: 1, EndTxNum: 3})
	if err != nil {
		t.Fatal(err)
	}
	mark := stage.JournalMark()
	input := &contractpb.SmartContract{Name: "one"}
	s.SetContract(addr, input)
	input.Name = "caller mutation after setter"
	if err = stage.FlushOrdinal(mark, 0); err != nil {
		t.Fatal(err)
	}
	mark = stage.JournalMark()
	snap := s.Snapshot()
	mutated := s.GetContract(addr)
	mutated.Name = "rollback"
	s.SetContract(addr, mutated)
	s.RevertToSnapshot(snap)
	if got := s.GetContract(addr); got == nil || got.Name != "one" {
		t.Fatal("rollback", got)
	}
	s.SetContract(addr, &contractpb.SmartContract{Name: "two"})
	if err = stage.FlushOrdinal(mark, 1); err != nil {
		t.Fatal(err)
	}
	mark = stage.JournalMark()
	s.SetContract(addr, &contractpb.SmartContract{Name: "two"})
	if err = stage.FlushOrdinal(mark, 2); err != nil {
		t.Fatal(err)
	}
	if err = stage.FlushFinal(); err != nil {
		t.Fatal(err)
	}
	s.dynProps.SetLatestBlockHeaderNumber(1)
	if _, _, err = s.CommitWithStatsOptions(CommitOptions{BlockNumber: 1, BlockHash: tcommon.Hash{1}, CommitmentTxNum: 3}); err != nil {
		t.Fatal(err)
	}
	assertCompactFullMuHash(t, disk)
	if err = rawdb.WriteBlockNumber(disk, tcommon.Hash{1}, 1); err != nil {
		t.Fatal(err)
	}
	if err = rawdb.WriteStateExecutionProperties(disk, rawdb.StateExecutionProperties{BlockNum: 1, Timestamp: 3000, BlockHash: tcommon.Hash{1}}); err != nil {
		t.Fatal(err)
	}
	var names []string
	var nums []uint64
	for _, row := range collectStateDomainChanges(t, disk, 1) {
		if row.FlatDomain == rawdb.StateFlatDomainKVLatest && row.Domain == kvdomains.ContractMetadata {
			var prev contractpb.SmartContract
			if err = statecodec.Unmarshal(row.Prev, &prev); err != nil {
				t.Fatal(err)
			}
			names = append(names, prev.Name)
			nums = append(nums, row.TxNum)
		}
	}
	if len(names) != 2 || names[0] != "before" || names[1] != "one" || nums[0] != 1 || nums[1] != 2 {
		t.Fatal("transaction before-images", names, nums)
	}
	dir := t.TempDir()
	built, err := snapshots.NewAggregator(dir).BuildLatest(disk, snapshots.AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 3})
	if err != nil {
		t.Fatal(err)
	}
	identity := snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: strings.Repeat("01", 32), ForkConfigHash: "sha256:" + strings.Repeat("02", 32)}
	built.Manifest.Chain = &identity
	if err = snapshots.PublishManifest(dir, built.Manifest); err != nil {
		t.Fatal(err)
	}
	restored := rawdb.NewMemoryDatabase()
	defer restored.Close()
	if _, err = snapshots.RestoreLatestFromVerifiedManifest(restored, dir, identity); err != nil {
		t.Fatal(err)
	}
	a := assertCompactFullMuHash(t, disk)
	b := assertCompactFullMuHash(t, restored)
	if a.Root != b.Root {
		t.Fatal("restored metadata root")
	}
}

func TestSetContractDeleteAndRecreateHistory(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x75)
	f.applyBlock(tcommon.Hash{1}, func(s *StateDB) {
		s.CreateAccount(addr, corepb.AccountType_Contract)
		s.SetContract(addr, &contractpb.SmartContract{Name: "original"})
	})
	f.applyBlock(tcommon.Hash{2}, func(s *StateDB) { s.SetContract(addr, nil) })
	f.applyBlock(tcommon.Hash{3}, func(s *StateDB) { s.SetContract(addr, &contractpb.SmartContract{Name: "third"}) })
	for height, want := range map[uint64]string{1: "original", 2: "", 3: "third"} {
		got, err := f.reader().ContractAt(addr, height)
		if err != nil {
			t.Fatal(err)
		}
		if want == "" {
			if got != nil {
				t.Fatal("deleted metadata", got)
			}
		} else if got == nil || got.Name != want {
			t.Fatal(height, got, want)
		}
	}
	f.applyBlock(tcommon.Hash{4}, func(s *StateDB) {
		s.DeleteAccount(addr)
		s.CreateAccount(addr, corepb.AccountType_Contract)
		s.SetContract(addr, &contractpb.SmartContract{Name: "new generation"})
	})
	got, err := f.reader().ContractAt(addr, 3)
	if err != nil || got == nil || got.Name != "third" {
		t.Fatal("generation history", got, err)
	}
}

func TestSetContractTypedPublishPreservesDeferredABIAndMuHash(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint("replacement=", replacement), func(t *testing.T) {
			base := newTestStateDB(t)
			addr := testAddr(0x76)
			base.CreateAccount(addr, corepb.AccountType_Contract)
			base.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes(), Name: "old"})
			abi := &contractpb.SmartContract_ABI{Entrys: []*contractpb.SmartContract_ABI_Entry{{Name: "keep"}}}
			if err := base.WriteContractABI(addr, abi); err != nil {
				t.Fatal(err)
			}
			baselineRoot, err := base.Commit()
			if err != nil {
				t.Fatal(err)
			}
			copyState := func() *StateDB {
				disk := rawdb.NewMemoryDatabase()
				t.Cleanup(func() { disk.Close() })
				it := base.db.DiskDB().NewIterator(nil, nil)
				for it.Next() {
					if err := disk.Put(append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...)); err != nil {
						t.Fatal(err)
					}
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
				it.Release()
				state, err := New(baselineRoot, NewDatabase(rawdb.WrapKeyValueStore(disk)))
				if err != nil {
					t.Fatal(err)
				}
				return state
			}
			serial, publisher := copyState(), copyState()
			var rec TransactionAccessRecorder
			rec.Reset(16)
			serial.SetTransactionAccessRecorder(&rec)
			mark := serial.DomainChangeJournalMark()
			serial.SetContract(addr, nil)
			if replacement {
				serial.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes(), Name: "replacement"})
			}
			serial.SetTransactionAccessRecorder(nil)
			writes, known, err := serial.CaptureTransactionWriteSet(mark, &rec, NewDynamicProperties())
			if err != nil || !known {
				t.Fatal(known, err)
			}
			inline, known, err := serial.CaptureTransactionRecorderWriteSetFiltered(&rec, NewDynamicProperties(), nil)
			if err != nil || !known || !reflect.DeepEqual(writes, inline) {
				t.Fatal("inline typed writes", known, err, writes, inline)
			}
			if err = publisher.ApplyTransactionWriteSet(writes, NewDynamicProperties(), ethrawdb.NewMemoryDatabase()); err != nil {
				t.Fatal(err)
			}
			var roots []tcommon.Hash
			for _, state := range []*StateDB{serial, publisher} {
				root, _, err := state.CommitWithStatsOptions(CommitOptions{BlockNumber: 1, BlockHash: tcommon.Hash{1}, CommitmentTxNum: 1})
				if err != nil {
					t.Fatal(err)
				}
				disk := state.db.DiskDB()
				assertCompactFullMuHash(t, disk)
				roots = append(roots, root)
				value, ok, err := rawdb.ReadStateKVLatest(disk, addr, 0, kvdomains.ContractABI, contractABIKVKey)
				if err != nil || ok != replacement {
					t.Fatal("deferred ABI", ok, err, value)
				}
			}
			if roots[0] != roots[1] {
				t.Fatal("serial/published roots differ", roots)
			}
		})
	}
}

func TestContractAtSurfacesCorruptMetadata(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := testAddr(0x71)

	if err := rawdb.WriteStateTxRange(db, 1, tcommon.Hash{0x01}, 1, 1); err != nil {
		t.Fatalf("write block 1 tx range: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, tcommon.Hash{0x02}, 2, 2); err != nil {
		t.Fatalf("write block 2 tx range: %v", err)
	}
	if err := rawdb.WriteStateKVGeneration(db, addr, 0); err != nil {
		t.Fatalf("write kv generation: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, addr, 0, kvdomains.ContractMetadata, contractMetaKVKey, []byte{0x80}); err != nil {
		t.Fatalf("write corrupt contract metadata: %v", err)
	}

	reader := NewPersistentHistoryReaderWithColdHistory(db, nil, 2, &keyedColdHistoryStub{})
	got, err := reader.ContractAt(addr, 1)
	if err == nil {
		t.Fatal("ContractAt corrupt metadata error = nil")
	}
	if got != nil {
		t.Fatalf("ContractAt corrupt metadata contract = %+v, want nil", got)
	}
	if !strings.Contains(err.Error(), "decode contract metadata at block 1") {
		t.Fatalf("ContractAt corrupt metadata error = %v, want decode contract metadata context", err)
	}
}

func TestContractAtUsesHistoricalAccountKVGeneration(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := testAddr(0x72)
	oldAccount := mustContractHistoryAccountEnvelope(t, 0)
	newAccount := mustContractHistoryAccountEnvelope(t, 1)
	oldMeta := mustContractHistoryMetadata(t, addr, "old-contract", []byte{0x01, 0x02})
	newMeta := mustContractHistoryMetadata(t, addr, "new-contract", []byte{0x03, 0x04})

	if err := rawdb.WriteStateTxRange(db, 1, tcommon.Hash{0x01}, 1, 1); err != nil {
		t.Fatalf("write block 1 tx range: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, tcommon.Hash{0x02}, 2, 2); err != nil {
		t.Fatalf("write block 2 tx range: %v", err)
	}
	if err := rawdb.WriteStateAccountLatest(db, addr, newAccount); err != nil {
		t.Fatalf("write latest account: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, addr, 1, kvdomains.ContractMetadata, contractMetaKVKey, newMeta); err != nil {
		t.Fatalf("write latest new metadata: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  tcommon.Hash{0x02},
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      addr,
		PrevExists: true,
		Prev:       oldAccount,
		NextExists: true,
		Next:       newAccount,
	}); err != nil {
		t.Fatalf("write account change: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  tcommon.Hash{0x02},
		TxNum:      2,
		Seq:        2,
		FlatDomain: rawdb.StateFlatDomainKVGeneration,
		Owner:      addr,
		PrevExists: true,
		Prev:       rawdb.EncodeStateKVGenerationValue(0),
		NextExists: true,
		Next:       rawdb.EncodeStateKVGenerationValue(1),
	}); err != nil {
		t.Fatalf("write generation change: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  tcommon.Hash{0x02},
		TxNum:      2,
		Seq:        3,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      addr,
		Generation: 0,
		Domain:     kvdomains.ContractMetadata,
		Key:        contractMetaKVKey,
		PrevExists: true,
		Prev:       oldMeta,
		NextExists: false,
	}); err != nil {
		t.Fatalf("write old metadata change: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  tcommon.Hash{0x02},
		TxNum:      2,
		Seq:        4,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      addr,
		Generation: 1,
		Domain:     kvdomains.ContractMetadata,
		Key:        contractMetaKVKey,
		PrevExists: false,
		NextExists: true,
		Next:       newMeta,
	}); err != nil {
		t.Fatalf("write new metadata change: %v", err)
	}

	reader := NewPersistentHistoryReaderWithColdHistory(db, nil, 2, &keyedColdHistoryStub{})
	gotOld, err := reader.ContractAt(addr, 1)
	if err != nil {
		t.Fatalf("ContractAt(block1): %v", err)
	}
	if gotOld == nil || gotOld.Name != "old-contract" || !bytes.Equal(gotOld.Bytecode, []byte{0x01, 0x02}) {
		t.Fatalf("ContractAt(block1) = %+v, want old-contract", gotOld)
	}
	gotNew, err := reader.ContractAt(addr, 2)
	if err != nil {
		t.Fatalf("ContractAt(block2): %v", err)
	}
	if gotNew == nil || gotNew.Name != "new-contract" || !bytes.Equal(gotNew.Bytecode, []byte{0x03, 0x04}) {
		t.Fatalf("ContractAt(block2) = %+v, want new-contract", gotNew)
	}
}

func mustContractHistoryAccountEnvelope(t *testing.T, generation uint64) []byte {
	t.Helper()
	envelope := &StateAccountV2{
		Version:             StateAccountVersion,
		AccountKVRoot:       EmptyKVRoot,
		AccountKVGeneration: generation,
	}
	raw, err := envelope.Encode()
	if err != nil {
		t.Fatalf("encode account envelope: %v", err)
	}
	return raw
}

func mustContractHistoryMetadata(t *testing.T, addr tcommon.Address, name string, bytecode []byte) []byte {
	t.Helper()
	raw, err := statecodec.Marshal(&contractpb.SmartContract{
		ContractAddress: addr.Bytes(),
		Name:            name,
		Bytecode:        bytecode,
	})
	if err != nil {
		t.Fatalf("marshal contract metadata: %v", err)
	}
	return raw
}
