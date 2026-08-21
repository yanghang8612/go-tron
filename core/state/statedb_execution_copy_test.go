package state

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

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
	if got := cp.GetCode(contract); !bytes.Equal(got, code) {
		t.Fatalf("execution-copy code = %x, want cached %x", got, code)
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
