package state

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Persist and reopen the account without touching its runtime bytes. A nil
// object code slice here means "not loaded", even though its hash is known.
func newUnloadedCodeCopyState(t *testing.T, sharedCacheOnly bool) (*StateDB, tcommon.Address, []byte) {
	t.Helper()
	cacheBytes := 0
	if sharedCacheOnly {
		cacheBytes = 4096
	}
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabaseWithConfig(disk, DatabaseConfig{CodeCacheSizeBytes: cacheBytes})
	t.Cleanup(func() { _ = db.Close() })
	source, err := New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0xc1)
	code := []byte{0x60, 0x01, 0x60, 0x00, 0x55, 0x00}
	source.CreateAccount(addr, corepb.AccountType_Contract)
	source.AddBalance(addr, 100)
	source.SetCode(addr, code)
	root, err := source.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if sharedCacheOnly {
		loader, err := New(root, db)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := loader.GetCodeStrict(addr); err != nil || !bytes.Equal(got, code) {
			t.Fatalf("cache warm-up = %x, err %v", got, err)
		}
		if err := rawdb.DeleteStateCode(disk, tcommon.Keccak256(code)); err != nil {
			t.Fatal(err)
		}
		if _, found, err := rawdb.ReadStateCodeStrict(disk, tcommon.Keccak256(code)); err != nil || found {
			t.Fatalf("cache-only fixture retained hot code: found=%v err=%v", found, err)
		}
	}
	reopened, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if balance := reopened.GetBalance(addr); balance != 100 {
		t.Fatalf("reopened balance = %d, want 100", balance)
	}
	obj := reopened.stateObjects[addr]
	if obj == nil {
		t.Fatal("account-only hydration did not retain the account")
	}
	if obj.code != nil || obj.codeDirty || obj.codeHash != tcommon.Keccak256(code) {
		t.Fatalf("account-only runtime: codeNil=%v codeLen=%d dirty=%v hash=%s",
			obj.code == nil, len(obj.code), obj.codeDirty, obj.codeHash.Hex())
	}
	return reopened, addr, code
}

func TestStateDBCopiesPreserveUnloadedRuntimeCode(t *testing.T) {
	for _, sharedCacheOnly := range []bool{false, true} {
		storage := "hot"
		if sharedCacheOnly {
			storage = "shared_cache_only"
		}
		for _, blockExecution := range []bool{false, true} {
			copyPath := "full"
			if blockExecution {
				copyPath = "dirty_block_execution"
			}
			t.Run(storage+"/"+copyPath, func(t *testing.T) {
				source, addr, code := newUnloadedCodeCopyState(t, sharedCacheOnly)
				copyFn := source.Copy
				if blockExecution {
					// A scalar update makes the account an eager execution-copy
					// candidate without loading or changing its contract code.
					source.AddBalance(addr, 1)
					copyFn = source.CopyBlockExecutionBase
				}
				cp, err := copyFn()
				if err != nil {
					t.Fatal(err)
				}
				copied := cp.stateObjects[addr]
				if copied == nil {
					t.Fatal("hydrated source account was omitted from copy")
				}
				if copied.code != nil {
					t.Errorf("copy turned unloaded nil code into non-nil code of length %d", len(copied.code))
				}
				got, err := cp.GetCodeStrict(addr)
				if err != nil || !bytes.Equal(got, code) {
					t.Fatalf("strict copied runtime = %x, err %v; want %x", got, err, code)
				}
				if source.stateObjects[addr].code != nil {
					t.Fatal("copy runtime read materialized the source object's code")
				}
				// The ordinary TVM read must also hydrate runtime bytes. Use a
				// separate object so the strict read above cannot mask an empty
				// code slice by having already populated this object's cache.
				ordinary, err := copyFn()
				if err != nil {
					t.Fatal(err)
				}
				if got := ordinary.GetCode(addr); !bytes.Equal(got, code) {
					t.Fatalf("ordinary copied runtime = %x, want %x", got, code)
				}
				if source.stateObjects[addr].code != nil {
					t.Fatal("ordinary copy runtime read materialized the source object's code")
				}
				if cp.GetBalance(addr) != source.GetBalance(addr) {
					t.Fatal("copy lost the source balance overlay")
				}
			})
		}
	}
}

func TestStateDBCopiesOwnLoadedRuntimeCode(t *testing.T) {
	for _, copyPath := range []string{"full", "clean_block_execution", "dirty_block_execution"} {
		t.Run(copyPath, func(t *testing.T) {
			source, addr, code := newUnloadedCodeCopyState(t, false)
			if got, err := source.GetCodeStrict(addr); err != nil || !bytes.Equal(got, code) {
				t.Fatalf("load source code = %x, err %v", got, err)
			}
			copyFn := source.Copy
			if copyPath != "full" {
				copyFn = source.CopyBlockExecutionBase
				if copyPath == "dirty_block_execution" {
					source.AddBalance(addr, 1)
				}
			}
			first, err := copyFn()
			if err != nil {
				t.Fatal(err)
			}
			second, err := copyFn()
			if err != nil {
				t.Fatal(err)
			}
			firstCode, err := first.GetCodeStrict(addr)
			if err != nil || !bytes.Equal(firstCode, code) {
				t.Fatalf("first copy code = %x, err %v", firstCode, err)
			}
			firstCode[0] ^= 0xff
			if got, err := source.GetCodeStrict(addr); err != nil || !bytes.Equal(got, code) {
				t.Fatalf("copy mutation changed source code: %x, err %v", got, err)
			}
			if got, err := second.GetCodeStrict(addr); err != nil || !bytes.Equal(got, code) {
				t.Fatalf("copy mutation changed sibling code: %x, err %v", got, err)
			}
		})
	}
}

func TestStateDBCopiesPreserveExplicitEmptyRuntimeCode(t *testing.T) {
	for _, materialized := range []bool{false, true} {
		codeState := "pending_empty"
		if materialized {
			codeState = "materialized_empty"
		}
		for _, blockExecution := range []bool{false, true} {
			copyPath := "full"
			if blockExecution {
				copyPath = "dirty_block_execution"
			}
			t.Run(codeState+"/"+copyPath, func(t *testing.T) {
				source, addr, _ := newUnloadedCodeCopyState(t, false)
				source.SetCode(addr, []byte{})
				obj := source.stateObjects[addr]
				if materialized {
					// Retain a successfully materialized empty value as distinct
					// from the pending-empty setter's nil code representation.
					obj.code = []byte{}
				}
				copyFn := source.Copy
				if blockExecution {
					copyFn = source.CopyBlockExecutionBase
				}
				cp, err := copyFn()
				if err != nil {
					t.Fatal(err)
				}
				copied := cp.stateObjects[addr]
				if copied == nil {
					t.Fatal("pending empty-code account was omitted from copy")
				}
				if (copied.code == nil) != (obj.code == nil) ||
					copied.codeDirty != obj.codeDirty || copied.codeHash != tcommon.Keccak256(nil) {
					t.Fatalf("copied empty runtime: codeNil=%v (want %v) codeLen=%d dirty=%v hash=%s",
						copied.code == nil, obj.code == nil, len(copied.code), copied.codeDirty, copied.codeHash.Hex())
				}
				if got, err := cp.GetCodeStrict(addr); err != nil || len(got) != 0 {
					t.Fatalf("explicit empty code = %x, err %v", got, err)
				}
				if got := cp.GetCodeHash(addr); got != tcommon.Keccak256(nil) {
					t.Fatalf("explicit empty code hash = %x, want Keccak256(empty)", got)
				}
			})
		}
	}
}
