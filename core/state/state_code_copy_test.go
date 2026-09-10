package state

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// An account can be materialized before its immutable code is read. The sender
// chain workers copy such accounts, and nil code must remain a lazy read rather
// than becoming an apparently loaded empty slice with a non-empty code hash.
func TestStateDBCopiesLoadUnmaterializedCode(t *testing.T) {
	for _, mode := range []string{"full", "block_execution"} {
		for _, source := range []string{"hot", "cold"} {
			for _, strict := range []bool{false, true} {
				name := mode + "/" + source + "/permissive"
				if strict {
					name = mode + "/" + source + "/strict"
				}
				t.Run(name, func(t *testing.T) {
					db := newTestStateDB(t).db
					t.Cleanup(func() { _ = db.Close() })
					db.codeCache.close() // Force the intended hot/cold reader in each copy.
					addr := testAddr(0x39)
					code := []byte{0x60, 0x2a, 0x60, 0x00, 0x52, 0x00}
					hash := tcommon.Keccak256(code)
					s := stateDBWithCodeHash(db, addr, hash)
					obj := s.stateObjects[addr]
					obj.dirtySet = s.dirtyObjects
					obj.markDirty() // Dirty account with unchanged, still-lazy code.
					if source == "hot" {
						if err := rawdb.WriteStateCode(db.DiskDB(), hash, code); err != nil {
							t.Fatal(err)
						}
					} else {
						s.SetCodeColdHistory(&countingColdCodeHistory{code: code}, 99)
					}
					copyFn := s.Copy
					if mode == "block_execution" {
						copyFn = s.CopyBlockExecutionBase
					}
					cp, err := copyFn()
					if err != nil {
						t.Fatal(err)
					}
					var got []byte
					if strict {
						got, err = cp.GetCodeStrict(addr)
					} else {
						got = cp.GetCode(addr)
					}
					if err != nil || !bytes.Equal(got, code) {
						t.Fatalf("copied lazy code = %x, err=%v; want %x", got, err, code)
					}
					if obj.code != nil || obj.codeDirty || obj.codeHash != hash {
						t.Fatal("loading the copy mutated the source's lazy code state")
					}
				})
			}
		}
	}
}

func TestStateDBCopiesPreserveCodeMaterialization(t *testing.T) {
	for _, mode := range []string{"full", "block_execution"} {
		for _, fixture := range []struct {
			name string
			code []byte
		}{{"nil", nil}, {"empty", []byte{}}, {"loaded", []byte{0x60, 0x01}}} {
			t.Run(mode+"/"+fixture.name, func(t *testing.T) {
				code := fixture.code
				s := newTestStateDB(t)
				t.Cleanup(func() { _ = s.db.Close() })
				addr := testAddr(0x3a)
				obj := s.getOrCreateAccount(addr)
				obj.code, obj.codeHash = bytes.Clone(code), tcommon.Keccak256(code)
				obj.codeDirty = true // Explicit writes/deletions must not hydrate old code.
				copyFn := s.Copy
				if mode == "block_execution" {
					copyFn = s.CopyBlockExecutionBase
				}
				cp, err := copyFn()
				if err != nil {
					t.Fatal(err)
				}
				copied := cp.stateObjects[addr]
				if copied == nil || (copied.code == nil) != (code == nil) || !bytes.Equal(copied.code, code) || !copied.codeDirty {
					t.Fatal("copy changed code materialization or dirty state")
				}
				if len(copied.code) != 0 {
					copied.code[0] ^= 1
					if !bytes.Equal(obj.code, code) {
						t.Fatal("copy shares mutable code with its source")
					}
				}
			})
		}
	}
}
