package state

import (
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Java's semantic absence for an encoded zero must not erase its physical
// before-image. These fixtures deliberately seal raw rows before using StateDB.
func TestMuHashStoragePhysicalZeroBefore(t *testing.T) {
	for _, raw := range [][]byte{{}, make([]byte, 32)} {
		for _, deleteFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes%d/delete%v", len(raw), deleteFirst), func(t *testing.T) {
				disk := rawdb.NewMemoryDatabase()
				defer disk.Close()
				db := NewDatabase(rawdb.WrapKeyValueStore(disk))
				defer db.Close()
				s, err := New(common.Hash{}, db)
				if err != nil {
					t.Fatal(err)
				}
				addr := testAddr(0xa9)
				key := common.Hash{31: 3}
				s.CreateAccount(addr, corepb.AccountType_Contract)
				if _, err = s.Commit(); err != nil {
					t.Fatal(err)
				}
				row := s.storageRowKey(addr, key)
				if err = rawdb.WriteStateKVLatest(disk, addr, 0, kvdomains.ContractStorage, row[:], raw); err != nil {
					t.Fatal(err)
				}
				root, err := domains.RebuildMuHashCommitment(disk)
				if err != nil {
					t.Fatal(err)
				}
				s, err = New(root, db)
				if err != nil {
					t.Fatal(err)
				}
				if v, exists := s.GetStateWithExist(addr, key); exists || v != (common.Hash{}) {
					t.Fatal("zero fixture has wrong Java semantics")
				}
				values := []common.Hash{{31: 9}, {}}
				if deleteFirst {
					values = []common.Hash{{}, {31: 9}}
				}
				for _, v := range values {
					s.SetState(addr, key, v)
					s.FinalizeTransaction()
					root, err = s.Commit()
					if err != nil {
						t.Fatal(err)
					}
					oracle := muhash3072.NewAccumulator()
					for _, prefix := range rawdb.StateCommitmentSourcePrefixes() {
						it := disk.NewIterator(prefix, nil)
						for it.Next() {
							e, err := rawdb.EncodeStateCommitmentElement(it.Key(), it.Value())
							if err != nil {
								t.Fatal(err)
							}
							if err = oracle.Insert(e); err != nil {
								t.Fatal(err)
							}
						}
						err = it.Error()
						it.Release()
						if err != nil {
							t.Fatal(err)
						}
					}
					expected, err := oracle.Digest()
					if err != nil || common.Hash(expected) != root {
						t.Fatalf("physical-set root mismatch got=%x want=%x err=%v", root, expected, err)
					}
				}
			})
		}
	}
}
