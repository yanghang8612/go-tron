package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/common/muhash3072"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type sloadLifecycleReadCounter struct {
	ethdb.KeyValueStore
	prefix []byte
	reads  atomic.Uint64
}

func (s *sloadLifecycleReadCounter) Get(key []byte) ([]byte, error) {
	if bytes.HasPrefix(key, s.prefix) {
		s.reads.Add(1)
	}
	return s.KeyValueStore.Get(key)
}

func (s *sloadLifecycleReadCounter) View(key []byte, fn func([]byte) error) error {
	if bytes.HasPrefix(key, s.prefix) {
		s.reads.Add(1)
	}
	return s.KeyValueStore.(interface {
		View([]byte, func([]byte) error) error
	}).View(key, fn)
}

// Keep the complete block trace identical across cache policies: roots and
// durable post-images must agree even as physical-latest point reads differ.
// These counts measure calls into the durable store, not SSD cache misses.
func TestSLOADFixedLifecycleEvidence(t *testing.T) {
	// Different cache policies must match a full physical-set MuHash oracle.
	var referenceRoots string
	for _, cacheBytes := range []int{0, 1 << 20} {
		t.Run(fmt.Sprintf("cache_%d", cacheBytes), func(t *testing.T) {
			disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()
			history, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer history.Close()
			routed := rawdb.NewDirectHistoryStore(disk, history)
			addr := testAddr(0xf3)
			counted := &sloadLifecycleReadCounter{KeyValueStore: routed, prefix: rawdb.StateKVLatestCommitmentKey(addr, 0, kvdomains.ContractStorage, nil)}
			db := NewDatabase(rawdb.WrapKeyValueStore(counted))
			defer db.Close()
			s, err := New(tcommon.Hash{}, db)
			if err != nil {
				t.Fatal(err)
			}
			s.CreateAccount(addr, corepb.AccountType_Contract)
			var slots [64]tcommon.Hash
			for j := range slots {
				binary.BigEndian.PutUint64(slots[j][24:], uint64(j+1))
				if j%4 != 0 {
					s.SetState(addr, slots[j], tcommon.Hash{31: byte(j + 1)})
				}
			}
			if _, err := s.Commit(); err != nil {
				t.Fatal(err)
			}
			// Include an encoded durable zero in addition to truly missing rows.
			row := s.storageRowKey(addr, slots[8])
			if err := rawdb.WriteStateKVLatest(disk, addr, 0, kvdomains.ContractStorage, row[:], make([]byte, 32)); err != nil {
				t.Fatal(err)
			}
			// The explicit present-zero raw fixture injection changes the physical
			// set; reseal it with the offline oracle before measuring incremental work.
			seedRoot, err := domains.RebuildMuHashCommitment(routed)
			if err != nil {
				t.Fatal(err)
			}
			seedHead, present, err := rawdb.ReadStateCommitmentHead(routed)
			if err != nil || !present || seedHead.Root != seedRoot {
				t.Fatal("fixture seed HEAD mismatch")
			}
			buf := blockbuffer.New(counted)
			buf.SetBaseReadCacheSizeWithTrunk(cacheBytes, 5, rawdb.CommitmentBranchKeyPrefix)
			s.SetAccountKVIndexStore(buf)
			scope := s.NewCommitScope()
			defer scope.Discard()
			counted.reads.Store(0)
			roots := sha256.New()
			for i := 0; i < 100; i++ {
				number := uint64(i + 1)
				buf.BeginBlock(pruneBlockHash(i+1), number)
				if i%5 == 0 {
					s.stateObjects[addr].releaseStorage()
				}
				for repeat := 0; repeat < 2; repeat++ {
					for j, slot := range slots {
						got, exists := s.GetStateWithExist(addr, slot)
						if j >= 4 && (exists != (j%4 != 0) || (exists && got != (tcommon.Hash{31: byte(j + 1)}))) {
							t.Fatalf("slot %d = %x/%v", j, got, exists)
						}
					}
				}
				for j := 0; j < 4; j++ {
					s.SetState(addr, slots[j], tcommon.Hash{30: byte(i & 1), 31: byte(j + 1)})
				}
				rev := s.Snapshot()
				s.SetState(addr, slots[5], tcommon.Hash{0: 0xaa})
				s.RevertToSnapshot(rev)
				if got, exists := s.GetStateWithExist(addr, slots[5]); !exists || got != (tcommon.Hash{31: 6}) {
					t.Fatalf("reverted %x/%v", got, exists)
				}
				root, _, err := s.CommitWithStatsOptionsInScope(scope, CommitOptions{BlockNumber: number})
				if err != nil {
					t.Fatal(err)
				}
				roots.Write(root[:])

				if i%7 == 0 {
					s.stateObjects[addr].releaseStorage()
					if got, exists := s.GetStateWithExist(addr, slots[0]); !exists || got != (tcommon.Hash{30: byte(i & 1), 31: 1}) {
						t.Fatalf("pending %x/%v", got, exists)
					}
				}
				if err := scope.FlushLatest(); err != nil {
					t.Fatal(err)
				}
				buf.CommitBlock()
				oracle := muhash3072.NewAccumulator()
				for _, prefix := range rawdb.StateCommitmentSourcePrefixes() {
					it := buf.NewIterator(prefix, nil)
					for it.Next() {
						element, e := rawdb.EncodeStateCommitmentElement(it.Key(), it.Value())
						if e != nil {
							t.Fatal(e)
						}
						if e = oracle.Insert(element); e != nil {
							t.Fatal(e)
						}
					}
					e := it.Error()
					it.Release()
					if e != nil {
						t.Fatal(e)
					}
				}
				expected, e := oracle.Digest()
				if e != nil || tcommon.Hash(expected) != root {
					t.Fatalf("block%d physical-set root=%x incremental=%x err=%v", i, expected, root, e)
				}
				if err := buf.Flush(counted); err != nil {
					t.Fatal(err)
				}
			}
			hash := fmt.Sprintf("%x", roots.Sum(nil))
			if referenceRoots == "" {
				referenceRoots = hash
			}
			if referenceRoots != hash {
				t.Fatalf("root sequence differs: %s != %s", hash, referenceRoots)
			}
			reads := counted.reads.Load()
			row = s.storageRowKey(addr, slots[5])
			if v, ok, err := rawdb.ReadStateKVLatest(disk, addr, 0, kvdomains.ContractStorage, row[:]); err != nil || !ok || tcommon.BytesToHash(v) != (tcommon.Hash{31: 6}) {
				t.Fatalf("durable %x/%v/%v", v, ok, err)
			}
			t.Logf("blocks=100 roots_sha256=%s durable_storage_point_reads=%d", hash, reads)
		})
	}
}

// Resolving the object once must retain every dependency consumed by a cold
// SLOAD, including the namespace generation and the physical logical row.
func TestSLOADResolvedObjectPreservesDependencies(t *testing.T) {
	s, reader, addr := stateWithStorageReader(t)
	reader.value = []byte{0x77}
	reader.found = true
	slot := tcommon.Hash{31: 0x21}
	row := s.storageRowKey(addr, slot)
	var recorder TransactionAccessRecorder
	recorder.Reset(8)
	s.SetTransactionAccessRecorder(&recorder)
	if got, exists := s.GetStateWithExist(addr, slot); !exists || got != (tcommon.Hash{31: 0x77}) {
		t.Fatalf("storage %x/%v", got, exists)
	}
	for _, key := range []TransactionAccessKey{
		{Kind: TransactionAccessStorage, Address: addr, StorageKey: slot},
		{Kind: TransactionAccessContractMetadata, Address: addr},
		{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldExistence},
		{Kind: TransactionAccessAccountKVGeneration, Address: addr},
		{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.ContractStorage, LogicalKey: string(row[:])},
	} {
		if mode := recorder.accessMode(key); mode != TransactionAccessRead {
			t.Errorf("dependency %+v mode=%v, want read", key, mode)
		}
	}
	// The pooled physical-row scratch may be overwritten on the next read.
	if _, exists := s.GetStateWithExist(addr, tcommon.Hash{31: 0x22}); !exists {
		t.Fatal("second storage row absent")
	}
	if mode := recorder.accessMode(TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.ContractStorage, LogicalKey: string(row[:])}); mode != TransactionAccessRead {
		t.Fatal("first logical key changed after scratch reuse")
	}
}

func TestSLOADColdMetadataAndLifecycle(t *testing.T) {
	for _, version := range []int32{0, 1} {
		t.Run(fmt.Sprintf("version_%d", version), func(t *testing.T) {
			source := newTestStateDB(t)
			addr := testAddr(0xf4)
			meta := &contractpb.SmartContract{Version: version, TrxHash: bytes.Repeat([]byte{0x91}, tcommon.HashLength)}
			a, b := tcommon.Hash{31: 1}, tcommon.Hash{31: 2}
			source.CreateAccount(addr, corepb.AccountType_Contract)
			source.SetContract(addr, meta)
			source.SetState(addr, a, tcommon.Hash{31: 0x61})
			source.SetState(addr, b, tcommon.Hash{31: 0x62})
			root, err := source.Commit()
			if err != nil {
				t.Fatal(err)
			}
			s, err := New(root, source.db)
			if err != nil {
				t.Fatal(err)
			}
			obj := s.getStateObject(addr)
			if obj.storageKeyLayoutCached || obj.contractRuntimeLoaded || obj.contractMeta != nil {
				t.Fatal("metadata unexpectedly warm")
			}
			var recorder TransactionAccessRecorder
			recorder.Reset(8)
			s.SetTransactionAccessRecorder(&recorder)
			if got, ok := s.GetStateWithExist(addr, a); !ok || got != (tcommon.Hash{31: 0x61}) {
				t.Fatalf("first cold SLOAD=%x/%v", got, ok)
			}
			row := javaStorageRowKey(addr, a, meta)
			for _, key := range []TransactionAccessKey{
				{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.ContractMetadata, LogicalKey: string(contractMetaKVKey)},
				{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.ContractStorage, LogicalKey: string(row[:])},
			} {
				if recorder.accessMode(key) != TransactionAccessRead {
					t.Fatalf("cold metadata dependency absent: %+v", key)
				}
			}
			s.SetTransactionAccessRecorder(nil)
			rev := s.Snapshot()
			replacement := &contractpb.SmartContract{Version: 1 - version, TrxHash: bytes.Repeat([]byte{0x92}, tcommon.HashLength)}
			row = javaStorageRowKey(addr, a, replacement)
			if err := rawdb.WriteStateKVLatest(s.db.DiskDB(), addr, 0, kvdomains.ContractStorage, row[:], []byte{0x71}); err != nil {
				t.Fatal(err)
			}
			s.SetContract(addr, replacement)
			obj.releaseStorage()
			if got, ok := s.GetStateWithExist(addr, a); !ok || got != (tcommon.Hash{31: 0x71}) {
				t.Fatalf("changed metadata SLOAD=%x/%v", got, ok)
			}
			s.RevertToSnapshot(rev)
			obj = s.stateObjects[addr]
			obj.releaseStorage()
			if got, ok := s.GetStateWithExist(addr, a); !ok || got != (tcommon.Hash{31: 0x61}) {
				t.Fatalf("reverted metadata SLOAD=%x/%v", got, ok)
			}
			rev = s.Snapshot()
			if err := s.ResetAccountKV(addr); err != nil {
				t.Fatal(err)
			}
			if got, ok := s.GetStateWithExist(addr, a); ok || got != (tcommon.Hash{}) {
				t.Fatalf("reset cached slot SLOAD=%x/%v", got, ok)
			}
			// B has never entered the object cache: a fresh-generation cold read must
			// not reach the old generation's durable row.
			if got, ok := s.GetStateWithExist(addr, b); ok || got != (tcommon.Hash{}) {
				t.Fatalf("fresh generation SLOAD=%x/%v", got, ok)
			}
			s.RevertToSnapshot(rev)
			if got, ok := s.GetStateWithExist(addr, a); !ok || got != (tcommon.Hash{31: 0x61}) {
				t.Fatalf("reverted cached generation SLOAD=%x/%v", got, ok)
			}
			if got, ok := s.GetStateWithExist(addr, b); !ok || got != (tcommon.Hash{31: 0x62}) {
				t.Fatalf("reverted generation SLOAD=%x/%v", got, ok)
			}
			for _, v := range []byte{0x81, 0x82} {
				s.SetTransactionVersionedValueReader(testTransactionVersionedReader{
					{Kind: TransactionAccessStorage, Address: addr, StorageKey: b}: {{txIndex: 2, value: TransactionWriteValue{Exists: true, Value: []byte{v}}}},
				}, 3)
				if got, ok := s.GetStateWithExist(addr, b); !ok || got != (tcommon.Hash{31: v}) {
					t.Fatalf("versioned rebind SLOAD=%x/%v want=%x", got, ok, v)
				}
			}
		})
	}
}

func TestSLOADNestedResetRevertRestoresDirtyNamespace(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(0xf5)
	a, b := tcommon.Hash{31: 1}, tcommon.Hash{31: 2}
	s.CreateAccount(addr, corepb.AccountType_Contract)
	s.SetContract(addr, &contractpb.SmartContract{Version: 1, TrxHash: bytes.Repeat([]byte{0x91}, tcommon.HashLength)})
	root, err := s.Commit()
	if err != nil {
		t.Fatal(err)
	}
	s, err = New(root, s.db)
	if err != nil {
		t.Fatal(err)
	}
	outerMeta := &contractpb.SmartContract{TrxHash: bytes.Repeat([]byte{0x92}, tcommon.HashLength)}
	s.SetContract(addr, outerMeta)
	s.SetState(addr, a, tcommon.Hash{31: 0x99})
	outer := s.Snapshot()
	if err := s.ResetAccountKV(addr); err != nil {
		t.Fatal(err)
	}
	// The reset owns a fresh namespace even when the old slot was dirty.
	if got, ok := s.GetStateWithExist(addr, a); ok || got != (tcommon.Hash{}) {
		t.Fatalf("reset dirty slot=%x/%v", got, ok)
	}
	// A retained caller pointer must not mutate the metadata journal pre-image.
	outerMeta.TrxHash[0] = 0xff
	innerMeta := &contractpb.SmartContract{Version: 1, TrxHash: bytes.Repeat([]byte{0x93}, tcommon.HashLength)}
	s.SetContract(addr, innerMeta)
	s.SetState(addr, b, tcommon.Hash{31: 0xaa})
	inner := s.Snapshot()
	if err := s.ResetAccountKV(addr); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.GetStateWithExist(addr, b); ok || got != (tcommon.Hash{}) {
		t.Fatalf("nested reset slot=%x/%v", got, ok)
	}
	s.RevertToSnapshot(inner)
	if got, ok := s.GetStateWithExist(addr, b); !ok || got != (tcommon.Hash{31: 0xaa}) {
		t.Fatalf("inner restored slot=%x/%v", got, ok)
	}
	if got := s.GetContract(addr); got == nil || got.Version != 1 || !bytes.Equal(got.TrxHash, bytes.Repeat([]byte{0x93}, tcommon.HashLength)) {
		t.Fatalf("inner dirty metadata=%v", got)
	}
	s.RevertToSnapshot(outer)
	if got, ok := s.GetStateWithExist(addr, a); !ok || got != (tcommon.Hash{31: 0x99}) {
		t.Fatalf("outer restored dirty slot=%x/%v", got, ok)
	}
	if got, ok := s.GetStateWithExist(addr, b); ok || got != (tcommon.Hash{}) {
		t.Fatalf("nested namespace leaked=%x/%v", got, ok)
	}
	if got := s.GetContract(addr); got == nil || got.Version != 0 || !bytes.Equal(got.TrxHash, bytes.Repeat([]byte{0x92}, tcommon.HashLength)) {
		t.Fatalf("outer dirty metadata=%v", got)
	}
	root, err = s.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, s.db)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := reloaded.GetStateWithExist(addr, a); !ok || got != (tcommon.Hash{31: 0x99}) {
		t.Fatalf("durable restored dirty slot=%x/%v", got, ok)
	}
	if got, ok := reloaded.GetStateWithExist(addr, b); ok || got != (tcommon.Hash{}) {
		t.Fatalf("durable nested namespace leaked=%x/%v", got, ok)
	}
	if got := reloaded.GetContract(addr); got == nil || got.Version != 0 || !bytes.Equal(got.TrxHash, bytes.Repeat([]byte{0x92}, tcommon.HashLength)) {
		t.Fatalf("durable dirty metadata=%v", got)
	}
}
