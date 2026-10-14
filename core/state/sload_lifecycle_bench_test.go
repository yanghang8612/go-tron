package state

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// BenchmarkSLOADLifecycle keeps the operation trace fixed: a block reads 64
// slots twice, overwrites four, and reverts a fifth. Every fifth block replaces
// the clean object cache, exercising durable and bounded-base-cache reads.
// The trace includes zero/missing rows and scope pending writes. A benchmark
// operation is one complete block, including state-root commit.
func BenchmarkSLOADLifecycle(b *testing.B) { benchmarkSLOADLifecycle(b, false) }

func BenchmarkSLOADDirectHistoryLifecycle(b *testing.B) { benchmarkSLOADLifecycle(b, true) }

func benchmarkSLOADLifecycle(b *testing.B, directHistory bool) {
	for _, cacheBytes := range []int{0, 1 << 20} {
		name := "uncached"
		if cacheBytes != 0 {
			name = "cached"
		}
		b.Run(name, func(b *testing.B) {
			disk := newSLOADBenchmarkStore(b, directHistory)
			defer disk.Close()
			db := NewDatabase(rawdb.WrapKeyValueStore(disk))
			defer db.Close()
			s, err := New(tcommon.Hash{}, db)
			if err != nil {
				b.Fatal(err)
			}
			addr := testAddr(0xf1)
			s.CreateAccount(addr, corepb.AccountType_Contract)
			var slots [64]tcommon.Hash
			for j := range slots {
				binary.BigEndian.PutUint64(slots[j][24:], uint64(j+1))
				if j%4 != 0 {
					s.SetState(addr, slots[j], tcommon.Hash{31: byte(j + 1)})
				}
			}
			if _, err := s.Commit(); err != nil {
				b.Fatal(err)
			}
			buf := blockbuffer.New(disk)
			buf.SetBaseReadCacheSize(cacheBytes)
			s.SetAccountKVIndexStore(buf)
			scope := s.NewCommitScope()
			defer scope.Discard()
			// Hydrate the same missing metadata as production, rather than inventing
			// metadata bytes; the derived Java storage layout stays unchanged.
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				number := uint64(i + 1)
				buf.BeginBlock(pruneBlockHash(i+1), number)
				if i%5 == 0 {
					s.stateObjects[addr].releaseStorage()
				}
				for repeat := 0; repeat < 2; repeat++ {
					for _, slot := range slots {
						s.GetStateWithExist(addr, slot)
					}
				}
				for j := 0; j < 4; j++ {
					s.SetState(addr, slots[j], tcommon.Hash{30: byte(i & 1), 31: byte(j + 1)})
				}
				rev := s.Snapshot()
				s.SetState(addr, slots[5], tcommon.Hash{0: 0xaa})
				s.RevertToSnapshot(rev)
				if got, exists := s.GetStateWithExist(addr, slots[5]); !exists || got != (tcommon.Hash{31: 6}) {
					b.Fatalf("reverted storage=%x/%v", got, exists)
				}
				if _, _, err := s.CommitWithStatsOptionsInScope(scope, CommitOptions{BlockNumber: number}); err != nil {
					b.Fatal(err)
				}
				if i%7 == 0 {
					s.stateObjects[addr].releaseStorage()
					if got, exists := s.GetStateWithExist(addr, slots[0]); !exists || got != (tcommon.Hash{30: byte(i & 1), 31: 1}) {
						b.Fatalf("scope pending storage=%x/%v", got, exists)
					}
				}
				if err := scope.FlushLatest(); err != nil {
					b.Fatal(err)
				}
				buf.CommitBlock()
				if err := buf.Flush(disk); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			row := s.storageRowKey(addr, slots[5])
			if v, ok, err := rawdb.ReadStateKVLatest(disk, addr, 0, kvdomains.ContractStorage, row[:]); err != nil || !ok || tcommon.BytesToHash(v) != (tcommon.Hash{31: 6}) {
				b.Fatalf("durable reverted storage=%x/%v/%v", v, ok, err)
			}
		})
	}
}

// BenchmarkSLOADReadPath isolates fixed repeated durable/cache reads from
// commitment work. Each cycle invalidates only the object cache; the base-cache
// policy itself is unchanged. Slots include present, zero and absent rows.
func BenchmarkSLOADReadPath(b *testing.B) { benchmarkSLOADReadPath(b, false) }

func BenchmarkSLOADDirectHistoryReadPath(b *testing.B) { benchmarkSLOADReadPath(b, true) }

func benchmarkSLOADReadPath(b *testing.B, directHistory bool) {
	for _, cacheBytes := range []int{0, 1 << 20} {
		name := "uncached"
		if cacheBytes != 0 {
			name = "cached"
		}
		b.Run(name, func(b *testing.B) {
			disk := newSLOADBenchmarkStore(b, directHistory)
			defer disk.Close()
			db := NewDatabase(rawdb.WrapKeyValueStore(disk))
			defer db.Close()
			s, err := New(tcommon.Hash{}, db)
			if err != nil {
				b.Fatal(err)
			}
			addr := testAddr(0xf2)
			s.CreateAccount(addr, corepb.AccountType_Contract)
			var slots [128]tcommon.Hash
			for j := range slots {
				binary.BigEndian.PutUint64(slots[j][24:], uint64(j+1))
				if j%2 == 0 {
					s.SetState(addr, slots[j], tcommon.Hash{31: byte(j + 1)})
				}
			}
			if _, err := s.Commit(); err != nil {
				b.Fatal(err)
			}
			// A durable encoded zero must read as absent, as java-tron does.
			for j := 1; j < len(slots); j += 8 {
				row := s.storageRowKey(addr, slots[j])
				if err := rawdb.WriteStateKVLatest(disk, addr, 0, kvdomains.ContractStorage, row[:], make([]byte, 32)); err != nil {
					b.Fatal(err)
				}
			}
			buf := blockbuffer.New(disk)
			buf.SetBaseReadCacheSize(cacheBytes)
			s.SetAccountKVIndexStore(buf)
			scope := s.NewCommitScope()
			defer scope.Discard()
			obj := s.stateObjects[addr]
			// Hydrate key layout and cached rows before timing.
			for _, slot := range slots {
				s.GetStateWithExist(addr, slot)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i&127 == 0 {
					clear(obj.storage)
				}
				got, exists := s.GetStateWithExist(addr, slots[i&127])
				if exists != (i&1 == 0) || (exists && got != (tcommon.Hash{31: byte((i & 127) + 1)})) {
					b.Fatal("fixed input mismatch")
				}
			}
		})
	}
}

// Production uses the fixed hot/history router even for a hot storage row.
// Keep both engines and the exact block/storage trace in the wrapper variant.
func newSLOADBenchmarkStore(b *testing.B, directHistory bool) ethdb.KeyValueStore {
	b.Helper()
	hot, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
	if err != nil {
		b.Fatal(err)
	}
	if !directHistory {
		return hot
	}
	history, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
	if err != nil {
		hot.Close()
		b.Fatal(err)
	}
	return rawdb.NewDirectHistoryStore(hot, history)
}
