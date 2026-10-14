package blockbuffer

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestCommitmentTailCapacityPreservesBaseReservations(t *testing.T) {
	const base, tail = (512 << 20) + 17, (512 << 20) + 31
	before := newBaseReadCacheWithTrunk(base, 4, rawdb.CommitmentBranchKeyPrefix)
	after := newBaseReadCacheWithTrunkAndTail(base, tail, 4, rawdb.CommitmentBranchKeyPrefix)
	if before.stats().budgets != after.stats().budgets || after.stats().capacity != base+tail {
		t.Fatalf("reservations/capacity: before=%+v after=%+v", before.stats(), after.stats())
	}
	if len(before.invalidations) != len(after.invalidations) {
		t.Fatal("tail capacity changed invalidation metadata")
	}
	for i := range before.shards {
		a, b := &before.shards[i], &after.shards[i]
		if len(a.admission) != len(b.admission) || len(a.nonCommitmentAdmission) != len(b.nonCommitmentAdmission) || a.limit != b.limit-b.additionalTail || a.limit > b.limit {
			t.Fatalf("shard %d changed base metadata", i)
		}
	}
	if newBaseReadCacheWithTrunkAndTail(0, tail, 4, rawdb.CommitmentBranchKeyPrefix) != nil {
		t.Fatal("tail enabled disabled base")
	}
	for _, invalid := range []int{-1, int(^uint(0) >> 1)} {
		c := newBaseReadCacheWithTrunkAndTail(base, invalid, 4, rawdb.CommitmentBranchKeyPrefix)
		if c.stats().capacity != base {
			t.Fatal("invalid tail budget did not preserve base")
		}
	}
}

func TestCommitmentTailCapacityPreservesIdleBackingBound(t *testing.T) {
	// Direct shard construction mirrors the existing recycling tests. The
	// additional resident budget must not grow the idle payload pool.
	s := baseReadCacheShard{limit: (8 << 10) + (1 << 20), additionalTail: 1 << 20}
	first := s.acquireEntryString("first", bytes.Repeat([]byte{1}, 768), false, 1)
	second := s.acquireEntryString("second", bytes.Repeat([]byte{2}, 768), false, 1)
	s.recycleEntry(first)
	s.recycleEntry(second)
	if s.freeValueBytes != 768 || second.value != nil {
		t.Fatalf("idle backing exceeded original base/8 bound: %d", s.freeValueBytes)
	}
}

// The fixture uses real parent snapshots, branch sessions, Pebble reads, layer
// publication and canonical flush. Values intentionally vary in length. It
// measures depth-6/7 persistence work; full trie-fold time is measured separately
// in domains, since this operation trace does not hash/decode branch payloads.
type commitmentTailReplay struct {
	disk   ethdb.KeyValueStore
	buf    *Buffer
	keys   [][]byte
	values [][]byte
	step   uint64
	mode   string
}

func newCommitmentTailReplay(t testing.TB, tail int, mode string) *commitmentTailReplay {
	t.Helper()
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = disk.Close() })
	r := &commitmentTailReplay{disk: disk, buf: New(disk), mode: mode}
	r.buf.SetBaseReadCacheSizeWithTrunkAndTail(64<<10, tail, 4, rawdb.CommitmentBranchKeyPrefix)
	for i := 0; i < 1024; i++ {
		r.keys = append(r.keys, commitmentAdmissionKey(uint32(i), byte(i&1), 1701))
		value := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 256+(i%3)*128)
		r.values = append(r.values, value)
		if err := disk.Put(r.keys[i], value); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func (r *commitmentTailReplay) run() (uint64, error) {
	r.buf.clearBaseReadCache()
	var durable uint64
	for block := 0; block < 8; block++ {
		r.step++
		r.buf.BeginBlock(bufHash(byte(r.step)), r.step)
		h, _ := r.buf.NewestInflight()
		view := r.buf.ViewLayer(h)
		session, err := view.NewCommitmentParentReadSession(2)
		if err != nil {
			return durable, err
		}
		read := func(id int, prefetch bool) error {
			key := r.keys[id]
			reader := id & 1
			prefixLen := len(key) - 6 - reader
			if prefetch {
				if _, err := session.(pointread.CommitmentParentPrefetchSession).PrefetchKeyParts(reader, key[:prefixLen], key[prefixLen:]); err != nil {
					return err
				}
			}
			found, err := session.ViewKeyParts(reader, key[:prefixLen], key[prefixLen:], func(value []byte, _ bool) error {
				if !bytes.Equal(value, r.values[id]) {
					return fmt.Errorf("wrong value for %d", id)
				}
				return nil
			})
			if err == nil && !found {
				return fmt.Errorf("missing branch %d", id)
			}
			return err
		}
		ids := make([]int, 0, 320)
		if r.mode != "cold_scan" {
			for i := 0; i < 192; i++ {
				ids = append(ids, (i*53+block*7)%192)
			}
		}
		if r.mode != "hot" {
			for i := 0; i < 96; i++ {
				ids = append(ids, 192+(block*96+i)%768)
			}
		}
		for _, id := range ids {
			if err := read(id, r.mode == "prefetch_scan"); err != nil {
				_ = session.Close()
				return durable, err
			}
		}
		if r.mode == "delayed_read" && block > 1 {
			for i := 0; i < 96; i++ {
				if err := read(192+((block-2)*96+i)%768, false); err != nil {
					_ = session.Close()
					return durable, err
				}
			}
		}
		for _, ctx := range session.(*commitmentParentReadSession).readContexts {
			durable += ctx.durableReads + ctx.prefetchDurable
		}
		if err := session.Close(); err != nil {
			return durable, err
		}
		for _, id := range ids {
			next := bytes.Repeat([]byte{byte(r.step), byte(id)}, 256+(id%3)*128)
			if err := view.Put(r.keys[id], next); err != nil {
				return durable, err
			}
			r.values[id] = next
		}
		if err := r.buf.CommitInflight(h); err != nil {
			return durable, err
		}
		if err := r.buf.FlushUpTo(r.step, r.disk); err != nil {
			return durable, err
		}
	}
	return durable, nil
}

func TestCommitmentTailCapacityFullSession(t *testing.T) {
	for _, mode := range []string{"hot", "cold_scan", "scan", "delayed_read", "prefetch_scan"} {
		t.Run(mode, func(t *testing.T) {
			var reads [3]uint64
			for i, tail := range []int{0, 64 << 10, 256 << 10} {
				r := newCommitmentTailReplay(t, tail, mode)
				var err error
				reads[i], err = r.run()
				if err != nil {
					t.Fatal(err)
				}
				checkCommitmentOwnershipBudget(t, r.buf.baseReadCache)
				stats := r.buf.baseReadCache.stats()
				var used int64
				for _, n := range stats.bytes {
					used += n
				}
				if used > stats.capacity {
					t.Fatalf("cache over budget: %d > %d", used, stats.capacity)
				}
			}
			t.Logf("durable parent reads: base=%d tail64KiB=%d tail256KiB=%d", reads[0], reads[1], reads[2])
			if mode == "hot" && reads[1] >= reads[0] {
				t.Fatal("larger tail did not retain hot working set")
			}
		})
	}
}

func BenchmarkCommitmentTailCapacityFullSession(b *testing.B) {
	for _, mode := range []string{"hot", "cold_scan", "scan", "delayed_read", "prefetch_scan"} {
		for _, tail := range []int{0, 64 << 10, 256 << 10} {
			b.Run(fmt.Sprintf("%s/tail_%d", mode, tail), func(b *testing.B) {
				r := newCommitmentTailReplay(b, tail, mode)
				var reads uint64
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					n, err := r.run()
					if err != nil {
						b.Fatal(err)
					}
					reads += n
				}
				b.ReportMetric(float64(reads)/float64(b.N), "durable_reads/op")
				b.ReportMetric(8, "blocks/op")
			})
		}
	}
}
