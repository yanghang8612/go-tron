package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type pendingPrefixTraceRow struct {
	owner      tcommon.Address
	generation uint64
	domain     kvdomains.KVDomain
	key, value []byte
}

func pendingPrefixTrace(n int) []pendingPrefixTraceRow {
	rows := make([]pendingPrefixTraceRow, n)
	domains := []kvdomains.KVDomain{kvdomains.AccountVotesAux, kvdomains.AccountAssetV2, kvdomains.AccountPermissionAux, kvdomains.SystemReward}
	for i := range rows {
		var a tcommon.Address
		a[0] = 0x41
		binary.BigEndian.PutUint32(a[16:], uint32(i/16+1))
		key := []byte{byte(i % 4), 0, 0, 0}
		value := bytes.Repeat([]byte{byte(i%251 + 1)}, 32)
		rows[i] = pendingPrefixTraceRow{a, uint64(i % 2), domains[(i/4)%4], key, value}
	}
	return rows
}

// This is an identical deterministic batch lifecycle in both compiled arms,
// not a hot-loop benchmark of an already-built index. Construction, all writes,
// first build, live inserts/overwrites/tombstones, partial publication/prune,
// post-prune reads/rebuild, final publication and reset are timed.
func pendingPrefixLifecycle(tb testing.TB, disk ethdb.KeyValueStore, rows []pendingPrefixTraceRow, queries int) uint64 {
	tb.Helper()
	buf := blockbuffer.New(disk)
	buf.SetMaxInflight(8)
	buf.SetBaseReadCacheSize(0)
	w := newAccountKVLatestDomainBatch(buf, zeroGeneration, nil, nil)
	put := func(r pendingPrefixTraceRow) {
		if err := w.put(r.owner, r.generation, r.domain, r.key, rawdb.EncodeStateKVLatestValue(r.value)); err != nil {
			tb.Fatal(err)
		}
	}
	half := len(rows) / 2
	for phase := 0; phase < 2; phase++ {
		n := uint64(phase + 1)
		buf.BeginBlock(pruneBlockHash(int(n)), n)
		w.commitBlock = n
		for _, r := range rows[phase*half : (phase+1)*half] {
			put(r)
		}
	}
	var sum uint64
	read := func(count int) {
		for q := 0; q < count; q++ {
			r := rows[(q*37)%len(rows)]
			if err := w.iterateLatestPrefix(r.owner, r.generation, r.domain, nil, func(k, v []byte) (bool, error) {
				for _, b := range k {
					sum = sum*33 + uint64(b)
				}
				for _, b := range v {
					sum = sum*33 + uint64(b)
				}
				return true, nil
			}); err != nil {
				tb.Fatal(err)
			}
		}
	}
	read((queries + 1) / 2)
	// Insert into a new bucket and an existing bucket after the lazy index exists;
	// update/delete/reinsert existing keys without duplicating enumeration members.
	for i := 0; i < 32 && i < len(rows); i++ {
		r := rows[i]
		put(r)
		if err := w.delete(r.owner, r.generation, r.domain, r.key); err != nil {
			tb.Fatal(err)
		}
		put(r)
		r.key = append([]byte{0xff}, r.key...)
		put(r)
	}
	read(queries / 2)
	buf.CommitBlock()
	if err := w.flushUpTo(1); err != nil {
		tb.Fatal(err)
	}
	read(queries)
	buf.CommitBlock()
	if err := w.flushUpTo(2); err != nil {
		tb.Fatal(err)
	}
	if err := buf.FlushUpTo(2, disk); err != nil {
		tb.Fatal(err)
	}
	read(queries)
	w.reset()
	buf.Discard()
	return sum
}

func pendingPrefixDenseTrace(n int) []pendingPrefixTraceRow {
	rows := pendingPrefixTrace(n)
	for i := range rows {
		rows[i].owner = rows[0].owner
		rows[i].generation = 0
		rows[i].domain = kvdomains.AccountAssetV2
		rows[i].key = make([]byte, 4)
		binary.BigEndian.PutUint32(rows[i].key, uint32(i))
	}
	return rows
}

var pendingPrefixShapes = []struct {
	name          string
	rows, queries int
	dense         bool
}{
	{"many_groups", 4096, 128, false}, {"small_overlay", 64, 16, false},
	{"write_only", 4096, 0, false}, {"single_query", 4096, 1, false},
	{"two_queries", 4096, 2, false}, {"five_queries", 4096, 5, false},
	{"dense_group", 4096, 16, true},
}
var pendingPrefixLifecycleSink uint64

func BenchmarkPendingPrefixLifecycle(b *testing.B) {
	for _, shape := range pendingPrefixShapes {
		b.Run(shape.name, func(b *testing.B) {
			disk, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := disk.Close(); err != nil {
					b.Error(err)
				}
			})
			rows := pendingPrefixTrace(shape.rows)
			if shape.dense {
				rows = pendingPrefixDenseTrace(shape.rows)
			}
			b.ReportAllocs()
			b.ResetTimer()
			cpuStart := frozenDemandCPU()
			for i := 0; i < b.N; i++ {
				pendingPrefixLifecycleSink = pendingPrefixLifecycle(b, disk, rows, shape.queries)
			}
			b.ReportMetric(float64(frozenDemandCPU()-cpuStart)/float64(b.N), "cpu-ns/op")
		})
	}
}

func TestPendingPrefixLifecycleTrace(t *testing.T) {
	for _, shape := range pendingPrefixShapes {
		disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
		if err != nil {
			t.Fatal(err)
		}
		rows := pendingPrefixTrace(shape.rows)
		if shape.dense {
			rows = pendingPrefixDenseTrace(shape.rows)
		}
		first := pendingPrefixLifecycle(t, disk, rows, shape.queries)
		second := pendingPrefixLifecycle(t, disk, rows, shape.queries)
		hash := sha256.New()
		it := disk.NewIterator(nil, nil)
		var length [8]byte
		for it.Next() {
			for _, v := range [][]byte{it.Key(), it.Value()} {
				binary.BigEndian.PutUint64(length[:], uint64(len(v)))
				hash.Write(length[:])
				hash.Write(v)
			}
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		it.Release()
		t.Logf("TRACE %s %016x %016x %x", shape.name, first, second, hash.Sum(nil))
		if err := disk.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPendingPrefixTraceDistribution(t *testing.T) {
	for _, n := range []int{64, 4096} {
		rows := pendingPrefixTrace(n)
		groups := map[string]int{}
		for _, r := range rows {
			groups[fmt.Sprintf("%x/%d/%d", r.owner.AccountID(), r.generation, r.domain)]++
		}
		min, max := n, 0
		for _, v := range groups {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		t.Logf("fixed synthetic trace: pending=%d owner/generation/domain groups=%d matching rows/group=%d..%d; not a mainnet replay", n, len(groups), min, max)
	}
}
