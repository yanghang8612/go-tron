package snapshots

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"testing"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// BenchmarkCommitmentBranchBaseBuildAB measures the cost rotation adds outside
// Pebble. The first case streams the complete mutable table into an immutable
// family; the second merges a 6.25% delta into an existing immutable base. The
// current format rewrites the complete base on every merge, so this benchmark
// must be interpreted together with rotation cadence, not as a one-time cost.
// Run with -benchtime=1x.
func BenchmarkCommitmentBranchBaseBuildAB(b *testing.B) {
	const (
		valueBytes = 530
		deltaEvery = 16
	)
	rows := commitmentABRows(b)

	b.Run("initial-full-pebble-scan", func(b *testing.B) {
		db, err := rawdb.NewPebbleDB(b.TempDir()+"/legacy", 16, 128)
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()
		seedCommitmentABRows(b, db, rawdb.LegacyCommitmentBranchKeyspace(), rows, valueBytes, nil)
		if err := db.Compact(nil, nil); err != nil {
			b.Fatal(err)
		}
		var output uint64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dir := b.TempDir()
			segment, accessor, btree, err := BuildCommitmentBranchSegmentFilesFromDB(
				db, dir, fmt.Sprintf("commitment/branches-%d.seg", i), 1, 1,
			)
			if err != nil {
				b.Fatal(err)
			}
			output += segment.Size + accessor.Size + btree.Size
		}
		b.StopTimer()
		b.SetBytes(int64(output) / int64(b.N))
		b.ReportMetric(float64(output)/float64(b.N)/(1<<20), "snapshot-MiB")
	})

	b.Run("merge-base-plus-6pct-delta", func(b *testing.B) {
		db, err := rawdb.NewPebbleDB(b.TempDir()+"/delta", 16, 128)
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()
		// Build the prior immutable base from a temporary complete table.
		seedCommitmentABRows(b, db, rawdb.LegacyCommitmentBranchKeyspace(), rows, valueBytes, nil)
		dir := b.TempDir()
		segment, accessor, btree, err := BuildCommitmentBranchSegmentFilesFromDB(
			db, dir, "commitment/base.seg", 1, 1,
		)
		if err != nil {
			b.Fatal(err)
		}
		if err := PublishManifest(dir, NewManifest(1, 1, []SegmentRef{segment, accessor, btree})); err != nil {
			b.Fatal(err)
		}
		delta, err := rawdb.NewCommitmentBranchDeltaKeyspace(1)
		if err != nil {
			b.Fatal(err)
		}
		seedCommitmentABRows(b, db, delta, rows, valueBytes, func(i int) bool { return i%deltaEvery == 0 })

		var output uint64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			refs, err := buildMergedCommitmentBranchLatest(
				db, dir,
				rawdb.CommitmentBranchBase{Generation: 1, SnapshotTxNum: 1},
				1, 2, fmt.Sprintf("commitment/merged-%d.seg", i),
			)
			if err != nil {
				b.Fatal(err)
			}
			for _, ref := range refs {
				output += ref.Size
			}
		}
		b.StopTimer()
		b.SetBytes(int64(output) / int64(b.N))
		b.ReportMetric(float64(output)/float64(b.N)/(1<<20), "snapshot-MiB")
	})
}

// commitmentABRows returns the benchmark cardinality. The default keeps the
// immutable segment larger than Pebble's minimum 16 MiB block cache without
// making an ordinary developer run excessively slow. Production-scale runs
// should override it, for example:
//
//	GTRON_COMMIT_AB_ROWS=1048576 go test ./core/state/snapshots -run '^$' \
//	  -bench '^BenchmarkCommitmentBranchPointLookupAB$' -benchtime=3s -count=3
func commitmentABRows(b *testing.B) int {
	b.Helper()
	const defaultRows = 1 << 18
	raw := os.Getenv("GTRON_COMMIT_AB_ROWS")
	if raw == "" {
		return defaultRows
	}
	rows, err := strconv.Atoi(raw)
	if err != nil || rows < 1024 {
		b.Fatalf("invalid GTRON_COMMIT_AB_ROWS=%q", raw)
	}
	return rows
}

// commitmentABPrefix encodes an ordinal as a valid 16-nibble commitment-trie
// path. Fixed-length paths make the generated key set deterministic while the
// multiplicative access permutation below still exercises random point reads.
func commitmentABPrefix(dst []byte, ordinal uint64) []byte {
	if cap(dst) < 16 {
		dst = make([]byte, 16)
	} else {
		dst = dst[:16]
	}
	for i := len(dst) - 1; i >= 0; i-- {
		dst[i] = byte(ordinal & 0x0f)
		ordinal >>= 4
	}
	return dst
}

func seedCommitmentABRows(b *testing.B, db ethdb.Batcher, keyspace rawdb.CommitmentBranchKeyspace, rows int, valueBytes int, only func(int) bool) uint64 {
	b.Helper()
	batch := db.NewBatch()
	defer batch.Close()
	value := make([]byte, valueBytes)
	var logical uint64
	var prefix [16]byte
	for i := 0; i < rows; i++ {
		if only != nil && !only(i) {
			continue
		}
		binary.BigEndian.PutUint64(value[:8], uint64(i))
		x := uint64(i)*0x9e3779b97f4a7c15 + 1
		for j := 8; j < len(value); j++ {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			value[j] = byte(x)
		}
		key := commitmentABPrefix(prefix[:0], uint64(i))
		if err := keyspace.Write(batch, key, value); err != nil {
			b.Fatal(err)
		}
		logical += uint64(len(key) + valueBytes)
		if batch.ValueSize() >= 8<<20 {
			if err := batch.Write(); err != nil {
				b.Fatal(err)
			}
			batch.Reset()
		}
	}
	if batch.ValueSize() != 0 {
		if err := batch.Write(); err != nil {
			b.Fatal(err)
		}
	}
	return logical
}

// BenchmarkCommitmentBranchPointLookupAB compares the current complete mutable
// Pebble table with the proposed immutable-base + generation-delta read path.
// It intentionally reports delta hits and base fallbacks separately: a base
// fallback performs a negative Pebble probe plus a sparse-B-tree/segment read,
// so rotation is expected to win on bounded LSM size/write amplification, not
// necessarily on every isolated point lookup.
func BenchmarkCommitmentBranchPointLookupAB(b *testing.B) {
	const (
		valueBytes = 530 // observed production branch-row mean is about 500 B
		deltaEvery = 16  // 6.25% of base rows changed since the rotation
	)
	rows := commitmentABRows(b)

	legacy, err := rawdb.NewPebbleDB(b.TempDir()+"/legacy", 16, 128)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = legacy.Close() })
	seedCommitmentABRows(b, legacy, rawdb.LegacyCommitmentBranchKeyspace(), rows, valueBytes, nil)
	if err := legacy.Compact(nil, nil); err != nil {
		b.Fatal(err)
	}

	snapshotDir := b.TempDir()
	segment, accessor, btree, err := BuildCommitmentBranchSegmentFilesFromDB(
		legacy, snapshotDir, "commitment/branches-ab.seg", 1, 1,
	)
	if err != nil {
		b.Fatal(err)
	}
	snapshotMiB := float64(segment.Size+accessor.Size+btree.Size) / (1 << 20)
	if err := PublishManifest(snapshotDir, NewManifest(1, 1, []SegmentRef{segment, accessor, btree})); err != nil {
		b.Fatal(err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		b.Fatal(err)
	}
	probeView, ok, err := mgr.OpenCommitmentBranchSnapshot(1)
	if err != nil || !ok || probeView == nil {
		b.Fatalf("probe commitment base: view=%v ok=%v err=%v", probeView, ok, err)
	}
	probe := probeView.(*CommitmentBranchPointView)
	residentIndexMiB := float64(uintptr(len(probe.index))*unsafe.Sizeof(latestBinaryBTreeEntry{})+uintptr(cap(probe.keyArena))) / (1 << 20)
	retainedScratchMiB := float64(cap(probe.scratchPool)*probe.maxBlockBytes) / (1 << 20)
	maxBlockKiB := float64(probe.maxBlockBytes) / (1 << 10)
	if err := probeView.Close(); err != nil {
		b.Fatal(err)
	}
	b.Run("open-resident-index", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(float64(btree.Size)/(1<<20), "btree-file-MiB")
		b.ReportMetric(residentIndexMiB, "resident-index-MiB")
		b.ReportMetric(retainedScratchMiB, "retained-scratch-max-MiB")
		b.ReportMetric(maxBlockKiB, "max-block-KiB")
		for i := 0; i < b.N; i++ {
			opened, ok, err := mgr.OpenCommitmentBranchSnapshot(1)
			if err != nil || !ok || opened == nil {
				b.Fatalf("open commitment base: view=%v ok=%v err=%v", opened, ok, err)
			}
			if err := opened.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	view, ok, err := mgr.OpenCommitmentBranchSnapshot(1)
	if err != nil || !ok || view == nil {
		b.Fatalf("open commitment base: view=%v ok=%v err=%v", view, ok, err)
	}
	b.Cleanup(func() { _ = view.Close() })

	deltaDB, err := rawdb.NewPebbleDB(b.TempDir()+"/delta", 16, 128)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = deltaDB.Close() })
	delta, err := rawdb.NewCommitmentBranchDeltaKeyspace(1)
	if err != nil {
		b.Fatal(err)
	}
	seedCommitmentABRows(b, deltaDB, delta, rows, valueBytes, func(i int) bool { return i%deltaEvery == 0 })
	if err := deltaDB.Compact(nil, nil); err != nil {
		b.Fatal(err)
	}

	// Multiplication by an odd constant is a permutation modulo the next power
	// of two. For non-powers of two, the final modulo remains deterministic.
	indexAt := func(i int) int {
		return int((uint64(i) * 0x9e3779b97f4a7c15) % uint64(rows))
	}
	var prefix [16]byte
	consume := func(value []byte) {
		if len(value) != valueBytes {
			b.Fatalf("value length %d, want %d", len(value), valueBytes)
		}
	}

	b.Run("legacy-full-table", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(snapshotMiB, "base-snapshot-MiB")
		for i := 0; i < b.N; i++ {
			key := commitmentABPrefix(prefix[:0], uint64(indexAt(i)))
			value, found, err := rawdb.LegacyCommitmentBranchKeyspace().ReadNoCopy(legacy, key)
			if err != nil || !found {
				b.Fatalf("legacy read found=%v err=%v", found, err)
			}
			consume(value)
		}
	})

	b.Run("base-delta/delta-hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(snapshotMiB, "base-snapshot-MiB")
		for i := 0; i < b.N; i++ {
			ordinal := indexAt(i) / deltaEvery * deltaEvery
			key := commitmentABPrefix(prefix[:0], uint64(ordinal))
			value, found, err := delta.ReadNoCopy(deltaDB, key)
			if err != nil || !found {
				b.Fatalf("delta read found=%v err=%v", found, err)
			}
			consume(value)
		}
	})

	b.Run("base-delta/base-fallback", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(snapshotMiB, "base-snapshot-MiB")
		for i := 0; i < b.N; i++ {
			ordinal := indexAt(i)
			if ordinal%deltaEvery == 0 {
				ordinal = (ordinal + 1) % rows
			}
			key := commitmentABPrefix(prefix[:0], uint64(ordinal))
			if _, found, err := delta.ReadNoCopy(deltaDB, key); err != nil {
				b.Fatal(err)
			} else if found {
				b.Fatal("base fallback unexpectedly hit delta")
			}
			value, found, err := view.Get(key)
			if err != nil || !found {
				b.Fatalf("base read found=%v err=%v", found, err)
			}
			consume(value)
		}
	})

	b.Run("base-delta/mixed-20pct-delta", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(snapshotMiB, "base-snapshot-MiB")
		for i := 0; i < b.N; i++ {
			ordinal := indexAt(i)
			if i%5 == 0 {
				ordinal = ordinal / deltaEvery * deltaEvery
			} else if ordinal%deltaEvery == 0 {
				ordinal = (ordinal + 1) % rows
			}
			key := commitmentABPrefix(prefix[:0], uint64(ordinal))
			value, found, err := delta.ReadNoCopy(deltaDB, key)
			if err != nil {
				b.Fatal(err)
			}
			if !found {
				value, found, err = view.Get(key)
				if err != nil || !found {
					b.Fatalf("mixed base read found=%v err=%v", found, err)
				}
			}
			consume(value)
		}
	})
}
