package pebbledb

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	commitmentABLegacyPrefix = "state-commitment-branch-v1-"
	commitmentABDeltaPrefix  = "state-commitment-branch-delta-v1-"
)

type commitmentABConfig struct {
	rows         int
	hotRows      int
	blocks       int
	updatesBlock int
	valueBytes   int
	tuneScale    int
}

func commitmentABEnvInt(b *testing.B, name string, fallback, minimum int) int {
	b.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum {
		b.Fatalf("invalid %s=%q", name, raw)
	}
	return value
}

func loadCommitmentABConfig(b *testing.B) commitmentABConfig {
	b.Helper()
	rows := commitmentABEnvInt(b, "GTRON_COMMIT_AB_ROWS", 1<<18, 1024)
	hotRows := commitmentABEnvInt(b, "GTRON_COMMIT_AB_HOT_ROWS", rows/16, 64)
	if hotRows > rows {
		b.Fatalf("GTRON_COMMIT_AB_HOT_ROWS=%d exceeds rows=%d", hotRows, rows)
	}
	return commitmentABConfig{
		rows:         rows,
		hotRows:      hotRows,
		blocks:       commitmentABEnvInt(b, "GTRON_COMMIT_AB_BLOCKS", 512, 1),
		updatesBlock: commitmentABEnvInt(b, "GTRON_COMMIT_AB_UPDATES", 256, 1),
		valueBytes:   commitmentABEnvInt(b, "GTRON_COMMIT_AB_VALUE_BYTES", 530, 32),
		// Scale the production 256 MiB/8 MiB/1 GiB geometry down together so
		// a laptop-sized run reaches the same LSM pressure regimes. Set scale=1
		// with a proportionally larger row/workload count for exact production
		// tunables.
		tuneScale: commitmentABEnvInt(b, "GTRON_COMMIT_AB_SCALE", 32, 1),
	}
}

func scaledCommitmentABOptions(scale int) Options {
	tune := DefaultOptions()
	if scale <= 1 {
		return tune
	}
	tune.MemTableSizeBytes /= uint64(scale)
	tune.TargetFileSizeBytes /= int64(scale)
	tune.LBaseMaxBytes /= int64(scale)
	// Preserve useful minimums if an intentionally tiny developer scale is
	// selected. The ratios remain documented in the benchmark output/command.
	if tune.MemTableSizeBytes < 1<<20 {
		tune.MemTableSizeBytes = 1 << 20
	}
	if tune.TargetFileSizeBytes < 64<<10 {
		tune.TargetFileSizeBytes = 64 << 10
	}
	if tune.LBaseMaxBytes < 4<<20 {
		tune.LBaseMaxBytes = 4 << 20
	}
	return tune
}

func commitmentABLogicalPrefix(dst []byte, ordinal uint64) []byte {
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

func commitmentABPhysicalKey(dst []byte, schema []byte, ordinal uint64) []byte {
	need := len(schema) + 16
	if cap(dst) < need {
		dst = make([]byte, need)
	} else {
		dst = dst[:need]
	}
	copy(dst, schema)
	commitmentABLogicalPrefix(dst[len(schema):len(schema)], ordinal)
	return dst
}

func commitmentABUpperBound(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0xff}
}

func commitmentABValue(dst []byte, ordinal, version uint64) []byte {
	binary.BigEndian.PutUint64(dst[:8], ordinal)
	binary.BigEndian.PutUint64(dst[8:16], version)
	// BranchData is hash-dominated. A small deterministic xorshift stream keeps
	// the synthetic value incompressible if compression is enabled in a future
	// production tuning change.
	x := ordinal*0x9e3779b97f4a7c15 ^ version
	for i := 16; i < len(dst); i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		dst[i] = byte(x)
	}
	return dst
}

func seedCommitmentABBase(b *testing.B, db *Database, schema []byte, cfg commitmentABConfig) {
	b.Helper()
	batch := db.db.NewBatch()
	defer batch.Close()
	key := make([]byte, len(schema)+16)
	value := make([]byte, cfg.valueBytes)
	for i := 0; i < cfg.rows; i++ {
		commitmentABPhysicalKey(key[:0], schema, uint64(i))
		commitmentABValue(value, uint64(i), 0)
		if err := batch.Set(key, value, nil); err != nil {
			b.Fatal(err)
		}
		if batch.Len() >= 16_384 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			batch.Reset()
		}
	}
	if batch.Len() != 0 {
		if err := batch.Commit(pebble.NoSync); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.db.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := db.db.Compact(schema, commitmentABUpperBound(schema), true); err != nil {
		b.Fatal(err)
	}
}

type commitmentABMetrics struct {
	walWritten      uint64
	flushWritten    uint64
	compactRead     uint64
	compactWritten  uint64
	diskBytes       uint64
	readAmp         int
	estimatedDebt   uint64
	compactionCount int64
}

func readCommitmentABMetrics(db *Database) commitmentABMetrics {
	m := db.db.Metrics()
	out := commitmentABMetrics{
		walWritten:      m.WAL.BytesWritten,
		diskBytes:       m.DiskSpaceUsage(),
		readAmp:         m.ReadAmp(),
		estimatedDebt:   m.Compact.EstimatedDebt,
		compactionCount: m.Compact.Count,
	}
	for i := range m.Levels {
		out.flushWritten += m.Levels[i].BytesFlushed
		out.compactRead += m.Levels[i].BytesRead
		out.compactWritten += m.Levels[i].BytesCompacted
	}
	return out
}

func commitmentABDelta(after, before commitmentABMetrics) commitmentABMetrics {
	return commitmentABMetrics{
		walWritten:      after.walWritten - before.walWritten,
		flushWritten:    after.flushWritten - before.flushWritten,
		compactRead:     after.compactRead - before.compactRead,
		compactWritten:  after.compactWritten - before.compactWritten,
		diskBytes:       after.diskBytes,
		readAmp:         after.readAmp,
		estimatedDebt:   after.estimatedDebt,
		compactionCount: after.compactionCount - before.compactionCount,
	}
}

func waitCommitmentABCompactions(db *Database) {
	deadline := time.Now().Add(30 * time.Second)
	var idleSince time.Time
	var idleDebt uint64
	for time.Now().Before(deadline) {
		compact := db.db.Metrics().Compact
		if compact.NumInProgress == 0 {
			if idleSince.IsZero() || compact.EstimatedDebt != idleDebt {
				idleSince = time.Now()
				idleDebt = compact.EstimatedDebt
			} else if time.Since(idleSince) >= 100*time.Millisecond {
				return
			}
		} else {
			idleSince = time.Time{}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runCommitmentABWrites(b *testing.B, name string, cfg commitmentABConfig, legacy bool) {
	b.Helper()
	tune := scaledCommitmentABOptions(cfg.tuneScale)
	db, err := New(b.TempDir(), 16, 128, fmt.Sprintf("commitment-ab-%s-", name), false, tune)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	legacySchema := []byte(commitmentABLegacyPrefix)
	deltaSchema := make([]byte, len(commitmentABDeltaPrefix)+8)
	copy(deltaSchema, commitmentABDeltaPrefix)
	binary.BigEndian.PutUint64(deltaSchema[len(commitmentABDeltaPrefix):], 1)
	schema := deltaSchema
	if legacy {
		schema = legacySchema
		seedCommitmentABBase(b, db, schema, cfg)
		waitCommitmentABCompactions(db)
	}
	before := readCommitmentABMetrics(db)

	key := make([]byte, len(schema)+16)
	value := make([]byte, cfg.valueBytes)
	logicalBytes := uint64(0)
	totalBlocks := b.N * cfg.blocks
	started := time.Now()
	b.ResetTimer()
	for round := 0; round < b.N; round++ {
		for block := 0; block < cfg.blocks; block++ {
			batch := db.db.NewBatch()
			version := uint64(round*cfg.blocks + block + 1)
			for update := 0; update < cfg.updatesBlock; update++ {
				hotOrdinal := (block*cfg.updatesBlock + update) % cfg.hotRows
				// Map the bounded hot set across the complete legacy key range;
				// this is the adversarial overlap shape that makes a complete
				// mutable table repeatedly compact unchanged cold rows.
				ordinal := (uint64(hotOrdinal) * 0x9e3779b97f4a7c15) % uint64(cfg.rows)
				commitmentABPhysicalKey(key[:0], schema, ordinal)
				commitmentABValue(value, ordinal, version)
				if err := batch.Set(key, value, nil); err != nil {
					b.Fatal(err)
				}
				logicalBytes += uint64(len(key) + len(value))
			}
			if err := batch.Commit(pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			if err := batch.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	batchElapsed := time.Since(started)

	drainStarted := time.Now()
	if err := db.db.Flush(); err != nil {
		b.Fatal(err)
	}
	waitCommitmentABCompactions(db)
	drainElapsed := time.Since(drainStarted)
	afterFlush := readCommitmentABMetrics(db)

	compactStarted := time.Now()
	if err := db.db.Compact(schema, commitmentABUpperBound(schema), true); err != nil {
		b.Fatal(err)
	}
	waitCommitmentABCompactions(db)
	compactElapsed := time.Since(compactStarted)
	afterCompact := readCommitmentABMetrics(db)

	flushDelta := commitmentABDelta(afterFlush, before)
	stableDelta := commitmentABDelta(afterCompact, before)
	physicalFlush := flushDelta.walWritten + flushDelta.flushWritten + flushDelta.compactWritten
	physicalStable := stableDelta.walWritten + stableDelta.flushWritten + stableDelta.compactWritten
	b.ReportMetric(float64(batchElapsed.Nanoseconds())/float64(totalBlocks), "ns/block")
	b.ReportMetric(float64(drainElapsed.Microseconds())/1000, "drain-ms")
	b.ReportMetric(float64(compactElapsed.Microseconds())/1000, "manual-compact-ms")
	b.ReportMetric(float64(logicalBytes)/(1<<20), "logical-MiB")
	b.ReportMetric(float64(flushDelta.walWritten)/(1<<20), "wal-MiB")
	b.ReportMetric(float64(flushDelta.flushWritten)/(1<<20), "flush-MiB")
	b.ReportMetric(float64(flushDelta.compactRead)/(1<<20), "auto-compact-read-MiB")
	b.ReportMetric(float64(flushDelta.compactWritten)/(1<<20), "auto-compact-write-MiB")
	b.ReportMetric(float64(physicalFlush)/float64(logicalBytes), "auto-writeamp-x")
	b.ReportMetric(float64(stableDelta.compactRead-flushDelta.compactRead)/(1<<20), "manual-compact-read-MiB")
	b.ReportMetric(float64(stableDelta.compactWritten-flushDelta.compactWritten)/(1<<20), "manual-compact-write-MiB")
	b.ReportMetric(float64(physicalStable)/float64(logicalBytes), "stable-writeamp-x")
	b.ReportMetric(float64(stableDelta.diskBytes)/(1<<20), "final-disk-MiB")
	b.ReportMetric(float64(stableDelta.readAmp), "final-readamp")
	b.ReportMetric(float64(stableDelta.estimatedDebt)/(1<<20), "final-debt-MiB")
	b.ReportMetric(float64(stableDelta.compactionCount), "compactions")
}

// BenchmarkCommitmentRotationPebbleAB measures the write-side mechanism behind
// periodic rotation. Both cases perform byte-identical logical mutations over
// the same scattered hot rows. legacy starts with the complete branch table in
// Pebble; base-delta keeps that immutable state out of the LSM and writes only
// the active generation. Run with -benchtime=1x: b.N represents repetitions of
// the whole configured trace, not individual blocks.
//
// Scaled, quick, reproducible run:
//
//	go test ./core/rawdb/pebbledb -run '^$' \
//	  -bench '^BenchmarkCommitmentRotationPebbleAB$' -benchtime=1x -count=3
//
// Exact production tunables (increase rows/workload proportionally):
//
//	GTRON_COMMIT_AB_SCALE=1 GTRON_COMMIT_AB_ROWS=1048576 \
//	GTRON_COMMIT_AB_HOT_ROWS=65536 GTRON_COMMIT_AB_BLOCKS=4096 \
//	GTRON_COMMIT_AB_UPDATES=256 go test ./core/rawdb/pebbledb -run '^$' \
//	  -bench '^BenchmarkCommitmentRotationPebbleAB$' -benchtime=1x -count=1
func BenchmarkCommitmentRotationPebbleAB(b *testing.B) {
	cfg := loadCommitmentABConfig(b)
	b.Run("legacy-complete-mutable", func(b *testing.B) {
		runCommitmentABWrites(b, "legacy", cfg, true)
	})
	b.Run("immutable-base-active-delta", func(b *testing.B) {
		runCommitmentABWrites(b, "delta", cfg, false)
	})
}
