package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// Retain the original collector call as an independent oracle for the exact
// dictionary bytes, digest and key IDs, and as the benchmark baseline.
func collectV6KeyWithoutDedup(build *stateDomainChangeV6Build, key []byte) error {
	return build.keys.PutEncoded(len(key), 0, func(dst, _ []byte) { copy(dst, key) })
}

func newV6KeyDedupTestBuild(t testing.TB, dir string, bufferLimit int) *stateDomainChangeV6Build {
	t.Helper()
	build, err := newStateDomainChangeV6Build(etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: bufferLimit}, dir, "history/test.seg")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(build.Close)
	return build
}

func TestV6KeyDedupExactDictionaryAndBorrowedInput(t *testing.T) {
	baseline := newV6KeyDedupTestBuild(t, t.TempDir(), 128)
	dedup := newV6KeyDedupTestBuild(t, t.TempDir(), 128)
	keys := [][]byte{[]byte("account/shared"), []byte("account/shared\x00"), []byte("account/shared\x00\xff"), []byte("delegation/shared"), []byte{0, 1, 0}}
	for i := 0; i < 400; i++ {
		key := keys[(i*3)%len(keys)]
		borrowed := append([]byte(nil), key...)
		if err := collectV6KeyWithoutDedup(baseline, borrowed); err != nil {
			t.Fatal(err)
		}
		if err := dedup.CollectLogicalKey(borrowed); err != nil {
			t.Fatal(err)
		}
		clear(borrowed)
	}
	for _, build := range []*stateDomainChangeV6Build{baseline, dedup} {
		if err := build.FinishDictionary(); err != nil {
			t.Fatal(err)
		}
	}
	if baseline.keyStats.Collected != 400 || dedup.keyStats.Collected != uint64(len(keys)) {
		t.Fatalf("collected baseline=%+v dedup=%+v", baseline.keyStats, dedup.keyStats)
	}
	if dedup.keyStats.SpilledRuns >= baseline.keyStats.SpilledRuns {
		t.Fatalf("duplicate key runs did not fall: baseline=%+v dedup=%+v", baseline.keyStats, dedup.keyStats)
	}
	before, err := os.ReadFile(baseline.dictName)
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(dedup.dictName)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || baseline.dictionaryDigest != dedup.dictionaryDigest || baseline.keyCount != dedup.keyCount {
		t.Fatal("prefilter changed published dictionary identity")
	}
	for _, key := range keys {
		beforeID, err := baseline.KeyID(key)
		if err != nil {
			t.Fatal(err)
		}
		afterID, err := dedup.KeyID(key)
		if err != nil || beforeID != afterID {
			t.Fatalf("key %x IDs %d/%d err=%v", key, beforeID, afterID, err)
		}
	}
	if dedup.keyDedup != nil || dedup.keyDedupBytes != 0 {
		t.Fatal("prefilter retained into posting/record construction")
	}
}

func TestV6KeyDedupBoundsAndETLAcrossResets(t *testing.T) {
	for _, tc := range []struct {
		name          string
		keyLen, count int
	}{
		{name: "entry_limit", keyLen: 16, count: stateDomainChangeV6KeyDedupMaxEntries + 1},
		{name: "byte_limit", keyLen: math.MaxUint16, count: stateDomainChangeV6KeyDedupMaxBytes/math.MaxUint16 + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			build := newV6KeyDedupTestBuild(t, t.TempDir(), 1<<20)
			key := make([]byte, tc.keyLen)
			first := append([]byte(nil), key...)
			resets := 0
			for i := 0; i < tc.count; i++ {
				binary.BigEndian.PutUint64(key[:8], uint64(i))
				previous := len(build.keyDedup)
				if err := build.CollectLogicalKey(key); err != nil {
					t.Fatal(err)
				}
				if i%8 == 0 {
					// Genuine duplicates retain the cache past its initial
					// trial so both independent resource limits are exercised.
					if err := build.CollectLogicalKey(key); err != nil {
						t.Fatal(err)
					}
				}
				if len(build.keyDedup) < previous {
					resets++
				}
				if len(build.keyDedup) > stateDomainChangeV6KeyDedupMaxEntries || build.keyDedupBytes > stateDomainChangeV6KeyDedupMaxBytes {
					t.Fatal("prefilter exceeded entry/owned-byte bounds")
				}
			}
			if resets != 1 {
				t.Fatalf("cache resets=%d, want one", resets)
			}
			if err := build.CollectLogicalKey(first); err != nil {
				t.Fatal(err)
			}
			if err := build.FinishDictionary(); err != nil {
				t.Fatal(err)
			}
			if build.keyCount != uint32(tc.count) || build.keyStats.Collected != uint64(tc.count+1) || build.keyStats.Applied != uint64(tc.count) {
				t.Fatalf("ETL failed to deduplicate across reset: keys=%d stats=%+v", build.keyCount, build.keyStats)
			}
		})
	}
}

func TestV6KeyDedupNoHitTrialFallsBackToETL(t *testing.T) {
	build := newV6KeyDedupTestBuild(t, t.TempDir(), 64<<10)
	const count = stateDomainChangeV6KeyDedupTrialKeys * 3
	var key [8]byte
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := build.CollectLogicalKey(key[:]); err != nil {
			t.Fatal(err)
		}
	}
	if !build.keyDedupDisabled || build.keyDedup != nil || build.keyDedupBytes != 0 || build.keyDedupTrial != stateDomainChangeV6KeyDedupTrialKeys {
		t.Fatal("all-unique input retained cache work past the bounded trial")
	}
	clear(key[:])
	if err := build.CollectLogicalKey(key[:]); err != nil {
		t.Fatal(err)
	}
	if err := build.FinishDictionary(); err != nil {
		t.Fatal(err)
	}
	if build.keyCount != count || build.keyStats.Collected != count+1 || build.keyStats.Applied != count {
		t.Fatalf("ETL fallback lost final deduplication: keys=%d stats=%+v", build.keyCount, build.keyStats)
	}
}

func TestV6KeyDedupLowHitTrialFallsBackToETL(t *testing.T) {
	build := newV6KeyDedupTestBuild(t, t.TempDir(), 64<<10)
	const count = stateDomainChangeV6KeyDedupTrialKeys * 3
	var key [8]byte
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := build.CollectLogicalKey(key[:]); err != nil {
			t.Fatal(err)
		}
		if i%32 == 0 {
			if err := build.CollectLogicalKey(key[:]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !build.keyDedupDisabled || build.keyDedup != nil || build.keyDedupBytes != 0 || build.keyDedupTrial != stateDomainChangeV6KeyDedupTrialKeys || build.keyDedupTrialHits == 0 {
		t.Fatal("rare hits retained cache work past the bounded trial")
	}
	before := build.keys.Stats().Collected
	if err := build.CollectLogicalKey(key[:]); err != nil {
		t.Fatal(err)
	}
	if build.keys.Stats().Collected != before+1 {
		t.Fatal("disabled prefilter did not send duplicate to ETL")
	}
	if err := build.FinishDictionary(); err != nil {
		t.Fatal(err)
	}
	if build.keyCount != count || build.keyStats.Applied != count {
		t.Fatalf("ETL fallback lost final deduplication: keys=%d stats=%+v", build.keyCount, build.keyStats)
	}
}

func TestV6KeyDedupFailureCancellationAndClose(t *testing.T) {
	build := newV6KeyDedupTestBuild(t, t.TempDir(), 128)
	key := []byte("unchanged-key")
	if err := build.CollectLogicalKey(key); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := build.FinishDictionaryContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}
	if err := build.FinishDictionary(); err != nil {
		t.Fatal(err)
	}
	if err := build.CollectLogicalKey(key); err == nil {
		t.Fatal("collection after dictionary finalization succeeded")
	}
	closed := newV6KeyDedupTestBuild(t, t.TempDir(), 128)
	if err := closed.CollectLogicalKey(key); err != nil {
		t.Fatal(err)
	}
	closed.Close()
	if err := closed.CollectLogicalKey(key); err == nil {
		t.Fatal("cache hid closed collector error")
	}
	failed := newV6KeyDedupTestBuild(t, t.TempDir(), 1)
	if err := os.RemoveAll(failed.keys.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := failed.CollectLogicalKey(key); err == nil {
		t.Fatal("spill unexpectedly succeeded after scratch removal")
	}
	if len(failed.keyDedup) != 0 || failed.keyDedupBytes != 0 {
		t.Fatal("failed ETL insertion populated prefilter")
	}
}

func BenchmarkV6KeyCollectionDedup(b *testing.B) {
	const rows = 1 << 18
	for _, distribution := range []struct {
		name   string
		unique int
	}{
		{name: "repeated-1024", unique: 1024},
		{name: "all-unique", unique: rows},
	} {
		keys := make([][]byte, distribution.unique)
		for i := range keys {
			keys[i] = make([]byte, 64)
			copy(keys[i], "account-or-delegation-key/")
			binary.BigEndian.PutUint64(keys[i][32:40], uint64(i))
		}
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/dedup=%t", distribution.name, enabled), func(b *testing.B) {
				root := b.TempDir()
				b.ReportAllocs()
				b.ResetTimer()
				var stats etl.Stats
				for i := 0; i < b.N; i++ {
					build, err := newStateDomainChangeV6Build(etl.Options{TempDir: root, BufferLimit: 1 << 20}, root, "history/test.seg")
					if err != nil {
						b.Fatal(err)
					}
					for row := 0; row < rows; row++ {
						key := keys[row%len(keys)]
						if enabled {
							err = build.CollectLogicalKey(key)
						} else {
							err = collectV6KeyWithoutDedup(build, key)
						}
						if err != nil {
							build.Close()
							b.Fatal(err)
						}
					}
					if err := build.FinishDictionary(); err != nil {
						build.Close()
						b.Fatal(err)
					}
					stats = build.keyStats
					if build.keyCount != uint32(distribution.unique) {
						b.Fatal("dictionary lost keys")
					}
					build.Close()
				}
				b.ReportMetric(float64(stats.Collected), "key-ETL-rows/op")
				b.ReportMetric(float64(stats.InputBytes), "key-ETL-input-B/op")
				b.ReportMetric(float64(stats.SpilledRuns), "key-ETL-runs/op")
			})
		}
	}
}
