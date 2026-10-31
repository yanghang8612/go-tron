package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Fixed-work opt-in benchmark for the complete production Validate or Publish
// lifecycle. Run one operation in each fresh process and compare output hashes
// across source revisions; automatic benchtime would change cumulative work.
func TestEventLogIndexCompanionFixedLifecycle(t *testing.T) {
	mode := os.Getenv("GTRON_EVENT_COMPANION_BENCH_MODE")
	if mode == "" {
		t.Skip("opt-in fixed-work manifest experiment")
	}
	arm := os.Getenv("GTRON_EVENT_COMPANION_BENCH_ARM")
	if arm != "baseline" && arm != "index" {
		t.Fatal("arm must identify baseline or index binary")
	}
	if mode != "validate" && mode != "publish" {
		t.Fatal("mode must be validate or publish")
	}
	count := 44580
	if v := os.Getenv("GTRON_EVENT_COMPANION_BENCH_EVENTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatal("invalid event count")
		}
		count = n
	}
	old := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(old)
	checksum := "sha256:" + strings.Repeat("a", 64)
	refs := make([]SegmentRef, 0, count*3)
	for i := 0; i < count; i++ {
		from, to := uint64(i*1000+1), uint64((i+1)*1000)
		refs = append(refs, SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: from, ToTxNum: to, Path: fmt.Sprintf("log/event-log-%d-%d-0123456789abcdef.seg", from, to), Size: 4096, Checksum: checksum})
		if os.Getenv("GTRON_EVENT_COMPANION_BENCH_NO_INDEX") != "1" {
			refs = append(refs, SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: from, ToTxNum: to, Path: fmt.Sprintf("log/event-log-index-%d-%d-0123456789abcdef.seg", from, to), Size: 1024, Checksum: checksum})
		}
	}
	// Target the observed 59,802,974-byte catalog while keeping 44,580 event
	// main refs. Additional valid block-numbered metadata models other families;
	// no inaccessible server path or fabricated segment-file read is required.
	target := 59802974
	if count != 44580 {
		target = 0
	}
	estimated := 0
	for _, ref := range refs {
		b, _ := json.Marshal(ref)
		estimated += len(b) + 1
	}
	for i := 0; estimated < target; i++ {
		ref := SegmentRef{Dataset: SegmentDatasetBalanceTrace, Kind: SegmentBalanceTrace, FromTxNum: uint64(i*1000 + 1), ToTxNum: uint64((i + 1) * 1000), Path: fmt.Sprintf("balance/balance-trace-%d-%d-0123456789abcdef.seg", i*1000+1, (i+1)*1000), Size: 4096, Checksum: checksum}
		b, _ := json.Marshal(ref)
		estimated += len(b) + 1
		refs = append(refs, ref)
	}
	m := NewManifest(1, uint64(count*1000), refs)
	m.PublishedUnix = 1793431627
	m.Generation = 52927
	// Fixed shuffle forces identical complete sort work for both publish arms.
	random := rand.New(rand.NewSource(20261031))
	random.Shuffle(len(m.Segments), func(i, j int) { m.Segments[i], m.Segments[j] = m.Segments[j], m.Segments[i] })
	beforeJSON, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	original := append([]SegmentRef(nil), m.Segments...)
	// A fresh publication must include detached cache seeding/copies. Do not
	// compare an idempotent cache hit to a fresh full publication.
	loadedManifestCache.mu.Lock()
	loadedManifestCache.clearLocked()
	loadedManifestCache.mu.Unlock()
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, strconv.Itoa(256<<20))
	dir := t.TempDir()
	runtime.GC()
	var mem0, mem1 runtime.MemStats
	runtime.ReadMemStats(&mem0)
	var cpu0, cpu1 syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &cpu0)
	started := time.Now()
	if mode == "validate" {
		err = m.Validate()
	} else {
		err = PublishManifest(dir, m)
	}
	wall := time.Since(started)
	syscall.Getrusage(syscall.RUSAGE_SELF, &cpu1)
	runtime.ReadMemStats(&mem1)
	if err != nil {
		t.Fatal(err)
	}
	var bytesWritten int
	var output []byte
	actualIndexes := count
	if os.Getenv("GTRON_EVENT_COMPANION_BENCH_NO_INDEX") == "1" {
		actualIndexes = 0
	}
	if mode == "publish" {
		output, err = os.ReadFile(filepath.Join(dir, ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		stat, err := os.Stat(filepath.Join(dir, ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		bytesWritten = int(stat.Size())
		if bytesWritten != len(output) {
			t.Fatal("stat bytes differ")
		}
		want := *m
		want.Segments = append([]SegmentRef(nil), original...)
		sortSegments(want.Segments)
		wantJSON, _ := json.Marshal(&want)
		if !bytes.Equal(output, wantJSON) {
			t.Fatal("publication bytes differ from sorted independent input")
		}
		loadedManifestCache.mu.Lock()
		cacheOK := loadedManifestCache.manifest != nil && loadedManifestCache.manifest != m && bytes.Equal(loadedManifestCache.data, output)
		loadedManifestCache.mu.Unlock()
		if !cacheOK {
			t.Fatal("publication cache did not retain detached same-byte manifest")
		}
		for _, entry := range mustReadEventCompanionBenchmarkDir(t, dir) {
			if entry.Name() != ManifestFile {
				t.Fatalf("leaked temp %s", entry.Name())
			}
		}
	} else {
		afterJSON, _ := json.Marshal(m)
		if !bytes.Equal(beforeJSON, afterJSON) || !reflect.DeepEqual(original, m.Segments) {
			t.Fatal("validation mutated manifest")
		}
		output = beforeJSON
	}
	digest := sha256.Sum256(output)
	peakRSS := uint64(cpu1.Maxrss)
	if runtime.GOOS != "darwin" {
		peakRSS *= 1024
	}
	row := map[string]any{"mode": mode, "arm": arm, "event_main_refs": count, "event_index_refs": actualIndexes, "total_refs": len(m.Segments), "manifest_bytes": len(output), "actual_manifest_file_stat_bytes": bytesWritten, "wall_ns": wall.Nanoseconds(), "user_cpu_ns": cpu1.Utime.Nano() - cpu0.Utime.Nano(), "sys_cpu_ns": cpu1.Stime.Nano() - cpu0.Stime.Nano(), "alloc_bytes": mem1.TotalAlloc - mem0.TotalAlloc, "alloc_objects": mem1.Mallocs - mem0.Mallocs, "process_peak_rss_including_seed_bytes": peakRSS, "output_sha256": hex.EncodeToString(digest[:]), "gomaxprocs": 4}
	encoded, _ := json.Marshal(row)
	t.Logf("EVENT_COMPANION_BENCH_RESULT %s", encoded)
}
func mustReadEventCompanionBenchmarkDir(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}
