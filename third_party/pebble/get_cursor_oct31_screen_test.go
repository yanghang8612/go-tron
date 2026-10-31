package pebble

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// This test-only screening fixture compares the existing immutable retention
// option. It is not a production policy change or whole-prefetch-pipeline proof.
// SST reads below are VFS ReadAt traffic, not block-device IO.
type oct31ReadFS struct {
	vfs.FS
	calls, bytes atomic.Uint64
}
type oct31ReadFile struct {
	vfs.File
	owner *oct31ReadFS
}

func (f *oct31ReadFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	x, err := f.FS.Open(name, opts...)
	if err == nil && strings.HasSuffix(name, ".sst") {
		return &oct31ReadFile{File: x, owner: f}, nil
	}
	return x, err
}
func (f *oct31ReadFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.owner.calls.Add(1)
	f.owner.bytes.Add(uint64(n))
	return n, err
}

func oct31EnvInt(t testing.TB, name string, fallback int) int {
	t.Helper()
	x := os.Getenv(name)
	if x == "" {
		return fallback
	}
	n, err := strconv.Atoi(x)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Public SST ingestion places overlapping sparse files above the existing L6
// base. A tiny test-only LBaseMaxBytes makes L1 eligible even for this small
// image; compactions stay disabled. Actual level populations are asserted.
func oct31BuildMiddleLevels(t testing.TB, db *DB) {
	t.Helper()
	for level := 5; level >= 1; level-- {
		name := filepath.Join(t.TempDir(), "layer.sst")
		file, err := vfs.Default.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{FilterPolicy: bloom.FilterPolicy(10)})
		for _, key := range []string{"a", fmt.Sprintf("layer%d", level), "z"} {
			if err = w.Set([]byte(key), []byte("sparse")); err != nil {
				t.Fatal(err)
			}
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		if err = db.Ingest([]string{name}); err != nil {
			t.Fatal(err)
		}
		if got := db.Metrics().Levels[level].NumFiles; got != 1 {
			t.Fatalf("ingested actual L%d files=%d want1", level, got)
		}
	}
}

func oct31MemoryFixture(t testing.TB, fs vfs.FS, cache *Cache) *cursorHotL0Fixture {
	t.Helper()
	db, err := Open(t.TempDir(), &Options{FS: fs, Cache: cache, DisableAutomaticCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	expected := make(map[string][]byte, 4096)
	for i := 0; i < 4096; i++ {
		key := fmt.Sprintf("m%05d", i)
		value := bytes.Repeat([]byte{byte(i % 251)}, 256)
		expected[key] = value
		if err = db.Set([]byte(key), value, NoSync); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := db.NewSnapshot()
	t.Cleanup(func() {
		if err := snapshot.Close(); err != nil {
			t.Error(err)
		}
	})
	return &cursorHotL0Fixture{db: db, snapshot: snapshot, expected: expected}
}

// Each invocation selects one arm and one fixed trace. Run with an explicit
// Nx benchtime, in a fresh process; do not let Go choose unequal work. Fixture
// construction is outside ns/op but belongs in external whole-process CPU/RSS.
// Every measured operation includes lazy first construction, all value release,
// and Close. No key warmup is performed. The first operation is cold in Pebble's
// cache; subsequent operations share the frozen cache horizon. The warm arm
// performs one identical complete ordinary-Get trace before the session loop.
func BenchmarkOct31CursorScreen(b *testing.B) {
	limit := oct31EnvInt(b, "OCT31_CURSOR_LIMIT", 4)
	layers := oct31EnvInt(b, "OCT31_CURSOR_L0", 2)
	reads := oct31EnvInt(b, "OCT31_CURSOR_READS", 256)
	cacheKiB := oct31EnvInt(b, "OCT31_CURSOR_CACHE_KIB", 64)
	if cacheKiB != 64 && cacheKiB != 4096 {
		b.Fatal("cache KiB must be 64 or 4096")
	}
	if limit != 4 && limit != 8 && limit != 16 {
		b.Fatal("limit must be 4, 8 or 16")
	}
	if layers != 1 && layers != 2 && layers != 4 && layers != 8 && layers != 16 {
		b.Fatal("L0 must be 1, 2, 4, 8 or 16")
	}
	if reads != 0 && reads != 1 && reads != 4 && reads != 16 && reads != 256 {
		b.Fatal("reads must be 0, 1, 4, 16 or 256")
	}
	fs := &oct31ReadFS{FS: vfs.Default}
	cache := NewCache(int64(cacheKiB) << 10)
	defer cache.Unref()
	var middle []func(*DB)
	if os.Getenv("OCT31_CURSOR_MULTILEVEL") == "1" {
		middle = append(middle, func(d *DB) { oct31BuildMiddleLevels(b, d) })
	}
	var f *cursorHotL0Fixture
	if os.Getenv("OCT31_CURSOR_MEMORY") == "1" {
		f = oct31MemoryFixture(b, fs, cache)
	} else {
		f = makeCursorHotL0FixtureOptions(b, layers, func(o *Options) {
			o.FS, o.Cache = fs, cache
			o.Experimental.ReadSamplingMultiplier = -1
			if len(middle) > 0 {
				o.LBaseMaxBytes = 1
			}
		}, middle...)
		// Close build-time readers and reopen with a fresh empty block/table cache.
		// OS page-cache state is not reset and is not described as device-cold.
		dir, levels := f.db.dirname, f.db.opts.Levels
		if err := f.snapshot.Close(); err != nil {
			b.Fatal(err)
		}
		f.snapshot = nil
		if err := f.db.Close(); err != nil {
			b.Fatal(err)
		}
		f.db = nil
		fresh := NewCache(int64(cacheKiB) << 10)
		defer fresh.Unref()
		var err error
		reopen := &Options{FS: fs, Cache: fresh, Levels: levels, ReadOnly: true}
		reopen.Experimental.ReadSamplingMultiplier = -1
		f.db, err = Open(dir, reopen)
		if err != nil {
			b.Fatal(err)
		}
		f.snapshot = f.db.NewSnapshot()
	}
	// Explicit 55% misses, including range-deleted and ordinary absent keys.
	// Independent literal map contains values from fixture writes, not Get.
	keys := make([][]byte, 4096)
	for i := range keys {
		prefix := "m"
		if i%20 < 11 {
			prefix = "n"
		}
		j := (i*4051 + 97) & 4095
		if prefix == "m" && j >= 1000 && j < 1200 {
			j += 200
		}
		keys[i] = []byte(fmt.Sprintf("%s%05d", prefix, j))
	}
	if os.Getenv("OCT31_CURSOR_WARM") == "1" {
		for _, key := range keys {
			_, closer, err := f.snapshot.Get(key)
			if err != nil && err != ErrNotFound {
				b.Fatal(err)
			}
			if closer != nil {
				if err = closer.Close(); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	var total GetCursorStats
	var hits uint64
	calls, readBytes := fs.calls.Load(), fs.bytes.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// Model the adapter's lazy inner ownership. An unused outer scope does
		// not allocate a GetCursor, readState or overflow backing.
		var c *GetCursor
		for j := 0; j < reads; j++ {
			if c == nil {
				var err error
				c, err = f.snapshot.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: limit, UseL6Filters: true})
				if err != nil {
					b.Fatal(err)
				}
			}
			key := keys[(n*reads+j)&4095]
			v, err := c.Get(key)
			want, present := f.expected[string(key)]
			if present {
				hits++
			}
			if present && (err != nil || !bytes.Equal(v, want)) || !present && err != ErrNotFound {
				b.Fatalf("literal oracle %q: present=%t err=%v", key, present, err)
			}
			if err = c.ReleaseValue(); err != nil {
				b.Fatal(err)
			}
		}
		if c != nil {
			if err := c.Close(); err != nil {
				b.Fatal(err)
			}
			s := c.Stats()
			total.TableOpens += s.TableOpens
			total.TableFallbackAllLeased += s.TableFallbackAllLeased
			total.TableEvictions += s.TableEvictions
			if s.MaxRetained > total.MaxRetained {
				total.MaxRetained = s.MaxRetained
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(fs.calls.Load()-calls)/float64(b.N), "ReadAt/scope")
	b.ReportMetric(float64(fs.bytes.Load()-readBytes)/float64(b.N), "VFS-B/scope")
	b.ReportMetric(float64(total.TableOpens)/float64(b.N), "opens/scope")
	b.ReportMetric(float64(total.TableFallbackAllLeased)/float64(b.N), "leased_fallback/scope")
	b.ReportMetric(float64(total.TableEvictions)/float64(b.N), "evictions/scope")
	b.ReportMetric(float64(total.MaxRetained), "peak_pairs")
	b.ReportMetric(float64(hits)/float64(b.N), "hits/scope")
}

func TestOct31CursorActualMiddleLevels(t *testing.T) {
	f := makeCursorHotL0FixtureOptions(t, 2, func(o *Options) { o.LBaseMaxBytes = 1 }, func(d *DB) { oct31BuildMiddleLevels(t, d) })
	for _, limit := range []int{4, 8, 16} {
		c, err := f.snapshot.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: limit, UseL6Filters: true})
		if err != nil {
			t.Fatal(err)
		}
		checkCursorHotL0Oracle(t, c, f)
		stats := c.Stats()
		if limit == 4 && stats.TableFallbackAllLeased == 0 {
			t.Fatalf("representative shape did not reproduce leased fallback: %+v", stats)
		}
		if limit >= 8 && stats.TableFallbackAllLeased != 0 {
			t.Fatalf("eight-table shape exceeded limit%d: %+v", limit, stats)
		}
		if err = c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOct31CursorL6RetentionLiteralOracle(t *testing.T) {
	for _, limit := range []int{4, 8, 16} {
		for _, l0 := range []int{1, 2, 4, 8, 16} {
			t.Run(fmt.Sprintf("limit%d/L0%d", limit, l0), func(t *testing.T) {
				f := makeCursorHotL0Fixture(t, l0)
				c, err := f.snapshot.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: limit, UseL6Filters: true})
				if err != nil {
					t.Fatal(err)
				}
				checkCursorHotL0Oracle(t, c, f)
				if err = c.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
