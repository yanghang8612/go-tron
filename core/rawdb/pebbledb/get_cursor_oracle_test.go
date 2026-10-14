package pebbledb

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func compareGetCursor(t *testing.T, s *pebble.Snapshot, c *pebble.GetCursor, keys [][]byte) {
	t.Helper()
	for _, key := range keys {
		want, closer, werr := s.Get(key)
		want = bytes.Clone(want)
		if closer != nil {
			if err := closer.Close(); werr == nil {
				werr = err
			}
		}
		got, gerr := c.Get(key)
		if !bytes.Equal(got, want) || !errors.Is(gerr, werr) {
			t.Fatalf("key %x: cursor %q/%v, oracle %q/%v", key, got, gerr, want, werr)
		}
		if err := c.ReleaseValue(); err != nil {
			t.Fatal(err)
		}
	}
}

// This root-module test runs in ordinary CI and compares the new lazy path
// against Snapshot.Get, not a merged iterator. Nested-module fault tests are
// additional checks, not the only coverage of the fork API.
func TestGetCursorSnapshotGetOracle(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprintf("split=%v", split), func(t *testing.T) {
			cmp := *pebble.DefaultComparer
			if split {
				cmp.Name = "cursor-test-prefix"
				cmp.Split = func(k []byte) int {
					if len(k) > 2 {
						return 2
					}
					return len(k)
				}
			}
			db, err := pebble.Open("oracle", &pebble.Options{FS: vfs.NewMem(), Comparer: &cmp, FormatMajorVersion: pebble.FormatNewest, DisableAutomaticCompactions: true, L0StopWritesThreshold: 32,
				Levels: []pebble.LevelOptions{{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.NoCompression, TargetFileSize: 8 << 10}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			keys := [][]byte{nil, []byte{0xff}, bytes.Repeat([]byte{0xfe}, 8192), []byte("p/single"), []byte("p/sized"), []byte("p/merge"), []byte("p/nobase"), []byte("p/merge-delete")}
			for i := 0; i < 128; i++ {
				keys = append(keys, []byte(fmt.Sprintf("p/%03d", i)))
			}
			for _, key := range keys {
				if bytes.Equal(key, []byte("p/nobase")) {
					continue
				}
				if err := db.Set(key, append([]byte("base:"), key...), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
			}
			early := db.NewSnapshot()
			t.Cleanup(func() {
				if early != nil {
					_ = early.Close()
				}
			})
			earlyCursor, err := early.NewGetCursor()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = earlyCursor.Close() })
			if err := db.Set([]byte("p/000"), []byte("after-snapshot-mutable"), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte("p/new-mutable"), []byte("future"), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			compareGetCursor(t, early, earlyCursor, [][]byte{[]byte("p/000"), []byte("p/new-mutable")})
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			for wave := 0; wave < 7; wave++ {
				for i := 0; i < 128; i++ {
					key := []byte(fmt.Sprintf("p/%03d", i))
					if i%7 == wave {
						if err := db.Set(key, []byte(fmt.Sprintf("wave-%d", wave)), pebble.NoSync); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := db.Merge([]byte("p/merge"), []byte(fmt.Sprintf("+%d", wave)), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				if err := db.Merge([]byte("p/nobase"), []byte("+n"), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				if err := db.DeleteRange([]byte(fmt.Sprintf("p/%03d", wave*10)), []byte(fmt.Sprintf("p/%03d", wave*10+3)), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				if wave == 0 {
					if err := db.SingleDelete([]byte("p/single"), pebble.NoSync); err != nil {
						t.Fatal(err)
					}
					if err := db.DeleteSized([]byte("p/sized"), uint32(len("base:p/sized")), pebble.NoSync); err != nil {
						t.Fatal(err)
					}
					if err := db.Merge([]byte("p/merge-delete"), []byte("+x"), pebble.NoSync); err != nil {
						t.Fatal(err)
					}
				}
				if wave == 1 {
					if err := db.Delete([]byte("p/merge-delete"), pebble.NoSync); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Set([]byte("p/empty"), nil, pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, []byte("p/empty"), []byte("p/001a"), []byte("p/003a"), []byte("p/missing"), []byte{0xff, 0xff})
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := db.DeleteRange([]byte("p/075"), []byte("p/081"), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			tables, err := db.SSTables(pebble.WithProperties())
			if err != nil {
				t.Fatal(err)
			}
			counts := make([]int, len(tables))
			rangeOnly := 0
			for l, files := range tables {
				counts[l] = len(files)
				for _, file := range files {
					if file.Properties.NumRangeDeletions > 0 && file.Properties.NumEntries == file.Properties.NumRangeDeletions {
						rangeOnly++
					}
				}
			}
			if db.Metrics().Levels[0].Sublevels < 4 || rangeOnly == 0 {
				t.Fatalf("fixture did not create overlapping sublevels/range-only SST: levels%v sublevels%d rangeOnly%d", counts, db.Metrics().Levels[0].Sublevels, rangeOnly)
			}
			t.Logf("before compact levels%v L0 sublevels%d range-only SST%d", counts, db.Metrics().Levels[0].Sublevels, rangeOnly)
			current := db.NewSnapshot()
			t.Cleanup(func() {
				if current != nil {
					_ = current.Close()
				}
			})
			currentCursor, err := current.NewGetCursor()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = currentCursor.Close() })
			prior := currentCursor.Stats().TableRequests
			compareGetCursor(t, current, currentCursor, [][]byte{[]byte("p/078")})
			if n := currentCursor.Stats().TableRequests - prior; n != 1 {
				t.Fatalf("range-only SST failed to stop lower lookups: requests%d", n)
			}
			// p/022 is absent from the wave2 SST's point set (i%%7!=2), but
			// covered by its tombstone. Bloom-negative point lookup must still
			// consult that file's range deletions and stop before the base.
			compareGetCursor(t, current, currentCursor, [][]byte{[]byte("p/022"), []byte("p/023"), []byte("p/020"), []byte("p/022")})
			if err := db.DeleteRange(nil, []byte{0xff, 0xff, 0xff}, pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte("p/missing"), []byte("future"), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := db.Compact(nil, []byte{0xff, 0xff, 0xff}, true); err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 6; pass++ {
				if err := db.Compact(nil, []byte{0xff, 0xff, 0xff}, true); err != nil {
					t.Fatal(err)
				}
			}
			tables, err = db.SSTables()
			if err != nil {
				t.Fatal(err)
			}
			for l, files := range tables {
				counts[l] = len(files)
			}
			if len(tables[6]) == 0 {
				t.Fatalf("fixture did not reachL6: %v", counts)
			}
			t.Logf("after compact levels%v", counts)
			late, err := early.NewGetCursor()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = late.Close() })
			ordered := make([][]byte, 0, len(keys)*4)
			ordered = append(ordered, keys...)
			for i := len(keys) - 1; i >= 0; i-- {
				ordered = append(ordered, keys[i])
			}
			random := rand.New(rand.NewSource(123))
			for i := 0; i < len(keys)*2; i++ {
				ordered = append(ordered, keys[random.Intn(len(keys))])
			}
			compareGetCursor(t, early, earlyCursor, ordered)
			compareGetCursor(t, early, late, ordered)
			compareGetCursor(t, current, currentCursor, ordered)
			// Overlapping L0 merge chains span more than four tables; slots visited
			// during one Get cannot be evicted while tombstone/value state is borrowed.
			stats := currentCursor.Stats()
			if stats.TableFallbacks == 0 {
				t.Fatalf("did not exercise >4 leased table fallback: %+v", stats)
			}
			if stats.MaxRetained > 4 {
				t.Fatalf("unbounded tables: %+v", stats)
			}
			t.Logf("cursor table stats: %+v", stats)
			var wg sync.WaitGroup
			for n := 0; n < 4; n++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c, err := current.NewGetCursor()
					if err != nil {
						t.Error(err)
						return
					}
					defer c.Close()
					compareGetCursor(t, current, c, ordered)
				}()
			}
			wg.Wait()
			for _, c := range []*pebble.GetCursor{earlyCursor, late, currentCursor} {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := c.Get(nil); !errors.Is(err, pebble.ErrClosed) {
					t.Fatal(err)
				}
			}
			if got := db.Metrics().TableIters; got != 0 {
				t.Fatalf("after Close: %d table iterators", got)
			}
			if err := early.Close(); err != nil {
				t.Fatal(err)
			}
			early = nil
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			current = nil
		})
	}
}

func TestGetCursorWrapperAllocatesOnlyForInPrefixRead(t *testing.T) {
	t.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	db, err := New(t.TempDir(), 16, 16, "test/get-cursor-lazy/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("p/key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	s, err := db.NewPointReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err := s.NewCursor([]byte("p/"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	concrete := c.(*pointReadCursor)
	if concrete.getCursor != nil {
		t.Fatal("unused cursor allocated")
	}
	if _, err := c.View([]byte("q/key"), func([]byte) error { t.Fatal("outside prefix callback"); return nil }); err != nil {
		t.Fatal(err)
	}
	if concrete.getCursor != nil {
		t.Fatal("outside prefix allocated")
	}
	if found, err := c.View([]byte("p/key"), func(v []byte) error {
		if string(v) != "value" {
			t.Fatal("wrong value")
		}
		return nil
	}); err != nil || !found {
		t.Fatalf("in prefix read %v/%v", found, err)
	}
	if concrete.getCursor == nil {
		t.Fatal("in prefix cursor not enabled")
	}
}

func TestGetCursorValueBlocksAndFetchError(t *testing.T) {
	cmp := *pebble.DefaultComparer
	cmp.Name = "cursor-valueblock-prefix"
	cmp.Split = func(k []byte) int {
		if len(k) > 2 {
			return 2
		}
		return len(k)
	}
	fs := &pointGetFailFS{FS: vfs.NewMem()}
	cache := pebble.NewCache(0)
	opts := &pebble.Options{FS: fs, Cache: cache, Comparer: &cmp, FormatMajorVersion: pebble.FormatNewest, DisableAutomaticCompactions: true,
		Levels: []pebble.LevelOptions{{Compression: pebble.NoCompression}}}
	opts.Experimental.EnableValueBlocks = func() bool { return true }
	db, err := pebble.Open("valueblocks", opts)
	cache.Unref()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		fs.fail.Store(false)
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	keys := make([][]byte, 64)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("p/%03d", i))
		if err := db.Set(keys[i], bytes.Repeat([]byte{byte(i)}, 4096), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	tables, err := db.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	var valueFile string
	for _, files := range tables {
		for _, file := range files {
			if file.Properties.NumValueBlocks > 0 {
				valueFile = fmt.Sprintf("valueblocks/%06d.sst", file.FileNum)
			}
		}
	}
	if valueFile == "" {
		t.Fatal("fixture has no actual value blocks")
	}
	file, err := fs.Open(valueFile)
	if err != nil {
		t.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(file)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{Comparer: &cmp})
	if err != nil {
		t.Fatal(err)
	}
	layout, err := reader.Layout()
	if err != nil {
		t.Fatal(err)
	}
	if len(layout.ValueBlock) == 0 {
		t.Fatal("value-block layout absent")
	}
	fs.failFrom = int64(layout.ValueBlock[0].Offset)
	fs.failTo = fs.failFrom + int64(layout.ValueBlock[0].Length)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	s := db.NewSnapshot()
	defer s.Close()
	c, err := s.NewGetCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Only value-block offsets fail. Index/data seeks remain valid, and the
	// explicit ValueAndErr extraction must deliver this lazy fetch failure.
	fs.fail.Store(true)
	if _, err := c.Get(keys[1]); !errors.Is(err, errPointGetIO) {
		t.Fatalf("lazy fetch error lost: %v", err)
	}
	fs.fail.Store(false)
	if err := c.Close(); err != nil {
		t.Fatalf("reported fetch error reappeared: %v", err)
	}
	fresh, err := s.NewGetCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	compareGetCursor(t, s, fresh, append(keys, keys...))
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	if got := db.Metrics().TableIters; got != 0 {
		t.Fatalf("valueblock cursor retained%d iterators", got)
	}
}
