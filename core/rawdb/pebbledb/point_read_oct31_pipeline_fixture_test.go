package pebbledb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// All helpers in this file are test-only and exported only to the external
// test package, which may import the real blockbuffer without an import cycle.
type Oct31PipelineCounters struct {
	ReadAt, Bytes, Snapshots, SnapshotClosed, Cursors, Closed, Active, Peak atomic.Uint64
	Opens, Fallback, Evictions, PeakPairs, Retained, RetainedPeak           atomic.Uint64
	Barrier                                                                 func()
}
type oct31PipelineFS struct {
	vfs.FS
	counts *Oct31PipelineCounters
}
type oct31PipelineFile struct {
	vfs.File
	counts *Oct31PipelineCounters
}

func (f *oct31PipelineFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	x, e := f.FS.Open(name, opts...)
	if e == nil && strings.HasSuffix(name, ".sst") {
		return &oct31PipelineFile{x, f.counts}, nil
	}
	return x, e
}
func (f *oct31PipelineFile) ReadAt(b []byte, off int64) (int, error) {
	n, e := f.File.ReadAt(b, off)
	f.counts.ReadAt.Add(1)
	f.counts.Bytes.Add(uint64(n))
	return n, e
}
func oct31PipelineOptions(cache *pebble.Cache, fs vfs.FS) *pebble.Options {
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		levels[i].FilterPolicy = bloom.FilterPolicy(10)
		levels[i].Compression = pebble.NoCompression
	}
	o := &pebble.Options{FS: fs, Cache: cache, Comparer: exactPointComparer, Levels: levels, DisableAutomaticCompactions: true, L0StopWritesThreshold: 64, LBaseMaxBytes: 1, MemTableSize: 16 << 20}
	o.Experimental.ReadSamplingMultiplier = -1
	return o
}

func Oct31SeedPipelineImage(t testing.TB, dir string, keys, values [][]byte, l0 int, middle, memory bool) {
	t.Helper()
	p, e := pebble.Open(dir, oct31PipelineOptions(nil, vfs.Default))
	if e != nil {
		t.Fatal(e)
	}
	if !memory {
		for i, k := range keys {
			if e = p.Set(k, values[i], pebble.NoSync); e != nil {
				t.Fatal(e)
			}
		}
	}
	if !memory {
		for _, k := range []string{"a", "z"} {
			if e = p.Set([]byte(k), []byte("boundary"), pebble.NoSync); e != nil {
				t.Fatal(e)
			}
		}
		if e = p.Flush(); e != nil {
			t.Fatal(e)
		}
		if e = p.Compact([]byte("a"), []byte("{"), false); e != nil {
			t.Fatal(e)
		}
		if p.Metrics().Levels[6].NumFiles == 0 {
			t.Fatal("missing actualL6")
		}
		if middle {
			for level := 5; level >= 1; level-- {
				name := filepath.Join(t.TempDir(), "layer.sst")
				file, err := vfs.Default.Create(name)
				if err != nil {
					t.Fatal(err)
				}
				w := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{Comparer: exactPointComparer, FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.NoCompression})
				for _, k := range []string{"a", fmt.Sprintf("layer%d", level), "z"} {
					if e = w.Set([]byte(k), []byte("sparse")); e != nil {
						t.Fatal(e)
					}
				}
				if e = w.Close(); e != nil {
					t.Fatal(e)
				}
				if e = p.Ingest([]string{name}); e != nil {
					t.Fatal(e)
				}
				if p.Metrics().Levels[level].NumFiles != 1 {
					t.Fatalf("missing actualL%d", level)
				}
			}
		}
		for layer := 0; layer < l0; layer++ {
			for _, k := range []string{"a", "z"} {
				if e = p.Set([]byte(k), []byte(fmt.Sprint(layer)), pebble.NoSync); e != nil {
					t.Fatal(e)
				}
			}
			// One quarter of present rows are upper-level hits, using identical bytes.
			if layer == l0-1 {
				for i := 0; i < len(keys); i += 4 {
					if e = p.Set(keys[i], values[i], pebble.NoSync); e != nil {
						t.Fatal(e)
					}
				}
			}
			if e = p.Flush(); e != nil {
				t.Fatal(e)
			}
		}
		m := p.Metrics()
		if m.Levels[0].Sublevels != int32(l0) {
			t.Fatalf("actualL0 %d want%d", m.Levels[0].Sublevels, l0)
		}
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
}

type Oct31PipelineStore struct {
	*Database
	Counts *Oct31PipelineCounters
}

func Oct31OpenPipelineImage(t testing.TB, image string, cacheKiB int, warm bool, keys, values [][]byte, memory bool) *Oct31PipelineStore {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "copy")
	if e := os.MkdirAll(dir, 0755); e != nil {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(image)
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatal("unexpected image subdirectory")
		}
		in, e := os.Open(filepath.Join(image, entry.Name()))
		if e != nil {
			t.Fatal(e)
		}
		out, e := os.Create(filepath.Join(dir, entry.Name()))
		if e != nil {
			t.Fatal(e)
		}
		_, copyErr := io.Copy(out, in)
		a, b := in.Close(), out.Close()
		if copyErr != nil || a != nil || b != nil {
			t.Fatalf("copy errors: %v/%v/%v", copyErr, a, b)
		}
	}
	counts := &Oct31PipelineCounters{}
	fs := &oct31PipelineFS{vfs.Default, counts}
	cache := pebble.NewCache(int64(cacheKiB) << 10)
	p, e := pebble.Open(dir, oct31PipelineOptions(cache, fs))
	cache.Unref()
	if e != nil {
		t.Fatal(e)
	}
	if memory {
		// Replay of many small WAL batches may flush immutable memtables during
		// Open. The sealed memory image is empty; seed one batch after Open so the
		// entire source resides in one sufficiently grown mutable memtable.
		batch := p.NewBatch()
		for i, key := range keys {
			if e = batch.Set(key, values[i], nil); e != nil {
				t.Fatal(e)
			}
		}
		// Pebble's memTableEntrySize charges the maximum20-level skiplist
		// node per row.512B/row conservatively covers that node+internal key and
		// alignment; keep another1MiB margin below the16MiB/2 largebatch cutoff.
		upper := batch.Len() + len(keys)*512
		if upper >= 7<<20 {
			t.Fatalf("memory seed may exceed conservative largebatch bound: repr%d upper%d", batch.Len(), upper)
		}
		if e = batch.Commit(pebble.NoSync); e != nil {
			t.Fatal(e)
		}
		if e = batch.Close(); e != nil {
			t.Fatal(e)
		}
		for level, m := range p.Metrics().Levels {
			if m.NumFiles != 0 {
				t.Fatalf("mem-only source hasSST atL%d: %d", level, m.NumFiles)
			}
		}
	}
	d := &Oct31PipelineStore{&Database{db: p, boundedPointGet: true, writeOptions: pebble.NoSync}, counts}
	if d.writeOptions == nil || d.writeOptions.GetSync() {
		t.Fatal("test writer does not match production NoSync default")
	}
	t.Cleanup(func() {
		if e := d.Close(); e != nil {
			t.Error(e)
		}
	})
	if warm {
		for _, k := range keys {
			_, c, e := p.Get(k)
			if e != nil && e != pebble.ErrNotFound {
				t.Fatal(e)
			}
			if c != nil {
				if e = c.Close(); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	return d
}
func (d *Oct31PipelineStore) NewPointReadSnapshot() (pointread.Snapshot, error) {
	s, e := d.Database.NewPointReadSnapshot()
	if e != nil {
		return nil, e
	}
	d.Counts.Snapshots.Add(1)
	return &oct31PipelineSnapshot{s, d.Counts}, nil
}

type oct31PipelineSnapshot struct {
	pointread.Snapshot
	counts *Oct31PipelineCounters
}

func (s *oct31PipelineSnapshot) Close() error {
	s.counts.SnapshotClosed.Add(1)
	return s.Snapshot.Close()
}

func (s *oct31PipelineSnapshot) NewCursor(prefix []byte) (pointread.Cursor, error) {
	c, e := s.Snapshot.NewCursor(prefix)
	if e != nil {
		return nil, e
	}
	s.counts.Cursors.Add(1)
	n := s.counts.Active.Add(1)
	for {
		p := s.counts.Peak.Load()
		if n <= p || s.counts.Peak.CompareAndSwap(p, n) {
			break
		}
	}
	return &oct31PipelineCursor{Cursor: c, counts: s.counts}, nil
}

type oct31PipelineCursor struct {
	pointread.Cursor
	counts *Oct31PipelineCounters
	first  bool
	closed bool
	pairs  uint64
}

func (c *oct31PipelineCursor) View(k []byte, fn func([]byte) error) (bool, error) {
	ok, e := c.Cursor.View(k, fn)
	inner := c.Cursor.(*pointReadCursor).getCursor
	if inner != nil {
		pairs := inner.Stats().MaxRetained
		if pairs > c.pairs {
			live := c.counts.Retained.Add(pairs - c.pairs)
			c.pairs = pairs
			for {
				peak := c.counts.RetainedPeak.Load()
				if live <= peak || c.counts.RetainedPeak.CompareAndSwap(peak, live) {
					break
				}
			}
		}
	}
	if !c.first {
		c.first = true
		if c.counts.Barrier != nil {
			c.counts.Barrier()
		}
	}
	return ok, e
}
func (c *oct31PipelineCursor) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	s := c.Cursor.(*pointReadCursor)
	if s.getCursor != nil {
		stats := s.getCursor.Stats()
		c.counts.Opens.Add(stats.TableOpens)
		c.counts.Fallback.Add(stats.TableFallbackAllLeased)
		c.counts.Evictions.Add(stats.TableEvictions)
		c.counts.PeakPairs.Add(stats.MaxRetained)
	}
	e := c.Cursor.Close()
	if s.getCursor != nil {
		panic("closed cursor retained inner")
	}
	c.counts.Active.Add(^uint64(0))
	if c.pairs > 0 {
		c.counts.Retained.Add(^(c.pairs - 1))
	}
	c.counts.Closed.Add(1)
	return e
}

func (d *Oct31PipelineStore) Layout() []int64 {
	m := d.db.Metrics()
	layout := make([]int64, 8)
	layout[0] = int64(m.Levels[0].Sublevels)
	for i := 0; i < 7; i++ {
		layout[i+1] = m.Levels[i].NumFiles
	}
	return layout
}

// DrainSetupWAL only settles fixture initialization, outside timed read work.
// It writes no state keys and never changes the Database NoSync default.
func (d *Oct31PipelineStore) DrainSetupWAL() error {
	b := d.db.NewBatch()
	defer b.Close()
	if e := b.LogData(nil, nil); e != nil {
		return e
	}
	return b.Commit(pebble.Sync)
}

// Logical and physical WAL counters plus completed flushes must remain unchanged
// throughout the main read lifecycle. Final Close is timed after this check.
func (d *Oct31PipelineStore) WriteAccounting() [3]uint64 {
	m := d.db.Metrics()
	return [3]uint64{m.WAL.BytesIn, m.WAL.BytesWritten, uint64(m.Flush.Count)}
}
