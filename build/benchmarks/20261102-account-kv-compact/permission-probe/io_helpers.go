package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

var exactComparer = func() *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = func(k []byte) int { return len(k) }
	return &c
}()

type readFS struct {
	vfs.FS
	calls, bytes atomic.Int64
}
type readFile struct {
	vfs.File
	owner *readFS
}

func (f *readFS) Open(name string, options ...vfs.OpenOption) (vfs.File, error) {
	v, e := f.FS.Open(name, options...)
	if e == nil && strings.HasSuffix(name, ".sst") {
		return &readFile{v, f}, nil
	}
	return v, e
}
func (f *readFile) ReadAt(p []byte, off int64) (int, error) {
	n, e := f.File.ReadAt(p, off)
	f.owner.calls.Add(1)
	f.owner.bytes.Add(int64(n))
	return n, e
}

func options(fs vfs.FS, cache *pebble.Cache, readOnly bool) *pebble.Options {
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	return &pebble.Options{FS: fs, Cache: cache, Comparer: exactComparer, Levels: levels, ReadOnly: readOnly, FormatMajorVersion: pebble.FormatNewest, DisableAutomaticCompactions: true, MaxConcurrentCompactions: func() int { return 1 }, MaxOpenFiles: 128, LBaseMaxBytes: 1}
}

func createDB(source, dir string) (map[string]any, error) {
	if e := os.Mkdir(dir, 0755); e != nil {
		return nil, e
	}
	input := filepath.Join(dir, "ingest-input.sst")
	src, e := os.Open(source)
	if e != nil {
		return nil, e
	}
	before, e := src.Stat()
	if e != nil {
		src.Close()
		return nil, e
	}
	dst, e := os.OpenFile(input, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		src.Close()
		return nil, e
	}
	h := sha256.New()
	n, e := io.Copy(io.MultiWriter(dst, h), src)
	src.Close()
	if e != nil {
		dst.Close()
		return nil, e
	}
	if e = dst.Sync(); e != nil {
		dst.Close()
		return nil, e
	}
	if e = dst.Close(); e != nil {
		return nil, e
	}
	copied, e := os.Stat(input)
	if e != nil {
		return nil, e
	}
	after, e := os.Stat(source)
	if e != nil || before.Size() != after.Size() || before.ModTime() != after.ModTime() {
		return nil, fmt.Errorf("source SST changed")
	}
	cache := pebble.NewCache(32 << 20)
	defer cache.Unref()
	dbdir := filepath.Join(dir, "db")
	db, e := pebble.Open(dbdir, options(vfs.Default, cache, false))
	if e != nil {
		return nil, e
	}
	defer db.Close()
	if e = db.Ingest([]string{input}); e != nil {
		return nil, e
	}
	tables, e := db.SSTables(pebble.WithProperties())
	if e != nil {
		return nil, e
	}
	count := 0
	var filter uint64
	var ingested string
	for level, files := range tables {
		for _, f := range files {
			count++
			if level != 6 {
				return nil, fmt.Errorf("ingest placed table at L%d; refuse to compact/rewrite it", level)
			}
			if f.Properties == nil || f.Properties.FilterSize == 0 {
				return nil, fmt.Errorf("missing full-key Bloom filter")
			}
			filter += f.Properties.FilterSize
			ingested = filepath.Join(dbdir, fmt.Sprintf("%06d.sst", f.FileNum))
		}
	}
	if count != 1 {
		return nil, fmt.Errorf("expected exactly one immutable L6 table, got %d", count)
	}
	actual, e := os.Stat(ingested)
	if e != nil {
		return nil, e
	}
	if !os.SameFile(copied, actual) || actual.Size() != n {
		return nil, fmt.Errorf("ingestion rewrote SST")
	}
	return map[string]any{"source_sst": source, "source_sst_bytes": n, "source_sst_sha256": hex.EncodeToString(h.Sum(nil)), "db_path": dbdir, "ingested_table_path": ingested, "l6_tables": count, "l6_filter_bytes": filter, "ingested_same_inode_as_independent_copy": true, "source_sst_preserved": true, "sst_rewritten": false}, nil
}
