// Physical-only compaction experiment on a second disposable copy.
// No logical deletes, production paths or service controls exist in this tool.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func allocation(dir string) (n uint64, err error) {
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		s, e := d.Info()
		if e != nil {
			return e
		}
		n += uint64(s.Size())
		return nil
	})
	return
}
func allocated(dir string) (n uint64, err error) {
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		s, e := d.Info()
		if e != nil {
			return e
		}
		n += uint64(s.Sys().(*syscall.Stat_t).Blocks) * 512
		return nil
	})
	return
}
func clone(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(src, p)
		if e != nil {
			return e
		}
		q := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.Mkdir(q, 0755)
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink rejected: %s", p)
		}
		if d.Name() == "LOCK" {
			return nil
		}
		if strings.HasSuffix(p, ".sst") {
			return os.Link(p, q)
		}
		in, e := os.Open(p)
		if e != nil {
			return e
		}
		defer in.Close()
		out, e := os.OpenFile(q, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if e != nil {
			return e
		}
		_, e = io.Copy(out, in)
		ce := out.Close()
		if e != nil {
			return e
		}
		return ce
	})
}
func main() {
	source := flag.String("source", "", "fixed maintenance chaindata copy")
	out := flag.String("out", "", "new experiment directory")
	flag.Parse()
	if !strings.Contains(*source, "/maintenance/") || filepath.Base(*source) != "chaindata" || !strings.Contains(*out, "/maintenance/") {
		panic("only explicit maintenance copies are permitted")
	}
	if e := os.Mkdir(*out, 0755); e != nil {
		panic(e)
	}
	dst := filepath.Join(*out, "chaindata")
	started := time.Now()
	if e := clone(*source, dst); e != nil {
		panic(e)
	}
	before, e := allocation(dst)
	if e != nil {
		panic(e)
	}
	beforeAllocated, e := allocated(dst)
	if e != nil {
		panic(e)
	}
	cache := pebble.NewCache(128 << 20)
	defer cache.Unref()
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		levels[i].TargetFileSize = 8 << 20 << i
		levels[i].FilterPolicy = bloom.FilterPolicy(10)
		if i < 6 {
			levels[i].Compression = pebble.NoCompression
		}
	}
	db, e := pebble.Open(dst, &pebble.Options{Cache: cache, DisableAutomaticCompactions: true, Levels: levels, LBaseMaxBytes: 1 << 30, MemTableSize: 64 << 20, MemTableStopWritesThreshold: 8, MaxConcurrentCompactions: func() int { return 1 }})
	if e != nil {
		panic(e)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	if e = db.Flush(); e != nil {
		panic(e)
	}
	var r rawdb.PhysicalSpaceRange
	for _, x := range rawdb.DiskSpaceObservationRanges() {
		if x.Name == "staged_body" {
			r = x
		}
	}
	if len(r.Start) == 0 {
		panic("missing schema-owned staging range")
	}
	count := func() uint64 {
		it, e := db.NewIter(&pebble.IterOptions{LowerBound: r.Start, UpperBound: r.End})
		if e != nil {
			panic(e)
		}
		defer it.Close()
		var n uint64
		for ok := it.First(); ok; ok = it.Next() {
			n++
		}
		if e = it.Error(); e != nil {
			panic(e)
		}
		return n
	}
	rowsBefore := count()
	if rowsBefore != 0 {
		panic("captured shutdown staging is not empty; report needs different interpretation")
	}
	pre, e := db.EstimateDiskUsage(r.Start, r.End)
	if e != nil {
		panic(e)
	}
	afterOpen, e := allocation(dst)
	if e != nil {
		panic(e)
	}
	afterOpenAllocated, e := allocated(dst)
	if e != nil {
		panic(e)
	}
	compactStarted := time.Now()
	if e = db.Compact(r.Start, r.End, false); e != nil {
		panic(e)
	}
	compactSeconds := time.Since(compactStarted).Seconds()
	post, e := db.EstimateDiskUsage(r.Start, r.End)
	if e != nil {
		panic(e)
	}
	rowsAfter := count()
	if rowsAfter != rowsBefore {
		panic("logical staged rows changed")
	}
	if e = db.Close(); e != nil {
		panic(e)
	}
	db = nil
	after, e := allocation(dst)
	if e != nil {
		panic(e)
	}
	afterAllocated, e := allocated(dst)
	if e != nil {
		panic(e)
	}
	result := map[string]any{"source": *source, "copy": dst, "scope": "physical-only manual staging-range compaction on disposable copy; all source SSTs retained unchanged; directory lengths are not actual server freed bytes", "no_logical_deletes": true, "source_copy_file_lengths": before, "after_open_and_flush_file_lengths": afterOpen, "after_compaction_and_close_file_lengths": after, "open_flush_file_length_delta": int64(afterOpen) - int64(before), "compaction_file_length_reduction": int64(afterOpen) - int64(after), "staged_rows_before": rowsBefore, "staged_rows_after": rowsAfter, "staging_range_sst_estimate_before": pre, "staging_range_sst_estimate_after": post, "compact_seconds": compactSeconds, "elapsed_seconds": time.Since(started).Seconds(), "complete": true}
	result["source_copy_allocated_bytes_including_directory"] = beforeAllocated
	result["after_open_flush_allocated_bytes_including_directory"] = afterOpenAllocated
	result["after_compaction_close_allocated_bytes_including_directory"] = afterAllocated
	result["compaction_directory_allocation_reduction"] = int64(afterOpenAllocated) - int64(afterAllocated)
	b, _ := json.MarshalIndent(result, "", "  ")
	if e = os.WriteFile(filepath.Join(*out, "result.json"), b, 0644); e != nil {
		panic(e)
	}
	fmt.Println(string(b))
}
