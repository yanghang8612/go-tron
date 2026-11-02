// Read only footer/meta-index/properties of explicitly named immutable SSTs.
// No database, iterator, filter block or data block is opened/read by this tool.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/pebble/sstable"
)

type readRange struct {
	Offset int64 `json:"offset"`
	Bytes  int   `json:"bytes"`
}
type trackedFile struct {
	*os.File
	Reads []readRange
}

func (f *trackedFile) ReadAt(b []byte, off int64) (int, error) {
	n, e := f.File.ReadAt(b, off)
	f.Reads = append(f.Reads, readRange{off, n})
	return n, e
}

type result struct {
	Path                string      `json:"path"`
	FileBytes           int64       `json:"file_bytes"`
	NumEntries          uint64      `json:"num_entries"`
	FilterSize          uint64      `json:"filter_size"`
	DataSize            uint64      `json:"data_size"`
	IndexSize           uint64      `json:"index_size"`
	RawKeySize          uint64      `json:"raw_key_size"`
	RawValueSize        uint64      `json:"raw_value_size"`
	ValueBlocksSize     uint64      `json:"value_blocks_size"`
	MetadataReadBytes   uint64      `json:"metadata_read_bytes"`
	DataRegionReadBytes uint64      `json:"data_region_read_bytes"`
	Reads               []readRange `json:"reads"`
}

func inspect(path string) (out result, err error) {
	f, e := os.Open(path)
	if e != nil {
		return out, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return out, e
	}
	if !st.Mode().IsRegular() {
		return out, fmt.Errorf("not a regular SST: %s", path)
	}
	t := &trackedFile{File: f}
	readable, e := sstable.NewSimpleReadable(t)
	if e != nil {
		return out, e
	}
	r, e := sstable.NewReader(readable, sstable.ReaderOptions{})
	if e != nil {
		return out, e
	}
	p := r.Properties
	out = result{Path: path, FileBytes: st.Size(), NumEntries: p.NumEntries, FilterSize: p.FilterSize, DataSize: p.DataSize, IndexSize: p.IndexSize, RawKeySize: p.RawKeySize, RawValueSize: p.RawValueSize, ValueBlocksSize: p.ValueBlocksSize}
	if e = r.Close(); e != nil {
		return out, e
	}
	out.Reads = t.Reads
	for _, v := range t.Reads {
		out.MetadataReadBytes += uint64(v.Bytes)
		lo := max(v.Offset, 0)
		hi := min(v.Offset+int64(v.Bytes), int64(p.DataSize))
		if hi > lo {
			out.DataRegionReadBytes += uint64(hi - lo)
		}
	}
	if out.DataRegionReadBytes != 0 {
		return out, fmt.Errorf("unexpected overlap with data region: %d B", out.DataRegionReadBytes)
	}
	return out, nil
}
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: sst-properties <immutable.sst> [...]")
		os.Exit(2)
	}
	rows := make([]result, 0, len(os.Args)-1)
	for _, name := range os.Args[1:] {
		r, e := inspect(name)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
		rows = append(rows, r)
	}
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	if err := e.Encode(map[string]any{"scope": "explicit immutable SST footer/metaindex/properties only; no DB open, no iterator, no data/filter block read", "observed_utc": time.Now().UTC().Format(time.RFC3339Nano), "files": rows}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
