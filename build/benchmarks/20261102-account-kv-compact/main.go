// The input is root's immutable per-domain export: repeated BE u32 key length,
// physical key, BE u32 value length, original wrapped physical value. This
// experiment creates standalone SSTs only, never a live DB or migration.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type row struct {
	key, old, candidate []byte
	resource            bool
}

func digestRecord(h io.Writer, key, value []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(key)))
	h.Write(n[:])
	h.Write(key)
	binary.BigEndian.PutUint32(n[:], uint32(len(value)))
	h.Write(n[:])
	h.Write(value)
}

type readCounter struct {
	*os.File
	calls, bytes int64
}

func (c *readCounter) ReadAt(p []byte, off int64) (int, error) {
	n, e := c.File.ReadAt(p, off)
	c.calls++
	c.bytes += int64(n)
	return n, e
}
func nextRecord(r io.Reader) ([]byte, []byte, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return nil, nil, err
	}
	kn := binary.BigEndian.Uint32(b[:])
	if kn > 1<<20 {
		return nil, nil, fmt.Errorf("key too large")
	}
	k := make([]byte, kn)
	if _, err := io.ReadFull(r, k); err != nil {
		return nil, nil, err
	}
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return nil, nil, err
	}
	vn := binary.BigEndian.Uint32(b[:])
	if vn > 16<<20 {
		return nil, nil, fmt.Errorf("value too large")
	}
	v := make([]byte, vn)
	_, err := io.ReadFull(r, v)
	return k, v, err
}
func transform(key, value []byte) (row, error) {
	_, _, domain, _, ok := rawdb.DecodeStateKVLatestKey(key)
	if !ok {
		return row{}, fmt.Errorf("not canonical state key %x", key)
	}
	r := row{key: key, old: value, candidate: value, resource: domain == kvdomains.AccountResourceAux}
	if !r.resource {
		return r, nil
	}
	logical, err := rawdb.DecodeStateKVLatestValue(value)
	if err != nil {
		return r, err
	}
	compact, err := compactResource(logical)
	if err != nil {
		return r, err
	}
	expanded, err := expandResource(compact)
	if err != nil {
		return r, err
	}
	if !bytes.Equal(expanded, logical) {
		return r, fmt.Errorf("canonical resource bytes changed")
	}
	r.candidate = rawdb.EncodeStateKVLatestValue(compact)
	return r, nil
}
func writeSST(path string, rows []row, candidate bool, compression sstable.Compression) (map[string]any, error) {
	f, err := vfs.Default.Create(path)
	if err != nil {
		return nil, err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{BlockSize: 4096, IndexBlockSize: 4096, Compression: compression, FilterPolicy: bloom.FilterPolicy(10), TableFormat: sstable.TableFormatPebblev4})
	started := time.Now()
	for _, r := range rows {
		v := r.old
		if candidate {
			v = r.candidate
		}
		if err = w.Set(r.key, v); err != nil {
			w.Close()
			return nil, err
		}
	}
	if err = w.Close(); err != nil {
		return nil, err
	}
	elapsed := time.Since(started)
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	h := sha256.New()
	n, err := io.Copy(h, fh)
	if err != nil {
		return nil, err
	}
	return map[string]any{"path": path, "bytes": n, "sha256": hex.EncodeToString(h.Sum(nil)), "write_wall_ns": elapsed.Nanoseconds()}, nil
}
func pointBench(path string, rows []row, candidate bool, operations, cacheMiB int) (map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rc := &readCounter{File: f}
	rd, err := sstable.NewSimpleReadable(rc)
	if err != nil {
		f.Close()
		return nil, err
	}
	cache := pebble.NewCache(int64(cacheMiB) << 20)
	defer cache.Unref()
	reader, err := sstable.NewReader(rd, sstable.ReaderOptions{Cache: cache, Filters: map[string]sstable.FilterPolicy{bloom.FilterPolicy(10).Name(): bloom.FilterPolicy(10)}})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	it, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if operations > len(rows) {
		operations = len(rows)
	}
	indices := rand.New(rand.NewSource(113177)).Perm(len(rows))[:operations]
	result := map[string]any{"cache_bytes": int64(cacheMiB) << 20, "scope": "standalone SST successful random SeekGE; first pass empty application cache, OS page cache uncontrolled; not full HTTP or OS-cold latency", "operations_per_pass": operations, "metadata_open_read_calls": rc.calls, "metadata_open_read_bytes": rc.bytes}
	for pass := 0; pass < 2; pass++ {
		calls, n := rc.calls, rc.bytes
		start := time.Now()
		var materialized int
		for _, i := range indices {
			r := rows[i]
			k, lv := it.SeekGE(r.key, 0)
			if k == nil || !bytes.Equal(k.UserKey, r.key) {
				return nil, fmt.Errorf("SST miss %x", r.key)
			}
			value, _, err := lv.Value(nil)
			if err != nil {
				return nil, err
			}
			logical, err := rawdb.DecodeStateKVLatestValue(value)
			if err != nil {
				return nil, err
			}
			if candidate && r.resource {
				logical, err = expandResource(logical)
				if err != nil {
					return nil, err
				}
			}
			old, err := rawdb.DecodeStateKVLatestValue(r.old)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(logical, old) {
				return nil, fmt.Errorf("SST result mismatch")
			}
			materialized += len(logical)
		}
		name := "empty_app_cache"
		if pass == 1 {
			name = "warm_app_cache"
		}
		result[name] = map[string]any{"wall_ns": time.Since(start).Nanoseconds(), "read_calls": rc.calls - calls, "read_bytes": rc.bytes - n, "logical_result_bytes": materialized}
	}
	return result, nil
}
func run(input, out string, maxRows, ops, cache int) error {
	started := time.Now()
	if err := os.Mkdir(out, 0755); err != nil {
		return err
	}
	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	rows := make([]row, 0, min(maxRows, 100000))
	domains := map[string]int{}
	var oldBytes, newBytes, keyBytes int64
	var resourceRows int
	subsetHash := sha256.New()
	for len(rows) < maxRows {
		k, v, err := nextRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(rows) > 0 && bytes.Compare(rows[len(rows)-1].key, k) >= 0 {
			return fmt.Errorf("input not strictly sorted")
		}
		row, err := transform(k, v)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		digestRecord(subsetHash, k, v)
		oldBytes += int64(len(v))
		newBytes += int64(len(row.candidate))
		keyBytes += int64(len(k))
		if row.resource {
			resourceRows++
		}
		_, _, domain, _, _ := rawdb.DecodeStateKVLatestKey(k)
		domains[kvdomains.Name(domain)]++
	}
	if len(rows) == 0 {
		return fmt.Errorf("empty input")
	}
	result := map[string]any{"input": input, "input_subset_sha256": hex.EncodeToString(subsetHash.Sum(nil)), "input_subset_bytes": keyBytes + oldBytes + int64(len(rows))*8, "input_hash_scope": "exact consumed record prefix; full source identity belongs to census export receipt", "rows": len(rows), "row_limit": maxRows, "resource_rows": resourceRows, "domains": domains, "key_bytes": keyBytes, "old_value_bytes": oldBytes, "candidate_value_bytes": newBytes, "canonical_old_bytes_exactly_restored_for_every_transformed_row": true, "scope": "fixed exported live-key subset rewritten equally; experimental resource codec not enabled in production; space comparison excludes LSM versions, WAL and compaction", "started_utc": started.UTC().Format(time.RFC3339Nano)}
	cases := map[string]any{}
	for _, c := range []struct {
		name  string
		codec sstable.Compression
	}{{"none", sstable.NoCompression}, {"snappy", sstable.SnappyCompression}, {"zstd", sstable.ZstdCompression}} {
		ab := map[string]any{}
		for _, candidate := range []bool{false, true} {
			name := "old"
			if candidate {
				name = "candidate"
			}
			p := filepath.Join(out, c.name+"-"+name+".sst")
			meta, err := writeSST(p, rows, candidate, c.codec)
			if err != nil {
				return err
			}
			perf, err := pointBench(p, rows, candidate, ops, cache)
			if err != nil {
				return err
			}
			meta["point_reads"] = perf
			ab[name] = meta
		}
		cases[c.name] = ab
	}
	result["sst_cases"] = cases
	result["finished_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(out, "results.json"), append(data, '\n'), 0644); err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
func main() {
	mode := flag.String("mode", "resource", "resource sparse codec or bounded owner bundle")
	bundleSize := flag.Int("bundle-rows", 8, "maximum 2..16 same-owner scalar-map rows per experimental bundle")
	input := flag.String("input", "", "immutable BE u32 keylen/key/u32 vallen/value row stream")
	out := flag.String("out", "", "new output directory; must not exist")
	maxRows := flag.Int("max-rows", 500000, "bounded input row limit")
	ops := flag.Int("point-ops", 10000, "successful random lookups per cache pass")
	cache := flag.Int("cache-mib", 32, "same application block cache per variant")
	flag.Parse()
	if *input == "" || *out == "" || *maxRows <= 0 || *ops <= 0 || *cache < 0 {
		panic("valid input/out/limits required")
	}
	var err error
	switch *mode {
	case "resource":
		err = run(*input, *out, *maxRows, *ops, *cache)
	case "bundle":
		err = runBundle(*input, *out, *maxRows, *ops, *cache, *bundleSize)
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		panic(err)
	}
}
