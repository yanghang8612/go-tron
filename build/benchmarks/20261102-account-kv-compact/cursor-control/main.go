// Production-cursor control over already-written immutable experiment SSTs.
// No production DB is opened and no SST is rewritten or compacted.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var exactComparer = func() *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = func(k []byte) int { return len(k) }
	return &c
}()

type sourceRow struct {
	key, wrapped, logical []byte
	owner                 common.Address
}
type query struct {
	key, logical []byte
	present      bool
}
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

func load(path string, limit int) ([]sourceRow, string, int64, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, "", 0, e
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	h := sha256.New()
	var consumed int64
	rows := make([]sourceRow, 0, min(limit, 100000))
	var n [4]byte
	for len(rows) < limit {
		if _, e = io.ReadFull(r, n[:]); e == io.EOF {
			break
		} else if e != nil {
			return nil, "", 0, e
		}
		kn := binary.BigEndian.Uint32(n[:])
		if kn > 1<<20 {
			return nil, "", 0, fmt.Errorf("key too long")
		}
		h.Write(n[:])
		k := make([]byte, kn)
		if _, e = io.ReadFull(r, k); e != nil {
			return nil, "", 0, e
		}
		h.Write(k)
		if _, e = io.ReadFull(r, n[:]); e != nil {
			return nil, "", 0, e
		}
		vn := binary.BigEndian.Uint32(n[:])
		if vn > 16<<20 {
			return nil, "", 0, fmt.Errorf("value too long")
		}
		h.Write(n[:])
		v := make([]byte, vn)
		if _, e = io.ReadFull(r, v); e != nil {
			return nil, "", 0, e
		}
		h.Write(v)
		if len(rows) > 0 && bytes.Compare(rows[len(rows)-1].key, k) >= 0 {
			return nil, "", 0, fmt.Errorf("unordered source")
		}
		owner, _, _, _, ok := rawdb.DecodeStateKVLatestKey(k)
		if !ok {
			return nil, "", 0, fmt.Errorf("bad source key")
		}
		logical, e := rawdb.DecodeStateKVLatestValueNoCopy(v)
		if e != nil {
			return nil, "", 0, e
		}
		rows = append(rows, sourceRow{k, v, logical, owner})
		consumed += int64(8 + kn + vn)
	}
	if len(rows) == 0 {
		return nil, "", 0, fmt.Errorf("empty cohort")
	}
	return rows, hex.EncodeToString(h.Sum(nil)), consumed, nil
}
func makeQueries(rows []sourceRow, ops int) map[string][]query {
	if ops > len(rows) {
		ops = len(rows)
	}
	indices := rand.New(rand.NewSource(113177)).Perm(len(rows))[:ops]
	result := map[string][]query{"positive": {}, "negative_same_owner": {}, "negative_owner_absent_in_cohort": {}}
	for _, i := range indices {
		r := rows[i]
		result["positive"] = append(result["positive"], query{r.key, r.logical, true})
		missing := append(append([]byte(nil), r.key...), 0)
		for {
			j := sort.Search(len(rows), func(j int) bool { return bytes.Compare(rows[j].key, missing) >= 0 })
			if j == len(rows) || !bytes.Equal(rows[j].key, missing) {
				break
			}
			missing = append(missing, 0)
		}
		result["negative_same_owner"] = append(result["negative_same_owner"], query{missing, nil, false})
		owner, generation, domain, logical, _ := rawdb.DecodeStateKVLatestKey(r.key)
		for step := 1; ; step++ {
			if step > 65535 {
				panic("cannot produce absent owner")
			}
			owner[9] ^= byte(step)
			owner[10] ^= byte(step >> 8)
			j := sort.Search(len(rows), func(j int) bool { return bytes.Compare(rows[j].owner[:], owner[:]) >= 0 })
			if j == len(rows) || rows[j].owner != owner {
				break
			}
		}
		missing = rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logical)
		result["negative_owner_absent_in_cohort"] = append(result["negative_owner_absent_in_cohort"], query{missing, nil, false})
	}
	return result
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

type scope struct {
	snapshot              *pebble.Snapshot
	cursor                *pebble.GetCursor
	iter                  *pebble.Iterator
	stats                 pebble.GetCursorStats
	predecessors, decoded uint64
}

func newScope(db *pebble.DB, packed bool) (*scope, error) {
	s := &scope{snapshot: db.NewSnapshot()}
	var e error
	if packed {
		s.iter, e = s.snapshot.NewIter(nil)
	} else {
		s.cursor, e = s.snapshot.NewGetCursorWithOptions(pebble.GetCursorOptions{MaxRetainedTables: 16, UseL6Filters: true})
	}
	if e != nil {
		s.snapshot.Close()
		return nil, e
	}
	return s, nil
}
func (s *scope) lookup(target []byte, packed bool) ([]byte, bool, error) {
	if !packed {
		v, e := s.cursor.Get(target)
		if e == pebble.ErrNotFound {
			return nil, false, nil
		}
		if e != nil {
			return nil, false, e
		}
		return v, true, nil
	}
	ok := s.iter.SeekGE(target)
	if !ok && !s.iter.Valid() && s.iter.Error() != nil {
		return nil, false, s.iter.Error()
	}
	if !ok || !bytes.Equal(s.iter.Key(), target) {
		ok = s.iter.SeekLT(target)
		s.predecessors++
	}
	if !ok {
		return nil, false, s.iter.Error()
	}
	key := s.iter.Key()
	value, e := s.iter.ValueAndErr()
	if e != nil {
		return nil, false, e
	}
	if bytes.HasPrefix(value, bundleMagic) {
		v, found, n, e := bundleValue(key, value, target)
		s.decoded += uint64(n)
		return v, found, e
	}
	s.decoded++
	return value, bytes.Equal(key, target), nil
}
func (s *scope) close() error {
	var e error
	if s.cursor != nil {
		e = s.cursor.Close()
		s.stats = s.cursor.Stats()
	}
	if s.iter != nil {
		e = errors.Join(e, s.iter.Close())
	}
	return errors.Join(e, s.snapshot.Close())
}
func addStats(dst *pebble.GetCursorStats, v pebble.GetCursorStats) {
	dst.TableRequests += v.TableRequests
	dst.TableOpens += v.TableOpens
	dst.TableReuses += v.TableReuses
	dst.TableFallbacks += v.TableFallbacks
	dst.TableEvictions += v.TableEvictions
	dst.TableFallbackIncompatible += v.TableFallbackIncompatible
	dst.TableFallbackAllLeased += v.TableFallbackAllLeased
	if v.MaxRetained > dst.MaxRetained {
		dst.MaxRetained = v.MaxRetained
	}
}

func measure(dbpath string, queries []query, packed, resourceCandidate bool, group int) (map[string]any, error) {
	fs := &readFS{FS: vfs.Default}
	cache := pebble.NewCache(32 << 20)
	defer cache.Unref()
	db, e := pebble.Open(dbpath, options(fs, cache, true))
	if e != nil {
		return nil, e
	}
	defer db.Close()
	result := map[string]any{"operations_per_pass": len(queries), "scope_group_rows": group, "block_cache_bytes": 32 << 20, "reader_policy": "snapshot GetCursor(MaxRetainedTables=16,UseL6Filters=true)", "open_sst_read_calls": fs.calls.Load(), "open_sst_read_bytes": fs.bytes.Load()}
	if packed {
		result["reader_policy"] = "snapshot unbounded iterator SeekGE then SeekLT containing bundle; exact original-key Bloom cannot be used"
	}
	for pass := 0; pass < 2; pass++ {
		beforeCalls, beforeBytes := fs.calls.Load(), fs.bytes.Load()
		beforeFilter := db.Metrics().Filter
		var beforeMem, afterMem runtime.MemStats
		runtime.ReadMemStats(&beforeMem)
		started := time.Now()
		var stats pebble.GetCursorStats
		var decoded, predecessors, scopes uint64
		for from := 0; from < len(queries); from += group {
			to := min(from+group, len(queries))
			s, e := newScope(db, packed)
			if e != nil {
				return nil, e
			}
			scopes++
			for _, q := range queries[from:to] {
				value, found, e := s.lookup(q.key, packed)
				if e != nil {
					s.close()
					return nil, e
				}
				if found != q.present {
					s.close()
					return nil, fmt.Errorf("presence mismatch %x: got=%t want=%t", q.key, found, q.present)
				}
				if found {
					logical, e := rawdb.DecodeStateKVLatestValueNoCopy(value)
					if e == nil && resourceCandidate {
						logical, e = expandResource(logical)
					}
					if e != nil || !bytes.Equal(logical, q.logical) {
						s.close()
						return nil, fmt.Errorf("canonical value mismatch: %v", e)
					}
					if s.cursor != nil {
						if e = s.cursor.ReleaseValue(); e != nil {
							s.close()
							return nil, e
						}
					}
				}
			}
			if e = s.close(); e != nil {
				return nil, e
			}
			addStats(&stats, s.stats)
			decoded += s.decoded
			predecessors += s.predecessors
		}
		elapsed := time.Since(started)
		runtime.ReadMemStats(&afterMem)
		afterFilter := db.Metrics().Filter
		label := "empty_app_cache"
		if pass == 1 {
			label = "warm_app_cache"
		}
		result[label] = map[string]any{"wall_ns": elapsed.Nanoseconds(), "sst_read_calls": fs.calls.Load() - beforeCalls, "sst_read_bytes": fs.bytes.Load() - beforeBytes, "bloom_rejections": afterFilter.Hits - beforeFilter.Hits, "bloom_may_contain": afterFilter.Misses - beforeFilter.Misses, "snapshot_scopes": scopes, "get_cursor_stats": stats, "decoded_bundle_rows": decoded, "predecessor_seeks": predecessors, "mallocs": afterMem.Mallocs - beforeMem.Mallocs, "allocated_bytes": afterMem.TotalAlloc - beforeMem.TotalAlloc, "all_results_verified": true}
	}
	return result, nil
}

func run(rowsPath, sstDir, out, mode string, limit, ops, rounds int, codecs []string) error {
	if mode != "resource" && mode != "bundle" {
		return fmt.Errorf("invalid mode")
	}
	if e := os.Mkdir(out, 0755); e != nil {
		return e
	}
	started := time.Now()
	rows, hash, subsetBytes, e := load(rowsPath, limit)
	if e != nil {
		return e
	}
	legacy, e := os.ReadFile(filepath.Join(sstDir, "results.json"))
	if e != nil {
		return e
	}
	var origin map[string]json.RawMessage
	if e = json.Unmarshal(legacy, &origin); e != nil {
		return e
	}
	var originHash string
	if e = json.Unmarshal(origin["input_subset_sha256"], &originHash); e != nil {
		return e
	}
	var originRows int
	rowField := "rows"
	if mode == "bundle" {
		rowField = "source_rows"
	}
	if e = json.Unmarshal(origin[rowField], &originRows); e != nil {
		return e
	}
	if originHash != hash || originRows != len(rows) {
		return fmt.Errorf("source cohort does not match frozen SST producer: hash/rows mismatch")
	}
	var producerCases map[string]map[string]struct {
		SHA   string `json:"sha256"`
		Bytes int64  `json:"bytes"`
	}
	if e = json.Unmarshal(origin["sst_cases"], &producerCases); e != nil {
		return e
	}
	workloads := makeQueries(rows, ops)
	result := map[string]any{"mode": mode, "rows": len(rows), "input_rows_path": rowsPath, "input_subset_sha256": hash, "input_subset_bytes": subsetBytes, "original_result_path": filepath.Join(sstDir, "results.json"), "original_result_sha256": fmt.Sprintf("%x", sha256.Sum256(legacy)), "started_utc": started.UTC().Format(time.RFC3339Nano), "scope": "production-like point cursor over existing single-L6 cohort SSTs; 32MiB app cache; fresh snapshot per 1 or 32 lookups; empty-app-cache and warm passes; OS page cache uncontrolled; synthetic positive/negative key workloads; not HTTP, not mixed production LSM, no whole DB/MuHash/snapshot correctness claim", "cases": map[string]any{}}
	for _, codec := range codecs {
		if codec != "none" && codec != "snappy" && codec != "zstd" {
			return fmt.Errorf("unsupported codec %s", codec)
		}
		caseDir := filepath.Join(out, codec)
		if e = os.Mkdir(caseDir, 0755); e != nil {
			return e
		}
		arms := map[string]map[string]any{}
		for _, arm := range []string{"old", "candidate"} {
			meta, e := createDB(filepath.Join(sstDir, codec+"-"+arm+".sst"), filepath.Join(caseDir, arm))
			if e != nil {
				return e
			}
			expected, ok := producerCases[codec][arm]
			if !ok || meta["source_sst_sha256"] != expected.SHA || meta["source_sst_bytes"] != expected.Bytes {
				return fmt.Errorf("%s/%s SST does not match frozen producer receipt", codec, arm)
			}
			arms[arm] = meta
		}
		var measurements []map[string]any
		for round := 0; round < rounds; round++ {
			order := []string{"old", "candidate"}
			if round%2 == 1 {
				order = []string{"candidate", "old"}
			}
			for _, workload := range []string{"positive", "negative_same_owner", "negative_owner_absent_in_cohort"} {
				for _, group := range []int{1, 32} {
					for _, arm := range order {
						packed := mode == "bundle" && arm == "candidate"
						perf, e := measure(arms[arm]["db_path"].(string), workloads[workload], packed, mode == "resource" && arm == "candidate", group)
						if e != nil {
							return e
						}
						if workload == "negative_same_owner" && !packed && len(workloads[workload]) > 1 {
							for _, phase := range []string{"empty_app_cache", "warm_app_cache"} {
								m := perf[phase].(map[string]any)
								if m["bloom_rejections"].(int64)+m["bloom_may_contain"].(int64) == 0 {
									return fmt.Errorf("exact L6 negative control did not exercise Bloom: %s/%s/%s", codec, arm, phase)
								}
							}
						}
						perf["round"] = round
						perf["arm"] = arm
						perf["workload"] = workload
						measurements = append(measurements, perf)
					}
				}
			}
		}
		result["cases"].(map[string]any)[codec] = map[string]any{"arms": arms, "measurements": measurements}
		if e = writeJSON(filepath.Join(out, "progress.json"), map[string]any{"codec_completed": codec, "at_utc": time.Now().UTC().Format(time.RFC3339Nano)}); e != nil {
			return e
		}
	}
	result["finished_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	return writeJSON(filepath.Join(out, "results.json"), result)
}
func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(path+".tmp", append(b, '\n'), 0644); e != nil {
		return e
	}
	return os.Rename(path+".tmp", path)
}
func main() {
	rows := flag.String("rows", "", "immutable original row stream")
	ssts := flag.String("sst-dir", "", "frozen resource-results/asset-results directory")
	out := flag.String("out", "", "new independent output directory")
	mode := flag.String("mode", "resource", "resource or bundle")
	limit := flag.Int("max-rows", 500000, "must match existing frozen cohort")
	ops := flag.Int("point-ops", 10000, "lookups per workload/pass")
	rounds := flag.Int("rounds", 2, "AB/BA rounds")
	codecs := flag.String("codecs", "none,snappy,zstd", "existing SST variants to ingest")
	flag.Parse()
	if *rows == "" || *ssts == "" || *out == "" || *limit < 1 || *ops < 1 || *rounds < 1 {
		panic("valid rows/sst-dir/out/limits required")
	}
	if e := run(*rows, *ssts, *out, *mode, *limit, *ops, *rounds, strings.Split(*codecs, ",")); e != nil {
		panic(e)
	}
	fmt.Println("ACCOUNT_CURSOR_CONTROL_COMPLETE")
}
