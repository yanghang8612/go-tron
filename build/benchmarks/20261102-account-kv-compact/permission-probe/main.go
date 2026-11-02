// Short-prefix permission census and standalone reversible value experiment.
// It never changes physical keys, production data, MuHash or snapshot formats.
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
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type sampleRow struct {
	key, old, candidate, native []byte
	owner                       common.Address
}
type extraction struct {
	Rows        int               `json:"permission_rows"`
	Scanned     uint64            `json:"all_kv_rows_scanned"`
	Processed   uint64            `json:"source_key_value_bytes_processed"`
	KeyBytes    uint64            `json:"permission_physical_key_bytes"`
	OldBytes    uint64            `json:"old_wrapped_value_bytes"`
	NewBytes    uint64            `json:"candidate_wrapped_value_bytes"`
	NativeBytes uint64            `json:"old_native_bytes"`
	Self        uint64            `json:"self_address_occurrences"`
	Unknown     uint64            `json:"rows_with_unknown_fields"`
	Modes       map[string]uint64 `json:"modes"`
	Lengths     map[int]uint64    `json:"old_wrapped_exact_length_histogram"`
	Ops         map[string]uint64 `json:"exact_operations_bitmaps"`
	Hash        string            `json:"source_cohort_sha256"`
	Bytes       uint64            `json:"source_cohort_bytes"`
	Complete    bool              `json:"requested_prefix_complete"`
}

func record(w io.Writer, k, v []byte) error {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(k)))
	if _, e := w.Write(n[:]); e != nil {
		return e
	}
	if _, e := w.Write(k); e != nil {
		return e
	}
	binary.BigEndian.PutUint32(n[:], uint32(len(v)))
	if _, e := w.Write(n[:]); e != nil {
		return e
	}
	_, e := w.Write(v)
	return e
}
func addRow(rows []sampleRow, stats *extraction, k, v []byte) ([]sampleRow, error) {
	owner, _, domain, key, ok := rawdb.DecodeStateKVLatestKey(k)
	if !ok || domain != kvdomains.AccountPermissionAux {
		return nil, fmt.Errorf("not PermissionAux")
	}
	native, e := rawdb.DecodeStateKVLatestValueNoCopy(v)
	if e != nil {
		return nil, e
	}
	candidate, mode, p, e := compact(owner, key, native)
	if e != nil {
		return nil, e
	}
	restored, e := expand(owner, candidate)
	if e != nil || !bytes.Equal(native, restored) {
		return nil, fmt.Errorf("native byte restoration mismatch: %v", e)
	}
	wrapped := rawdb.EncodeStateKVLatestValue(candidate)
	if len(wrapped) >= len(v) {
		if !bytes.Equal(wrapped, v) {
			mode = "raw_no_net_value_gain"
		}
		wrapped = v
	}
	k = bytes.Clone(k)
	v = bytes.Clone(v)
	native, e = rawdb.DecodeStateKVLatestValueNoCopy(v)
	if e != nil {
		return nil, e
	}
	if bytes.Equal(wrapped, v) {
		wrapped = v
	}
	rows = append(rows, sampleRow{k, v, wrapped, native, owner})
	stats.Rows++
	stats.KeyBytes += uint64(len(k))
	stats.OldBytes += uint64(len(v))
	stats.NewBytes += uint64(len(wrapped))
	stats.NativeBytes += uint64(len(native))
	stats.Self += uint64(len(p.self))
	if len(p.unknown) > 0 || p.keyUnknown {
		stats.Unknown++
	}
	stats.Modes[mode]++
	stats.Lengths[len(v)]++
	stats.Ops[hex.EncodeToString(p.ops)]++
	return rows, nil
}
func extract(source, out string, limit int, maxScan uint64, rate int) ([]sampleRow, *extraction, error) {
	resolved, e := filepath.EvalSymlinks(source)
	if e != nil {
		return nil, nil, e
	}
	resolved, e = filepath.Abs(resolved)
	if e != nil {
		return nil, nil, e
	}
	if !strings.Contains(filepath.ToSlash(resolved), "/maintenance/") || filepath.Base(resolved) != "chaindata" || strings.Contains(filepath.ToSlash(resolved), "/main/datadir/") {
		return nil, nil, fmt.Errorf("only maintenance copy ending /chaindata allowed")
	}
	cache := pebble.NewCache(128 << 20)
	defer cache.Unref()
	db, e := pebble.Open(resolved, options(vfs.Default, cache, true))
	if e != nil {
		return nil, nil, e
	}
	defer db.Close()
	var bound rawdb.PhysicalSpaceRange
	for _, r := range rawdb.DiskSpaceObservationRanges() {
		if r.Name == "account_kv_latest" {
			bound = r
		}
	}
	if len(bound.Start) == 0 {
		return nil, nil, fmt.Errorf("schema range missing")
	}
	it, e := db.NewIter(&pebble.IterOptions{LowerBound: bound.Start, UpperBound: bound.End})
	if e != nil {
		return nil, nil, e
	}
	defer it.Close()
	f, e := os.OpenFile(filepath.Join(out, "permission-cohort.bin"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		return nil, nil, e
	}
	defer f.Close()
	hash := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(f, hash), 1<<20)
	stats := &extraction{Modes: map[string]uint64{}, Lengths: map[int]uint64{}, Ops: map[string]uint64{}}
	rows := make([]sampleRow, 0, limit)
	start := time.Now()
	last := time.Time{}
	for ok := it.First(); ok && len(rows) < limit; ok = it.Next() {
		k, v := it.Key(), it.Value()
		stats.Scanned++
		stats.Processed += uint64(len(k) + len(v))
		if maxScan > 0 && stats.Scanned > maxScan {
			return nil, stats, fmt.Errorf("scanned row safety bound reached before cohort full")
		}
		if len(k) != common.HashLength {
			_, _, domain, _, valid := rawdb.DecodeStateKVLatestKey(k)
			if !valid {
				return nil, stats, fmt.Errorf("bad source KV key")
			}
			if domain == kvdomains.AccountPermissionAux {
				rows, e = addRow(rows, stats, k, v)
				if e != nil {
					return nil, stats, e
				}
				if e = record(writer, k, v); e != nil {
					return nil, stats, e
				}
				stats.Bytes += uint64(8 + len(k) + len(v))
				if len(rows) == limit {
					break
				}
			}
		}
		if stats.Scanned%4096 == 0 {
			if rate > 0 {
				target := time.Duration(float64(stats.Processed) / float64(uint64(rate)<<20) * float64(time.Second))
				if delay := target - time.Since(start); delay > 0 {
					time.Sleep(delay)
				}
			}
			if time.Since(last) >= 5*time.Second {
				last = time.Now()
				if e = writeJSON(filepath.Join(out, "progress.json"), map[string]any{"phase": "short_prefix_extract", "elapsed_sec": time.Since(start).Seconds(), "stats": stats}); e != nil {
					return nil, stats, e
				}
			}
		}
	}
	if e = it.Error(); e != nil {
		return nil, stats, e
	}
	if e = writer.Flush(); e != nil {
		return nil, stats, e
	}
	if e = f.Sync(); e != nil {
		return nil, stats, e
	}
	stats.Hash = hex.EncodeToString(hash.Sum(nil))
	stats.Complete = len(rows) == limit
	if len(rows) == 0 {
		return nil, stats, fmt.Errorf("no permission rows")
	}
	if !stats.Complete {
		return nil, stats, fmt.Errorf("source exhausted before requested bounded cohort")
	}
	return rows, stats, nil
}
func writeSST(path string, rows []sampleRow, candidate bool) (map[string]any, error) {
	f, e := vfs.Default.Create(path)
	if e != nil {
		return nil, e
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression, BlockSize: 4096, IndexBlockSize: 4096, FilterPolicy: bloom.FilterPolicy(10), TableFormat: sstable.TableFormatPebblev4})
	started := time.Now()
	for _, r := range rows {
		v := r.old
		if candidate {
			v = r.candidate
		}
		if e = w.Set(r.key, v); e != nil {
			w.Close()
			return nil, e
		}
	}
	if e = w.Close(); e != nil {
		return nil, e
	}
	elapsed := time.Since(started)
	f2, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f2.Close()
	h := sha256.New()
	n, e := io.Copy(h, f2)
	if e != nil {
		return nil, e
	}
	return map[string]any{"path": path, "bytes": n, "sha256": hex.EncodeToString(h.Sum(nil)), "write_wall_ns": elapsed.Nanoseconds()}, nil
}

type request struct {
	key, native []byte
	owner       common.Address
	present     bool
}

func requests(rows []sampleRow, n int, negative bool) []request {
	n = min(n, len(rows))
	indices := rand.New(rand.NewSource(113177)).Perm(len(rows))[:n]
	q := make([]request, 0, n)
	for _, i := range indices {
		r := rows[i]
		if !negative {
			q = append(q, request{r.key, r.native, r.owner, true})
			continue
		}
		owner, generation, domain, _, _ := rawdb.DecodeStateKVLatestKey(r.key)
		var k []byte
		for id := uint32(0x7fffffff); ; id-- {
			logical := []byte{2, 0, 0, 0, 0}
			binary.BigEndian.PutUint32(logical[1:], id)
			k = rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logical)
			j := sort.Search(len(rows), func(j int) bool { return bytes.Compare(rows[j].key, k) >= 0 })
			if j == len(rows) || !bytes.Equal(rows[j].key, k) {
				break
			}
		}
		q = append(q, request{k, nil, owner, false})
	}
	return q
}
func point(path string, q []request, candidate bool) (map[string]any, error) {
	fs := &readFS{FS: vfs.Default}
	cache := pebble.NewCache(32 << 20)
	defer cache.Unref()
	db, e := pebble.Open(path, options(fs, cache, true))
	if e != nil {
		return nil, e
	}
	defer db.Close()
	out := map[string]any{"operations_per_pass": len(q), "cache_bytes": 32 << 20, "cursor": "Snapshot.NewGetCursorWithOptions(MaxRetainedTables=16,UseL6Filters=true)", "scope": "single cursor for one workload pass; first application cache empty, OS cache uncontrolled; not HTTP"}
	for pass := 0; pass < 2; pass++ {
		calls, n := fs.calls.Load(), fs.bytes.Load()
		filters := db.Metrics().Filter
		snapshot := db.NewSnapshot()
		cursor, e := snapshot.NewGetCursorWithOptions(pebble.GetCursorOptions{MaxRetainedTables: 16, UseL6Filters: true})
		if e != nil {
			snapshot.Close()
			return nil, e
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		for _, r := range q {
			v, err := cursor.Get(r.key)
			found := err == nil
			if err != nil && err != pebble.ErrNotFound {
				cursor.Close()
				snapshot.Close()
				return nil, err
			}
			if found != r.present {
				cursor.Close()
				snapshot.Close()
				return nil, fmt.Errorf("point presence mismatch")
			}
			if found {
				native, e := rawdb.DecodeStateKVLatestValueNoCopy(v)
				if e == nil && candidate {
					native, e = expand(r.owner, native)
				}
				if e != nil || !bytes.Equal(native, r.native) {
					cursor.Close()
					snapshot.Close()
					return nil, fmt.Errorf("point native mismatch: %v", e)
				}
				if e = cursor.ReleaseValue(); e != nil {
					cursor.Close()
					snapshot.Close()
					return nil, e
				}
			}
		}
		if e = cursor.Close(); e != nil {
			snapshot.Close()
			return nil, e
		}
		if e = snapshot.Close(); e != nil {
			return nil, e
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		f := db.Metrics().Filter
		label := "empty_app_cache"
		if pass == 1 {
			label = "warm_app_cache"
		}
		out[label] = map[string]any{"wall_ns": elapsed.Nanoseconds(), "sst_read_calls": fs.calls.Load() - calls, "sst_read_bytes": fs.bytes.Load() - n, "bloom_rejections": f.Hits - filters.Hits, "bloom_may_contain": f.Misses - filters.Misses, "mallocs": after.Mallocs - before.Mallocs, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "cursor_stats": cursor.Stats(), "every_result_verified": true}
	}
	return out, nil
}
func run(source, out string, limit, ops, rate int, maxScan uint64) error {
	resolved, e := filepath.EvalSymlinks(source)
	if e != nil {
		return e
	}
	resolved, e = filepath.Abs(resolved)
	if e != nil {
		return e
	}
	absoluteOut, e := filepath.Abs(out)
	if e != nil {
		return e
	}
	if absoluteOut == resolved || strings.HasPrefix(absoluteOut, resolved+string(os.PathSeparator)) || strings.Contains(filepath.ToSlash(absoluteOut), "/main/datadir/") {
		return fmt.Errorf("output must be outside database and production datadir")
	}
	if e := os.Mkdir(out, 0755); e != nil {
		return e
	}
	start := time.Now()
	rows, stats, e := extract(source, out, limit, maxScan, rate)
	if e != nil {
		return e
	}
	result := map[string]any{"source": source, "cohort": stats, "all_candidate_rows_restore_exact_old_native": true, "physical_keys_unchanged": true, "scope": "short owner-sorted PermissionAux prefix cohort; mixed semantic generations retained; actual fresh Snappy SSTs only, not whole-DB savings; no production/MuHash/history/snapshot-format change", "started_utc": start.UTC().Format(time.RFC3339Nano)}
	arms := map[string]map[string]any{}
	for _, arm := range []string{"baseline", "candidate"} {
		candidate := arm == "candidate"
		path := filepath.Join(out, arm+".sst")
		m, e := writeSST(path, rows, candidate)
		if e != nil {
			return e
		}
		db, e := createDB(path, filepath.Join(out, arm+"-control"))
		if e != nil {
			return e
		}
		m["db"] = db
		arms[arm] = m
	}
	var measurements []map[string]any
	for round := 0; round < 2; round++ {
		order := []string{"baseline", "candidate"}
		if round == 1 {
			order = []string{"candidate", "baseline"}
		}
		for _, negative := range []bool{false, true} {
			q := requests(rows, ops, negative)
			for _, arm := range order {
				m, e := point(arms[arm]["db"].(map[string]any)["db_path"].(string), q, arm == "candidate")
				if e != nil {
					return e
				}
				m["arm"], m["negative"], m["round"] = arm, negative, round
				measurements = append(measurements, m)
			}
		}
	}
	result["arms"] = arms
	result["point_measurements"] = measurements
	result["finished_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	if e = writeJSON(filepath.Join(out, "results.json"), result); e != nil {
		return e
	}
	return writeJSON(filepath.Join(out, "complete.json"), map[string]any{"complete": true, "rows": len(rows), "source_cohort_sha256": stats.Hash, "every_native_restored": true})
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
	source := flag.String("source", "", "immutable maintenance copy /chaindata")
	out := flag.String("out", "", "new independent output")
	limit := flag.Int("rows", 500000, "permission prefix rows")
	ops := flag.Int("point-ops", 10000, "positive and negative lookups per pass")
	rate := flag.Int("logical-mib-sec", 128, "short-prefix input rate ceiling")
	max := flag.Uint64("max-scanned-kv", 10000000, "fail rather than repeat a full KV scan")
	flag.Parse()
	if *source == "" || *out == "" || *limit < 1 || *ops < 1 || *rate < 0 {
		panic("valid source/out/limits required")
	}
	if e := run(*source, *out, *limit, *ops, *rate, *max); e != nil {
		panic(e)
	}
	fmt.Println("PERMISSION_PROBE_COMPLETE")
}
