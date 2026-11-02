// Independent performance control for the frozen ed335 storage-pages cohort.
// This executable never builds/ingests SSTs or mutates either experiment DB.
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
	"hash"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const frozenSource = "ed33556bd99b45067e7712bb2f935ec4177219b3"

var exactComparer = func() *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = func(k []byte) int { return len(k) }
	return &c
}()

type cohort struct {
	Statistics  struct{ SelectedRows uint64 }
	SelectedSHA string                                `json:"selected_rows_framed_sha256"`
	PageCodec   string                                `json:"page_codec"`
	Benchmarks  []struct{ Stats struct{ Count int } } `json:"random_lookup_benchmarks"`
}

type counter struct{ calls, bytes atomic.Uint64 }
type countFS struct {
	vfs.FS
	c *counter
}
type countFile struct {
	vfs.File
	c *counter
}

func (f countFS) Open(name string, o ...vfs.OpenOption) (vfs.File, error) {
	v, e := f.FS.Open(name, o...)
	if e != nil {
		return nil, e
	}
	if strings.HasSuffix(name, ".sst") {
		return countFile{v, f.c}, nil
	}
	return v, nil
}
func (f countFile) ReadAt(p []byte, off int64) (int, error) {
	n, e := f.File.ReadAt(p, off)
	f.c.calls.Add(1)
	f.c.bytes.Add(uint64(n))
	return n, e
}

type opened struct {
	db    *pebble.DB
	cache *pebble.Cache
	c     *counter
}

func openDB(path string, cacheBytes int64) (*opened, error) {
	o := &opened{cache: pebble.NewCache(cacheBytes), c: new(counter)}
	db, e := pebble.Open(path, &pebble.Options{ReadOnly: true, Cache: o.cache, FS: countFS{vfs.Default, o.c}, Comparer: exactComparer, Filters: map[string]pebble.FilterPolicy{bloom.FilterPolicy(10).Name(): bloom.FilterPolicy(10)}, MaxOpenFiles: 128})
	if e != nil {
		o.cache.Unref()
		return nil, e
	}
	o.db = db
	return o, nil
}
func (o *opened) close() error {
	if o == nil {
		return nil
	}
	e := o.db.Close()
	o.cache.Unref()
	return e
}

type scope struct {
	snapshot *pebble.Snapshot
	cursor   *pebble.GetCursor
	iter     *pebble.Iterator
}

func newScope(db *pebble.DB, method string) (*scope, error) {
	s := &scope{snapshot: db.NewSnapshot()}
	var e error
	switch method {
	case "production_exact":
		s.cursor, e = s.snapshot.NewGetCursorWithOptions(pebble.GetCursorOptions{MaxRetainedTables: 16, UseL6Filters: true})
	case "pages_range":
		s.iter, e = s.snapshot.NewIter(&pebble.IterOptions{KeyTypes: pebble.IterKeyTypePointsOnly})
	case "ordinary_get_control":
	default:
		e = fmt.Errorf("unknown method %q", method)
	}
	if e != nil {
		_ = s.snapshot.Close()
		return nil, e
	}
	return s, nil
}
func (s *scope) get(db *pebble.DB, key []byte) ([]byte, bool, error) {
	if s.cursor != nil {
		v, e := s.cursor.Get(key)
		if e == pebble.ErrNotFound {
			return nil, false, nil
		}
		if e != nil {
			return nil, false, e
		}
		v = bytes.Clone(v)
		e = s.cursor.ReleaseValue()
		return v, true, e
	}
	if s.iter != nil {
		if !s.iter.SeekGE(key) {
			return nil, false, s.iter.Error()
		}
		v, ok, e := lookupPage(s.iter.Key(), s.iter.Value(), key)
		return bytes.Clone(v), ok, e
	}
	v, c, e := db.Get(key)
	if e == pebble.ErrNotFound {
		return nil, false, nil
	}
	if e != nil {
		return nil, false, e
	}
	v = bytes.Clone(v)
	e = c.Close()
	return v, true, e
}
func (s *scope) close() (pebble.GetCursorStats, error) {
	var st pebble.GetCursorStats
	var e error
	if s.cursor != nil {
		st = s.cursor.Stats()
		e = s.cursor.Close()
	}
	if s.iter != nil {
		e = errors.Join(e, s.iter.Close())
	}
	return st, errors.Join(e, s.snapshot.Close())
}

type distribution struct {
	Count                              int `json:"count"`
	MeanMS, P50MS, P95MS, P99MS, MaxMS float64
}

func dist(times []float64) distribution {
	d := distribution{Count: len(times)}
	if len(times) == 0 {
		return d
	}
	v := append([]float64(nil), times...)
	sort.Float64s(v)
	for _, x := range v {
		d.MeanMS += x
	}
	d.MeanMS /= float64(len(v))
	d.P50MS = v[(len(v)-1)/2]
	d.P95MS = v[int(float64(len(v)-1)*.95)]
	d.P99MS = v[int(float64(len(v)-1)*.99)]
	d.MaxMS = v[len(v)-1]
	return d
}
func cpuMS() float64 {
	var r syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return float64(r.Utime.Sec+r.Stime.Sec)*1000 + float64(r.Utime.Usec+r.Stime.Usec)/1000
}
func ms(t time.Duration) float64 { return float64(t.Nanoseconds()) / 1e6 }

type result struct {
	Method, CacheMode                                                              string
	ScopeSize, Round, OrderPosition, Queries, Hits, Misses, Scopes                 int
	Query, FirstQuery, FollowingQueries, ScopeLifecycle, HitQuery, MissQuery       distribution
	ScopeWallTotalMS, QueryWallTotalMS, ScopeCPUms, QueryLoopCPUms                 float64
	SSTReadAtCalls, SSTReadBytes, DBOpenReadAtCalls, DBOpenReadBytes               uint64
	HitSSTReadAtCalls, HitSSTReadBytes, MissSSTReadAtCalls, MissSSTReadBytes       uint64
	CursorTableRequests, CursorTableOpens, CursorTableReuses, CursorTableFallbacks uint64
	CursorPeakRetained                                                             uint64
	OutputSHA256                                                                   string
	QueriesVerified                                                                bool
	LevelFiles                                                                     []int64
}

func addReply(h hash.Hash, key, value []byte, found bool) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(key)))
	_, _ = h.Write(b[:])
	_, _ = h.Write(key)
	tag := byte(0)
	if found {
		tag = 1
	}
	_, _ = h.Write([]byte{tag})
	binary.BigEndian.PutUint32(b[:], uint32(len(value)))
	_, _ = h.Write(b[:])
	_, _ = h.Write(value)
}
func run(path, method string, queries []row, scopeSize int, cold bool, cacheBytes int64) (out result, err error) {
	out.Method = method
	out.ScopeSize = scopeSize
	out.CacheMode = "bounded Pebble cache after one same-protocol warm-up; filesystem warm"
	if cold {
		out.CacheMode = "per-scope experiment-SST FADV_DONTNEED + fresh Pebble cache; DB-open excluded; OS advice is not a device-cache reset"
	}
	var o *opened
	defer func() {
		if e := o.close(); err == nil {
			err = e
		}
	}()
	open := func() error {
		var e error
		o, e = openDB(path, cacheBytes)
		if e != nil {
			return e
		}
		out.DBOpenReadAtCalls += o.c.calls.Load()
		out.DBOpenReadBytes += o.c.bytes.Load()
		if out.LevelFiles == nil {
			m := o.db.Metrics()
			for _, l := range m.Levels {
				out.LevelFiles = append(out.LevelFiles, l.NumFiles)
			}
			if method == "production_exact" && out.LevelFiles[6] == 0 {
				return errors.New("production L6 control requires actual L6 experiment SSTs")
			}
		}
		return nil
	}
	if !cold {
		if err = open(); err != nil {
			return out, err
		}
		for lo := 0; lo < len(queries); lo += scopeSize {
			hi := min(lo+scopeSize, len(queries))
			s, e := newScope(o.db, method)
			if e != nil {
				return out, e
			}
			for _, q := range queries[lo:hi] {
				_, _, e = s.get(o.db, q.key)
				if e != nil {
					_, _ = s.close()
					return out, e
				}
			}
			if _, e = s.close(); e != nil {
				return out, e
			}
		}
	}
	var qt, first, follow, hitTimes, missTimes, scopes []float64
	reply := sha256.New()
	for lo := 0; lo < len(queries); lo += scopeSize {
		hi := min(lo+scopeSize, len(queries))
		if cold {
			previous := o
			o = nil
			if e := previous.close(); e != nil {
				return out, e
			}
			if e := evictSST(path); e != nil {
				return out, e
			}
			if err = open(); err != nil {
				return out, err
			}
		}
		callsBefore, bytesBefore := o.c.calls.Load(), o.c.bytes.Load()
		scopeCPU := cpuMS()
		scopeStart := time.Now()
		s, e := newScope(o.db, method)
		if e != nil {
			return out, e
		}
		queryCPU := cpuMS()
		for i, q := range queries[lo:hi] {
			a, b := o.c.calls.Load(), o.c.bytes.Load()
			t := time.Now()
			v, ok, e := s.get(o.db, q.key)
			elapsed := ms(time.Since(t))
			if e != nil {
				_, _ = s.close()
				return out, e
			}
			if ok != (q.value != nil) || ok && !bytes.Equal(v, q.value) {
				_, _ = s.close()
				return out, fmt.Errorf("%s query %d returned different presence/value", method, lo+i)
			}
			qt = append(qt, elapsed)
			out.QueryWallTotalMS += elapsed
			if i == 0 {
				first = append(first, elapsed)
			} else {
				follow = append(follow, elapsed)
			}
			if ok {
				out.Hits++
				hitTimes = append(hitTimes, elapsed)
				out.HitSSTReadAtCalls += o.c.calls.Load() - a
				out.HitSSTReadBytes += o.c.bytes.Load() - b
			} else {
				out.Misses++
				missTimes = append(missTimes, elapsed)
				out.MissSSTReadAtCalls += o.c.calls.Load() - a
				out.MissSSTReadBytes += o.c.bytes.Load() - b
			}
			addReply(reply, q.key, v, ok)
			out.Queries++
		}
		out.QueryLoopCPUms += cpuMS() - queryCPU
		st, e := s.close()
		elapsed := ms(time.Since(scopeStart))
		out.ScopeWallTotalMS += elapsed
		out.ScopeCPUms += cpuMS() - scopeCPU
		out.SSTReadAtCalls += o.c.calls.Load() - callsBefore
		out.SSTReadBytes += o.c.bytes.Load() - bytesBefore
		if e != nil {
			return out, e
		}
		out.Scopes++
		scopes = append(scopes, elapsed)
		out.CursorTableRequests += st.TableRequests
		out.CursorTableOpens += st.TableOpens
		out.CursorTableReuses += st.TableReuses
		out.CursorTableFallbacks += st.TableFallbacks
		out.CursorPeakRetained = max(out.CursorPeakRetained, st.MaxRetained)
	}
	out.Query = dist(qt)
	out.FirstQuery = dist(first)
	out.FollowingQueries = dist(follow)
	out.HitQuery = dist(hitTimes)
	out.MissQuery = dist(missTimes)
	out.ScopeLifecycle = dist(scopes)
	out.OutputSHA256 = hex.EncodeToString(reply.Sum(nil))
	out.QueriesVerified = true
	return out, nil
}

func readRow(r io.Reader) (row, error) {
	var b [4]byte
	if _, e := io.ReadFull(r, b[:]); e != nil {
		return row{}, e
	}
	n := binary.BigEndian.Uint32(b[:])
	if n < 1 || n > 1024 {
		return row{}, errors.New("invalid key length")
	}
	k := make([]byte, n)
	if _, e := io.ReadFull(r, k); e != nil {
		return row{}, e
	}
	if _, e := io.ReadFull(r, b[:]); e != nil {
		return row{}, e
	}
	n = binary.BigEndian.Uint32(b[:])
	if n < 1 || n > 65535 {
		return row{}, errors.New("invalid wrapped value length")
	}
	v := make([]byte, n)
	_, e := io.ReadFull(r, v)
	return row{k, v}, e
}
func frame(h io.Writer, r row) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(r.key)))
	_, _ = h.Write(b[:])
	_, _ = h.Write(r.key)
	binary.BigEndian.PutUint32(b[:], uint32(len(r.value)))
	_, _ = h.Write(b[:])
	_, _ = h.Write(r.value)
}
func sample(input, baseline string, c cohort, n int) ([]row, string, error) {
	f, e := os.Open(input)
	if e != nil {
		return nil, "", e
	}
	defer f.Close()
	b := bufio.NewReaderSize(f, 4<<20)
	rng := rand.New(rand.NewSource(417))
	h := sha256.New()
	var samples []row
	var previous []byte
	for i := uint64(1); i <= c.Statistics.SelectedRows; i++ {
		r, e := readRow(b)
		if e != nil {
			return nil, "", e
		}
		_, _, d, key, ok := rawdb.DecodeStateKVLatestKey(r.key)
		if !ok || d != kvdomains.ContractStorage || len(key) != 32 {
			return nil, "", fmt.Errorf("input row %d not full ContractStorage key", i)
		}
		if e = rawdb.ValidateStateKVLatestValue(r.value); e != nil {
			return nil, "", e
		}
		if previous != nil && bytes.Compare(previous, r.key) >= 0 {
			return nil, "", errors.New("input not strictly sorted")
		}
		previous = append(previous[:0], r.key...)
		frame(h, r)
		if len(samples) < n {
			samples = append(samples, r)
		} else if j := rng.Uint64() % i; j < uint64(len(samples)) {
			samples[j] = r
		}
	}
	sha := hex.EncodeToString(h.Sum(nil))
	if sha != c.SelectedSHA {
		return nil, sha, errors.New("cohort row count/input framed SHA does not match frozen experiment; owner-filtered cohorts need a matching selected export")
	}
	o, e := openDB(baseline, 8<<20)
	if e != nil {
		return nil, sha, e
	}
	defer o.close()
	s, e := newScope(o.db, "production_exact")
	if e != nil {
		return nil, sha, e
	}
	defer s.close()
	queries := make([]row, 0, len(samples)*2)
	for _, q := range samples {
		v, ok, e := s.get(o.db, q.key)
		if e != nil || !ok || !bytes.Equal(v, q.value) {
			return nil, sha, fmt.Errorf("baseline DB disagrees with input sample: %v", e)
		}
		queries = append(queries, q)
		k := bytes.Clone(q.key)
		k[len(k)-1] ^= 0x5a
		v, ok, e = s.get(o.db, k)
		if e != nil {
			return nil, sha, e
		}
		if !ok {
			v = nil
		}
		queries = append(queries, row{k, v})
	}
	rng.Shuffle(len(queries), func(i, j int) { queries[i], queries[j] = queries[j], queries[i] })
	return queries, sha, nil
}

func main() {
	if e := mainErr(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func mainErr() error {
	root := flag.String("root", "", "existing frozen storage-results root with baseline/db and pages/db")
	input := flag.String("input", "", "same source stream used to create cohort")
	out := flag.String("out", "", "new output directory, must not exist")
	queriesFlag := flag.Int("queries", 0, "reservoir size; 0 reuses frozen result count")
	coldN := flag.Int("cold-queries", 128, "same selected query prefix in OS-cold controls; 0 disables")
	rounds := flag.Int("rounds", 2, "even paired rounds: AB then BA by default")
	cacheMiB := flag.Int64("cache-mib", 8, "equal bounded Pebble block cache per variant")
	ordinary := flag.Bool("ordinary-get-control", true, "also retain ordinary DB.Get as an explicitly non-production control")
	flag.Parse()
	if *root == "" || *input == "" || *out == "" || *rounds < 2 || *rounds%2 != 0 || *cacheMiB < 1 || *coldN < 0 {
		return errors.New("invalid required paths/paired rounds/cache/cold count")
	}
	if _, e := os.Stat(*out); !os.IsNotExist(e) {
		return errors.New("output must be new")
	}
	data, e := os.ReadFile(filepath.Join(*root, "result.json"))
	if e != nil {
		return e
	}
	var c cohort
	if e = json.Unmarshal(data, &c); e != nil {
		return e
	}
	if !strings.HasPrefix(c.PageCodec, "SPG2") || c.Statistics.SelectedRows == 0 || c.SelectedSHA == "" {
		return errors.New("not a supported frozen SPG2 cohort")
	}
	n := *queriesFlag
	if n == 0 {
		if len(c.Benchmarks) == 0 || c.Benchmarks[0].Stats.Count%2 != 0 {
			return errors.New("frozen query count missing")
		}
		n = c.Benchmarks[0].Stats.Count / 2
	}
	if n < 1 || n > 65536 {
		return errors.New("query reservoir must be in 1..65536")
	}
	baseline := filepath.Join(*root, "baseline", "db")
	pages := filepath.Join(*root, "pages", "db")
	for _, p := range []string{baseline, pages} {
		if _, e = os.Stat(filepath.Join(p, "CURRENT")); e != nil {
			return fmt.Errorf("existing experiment DB unavailable %s: %w", p, e)
		}
	}
	q, inputSHA, e := sample(*input, baseline, c, n)
	if e != nil {
		return e
	}
	if e = os.Mkdir(*out, 0755); e != nil {
		return e
	}
	qb := new(bytes.Buffer)
	for _, r := range q {
		frame(qb, r)
	}
	if e = os.WriteFile(filepath.Join(*out, "queries.rows"), qb.Bytes(), 0644); e != nil {
		return e
	}
	meta := map[string]any{"version": "cursor-control-v2", "frozen_storage_source": frozenSource, "existing_root": *root, "input": *input, "selected_rows": c.Statistics.SelectedRows, "selected_rows_framed_sha256": inputSHA, "query_stream_sha256": fmt.Sprintf("%x", sha256.Sum256(qb.Bytes())), "query_count": len(q), "reservoir_size": n, "seed": 417, "cache_mib": *cacheMiB, "rounds": *rounds, "GOOS": runtime.GOOS, "runtime": runtime.Version(), "production_method": "Snapshot.GetCursor(MaxRetainedTables=16,UseL6Filters=true), full-key comparer Split=len(key)", "candidate_method": "Snapshot iterator ordinary range SeekGE + bounded SPG2 decode; never SeekPrefixGE", "scope": "same existing SSTs; no SST rewrite, production mutation, HTTP execution or device-cache reset; fresh1/group32 include snapshot+cursor lifecycle, DB-open excluded; query CPU includes loop/timer/hash verification overhead"}
	var results []result
	for _, cold := range []bool{false, true} {
		if cold && *coldN == 0 {
			continue
		}
		selected := q
		if cold && len(selected) > *coldN {
			selected = selected[:*coldN]
		}
		for _, group := range []int{1, 32} {
			for round := 0; round < *rounds; round++ {
				methods := []string{"production_exact", "pages_range"}
				if round%2 != 0 {
					methods[0], methods[1] = methods[1], methods[0]
				}
				if *ordinary {
					methods = append(methods, "ordinary_get_control")
				}
				var expectedSHA string
				for order, method := range methods {
					path := baseline
					if method == "pages_range" {
						path = pages
					}
					r, e := run(path, method, selected, group, cold, *cacheMiB<<20)
					if e != nil {
						return e
					}
					r.Round = round
					r.OrderPosition = order
					if expectedSHA == "" {
						expectedSHA = r.OutputSHA256
					} else if r.OutputSHA256 != expectedSHA {
						return errors.New("paired result hashes differ")
					}
					results = append(results, r)
					fmt.Printf("CURSOR_CONTROL_PROGRESS cold=%v scope=%d round=%d method=%s query_p50_ms=%.6f read_bytes=%d\n", cold, group, round, method, r.Query.P50MS, r.SSTReadBytes)
					partial := map[string]any{"metadata": meta, "complete": false, "results": results}
					b, _ := json.MarshalIndent(partial, "", "  ")
					if e = os.WriteFile(filepath.Join(*out, "progress.json"), b, 0644); e != nil {
						return e
					}
				}
			}
		}
	}
	meta["results"] = results
	meta["complete"] = true
	meta["finished_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	b, e := json.MarshalIndent(meta, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(*out, "result.json"), append(b, '\n'), 0644); e != nil {
		return e
	}
	fmt.Printf("CURSOR_CONTROL_COMPLETE %s\n", filepath.Join(*out, "result.json"))
	return nil
}
