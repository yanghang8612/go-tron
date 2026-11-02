package main

import (
	"bufio"
	"bytes"
	"container/heap"
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
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type ownerStat struct {
	Owner                                     string `json:"owner"`
	Rows, PhysicalKeyBytes, WrappedValueBytes uint64
}
type ownerHeap []ownerStat

func (h ownerHeap) Len() int           { return len(h) }
func (h ownerHeap) Less(i, j int) bool { return h[i].Rows < h[j].Rows }
func (h ownerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *ownerHeap) Push(v any)        { *h = append(*h, v.(ownerStat)) }
func (h *ownerHeap) Pop() any          { n := len(*h); v := (*h)[n-1]; *h = (*h)[:n-1]; return v }

type stats struct {
	SourceRows, SelectedRows, PhysicalKeyBytes, WrappedValueBytes uint64
	PresentZeroWords, AllOnesWords                                uint64
	LogicalValueLengths, WrappedValueLengths                      map[int]uint64
	TopOwners                                                     []ownerStat
}
type fileStat struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}
type builder struct {
	dir                                string
	compression                        pebble.Compression
	blockSize                          int
	minPageRows                        int
	pageBudget                         int
	pendingUpperBytes                  int
	index                              int
	base, candidate                    *sstable.Writer
	pending                            []row
	files                              [2][]fileStat
	pages, dictPages, pagePayloadBytes uint64
	restored                           hash.Hash
}

func (w *builder) open() error {
	for i, name := range []string{"baseline", "pages"} {
		d := filepath.Join(w.dir, name)
		if e := os.MkdirAll(d, 0755); e != nil {
			return e
		}
		p := filepath.Join(d, fmt.Sprintf("%06d.sst", w.index))
		f, e := vfs.Default.Create(p)
		if e != nil {
			return e
		}
		x := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: w.compression, BlockSize: w.blockSize, BlockRestartInterval: 16, FilterPolicy: bloom.FilterPolicy(10), FilterType: sstable.TableFilter})
		if i == 0 {
			w.base = x
		} else {
			w.candidate = x
		}
		w.files[i] = append(w.files[i], fileStat{Path: p})
	}
	return nil
}
func framed(h io.Writer, r row) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(r.key)))
	_, _ = h.Write(b[:])
	_, _ = h.Write(r.key)
	binary.BigEndian.PutUint32(b[:], uint32(len(r.value)))
	_, _ = h.Write(b[:])
	_, _ = h.Write(r.value)
}
func (w *builder) flushPage() error {
	if len(w.pending) == 0 {
		return nil
	}
	if len(w.pending) < w.minPageRows {
		for _, r := range w.pending {
			if e := w.candidate.Set(r.key, r.value); e != nil {
				return e
			}
			framed(w.restored, r)
		}
		w.pending = w.pending[:0]
		w.pendingUpperBytes = 0
		return nil
	}
	b, e := encodePage(w.pending)
	if e != nil {
		return e
	}
	last := w.pending[len(w.pending)-1].key
	if e = w.candidate.Set(last, b); e != nil {
		return e
	}
	p, e := parsePage(b)
	if e != nil {
		return e
	}
	prefix := last[:len(last)-p.width]
	for i := 0; i < p.n; i++ {
		k := append(append([]byte(nil), prefix...), p.keys[i*p.width:(i+1)*p.width]...)
		v := p.value(i)
		if !bytes.Equal(k, w.pending[i].key) || !bytes.Equal(v, w.pending[i].value) {
			return errors.New("page round trip differs from input")
		}
		framed(w.restored, row{k, v})
	}
	w.pages++
	if len(p.ids) > 0 {
		w.dictPages++
	}
	w.pagePayloadBytes += uint64(len(b))
	w.pending = w.pending[:0]
	w.pendingUpperBytes = 0
	return nil
}
func (w *builder) add(r row, pageRows int) error {
	if len(w.pending) > 0 && (len(w.pending) >= pageRows || pageHeader+2+w.pendingUpperBytes+18+len(r.value) > w.pageBudget || !bytes.Equal(w.pending[0].key[:len(r.key)-storageTail], r.key[:len(r.key)-storageTail])) {
		if e := w.flushPage(); e != nil {
			return e
		}
	}
	if e := w.base.Set(r.key, r.value); e != nil {
		return e
	}
	w.pending = append(w.pending, r)
	w.pendingUpperBytes += 18 + len(r.value)
	return nil
}
func (w *builder) closeShard() error {
	if e := w.flushPage(); e != nil {
		return e
	}
	for i, x := range []*sstable.Writer{w.base, w.candidate} {
		if e := x.Close(); e != nil {
			return e
		}
		j := len(w.files[i]) - 1
		s, e := os.Stat(w.files[i][j].Path)
		if e != nil {
			return e
		}
		w.files[i][j].Bytes = s.Size()
	}
	w.index++
	return nil
}

func readRow(r io.Reader) (row, error) {
	var n [4]byte
	_, e := io.ReadFull(r, n[:])
	if e != nil {
		return row{}, e
	}
	k := binary.BigEndian.Uint32(n[:])
	if k == 0 || k > 1024 {
		return row{}, fmt.Errorf("invalid physical key length %d", k)
	}
	key := make([]byte, k)
	if _, e = io.ReadFull(r, key); e != nil {
		return row{}, e
	}
	if _, e = io.ReadFull(r, n[:]); e != nil {
		return row{}, e
	}
	v := binary.BigEndian.Uint32(n[:])
	if v > 65535 {
		return row{}, fmt.Errorf("oversized value %d", v)
	}
	val := make([]byte, v)
	_, e = io.ReadFull(r, val)
	return row{key, val}, e
}

type readCounter struct{ calls, bytes atomic.Uint64 }
type countedFS struct {
	vfs.FS
	count *readCounter
}
type countedFile struct {
	vfs.File
	count *readCounter
}

func (f countedFile) ReadAt(p []byte, off int64) (int, error) {
	n, e := f.File.ReadAt(p, off)
	f.count.calls.Add(1)
	f.count.bytes.Add(uint64(n))
	return n, e
}
func (f countedFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	v, e := f.FS.Open(name, opts...)
	if e != nil {
		return nil, e
	}
	if strings.HasSuffix(name, ".sst") {
		return countedFile{v, f.count}, nil
	}
	return v, nil
}

type quantiles struct {
	Count                                     int
	P50MS, P95MS, P99MS, MaxMS, MeanMS, CPUms float64
	SSTReadAtCalls, SSTReadBytes              uint64
}
type benchResult struct {
	Variant, CacheMode, ReadMethod string
	Hits, Misses, ReturnedBytes    int
	Stats                          quantiles
	SourceFileEvictionError        string `json:",omitempty"`
}

func summarize(samples []float64, cpu float64, calls, readBytes uint64) quantiles {
	sort.Float64s(samples)
	q := quantiles{Count: len(samples), CPUms: cpu, SSTReadAtCalls: calls, SSTReadBytes: readBytes}
	if len(samples) == 0 {
		return q
	}
	total := 0.0
	for _, v := range samples {
		total += v
	}
	at := func(p float64) float64 { i := int(float64(len(samples)-1) * p); return samples[i] }
	q.P50MS = at(.5)
	q.P95MS = at(.95)
	q.P99MS = at(.99)
	q.MaxMS = samples[len(samples)-1]
	q.MeanMS = total / float64(len(samples))
	return q
}
func cpuTime() float64 {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0
	}
	return float64(r.Utime.Sec+r.Stime.Sec)*1000 + float64(r.Utime.Usec+r.Stime.Usec)/1000
}

func runBench(dir, variant string, queries []row, cold bool, cacheBytes int64) (out benchResult, err error) {
	out.Variant = variant
	out.CacheMode = "warm filesystem, bounded Pebble cache"
	out.ReadMethod = "Pebble.Get"
	if variant == "pages" {
		out.ReadMethod = "bounded-page SeekGE + binary-search tail"
	}
	if cold {
		out.CacheMode = "per-query SST fadvise(DONTNEED), fresh Pebble cache; DB-open time excluded"
	}
	count := new(readCounter)
	var db *pebble.DB
	var cache *pebble.Cache
	open := func() error {
		cache = pebble.NewCache(cacheBytes)
		var e error
		db, e = pebble.Open(dir, &pebble.Options{ReadOnly: true, Cache: cache, FS: countedFS{vfs.Default, count}, Filters: map[string]pebble.FilterPolicy{bloom.FilterPolicy(10).Name(): bloom.FilterPolicy(10)}})
		if e != nil {
			cache.Unref()
		}
		return e
	}
	close := func() error {
		if db == nil {
			return nil
		}
		e := db.Close()
		cache.Unref()
		db = nil
		return e
	}
	defer func() {
		if e := close(); err == nil {
			err = e
		}
	}()
	read := func(key []byte) ([]byte, bool, error) {
		if variant == "baseline" {
			v, closer, e := db.Get(key)
			if errors.Is(e, pebble.ErrNotFound) {
				return nil, false, nil
			}
			if e != nil {
				return nil, false, e
			}
			v = append([]byte(nil), v...)
			e = closer.Close()
			return v, true, e
		}
		it, e := db.NewIter(&pebble.IterOptions{})
		if e != nil {
			return nil, false, e
		}
		defer it.Close()
		if !it.SeekGE(key) {
			return nil, false, it.Error()
		}
		v, ok, e := lookupPage(it.Key(), it.Value(), key)
		return append([]byte(nil), v...), ok, e
	}
	if !cold {
		if err = open(); err != nil {
			return out, err
		}
		for _, q := range queries {
			if _, _, err = read(q.key); err != nil {
				return out, err
			}
		}
	}
	var times []float64
	cpu := 0.0
	var readCalls, readBytes uint64
	for _, q := range queries {
		if cold {
			if err = close(); err != nil {
				return out, err
			}
			if err = evictSST(dir); err != nil {
				out.SourceFileEvictionError = err.Error()
				return out, err
			}
			if err = open(); err != nil {
				return out, err
			}
		}
		a, b := count.calls.Load(), count.bytes.Load()
		c := cpuTime()
		start := time.Now()
		v, ok, e := read(q.key)
		elapsed := time.Since(start)
		cpu += cpuTime() - c
		readCalls += count.calls.Load() - a
		readBytes += count.bytes.Load() - b
		if e != nil {
			return out, e
		}
		if ok != (q.value != nil) || ok && !bytes.Equal(v, q.value) {
			return out, fmt.Errorf("%s query returned different bytes/presence", variant)
		}
		if ok {
			out.Hits++
			out.ReturnedBytes += len(v)
		} else {
			out.Misses++
		}
		times = append(times, float64(elapsed.Nanoseconds())/1e6)
	}
	out.Stats = summarize(times, cpu, readCalls, readBytes)
	return out, nil
}

func ingest(dir string, files []fileStat) error {
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	db, e := pebble.Open(dir, &pebble.Options{Cache: cache, DisableAutomaticCompactions: true, Filters: map[string]pebble.FilterPolicy{bloom.FilterPolicy(10).Name(): bloom.FilterPolicy(10)}})
	if e != nil {
		return e
	}
	paths := make([]string, len(files))
	for i, x := range files {
		paths[i] = x.Path
	}
	if e = db.Ingest(paths); e != nil {
		_ = db.Close()
		return e
	}
	return db.Close()
}

func allocatedTree(dir string) (int64, error) {
	seen := map[[2]uint64]bool{}
	var total int64
	e := filepath.Walk(dir, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		s, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("stat allocation unavailable")
		}
		id := [2]uint64{uint64(s.Dev), uint64(s.Ino)}
		if !seen[id] {
			seen[id] = true
			total += s.Blocks * 512
		}
		return nil
	})
	return total, e
}

func fixture(file string, n int) error {
	f, e := os.Create(file)
	if e != nil {
		return e
	}
	defer f.Close()
	buf := bufio.NewWriterSize(f, 1<<20)
	db := newKeyCapture()
	rng := rand.New(rand.NewSource(417))
	var owner common.Address
	owner[0] = 0x41
	var slot [32]byte
	var group []row
	flush := func() {
		sort.Slice(group, func(i, j int) bool { return bytes.Compare(group[i].key, group[j].key) < 0 })
		for _, r := range group {
			framed(buf, r)
		}
		group = group[:0]
	}
	for i := 0; i < n; i++ {
		if i > 0 && i%4096 == 0 {
			flush()
		}
		binary.BigEndian.PutUint64(owner[13:], uint64(i/4096))
		var counter [8]byte
		binary.BigEndian.PutUint64(counter[:], uint64(i%4096))
		tail := sha256.Sum256(counter[:])
		copy(slot[16:], tail[16:])
		var v [32]byte
		switch i % 4 {
		case 0:
			for j := range v {
				v[j] = 255
			}
		case 1:
			binary.BigEndian.PutUint64(v[24:], uint64(i*99))
		default:
			_, _ = rng.Read(v[:])
		}
		if e = rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, slot[:], v[:]); e != nil {
			return e
		}
		group = append(group, row{append([]byte(nil), db.key...), append([]byte(nil), db.value...)})
	}
	flush()
	if e = buf.Flush(); e != nil {
		return e
	}
	return f.Sync()
}

type keyCapture struct{ key, value []byte }

func newKeyCapture() *keyCapture { return new(keyCapture) }
func (k *keyCapture) Put(a, b []byte) error {
	k.key = append(k.key[:0], a...)
	k.value = append(k.value[:0], b...)
	return nil
}
func (*keyCapture) Delete([]byte) error { return nil }

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	in := flag.String("input", "", "u32 BE physical-key length/key/value length/wrapped-value stream")
	out := flag.String("out", "", "new result directory; must not exist")
	limit := flag.Uint64("limit-rows", 0, "selected row cap, 0 is full input")
	owners := flag.String("owners", "", "comma-separated full TRON owner hex addresses; empty selects all")
	pageRows := flag.Int("page-rows", 128, "maximum rows per bounded page (1..128)")
	minPageRows := flag.Int("min-page-rows", 8, "smaller groups keep exact ordinary row encoding")
	blockSize := flag.Int("block-size", 4096, "equal SST block size")
	compression := flag.String("compression", "snappy", "none, snappy, zstd")
	shardBytes := flag.Uint64("shard-input-bytes", 64<<20, "rotate both versions after the same canonical input-byte cut")
	nQueries := flag.Int("queries", 2048, "bounded reservoir of random hit/miss query samples")
	coldQueries := flag.Int("cold-queries", 32, "OS-cold Linux samples; 0 disables")
	fixtureRows := flag.Int("fixture-rows", 0, "write a deterministic synthetic stream to -input and exit")
	statsOnly := flag.Bool("stats-only", false, "scan distributions without SST construction")
	flag.Parse()
	if *in == "" {
		return errors.New("-input required")
	}
	if *fixtureRows > 0 {
		return fixture(*in, *fixtureRows)
	}
	if *out == "" || *pageRows < 1 || *pageRows > 128 || *minPageRows < 1 || *minPageRows > *pageRows || *nQueries < 1 || *blockSize < 256 || *shardBytes < 1<<20 {
		return errors.New("invalid out/page/query/shard settings")
	}
	if _, e := os.Stat(*out); !os.IsNotExist(e) {
		return fmt.Errorf("output must be new: %s", *out)
	}
	if e := os.MkdirAll(*out, 0755); e != nil {
		return e
	}
	cmp := pebble.SnappyCompression
	switch *compression {
	case "none":
		cmp = pebble.NoCompression
	case "snappy":
	case "zstd":
		cmp = pebble.ZstdCompression
	default:
		return errors.New("unsupported compression")
	}
	selected := map[common.Address]bool{}
	for _, s := range strings.Split(*owners, ",") {
		s = strings.TrimPrefix(strings.TrimSpace(s), "0x")
		if s == "" {
			continue
		}
		b, e := hex.DecodeString(s)
		if e != nil || len(b) != 21 {
			return fmt.Errorf("invalid owner %q", s)
		}
		var a common.Address
		copy(a[:], b)
		selected[a] = true
	}
	f, e := os.Open(*in)
	if e != nil {
		return e
	}
	defer f.Close()
	sourceStat, e := f.Stat()
	if e != nil {
		return e
	}
	reader := bufio.NewReaderSize(f, 4<<20)
	sourceHash := sha256.New()
	restored := sha256.New()
	s := stats{LogicalValueLengths: map[int]uint64{}, WrappedValueLengths: map[int]uint64{}}
	var currentOwner common.Address
	var currentOwnerStat *ownerStat
	var topOwners ownerHeap
	flushOwner := func() {
		if currentOwnerStat == nil {
			return
		}
		if len(topOwners) < 100 {
			heap.Push(&topOwners, *currentOwnerStat)
		} else if currentOwnerStat.Rows > topOwners[0].Rows {
			topOwners[0] = *currentOwnerStat
			heap.Fix(&topOwners, 0)
		}
	}
	rng := rand.New(rand.NewSource(417))
	var samples []row
	var previous []byte
	w := builder{dir: *out, compression: cmp, blockSize: *blockSize, restored: restored, minPageRows: *minPageRows, pageBudget: *blockSize - 96}
	opened := false
	shardInput := uint64(0)
	start := time.Now()
	cpuStart := cpuTime()
	for {
		r, e := readRow(reader)
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		s.SourceRows++
		owner, _, domain, logical, ok := rawdb.DecodeStateKVLatestKey(r.key)
		if !ok || domain != kvdomains.ContractStorage || len(logical) != 32 {
			return fmt.Errorf("source row %d is not a complete ContractStorage key", s.SourceRows)
		}
		if previous != nil && bytes.Compare(previous, r.key) >= 0 {
			return errors.New("input not strictly sorted")
		}
		previous = append(previous[:0], r.key...)
		if len(selected) > 0 && !selected[owner] {
			continue
		}
		value, e := rawdb.DecodeStateKVLatestValue(r.value)
		if e != nil || len(value) > 32 {
			return fmt.Errorf("source row %d invalid wrapped word: %v length %d", s.SourceRows, e, len(value))
		}
		s.SelectedRows++
		s.PhysicalKeyBytes += uint64(len(r.key))
		s.WrappedValueBytes += uint64(len(r.value))
		s.LogicalValueLengths[len(value)]++
		s.WrappedValueLengths[len(r.value)]++
		if bytes.Equal(value, make([]byte, 32)) {
			s.PresentZeroWords++
		}
		if bytes.Equal(value, bytes.Repeat([]byte{255}, 32)) {
			s.AllOnesWords++
		}
		if currentOwnerStat == nil || currentOwner != owner {
			flushOwner()
			currentOwner = owner
			currentOwnerStat = &ownerStat{Owner: owner.Hex()}
		}
		a := currentOwnerStat
		a.Rows++
		a.PhysicalKeyBytes += uint64(len(r.key))
		a.WrappedValueBytes += uint64(len(r.value))
		framed(sourceHash, r)
		if len(samples) < *nQueries {
			samples = append(samples, r)
		} else if j := rng.Uint64() % s.SelectedRows; j < uint64(len(samples)) {
			samples[j] = r
		}
		if !*statsOnly {
			if !opened {
				if e = w.open(); e != nil {
					return e
				}
				opened = true
			}
			if e = w.add(r, *pageRows); e != nil {
				return e
			}
			shardInput += uint64(8 + len(r.key) + len(r.value))
			if shardInput >= *shardBytes {
				if e = w.closeShard(); e != nil {
					return e
				}
				opened = false
				shardInput = 0
			}
		}
		if *limit > 0 && s.SelectedRows >= *limit {
			break
		}
	}
	if opened {
		if e = w.closeShard(); e != nil {
			return e
		}
	}
	flushOwner()
	s.TopOwners = append(s.TopOwners, topOwners...)
	sort.Slice(s.TopOwners, func(i, j int) bool { return s.TopOwners[i].Rows > s.TopOwners[j].Rows })
	if len(s.TopOwners) > 100 {
		s.TopOwners = s.TopOwners[:100]
	}
	duration := time.Since(start)
	buildCPU := cpuTime() - cpuStart
	result := map[string]any{"scope": "offline immutable ContractStorage live-row stream; no production integration; same canonical input cuts/settings; pages preserve old logical source bytes, not cross-format MuHash roots", "source_path": *in, "source_file_bytes": sourceStat.Size(), "source_file_mtime": sourceStat.ModTime().UTC(), "statistics": s, "selected_rows_framed_sha256": hex.EncodeToString(sourceHash.Sum(nil)), "page_rows_bound": *pageRows, "compression": *compression, "block_size": *blockSize, "restart_interval": 16, "filter_bits_per_key": 10, "build_wall_ms": float64(duration.Nanoseconds()) / 1e6, "build_cpu_ms": buildCPU, "runtime": runtime.Version(), "GOOS": runtime.GOOS}
	result["min_page_rows"] = *minPageRows
	result["page_value_upper_bound_bytes"] = *blockSize - 96
	result["page_codec"] = "SPG2: common tail prefix, optional local value dictionary, u16 offsets; short groups retain ordinary values"
	if !*statsOnly {
		if !bytes.Equal(sourceHash.Sum(nil), restored.Sum(nil)) {
			return errors.New("full selected-source round-trip hash mismatch")
		}
		result["restored_rows_framed_sha256"] = hex.EncodeToString(restored.Sum(nil))
		result["full_selected_row_roundtrip_equal"] = true
		result["pages"] = w.pages
		result["dictionary_pages"] = w.dictPages
		result["page_value_bytes"] = w.pagePayloadBytes
		result["sst_files"] = w.files
		totals := [2]int64{}
		for i, files := range w.files {
			for _, x := range files {
				totals[i] += x.Bytes
			}
		}
		result["sst_total_bytes"] = totals
		result["sst_net_saved_bytes"] = totals[0] - totals[1]
		if totals[0] > 0 {
			result["sst_net_saved_percent"] = 100 * float64(totals[0]-totals[1]) / float64(totals[0])
		}
		if s.SelectedRows > 0 {
			for i, name := range []string{"baseline", "pages"} {
				if e = ingest(filepath.Join(*out, name, "db"), w.files[i]); e != nil {
					return e
				}
			}
			// Obtain miss oracles from the baseline; a flipped tail can itself be
			// a real slot. Never assume that fabricated keys must be absent.
			base, e := pebble.Open(filepath.Join(*out, "baseline", "db"), &pebble.Options{ReadOnly: true, Filters: map[string]pebble.FilterPolicy{bloom.FilterPolicy(10).Name(): bloom.FilterPolicy(10)}})
			if e != nil {
				return e
			}
			queries := make([]row, 0, len(samples)*2)
			for _, q := range samples {
				queries = append(queries, q)
				k := append([]byte(nil), q.key...)
				k[len(k)-1] ^= 0x5a
				v, c, e := base.Get(k)
				if e == nil {
					v = append([]byte(nil), v...)
					if e = c.Close(); e != nil {
						return e
					}
				} else if errors.Is(e, pebble.ErrNotFound) {
					v = nil
				} else {
					return e
				}
				queries = append(queries, row{k, v})
			}
			if e = base.Close(); e != nil {
				return e
			}
			rng.Shuffle(len(queries), func(i, j int) { queries[i], queries[j] = queries[j], queries[i] })
			benchmarks := []benchResult{}
			for _, name := range []string{"baseline", "pages"} {
				b, e := runBench(filepath.Join(*out, name, "db"), name, queries, false, 8<<20)
				if e != nil {
					return e
				}
				benchmarks = append(benchmarks, b)
			}
			if *coldQueries > 0 {
				q := queries
				if len(q) > *coldQueries {
					q = q[:*coldQueries]
				}
				for _, name := range []string{"pages", "baseline"} {
					b, e := runBench(filepath.Join(*out, name, "db"), name, q, true, 8<<20)
					if e != nil {
						return e
					}
					benchmarks = append(benchmarks, b)
				}
			}
			result["random_lookup_benchmarks"] = benchmarks
		}
		alloc := [2]int64{}
		for i, name := range []string{"baseline", "pages"} {
			alloc[i], e = allocatedTree(filepath.Join(*out, name))
			if e != nil {
				return e
			}
		}
		result["experiment_inode_dedup_allocated_bytes_including_db_control_and_directories"] = alloc
		result["experiment_net_allocated_saved_bytes"] = alloc[0] - alloc[1]
	}
	b, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(*out, "result.json"), append(b, '\n'), 0644); e != nil {
		return e
	}
	fmt.Printf("CONTRACT_STORAGE_EXPERIMENT_DONE %s rows=%d wall=%s\n", filepath.Join(*out, "result.json"), s.SelectedRows, duration)
	return nil
}
