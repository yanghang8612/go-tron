// Command reward-state-compact is an offline experiment. It consumes the
// root census row stream: u32BE key length, physical key, u32BE value length,
// exact wrapped value, repeated to EOF. It never opens a live database.
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
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type subtypeStat struct {
	Rows             uint64 `json:"rows"`
	PhysicalKeyBytes uint64 `json:"physical_key_bytes"`
	WrappedBytes     uint64 `json:"wrapped_bytes"`
	LogicalBytes     uint64 `json:"logical_bytes"`
	CompactBytes     uint64 `json:"candidate_wrapped_bytes"`
}
type fieldStat struct {
	Rows         uint64 `json:"present_rows"`
	PayloadBytes uint64 `json:"native_payload_bytes"`
}
type tableStat struct {
	Variant     string `json:"variant"`
	Compression string `json:"compression"`
	Files       int    `json:"files"`
	Bytes       int64  `json:"file_bytes"`
	Rows        uint64 `json:"rows"`
	WriteNanos  int64  `json:"writer_set_and_close_nanos"`
}
type tableWriter struct {
	dir         string
	compression sstable.Compression
	stat        tableStat
	writer      *sstable.Writer
	file        string
}

func (w *tableWriter) open() error {
	w.file = filepath.Join(w.dir, fmt.Sprintf("%s-%s-%05d.sst", w.stat.Variant, w.stat.Compression, w.stat.Files))
	f, err := vfs.Default.Create(w.file)
	if err != nil {
		return err
	}
	w.writer = sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Compression: w.compression, BlockSize: 4096, IndexBlockSize: 4096,
		FilterPolicy: bloom.FilterPolicy(10), TableFormat: sstable.TableFormatPebblev4,
	})
	return nil
}
func (w *tableWriter) set(k, v []byte) error {
	if w.writer == nil {
		if err := w.open(); err != nil {
			return err
		}
	}
	t := time.Now()
	err := w.writer.Set(k, v)
	w.stat.WriteNanos += time.Since(t).Nanoseconds()
	w.stat.Rows++
	return err
}
func (w *tableWriter) close() error {
	if w.writer == nil {
		return nil
	}
	t := time.Now()
	err := w.writer.Close()
	w.stat.WriteNanos += time.Since(t).Nanoseconds()
	w.writer = nil
	if err != nil {
		return err
	}
	s, err := os.Stat(w.file)
	if err != nil {
		return err
	}
	w.stat.Bytes += s.Size()
	w.stat.Files++
	return nil
}

func subtype(k []byte) string {
	if bytes.HasPrefix(k, []byte("dl-end-")) && len(k) == 28 {
		return "end-cycle"
	}
	if bytes.HasPrefix(k, []byte("dl-")) && len(k) == 24 {
		return "begin-cycle"
	}
	if len(k) > 34 && bytes.HasPrefix(k, []byte("dl-")) && k[11] == '-' && k[33] == '-' {
		s := string(k[34:])
		switch s {
		case "account-vote", "brokerage", "reward", "vi", "vote":
			return s
		}
	}
	return "unknown"
}
func topFields(raw []byte, out map[string]*fieldStat) error {
	if !bytes.HasPrefix(raw, nativeMagic) {
		return fmt.Errorf("account-vote not native")
	}
	d := decoder{b: raw[len(nativeMagic):]}
	n, e := d.varint()
	if e != nil {
		return e
	}
	for i := uint64(0); i < n; i++ {
		num, e := d.varint()
		if e != nil {
			return e
		}
		if _, e := d.take(1); e != nil {
			return e
		}
		payload, e := d.blob()
		if e != nil {
			return e
		}
		key := fmt.Sprintf("%d", num)
		s := out[key]
		if s == nil {
			s = &fieldStat{}
			out[key] = s
		}
		s.Rows++
		s.PayloadBytes += uint64(len(payload))
	}
	unknown, e := d.blob()
	if e != nil {
		return e
	}
	if len(unknown) > 0 {
		s := out["unknown"]
		if s == nil {
			s = &fieldStat{}
			out["unknown"] = s
		}
		s.Rows++
		s.PayloadBytes += uint64(len(unknown))
	}
	return d.done()
}

type sample struct{ native, compact []byte }

var selectedDecoder = decodeCompactSnapshotFast

func benchmark(samples []sample, iterations int) map[string]any {
	if len(samples) == 0 {
		return map[string]any{"samples": 0}
	}
	order := rand.New(rand.NewSource(981173)).Perm(len(samples))
	durations := make([]int64, 0, len(samples)*iterations)
	var checksum uint64
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	for r := 0; r < iterations; r++ {
		for _, i := range order {
			t := time.Now()
			back, err := selectedDecoder(samples[i].compact)
			if err != nil || !bytes.Equal(back, samples[i].native) {
				panic(fmt.Sprintf("microbenchmark return mismatch: %v", err))
			}
			checksum += uint64(len(back))
			durations = append(durations, time.Since(t).Nanoseconds())
		}
	}
	elapsed := time.Since(start).Nanoseconds()
	runtime.ReadMemStats(&after)
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	quantile := func(q float64) int64 { return durations[int(float64(len(durations)-1)*q)] }
	return map[string]any{
		"scope":     "In-memory complete single-value native-byte restoration, includes exact returned-byte comparison; no DB I/O or HTTP",
		"selection": "Deterministic stream stride every 251st account-vote, capped at 4096 values and 32MiB combined bytes",
		"samples":   len(samples), "operations": len(durations), "iterations": iterations,
		"total_nanos": elapsed, "ns_per_op": float64(elapsed) / float64(len(durations)),
		"p50_nanos": quantile(.50), "p95_nanos": quantile(.95), "p99_nanos": quantile(.99),
		"allocated_bytes_per_op": float64(after.TotalAlloc-before.TotalAlloc) / float64(len(durations)),
		"allocations_per_op":     float64(after.Mallocs-before.Mallocs) / float64(len(durations)), "checksum": checksum,
	}
}

func benchmarkAPI(samples []sample, iterations int) map[string]any {
	if len(samples) == 0 {
		return map[string]any{"samples": 0}
	}
	order := rand.New(rand.NewSource(981173)).Perm(len(samples))
	baseline := make([]int64, 0, len(samples)*iterations)
	candidate := make([]int64, 0, len(samples)*iterations)
	decode := func(s sample, compact bool) ([]byte, int64) {
		start := time.Now()
		data := s.native
		if compact {
			var err error
			data, err = selectedDecoder(s.compact)
			if err != nil {
				panic(err)
			}
		}
		var account corepb.Account
		if err := statecodec.Unmarshal(data, &account); err != nil {
			panic(err)
		}
		p, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&account)
		if err != nil {
			panic(err)
		}
		return p, time.Since(start).Nanoseconds()
	}
	for round := 0; round < iterations; round++ {
		for _, i := range order {
			var a, b []byte
			var ta, tb int64
			if round%2 == 0 {
				a, ta = decode(samples[i], false)
				b, tb = decode(samples[i], true)
			} else {
				b, tb = decode(samples[i], true)
				a, ta = decode(samples[i], false)
			}
			if !bytes.Equal(a, b) {
				panic("complete reward account API value mismatch")
			}
			baseline = append(baseline, ta)
			candidate = append(candidate, tb)
		}
	}
	stats := func(a []int64) map[string]any {
		var sum int64
		for _, n := range a {
			sum += n
		}
		sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
		return map[string]any{"operations": len(a), "mean_nanos": float64(sum) / float64(len(a)), "p50_nanos": a[(len(a)-1)/2], "p95_nanos": a[int(float64(len(a)-1)*.95)], "p99_nanos": a[int(float64(len(a)-1)*.99)]}
	}
	return map[string]any{
		"scope":   "In-memory complete statecodec.Unmarshal Account plus deterministic protobuf API value; candidate restores canonical native bytes first. Interleaved AB/BA, exact returned API bytes checked. No DB I/O or HTTP.",
		"samples": len(samples), "baseline": stats(baseline), "candidate": stats(candidate), "api_return_byte_mismatches": 0,
	}
}

func cpuNanos() int64 {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0
	}
	return (r.Utime.Sec+r.Stime.Sec)*1e9 + int64(r.Utime.Usec+r.Stime.Usec)*1e3
}
func run() error {
	rows := flag.String("rows", "", "fixed SystemReward physical row stream (never a live database)")
	out := flag.String("out", "", "new independent output directory")
	chunk := flag.Int64("input-chunk-mib", 64, "equal input bytes per SST cohort")
	iters := flag.Int("decode-iterations", 7, "in-memory sample restoration rounds")
	decoderName := flag.String("decoder", "fast", "candidate restoration implementation: fast or reference")
	flag.Parse()
	if *rows == "" || *out == "" || *chunk < 1 || *iters < 1 {
		return fmt.Errorf("require -rows FILE -out NEW_DIRECTORY; positive budgets")
	}
	switch *decoderName {
	case "fast":
		selectedDecoder = decodeCompactSnapshotFast
	case "reference":
		selectedDecoder = decodeCompactSnapshot
	default:
		return fmt.Errorf("unknown decoder %q", *decoderName)
	}
	if _, e := os.Stat(*out); !os.IsNotExist(e) {
		return fmt.Errorf("output must not exist: %s", *out)
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	f, err := os.Open(*rows)
	if err != nil {
		return err
	}
	defer f.Close()
	hash := sha256.New()
	r := bufio.NewReaderSize(io.TeeReader(f, hash), 1<<20)
	var writers []*tableWriter
	for _, c := range []struct {
		name  string
		type_ sstable.Compression
	}{{"raw", sstable.NoCompression}, {"snappy", sstable.SnappyCompression}} {
		for _, name := range []string{"baseline", "compact"} {
			writers = append(writers, &tableWriter{dir: *out, compression: c.type_, stat: tableStat{Variant: name, Compression: c.name}})
		}
	}
	stats := map[string]*subtypeStat{}
	fields := map[string]*fieldStat{}
	var count, votes, changed, inputBytes, originalNativeBytes, candidateNativeBytes uint64
	var encodeNanos, verifyNanos int64
	var chunkBytes int64
	var prev []byte
	var samples []sample
	var sampleBytes int
	started, cpuStart := time.Now(), cpuNanos()
	var header [4]byte
	for {
		n, e := io.ReadFull(r, header[:])
		if e == io.EOF && n == 0 {
			break
		}
		if e != nil {
			return e
		}
		kl := binary.BigEndian.Uint32(header[:])
		if kl == 0 || kl > 2<<20 {
			return fmt.Errorf("invalid key length %d", kl)
		}
		key := make([]byte, kl)
		if _, e := io.ReadFull(r, key); e != nil {
			return e
		}
		if _, e := io.ReadFull(r, header[:]); e != nil {
			return e
		}
		vl := binary.BigEndian.Uint32(header[:])
		if vl > 32<<20 {
			return fmt.Errorf("invalid value length %d", vl)
		}
		wrapped := make([]byte, vl)
		if _, e := io.ReadFull(r, wrapped); e != nil {
			return e
		}
		if prev != nil && bytes.Compare(prev, key) >= 0 {
			return fmt.Errorf("unordered/duplicate physical key")
		}
		prev = append(prev[:0], key...)
		_, _, domain, logicalKey, ok := rawdb.DecodeStateKVLatestKey(key)
		if !ok || domain != kvdomains.SystemReward {
			return fmt.Errorf("not a SystemReward physical key: %x", key)
		}
		native, e := rawdb.DecodeStateKVLatestValue(wrapped)
		if e != nil {
			return e
		}
		kind := subtype(logicalKey)
		s := stats[kind]
		if s == nil {
			s = &subtypeStat{}
			stats[kind] = s
		}
		s.Rows++
		s.PhysicalKeyBytes += uint64(len(key))
		s.WrappedBytes += uint64(len(wrapped))
		s.LogicalBytes += uint64(len(native))
		candidate := wrapped
		if kind == "account-vote" {
			votes++
			if e := topFields(native, fields); e != nil {
				return e
			}
			t := time.Now()
			compact, e := encodeCompactSnapshot(native)
			encodeNanos += time.Since(t).Nanoseconds()
			if e != nil {
				return e
			}
			t = time.Now()
			back, e := selectedDecoder(compact)
			if e != nil || !bytes.Equal(back, native) {
				return fmt.Errorf("snapshot canonical native byte mismatch at key %x: %v", key, e)
			}
			reference, e := decodeCompactSnapshot(compact)
			if e != nil || !bytes.Equal(reference, native) || !bytes.Equal(reference, back) {
				return fmt.Errorf("independent reference native byte mismatch at key %x: %v", key, e)
			}
			verifyNanos += time.Since(t).Nanoseconds()
			originalNativeBytes += uint64(len(native))
			candidateNativeBytes += uint64(len(compact))
			cw := rawdb.EncodeStateKVLatestValue(compact)
			if len(cw) < len(wrapped) {
				candidate = cw
				changed++
			}
			if votes%251 == 1 && len(samples) < 4096 && sampleBytes+len(native)+len(compact) <= 32<<20 {
				samples = append(samples, sample{native: append([]byte(nil), native...), compact: compact})
				sampleBytes += len(native) + len(compact)
			}
		}
		s.CompactBytes += uint64(len(candidate))
		for _, w := range writers {
			v := wrapped
			if w.stat.Variant == "compact" {
				v = candidate
			}
			if e := w.set(key, v); e != nil {
				return e
			}
		}
		count++
		nbytes := int64(8) + int64(len(key)+len(wrapped))
		inputBytes += uint64(nbytes)
		chunkBytes += nbytes
		if chunkBytes >= *chunk<<20 {
			for _, w := range writers {
				if e := w.close(); e != nil {
					return e
				}
			}
			chunkBytes = 0
		}
		if count%1000000 == 0 {
			fmt.Fprintf(os.Stderr, "rows=%d account_vote=%d input_bytes=%d elapsed=%s\n", count, votes, inputBytes, time.Since(started).Round(time.Second))
		}
	}
	var tables []tableStat
	for _, w := range writers {
		if e := w.close(); e != nil {
			return e
		}
		tables = append(tables, w.stat)
	}
	metrics := map[string]any{
		"scope":       "Offline exact SystemReward logical-live row cohort; matched input/grouping, physical keys, block/filter/table-format parameters; full returned canonical native bytes checked for every account-vote; not production whole-DB saving or HTTP performance",
		"source_rows": *rows, "input_stream_bytes": inputBytes, "input_stream_sha256": hex.EncodeToString(hash.Sum(nil)),
		"rows": count, "account_vote_rows": votes, "changed_rows": changed,
		"account_vote_native_bytes": originalNativeBytes, "account_vote_compact_bytes_before_fallback": candidateNativeBytes,
		"subtypes": stats, "account_fields": fields, "tables": tables,
		"equal_input_chunk_bytes": *chunk << 20, "sst_block_bytes": 4096, "sst_index_block_bytes": 4096,
		"bloom_bits_per_key": 10, "table_format": "PebbleV4", "canonical_native_byte_mismatches": 0,
		"selected_decoder": *decoderName, "independent_reference_native_byte_mismatches": 0,
		"encode_nanos": encodeNanos, "round_trip_verification_nanos": verifyNanos,
		"build_wall_nanos": time.Since(started).Nanoseconds(), "build_process_cpu_nanos": cpuNanos() - cpuStart,
		"started_utc": started.UTC().Format(time.RFC3339Nano), "finished_utc": time.Now().UTC().Format(time.RFC3339Nano),
		"decode_microbenchmark": benchmark(samples, *iters), "account_api_microbenchmark": benchmarkAPI(samples, *iters), "go_version": runtime.Version(),
	}
	encoded, e := json.MarshalIndent(metrics, "", "  ")
	if e != nil {
		return e
	}
	if e := os.WriteFile(filepath.Join(*out, "results.json"), append(encoded, '\n'), 0644); e != nil {
		return e
	}
	fmt.Println(string(encoded))
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(err.Error()))
		os.Exit(1)
	}
}
