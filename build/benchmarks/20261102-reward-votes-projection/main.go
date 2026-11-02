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

type tableStat struct {
	Variant     string `json:"variant"`
	Compression string `json:"compression"`
	Files       int    `json:"files"`
	Bytes       int64  `json:"file_bytes"`
	Rows        uint64 `json:"rows"`
}
type writer struct {
	dir, path string
	w         *sstable.Writer
	stat      tableStat
}

func (w *writer) set(k, v []byte) error {
	if w.w == nil {
		w.path = filepath.Join(w.dir, fmt.Sprintf("votes-only-snappy-%05d.sst", w.stat.Files))
		f, e := vfs.Default.Create(w.path)
		if e != nil {
			return e
		}
		w.w = sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression, BlockSize: 4096, IndexBlockSize: 4096, FilterPolicy: bloom.FilterPolicy(10), TableFormat: sstable.TableFormatPebblev4})
	}
	if e := w.w.Set(k, v); e != nil {
		return e
	}
	w.stat.Rows++
	return nil
}
func (w *writer) close() error {
	if w.w == nil {
		return nil
	}
	if e := w.w.Close(); e != nil {
		return e
	}
	w.w = nil
	s, e := os.Stat(w.path)
	if e != nil {
		return e
	}
	w.stat.Bytes += s.Size()
	w.stat.Files++
	return nil
}

type sample struct{ native, projected []byte }
type rowStat struct {
	Rows             uint64 `json:"rows"`
	OriginalWrapped  uint64 `json:"original_wrapped_bytes"`
	CandidateWrapped uint64 `json:"candidate_wrapped_bytes"`
}

func category(k []byte) string {
	if bytes.HasPrefix(k, []byte("dl-end-")) && len(k) == 28 {
		return "end-cycle"
	}
	if bytes.HasPrefix(k, []byte("dl-")) && len(k) == 24 {
		return "begin-cycle"
	}
	if len(k) > 34 && bytes.HasPrefix(k, []byte("dl-")) && k[11] == '-' && k[33] == '-' {
		switch s := string(k[34:]); s {
		case "account-vote", "brokerage", "reward", "vi", "vote":
			return s
		}
	}
	return "unknown"
}
func stats(a []int64) map[string]any {
	var sum int64
	for _, n := range a {
		sum += n
	}
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return map[string]any{"operations": len(a), "mean_nanos": float64(sum) / float64(len(a)), "p50_nanos": a[(len(a)-1)/2], "p95_nanos": a[int(float64(len(a)-1)*.95)], "p99_nanos": a[int(float64(len(a)-1)*.99)]}
}
func benchmark(samples []sample, iterations int) map[string]any {
	if len(samples) == 0 {
		return map[string]any{"samples": 0}
	}
	order := rand.New(rand.NewSource(41529)).Perm(len(samples))
	a := make([]int64, 0, len(samples)*iterations)
	b := make([]int64, 0, cap(a))
	read := func(raw []byte) ([]byte, int64) {
		start := time.Now()
		var x corepb.Account
		if e := statecodec.Unmarshal(raw, &x); e != nil {
			panic(e)
		}
		v, e := (proto.MarshalOptions{Deterministic: true}).Marshal(&corepb.Account{Votes: x.Votes})
		if e != nil {
			panic(e)
		}
		return v, time.Since(start).Nanoseconds()
	}
	for round := 0; round < iterations; round++ {
		for _, i := range order {
			var av, bv []byte
			var an, bn int64
			if round%2 == 0 {
				av, an = read(samples[i].native)
				bv, bn = read(samples[i].projected)
			} else {
				bv, bn = read(samples[i].projected)
				av, an = read(samples[i].native)
			}
			if !bytes.Equal(av, bv) {
				panic("Votes semantic return mismatch in sample benchmark")
			}
			a = append(a, an)
			b = append(b, bn)
		}
	}
	return map[string]any{"scope": "In-memory native Account decode then deterministic Votes-only semantic result. Full Account returns intentionally differ. Interleaved AB/BA; no HTTP/DB I/O.", "samples": len(samples), "baseline_full_snapshot_decode": stats(a), "candidate_votes_projection_decode": stats(b), "votes_semantic_return_byte_mismatches": 0}
}
func run() error {
	input := flag.String("rows", "", "fixed SystemReward physical row stream")
	out := flag.String("out", "", "new independent output directory")
	basePath := flag.String("baseline-results", "", "prior identical input baseline results.json, optional for synthetic fixtures")
	chunk := flag.Int64("input-chunk-mib", 64, "same original-input cohort boundaries")
	iterations := flag.Int("decode-iterations", 7, "typed Votes read sample rounds")
	flag.Parse()
	if *input == "" || *out == "" || *chunk < 1 || *iterations < 1 {
		return fmt.Errorf("require -rows FILE -out NEWDIR and positive budgets")
	}
	if _, e := os.Stat(*out); !os.IsNotExist(e) {
		return fmt.Errorf("output exists or is inaccessible")
	}
	if e := os.MkdirAll(*out, 0755); e != nil {
		return e
	}
	f, e := os.Open(*input)
	if e != nil {
		return e
	}
	defer f.Close()
	h := sha256.New()
	r := bufio.NewReaderSize(io.TeeReader(f, h), 1<<20)
	w := writer{dir: *out, stat: tableStat{Variant: "votes-only-semantic-projection", Compression: "snappy"}}
	started := time.Now()
	var rows, voteRows, voteEntries, emptySnapshots, unknownVoteBytes, inputBytes, originalSnapshotBytes, projectedSnapshotBytes uint64
	var projectNanos, verifyNanos, chunkBytes int64
	subtypes := map[string]*rowStat{}
	var previous []byte
	var header [4]byte
	var samples []sample
	sampleBytes, sampleSkips := 0, 0
	rng := rand.New(rand.NewSource(981173))
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
			return fmt.Errorf("invalid key size")
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
			return fmt.Errorf("invalid value size")
		}
		wrapped := make([]byte, vl)
		if _, e := io.ReadFull(r, wrapped); e != nil {
			return e
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			return fmt.Errorf("unordered/duplicate input key")
		}
		previous = append(previous[:0], key...)
		_, _, domain, k, ok := rawdb.DecodeStateKVLatestKey(key)
		if !ok || domain != kvdomains.SystemReward {
			return fmt.Errorf("non-SystemReward source row")
		}
		kind := category(k)
		candidate := wrapped
		if kind == "account-vote" {
			voteRows++
			native, e := rawdb.DecodeStateKVLatestValue(wrapped)
			if e != nil {
				return e
			}
			t := time.Now()
			projected, sourceVotes, e := projectVotes(native)
			projectNanos += time.Since(t).Nanoseconds()
			if e != nil {
				return e
			}
			t = time.Now()
			var account corepb.Account
			if e := statecodec.Unmarshal(projected, &account); e != nil {
				return e
			}
			unknown, e := verifyVotes(sourceVotes, account.Votes)
			if e != nil {
				return e
			}
			unknownVoteBytes += unknown
			voteEntries += uint64(len(account.Votes))
			again, e := statecodec.Marshal(&account)
			if e != nil || !bytes.Equal(again, projected) {
				return fmt.Errorf("projected native decode/encode mismatch: %v", e)
			}
			present, empty := projectedPresence(projected, true)
			if !present {
				return fmt.Errorf("present source snapshot lost")
			}
			if empty {
				emptySnapshots++
			}
			verifyNanos += time.Since(t).Nanoseconds()
			candidate = rawdb.EncodeStateKVLatestValue(projected)
			originalSnapshotBytes += uint64(len(native))
			projectedSnapshotBytes += uint64(len(projected))
			slot := -1
			if len(samples) < 4096 {
				slot = len(samples)
			} else if j := rng.Int63n(int64(voteRows)); j < 4096 {
				slot = int(j)
			}
			if slot >= 0 {
				old := 0
				if slot < len(samples) {
					old = len(samples[slot].native) + len(samples[slot].projected)
				}
				cost := len(native) + len(projected)
				if sampleBytes-old+cost <= 32<<20 {
					x := sample{native: append([]byte(nil), native...), projected: append([]byte(nil), projected...)}
					if slot == len(samples) {
						samples = append(samples, x)
					} else {
						samples[slot] = x
					}
					sampleBytes += cost - old
				} else {
					sampleSkips++
				}
			}
		}
		s := subtypes[kind]
		if s == nil {
			s = &rowStat{}
			subtypes[kind] = s
		}
		s.Rows++
		s.OriginalWrapped += uint64(len(wrapped))
		s.CandidateWrapped += uint64(len(candidate))
		if e := w.set(key, candidate); e != nil {
			return e
		}
		rows++
		nb := int64(8 + len(key) + len(wrapped))
		inputBytes += uint64(nb)
		chunkBytes += nb
		if chunkBytes >= *chunk<<20 {
			if e := w.close(); e != nil {
				return e
			}
			chunkBytes = 0
		}
		if rows%1000000 == 0 {
			fmt.Fprintf(os.Stderr, "rows=%d snapshots=%d elapsed=%s\n", rows, voteRows, time.Since(started).Round(time.Second))
		}
	}
	if e := w.close(); e != nil {
		return e
	}
	inputSHA := hex.EncodeToString(h.Sum(nil))
	comparison := map[string]any{"available": false}
	if *basePath != "" {
		b, e := os.ReadFile(*basePath)
		if e != nil {
			return e
		}
		var base struct {
			Rows   uint64      `json:"rows"`
			Hash   string      `json:"input_stream_sha256"`
			Input  uint64      `json:"input_stream_bytes"`
			Chunk  int64       `json:"equal_input_chunk_bytes"`
			Block  int         `json:"sst_block_bytes"`
			Index  int         `json:"sst_index_block_bytes"`
			Bloom  int         `json:"bloom_bits_per_key"`
			Format string      `json:"table_format"`
			Tables []tableStat `json:"tables"`
		}
		if e := json.Unmarshal(b, &base); e != nil {
			return e
		}
		if base.Rows != rows || base.Hash != inputSHA || base.Input != inputBytes || base.Chunk != *chunk<<20 || base.Block != 4096 || base.Index != 4096 || base.Bloom != 10 || base.Format != "PebbleV4" {
			return fmt.Errorf("baseline cohort/settings do not match")
		}
		found := false
		for _, t := range base.Tables {
			if t.Variant == "baseline" && t.Compression == "snappy" {
				if t.Rows != rows || t.Files != w.stat.Files {
					return fmt.Errorf("baseline file/row cohort mismatch")
				}
				comparison = map[string]any{"available": true, "baseline_file_bytes": t.Bytes, "candidate_file_bytes": w.stat.Bytes, "saved_bytes": t.Bytes - w.stat.Bytes, "saved_percent": float64(t.Bytes-w.stat.Bytes) / float64(t.Bytes) * 100, "baseline_results_sha256": fmt.Sprintf("%x", sha256.Sum256(b)), "source_sha_rows_input_chunk_and_sst_parameters_match": true}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("baseline Snappy table not found")
		}
	}
	result := map[string]any{"scope": "NEW SEMANTIC STATE MODEL RESEARCH ONLY. Full Account snapshot fields are deliberately discarded; original full Account/native/MuHash cannot be reconstructed. Not installable into production.", "source_rows": *input, "input_stream_sha256": inputSHA, "input_stream_bytes": inputBytes, "rows": rows, "account_vote_rows": voteRows, "original_native_snapshot_bytes": originalSnapshotBytes, "projected_native_snapshot_bytes": projectedSnapshotBytes, "vote_entries": voteEntries, "nested_vote_unknown_bytes_preserved": unknownVoteBytes, "present_empty_snapshots": emptySnapshots, "row_presence_mismatches": 0, "votes_order_address_count_unknown_mismatches": 0, "projected_native_decode_reencode_mismatches": 0, "full_account_byte_equality_claimed": false, "production_muhash_verified": false, "subtypes": subtypes, "candidate_table": w.stat, "comparison": comparison, "equal_original_input_chunk_bytes": *chunk << 20, "sst_block_bytes": 4096, "sst_index_block_bytes": 4096, "bloom_bits_per_key": 10, "table_format": "PebbleV4", "projection_nanos": projectNanos, "votes_verification_nanos": verifyNanos, "build_wall_nanos": time.Since(started).Nanoseconds(), "sample_selection": "Algorithm R seeded reservoir across the entire account-vote stream, capacity4096, 32MiB cap; memory-rejection count reported", "sample_memory_rejections": sampleSkips, "sample_memory_bytes": sampleBytes, "votes_read_microbenchmark": benchmark(samples, *iterations), "go_version": runtime.Version(), "started_utc": started.UTC().Format(time.RFC3339Nano), "finished_utc": time.Now().UTC().Format(time.RFC3339Nano)}
	b, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		return e
	}
	if e := os.WriteFile(filepath.Join(*out, "results.json"), append(b, '\n'), 0644); e != nil {
		return e
	}
	fmt.Println(string(b))
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
