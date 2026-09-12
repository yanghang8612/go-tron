package rawdb

// This opt-in experiment uses synthetic data and real on-disk Pebble. It is not
// a throughput acceptance test: automatic compaction scheduling is asynchronous.
// No manual Compact call is used, including during fixture preparation.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type rangePruneABStore struct {
	db    *pebble.DB
	batch *pebble.Batch
}

func (s *rangePruneABStore) Get(key []byte) ([]byte, error) {
	v, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	value := bytes.Clone(v)
	return value, closer.Close()
}
func (s *rangePruneABStore) Has(key []byte) (bool, error) {
	_, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}
func (s *rangePruneABStore) Put(key, value []byte) error { return s.batch.Set(key, value, nil) }
func (s *rangePruneABStore) Delete(key []byte) error     { return s.batch.Delete(key, nil) }
func (s *rangePruneABStore) DeleteRange(start, end []byte) error {
	return s.batch.DeleteRange(start, end, nil)
}
func (s *rangePruneABStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	lower := append(bytes.Clone(prefix), start...)
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			upper = upper[:i+1]
			break
		}
		if i == 0 {
			upper = nil
		}
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	return &rangePruneABIterator{it: it, err: err}
}

type rangePruneABIterator struct {
	it      *pebble.Iterator
	err     error
	started bool
}

func (it *rangePruneABIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.started {
		it.started = true
		return it.it.First()
	}
	return it.it.Next()
}
func (it *rangePruneABIterator) Key() []byte   { return it.it.Key() }
func (it *rangePruneABIterator) Value() []byte { return it.it.Value() }
func (it *rangePruneABIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.it.Error()
}
func (it *rangePruneABIterator) Release() {
	if it.it != nil {
		_ = it.it.Close()
	}
}

type rangePruneABMetrics struct {
	SSTFileBytes    int64   `json:"sst_file_length_bytes"`
	SSTFiles        int     `json:"sst_files"`
	LevelBytes      []int64 `json:"level_bytes"`
	LevelFiles      []int64 `json:"level_files"`
	Compactions     int64   `json:"compactions"`
	DeleteOnly      int64   `json:"delete_only"`
	CompactRead     uint64  `json:"compaction_read_bytes"`
	CompactWritten  uint64  `json:"compaction_written_bytes"`
	Debt            uint64  `json:"estimated_debt"`
	InProgress      int64   `json:"compactions_in_progress"`
	RangeTombstones uint64  `json:"sst_range_tombstones"`
	PointTombstones uint64  `json:"sst_point_tombstones"`
	Format          uint64  `json:"format_major_version"`
}

func rangePruneABReadMetrics(t *testing.T, db *pebble.DB, path string) rangePruneABMetrics {
	t.Helper()
	m := db.Metrics()
	r := rangePruneABMetrics{Compactions: m.Compact.Count, DeleteOnly: m.Compact.DeleteOnlyCount, Debt: m.Compact.EstimatedDebt, InProgress: m.Compact.NumInProgress, Format: uint64(db.FormatMajorVersion())}
	for _, l := range m.Levels {
		r.LevelBytes = append(r.LevelBytes, l.Size)
		r.LevelFiles = append(r.LevelFiles, l.NumFiles)
		r.CompactRead += l.BytesRead
		r.CompactWritten += l.BytesCompacted
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sst" {
			info, err := entry.Info()
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			r.SSTFileBytes += info.Size()
			r.SSTFiles++
		}
	}
	tables, err := db.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	for _, level := range tables {
		for _, table := range level {
			r.RangeTombstones += table.Properties.NumRangeDeletions
			r.PointTombstones += table.Properties.NumDeletions - table.Properties.NumRangeDeletions
		}
	}
	return r
}

func rangePruneABOptions() *pebble.Options {
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		levels[i] = pebble.LevelOptions{Compression: pebble.NoCompression, TargetFileSize: 2 << 20}
	}
	return &pebble.Options{FormatMajorVersion: pebble.FormatMostCompatible, MemTableSize: 16 << 20, MemTableStopWritesThreshold: 4, LBaseMaxBytes: 64 << 20, L0CompactionThreshold: 4, L0StopWritesThreshold: 32, MaxConcurrentCompactions: func() int { return 1 }, Levels: levels}
}

func rangePruneABSmallKey(n uint64, high bool) []byte {
	var owner common.Address
	owner[0] = common.AddressPrefixMainnet
	binary.BigEndian.PutUint64(owner[len(owner)-8:], n)
	if high {
		return stateKVLatestKey(owner, 0, kvdomains.ContractStorage, []byte("slot"))
	}
	return stateAccountLatestKey(owner)
}

func rangePruneABIterate(t *testing.T, db *pebble.DB, rounds int) float64 {
	t.Helper()
	start := time.Now()
	for i := 0; i < rounds; i++ {
		it, err := db.NewIter(&pebble.IterOptions{LowerBound: stateChangeSetKey(1, 0), UpperBound: stateChangeSetKey(1025, 0)})
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for valid := it.First(); valid && count < 16; valid = it.Next() {
			if !bytes.Equal(it.Key(), stateChangeSetKey(uint64(769+count), 0)) || len(it.Value()) != 128<<10 {
				t.Fatalf("invalid live iterator row: %x", it.Key())
			}
			count++
		}
		if count != 16 || it.Error() != nil {
			t.Fatalf("iterator count=%d err=%v", count, it.Error())
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return float64(time.Since(start).Nanoseconds()) / float64(rounds)
}

func TestStateDomainChangeRangePruneAutomaticCompactionAB(t *testing.T) {
	if os.Getenv("GTRON_RANGE_PRUNE_AB") != "1" {
		t.Skip("opt-in bounded synthetic on-disk A/B")
	}
	type result struct {
		Shape             string                       `json:"shape"`
		Variant           string                       `json:"variant"`
		Repeat            int                          `json:"repeat"`
		Stats             StateDomainChangeDeleteStats `json:"logical_delete_stats"`
		DeleteScanMS      float64                      `json:"delete_scan_ms"`
		CommitMS          float64                      `json:"batch_commit_ms"`
		DeleteBatchBytes  int                          `json:"delete_batch_encoded_bytes"`
		PrefixIterNS      float64                      `json:"prefix_iterator_ns_per_16_rows"`
		EarlyPrefixIterNS float64                      `json:"early_prefix_iterator_ns_per_16_rows"`
		Before            rangePruneABMetrics          `json:"before"`
		AfterFlush        rangePruneABMetrics          `json:"after_delete_flush"`
		After             rangePruneABMetrics          `json:"after_fixed_flow_and_wait"`
	}
	var results []result
	for _, shape := range []string{"history-only-delete-table", "mixed-small-writes-delete-table"} {
		seedPath := filepath.Join(t.TempDir(), "seed")
		seed, err := pebble.Open(seedPath, rangePruneABOptions())
		if err != nil {
			t.Fatal(err)
		}
		value := bytes.Repeat([]byte{0xfd}, 128<<10)
		for first := uint64(1); first <= 1024; first += 32 {
			b := seed.NewBatch()
			for n := first; n < first+32; n++ {
				if err := b.Set(stateChangeSetKey(n, 0), value, nil); err != nil {
					t.Fatal(err)
				}
			}
			// Rewriting this small sentinel produces overlap under automatic L0
			// selection; preparation also uses no manual Compact call.
			if err := b.Set(stateAccountLatestKey(common.Address{1}), []byte{byte(first)}, nil); err != nil {
				t.Fatal(err)
			}
			if err := b.Commit(pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			_ = b.Close()
			if err := seed.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		b := seed.NewBatch()
		store := &rangePruneABStore{db: seed, batch: b}
		for n := uint64(0); n < 20000; n++ {
			for _, high := range []bool{false, true} {
				if err := store.Put(rangePruneABSmallKey(n, high), bytes.Repeat([]byte{7}, 64)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := WriteStageProgressWithHash(store, StageStateHistoryIndex, 1024, common.Hash{1}); err != nil {
			t.Fatal(err)
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		_ = b.Close()
		if err := seed.Flush(); err != nil {
			t.Fatal(err)
		}
		// Fixed bounded settle period is common to all immutable checkpoints.
		time.Sleep(2 * time.Second)
		paths := make([]string, 6)
		for i := range paths {
			paths[i] = filepath.Join(t.TempDir(), "clone")
			if err := seed.Checkpoint(paths[i]); err != nil {
				t.Fatal(err)
			}
		}
		if err := seed.Close(); err != nil {
			t.Fatal(err)
		}
		variants := []string{"point", "coarse-256", "per-block-range", "per-block-range", "coarse-256", "point"}
		for index, variant := range variants {
			path := paths[index]
			db, err := pebble.Open(path, rangePruneABOptions())
			if err != nil {
				t.Fatal(err)
			}
			r := result{Shape: shape, Variant: variant, Repeat: index / 3, Before: rangePruneABReadMetrics(t, db, path)}
			batch := db.NewBatch()
			store := &rangePruneABStore{db: db, batch: batch}
			opts := StateDomainChangeDeleteOptions{}
			if variant != "point" {
				opts = StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 64, MaxRangeBlocks: 256}
				if variant == "per-block-range" {
					opts.MinRangeBlocks = 1
					opts.MaxRangeBlocks = 1
				}
			}
			start := time.Now()
			r.Stats, err = DeleteStateDomainChangeBlocksWithOptions(store, rangePruneBlocks(1, 768), opts)
			r.DeleteScanMS = float64(time.Since(start).Nanoseconds()) / 1e6
			if err != nil {
				t.Fatal(err)
			}
			r.DeleteBatchBytes = len(batch.Repr())
			if shape == "mixed-small-writes-delete-table" {
				for n := uint64(0); n < 20000; n++ {
					for _, high := range []bool{false, true} {
						if err := store.Put(rangePruneABSmallKey(n, high), bytes.Repeat([]byte{9}, 64)); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			start = time.Now()
			if err := batch.Commit(pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			r.CommitMS = float64(time.Since(start).Nanoseconds()) / 1e6
			_ = batch.Close()
			r.EarlyPrefixIterNS = rangePruneABIterate(t, db, 100)
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			r.AfterFlush = rangePruneABReadMetrics(t, db, path)
			// Identical post-delete foreground stream: eight 128 KiB small-key
			// writes and Flush calls. Only deletion representation differs.
			for step := 0; step < 8; step++ {
				batch := db.NewBatch()
				for n := uint64(0); n < 2048; n++ {
					if err := batch.Set(rangePruneABSmallKey(n, step%2 == 0), bytes.Repeat([]byte{byte(step)}, 64), nil); err != nil {
						t.Fatal(err)
					}
				}
				if err := batch.Commit(pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				_ = batch.Close()
				if err := db.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(time.Second)
			r.PrefixIterNS = rangePruneABIterate(t, db, 500)
			r.After = rangePruneABReadMetrics(t, db, path)
			for _, n := range []uint64{1, 384, 768} {
				_, closer, err := db.Get(stateChangeSetKey(n, 0))
				if closer != nil {
					_ = closer.Close()
				}
				if err != pebble.ErrNotFound {
					t.Fatalf("deleted read=%d err=%v", n, err)
				}
			}
			if r.After.Format != uint64(pebble.FormatMostCompatible) {
				t.Fatal("format unexpectedly upgraded")
			}
			results = append(results, r)
			t.Logf("%s %s repeat=%d scan=%.3fms batch=%dB disk=%d->%d delete-only=%d iter=%.0fns", shape, variant, r.Repeat, r.DeleteScanMS, r.DeleteBatchBytes, r.Before.SSTFileBytes, r.After.SSTFileBytes, r.After.DeleteOnly-r.Before.DeleteOnly, r.PrefixIterNS)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	report := map[string]any{"synthetic": true, "manual_compact_calls": 0, "dataset_history_bytes": 128 << 20, "selected_history_bytes": 96 << 20, "blocks": 1024, "selected_blocks": 768, "setup": "same checkpoint per shape; Format1, 16MiB memtable, 2MiB target SST, 64MiB base, L0 threshold4, one compactor, no compression", "flow": "delete+optional 40000x64B small updates; Flush; 8x2048x64B updates+Flush; fixed1s wait; 100 early and 500 final prefix seeks reading16 live rows", "limitations": []string{"synthetic scheduling and smaller options cannot predict production wall-time or byte savings", "SST file lengths are observed files, not allocated filesystem du; physical unlink may lag", "iterator tests measure one retained history prefix, not all online iterators", "counter deltas valid only inside each independently opened DB; no manual Compact call anywhere"}, "results": results}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("GTRON_RANGE_PRUNE_AB_OUTPUT"); output != "" {
		if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(output, append(encoded, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	} else {
		fmt.Println(string(encoded))
	}
}
