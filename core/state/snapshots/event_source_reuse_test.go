package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Recheck narrow queries distributed over an already measured immutable pair.
// The original full-segment source/cold comparison need not be repeated after
// metadata-only fixes. Every sampled result still uses its own raw-source oracle.
func TestIndependentEventFixedPairFinalReader(t *testing.T) {
	catalog := os.Getenv("GTRON_EVENT_REUSE_CATALOG")
	if catalog == "" {
		t.Skip("set GTRON_EVENT_REUSE_CATALOG for final immutable-pair reads")
	}
	out := os.Getenv("GTRON_EVENT_REUSE_OUT")
	if out == "" {
		t.Fatal("GTRON_EVENT_REUSE_OUT is required")
	}
	data, err := os.ReadFile(filepath.Join(catalog, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var prior struct {
		Source string `json:"source_chaindata"`
		From   uint64 `json:"from_block"`
		To     uint64 `json:"to_block"`
		Rows   uint64 `json:"log_rows"`
		Digest string `json:"source_log_digest"`
		Equal  bool   `json:"all_return_values_equal"`
	}
	if err := json.Unmarshal(data, &prior); err != nil {
		t.Fatal(err)
	}
	if !prior.Equal || prior.Source == "" || prior.From == 0 || prior.To < prior.From || prior.To-prior.From >= 65536 || prior.Digest == "" {
		t.Fatal("missing prior full-range oracle")
	}
	catalog, err = filepath.EvalSymlinks(catalog)
	if err != nil {
		t.Fatal(err)
	}
	catalog, _ = filepath.Abs(catalog)
	out, err = filepath.Abs(out)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(out))
	if err != nil {
		t.Fatal(err)
	}
	out = filepath.Join(parent, filepath.Base(out))
	if out == catalog || strings.HasPrefix(out, catalog+string(os.PathSeparator)) {
		t.Fatal("new result must be outside prior immutable catalog")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	base, err := rawdb.NewPebbleDBReadOnly(prior.Source, 256, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	chainDB := rawdb.NewChainDB(base, nil)
	properties, err := rawdb.ReadStateExecutionProperties(chainDB)
	if err != nil {
		t.Fatal(err)
	}
	chain := &coldBuilderChain{db: chainDB, solidified: properties.SolidifiedBlockNum}
	manager, err := OpenManager(catalog)
	if err != nil {
		t.Fatal(err)
	}
	manager.SetEventLogCanonicalSource(chain)
	type oracle struct {
		block, rows uint64
		digest      string
	}
	var queries []oracle
	sourceStarted, sourceCPU := time.Now(), eventPerfCPU()
	for i := uint64(0); i < 64; i++ {
		block := prior.From + (prior.To-prior.From)*i/63
		digest, rows, err := eventPerfDigest(func(fn func(EventLog) (bool, error)) error {
			return (eventLogV3ChainReader{chain: chainDB}).IterateEventLogs(block, block, EventLogFilter{}, fn)
		})
		if err != nil {
			t.Fatal(err)
		}
		queries = append(queries, oracle{block, rows, digest})
	}
	sourceWallNS, sourceCPUNS := time.Since(sourceStarted).Nanoseconds(), eventPerfCPU()-sourceCPU
	var results []map[string]any
	for pass := 0; pass < 2; pass++ {
		var latencies []int64
		var queryResults []map[string]any
		for _, q := range queries {
			started, cpu := time.Now(), eventPerfCPU()
			digest, rows, err := eventPerfDigest(func(fn func(EventLog) (bool, error)) error {
				return manager.IterateEventLogs(q.block, q.block, EventLogFilter{}, fn)
			})
			wall, cpuNS := time.Since(started).Nanoseconds(), eventPerfCPU()-cpu
			if err != nil || digest != q.digest || rows != q.rows {
				t.Fatal("final narrow reader differs from raw-source oracle", q.block, rows, err)
			}
			latencies = append(latencies, wall)
			queryResults = append(queryResults, map[string]any{"block": q.block, "wall_ns": wall, "process_cpu_ns": cpuNS, "log_rows": rows, "digest": digest, "all_return_values_equal": true})
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		results = append(results, map[string]any{"pass": pass, "p50_wall_ns": latencies[len(latencies)/2], "max_wall_ns": latencies[len(latencies)-1], "queries": queryResults, "all_return_values_equal": true})
	}
	report := map[string]any{"catalog": catalog, "source_chaindata": prior.Source, "from_block": prior.From, "to_block": prior.To, "results": results,
		"source_query_wall_ns": sourceWallNS, "source_query_process_cpu_ns": sourceCPUNS, "sample_blocks": len(queries), "original_full_range_log_rows": prior.Rows, "original_full_range_digest": prior.Digest,
		"scope": "Final production reader; 64 narrow block queries distributed over the full immutable segment, every value checked against raw source; first/warm application cache; OS cache uncontrolled; not HTTP or a second whole-segment scan"}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "results.json"), append(encoded, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("EVENT_FINAL_READER_OK", out)
}
