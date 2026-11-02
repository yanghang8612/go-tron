package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Recheck the final reader against an already measured immutable event pair.
// This avoids repeating the expensive source scan/build after metadata-only
// fixes. Both source DB and prior catalog are read-only; results use a new dir.
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
	if !prior.Equal || prior.Source == "" || prior.From == 0 || prior.To < prior.From || prior.Digest == "" {
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
	if filepath.Dir(out) == catalog || out == catalog {
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
	var results []map[string]any
	for pass := 0; pass < 2; pass++ {
		started, cpu := time.Now(), eventPerfCPU()
		digest, rows, err := eventPerfDigest(func(fn func(EventLog) (bool, error)) error {
			return manager.IterateEventLogs(prior.From, prior.To, EventLogFilter{}, fn)
		})
		wall, cpuNS := time.Since(started).Nanoseconds(), eventPerfCPU()-cpu
		if err != nil || digest != prior.Digest || rows != prior.Rows {
			t.Fatal("final reader differs from original raw-source oracle", rows, err)
		}
		results = append(results, map[string]any{"pass": pass, "wall_ns": wall, "process_cpu_ns": cpuNS, "log_rows": rows, "digest": digest, "all_return_values_equal": true})
	}
	report := map[string]any{"catalog": catalog, "source_chaindata": prior.Source, "from_block": prior.From, "to_block": prior.To, "results": results,
		"scope": "Final production reader; first/warm application cache; existing full-range raw-source oracle; OS cache uncontrolled; not HTTP or whole-genesis throughput"}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "results.json"), append(encoded, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("EVENT_FINAL_READER_OK", out)
}
