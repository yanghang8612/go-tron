package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func checkChunkCacheIterationStats(t *testing.T, i historyColdBenchmarkIteration, enabled bool) {
	t.Helper()
	before, after := i.SharedChunkCacheBeforeClose, i.SharedChunkCacheAfterClose
	if !enabled {
		if before != nil || after != nil {
			t.Fatal("disabled cache emitted stats")
		}
		return
	}
	if before == nil || after == nil || before.Closed || !after.Closed || before.PayloadBudgetBytes != 64<<20 || before.EntryLimit != 4096 || before.PayloadBytes > 64<<20 || before.PeakPayloadBytes > 64<<20 || before.Entries > 4096 || before.PeakEntries > 4096 || after.PayloadBytes != 0 || after.Entries != 0 {
		t.Fatalf("cache bounds/lifecycle before=%+v after=%+v", before, after)
	}
	want := *before
	want.PayloadBytes, want.Entries, want.Closed = 0, 0, true
	if *after != want {
		t.Fatal("close lost cumulative/peak counters")
	}
}

func TestDBHistoryChunkCacheCompleteTrioByteEquivalence(t *testing.T) {
	source, input, _ := parallelHistoryFixture(t)
	sourceBefore, inputBefore := historyInspectionFileHashes(t, source), historyInspectionFileHashes(t, input)
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "invalid-scope-sentinel")
	var baseline historyColdBenchmarkReport
	var files [][]byte
	for _, cfg := range []struct {
		cache, pipeline bool
		workers, cdc    int
	}{{false, false, 2, 0}, {true, false, 2, 0}, {false, true, 4, 1}, {true, true, 4, 1}, {true, true, 2, 1}, {true, true, 8, 1}} {
		t.Run(fmt.Sprintf("cache%v/read%d/pipeline%v/cdc%d", cfg.cache, cfg.workers, cfg.pipeline, cfg.cdc), func(t *testing.T) {
			opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "out"))
			opts.SharedChunkCache = cfg.cache
			opts.SharedReadPipeline = cfg.pipeline
			opts.SharedReadWorkers = cfg.workers
			opts.CDCCompressionWorkers = cfg.cdc
			opts.Iterations = 2
			report, err := benchmarkHistoryCold(context.Background(), opts)
			if err != nil || !report.Complete || !report.PhysicalVerified || report.SourceDigest.Rows != 56 || len(report.Iterations) != 2 {
				t.Fatalf("report=%+v err=%v", report, err)
			}
			for n, i := range report.Iterations {
				if !i.Equivalent || len(i.Refs) != 3 {
					t.Fatalf("incomplete trio %+v", i)
				}
				checkChunkCacheIterationStats(t, i, cfg.cache)
				if cfg.cache && (i.SharedChunkCacheBeforeClose.Hits == 0 || i.SharedChunkCacheBeforeClose.Misses == 0) {
					t.Fatal("fresh per-trio cache not exercised")
				}
				var current [][]byte
				for _, ref := range i.Refs {
					data, err := os.ReadFile(filepath.Join(opts.OutputDir, fmt.Sprintf("iteration-%02d", n+1), ref.Path))
					if err != nil {
						t.Fatal(err)
					}
					current = append(current, data)
				}
				if baseline.ManifestSHA256 == "" {
					baseline, files = report, current
				}
				if report.ManifestSHA256 != baseline.ManifestSHA256 || report.SourceDigest != baseline.SourceDigest || i.Digest != baseline.Iterations[0].Digest || i.OutputBytes != baseline.Iterations[0].OutputBytes || !reflect.DeepEqual(i.Refs, baseline.Iterations[0].Refs) || !reflect.DeepEqual(current, files) {
					t.Fatal("cache/worker/CDC settings changed complete output")
				}
			}
		})
	}
	if os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT") != "invalid-scope-sentinel" || !reflect.DeepEqual(sourceBefore, historyInspectionFileHashes(t, source)) || !reflect.DeepEqual(inputBefore, historyInspectionFileHashes(t, input)) {
		t.Fatal("diagnostic mutated environment/source")
	}
}

func TestDBHistoryChunkCacheCLIAndOptions(t *testing.T) {
	_, input, _ := coldBenchmarkFixture(t, false)
	var output bytes.Buffer
	app := &cli.App{Writer: &output, Commands: []*cli.Command{dbHistoryColdBenchmarkCommand()}}
	if err := app.Run([]string{"gtron", "benchmark-history-cold", "--input-dir", input, "--output-dir", filepath.Join(t.TempDir(), "out"), "--iterations", "1", "--shared-read-pipeline=true", "--shared-read-workers=4", "--shared-chunk-cache=true", "--cdc-compression-workers=1"}); err != nil {
		t.Fatal(err)
	}
	var report historyColdBenchmarkReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil || !report.Complete || !report.Options.SharedChunkCache || report.Options.CDCCompressionWorkers != 1 {
		t.Fatalf("CLI report=%+v err=%v", report, err)
	}
	checkChunkCacheIterationStats(t, report.Iterations[0], true)
	encoded, _ := json.Marshal(coldBenchmarkOptions(input, "unused"))
	if !bytes.Contains(encoded, []byte(`"shared_chunk_cache":false`)) || !bytes.Contains(encoded, []byte(`"cdc_compression_workers":0`)) {
		t.Fatal("default options lost explicit values")
	}
	for _, mutate := range []func(*historyColdBenchmarkOptions){func(o *historyColdBenchmarkOptions) { o.SharedChunkCache = true; o.CopyMode = "defensive" }, func(o *historyColdBenchmarkOptions) { o.CDCCompressionWorkers = -1 }, func(o *historyColdBenchmarkOptions) { o.CDCCompressionWorkers = 2 }} {
		opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "bad"))
		mutate(&opts)
		if r, err := benchmarkHistoryCold(context.Background(), opts); err == nil || r.Complete || len(r.Iterations) != 0 {
			t.Fatalf("invalid options performed work %+v %v", r, err)
		}
		if _, err := os.Stat(opts.OutputDir); !os.IsNotExist(err) {
			t.Fatal("invalid cache options created output")
		}
	}
}

type failingChunkCacheView struct {
	rawdb.StateHistoryReadView
	failure error
}

func (v failingChunkCacheView) GetReturnsOwnedBytes() bool        { return true }
func (v failingChunkCacheView) ConcurrentOwnedHistoryReads() bool { return true }
func (v failingChunkCacheView) Get([]byte) ([]byte, error)        { return nil, v.failure }

func TestDBHistoryChunkCacheBuildFailureClearsScope(t *testing.T) {
	_, input, m := coldBenchmarkFixture(t, false)
	db, err := rawdb.NewPebbleDBReadOnly(filepath.Join(input, "pebble"), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	failure := errors.New("cache build read failure")
	i, err := runHistoryColdIterationWithChunkCache(context.Background(), failingChunkCacheView{view, failure}, t.TempDir(), m.Export, 1, false, "", "auto", true, 4, true, 1)
	if !errors.Is(err, failure) || i.Equivalent {
		t.Fatalf("failed build hidden: %+v %v", i, err)
	}
	checkChunkCacheIterationStats(t, i, true)
	if i.SharedChunkCacheBeforeClose.Inserts != 0 {
		t.Fatal("failed chunk entered cache")
	}
}
