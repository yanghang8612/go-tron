package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func TestDBHistoryReferenceBenchmarkPrivateComplete(t *testing.T) {
	for _, repairs := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared", true: "repair"}[repairs], func(t *testing.T) {
			source, input, manifest := coldBenchmarkFixture(t, repairs)
			sourceBefore, inputBefore := historyInspectionFileHashes(t, source), historyInspectionFileHashes(t, input)
			opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "reference"))
			opts.ReferenceContainer, opts.SharedChunkCache = true, true
			report, err := benchmarkHistoryCold(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			if !report.Complete || !report.PhysicalVerified || report.CompressionFormat != "reference-v1" || len(report.Iterations) != 1 {
				t.Fatalf("incomplete report: %+v", report)
			}
			run := report.Iterations[0]
			if !run.Equivalent || run.Digest != report.SourceDigest || run.ReferenceBuild == nil || len(run.Refs) != 3 {
				t.Fatalf("reference verification: %+v", run)
			}
			stats := run.ReferenceBuild
			if stats.SourceBlocks != manifest.Export.Blocks || stats.Rows != run.Digest.Rows || stats.PrevBytes != run.Digest.PrevBytes || stats.SpoolBytes >= stats.PrevBytes || stats.Container.PhysicalBytes == 0 || stats.Container.StoredChunkBytes >= stats.PrevBytes {
				t.Fatalf("work statistics: %+v", stats)
			}
			if repairs && stats.FallbackBlocks == 0 {
				t.Fatal("repair did not use the reported compatibility path")
			}
			if !reflect.DeepEqual(sourceBefore, historyInspectionFileHashes(t, source)) || !reflect.DeepEqual(inputBefore, historyInspectionFileHashes(t, input)) {
				t.Fatal("source or private export changed")
			}
			for _, ref := range run.Refs {
				if ref.Kind != "history" {
					continue
				}
				file, err := os.Open(filepath.Join(opts.OutputDir, "iteration-01", ref.Path))
				if err != nil {
					t.Fatal(err)
				}
				var magic [8]byte
				_, err = file.ReadAt(magic[:], 0)
				_ = file.Close()
				if err != nil || string(magic[:]) != "GTHREF01" {
					t.Fatalf("new container magic: %q %v", magic, err)
				}
			}
		})
	}
}

func TestDBHistoryReferenceBenchmarkCLIAndInvalidTopology(t *testing.T) {
	_, input, _ := coldBenchmarkFixture(t, false)
	var stdout bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stdout, Commands: []*cli.Command{dbHistoryColdBenchmarkCommand()}}
	if err := app.Run([]string{"gtron", "benchmark-history-cold", "--input-dir", input, "--output-dir", filepath.Join(t.TempDir(), "reference"), "--iterations", "1", "--reference-container", "--shared-chunk-cache"}); err != nil {
		t.Fatal(err)
	}
	var report historyColdBenchmarkReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Options.ReferenceContainer || !report.Complete || report.Iterations[0].ReferenceBuild == nil {
		t.Fatalf("CLI report: %+v", report)
	}
	for _, mutate := range []func(*historyColdBenchmarkOptions){
		func(o *historyColdBenchmarkOptions) { o.CopyMode = "defensive" },
		func(o *historyColdBenchmarkOptions) { o.CDCCompressionWorkers = 1 },
		func(o *historyColdBenchmarkOptions) { o.CompressionFormat = "3" },
	} {
		opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "invalid"))
		opts.ReferenceContainer = true
		mutate(&opts)
		if err := validateHistoryColdOptions(opts); err == nil {
			t.Fatalf("invalid topology accepted: %+v", opts)
		}
	}
}

func TestDBHistoryReferenceBenchmarkPipelined(t *testing.T) {
	for _, repair := range []bool{false, true} {
		_, input, _ := coldBenchmarkFixture(t, repair)
		var oracle []snapshots.SegmentRef
		for _, workers := range []int{0, 2, 4, 8} {
			opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "reference"))
			opts.ReferenceContainer, opts.SharedChunkCache = true, true
			if workers != 0 {
				opts.SharedReadPipeline, opts.SharedReadWorkers = true, workers
			}
			report, err := benchmarkHistoryCold(context.Background(), opts)
			if err != nil || !report.Complete || !report.Iterations[0].Equivalent {
				t.Fatalf("workers=%d repair=%v %+v %v", workers, repair, report, err)
			}
			refs := report.Iterations[0].Refs
			if oracle == nil {
				oracle = refs
			} else if !reflect.DeepEqual(oracle, refs) {
				t.Fatalf("worker topology changed output %d", workers)
			}
		}
	}
}
