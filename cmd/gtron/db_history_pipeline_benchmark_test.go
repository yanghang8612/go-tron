package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func TestDBHistorySharedPipelineFullTrioByteEquivalence(t *testing.T) {
	source, input, _ := parallelHistoryFixture(t)
	sourceBefore, inputBefore := historyInspectionFileHashes(t, source), historyInspectionFileHashes(t, input)
	for _, format := range []string{"auto", "2", "3"} {
		t.Run(format, func(t *testing.T) {
			var serial historyColdBenchmarkReport
			var files [][]byte
			for _, pipeline := range []bool{false, true} {
				opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "out"))
				opts.SharedReadPipeline = pipeline
				opts.CompressionFormat = format
				report, err := benchmarkHistoryCold(context.Background(), opts)
				if err != nil || !report.Complete || !report.PhysicalVerified || len(report.Iterations) != 1 || !report.Iterations[0].Equivalent || len(report.Iterations[0].Refs) != 3 || report.SourceDigest.Rows != 56 {
					t.Fatalf("pipeline=%v report=%+v err=%v", pipeline, report, err)
				}
				var current [][]byte
				for _, ref := range report.Iterations[0].Refs {
					data, err := os.ReadFile(filepath.Join(opts.OutputDir, "iteration-01", ref.Path))
					if err != nil {
						t.Fatal(err)
					}
					current = append(current, data)
				}
				if !pipeline {
					serial, files = report, current
				} else if report.ManifestSHA256 != serial.ManifestSHA256 || report.SourceDigest != serial.SourceDigest || report.Iterations[0].Digest != serial.Iterations[0].Digest || report.Iterations[0].OutputBytes != serial.Iterations[0].OutputBytes || !reflect.DeepEqual(report.Iterations[0].Refs, serial.Iterations[0].Refs) || !reflect.DeepEqual(current, files) {
					t.Fatal("single trio logical or physical output changed")
				}
				if _, err := os.Stat(filepath.Join(opts.OutputDir, snapshots.ManifestFile)); !os.IsNotExist(err) {
					t.Fatal("diagnostic published a manifest")
				}
			}
		})
	}
	if !reflect.DeepEqual(sourceBefore, historyInspectionFileHashes(t, source)) || !reflect.DeepEqual(inputBefore, historyInspectionFileHashes(t, input)) {
		t.Fatal("read-only source changed")
	}
}

func TestDBHistorySharedPipelineFlagCapabilityAndCancellation(t *testing.T) {
	_, input, m := coldBenchmarkFixture(t, false)
	var stdout bytes.Buffer
	app := &cli.App{Writer: &stdout, Commands: []*cli.Command{dbHistoryColdBenchmarkCommand()}}
	if err := app.Run([]string{"gtron", "benchmark-history-cold", "--input-dir", input, "--output-dir", filepath.Join(t.TempDir(), "out"), "--iterations", "1", "--shared-read-pipeline=true"}); err != nil {
		t.Fatal(err)
	}
	var report historyColdBenchmarkReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || !report.Options.SharedReadPipeline || !report.Complete {
		t.Fatalf("CLI report: %v %+v", err, report)
	}
	encoded, err := json.Marshal(historyColdBenchmarkOptions{})
	if err != nil || !bytes.Contains(encoded, []byte(`"shared_read_pipeline":false`)) {
		t.Fatal("default false missing from report", err)
	}
	opts := coldBenchmarkOptions(input, filepath.Join(t.TempDir(), "bad"))
	opts.SharedReadPipeline = true
	opts.CopyMode = "defensive"
	if r, err := benchmarkHistoryCold(context.Background(), opts); err == nil || r.Complete {
		t.Fatal("pipeline accepted hidden ownership")
	}
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
	e := m.Export
	build := func(ctx context.Context, v rawdb.StateHistoryReadView) ([]snapshots.SegmentRef, error) {
		return snapshots.BuildDiagnosticStateHistoryTrioWithReadPipelineContext(ctx, v, t.TempDir(), e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock, "state-domain-change-pipeline.seg", "auto", etl.Options{}, true)
	}
	if refs, err := build(context.Background(), defensiveHistoryColdView{view}); !errors.Is(err, rawdb.ErrStateHistoryPipelineView) || len(refs) != 0 {
		t.Fatalf("unsupported source refs=%v err=%v", refs, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if refs, err := build(ctx, view); !errors.Is(err, context.Canceled) || len(refs) != 0 {
		t.Fatalf("canceled build refs=%v err=%v", refs, err)
	}
}
