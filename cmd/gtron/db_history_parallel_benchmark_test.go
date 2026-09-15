package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/urfave/cli/v2"
)

func parallelHistoryFixture(t *testing.T) (string, string, historyRangeExportManifest) {
	t.Helper()
	source := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(source), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	rawdb.SetStateHistoryCrossBlockDedup(true)
	defer rawdb.SetStateHistoryCrossBlockDedup(false)
	previous := make([]byte, 128<<10)
	_, _ = rand.New(rand.NewSource(202609151)).Read(previous)
	var parent common.Hash
	var nextTx uint64 = 1
	for n := uint64(1); n <= 17; n++ {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n), Timestamp: int64(n * 3000), ParentHash: parent.Bytes()}}})
		parent = block.Hash()
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		begin, end := nextTx, nextTx+n%3
		nextTx = end + 1
		if err := rawdb.WriteStateTxRange(db, n, block.Hash(), begin, end); err != nil {
			t.Fatal(err)
		}
		if n%7 != 0 {
			buffer := blockbuffer.New(db)
			buffer.BeginBlock(block.Hash(), n)
			row := &rawdb.StateDomainChange{BlockNum: n, TxNum: begin, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 1}, Generation: n % 3, Domain: kvdomains.SystemDelegation, Key: []byte("large"), PrevExists: true, Prev: previous}
			empty := *row
			empty.TxNum, empty.Seq, empty.Key, empty.Prev = end, 2, []byte("empty"), nil
			absent := empty
			absent.Seq, absent.Key, absent.PrevExists = 3, []byte("absent"), false
			duplicate := *row
			duplicate.Seq = 4
			// Keep the source transaction order canonical while preserving a
			// duplicate archival value at the same transaction position.
			duplicate.TxNum = end
			rows := []*rawdb.StateDomainChange{row, &empty, &absent, &duplicate}
			if err := rawdb.WriteStateDomainChangeBlockRows(buffer, rows); err != nil {
				t.Fatal(err)
			}
			buffer.CommitBlock()
			if err := buffer.Flush(db); err != nil {
				t.Fatal(err)
			}
			buffer.Discard()
		}
		if n == 1 {
			if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotBuild, n, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
		if n == 17 {
			if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, n, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "input")
	m, text, err := runHistoryRangeTest(context.Background(), source, input, "--blocks", "16")
	if err != nil || !m.Export.Complete || m.Export.Blocks != 16 || m.Export.UniqueChunkCount == 0 {
		t.Fatalf("parallel fixture: %+v %v %s", m.Export, err, text)
	}
	return source, input, m
}

func parallelHistoryOptions(input, output string, workers, segments int) historyParallelOptions {
	return historyParallelOptions{InputDir: input, OutputDir: output, Workers: workers, Segments: segments, Iterations: 1, MaxDuration: 2 * time.Minute}
}

func TestDBHistoryParallelCompletePartitionsReadOnly(t *testing.T) {
	source, input, _ := parallelHistoryFixture(t)
	sourceBefore, inputBefore := historyInspectionFileHashes(t, source), historyInspectionFileHashes(t, input)
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "invalid-parallel-control")
	var original snapshots.StateHistoryDiagnosticDigest
	var manifests string
	for _, counts := range [][2]int{{1, 1}, {1, 2}, {2, 2}, {1, 4}, {2, 4}, {4, 4}} {
		name := fmt.Sprintf("w%d-s%d", counts[0], counts[1])
		t.Run(name, func(t *testing.T) {
			opts := parallelHistoryOptions(input, filepath.Join(t.TempDir(), "output"), counts[0], counts[1])
			opts.CPUProfile = counts == [2]int{2, 2}
			report, err := benchmarkHistoryParallel(context.Background(), opts)
			if err != nil || !report.Complete || !report.PhysicalVerified || !report.PartitionCoverageVerified || report.SourceDigest.TxRanges != 16 || len(report.Ranges) != counts[1] || len(report.Iterations) != 1 {
				t.Fatalf("report=%+v err=%v", report, err)
			}
			if original.RowSHA256 == "" {
				original, manifests = report.SourceDigest, report.ManifestSHA256
			} else if report.SourceDigest != original || report.ManifestSHA256 != manifests {
				t.Fatal("configuration changed source identity or logical content")
			}
			if report.SourceDigest.Rows != 56 || report.SourceDigest.LargePrevRows != 28 || report.SourceDigest.LargePrevBytes != 28*(128<<10) {
				t.Fatalf("duplicate/empty/missing source shape changed: %+v", report.SourceDigest)
			}
			if report.Budget.ETLPerCollectorBytes*2*counts[0] != 64<<20 || report.CopyMode != "owned" || report.CompressionFormat != "auto" || !strings.Contains(report.Budget.LimitScope, "not a hard heap/RSS") {
				t.Fatal("aggregate thresholds or fixed policy incorrect")
			}
			iteration := report.Iterations[0]
			if !iteration.Equivalent || iteration.BuildWallNanos <= 0 || iteration.VerificationWallNanos <= 0 || iteration.BuildAllocatedBytes == 0 || iteration.OutputBytes == 0 || iteration.Statistics != historyParallelStatisticsOnly(original) {
				t.Fatalf("group accounting/union: %+v", iteration)
			}
			for i, part := range iteration.Segments {
				if !part.Started || !part.Equivalent || len(part.Refs) != 3 || part.Digest != report.Ranges[i].SourceDigest || part.OutputBytes == 0 {
					t.Fatalf("incomplete part: %+v", part)
				}
			}
			if opts.CPUProfile && (report.CPUProfileSHA256 == "" || !strings.Contains(report.CPUProfileScope, "group build")) {
				t.Fatal("missing first-group CPU profile identity")
			}
			data, err := os.ReadFile(filepath.Join(opts.OutputDir, "report.json"))
			if err != nil {
				t.Fatal(err)
			}
			var durable historyParallelReport
			if err := json.Unmarshal(data, &durable); err != nil || !reflect.DeepEqual(durable, report) {
				t.Fatalf("durable report: %v", err)
			}
			if _, err := os.Stat(filepath.Join(opts.OutputDir, snapshots.ManifestFile)); !os.IsNotExist(err) {
				t.Fatal("diagnostic published a production manifest")
			}
		})
	}
	if os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT") != "invalid-parallel-control" || !reflect.DeepEqual(sourceBefore, historyInspectionFileHashes(t, source)) || !reflect.DeepEqual(inputBefore, historyInspectionFileHashes(t, input)) {
		t.Fatal("diagnostic changed environment, source or captured private input")
	}
}

func TestDBHistoryParallelPlanBoundsAndStatistics(t *testing.T) {
	var e rawdb.StateHistoryRangeExportReport
	e.FromBlock, e.ToBlock, e.Blocks, e.FromTxNum, e.ToTxNum = math.MaxUint64-15, math.MaxUint64, 16, math.MaxUint64-31, math.MaxUint64
	for i := uint64(0); i < 16; i++ {
		e.BlockDetails = append(e.BlockDetails, rawdb.StateHistoryRangeExportBlock{Block: e.FromBlock + i, CanonicalHash: "0x" + strings.Repeat("01", 32), BeginTxNum: e.FromTxNum + i*2, EndTxNum: e.FromTxNum + i*2 + 1})
	}
	for _, n := range []int{1, 2, 4} {
		plans, err := planHistoryParallelRanges(e, n)
		if err != nil || len(plans) != n || plans[0].FromBlock != e.FromBlock || plans[n-1].ToBlock != e.ToBlock || plans[n-1].ToTxNum != e.ToTxNum {
			t.Fatalf("max boundary: %+v %v", plans, err)
		}
		for i := 1; i < len(plans); i++ {
			if plans[i-1].ToBlock+1 != plans[i].FromBlock || plans[i-1].ToTxNum+1 != plans[i].FromTxNum {
				t.Fatal("partition gap")
			}
		}
	}
	for _, mutate := range []func(*rawdb.StateHistoryRangeExportReport){
		func(e *rawdb.StateHistoryRangeExportReport) { e.Blocks = 15 },
		func(e *rawdb.StateHistoryRangeExportReport) { e.FromTxNum++ },
		func(e *rawdb.StateHistoryRangeExportReport) { e.BlockDetails[4].Block-- },
		func(e *rawdb.StateHistoryRangeExportReport) { e.BlockDetails[4].BeginTxNum-- },
		func(e *rawdb.StateHistoryRangeExportReport) { e.BlockDetails[4].EndTxNum = math.MaxUint64 },
		func(e *rawdb.StateHistoryRangeExportReport) { e.BlockDetails[4].CanonicalHash = "bad" },
		func(e *rawdb.StateHistoryRangeExportReport) { e.DeclaredDecodedBytes++ },
		func(e *rawdb.StateHistoryRangeExportReport) { e.BlockDetails[4].DeclaredDecodedBytes = math.MaxUint64 },
	} {
		bad := e
		bad.BlockDetails = append([]rawdb.StateHistoryRangeExportBlock(nil), e.BlockDetails...)
		mutate(&bad)
		if _, err := planHistoryParallelRanges(bad, 4); err == nil {
			t.Fatal("invalid plan accepted")
		}
	}
	if _, err := historyParallelStatistics([]snapshots.StateHistoryDiagnosticDigest{{Rows: math.MaxUint64}, {Rows: 1}}); err == nil {
		t.Fatal("statistics overflow accepted")
	}
	stats, err := historyParallelStatistics([]snapshots.StateHistoryDiagnosticDigest{{Rows: 1, MaxPrevBytes: 7, RowSHA256: "not-composable"}, {Rows: 2, MaxPrevBytes: 4, TxRangeSHA256: "not-composable"}})
	if err != nil || stats.Rows != 3 || stats.MaxPrevBytes != 7 || stats.RowSHA256 != "" || stats.TxRangeSHA256 != "" {
		t.Fatalf("stats are not sums/max only: %+v %v", stats, err)
	}
}

func TestDBHistoryParallelCancellationJoinsWorkers(t *testing.T) {
	failed := errors.New("planned worker failure")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan int, 2)
	failNow, releaseSecond := make(chan struct{}), make(chan struct{})
	completed := make(chan []error, 1)
	var active, peak atomic.Int32
	go func() {
		completed <- executeHistoryParallel(ctx, 4, 2, func(ctx context.Context, i int) error {
			n := active.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			defer active.Add(-1)
			started <- i
			if i == 0 {
				<-failNow
				return failed
			}
			<-ctx.Done()
			<-releaseSecond // model a non-interruptible operation's final return
			return ctx.Err()
		})
	}()
	<-started
	<-started
	close(failNow)
	// The second operation cannot exit, so the group cannot have returned.
	select {
	case <-completed:
		t.Fatal("returned before admitted worker exited")
	default:
	}
	close(releaseSecond)
	errs := <-completed
	if active.Load() != 0 || peak.Load() != 2 || !errors.Is(errs[0], failed) || !errors.Is(errs[1], context.Canceled) || !errors.Is(errs[2], context.Canceled) || !errors.Is(errs[3], context.Canceled) {
		t.Fatalf("join/cancel/admission: peak=%d active=%d errs=%v", peak.Load(), active.Load(), errs)
	}
	cancel()
	called := false
	errs = executeHistoryParallel(ctx, 4, 4, func(context.Context, int) error { called = true; return nil })
	if called || len(errs) != 4 {
		t.Fatal("pre-canceled group launched work")
	}
	for _, err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Fatal("missing canceled result")
		}
	}
}

func TestDBHistoryParallelSharedOpenCloseAndFailures(t *testing.T) {
	var sequence []string
	viewErr, dbErr := errors.New("view close"), errors.New("db close")
	opened := historyColdInput{release: func() error { sequence = append(sequence, "view"); return viewErr }, closeDB: func() error { sequence = append(sequence, "db"); return dbErr }}
	err := opened.Close()
	if !errors.Is(err, viewErr) || !errors.Is(err, dbErr) || !reflect.DeepEqual(sequence, []string{"view", "db"}) || opened.Close() != nil || len(sequence) != 2 {
		t.Fatalf("release/close order or ownership: %v %v", sequence, err)
	}
	source, input, _ := parallelHistoryFixture(t)
	for _, output := range []string{filepath.Join(input, "bad"), filepath.Join(source, "bad"), input} {
		report, err := benchmarkHistoryParallel(context.Background(), parallelHistoryOptions(input, output, 1, 1))
		if err == nil || report.Complete {
			t.Fatal("unsafe output path accepted")
		}
	}
	opts := parallelHistoryOptions(input, filepath.Join(t.TempDir(), "canceled"), 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if report, err := benchmarkHistoryParallel(ctx, opts); !errors.Is(err, context.Canceled) || report.Complete {
		t.Fatal("pre-canceled command accepted")
	}
	if _, err := os.Stat(opts.OutputDir); !os.IsNotExist(err) {
		t.Fatal("pre-canceled command created output")
	}
	for _, mutate := range []func(*historyParallelOptions){
		func(o *historyParallelOptions) { o.Workers = 3 }, func(o *historyParallelOptions) { o.Segments = 3 },
		func(o *historyParallelOptions) { o.Workers = 4; o.Segments = 2 }, func(o *historyParallelOptions) { o.Iterations = 4 },
		func(o *historyParallelOptions) { o.MaxDuration = 5*time.Minute + 1 }, func(o *historyParallelOptions) { o.MaxDuration = 0 },
	} {
		bad := opts
		mutate(&bad)
		if err := validateHistoryParallelOptions(bad); err == nil {
			t.Fatal("invalid bounded configuration accepted")
		}
	}
	// All existing physical/canonical checks are shared with serial replay.
	db, err := rawdb.NewPebbleDB(filepath.Join(input, "pebble"), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("extra-unlisted-row"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := benchmarkHistoryParallel(context.Background(), parallelHistoryOptions(input, filepath.Join(t.TempDir(), "corrupt"), 4, 4))
	if err == nil || report.Complete || report.PhysicalVerified || len(report.Iterations) != 0 {
		t.Fatal("corrupt physical input launched workers")
	}
}

func TestDBHistoryParallelCLI(t *testing.T) {
	var out bytes.Buffer
	app := &cli.App{Writer: &out, ErrWriter: &out, Commands: []*cli.Command{dbHistoryParallelBenchmarkCommand()}}
	err := app.Run([]string{"gtron", "benchmark-history-parallel", "--input-dir", "/missing", "--output-dir", "/missing-output", "--workers", "4", "--segments", "2"})
	var report historyParallelReport
	if err == nil || json.Unmarshal(out.Bytes(), &report) != nil || report.Complete || report.Options.Workers != 4 || report.Options.Segments != 2 {
		t.Fatalf("CLI options/report: %s %v", out.String(), err)
	}
	command := dbHistoryParallelBenchmarkCommand()
	for _, flag := range command.Flags {
		for _, name := range flag.Names() {
			if name == "copy-mode" || name == "compression-format" || name == "datadir" {
				t.Fatalf("unexpected mutable production policy flag %s", name)
			}
		}
	}
}

func TestDBHistoryParallelFreshManifestPlanAndETL(t *testing.T) {
	_, input, manifest := parallelHistoryFixture(t)
	opened, err := openHistoryColdInput(input, filepath.Join(t.TempDir(), "opened"))
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	ctx := context.Background()
	verified, err := verifyHistoryColdInputReport(ctx, opened.view, manifest.Export)
	if err != nil || !reflect.DeepEqual(verified, manifest.Export) {
		t.Fatalf("fresh report: %v", err)
	}
	bad := manifest.Export
	bad.BlockDetails = append([]rawdb.StateHistoryRangeExportBlock(nil), verified.BlockDetails...)
	bad.BlockDetails[4].BeginTxNum++
	if _, err := verifyHistoryColdInputReport(ctx, opened.view, bad); err == nil {
		t.Fatal("manifest block metadata supplied the plan without fresh verification")
	}
	bad = manifest.Export
	bad.Codecs = make(map[string]rawdb.StateHistoryRangeExportCodec)
	for key, value := range manifest.Export.Codecs {
		value.Rows++
		bad.Codecs[key] = value
	}
	if _, err := verifyHistoryColdInputReport(ctx, opened.view, bad); err == nil {
		t.Fatal("unverified manifest codec counts accepted")
	}
	plans, err := planHistoryParallelRanges(verified, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := range plans {
		p := &plans[i]
		p.SourceDigest, err = snapshots.DigestHotStateHistoryContext(ctx, opened.view, filepath.Join(opened.output, fmt.Sprintf("digest-%d", i)), p.FromTxNum, p.ToTxNum, p.FromBlock, p.ToBlock, rawdb.StateHistoryRangeExportMaxDecodedBytes)
		if err != nil {
			t.Fatal(err)
		}
	}
	// A one-byte threshold forces dictionary/posting external runs. This is a
	// correctness test, not one of the command's fixed production comparisons.
	spilled, err := runHistoryParallelIteration(ctx, opened.view, t.TempDir(), plans, 2, 1, "")
	if err != nil {
		t.Fatalf("forced ETL spill: %v", err)
	}
	for i, p := range plans {
		refs, err := snapshots.BuildDiagnosticStateHistoryTrioContext(ctx, opened.view, t.TempDir(), p.FromTxNum, p.ToTxNum, p.FromBlock, p.ToBlock, "history/state-domain-change-range.seg", "auto")
		if err != nil || !reflect.DeepEqual(refs, spilled.Segments[i].Refs) {
			t.Fatalf("explicit threshold changed complete bytes/default builder: %v", err)
		}
	}
	plans[1].SourceDigest.RowSHA256 = strings.Repeat("00", 32)
	mismatch, err := runHistoryParallelIteration(ctx, opened.view, t.TempDir(), plans, 2, 1, "")
	if err == nil || !strings.Contains(err.Error(), "digest mismatch") || !mismatch.Segments[0].Equivalent || mismatch.Segments[1].Equivalent {
		t.Fatalf("cold comparison failed to reject mismatch: %+v %v", mismatch, err)
	}
}
