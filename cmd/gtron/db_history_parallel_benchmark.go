package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

const historyParallelBlocks = uint64(16)
const historyParallelETLThreshold = 64 << 20

type historyParallelOptions struct {
	InputDir    string        `json:"input_dir"`
	OutputDir   string        `json:"output_dir"`
	Workers     int           `json:"workers"`
	Segments    int           `json:"segments"`
	Iterations  int           `json:"iterations"`
	MaxDuration time.Duration `json:"max_duration_ns"`
	CPUProfile  bool          `json:"cpu_profile"`
}

type historyParallelRange struct {
	Index                int                                    `json:"index"`
	FromBlock            uint64                                 `json:"from_block"`
	ToBlock              uint64                                 `json:"to_block"`
	FromTxNum            uint64                                 `json:"from_tx_num"`
	ToTxNum              uint64                                 `json:"to_tx_num"`
	DeclaredDecodedBytes uint64                                 `json:"declared_decoded_bytes"`
	SourceDigest         snapshots.StateHistoryDiagnosticDigest `json:"source_digest"`
}

type historyParallelSegment struct {
	Index          int                                    `json:"index"`
	Started        bool                                   `json:"started"`
	BuildWallNanos int64                                  `json:"build_wall_nanos"`
	Refs           []snapshots.SegmentRef                 `json:"refs"`
	OutputBytes    uint64                                 `json:"output_bytes"`
	Digest         snapshots.StateHistoryDiagnosticDigest `json:"digest"`
	Equivalent     bool                                   `json:"equivalent"`
	Error          string                                 `json:"error,omitempty"`
}

type historyParallelIteration struct {
	Iteration             int                                    `json:"iteration"`
	BuildWallNanos        int64                                  `json:"build_wall_nanos"`
	BuildAllocatedBytes   uint64                                 `json:"build_allocated_bytes"`
	BuildAllocations      uint64                                 `json:"build_allocations"`
	BuildGCCycles         uint32                                 `json:"build_gc_cycles"`
	VerificationWallNanos int64                                  `json:"verification_wall_nanos"`
	OutputBytes           uint64                                 `json:"output_bytes"`
	Segments              []historyParallelSegment               `json:"segments"`
	Statistics            snapshots.StateHistoryDiagnosticDigest `json:"statistics"`
	Equivalent            bool                                   `json:"equivalent"`
	Error                 string                                 `json:"error,omitempty"`
}

type historyParallelBudget struct {
	ETLAggregateThresholdBytes  int    `json:"etl_aggregate_threshold_bytes"`
	ETLPerCollectorBytes        int    `json:"etl_per_collector_bytes"`
	ETLCollectorsPerWorker      int    `json:"etl_collectors_per_worker"`
	MaxWorkers                  int    `json:"max_workers"`
	MaxInputPhysicalBytes       uint64 `json:"max_input_physical_bytes"`
	MaxInputPhysicalRows        uint64 `json:"max_input_physical_rows"`
	MaxInputDeclaredBytes       uint64 `json:"max_input_declared_bytes"`
	SharedPackMaxBytesPerWorker uint64 `json:"shared_pack_max_bytes_per_worker"`
	V6KeyTableMaxPerWorker      uint64 `json:"v6_key_table_max_bytes_per_worker"`
	CDCPayloadMaxPerWorker      uint64 `json:"cdc_dictionary_payload_max_bytes_per_worker"`
	LimitScope                  string `json:"limit_scope"`
}

type historyParallelReport struct {
	Version                     int                                    `json:"version"`
	Complete                    bool                                   `json:"complete"`
	StartedUTC                  string                                 `json:"started_utc"`
	ElapsedNanos                int64                                  `json:"elapsed_nanos"`
	Options                     historyParallelOptions                 `json:"options"`
	GoVersion                   string                                 `json:"go_version"`
	GOOS                        string                                 `json:"goos"`
	GOARCH                      string                                 `json:"goarch"`
	GOMAXPROCS                  int                                    `json:"gomaxprocs"`
	CopyMode                    string                                 `json:"copy_mode"`
	CompressionFormat           string                                 `json:"compression_format"`
	Scope                       string                                 `json:"scope"`
	ManifestSHA256              string                                 `json:"manifest_file_sha256"`
	Export                      rawdb.StateHistoryRangeExportReport    `json:"export"`
	Budget                      historyParallelBudget                  `json:"budget"`
	PhysicalVerified            bool                                   `json:"physical_verified"`
	PartitionCoverageVerified   bool                                   `json:"partition_coverage_verified"`
	SourceDigest                snapshots.StateHistoryDiagnosticDigest `json:"source_digest"`
	SourceVerificationWallNanos int64                                  `json:"source_verification_wall_nanos"`
	Ranges                      []historyParallelRange                 `json:"ranges"`
	Iterations                  []historyParallelIteration             `json:"iterations"`
	CPUProfileSHA256            string                                 `json:"cpu_profile_sha256,omitempty"`
	CPUProfileScope             string                                 `json:"cpu_profile_scope,omitempty"`
	Error                       string                                 `json:"error,omitempty"`
}

func dbHistoryParallelBenchmarkCommand() *cli.Command {
	return &cli.Command{Name: "benchmark-history-parallel", Usage: "Compare bounded parallel cold trios on the same authenticated private 16-block export",
		Description: "Offline only. One real read-only Pebble snapshot serves all workers; owned Get and auto compression are fixed. Segments split the same 16 blocks into contiguous canonical block/tx ranges. The timer covers all complete trio builds and joining workers; physical/source/full cold verification runs outside it. There is no production manifest publication, pruning, GC or node. Aggregate ETL thresholds total 64MiB; codec, key tables, allocator and pools are additional memory. Deadline cancellation is cooperative and waits for every admitted worker. Read caches warm across iterations; allocations/profile are process-wide.",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "input-dir", Required: true}, &cli.StringFlag{Name: "output-dir", Required: true},
			&cli.IntFlag{Name: "workers", Value: 1}, &cli.IntFlag{Name: "segments", Value: 1},
			&cli.IntFlag{Name: "iterations", Value: 3}, &cli.DurationFlag{Name: "max-duration", Value: 5 * time.Minute},
			&cli.BoolFlag{Name: "cpu-profile", Usage: "Profile the first complete group build; excludes all external verification"},
		}, Action: dbHistoryParallelBenchmarkCmd}
}

func dbHistoryParallelBenchmarkCmd(ctx *cli.Context) error {
	opts := historyParallelOptions{InputDir: ctx.String("input-dir"), OutputDir: ctx.String("output-dir"), Workers: ctx.Int("workers"), Segments: ctx.Int("segments"), Iterations: ctx.Int("iterations"), MaxDuration: ctx.Duration("max-duration"), CPUProfile: ctx.Bool("cpu-profile")}
	report, err := benchmarkHistoryParallel(ctx.Context, opts)
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	return errors.Join(err, json.NewEncoder(writer).Encode(report))
}

func validHistoryParallelCount(n int) bool { return n == 1 || n == 2 || n == 4 }

func validateHistoryParallelOptions(opts historyParallelOptions) error {
	if !filepath.IsAbs(opts.InputDir) || !filepath.IsAbs(opts.OutputDir) || !validHistoryParallelCount(opts.Workers) || !validHistoryParallelCount(opts.Segments) || opts.Workers > opts.Segments || opts.Iterations < 1 || opts.Iterations > 3 || opts.MaxDuration <= 0 || opts.MaxDuration > 5*time.Minute {
		return errors.New("history parallel benchmark requires absolute input/output paths, workers and segments in {1,2,4}, workers<=segments, 1..3 iterations, and duration (0,5m]")
	}
	return nil
}

// This validates a partition of the already physically re-exported manifest.
// verifyHistoryColdInput is the canonical hash/parent/tx binding authority; this
// planner never substitutes numeric continuity for that source authentication.
func planHistoryParallelRanges(e rawdb.StateHistoryRangeExportReport, segments int) ([]historyParallelRange, error) {
	if !validHistoryParallelCount(segments) || e.FromBlock > e.ToBlock || e.ToBlock-e.FromBlock != historyParallelBlocks-1 || e.Blocks != historyParallelBlocks || len(e.BlockDetails) != int(historyParallelBlocks) || e.FromTxNum > e.ToTxNum {
		return nil, errors.New("history parallel benchmark requires the complete original 16-block export")
	}
	var declared uint64
	for i, block := range e.BlockDetails {
		hash, err := hex.DecodeString(strings.TrimPrefix(block.CanonicalHash, "0x"))
		if block.Block != e.FromBlock+uint64(i) || block.BeginTxNum > block.EndTxNum || err != nil || len(hash) != 32 || block.DeclaredDecodedBytes > rawdb.StateHistoryRangeExportMaxDecodedBytes-declared {
			return nil, errors.New("invalid parallel block plan identity, bounds or declared bytes")
		}
		if i > 0 {
			previous := e.BlockDetails[i-1]
			if previous.EndTxNum == math.MaxUint64 || block.BeginTxNum != previous.EndTxNum+1 {
				return nil, errors.New("parallel block plan tx ranges are not contiguous")
			}
		}
		declared += block.DeclaredDecodedBytes
	}
	if e.BlockDetails[0].BeginTxNum != e.FromTxNum || e.BlockDetails[len(e.BlockDetails)-1].EndTxNum != e.ToTxNum || declared != e.DeclaredDecodedBytes {
		return nil, errors.New("parallel block plan does not cover complete export bounds and bytes")
	}
	plans := make([]historyParallelRange, segments)
	width := int(historyParallelBlocks) / segments
	for i := range plans {
		blocks := e.BlockDetails[i*width : (i+1)*width]
		plan := historyParallelRange{Index: i, FromBlock: blocks[0].Block, ToBlock: blocks[len(blocks)-1].Block, FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum}
		for _, block := range blocks {
			plan.DeclaredDecodedBytes += block.DeclaredDecodedBytes
		}
		plans[i] = plan
	}
	return plans, nil
}

// Digests cannot be combined by concatenating their SHA strings. This sums only
// statistics (with max for MaxPrevBytes). The complete source digest, exact
// partition coverage and independent per-range hot/cold digests prove the union.
func historyParallelStatistics(parts []snapshots.StateHistoryDiagnosticDigest) (snapshots.StateHistoryDiagnosticDigest, error) {
	var sum snapshots.StateHistoryDiagnosticDigest
	for _, d := range parts {
		targets := []*uint64{&sum.Rows, &sum.PayloadBytes, &sum.PrevBytes, &sum.LargePrevRows, &sum.LargePrevBytes, &sum.DelegationRows, &sum.DelegationPrevBytes, &sum.TxRanges}
		values := []uint64{d.Rows, d.PayloadBytes, d.PrevBytes, d.LargePrevRows, d.LargePrevBytes, d.DelegationRows, d.DelegationPrevBytes, d.TxRanges}
		for i, target := range targets {
			if values[i] > math.MaxUint64-*target {
				return sum, errors.New("parallel digest statistics overflow")
			}
			*target += values[i]
		}
		sum.MaxPrevBytes = max(sum.MaxPrevBytes, d.MaxPrevBytes)
	}
	return sum, nil
}

func historyParallelStatisticsOnly(d snapshots.StateHistoryDiagnosticDigest) snapshots.StateHistoryDiagnosticDigest {
	d.RowSHA256, d.TxRangeSHA256 = "", ""
	return d
}

func benchmarkHistoryParallel(parent context.Context, opts historyParallelOptions) (report historyParallelReport, resultErr error) {
	start := time.Now()
	report = historyParallelReport{Version: 1, StartedUTC: start.UTC().Format(time.RFC3339Nano), Options: opts, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), CopyMode: "owned", CompressionFormat: "auto",
		Scope: "fixed original 16 blocks; complete trio group wall incl. join, no production manifest publication/prune/GC; verification outside timing; allocations/profile process-wide; warm read caches; cooperative deadline"}
	var output string
	defer func() {
		report.ElapsedNanos = time.Since(start).Nanoseconds()
		report.Complete = resultErr == nil
		if resultErr != nil {
			report.Error = resultErr.Error()
		}
		if output != "" {
			data, err := json.MarshalIndent(report, "", "  ")
			if err == nil {
				err = writeHistoryDiagnosticFile(filepath.Join(output, "report.json"), append(data, '\n'))
			}
			if err != nil {
				resultErr = errors.Join(resultErr, err)
				report.Complete, report.Error = false, resultErr.Error()
			}
		}
	}()
	if err := validateHistoryParallelOptions(opts); err != nil {
		return report, err
	}
	report.Budget = historyParallelBudget{ETLAggregateThresholdBytes: historyParallelETLThreshold, ETLPerCollectorBytes: historyParallelETLThreshold / (2 * opts.Workers), ETLCollectorsPerWorker: 2, MaxWorkers: 4,
		MaxInputPhysicalBytes: rawdb.StateHistoryRangeExportMaxBytes, MaxInputPhysicalRows: rawdb.StateHistoryRangeExportMaxRows, MaxInputDeclaredBytes: rawdb.StateHistoryRangeExportMaxDecodedBytes,
		SharedPackMaxBytesPerWorker: 128 << 20, V6KeyTableMaxPerWorker: 512 << 20, CDCPayloadMaxPerWorker: 64 << 20,
		LimitScope: "64MiB is aggregate configured ETL spill thresholds, not a hard heap/RSS bound; append overshoot, entry/order/arena overhead, retained pools, key dedup/table, codecs, frames and compression pipelines are additional; stated per-worker maxima multiply by admitted workers; no process memory reservation"}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, opts.MaxDuration)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return report, err
	}
	opened, err := openHistoryColdInput(opts.InputDir, opts.OutputDir)
	output, report.ManifestSHA256, report.Export = opened.output, opened.manifestSHA256, opened.manifest.Export
	defer func() { resultErr = errors.Join(resultErr, opened.Close()) }()
	if err != nil {
		return report, err
	}
	// Only this opener-created real private Pebble snapshot is shared between
	// workers. Pinned/owned markers alone do not certify arbitrary view safety.
	view := opened.view
	owned, ok := view.(pointread.OwnedKeyValueReader)
	if !ok || !owned.GetReturnsOwnedBytes() || !view.IsPinnedKeyValueView() {
		return report, errors.New("parallel diagnostic requires its private pinned Pebble snapshot with owned Get")
	}
	verified, err := verifyHistoryColdInputReport(ctx, view, report.Export)
	if err != nil {
		return report, err
	}
	report.PhysicalVerified = true
	report.Ranges, err = planHistoryParallelRanges(verified, opts.Segments)
	if err != nil {
		return report, err
	}
	verificationStarted := time.Now()
	e := report.Export
	report.SourceDigest, err = snapshots.DigestHotStateHistoryContext(ctx, view, filepath.Join(output, "source-etl"), e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock, rawdb.StateHistoryRangeExportMaxDecodedBytes)
	if err != nil {
		return report, err
	}
	parts := make([]snapshots.StateHistoryDiagnosticDigest, len(report.Ranges))
	for i := range report.Ranges {
		plan := &report.Ranges[i]
		plan.SourceDigest, err = snapshots.DigestHotStateHistoryContext(ctx, view, filepath.Join(output, fmt.Sprintf("source-etl-%02d", i)), plan.FromTxNum, plan.ToTxNum, plan.FromBlock, plan.ToBlock, rawdb.StateHistoryRangeExportMaxDecodedBytes)
		if err != nil {
			return report, err
		}
		if plan.SourceDigest.TxRanges != plan.ToBlock-plan.FromBlock+1 {
			return report, errors.New("source partition missing block tx ranges")
		}
		parts[i] = plan.SourceDigest
	}
	statistics, err := historyParallelStatistics(parts)
	if err != nil || statistics != historyParallelStatisticsOnly(report.SourceDigest) || report.SourceDigest.TxRanges != historyParallelBlocks {
		return report, errors.Join(err, errors.New("source partition statistics differ from the complete input"))
	}
	report.SourceVerificationWallNanos = time.Since(verificationStarted).Nanoseconds()
	report.PartitionCoverageVerified = true
	for n := 1; n <= opts.Iterations; n++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		dir := filepath.Join(output, fmt.Sprintf("iteration-%02d", n))
		if err := os.Mkdir(dir, 0700); err != nil {
			return report, err
		}
		profilePath := ""
		if opts.CPUProfile && n == 1 {
			profilePath = filepath.Join(output, "cpu.pprof")
		}
		iteration, err := runHistoryParallelIteration(ctx, view, dir, report.Ranges, opts.Workers, report.Budget.ETLPerCollectorBytes, profilePath)
		iteration.Iteration = n
		if err == nil && iteration.Statistics != historyParallelStatisticsOnly(report.SourceDigest) {
			err = errors.New("cold union statistics differ from complete source")
		}
		if profilePath != "" {
			profile, profileErr := readHistoryDiagnosticFile(profilePath, 64<<20)
			if profileErr == nil {
				hash := sha256.Sum256(profile)
				report.CPUProfileSHA256 = hex.EncodeToString(hash[:])
				report.CPUProfileScope = "iteration 1 complete trio group build and join only; process-wide; no source/cold external verification or production publication"
			}
			err = errors.Join(err, profileErr)
		}
		iteration.Equivalent = err == nil
		if err != nil {
			iteration.Error = err.Error()
		}
		report.Iterations = append(report.Iterations, iteration)
		if err != nil {
			return report, err
		}
	}
	return report, ctx.Err()
}

// A bounded worker group, not independently admitted maintenance tasks. Every
// started call returns before this function does, including after cancellation.
// Results remain in plan order. The caller owns the snapshot until after join.
func executeHistoryParallel(ctx context.Context, count, workers int, build func(context.Context, int) error) []error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make([]error, count)
	var mu sync.Mutex
	var wg sync.WaitGroup
	next := 0
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if next == count || ctx.Err() != nil {
					mu.Unlock()
					return
				}
				i := next
				next++
				mu.Unlock()
				errs[i] = build(ctx, i)
				if errs[i] != nil {
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	for i := next; i < count; i++ {
		errs[i] = fmt.Errorf("segment %d not started: %w", i, ctx.Err())
	}
	return errs
}

func runHistoryParallelIteration(ctx context.Context, view rawdb.StateHistoryReadView, dir string, plans []historyParallelRange, workers, etlThreshold int, profilePath string) (out historyParallelIteration, resultErr error) {
	out.Segments = make([]historyParallelSegment, len(plans))
	dirs := make([]string, len(plans))
	for i := range plans {
		dirs[i] = filepath.Join(dir, fmt.Sprintf("segment-%02d", i))
		out.Segments[i].Index = i
		if err := os.Mkdir(dirs[i], 0700); err != nil {
			return out, err
		}
	}
	var profileFile *os.File
	if profilePath != "" {
		var err error
		profileFile, err = os.OpenFile(profilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return out, err
		}
		if err := pprof.StartCPUProfile(profileFile); err != nil {
			return out, errors.Join(err, profileFile.Close())
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	errs := executeHistoryParallel(ctx, len(plans), workers, func(buildCtx context.Context, i int) error {
		plan := plans[i]
		out.Segments[i].Started = true
		start := time.Now()
		refs, err := snapshots.BuildDiagnosticStateHistoryTrioWithETLContext(buildCtx, view, dirs[i], plan.FromTxNum, plan.ToTxNum, plan.FromBlock, plan.ToBlock, "history/state-domain-change-range.seg", "auto", etl.Options{BufferLimit: etlThreshold})
		out.Segments[i].Refs, out.Segments[i].BuildWallNanos = refs, time.Since(start).Nanoseconds()
		return err
	})
	out.BuildWallNanos = time.Since(started).Nanoseconds()
	runtime.ReadMemStats(&after)
	out.BuildAllocatedBytes, out.BuildAllocations, out.BuildGCCycles = after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, after.NumGC-before.NumGC
	for i, err := range errs {
		if err != nil {
			out.Segments[i].Error = err.Error()
			resultErr = errors.Join(resultErr, fmt.Errorf("segment %d: %w", i, err))
		}
	}
	resultErr = errors.Join(resultErr, ctx.Err())
	if profileFile != nil {
		pprof.StopCPUProfile()
		resultErr = errors.Join(resultErr, profileFile.Sync(), profileFile.Close())
	}
	if resultErr != nil {
		return out, resultErr
	}
	verificationStarted := time.Now()
	defer func() { out.VerificationWallNanos = time.Since(verificationStarted).Nanoseconds() }()
	parts := make([]snapshots.StateHistoryDiagnosticDigest, len(plans))
	for i := range plans {
		segment := &out.Segments[i]
		for _, ref := range segment.Refs {
			info, err := os.Lstat(filepath.Join(dirs[i], ref.Path))
			if err != nil {
				return out, err
			}
			if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != ref.Size || ref.FromTxNum != plans[i].FromTxNum || ref.ToTxNum != plans[i].ToTxNum {
				return out, errors.New("parallel cold output type, size or planned range mismatch")
			}
			if ref.Size > math.MaxUint64-out.OutputBytes {
				return out, errors.New("parallel cold output byte total overflow")
			}
			segment.OutputBytes += ref.Size
			out.OutputBytes += ref.Size
		}
		digest, err := snapshots.DigestColdStateHistoryContext(ctx, dirs[i], segment.Refs)
		segment.Digest = digest
		if err == nil && digest != plans[i].SourceDigest {
			err = errors.New("complete parallel hot/cold row or tx-range digest mismatch")
		}
		if err != nil {
			segment.Error = err.Error()
			return out, err
		}
		segment.Equivalent, parts[i] = true, digest
	}
	out.Statistics, resultErr = historyParallelStatistics(parts)
	return out, resultErr
}
