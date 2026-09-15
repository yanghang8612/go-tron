package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

type historyColdBenchmarkOptions struct {
	InputDir    string        `json:"input_dir"`
	OutputDir   string        `json:"output_dir"`
	Iterations  int           `json:"iterations"`
	MaxDuration time.Duration `json:"max_duration_ns"`
	CopyMode    string        `json:"copy_mode"`
	CPUProfile  bool          `json:"cpu_profile"`
}

type historyColdBenchmarkIteration struct {
	Iteration             int                                    `json:"iteration"`
	BuildWallNanos        int64                                  `json:"build_wall_nanos"`
	BuildAllocatedBytes   uint64                                 `json:"build_allocated_bytes"`
	BuildAllocations      uint64                                 `json:"build_allocations"`
	BuildGCCycles         uint32                                 `json:"build_gc_cycles"`
	OutputBytes           uint64                                 `json:"output_bytes"`
	Refs                  []snapshots.SegmentRef                 `json:"refs"`
	VerificationWallNanos int64                                  `json:"verification_wall_nanos"`
	Digest                snapshots.StateHistoryDiagnosticDigest `json:"digest"`
	Equivalent            bool                                   `json:"equivalent"`
	Error                 string                                 `json:"error,omitempty"`
}

type historyColdBenchmarkReport struct {
	Version                     int                                    `json:"version"`
	Complete                    bool                                   `json:"complete"`
	StartedUTC                  string                                 `json:"started_utc"`
	ElapsedNanos                int64                                  `json:"elapsed_nanos"`
	Options                     historyColdBenchmarkOptions            `json:"options"`
	GoVersion                   string                                 `json:"go_version"`
	GOOS                        string                                 `json:"goos"`
	GOARCH                      string                                 `json:"goarch"`
	GOMAXPROCS                  int                                    `json:"gomaxprocs"`
	CompressionFormat           string                                 `json:"compression_format"`
	ManifestSHA256              string                                 `json:"manifest_file_sha256"`
	Export                      rawdb.StateHistoryRangeExportReport    `json:"export"`
	PhysicalVerified            bool                                   `json:"physical_verified"`
	SourceDigest                snapshots.StateHistoryDiagnosticDigest `json:"source_digest"`
	SourceVerificationWallNanos int64                                  `json:"source_verification_wall_nanos"`
	Iterations                  []historyColdBenchmarkIteration        `json:"iterations"`
	CPUProfileSHA256            string                                 `json:"cpu_profile_sha256,omitempty"`
	CPUProfileScope             string                                 `json:"cpu_profile_scope,omitempty"`
	Error                       string                                 `json:"error,omitempty"`
}

func dbHistoryColdBenchmarkCommand() *cli.Command {
	return &cli.Command{Name: "benchmark-history-cold", Usage: "Replay a complete bounded cold-history trio from an authenticated private range export",
		Description: "Standalone offline diagnostic: opens only input-dir/pebble read-only and writes a new private output directory. Format 3 is explicit and does not change environment. Checks every physical input row, authenticates every logical history row and tx range, then times only complete production trio builds. Full cold re-read and companion verification are outside build timing. No node, publication, pruning or service operation. Read caches are warm across iterations; allocations and optional CPU profile are process-wide. Deadlines are cooperative, not interruptible storage-call timeouts.",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "input-dir", Required: true}, &cli.StringFlag{Name: "output-dir", Required: true},
			&cli.IntFlag{Name: "iterations", Value: 3}, &cli.DurationFlag{Name: "max-duration", Value: 5 * time.Minute},
			&cli.StringFlag{Name: "copy-mode", Value: "owned", Usage: "owned or defensive; only private snapshot Get ownership capability differs"},
			&cli.BoolFlag{Name: "cpu-profile", Usage: "Write output-dir/cpu.pprof for the first complete build only"},
		}, Action: dbHistoryColdBenchmarkCmd}
}

func dbHistoryColdBenchmarkCmd(ctx *cli.Context) error {
	opts := historyColdBenchmarkOptions{InputDir: ctx.String("input-dir"), OutputDir: ctx.String("output-dir"), Iterations: ctx.Int("iterations"), MaxDuration: ctx.Duration("max-duration"), CopyMode: ctx.String("copy-mode"), CPUProfile: ctx.Bool("cpu-profile")}
	report, err := benchmarkHistoryCold(ctx.Context, opts)
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	return errors.Join(err, json.NewEncoder(writer).Encode(report))
}

// Embedding the deliberately narrow view hides optional Get ownership only.
// Nested AcquireStateHistoryReadView returns this same pinned wrapper unchanged.
type defensiveHistoryColdView struct{ rawdb.StateHistoryReadView }
type historyColdDiscardWriter struct{}

func (historyColdDiscardWriter) Put(_, _ []byte) error { return nil }
func (historyColdDiscardWriter) Delete([]byte) error {
	return errors.New("unexpected deletion during history verification")
}

func validateHistoryColdOptions(opts historyColdBenchmarkOptions) error {
	if !filepath.IsAbs(opts.InputDir) || !filepath.IsAbs(opts.OutputDir) || opts.Iterations < 1 || opts.Iterations > 5 || opts.MaxDuration <= 0 || opts.MaxDuration > 10*time.Minute || (opts.CopyMode != "owned" && opts.CopyMode != "defensive") {
		return errors.New("history cold benchmark requires absolute input/output paths, 1..5 iterations, duration (0,10m], and copy-mode owned|defensive")
	}
	return nil
}

func pathInsideHistoryCold(path, parent string) bool {
	rel, err := filepath.Rel(parent, path)
	return err == nil && rel != ".." && !bytes.HasPrefix([]byte(rel), []byte(".."+string(filepath.Separator)))
}

func validateHistoryColdManifest(m historyRangeExportManifest) error {
	e := m.Export
	if m.Version != 1 || m.Error != "" || !e.Complete || e.Error != "" || !filepath.IsAbs(m.SourceChaindata) ||
		e.FromBlock > e.ToBlock || e.ToBlock-e.FromBlock >= rawdb.StateHistoryRangeExportMaxBlocks ||
		e.Blocks != e.ToBlock-e.FromBlock+1 || e.FromTxNum > e.ToTxNum ||
		e.FromBlock <= m.CoveredBlock || e.ToBlock > m.FinishBlock ||
		e.PhysicalRows == 0 || e.PhysicalRows > rawdb.StateHistoryRangeExportMaxRows || e.PhysicalRows != uint64(len(e.Entries)) ||
		e.PhysicalBytes == 0 || e.PhysicalBytes > rawdb.StateHistoryRangeExportMaxBytes || e.DeclaredDecodedBytes > rawdb.StateHistoryRangeExportMaxDecodedBytes {
		return errors.New("invalid or incomplete bounded history range manifest")
	}
	if _, err := time.Parse(time.RFC3339Nano, m.CreatedUTC); err != nil {
		return fmt.Errorf("invalid history export timestamp: %w", err)
	}
	return nil
}

// Verify the complete iterator, not just listed Gets: missing, altered and extra
// keys are fatal. Re-export to a discard writer independently recomputes closure,
// canonical block/range proof and declared expansion budgets from these bytes.
func verifyHistoryColdInput(ctx context.Context, view rawdb.StateHistoryReadView, expected rawdb.StateHistoryRangeExportReport) error {
	it := view.NewIterator(nil, nil)
	defer it.Release()
	var rows, physical uint64
	var previous []byte
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rows >= uint64(len(expected.Entries)) {
			return errors.New("unexpected extra physical history row")
		}
		e := expected.Entries[rows]
		key, err := hex.DecodeString(e.KeyHex)
		if err != nil || len(key) == 0 || (previous != nil && bytes.Compare(previous, key) >= 0) || !bytes.Equal(key, it.Key()) {
			return fmt.Errorf("physical history key mismatch at row %d", rows)
		}
		want, err := hex.DecodeString(e.ValueSHA256)
		sum := sha256.Sum256(it.Value())
		if err != nil || len(want) != sha256.Size || uint64(len(it.Value())) != e.ValueBytes || !bytes.Equal(sum[:], want) {
			return fmt.Errorf("physical history value mismatch at row %d", rows)
		}
		physical += uint64(len(key)) + e.ValueBytes
		if physical > rawdb.StateHistoryRangeExportMaxBytes {
			return errors.New("physical history budget exceeded")
		}
		previous = key
		rows++
	}
	if err := it.Error(); err != nil {
		return err
	}
	if rows != expected.PhysicalRows || physical != expected.PhysicalBytes {
		return errors.New("physical history row/byte total mismatch")
	}
	replayed, err := rawdb.ExportStateHistoryRange(ctx, view, historyColdDiscardWriter{}, rawdb.StateHistoryRangeExportOptions{
		FromBlock: expected.FromBlock, ToBlock: expected.ToBlock, MaxBytes: rawdb.StateHistoryRangeExportMaxBytes,
		MaxRows: rawdb.StateHistoryRangeExportMaxRows, MaxDecodedBytes: rawdb.StateHistoryRangeExportMaxDecodedBytes})
	if err != nil {
		return err
	}
	if !replayed.Complete || !reflect.DeepEqual(replayed, expected) {
		return errors.New("history range closure, digest or declared size mismatch")
	}
	return ctx.Err()
}

func benchmarkHistoryCold(parent context.Context, opts historyColdBenchmarkOptions) (report historyColdBenchmarkReport, resultErr error) {
	start := time.Now()
	report = historyColdBenchmarkReport{Version: 1, StartedUTC: start.UTC().Format(time.RFC3339Nano), Options: opts, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), CompressionFormat: "3"}
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
				report.Complete = false
				report.Error = resultErr.Error()
			}
		}
	}()
	if err := validateHistoryColdOptions(opts); err != nil {
		return report, err
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, opts.MaxDuration)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return report, err
	}
	input, err := filepath.EvalSymlinks(opts.InputDir)
	if err != nil {
		return report, err
	}
	data, err := readHistoryDiagnosticFile(filepath.Join(input, "manifest.json"), 128<<20)
	if err != nil {
		return report, err
	}
	sum := sha256.Sum256(data)
	report.ManifestSHA256 = hex.EncodeToString(sum[:])
	var manifest historyRangeExportManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return report, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return report, errors.New("trailing history export manifest content")
	}
	if err := validateHistoryColdManifest(manifest); err != nil {
		return report, err
	}
	report.Export = manifest.Export
	pebblePath := filepath.Join(input, "pebble")
	realPebble, err := filepath.EvalSymlinks(pebblePath)
	if err != nil || realPebble != pebblePath {
		return report, errors.New("input pebble must be a real directory inside the export")
	}
	parentDir, err := filepath.EvalSymlinks(filepath.Dir(filepath.Clean(opts.OutputDir)))
	if err != nil {
		return report, err
	}
	requested := filepath.Join(parentDir, filepath.Base(filepath.Clean(opts.OutputDir)))
	// The original source may be absent on the replay host. If present resolve
	// aliases; otherwise retain the absolute recorded path as an exclusion.
	sourcePath := filepath.Clean(manifest.SourceChaindata)
	if filepath.Base(sourcePath) != "chaindata" || filepath.Base(filepath.Dir(sourcePath)) != "gtron" {
		return report, errors.New("manifest source must identify datadir/gtron/chaindata")
	}
	sourceParent := filepath.Dir(filepath.Dir(sourcePath))
	if resolved, err := filepath.EvalSymlinks(sourceParent); err == nil {
		sourceParent = resolved
	}
	if pathInsideHistoryCold(requested, input) || pathInsideHistoryCold(requested, sourceParent) {
		return report, errors.New("benchmark output must be outside input and original source datadir")
	}
	directory, err := newHistoryPackExport(requested, input)
	if err != nil {
		return report, err
	}
	output = directory.directory
	db, err := rawdb.NewPebbleDBReadOnly(pebblePath, 64, 128)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, release()) }()
	if opts.CopyMode == "defensive" {
		view = defensiveHistoryColdView{view}
	} else {
		owned, ok := view.(pointread.OwnedKeyValueReader)
		if !ok || !owned.GetReturnsOwnedBytes() {
			return report, errors.New("private snapshot lacks explicit owned Get capability")
		}
	}
	if err := verifyHistoryColdInput(ctx, view, manifest.Export); err != nil {
		return report, err
	}
	report.PhysicalVerified = true
	verifyStart := time.Now()
	e := manifest.Export
	report.SourceDigest, err = snapshots.DigestHotStateHistoryContext(ctx, view, filepath.Join(output, "source-etl"), e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock, rawdb.StateHistoryRangeExportMaxDecodedBytes)
	report.SourceVerificationWallNanos = time.Since(verifyStart).Nanoseconds()
	if err != nil {
		return report, err
	}
	for n := 1; n <= opts.Iterations; n++ {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		runDir := filepath.Join(output, fmt.Sprintf("iteration-%02d", n))
		if err := os.Mkdir(runDir, 0700); err != nil {
			return report, err
		}
		iteration, err := runHistoryColdIteration(ctx, view, runDir, e, n, opts.CPUProfile && n == 1, filepath.Join(output, "cpu.pprof"))
		if err == nil && iteration.Digest != report.SourceDigest {
			err = errors.New("complete hot/cold logical row or tx-range digest mismatch")
		}
		iteration.Equivalent = err == nil
		if err != nil {
			iteration.Error = err.Error()
		}
		report.Iterations = append(report.Iterations, iteration)
		if opts.CPUProfile && n == 1 {
			profile, readErr := readHistoryDiagnosticFile(filepath.Join(output, "cpu.pprof"), 64<<20)
			if readErr == nil {
				digest := sha256.Sum256(profile)
				report.CPUProfileSHA256 = hex.EncodeToString(digest[:])
				report.CPUProfileScope = "iteration 1 production trio build only; process-wide; excludes physical/source/cold verification"
			}
			err = errors.Join(err, readErr)
		}
		if err != nil {
			return report, err
		}
	}
	return report, ctx.Err()
}

func runHistoryColdIteration(ctx context.Context, view rawdb.StateHistoryReadView, dir string, e rawdb.StateHistoryRangeExportReport, n int, profile bool, profilePath string) (out historyColdBenchmarkIteration, resultErr error) {
	out.Iteration = n
	var profileFile *os.File
	if profile {
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
	start := time.Now()
	refs, err := snapshots.BuildDiagnosticStateHistoryTrioContext(ctx, view, dir, e.FromTxNum, e.ToTxNum, e.FromBlock, e.ToBlock, "history/state-domain-change-range.seg")
	out.BuildWallNanos = time.Since(start).Nanoseconds()
	runtime.ReadMemStats(&after)
	out.BuildAllocatedBytes = after.TotalAlloc - before.TotalAlloc
	out.BuildAllocations = after.Mallocs - before.Mallocs
	out.BuildGCCycles = after.NumGC - before.NumGC
	if profile {
		pprof.StopCPUProfile()
		err = errors.Join(err, profileFile.Sync(), profileFile.Close())
	}
	out.Refs = refs
	if err != nil {
		return out, err
	}
	for _, ref := range refs {
		info, err := os.Lstat(filepath.Join(dir, ref.Path))
		if err != nil {
			return out, err
		}
		if !info.Mode().IsRegular() || uint64(info.Size()) != ref.Size {
			return out, errors.New("cold output file size or type mismatch")
		}
		out.OutputBytes += uint64(info.Size())
	}
	start = time.Now()
	out.Digest, err = snapshots.DigestColdStateHistoryContext(ctx, dir, refs)
	out.VerificationWallNanos = time.Since(start).Nanoseconds()
	return out, err
}
