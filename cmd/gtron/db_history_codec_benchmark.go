package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func dbHistoryCodecBenchmarkCommand() *cli.Command {
	return &cli.Command{Name: "benchmark-history-codecs", Usage: "Compare lossless codecs on private exported diagnostic packs, without opening a database (JSON)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "export-packs", Required: true, Usage: "Directory created by db inspect-history-prev --export-packs"},
			&cli.DurationFlag{Name: "max-duration", Value: 2 * time.Minute, Usage: "Cooperative whole benchmark timeout, maximum 10m"},
		}, Action: dbHistoryCodecBenchmarkCmd}
}

type historyCodecBenchmarkTotal struct {
	Name               string `json:"name"`
	StoredBytes        uint64 `json:"stored_bytes"`
	EncodeWallNS       int64  `json:"encode_wall_ns"`
	EncodeProcessCPUNS int64  `json:"encode_process_cpu_ns"`
	DecodeWallNS       int64  `json:"decode_wall_ns"`
	DecodeProcessCPUNS int64  `json:"decode_process_cpu_ns"`
}

type historyCodecBenchmarkReport struct {
	Scope                    string                              `json:"scope"`
	Complete                 bool                                `json:"complete"`
	Error                    string                              `json:"error,omitempty"`
	ExportInspectionComplete bool                                `json:"export_inspection_complete"`
	ManifestSHA256           string                              `json:"manifest_sha256"`
	ManifestVersion          int                                 `json:"manifest_version"`
	MaterializedPacks        int                                 `json:"materialized_packs"`
	PlannedPacks             int                                 `json:"planned_packs"`
	ElapsedSeconds           float64                             `json:"elapsed_seconds"`
	Stats                    rawdb.HistoryCodecBenchmarkStats    `json:"stats"`
	Samples                  []rawdb.HistoryCodecBenchmarkSample `json:"samples"`
	Totals                   []historyCodecBenchmarkTotal        `json:"totals_completed_samples"`
}

// Verify the opened inode as well as the path before reading. Files are read
// only; lengths and checksums are checked again against the export manifest.
func readHistoryDiagnosticFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > limit {
		return nil, fmt.Errorf("diagnostic file is not a bounded regular file: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, opened) || info.Size() != opened.Size() {
		return nil, errors.New("diagnostic file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != info.Size() {
		return nil, errors.New("diagnostic file changed length")
	}
	return data, nil
}

func benchmarkHistoryExport(ctx context.Context, directory string) (report historyCodecBenchmarkReport, resultErr error) {
	started := time.Now()
	report.Scope = "self-contained exports of seq=0 samples only; existing candidate measures the exported representation, not original shared pack/chunk storage; totals include completed samples only; timings are standalone wall and whole-process CPU, not online import throughput; zstd is an experiment, not a production format"
	defer func() {
		report.ElapsedSeconds = time.Since(started).Seconds()
		if resultErr != nil {
			report.Complete = false
			report.Error = resultErr.Error()
		}
	}()
	manifestBytes, err := readHistoryDiagnosticFile(filepath.Join(directory, "manifest.json"), 2<<20)
	if err != nil {
		return report, err
	}
	digest := sha256.Sum256(manifestBytes)
	report.ManifestSHA256 = hex.EncodeToString(digest[:])
	var manifest historyPackExportManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return report, err
	}
	if manifest.Version != 1 && manifest.Version != 2 || len(manifest.Entries) == 0 || len(manifest.Entries) > 256 {
		return report, errors.New("expected version1 or version2 export with 1..256 complete pack entries")
	}
	report.ManifestVersion = manifest.Version
	report.ExportInspectionComplete = manifest.Complete
	report.PlannedPacks = len(manifest.Entries)
	var totalEncoded, totalDecoded uint64
	for i, entry := range manifest.Entries {
		if err := validateHistoryPackExportEntry(manifest.Version, entry); err != nil {
			return report, err
		}
		if entry.Materialized {
			report.MaterializedPacks++
		}
		if entry.File != fmt.Sprintf("%020d.pack", entry.Block) || (i > 0 && entry.Block <= manifest.Entries[i-1].Block) {
			return report, errors.New("export entries must have exact filenames and unique ascending heights")
		}
		if entry.EncodedBytes == 0 || entry.EncodedBytes > 128<<20 || entry.DecodedBytes == 0 || entry.DecodedBytes > 128<<20 {
			return report, errors.New("export entry exceeds pack limits")
		}
		totalEncoded += entry.EncodedBytes
		totalDecoded += entry.DecodedBytes
		if totalEncoded > 1<<30 || totalDecoded > 1<<30 {
			return report, errors.New("export exceeds cumulative 1GiB budgets")
		}
	}
	benchmark, err := rawdb.NewHistoryCodecBenchmark(rawdb.HistoryCodecBenchmarkOptions{})
	if err != nil {
		return report, err
	}
	defer func() { report.Stats = benchmark.Stats(); resultErr = errors.Join(resultErr, benchmark.Close()) }()
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		encoded, err := readHistoryDiagnosticFile(filepath.Join(directory, entry.File), int64(entry.EncodedBytes))
		if err != nil {
			return report, err
		}
		digest := sha256.Sum256(encoded)
		if uint64(len(encoded)) != entry.EncodedBytes || hex.EncodeToString(digest[:]) != entry.SHA256 {
			return report, fmt.Errorf("export pack %d checksum or length differs", entry.Block)
		}
		codec, decodedBytes, err := rawdb.InspectStateHistoryPackEncoding(encoded)
		if err != nil || codec != entry.Codec || decodedBytes != entry.DecodedBytes {
			return report, fmt.Errorf("export pack %d codec or decoded length differs: %w", entry.Block, errors.Join(err, errors.New("export encoding metadata mismatch")))
		}
		sample, err := benchmark.BenchmarkPack(ctx, entry.Block, encoded)
		report.Samples = append(report.Samples, sample)
		if err != nil {
			return report, err
		}
		if !sample.Completed || sample.DecodedBytes != entry.DecodedBytes {
			return report, errors.New("benchmark decoded length or completion differs from export")
		}
		for i, candidate := range sample.Candidates {
			if len(report.Totals) <= i {
				report.Totals = append(report.Totals, historyCodecBenchmarkTotal{Name: candidate.Name})
			}
			total := &report.Totals[i]
			if total.Name != candidate.Name || !candidate.ByteExact {
				return report, errors.New("codec candidate identity or byte exactness differs")
			}
			total.StoredBytes += candidate.StoredBytes
			if candidate.Encode != nil {
				total.EncodeWallNS += candidate.Encode.WallNS
				total.EncodeProcessCPUNS += candidate.Encode.ProcessCPUNS
			}
			total.DecodeWallNS += candidate.Decode.WallNS
			total.DecodeProcessCPUNS += candidate.Decode.ProcessCPUNS
		}
	}
	report.Complete = true
	return report, nil
}

func dbHistoryCodecBenchmarkCmd(ctx *cli.Context) error {
	duration := ctx.Duration("max-duration")
	if duration <= 0 || duration > 10*time.Minute {
		return errors.New("max-duration must be in (0,10m]")
	}
	bounded, cancel := context.WithTimeout(ctx.Context, duration)
	defer cancel()
	report, err := benchmarkHistoryExport(bounded, ctx.String("export-packs"))
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return errors.Join(err, encoder.Encode(report))
}
