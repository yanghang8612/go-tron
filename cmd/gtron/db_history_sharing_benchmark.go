package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func dbHistorySharingBenchmarkCommand() *cli.Command {
	return &cli.Command{Name: "benchmark-history-sharing", Usage: "Replay exported packs at original heights in a new private temporary Pebble, then close/reopen and verify lossless sharing (JSON)", Flags: []cli.Flag{
		&cli.StringFlag{Name: "export-packs", Required: true},
		&cli.DurationFlag{Name: "max-duration", Value: 2 * time.Minute, Usage: "Cooperative timeout, at most 10m"},
	}, Action: func(ctx *cli.Context) error {
		d := ctx.Duration("max-duration")
		if d <= 0 || d > 10*time.Minute {
			return errors.New("max-duration must be in (0,10m]")
		}
		bounded, cancel := context.WithTimeout(ctx.Context, d)
		defer cancel()
		report, err := benchmarkHistorySharingExport(bounded, ctx.String("export-packs"))
		writer := ctx.App.Writer
		if writer == nil {
			writer = os.Stdout
		}
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return errors.Join(err, encoder.Encode(report))
	}}
}

type historySharingBenchmarkReport struct {
	Scope                    string                       `json:"scope"`
	Complete                 bool                         `json:"complete"`
	Error                    string                       `json:"error,omitempty"`
	ElapsedSeconds           float64                      `json:"elapsed_seconds"`
	ManifestSHA256           string                       `json:"manifest_sha256"`
	ExportInspectionComplete bool                         `json:"export_inspection_complete"`
	PlannedPacks             int                          `json:"planned_packs"`
	Buckets                  int                          `json:"buckets"`
	Consecutive              bool                         `json:"consecutive"`
	SharedPacks              uint64                       `json:"shared_packs"`
	BaselinePackBytes        uint64                       `json:"baseline_pack_bytes"`
	BaselineKVBytes          uint64                       `json:"baseline_kv_bytes"`
	ReopenedKVBytes          uint64                       `json:"reopened_kv_bytes"`
	SavedKVFraction          float64                      `json:"saved_kv_fraction"`
	Samples                  []rawdb.HistorySharingSample `json:"samples"`
}

func benchmarkHistorySharingExport(ctx context.Context, directory string) (report historySharingBenchmarkReport, resultErr error) {
	started := time.Now()
	report.Scope = "same original heights and bucket boundaries; baseline and candidate both use block-dedup=true; new private Pebble only; baseline is current standalone production codec on identical original RLP; replay KV bytes include every pack, unique chunk, key and metadata; excludes WAL/SST and is not online throughput or canonical chain validation"
	defer func() {
		report.ElapsedSeconds = time.Since(started).Seconds()
		if resultErr != nil {
			report.Complete = false
			report.Error = resultErr.Error()
		}
	}()
	data, err := readHistoryDiagnosticFile(filepath.Join(directory, "manifest.json"), 2<<20)
	if err != nil {
		return report, err
	}
	digest := sha256.Sum256(data)
	report.ManifestSHA256 = hex.EncodeToString(digest[:])
	var manifest historyPackExportManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return report, err
	}
	if (manifest.Version != 1 && manifest.Version != 2) || len(manifest.Entries) == 0 || len(manifest.Entries) > 256 {
		return report, errors.New("expected export manifest version 1 or 2 with 1..256 entries")
	}
	report.ExportInspectionComplete, report.PlannedPacks, report.Consecutive = manifest.Complete, len(manifest.Entries), true
	var encoded, decoded uint64
	buckets := make(map[uint64]bool)
	for i, entry := range manifest.Entries {
		if err := validateHistoryPackExportEntry(manifest.Version, entry); err != nil {
			return report, err
		}
		if entry.File != fmt.Sprintf("%020d.pack", entry.Block) || (i > 0 && entry.Block <= manifest.Entries[i-1].Block) {
			return report, errors.New("export entries must have exact filenames and ascending unique heights")
		}
		if entry.EncodedBytes == 0 || entry.EncodedBytes > 128<<20 || entry.DecodedBytes == 0 || entry.DecodedBytes > 128<<20 {
			return report, errors.New("export exceeds pack budgets")
		}
		encoded += entry.EncodedBytes
		decoded += entry.DecodedBytes
		if encoded > 1<<30 || decoded > 1<<30 {
			return report, errors.New("export exceeds cumulative 1GiB budgets")
		}
		if i > 0 && entry.Block-manifest.Entries[i-1].Block != 1 {
			report.Consecutive = false
		}
		buckets[entry.Block/rawdb.StateHistoryChunkBucketBlocks] = true
	}
	report.Buckets = len(buckets)
	benchmark, err := rawdb.NewHistorySharingBenchmark()
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, benchmark.Close()) }()
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		data, err := readHistoryDiagnosticFile(filepath.Join(directory, entry.File), int64(entry.EncodedBytes))
		if err != nil {
			return report, err
		}
		digest := sha256.Sum256(data)
		if uint64(len(data)) != entry.EncodedBytes || hex.EncodeToString(digest[:]) != entry.SHA256 {
			return report, fmt.Errorf("export pack %d checksum or length differs", entry.Block)
		}
		codec, size, err := rawdb.InspectStateHistoryPackEncoding(data)
		if err != nil {
			return report, err
		}
		if codec != entry.Codec || size != entry.DecodedBytes {
			return report, fmt.Errorf("export pack %d codec or decoded length differs", entry.Block)
		}
		sample, err := benchmark.AddPack(ctx, entry.Block, data)
		if err != nil {
			return report, err
		}
		report.Samples = append(report.Samples, sample)
		if sample.Shared {
			report.SharedPacks++
		}
		report.BaselinePackBytes += sample.BaselinePackBytes
		report.BaselineKVBytes += sample.BaselineKVBytes
	}
	samples, total, err := benchmark.ReopenAndVerify(ctx)
	if err != nil {
		return report, err
	}
	report.Samples, report.ReopenedKVBytes = samples, total
	if report.BaselineKVBytes > 0 {
		report.SavedKVFraction = 1 - float64(total)/float64(report.BaselineKVBytes)
	}
	report.Complete = true
	return report, nil
}
