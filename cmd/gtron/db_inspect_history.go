package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func dbInspectHistoryCommand() *cli.Command {
	defaults := rawdb.DefaultHistoryPrevInspectOptions()
	return &cli.Command{
		Name:        "inspect-history-prev",
		Usage:       "Sample Prev sizes and key identities in offline history block packs (JSON)",
		Description: "Stop the node first: this command requires the exclusive Pebble directory lock and opens an existing database read-only. It selects one height per non-overlapping stratum in an explicit inclusive range, retains missing packs, and reads only exact modern seq=0 pack keys. No full scan, ancient access, node startup, database writes, or compaction. Optional exports write only to a new diagnostic directory outside chaindata. Partial/budget/error reports are printed as JSON and return a nonzero exit status. Limits are cooperative; a single point read or decode cannot be interrupted.",
		Flags: []cli.Flag{
			dataDirFlag, dbCacheFlag, dbHandlesFlag,
			&cli.Uint64Flag{Name: "from-block", Required: true, Usage: "First sampled stratum height, inclusive"},
			&cli.Uint64Flag{Name: "to-block", Required: true, Usage: "Last sampled stratum height, inclusive"},
			&cli.Uint64Flag{Name: "seed", Value: defaults.Seed, Usage: "Deterministic PCG sampling seed"},
			&cli.IntFlag{Name: "samples", Value: defaults.Samples, Usage: "Total selected heights (maximum 4096; limited to range width)"},
			&cli.Uint64Flag{Name: "max-encoded-bytes", Value: defaults.MaxEncodedBytes, Usage: "Cumulative accepted encoded payload budget in bytes (maximum 1GiB)"},
			&cli.Uint64Flag{Name: "max-decoded-bytes", Value: defaults.MaxDecodedBytes, Usage: "Cumulative decoded allocation budget in bytes (maximum 4GiB; each pack at most 128MiB)"},
			&cli.Uint64Flag{Name: "max-rows", Value: defaults.MaxRows, Usage: "Maximum processed rows (maximum 10000000)"},
			&cli.DurationFlag{Name: "max-duration", Value: defaults.MaxDuration, Usage: "Cooperative inspection duration budget, excluding database open/close (maximum 5m)"},
			&cli.StringFlag{Name: "export-packs", Usage: "Create a private diagnostic directory outside chaindata and export only fully decoded sampled packs for later offline benchmarks"},
		},
		Action: dbInspectHistoryCmd,
	}
}

func dbInspectHistoryCmd(ctx *cli.Context) error {
	opts := rawdb.HistoryPrevInspectOptions{
		FromBlock: ctx.Uint64("from-block"), ToBlock: ctx.Uint64("to-block"), Seed: ctx.Uint64("seed"), Samples: ctx.Int("samples"),
		MaxEncodedBytes: ctx.Uint64("max-encoded-bytes"), MaxDecodedBytes: ctx.Uint64("max-decoded-bytes"), MaxRows: ctx.Uint64("max-rows"), MaxDuration: ctx.Duration("max-duration"),
	}
	if err := opts.Validate(); err != nil {
		return err
	}
	cache := intFlagOrDefault(ctx, "db.cache", dbCacheFlag.Value)
	handles := intFlagOrDefault(ctx, "db.handles", dbHandlesFlag.Value)
	if cache <= 0 || handles <= 0 {
		return errors.New("--db.cache and --db.handles must be positive")
	}
	if err := ctx.Context.Err(); err != nil {
		return err
	}
	path := chainDataDir(ctx.String("datadir"))
	db, err := rawdb.NewPebbleDBReadOnly(path, cache, handles)
	if err != nil {
		return fmt.Errorf("open existing chaindata read-only %q (stop gtron to release its database lock): %w", path, err)
	}
	var exported *historyPackExport
	if directory := ctx.String("export-packs"); directory != "" {
		exported, err = newHistoryPackExport(directory, path)
		if err != nil {
			return errors.Join(err, db.Close())
		}
		opts.OnCompletePack = exported.writePack
	}
	report, inspectErr := rawdb.InspectStateHistoryPrev(ctx.Context, db, opts)
	closeErr := db.Close()
	if closeErr != nil {
		report.Complete = false
		report.StopReason = "close_error"
		report.Error = errors.Join(inspectErr, closeErr).Error()
	}
	if exported != nil {
		if err := exported.finish(report); err != nil {
			inspectErr = errors.Join(inspectErr, err)
			report.Complete = false
			report.StopReason = "export_manifest_error"
			report.Error = inspectErr.Error()
		}
	}
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	output := struct {
		ChaindataPath string                      `json:"chaindata_path"`
		Inspection    rawdb.HistoryPrevInspection `json:"inspection"`
	}{path, report}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return errors.Join(inspectErr, closeErr, encoder.Encode(output))
}
