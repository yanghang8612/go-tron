package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

type historyRangeExportManifest struct {
	Version         int                                 `json:"version"`
	SourceChaindata string                              `json:"source_chaindata"`
	CreatedUTC      string                              `json:"created_utc"`
	FinishBlock     uint64                              `json:"finish_block"`
	CoveredBlock    uint64                              `json:"covered_block"`
	Export          rawdb.StateHistoryRangeExportReport `json:"export"`
	Error           string                              `json:"error,omitempty"`
}

func dbHistoryRangeExportCommand() *cli.Command {
	return &cli.Command{
		Name: "export-history-range", Usage: "Copy an offline bounded physical history range into a new private diagnostic Pebble without re-encoding shared packs",
		Description: "Stop the node first. Opens existing chaindata read-only and pins one snapshot. Copies original physical rows, shared chunks and canonical block proofs; does not mutate production, open ancient, or create a node. Default range begins after the verified cold watermark. Output must be a new absolute directory outside the entire source datadir. Partial output is marked incomplete and is not a backup. Logical content authentication belongs to subsequent offline replay. Time limits are cooperative and do not interrupt a storage call.",
		Flags: []cli.Flag{dataDirFlag, dbCacheFlag, dbHandlesFlag,
			&cli.StringFlag{Name: "output-dir", Required: true},
			&cli.Uint64Flag{Name: "from-block", Usage: "First block inclusive; omitted selects verified SnapshotBuild+1"},
			&cli.Uint64Flag{Name: "blocks", Value: 16, Usage: "Consecutive block count (1..256)"},
			&cli.Uint64Flag{Name: "max-bytes", Value: 512 << 20, Usage: "Physical key/value budget, at most 1GiB"},
			&cli.Uint64Flag{Name: "max-decoded-bytes", Value: 1 << 30, Usage: "Declared expanded pack budget, at most 4GiB"},
			&cli.Uint64Flag{Name: "max-rows", Value: 262144, Usage: "Physical key count limit, at most 262144"},
			&cli.DurationFlag{Name: "max-duration", Value: time.Minute, Usage: "Cooperative range-copy deadline, at most 5m"},
		}, Action: dbHistoryRangeExportCmd,
	}
}

func dbHistoryRangeExportCmd(ctx *cli.Context) error {
	count, start, duration := ctx.Uint64("blocks"), ctx.Uint64("from-block"), ctx.Duration("max-duration")
	opts := rawdb.StateHistoryRangeExportOptions{MaxBytes: ctx.Uint64("max-bytes"), MaxDecodedBytes: ctx.Uint64("max-decoded-bytes"), MaxRows: ctx.Uint64("max-rows")}
	if count == 0 || count > 256 || start > math.MaxUint64-(count-1) || duration <= 0 || duration > 5*time.Minute ||
		opts.MaxBytes == 0 || opts.MaxBytes > 1<<30 || opts.MaxDecodedBytes == 0 || opts.MaxDecodedBytes > 4<<30 || opts.MaxRows == 0 || opts.MaxRows > 262144 {
		return errors.New("invalid physical history export range or budgets")
	}
	if !filepath.IsAbs(ctx.String("output-dir")) {
		return errors.New("--output-dir must be a new absolute directory")
	}
	cache, handles := intFlagOrDefault(ctx, "db.cache", dbCacheFlag.Value), intFlagOrDefault(ctx, "db.handles", dbHandlesFlag.Value)
	if cache <= 0 || handles <= 0 {
		return errors.New("--db.cache and --db.handles must be positive")
	}
	if err := ctx.Context.Err(); err != nil {
		return err
	}
	path := chainDataDir(ctx.String("datadir"))
	db, err := rawdb.NewPebbleDBReadOnly(path, cache, handles)
	if err != nil {
		return fmt.Errorf("open existing chaindata read-only (stop gtron first): %w", err)
	}
	view, release, err := rawdb.AcquireStateHistoryReadView(db)
	if err != nil {
		return errors.Join(err, db.Close())
	}
	finish, hasFinish, finishErr := rawdb.ReadVerifiedStageProgressBlock(view, rawdb.StageFinish)
	covered, hasCovered, coveredErr := rawdb.ReadVerifiedStageProgressBlock(view, rawdb.StageSnapshotBuild)
	if finishErr != nil || coveredErr != nil || !hasFinish || !hasCovered || covered > finish {
		return errors.Join(errors.New("export requires hash-verified Finish and SnapshotBuild watermarks"), finishErr, coveredErr, release(), db.Close())
	}
	if !ctx.IsSet("from-block") {
		if covered == math.MaxUint64 {
			return errors.Join(errors.New("cold watermark has no successor"), release(), db.Close())
		}
		start = covered + 1
	}
	if start <= covered || start > math.MaxUint64-(count-1) || start+count-1 > finish {
		return errors.Join(errors.New("export range must be after cold coverage and at or below durable Finish"), release(), db.Close())
	}
	opts.FromBlock, opts.ToBlock = start, start+count-1
	// Reuse the exclusive private-directory creator, excluding the whole
	// production datadir rather than only its chaindata child.
	out, err := newHistoryPackExport(ctx.String("output-dir"), ctx.String("datadir"))
	if err != nil {
		return errors.Join(err, release(), db.Close())
	}
	copyDB, err := rawdb.NewPebbleDB(filepath.Join(out.directory, "pebble"), min(cache, 64), min(handles, 128))
	if err != nil {
		return errors.Join(err, release(), db.Close())
	}
	bounded, cancel := context.WithTimeout(ctx.Context, duration)
	report, exportErr := rawdb.ExportStateHistoryRange(bounded, view, copyDB, opts)
	cancel()
	closeErr := errors.Join(copyDB.Close(), release(), db.Close())
	manifest := historyRangeExportManifest{Version: 1, SourceChaindata: path, CreatedUTC: time.Now().UTC().Format(time.RFC3339Nano), FinishBlock: finish, CoveredBlock: covered, Export: report}
	resultErr := errors.Join(exportErr, closeErr)
	if resultErr != nil {
		manifest.Export.Complete = false
		manifest.Error = resultErr.Error()
	}
	data, jsonErr := json.MarshalIndent(manifest, "", "  ")
	if jsonErr == nil {
		jsonErr = writeHistoryDiagnosticFile(filepath.Join(out.directory, "manifest.json"), append(data, '\n'))
	}
	if jsonErr != nil {
		manifest.Export.Complete = false
		manifest.Error = errors.Join(resultErr, jsonErr).Error()
	}
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	return errors.Join(resultErr, jsonErr, json.NewEncoder(writer).Encode(manifest))
}
