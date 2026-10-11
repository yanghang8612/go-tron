package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func dbMigrateHistoryReferenceCommand() *cli.Command {
	return dbHistoryMigrationCommand(false)
}

func dbMigrateHistoryCDCCommand() *cli.Command { return dbHistoryMigrationCommand(true) }

func dbHistoryMigrationCommand(cdc bool) *cli.Command {
	command := &cli.Command{
		Name: "migrate-history-reference", Usage: "Migrate cold history in place, one verified trio at a time, with the node stopped",
		Description: "Default is a read-only plan. --yes rewrites one trio at a time inside the original snapshot directory, verifies identical logical bytes, atomically switches the manifest, then removes that trio's unleased old files. No database copy is made. A small durable journal resumes interrupted publication or reclamation. Keep the node and automatic deployment stopped until this command exits; restart only a binary supporting the reference container.",
		Flags: []cli.Flag{dataDirFlag, snapshotDirFlag,
			&cli.BoolFlag{Name: "yes", Usage: "Perform the in-place migration and reclaim replaced source files"},
			&cli.Uint64Flag{Name: "max-trios", Value: 1, Usage: "Maximum trios migrated by this invocation (0 = all remaining)"},
			&cli.Uint64Flag{Name: "min-free-gib", Value: 64, Usage: "Free-space reserve beyond this trio's work estimate"},
			&cli.Uint64Flag{Name: "max-work-gib", Value: 128, Usage: "Maximum additional work estimate admitted for a single trio"},
		}, Action: dbMigrateHistoryReferenceCmd,
	}
	if cdc {
		command.Name = "migrate-history-cdc"
		command.Usage = "Rewrite R1/V2 cold history as CDC V3, preserving logical bytes"
		command.Description = "Offline, newest first, one verified trio at a time. Preserves every logical byte and companion offset. Default is read-only; --yes publishes and reclaims replaced files with a durable recovery journal. Keep node and automatic deployment stopped. Default keeps files without measured savings; --allow-growth explicitly forces format unification."
		command.Flags = append(command.Flags, &cli.Uint64Flag{Name: "from-tx", Usage: "Only complete trios at or above this txNum"}, &cli.Uint64Flag{Name: "to-tx", Usage: "Only complete trios at or below this txNum (0 = unbounded)"}, &cli.Uint64Flag{Name: "min-savings-percent", Value: 5, Usage: "Minimum measured history file saving before publication (ignored with --allow-growth)"}, &cli.BoolFlag{Name: "r1-only", Usage: "Select only existing R1 files; any pending journal is resumed first"}, &cli.BoolFlag{Name: "allow-growth", Usage: "Publish CDC even when larger; bypass --min-savings-percent for explicit format unification"})
	}
	return command
}

type historyReferenceMigrationReport struct {
	DryRun                   bool                                       `json:"dryRun"`
	DatabasePath             string                                     `json:"databasePath"`
	SnapshotDir              string                                     `json:"snapshotDir"`
	FreeBytesBefore          uint64                                     `json:"freeBytesBefore"`
	FreeBytesAfter           uint64                                     `json:"freeBytesAfter"`
	LargestAdmittedWorkBytes uint64                                     `json:"largestAdmittedWorkBytes"`
	Result                   *snapshots.HistoryReferenceMigrationResult `json:"result,omitempty"`
	Error                    string                                     `json:"error,omitempty"`
}

func dbMigrateHistoryReferenceCmd(ctx *cli.Context) (retErr error) {
	report := historyReferenceMigrationReport{DryRun: !ctx.Bool("yes")}
	defer func() {
		if retErr != nil {
			report.Error = retErr.Error()
		}
		encoder := json.NewEncoder(ctx.App.Writer)
		encoder.SetIndent("", "  ")
		retErr = errors.Join(retErr, encoder.Encode(report))
	}()
	if !ctx.IsSet("datadir") {
		return errors.New("--datadir is required")
	}
	if err := contextOrBackground(ctx).Err(); err != nil {
		return err
	}
	var floor uint64
	var err error
	if ctx.Uint64("min-free-gib") != 0 {
		floor, err = maintenanceGiB(ctx.Uint64("min-free-gib"))
		if err != nil {
			return err
		}
	}
	maxWork, err := maintenanceGiB(ctx.Uint64("max-work-gib"))
	if err != nil {
		return err
	}
	if maxWork == 0 {
		return errors.New("--max-work-gib must be positive")
	}
	path, err := filepath.Abs(chainDataDir(ctx.String("datadir")))
	if err != nil {
		return err
	}
	dir, err := filepath.Abs(snapshotDir(ctx, ctx.String("datadir")))
	if err != nil {
		return err
	}
	report.DatabasePath, report.SnapshotDir = path, dir
	for _, p := range []string{path, dir} {
		info, e := os.Stat(p)
		if e != nil {
			return e
		}
		if !info.IsDir() {
			return fmt.Errorf("%s must be an existing directory", p)
		}
	}
	defaultDir, err := filepath.EvalSymlinks(stateSnapshotsDir(ctx.String("datadir")))
	if err != nil {
		return err
	}
	actualDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return err
	}
	defaultDir, err = filepath.Abs(defaultDir)
	if err != nil {
		return err
	}
	if actualDir != defaultDir {
		return errors.New("in-place migration requires this datadir's default snapshot directory so the database lock protects its publisher")
	}
	// Pebble's read-only open still takes its exclusive LOCK. Keep it for the
	// entire manifest rewrite, including resumed deletion, to exclude the node.
	db, err := rawdb.NewPebbleDBReadOnly(path, 64, 128)
	if err != nil {
		return fmt.Errorf("offline migration requires an exclusive database lock: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	report.FreeBytesBefore, err = offlineSpaceAvailable(dir, 0, 0)
	if err != nil {
		return err
	}
	defer func() {
		var e error
		report.FreeBytesAfter, e = offlineSpaceAvailable(dir, 0, 0)
		retErr = errors.Join(retErr, e)
	}()
	before := func(refs []snapshots.SegmentRef) error {
		estimate := snapshots.HistoryReferenceTranscodeWorkBytesContext
		if ctx.Command.Name == "migrate-history-cdc" {
			estimate = snapshots.HistoryCDCTranscodeWorkBytesContext
		}
		work, e := estimate(contextOrBackground(ctx), dir, refs)
		if e != nil {
			return e
		}
		if work > maxWork {
			return fmt.Errorf("trio needs estimated %d additional bytes, exceeding --max-work-gib %d", work, ctx.Uint64("max-work-gib"))
		}
		if _, e = offlineSpaceAvailable(dir, floor, work); e != nil {
			return e
		}
		report.LargestAdmittedWorkBytes = max(report.LargestAdmittedWorkBytes, work)
		return nil
	}
	if ctx.Command.Name == "migrate-history-cdc" {
		report.Result, err = snapshots.MigrateHistoryCDCContext(contextOrBackground(ctx), dir, snapshots.HistoryCDCMigrationOptions{R1Only: ctx.Bool("r1-only"), AllowGrowth: ctx.Bool("allow-growth"), DryRun: report.DryRun, MaxTrios: ctx.Uint64("max-trios"), FromTxNum: ctx.Uint64("from-tx"), ToTxNum: ctx.Uint64("to-tx"), MinSavingsPercent: ctx.Uint64("min-savings-percent"), BeforeTrio: before, OnProgress: func(p snapshots.HistoryReferenceMigrationProgress) {
			data, _ := json.Marshal(p)
			fmt.Fprintln(ctx.App.ErrWriter, string(data))
		}})
		return err
	}
	report.Result, err = snapshots.MigrateHistoryReferenceContext(contextOrBackground(ctx), dir, snapshots.HistoryReferenceMigrationOptions{
		DryRun: report.DryRun, MaxTrios: ctx.Uint64("max-trios"), BeforeTrio: before,
	})
	return err
}
