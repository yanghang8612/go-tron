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

func dbAdoptHistoryAccessorsCommand() *cli.Command {
	return &cli.Command{
		Name:        "adopt-history-accessors-v11",
		Usage:       "Verify and adopt a bounded sealed V7-to-V11 batch, retaining every old file",
		Description: "Default is a read-only comparison. --yes requires the node stopped and an exclusive database lock. At most four explicit trios are admitted. Existing history/index bytes are reused under companion aliases. Old files and hardlink backups remain outside automatic retirement. --rollback reverses only this receipt's exact refs on top of the current manifest, preserving other progress. No reclamation is performed.",
		Flags: []cli.Flag{dataDirFlag, snapshotDirFlag,
			&cli.StringFlag{Name: "plan", Usage: "JSON object with Entries: exact History/Accessor/Inverted/Candidate segment refs"},
			&cli.StringFlag{Name: "candidate-dir", Usage: "Directory containing sealed V11 candidate files"},
			&cli.StringFlag{Name: "receipt-dir", Required: true, Usage: "New receipt/backup directory outside the snapshot directory; reuse it to resume or roll back"},
			&cli.UintFlag{Name: "max-trios", Value: 1, Usage: "First one to four explicit plan entries to admit"},
			&cli.BoolFlag{Name: "yes", Usage: "Apply or resume with the node stopped; retain old files"},
			&cli.BoolFlag{Name: "rollback", Usage: "Reverse the receipt's adopted refs without replacing unrelated manifest progress"},
		},
		Action: dbAdoptHistoryAccessorsCmd,
	}
}

func dbAdoptHistoryAccessorsCmd(ctx *cli.Context) (retErr error) {
	var report *snapshots.HistoryAccessorAdoptionReport
	defer func() {
		out := struct {
			Result *snapshots.HistoryAccessorAdoptionReport
			Error  string
		}{Result: report}
		if retErr != nil {
			out.Error = retErr.Error()
		}
		e := json.NewEncoder(ctx.App.Writer)
		e.SetIndent("", "  ")
		retErr = errors.Join(retErr, e.Encode(out))
	}()
	if !ctx.IsSet("datadir") {
		return errors.New("--datadir is required")
	}
	limit := ctx.Uint("max-trios")
	if limit == 0 || limit > 4 {
		return errors.New("--max-trios must be between one and four")
	}
	dataDir, err := filepath.Abs(ctx.String("datadir"))
	if err != nil {
		return err
	}
	dir, err := filepath.EvalSymlinks(snapshotDir(ctx, dataDir))
	if err != nil {
		return err
	}
	want, err := filepath.EvalSymlinks(stateSnapshotsDir(dataDir))
	if err != nil {
		return err
	}
	if dir != want {
		return errors.New("adoption requires the datadir's default snapshot directory")
	}
	opts := snapshots.HistoryAccessorAdoptionOptions{CandidateDir: ctx.String("candidate-dir"), ReceiptDir: ctx.String("receipt-dir"), Apply: ctx.Bool("yes"), Rollback: ctx.Bool("rollback")}
	// A journal, when present, is authoritative for its bounded resume. A plan
	// is required for initial comparison/application, never for rollback.
	if ctx.String("plan") != "" {
		p := ctx.String("plan")
		info, e := os.Lstat(p)
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 2<<20 {
			return errors.New("plan must be a regular JSON file at most 2 MiB")
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		var plan struct {
			Entries []snapshots.HistoryAccessorAdoption
		}
		if e = json.Unmarshal(b, &plan); e != nil {
			return e
		}
		if len(plan.Entries) == 0 {
			return errors.New("adoption plan is empty")
		}
		if len(plan.Entries) > int(limit) {
			plan.Entries = plan.Entries[:limit]
		}
		opts.Entries = plan.Entries
	}
	if opts.Apply {
		// This is the same exclusive Pebble LOCK used by the node and the other
		// offline migration commands. Never operate on the live writer's files.
		db, e := rawdb.NewPebbleDBReadOnly(chainDataDir(dataDir), 64, 128)
		if e != nil {
			return fmt.Errorf("adoption requires the node stopped and exclusive database lock: %w", e)
		}
		defer func() { retErr = errors.Join(retErr, db.Close()) }()
		if _, e = offlineSpaceAvailable(dir, 64<<30, 512<<20); e != nil {
			return e
		}
	}
	report, err = snapshots.AdoptHistoryAccessorsV11Context(contextOrBackground(ctx), dir, opts)
	return err
}
