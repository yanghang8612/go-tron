package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func dbReclaimEmptyHistoryCommand() *cli.Command {
	return &cli.Command{
		Name:        "reclaim-empty-history",
		Usage:       "Inspect or reclaim an already deleted, bounded history range while the node is stopped",
		Description: "Prints a read-only JSON plan by default. --yes permits only budgeted physical compaction of a proven empty range. It never starts a node or deletes logical rows. Keep automatic deployment stopped.",
		Flags: []cli.Flag{
			dataDirFlag, snapshotDirFlag,
			&cli.Uint64Flag{Name: "from-block", Usage: "First block in the empty range (inclusive)"},
			&cli.Uint64Flag{Name: "through-block", Usage: "Last block in the empty range (inclusive); required"},
			&cli.Uint64Flag{Name: "min-free-gib", Value: 64, Usage: "Free-space reserve, excluding an extra 1 GiB for Pebble control files"},
			&cli.Uint64Flag{Name: "max-sst-write-gib", Value: 8, Usage: "Hard cumulative SST allocation budget for recovery and compaction"},
			&cli.BoolFlag{Name: "allow-wal-replay", Usage: "Allow budgeted synchronous WAL recovery when entering writable maintenance"},
			&cli.BoolFlag{Name: "yes", Usage: "Execute the bounded physical compaction"},
		},
		Action: dbReclaimEmptyHistoryCmd,
	}
}

type reclaimEmptyHistoryReport struct {
	DryRun              bool                           `json:"dry_run"`
	Chaindata           string                         `json:"chaindata"`
	FromBlock           uint64                         `json:"from_block"`
	ThroughBlock        uint64                         `json:"through_block"`
	ManifestSHA256      string                         `json:"manifest_sha256"`
	Boundary            offlineChainBoundary           `json:"boundary"`
	Inspection          pebbledb.MaintenanceInspection `json:"inspection"`
	Result              *pebbledb.MaintenanceResult    `json:"result,omitempty"`
	FreeBytesBefore     uint64                         `json:"free_bytes_before"`
	FreeBytesAfter      uint64                         `json:"free_bytes_after"`
	VerifiedAfterReopen bool                           `json:"verified_after_reopen"`
	ElapsedSeconds      float64                        `json:"elapsed_seconds"`
	Error               string                         `json:"error,omitempty"`
}

func maintenanceGiB(value uint64) (uint64, error) {
	if value == 0 || value > ^uint64(0)>>30 {
		return 0, fmt.Errorf("maintenance GiB limits must be positive and not overflow")
	}
	return value << 30, nil
}

func offlineManifestDigest(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func dbReclaimEmptyHistoryCmd(ctx *cli.Context) error {
	if !ctx.IsSet("datadir") || !ctx.IsSet("through-block") {
		return fmt.Errorf("--datadir and --through-block are required")
	}
	start, end, err := rawdb.StateHistoryBlockRangeBounds(ctx.Uint64("from-block"), ctx.Uint64("through-block"))
	if err != nil {
		return err
	}
	floor, err := maintenanceGiB(ctx.Uint64("min-free-gib"))
	if err != nil {
		return err
	}
	budget, err := maintenanceGiB(ctx.Uint64("max-sst-write-gib"))
	if err != nil {
		return err
	}
	path := chainDataDir(ctx.String("datadir"))
	dir := snapshotDir(ctx, ctx.String("datadir"))
	digest, err := offlineManifestDigest(dir)
	if err != nil {
		return err
	}
	manifest, err := statesnapshots.LoadProductionManifest(dir)
	if err != nil {
		return err
	}
	loadedDigest, err := offlineManifestDigest(dir)
	if err != nil {
		return err
	}
	if loadedDigest != digest {
		return fmt.Errorf("manifest changed while loading maintenance preflight")
	}
	if manifest.Progress == nil || manifest.Progress.HotPruneBlockNum == 0 || ctx.Uint64("through-block") > manifest.Progress.HotPruneBlockNum {
		return fmt.Errorf("requested range must be at or below the durable manifest hot-prune watermark")
	}
	opts := pebbledb.MaintenanceOptions{MinFreeBytes: floor, MaxSSTWriteBytes: budget, AllowWALReplay: ctx.Bool("allow-wal-replay")}
	db, err := pebbledb.OpenMaintenance(path, opts)
	if err != nil {
		return err
	}
	defer db.Close()
	boundary, err := readOfflineChainBoundary(db)
	if err != nil {
		return err
	}
	if manifest.Progress.HotPruneBlockNum > boundary.SolidifiedBlock {
		return fmt.Errorf("manifest prune watermark exceeds solidified head")
	}
	inspection, err := db.Inspect(start, end)
	if err != nil {
		return err
	}
	free, err := offlineSpaceAvailable(path, 0, 0)
	if err != nil {
		return err
	}
	report := reclaimEmptyHistoryReport{DryRun: !ctx.Bool("yes"), Chaindata: path, FromBlock: ctx.Uint64("from-block"), ThroughBlock: ctx.Uint64("through-block"), ManifestSHA256: digest, Boundary: boundary, Inspection: inspection, FreeBytesBefore: free}
	started := time.Now()
	if ctx.Bool("yes") {
		if err = contextOrBackground(ctx).Err(); err != nil {
			return err
		}
		current, readErr := offlineManifestDigest(dir)
		if readErr != nil || current != digest {
			return errors.Join(readErr, fmt.Errorf("manifest changed during maintenance preflight"))
		}
		fmt.Fprintf(ctx.App.ErrWriter, "Compacting empty history [%d,%d] with reserve=%d and cumulative SST budget=%d; no node is started.\n", report.FromBlock, report.ThroughBlock, floor, budget)
		result, compactErr := db.CompactEmptyRange(start, end)
		report.Result = &result
		err = errors.Join(compactErr, db.Close())
		// Reopen even after a failed budget admission: recovery may have made
		// progress, but logical chain data and the snapshot manifest must agree.
		check, openErr := pebbledb.OpenMaintenance(path, opts)
		err = errors.Join(err, openErr)
		if openErr == nil {
			after, checkErr := readOfflineChainBoundary(check)
			if checkErr == nil && after != boundary {
				checkErr = fmt.Errorf("chain boundary changed across physical compaction")
			}
			current, digestErr := offlineManifestDigest(dir)
			if digestErr == nil && current != digest {
				digestErr = fmt.Errorf("snapshot manifest changed across physical compaction")
			}
			post, inspectErr := check.Inspect(start, end)
			if inspectErr == nil && !post.Empty {
				inspectErr = fmt.Errorf("empty history range became live")
			}
			closeErr := check.Close()
			report.VerifiedAfterReopen = checkErr == nil && digestErr == nil && inspectErr == nil && closeErr == nil
			err = errors.Join(err, checkErr, digestErr, inspectErr, closeErr)
		}
	}
	err = errors.Join(err, db.Close())
	var freeErr error
	report.FreeBytesAfter, freeErr = offlineSpaceAvailable(path, 0, 0)
	err = errors.Join(err, freeErr)
	report.ElapsedSeconds = time.Since(started).Seconds()
	if err != nil {
		report.Error = err.Error()
	}
	enc := json.NewEncoder(ctx.App.Writer)
	enc.SetIndent("", "  ")
	return errors.Join(err, enc.Encode(report))
}
