package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

var (
	dbCompactStateHistoryYesFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm the offline state-history compaction",
	}
	dbCompactStateHistoryProgressFlag = &cli.DurationFlag{
		Name:  "progress",
		Usage: "Compaction heartbeat interval (0 disables)",
		Value: 30 * time.Second,
	}
	dbCompactStateHistoryJSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the compaction summary as JSON",
	}
)

type compactStateHistoryOutput struct {
	ChaindataPath             string  `json:"chaindata_path"`
	CompactedChangeSets       bool    `json:"compacted_changesets"`
	CompactedPostingIndex     bool    `json:"compacted_posting_index"`
	UsedPruneWatermark        bool    `json:"used_prune_watermark"`
	PrunedThroughBlock        uint64  `json:"pruned_through_block,omitempty"`
	PostingRowsScanned        uint64  `json:"posting_rows_scanned"`
	StalePostingRowsDeleted   uint64  `json:"stale_posting_rows_deleted"`
	DirectoryRowsScanned      uint64  `json:"directory_rows_scanned"`
	StaleDirectoryRowsDeleted uint64  `json:"stale_directory_rows_deleted"`
	PhysicalBytesBefore       uint64  `json:"physical_bytes_before"`
	PhysicalBytesAfter        uint64  `json:"physical_bytes_after"`
	ReclaimedPhysicalBytes    uint64  `json:"reclaimed_physical_bytes"`
	ElapsedSeconds            float64 `json:"elapsed_seconds"`
}

func dbCompactStateHistoryCommand() *cli.Command {
	return &cli.Command{
		Name:        "compact-state-history",
		Usage:       "Reclaim SST space left by pruned hot state history",
		Description: "The node using this datadir must be stopped. This command removes only stale posting frames/directories, then compacts state-history tombstones without deleting live history.",
		Flags: []cli.Flag{
			dataDirFlag,
			snapshotDirFlag,
			dbCacheFlag,
			dbHandlesFlag,
			dbMemtableFlag,
			dbTargetFileSizeFlag,
			dbLBaseMaxSizeFlag,
			dbL0CompactionFlag,
			dbL0StopFlag,
			dbCompactStateHistoryYesFlag,
			dbCompactStateHistoryProgressFlag,
			dbCompactStateHistoryJSONFlag,
		},
		Action: dbCompactStateHistoryCmd,
	}
}

func dbCompactStateHistoryCmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return fmt.Errorf("refusing to compact state history without --yes; stop gtron and rerun with explicit confirmation")
	}
	if ctx.Duration("progress") < 0 {
		return fmt.Errorf("--progress must be >= 0")
	}
	path := chainDataDir(ctx.String("datadir"))
	physicalBefore, err := directoryBytes(path)
	if err != nil {
		return fmt.Errorf("measure chaindata before state history compaction: %w", err)
	}
	db, err := openPebbleDB(ctx, path)
	if err != nil {
		return fmt.Errorf("open chaindata %q (stop gtron before compaction): %w", path, err)
	}
	dbOpen := true
	defer func() {
		if dbOpen {
			_ = db.Close()
		}
	}()

	errWriter := ctx.App.ErrWriter
	if errWriter == nil {
		errWriter = os.Stderr
	}
	started := time.Now()
	pruneContext := contextOrBackground(ctx)
	pruneWatermark, usePruneWatermark, err := stateHistoryPruneWatermark(ctx, ctx.String("datadir"))
	if err != nil {
		return err
	}
	var postingPrune rawdb.StateChangePostingPruneResult
	if usePruneWatermark {
		fmt.Fprintf(errWriter, "Pruning stale state-change index through block %d with sequential watermark scan...\n", pruneWatermark)
		postingPrune, err = rawdb.PruneStaleStateChangePostingIndexThroughContextWithProgress(
			pruneContext,
			db,
			pruneWatermark,
			stateHistoryPruneProgressReporter(ctx.Duration("progress"), errWriter),
		)
	} else {
		fmt.Fprintln(errWriter, "No durable hot-prune watermark found; falling back to the conservative state-change index scan...")
		postingPrune, err = rawdb.PruneStaleStateChangePostingIndexContext(pruneContext, db)
	}
	if err != nil {
		return fmt.Errorf("prune stale state change postings: %w", err)
	}
	if usePruneWatermark {
		if err := statesnapshots.UpdateStateChangeIndexPruneProgress(snapshotDir(ctx, ctx.String("datadir")), pruneWatermark); err != nil {
			return fmt.Errorf("publish state-change index prune progress through block %d: %w", pruneWatermark, err)
		}
	}
	changeSetStart, changeSetLimit := rawdb.StateHistoryKeyspaceBounds()
	if err := compactKeyRangeWithHeartbeat(db, "state-changeset-v2-*", changeSetStart, changeSetLimit, ctx.Duration("progress"), errWriter); err != nil {
		return fmt.Errorf("compact state changesets: %w", err)
	}
	postingStart, postingLimit, directoryStart, directoryLimit := rawdb.StateHistoryPostingKeyspaceBounds()
	if err := compactKeyRangeWithHeartbeat(db, "state-change-posting-v3-*", postingStart, postingLimit, ctx.Duration("progress"), errWriter); err != nil {
		return fmt.Errorf("compact state change postings: %w", err)
	}
	if err := compactKeyRangeWithHeartbeat(db, "state-change-keys-v3-*", directoryStart, directoryLimit, ctx.Duration("progress"), errWriter); err != nil {
		return fmt.Errorf("compact state change key directory: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close chaindata after state history compaction: %w", err)
	}
	dbOpen = false
	physicalAfter, err := directoryBytes(path)
	if err != nil {
		return fmt.Errorf("measure chaindata after state history compaction: %w", err)
	}
	reclaimed := uint64(0)
	if physicalBefore > physicalAfter {
		reclaimed = physicalBefore - physicalAfter
	}
	result := compactStateHistoryOutput{
		ChaindataPath:             path,
		CompactedChangeSets:       true,
		CompactedPostingIndex:     true,
		UsedPruneWatermark:        usePruneWatermark,
		PrunedThroughBlock:        pruneWatermark,
		PostingRowsScanned:        postingPrune.PostingRowsScanned,
		StalePostingRowsDeleted:   postingPrune.PostingRowsDeleted,
		DirectoryRowsScanned:      postingPrune.DirectoryRowsScanned,
		StaleDirectoryRowsDeleted: postingPrune.DirectoryRowsDeleted,
		PhysicalBytesBefore:       physicalBefore,
		PhysicalBytesAfter:        physicalAfter,
		ReclaimedPhysicalBytes:    reclaimed,
		ElapsedSeconds:            time.Since(started).Seconds(),
	}
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	if ctx.Bool("json") {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	fmt.Fprintf(writer, "Compacted state history in %s; physical=%s -> %s reclaimed=%s elapsed=%s\n",
		path, formatIEC(physicalBefore), formatIEC(physicalAfter), formatIEC(reclaimed), time.Since(started).Round(time.Millisecond))
	return nil
}

func stateHistoryPruneWatermark(ctx *cli.Context, dataDir string) (uint64, bool, error) {
	dir := snapshotDir(ctx, dataDir)
	manifest, err := statesnapshots.LoadProductionManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("load state snapshot manifest %q: %w", dir, err)
	}
	if manifest.Progress == nil || manifest.Progress.HotPruneBlockNum == 0 {
		return 0, false, nil
	}
	return manifest.Progress.HotPruneBlockNum, true, nil
}

func stateHistoryPruneProgressReporter(interval time.Duration, writer interface{ Write([]byte) (int, error) }) rawdb.StateChangePostingPruneProgressFn {
	if interval <= 0 || writer == nil {
		return nil
	}
	started := time.Now()
	lastReport := started.Add(-interval)
	lastPhase := ""
	return func(progress rawdb.StateChangePostingPruneProgress) {
		now := time.Now()
		phaseChanged := progress.Phase != lastPhase
		phaseComplete := progress.Phase == "postings-complete" || progress.Phase == "directory-complete"
		if !phaseChanged && !phaseComplete && now.Sub(lastReport) < interval {
			return
		}
		fmt.Fprintf(writer,
			"pruning stale state-change index phase=%s postingScanned=%d postingDeleted=%d directoryScanned=%d directoryDeleted=%d elapsed=%s\n",
			progress.Phase,
			progress.Result.PostingRowsScanned,
			progress.Result.PostingRowsDeleted,
			progress.Result.DirectoryRowsScanned,
			progress.Result.DirectoryRowsDeleted,
			now.Sub(started).Round(time.Millisecond),
		)
		lastPhase = progress.Phase
		lastReport = now
	}
}
