package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
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
	ChaindataPath          string  `json:"chaindata_path"`
	CompactedChangeSets    bool    `json:"compacted_changesets"`
	CompactedChangeIndex   bool    `json:"compacted_change_index"`
	PhysicalBytesBefore    uint64  `json:"physical_bytes_before"`
	PhysicalBytesAfter     uint64  `json:"physical_bytes_after"`
	ReclaimedPhysicalBytes uint64  `json:"reclaimed_physical_bytes"`
	ElapsedSeconds         float64 `json:"elapsed_seconds"`
}

func dbCompactStateHistoryCommand() *cli.Command {
	return &cli.Command{
		Name:        "compact-state-history",
		Usage:       "Reclaim SST space left by pruned hot state history",
		Description: "The node using this datadir must be stopped. This command does not delete live rows; it only compacts state-changeset-v2-* and state-change-index-v2-* tombstones.",
		Flags: []cli.Flag{
			dataDirFlag,
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
	changeSetStart, changeSetLimit, changeIndexStart, changeIndexLimit := rawdb.StateHistoryKeyspaceBounds()
	if err := compactKeyRangeWithHeartbeat(db, "state-changeset-v2-*", changeSetStart, changeSetLimit, ctx.Duration("progress"), errWriter); err != nil {
		return fmt.Errorf("compact state changesets: %w", err)
	}
	if err := compactKeyRangeWithHeartbeat(db, "state-change-index-v2-*", changeIndexStart, changeIndexLimit, ctx.Duration("progress"), errWriter); err != nil {
		return fmt.Errorf("compact state change index: %w", err)
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
		ChaindataPath:          path,
		CompactedChangeSets:    true,
		CompactedChangeIndex:   true,
		PhysicalBytesBefore:    physicalBefore,
		PhysicalBytesAfter:     physicalAfter,
		ReclaimedPhysicalBytes: reclaimed,
		ElapsedSeconds:         time.Since(started).Seconds(),
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
