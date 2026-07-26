package main

import (
	"fmt"
	"os"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

var (
	dbPruneTxInfoConfirmFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm deletion of all legacy ti-* rows",
	}
	dbPruneTxInfoCompactFlag = &cli.BoolFlag{
		Name:  "compact",
		Usage: "Immediately compact the ti-* range to reclaim disk (high I/O and temporary disk usage)",
	}
)

func dbPruneTxInfoCommand() *cli.Command {
	return &cli.Command{
		Name:        "prune-tx-info",
		Usage:       "Delete redundant legacy ti-* transaction-info rows",
		Description: "The node must be stopped. This command is only safe after upgrading to a binary whose transaction-info reads resolve tx-* through tib-*/ancient tx_infos. Older binaries must not be used after pruning.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbCacheFlag,
			dbHandlesFlag,
			dbMemtableFlag,
			dbTargetFileSizeFlag,
			dbLBaseMaxSizeFlag,
			dbL0CompactionFlag,
			dbL0StopFlag,
			dbPruneTxInfoConfirmFlag,
			dbPruneTxInfoCompactFlag,
		},
		Action: dbPruneTxInfoCmd,
	}
}

func dbPruneTxInfoCmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return fmt.Errorf("refusing to delete ti-* rows without --yes")
	}
	path := chainDataDir(ctx.String("datadir"))
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat chaindata %q: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("chaindata path %q is not a directory", path)
	}
	physicalBefore, err := directoryBytes(path)
	if err != nil {
		return fmt.Errorf("measure chaindata before pruning: %w", err)
	}

	db, err := openPebbleDB(ctx, path)
	if err != nil {
		return fmt.Errorf("open chaindata %q (stop gtron before pruning): %w", path, err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()
	hasRows, err := rawdb.HasLegacyTransactionInfos(db)
	if err != nil {
		return fmt.Errorf("check legacy transaction info rows: %w", err)
	}
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	if !hasRows {
		fmt.Fprintf(writer, "No live ti-* rows found in %s; nothing to prune.\n", path)
		return nil
	}

	started := time.Now()
	if err := rawdb.DeleteLegacyTransactionInfos(db); err != nil {
		return fmt.Errorf("delete legacy transaction info range: %w", err)
	}
	if err := db.SyncKeyValue(); err != nil {
		return fmt.Errorf("sync transaction info range tombstone: %w", err)
	}
	if stillHasRows, err := rawdb.HasLegacyTransactionInfos(db); err != nil {
		return fmt.Errorf("verify legacy transaction info deletion: %w", err)
	} else if stillHasRows {
		return fmt.Errorf("legacy transaction info rows remain visible after DeleteRange")
	}
	fmt.Fprintf(writer, "Deleted logical ti-* keyspace in %s (%s).\n", path, time.Since(started).Round(time.Millisecond))

	if ctx.Bool("compact") {
		fmt.Fprintln(writer, "Compacting ti-* range; this may take hours on a mainnet database...")
		compactStarted := time.Now()
		if err := rawdb.CompactLegacyTransactionInfos(db); err != nil {
			return fmt.Errorf("compact legacy transaction info range: %w", err)
		}
		fmt.Fprintf(writer, "Compacted ti-* range in %s.\n", time.Since(compactStarted).Round(time.Second))
	} else {
		fmt.Fprintln(writer, "Physical SST bytes will be reclaimed by background compaction; rerun with --compact for immediate offline reclamation.")
	}
	// Close first so Pebble finishes pending table replacement/deletion before
	// walking the directory. Otherwise an SST can disappear between WalkDir and
	// DirEntry.Info after a manual compaction.
	if err := db.Close(); err != nil {
		return fmt.Errorf("close chaindata after pruning: %w", err)
	}
	dbClosed = true
	physicalAfter, err := directoryBytes(path)
	if err != nil {
		return fmt.Errorf("measure chaindata after pruning: %w", err)
	}
	fmt.Fprintf(writer, "Chaindata physical files: %s before, %s after.\n", formatIEC(physicalBefore), formatIEC(physicalAfter))
	return nil
}
