package main

import (
	"fmt"
	"os"
	"time"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

var (
	dbPruneFrozenHotConfirmFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm deletion of frozen b-* and tib-* rows from chaindata",
	}
	dbPruneFrozenHotCompactFlag = &cli.BoolFlag{
		Name:  "compact",
		Usage: "Immediately compact both deleted key ranges (high I/O and temporary disk usage)",
	}
)

func dbPruneFrozenHotCommand() *cli.Command {
	return &cli.Command{
		Name:        "prune-frozen-hot",
		Usage:       "Delete hot block bodies and transaction results already covered by ancient",
		Description: "The node must be stopped. The command first requires aligned bodies, tx_infos, and state_roots ancient counts, then range-deletes only b-*/tib-* rows below that common exclusive count. Hash and transaction indexes remain untouched.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbCacheFlag,
			dbHandlesFlag,
			dbMemtableFlag,
			dbTargetFileSizeFlag,
			dbLBaseMaxSizeFlag,
			dbL0CompactionFlag,
			dbL0StopFlag,
			dbPruneFrozenHotConfirmFlag,
			dbPruneFrozenHotCompactFlag,
		},
		Action: dbPruneFrozenHotCmd,
	}
}

func dbPruneFrozenHotCmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return fmt.Errorf("refusing to delete frozen hot rows without --yes")
	}
	chaindataPath := chainDataDir(ctx.String("datadir"))
	if err := requireDirectory(chaindataPath, "chaindata"); err != nil {
		return err
	}
	ancientPath := ancientDataDir(ctx.String("datadir"))
	if err := requireDirectory(ancientPath, "ancient"); err != nil {
		return err
	}

	physicalBefore, err := directoryBytes(chaindataPath)
	if err != nil {
		return fmt.Errorf("measure chaindata before pruning: %w", err)
	}
	ancientCount, err := validatedAncientCount(ancientPath)
	if err != nil {
		return err
	}
	if ancientCount == 0 {
		return fmt.Errorf("ancient database is empty; refusing to delete hot rows")
	}

	db, err := openPebbleDB(ctx, chaindataPath)
	if err != nil {
		return fmt.Errorf("open chaindata %q (stop gtron before pruning): %w", chaindataPath, err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	started := time.Now()
	if err := rawdb.DeleteFrozenBlockRange(db, 0, ancientCount-1); err != nil {
		return fmt.Errorf("delete frozen hot range [0,%d): %w", ancientCount, err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageFreezerHotPrune, ancientCount); err != nil {
		return fmt.Errorf("record frozen hot prune progress: %w", err)
	}
	if err := db.SyncKeyValue(); err != nil {
		return fmt.Errorf("sync frozen hot range tombstones: %w", err)
	}
	fmt.Fprintf(writer, "Deleted any live b-* and tib-* rows in frozen range [0,%d) (%s).\n",
		ancientCount, time.Since(started).Round(time.Millisecond))

	if ctx.Bool("compact") {
		fmt.Fprintln(writer, "Compacting frozen b-* and tib-* ranges; this may take hours...")
		compactStarted := time.Now()
		start, limit := rawdb.BlockRangeBounds(0, ancientCount-1)
		if err := db.Compact(start, limit); err != nil {
			return fmt.Errorf("compact frozen block-body range: %w", err)
		}
		start, limit = rawdb.TransactionInfoBlockRangeBounds(0, ancientCount-1)
		if err := db.Compact(start, limit); err != nil {
			return fmt.Errorf("compact frozen transaction-info range: %w", err)
		}
		fmt.Fprintf(writer, "Compacted frozen b-* and tib-* ranges in %s.\n", time.Since(compactStarted).Round(time.Second))
	} else {
		fmt.Fprintln(writer, "Rows are now logically absent; rerun with --compact for immediate offline physical reclamation.")
	}

	if err := db.Close(); err != nil {
		return fmt.Errorf("close chaindata after pruning: %w", err)
	}
	dbClosed = true
	physicalAfter, err := directoryBytes(chaindataPath)
	if err != nil {
		return fmt.Errorf("measure chaindata after pruning: %w", err)
	}
	fmt.Fprintf(writer, "Chaindata physical files: %s before, %s after.\n", formatIEC(physicalBefore), formatIEC(physicalAfter))
	return nil
}

func requireDirectory(path, label string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s %q: %w", label, path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s path %q is not a directory", label, path)
	}
	return nil
}

func validatedAncientCount(path string) (uint64, error) {
	store, err := rawdbfreezer.NewFreezer(path, "", true, rawdbfreezer.FreezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return 0, fmt.Errorf("open ancient read-only %q (stop gtron before pruning): %w", path, err)
	}
	defer store.Close()

	var count uint64
	for i, table := range []string{"bodies", "tx_infos", "state_roots"} {
		rows, err := store.AncientCount(table)
		if err != nil {
			return 0, fmt.Errorf("read ancient %s count: %w", table, err)
		}
		if i == 0 {
			count = rows
		} else if rows != count {
			return 0, fmt.Errorf("ancient table count mismatch: bodies=%d, %s=%d", count, table, rows)
		}
	}
	if count == 0 {
		return 0, nil
	}
	for _, number := range []uint64{0, count - 1} {
		body, err := store.Ancient("bodies", number)
		if err != nil {
			return 0, fmt.Errorf("verify ancient body %d: %w", number, err)
		}
		if len(body) == 0 {
			return 0, fmt.Errorf("verify ancient body %d: empty canonical body", number)
		}
		for _, table := range []string{"tx_infos", "state_roots"} {
			if _, err := store.Ancient(table, number); err != nil {
				return 0, fmt.Errorf("verify ancient %s %d: %w", table, number, err)
			}
		}
	}
	return count, nil
}
