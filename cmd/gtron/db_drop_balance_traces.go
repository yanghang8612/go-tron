package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

var (
	dbDropBalanceTracesYesFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm permanent deletion of every hot btrace-* and at-* row",
	}
	dbDropBalanceTracesCompactFlag = &cli.BoolFlag{
		Name:  "compact",
		Usage: "Synchronously compact both deleted key ranges to reclaim SST space",
	}
	dbDropBalanceTracesProgressFlag = &cli.DurationFlag{
		Name:  "progress",
		Usage: "Compaction heartbeat interval (0 disables)",
		Value: 30 * time.Second,
	}
	dbDropBalanceTracesJSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the deletion summary as JSON",
	}
)

type dropBalanceTracesOutput struct {
	ChaindataPath          string  `json:"chaindata_path"`
	DroppedBlockTraces     bool    `json:"dropped_block_traces"`
	DroppedAccountTraces   bool    `json:"dropped_account_traces"`
	Compacted              bool    `json:"compacted"`
	PhysicalBytesBefore    uint64  `json:"physical_bytes_before"`
	PhysicalBytesAfter     uint64  `json:"physical_bytes_after"`
	ReclaimedPhysicalBytes uint64  `json:"reclaimed_physical_bytes"`
	ElapsedSeconds         float64 `json:"elapsed_seconds"`
}

func dbDropBalanceTracesCommand() *cli.Command {
	return &cli.Command{
		Name:        "drop-balance-traces",
		Usage:       "Delete obsolete TRON block/account balance trace keyspaces",
		Description: "The node using this datadir must be stopped. Ethereum-compatible StateDomainChange history, transaction indexes, block data and Ancient files are not touched.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbCacheFlag,
			dbHandlesFlag,
			dbMemtableFlag,
			dbTargetFileSizeFlag,
			dbLBaseMaxSizeFlag,
			dbL0CompactionFlag,
			dbL0StopFlag,
			dbDropBalanceTracesYesFlag,
			dbDropBalanceTracesCompactFlag,
			dbDropBalanceTracesProgressFlag,
			dbDropBalanceTracesJSONFlag,
		},
		Action: dbDropBalanceTracesCmd,
	}
}

func dbDropBalanceTracesCmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return fmt.Errorf("refusing to delete balance traces without --yes; stop gtron and rerun with explicit confirmation")
	}
	if ctx.Duration("progress") < 0 {
		return fmt.Errorf("--progress must be >= 0")
	}
	path := chainDataDir(ctx.String("datadir"))
	physicalBefore, err := directoryBytes(path)
	if err != nil {
		return fmt.Errorf("measure chaindata before balance trace deletion: %w", err)
	}
	db, err := openPebbleDB(ctx, path)
	if err != nil {
		return fmt.Errorf("open chaindata %q (stop gtron before deletion): %w", path, err)
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
	fmt.Fprintln(errWriter, "Deleting hot btrace-* and at-* keyspaces...")
	if err := rawdb.DropBalanceTraceKeyspaces(db); err != nil {
		return err
	}
	fmt.Fprintf(errWriter, "Deleted logical balance trace keyspaces in %s.\n", time.Since(started).Round(time.Millisecond))

	if ctx.Bool("compact") {
		blockStart, blockLimit, accountStart, accountLimit := rawdb.BalanceTraceKeyspaceBounds()
		if err := compactKeyRangeWithHeartbeat(db, "btrace-*", blockStart, blockLimit, ctx.Duration("progress"), errWriter); err != nil {
			return fmt.Errorf("balance trace rows are deleted but btrace compaction failed: %w", err)
		}
		if err := compactKeyRangeWithHeartbeat(db, "at-*", accountStart, accountLimit, ctx.Duration("progress"), errWriter); err != nil {
			return fmt.Errorf("balance trace rows are deleted but account trace compaction failed: %w", err)
		}
	}
	closeErr := db.Close()
	dbOpen = false
	if closeErr != nil {
		return fmt.Errorf("close chaindata after balance trace deletion: %w", closeErr)
	}
	physicalAfter, err := directoryBytes(path)
	if err != nil {
		return fmt.Errorf("measure chaindata after balance trace deletion: %w", err)
	}
	reclaimed := uint64(0)
	if physicalBefore > physicalAfter {
		reclaimed = physicalBefore - physicalAfter
	}
	result := dropBalanceTracesOutput{
		ChaindataPath:          path,
		DroppedBlockTraces:     true,
		DroppedAccountTraces:   true,
		Compacted:              ctx.Bool("compact"),
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
	fmt.Fprintf(writer, "Dropped btrace-* and at-* from %s; compacted=%t physical=%s -> %s reclaimed=%s elapsed=%s\n",
		path, result.Compacted, formatIEC(physicalBefore), formatIEC(physicalAfter), formatIEC(reclaimed), time.Since(started).Round(time.Millisecond))
	return nil
}

func compactKeyRangeWithHeartbeat(db ethdb.KeyValueStore, name string, start, limit []byte, interval time.Duration, writer io.Writer) error {
	started := time.Now()
	fmt.Fprintf(writer, "Compacting %s; this may take hours...\n", name)
	var wg sync.WaitGroup
	done := make(chan struct{})
	if interval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					fmt.Fprintf(writer, "compacting %s elapsed=%s\n", name, time.Since(started).Round(time.Second))
				case <-done:
					return
				}
			}
		}()
	}
	err := db.Compact(start, limit)
	close(done)
	wg.Wait()
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "Compacted %s in %s.\n", name, time.Since(started).Round(time.Millisecond))
	return nil
}
