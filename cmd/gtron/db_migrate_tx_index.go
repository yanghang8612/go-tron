package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

const txIndexMigrationBatchBytes = 16 << 20

// The uncovered tail is normally only the freezer margin plus a partial V2
// segment. Refuse an unexpectedly huge atomic rewrite before it can exhaust an
// offline maintenance host; advancing V2 coverage makes the tail small again.
const txIndexMigrationMaxHotTailBatchBytes = 1 << 30

var (
	dbMigrateTxIndexYesFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm publication of the cold index and deletion of covered tx-* rows",
	}
	dbMigrateTxIndexPrefixBitsFlag = &cli.UintFlag{
		Name:  "prefix-bits",
		Usage: "Leading hash bits used by the immutable bucket directory",
		Value: 20,
	}
	dbMigrateTxIndexCompactFlag = &cli.BoolFlag{
		Name:  "compact",
		Usage: "Immediately compact the tx-* range after deletion (high I/O and temporary disk usage)",
	}
	dbMigrateTxIndexProgressFlag = &cli.DurationFlag{
		Name:  "progress",
		Usage: "Progress reporting interval (0 disables)",
		Value: 30 * time.Second,
	}
	dbMigrateTxIndexJSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the migration summary as JSON",
	}
)

type txIndexMigrationOutput struct {
	AncientPath         string  `json:"ancient_path"`
	ChaindataPath       string  `json:"chaindata_path"`
	StartBlock          uint64  `json:"start_block"`
	EndBlock            uint64  `json:"end_block"`
	IndexedTransactions uint64  `json:"indexed_transactions"`
	DeletedHotRows      uint64  `json:"deleted_hot_rows"`
	RetainedHotRows     uint64  `json:"retained_hot_rows"`
	ScannedHotRows      uint64  `json:"scanned_hot_rows"`
	RunBytes            uint64  `json:"run_bytes"`
	RecoveredBuiltRun   bool    `json:"recovered_built_run"`
	Compacted           bool    `json:"compacted"`
	PhysicalBytesBefore uint64  `json:"chaindata_physical_bytes_before"`
	PhysicalBytesAfter  uint64  `json:"chaindata_physical_bytes_after"`
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
}

func dbMigrateTxIndexCommand() *cli.Command {
	return &cli.Command{
		Name:        "migrate-tx-index",
		Usage:       "Move V2-covered transaction hash indexes from Pebble to immutable runs",
		Description: "The node using this datadir must be stopped. A checksummed run is built and verified, then atomically published before covered tx-* rows are deleted. Interrupted deletion is safe to resume by rerunning the command.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbCacheFlag,
			dbHandlesFlag,
			dbMemtableFlag,
			dbTargetFileSizeFlag,
			dbLBaseMaxSizeFlag,
			dbL0CompactionFlag,
			dbL0StopFlag,
			dbMigrateTxIndexYesFlag,
			dbMigrateTxIndexPrefixBitsFlag,
			dbMigrateTxIndexCompactFlag,
			dbMigrateTxIndexProgressFlag,
			dbMigrateTxIndexJSONFlag,
		},
		Action: dbMigrateTxIndexCmd,
	}
}

func dbMigrateTxIndexCmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return fmt.Errorf("refusing to migrate tx-* rows without --yes; stop gtron and rerun with explicit confirmation")
	}
	prefixBits := ctx.Uint("prefix-bits")
	if prefixBits < 8 || prefixBits > 24 {
		return fmt.Errorf("--prefix-bits must be between 8 and 24")
	}
	progressInterval := ctx.Duration("progress")
	if progressInterval < 0 {
		return fmt.Errorf("--progress must be >= 0")
	}
	ancientPath := ancientDataDir(ctx.String("datadir"))
	chaindataPath := chainDataDir(ctx.String("datadir"))
	if info, err := os.Stat(chaindataPath); err != nil {
		return fmt.Errorf("stat chaindata %q: %w", chaindataPath, err)
	} else if !info.IsDir() {
		return fmt.Errorf("chaindata path %q is not a directory", chaindataPath)
	}
	physicalBefore, err := directoryBytes(chaindataPath)
	if err != nil {
		return fmt.Errorf("measure chaindata before migration: %w", err)
	}

	// Hold the freezer lock through publication and Pebble deletion. Otherwise
	// an automatic deployment could start a node with the old manifest between
	// the coverage snapshot and deletion of the corresponding hot rows.
	ancient, err := rawdbfreezer.NewFreezer(ancientPath, "", true, rawdbfreezer.FreezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open ancient %q (stop gtron before migration): %w", ancientPath, err)
	}
	ancientClosed := false
	defer func() {
		if !ancientClosed {
			_ = ancient.Close()
		}
	}()
	startBlock := ancient.TransactionIndexCoverage()
	endBlock := ancient.V2Coverage()
	if startBlock > endBlock {
		return fmt.Errorf("transaction index coverage %d exceeds ancient V2 coverage %d", startBlock, endBlock)
	}
	if endBlock == 0 {
		return fmt.Errorf("ancient V2 has no published coverage; run migrate-ancient-v2 first")
	}

	db, err := openPebbleDB(ctx, chaindataPath)
	if err != nil {
		return fmt.Errorf("open chaindata %q (stop gtron before migration): %w", chaindataPath, err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()
	errWriter := ctx.App.ErrWriter
	if errWriter == nil {
		errWriter = os.Stderr
	}
	started := time.Now()
	output := txIndexMigrationOutput{
		AncientPath:         ancientPath,
		ChaindataPath:       chaindataPath,
		StartBlock:          startBlock,
		EndBlock:            endBlock,
		PhysicalBytesBefore: physicalBefore,
	}

	if startBlock < endBlock {
		path := rawdbfreezer.TransactionIndexRunPath(ancientPath, startBlock, endBlock)
		result, recovered, err := buildOrRecoverTransactionIndexRun(db, path, startBlock, endBlock, uint32(prefixBits), progressInterval, errWriter)
		if err != nil {
			return err
		}
		output.IndexedTransactions = result.Rows
		output.RunBytes = result.FileBytes
		output.RecoveredBuiltRun = recovered
		if err := runTxIndexMaintenanceHeartbeat(errWriter, progressInterval, "verifying and publishing transaction index", func() error {
			return rawdbfreezer.PublishTransactionIndexRun(ancientPath, result)
		}); err != nil {
			return fmt.Errorf("publish transaction index run: %w", err)
		}
		fmt.Fprintf(errWriter, "published transaction index range=[%d,%d) rows=%d size=%s\n", startBlock, endBlock, result.Rows, formatIEC(result.FileBytes))
	}
	store, err := rawdbfreezer.OpenTransactionIndexStore(ancientPath)
	if err != nil {
		return fmt.Errorf("verify published transaction index: %w", err)
	}
	if store.Coverage() != endBlock {
		store.Close()
		return fmt.Errorf("published transaction index coverage=%d, want %d", store.Coverage(), endBlock)
	}
	if err := store.Close(); err != nil {
		return fmt.Errorf("close verified transaction index: %w", err)
	}

	scanned, deleted, retained, err := replaceCoveredTransactionIndexes(db, endBlock, progressInterval, errWriter)
	if err != nil {
		return err
	}
	output.ScannedHotRows = scanned
	output.DeletedHotRows = deleted
	output.RetainedHotRows = retained
	if err := rawdb.WriteStageProgress(db, rawdb.StageFreezerTxIndexPrune, endBlock); err != nil {
		return fmt.Errorf("write transaction-index prune progress: %w", err)
	}
	if syncer, ok := db.(interface{ SyncKeyValue() error }); !ok {
		return fmt.Errorf("chaindata does not support an explicit sync barrier")
	} else if err := syncer.SyncKeyValue(); err != nil {
		return fmt.Errorf("sync deleted transaction indexes: %w", err)
	}
	if ctx.Bool("compact") {
		compacter, ok := db.(ethdb.Compacter)
		if !ok {
			return fmt.Errorf("chaindata does not support compaction")
		}
		fmt.Fprintln(errWriter, "compacting tx-* range; this may take hours on a mainnet database...")
		if err := runTxIndexMaintenanceHeartbeat(errWriter, progressInterval, "compacting tx-*", func() error {
			return rawdb.CompactTransactionIndexes(compacter)
		}); err != nil {
			return fmt.Errorf("compact transaction indexes: %w", err)
		}
		output.Compacted = true
	}
	if err := ancient.Close(); err != nil {
		return fmt.Errorf("close ancient after migration: %w", err)
	}
	ancientClosed = true
	if err := db.Close(); err != nil {
		return fmt.Errorf("close chaindata after migration: %w", err)
	}
	dbClosed = true
	physicalAfter, err := directoryBytes(chaindataPath)
	if err != nil {
		return fmt.Errorf("measure chaindata after migration: %w", err)
	}
	output.PhysicalBytesAfter = physicalAfter
	output.ElapsedSeconds = time.Since(started).Seconds()

	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	if ctx.Bool("json") {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}
	fmt.Fprintf(writer, "Transaction index coverage: [%d,%d), %d indexed; deleted %d covered hot rows and retained %d hot-tail rows after scanning %d rows.\n", output.StartBlock, output.EndBlock, output.IndexedTransactions, output.DeletedHotRows, output.RetainedHotRows, output.ScannedHotRows)
	fmt.Fprintf(writer, "Cold run bytes: %s. Chaindata physical files: %s before, %s after.\n", formatIEC(output.RunBytes), formatIEC(output.PhysicalBytesBefore), formatIEC(output.PhysicalBytesAfter))
	if !output.Compacted {
		fmt.Fprintln(writer, "Physical SST bytes will be reclaimed by background compaction; rerun with --compact for immediate offline reclamation.")
	}
	return nil
}

func buildOrRecoverTransactionIndexRun(db ethdb.Iteratee, path string, startBlock, endBlock uint64, prefixBits uint32, progress time.Duration, progressWriter interface{ Write([]byte) (int, error) }) (rawdbfreezer.TransactionIndexBuildResult, bool, error) {
	if _, err := os.Stat(path); err == nil {
		run, err := rawdbfreezer.OpenTransactionIndexRun(path)
		if err != nil {
			return rawdbfreezer.TransactionIndexBuildResult{}, false, fmt.Errorf("open unpublished transaction index run %q: %w", path, err)
		}
		defer run.Close()
		if run.StartBlock() != startBlock || run.EndBlock() != endBlock || run.PrefixBits() != prefixBits {
			return rawdbfreezer.TransactionIndexBuildResult{}, false, fmt.Errorf("unpublished transaction index run %q has incompatible metadata", path)
		}
		if err := runTxIndexMaintenanceHeartbeat(progressWriter, progress, "verifying recovered transaction index", run.Verify); err != nil {
			return rawdbfreezer.TransactionIndexBuildResult{}, false, fmt.Errorf("verify unpublished transaction index run %q: %w", path, err)
		}
		return rawdbfreezer.TransactionIndexBuildResult{Path: path, Rows: run.Rows(), StartBlock: startBlock, EndBlock: endBlock, PrefixBits: prefixBits, FileBytes: run.Size()}, true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return rawdbfreezer.TransactionIndexBuildResult{}, false, err
	}
	var selected uint64
	started := time.Now()
	lastProgress := started
	result, err := rawdbfreezer.BuildTransactionIndexRun(path, rawdbfreezer.TransactionIndexBuildOptions{
		PrefixBits: prefixBits,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		Iterate: func(yield func(rawdbfreezer.TransactionIndexEntry) error) error {
			_, _, err := rawdb.VisitTransactionIndexesByBlockRange(db, startBlock, endBlock, func(sample rawdb.TransactionIndexSample) error {
				if err := yield(rawdbfreezer.TransactionIndexEntry{Hash: sample.Hash, Location: sample.Location}); err != nil {
					return err
				}
				selected++
				if progress > 0 && time.Since(lastProgress) >= progress {
					fmt.Fprintf(progressWriter, "building transaction index range=[%d,%d) rows=%d elapsed=%s\n", startBlock, endBlock, selected, time.Since(started).Round(time.Second))
					lastProgress = time.Now()
				}
				return nil
			})
			return err
		},
	})
	if err != nil {
		return result, false, fmt.Errorf("build transaction index run: %w", err)
	}
	return result, false, nil
}

func replaceCoveredTransactionIndexes(db ethdb.KeyValueStore, coverage uint64, progress time.Duration, progressWriter interface{ Write([]byte) (int, error) }) (uint64, uint64, uint64, error) {
	batch := db.NewBatchWithSize(txIndexMigrationBatchBytes)
	defer batch.Reset()
	start, limit := rawdb.TransactionIndexRangeBounds()
	if err := batch.DeleteRange(start, limit); err != nil {
		return 0, 0, 0, fmt.Errorf("replace covered transaction indexes: range delete: %w", err)
	}
	var covered, retained uint64
	started := time.Now()
	lastProgress := started
	scanned, err := rawdb.VisitTransactionIndexes(db, func(sample rawdb.TransactionIndexSample) error {
		if rawdb.TransactionIndexLocationBlock(sample.Location) < coverage {
			covered++
		} else {
			if err := rawdb.WriteEncodedTransactionLocation(batch, sample.Hash[:], sample.Location); err != nil {
				return err
			}
			retained++
			if batch.ValueSize() > txIndexMigrationMaxHotTailBatchBytes {
				return fmt.Errorf("uncovered tx-* tail exceeds %s atomic batch limit; advance ancient V2 coverage before retrying", formatIEC(txIndexMigrationMaxHotTailBatchBytes))
			}
		}
		if progress > 0 && time.Since(lastProgress) >= progress {
			fmt.Fprintf(progressWriter, "planning atomic tx-* replacement scanned=%d covered=%d retained=%d elapsed=%s\n", covered+retained, covered, retained, time.Since(started).Round(time.Second))
			lastProgress = time.Now()
		}
		return nil
	})
	if err != nil {
		return scanned, covered, retained, fmt.Errorf("replace covered transaction indexes: %w", err)
	}
	if scanned != covered+retained {
		return scanned, covered, retained, fmt.Errorf("replace covered transaction indexes: scanned=%d covered=%d retained=%d", scanned, covered, retained)
	}
	if covered == 0 {
		return scanned, 0, retained, nil
	}
	if err := batch.Write(); err != nil {
		return scanned, covered, retained, fmt.Errorf("atomically replace transaction indexes: %w", err)
	}
	return scanned, covered, retained, nil
}

func runTxIndexMaintenanceHeartbeat(writer interface{ Write([]byte) (int, error) }, interval time.Duration, phase string, work func() error) error {
	if interval <= 0 {
		return work()
	}
	started := time.Now()
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Fprintf(writer, "%s elapsed=%s\n", phase, time.Since(started).Round(time.Second))
			case <-done:
				return
			}
		}
	}()
	err := work()
	close(done)
	<-stopped
	return err
}
