package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

var (
	dbCompactAncientTxInfoV2YesFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm the offline V2 tx_infos rewrite",
	}
	dbCompactAncientTxInfoV2MaxSegmentsFlag = &cli.Uint64Flag{
		Name:  "max-segments",
		Usage: "Stop after rewriting N segments (0 rewrites every pending segment)",
	}
	dbCompactAncientTxInfoV2JSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the rewrite summary as JSON",
	}
)

type ancientTxInfoV2CompactOutput struct {
	AncientPath         string  `json:"ancient_path"`
	Segments            uint64  `json:"segments"`
	Rows                uint64  `json:"rows"`
	DuplicateBytes      uint64  `json:"duplicate_bytes_removed_before_compression"`
	IndexedTransactions uint64  `json:"indexed_transactions"`
	PhysicalBytesBefore uint64  `json:"physical_bytes_before"`
	PhysicalBytesAfter  uint64  `json:"physical_bytes_after"`
	ReclaimedBytes      uint64  `json:"reclaimed_bytes"`
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
}

func dbCompactAncientTxInfoV2Command() *cli.Command {
	return &cli.Command{
		Name:        "compact-ancient-tx-info-v2",
		Usage:       "Remove duplicate transaction IDs from existing V2 tx_infos segments",
		Description: "The node using this datadir must be stopped. Each replacement segment is checksummed, verified and atomically published before the old segment file is removed. Interrupted runs resume from the last published manifest.",
		Flags: []cli.Flag{
			dataDirFlag,
			dbCacheFlag,
			dbHandlesFlag,
			dbMemtableFlag,
			dbTargetFileSizeFlag,
			dbLBaseMaxSizeFlag,
			dbL0CompactionFlag,
			dbL0StopFlag,
			dbCompactAncientTxInfoV2YesFlag,
			dbCompactAncientTxInfoV2MaxSegmentsFlag,
			dbCompactAncientTxInfoV2JSONFlag,
		},
		Action: dbCompactAncientTxInfoV2Cmd,
	}
}

func dbCompactAncientTxInfoV2Cmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return fmt.Errorf("refusing to rewrite V2 tx_infos without --yes; stop gtron and rerun with explicit confirmation")
	}
	path := ancientDataDir(ctx.String("datadir"))
	chaindataPath := chainDataDir(ctx.String("datadir"))
	info, err := os.Stat(chaindataPath)
	if err != nil {
		return fmt.Errorf("stat chaindata %q: %w", chaindataPath, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("chaindata path %q is not a directory", chaindataPath)
	}
	db, err := openPebbleDB(ctx, chaindataPath)
	if err != nil {
		return fmt.Errorf("open chaindata %q (stop gtron before rewrite): %w", chaindataPath, err)
	}
	defer db.Close()
	indexWriter := newTransactionLocationUpgradeWriter(db)
	defer indexWriter.close()

	freezer, err := rawdbfreezer.NewFreezer(path, "", false, rawdbfreezer.FreezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open ancient %q (stop gtron before rewrite): %w", path, err)
	}
	defer freezer.Close()
	errWriter := ctx.App.ErrWriter
	if errWriter == nil {
		errWriter = os.Stderr
	}
	result, err := freezer.RewriteV2TransactionInfos(rawdbfreezer.V2TxInfoRewriteOptions{
		MaxSegments: ctx.Uint64("max-segments"),
		Observe: func(number uint64, txInfo, body []byte) error {
			return indexWriter.addBlock(number, txInfo, body)
		},
		BeforePublish: indexWriter.flushAndSync,
		Transform: func(_ uint64, txInfo, body []byte) ([]byte, uint64, error) {
			compact, _, removed, err := rawdb.CompactTransactionInfoIDsForBlock(txInfo, body)
			if err != nil {
				return txInfo, 0, nil
			}
			return compact, uint64(removed), nil
		},
		Progress: func(progress rawdbfreezer.V2TxInfoRewriteProgress) {
			fmt.Fprintf(errWriter, "compacted tx_infos segment=%d range=[%d,%d) rows=%d duplicate=%s elapsed=%s\n",
				progress.Segment, progress.Start, progress.End, progress.Rows,
				formatIEC(progress.RemovedBytes), progress.Elapsed.Round(time.Millisecond))
		},
	})
	if err != nil {
		return err
	}
	output := ancientTxInfoV2CompactOutput{
		AncientPath:         path,
		Segments:            result.Segments,
		Rows:                result.Rows,
		DuplicateBytes:      result.RemovedBytes,
		IndexedTransactions: indexWriter.indexed,
		PhysicalBytesBefore: result.PhysicalBytesBefore,
		PhysicalBytesAfter:  result.PhysicalBytesAfter,
		ElapsedSeconds:      result.Elapsed.Seconds(),
	}
	if output.PhysicalBytesBefore > output.PhysicalBytesAfter {
		output.ReclaimedBytes = output.PhysicalBytesBefore - output.PhysicalBytesAfter
	}
	writer := ctx.App.Writer
	if writer == nil {
		writer = os.Stdout
	}
	if ctx.Bool("json") {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}
	fmt.Fprintf(writer, "Ancient V2 tx_infos compacted: %d segments, %d rows in %s.\n",
		output.Segments, output.Rows, time.Duration(output.ElapsedSeconds*float64(time.Second)).Round(time.Millisecond))
	fmt.Fprintf(writer, "Duplicate protobuf bytes removed before compression: %s; upgraded %d tx indexes. Physical tx_infos: %s before, %s after, %s reclaimed.\n",
		formatIEC(output.DuplicateBytes), output.IndexedTransactions, formatIEC(output.PhysicalBytesBefore), formatIEC(output.PhysicalBytesAfter), formatIEC(output.ReclaimedBytes))
	return nil
}

const transactionLocationUpgradeBatchBytes = 8 << 20

type transactionLocationUpgradeWriter struct {
	db      ethdb.KeyValueStore
	batch   ethdb.Batch
	indexed uint64
}

func newTransactionLocationUpgradeWriter(db ethdb.KeyValueStore) *transactionLocationUpgradeWriter {
	return &transactionLocationUpgradeWriter{db: db, batch: db.NewBatchWithSize(transactionLocationUpgradeBatchBytes)}
}

func (w *transactionLocationUpgradeWriter) addBlock(number uint64, retData, blockData []byte) error {
	return rawdb.VisitValidatedTransactionInfoIDs(retData, blockData, func(ordinal int, id []byte) error {
		if err := rawdb.WriteTransactionLocation(w.batch, id, number, ordinal); err != nil {
			return err
		}
		w.indexed++
		if w.batch.ValueSize() >= transactionLocationUpgradeBatchBytes {
			return w.flush()
		}
		return nil
	})
}

func (w *transactionLocationUpgradeWriter) flush() error {
	if w.batch.ValueSize() == 0 {
		return nil
	}
	if err := w.batch.Write(); err != nil {
		return err
	}
	w.batch.Reset()
	return nil
}

func (w *transactionLocationUpgradeWriter) flushAndSync() error {
	if err := w.flush(); err != nil {
		return err
	}
	syncer, ok := w.db.(interface{ SyncKeyValue() error })
	if !ok {
		return fmt.Errorf("chaindata does not support an explicit sync barrier")
	}
	return syncer.SyncKeyValue()
}

func (w *transactionLocationUpgradeWriter) close() {
	if w.batch != nil {
		w.batch.Reset()
	}
}
