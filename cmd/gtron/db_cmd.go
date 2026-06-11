package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/urfave/cli/v2"
)

var (
	dbFromBlockFlag = &cli.Uint64Flag{
		Name:  "db.from-block",
		Usage: "First block number to rebuild, inclusive",
	}
	dbToBlockFlag = &cli.Uint64Flag{
		Name:  "db.to-block",
		Usage: "Last block number to rebuild, inclusive; defaults to current head when unset",
	}
	dbETLTempDirFlag = &cli.StringFlag{
		Name:  "db.etl.tempdir",
		Usage: "Parent directory for temporary ETL run files",
	}
	dbETLBufferMiBFlag = &cli.Uint64Flag{
		Name:  "db.etl.buffer",
		Usage: "ETL memory buffer limit in MiB (0 = default)",
	}
	dbETLBatchMiBFlag = &cli.Uint64Flag{
		Name:  "db.etl.batch",
		Usage: "ETL output batch size in MiB (0 = default)",
	}
)

func dbCommand() *cli.Command {
	return &cli.Command{
		Name:  "db",
		Usage: "Database maintenance utilities",
		Subcommands: []*cli.Command{
			{
				Name:  "rebuild-tx-indexes",
				Usage: "Rebuild transaction lookup/info indexes from retained blocks",
				Flags: []cli.Flag{
					dataDirFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					dbFromBlockFlag,
					dbToBlockFlag,
					dbETLTempDirFlag,
					dbETLBufferMiBFlag,
					dbETLBatchMiBFlag,
				},
				Action: dbRebuildTxIndexesCmd,
			},
			{
				Name:  "rebuild-section-blooms",
				Usage: "Rebuild java-tron section bloom rows from TransactionInfo logs",
				Flags: []cli.Flag{
					dataDirFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					dbFromBlockFlag,
					dbToBlockFlag,
					dbETLTempDirFlag,
					dbETLBufferMiBFlag,
					dbETLBatchMiBFlag,
				},
				Action: dbRebuildSectionBloomsCmd,
			},
			{
				Name:  "rebuild-account-traces",
				Usage: "Rebuild account balance trace rows from retained BlockBalanceTrace rows",
				Flags: []cli.Flag{
					dataDirFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					dbFromBlockFlag,
					dbToBlockFlag,
					dbETLTempDirFlag,
					dbETLBufferMiBFlag,
					dbETLBatchMiBFlag,
				},
				Action: dbRebuildAccountTracesCmd,
			},
			{
				Name:  "audit-balance-traces",
				Usage: "Audit canonical block coverage for retained BlockBalanceTrace rows",
				Flags: []cli.Flag{
					dataDirFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					dbFromBlockFlag,
					dbToBlockFlag,
				},
				Action: dbAuditBalanceTracesCmd,
			},
		},
	}
}

func dbRebuildTxIndexesCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	ancientReader, closeAncient, err := openSnapshotPruneAncientReader(cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

	chainDB := rawdb.NewChainDB(db, ancientReader)
	fromBlock := ctx.Uint64("db.from-block")
	toBlock, err := dbRebuildToBlock(ctx, chainDB)
	if err != nil {
		return err
	}
	opts, err := dbETLOptions(ctx)
	if err != nil {
		return err
	}
	result, err := rawdb.RebuildTransactionDerivedIndexesFromBlocks(chainDB, db, fromBlock, toBlock, opts)
	if err != nil {
		return err
	}
	fmt.Printf("Transaction indexes rebuilt: blocks=[%d,%d] scanned=%d txIndexes=%d txInfoBlocks=%d txInfos=%d etlApplied=%d etlRuns=%d\n",
		result.FromBlock,
		result.ToBlock,
		result.BlocksScanned,
		result.TransactionsIndexed,
		result.BlocksWithTxInfo,
		result.TransactionInfosIndexed,
		result.ETL.Applied,
		result.ETL.SpilledRuns,
	)
	return nil
}

func dbRebuildSectionBloomsCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	ancientReader, closeAncient, err := openSnapshotPruneAncientReader(cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

	chainDB := rawdb.NewChainDB(db, ancientReader)
	fromBlock := ctx.Uint64("db.from-block")
	toBlock, err := dbRebuildToBlock(ctx, chainDB)
	if err != nil {
		return err
	}
	opts, err := dbETLOptions(ctx)
	if err != nil {
		return err
	}
	result, err := rawdb.RebuildSectionBloomsFromTransactionInfos(chainDB, db, db, fromBlock, toBlock, opts)
	if err != nil {
		return err
	}
	fmt.Printf("Section blooms rebuilt: blocks=[%d,%d] scanned=%d txInfoBlocks=%d logBlocks=%d logs=%d bloomItems=%d bloomBits=%d rows=%d etlApplied=%d etlRuns=%d\n",
		result.FromBlock,
		result.ToBlock,
		result.BlocksScanned,
		result.BlocksWithTransactionInfos,
		result.BlocksWithLogs,
		result.LogEntriesIndexed,
		result.BloomItemsIndexed,
		result.BloomBitsIndexed,
		result.SectionBloomRows,
		result.ETL.Applied,
		result.ETL.SpilledRuns,
	)
	return nil
}

func dbRebuildAccountTracesCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	ancientReader, closeAncient, err := openSnapshotPruneAncientReader(cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

	chainDB := rawdb.NewChainDB(db, ancientReader)
	fromBlock := ctx.Uint64("db.from-block")
	toBlock, err := dbRebuildToBlock(ctx, chainDB)
	if err != nil {
		return err
	}
	opts, err := dbETLOptions(ctx)
	if err != nil {
		return err
	}
	result, err := rawdb.RebuildAccountTracesFromBlockBalanceTraces(chainDB, chainDB, db, fromBlock, toBlock, opts)
	if err != nil {
		return err
	}
	fmt.Printf("Account traces rebuilt: blocks=[%d,%d] scanned=%d balanceTraceBlocks=%d txTraces=%d operations=%d accountTraceRows=%d etlApplied=%d etlRuns=%d\n",
		result.FromBlock,
		result.ToBlock,
		result.BlocksScanned,
		result.BlocksWithBalanceTrace,
		result.TransactionsScanned,
		result.OperationsApplied,
		result.AccountTraceRows,
		result.ETL.Applied,
		result.ETL.SpilledRuns,
	)
	return nil
}

func dbAuditBalanceTracesCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	ancientReader, closeAncient, err := openSnapshotPruneAncientReader(cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

	chainDB := rawdb.NewChainDB(db, ancientReader)
	fromBlock := ctx.Uint64("db.from-block")
	toBlock, err := dbRebuildToBlock(ctx, chainDB)
	if err != nil {
		return err
	}
	result, err := rawdb.AuditBlockBalanceTraceCoverage(chainDB, chainDB, fromBlock, toBlock, 10)
	if err != nil {
		return err
	}
	fmt.Printf("Balance trace coverage: blocks=[%d,%d] scanned=%d traceBlocks=%d missing=%d mismatched=%d emptyTxTraceBlocks=%d\n",
		result.FromBlock,
		result.ToBlock,
		result.BlocksScanned,
		result.BlocksWithBalanceTrace,
		result.MissingBlockBalanceTrace,
		result.MismatchedBlockBalanceTrace,
		result.BlocksWithEmptyTxTrace,
	)
	for _, issue := range result.Issues {
		fmt.Printf("Balance trace coverage issue: block=%d kind=%s detail=%s\n", issue.BlockNum, issue.Kind, issue.Detail)
	}
	if !result.Complete() {
		return fmt.Errorf("balance trace coverage incomplete: missing=%d mismatched=%d",
			result.MissingBlockBalanceTrace,
			result.MismatchedBlockBalanceTrace,
		)
	}
	return nil
}

func dbRebuildToBlock(ctx *cli.Context, chainDB *rawdb.ChainDB) (uint64, error) {
	if ctx.IsSet("db.to-block") {
		toBlock := ctx.Uint64("db.to-block")
		if toBlock < ctx.Uint64("db.from-block") {
			return 0, fmt.Errorf("db rebuild block range [%d,%d] is inverted", ctx.Uint64("db.from-block"), toBlock)
		}
		return toBlock, nil
	}
	head := rawdb.ReadHeadBlockHash(chainDB)
	if head == (common.Hash{}) {
		return 0, fmt.Errorf("db rebuild requires --db.to-block when no head block is recorded")
	}
	num := rawdb.ReadBlockNumber(chainDB, head)
	if num == nil {
		return 0, fmt.Errorf("db rebuild cannot resolve head block number for hash %x", head[:])
	}
	if *num < ctx.Uint64("db.from-block") {
		return 0, fmt.Errorf("db rebuild block range [%d,%d] is inverted", ctx.Uint64("db.from-block"), *num)
	}
	return *num, nil
}

func dbETLOptions(ctx *cli.Context) (etl.Options, error) {
	buffer, err := mibToIntBytes(ctx.Uint64("db.etl.buffer"), "db.etl.buffer")
	if err != nil {
		return etl.Options{}, err
	}
	batch, err := mibToIntBytes(ctx.Uint64("db.etl.batch"), "db.etl.batch")
	if err != nil {
		return etl.Options{}, err
	}
	return etl.Options{
		TempDir:     strings.TrimSpace(ctx.String("db.etl.tempdir")),
		BufferLimit: buffer,
		BatchSize:   batch,
	}, nil
}

func mibToIntBytes(mib uint64, flag string) (int, error) {
	if mib == 0 {
		return 0, nil
	}
	if mib > uint64(math.MaxInt)/(1024*1024) {
		return 0, fmt.Errorf("--%s is too large", flag)
	}
	return int(mib * 1024 * 1024), nil
}
