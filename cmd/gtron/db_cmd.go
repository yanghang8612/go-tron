package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
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
	dbReplayTempDirFlag = &cli.StringFlag{
		Name:  "db.replay.tempdir",
		Usage: "Parent directory for temporary isolated replay databases",
	}
	dbReplayDirFlag = &cli.StringFlag{
		Name:  "db.replay.dir",
		Usage: "Persistent isolated replay database directory for resumable balance trace backfills",
	}
	dbBalanceTraceOverwriteFlag = &cli.BoolFlag{
		Name:  "db.balance-trace.overwrite",
		Usage: "Overwrite existing balance trace rows when replay output differs",
	}
	dbStageVerifyFlag = &cli.BoolFlag{
		Name:  "db.stage.verify",
		Usage: "Fail stage-status when present stage rows are unverifiable or canonical stages are not hash-bound",
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
			{
				Name:  "backfill-balance-traces",
				Usage: "Backfill BlockBalanceTrace/AccountTrace rows by replaying canonical blocks in an isolated database",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					witnessKeyFlag,
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
					dbReplayTempDirFlag,
					dbReplayDirFlag,
					dbBalanceTraceOverwriteFlag,
					snapshotDirFlag,
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
				},
				Action: dbBackfillBalanceTracesCmd,
			},
			{
				Name:  "freezer-status",
				Usage: "Print chain freezer head/tail and per-table physical prune status",
				Flags: []cli.Flag{
					dataDirFlag,
				},
				Action: dbFreezerStatusCmd,
			},
			{
				Name:  "stage-status",
				Usage: "Print staged sync/snapshot/prune/freezer progress",
				Flags: []cli.Flag{
					dataDirFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					dbStageVerifyFlag,
				},
				Action: dbStageStatusCmd,
			},
		},
	}
}

func dbFreezerStatusCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	f, err := rawdbfreezer.NewFreezer(ancientDataDir(cfg.DataDir), "", true, freezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open freezer: %w", err)
	}
	defer f.Close()

	stats, err := f.Stats()
	if err != nil {
		return fmt.Errorf("read freezer status: %w", err)
	}
	fmt.Printf("Freezer status: datadir=%s readonly=%t head=%d tail=%d tables=%d\n",
		stats.Datadir, stats.ReadOnly, stats.Head, stats.Tail, len(stats.Tables))
	for _, table := range stats.Tables {
		fmt.Printf("Freezer table: name=%s head=%d physicalTail=%d hiddenTail=%d prunable=%t noSnappy=%t tailFile=%d headFile=%d headBytes=%d visibleSize=%d hiddenSize=%d\n",
			table.Name, table.Head, table.PhysicalTail, table.HiddenTail, table.Prunable, table.NoSnappy,
			table.TailFile, table.HeadFile, table.HeadBytes, table.VisibleSize, table.HiddenSize)
	}
	return nil
}

func dbStageStatusCmd(ctx *cli.Context) error {
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

	return dbPrintStageStatus(db, rawdb.NewChainDB(db, ancientReader), cfg.DataDir, dbStageStatusOptions{
		Verify: ctx.Bool("db.stage.verify"),
	})
}

type dbStageStatusRow struct {
	stage         rawdb.StageID
	group         string
	present       bool
	progress      rawdb.StageProgress
	verified      string
	canonicalHash common.Hash
}

type dbStageStatusOptions struct {
	Verify bool
}

func dbPrintStageStatus(db ethdb.KeyValueStore, canonical ethdb.KeyValueReader, dataDir string, opts dbStageStatusOptions) error {
	rows, err := dbStageStatusRows(db, canonical)
	if err != nil {
		return err
	}
	present := 0
	for _, row := range rows {
		if row.present {
			present++
		}
	}
	fmt.Printf("Stage status: datadir=%s known=%d rows=%d\n", dataDir, len(rawdb.KnownStageProgressStages()), present)
	for _, row := range rows {
		if !row.present {
			fmt.Printf("Stage progress: group=%s name=%s status=missing\n", row.group, row.stage)
			continue
		}
		if !row.progress.HasBlockHash {
			fmt.Printf("Stage progress: group=%s name=%s value=%d hash=none verified=%s\n",
				row.group, row.stage, row.progress.BlockNum, row.verified)
			continue
		}
		fmt.Printf("Stage progress: group=%s name=%s value=%d hash=%x verified=%s",
			row.group, row.stage, row.progress.BlockNum, row.progress.BlockHash, row.verified)
		if row.verified != "canonical" {
			fmt.Printf(" canonicalHash=%x", row.canonicalHash)
		}
		fmt.Println()
	}
	if opts.Verify {
		issues := dbStageStatusVerificationIssues(rows)
		issues = append(issues, dbStageStatusSnapshotCoverageIssues(rows, stateSnapshotsDir(dataDir))...)
		if len(issues) > 0 {
			return fmt.Errorf("stage status verification failed: %s", strings.Join(issues, "; "))
		}
	}
	return nil
}

func dbStageStatusVerificationIssues(rows []dbStageStatusRow) []string {
	var issues []string
	for _, row := range rows {
		if !row.present {
			continue
		}
		if !dbStageStatusRequiresCanonicalVerification(row.stage) {
			continue
		}
		if row.progress.HasBlockHash {
			if row.verified != "canonical" {
				issues = append(issues, fmt.Sprintf("%s verified=%s", row.stage, row.verified))
			}
			continue
		}
		if row.group == "canonical" {
			issues = append(issues, fmt.Sprintf("%s verified=unbound", row.stage))
		}
	}
	issues = append(issues, dbStageStatusPipelineOrderIssues(rows)...)
	return issues
}

func dbStageStatusPipelineOrderIssues(rows []dbStageStatusRow) []string {
	byStage := make(map[rawdb.StageID]dbStageStatusRow, len(rows))
	for _, row := range rows {
		if row.present {
			byStage[row.stage] = row
		}
	}
	var issues []string
	for _, pair := range []struct {
		downstream rawdb.StageID
		upstream   rawdb.StageID
	}{
		{rawdb.StageBodies, rawdb.StageHeaders},
		{rawdb.StageExecution, rawdb.StageBodies},
		{rawdb.StageCommitment, rawdb.StageExecution},
		{rawdb.StageFinish, rawdb.StageCommitment},
		{rawdb.StageSnapshotBuild, rawdb.StageFinish},
		{rawdb.StageSnapshotLatestBuild, rawdb.StageFinish},
		{rawdb.StageSnapshotEventLogBuild, rawdb.StageFinish},
		{rawdb.StageSnapshotPrune, rawdb.StageFinish},
		{rawdb.StageChainFreezer, rawdb.StageFinish},
		{rawdb.StageSnapshotSectionBloomPrune, rawdb.StageFinish},
		{rawdb.StageSnapshotBalanceTracePrune, rawdb.StageFinish},
		{rawdb.StageSyncBodiesReady, rawdb.StageSyncBodies},
		{rawdb.StageSyncImport, rawdb.StageSyncBodiesReady},
		{rawdb.StageSyncExecution, rawdb.StageSyncImport},
		{rawdb.StageSyncCommitment, rawdb.StageSyncExecution},
		{rawdb.StageSyncFinish, rawdb.StageSyncCommitment},
		{rawdb.StageSnapshotChainLookupPrune, rawdb.StageChainFreezer},
		{rawdb.StageSnapshotChainFreezerTailPrune, rawdb.StageSnapshotChainLookupPrune},
		{rawdb.StageSnapshotChainFreezerTailPrune, rawdb.StageSnapshotEventLogBuild},
	} {
		down, downOK := byStage[pair.downstream]
		up, upOK := byStage[pair.upstream]
		if !downOK {
			continue
		}
		if !upOK {
			if dbStageStatusRequiresUpstreamPresence(pair.downstream, pair.upstream) {
				issues = append(issues, fmt.Sprintf("%s requires %s", pair.downstream, pair.upstream))
			}
			continue
		}
		if down.progress.BlockNum <= up.progress.BlockNum {
			continue
		}
		issues = append(issues, fmt.Sprintf("%s=%d ahead of %s=%d",
			pair.downstream, down.progress.BlockNum, pair.upstream, up.progress.BlockNum))
	}
	return issues
}

func dbStageStatusSnapshotCoverageIssues(rows []dbStageStatusRow, snapshotDir string) []string {
	byStage := make(map[rawdb.StageID]dbStageStatusRow, len(rows))
	for _, row := range rows {
		if row.present {
			byStage[row.stage] = row
		}
	}
	needsCoverage := false
	for _, stage := range []rawdb.StageID{
		rawdb.StageSnapshotEventLogBuild,
		rawdb.StageSnapshotChainLookupPrune,
		rawdb.StageSnapshotSectionBloomPrune,
		rawdb.StageSnapshotBalanceTracePrune,
		rawdb.StageSnapshotChainFreezerTailPrune,
	} {
		if _, ok := byStage[stage]; ok {
			needsCoverage = true
			break
		}
	}
	if !needsCoverage {
		return nil
	}
	mgr, err := statesnapshots.OpenManager(snapshotDir)
	if err != nil {
		return []string{fmt.Sprintf("snapshot coverage unreadable: %v", err)}
	}
	var issues []string
	check := func(stage rawdb.StageID, block uint64, label string, fromBlock uint64, covered func(uint64, uint64) (bool, error)) {
		ok, err := covered(fromBlock, block)
		if err != nil {
			issues = append(issues, fmt.Sprintf("%s=%d cold %s coverage error: %v", stage, block, label, err))
			return
		}
		if !ok {
			issues = append(issues, fmt.Sprintf("%s=%d missing cold %s coverage [%d,%d]", stage, block, label, fromBlock, block))
		}
	}
	if row, ok := byStage[rawdb.StageSnapshotEventLogBuild]; ok && row.progress.BlockNum > 0 {
		check(row.stage, row.progress.BlockNum, "indexed event-log", 1, mgr.EventLogIndexedRangeCovered)
	}
	if row, ok := byStage[rawdb.StageSnapshotChainLookupPrune]; ok {
		check(row.stage, row.progress.BlockNum, "chain-index", 0, mgr.ChainIndexRangeCovered)
	}
	if row, ok := byStage[rawdb.StageSnapshotSectionBloomPrune]; ok {
		check(row.stage, row.progress.BlockNum, "section-bloom", 0, mgr.SectionBloomRangeCovered)
	}
	if row, ok := byStage[rawdb.StageSnapshotBalanceTracePrune]; ok {
		check(row.stage, row.progress.BlockNum, "balance-trace", 0, mgr.BalanceTraceRangeCovered)
	}
	if row, ok := byStage[rawdb.StageSnapshotChainFreezerTailPrune]; ok {
		check(row.stage, row.progress.BlockNum, "chain-freezer", 0, mgr.ChainFreezerRangeCovered)
		if row.progress.BlockNum > 0 {
			check(row.stage, row.progress.BlockNum, "indexed event-log", 1, mgr.EventLogIndexedRangeCovered)
		}
	}
	return issues
}

func dbStageStatusRequiresUpstreamPresence(downstream, upstream rawdb.StageID) bool {
	switch downstream {
	case rawdb.StageSnapshotBuild,
		rawdb.StageSnapshotLatestBuild,
		rawdb.StageSnapshotEventLogBuild,
		rawdb.StageSnapshotPrune,
		rawdb.StageChainFreezer,
		rawdb.StageSnapshotSectionBloomPrune,
		rawdb.StageSnapshotBalanceTracePrune:
		return upstream == rawdb.StageFinish
	case rawdb.StageSnapshotChainLookupPrune:
		return upstream == rawdb.StageChainFreezer
	case rawdb.StageSnapshotChainFreezerTailPrune:
		return upstream == rawdb.StageSnapshotChainLookupPrune || upstream == rawdb.StageSnapshotEventLogBuild
	default:
		return false
	}
}

func dbStageStatusRequiresCanonicalVerification(stage rawdb.StageID) bool {
	switch stage {
	case rawdb.StageHeaders,
		rawdb.StageBodies,
		rawdb.StageExecution,
		rawdb.StageCommitment,
		rawdb.StageFinish,
		rawdb.StageSyncImport,
		rawdb.StageSyncExecution,
		rawdb.StageSyncCommitment,
		rawdb.StageSyncFinish:
		return true
	default:
		return false
	}
}

func dbStageStatusRows(db ethdb.Iteratee, canonical ethdb.KeyValueReader) ([]dbStageStatusRow, error) {
	progress := make(map[rawdb.StageID]rawdb.StageProgress)
	if err := rawdb.IterateStageProgress(db, func(row rawdb.StageProgress) (bool, error) {
		progress[row.Stage] = row
		return true, nil
	}); err != nil {
		return nil, err
	}

	known := rawdb.KnownStageProgressStages()
	seen := make(map[rawdb.StageID]struct{}, len(known))
	rows := make([]dbStageStatusRow, 0, len(known)+len(progress))
	for _, stage := range known {
		seen[stage] = struct{}{}
		row, ok := progress[stage]
		rows = append(rows, dbStageStatusRowFor(stage, row, ok, canonical))
	}

	var unknown []rawdb.StageID
	for stage := range progress {
		if _, ok := seen[stage]; ok {
			continue
		}
		unknown = append(unknown, stage)
	}
	sort.Slice(unknown, func(i, j int) bool { return string(unknown[i]) < string(unknown[j]) })
	for _, stage := range unknown {
		rows = append(rows, dbStageStatusRowFor(stage, progress[stage], true, canonical))
	}
	return rows, nil
}

func dbStageStatusRowFor(stage rawdb.StageID, progress rawdb.StageProgress, present bool, canonical ethdb.KeyValueReader) dbStageStatusRow {
	row := dbStageStatusRow{
		stage:   stage,
		group:   dbStageStatusGroup(stage),
		present: present,
	}
	if !present {
		return row
	}
	row.progress = progress
	row.verified, row.canonicalHash = dbStageStatusVerification(progress, canonical)
	return row
}

func dbStageStatusVerification(progress rawdb.StageProgress, canonical ethdb.KeyValueReader) (string, common.Hash) {
	if !progress.HasBlockHash {
		return "unbound", common.Hash{}
	}
	if canonical == nil {
		return "unchecked", common.Hash{}
	}
	canonicalHash := rawdb.ReadBlockHashByNumber(canonical, progress.BlockNum)
	if canonicalHash == (common.Hash{}) {
		return "missing-canonical", canonicalHash
	}
	if canonicalHash != progress.BlockHash {
		return "mismatch", canonicalHash
	}
	return "canonical", canonicalHash
}

func dbStageStatusGroup(stage rawdb.StageID) string {
	switch stage {
	case rawdb.StageHeaders, rawdb.StageBodies, rawdb.StageExecution, rawdb.StageCommitment, rawdb.StageFinish:
		return "canonical"
	case rawdb.StageSyncInventory, rawdb.StageSyncBodies, rawdb.StageSyncBodiesReady, rawdb.StageSyncImport, rawdb.StageSyncExecution, rawdb.StageSyncCommitment, rawdb.StageSyncFinish:
		return "sync"
	case rawdb.StageSnapshotInstall, rawdb.StageSnapshotBuild, rawdb.StageSnapshotLatestBuild, rawdb.StageSnapshotEventLogBuild, rawdb.StageSnapshotLatest, rawdb.StageSnapshotHistory, rawdb.StageSnapshotAccessor, rawdb.StageSnapshotCommitmentFlush:
		return "snapshot"
	case rawdb.StageSnapshotHotPrune, rawdb.StageSnapshotPrune, rawdb.StageSnapshotChainLookupPrune, rawdb.StageSnapshotSectionBloomPrune, rawdb.StageSnapshotBalanceTracePrune, rawdb.StageSnapshotChainFreezerTailPrune:
		return "prune"
	case rawdb.StageChainFreezer:
		return "freezer"
	default:
		return "unknown"
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
	fmt.Printf("Balance trace coverage: blocks=[%d,%d] scanned=%d traceBlocks=%d missingBlockTraces=%d missingAccountTraces=%d mismatched=%d emptyTxTraceBlocks=%d\n",
		result.FromBlock,
		result.ToBlock,
		result.BlocksScanned,
		result.BlocksWithBalanceTrace,
		result.MissingBlockBalanceTrace,
		result.MissingAccountTrace,
		result.MismatchedBlockBalanceTrace,
		result.BlocksWithEmptyTxTrace,
	)
	for _, issue := range result.Issues {
		fmt.Printf("Balance trace coverage issue: block=%d kind=%s detail=%s\n", issue.BlockNum, issue.Kind, issue.Detail)
	}
	if !result.Complete() {
		return fmt.Errorf("balance trace coverage incomplete: missingBlockTraces=%d missingAccountTraces=%d mismatched=%d",
			result.MissingBlockBalanceTrace,
			result.MissingAccountTrace,
			result.MismatchedBlockBalanceTrace,
		)
	}
	return nil
}

func dbBackfillBalanceTracesCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := dbReplayGenesis(ctx)
	if err != nil {
		return err
	}
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
	replayDir, cleanupReplay, err := dbBalanceTraceReplayDir(ctx)
	if err != nil {
		return err
	}
	defer cleanupReplay()

	replayDB, err := openPebbleDB(ctx, replayDir)
	if err != nil {
		return fmt.Errorf("open replay database: %w", err)
	}
	defer replayDB.Close()
	etlOpts, err := dbETLOptions(ctx)
	if err != nil {
		return err
	}

	seed, err := dbSeedBalanceTraceReplayFromSnapshot(ctx, cfg.DataDir, chainDB, replayDB, genesis, etlOpts)
	if err != nil {
		return err
	}
	if seed != nil {
		if seed.Skipped {
			fmt.Printf("Balance trace replay snapshot seed skipped: existingHead=%d snapshotDir=%s\n", seed.ExistingHeadBlock, seed.SnapshotDir)
		} else {
			if fromBlock <= seed.Boundary.BlockNum {
				return fmt.Errorf("balance trace replay snapshot seed at block %d can only backfill from block %d or later; requested --db.from-block=%d", seed.Boundary.BlockNum, seed.Boundary.BlockNum+1, fromBlock)
			}
			fmt.Printf("Balance trace replay snapshot seeded: block=%d hash=%x txNum=%d copiedBlocks=%d snapshotDir=%s\n",
				seed.Boundary.BlockNum,
				seed.Boundary.BlockHash,
				seed.Boundary.TxNum,
				seed.CopiedRecentBlocks,
				seed.SnapshotDir,
			)
		}
	}

	lastProgress := uint64(0)
	result, err := core.BackfillBalanceTracesByReplay(chainDB, db, replayDB, genesis, core.BalanceTraceReplayBackfillOptions{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
		Overwrite: ctx.Bool("db.balance-trace.overwrite"),
		ETL:       etlOpts,
		Progress: func(p core.BalanceTraceReplayBackfillProgress) {
			if p.Block == p.Target || p.Block-lastProgress >= 10000 {
				lastProgress = p.Block
				fmt.Printf("Balance trace %s: block=%d target=%d\n", p.Phase, p.Block, p.Target)
			}
		},
	})
	if err != nil {
		return err
	}
	fmt.Printf("Balance traces backfilled: blocks=[%d,%d] replayStart=%d replayHead=%d replayed=%d backfilled=%d blockTraceRows=%d accountTraceRows=%d existingBlockTraces=%d existingAccountTraces=%d etlApplied=%d etlRuns=%d replayDir=%s\n",
		result.FromBlock,
		result.ToBlock,
		result.ReplayStartBlock,
		result.ReplayHeadBlock,
		result.BlocksReplayed,
		result.BlocksBackfilled,
		result.BlockTraceRows,
		result.AccountTraceRows,
		result.ExistingBlockTraces,
		result.ExistingAccountTraces,
		result.ETL.Applied,
		result.ETL.SpilledRuns,
		replayDir,
	)
	return nil
}

type dbBalanceTraceReplaySnapshotSeedResult struct {
	SnapshotDir        string
	Boundary           *statesnapshots.RestoreCanonicalBoundaryResult
	CopiedRecentBlocks uint64
	ExistingHeadBlock  uint64
	Skipped            bool
}

func dbSeedBalanceTraceReplayFromSnapshot(ctx *cli.Context, dataDir string, source *rawdb.ChainDB, replayDB ethdb.KeyValueStore, genesis *params.Genesis, etlOpts etl.Options) (*dbBalanceTraceReplaySnapshotSeedResult, error) {
	if ctx == nil || !ctx.IsSet("snapshot.dir") {
		return nil, nil
	}
	if source == nil {
		return nil, errors.New("balance trace replay snapshot seed: nil source chain")
	}
	if replayDB == nil {
		return nil, errors.New("balance trace replay snapshot seed: nil replay database")
	}
	if genesis == nil || genesis.Config == nil {
		return nil, errors.New("balance trace replay snapshot seed: nil genesis")
	}
	dir := snapshotDir(ctx, dataDir)
	result := &dbBalanceTraceReplaySnapshotSeedResult{SnapshotDir: dir}

	replayChain := rawdb.NewChainDB(replayDB, rawdb.NoopAncient{})
	headHash := rawdb.ReadHeadBlockHash(replayChain)
	if headHash != (common.Hash{}) {
		headNum := rawdb.ReadBlockNumber(replayChain, headHash)
		if headNum == nil {
			return nil, fmt.Errorf("balance trace replay snapshot seed: existing replay head %x has no block number", headHash)
		}
		if *headNum > 0 {
			result.ExistingHeadBlock = *headNum
			result.Skipped = true
			return result, nil
		}
	}

	trustedKeys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		return nil, err
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return nil, err
	}
	chainConfig, genesisHash, err := core.SetupGenesisBlockWithAncient(replayDB, rawdb.NoopAncient{}, genesis)
	if err != nil {
		return nil, fmt.Errorf("balance trace replay snapshot seed setup genesis: %w", err)
	}
	genesisRoot := rawdb.ReadGenesisStateRoot(replayDB)
	if err := rawdb.ResetMutableState(replayDB); err != nil {
		return nil, fmt.Errorf("balance trace replay snapshot seed reset mutable state: %w", err)
	}
	rawdb.WriteGenesisStateRoot(replayDB, genesisRoot)
	dbWriteGenesisWitnesses(replayDB, genesis)

	identity := snapshotExpectedChainIdentity(chainConfig, genesis, genesisHash, forkConfigHash)
	restoreOpts := snapshotRestoreVerificationOptions(replayDB)
	restoreOpts.ETL = statesnapshots.RestoreETLOptions{
		TempDir:     etlOpts.TempDir,
		BufferLimit: etlOpts.BufferLimit,
		BatchSize:   etlOpts.BatchSize,
	}
	if _, err := statesnapshots.RestoreSnapshotFromVerifiedCatalogWithOptions(replayDB, dir, identity, trustedKeys, restoreOpts); err != nil {
		return nil, fmt.Errorf("balance trace replay snapshot seed restore: %w", err)
	}

	boundary, err := statesnapshots.InstallCanonicalBoundaryFromVerifiedCatalog(replayDB, source, dir, identity, trustedKeys)
	if err != nil {
		return nil, fmt.Errorf("balance trace replay snapshot seed canonical boundary: %w", err)
	}
	if boundary.BlockNum == 0 {
		return nil, errors.New("balance trace replay snapshot seed: snapshot boundary at genesis cannot seed block trace replay")
	}
	boundaryRoot, ok, err := rawdb.ReadLatestDomainCommitmentRoot(replayDB)
	if err != nil {
		return nil, err
	}
	if !ok || boundaryRoot == (common.Hash{}) {
		return nil, errors.New("balance trace replay snapshot seed: restored snapshot has no latest commitment root")
	}
	if sourceRoot := rawdb.ReadBlockStateRoot(source, boundary.BlockHash); sourceRoot != (common.Hash{}) && sourceRoot != boundaryRoot {
		return nil, fmt.Errorf("balance trace replay snapshot seed: restored root %x does not match source block %d root %x", boundaryRoot, boundary.BlockNum, sourceRoot)
	}
	if err := rawdb.WriteBlockStateRoot(replayDB, boundary.BlockHash, boundaryRoot); err != nil {
		return nil, fmt.Errorf("balance trace replay snapshot seed write boundary state root: %w", err)
	}
	copied, err := dbCopyReplayRecentChainWindow(source, replayDB, boundary.BlockNum)
	if err != nil {
		return nil, err
	}
	result.Boundary = boundary
	result.CopiedRecentBlocks = copied
	return result, nil
}

func dbWriteGenesisWitnesses(db ethdb.KeyValueWriter, genesis *params.Genesis) {
	if db == nil || genesis == nil {
		return
	}
	witnesses := make([]rawdb.GenesisWitness, 0, len(genesis.Witnesses))
	for _, gw := range genesis.Witnesses {
		witnesses = append(witnesses, rawdb.GenesisWitness{
			Address:   gw.Address,
			VoteCount: gw.VoteCount,
		})
	}
	rawdb.WriteGenesisWitnesses(db, witnesses)
}

func dbCopyReplayRecentChainWindow(source *rawdb.ChainDB, target ethdb.KeyValueWriter, boundary uint64) (uint64, error) {
	if source == nil {
		return 0, errors.New("balance trace replay snapshot seed: nil source chain")
	}
	if target == nil {
		return 0, errors.New("balance trace replay snapshot seed: nil replay target")
	}
	from := uint64(0)
	if boundary > 0xffff {
		from = boundary - 0xffff
	}
	var copied uint64
	for blockNum := from; blockNum <= boundary; blockNum++ {
		block := rawdb.ReadBlock(source, blockNum)
		if block == nil {
			return copied, fmt.Errorf("balance trace replay snapshot seed: missing source block %d for recent execution window", blockNum)
		}
		if err := rawdb.WriteBlock(target, block); err != nil {
			return copied, fmt.Errorf("balance trace replay snapshot seed write block %d: %w", blockNum, err)
		}
		if err := rawdb.WriteTaposRef(target, blockNum, block.Hash()); err != nil {
			return copied, fmt.Errorf("balance trace replay snapshot seed write tapos block %d: %w", blockNum, err)
		}
		if root := rawdb.ReadBlockStateRoot(source, block.Hash()); root != (common.Hash{}) {
			if err := rawdb.WriteBlockStateRoot(target, block.Hash(), root); err != nil {
				return copied, fmt.Errorf("balance trace replay snapshot seed write block %d state root: %w", blockNum, err)
			}
		}
		copied++
	}
	return copied, nil
}

func dbBalanceTraceReplayDir(ctx *cli.Context) (string, func(), error) {
	if dir := strings.TrimSpace(ctx.String("db.replay.dir")); dir != "" {
		if strings.TrimSpace(ctx.String("db.replay.tempdir")) != "" {
			return "", func() {}, fmt.Errorf("--db.replay.dir and --db.replay.tempdir are mutually exclusive")
		}
		return dir, func() {}, nil
	}
	dir, err := os.MkdirTemp(strings.TrimSpace(ctx.String("db.replay.tempdir")), "gtron-balance-trace-replay-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("create replay tempdir: %w", err)
	}
	return dir, func() { _ = os.RemoveAll(dir) }, nil
}

func dbReplayGenesis(ctx *cli.Context) (*params.Genesis, error) {
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.Bool("dev") {
		return genesis, nil
	}
	key, err := parseWitnessKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("dev mode requires --witness.key: %w", err)
	}
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	return makeDevGenesis(witnessAddr, ctx.Bool("dev.full-features"), ctx.Int64("dev.maintenance-interval")), nil
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
