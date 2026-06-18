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
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
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
				Name:  "freezer-alerts",
				Usage: "Check chain freezer persisted alert conditions for soak monitoring",
				Flags: []cli.Flag{
					dataDirFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
				},
				Action: dbFreezerAlertsCmd,
			},
			{
				Name:  "storage-alerts",
				Usage: "Check chain freezer, stage, and cold-coverage alert conditions for soak monitoring",
				Flags: []cli.Flag{
					dataDirFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
				},
				Action: dbStorageAlertsCmd,
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
	repairRecordedAt := stats.Repair.RecordedAt
	if repairRecordedAt == "" {
		repairRecordedAt = "-"
	}
	fmt.Printf("Freezer status: datadir=%s readonly=%t head=%d tail=%d tables=%d repairApplied=%t repairTables=%d repairTargetHead=%d repairTargetTail=%d repairRecordedAt=%s\n",
		stats.Datadir, stats.ReadOnly, stats.Head, stats.Tail, len(stats.Tables),
		stats.Repair.Applied, len(stats.Repair.Tables), stats.Repair.TargetHead, stats.Repair.TargetTail, repairRecordedAt)
	for _, table := range stats.Repair.Tables {
		fmt.Printf("Freezer repair table: name=%s headBefore=%d headAfter=%d hiddenTailBefore=%d hiddenTailAfter=%d\n",
			table.Name, table.HeadBefore, table.HeadAfter, table.HiddenTailBefore, table.HiddenTailAfter)
	}
	for _, table := range stats.Tables {
		fmt.Printf("Freezer table: name=%s head=%d physicalTail=%d hiddenTail=%d prunable=%t noSnappy=%t tailFile=%d headFile=%d headBytes=%d visibleSize=%d hiddenSize=%d\n",
			table.Name, table.Head, table.PhysicalTail, table.HiddenTail, table.Prunable, table.NoSnappy,
			table.TailFile, table.HeadFile, table.HeadBytes, table.VisibleSize, table.HiddenSize)
	}
	return nil
}

type dbFreezerAlertIssue struct {
	severity string
	kind     string
	detail   string
}

type dbSnapshotAlertIssue struct {
	severity string
	kind     string
	detail   string
}

func dbFreezerAlertsCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	f, err := rawdbfreezer.NewFreezer(ancientDataDir(cfg.DataDir), "", true, freezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open freezer: %w", err)
	}
	defer f.Close()

	stats, err := f.Stats()
	if err != nil {
		return fmt.Errorf("read freezer status: %w", err)
	}
	stage, hasStage, err := rawdb.ReadStageProgress(db, rawdb.StageChainFreezer)
	if err != nil {
		return fmt.Errorf("read chain freezer stage: %w", err)
	}

	issues := dbFreezerAlertIssues(stats, stage, hasStage)
	status := "ok"
	if dbFreezerAlertHasCritical(issues) {
		status = "critical"
	} else if len(issues) > 0 {
		status = "warning"
	}
	stageLabel := "-"
	if hasStage {
		stageLabel = fmt.Sprintf("%d", stage)
	}
	fmt.Printf("Freezer alerts: datadir=%s status=%s issues=%d head=%d tail=%d chainFreezerStage=%s repairApplied=%t hiddenSize=%d\n",
		cfg.DataDir, status, len(issues), stats.Head, stats.Tail, stageLabel, stats.Repair.Applied, dbFreezerHiddenSize(stats))
	for _, issue := range issues {
		fmt.Printf("Freezer alert: severity=%s kind=%s detail=%s\n", issue.severity, issue.kind, issue.detail)
	}
	if status == "critical" {
		return fmt.Errorf("freezer alerts failed: %s", dbFreezerAlertSummary(issues))
	}
	return nil
}

func dbStorageAlertsCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	f, err := rawdbfreezer.NewFreezer(ancientDataDir(cfg.DataDir), "", true, freezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open freezer: %w", err)
	}
	defer f.Close()
	chainDB := rawdb.NewChainDB(db, rawdb.NewFreezerReader(f))

	stats, err := f.Stats()
	if err != nil {
		return fmt.Errorf("read freezer status: %w", err)
	}
	stage, hasStage, err := rawdb.ReadStageProgress(db, rawdb.StageChainFreezer)
	if err != nil {
		return fmt.Errorf("read chain freezer stage: %w", err)
	}
	freezerIssues := dbFreezerAlertIssues(stats, stage, hasStage)
	freezerStatus := dbFreezerAlertStatus(freezerIssues)

	stageRows, err := dbStageStatusRows(db, chainDB)
	if err != nil {
		return err
	}
	stageIssues := dbStageStatusVerificationIssues(stageRows)
	stageIssues = append(stageIssues, dbStageStatusStagedBodyIssues(db, stageRows)...)
	stageIssues = append(stageIssues, dbStageStatusSnapshotCoverageIssues(stageRows, stateSnapshotsDir(cfg.DataDir))...)
	stageStatus := "ok"
	if len(stageIssues) > 0 {
		stageStatus = "critical"
	}

	snapshotInspection, snapshotIssues := dbSnapshotRetiredAlertIssues(stateSnapshotsDir(cfg.DataDir))
	snapshotStatus := dbSnapshotAlertStatus(snapshotIssues)

	status := "ok"
	if freezerStatus == "critical" || stageStatus == "critical" || snapshotStatus == "critical" {
		status = "critical"
	} else if freezerStatus == "warning" || snapshotStatus == "warning" {
		status = "warning"
	}
	fmt.Printf("Storage alerts: datadir=%s status=%s freezerStatus=%s freezerIssues=%d stageStatus=%s stageIssues=%d snapshotStatus=%s snapshotIssues=%d retiredSegments=%d retiredFiles=%d retiredMissing=%d retiredSkippedActive=%d retiredBytes=%d hiddenSize=%d\n",
		cfg.DataDir, status, freezerStatus, len(freezerIssues), stageStatus, len(stageIssues),
		snapshotStatus, len(snapshotIssues), snapshotInspection.RetiredSegments, snapshotInspection.FilesPresent,
		snapshotInspection.FilesMissing, snapshotInspection.FilesSkippedActive, snapshotInspection.BytesPresent,
		dbFreezerHiddenSize(stats))
	for _, issue := range freezerIssues {
		fmt.Printf("Storage freezer alert: severity=%s kind=%s detail=%s\n", issue.severity, issue.kind, issue.detail)
	}
	for _, issue := range stageIssues {
		fmt.Printf("Storage stage alert: severity=critical detail=%s\n", issue)
	}
	for _, issue := range snapshotIssues {
		fmt.Printf("Storage snapshot alert: severity=%s kind=%s detail=%s\n", issue.severity, issue.kind, issue.detail)
	}
	if status == "critical" {
		return fmt.Errorf("storage alerts failed: freezer=%s stage=%s snapshot=%s",
			dbFreezerAlertSummary(freezerIssues), dbStageAlertSummary(stageIssues), dbSnapshotAlertSummary(snapshotIssues))
	}
	return nil
}

func dbFreezerAlertStatus(issues []dbFreezerAlertIssue) string {
	if dbFreezerAlertHasCritical(issues) {
		return "critical"
	}
	if len(issues) > 0 {
		return "warning"
	}
	return "ok"
}

func dbSnapshotRetiredAlertIssues(snapshotDir string) (*statesnapshots.RetiredSegmentFileInspection, []dbSnapshotAlertIssue) {
	inspection, err := statesnapshots.InspectRetiredSegmentFiles(snapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return &statesnapshots.RetiredSegmentFileInspection{}, nil
		}
		return &statesnapshots.RetiredSegmentFileInspection{}, []dbSnapshotAlertIssue{{
			severity: "critical",
			kind:     "retired-inspect-failed",
			detail:   err.Error(),
		}}
	}
	var issues []dbSnapshotAlertIssue
	if inspection.FilesPresent > 0 {
		issues = append(issues, dbSnapshotAlertIssue{
			severity: "warning",
			kind:     "retired-prune-pending",
			detail:   fmt.Sprintf("%d retired snapshot file(s) still occupy %d bytes", inspection.FilesPresent, inspection.BytesPresent),
		})
	}
	if inspection.FilesSkippedActive > 0 {
		issues = append(issues, dbSnapshotAlertIssue{
			severity: "warning",
			kind:     "retired-active-path",
			detail:   fmt.Sprintf("%d retired snapshot ref(s) still point at active manifest files", inspection.FilesSkippedActive),
		})
	}
	return inspection, issues
}

func dbSnapshotAlertStatus(issues []dbSnapshotAlertIssue) string {
	for _, issue := range issues {
		if issue.severity == "critical" {
			return "critical"
		}
	}
	if len(issues) > 0 {
		return "warning"
	}
	return "ok"
}

func dbFreezerAlertIssues(stats rawdbfreezer.Stats, chainFreezerStage uint64, hasChainFreezerStage bool) []dbFreezerAlertIssue {
	var issues []dbFreezerAlertIssue
	add := func(severity, kind, format string, args ...interface{}) {
		issues = append(issues, dbFreezerAlertIssue{
			severity: severity,
			kind:     kind,
			detail:   fmt.Sprintf(format, args...),
		})
	}

	if stats.Tail > stats.Head {
		add("critical", "tail-ahead-of-head", "freezer tail %d exceeds head %d", stats.Tail, stats.Head)
	}
	if stats.Repair.Applied {
		add("critical", "repair-applied", "last writable open repaired %d table(s) to head=%d tail=%d recordedAt=%s",
			len(stats.Repair.Tables), stats.Repair.TargetHead, stats.Repair.TargetTail, stats.Repair.RecordedAt)
	}
	if stats.Head > 0 && !hasChainFreezerStage {
		add("critical", "chain-freezer-stage-missing", "freezer head=%d but %s stage is missing", stats.Head, rawdb.StageChainFreezer)
	}
	if hasChainFreezerStage {
		switch {
		case stats.Head == 0:
			add("critical", "chain-freezer-stage-with-empty-freezer", "%s=%d but freezer head is 0", rawdb.StageChainFreezer, chainFreezerStage)
		case chainFreezerStage >= stats.Head:
			add("critical", "chain-freezer-stage-ahead", "%s=%d exceeds freezer max block %d", rawdb.StageChainFreezer, chainFreezerStage, stats.Head-1)
		case chainFreezerStage < stats.Tail:
			add("critical", "chain-freezer-stage-behind-tail", "%s=%d is below freezer visible tail %d", rawdb.StageChainFreezer, chainFreezerStage, stats.Tail)
		}
	}
	for _, table := range stats.Tables {
		if table.Head != stats.Head {
			add("critical", "table-head-mismatch", "table %s head=%d freezerHead=%d", table.Name, table.Head, stats.Head)
		}
		if table.HiddenTail > table.Head {
			add("critical", "table-hidden-tail-ahead", "table %s hiddenTail=%d head=%d", table.Name, table.HiddenTail, table.Head)
		}
		if table.PhysicalTail > table.HiddenTail {
			add("critical", "table-physical-tail-ahead", "table %s physicalTail=%d hiddenTail=%d", table.Name, table.PhysicalTail, table.HiddenTail)
		}
		if table.Prunable {
			if table.HiddenTail != stats.Tail {
				add("critical", "table-tail-mismatch", "table %s hiddenTail=%d freezerTail=%d", table.Name, table.HiddenTail, stats.Tail)
			}
		} else if table.HiddenTail != 0 {
			add("critical", "non-prunable-hidden-tail", "table %s hiddenTail=%d", table.Name, table.HiddenTail)
		}
	}
	if hidden := dbFreezerHiddenSize(stats); hidden > 0 {
		add("warning", "physical-prune-pending", "freezer has %d hidden bytes waiting for physical tail-file pruning", hidden)
	}
	return issues
}

func dbFreezerAlertHasCritical(issues []dbFreezerAlertIssue) bool {
	for _, issue := range issues {
		if issue.severity == "critical" {
			return true
		}
	}
	return false
}

func dbFreezerAlertSummary(issues []dbFreezerAlertIssue) string {
	var critical []string
	for _, issue := range issues {
		if issue.severity == "critical" {
			critical = append(critical, issue.kind)
		}
	}
	if len(critical) == 0 {
		return "no critical issues"
	}
	return strings.Join(critical, ",")
}

func dbStageAlertSummary(issues []string) string {
	if len(issues) == 0 {
		return "ok"
	}
	return strings.Join(issues, "; ")
}

func dbSnapshotAlertSummary(issues []dbSnapshotAlertIssue) string {
	var critical []string
	for _, issue := range issues {
		if issue.severity == "critical" {
			critical = append(critical, issue.kind)
		}
	}
	if len(critical) == 0 {
		return "no critical issues"
	}
	return strings.Join(critical, ",")
}

func dbFreezerHiddenSize(stats rawdbfreezer.Stats) uint64 {
	var hidden uint64
	for _, table := range stats.Tables {
		hidden += table.HiddenSize
	}
	return hidden
}

func openSnapshotPruneChainDB(db ethdb.KeyValueStore, dataDir string) (*rawdb.ChainDB, func(), error) {
	ancientReader, closeAncient, err := openSnapshotPruneAncientReader(dataDir)
	if err != nil {
		return nil, closeAncient, err
	}
	snapshotManager, err := statesnapshots.OpenManager(stateSnapshotsDir(dataDir))
	if err != nil {
		closeAncient()
		return nil, func() {}, fmt.Errorf("open state snapshots: %w", err)
	}
	chainDB := rawdb.NewChainDB(db, rawdb.NewFallbackAncientReader(ancientReader, snapshotManager))
	chainDB.SetChainIndexReader(snapshotManager)
	chainDB.SetBalanceTraceReader(snapshotManager)
	chainDB.SetSectionBloomReader(snapshotManager)
	chainDB.SetEventLogReader(snapshotManager)
	return chainDB, closeAncient, nil
}

func dbStageStatusCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	chainDB, closeAncient, err := openSnapshotPruneChainDB(db, cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

	return dbPrintStageStatus(db, chainDB, cfg.DataDir, dbStageStatusOptions{
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
		issues = append(issues, dbStageStatusStagedBodyIssues(db, rows)...)
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

func dbStageStatusStagedBodyIssues(db ethdb.KeyValueReader, rows []dbStageStatusRow) []string {
	if db == nil {
		return nil
	}
	var issues []string
	for _, row := range rows {
		if !row.present {
			continue
		}
		switch row.stage {
		case rawdb.StageSyncBodies:
			progress := syncdl.ReadStagedBodyProgress(db, rawdb.StageSyncBodies)
			if !progress.Valid() {
				issues = append(issues, dbStageStatusStagedBodyProgressIssue(rawdb.StageSyncBodies, progress))
			}
		case rawdb.StageSyncBodiesReady:
			ready := syncdl.ReadStagedBodyReadyDrainLimit(db, row.progress.BlockNum)
			if !ready.Valid() {
				issues = append(issues, dbStageStatusStagedBodyReadyIssue(ready))
			}
		}
	}
	return issues
}

func dbStageStatusStagedBodyReadyIssue(ready syncdl.StagedBodyReadyLimit) string {
	parts := []string{
		fmt.Sprintf("%s staged-body status=%s block=%d", rawdb.StageSyncBodiesReady, dbStageStatusStagedBodyStatus(ready.Status), ready.StageRow.BlockNum),
	}
	if ready.StageRow.HasBlockHash {
		parts = append(parts, fmt.Sprintf("hash=%x", ready.StageRow.BlockHash))
	}
	if ready.StagedRow.Number != 0 || ready.StagedHash != (common.Hash{}) {
		parts = append(parts, fmt.Sprintf("stagedBlock=%d stagedHash=%x", ready.StagedRow.Number, ready.StagedHash))
	}
	if ready.StageError != nil {
		parts = append(parts, fmt.Sprintf("stageError=%q", ready.StageError.Error()))
	}
	if ready.ReadError != nil {
		parts = append(parts, fmt.Sprintf("readError=%q", ready.ReadError.Error()))
	}
	return strings.Join(parts, " ")
}

func dbStageStatusStagedBodyProgressIssue(stage rawdb.StageID, progress syncdl.StagedBodyProgressCheck) string {
	parts := []string{
		fmt.Sprintf("%s staged-body status=%s block=%d", stage, dbStageStatusStagedBodyProgressStatus(progress.Status), progress.StageRow.BlockNum),
	}
	if progress.StageRow.HasBlockHash {
		parts = append(parts, fmt.Sprintf("hash=%x", progress.StageRow.BlockHash))
	}
	if progress.StagedRow.Number != 0 || progress.StagedHash != (common.Hash{}) {
		parts = append(parts, fmt.Sprintf("stagedBlock=%d stagedHash=%x", progress.StagedRow.Number, progress.StagedHash))
	}
	if progress.StageError != nil {
		parts = append(parts, fmt.Sprintf("stageError=%q", progress.StageError.Error()))
	}
	if progress.ReadError != nil {
		parts = append(parts, fmt.Sprintf("readError=%q", progress.ReadError.Error()))
	}
	return strings.Join(parts, " ")
}

func dbStageStatusStagedBodyStatus(status syncdl.StagedBodyReadyLimitStatus) string {
	switch status {
	case syncdl.StagedBodyReadyLimitMissing:
		return "missing"
	case syncdl.StagedBodyReadyLimitProgressReadError:
		return "progress-read-error"
	case syncdl.StagedBodyReadyLimitUnbound:
		return "unbound"
	case syncdl.StagedBodyReadyLimitStale:
		return "stale"
	case syncdl.StagedBodyReadyLimitReadError:
		return "staged-read-error"
	case syncdl.StagedBodyReadyLimitStagedMissing:
		return "staged-missing"
	case syncdl.StagedBodyReadyLimitNumberMismatch:
		return "number-mismatch"
	case syncdl.StagedBodyReadyLimitHashMismatch:
		return "hash-mismatch"
	case syncdl.StagedBodyReadyLimitValid:
		return "valid"
	default:
		return "unknown"
	}
}

func dbStageStatusStagedBodyProgressStatus(status syncdl.StagedBodyProgressStatus) string {
	switch status {
	case syncdl.StagedBodyProgressMissing:
		return "missing"
	case syncdl.StagedBodyProgressReadError:
		return "progress-read-error"
	case syncdl.StagedBodyProgressUnbound:
		return "unbound"
	case syncdl.StagedBodyProgressStagedReadError:
		return "staged-read-error"
	case syncdl.StagedBodyProgressStagedMissing:
		return "staged-missing"
	case syncdl.StagedBodyProgressNumberMismatch:
		return "number-mismatch"
	case syncdl.StagedBodyProgressHashMismatch:
		return "hash-mismatch"
	case syncdl.StagedBodyProgressValid:
		return "valid"
	default:
		return "unknown"
	}
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
	for _, issue := range syncdl.CheckSyncPipelineProgressOrder(dbStageStatusSyncProgressRows(byStage), syncdl.SyncPipelineProgressOrderOptions{}) {
		issues = append(issues, issue.String())
	}
	return issues
}

func dbStageStatusSyncProgressRows(rows map[rawdb.StageID]dbStageStatusRow) map[rawdb.StageID]rawdb.StageProgress {
	progress := make(map[rawdb.StageID]rawdb.StageProgress)
	for _, stage := range syncdl.FullSyncPipelineProgressStages() {
		row, ok := rows[stage]
		if ok {
			progress[stage] = row.progress
		}
	}
	return progress
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
		rawdb.StageSnapshotLatest,
		rawdb.StageSnapshotHistory,
		rawdb.StageSnapshotAccessor,
		rawdb.StageSnapshotCommitmentFlush,
		rawdb.StageSnapshotHotPrune,
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
	manifest := mgr.Manifest()
	var progress *statesnapshots.Progress
	if manifest != nil {
		progress = manifest.Progress
	}
	checkProgress := func(stage rawdb.StageID, label string, covered uint64) {
		row, ok := byStage[stage]
		if !ok || row.progress.BlockNum == 0 {
			return
		}
		if progress == nil || covered == 0 {
			issues = append(issues, fmt.Sprintf("%s=%d missing snapshot manifest %s progress", stage, row.progress.BlockNum, label))
			return
		}
		if row.progress.BlockNum > covered {
			issues = append(issues, fmt.Sprintf("%s=%d ahead of snapshot manifest %s progress %d", stage, row.progress.BlockNum, label, covered))
		}
	}
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
	if progress != nil {
		checkProgress(rawdb.StageSnapshotLatest, "latest", progress.LatestBuildTxNum)
		checkProgress(rawdb.StageSnapshotHistory, "history", progress.HistoryBuildTxNum)
		checkProgress(rawdb.StageSnapshotAccessor, "accessor", progress.AccessorBuildTxNum)
		checkProgress(rawdb.StageSnapshotCommitmentFlush, "commitment-flush", progress.CommitmentFlushTxNum)
		checkProgress(rawdb.StageSnapshotHotPrune, "hot-prune", progress.HotPruneTxNum)
	} else {
		checkProgress(rawdb.StageSnapshotLatest, "latest", 0)
		checkProgress(rawdb.StageSnapshotHistory, "history", 0)
		checkProgress(rawdb.StageSnapshotAccessor, "accessor", 0)
		checkProgress(rawdb.StageSnapshotCommitmentFlush, "commitment-flush", 0)
		checkProgress(rawdb.StageSnapshotHotPrune, "hot-prune", 0)
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

	chainDB, closeAncient, err := openSnapshotPruneChainDB(db, cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

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

	chainDB, closeAncient, err := openSnapshotPruneChainDB(db, cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

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

	chainDB, closeAncient, err := openSnapshotPruneChainDB(db, cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

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

	chainDB, closeAncient, err := openSnapshotPruneChainDB(db, cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

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

	chainDB, closeAncient, err := openSnapshotPruneChainDB(db, cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

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
		FromBlock:         fromBlock,
		ToBlock:           toBlock,
		Overwrite:         ctx.Bool("db.balance-trace.overwrite"),
		ETL:               etlOpts,
		TargetTraceReader: chainDB,
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
