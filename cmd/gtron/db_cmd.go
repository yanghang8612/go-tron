package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
		Usage: "Fail stage-status when present stage rows are unverifiable or canonical/cold-coverage stages are not hash-bound",
	}
	dbAlertJSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Emit machine-readable JSON alert output",
	}
	dbAlertPrometheusFlag = &cli.BoolFlag{
		Name:  "prometheus",
		Usage: "Emit Prometheus text-format storage alert metrics",
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
					dbAlertJSONFlag,
					dbAlertPrometheusFlag,
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
					dbAlertJSONFlag,
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

type dbModeAlertIssue struct {
	severity string
	kind     string
	detail   string
}

type dbStorageAlertIssueJSON struct {
	Severity string `json:"severity"`
	Kind     string `json:"kind,omitempty"`
	Detail   string `json:"detail"`
}

type dbStorageAlertsJSON struct {
	Datadir                      string                    `json:"datadir"`
	Status                       string                    `json:"status"`
	FreezerStatus                string                    `json:"freezerStatus"`
	FreezerIssues                int                       `json:"freezerIssues"`
	FreezerAlertDetails          []dbStorageAlertIssueJSON `json:"freezerAlertDetails"`
	FreezerAlertHiddenBytes      uint64                    `json:"freezerAlertHiddenBytes"`
	StageStatus                  string                    `json:"stageStatus"`
	StageIssues                  int                       `json:"stageIssues"`
	StageVerifyDetails           []dbStorageAlertIssueJSON `json:"stageVerifyDetails"`
	StagePipeline                dbStageStatusPipelineJSON `json:"stagePipeline"`
	ModeStatus                   string                    `json:"modeStatus"`
	ModeIssues                   int                       `json:"modeIssues"`
	ModeAlertDetails             []dbStorageAlertIssueJSON `json:"modeAlertDetails"`
	PruneMode                    string                    `json:"pruneMode"`
	PruneModePersisted           bool                      `json:"pruneModePersisted"`
	SnapshotStatus               string                    `json:"snapshotStatus"`
	SnapshotIssues               int                       `json:"snapshotIssues"`
	SnapshotAlertDetails         []dbStorageAlertIssueJSON `json:"snapshotAlertDetails"`
	SnapshotRetiredSegments      int                       `json:"snapshotRetiredSegments"`
	SnapshotRetiredFiles         int                       `json:"snapshotRetiredFiles"`
	SnapshotRetiredMissing       int                       `json:"snapshotRetiredMissing"`
	SnapshotRetiredSkippedActive int                       `json:"snapshotRetiredSkippedActive"`
	SnapshotRetiredBytes         uint64                    `json:"snapshotRetiredBytes"`
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
	chainDB, err := dbFreezerAlertChainDB(db, f, cfg.DataDir)
	if err != nil {
		return err
	}

	stats, err := f.Stats()
	if err != nil {
		return fmt.Errorf("read freezer status: %w", err)
	}
	stage, hasStage, err := rawdb.ReadStageProgressRow(db, rawdb.StageChainFreezer)
	if err != nil {
		return fmt.Errorf("read chain freezer stage: %w", err)
	}
	tailPruneStage, hasTailPruneStage, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotChainFreezerTailPrune)
	if err != nil {
		return fmt.Errorf("read chain freezer tail prune stage: %w", err)
	}

	issues := dbFreezerAlertIssues(stats, stage, hasStage, tailPruneStage, hasTailPruneStage)
	issues = append(issues, dbFreezerAlertStageProofIssues(chainDB, stage, hasStage, tailPruneStage, hasTailPruneStage)...)
	status := "ok"
	if dbFreezerAlertHasCritical(issues) {
		status = "critical"
	} else if len(issues) > 0 {
		status = "warning"
	}
	stageLabel := "-"
	if hasStage {
		stageLabel = fmt.Sprintf("%d", stage.BlockNum)
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
	if ctx.Bool(dbAlertJSONFlag.Name) && ctx.Bool(dbAlertPrometheusFlag.Name) {
		return fmt.Errorf("--%s and --%s are mutually exclusive", dbAlertJSONFlag.Name, dbAlertPrometheusFlag.Name)
	}
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
	chainDB, err := dbFreezerAlertChainDB(db, f, cfg.DataDir)
	if err != nil {
		return err
	}

	stats, err := f.Stats()
	if err != nil {
		return fmt.Errorf("read freezer status: %w", err)
	}
	stage, hasStage, err := rawdb.ReadStageProgressRow(db, rawdb.StageChainFreezer)
	if err != nil {
		return fmt.Errorf("read chain freezer stage: %w", err)
	}
	tailPruneStage, hasTailPruneStage, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotChainFreezerTailPrune)
	if err != nil {
		return fmt.Errorf("read chain freezer tail prune stage: %w", err)
	}
	freezerIssues := dbFreezerAlertIssues(stats, stage, hasStage, tailPruneStage, hasTailPruneStage)
	freezerIssues = append(freezerIssues, dbFreezerAlertStageProofIssues(chainDB, stage, hasStage, tailPruneStage, hasTailPruneStage)...)
	freezerStatus := dbFreezerAlertStatus(freezerIssues)

	stageRows, err := dbStageStatusRows(db, chainDB)
	if err != nil {
		return err
	}
	stageIssueDetails := dbStageStatusIssueDetails(stageRows, db, stateSnapshotsDir(cfg.DataDir))
	stageIssues := dbStageStatusIssueStrings(stageIssueDetails)
	stageStatus := "ok"
	if len(stageIssues) > 0 {
		stageStatus = "critical"
	}
	stagePipeline := dbStageStatusPipeline(stageRows)
	pruneMode, pruneModePersisted, modeIssues := dbModeAlertIssues(db, stageRows)
	modeStatus := dbModeAlertStatus(modeIssues)

	snapshotInspection, snapshotIssues := dbSnapshotRetiredAlertIssues(stateSnapshotsDir(cfg.DataDir))
	snapshotStatus := dbSnapshotAlertStatus(snapshotIssues)

	status := "ok"
	if freezerStatus == "critical" || stageStatus == "critical" || modeStatus == "critical" || snapshotStatus == "critical" {
		status = "critical"
	} else if freezerStatus == "warning" || modeStatus == "warning" || snapshotStatus == "warning" {
		status = "warning"
	}
	report := dbStorageAlertsJSON{
		Datadir:                      cfg.DataDir,
		Status:                       status,
		FreezerStatus:                freezerStatus,
		FreezerIssues:                len(freezerIssues),
		FreezerAlertDetails:          dbFreezerAlertIssuesJSON(freezerIssues),
		FreezerAlertHiddenBytes:      dbFreezerHiddenSize(stats),
		StageStatus:                  stageStatus,
		StageIssues:                  len(stageIssues),
		StageVerifyDetails:           dbStageAlertIssueDetailsJSON(stageIssueDetails),
		StagePipeline:                dbStageStatusPipelineJSONForCursor(stagePipeline),
		ModeStatus:                   modeStatus,
		ModeIssues:                   len(modeIssues),
		ModeAlertDetails:             dbModeAlertIssuesJSON(modeIssues),
		PruneMode:                    pruneMode,
		PruneModePersisted:           pruneModePersisted,
		SnapshotStatus:               snapshotStatus,
		SnapshotIssues:               len(snapshotIssues),
		SnapshotAlertDetails:         dbSnapshotAlertIssuesJSON(snapshotIssues),
		SnapshotRetiredSegments:      snapshotInspection.RetiredSegments,
		SnapshotRetiredFiles:         snapshotInspection.FilesPresent,
		SnapshotRetiredMissing:       snapshotInspection.FilesMissing,
		SnapshotRetiredSkippedActive: snapshotInspection.FilesSkippedActive,
		SnapshotRetiredBytes:         snapshotInspection.BytesPresent,
	}
	if ctx.Bool(dbAlertJSONFlag.Name) {
		enc := json.NewEncoder(os.Stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(report); err != nil {
			return fmt.Errorf("encode storage alerts json: %w", err)
		}
		if status == "critical" {
			return fmt.Errorf("storage alerts failed: freezer=%s stage=%s mode=%s snapshot=%s",
				dbFreezerAlertSummary(freezerIssues), dbStageAlertSummary(stageIssues), dbModeAlertSummary(modeIssues), dbSnapshotAlertSummary(snapshotIssues))
		}
		return nil
	}
	if ctx.Bool(dbAlertPrometheusFlag.Name) {
		dbWriteStorageAlertsPrometheus(os.Stdout, report)
		if status == "critical" {
			return fmt.Errorf("storage alerts failed: freezer=%s stage=%s mode=%s snapshot=%s",
				dbFreezerAlertSummary(freezerIssues), dbStageAlertSummary(stageIssues), dbModeAlertSummary(modeIssues), dbSnapshotAlertSummary(snapshotIssues))
		}
		return nil
	}
	fmt.Printf("Storage alerts: datadir=%s status=%s freezerStatus=%s freezerIssues=%d stageStatus=%s stageIssues=%d stagePipelineComplete=%t stagePipelinePending=%d stagePipelineIssues=%d",
		cfg.DataDir, status, freezerStatus, len(freezerIssues), stageStatus, len(stageIssues),
		stagePipeline.Complete, stagePipeline.Pending, len(stagePipeline.Issues))
	if len(stagePipeline.Tasks) > 0 {
		next := stagePipeline.Tasks[0]
		fmt.Printf(" stagePipelineNext=%s stagePipelineNextStatus=%s stagePipelineNextTarget=%d stagePipelineNextUpstream=%s stagePipelineNextCurrent=%d",
			next.Stage, next.Status, next.TargetBlock, next.Upstream, next.CurrentBlock)
	}
	fmt.Printf(" modeStatus=%s modeIssues=%d pruneMode=%s pruneModePersisted=%t snapshotStatus=%s snapshotIssues=%d retiredSegments=%d retiredFiles=%d retiredMissing=%d retiredSkippedActive=%d retiredBytes=%d hiddenSize=%d\n",
		modeStatus, len(modeIssues), pruneMode, pruneModePersisted,
		snapshotStatus, len(snapshotIssues), snapshotInspection.RetiredSegments, snapshotInspection.FilesPresent,
		snapshotInspection.FilesMissing, snapshotInspection.FilesSkippedActive, snapshotInspection.BytesPresent,
		dbFreezerHiddenSize(stats))
	for _, issue := range freezerIssues {
		fmt.Printf("Storage freezer alert: severity=%s kind=%s detail=%s\n", issue.severity, issue.kind, issue.detail)
	}
	for _, issue := range report.StageVerifyDetails {
		severity := issue.Severity
		if severity == "" {
			severity = "critical"
		}
		kind := issue.Kind
		if kind == "" {
			kind = "unclassified"
		}
		fmt.Printf("Storage stage alert: severity=%s kind=%s detail=%s\n", severity, kind, issue.Detail)
	}
	for _, issue := range modeIssues {
		fmt.Printf("Storage mode alert: severity=%s kind=%s detail=%s\n", issue.severity, issue.kind, issue.detail)
	}
	for _, issue := range snapshotIssues {
		fmt.Printf("Storage snapshot alert: severity=%s kind=%s detail=%s\n", issue.severity, issue.kind, issue.detail)
	}
	if status == "critical" {
		return fmt.Errorf("storage alerts failed: freezer=%s stage=%s mode=%s snapshot=%s",
			dbFreezerAlertSummary(freezerIssues), dbStageAlertSummary(stageIssues), dbModeAlertSummary(modeIssues), dbSnapshotAlertSummary(snapshotIssues))
	}
	return nil
}

func dbWriteStorageAlertsPrometheus(w io.Writer, report dbStorageAlertsJSON) {
	labels := dbPrometheusLabels(map[string]string{"datadir": report.Datadir})
	fmt.Fprintln(w, "# HELP gtron_storage_alert_status Overall storage alert status: 0=ok, 1=warning, 2=critical.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_status gauge")
	fmt.Fprintf(w, "gtron_storage_alert_status{%s} %d\n", labels, dbStorageAlertStatusMetricValue(report.Status))

	fmt.Fprintln(w, "# HELP gtron_storage_alert_component_status Component storage alert status: 0=ok, 1=warning, 2=critical.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_component_status gauge")
	fmt.Fprintln(w, "# HELP gtron_storage_alert_component_issues Component storage alert issue count.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_component_issues gauge")
	for _, component := range []struct {
		name   string
		status string
		issues int
	}{
		{name: "freezer", status: report.FreezerStatus, issues: report.FreezerIssues},
		{name: "stage", status: report.StageStatus, issues: report.StageIssues},
		{name: "mode", status: report.ModeStatus, issues: report.ModeIssues},
		{name: "snapshot", status: report.SnapshotStatus, issues: report.SnapshotIssues},
	} {
		componentLabels := dbPrometheusLabels(map[string]string{
			"component": component.name,
			"datadir":   report.Datadir,
		})
		fmt.Fprintf(w, "gtron_storage_alert_component_status{%s} %d\n", componentLabels, dbStorageAlertStatusMetricValue(component.status))
		fmt.Fprintf(w, "gtron_storage_alert_component_issues{%s} %d\n", componentLabels, component.issues)
	}

	dbWriteStorageAlertIssuePrometheus(w, report)
	dbWriteStorageStagePipelinePrometheus(w, report)

	fmt.Fprintln(w, "# HELP gtron_storage_alert_freezer_hidden_bytes Bytes hidden by freezer virtual-tail pruning.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_freezer_hidden_bytes gauge")
	fmt.Fprintf(w, "gtron_storage_alert_freezer_hidden_bytes{%s} %d\n", labels, report.FreezerAlertHiddenBytes)

	fmt.Fprintln(w, "# HELP gtron_storage_alert_snapshot_retired_segments Retired snapshot segments recorded in the manifest.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_snapshot_retired_segments gauge")
	fmt.Fprintf(w, "gtron_storage_alert_snapshot_retired_segments{%s} %d\n", labels, report.SnapshotRetiredSegments)
	fmt.Fprintln(w, "# HELP gtron_storage_alert_snapshot_retired_files Retired snapshot files still present on disk.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_snapshot_retired_files gauge")
	fmt.Fprintf(w, "gtron_storage_alert_snapshot_retired_files{%s} %d\n", labels, report.SnapshotRetiredFiles)
	fmt.Fprintln(w, "# HELP gtron_storage_alert_snapshot_retired_missing Retired snapshot files missing from disk.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_snapshot_retired_missing gauge")
	fmt.Fprintf(w, "gtron_storage_alert_snapshot_retired_missing{%s} %d\n", labels, report.SnapshotRetiredMissing)
	fmt.Fprintln(w, "# HELP gtron_storage_alert_snapshot_retired_skipped_active Retired snapshot files skipped because they are still active manifest files.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_snapshot_retired_skipped_active gauge")
	fmt.Fprintf(w, "gtron_storage_alert_snapshot_retired_skipped_active{%s} %d\n", labels, report.SnapshotRetiredSkippedActive)
	fmt.Fprintln(w, "# HELP gtron_storage_alert_snapshot_retired_bytes Retired snapshot bytes still present on disk.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_snapshot_retired_bytes gauge")
	fmt.Fprintf(w, "gtron_storage_alert_snapshot_retired_bytes{%s} %d\n", labels, report.SnapshotRetiredBytes)

	modeLabels := dbPrometheusLabels(map[string]string{
		"datadir":   report.Datadir,
		"mode":      report.PruneMode,
		"persisted": fmt.Sprintf("%t", report.PruneModePersisted),
	})
	fmt.Fprintln(w, "# HELP gtron_storage_prune_mode_info Persisted Erigon-style prune mode selected for this datadir.")
	fmt.Fprintln(w, "# TYPE gtron_storage_prune_mode_info gauge")
	fmt.Fprintf(w, "gtron_storage_prune_mode_info{%s} 1\n", modeLabels)
}

func dbWriteStorageStagePipelinePrometheus(w io.Writer, report dbStorageAlertsJSON) {
	labels := dbPrometheusLabels(map[string]string{"datadir": report.Datadir})
	fmt.Fprintln(w, "# HELP gtron_storage_stage_pipeline_complete Stage pipeline cursor completion: 1=complete, 0=pending or blocked.")
	fmt.Fprintln(w, "# TYPE gtron_storage_stage_pipeline_complete gauge")
	complete := 0
	if report.StagePipeline.Complete {
		complete = 1
	}
	fmt.Fprintf(w, "gtron_storage_stage_pipeline_complete{%s} %d\n", labels, complete)

	fmt.Fprintln(w, "# HELP gtron_storage_stage_pipeline_pending Pending canonical or storage-maintenance stage edges.")
	fmt.Fprintln(w, "# TYPE gtron_storage_stage_pipeline_pending gauge")
	fmt.Fprintf(w, "gtron_storage_stage_pipeline_pending{%s} %d\n", labels, report.StagePipeline.Pending)

	fmt.Fprintln(w, "# HELP gtron_storage_stage_pipeline_issues Stage pipeline ordering/hash issues blocking scheduling.")
	fmt.Fprintln(w, "# TYPE gtron_storage_stage_pipeline_issues gauge")
	fmt.Fprintf(w, "gtron_storage_stage_pipeline_issues{%s} %d\n", labels, report.StagePipeline.Issues)

	if len(report.StagePipeline.Tasks) == 0 {
		return
	}
	next := report.StagePipeline.Tasks[0]
	taskLabels := dbPrometheusLabels(map[string]string{
		"datadir":  report.Datadir,
		"stage":    next.Stage,
		"status":   next.Status,
		"upstream": next.Upstream,
	})
	fmt.Fprintln(w, "# HELP gtron_storage_stage_pipeline_next_target_block Target block for the next schedulable canonical or storage-maintenance stage edge.")
	fmt.Fprintln(w, "# TYPE gtron_storage_stage_pipeline_next_target_block gauge")
	fmt.Fprintf(w, "gtron_storage_stage_pipeline_next_target_block{%s} %d\n", taskLabels, next.TargetValue)
	fmt.Fprintln(w, "# HELP gtron_storage_stage_pipeline_next_current_block Current block for the next schedulable canonical or storage-maintenance stage edge.")
	fmt.Fprintln(w, "# TYPE gtron_storage_stage_pipeline_next_current_block gauge")
	fmt.Fprintf(w, "gtron_storage_stage_pipeline_next_current_block{%s} %d\n", taskLabels, next.CurrentValue)
}

func dbWriteStorageAlertIssuePrometheus(w io.Writer, report dbStorageAlertsJSON) {
	type issueKey struct {
		component string
		severity  string
		kind      string
	}
	counts := make(map[issueKey]int)
	add := func(component string, issues []dbStorageAlertIssueJSON) {
		for _, issue := range issues {
			severity := issue.Severity
			if severity == "" {
				severity = "unknown"
			}
			kind := issue.Kind
			if kind == "" {
				kind = "unclassified"
			}
			counts[issueKey{component: component, severity: severity, kind: kind}]++
		}
	}
	add("freezer", report.FreezerAlertDetails)
	add("stage", report.StageVerifyDetails)
	add("mode", report.ModeAlertDetails)
	add("snapshot", report.SnapshotAlertDetails)

	fmt.Fprintln(w, "# HELP gtron_storage_alert_issue Storage alert issue count by component, severity, and issue kind.")
	fmt.Fprintln(w, "# TYPE gtron_storage_alert_issue gauge")
	if len(counts) == 0 {
		return
	}
	keys := make([]issueKey, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].component != keys[j].component {
			return keys[i].component < keys[j].component
		}
		if keys[i].severity != keys[j].severity {
			return keys[i].severity < keys[j].severity
		}
		return keys[i].kind < keys[j].kind
	})
	for _, key := range keys {
		issueLabels := dbPrometheusLabels(map[string]string{
			"component": key.component,
			"datadir":   report.Datadir,
			"kind":      key.kind,
			"severity":  key.severity,
		})
		fmt.Fprintf(w, "gtron_storage_alert_issue{%s} %d\n", issueLabels, counts[key])
	}
}

func dbStorageAlertStatusMetricValue(status string) int {
	switch status {
	case "ok":
		return 0
	case "warning":
		return 1
	case "critical":
		return 2
	default:
		return -1
	}
}

func dbPrometheusLabels(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", key, dbPrometheusLabelValue(labels[key])))
	}
	return strings.Join(parts, ",")
}

func dbPrometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func dbFreezerAlertIssuesJSON(issues []dbFreezerAlertIssue) []dbStorageAlertIssueJSON {
	out := make([]dbStorageAlertIssueJSON, 0, len(issues))
	for _, issue := range issues {
		out = append(out, dbStorageAlertIssueJSON{
			Severity: issue.severity,
			Kind:     issue.kind,
			Detail:   issue.detail,
		})
	}
	return out
}

func dbStageAlertIssueDetailsJSON(details []dbStageStatusIssueJSON) []dbStorageAlertIssueJSON {
	out := make([]dbStorageAlertIssueJSON, 0, len(details))
	for _, detail := range details {
		out = append(out, dbStorageAlertIssueJSON{
			Severity: detail.Severity,
			Kind:     detail.Kind,
			Detail:   detail.Detail,
		})
	}
	return out
}

func dbSnapshotAlertIssuesJSON(issues []dbSnapshotAlertIssue) []dbStorageAlertIssueJSON {
	out := make([]dbStorageAlertIssueJSON, 0, len(issues))
	for _, issue := range issues {
		out = append(out, dbStorageAlertIssueJSON{
			Severity: issue.severity,
			Kind:     issue.kind,
			Detail:   issue.detail,
		})
	}
	return out
}

func dbModeAlertIssuesJSON(issues []dbModeAlertIssue) []dbStorageAlertIssueJSON {
	out := make([]dbStorageAlertIssueJSON, 0, len(issues))
	for _, issue := range issues {
		out = append(out, dbStorageAlertIssueJSON{
			Severity: issue.severity,
			Kind:     issue.kind,
			Detail:   issue.detail,
		})
	}
	return out
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

func dbModeAlertStatus(issues []dbModeAlertIssue) string {
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

func dbModeAlertIssues(db ethdb.KeyValueReader, rows []dbStageStatusRow) (string, bool, []dbModeAlertIssue) {
	mode, ok, err := rawdb.ReadHistoryPruneMode(db)
	if err != nil {
		return "invalid", ok, []dbModeAlertIssue{{
			severity: "critical",
			kind:     "prune-mode-invalid",
			detail:   err.Error(),
		}}
	}
	if !ok {
		return "unknown", false, nil
	}
	normalised, err := normaliseHistoryMode(mode)
	if err != nil {
		return mode, true, []dbModeAlertIssue{{
			severity: "critical",
			kind:     "prune-mode-invalid",
			detail:   err.Error(),
		}}
	}
	var issues []dbModeAlertIssue
	byStage := make(map[rawdb.StageID]dbStageStatusRow, len(rows))
	for _, row := range rows {
		if row.present {
			byStage[row.stage] = row
		}
	}
	for _, stage := range historyPruneModeConflictStages(normalised) {
		if row, ok := byStage[stage]; ok {
			kind, detail, ok := historyPruneModeStageConflictFor(normalised, stage, row.progress.BlockNum)
			if ok {
				issues = append(issues, dbModeAlertIssue{
					severity: "critical",
					kind:     kind,
					detail:   detail,
				})
			}
		}
	}
	return normalised, true, issues
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

func dbFreezerAlertChainDB(db ethdb.KeyValueStore, freezer *rawdbfreezer.Freezer, dataDir string) (*rawdb.ChainDB, error) {
	if db == nil {
		return nil, errors.New("nil freezer alert database")
	}
	snapshotManager, err := statesnapshots.OpenManager(stateSnapshotsDir(dataDir))
	if err != nil {
		return nil, fmt.Errorf("open state snapshots: %w", err)
	}
	chainDB := rawdb.NewChainDB(db, rawdb.NewFallbackAncientReader(rawdb.NewFreezerReader(freezer), snapshotManager))
	chainDB.SetChainIndexReader(snapshotManager)
	return chainDB, nil
}

func dbFreezerAlertIssues(stats rawdbfreezer.Stats, chainFreezerStage rawdb.StageProgress, hasChainFreezerStage bool, tailPruneStage rawdb.StageProgress, hasTailPruneStage bool) []dbFreezerAlertIssue {
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
		case !chainFreezerStage.HasBlockHash:
			add("critical", "chain-freezer-stage-unbound", "%s=%d is not hash-bound", rawdb.StageChainFreezer, chainFreezerStage.BlockNum)
		case stats.Head == 0:
			add("critical", "chain-freezer-stage-with-empty-freezer", "%s=%d but freezer head is 0", rawdb.StageChainFreezer, chainFreezerStage.BlockNum)
		case chainFreezerStage.BlockNum >= stats.Head:
			add("critical", "chain-freezer-stage-ahead", "%s=%d exceeds freezer max block %d", rawdb.StageChainFreezer, chainFreezerStage.BlockNum, stats.Head-1)
		case chainFreezerStage.BlockNum < stats.Tail:
			add("critical", "chain-freezer-stage-behind-tail", "%s=%d is below freezer visible tail %d", rawdb.StageChainFreezer, chainFreezerStage.BlockNum, stats.Tail)
		}
	}
	if stats.Tail == 0 {
		if hasTailPruneStage {
			add("critical", "tail-prune-stage-without-hidden-tail", "%s=%d but freezer tail is 0", rawdb.StageSnapshotChainFreezerTailPrune, tailPruneStage.BlockNum)
		}
	} else if !hasTailPruneStage {
		add("critical", "tail-prune-stage-missing", "freezer tail=%d but %s stage is missing", stats.Tail, rawdb.StageSnapshotChainFreezerTailPrune)
	} else if !tailPruneStage.HasBlockHash {
		add("critical", "tail-prune-stage-unbound", "%s=%d is not hash-bound", rawdb.StageSnapshotChainFreezerTailPrune, tailPruneStage.BlockNum)
	} else {
		prunedThrough := stats.Tail - 1
		switch {
		case tailPruneStage.BlockNum < prunedThrough:
			add("critical", "tail-prune-stage-behind-tail", "%s=%d is below freezer pruned-through block %d", rawdb.StageSnapshotChainFreezerTailPrune, tailPruneStage.BlockNum, prunedThrough)
		case tailPruneStage.BlockNum > prunedThrough:
			add("critical", "tail-prune-stage-ahead-of-tail", "%s=%d exceeds freezer pruned-through block %d", rawdb.StageSnapshotChainFreezerTailPrune, tailPruneStage.BlockNum, prunedThrough)
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

func dbFreezerAlertStageProofIssues(chainDB ethdb.KeyValueReader, chainFreezerStage rawdb.StageProgress, hasChainFreezerStage bool, tailPruneStage rawdb.StageProgress, hasTailPruneStage bool) []dbFreezerAlertIssue {
	var issues []dbFreezerAlertIssue
	issues = append(issues, dbFreezerAlertOneStageProofIssues(chainDB, chainFreezerStage, hasChainFreezerStage, "chain-freezer-stage")...)
	issues = append(issues, dbFreezerAlertOneStageProofIssues(chainDB, tailPruneStage, hasTailPruneStage, "tail-prune-stage")...)
	return issues
}

func dbFreezerAlertOneStageProofIssues(chainDB ethdb.KeyValueReader, stage rawdb.StageProgress, hasStage bool, kindPrefix string) []dbFreezerAlertIssue {
	if chainDB == nil || !hasStage || !stage.HasBlockHash {
		return nil
	}
	canonical, ok, err := rawdb.ReadBlockHashByNumberStrict(chainDB, stage.BlockNum)
	if err != nil {
		return []dbFreezerAlertIssue{{
			severity: "critical",
			kind:     kindPrefix + "-canonical-error",
			detail: fmt.Sprintf("%s=%d hash %x canonical hash lookup failed: %v",
				stage.Stage, stage.BlockNum, stage.BlockHash, err),
		}}
	}
	if !ok || canonical == (common.Hash{}) {
		return []dbFreezerAlertIssue{{
			severity: "critical",
			kind:     kindPrefix + "-missing-canonical",
			detail: fmt.Sprintf("%s=%d hash %x cannot be verified because canonical block is unavailable",
				stage.Stage, stage.BlockNum, stage.BlockHash),
		}}
	}
	if canonical != stage.BlockHash {
		return []dbFreezerAlertIssue{{
			severity: "critical",
			kind:     kindPrefix + "-hash-mismatch",
			detail: fmt.Sprintf("%s=%d hash %x does not match canonical hash %x",
				stage.Stage, stage.BlockNum, stage.BlockHash, canonical),
		}}
	}
	return nil
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

func dbModeAlertSummary(issues []dbModeAlertIssue) string {
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
		JSON:   ctx.Bool(dbAlertJSONFlag.Name),
	})
}

type dbStageStatusRow struct {
	stage         rawdb.StageID
	group         string
	present       bool
	progress      rawdb.StageProgress
	verified      string
	canonicalHash common.Hash
	details       []string
}

type dbStageStatusOptions struct {
	Verify bool
	JSON   bool
}

type dbStageStatusJSON struct {
	Datadir      string                    `json:"datadir"`
	Known        int                       `json:"known"`
	Rows         int                       `json:"rows"`
	Status       string                    `json:"status"`
	Verify       bool                      `json:"verify"`
	Pipeline     dbStageStatusPipelineJSON `json:"pipeline"`
	Stages       []dbStageStatusRowJSON    `json:"stages"`
	Issues       []string                  `json:"issues,omitempty"`
	IssueDetails []dbStageStatusIssueJSON  `json:"issueDetails,omitempty"`
}

type dbStageStatusRowJSON struct {
	Group         string   `json:"group"`
	Name          string   `json:"name"`
	Present       bool     `json:"present"`
	Status        string   `json:"status"`
	Value         uint64   `json:"value,omitempty"`
	Hash          string   `json:"hash,omitempty"`
	Verified      string   `json:"verified,omitempty"`
	CanonicalHash string   `json:"canonicalHash,omitempty"`
	Details       []string `json:"details,omitempty"`
}

type dbStageStatusIssueJSON struct {
	Severity        string `json:"severity"`
	Kind            string `json:"kind"`
	Detail          string `json:"detail"`
	Stage           string `json:"stage,omitempty"`
	Verified        string `json:"verified,omitempty"`
	Value           uint64 `json:"value,omitempty"`
	Downstream      string `json:"downstream,omitempty"`
	DownstreamValue uint64 `json:"downstreamValue,omitempty"`
	Upstream        string `json:"upstream,omitempty"`
	UpstreamValue   uint64 `json:"upstreamValue,omitempty"`
	MissingUpstream bool   `json:"missingUpstream,omitempty"`
	HashMismatch    bool   `json:"hashMismatch,omitempty"`
}

type dbStageStatusPipelineJSON struct {
	Complete bool                            `json:"complete"`
	Pending  int                             `json:"pending"`
	Issues   int                             `json:"issues"`
	Tasks    []dbStageStatusPipelineTaskJSON `json:"tasks,omitempty"`
}

type dbStageStatusPipelineTaskJSON struct {
	Stage        string `json:"stage"`
	Upstream     string `json:"upstream"`
	Status       string `json:"status"`
	TargetValue  uint64 `json:"targetValue"`
	TargetHash   string `json:"targetHash,omitempty"`
	CurrentValue uint64 `json:"currentValue,omitempty"`
	CurrentHash  string `json:"currentHash,omitempty"`
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
	var issues []string
	var issueDetails []dbStageStatusIssueJSON
	if opts.JSON || opts.Verify {
		issueDetails = dbStageStatusIssueDetails(rows, db, stateSnapshotsDir(dataDir))
		issues = dbStageStatusIssueStrings(issueDetails)
	}
	pipeline := dbStageStatusPipeline(rows)
	if opts.JSON {
		report := dbStageStatusJSON{
			Datadir:      dataDir,
			Known:        len(rawdb.KnownStageProgressStages()),
			Rows:         present,
			Status:       "ok",
			Verify:       opts.Verify,
			Pipeline:     dbStageStatusPipelineJSONForCursor(pipeline),
			Stages:       dbStageStatusRowsJSON(rows),
			Issues:       issues,
			IssueDetails: issueDetails,
		}
		if len(issues) > 0 {
			report.Status = "critical"
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(report); err != nil {
			return fmt.Errorf("encode stage status json: %w", err)
		}
		if opts.Verify && len(issues) > 0 {
			return fmt.Errorf("stage status verification failed: %s", strings.Join(issues, "; "))
		}
		return nil
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
		if row.canonicalHash != (common.Hash{}) && row.verified != "canonical" {
			fmt.Printf(" canonicalHash=%x", row.canonicalHash)
		}
		for _, detail := range row.details {
			fmt.Printf(" %s", detail)
		}
		fmt.Println()
	}
	fmt.Printf("Stage pipeline: complete=%t pending=%d issues=%d\n", pipeline.Complete, pipeline.Pending, len(pipeline.Issues))
	for _, task := range pipeline.Tasks {
		fmt.Printf("Stage pipeline task: stage=%s upstream=%s status=%s target=%d",
			task.Stage, task.Upstream, task.Status, task.TargetBlock)
		if task.TargetHasHash {
			fmt.Printf(" targetHash=%x", task.TargetHash)
		}
		if task.CurrentBlock != 0 || task.CurrentHasHash {
			fmt.Printf(" current=%d", task.CurrentBlock)
			if task.CurrentHasHash {
				fmt.Printf(" currentHash=%x", task.CurrentHash)
			}
		}
		fmt.Println()
	}
	if opts.Verify && len(issues) > 0 {
		return fmt.Errorf("stage status verification failed: %s", strings.Join(issues, "; "))
	}
	return nil
}

func dbStageStatusRowsJSON(rows []dbStageStatusRow) []dbStageStatusRowJSON {
	out := make([]dbStageStatusRowJSON, 0, len(rows))
	for _, row := range rows {
		item := dbStageStatusRowJSON{
			Group:   row.group,
			Name:    string(row.stage),
			Present: row.present,
			Status:  "missing",
		}
		if row.present {
			item.Status = "present"
			item.Value = row.progress.BlockNum
			item.Verified = row.verified
			if row.progress.HasBlockHash {
				item.Hash = fmt.Sprintf("%x", row.progress.BlockHash)
			}
			if row.canonicalHash != (common.Hash{}) && row.verified != "canonical" {
				item.CanonicalHash = fmt.Sprintf("%x", row.canonicalHash)
			}
			item.Details = append([]string(nil), row.details...)
		}
		out = append(out, item)
	}
	return out
}

func dbStageStatusPipeline(rows []dbStageStatusRow) rawdb.StageProgressPipelineCursor {
	byStage := make(map[rawdb.StageID]dbStageStatusRow, len(rows))
	for _, row := range rows {
		if row.present {
			byStage[row.stage] = row
		}
	}
	return rawdb.PlanStageProgressPipelineCursor(dbStageStatusProgressRows(byStage))
}

func dbStageStatusPipelineJSONForCursor(cursor rawdb.StageProgressPipelineCursor) dbStageStatusPipelineJSON {
	out := dbStageStatusPipelineJSON{
		Complete: cursor.Complete,
		Pending:  cursor.Pending,
		Issues:   len(cursor.Issues),
	}
	if len(cursor.Tasks) == 0 {
		return out
	}
	out.Tasks = make([]dbStageStatusPipelineTaskJSON, 0, len(cursor.Tasks))
	for _, task := range cursor.Tasks {
		item := dbStageStatusPipelineTaskJSON{
			Stage:        string(task.Stage),
			Upstream:     string(task.Upstream),
			Status:       string(task.Status),
			TargetValue:  task.TargetBlock,
			CurrentValue: task.CurrentBlock,
		}
		if task.TargetHasHash {
			item.TargetHash = fmt.Sprintf("%x", task.TargetHash)
		}
		if task.CurrentHasHash {
			item.CurrentHash = fmt.Sprintf("%x", task.CurrentHash)
		}
		out.Tasks = append(out.Tasks, item)
	}
	return out
}

func dbStageStatusVerificationIssues(rows []dbStageStatusRow) []string {
	return dbStageStatusIssueStrings(dbStageStatusVerificationIssueDetails(rows))
}

func dbStageStatusIssueStrings(details []dbStageStatusIssueJSON) []string {
	if len(details) == 0 {
		return nil
	}
	issues := make([]string, 0, len(details))
	for _, detail := range details {
		issues = append(issues, detail.Detail)
	}
	return issues
}

func dbStageStatusIssueDetails(rows []dbStageStatusRow, db ethdb.KeyValueReader, snapshotDir string) []dbStageStatusIssueJSON {
	var details []dbStageStatusIssueJSON
	details = append(details, dbStageStatusVerificationIssueDetails(rows)...)
	for _, issue := range dbStageStatusStagedBodyIssues(db, rows) {
		details = append(details, dbStageStatusTextIssueJSON("staged-body", issue))
	}
	for _, issue := range dbStageStatusSnapshotCoverageIssues(rows, snapshotDir) {
		details = append(details, dbStageStatusTextIssueJSON("snapshot-coverage", issue))
	}
	return details
}

func dbStageStatusVerificationIssueDetails(rows []dbStageStatusRow) []dbStageStatusIssueJSON {
	var details []dbStageStatusIssueJSON
	for _, row := range rows {
		if !row.present {
			continue
		}
		if !dbStageStatusRequiresCanonicalVerification(row.stage) {
			continue
		}
		verified := row.verified
		if row.progress.HasBlockHash {
			if verified != "canonical" {
				details = append(details, dbStageStatusIssueJSON{
					Severity: "critical",
					Kind:     "stage-verification",
					Stage:    string(row.stage),
					Value:    row.progress.BlockNum,
					Verified: verified,
					Detail:   dbStageStatusVerificationIssueDetail(row, verified),
				})
			}
			continue
		}
		details = append(details, dbStageStatusIssueJSON{
			Severity: "critical",
			Kind:     "stage-verification",
			Stage:    string(row.stage),
			Value:    row.progress.BlockNum,
			Verified: "unbound",
			Detail:   dbStageStatusVerificationIssueDetail(row, "unbound"),
		})
	}
	details = append(details, dbStageStatusPipelineOrderIssueDetails(rows)...)
	return details
}

func dbStageStatusVerificationIssueDetail(row dbStageStatusRow, verified string) string {
	detail := fmt.Sprintf("%s verified=%s", row.stage, verified)
	if len(row.details) > 0 {
		detail += " " + strings.Join(row.details, " ")
	}
	return detail
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
	return dbStageStatusIssueStrings(dbStageStatusPipelineOrderIssueDetails(rows))
}

func dbStageStatusPipelineOrderIssueDetails(rows []dbStageStatusRow) []dbStageStatusIssueJSON {
	byStage := make(map[rawdb.StageID]dbStageStatusRow, len(rows))
	for _, row := range rows {
		if row.present {
			byStage[row.stage] = row
		}
	}
	var details []dbStageStatusIssueJSON
	for _, issue := range rawdb.CheckStageProgressOrder(dbStageStatusProgressRows(byStage)) {
		details = append(details, dbStageStatusRawdbOrderIssueJSON(issue))
	}
	for _, issue := range syncdl.CheckSyncPipelineProgressOrder(dbStageStatusSyncProgressRows(byStage), syncdl.SyncPipelineProgressOrderOptions{}) {
		details = append(details, dbStageStatusSyncOrderIssueJSON(issue))
	}
	return details
}

func dbStageStatusRawdbOrderIssueJSON(issue rawdb.StageProgressOrderIssue) dbStageStatusIssueJSON {
	return dbStageStatusIssueJSON{
		Severity:        "critical",
		Kind:            "stage-order",
		Detail:          issue.String(),
		Downstream:      string(issue.Downstream),
		DownstreamValue: issue.DownstreamBlock,
		Upstream:        string(issue.Upstream),
		UpstreamValue:   issue.UpstreamBlock,
		MissingUpstream: issue.MissingUpstream,
		HashMismatch:    issue.HashMismatch,
	}
}

func dbStageStatusSyncOrderIssueJSON(issue syncdl.SyncPipelineProgressOrderIssue) dbStageStatusIssueJSON {
	return dbStageStatusIssueJSON{
		Severity:        "critical",
		Kind:            "sync-stage-order",
		Detail:          issue.String(),
		Downstream:      string(issue.Downstream),
		DownstreamValue: issue.DownstreamBlock,
		Upstream:        string(issue.Upstream),
		UpstreamValue:   issue.UpstreamBlock,
		MissingUpstream: issue.MissingUpstream,
		HashMismatch:    issue.HashMismatch,
	}
}

func dbStageStatusTextIssueJSON(kind string, issue string) dbStageStatusIssueJSON {
	return dbStageStatusIssueJSON{
		Severity: "critical",
		Kind:     kind,
		Detail:   issue,
		Stage:    dbStageStatusIssueStage(issue),
	}
}

func dbStageStatusIssueStage(issue string) string {
	stage, _, ok := strings.Cut(issue, " ")
	if !ok {
		stage, _, _ = strings.Cut(issue, "=")
	}
	return stage
}

func dbStageStatusProgressRows(rows map[rawdb.StageID]dbStageStatusRow) map[rawdb.StageID]rawdb.StageProgress {
	progress := make(map[rawdb.StageID]rawdb.StageProgress)
	for _, pair := range rawdb.StageProgressOrderPairs() {
		if row, ok := rows[pair.Downstream]; ok {
			progress[pair.Downstream] = row.progress
		}
		if row, ok := rows[pair.Upstream]; ok {
			progress[pair.Upstream] = row.progress
		}
	}
	return progress
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
		if block < fromBlock {
			return
		}
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
		check(row.stage, row.progress.BlockNum, "balance-trace", 1, mgr.BalanceTraceRangeCovered)
	}
	if row, ok := byStage[rawdb.StageSnapshotChainFreezerTailPrune]; ok {
		check(row.stage, row.progress.BlockNum, "chain-freezer", 0, mgr.ChainFreezerRangeCovered)
		check(row.stage, row.progress.BlockNum, "chain-index", 0, mgr.ChainIndexRangeCovered)
		if row.progress.BlockNum > 0 {
			check(row.stage, row.progress.BlockNum, "indexed event-log", 1, mgr.EventLogIndexedRangeCovered)
		}
	}
	return issues
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
		rawdb.StageSyncFinish,
		rawdb.StageChainFreezer,
		rawdb.StageSnapshotBuild,
		rawdb.StageSnapshotLatestBuild,
		rawdb.StageSnapshotEventLogBuild,
		rawdb.StageSnapshotChainLookupPrune,
		rawdb.StageSnapshotSectionBloomPrune,
		rawdb.StageSnapshotBalanceTracePrune,
		rawdb.StageSnapshotChainFreezerTailPrune:
		return true
	default:
		return false
	}
}

type dbStageStatusDB interface {
	ethdb.Iteratee
	ethdb.KeyValueReader
}

func dbStageStatusRows(db dbStageStatusDB, canonical ethdb.KeyValueReader) ([]dbStageStatusRow, error) {
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
		rows = append(rows, dbStageStatusRowFor(stage, row, ok, db, canonical))
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
		rows = append(rows, dbStageStatusRowFor(stage, progress[stage], true, db, canonical))
	}
	return rows, nil
}

func dbStageStatusRowFor(stage rawdb.StageID, progress rawdb.StageProgress, present bool, db ethdb.KeyValueReader, canonical ethdb.KeyValueReader) dbStageStatusRow {
	row := dbStageStatusRow{
		stage:   stage,
		group:   dbStageStatusGroup(stage),
		present: present,
	}
	if !present {
		return row
	}
	row.progress = progress
	row.verified, row.canonicalHash, row.details = dbStageStatusVerification(stage, progress, db, canonical)
	return row
}

func dbStageStatusVerification(stage rawdb.StageID, progress rawdb.StageProgress, db ethdb.KeyValueReader, canonical ethdb.KeyValueReader) (string, common.Hash, []string) {
	if !progress.HasBlockHash {
		return "unbound", common.Hash{}, nil
	}
	switch stage {
	case rawdb.StageSyncBodies:
		check := syncdl.ReadStagedBodyProgress(db, rawdb.StageSyncBodies)
		return dbStageStatusStagedBodyProgressVerification(check), common.Hash{}, dbStageStatusStagedBodyProgressDetails(check)
	case rawdb.StageSyncBodiesReady:
		ready := syncdl.ReadStagedBodyReadyDrainLimit(db, progress.BlockNum)
		return dbStageStatusStagedBodyReadyVerification(ready), common.Hash{}, dbStageStatusStagedBodyReadyDetails(ready)
	}
	if canonical == nil {
		return "unchecked", common.Hash{}, nil
	}
	canonicalHash, details := dbStageStatusCanonicalHash(canonical, progress)
	if canonicalHash == (common.Hash{}) {
		return "missing-canonical", canonicalHash, details
	}
	if canonicalHash != progress.BlockHash {
		return "mismatch", canonicalHash, details
	}
	return "canonical", canonicalHash, details
}

func dbStageStatusCanonicalHash(canonical ethdb.KeyValueReader, progress rawdb.StageProgress) (common.Hash, []string) {
	canonicalHash, ok, err := rawdb.ReadBlockHashByNumberStrict(canonical, progress.BlockNum)
	if err != nil {
		return common.Hash{}, []string{fmt.Sprintf("canonicalError=%q", err.Error())}
	}
	if ok && canonicalHash != (common.Hash{}) {
		return canonicalHash, nil
	}
	if !dbStageStatusAllowsStateTxRangeHashFallback(progress.Stage) {
		return common.Hash{}, nil
	}
	row, ok, err := rawdb.ReadStateTxRange(canonical, progress.BlockNum)
	if err != nil {
		return common.Hash{}, []string{fmt.Sprintf("stateTxRangeError=%q", err.Error())}
	}
	if !ok {
		return common.Hash{}, nil
	}
	return row.BlockHash, nil
}

func dbStageStatusAllowsStateTxRangeHashFallback(stage rawdb.StageID) bool {
	switch stage {
	case rawdb.StageSnapshotEventLogBuild,
		rawdb.StageSnapshotSectionBloomPrune,
		rawdb.StageSnapshotBalanceTracePrune:
		return true
	default:
		return false
	}
}

func dbStageStatusStagedBodyProgressVerification(progress syncdl.StagedBodyProgressCheck) string {
	if progress.Valid() {
		return "staged"
	}
	return dbStageStatusStagedBodyVerificationLabel(dbStageStatusStagedBodyProgressStatus(progress.Status))
}

func dbStageStatusStagedBodyReadyVerification(ready syncdl.StagedBodyReadyLimit) string {
	if ready.Valid() {
		return "staged"
	}
	return dbStageStatusStagedBodyVerificationLabel(dbStageStatusStagedBodyStatus(ready.Status))
}

func dbStageStatusStagedBodyVerificationLabel(status string) string {
	return "staged-" + strings.TrimPrefix(status, "staged-")
}

func dbStageStatusStagedBodyProgressDetails(progress syncdl.StagedBodyProgressCheck) []string {
	if progress.Valid() {
		return nil
	}
	return dbStageStatusStagedBodyDetails(progress.StagedRow.Number, progress.StagedHash)
}

func dbStageStatusStagedBodyReadyDetails(ready syncdl.StagedBodyReadyLimit) []string {
	if ready.Valid() {
		return nil
	}
	return dbStageStatusStagedBodyDetails(ready.StagedRow.Number, ready.StagedHash)
}

func dbStageStatusStagedBodyDetails(stagedBlock uint64, stagedHash common.Hash) []string {
	if stagedBlock == 0 && stagedHash == (common.Hash{}) {
		return nil
	}
	return []string{
		fmt.Sprintf("stagedBlock=%d", stagedBlock),
		fmt.Sprintf("stagedHash=%x", stagedHash),
	}
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
	sourceRoot, sourceRootOK, err := rawdb.ReadBlockStateRootStrict(source, boundary.BlockHash)
	if err != nil {
		return nil, fmt.Errorf("balance trace replay snapshot seed read source boundary state root: %w", err)
	}
	if sourceRootOK && sourceRoot != boundaryRoot {
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
		block, ok, err := rawdb.ReadBlockStrict(source, blockNum)
		if err != nil {
			return copied, fmt.Errorf("balance trace replay snapshot seed: source block %d for recent execution window is corrupt: %w", blockNum, err)
		}
		if !ok {
			return copied, fmt.Errorf("balance trace replay snapshot seed: missing source block %d for recent execution window", blockNum)
		}
		root, hasRoot, err := rawdb.ReadBlockStateRootStrict(source, block.Hash())
		if err != nil {
			return copied, fmt.Errorf("balance trace replay snapshot seed read block %d state root: %w", blockNum, err)
		}
		if err := rawdb.WriteBlock(target, block); err != nil {
			return copied, fmt.Errorf("balance trace replay snapshot seed write block %d: %w", blockNum, err)
		}
		if err := rawdb.WriteTaposRef(target, blockNum, block.Hash()); err != nil {
			return copied, fmt.Errorf("balance trace replay snapshot seed write tapos block %d: %w", blockNum, err)
		}
		if hasRoot {
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
