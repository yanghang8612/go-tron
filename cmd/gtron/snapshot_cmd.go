package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	corestate "github.com/tronprotocol/go-tron/core/state"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

var (
	snapshotBootstrapFlag = &cli.BoolFlag{
		Name:  "snapshot.bootstrap",
		Usage: "Before starting a fresh node, fetch and restore a signed remote snapshot, then continue normal sync",
	}
	snapshotDirFlag = &cli.StringFlag{
		Name:  "snapshot.dir",
		Usage: "Local state snapshot directory containing snapshot-catalog.json and manifest.json",
	}
	snapshotURLFlag = &cli.StringFlag{
		Name:    "snapshot.url",
		Usage:   "HTTP(S) base URL for a remote snapshot directory",
		EnvVars: []string{"GTRON_SNAPSHOT_URL"},
	}
	snapshotResetFlag = &cli.BoolFlag{
		Name:  "snapshot.reset",
		Usage: "Delete the local snapshot directory before fetching the remote snapshot",
	}
	snapshotFetchConcurrencyFlag = &cli.IntFlag{
		Name:  "snapshot.fetch.concurrency",
		Usage: "Maximum concurrent snapshot segment downloads (0 = default)",
	}
	snapshotTrustedCatalogKeyFlag = &cli.StringSliceFlag{
		Name:    "snapshot.trusted-key",
		Usage:   "Trusted Ed25519 snapshot catalog public key as hex; repeatable or comma-separated",
		EnvVars: []string{"GTRON_SNAPSHOT_TRUSTED_KEY"},
	}
	snapshotTrustedCatalogKeyFileFlag = &cli.StringFlag{
		Name:    "snapshot.trusted-key-file",
		Usage:   "File containing trusted Ed25519 snapshot catalog public keys, one per line; # comments and comma-separated entries are allowed",
		EnvVars: []string{"GTRON_SNAPSHOT_TRUSTED_KEY_FILE"},
	}
	snapshotForkConfigHashFlag = &cli.StringFlag{
		Name:    "snapshot.fork-config-hash",
		Usage:   "Expected fork config hash as sha256:<hex>; required when the catalog carries forkConfigHash",
		EnvVars: []string{"GTRON_SNAPSHOT_FORK_CONFIG_HASH"},
	}
	snapshotCatalogSigningKeyFlag = &cli.StringFlag{
		Name:    "snapshot.signing-key",
		Usage:   "Ed25519 catalog signing key as a 32-byte seed or 64-byte private key in hex",
		EnvVars: []string{"GTRON_SNAPSHOT_SIGNING_KEY"},
	}
	snapshotCatalogSigningKeyFileFlag = &cli.StringFlag{
		Name:    "snapshot.signing-key-file",
		Usage:   "File containing the Ed25519 catalog signing key as a 32-byte seed or 64-byte private key in hex",
		EnvVars: []string{"GTRON_SNAPSHOT_SIGNING_KEY_FILE"},
	}
	snapshotServeFlag = &cli.BoolFlag{
		Name:    "snapshot.serve",
		Usage:   "Serve the signed catalog and immutable published snapshot generations over a dedicated HTTP listener",
		EnvVars: []string{"GTRON_SNAPSHOT_SERVE"},
	}
	snapshotServeAddrFlag = &cli.StringFlag{
		Name:    "snapshot.serve.addr",
		Usage:   "Snapshot HTTP listener address",
		Value:   "127.0.0.1",
		EnvVars: []string{"GTRON_SNAPSHOT_SERVE_ADDR"},
	}
	snapshotServePortFlag = &cli.IntFlag{
		Name:    "snapshot.serve.port",
		Usage:   "Snapshot HTTP listener port",
		Value:   6072,
		EnvVars: []string{"GTRON_SNAPSHOT_SERVE_PORT"},
	}
	snapshotCatalogRetainFlag = &cli.IntFlag{
		Name:    "snapshot.catalog-retain",
		Usage:   "Minimum number of immutable published catalog generations retained for resumable downloads",
		Value:   statesnapshots.DefaultPublishedSnapshotRetain,
		EnvVars: []string{"GTRON_SNAPSHOT_CATALOG_RETAIN"},
	}
	snapshotCatalogGraceFlag = &cli.DurationFlag{
		Name:    "snapshot.catalog-grace",
		Usage:   "Minimum age before an obsolete published catalog generation and its segment leases may expire",
		Value:   statesnapshots.DefaultPublishedSnapshotGrace,
		EnvVars: []string{"GTRON_SNAPSHOT_CATALOG_GRACE"},
	}
	snapshotFromBlockFlag = &cli.Uint64Flag{
		Name:  "snapshot.from-block",
		Usage: "First chain-freezer block number to snapshot, inclusive",
	}
	snapshotToBlockFlag = &cli.Uint64Flag{
		Name:  "snapshot.to-block",
		Usage: "Last chain-freezer block number to snapshot, inclusive",
	}
	snapshotFromColdFlag = &cli.BoolFlag{
		Name:  "snapshot.from-cold",
		Usage: "Build derived snapshot segments from existing verified cold sidecars in --snapshot.dir instead of hot rawdb rows",
	}
	snapshotETLTempDirFlag = &cli.StringFlag{
		Name:    "snapshot.etl.tempdir",
		Usage:   "Parent directory for temporary snapshot ETL run files",
		EnvVars: []string{"GTRON_SNAPSHOT_ETL_TEMPDIR"},
	}
	snapshotETLBufferMiBFlag = &cli.Uint64Flag{
		Name:    "snapshot.etl.buffer",
		Usage:   "Snapshot ETL memory buffer limit in MiB (0 = default)",
		EnvVars: []string{"GTRON_SNAPSHOT_ETL_BUFFER"},
	}
	snapshotETLBatchMiBFlag = &cli.Uint64Flag{
		Name:    "snapshot.etl.batch",
		Usage:   "Snapshot ETL output batch size in MiB (0 = default)",
		EnvVars: []string{"GTRON_SNAPSHOT_ETL_BATCH"},
	}
	snapshotEventLogSampleSegmentsFlag = &cli.Uint64Flag{
		Name:  "snapshot.event-log.sample-segments",
		Usage: "Evenly sample this many whole active event-log segments (0 = scan all)",
		Value: 16,
	}
	snapshotHistorySampleSegmentsFlag = &cli.Uint64Flag{
		Name:  "snapshot.history.sample-segments",
		Usage: "Sample this many active state history/accessor/index trios",
		Value: 8,
	}
	snapshotHistorySampleIndexEntriesFlag = &cli.Uint64Flag{
		Name:  "snapshot.history.sample-index-entries",
		Usage: "Maximum transaction index entries sampled per selected history segment",
		Value: 32 * 1024,
	}
	snapshotHistorySampleAccessorBlocksFlag = &cli.Uint64Flag{
		Name:  "snapshot.history.sample-accessor-blocks",
		Usage: "Maximum key-oriented accessor blocks sampled per selected history segment",
		Value: 64,
	}
	snapshotHistorySampleMiBFlag = &cli.Uint64Flag{
		Name:  "snapshot.history.sample-mib",
		Usage: "Approximate uncompressed history MiB recompressed per selected segment and candidate frame size",
		Value: 8,
	}
	snapshotHistoryProgressFlag = &cli.DurationFlag{
		Name:  "progress",
		Usage: "History-space benchmark progress reporting interval (0 disables)",
		Value: 30 * time.Second,
	}
	snapshotHistoryMigrateYesFlag = &cli.BoolFlag{
		Name:  "yes",
		Usage: "Confirm the offline rewrite and atomic publication of active history trios",
	}
	snapshotHistoryMigrateMaxTriosFlag = &cli.Uint64Flag{
		Name:  "max-trios",
		Usage: "Maximum non-current history trios to rewrite in this run (0 = all)",
	}
	snapshotHistoryMigrateJSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Write the migration summary as JSON",
	}
	snapshotEventLogVersionFlag = &cli.UintFlag{
		Name:    "snapshot.event-log.version",
		Usage:   "Main event-log snapshot writer version: 2 (legacy) or 3 (dictionary + framed Zstd)",
		Value:   statesnapshots.EventLogSegmentV3Version,
		EnvVars: []string{"GTRON_SNAPSHOT_EVENT_LOG_VERSION"},
	}
	snapshotEventLogV3MergeFlag = &cli.Uint64Flag{
		Name:  "snapshot.event-log.merge",
		Usage: "Number of consecutive active event-log segments to merge when --snapshot.to-block is omitted",
		Value: 8,
	}
	snapshotEventLogV3PublishFlag = &cli.BoolFlag{
		Name:  "publish",
		Usage: "Atomically publish the verified V3 segment pair into manifest.json (otherwise build and verify only)",
	}
)

func snapshotCommand() *cli.Command {
	return &cli.Command{
		Name:  "snapshot",
		Usage: "Manage verified state snapshots",
		Subcommands: []*cli.Command{
			{
				Name:  "restore",
				Usage: "Restore latest state and state-domain history from a signed local snapshot catalog",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
				},
				Action: snapshotRestoreCmd,
			},
			{
				Name:  "fetch",
				Usage: "Download a signed remote snapshot catalog, manifest, and active segments",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					snapshotDirFlag,
					snapshotURLFlag,
					snapshotResetFlag,
					snapshotFetchConcurrencyFlag,
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
				},
				Action: snapshotFetchCmd,
			},
			{
				Name:  "verify",
				Usage: "Verify a signed local snapshot catalog, manifest, and active segments",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					snapshotDirFlag,
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
				},
				Action: snapshotVerifyCmd,
			},
			{
				Name:  "bootstrap",
				Usage: "Fetch a signed remote snapshot and restore it into the local datadir",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotURLFlag,
					snapshotResetFlag,
					snapshotFetchConcurrencyFlag,
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
				},
				Action: snapshotBootstrapCmd,
			},
			{
				Name:  "publish-catalog",
				Usage: "Sign the local production snapshot manifest as snapshot-catalog.json",
				Flags: []cli.Flag{
					dataDirFlag,
					snapshotDirFlag,
					snapshotCatalogSigningKeyFlag,
					snapshotCatalogSigningKeyFileFlag,
				},
				Action: snapshotPublishCatalogCmd,
			},
			{
				Name:  "build-freezer",
				Usage: "Build a chain-freezer snapshot segment from local ancient rows",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					witnessKeyFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					snapshotDirFlag,
					snapshotFromBlockFlag,
					snapshotToBlockFlag,
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
				},
				Action: snapshotBuildFreezerCmd,
			},
			{
				Name:  "build-balance-traces",
				Usage: "Build a cold account/balance trace snapshot segment from local rawdb trace rows",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					witnessKeyFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotFromBlockFlag,
					snapshotToBlockFlag,
					snapshotFromColdFlag,
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
				},
				Action: snapshotBuildBalanceTracesCmd,
			},
			{
				Name:  "build-section-blooms",
				Usage: "Build a cold section-bloom snapshot segment from local rawdb bloom rows",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					witnessKeyFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotFromBlockFlag,
					snapshotToBlockFlag,
					snapshotFromColdFlag,
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
				},
				Action: snapshotBuildSectionBloomsCmd,
			},
			{
				Name:  "build-event-logs",
				Usage: "Build a cold event-log snapshot segment from retained blocks and transaction infos",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					witnessKeyFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotFromBlockFlag,
					snapshotToBlockFlag,
					snapshotFromColdFlag,
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
					snapshotEventLogVersionFlag,
				},
				Action: snapshotBuildEventLogsCmd,
			},
			{
				Name:  "build-derived-indexes",
				Usage: "Build cold balance-trace, section-bloom, and event-log snapshot segments",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					witnessKeyFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotFromBlockFlag,
					snapshotToBlockFlag,
					snapshotFromColdFlag,
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
					snapshotEventLogVersionFlag,
				},
				Action: snapshotBuildDerivedIndexesCmd,
			},
			{
				Name:  "event-log-index-stats",
				Usage: "Print event-log address/topic index key and posting statistics",
				Flags: []cli.Flag{
					dataDirFlag,
					snapshotDirFlag,
				},
				Action: snapshotEventLogIndexStatsCmd,
			},
			{
				Name:  "event-log-space-benchmark",
				Usage: "Inspect active event-log physical layout and simulate space candidates without opening chaindata",
				Flags: []cli.Flag{
					dataDirFlag,
					snapshotDirFlag,
					snapshotEventLogSampleSegmentsFlag,
				},
				Action: snapshotEventLogSpaceBenchmarkCmd,
			},
			{
				Name:  "history-space-benchmark",
				Usage: "Inspect active state history/accessor/index layout and simulate compact formats without opening chaindata",
				Flags: []cli.Flag{
					dataDirFlag,
					snapshotDirFlag,
					snapshotHistorySampleSegmentsFlag,
					snapshotHistorySampleIndexEntriesFlag,
					snapshotHistorySampleAccessorBlocksFlag,
					snapshotHistorySampleMiBFlag,
					snapshotHistoryProgressFlag,
				},
				Action: snapshotHistorySpaceBenchmarkCmd,
			},
			{
				Name:        "migrate-history-v7",
				Usage:       "Rewrite active state history trios into the current compact V7 layout",
				Description: "The node using this snapshot directory must be stopped. Each verified trio is published atomically, and rerunning resumes by skipping trios already in the current layout.",
				Flags: []cli.Flag{
					dataDirFlag,
					snapshotDirFlag,
					snapshotHistoryMigrateYesFlag,
					snapshotHistoryMigrateMaxTriosFlag,
					snapshotHistoryMigrateJSONFlag,
				},
				Action: snapshotMigrateHistoryV7Cmd,
			},
			{
				Name:  "migrate-event-logs-v3",
				Usage: "Rewrite consecutive active event-log segments as an experimental V3 main segment without opening chaindata",
				Flags: []cli.Flag{
					dataDirFlag,
					snapshotDirFlag,
					snapshotFromBlockFlag,
					snapshotToBlockFlag,
					snapshotEventLogV3MergeFlag,
					snapshotEventLogV3PublishFlag,
				},
				Action: snapshotMigrateEventLogsV3Cmd,
			},
			{
				Name:  "prune-chain-lookups",
				Usage: "Delete hot block/transaction lookup indexes covered by verified chain-freezer sidecars",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					witnessKeyFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
				},
				Action: snapshotPruneChainLookupsCmd,
			},
			{
				Name:  "prune-balance-traces",
				Usage: "Delete hot account/balance trace rows covered by verified cold balance-trace segments",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					witnessKeyFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
				},
				Action: snapshotPruneBalanceTracesCmd,
			},
			{
				Name:  "prune-section-blooms",
				Usage: "Delete hot section-bloom rows covered by verified cold section-bloom segments",
				Flags: []cli.Flag{
					dataDirFlag,
					testnetFlag,
					genesisFileFlag,
					devFlag,
					witnessKeyFlag,
					devFullFeaturesFlag,
					devMaintenanceIntervalFlag,
					dbCacheFlag,
					dbHandlesFlag,
					dbMemtableFlag,
					dbL0CompactionFlag,
					dbL0StopFlag,
					snapshotDirFlag,
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
				},
				Action: snapshotPruneSectionBloomsCmd,
			},
			{
				Name:  "prune-retired",
				Usage: "Delete retired snapshot segment files that are no longer active",
				Flags: []cli.Flag{
					dataDirFlag,
					snapshotDirFlag,
					snapshotCatalogRetainFlag,
					snapshotCatalogGraceFlag,
				},
				Action: snapshotPruneRetiredCmd,
			},
		},
	}
}

func snapshotFetchCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}
	trustedKeys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		return err
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, forkConfigHash)
	if err != nil {
		return err
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	baseURL, err := snapshotRemoteURL(ctx)
	if err != nil {
		return err
	}
	if err := statesnapshots.ValidateRemoteSnapshotBaseURL(baseURL); err != nil {
		return err
	}
	concurrency := ctx.Int("snapshot.fetch.concurrency")
	if err := statesnapshots.ValidateRemoteSnapshotFetchConcurrency(concurrency); err != nil {
		return err
	}
	if ctx.Bool("snapshot.reset") {
		if err := resetSnapshotFetchDir(dir); err != nil {
			return err
		}
	}
	result, err := statesnapshots.FetchRemoteSnapshot(contextOrBackground(ctx), statesnapshots.FetchRemoteSnapshotOptions{
		BaseURL:                baseURL,
		Dir:                    dir,
		Expected:               identity,
		TrustedKeys:            trustedKeys,
		MaxConcurrentDownloads: concurrency,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot fetched: txRange=[%d,%d] activeSegments=%d filesDownloaded=%d bytesDownloaded=%d\n",
		result.Catalog.VisibleTxStart,
		result.Catalog.VisibleTxEnd,
		result.Verification.ActiveSegments,
		result.FilesDownloaded,
		result.BytesDownloaded,
	)
	return nil
}

func snapshotVerifyCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}
	trustedKeys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		return err
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, forkConfigHash)
	if err != nil {
		return err
	}
	catalog, report, err := statesnapshots.VerifySignedSnapshotCatalog(snapshotDir(ctx, cfg.DataDir), identity, trustedKeys)
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot verified: txRange=[%d,%d] activeSegments=%d signer=%s manifestChecksum=%s\n",
		catalog.VisibleTxStart,
		catalog.VisibleTxEnd,
		report.ActiveSegments,
		catalog.Signer,
		catalog.ManifestChecksum,
	)
	return nil
}

func snapshotBootstrapCmd(ctx *cli.Context) error {
	if err := snapshotBootstrapPreflightRestoreTarget(ctx); err != nil {
		return err
	}
	if err := snapshotFetchCmd(ctx); err != nil {
		return err
	}
	return snapshotRestoreCmd(ctx)
}

// bootstrapRuntimeSnapshot keeps startup opt-in and reuses the same verified
// restore path as the explicit snapshot bootstrap command.
func bootstrapRuntimeSnapshot(ctx *cli.Context) error {
	if !ctx.Bool("snapshot.bootstrap") {
		return nil
	}
	if ctx.Bool("dev") {
		return errors.New("--snapshot.bootstrap does not support --dev")
	}
	if err := snapshotBootstrapCmd(ctx); err != nil {
		return fmt.Errorf("bootstrap remote snapshot: %w", err)
	}
	return nil
}

func snapshotRestoreCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	trustedKeys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		return err
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}

	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	ancientStore, ancientReader, closeAncient, err := openSnapshotRestoreAncientStore(cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()

	chainConfig, genesisHash, err := core.SetupGenesisBlockWithAncient(db, ancientReader, genesis)
	if err != nil {
		return fmt.Errorf("setup genesis: %w", err)
	}
	if err := ensureSnapshotRestoreBootstrapDatadir(db, genesisHash); err != nil {
		return err
	}
	identity := snapshotExpectedChainIdentity(chainConfig, genesis, genesisHash, forkConfigHash)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	restoreOpts := snapshotRestoreVerificationOptions(db)
	restoreOpts.ETL = etlOpts
	if _, manifest, _, err := statesnapshots.VerifySignedSnapshotCatalogManifest(dir, identity, trustedKeys); err != nil {
		return err
	} else if err := statesnapshots.VerifyChainFreezerRestoreTarget(ancientReader, dir, manifest); err != nil {
		return err
	}
	result, err := statesnapshots.RestoreSnapshotFromVerifiedCatalogWithOptions(db, dir, identity, trustedKeys, restoreOpts)
	if err != nil {
		return err
	}
	freezerResult, err := statesnapshots.RestoreChainFreezerFromVerifiedCatalogWithOptions(ancientStore, dir, identity, trustedKeys, statesnapshots.RestoreChainFreezerOptions{
		IndexWriter:       db,
		ProgressWriter:    db,
		PreferColdIndexes: true,
		ETL:               etlOpts,
	})
	if err != nil {
		return err
	}
	var canonicalBoundary *statesnapshots.RestoreCanonicalBoundaryResult
	if freezerResult.HasRange {
		canonicalBoundary, err = statesnapshots.InstallCanonicalBoundaryFromVerifiedCatalog(db, rawdb.NewChainDB(db, ancientStore), dir, identity, trustedKeys)
		if err != nil {
			return err
		}
	}
	fmt.Printf("Snapshot restored: txNum=%d activeSegments=%d changes=%d txRanges=%d snapshotInstall=%d\n",
		result.RestoredTxNum,
		result.Verification.ActiveSegments,
		result.ChangesRestored,
		result.TxRangesRestored,
		result.RestoredTxNum,
	)
	if freezerResult.HasRange {
		fmt.Printf("Chain freezer restored: blocks=[%d,%d] count=%d coldIndexSegments=%d blockIndexes=%d txIndexes=%d\n",
			freezerResult.FromBlock,
			freezerResult.ToBlock,
			freezerResult.BlocksRestored,
			freezerResult.ColdIndexSegments,
			freezerResult.BlockIndexesRestored,
			freezerResult.TxIndexesRestored,
		)
	} else {
		fmt.Println("Chain freezer restored: no chain-freezer segments in snapshot.")
	}
	if canonicalBoundary != nil {
		fmt.Printf("Canonical boundary installed: block=%d hash=%x txNum=%d\n",
			canonicalBoundary.BlockNum,
			canonicalBoundary.BlockHash,
			canonicalBoundary.TxNum,
		)
		fmt.Println("Canonical Headers/Bodies/Execution stages were advanced to the verified snapshot boundary; start normal sync to resume from there.")
	} else {
		fmt.Println("Canonical Headers/Bodies/Execution stages were not advanced; start normal sync to resume from verified state.")
	}
	return nil
}

func snapshotPublishCatalogCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	dir := snapshotDir(ctx, cfg.DataDir)
	key, err := snapshotCatalogSigningKey(ctx)
	if err != nil {
		return err
	}
	catalog, err := statesnapshots.PublishSignedSnapshotCatalog(dir, key)
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot catalog published: %s signer=%s txRange=[%d,%d] manifestChecksum=%s\n",
		statesnapshots.SnapshotCatalogFile,
		catalog.Signer,
		catalog.VisibleTxStart,
		catalog.VisibleTxEnd,
		catalog.ManifestChecksum,
	)
	return nil
}

func snapshotBuildFreezerCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	if !ctx.IsSet("snapshot.to-block") {
		return errors.New("snapshot freezer build requires --snapshot.to-block")
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, forkConfigHash)
	if err != nil {
		return err
	}
	fromBlock := ctx.Uint64("snapshot.from-block")
	toBlock := ctx.Uint64("snapshot.to-block")
	if toBlock < fromBlock {
		return fmt.Errorf("snapshot freezer block range [%d,%d] is inverted", fromBlock, toBlock)
	}
	ancientPath := ancientDataDir(cfg.DataDir)
	if info, err := os.Stat(ancientPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("snapshot freezer build requires existing freezer directory %s", ancientPath)
		}
		return fmt.Errorf("stat freezer: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("stat freezer: %s is not a directory", ancientPath)
	}
	fz, err := rawdbfreezer.NewFreezer(ancientPath, "", false, freezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return fmt.Errorf("open freezer: %w", err)
	}
	defer fz.Close()

	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	result, err := statesnapshots.NewAggregator(dir).BuildChainFreezerWithOptions(rawdb.NewFreezerReader(fz), fromBlock, toBlock, statesnapshots.AggregatorBuildChainFreezerOptions{
		ETL: etlOpts,
	})
	if err != nil {
		return err
	}
	if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
		return err
	}
	paths := make([]string, 0, len(result.Segments))
	for _, ref := range result.Segments {
		paths = append(paths, ref.Path)
	}
	var generation uint64
	var activeSegments int
	if result.Manifest != nil {
		generation = result.Manifest.Generation
		activeSegments = len(result.Manifest.Segments)
	}
	fmt.Printf("Chain freezer snapshot built: blocks=[%d,%d] paths=%s manifestGeneration=%d activeSegments=%d\n",
		fromBlock,
		toBlock,
		strings.Join(paths, ","),
		generation,
		activeSegments,
	)
	return nil
}

func snapshotBuildBalanceTracesCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	if !ctx.IsSet("snapshot.to-block") {
		return errors.New("snapshot balance trace build requires --snapshot.to-block")
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, forkConfigHash)
	if err != nil {
		return err
	}
	fromBlock := ctx.Uint64("snapshot.from-block")
	toBlock := ctx.Uint64("snapshot.to-block")
	if toBlock < fromBlock {
		return fmt.Errorf("snapshot balance trace block range [%d,%d] is inverted", fromBlock, toBlock)
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	var result *statesnapshots.AggregatorBuildResult
	if ctx.Bool("snapshot.from-cold") {
		mgr, err := statesnapshots.OpenManager(dir)
		if err != nil {
			return err
		}
		if err := requireSnapshotColdCoverage("balance-trace", fromBlock, toBlock, mgr.BalanceTraceRangeCovered); err != nil {
			return err
		}
		result, err = statesnapshots.NewAggregator(dir).BuildBalanceTracesFromReaderWithOptions(mgr, fromBlock, toBlock, etlOpts)
		if err != nil {
			return err
		}
		if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
			return err
		}
		printSnapshotBuildResult("Balance trace snapshot built", fromBlock, toBlock, result)
		return nil
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
	coverage, err := rawdb.AuditBlockBalanceTraceCoverage(chainDB, chainDB, fromBlock, toBlock, 10)
	if err != nil {
		return err
	}
	if !coverage.Complete() {
		for _, issue := range coverage.Issues {
			fmt.Printf("Balance trace coverage issue: block=%d kind=%s detail=%s\n", issue.BlockNum, issue.Kind, issue.Detail)
		}
		return fmt.Errorf("snapshot balance trace build requires complete coverage over [%d,%d]: missingBlockTraces=%d missingAccountTraces=%d mismatched=%d",
			fromBlock,
			toBlock,
			coverage.MissingBlockBalanceTrace,
			coverage.MissingAccountTrace,
			coverage.MismatchedBlockBalanceTrace,
		)
	}

	result, err = statesnapshots.NewAggregator(dir).BuildBalanceTracesWithOptions(db, fromBlock, toBlock, etlOpts)
	if err != nil {
		return err
	}
	if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
		return err
	}
	paths := make([]string, 0, len(result.Segments))
	for _, ref := range result.Segments {
		paths = append(paths, ref.Path)
	}
	var generation uint64
	var activeSegments int
	if result.Manifest != nil {
		generation = result.Manifest.Generation
		activeSegments = len(result.Manifest.Segments)
	}
	fmt.Printf("Balance trace snapshot built: blocks=[%d,%d] paths=%s manifestGeneration=%d activeSegments=%d\n",
		fromBlock,
		toBlock,
		strings.Join(paths, ","),
		generation,
		activeSegments,
	)
	return nil
}

func snapshotBuildSectionBloomsCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	if !ctx.IsSet("snapshot.to-block") {
		return errors.New("snapshot section bloom build requires --snapshot.to-block")
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, forkConfigHash)
	if err != nil {
		return err
	}
	fromBlock := ctx.Uint64("snapshot.from-block")
	toBlock := ctx.Uint64("snapshot.to-block")
	if toBlock < fromBlock {
		return fmt.Errorf("snapshot section bloom block range [%d,%d] is inverted", fromBlock, toBlock)
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	var result *statesnapshots.AggregatorBuildResult
	if ctx.Bool("snapshot.from-cold") {
		mgr, err := statesnapshots.OpenManager(dir)
		if err != nil {
			return err
		}
		if err := requireSnapshotColdCoverage("section-bloom", fromBlock, toBlock, mgr.SectionBloomRangeCovered); err != nil {
			return err
		}
		result, err = statesnapshots.NewAggregator(dir).BuildSectionBloomsFromReaderWithOptions(mgr, fromBlock, toBlock, etlOpts)
		if err != nil {
			return err
		}
		if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
			return err
		}
		printSnapshotBuildResult("Section bloom snapshot built", fromBlock, toBlock, result)
		return nil
	}
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	result, err = statesnapshots.NewAggregator(dir).BuildSectionBloomsWithOptions(db, fromBlock, toBlock, etlOpts)
	if err != nil {
		return err
	}
	if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
		return err
	}
	paths := make([]string, 0, len(result.Segments))
	for _, ref := range result.Segments {
		paths = append(paths, ref.Path)
	}
	var generation uint64
	var activeSegments int
	if result.Manifest != nil {
		generation = result.Manifest.Generation
		activeSegments = len(result.Manifest.Segments)
	}
	fmt.Printf("Section bloom snapshot built: blocks=[%d,%d] paths=%s manifestGeneration=%d activeSegments=%d\n",
		fromBlock,
		toBlock,
		strings.Join(paths, ","),
		generation,
		activeSegments,
	)
	return nil
}

func snapshotBuildEventLogsCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	if !ctx.IsSet("snapshot.to-block") {
		return errors.New("snapshot event log build requires --snapshot.to-block")
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, forkConfigHash)
	if err != nil {
		return err
	}
	fromBlock := ctx.Uint64("snapshot.from-block")
	toBlock := ctx.Uint64("snapshot.to-block")
	if toBlock < fromBlock {
		return fmt.Errorf("snapshot event log block range [%d,%d] is inverted", fromBlock, toBlock)
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	eventLogVersion, err := snapshotEventLogBuildVersion(ctx)
	if err != nil {
		return err
	}
	var result *statesnapshots.AggregatorBuildResult
	if ctx.Bool("snapshot.from-cold") {
		mgr, err := statesnapshots.OpenManager(dir)
		if err != nil {
			return err
		}
		if err := requireSnapshotColdCoverage("event-log", fromBlock, toBlock, mgr.EventLogRangeCovered); err != nil {
			return err
		}
		result, err = statesnapshots.NewAggregator(dir).BuildEventLogsFromReaderWithBuildOptions(mgr, fromBlock, toBlock, statesnapshots.EventLogBuildOptions{Version: eventLogVersion, ETL: etlOpts})
		if err != nil {
			return err
		}
		if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
			return err
		}
		printSnapshotBuildResult("Event log snapshot built", fromBlock, toBlock, result)
		return nil
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

	result, err = statesnapshots.NewAggregator(dir).BuildEventLogsWithBuildOptions(rawdb.NewChainDB(db, ancientReader), fromBlock, toBlock, statesnapshots.EventLogBuildOptions{Version: eventLogVersion, ETL: etlOpts})
	if err != nil {
		return err
	}
	if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
		return err
	}
	paths := make([]string, 0, len(result.Segments))
	for _, ref := range result.Segments {
		paths = append(paths, ref.Path)
	}
	var generation uint64
	var activeSegments int
	if result.Manifest != nil {
		generation = result.Manifest.Generation
		activeSegments = len(result.Manifest.Segments)
	}
	fmt.Printf("Event log snapshot built: blocks=[%d,%d] paths=%s manifestGeneration=%d activeSegments=%d\n",
		fromBlock,
		toBlock,
		strings.Join(paths, ","),
		generation,
		activeSegments,
	)
	return nil
}

func snapshotBuildDerivedIndexesCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	if !ctx.IsSet("snapshot.to-block") {
		return errors.New("snapshot derived index build requires --snapshot.to-block")
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromContext(ctx, forkConfigHash)
	if err != nil {
		return err
	}
	fromBlock := ctx.Uint64("snapshot.from-block")
	toBlock := ctx.Uint64("snapshot.to-block")
	if toBlock < fromBlock {
		return fmt.Errorf("snapshot derived index block range [%d,%d] is inverted", fromBlock, toBlock)
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	eventLogVersion, err := snapshotEventLogBuildVersion(ctx)
	if err != nil {
		return err
	}
	if ctx.Bool("snapshot.from-cold") {
		result, err := snapshotBuildDerivedIndexesFromCold(dir, fromBlock, toBlock, eventLogVersion, etlOpts)
		if err != nil {
			return err
		}
		if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
			return err
		}
		printSnapshotBuildResult("Derived snapshot indexes built", fromBlock, toBlock, result)
		return nil
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
	coverage, err := rawdb.AuditBlockBalanceTraceCoverage(chainDB, chainDB, fromBlock, toBlock, 10)
	if err != nil {
		return err
	}
	if !coverage.Complete() {
		for _, issue := range coverage.Issues {
			fmt.Printf("Balance trace coverage issue: block=%d kind=%s detail=%s\n", issue.BlockNum, issue.Kind, issue.Detail)
		}
		return fmt.Errorf("snapshot derived index build requires complete balance trace coverage over [%d,%d]: missingBlockTraces=%d missingAccountTraces=%d mismatched=%d",
			fromBlock,
			toBlock,
			coverage.MissingBlockBalanceTrace,
			coverage.MissingAccountTrace,
			coverage.MismatchedBlockBalanceTrace,
		)
	}

	result, err := statesnapshots.NewAggregator(dir).BuildDerivedIndexes(chainDB, fromBlock, toBlock, statesnapshots.AggregatorBuildDerivedOptions{
		BalanceTraces:   true,
		SectionBlooms:   true,
		EventLogs:       true,
		EventLogVersion: eventLogVersion,
		ETL:             etlOpts,
	})
	if err != nil {
		return err
	}
	if err := ensureSnapshotManifestChainIdentity(dir, identity); err != nil {
		return err
	}
	paths := make([]string, 0, len(result.Segments))
	for _, ref := range result.Segments {
		paths = append(paths, ref.Path)
	}
	var generation uint64
	var activeSegments int
	if result.Manifest != nil {
		generation = result.Manifest.Generation
		activeSegments = len(result.Manifest.Segments)
	}
	fmt.Printf("Derived snapshot indexes built: blocks=[%d,%d] paths=%s manifestGeneration=%d activeSegments=%d\n",
		fromBlock,
		toBlock,
		strings.Join(paths, ","),
		generation,
		activeSegments,
	)
	return nil
}

func snapshotBuildDerivedIndexesFromCold(dir string, fromBlock, toBlock uint64, eventLogVersion uint32, etlOpts statesnapshots.RestoreETLOptions) (*statesnapshots.AggregatorBuildResult, error) {
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		return nil, err
	}
	if err := requireSnapshotColdCoverage("balance-trace", fromBlock, toBlock, mgr.BalanceTraceRangeCovered); err != nil {
		return nil, err
	}
	if err := requireSnapshotColdCoverage("section-bloom", fromBlock, toBlock, mgr.SectionBloomRangeCovered); err != nil {
		return nil, err
	}
	if err := requireSnapshotColdCoverage("event-log", fromBlock, toBlock, mgr.EventLogRangeCovered); err != nil {
		return nil, err
	}

	aggregator := statesnapshots.NewAggregator(dir)
	combined := &statesnapshots.AggregatorBuildResult{}
	for _, build := range []func() (*statesnapshots.AggregatorBuildResult, error){
		func() (*statesnapshots.AggregatorBuildResult, error) {
			return aggregator.BuildBalanceTracesFromReaderWithOptions(mgr, fromBlock, toBlock, etlOpts)
		},
		func() (*statesnapshots.AggregatorBuildResult, error) {
			return aggregator.BuildSectionBloomsFromReaderWithOptions(mgr, fromBlock, toBlock, etlOpts)
		},
		func() (*statesnapshots.AggregatorBuildResult, error) {
			return aggregator.BuildEventLogsFromReaderWithBuildOptions(mgr, fromBlock, toBlock, statesnapshots.EventLogBuildOptions{Version: eventLogVersion, ETL: etlOpts})
		},
	} {
		result, err := build()
		if err != nil {
			return nil, err
		}
		combined.Manifest = result.Manifest
		combined.Segments = append(combined.Segments, result.Segments...)
	}
	return combined, nil
}

func requireSnapshotColdCoverage(label string, fromBlock, toBlock uint64, check func(uint64, uint64) (bool, error)) error {
	covered, err := check(fromBlock, toBlock)
	if err != nil {
		return err
	}
	if !covered {
		return fmt.Errorf("snapshot cold %s build requires verified cold coverage over [%d,%d]", label, fromBlock, toBlock)
	}
	return nil
}

func printSnapshotBuildResult(label string, fromBlock, toBlock uint64, result *statesnapshots.AggregatorBuildResult) {
	var paths []string
	var generation uint64
	var activeSegments int
	if result != nil {
		paths = make([]string, 0, len(result.Segments))
		for _, ref := range result.Segments {
			paths = append(paths, ref.Path)
		}
		if result.Manifest != nil {
			generation = result.Manifest.Generation
			activeSegments = len(result.Manifest.Segments)
		}
	}
	fmt.Printf("%s: blocks=[%d,%d] paths=%s manifestGeneration=%d activeSegments=%d\n",
		label,
		fromBlock,
		toBlock,
		strings.Join(paths, ","),
		generation,
		activeSegments,
	)
}

func ensureSnapshotManifestChainIdentity(dir string, identity statesnapshots.ChainIdentity) error {
	_, err := statesnapshots.EnsureProductionManifestChainIdentity(dir, identity)
	return err
}

func snapshotETLOptions(ctx *cli.Context) (statesnapshots.RestoreETLOptions, error) {
	buffer, err := mibToIntBytes(ctx.Uint64("snapshot.etl.buffer"), "snapshot.etl.buffer")
	if err != nil {
		return statesnapshots.RestoreETLOptions{}, err
	}
	batch, err := mibToIntBytes(ctx.Uint64("snapshot.etl.batch"), "snapshot.etl.batch")
	if err != nil {
		return statesnapshots.RestoreETLOptions{}, err
	}
	return statesnapshots.RestoreETLOptions{
		TempDir:     strings.TrimSpace(ctx.String("snapshot.etl.tempdir")),
		BufferLimit: buffer,
		BatchSize:   batch,
	}, nil
}

func snapshotPruneChainLookupsCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := snapshotGenesisFromContext(ctx)
	if err != nil {
		return err
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	trustedKeys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		return err
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
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

	chainConfig, genesisHash, err := core.SetupGenesisBlockWithAncient(db, ancientReader, genesis)
	if err != nil {
		return fmt.Errorf("setup genesis: %w", err)
	}
	identity := snapshotExpectedChainIdentity(chainConfig, genesis, genesisHash, forkConfigHash)
	result, err := pruneVerifiedHotChainLookups(rawdb.NewChainDB(db, ancientReader), dir, identity, trustedKeys)
	if err != nil {
		return err
	}
	prunedRange := "none"
	if result.HasRange {
		prunedRange = fmt.Sprintf("[%d,%d]", result.FromBlock, result.ToBlock)
	}
	fmt.Printf("Chain lookup rows pruned: range=%s coldIndexSegments=%d missingIndexSegments=%d blockIndexes=%d stateRoots=%d txIndexes=%d txInfos=%d\n",
		prunedRange,
		result.ColdIndexSegments,
		result.MissingIndexSegments,
		result.BlockIndexesDeleted,
		result.StateRootsDeleted,
		result.TxIndexesDeleted,
		result.TxInfosDeleted,
	)
	return nil
}

func pruneVerifiedHotChainLookups(db ethdb.KeyValueStore, dir string, identity statesnapshots.ChainIdentity, trustedKeys []ed25519.PublicKey) (*statesnapshots.PruneHotChainLookupResult, error) {
	_, manifest, _, err := statesnapshots.VerifySignedSnapshotCatalogManifest(dir, identity, trustedKeys)
	if err != nil {
		return nil, err
	}
	return statesnapshots.PruneHotChainLookupsWithProgress(db, dir, manifest)
}

func snapshotPruneBalanceTracesCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := snapshotGenesisFromContext(ctx)
	if err != nil {
		return err
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	trustedKeys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		return err
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
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

	chainConfig, genesisHash, err := core.SetupGenesisBlockWithAncient(db, ancientReader, genesis)
	if err != nil {
		return fmt.Errorf("setup genesis: %w", err)
	}
	identity := snapshotExpectedChainIdentity(chainConfig, genesis, genesisHash, forkConfigHash)
	result, err := pruneVerifiedHotBalanceTraces(db, dir, identity, trustedKeys)
	if err != nil {
		return err
	}
	prunedRange := "none"
	if result.HasRange {
		prunedRange = fmt.Sprintf("[%d,%d]", result.FromBlock, result.ToBlock)
	}
	fmt.Printf("Balance trace rows pruned: range=%s coldTraceSegments=%d blockTraces=%d accountTraces=%d\n",
		prunedRange,
		result.ColdTraceSegments,
		result.BlockTracesDeleted,
		result.AccountTracesDeleted,
	)
	return nil
}

func pruneVerifiedHotBalanceTraces(db ethdb.KeyValueStore, dir string, identity statesnapshots.ChainIdentity, trustedKeys []ed25519.PublicKey) (*statesnapshots.PruneHotBalanceTraceResult, error) {
	_, manifest, _, err := statesnapshots.VerifySignedSnapshotCatalogManifest(dir, identity, trustedKeys)
	if err != nil {
		return nil, err
	}
	return statesnapshots.PruneHotBalanceTracesWithProgress(db, dir, manifest)
}

func snapshotPruneSectionBloomsCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := snapshotGenesisFromContext(ctx)
	if err != nil {
		return err
	}
	dir := snapshotDir(ctx, cfg.DataDir)
	trustedKeys, err := snapshotTrustedCatalogKeys(ctx)
	if err != nil {
		return err
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
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

	chainConfig, genesisHash, err := core.SetupGenesisBlockWithAncient(db, ancientReader, genesis)
	if err != nil {
		return fmt.Errorf("setup genesis: %w", err)
	}
	identity := snapshotExpectedChainIdentity(chainConfig, genesis, genesisHash, forkConfigHash)
	result, err := pruneVerifiedHotSectionBlooms(rawdb.NewChainDB(db, ancientReader), dir, identity, trustedKeys)
	if err != nil {
		return err
	}
	prunedRange := "none"
	if result.HasRange {
		prunedRange = fmt.Sprintf("[%d,%d]", result.FromSection, result.ToSection)
	}
	fmt.Printf("Section bloom rows pruned: sections=%s coldBloomSegments=%d rows=%d\n",
		prunedRange,
		result.ColdBloomSegments,
		result.RowsDeleted,
	)
	return nil
}

func pruneVerifiedHotSectionBlooms(db *rawdb.ChainDB, dir string, identity statesnapshots.ChainIdentity, trustedKeys []ed25519.PublicKey) (*statesnapshots.PruneHotSectionBloomResult, error) {
	_, manifest, _, err := statesnapshots.VerifySignedSnapshotCatalogManifest(dir, identity, trustedKeys)
	if err != nil {
		return nil, err
	}
	return statesnapshots.PruneHotSectionBloomsWithProgress(db, dir, manifest)
}

func snapshotPruneRetiredCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	dir := snapshotDir(ctx, cfg.DataDir)
	retain := ctx.Int("snapshot.catalog-retain")
	if retain <= 0 {
		if ctx.IsSet("snapshot.catalog-retain") {
			return fmt.Errorf("--snapshot.catalog-retain must be positive, got %d", retain)
		}
		retain = statesnapshots.DefaultPublishedSnapshotRetain
	}
	grace := ctx.Duration("snapshot.catalog-grace")
	if grace <= 0 {
		if ctx.IsSet("snapshot.catalog-grace") {
			return fmt.Errorf("--snapshot.catalog-grace must be positive, got %s", grace)
		}
		grace = statesnapshots.DefaultPublishedSnapshotGrace
	}
	published, err := statesnapshots.PrunePublishedSnapshotManifests(
		dir,
		retain,
		grace,
	)
	if err != nil {
		return err
	}
	result, err := statepruning.PruneRetiredSnapshotFilesContext(ctx.Context, dir)
	if err != nil {
		return err
	}
	fmt.Printf("Retired snapshot segments pruned: publishedExpired=%d retired=%d deleted=%d missing=%d skippedActive=%d skippedPublished=%d bytesDeleted=%d\n",
		published.Deleted,
		result.RetiredSegments,
		result.FilesDeleted,
		result.FilesMissing,
		result.FilesSkippedActive,
		result.FilesSkippedPublished,
		result.BytesDeleted,
	)
	return nil
}

func snapshotEventLogIndexStatsCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	dir := snapshotDir(ctx, cfg.DataDir)
	inspection, err := statesnapshots.InspectEventLogIndexes(dir)
	if err != nil {
		return err
	}
	fromBlock, toBlock := "-1", "-1"
	var minBlock, maxBlock uint64
	for i, stats := range inspection.Segments {
		if i == 0 || stats.FromBlock < minBlock {
			minBlock = stats.FromBlock
		}
		if i == 0 || stats.ToBlock > maxBlock {
			maxBlock = stats.ToBlock
		}
	}
	if len(inspection.Segments) > 0 {
		fromBlock = strconv.FormatUint(minBlock, 10)
		toBlock = strconv.FormatUint(maxBlock, 10)
	}
	fmt.Printf("Event log index stats: dir=%s segments=%d fromBlock=%s toBlock=%s addressKeys=%d addressPostings=%d addressAvgPostingsMilli=%d addressMaxPostings=%d addressSingletonKeys=%d addressMultiPostingKeys=%d topicKeys=%d topicPostings=%d topicAvgPostingsMilli=%d topicMaxPostings=%d topicSingletonKeys=%d topicMultiPostingKeys=%d\n",
		dir,
		len(inspection.Segments),
		fromBlock,
		toBlock,
		inspection.Address.Keys,
		inspection.Address.Postings,
		inspection.Address.AveragePostingsPerKeyMilli,
		inspection.Address.MaxPostingsPerKey,
		inspection.Address.SingletonKeys,
		inspection.Address.MultiPostingKeys,
		inspection.Topic.Keys,
		inspection.Topic.Postings,
		inspection.Topic.AveragePostingsPerKeyMilli,
		inspection.Topic.MaxPostingsPerKey,
		inspection.Topic.SingletonKeys,
		inspection.Topic.MultiPostingKeys,
	)
	for _, stats := range inspection.Segments {
		fmt.Printf("Event log index segment: path=%s range=[%d,%d] size=%d addressKeys=%d addressPostings=%d addressAvgPostingsMilli=%d addressMaxPostings=%d addressSingletonKeys=%d addressMultiPostingKeys=%d topicKeys=%d topicPostings=%d topicAvgPostingsMilli=%d topicMaxPostings=%d topicSingletonKeys=%d topicMultiPostingKeys=%d\n",
			stats.Path,
			stats.FromBlock,
			stats.ToBlock,
			stats.Size,
			stats.Address.Keys,
			stats.Address.Postings,
			stats.Address.AveragePostingsPerKeyMilli,
			stats.Address.MaxPostingsPerKey,
			stats.Address.SingletonKeys,
			stats.Address.MultiPostingKeys,
			stats.Topic.Keys,
			stats.Topic.Postings,
			stats.Topic.AveragePostingsPerKeyMilli,
			stats.Topic.MaxPostingsPerKey,
			stats.Topic.SingletonKeys,
			stats.Topic.MultiPostingKeys,
		)
	}
	return nil
}

func snapshotEventLogSpaceBenchmarkCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	dir := snapshotDir(ctx, cfg.DataDir)
	inspection, err := statesnapshots.InspectEventLogSpace(dir, statesnapshots.EventLogSpaceInspectOptions{
		SampleSegments: ctx.Uint64("snapshot.event-log.sample-segments"),
		Context:        contextOrBackground(ctx),
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(inspection)
}

func snapshotHistorySpaceBenchmarkCmd(ctx *cli.Context) error {
	const mib = uint64(1 << 20)
	sampleMiB := ctx.Uint64("snapshot.history.sample-mib")
	if sampleMiB > ^uint64(0)/mib {
		return errors.New("--snapshot.history.sample-mib is too large")
	}
	progressInterval := ctx.Duration("progress")
	if progressInterval < 0 {
		return errors.New("--progress must not be negative")
	}
	cfg := makeConfig(ctx)
	inspection, err := statesnapshots.InspectHistorySpace(snapshotDir(ctx, cfg.DataDir), statesnapshots.HistorySpaceInspectOptions{
		SampleSegments:       ctx.Uint64("snapshot.history.sample-segments"),
		SampleIndexEntries:   ctx.Uint64("snapshot.history.sample-index-entries"),
		SampleAccessorBlocks: ctx.Uint64("snapshot.history.sample-accessor-blocks"),
		SampleHistoryBytes:   sampleMiB * mib,
		ProgressInterval:     progressInterval,
		Progress: func(progress statesnapshots.HistorySpaceInspectProgress) {
			fmt.Fprintf(os.Stderr, "history-space benchmark phase=%s trios=%d/%d elapsed=%s path=%s\n",
				progress.Phase,
				progress.CompletedTrios,
				progress.TotalTrios,
				progress.Elapsed.Round(time.Millisecond),
				progress.HistoryPath,
			)
		},
		Context: contextOrBackground(ctx),
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(inspection)
}

func snapshotMigrateHistoryV7Cmd(ctx *cli.Context) error {
	if !ctx.Bool("yes") {
		return errors.New("refusing to migrate history without --yes; stop gtron and rerun with explicit confirmation")
	}
	cfg := makeConfig(ctx)
	result, err := statesnapshots.MigrateHistoryV7(snapshotDir(ctx, cfg.DataDir), statesnapshots.HistoryV7MigrationOptions{
		Context:  contextOrBackground(ctx),
		MaxTrios: ctx.Uint64("max-trios"),
		OnProgress: func(progress statesnapshots.HistoryV7MigrationProgress) {
			fmt.Fprintf(os.Stderr, "history V7 migration trios=%d/%d migrated=%d range=[%d,%d] active=%s->%s elapsed=%s path=%s\n",
				progress.CompletedTrios,
				progress.TotalTrios,
				progress.MigratedTrios,
				progress.FromTxNum,
				progress.ToTxNum,
				formatIEC(progress.ActiveBytesBefore),
				formatIEC(progress.ActiveBytesAfter),
				progress.Elapsed.Round(time.Millisecond),
				progress.CurrentHistory,
			)
		},
	})
	if err != nil {
		return err
	}
	if ctx.Bool("json") {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	fmt.Printf("History V7 migration completed: trios=%d current=%d migrated=%d remaining=%d active=%s->%s retiredAdded=%s elapsed=%s\n",
		result.TotalTrios,
		result.AlreadyCurrent,
		result.MigratedTrios,
		result.RemainingTrios,
		formatIEC(result.ActiveBytesBefore),
		formatIEC(result.ActiveBytesAfter),
		formatIEC(result.RetiredBytesAdded),
		time.Duration(result.ElapsedSeconds*float64(time.Second)).Round(time.Millisecond),
	)
	return nil
}

func snapshotMigrateEventLogsV3Cmd(ctx *cli.Context) error {
	if !ctx.IsSet("snapshot.from-block") {
		return errors.New("snapshot V3 migration requires --snapshot.from-block at an active event-log segment boundary")
	}
	cfg := makeConfig(ctx)
	result, err := statesnapshots.MigrateEventLogsV3(snapshotDir(ctx, cfg.DataDir), statesnapshots.EventLogV3MigrationOptions{
		FromBlock:  ctx.Uint64("snapshot.from-block"),
		ToBlock:    ctx.Uint64("snapshot.to-block"),
		ToBlockSet: ctx.IsSet("snapshot.to-block"),
		Merge:      ctx.Uint64("snapshot.event-log.merge"),
		Publish:    ctx.Bool("publish"),
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func snapshotRestoreVerificationOptions(db ethdb.KeyValueStore) statesnapshots.RestoreVerifiedSnapshotOptions {
	return statesnapshots.RestoreVerifiedSnapshotOptions{
		Boundary: statesnapshots.VerifyRestoredSnapshotBoundaryOptions{
			RequireIndependentCommitmentRoot: true,
			RebuildCommitmentRoot: func() (common.Hash, error) {
				commitmentDB, ok := any(db).(statedomains.CommitmentDB)
				if !ok {
					return common.Hash{}, errors.New("snapshot restore commitment root rebuild requires reader/writer/iterator database")
				}
				return statedomains.NewStagedCommitmentStore(commitmentDB).Rebuild()
			},
		},
	}
}

func contextOrBackground(ctx *cli.Context) context.Context {
	if ctx != nil && ctx.Context != nil {
		return ctx.Context
	}
	return context.Background()
}

func snapshotEventLogBuildVersion(ctx *cli.Context) (uint32, error) {
	version := uint32(ctx.Uint("snapshot.event-log.version"))
	if version != statesnapshots.EventLogSegmentVersion && version != statesnapshots.EventLogSegmentV3Version {
		return 0, fmt.Errorf("--snapshot.event-log.version must be 2 or 3, got %d", version)
	}
	return version, nil
}

func snapshotDir(ctx *cli.Context, dataDir string) string {
	if dir := strings.TrimSpace(ctx.String("snapshot.dir")); dir != "" {
		return dir
	}
	return stateSnapshotsDir(dataDir)
}

func snapshotRemoteURL(ctx *cli.Context) (string, error) {
	url := strings.TrimSpace(ctx.String("snapshot.url"))
	if url == "" {
		return "", errors.New("snapshot fetch requires --snapshot.url or GTRON_SNAPSHOT_URL")
	}
	return url, nil
}

func snapshotBootstrapPreflightRestoreTarget(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}
	forkConfigHash, err := normaliseSnapshotForkConfigHash(ctx.String("snapshot.fork-config-hash"))
	if err != nil {
		return err
	}
	identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, forkConfigHash)
	if err != nil {
		return err
	}

	dbPath := chainDataDir(cfg.DataDir)
	if info, err := os.Stat(dbPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat database: %w", err)
		}
	} else {
		if !info.IsDir() {
			return fmt.Errorf("stat database: %s is not a directory", dbPath)
		}
		db, err := openPebbleDB(ctx, dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer db.Close()
		if err := ensureSnapshotRestoreBootstrapDatadir(db, common.HexToHash(identity.GenesisHash)); err != nil {
			return err
		}
	}

	ancientReader, closeAncient, err := openSnapshotPruneAncientReader(cfg.DataDir)
	if err != nil {
		return err
	}
	defer closeAncient()
	return ensureSnapshotRestoreEmptyFreezer(ancientReader)
}

func resetSnapshotFetchDir(dir string) error {
	clean := filepath.Clean(strings.TrimSpace(dir))
	if clean == "" || clean == "." || clean == string(filepath.Separator) {
		return fmt.Errorf("refusing to reset unsafe snapshot directory %q", dir)
	}
	if err := os.RemoveAll(clean); err != nil {
		return fmt.Errorf("reset snapshot directory %s: %w", clean, err)
	}
	return nil
}

func snapshotTrustedCatalogKeys(ctx *cli.Context) ([]ed25519.PublicKey, error) {
	values := append([]string(nil), ctx.StringSlice("snapshot.trusted-key")...)
	if path := strings.TrimSpace(ctx.String("snapshot.trusted-key-file")); path != "" {
		fileValues, err := readSnapshotTrustedCatalogKeyFile(path)
		if err != nil {
			return nil, err
		}
		values = append(values, fileValues...)
	}
	return parseSnapshotTrustedCatalogKeys(values)
}

func readSnapshotTrustedCatalogKeyFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot trusted key file %s: %w", path, err)
	}
	var values []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if cut := strings.IndexByte(line, '#'); cut >= 0 {
			line = strings.TrimSpace(line[:cut])
		}
		if line == "" {
			continue
		}
		values = append(values, line)
	}
	return values, nil
}

func parseSnapshotTrustedCatalogKeys(values []string) ([]ed25519.PublicKey, error) {
	var out []ed25519.PublicKey
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			key, err := parseSnapshotCatalogPublicKey(part)
			if err != nil {
				return nil, err
			}
			if key != nil {
				out = append(out, key)
			}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("snapshot catalog verification requires at least one --snapshot.trusted-key, --snapshot.trusted-key-file, GTRON_SNAPSHOT_TRUSTED_KEY, or GTRON_SNAPSHOT_TRUSTED_KEY_FILE")
	}
	return out, nil
}

func snapshotCatalogSigningKey(ctx *cli.Context) (ed25519.PrivateKey, error) {
	if path := strings.TrimSpace(ctx.String("snapshot.signing-key-file")); path != "" {
		raw, err := readSnapshotCatalogSigningKeyFile(path)
		if err != nil {
			return nil, err
		}
		return parseSnapshotCatalogPrivateKey(raw)
	}
	return parseSnapshotCatalogPrivateKey(ctx.String("snapshot.signing-key"))
}

func runtimeSnapshotCatalogSigningKey(ctx *cli.Context) (ed25519.PrivateKey, bool, error) {
	path := strings.TrimSpace(ctx.String("snapshot.catalog-signing-key-file"))
	if path == "" {
		return nil, false, nil
	}
	raw, err := readSnapshotCatalogSigningKeyFile(path)
	if err != nil {
		return nil, false, err
	}
	key, err := parseSnapshotCatalogPrivateKey(raw)
	if err != nil {
		return nil, false, err
	}
	return key, true, nil
}

func readSnapshotCatalogSigningKeyFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read snapshot signing key file %s: %w", path, err)
	}
	var value string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if cut := strings.IndexByte(line, '#'); cut >= 0 {
			line = strings.TrimSpace(line[:cut])
		}
		if line == "" {
			continue
		}
		if value != "" {
			return "", fmt.Errorf("snapshot signing key file %s contains multiple keys", path)
		}
		value = line
	}
	if value == "" {
		return "", fmt.Errorf("snapshot signing key file %s is empty", path)
	}
	return value, nil
}

func parseSnapshotCatalogPublicKey(raw string) (ed25519.PublicKey, error) {
	data, err := decodeSnapshotHex("snapshot trusted key", raw)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("snapshot trusted key length %d, want %d", len(data), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(data), nil
}

func parseSnapshotCatalogPrivateKey(raw string) (ed25519.PrivateKey, error) {
	data, err := decodeSnapshotHex("snapshot signing key", raw)
	if err != nil {
		return nil, err
	}
	switch len(data) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(data), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(data), nil
	default:
		return nil, fmt.Errorf("snapshot signing key length %d, want %d-byte seed or %d-byte private key", len(data), ed25519.SeedSize, ed25519.PrivateKeySize)
	}
}

func decodeSnapshotHex(field, raw string) ([]byte, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return nil, nil
	}
	value = strings.TrimPrefix(value, "ed25519:")
	value = strings.TrimPrefix(value, "0x")
	data, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("%s is not valid hex: %w", field, err)
	}
	return data, nil
}

func normaliseSnapshotForkConfigHash(raw string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return "", nil
	}
	value = strings.TrimPrefix(value, "sha256:")
	if len(value) != 64 {
		return "", fmt.Errorf("snapshot fork config hash has %d hex chars, want 64", len(value))
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", fmt.Errorf("snapshot fork config hash is not hex: %w", err)
	}
	return "sha256:" + value, nil
}

func snapshotExpectedChainIdentityFromContext(ctx *cli.Context, forkConfigHash string) (statesnapshots.ChainIdentity, error) {
	genesis, err := snapshotGenesisFromContext(ctx)
	if err != nil {
		return statesnapshots.ChainIdentity{}, err
	}
	return snapshotExpectedChainIdentityFromGenesis(genesis, forkConfigHash)
}

func snapshotGenesisFromContext(ctx *cli.Context) (*params.Genesis, error) {
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.Bool("dev") {
		return genesis, nil
	}
	key, err := parseWitnessKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("dev snapshot command requires --witness.key: %w", err)
	}
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	return makeDevGenesis(witnessAddr, ctx.Bool("dev.full-features"), ctx.Int64("dev.maintenance-interval")), nil
}

func snapshotExpectedChainIdentityFromGenesis(genesis *params.Genesis, forkConfigHash string) (statesnapshots.ChainIdentity, error) {
	if genesis == nil || genesis.Config == nil {
		return statesnapshots.ChainIdentity{}, errors.New("snapshot chain identity requires genesis config")
	}
	db := rawdb.NewMemoryDatabase()
	block, err := core.GenesisToBlock(genesis, corestate.NewDatabase(rawdb.WrapKeyValueStore(db)))
	if err != nil {
		return statesnapshots.ChainIdentity{}, fmt.Errorf("build genesis block: %w", err)
	}
	return snapshotExpectedChainIdentity(genesis.Config, genesis, block.Hash(), forkConfigHash), nil
}

func snapshotExpectedChainIdentity(chainConfig *params.ChainConfig, genesis *params.Genesis, genesisHash common.Hash, forkConfigHash string) statesnapshots.ChainIdentity {
	var chainID int64
	if chainConfig != nil {
		chainID = chainConfig.ChainID
	}
	return statesnapshots.ChainIdentity{
		ChainID:        chainID,
		NetworkID:      resolveNetworkID(genesis),
		GenesisHash:    hex.EncodeToString(genesisHash[:]),
		ForkConfigHash: forkConfigHash,
	}
}

func ensureSnapshotRestoreBootstrapDatadir(db ethdb.KeyValueReader, genesisHash common.Hash) error {
	head, ok, err := rawdb.ReadHeadBlockHashStrict(db)
	if err != nil {
		return fmt.Errorf("snapshot restore read head block hash: %w", err)
	}
	if !ok || head == (common.Hash{}) || head == genesisHash {
		return nil
	}
	return fmt.Errorf("snapshot restore refuses non-genesis datadir: head=%x genesis=%x; use a fresh datadir or an explicit reset workflow", head, genesisHash)
}

func openSnapshotRestoreAncientStore(dataDir string) (statesnapshots.ChainFreezerAncientStore, rawdb.AncientReader, func(), error) {
	ancientPath := ancientDataDir(dataDir)
	if info, err := os.Stat(ancientPath); err != nil && !os.IsNotExist(err) {
		return nil, nil, func() {}, fmt.Errorf("stat freezer: %w", err)
	} else if err == nil && !info.IsDir() {
		return nil, nil, func() {}, fmt.Errorf("stat freezer: %s is not a directory", ancientPath)
	}
	fz, err := rawdbfreezer.NewFreezer(ancientPath, "", false, freezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("open freezer: %w", err)
	}
	reader := rawdb.NewFreezerReader(fz)
	if err := ensureSnapshotRestoreEmptyFreezer(reader); err != nil {
		_ = fz.Close()
		return nil, nil, func() {}, err
	}
	store := newFreezerStore(fz)
	return store, reader, func() { _ = fz.Close() }, nil
}

func ensureSnapshotRestoreEmptyFreezer(reader rawdb.AncientReader) error {
	if reader == nil {
		return nil
	}
	var nonEmpty []string
	for _, table := range []string{rawdb.AncientBlocksTable, rawdb.AncientTxInfosTable, rawdb.AncientStateRootsTable} {
		count, err := reader.AncientCount(table)
		if err != nil {
			return fmt.Errorf("inspect freezer %s count: %w", table, err)
		}
		if count != 0 {
			nonEmpty = append(nonEmpty, fmt.Sprintf("%s=%d", table, count))
		}
	}
	if len(nonEmpty) == 0 {
		return nil
	}
	return fmt.Errorf("snapshot restore refuses non-empty freezer: %s; use a fresh datadir or an explicit reset workflow", strings.Join(nonEmpty, " "))
}

func openSnapshotPruneAncientReader(dataDir string) (rawdb.AncientReader, func(), error) {
	ancientPath := ancientDataDir(dataDir)
	if info, err := os.Stat(ancientPath); err != nil {
		if os.IsNotExist(err) {
			return rawdb.NoopAncient{}, func() {}, nil
		}
		return nil, func() {}, fmt.Errorf("stat freezer: %w", err)
	} else if !info.IsDir() {
		return nil, func() {}, fmt.Errorf("stat freezer: %s is not a directory", ancientPath)
	}
	fz, err := rawdbfreezer.NewFreezer(ancientPath, "", true, freezerTableSize, chainfreezer.FreezerTableSet())
	if err != nil {
		return nil, func() {}, fmt.Errorf("open freezer: %w", err)
	}
	return rawdb.NewFreezerReader(fz), func() { _ = fz.Close() }, nil
}
