package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	corestate "github.com/tronprotocol/go-tron/core/state"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

var (
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
		Name:  "snapshot.signing-key",
		Usage: "Ed25519 catalog signing key as a 32-byte seed or 64-byte private key in hex",
	}
	snapshotFromBlockFlag = &cli.Uint64Flag{
		Name:  "snapshot.from-block",
		Usage: "First chain-freezer block number to snapshot, inclusive",
	}
	snapshotToBlockFlag = &cli.Uint64Flag{
		Name:  "snapshot.to-block",
		Usage: "Last chain-freezer block number to snapshot, inclusive",
	}
	snapshotETLTempDirFlag = &cli.StringFlag{
		Name:  "snapshot.etl.tempdir",
		Usage: "Parent directory for temporary snapshot ETL run files",
	}
	snapshotETLBufferMiBFlag = &cli.Uint64Flag{
		Name:  "snapshot.etl.buffer",
		Usage: "Snapshot ETL memory buffer limit in MiB (0 = default)",
	}
	snapshotETLBatchMiBFlag = &cli.Uint64Flag{
		Name:  "snapshot.etl.batch",
		Usage: "Snapshot ETL output batch size in MiB (0 = default)",
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
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
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
					snapshotForkConfigHashFlag,
					snapshotETLTempDirFlag,
					snapshotETLBufferMiBFlag,
					snapshotETLBatchMiBFlag,
				},
				Action: snapshotBuildDerivedIndexesCmd,
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
	if ctx.Bool("snapshot.reset") {
		if err := resetSnapshotFetchDir(dir); err != nil {
			return err
		}
	}
	baseURL, err := snapshotRemoteURL(ctx)
	if err != nil {
		return err
	}
	result, err := statesnapshots.FetchRemoteSnapshot(contextOrBackground(ctx), statesnapshots.FetchRemoteSnapshotOptions{
		BaseURL:                baseURL,
		Dir:                    dir,
		Expected:               identity,
		TrustedKeys:            trustedKeys,
		MaxConcurrentDownloads: ctx.Int("snapshot.fetch.concurrency"),
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
	if err := snapshotFetchCmd(ctx); err != nil {
		return err
	}
	return snapshotRestoreCmd(ctx)
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
		fmt.Printf("Chain freezer restored: blocks=[%d,%d] count=%d coldIndexSegments=%d blockIndexes=%d txIndexes=%d txInfos=%d\n",
			freezerResult.FromBlock,
			freezerResult.ToBlock,
			freezerResult.BlocksRestored,
			freezerResult.ColdIndexSegments,
			freezerResult.BlockIndexesRestored,
			freezerResult.TxIndexesRestored,
			freezerResult.TxInfosRestored,
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
	key, err := parseSnapshotCatalogPrivateKey(ctx.String("snapshot.signing-key"))
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

	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	result, err := statesnapshots.NewAggregator(dir).BuildBalanceTracesWithOptions(db, fromBlock, toBlock, etlOpts)
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
	db, err := openPebbleDB(ctx, chainDataDir(cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	result, err := statesnapshots.NewAggregator(dir).BuildSectionBloomsWithOptions(db, fromBlock, toBlock, etlOpts)
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

	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	result, err := statesnapshots.NewAggregator(dir).BuildEventLogsWithOptions(rawdb.NewChainDB(db, ancientReader), fromBlock, toBlock, etlOpts)
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

	dir := snapshotDir(ctx, cfg.DataDir)
	etlOpts, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	result, err := statesnapshots.NewAggregator(dir).BuildDerivedIndexes(chainDB, fromBlock, toBlock, statesnapshots.AggregatorBuildDerivedOptions{
		BalanceTraces: true,
		SectionBlooms: true,
		EventLogs:     true,
		ETL:           etlOpts,
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

func ensureSnapshotManifestChainIdentity(dir string, identity statesnapshots.ChainIdentity) error {
	manifest, err := statesnapshots.LoadProductionManifest(dir)
	if err != nil {
		return err
	}
	if manifest.Chain != nil {
		return manifest.ValidateChainIdentity(identity)
	}
	manifest.Chain = &identity
	return statesnapshots.PublishManifest(dir, manifest)
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
	result, err := pruneVerifiedHotChainLookups(db, dir, identity, trustedKeys)
	if err != nil {
		return err
	}
	prunedRange := "none"
	if result.HasRange {
		prunedRange = fmt.Sprintf("[%d,%d]", result.FromBlock, result.ToBlock)
	}
	fmt.Printf("Chain lookup rows pruned: range=%s coldIndexSegments=%d blockIndexes=%d stateRoots=%d txIndexes=%d txInfos=%d\n",
		prunedRange,
		result.ColdIndexSegments,
		result.BlockIndexesDeleted,
		result.StateRootsDeleted,
		result.TxIndexesDeleted,
		result.TxInfosDeleted,
	)
	return nil
}

func pruneVerifiedHotChainLookups(db ethdb.KeyValueStore, dir string, identity statesnapshots.ChainIdentity, trustedKeys []ed25519.PublicKey) (*statesnapshots.PruneHotChainLookupResult, error) {
	if _, _, err := statesnapshots.VerifySignedSnapshotCatalog(dir, identity, trustedKeys); err != nil {
		return nil, err
	}
	manifest, err := statesnapshots.LoadProductionManifest(dir)
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
	if _, _, err := statesnapshots.VerifySignedSnapshotCatalog(dir, identity, trustedKeys); err != nil {
		return nil, err
	}
	manifest, err := statesnapshots.LoadProductionManifest(dir)
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
	result, err := pruneVerifiedHotSectionBlooms(db, dir, identity, trustedKeys)
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

func pruneVerifiedHotSectionBlooms(db ethdb.KeyValueStore, dir string, identity statesnapshots.ChainIdentity, trustedKeys []ed25519.PublicKey) (*statesnapshots.PruneHotSectionBloomResult, error) {
	if _, _, err := statesnapshots.VerifySignedSnapshotCatalog(dir, identity, trustedKeys); err != nil {
		return nil, err
	}
	manifest, err := statesnapshots.LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	return statesnapshots.PruneHotSectionBloomsWithProgress(db, dir, manifest)
}

func snapshotPruneRetiredCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	dir := snapshotDir(ctx, cfg.DataDir)
	result, err := statesnapshots.PruneRetiredSegmentFiles(dir)
	if err != nil {
		return err
	}
	fmt.Printf("Retired snapshot segments pruned: retired=%d deleted=%d missing=%d skippedActive=%d bytesDeleted=%d\n",
		result.RetiredSegments,
		result.FilesDeleted,
		result.FilesMissing,
		result.FilesSkippedActive,
		result.BytesDeleted,
	)
	return nil
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
	head := rawdb.ReadHeadBlockHash(db)
	if head == (common.Hash{}) || head == genesisHash {
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
	store := newFreezerStore(fz)
	return store, rawdb.NewFreezerReader(fz), func() { _ = fz.Close() }, nil
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
