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
		Name:  "snapshot.url",
		Usage: "HTTP(S) base URL for a remote snapshot directory",
	}
	snapshotResetFlag = &cli.BoolFlag{
		Name:  "snapshot.reset",
		Usage: "Delete the local snapshot directory before fetching the remote snapshot",
	}
	snapshotTrustedCatalogKeyFlag = &cli.StringSliceFlag{
		Name:  "snapshot.trusted-key",
		Usage: "Trusted Ed25519 snapshot catalog public key as hex; repeatable or comma-separated",
	}
	snapshotTrustedCatalogKeyFileFlag = &cli.StringFlag{
		Name:  "snapshot.trusted-key-file",
		Usage: "File containing trusted Ed25519 snapshot catalog public keys, one per line; # comments and comma-separated entries are allowed",
	}
	snapshotForkConfigHashFlag = &cli.StringFlag{
		Name:  "snapshot.fork-config-hash",
		Usage: "Expected fork config hash as sha256:<hex>; required when the catalog carries forkConfigHash",
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
					snapshotTrustedCatalogKeyFlag,
					snapshotTrustedCatalogKeyFileFlag,
					snapshotForkConfigHashFlag,
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
				},
				Action: snapshotBuildFreezerCmd,
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
	result, err := statesnapshots.FetchRemoteSnapshot(contextOrBackground(ctx), statesnapshots.FetchRemoteSnapshotOptions{
		BaseURL:     ctx.String("snapshot.url"),
		Dir:         dir,
		Expected:    identity,
		TrustedKeys: trustedKeys,
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
	result, err := statesnapshots.RestoreSnapshotFromVerifiedCatalogWithOptions(db, dir, identity, trustedKeys, snapshotRestoreVerificationOptions(db))
	if err != nil {
		return err
	}
	freezerResult, err := statesnapshots.RestoreChainFreezerFromVerifiedCatalogWithOptions(ancientStore, dir, identity, trustedKeys, statesnapshots.RestoreChainFreezerOptions{
		IndexWriter:       db,
		ProgressWriter:    db,
		PreferColdIndexes: true,
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
	result, err := statesnapshots.NewAggregator(dir).BuildChainFreezer(rawdb.NewFreezerReader(fz), fromBlock, toBlock)
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
	fmt.Printf("Chain lookup rows pruned: coldIndexSegments=%d blockIndexes=%d stateRoots=%d txIndexes=%d txInfos=%d\n",
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
		return nil, errors.New("snapshot catalog verification requires at least one --snapshot.trusted-key")
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
