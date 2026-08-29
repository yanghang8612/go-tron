package main

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	gethmetrics "github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/producer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbetl "github.com/tronprotocol/go-tron/core/rawdb/etl"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/internal/debugapi"
	"github.com/tronprotocol/go-tron/internal/grpcapi"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/metricsapi"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	tnet "github.com/tronprotocol/go-tron/net"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	"github.com/tronprotocol/go-tron/node"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/tronprotocol/go-tron/p2p/discover"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

const (
	domainStateReorgWindow           uint64 = 128
	gtronVersion                            = "0.3.0-dev"
	runtimeSnapshotETLBuffer                = 256 << 20
	snapshotCatchupBuildInterval            = time.Minute
	snapshotCatchupHeavyWorkCooldown        = 3 * time.Second
	heavyWorkRecoveryCooldown               = 15 * time.Second
	heavyWorkCooldownMinDuration            = 250 * time.Millisecond
)

var metricsOnce sync.Once

// closeRuntimeStore converts a cleanup panic into an ordinary shutdown error
// with a complete stack. The chain state has already been synchronously
// flushed before runtime stores are closed, so one faulty file-cache close
// must not turn a requested systemd stop into an abnormal exit and restart
// loop. The stack keeps the underlying close bug diagnosable in production.
func closeRuntimeStore(name string, closeFn func() error) (err error) {
	if closeFn == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%s close panic: %v\n%s", name, recovered, debug.Stack())
		}
	}()
	if err := closeFn(); err != nil {
		return fmt.Errorf("%s close: %w", name, err)
	}
	return nil
}

func applyRuntimeSnapshotETLDefaults(opts statesnapshots.RestoreETLOptions) statesnapshots.RestoreETLOptions {
	if opts.BufferLimit == 0 {
		// Production history builds own two independently sorted accessor
		// streams. The 64 MiB library default forced dozens of NVMe spill runs
		// in dense mainnet windows despite a low live heap. Four times the
		// per-collector buffer remains comfortably below the service memory
		// budget and materially shortens the external tournament merge.
		opts.BufferLimit = runtimeSnapshotETLBuffer
	}
	return opts
}

func enableMetrics() {
	metricsOnce.Do(func() {
		gethmetrics.Enable()
		gethmetrics.GetOrRegisterGauge("gtron/info", nil).Update(1)
		go gethmetrics.CollectProcessMetrics(3 * time.Second)
	})
}

var (
	dataDirFlag = &cli.StringFlag{
		Name:  "datadir",
		Usage: "Data directory for the database and keystore",
		Value: defaultDataDir(),
	}
	p2pPortFlag = &cli.IntFlag{
		Name:  "p2p.port",
		Usage: "P2P listening port",
		Value: 18888,
	}
	discoverPortFlag = &cli.IntFlag{
		Name:  "discover.port",
		Usage: "Kademlia discovery UDP port (0 → reuse --p2p.port)",
		Value: 0,
	}
	externalIPFlag = &cli.StringFlag{
		Name:  "external.ip",
		Usage: "IPv4 address advertised in P2P discovery and hello messages",
	}
	httpPortFlag = &cli.IntFlag{
		Name:  "http.port",
		Usage: "HTTP API port",
		Value: 8090,
	}
	jsonrpcPortFlag = &cli.IntFlag{
		Name:  "jsonrpc.port",
		Usage: "JSON-RPC port",
		Value: 8545,
	}
	testnetFlag = &cli.BoolFlag{
		Name:  "testnet",
		Usage: "Use Nile testnet",
	}
	witnessFlag = &cli.BoolFlag{
		Name:  "witness",
		Usage: "Enable block production",
	}
	witnessKeyFlag = &cli.StringFlag{
		Name:  "witness.key",
		Usage: "Witness private key (hex-encoded)",
	}
	witnessKeysFileFlag = &cli.StringFlag{
		Name:  "witness.keys-file",
		Usage: "Path to a file with one hex-encoded SR private key per line (multi-SR PBFT testing)",
	}
	devFlag = &cli.BoolFlag{
		Name:  "dev",
		Usage: "Dev mode: single-witness chain using the provided witness key",
	}
	devFullFeaturesFlag = &cli.BoolFlag{
		Name:  "dev.full-features",
		Usage: "Enable all mainnet-activated allow_* feature flags in dev genesis (default true)",
		Value: true,
	}
	devMaintenanceIntervalFlag = &cli.Int64Flag{
		Name:  "dev.maintenance-interval",
		Usage: "Maintenance interval in ms for dev genesis (set 30000 to test proposal activation quickly)",
		Value: 21600000,
	}
	genesisFileFlag = &cli.StringFlag{
		Name:  "genesis",
		Usage: "Path to a JSON genesis file (custom-chain bootstrap; mutually exclusive with --testnet/--dev)",
	}
	seednodeFlag = &cli.StringSliceFlag{
		Name:  "seednode",
		Usage: "Seed node address (host:port), can be specified multiple times",
	}
	maxpeersFlag = &cli.IntFlag{
		Name:  "maxpeers",
		Usage: "Maximum number of P2P peers",
		Value: 30,
	}
	grpcPortFlag = &cli.IntFlag{
		Name:  "grpc.port",
		Usage: "gRPC Wallet service port (0 = disabled)",
		Value: 50051,
	}
	pprofPortFlag = &cli.IntFlag{
		Name:  "pprof.port",
		Usage: "HTTP port for pprof + debug endpoints (0 = disabled)",
		Value: 0,
	}
	pprofAddrFlag = &cli.StringFlag{
		Name:  "pprof.addr",
		Usage: "Bind address for the pprof endpoint (defaults to 127.0.0.1)",
		Value: "127.0.0.1",
	}
	metricsEnabledFlag = &cli.BoolFlag{
		Name:    "metrics",
		Usage:   "Enable the dedicated Prometheus metrics endpoint",
		EnvVars: []string{"GTRON_METRICS"},
	}
	metricsAddrFlag = &cli.StringFlag{
		Name:    "metrics.addr",
		Usage:   "Bind address for Prometheus metrics",
		Value:   "127.0.0.1",
		EnvVars: []string{"GTRON_METRICS_ADDR"},
	}
	metricsPortFlag = &cli.IntFlag{
		Name:    "metrics.port",
		Usage:   "HTTP port for Prometheus metrics",
		Value:   6061,
		EnvVars: []string{"GTRON_METRICS_PORT"},
	}
	verbosityFlag = &cli.IntFlag{
		Name:  "verbosity",
		Usage: "Log verbosity (0=Crit 1=Error 2=Warn 3=Info 4=Debug 5=Trace)",
		Value: 3,
	}
	logFormatFlag = &cli.StringFlag{
		Name:  "log.format",
		Usage: "Log output format: terminal|json|logfmt",
		Value: "terminal",
	}
	logFileFlag = &cli.StringFlag{
		Name:  "log.file",
		Usage: "Optional rotating log file path",
	}
	logFileFormatFlag = &cli.StringFlag{
		Name:    "log.file.format",
		Usage:   "File log format: terminal|json|logfmt",
		Value:   "json",
		EnvVars: []string{"GTRON_LOG_FILE_FORMAT"},
	}
	logFileVerbosityFlag = &cli.IntFlag{
		Name:    "log.file.verbosity",
		Usage:   "File log verbosity (0-5; -1 inherits --verbosity)",
		Value:   -1,
		EnvVars: []string{"GTRON_LOG_FILE_VERBOSITY"},
	}
	logFileMaxSizeFlag = &cli.IntFlag{
		Name:    "log.file.max-size",
		Usage:   "Rotate the log file after it reaches this size in MiB",
		Value:   100,
		EnvVars: []string{"GTRON_LOG_FILE_MAX_SIZE"},
	}
	logFileMaxBackupsFlag = &cli.IntFlag{
		Name:    "log.file.max-backups",
		Usage:   "Maximum number of rotated log files to retain (0 = unlimited by count)",
		Value:   3,
		EnvVars: []string{"GTRON_LOG_FILE_MAX_BACKUPS"},
	}
	logFileMaxAgeFlag = &cli.IntFlag{
		Name:    "log.file.max-age",
		Usage:   "Maximum age in days for rotated log files (0 = unlimited by age)",
		Value:   28,
		EnvVars: []string{"GTRON_LOG_FILE_MAX_AGE"},
	}
	logFileCompressFlag = &cli.BoolFlag{
		Name:    "log.file.compress",
		Usage:   "Compress rotated log files",
		Value:   true,
		EnvVars: []string{"GTRON_LOG_FILE_COMPRESS"},
	}
	logModuleFlag = &cli.StringSliceFlag{
		Name:  "log.module",
		Usage: "Per-module log level override (module=trace|debug|info|warn|error|crit or 0-5); repeatable, e.g. --log.module net/sync=debug --log.module p2p=warn",
	}
	pruneModeFlag = &cli.StringFlag{
		Name:  "prune.mode",
		Usage: "Erigon-style retention mode: full | blocks | minimal | snap | archive",
	}
	historyEnabledFlag = &cli.BoolFlag{
		Name:  "history.enabled",
		Usage: "Turn on flat temporal state capture. Explicit prune modes imply it.",
	}
	snapshotCatalogSigningKeyFileRuntimeFlag = &cli.StringFlag{
		Name:    "snapshot.catalog-signing-key-file",
		Usage:   "File with the Ed25519 key used to sign each newly published runtime cold snapshot catalog (snap/archive modes)",
		EnvVars: []string{"GTRON_SNAPSHOT_CATALOG_SIGNING_KEY_FILE"},
	}
	stateTrieCacheFlag = &cli.IntFlag{
		Name:  "state.trie.cache",
		Usage: "Hash-trie clean-node cache size in MiB (-1 auto from --db.cache, 0 disables)",
		Value: -1,
	}
	stateCodeCacheFlag = &cli.IntFlag{
		Name:  "state.code.cache",
		Usage: "Immutable contract bytecode cache size in MiB (0 disables)",
		Value: state.DefaultStateCodeCacheSizeBytes / (1024 * 1024),
	}
	stateCommitmentCacheFlag = &cli.IntFlag{
		Name:  "state.commitment.cache",
		Usage: "Generation-safe commitment/flat-latest base-read cache size in MiB (0 disables)",
		Value: 512,
	}
	configFileFlag = &cli.StringFlag{
		Name:  "config",
		Usage: "Path to a TOML config file (currently understood: [history])",
	}
	dbCacheFlag = &cli.IntFlag{
		Name:  "db.cache",
		Usage: "Pebble read cache size in MiB",
		Value: 256,
	}
	dbHandlesFlag = &cli.IntFlag{
		Name:  "db.handles",
		Usage: "Maximum number of Pebble files to keep open",
		Value: 500,
	}
	dbMemtableFlag = &cli.Uint64Flag{
		Name:  "db.memtable",
		Usage: "Pebble memtable size in MiB",
		Value: 256,
	}
	dbTargetFileSizeFlag = &cli.Uint64Flag{
		Name:  "db.target-file-size",
		Usage: "Pebble L0 target SST size in MiB (doubles per level)",
		Value: 8,
	}
	dbLBaseMaxSizeFlag = &cli.Uint64Flag{
		Name:  "db.lbase-max-size",
		Usage: "Pebble dynamic base-level maximum size in MiB",
		Value: 1024,
	}
	dbL0CompactionFlag = &cli.IntFlag{
		Name:  "db.l0.compact",
		Usage: "Pebble L0 compaction threshold",
		Value: 8,
	}
	dbL0StopFlag = &cli.IntFlag{
		Name:  "db.l0.stop",
		Usage: "Pebble L0 stop-writes threshold",
		Value: 64,
	}
	freezerDisableFlag = &cli.BoolFlag{
		Name:  "freezer.disable",
		Usage: "Disable background freezing; existing ancient data remains readable",
	}
	freezerIntervalFlag = &cli.DurationFlag{
		Name:  "freezer.interval",
		Usage: "Interval between chain freezer passes",
		Value: defaultFreezerInterval(),
	}
	freezerMarginFlag = &cli.Uint64Flag{
		Name:  "freezer.margin",
		Usage: "Blocks to keep hot below the solidified line",
		Value: defaultFreezerMargin(),
	}
	freezerBatchFlag = &cli.Uint64Flag{
		Name:  "freezer.batch",
		Usage: "Maximum blocks frozen per freezer pass",
		Value: defaultFreezerBatch(),
	}
	freezerTxIndexDisableFlag = &cli.BoolFlag{
		Name:  "freezer.tx-index.disable",
		Usage: "Disable automatic archival of V2-covered transaction indexes",
	}
	freezerDirectV2DisableFlag = &cli.BoolFlag{
		Name:  "freezer.direct-v2.disable",
		Usage: "Disable direct Ancient V2 publication; an existing direct-only layout pauses instead of falling back to V1",
	}
	syncRestartFromFlag = &cli.Uint64Flag{
		Name:  "sync.restart-from",
		Usage: "Before starting P2P sync, rebuild local state to this canonical historical block height and continue syncing from height+1",
	}
	syncImportBatchFlag = &cli.IntFlag{
		Name:    "sync.import-batch",
		Usage:   "Maximum staged block bodies imported per local sync pass (1-1024; wire fetch batch stays 100)",
		Value:   tsync.MaxImportBatch,
		EnvVars: []string{"GTRON_SYNC_IMPORT_BATCH"},
	}
	syncETLTempDirFlag = &cli.StringFlag{
		Name:    "sync.etl.tempdir",
		Usage:   "Parent directory for sorted TxLookup ETL run files during bulk sync",
		EnvVars: []string{"GTRON_SYNC_ETL_TEMPDIR"},
	}
	syncETLBufferMiBFlag = &cli.Uint64Flag{
		Name:    "sync.etl.buffer",
		Usage:   "TxLookup ETL memory buffer in MiB during bulk sync (0 = default)",
		EnvVars: []string{"GTRON_SYNC_ETL_BUFFER"},
	}
	syncETLBatchMiBFlag = &cli.Uint64Flag{
		Name:    "sync.etl.batch",
		Usage:   "TxLookup ETL output batch size in MiB during bulk sync (0 = default)",
		EnvVars: []string{"GTRON_SYNC_ETL_BATCH"},
	}
	syncAsyncCommitFlag = &cli.BoolFlag{
		Name:  "sync.async-commit",
		Usage: "Pipeline staged-sync state commits",
	}
	execParallelTransfersFlag = &cli.BoolFlag{
		Name:    "exec.parallel-transfers",
		Usage:   "Pre-execute plain transfers, validate typed read versions, publish in block order, and replay conflicts serially",
		EnvVars: []string{"GTRON_EXEC_PARALLEL_TRANSFERS"},
	}
	execParallelVMFlag = &cli.BoolFlag{
		Name:    "exec.parallel-vm",
		Usage:   "Enable sparse speculative VM publication with canonical-boundary serial verification and serial fallback",
		EnvVars: []string{"GTRON_EXEC_PARALLEL_VM"},
	}
	syncStopAtFlag = &cli.Uint64Flag{
		Name:  "sync.stop-at",
		Usage: "Pause block sync after importing this height (inclusive), for database parity audits",
	}
)

var app = &cli.App{
	Name:    "gtron",
	Usage:   "TRON blockchain node (Go implementation)",
	Version: gtronVersion,
	Flags: []cli.Flag{
		dataDirFlag,
		p2pPortFlag,
		discoverPortFlag,
		externalIPFlag,
		httpPortFlag,
		jsonrpcPortFlag,
		grpcPortFlag,
		pprofPortFlag,
		pprofAddrFlag,
		metricsEnabledFlag,
		metricsAddrFlag,
		metricsPortFlag,
		testnetFlag,
		witnessFlag,
		witnessKeyFlag,
		witnessKeysFileFlag,
		devFlag,
		devFullFeaturesFlag,
		devMaintenanceIntervalFlag,
		genesisFileFlag,
		seednodeFlag,
		maxpeersFlag,
		verbosityFlag,
		logFormatFlag,
		logFileFlag,
		logFileFormatFlag,
		logFileVerbosityFlag,
		logFileMaxSizeFlag,
		logFileMaxBackupsFlag,
		logFileMaxAgeFlag,
		logFileCompressFlag,
		logModuleFlag,
		pruneModeFlag,
		historyEnabledFlag,
		snapshotBootstrapFlag,
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
		snapshotEventLogVersionFlag,
		snapshotCatalogSigningKeyFileRuntimeFlag,
		snapshotServeFlag,
		snapshotServeAddrFlag,
		snapshotServePortFlag,
		snapshotCatalogRetainFlag,
		snapshotCatalogGraceFlag,
		stateTrieCacheFlag,
		stateCodeCacheFlag,
		stateCommitmentCacheFlag,
		configFileFlag,
		dbCacheFlag,
		dbHandlesFlag,
		dbMemtableFlag,
		dbTargetFileSizeFlag,
		dbLBaseMaxSizeFlag,
		dbL0CompactionFlag,
		dbL0StopFlag,
		freezerDisableFlag,
		freezerIntervalFlag,
		freezerMarginFlag,
		freezerBatchFlag,
		freezerTxIndexDisableFlag,
		freezerDirectV2DisableFlag,
		syncRestartFromFlag,
		syncImportBatchFlag,
		syncETLTempDirFlag,
		syncETLBufferMiBFlag,
		syncETLBatchMiBFlag,
		syncAsyncCommitFlag,
		execParallelTransfersFlag,
		execParallelVMFlag,
		syncStopAtFlag,
	},
	Before: func(ctx *cli.Context) error {
		if err := log.SetupWithOptions(log.SetupOptions{
			Verbosity:      ctx.Int(verbosityFlag.Name),
			Format:         ctx.String(logFormatFlag.Name),
			File:           ctx.String(logFileFlag.Name),
			FileFormat:     ctx.String(logFileFormatFlag.Name),
			Modules:        ctx.StringSlice(logModuleFlag.Name),
			FileVerbosity:  ctx.Int(logFileVerbosityFlag.Name),
			FileMaxSizeMB:  ctx.Int(logFileMaxSizeFlag.Name),
			FileMaxBackups: ctx.Int(logFileMaxBackupsFlag.Name),
			FileMaxAgeDays: ctx.Int(logFileMaxAgeFlag.Name),
			FileCompress:   ctx.Bool(logFileCompressFlag.Name),
		}); err != nil {
			return err
		}
		log.Info("Starting gtron",
			"version", gtronVersion,
			"command", log.RedactArgs(os.Args,
				"witness.key",
				"witness.keys-file",
				"snapshot.signing-key",
				"snapshot.signing-key-file",
				"snapshot.catalog-signing-key-file",
				"snapshot.url"))
		if ctx.Bool(metricsEnabledFlag.Name) {
			enableMetrics()
		}
		return nil
	},
	After: func(_ *cli.Context) error {
		return log.Close()
	},
	Action: gtron,
	Commands: []*cli.Command{
		{
			Name:   "version",
			Usage:  "Print version information",
			Action: versionCmd,
		},
		{
			Name:  "init",
			Usage: "Initialize genesis block",
			Flags: []cli.Flag{
				dataDirFlag,
				snapshotDirFlag,
				testnetFlag,
				genesisFileFlag,
				dbCacheFlag,
				dbHandlesFlag,
				dbMemtableFlag,
				dbTargetFileSizeFlag,
				dbLBaseMaxSizeFlag,
				dbL0CompactionFlag,
				dbL0StopFlag,
			},
			Action: initCmd,
		},
		dbCommand(),
		snapshotCommand(),
	},
}

func initCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}
	dbPath := chainDataDir(cfg.DataDir)

	db, err := openPebbleDB(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	ancientReader := rawdb.AncientReader(rawdb.NoopAncient{})
	ancientPath := ancientDataDir(cfg.DataDir)
	if info, err := os.Stat(ancientPath); err == nil && info.IsDir() {
		fz, err := rawdbfreezer.NewFreezer(ancientPath, "", false, freezerTableSize, chainfreezer.FreezerTableSet())
		if err != nil {
			return fmt.Errorf("open freezer: %w", err)
		}
		defer fz.Close()
		ancientReader = rawdb.NewFreezerReader(fz)
	}
	stateSnapshotManager, err := statesnapshots.OpenManager(snapshotDir(ctx, cfg.DataDir))
	if err != nil {
		return fmt.Errorf("open state snapshots: %w", err)
	}
	ancientReader = rawdb.NewFallbackAncientReader(ancientReader, stateSnapshotManager)

	config, hash, err := core.SetupGenesisBlockWithAncient(db, ancientReader, genesis)
	if err != nil {
		return fmt.Errorf("setup genesis: %w", err)
	}
	fmt.Printf("Genesis initialized: chain=%d hash=%x\n", config.ChainID, hash)
	return nil
}

func gtron(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	if err := validateMetricsConfig(cfg); err != nil {
		return err
	}
	if cfg.MetricsEnabled {
		enableMetrics()
	}
	snapshotETL, err := snapshotETLOptions(ctx)
	if err != nil {
		return err
	}
	snapshotETL = applyRuntimeSnapshotETLDefaults(snapshotETL)
	if err := validateSyncImportBatch(cfg.SyncImportBatch); err != nil {
		return err
	}
	log.Info("Cold snapshot compression enabled", "history", true, "latest", true)
	dbPath := chainDataDir(cfg.DataDir)

	// In dev mode, parse witness key early so we can build the genesis with it
	var devWitnessKey *ecdsa.PrivateKey
	if ctx.Bool("dev") {
		key, err := parseWitnessKey(ctx)
		if err != nil {
			return fmt.Errorf("dev mode requires --witness.key: %w", err)
		}
		devWitnessKey = key
	}

	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}
	if ctx.Bool("dev") {
		witnessAddr := crypto.PubkeyToAddress(&devWitnessKey.PublicKey)
		genesis = makeDevGenesis(witnessAddr, ctx.Bool("dev.full-features"), ctx.Int64("dev.maintenance-interval"))
		log.Info("Dev genesis configured", "witness", fmt.Sprintf("%x", witnessAddr[:6]))
	}
	if ctx.String("genesis") != "" {
		log.Info("Custom genesis loaded",
			"chain", genesis.Config.ChainID,
			"p2pVersion", genesis.Config.P2PVersion,
			"witnesses", len(genesis.Witnesses),
			"accounts", len(genesis.Accounts))
	}
	if ctx.Bool("snapshot.bootstrap") {
		log.Info("Bootstrapping verified remote snapshot before node startup")
		if err := bootstrapRuntimeSnapshot(ctx); err != nil {
			return err
		}
		log.Info("Verified remote snapshot bootstrap completed")
	}

	// Open database
	db, err := openPebbleDB(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	var ancientStore *rawdbfreezer.Freezer
	var storesCloseOnce sync.Once
	closeStores := func() {
		storesCloseOnce.Do(func() {
			if ancientStore != nil {
				if err := closeRuntimeStore("ancient database", ancientStore.Close); err != nil {
					log.Error("Ancient database close failed", "err", err)
				}
				ancientStore = nil
			}
			if err := closeRuntimeStore("chaindata", db.Close); err != nil {
				log.Error("Chaindata close failed", "err", err)
			}
		})
	}

	freezerCfg := makeFreezerConfig(ctx)
	ancientReader := rawdb.AncientReader(rawdb.NoopAncient{})
	ancientPath := ancientDataDir(cfg.DataDir)
	if shouldOpenFreezer(ancientPath, freezerCfg) {
		ancientStore, err = rawdbfreezer.NewFreezer(ancientPath, "", false, freezerTableSize, chainfreezer.FreezerTableSet())
		if err != nil {
			closeStores()
			return fmt.Errorf("open freezer: %w", err)
		}
		ancientReader = rawdb.NewFreezerReader(ancientStore)
	}
	stateSnapshotDir := snapshotDir(ctx, cfg.DataDir)
	eventLogVersion, err := snapshotEventLogBuildVersion(ctx)
	if err != nil {
		closeStores()
		return err
	}
	log.Info("Event-log snapshot writer configured", "version", eventLogVersion)
	if snapshotETL.TempDir == "" {
		stats, cleanupErr := rawdbetl.CleanupStaleCollectors(filepath.Join(stateSnapshotDir, "etl"))
		if cleanupErr != nil {
			log.Warn("Failed to clean stale snapshot ETL scratch", "dir", filepath.Join(stateSnapshotDir, "etl"), "err", cleanupErr)
		} else if stats.Directories != 0 {
			log.Info("Cleaned stale snapshot ETL scratch",
				"dir", filepath.Join(stateSnapshotDir, "etl"),
				"directories", stats.Directories,
				"files", stats.Files,
				"bytes", stats.Bytes)
		}
	}
	if stats, cleanupErr := statesnapshots.CleanupStaleTempFiles(stateSnapshotDir, statesnapshots.DefaultStaleTempFileAge); cleanupErr != nil {
		log.Warn("Failed to clean stale snapshot temporary files", "dir", stateSnapshotDir, "err", cleanupErr)
	} else if stats.Files != 0 {
		log.Info("Cleaned stale snapshot temporary files",
			"dir", stateSnapshotDir,
			"files", stats.Files,
			"bytes", stats.Bytes,
			"minimumAge", statesnapshots.DefaultStaleTempFileAge)
	}
	// One process-wide verification cache connects the trusted handoff from
	// cold-history/freezer builders to the live snapshot reader. Keeping the
	// manager on a private cache would force it to replay freshly built event-log
	// segments before the direct V2 freezer could consume their coverage.
	chainFreezerVerificationCache := statesnapshots.NewChainFreezerVerificationCache(stateSnapshotDir)
	if err := chainFreezerVerificationCache.LoadError(); err != nil {
		log.Warn("Chain-freezer verification cache ignored; full verification will rebuild it", "err", err)
	}
	stateSnapshotManager, err := statesnapshots.OpenManagerWithChainVerificationCache(stateSnapshotDir, chainFreezerVerificationCache)
	if err != nil {
		closeStores()
		return fmt.Errorf("open state snapshots: %w", err)
	}
	ancientReader = rawdb.NewFallbackAncientReader(ancientReader, stateSnapshotManager)

	// Setup genesis (idempotent)
	chainConfig, genesisHash, err := core.SetupGenesisBlockWithAncient(db, ancientReader, genesis)
	if err != nil {
		closeStores()
		return fmt.Errorf("setup genesis: %w", err)
	}

	// Apply operator-supplied flat temporal-state retention settings
	// (--prune.mode / [history] in --config). Done after SetupGenesisBlock
	// because it returns a pointer into genesis.Config we now mutate.
	// HistoryMode is operator-level (not consensus-relevant) so this
	// mutation is safe.
	if err := applyHistoryConfig(ctx, chainConfig); err != nil {
		closeStores()
		return err
	}
	if err := ensureHistoryPruneModeLocked(db, chainConfig.EffectiveHistoryMode()); err != nil {
		closeStores()
		return err
	}
	// Snap mode continuously publishes authenticated event-log segments ahead
	// of the direct V2 freezer. Once a complete freezer segment is covered, its
	// receipt rows can omit duplicate logs and reconstruct them from the cold
	// sidecar. Other modes retain self-contained receipts because they do not
	// guarantee that lifecycle.
	freezerCfg.ExternalizeV2ReceiptLogs = chainConfig.EffectiveHistoryMode() == params.HistoryModeSnap && chainConfig.HistoryEnabled
	if ancientStore != nil {
		if err := validateAncientV2PruneMode(chainConfig, ancientStore.V2Coverage()); err != nil {
			closeStores()
			return err
		}
		if chainConfig.EffectiveHistoryMode() == params.HistoryModeMinimal {
			freezerCfg.V2Enabled = false
		}
	}
	snapshotCatalogSigningKey, snapshotCatalogSigningEnabled, err := runtimeSnapshotCatalogSigningKey(ctx)
	if err != nil {
		closeStores()
		return err
	}
	snapshotCatalogHistoryMode := chainConfig.EffectiveHistoryMode()
	if snapshotCatalogSigningEnabled && (!chainConfig.HistoryEnabled ||
		(snapshotCatalogHistoryMode != params.HistoryModeSnap && snapshotCatalogHistoryMode != params.HistoryModeArchive)) {
		closeStores()
		return errors.New("--snapshot.catalog-signing-key-file requires snap or archive history mode with history capture enabled")
	}
	snapshotServeEnabled := ctx.Bool("snapshot.serve")
	if snapshotServeEnabled && !snapshotCatalogSigningEnabled {
		closeStores()
		return errors.New("--snapshot.serve requires --snapshot.catalog-signing-key-file so every hosted generation is signed")
	}
	if snapshotServeEnabled {
		port := ctx.Int("snapshot.serve.port")
		if port <= 0 || port > 65535 {
			closeStores()
			return fmt.Errorf("--snapshot.serve.port must be between 1 and 65535, got %d", port)
		}
	}
	snapshotCatalogRetain := ctx.Int("snapshot.catalog-retain")
	if snapshotCatalogRetain <= 0 {
		closeStores()
		return fmt.Errorf("--snapshot.catalog-retain must be positive, got %d", snapshotCatalogRetain)
	}
	snapshotCatalogGrace := ctx.Duration("snapshot.catalog-grace")
	if snapshotCatalogGrace <= 0 {
		closeStores()
		return fmt.Errorf("--snapshot.catalog-grace must be positive, got %s", snapshotCatalogGrace)
	}
	var snapshotCatalogChain *statesnapshots.ChainIdentity
	if snapshotCatalogSigningEnabled {
		identity := snapshotExpectedChainIdentity(chainConfig, genesis, genesisHash, "")
		snapshotCatalogChain = &identity
	}
	log.Info("Latest-domain state storage enabled", "mode", "latest")

	// Create blockchain
	stateDBConfig, err := makeStateDatabaseConfig(ctx)
	if err != nil {
		closeStores()
		return err
	}
	if stateDBConfig.CleanTrieCacheSizeBytes > 0 {
		log.Info("State trie node cache enabled", "cacheMiB", stateDBConfig.CleanTrieCacheSizeBytes/(1024*1024))
	} else {
		log.Info("State trie node cache disabled")
	}
	if stateDBConfig.CodeCacheSizeBytes > 0 {
		log.Info("Immutable contract code cache enabled", "cacheMiB", stateDBConfig.CodeCacheSizeBytes/(1024*1024))
	} else {
		log.Info("Immutable contract code cache disabled")
	}
	commitmentCacheMiB := ctx.Int("state.commitment.cache")
	if commitmentCacheMiB < 0 {
		closeStores()
		return fmt.Errorf("--state.commitment.cache must be >= 0")
	}
	sdb := state.NewDatabaseWithConfig(rawdb.WrapKeyValueStore(db), stateDBConfig)
	bc, err := core.NewBlockChainWithAncient(db, sdb, chainConfig, ancientReader)
	if err != nil {
		closeStores()
		return fmt.Errorf("create blockchain: %w", err)
	}
	bc.SetCommitmentBranchCacheSize(commitmentCacheMiB * 1024 * 1024)
	if ctx.Bool(execParallelTransfersFlag.Name) {
		bc.SetParallelTransferExecution(true)
		log.Info("Parallel Transfer execution enabled", "workers", 4)
	}
	parallelVMEnabled := ctx.Bool(execParallelVMFlag.Name)
	bc.SetParallelVMExecution(parallelVMEnabled)
	log.Info("Speculative VM publication configured", "enabled", parallelVMEnabled, "workers", 4, "serialOracle", true)
	if commitmentCacheMiB > 0 {
		log.Info("Commitment and flat-latest base-read cache enabled", "cacheMiB", commitmentCacheMiB)
	} else {
		log.Info("Commitment branch base-read cache disabled")
	}
	// Async/pipelined commit is OFF by default and deliberately not a
	// chain-config / proposal value (it changes only the internal commit
	// schedule, never any wire-observable byte). The explicit CLI flag keeps
	// this operational choice visible in the service definition.
	if shouldEnableAsyncCommit(ctx) {
		bc.SetAsyncCommit(true)
		// Every depth amortizes the shared staged-sync session across local import
		// batches. Depth > 2 additionally buffers the commit queue; depth 2 keeps
		// the conservative rendezvous worker behavior.
		log.Info("Async commit enabled",
			"depth", bc.PipelinedCommitDepth())
	}
	if ctx.IsSet("sync.restart-from") {
		target := ctx.Uint64("sync.restart-from")
		lastProgress := uint64(0)
		log.Info("Historical sync restart requested", "target", target, "currentHead", bc.CurrentBlock().Number())
		if err := bc.RestartSyncFromHeight(target, genesis, ancientStore, func(p core.RestartSyncProgress) {
			switch p.Phase {
			case "replay":
				if p.Block == p.Target || p.Block-lastProgress >= 10000 {
					lastProgress = p.Block
					log.Info("Historical sync restart replaying", "block", p.Block, "target", p.Target)
				}
			default:
				log.Info("Historical sync restart phase", "phase", p.Phase, "block", p.Block, "target", p.Target)
			}
		}); err != nil {
			// A failed --sync.restart-from leaves a partial materialized image.
			// Release the stores without calling bc.Close so no additional
			// partial buffer state is flushed; the operator re-runs the command.
			closeStores()
			return err
		}
		log.Info("Historical sync restart complete", "head", bc.CurrentBlock().Number(), "hash", fmt.Sprintf("%x", bc.CurrentBlock().Hash()))
	}
	// Create transaction pool
	pool := txpool.New()

	// Create DPoS engine and wire it into the chain for header verification
	// in applyBlock (signature recovery, scheduled-witness match, post-fork
	// timestamp alignment). Without SetEngine, applyBlock skips verification —
	// fine for tests but not for production.
	engine := dpos.New(bc)
	bc.SetEngine(engine)

	// Create backend + API server
	backend := core.NewTronBackend(bc, pool)
	bc.SetStateCodeColdHistory(stateSnapshotManager)
	bc.SetStateCommitmentColdHistory(stateSnapshotManager)
	bc.ChainDB().SetChainIndexReader(stateSnapshotManager)
	bc.ChainDB().SetBalanceTraceReader(stateSnapshotManager)
	bc.ChainDB().SetSectionBloomReader(stateSnapshotManager)
	bc.ChainDB().SetEventLogReader(stateSnapshotManager)
	backend.SetStateColdHistory(stateSnapshotManager)
	if manifest := stateSnapshotManager.Manifest(); manifest != nil {
		log.Info("State snapshots loaded",
			"dir", stateSnapshotDir,
			"visibleTxStart", manifest.VisibleTxStart,
			"visibleTxEnd", manifest.VisibleTxEnd,
			"segments", len(manifest.Segments))
	}
	apiServer := tronapi.NewServer(backend, cfg.HTTPPort)
	jrpcServer := jsonrpc.NewServer(backend, cfg.JSONRPCPort)
	grpcServer := grpcapi.NewServer(backend, fmt.Sprintf(":%d", cfg.GRPCPort))

	// Create P2P layer
	broadcaster := tnet.NewBroadcastService(nil)
	handler := tnet.NewTronHandler(bc, pool, broadcaster)
	syncService := tnet.NewSyncService(bc, handler)
	if ctx.IsSet("sync.stop-at") {
		stopHeight := ctx.Uint64("sync.stop-at")
		if bc.CurrentBlock().Number() > stopHeight {
			closeStores()
			return fmt.Errorf("--sync.stop-at %d is below current head %d", stopHeight, bc.CurrentBlock().Number())
		}
		syncService.SetStopAtHeight(stopHeight)
		log.Info("Sync stop height configured", "height", stopHeight)
	}
	handler.SetSyncService(syncService)

	nodeID, err := node.LoadOrCreateNodeID(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("load node ID: %w", err)
	}

	networkID := resolveNetworkID(genesis)

	externalIP := cfg.ExternalIP
	if externalIP == "" {
		externalIP = "127.0.0.1"
		log.Warn("P2P external IP not configured; advertising loopback",
			"advertised", externalIP,
			"hint", "set --external.ip to a publicly reachable address for inbound peers")
	}

	// Construct Kademlia discovery service. The UDP port mirrors the TCP P2P
	// port unless --discover.port was set explicitly. SetOnNewPeer is patched
	// in below once p2pServer exists.
	discoverPort := cfg.DiscoverPort
	if discoverPort == 0 {
		discoverPort = cfg.P2PPort
	}
	discSvc, err := discover.NewService(
		fmt.Sprintf(":%d", discoverPort), nodeID, networkID, nil,
	)
	if err != nil {
		closeStores()
		return fmt.Errorf("create discovery service: %w", err)
	}
	if externalIP != "" {
		discSvc.SetExternalIP(externalIP)
	}

	// Built-in bootstrap nodes are fed into the discovery routing table so the
	// node can find peers even when no --seednode is set or all of them are
	// rate-limited. Skipped for private chains (--genesis / --dev) where the
	// public bootstrap lists don't apply.
	var bootstrapNodes []string
	switch {
	case ctx.String("genesis") != "" || ctx.Bool("dev"):
		// private/dev chain — leave empty
	case ctx.Bool("testnet"):
		bootstrapNodes = params.NileBootstrapNodes
	default:
		bootstrapNodes = params.MainnetBootstrapNodes
	}

	p2pServer := p2p.NewServer(p2p.ServerConfig{
		ListenAddr:     fmt.Sprintf(":%d", cfg.P2PPort),
		MaxPeers:       cfg.MaxPeers,
		SeedNodes:      cfg.SeedNodes,
		BootstrapNodes: bootstrapNodes,
		Discovery:      discSvc,
		PeerCachePath:  filepath.Join(cfg.DataDir, "p2p-peers"),
		NodeID:         nodeID,
		NetworkID:      networkID,
		ExternalIP:     externalIP,
		Port:           int32(cfg.P2PPort),
	}, handler)
	// onNewPeer fires on every pong, including from already-connected peers.
	// The server suppresses dial-throttle noise while retaining real failures
	// at debug level so low-peer incidents are diagnosable.
	discSvc.SetOnNewPeer(p2pServer.AddDiscoveredPeer)
	handler.SetServer(p2pServer)
	handler.StartKeepAlive()
	syncService.Start()
	broadcaster.Start()
	broadcaster.SetPeersFunc(handler.HandshakedPeers)
	backend.SetTxBroadcaster(broadcaster)
	backend.SetPeerLister(func() []*tronapi.PeerInfo {
		peers := handler.HandshakedPeers()
		result := make([]*tronapi.PeerInfo, 0, len(peers))
		for _, p := range peers {
			host, portStr, err := net.SplitHostPort(p.ID())
			if err != nil {
				continue
			}
			port, _ := strconv.Atoi(portStr)
			result = append(result, &tronapi.PeerInfo{Host: host, Port: port})
		}
		return result
	})
	backend.SetSyncInfoProvider(func() tronapi.SyncInfo {
		status := syncService.Status()
		info := tronapi.SyncInfo{
			Active:                status.Active,
			Paused:                status.Paused,
			PeerCount:             handler.HandshakedPeerCount(),
			SyncPeerCount:         status.SyncPeerCount,
			TargetHead:            status.TargetHead,
			AppliedTip:            status.AppliedTip,
			SessionBlocks:         status.SessionBlocks,
			SessionTransactions:   status.SessionTransactions,
			Remaining:             status.Remaining,
			Inflight:              status.Inflight,
			BufferedBlocks:        status.BufferedBlocks,
			BufferedBytes:         status.BufferedBytes,
			FetchBackpressured:    status.FetchBackpressured,
			RequestedBlocks:       status.RequestedBlocks,
			RetryBlocks:           status.RetryBlocks,
			RetainedDecodedBlocks: status.RetainedDecodedBlocks,
			RetainedDecodedBytes:  status.RetainedDecodedBytes,
			PauseBlock:            status.PauseBlock,
			LastPeerFailure:       status.LastPeerFailure,
		}
		if !status.PauseTime.IsZero() {
			info.PauseTime = status.PauseTime.UTC().Format(time.RFC3339Nano)
		}
		if status.PauseError != nil {
			info.PauseError = status.PauseError.Error()
		}
		if !status.LastPeerFailureTime.IsZero() {
			info.LastPeerFailureTime = status.LastPeerFailureTime.UTC().Format(time.RFC3339Nano)
		}
		return info
	})

	// Wire PBFT block hook before node start so commit results are validated
	// when blocks arrive via sync or broadcast.
	pbftDataSync := handler.PbftDataSync()
	bc.AddBlockHook(pbftDataSync.ProcessOnBlock)

	// Create node and register services
	stack, err := node.New(cfg)
	if err != nil {
		closeStores()
		return err
	}
	stack.RegisterLifecycle(p2pServer)
	if snapshotServeEnabled {
		addr := strings.TrimSpace(ctx.String("snapshot.serve.addr"))
		if addr == "" {
			addr = "127.0.0.1"
		}
		stack.RegisterLifecycle(statesnapshots.NewHTTPServer(statesnapshots.HTTPServerConfig{
			Addr: net.JoinHostPort(addr, strconv.Itoa(ctx.Int("snapshot.serve.port"))),
			Dir:  stateSnapshotDir,
		}))
	}
	stack.RegisterLifecycle(apiServer)
	stack.RegisterLifecycle(jrpcServer)
	if cfg.GRPCPort > 0 {
		stack.RegisterLifecycle(grpcServer)
	}
	if cfg.PProfPort > 0 {
		addr := cfg.PProfAddr
		if addr == "" {
			addr = "127.0.0.1"
		}
		stack.RegisterLifecycle(debugapi.NewServer(fmt.Sprintf("%s:%d", addr, cfg.PProfPort)))
	}
	if cfg.MetricsEnabled {
		stack.RegisterLifecycle(metricsapi.NewNodeCollector(metricsapi.NodeSources{
			HeadBlock: func() uint64 {
				return bc.CurrentBlock().Number()
			},
			SolidifiedBlock: func() int64 {
				return bc.DynProps().LatestSolidifiedBlockNum()
			},
			ConnectedPeers:      p2pServer.PeerCount,
			HandshakedPeers:     handler.HandshakedPeerCount,
			PendingTransactions: pool.Count,
			Syncing:             syncService.IsSyncing,
			SyncRemainingBlocks: syncService.SyncRemainingBlocks,
		}))
		stack.RegisterLifecycle(metricsapi.NewServer(fmt.Sprintf("%s:%d", cfg.MetricsAddr, cfg.MetricsPort)))
	}
	stack.RegisterLifecycle(handler.PbftHandler())
	stack.RegisterLifecycle(pbftDataSync)

	chainLookupPruneLifecycleWired := false
	sectionBloomPruneLifecycleWired := false
	balanceTracePruneLifecycleWired := false
	retiredPruneLifecycleWired := false
	heavyWorkGate := maintenance.NewHeavyWorkGateWithCooldownAfter(heavyWorkRecoveryCooldown, heavyWorkCooldownMinDuration)
	var domainLifecycle *statepruning.SnapshotLifecycle
	var chainFreezerSnapshotBuild statepruning.ChainFreezerBuildFunc
	if shouldEnableChainFreezerSnapshotBuilder(chainConfig, ancientStore != nil, freezerCfg.Enabled) {
		chainFreezerSnapshotBuild = func() (statesnapshots.ChainFreezerSnapshotPassResult, error) {
			return statesnapshots.BuildChainFreezerSnapshotPass(ancientStore, bc.ChainDB(), statesnapshots.ChainFreezerSnapshotConfig{
				Dir:               stateSnapshotDir,
				BuildEventLogs:    true,
				EventLogVersion:   eventLogVersion,
				HeavyWorkGate:     heavyWorkGate,
				VerificationCache: chainFreezerVerificationCache,
			})
		}
	}
	if shouldEnableDomainStatePruner(chainConfig) {
		prunePolicy := domainStatePrunePolicy(chainConfig, domainStateReorgWindow)
		historyMode := chainConfig.EffectiveHistoryMode()
		coldStateSnapshotsEnabled := (historyMode == params.HistoryModeSnap || historyMode == params.HistoryModeArchive) && chainConfig.HistoryEnabled
		buildDerivedSnapshots := historyMode == params.HistoryModeSnap
		historyDataset := statesnapshots.SegmentDatasetStateDomainChange
		var syncEventLogTargetBlock func() (uint64, bool)
		if buildDerivedSnapshots && freezerCfg.Enabled && freezerCfg.V2Enabled && freezerCfg.DirectV2 &&
			freezerCfg.ExternalizeV2ReceiptLogs && ancientStore != nil && freezerCfg.V2SegmentBlocks > 0 {
			syncEventLogTargetBlock = func() (uint64, bool) {
				coverage := ancientStore.V2Coverage()
				if !ancientStore.CanAppendV2Direct(coverage) ||
					(freezerCfg.V2PromotionAllowed != nil && !freezerCfg.V2PromotionAllowed()) {
					return 0, false
				}
				if coverage > ^uint64(0)-freezerCfg.V2SegmentBlocks {
					return ^uint64(0), true
				}
				return coverage + freezerCfg.V2SegmentBlocks - 1, true
			}
		}
		var chainLookupPrune statepruning.ChainLookupPruneFunc
		var sectionBloomPrune statepruning.SectionBloomPruneFunc
		var balanceTracePrune statepruning.BalanceTracePruneFunc
		if shouldEnableChainLookupPruner(chainConfig) {
			chainLookupPrune = func() (*statesnapshots.PruneHotChainLookupResult, error) {
				manifest, err := statesnapshots.LoadProductionManifest(stateSnapshotDir)
				if err != nil {
					if os.IsNotExist(err) {
						return nil, nil
					}
					return nil, err
				}
				return statesnapshots.PruneHotChainLookupsWithProgress(rawdb.NewChainDB(db, ancientReader), stateSnapshotDir, manifest)
			}
			sectionBloomPrune = func() (*statesnapshots.PruneHotSectionBloomResult, error) {
				manifest, err := statesnapshots.LoadProductionManifest(stateSnapshotDir)
				if err != nil {
					if os.IsNotExist(err) {
						return nil, nil
					}
					return nil, err
				}
				return statesnapshots.PruneHotSectionBloomsWithProgress(rawdb.NewChainDB(db, ancientReader), stateSnapshotDir, manifest)
			}
			balanceTracePrune = func() (*statesnapshots.PruneHotBalanceTraceResult, error) {
				manifest, err := statesnapshots.LoadProductionManifest(stateSnapshotDir)
				if err != nil {
					if os.IsNotExist(err) {
						return nil, nil
					}
					return nil, err
				}
				return statesnapshots.PruneHotBalanceTracesWithProgress(db, stateSnapshotDir, manifest)
			}
		}
		stateChangeIndexPruner := statepruning.StateChangeIndexPruner{
			DB:            db,
			SnapshotDir:   stateSnapshotDir,
			HeavyWorkGate: heavyWorkGate,
		}
		domainLifecycle = statepruning.NewSnapshotLifecycle(newDomainPrunerChainSource(bc, syncService), statepruning.SnapshotLifecycleConfig{
			Snapshot: statesnapshots.Config{
				Dir:                         stateSnapshotDir,
				Enabled:                     coldStateSnapshotsEnabled,
				HistoryDataset:              historyDataset,
				HistoryWindow:               prunePolicy.HistoryWindow,
				ETL:                         snapshotETL,
				CatchupBuildMinInterval:     snapshotCatchupBuildInterval,
				CatchupUnthrottledLagBlocks: prunePolicy.HistoryWindow,
				CatchupHeavyWorkCooldown:    snapshotCatchupHeavyWorkCooldown,
				HeavyWorkGate:               heavyWorkGate,
				BuildSectionBlooms:          buildDerivedSnapshots,
				BuildBalanceTraces:          buildDerivedSnapshots,
				BuildEventLogs:              buildDerivedSnapshots,
				EventLogVersion:             eventLogVersion,
				ColdChainVerificationCache:  chainFreezerVerificationCache,
				CatalogSigningKey:           snapshotCatalogSigningKey,
				CatalogChain:                snapshotCatalogChain,
				// LatestBuildBlocks controls how often latest-dataset snapshots
				// (accounts, KV, commitment-branch, etc.) are rebuilt; all latest
				// datasets share this single coarse cadence. Operators may tune it.
				LatestBuildBlocks:                     statesnapshots.DefaultLatestBuildBlocks,
				DeferLatestBuildWhileSyncing:          true,
				BuildCommitmentBranchBaseWhileSyncing: true,
				DeferHistoryBuildWhileSyncing:         shouldDeferColdSnapshotHistoryWhileSyncing(chainConfig),
				MaxDeferredHistoryBlocks:              maxDeferredColdHistoryBlocks(prunePolicy.HistoryWindow),
				DeferDerivedSidecarsWhileSyncing:      buildDerivedSnapshots,
				BuildEventLogsWhileSyncing:            buildDerivedSnapshots && freezerCfg.ExternalizeV2ReceiptLogs,
				SyncEventLogCatchupBlocks:             freezerCfg.V2SegmentBlocks,
				SyncEventLogTargetBlock:               syncEventLogTargetBlock,
			},
			Pruner: statepruning.PrunerConfig{
				Policy:                          prunePolicy,
				SnapshotDir:                     stateSnapshotDir,
				MaxSyncLag:                      domainStatePrunerMaxSyncLag(chainConfig, prunePolicy),
				DeferStateCodePruneWhileSyncing: prunePolicy.Mode == statepruning.ModeSnap,
			},
			ChainFreezerBuild:     chainFreezerSnapshotBuild,
			ChainLookupPrune:      chainLookupPrune,
			SectionBloomPrune:     sectionBloomPrune,
			BalanceTracePrune:     balanceTracePrune,
			StateChangeIndexPrune: stateChangeIndexPruner.OnePass,
			// Retired-file deletion verifies the complete active manifest first.
			// Keep that CPU/IO-heavy safety gate off the historical import path;
			// AddSyncCompleteHook below wakes the lifecycle as soon as sync ends.
			DeferRetiredPruneWhileSyncing: true,
			RetiredPrune: func(ctx context.Context, verifyActive statesnapshots.ActiveManifestVerifier) (*statesnapshots.PruneRetiredSegmentFilesResult, error) {
				if _, err := statesnapshots.LoadProductionManifest(stateSnapshotDir); err != nil {
					if os.IsNotExist(err) {
						return nil, nil
					}
					return nil, err
				}
				if _, err := statesnapshots.PrunePublishedSnapshotManifests(stateSnapshotDir, snapshotCatalogRetain, snapshotCatalogGrace); err != nil {
					return nil, err
				}
				return statesnapshots.PruneRetiredSegmentFilesContextWithVerifier(ctx, stateSnapshotDir, verifyActive)
			},
		})
		stack.RegisterLifecycle(domainLifecycle)
		syncService.AddSyncCompleteHook(domainLifecycle.RequestPass)
		chainLookupPruneLifecycleWired = chainLookupPrune != nil
		sectionBloomPruneLifecycleWired = sectionBloomPrune != nil
		balanceTracePruneLifecycleWired = balanceTracePrune != nil
		retiredPruneLifecycleWired = true
		log.Info("Domain state snapshot/prune lifecycle enabled",
			"mode", prunePolicy.Mode,
			"snapshotEnabled", coldStateSnapshotsEnabled,
			"chainFreezerBuild", chainFreezerSnapshotBuild != nil,
			"chainLookupPrune", chainLookupPrune != nil,
			"sectionBloomPrune", sectionBloomPrune != nil,
			"balanceTracePrune", balanceTracePrune != nil,
			"stateChangeIndexPrune", true,
			"retiredPrune", true,
			"dataset", historyDataset,
			"historyWindow", prunePolicy.HistoryWindow,
			"reorgWindow", prunePolicy.ReorgWindow,
			"etlTempDir", snapshotETL.TempDir,
			"etlBufferBytes", snapshotETL.BufferLimit,
			"etlBatchBytes", snapshotETL.BatchSize,
			"catchupBuildMinInterval", snapshotCatchupBuildInterval,
			"catchupUnthrottledLagBlocks", prunePolicy.HistoryWindow,
			"catchupHeavyWorkCooldown", snapshotCatchupHeavyWorkCooldown,
			"deferHistoryBuildWhileSyncing", shouldDeferColdSnapshotHistoryWhileSyncing(chainConfig),
			"maxDeferredHistoryBlocks", maxDeferredColdHistoryBlocks(prunePolicy.HistoryWindow),
			"deferDerivedSidecarsWhileSyncing", buildDerivedSnapshots,
			"buildEventLogsWhileSyncing", buildDerivedSnapshots && freezerCfg.ExternalizeV2ReceiptLogs,
			"syncEventLogCatchupBlocks", freezerCfg.V2SegmentBlocks,
			"syncEventLogFreezerHandoff", syncEventLogTargetBlock != nil,
			"maxPruneSyncLag", domainStatePrunerMaxSyncLag(chainConfig, prunePolicy),
			"deferStateCodePruneWhileSyncing", prunePolicy.Mode == statepruning.ModeSnap,
			"heavyWorkRecoveryCooldown", heavyWorkRecoveryCooldown,
			"heavyWorkCooldownMinDuration", heavyWorkCooldownMinDuration,
			"sharedHeavyWorkGate", true,
			"catalogRetain", snapshotCatalogRetain,
			"catalogGrace", snapshotCatalogGrace,
			"snapshotDir", stateSnapshotDir)
	} else {
		log.Info("Domain state pruning disabled", "mode", chainConfig.EffectiveHistoryMode())
	}
	if !chainLookupPruneLifecycleWired && shouldEnableChainLookupPruner(chainConfig) {
		stack.RegisterLifecycle(statesnapshots.NewChainLookupPruneLifecycle(rawdb.NewChainDB(db, ancientReader), statesnapshots.ChainLookupPruneLifecycleConfig{
			Dir: stateSnapshotDir,
		}))
		log.Info("Chain lookup prune lifecycle enabled",
			"mode", chainConfig.EffectiveHistoryMode(),
			"snapshotDir", stateSnapshotDir)
	}
	if !sectionBloomPruneLifecycleWired && shouldEnableChainLookupPruner(chainConfig) {
		stack.RegisterLifecycle(statesnapshots.NewSectionBloomPruneLifecycle(rawdb.NewChainDB(db, ancientReader), statesnapshots.SectionBloomPruneLifecycleConfig{
			Dir: stateSnapshotDir,
		}))
		log.Info("Section bloom prune lifecycle enabled",
			"mode", chainConfig.EffectiveHistoryMode(),
			"snapshotDir", stateSnapshotDir)
	}
	if !balanceTracePruneLifecycleWired && shouldEnableChainLookupPruner(chainConfig) {
		stack.RegisterLifecycle(statesnapshots.NewBalanceTracePruneLifecycle(db, statesnapshots.BalanceTracePruneLifecycleConfig{
			Dir: stateSnapshotDir,
		}))
		log.Info("Balance trace prune lifecycle enabled",
			"mode", chainConfig.EffectiveHistoryMode(),
			"snapshotDir", stateSnapshotDir)
	}
	if !retiredPruneLifecycleWired && shouldEnableChainLookupPruner(chainConfig) {
		stack.RegisterLifecycle(statesnapshots.NewRetiredPruneLifecycle(statesnapshots.RetiredPruneLifecycleConfig{
			Dir:             stateSnapshotDir,
			PublishedRetain: snapshotCatalogRetain,
			PublishedGrace:  snapshotCatalogGrace,
		}))
		log.Info("Retired snapshot segment prune lifecycle enabled",
			"mode", chainConfig.EffectiveHistoryMode(),
			"snapshotDir", stateSnapshotDir)
	}
	if ancientStore != nil && shouldEnableChainFreezerTailPruner(chainConfig) {
		retainBlocks := statesnapshots.EffectiveChainFreezerTailRetainBlocks(chainConfig.EffectiveHistoryPruneWindow())
		chainFreezerTailLifecycle := statesnapshots.NewChainFreezerTailPruneLifecycle(bc.ChainDB(), ancientStore, stateSnapshotManager, statesnapshots.ChainFreezerTailPruneLifecycleConfig{
			RetainBlocks:  retainBlocks,
			HeavyWorkGate: heavyWorkGate,
			HeadBlock: func() uint64 {
				if head := bc.CurrentBlock(); head != nil {
					return head.Number()
				}
				return 0
			},
		})
		stack.RegisterLifecycle(chainFreezerTailLifecycle)
		if domainLifecycle != nil {
			domainLifecycle.AddPassCompleteHook(chainFreezerTailLifecycle.RequestPass)
		}
		log.Info("Chain freezer tail prune lifecycle enabled",
			"mode", chainConfig.EffectiveHistoryMode(),
			"retainBlocks", retainBlocks,
			"snapshotDir", stateSnapshotDir)
	}

	var freezerRunner *chainfreezer.Runner
	if ancientStore != nil && freezerCfg.Enabled {
		freezerCfg.SyncActive = syncService.IsSyncing
		freezerCfg.HeavyWorkGate = heavyWorkGate
		freezerRunner = chainfreezer.New(newFreezerChainSource(bc), newFreezerStore(ancientStore), freezerCfg)
		if freezerRunner != nil {
			syncService.AddSyncCompleteHook(freezerRunner.RequestPass)
			if domainLifecycle != nil {
				freezerRunner.AddChainFreezerAdvanceHook(domainLifecycle.RequestPass)
			}
			stack.RegisterLifecycle(freezerRunner)
			log.Info("Chain freezer enabled",
				"ancient", ancientPath,
				"margin", freezerCfg.MarginBlocks,
				"batch", freezerCfg.BatchBlocks,
				"interval", freezerCfg.Interval,
				"v2", freezerCfg.V2Enabled,
				"receiptLogsExternal", freezerCfg.ExternalizeV2ReceiptLogs,
				"txIndex", freezerCfg.TransactionIndexEnabled,
				"txIndexPrefixBits", freezerCfg.TransactionIndexPrefixBits)
		}
	} else if ancientStore != nil {
		log.Info("Chain freezer disabled; existing ancient data readable", "ancient", ancientPath)
	}

	// Start block producer only when --witness is explicitly set.
	// A node can join a dev chain with --dev --witness.key (for genesis) without
	// producing blocks by omitting --witness.
	if ctx.Bool("witness") {
		var key *ecdsa.PrivateKey
		if devWitnessKey != nil {
			key = devWitnessKey
		} else {
			var err error
			key, err = parseWitnessKey(ctx)
			if err != nil {
				closeStores()
				return fmt.Errorf("witness key: %w", err)
			}
		}
		witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
		// Verify witness is in active list
		activeWitnesses := bc.ActiveWitnesses()
		found := false
		for _, aw := range activeWitnesses {
			if aw == witnessAddr {
				found = true
				break
			}
		}
		if !found {
			log.Warn("Witness key is not in the active witness set; block production will not start",
				"witness", fmt.Sprintf("%x", witnessAddr[:6]),
				"activeWitnesses", len(activeWitnesses),
				"hint", "use --dev mode to create a single-witness dev chain with this key")
		} else {
			log.Info("Witness mode enabled", "witness", fmt.Sprintf("%x", witnessAddr[:6]))
		}
		prod := producer.New(bc, pool, engine, key)
		prod.BlockCallback = func(block *types.Block) {
			broadcaster.BroadcastBlock(block)
		}
		stack.RegisterLifecycle(prod)

		// M6b slice 2: wire the SR-side PBFT producer. The producer:
		//   - emits a BLOCK PREPREPARE on every successful InsertBlock
		//   - emits an SRL PREPREPARE on every maintenance boundary
		//   - emits PREPARE / COMMIT in response to inbound state-machine
		//     transitions (via PbftHandler.SetProducer)
		// Multi-SR keys are loaded from --witness.keys-file when set; the
		// primary --witness.key is also included.
		srKeys := []*ecdsa.PrivateKey{key}
		if path := ctx.String("witness.keys-file"); path != "" {
			extra, err := parseWitnessKeysFile(path)
			if err != nil {
				closeStores()
				return fmt.Errorf("witness keys file: %w", err)
			}
			srKeys = append(srKeys, extra...)
		}
		pbftProducer := tnet.NewPbftProducer(bc, bc.DB(), p2pServer, syncService, srKeys...)
		if pbftProducer != nil {
			handler.PbftHandler().SetProducer(pbftProducer)
			bc.AddBlockHook(pbftProducer.OnBlockApplied)
			bc.AddMaintenanceHook(pbftProducer.OnMaintenance)
		}
	}

	// Start
	if err := stack.Start(); err != nil {
		closeStores()
		return err
	}

	log.Info("gtron started",
		"chain", chainConfig.ChainID,
		"head", bc.CurrentBlock().Number(),
		"http", fmt.Sprintf(":%d", cfg.HTTPPort),
		"jsonrpc", fmt.Sprintf(":%d", cfg.JSONRPCPort),
		"grpc", cfg.GRPCPort,
		"p2p", fmt.Sprintf(":%d", cfg.P2PPort),
		"syncImportBatch", cfg.SyncImportBatch,
		"datadir", cfg.DataDir)

	// Wait for interrupt
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	log.Info("Shutting down")
	// Signal freezer cancellation before draining sync. The lifecycle Stop call
	// below still joins the goroutine before any store is closed, while safe
	// rollback/fsync work overlaps the active sync-import drain.
	if freezerRunner != nil {
		freezerRunner.BeginStop()
	}
	broadcaster.Stop()
	syncService.Stop()
	stack.Stop()
	// Flush the BlockChain's per-block buffer before closing the underlying
	// store so LastBlock, state roots, and latest-domain rows restart from the
	// same head.
	if err := bc.Close(); err != nil {
		log.Error("Blockchain close failed", "err", err)
	}
	closeStores()
	return nil
}

func shouldEnableAsyncCommit(ctx *cli.Context) bool {
	return ctx != nil && ctx.Bool("sync.async-commit")
}

func versionCmd(ctx *cli.Context) error {
	fmt.Printf("gtron version %s\n", ctx.App.Version)
	return nil
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
