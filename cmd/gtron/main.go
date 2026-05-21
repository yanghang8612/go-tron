package main

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/historyprune"
	"github.com/tronprotocol/go-tron/core/producer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/internal/debugapi"
	"github.com/tronprotocol/go-tron/internal/grpcapi"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	tnet "github.com/tronprotocol/go-tron/net"
	"github.com/tronprotocol/go-tron/node"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/tronprotocol/go-tron/p2p/discover"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

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
		Usage: "Optional log file path; records are tee'd to this file in JSON",
	}
	logModuleFlag = &cli.StringSliceFlag{
		Name:  "log.module",
		Usage: "Per-module log level override (module=trace|debug|info|warn|error|crit or 0-5); repeatable, e.g. --log.module net/sync=debug --log.module p2p=warn",
	}
	gcmodeFlag = &cli.StringFlag{
		Name:  "gcmode",
		Usage: "State History Index retention: full (prune to last prune_window blocks) | archive (keep forever)",
		Value: params.HistoryModeFull,
	}
	historyEnabledFlag = &cli.BoolFlag{
		Name:  "history.enabled",
		Usage: "Turn on the State History Index capture path. Required to actually populate (and, in full mode, prune) the index; archive mode implies it.",
	}
	configFileFlag = &cli.StringFlag{
		Name:  "config",
		Usage: "Path to a TOML config file (currently understood: [history] enabled, mode, prune_window)",
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
		Value: 64,
	}
	dbL0CompactionFlag = &cli.IntFlag{
		Name:  "db.l0.compact",
		Usage: "Pebble L0 compaction threshold",
		Value: 4,
	}
	dbL0StopFlag = &cli.IntFlag{
		Name:  "db.l0.stop",
		Usage: "Pebble L0 stop-writes threshold",
		Value: 24,
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
)

var app = &cli.App{
	Name:    "gtron",
	Usage:   "TRON blockchain node (Go implementation)",
	Version: "0.3.0-dev",
	Flags: []cli.Flag{
		dataDirFlag,
		p2pPortFlag,
		discoverPortFlag,
		httpPortFlag,
		jsonrpcPortFlag,
		grpcPortFlag,
		pprofPortFlag,
		pprofAddrFlag,
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
		logModuleFlag,
		gcmodeFlag,
		historyEnabledFlag,
		configFileFlag,
		dbCacheFlag,
		dbHandlesFlag,
		dbMemtableFlag,
		dbL0CompactionFlag,
		dbL0StopFlag,
		freezerDisableFlag,
		freezerIntervalFlag,
		freezerMarginFlag,
		freezerBatchFlag,
	},
	Before: func(ctx *cli.Context) error {
		return log.SetupWithModules(ctx.Int("verbosity"), ctx.String("log.format"), ctx.String("log.file"), ctx.StringSlice("log.module"))
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
				testnetFlag,
				genesisFileFlag,
				dbCacheFlag,
				dbHandlesFlag,
				dbMemtableFlag,
				dbL0CompactionFlag,
				dbL0StopFlag,
			},
			Action: initCmd,
		},
		historyCommand,
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

	config, hash, err := core.SetupGenesisBlockWithAncient(db, ancientReader, genesis)
	if err != nil {
		return fmt.Errorf("setup genesis: %w", err)
	}
	fmt.Printf("Genesis initialized: chain=%d hash=%x\n", config.ChainID, hash)
	return nil
}

func gtron(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
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

	// Open database
	db, err := openPebbleDB(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	var ancientStore *rawdbfreezer.Freezer
	closeStores := func() {
		if ancientStore != nil {
			_ = ancientStore.Close()
			ancientStore = nil
		}
		_ = db.Close()
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

	// Setup genesis (idempotent)
	chainConfig, _, err := core.SetupGenesisBlockWithAncient(db, ancientReader, genesis)
	if err != nil {
		closeStores()
		return fmt.Errorf("setup genesis: %w", err)
	}

	// Apply operator-supplied State History Index retention settings
	// (--gcmode / [history] in --config). Done after SetupGenesisBlock
	// because it returns a pointer into genesis.Config we now mutate.
	// HistoryMode is operator-level (not consensus-relevant) so this
	// mutation is safe.
	if err := applyHistoryConfig(ctx, chainConfig); err != nil {
		closeStores()
		return err
	}

	// Create blockchain
	sdb := state.NewDatabase(rawdb.WrapKeyValueStore(db))
	bc, err := core.NewBlockChainWithAncient(db, sdb, chainConfig, ancientReader)
	if err != nil {
		closeStores()
		return fmt.Errorf("create blockchain: %w", err)
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
	apiServer := tronapi.NewServer(backend, cfg.HTTPPort)
	jrpcServer := jsonrpc.NewServer(backend, cfg.JSONRPCPort)
	grpcServer := grpcapi.NewServer(backend, fmt.Sprintf(":%d", cfg.GRPCPort))

	// Create P2P layer
	broadcaster := tnet.NewBroadcastService(nil)
	handler := tnet.NewTronHandler(bc, pool, broadcaster)
	syncService := tnet.NewSyncService(bc, handler)
	handler.SetSyncService(syncService)

	nodeID, err := node.LoadOrCreateNodeID(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("load node ID: %w", err)
	}

	networkID := resolveNetworkID(genesis)

	externalIP := cfg.ExternalIP
	if externalIP == "" {
		externalIP = "127.0.0.1"
	}

	// Construct Kademlia discovery service. The UDP port mirrors the TCP P2P
	// port unless --discover.port was set explicitly. SetOnNewPeer is patched
	// in below once p2pServer exists; AddPeer is the only callback the server
	// surface exposes for new candidates.
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
		NodeID:         nodeID,
		NetworkID:      networkID,
		ExternalIP:     externalIP,
		Port:           int32(cfg.P2PPort),
	}, handler)
	// onNewPeer fires on every pong, including from already-connected peers;
	// swallow the resulting duplicate/per-IP-cap errors instead of logging.
	discSvc.SetOnNewPeer(func(addr string) {
		_ = p2pServer.AddPeer(addr)
	})
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
	stack.RegisterLifecycle(handler.PbftHandler())
	stack.RegisterLifecycle(pbftDataSync)

	// State History Index pruner: only registered when the chain is in
	// "full" retention mode AND history capture is on. Archive mode
	// skips registration entirely so the index grows linearly (and so
	// the operator can confirm intent via gtron logs at start time).
	if chainConfig.HistoryEnabled && chainConfig.EffectiveHistoryMode() == params.HistoryModeFull {
		pruner := historyprune.New(newPrunerChainSource(bc), historyprune.PrunerConfig{
			Window: chainConfig.EffectiveHistoryPruneWindow(),
		})
		stack.RegisterLifecycle(pruner)
		log.Info("History pruner enabled", "window", chainConfig.EffectiveHistoryPruneWindow())
	} else if chainConfig.HistoryEnabled {
		log.Info("History capture enabled", "mode", params.HistoryModeArchive, "pruning", false)
	}

	if ancientStore != nil && freezerCfg.Enabled {
		freezerRunner := chainfreezer.New(newFreezerChainSource(bc), newFreezerStore(ancientStore), freezerCfg)
		if freezerRunner != nil {
			stack.RegisterLifecycle(freezerRunner)
			log.Info("Chain freezer enabled",
				"ancient", ancientPath,
				"margin", freezerCfg.MarginBlocks,
				"batch", freezerCfg.BatchBlocks,
				"interval", freezerCfg.Interval)
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
		"datadir", cfg.DataDir)

	// Wait for interrupt
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	log.Info("Shutting down")
	broadcaster.Stop()
	syncService.Stop()
	stack.Stop()
	// Flush the BlockChain's per-block buffer up to the solidified line
	// before closing the underlying store. Layers above solidified are
	// dropped — see core.BlockChain.Close for the trade-off rationale.
	if err := bc.Close(); err != nil {
		log.Error("Blockchain close failed", "err", err)
	}
	closeStores()
	return nil
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
