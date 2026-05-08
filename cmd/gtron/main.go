package main

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/producer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
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
	},
	Action: gtron,
	Commands: []*cli.Command{
		{
			Name:   "version",
			Usage:  "Print version information",
			Action: versionCmd,
		},
		{
			Name:   "init",
			Usage:  "Initialize genesis block",
			Flags:  []cli.Flag{dataDirFlag, testnetFlag, genesisFileFlag},
			Action: initCmd,
		},
	},
}

func initCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis, err := makeGenesis(ctx)
	if err != nil {
		return err
	}
	dbPath := chainDataDir(cfg.DataDir)

	db, err := rawdb.NewPebbleDB(dbPath, 256, 500)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	config, hash, err := core.SetupGenesisBlock(db, genesis)
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
		fmt.Printf("Dev mode: single-witness genesis (witness=%x)\n", witnessAddr[:6])
	}
	if ctx.String("genesis") != "" {
		// Custom-chain bootstrap: derive networkId from the file so the libp2p
		// HelloMessage matches the peer (e.g. java-tron private chain runs at
		// p2p.version=0).
		cfg.NetworkID = genesis.Config.P2PVersion
		fmt.Printf("Custom genesis: chain=%d p2p_version=%d witnesses=%d accounts=%d\n",
			genesis.Config.ChainID, genesis.Config.P2PVersion,
			len(genesis.Witnesses), len(genesis.Accounts))
	}

	// Open database
	db, err := rawdb.NewPebbleDB(dbPath, 256, 500)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	// Setup genesis (idempotent)
	chainConfig, _, err := core.SetupGenesisBlock(db, genesis)
	if err != nil {
		db.Close()
		return fmt.Errorf("setup genesis: %w", err)
	}

	// Create blockchain
	sdb := state.NewDatabase(rawdb.WrapKeyValueStore(db))
	bc, err := core.NewBlockChain(db, sdb, chainConfig)
	if err != nil {
		db.Close()
		return fmt.Errorf("create blockchain: %w", err)
	}

	// Create transaction pool
	pool := txpool.New()

	// Create DPoS engine
	engine := dpos.New(bc)

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

	// Resolve network ID: config override > params default. When --genesis
	// is set, treat NetworkID==0 as explicit (some private chains run with
	// java-tron's `p2p.version = 0`); otherwise fall back to mainnet.
	networkID := cfg.NetworkID
	if networkID == 0 && ctx.String("genesis") == "" {
		networkID = params.MainnetNetworkID
	}

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
		db.Close()
		return fmt.Errorf("create discovery service: %w", err)
	}

	p2pServer := p2p.NewServer(p2p.ServerConfig{
		ListenAddr: fmt.Sprintf(":%d", cfg.P2PPort),
		MaxPeers:   cfg.MaxPeers,
		SeedNodes:  cfg.SeedNodes,
		Discovery:  discSvc,
		NodeID:     nodeID,
		NetworkID:  networkID,
		ExternalIP: externalIP,
		Port:       int32(cfg.P2PPort),
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
		db.Close()
		return err
	}
	stack.RegisterLifecycle(p2pServer)
	stack.RegisterLifecycle(apiServer)
	stack.RegisterLifecycle(jrpcServer)
	if cfg.GRPCPort > 0 {
		stack.RegisterLifecycle(grpcServer)
	}
	stack.RegisterLifecycle(handler.PbftHandler())
	stack.RegisterLifecycle(pbftDataSync)

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
				db.Close()
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
			fmt.Printf("WARNING: witness %x is NOT in the active witness list (%d witnesses). No blocks will be produced.\n", witnessAddr[:6], len(activeWitnesses))
			fmt.Println("Hint: use --dev mode to create a single-witness dev chain with your key.")
		} else {
			fmt.Printf("Witness mode enabled (address=%x)\n", witnessAddr[:6])
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
				db.Close()
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
		db.Close()
		return err
	}

	fmt.Printf("gtron started (chain=%d, block=%d, http=:%d, datadir=%s)\n",
		chainConfig.ChainID, bc.CurrentBlock().Number(), cfg.HTTPPort, cfg.DataDir)

	// Wait for interrupt
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	fmt.Println("\nShutting down...")
	broadcaster.Stop()
	syncService.Stop()
	stack.Stop()
	// Flush the BlockChain's per-block buffer up to the solidified line
	// before closing the underlying store. Layers above solidified are
	// dropped — see core.BlockChain.Close for the trade-off rationale.
	if err := bc.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "blockchain close: %v\n", err)
	}
	db.Close()
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
