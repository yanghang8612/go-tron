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
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	tnet "github.com/tronprotocol/go-tron/net"
	"github.com/tronprotocol/go-tron/node"
	"github.com/tronprotocol/go-tron/p2p"
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
	devFlag = &cli.BoolFlag{
		Name:  "dev",
		Usage: "Dev mode: single-witness chain using the provided witness key",
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
)

var app = &cli.App{
	Name:    "gtron",
	Usage:   "TRON blockchain node (Go implementation)",
	Version: "0.3.0-dev",
	Flags: []cli.Flag{
		dataDirFlag,
		p2pPortFlag,
		httpPortFlag,
		jsonrpcPortFlag,
		testnetFlag,
		witnessFlag,
		witnessKeyFlag,
		devFlag,
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
			Name:  "init",
			Usage: "Initialize genesis block",
			Flags: []cli.Flag{dataDirFlag, testnetFlag},
			Action: initCmd,
		},
	},
}

func initCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis := makeGenesis(ctx)
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

	genesis := makeGenesis(ctx)
	if ctx.Bool("dev") {
		witnessAddr := crypto.PubkeyToAddress(&devWitnessKey.PublicKey)
		genesis = makeDevGenesis(witnessAddr)
		fmt.Printf("Dev mode: single-witness genesis (witness=%x)\n", witnessAddr[:6])
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

	// Create P2P layer
	broadcaster := tnet.NewBroadcastService(nil)
	handler := tnet.NewTronHandler(bc, pool, broadcaster)
	syncService := tnet.NewSyncService(bc, handler)
	handler.SetSyncService(syncService)

	p2pServer := p2p.NewServer(p2p.ServerConfig{
		ListenAddr: fmt.Sprintf(":%d", cfg.P2PPort),
		MaxPeers:   cfg.MaxPeers,
		SeedNodes:  cfg.SeedNodes,
	}, handler)
	handler.SetServer(p2pServer)
	handler.StartKeepAlive()
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

	// Create node and register services
	stack, err := node.New(cfg)
	if err != nil {
		db.Close()
		return err
	}
	stack.RegisterLifecycle(p2pServer)
	stack.RegisterLifecycle(apiServer)
	stack.RegisterLifecycle(jrpcServer)

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
	stack.Stop()
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
