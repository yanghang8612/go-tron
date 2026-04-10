package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	"github.com/tronprotocol/go-tron/node"
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
)

var app = &cli.App{
	Name:    "gtron",
	Usage:   "TRON blockchain node (Go implementation)",
	Version: "0.2.0-dev",
	Flags: []cli.Flag{
		dataDirFlag,
		p2pPortFlag,
		httpPortFlag,
		jsonrpcPortFlag,
		testnetFlag,
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
	genesis := makeGenesis(ctx)
	dbPath := chainDataDir(cfg.DataDir)

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

	// Create backend + API server
	backend := core.NewTronBackend(bc, pool)
	apiServer := tronapi.NewServer(backend, cfg.HTTPPort)

	// Create node and register services
	stack, err := node.New(cfg)
	if err != nil {
		db.Close()
		return err
	}
	stack.RegisterLifecycle(apiServer)

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
