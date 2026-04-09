package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	Version: "0.1.0-dev",
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
	},
}

func gtron(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	stack, err := node.New(cfg)
	if err != nil {
		return err
	}

	if err := stack.Start(); err != nil {
		return err
	}
	fmt.Printf("gtron started (datadir=%s, http=%d, p2p=%d)\n",
		cfg.DataDir, cfg.HTTPPort, cfg.P2PPort)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	fmt.Println("\nShutting down...")
	stack.Stop()
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
