package main

import (
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/node"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

func defaultDataDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".gtron")
}

func makeConfig(ctx *cli.Context) *node.Config {
	return &node.Config{
		DataDir:     ctx.String("datadir"),
		P2PPort:     ctx.Int("p2p.port"),
		HTTPPort:    ctx.Int("http.port"),
		JSONRPCPort: ctx.Int("jsonrpc.port"),
	}
}

func makeGenesis(ctx *cli.Context) *params.Genesis {
	if ctx.Bool("testnet") {
		return params.DefaultNileGenesis()
	}
	return params.DefaultMainnetGenesis()
}

func chainDataDir(dataDir string) string {
	return filepath.Join(dataDir, "gtron", "chaindata")
}
