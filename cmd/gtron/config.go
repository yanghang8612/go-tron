package main

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/crypto"
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

func parseWitnessKey(ctx *cli.Context) (*ecdsa.PrivateKey, error) {
	hexKey := ctx.String("witness.key")
	if hexKey == "" {
		return nil, fmt.Errorf("--witness.key is required when --witness is set")
	}
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("invalid hex key: %w", err)
	}
	return crypto.BytesToPrivateKey(keyBytes)
}
