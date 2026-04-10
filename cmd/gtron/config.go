package main

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	tcommon "github.com/tronprotocol/go-tron/common"
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

// makeDevGenesis creates a minimal single-witness development genesis.
func makeDevGenesis(witnessAddr tcommon.Address) *params.Genesis {
	return &params.Genesis{
		Config:    &params.ChainConfig{ChainID: 9999, P2PVersion: 1},
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000, AccountName: "dev"},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 100000, URL: "http://dev-witness"},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": 21600000,
			"transaction_fee":           10,
			"witness_pay_per_block":     16000000,
			"witness_standby_allowance": 115200000000,
			"total_net_limit":           43200000000,
		},
	}
}
