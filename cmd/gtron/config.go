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
		GRPCPort:    ctx.Int("grpc.port"),
		SeedNodes:   ctx.StringSlice("seednode"),
		MaxPeers:    ctx.Int("maxpeers"),
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
// fullFeatures enables all mainnet-activated allow_* flags in DynamicProperties.
// maintenanceInterval sets the maintenance_time_interval (ms).
func makeDevGenesis(witnessAddr tcommon.Address, fullFeatures bool, maintenanceInterval int64) *params.Genesis {
	dp := map[string]int64{
		"maintenance_time_interval": maintenanceInterval,
		"transaction_fee":           10,
		"witness_pay_per_block":     16000000,
		"witness_standby_allowance": 115200000000,
		"total_net_limit":           43200000000,
	}
	if fullFeatures {
		featureFlags := map[string]int64{
			"allow_new_resource_model":              1,
			"allow_same_token_name":                 1,
			"allow_delegate_resource":               1,
			"allow_multi_sign":                      1,
			"change_delegation":                     1,
			"allow_creation_of_contracts":           1,
			"allow_tvm_transfer_trc10":              1,
			"allow_tvm_constantinople":              1,
			"allow_tvm_solidity059":                 1,
			"allow_tvm_istanbul":                    1,
			"allow_market_transaction":              1,
			"allow_tvm_freeze":                      1,
			"allow_tvm_vote":                        1,
			"allow_pbft":                            1,
			"allow_tvm_london":                      1,
			"allow_tvm_compatible_evm":              1,
			"allow_tvm_blob":                        1,
			"allow_tvm_cancun":                      1,
			"allow_cancel_all_unfreeze_v2":          1,
			"allow_delegate_optimization":           1,
			"allow_update_account_name":             1,
			"allow_strict_math":                     1,
			"allow_tvm_selfdestruct_restriction":    1,
			"allow_tvm_shanghai":                    1,
			"allow_tvm_osaka":                       1,
			"allow_account_asset_optimization":      1,
			"allow_asset_optimization":              1,
			"allow_blackhole_optimization":          1,
		}
		for k, v := range featureFlags {
			dp[k] = v
		}
	}
	return &params.Genesis{
		Config:    &params.ChainConfig{ChainID: 9999, P2PVersion: 1},
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000, AccountName: "dev"},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 100000, URL: "http://dev-witness"},
		},
		DynamicProperties: dp,
	}
}
