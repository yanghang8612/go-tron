package main

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

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

func makeGenesis(ctx *cli.Context) (*params.Genesis, error) {
	gFile := ctx.String("genesis")
	if gFile != "" {
		if ctx.Bool("testnet") || ctx.Bool("dev") {
			return nil, fmt.Errorf("--genesis is mutually exclusive with --testnet and --dev")
		}
		g, err := loadGenesisFile(gFile)
		if err != nil {
			return nil, err
		}
		return g, nil
	}
	if ctx.Bool("testnet") {
		return params.DefaultNileGenesis(), nil
	}
	return params.DefaultMainnetGenesis(), nil
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

// parseWitnessKeysFile reads N hex-encoded private keys, one per line, from
// path. Used by --witness.keys-file for multi-SR PBFT testing where one
// process holds multiple SR keys. Lines starting with `#` and blank lines
// are ignored.
func parseWitnessKeysFile(path string) ([]*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var keys []*ecdsa.PrivateKey
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		raw, err := hex.DecodeString(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid hex: %w", i+1, err)
		}
		k, err := crypto.BytesToPrivateKey(raw)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no keys found in %s", path)
	}
	return keys, nil
}

// makeDevGenesis creates a minimal single-witness development genesis.
// fullFeatures enables all mainnet-activated allow_* flags in DynamicProperties.
// maintenanceInterval sets the maintenance_time_interval (ms).
func makeDevGenesis(witnessAddr tcommon.Address, fullFeatures bool, maintenanceInterval int64) *params.Genesis {
	nowMs := time.Now().UnixMilli()
	dp := map[string]int64{
		"maintenance_time_interval": maintenanceInterval,
		"next_maintenance_time":     nowMs + maintenanceInterval,
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
			"unfreeze_delay_days":                   14,
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
