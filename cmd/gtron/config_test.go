package main

import (
	"flag"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

var testWitnessAddr = tcommon.Address{0x01}

func makeNodeConfigFlagSet(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		dataDirFlag,
		p2pPortFlag,
		discoverPortFlag,
		externalIPFlag,
		httpPortFlag,
		jsonrpcPortFlag,
		grpcPortFlag,
		pprofPortFlag,
		pprofAddrFlag,
		seednodeFlag,
		maxpeersFlag,
		syncImportBatchFlag,
	}
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	for _, f := range app.Flags {
		if err := f.Apply(set); err != nil {
			t.Fatalf("apply flag: %v", err)
		}
	}
	if err := set.Parse(argv); err != nil {
		t.Fatalf("parse argv: %v", err)
	}
	return cli.NewContext(app, set, nil)
}

func TestMakeConfigSyncImportBatchDefault(t *testing.T) {
	cfg := makeConfig(makeNodeConfigFlagSet(t, nil))
	if cfg.SyncImportBatch != tsync.MaxImportBatch {
		t.Fatalf("SyncImportBatch default = %d, want %d", cfg.SyncImportBatch, tsync.MaxImportBatch)
	}
}

func TestMakeConfigSyncImportBatchOverride(t *testing.T) {
	cfg := makeConfig(makeNodeConfigFlagSet(t, []string{"--sync.import-batch", "12"}))
	if cfg.SyncImportBatch != 12 {
		t.Fatalf("SyncImportBatch override = %d, want 12", cfg.SyncImportBatch)
	}
}

func TestResolveNetworkID_Mainnet(t *testing.T) {
	got := resolveNetworkID(params.DefaultMainnetGenesis())
	if got != params.MainnetNetworkID {
		t.Fatalf("mainnet networkID: got %d, want %d", got, params.MainnetNetworkID)
	}
}

func TestResolveNetworkID_Nile(t *testing.T) {
	got := resolveNetworkID(params.DefaultNileGenesis())
	if got != params.NileNetworkID {
		t.Fatalf("Nile networkID: got %d, want %d", got, params.NileNetworkID)
	}
}

func TestResolveNetworkID_PrivateChainP2PVersionZero(t *testing.T) {
	g := &params.Genesis{Config: &params.ChainConfig{P2PVersion: 0}}
	if got := resolveNetworkID(g); got != 0 {
		t.Fatalf("private chain (P2PVersion=0) networkID: got %d, want 0", got)
	}
}

func TestResolveNetworkID_CustomP2PVersion(t *testing.T) {
	g := &params.Genesis{Config: &params.ChainConfig{P2PVersion: 42}}
	if got := resolveNetworkID(g); got != 42 {
		t.Fatalf("custom P2PVersion=42 networkID: got %d, want 42", got)
	}
}

func TestMakeDevGenesisFullFeatures(t *testing.T) {
	g := makeDevGenesis(testWitnessAddr, true, 21600000)
	dp := g.DynamicProperties

	checks := []string{
		"allow_new_resource_model",
		"allow_creation_of_contracts",
		"allow_tvm_istanbul",
	}
	for _, key := range checks {
		if dp[key] != 1 {
			t.Errorf("expected DynamicProperties[%q] == 1, got %d", key, dp[key])
		}
	}
	if dp["maintenance_time_interval"] != 21600000 {
		t.Errorf("expected maintenance_time_interval == 21600000, got %d", dp["maintenance_time_interval"])
	}
}

func TestMakeDevGenesisNoFullFeatures(t *testing.T) {
	g := makeDevGenesis(testWitnessAddr, false, 21600000)
	dp := g.DynamicProperties

	if _, ok := dp["allow_new_resource_model"]; ok {
		if dp["allow_new_resource_model"] != 0 {
			t.Errorf("expected allow_new_resource_model absent or 0, got %d", dp["allow_new_resource_model"])
		}
	}
	if dp["maintenance_time_interval"] != 21600000 {
		t.Errorf("expected maintenance_time_interval == 21600000, got %d", dp["maintenance_time_interval"])
	}
}

func TestMakeDevGenesisCustomInterval(t *testing.T) {
	g := makeDevGenesis(testWitnessAddr, false, 30000)
	if g.DynamicProperties["maintenance_time_interval"] != 30000 {
		t.Errorf("expected maintenance_time_interval == 30000, got %d", g.DynamicProperties["maintenance_time_interval"])
	}
}
