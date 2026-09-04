package main

import (
	"flag"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	"github.com/tronprotocol/go-tron/node"
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
		metricsEnabledFlag,
		metricsAddrFlag,
		metricsPortFlag,
		seednodeFlag,
		maxpeersFlag,
		syncImportBatchFlag,
		syncETLTempDirFlag,
		syncETLBufferMiBFlag,
		syncETLBatchMiBFlag,
		syncAsyncCommitFlag,
		execParallelTransfersFlag,
		execParallelVMFlag,
		vmSaveInternalTxFlag,
		vmSaveFeaturedInternalTxFlag,
		vmSaveCancelAllUnfreezeV2DetailsFlag,
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

func makeFreezerConfigFlagSet(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		freezerDisableFlag,
		freezerIntervalFlag,
		freezerMarginFlag,
		freezerBatchFlag,
		freezerTxIndexDisableFlag,
		freezerDirectV2DisableFlag,
	}
	set := flag.NewFlagSet("freezer-test", flag.ContinueOnError)
	for _, cliFlag := range app.Flags {
		if err := cliFlag.Apply(set); err != nil {
			t.Fatalf("apply freezer flag: %v", err)
		}
	}
	if err := set.Parse(argv); err != nil {
		t.Fatalf("parse freezer argv: %v", err)
	}
	return cli.NewContext(app, set, nil)
}

func TestMakeFreezerConfigDirectV2KillSwitch(t *testing.T) {
	cfg := makeFreezerConfig(makeFreezerConfigFlagSet(t, nil))
	if !cfg.Enabled || !cfg.V2Enabled || !cfg.DirectV2 || !cfg.TransactionIndexEnabled {
		t.Fatalf("default freezer config does not use direct V2: %+v", cfg)
	}

	cfg = makeFreezerConfig(makeFreezerConfigFlagSet(t, []string{"--freezer.direct-v2.disable"}))
	if cfg.DirectV2 {
		t.Fatalf("direct V2 kill switch was ignored: %+v", cfg)
	}
}

func TestMakeConfigSyncImportBatchDefault(t *testing.T) {
	cfg := makeConfig(makeNodeConfigFlagSet(t, nil))
	if cfg.SyncImportBatch != tsync.MaxImportBatch {
		t.Fatalf("SyncImportBatch default = %d, want %d", cfg.SyncImportBatch, tsync.MaxImportBatch)
	}
}

func TestMakeConfigVMInternalTransactionPersistence(t *testing.T) {
	cfg := makeConfig(makeNodeConfigFlagSet(t, nil))
	if cfg.SaveInternalTx || cfg.SaveFeaturedInternalTx || cfg.SaveCancelAllUnfreezeV2Details {
		t.Fatalf("default VM internal transaction config = %+v", cfg)
	}

	cfg = makeConfig(makeNodeConfigFlagSet(t, []string{
		"--vm.save-internal-tx",
		"--vm.save-featured-internal-tx",
		"--vm.save-cancel-all-unfreeze-v2-details",
	}))
	if !cfg.SaveInternalTx || !cfg.SaveFeaturedInternalTx || !cfg.SaveCancelAllUnfreezeV2Details {
		t.Fatalf("overridden VM internal transaction config = %+v", cfg)
	}
}

func TestMakeConfigMetrics(t *testing.T) {
	cfg := makeConfig(makeNodeConfigFlagSet(t, nil))
	if cfg.MetricsEnabled {
		t.Fatal("metrics enabled by default")
	}
	if cfg.MetricsAddr != "127.0.0.1" || cfg.MetricsPort != 6061 {
		t.Fatalf("default metrics endpoint = %s:%d", cfg.MetricsAddr, cfg.MetricsPort)
	}

	cfg = makeConfig(makeNodeConfigFlagSet(t, []string{
		"--metrics",
		"--metrics.addr", "0.0.0.0",
		"--metrics.port", "9091",
	}))
	if !cfg.MetricsEnabled || cfg.MetricsAddr != "0.0.0.0" || cfg.MetricsPort != 9091 {
		t.Fatalf("overridden metrics config = %+v", cfg)
	}
}

func TestValidateMetricsConfig(t *testing.T) {
	if err := validateMetricsConfig(&node.Config{}); err != nil {
		t.Fatalf("disabled metrics config: %v", err)
	}
	for _, cfg := range []*node.Config{
		{MetricsEnabled: true, MetricsPort: 6061},
		{MetricsEnabled: true, MetricsAddr: "127.0.0.1", MetricsPort: 0},
		{MetricsEnabled: true, MetricsAddr: "127.0.0.1", MetricsPort: 65536},
	} {
		if err := validateMetricsConfig(cfg); err == nil {
			t.Fatalf("invalid metrics config accepted: %+v", cfg)
		}
	}
	if err := validateMetricsConfig(&node.Config{
		MetricsEnabled: true,
		MetricsAddr:    "127.0.0.1",
		MetricsPort:    6061,
	}); err != nil {
		t.Fatalf("valid metrics config: %v", err)
	}
}

func TestMakeConfigSyncImportBatchOverride(t *testing.T) {
	cfg := makeConfig(makeNodeConfigFlagSet(t, []string{"--sync.import-batch", "12"}))
	if cfg.SyncImportBatch != 12 {
		t.Fatalf("SyncImportBatch override = %d, want 12", cfg.SyncImportBatch)
	}
}

func TestValidateSyncImportBatch(t *testing.T) {
	for _, size := range []int{0, -1, tsync.MaxStagedImportBatch + 1} {
		if err := validateSyncImportBatch(size); err == nil {
			t.Fatalf("validateSyncImportBatch(%d) succeeded", size)
		}
	}
	if err := validateSyncImportBatch(tsync.MaxStagedImportBatch); err != nil {
		t.Fatalf("validateSyncImportBatch(max) = %v", err)
	}
}

func TestGtronRejectsInvalidSyncImportBatchBeforeStartup(t *testing.T) {
	ctx := makeNodeConfigFlagSet(t, []string{"--sync.import-batch", "0"})
	if err := gtron(ctx); err == nil || !strings.Contains(err.Error(), "sync import batch") {
		t.Fatalf("gtron invalid sync import batch error = %v, want startup validation error", err)
	}
}

func TestSyncImportBatchEnvironmentAlias(t *testing.T) {
	t.Setenv("GTRON_SYNC_IMPORT_BATCH", "12")
	app := cli.NewApp()
	app.Flags = []cli.Flag{syncImportBatchFlag}
	var got int
	app.Action = func(ctx *cli.Context) error {
		got = makeConfig(ctx).SyncImportBatch
		return nil
	}
	if err := app.Run([]string{"gtron"}); err != nil {
		t.Fatalf("run sync import batch environment alias: %v", err)
	}
	if got != 12 {
		t.Fatalf("SyncImportBatch environment value = %d, want 12", got)
	}
}

func TestSyncTransactionLookupETLOptions(t *testing.T) {
	tempDir := t.TempDir()
	cfg := makeConfig(makeNodeConfigFlagSet(t, []string{
		"--sync.etl.tempdir", tempDir,
		"--sync.etl.buffer", "12",
		"--sync.etl.batch", "3",
	}))
	opts, err := syncTransactionLookupETLOptions(cfg)
	if err != nil {
		t.Fatalf("syncTransactionLookupETLOptions: %v", err)
	}
	if opts.TempDir != tempDir || opts.BufferLimit != 12*1024*1024 || opts.BatchSize != 3*1024*1024 {
		t.Fatalf("sync TxLookup ETL options = %+v", opts)
	}
}

func TestSyncTransactionLookupETLOptionsDefaultsAndOverflow(t *testing.T) {
	opts, err := syncTransactionLookupETLOptions(makeConfig(makeNodeConfigFlagSet(t, nil)))
	if err != nil {
		t.Fatalf("default syncTransactionLookupETLOptions: %v", err)
	}
	if opts.TempDir != "" || opts.BufferLimit != 0 || opts.BatchSize != 0 {
		t.Fatalf("default sync TxLookup ETL options = %+v, want zero values", opts)
	}
	if _, err := syncTransactionLookupETLOptions(&node.Config{SyncETLBufferMiB: ^uint64(0)}); err == nil {
		t.Fatal("syncTransactionLookupETLOptions accepted overflowing buffer")
	}
}

func TestShouldEnableAsyncCommit(t *testing.T) {
	if shouldEnableAsyncCommit(makeNodeConfigFlagSet(t, nil)) {
		t.Fatal("async commit enabled by default")
	}
	if !shouldEnableAsyncCommit(makeNodeConfigFlagSet(t, []string{"--sync.async-commit"})) {
		t.Fatal("explicit async commit flag was ignored")
	}
}

func TestParallelTransferExecutionFlagDefaultsOff(t *testing.T) {
	if makeNodeConfigFlagSet(t, nil).Bool(execParallelTransfersFlag.Name) {
		t.Fatal("parallel Transfer execution enabled by default")
	}
	if !makeNodeConfigFlagSet(t, []string{"--exec.parallel-transfers"}).Bool(execParallelTransfersFlag.Name) {
		t.Fatal("explicit parallel Transfer execution flag was ignored")
	}
}

func TestParallelVMExecutionFlagDefaultsOff(t *testing.T) {
	if makeNodeConfigFlagSet(t, nil).Bool(execParallelVMFlag.Name) {
		t.Fatal("parallel VM execution enabled by default")
	}
	if !makeNodeConfigFlagSet(t, []string{"--exec.parallel-vm"}).Bool(execParallelVMFlag.Name) {
		t.Fatal("explicit parallel VM execution flag was ignored")
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
