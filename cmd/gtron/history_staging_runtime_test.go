package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corestate "github.com/tronprotocol/go-tron/core/state"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

func historyStagingRuntimeTestContext(t *testing.T, enabled bool) *cli.Context {
	t.Helper()
	set := flag.NewFlagSet("history-staging-runtime-test", flag.ContinueOnError)
	set.Bool("history.staging", false, "")
	if enabled {
		if err := set.Set("history.staging", "true"); err != nil {
			t.Fatal(err)
		}
	}
	return cli.NewContext(nil, set, nil)
}

func TestHistoryStagingFreshIdentitySurvivesRestartWithoutMarker(t *testing.T) {
	datadir := filepath.Join(t.TempDir(), "datadir")
	source, cold := chainDataDir(datadir), stateSnapshotsDir(datadir)
	first, err := resolveHistoryStagingRuntimeRequest(historyStagingRuntimeTestContext(t, true), datadir, source, cold)
	if err != nil || !first.Enabled || !first.Fresh || first.Persisted {
		t.Fatalf("fresh request = %+v, %v", first, err)
	}
	hot, err := rawdb.NewPebbleDB(first.Paths.Source, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	genesis := params.DefaultMainnetGenesis()
	_, genesisHash, err := core.SetupGenesisBlock(hot, genesis)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := rawdb.NewHistoryStagingPebbleDB(first.Paths.Target, 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	identity := historyStagingIdentity(first.Paths, genesisHash, uint64(params.MainnetNetworkID))
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := publishFreshHistoryStagingBarrier(manager, first.ExecutableSHA256, 0, genesisHash, genesisHash); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	if err := hot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(datadir), "HISTORY_STAGING_READER_REQUIRED.json")); !os.IsNotExist(err) {
		t.Fatalf("fresh local node unexpectedly has deployment marker: %v", err)
	}
	second, err := resolveHistoryStagingRuntimeRequest(historyStagingRuntimeTestContext(t, false), datadir, source, cold)
	if err != nil || !second.Enabled || !second.Fresh || !second.Persisted || second.Paths != first.Paths {
		t.Fatalf("restarted request = %+v, %v", second, err)
	}
	hot, err = rawdb.NewPebbleDB(second.Paths.Source, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err = rawdb.NewHistoryStagingPebbleDB(second.Paths.Target, 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	manager, err = rawdb.NewHistoryStagingManager(hot, stage, identity)
	if err != nil || manager.VerifyIdentity() != nil {
		t.Fatalf("reopened store identity = %v", err)
	}
	if err := publishFreshHistoryStagingBarrier(manager, second.ExecutableSHA256, 0, genesisHash, genesisHash); err != nil {
		t.Fatal(err)
	}
	// The runtime has no configured fork-config digest. A bound manifest may
	// carry one, while its primary chain fields still have to match hot state.
	chain := statesnapshots.ChainIdentity{ChainID: genesis.Config.ChainID,
		NetworkID: int32(params.MainnetNetworkID), GenesisHash: genesisHash.Hex(),
		ForkConfigHash: "sha256:" + strings.Repeat("a", 64)}
	manifest := statesnapshots.NewManifestForChain(0, 0, nil, chain)
	if err := statesnapshots.PublishManifest(second.Paths.Cold, manifest); err != nil {
		t.Fatal(err)
	}
	coldManager, err := statesnapshots.OpenManager(second.Paths.Cold)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := core.NewBlockChainWithAncient(hot, corestate.NewDatabase(rawdb.WrapKeyValueStore(hot)), genesis.Config, rawdb.NoopAncient{})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	bc.SetStateCodeColdHistory(coldManager)
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := statesnapshots.BindHistoryStagingColdRetention(second.Paths.Cold, manager); err != nil {
		t.Fatal(err)
	}
	if err := bc.VerifyHistoryStagingRuntimeReady(context.Background()); err != nil {
		t.Fatalf("restarted staged reader preflight: %v", err)
	}
	for _, wrong := range []statesnapshots.ChainIdentity{
		{ChainID: chain.ChainID + 1, NetworkID: chain.NetworkID, GenesisHash: chain.GenesisHash, ForkConfigHash: chain.ForkConfigHash},
		{ChainID: chain.ChainID, NetworkID: chain.NetworkID + 1, GenesisHash: chain.GenesisHash, ForkConfigHash: chain.ForkConfigHash},
		{ChainID: chain.ChainID, NetworkID: chain.NetworkID, GenesisHash: strings.Repeat("b", 64), ForkConfigHash: chain.ForkConfigHash},
	} {
		manifest.Chain = &wrong
		if err := statesnapshots.PublishManifest(second.Paths.Cold, manifest); err != nil {
			t.Fatal(err)
		}
		if err := bc.VerifyHistoryStagingRuntimeReady(context.Background()); err == nil ||
			!strings.Contains(err.Error(), "primary chain identity mismatch") {
			t.Fatalf("wrong primary chain identity accepted: %+v: %v", wrong, err)
		}
	}
}

func TestHistoryStagingRefusesLegacyStoreBeforeMutation(t *testing.T) {
	datadir := filepath.Join(t.TempDir(), "datadir")
	source := chainDataDir(datadir)
	hot, err := rawdb.NewPebbleDB(source, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.SetupGenesisBlock(hot, params.DefaultMainnetGenesis()); err != nil {
		t.Fatal(err)
	}
	if err := hot.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = resolveHistoryStagingRuntimeRequest(historyStagingRuntimeTestContext(t, true), datadir, source, stateSnapshotsDir(datadir))
	if err == nil || !strings.Contains(err.Error(), "offline history staging migration") {
		t.Fatalf("legacy store enabled in place: %v", err)
	}
	hot, err = rawdb.NewPebbleDBReadOnly(source, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	if _, present, err := rawdb.ReadHistoryStagingIdentity(hot); err != nil || present {
		t.Fatalf("legacy source identity changed: %v %v", present, err)
	}
	if _, err := os.Stat(defaultHistoryStagingDir(datadir)); !os.IsNotExist(err) {
		t.Fatalf("legacy request created stage directory: %v", err)
	}
}

func TestHistoryStagingCapabilityMatchesDeploymentGuard(t *testing.T) {
	var output bytes.Buffer
	app := &cli.App{Writer: &output, Commands: []*cli.Command{dbHistoryStagingCommand()}}
	if err := app.Run([]string{"gtron", "history-staging", "capability"}); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got["format_version"] != float64(1) ||
		got["history_staging_reader"] != true || got["protocol_version"] != float64(1) {
		t.Fatalf("deployment guard capability contract = %v", got)
	}
}

func TestHistoryStagingRuntimeRejectsDisabledOrUnsupportedHistory(t *testing.T) {
	for _, cfg := range []*params.ChainConfig{
		{HistoryMode: params.HistoryModeSnap, HistoryEnabled: false},
		{HistoryMode: params.HistoryModeFull, HistoryEnabled: true},
		{HistoryMode: params.HistoryModeMinimal, HistoryEnabled: true},
	} {
		if err := validateHistoryStagingRuntimeConfig(cfg); err == nil {
			t.Fatalf("unsupported staged reader config accepted: %+v", cfg)
		}
	}
	for _, mode := range []string{params.HistoryModeSnap, params.HistoryModeArchive} {
		if err := validateHistoryStagingRuntimeConfig(&params.ChainConfig{HistoryMode: mode, HistoryEnabled: true}); err != nil {
			t.Fatalf("supported %s config rejected: %v", mode, err)
		}
	}
}

func TestHistoryStagingFreeSpaceCanPlanBeforeTargetExists(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "chaindata")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "not-yet-created", "history-staging")
	free, err := historyStagingMinimumFreeBytes(source, target)
	if err != nil || free == 0 {
		t.Fatalf("dry-run free-space probe = %d, %v", free, err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("free-space probe created target: %v", err)
	}
}

func TestHistoryStagingReaderMarkerFreezesVerifiedModeAndWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "HISTORY_STAGING_READER_REQUIRED.json")
	marker := historyStagingReaderMarker{Version: 1, FormatVersion: 1,
		BinarySHA256: strings.Repeat("a", 64), SourceCommit: strings.Repeat("b", 40),
		HistoryWindow: 65536, PruneMode: params.HistoryModeSnap}
	data, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, present, err := readHistoryStagingReaderMarker(path)
	if err != nil || !present || got != marker {
		t.Fatalf("reader marker = %+v/%v/%v", got, present, err)
	}
	marker.HistoryWindow = 0
	data, err = json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readHistoryStagingReaderMarker(path); err == nil {
		t.Fatal("reader marker without verified history window was accepted")
	}
}
