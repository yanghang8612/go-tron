package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

func makeStatePrefetchFlagSet(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		statePrefetchEnabledFlag,
		statePrefetchWorkersFlag,
		statePrefetchLookaheadFlag,
		configFileFlag,
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

func TestApplyStatePrefetchConfig_DefaultsOff(t *testing.T) {
	ctx := makeStatePrefetchFlagSet(t, nil)
	cfg := &params.ChainConfig{}
	if err := applyStatePrefetchConfig(ctx, cfg); err != nil {
		t.Fatalf("applyStatePrefetchConfig: %v", err)
	}
	if cfg.StatePrefetchEnabled {
		t.Fatal("state prefetch should default off until soak evidence flips the default")
	}
	if got := cfg.EffectiveStatePrefetchLookahead(); got != params.StatePrefetchDefaultLookahead {
		t.Fatalf("lookahead = %d, want %d", got, params.StatePrefetchDefaultLookahead)
	}
}

func TestApplyStatePrefetchConfig_PreservesNetworkDefault(t *testing.T) {
	ctx := makeStatePrefetchFlagSet(t, nil)
	cfg := &params.ChainConfig{StatePrefetchEnabled: true, StatePrefetchWorkers: 4, StatePrefetchLookahead: 8}
	if err := applyStatePrefetchConfig(ctx, cfg); err != nil {
		t.Fatalf("applyStatePrefetchConfig: %v", err)
	}
	if !cfg.StatePrefetchEnabled || cfg.StatePrefetchWorkers != 4 || cfg.StatePrefetchLookahead != 8 {
		t.Fatalf("network defaults changed: enabled=%v workers=%d lookahead=%d", cfg.StatePrefetchEnabled, cfg.StatePrefetchWorkers, cfg.StatePrefetchLookahead)
	}
}

func TestApplyStatePrefetchConfig_CanDisableNetworkDefault(t *testing.T) {
	ctx := makeStatePrefetchFlagSet(t, []string{"--state.prefetch.enabled=false"})
	cfg := &params.ChainConfig{StatePrefetchEnabled: true, StatePrefetchWorkers: 4, StatePrefetchLookahead: 8}
	if err := applyStatePrefetchConfig(ctx, cfg); err != nil {
		t.Fatalf("applyStatePrefetchConfig: %v", err)
	}
	if cfg.StatePrefetchEnabled {
		t.Fatal("explicit false did not disable the network default")
	}
}

func TestApplyStatePrefetchConfig_TOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	body := `[state.prefetch]
enabled = true
workers = 3
lookahead = 5
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeStatePrefetchFlagSet(t, []string{"--config", path})
	cfg := &params.ChainConfig{}
	if err := applyStatePrefetchConfig(ctx, cfg); err != nil {
		t.Fatalf("applyStatePrefetchConfig: %v", err)
	}
	if !cfg.StatePrefetchEnabled {
		t.Fatal("TOML enabled did not turn on state prefetch")
	}
	if cfg.StatePrefetchWorkers != 3 {
		t.Fatalf("workers = %d, want 3", cfg.StatePrefetchWorkers)
	}
	if cfg.StatePrefetchLookahead != 5 {
		t.Fatalf("lookahead = %d, want 5", cfg.StatePrefetchLookahead)
	}
}

func TestApplyStatePrefetchConfig_CLIOverridesTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	body := `[state.prefetch]
enabled = false
workers = 2
lookahead = 4
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeStatePrefetchFlagSet(t, []string{
		"--config", path,
		"--state.prefetch.enabled",
		"--state.prefetch.workers", "6",
		"--state.prefetch.lookahead", "9",
	})
	cfg := &params.ChainConfig{}
	if err := applyStatePrefetchConfig(ctx, cfg); err != nil {
		t.Fatalf("applyStatePrefetchConfig: %v", err)
	}
	if !cfg.StatePrefetchEnabled {
		t.Fatal("CLI enabled did not override TOML disabled")
	}
	if cfg.StatePrefetchWorkers != 6 {
		t.Fatalf("workers = %d, want 6", cfg.StatePrefetchWorkers)
	}
	if cfg.StatePrefetchLookahead != 9 {
		t.Fatalf("lookahead = %d, want 9", cfg.StatePrefetchLookahead)
	}
}

func TestApplyStatePrefetchConfig_RejectsNegative(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	if err := os.WriteFile(path, []byte("[state.prefetch]\nworkers = -1\n"), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeStatePrefetchFlagSet(t, []string{"--config", path})
	cfg := &params.ChainConfig{}
	if err := applyStatePrefetchConfig(ctx, cfg); err == nil {
		t.Fatal("expected negative workers to fail")
	}
}
