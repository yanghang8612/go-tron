package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

// makeHistoryFlagSet builds a flag.FlagSet pre-populated with the
// gcmode + config flags that applyHistoryConfig consults. Tests parse
// per-case argv into this set and then wrap it in a cli.Context the way
// urfave/cli does in production.
func makeHistoryFlagSet(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{gcmodeFlag, configFileFlag}
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

func TestApplyHistoryConfig_DefaultsToFull(t *testing.T) {
	ctx := makeHistoryFlagSet(t, nil)
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeFull {
		t.Errorf("default mode = %q, want %q", got, params.HistoryModeFull)
	}
	if got := cfg.EffectiveHistoryPruneWindow(); got != params.HistoryDefaultPruneWindow {
		t.Errorf("default window = %d, want %d", got, params.HistoryDefaultPruneWindow)
	}
	// Full mode must NOT auto-enable HistoryEnabled — that path is the
	// zero-cost default for non-archive operators.
	if cfg.HistoryEnabled {
		t.Error("HistoryEnabled was auto-flipped in full mode (expected off)")
	}
}

func TestApplyHistoryConfig_GcmodeArchive(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--gcmode", "archive"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeArchive {
		t.Errorf("--gcmode archive: mode = %q, want %q", got, params.HistoryModeArchive)
	}
	// Archive mode flips HistoryEnabled on — otherwise the archive is
	// silent.
	if !cfg.HistoryEnabled {
		t.Error("archive mode did not auto-enable HistoryEnabled")
	}
}

func TestApplyHistoryConfig_GcmodeUnknownErrors(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--gcmode", "weird"})
	cfg := &params.ChainConfig{}
	err := applyHistoryConfig(ctx, cfg)
	if err == nil {
		t.Fatal("expected error for unknown --gcmode")
	}
}

func TestApplyHistoryConfig_TOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	body := `# operator config
[history]
mode = "archive"
prune_window = 12345  # ignored in archive mode but kept for symmetry
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeArchive {
		t.Errorf("TOML mode = %q, want %q", got, params.HistoryModeArchive)
	}
	if cfg.HistoryPruneWindow != 12345 {
		t.Errorf("TOML prune_window = %d, want 12345", cfg.HistoryPruneWindow)
	}
}

// TestApplyHistoryConfig_CLIOverridesTOML asserts the precedence: a
// --gcmode flag wins over a [history] mode in the TOML.
func TestApplyHistoryConfig_CLIOverridesTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	body := "[history]\nmode = \"archive\"\nprune_window = 99\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path, "--gcmode", "full"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeFull {
		t.Errorf("CLI override: mode = %q, want %q", got, params.HistoryModeFull)
	}
	if cfg.HistoryPruneWindow != 99 {
		t.Errorf("TOML prune_window not retained when CLI only overrode mode: %d, want 99", cfg.HistoryPruneWindow)
	}
}

func TestApplyHistoryConfig_TOMLMissingFileIsNoOp(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--config", "/definitely/not/a/real/path.toml"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig (missing file): %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeFull {
		t.Errorf("missing config: mode = %q, want default %q", got, params.HistoryModeFull)
	}
}

func TestApplyHistoryConfig_TOMLMalformedErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.toml")
	body := "[history]\nthis line has no equals sign\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path})
	cfg := &params.ChainConfig{}
	err := applyHistoryConfig(ctx, cfg)
	if err == nil {
		t.Fatal("expected error for malformed [history] section")
	}
}

// TestApplyHistoryConfig_TOMLNoHistorySection asserts the loader is
// forward-compatible: a TOML with other sections but no [history] is a
// no-op rather than an error.
func TestApplyHistoryConfig_TOMLNoHistorySection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "other.toml")
	body := "[network]\nport = 18888\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if cfg.HistoryPruneWindow != 0 {
		t.Errorf("expected zero (untouched) prune_window, got %d", cfg.HistoryPruneWindow)
	}
}

// TestApplyHistoryConfig_GcmodeFullDoesNotAutoEnable asserts the
// archive-flip-HistoryEnabled rule is restricted to archive mode. A
// full-mode operator must opt into HistoryEnabled explicitly (slice 2)
// — slice 5 doesn't change that contract.
func TestApplyHistoryConfig_GcmodeFullDoesNotAutoEnable(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--gcmode", "full"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if cfg.HistoryEnabled {
		t.Error("--gcmode=full unexpectedly turned on HistoryEnabled")
	}
}
