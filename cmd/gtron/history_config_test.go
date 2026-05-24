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
	app.Flags = []cli.Flag{gcmodeFlag, historyEnabledFlag, configFileFlag}
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

func TestApplyHistoryConfig_TOMLMissingFileErrors(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--config", "/definitely/not/a/real/path.toml"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err == nil {
		t.Fatal("expected error for explicit missing --config")
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

// TestApplyHistoryConfig_FullModeEnabledIsReachable is the regression for the
// slice-5 spec-review escalation: full-mode pruning was operationally
// unreachable because nothing flipped HistoryEnabled on outside archive mode.
// `--history.enabled` (or [history] enabled = true) is the canonical opt-in;
// combined with the default full mode it yields a captured-and-pruned index.
func TestApplyHistoryConfig_FullModeEnabledIsReachable(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--gcmode", "full", "--history.enabled"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeFull {
		t.Errorf("mode = %q, want full", got)
	}
	if !cfg.HistoryEnabled {
		t.Fatal("--history.enabled did not turn on HistoryEnabled in full mode")
	}
}

// TestApplyHistoryConfig_EnabledViaTOML covers the [history] enabled key.
func TestApplyHistoryConfig_EnabledViaTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	if err := os.WriteFile(path, []byte("[history]\nenabled = true\n"), 0o600); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if !cfg.HistoryEnabled {
		t.Fatal("[history] enabled = true did not turn on HistoryEnabled")
	}
	// TOML enable + default full mode = pruned-history node.
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeFull {
		t.Errorf("mode = %q, want full", got)
	}
}

// TestApplyHistoryConfig_CLIEnabledOverridesTOMLDisabled pins the precedence:
// an explicit --history.enabled beats [history] enabled = false.
func TestApplyHistoryConfig_CLIEnabledOverridesTOMLDisabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	if err := os.WriteFile(path, []byte("[history]\nenabled = false\n"), 0o600); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path, "--history.enabled"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if !cfg.HistoryEnabled {
		t.Fatal("CLI --history.enabled should override TOML enabled=false")
	}
}

func TestShouldEnableDomainStatePruner(t *testing.T) {
	tests := []struct {
		name string
		cfg  params.ChainConfig
		want bool
	}{
		{
			name: "plain full stays zero cost",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeFull},
			want: false,
		},
		{
			name: "full history capture needs pruning",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeFull, HistoryEnabled: true},
			want: true,
		},
		{
			name: "full checkpoints need pruning",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeFull, StateCommitmentCheckpoints: true},
			want: true,
		},
		{
			name: "archive never prunes",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeArchive, HistoryEnabled: true, StateCommitmentCheckpoints: true},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldEnableDomainStatePruner(&tt.cfg); got != tt.want {
				t.Fatalf("shouldEnableDomainStatePruner = %v, want %v", got, tt.want)
			}
		})
	}
}
