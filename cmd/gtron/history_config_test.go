package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
	tnet "github.com/tronprotocol/go-tron/net"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

// makeHistoryFlagSet builds a flag.FlagSet pre-populated with the
// prune + config flags that applyHistoryConfig consults. Tests parse
// per-case argv into this set and then wrap it in a cli.Context the way
// urfave/cli does in production.
func makeHistoryFlagSet(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{pruneModeFlag, historyEnabledFlag, configFileFlag}
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

func TestApplyHistoryConfig_PruneModeArchive(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--prune.mode", "archive"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeArchive {
		t.Errorf("--prune.mode archive: mode = %q, want %q", got, params.HistoryModeArchive)
	}
	if !cfg.HistoryEnabled {
		t.Error("archive prune mode did not auto-enable HistoryEnabled")
	}
}

func TestApplyHistoryConfig_PruneModeSnap(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--prune.mode", "snap"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeSnap {
		t.Errorf("--prune.mode snap: mode = %q, want %q", got, params.HistoryModeSnap)
	}
	if !cfg.HistoryEnabled {
		t.Error("snap prune mode did not auto-enable HistoryEnabled")
	}
}

func TestApplyHistoryConfig_PruneModeBlocksAndMinimal(t *testing.T) {
	for _, mode := range []string{params.HistoryModeBlocks, params.HistoryModeMinimal} {
		t.Run(mode, func(t *testing.T) {
			ctx := makeHistoryFlagSet(t, []string{"--prune.mode", mode})
			cfg := &params.ChainConfig{}
			if err := applyHistoryConfig(ctx, cfg); err != nil {
				t.Fatalf("applyHistoryConfig: %v", err)
			}
			if got := cfg.EffectiveHistoryMode(); got != mode {
				t.Errorf("--prune.mode %s: mode = %q, want %q", mode, got, mode)
			}
			if !cfg.HistoryEnabled {
				t.Error("explicit blocks/minimal prune modes should enable state history capture")
			}
		})
	}
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

func TestApplyHistoryConfig_TOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	body := `# operator config
[history]
mode = "archive"
prune_window = 12345  # hot duplicate retention; cold history remains permanent
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

// TestApplyHistoryConfig_CLIOverridesTOML asserts the precedence: the
// --prune.mode flag wins over a [history] mode in the TOML.
func TestApplyHistoryConfig_CLIOverridesTOML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	body := "[history]\nmode = \"archive\"\nprune_window = 99\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path, "--prune.mode", "full"})
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

// TestApplyHistoryConfig_ExplicitPruneModeFullEnablesHistory asserts the
// Erigon-style mode contract: an operator who explicitly requests full mode gets
// the recent-history capture needed for full-mode state retention. The no-flag
// default remains zero-cost and is covered by TestApplyHistoryConfig_DefaultsToFull.
func TestApplyHistoryConfig_ExplicitPruneModeFullEnablesHistory(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--prune.mode", "full"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if !cfg.HistoryEnabled {
		t.Error("--prune.mode=full did not turn on HistoryEnabled")
	}
}

// TestApplyHistoryConfig_FullModeEnabledIsReachable keeps the direct
// --history.enabled opt-in usable for operators who leave prune.mode unset but
// still want captured-and-pruned full-mode state history.
func TestApplyHistoryConfig_FullModeEnabledIsReachable(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--prune.mode", "full", "--history.enabled"})
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

func TestApplyHistoryConfig_TOMLModeFullEnablesHistory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	if err := os.WriteFile(path, []byte("[history]\nmode = \"full\"\n"), 0o600); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeFull {
		t.Errorf("mode = %q, want full", got)
	}
	if !cfg.HistoryEnabled {
		t.Fatal("[history] mode=full did not turn on HistoryEnabled")
	}
}

func TestApplyHistoryConfig_ExplicitModeOverridesHistoryDisabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gtron.toml")
	if err := os.WriteFile(path, []byte("[history]\nmode = \"blocks\"\nenabled = false\n"), 0o600); err != nil {
		t.Fatalf("write toml: %v", err)
	}
	ctx := makeHistoryFlagSet(t, []string{"--config", path})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeBlocks {
		t.Errorf("mode = %q, want blocks", got)
	}
	if !cfg.HistoryEnabled {
		t.Fatal("explicit prune mode should override enabled=false")
	}
}

func TestApplyHistoryConfig_CLIPruneModeOverridesHistoryDisabled(t *testing.T) {
	ctx := makeHistoryFlagSet(t, []string{"--prune.mode", "full", "--history.enabled=false"})
	cfg := &params.ChainConfig{}
	if err := applyHistoryConfig(ctx, cfg); err != nil {
		t.Fatalf("applyHistoryConfig: %v", err)
	}
	if got := cfg.EffectiveHistoryMode(); got != params.HistoryModeFull {
		t.Errorf("mode = %q, want full", got)
	}
	if !cfg.HistoryEnabled {
		t.Fatal("--prune.mode should override --history.enabled=false")
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
			name: "blocks history capture needs pruning",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeBlocks, HistoryEnabled: true},
			want: true,
		},
		{
			name: "minimal history capture needs pruning",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeMinimal, HistoryEnabled: true},
			want: true,
		},
		{
			name: "archive prunes only verified cold duplicates",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeArchive, HistoryEnabled: true},
			want: true,
		},
		{
			name: "snap history capture needs pruning",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeSnap, HistoryEnabled: true},
			want: true,
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

func TestShouldEnableChainFreezerSnapshotBuilder(t *testing.T) {
	for _, tc := range []struct {
		name             string
		mode             string
		ancientAvailable bool
		freezerEnabled   bool
		want             bool
	}{
		{name: "minimal with ancient freezer", mode: params.HistoryModeMinimal, ancientAvailable: true, freezerEnabled: true, want: true},
		{name: "minimal without ancient", mode: params.HistoryModeMinimal, freezerEnabled: true},
		{name: "minimal freezer disabled", mode: params.HistoryModeMinimal, ancientAvailable: true},
		{name: "full retains local ancient", mode: params.HistoryModeFull, ancientAvailable: true, freezerEnabled: true},
		{name: "snap retains local ancient", mode: params.HistoryModeSnap, ancientAvailable: true, freezerEnabled: true},
		{name: "blocks retains local ancient", mode: params.HistoryModeBlocks, ancientAvailable: true, freezerEnabled: true},
		{name: "archive retains local ancient", mode: params.HistoryModeArchive, ancientAvailable: true, freezerEnabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &params.ChainConfig{HistoryMode: tc.mode}
			if got := shouldEnableChainFreezerSnapshotBuilder(cfg, tc.ancientAvailable, tc.freezerEnabled); got != tc.want {
				t.Fatalf("shouldEnableChainFreezerSnapshotBuilder(%q, ancient=%v, freezer=%v) = %v, want %v", tc.mode, tc.ancientAvailable, tc.freezerEnabled, got, tc.want)
			}
		})
	}
}

func TestShouldDeferColdSnapshotHistoryWhileSyncing(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *params.ChainConfig
		want bool
	}{
		{name: "snap history", cfg: &params.ChainConfig{HistoryMode: params.HistoryModeSnap, HistoryEnabled: true}, want: true},
		{name: "archive history", cfg: &params.ChainConfig{HistoryMode: params.HistoryModeArchive, HistoryEnabled: true}, want: true},
		{name: "snap without capture", cfg: &params.ChainConfig{HistoryMode: params.HistoryModeSnap}},
		{name: "full history", cfg: &params.ChainConfig{HistoryMode: params.HistoryModeFull, HistoryEnabled: true}},
		{name: "nil config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldDeferColdSnapshotHistoryWhileSyncing(tc.cfg); got != tc.want {
				t.Fatalf("shouldDeferColdSnapshotHistoryWhileSyncing = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMaxDeferredColdHistoryBlocksSaturates(t *testing.T) {
	if got := maxDeferredColdHistoryBlocks(65_536); got != 262_144 {
		t.Fatalf("ordinary cap = %d, want 262144", got)
	}
	if got := maxDeferredColdHistoryBlocks(^uint64(0)); got != ^uint64(0) {
		t.Fatalf("overflow cap = %d, want MaxUint64", got)
	}
}

func TestMaxBusyDeferredColdHistoryBlocksSaturates(t *testing.T) {
	if got := maxBusyDeferredColdHistoryBlocks(262_144); got != 524_288 {
		t.Fatalf("ordinary hard cap = %d, want 524288", got)
	}
	if got := maxBusyDeferredColdHistoryBlocks(^uint64(0)); got != ^uint64(0) {
		t.Fatalf("overflow hard cap = %d, want MaxUint64", got)
	}
}

func TestSyncImporterMaintenanceAdmissionRequiresSustainedIdle(t *testing.T) {
	admission := newSyncImporterMaintenanceAdmission(30 * time.Second)
	now := time.Unix(100, 0)
	if !admission.Ready(tnet.SyncStatus{}, now) {
		t.Fatal("inactive importer should admit maintenance")
	}
	if !admission.Ready(tnet.SyncStatus{Active: true, Paused: true, BufferedBlocks: 10}, now) {
		t.Fatal("paused importer should admit maintenance")
	}

	idle := tnet.SyncStatus{Active: true, TargetHead: 1_000, AppliedTip: 100, SessionBlocks: 100}
	if admission.Ready(idle, now) {
		t.Fatal("first idle observation should establish, not satisfy, quiet period")
	}
	if admission.Ready(idle, now.Add(29*time.Second)) {
		t.Fatal("idle importer admitted before quiet period")
	}
	if !admission.Ready(idle, now.Add(30*time.Second)) {
		t.Fatal("sustained idle importer should admit after quiet period")
	}

	progressed := idle
	progressed.AppliedTip++
	progressed.SessionBlocks++
	if admission.Ready(progressed, now.Add(time.Minute)) {
		t.Fatal("new import progress should restart quiet period")
	}
	changedTarget := progressed
	changedTarget.TargetHead++
	if admission.Ready(changedTarget, now.Add(2*time.Minute)) {
		t.Fatal("sync target change should restart quiet period")
	}
	newSession := changedTarget
	newSession.SessionBlocks = 0
	if admission.Ready(newSession, now.Add(3*time.Minute)) {
		t.Fatal("sync session reset should restart quiet period")
	}
	busy := newSession
	busy.BufferedBlocks = 1
	if admission.Ready(busy, now.Add(4*time.Minute)) {
		t.Fatal("buffered foreground work should reject maintenance")
	}
	busy.BufferedBlocks = 0
	if admission.Ready(busy, now.Add(5*time.Minute)) {
		t.Fatal("first observation after foreground work should restart quiet period")
	}
}

func TestShouldEnableChainLookupPruner(t *testing.T) {
	tests := []struct {
		name string
		cfg  params.ChainConfig
		want bool
	}{
		{
			name: "plain full prunes chain lookups",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeFull},
			want: true,
		},
		{
			name: "blocks prunes chain lookups without history capture",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeBlocks},
			want: true,
		},
		{
			name: "minimal prunes chain lookups without history capture",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeMinimal},
			want: true,
		},
		{
			name: "snap prunes chain lookups",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeSnap, HistoryEnabled: true},
			want: true,
		},
		{
			name: "archive keeps chain lookups",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeArchive, HistoryEnabled: true},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldEnableChainLookupPruner(&tt.cfg); got != tt.want {
				t.Fatalf("shouldEnableChainLookupPruner = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldEnableChainFreezerTailPruner(t *testing.T) {
	tests := []struct {
		name string
		cfg  params.ChainConfig
		want bool
	}{
		{
			name: "full keeps local block freezer history",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeFull},
			want: false,
		},
		{
			name: "blocks keeps complete local block freezer history",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeBlocks},
			want: false,
		},
		{
			name: "minimal prunes local block freezer tail",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeMinimal},
			want: true,
		},
		{
			name: "snap keeps local block freezer history",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeSnap, HistoryEnabled: true},
			want: false,
		},
		{
			name: "archive keeps local block freezer history",
			cfg:  params.ChainConfig{HistoryMode: params.HistoryModeArchive, HistoryEnabled: true},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldEnableChainFreezerTailPruner(&tt.cfg); got != tt.want {
				t.Fatalf("shouldEnableChainFreezerTailPruner = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateAncientV2PruneMode(t *testing.T) {
	for _, mode := range []string{
		params.HistoryModeArchive,
		params.HistoryModeFull,
		params.HistoryModeBlocks,
		params.HistoryModeSnap,
	} {
		t.Run(mode, func(t *testing.T) {
			if err := validateAncientV2PruneMode(&params.ChainConfig{HistoryMode: mode}, 65_536); err != nil {
				t.Fatalf("validateAncientV2PruneMode: %v", err)
			}
		})
	}
	if err := validateAncientV2PruneMode(&params.ChainConfig{HistoryMode: params.HistoryModeMinimal}, 65_536); err == nil {
		t.Fatal("minimal mode accepted local Ancient V2 coverage")
	}
	if err := validateAncientV2PruneMode(&params.ChainConfig{HistoryMode: params.HistoryModeMinimal}, 0); err != nil {
		t.Fatalf("empty V2 coverage rejected: %v", err)
	}
}

func TestDomainStatePrunePolicyPreservesOperatorMode(t *testing.T) {
	tests := []struct {
		mode string
		want statepruning.Mode
	}{
		{mode: params.HistoryModeFull, want: statepruning.ModeFull},
		{mode: params.HistoryModeBlocks, want: statepruning.ModeBlocks},
		{mode: params.HistoryModeMinimal, want: statepruning.ModeMinimal},
		{mode: params.HistoryModeSnap, want: statepruning.ModeSnap},
		{mode: params.HistoryModeArchive, want: statepruning.ModeArchive},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			policy := domainStatePrunePolicy(&params.ChainConfig{
				HistoryMode:        tt.mode,
				HistoryPruneWindow: 10,
			}, 3)
			if policy.Mode != tt.want {
				t.Fatalf("policy mode = %q, want %q", policy.Mode, tt.want)
			}
			if policy.HistoryWindow != 10 || policy.ReorgWindow != 3 {
				t.Fatalf("policy windows = history:%d reorg:%d, want 10/3", policy.HistoryWindow, policy.ReorgWindow)
			}
		})
	}
}

func TestDomainStatePrunePolicyUsesModeDefaultWindow(t *testing.T) {
	tests := []struct {
		mode string
		want uint64
	}{
		{mode: params.HistoryModeFull, want: params.HistoryDefaultPruneWindow},
		{mode: params.HistoryModeBlocks, want: params.HistoryDefaultPruneWindow},
		{mode: params.HistoryModeSnap, want: params.HistoryColdDefaultPruneWindow},
		{mode: params.HistoryModeMinimal, want: params.HistoryMinimalDefaultPruneWindow},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			policy := domainStatePrunePolicy(&params.ChainConfig{
				HistoryMode: tt.mode,
			}, 64)
			if policy.HistoryWindow != tt.want {
				t.Fatalf("policy history window = %d, want %d", policy.HistoryWindow, tt.want)
			}
			if policy.ReorgWindow != 64 {
				t.Fatalf("policy reorg window = %d, want 64", policy.ReorgWindow)
			}
		})
	}
}

func TestDomainStatePrunerMaxSyncLag(t *testing.T) {
	policy := statepruning.FullPolicy(12345, 256)
	for _, mode := range []string{
		params.HistoryModeFull,
		params.HistoryModeBlocks,
		params.HistoryModeMinimal,
		params.HistoryModeSnap,
	} {
		t.Run(mode+" prunes while catching up", func(t *testing.T) {
			cfg := &params.ChainConfig{HistoryMode: mode}
			if got := domainStatePrunerMaxSyncLag(cfg, policy); got != 0 {
				t.Fatalf("max sync lag = %d, want disabled catch-up gate", got)
			}
		})
	}

	t.Run("archive retains catch-up guard", func(t *testing.T) {
		cfg := &params.ChainConfig{HistoryMode: params.HistoryModeArchive}
		if got := domainStatePrunerMaxSyncLag(cfg, policy); got != policy.HistoryWindow {
			t.Fatalf("max sync lag = %d, want %d", got, policy.HistoryWindow)
		}
	})

	t.Run("nil config is conservative", func(t *testing.T) {
		if got := domainStatePrunerMaxSyncLag(nil, policy); got != policy.HistoryWindow {
			t.Fatalf("max sync lag = %d, want %d", got, policy.HistoryWindow)
		}
	})
}

func TestDomainStatePrunePolicyCapsReorgWindow(t *testing.T) {
	policy := domainStatePrunePolicy(&params.ChainConfig{
		HistoryMode:        params.HistoryModeMinimal,
		HistoryPruneWindow: 2,
	}, 10)
	if policy.Mode != statepruning.ModeMinimal || policy.HistoryWindow != 2 || policy.ReorgWindow != 2 {
		t.Fatalf("policy = %+v, want minimal with capped 2/2 windows", policy)
	}
}

func TestEnsureHistoryPruneModeLockedWritesInitialMode(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	if err := ensureHistoryPruneModeLocked(db, params.HistoryModeArchive); err != nil {
		t.Fatalf("ensureHistoryPruneModeLocked: %v", err)
	}
	mode, ok, err := rawdb.ReadHistoryPruneMode(db)
	if err != nil || !ok || mode != params.HistoryModeArchive {
		t.Fatalf("stored prune mode: mode=%q ok=%v err=%v", mode, ok, err)
	}
}

func TestEnsureHistoryPruneModeLockedAllowsSameMode(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteHistoryPruneMode(db, params.HistoryModeSnap); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}

	if err := ensureHistoryPruneModeLocked(db, params.HistoryModeSnap); err != nil {
		t.Fatalf("ensureHistoryPruneModeLocked same mode: %v", err)
	}
}

func TestEnsureHistoryPruneModeLockedRejectsModeChange(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteHistoryPruneMode(db, params.HistoryModeArchive); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}

	err := ensureHistoryPruneModeLocked(db, params.HistoryModeFull)
	if err == nil {
		t.Fatal("expected prune mode mismatch error")
	}
	if !strings.Contains(err.Error(), `stored "archive"`) || !strings.Contains(err.Error(), `requested "full"`) {
		t.Fatalf("unexpected mismatch error: %v", err)
	}
}

func TestEnsureHistoryPruneModeLockedAllowsArchiveColdHistoryPruneStages(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteHistoryPruneMode(db, params.HistoryModeArchive); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotHotPrune, 12); err != nil {
		t.Fatalf("write hot prune stage: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotPrune, 12); err != nil {
		t.Fatalf("write prune stage: %v", err)
	}

	if err := ensureHistoryPruneModeLocked(db, params.HistoryModeArchive); err != nil {
		t.Fatalf("archive cold-history prune stages rejected: %v", err)
	}
}

func TestEnsureHistoryPruneModeLockedRejectsArchiveChainLookupPruneStage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteHistoryPruneMode(db, params.HistoryModeArchive); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 12); err != nil {
		t.Fatalf("write chain lookup prune stage: %v", err)
	}

	err := ensureHistoryPruneModeLocked(db, params.HistoryModeArchive)
	if err == nil || !strings.Contains(err.Error(), "archive-prune-stage") || !strings.Contains(err.Error(), string(rawdb.StageSnapshotChainLookupPrune)) {
		t.Fatalf("archive chain lookup prune conflict error = %v", err)
	}
}

func TestEnsureHistoryPruneModeLockedRejectsLegacyArchivePruneStageBeforePersist(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 7); err != nil {
		t.Fatalf("write chain lookup prune stage: %v", err)
	}

	err := ensureHistoryPruneModeLocked(db, params.HistoryModeArchive)
	if err == nil {
		t.Fatal("expected archive prune stage conflict")
	}
	if _, ok, readErr := rawdb.ReadHistoryPruneMode(db); readErr != nil || ok {
		t.Fatalf("prune mode persisted despite startup conflict: ok=%v err=%v", ok, readErr)
	}
}

func TestEnsureHistoryPruneModeLockedRejectsTailPruneOutsideMinimal(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteHistoryPruneMode(db, params.HistoryModeBlocks); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainFreezerTailPrune, 5); err != nil {
		t.Fatalf("write tail prune stage: %v", err)
	}

	err := ensureHistoryPruneModeLocked(db, params.HistoryModeBlocks)
	if err == nil {
		t.Fatal("expected tail prune mode conflict")
	}
	if !strings.Contains(err.Error(), "tail-prune-mode-mismatch") || !strings.Contains(err.Error(), string(rawdb.StageSnapshotChainFreezerTailPrune)) {
		t.Fatalf("unexpected tail prune conflict error: %v", err)
	}
}

func TestEnsureHistoryPruneModeLockedAllowsTailPruneInMinimal(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteHistoryPruneMode(db, params.HistoryModeMinimal); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainFreezerTailPrune, 5); err != nil {
		t.Fatalf("write tail prune stage: %v", err)
	}

	if err := ensureHistoryPruneModeLocked(db, params.HistoryModeMinimal); err != nil {
		t.Fatalf("minimal tail prune should be allowed: %v", err)
	}
}
