package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
	tnet "github.com/tronprotocol/go-tron/net"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

// applyHistoryConfig wires the operator-level flat temporal-state retention
// settings into a chain config. Precedence (highest first):
//
//  1. --prune.mode CLI flag
//  2. [history] section in the TOML file (when --config is set)
//  3. params.ChainConfig defaults
//
// Slice 5 deliberately keeps this surface tiny: two scalars (mode,
// prune_window) and no nesting. A future "wider node config TOML" slice
// can hoist this into a richer loader; until then a focused
// section-parser keeps the dep tree clean.
//
// applyHistoryConfig also turns HistoryEnabled on whenever the operator has
// explicitly selected an Erigon-style prune mode OR has explicitly opted in via
// --history.enabled / [history] enabled. An unset mode still defaults to "full"
// without enabling capture, preserving the legacy zero-cost default. Once an
// operator explicitly writes a mode, that mode owns the expected history
// retention semantics instead of being only a label.
//
// Precedence for the enable toggle: --history.enabled CLI flag (when set)
// overrides [history] enabled TOML. A later explicit prune-mode implication can
// still force capture back on, because a requested retention mode without
// capture is operationally inconsistent. The function returns an error only
// when the TOML file exists but is malformed.
func applyHistoryConfig(ctx *cli.Context, cfg *params.ChainConfig) error {
	if cfg == nil {
		return nil
	}

	// Step 1: load [history] from the TOML config file when present.
	tomlMode, tomlWindow, tomlEnabled, tomlPresent, err := loadHistoryTOML(ctx.String("config"))
	if err != nil {
		return err
	}
	if tomlPresent {
		if tomlMode != "" {
			cfg.HistoryMode = tomlMode
		}
		if tomlWindow > 0 {
			cfg.HistoryPruneWindow = tomlWindow
		}
		if tomlEnabled != nil {
			cfg.HistoryEnabled = *tomlEnabled
		}
	}

	// Step 2: CLI flags override the TOML. cli/v2 treats flags with a
	// default value as "set" even when the user didn't pass them; we
	// detect explicit setting via IsSet so a TOML value is not trampled.
	if mode, ok, err := historyModeFromFlags(ctx); err != nil {
		return err
	} else if ok {
		cfg.HistoryMode = mode
	}
	if ctx.IsSet("history.enabled") {
		cfg.HistoryEnabled = ctx.Bool("history.enabled")
	}

	// Step 3: explicit retention modes imply temporal capture. Without
	// HistoryEnabled the on-disk history stays empty and the mode cannot deliver
	// the state-retention behavior it advertises. Archive/snap also imply
	// capture when sourced from chain defaults because they are never meaningful
	// without history rows.
	modeExplicit := (tomlPresent && tomlMode != "") || modeFlagExplicit(ctx)
	switch {
	case modeExplicit:
		cfg.HistoryEnabled = true
	case cfg.EffectiveHistoryMode() == params.HistoryModeArchive || cfg.EffectiveHistoryMode() == params.HistoryModeSnap:
		cfg.HistoryEnabled = true
	}
	return nil
}

func modeFlagExplicit(ctx *cli.Context) bool {
	return ctx != nil && ctx.IsSet("prune.mode")
}

func historyModeFromFlags(ctx *cli.Context) (string, bool, error) {
	if ctx == nil {
		return "", false, nil
	}
	if !ctx.IsSet("prune.mode") {
		return "", false, nil
	}
	mode, err := normaliseHistoryMode(ctx.String("prune.mode"))
	return mode, true, err
}

func shouldEnableDomainStatePruner(cfg *params.ChainConfig) bool {
	if cfg == nil {
		return false
	}
	switch cfg.EffectiveHistoryMode() {
	case params.HistoryModeFull, params.HistoryModeSnap, params.HistoryModeArchive, params.HistoryModeBlocks, params.HistoryModeMinimal:
	default:
		return false
	}
	return cfg.HistoryEnabled
}

func shouldEnableChainLookupPruner(cfg *params.ChainConfig) bool {
	if cfg == nil {
		return false
	}
	switch cfg.EffectiveHistoryMode() {
	case params.HistoryModeFull, params.HistoryModeSnap, params.HistoryModeBlocks, params.HistoryModeMinimal:
		return true
	default:
		return false
	}
}

func shouldEnableChainFreezerTailPruner(cfg *params.ChainConfig) bool {
	return cfg != nil && cfg.EffectiveHistoryMode() == params.HistoryModeMinimal
}

func validateAncientV2PruneMode(cfg *params.ChainConfig, coverage uint64) error {
	if coverage == 0 || cfg == nil || cfg.EffectiveHistoryMode() != params.HistoryModeMinimal {
		return nil
	}
	return fmt.Errorf("datadir has local Ancient V2 coverage [0,%d), which is incompatible with minimal-mode freezer tail pruning; use the persisted full/blocks mode or migrate through a verified cold snapshot", coverage)
}

// shouldEnableChainFreezerSnapshotBuilder keeps the duplicate cold
// chain-freezer snapshot files limited to minimal mode. Other modes retain the
// local ancient source, so publishing a second full chain copy would consume
// more disk than the lookup rows it could later replace. Minimal mode is the
// only mode that advances the local freezer tail after verified cold coverage
// is available, making this build path a net storage reduction.
func shouldEnableChainFreezerSnapshotBuilder(cfg *params.ChainConfig, ancientAvailable, freezerEnabled bool) bool {
	return ancientAvailable && freezerEnabled && shouldEnableChainFreezerTailPruner(cfg)
}

// shouldDeferColdSnapshotHistoryWhileSyncing lets cold-backed modes accumulate
// a bounded hot-history backlog while deep sync owns the foreground. The cold
// builder's MaxDeferredHistoryBlocks high-water mark forces accelerated passes
// before that backlog can grow without bound; full, blocks, and minimal do not
// run this state-history snapshot builder.
func shouldDeferColdSnapshotHistoryWhileSyncing(cfg *params.ChainConfig) bool {
	if cfg == nil {
		return false
	}
	switch cfg.EffectiveHistoryMode() {
	case params.HistoryModeSnap, params.HistoryModeArchive:
		return cfg.HistoryEnabled
	default:
		return false
	}
}

func maxDeferredColdHistoryBlocks(historyWindow uint64) uint64 {
	const multiplier = uint64(4)
	if historyWindow > ^uint64(0)/multiplier {
		return ^uint64(0)
	}
	return historyWindow * multiplier
}

func maxBusyDeferredColdHistoryBlocks(maxDeferred uint64) uint64 {
	const multiplier = uint64(2)
	if maxDeferred > ^uint64(0)/multiplier {
		return ^uint64(0)
	}
	return maxDeferred * multiplier
}

// syncImporterMaintenanceAdmission requires an active downloader to remain
// supply-idle across observations before admitting cold history. Comparing the
// monotonic session progress prevents a transient empty buffer between import
// batches from looking like sustained spare capacity. The builder's hard
// backlog cap remains the independent liveness fallback.
type syncImporterMaintenanceAdmission struct {
	mu          sync.Mutex
	quietPeriod time.Duration
	idleSince   time.Time
	observed    bool
	targetHead  uint64
	appliedTip  uint64
	blocks      int
}

func newSyncImporterMaintenanceAdmission(quietPeriod time.Duration) *syncImporterMaintenanceAdmission {
	if quietPeriod < 0 {
		quietPeriod = 0
	}
	return &syncImporterMaintenanceAdmission{quietPeriod: quietPeriod}
}

func (a *syncImporterMaintenanceAdmission) Ready(status tnet.SyncStatus, now time.Time) bool {
	if a == nil {
		return true
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !status.Active || status.Paused {
		a.idleSince = time.Time{}
		a.observed = false
		return true
	}
	progressUnchanged := a.observed && a.targetHead == status.TargetHead && a.appliedTip == status.AppliedTip && a.blocks == status.SessionBlocks
	a.observed = true
	a.targetHead = status.TargetHead
	a.appliedTip = status.AppliedTip
	a.blocks = status.SessionBlocks
	if status.BufferedBlocks > 0 || status.Inflight > 0 {
		a.idleSince = time.Time{}
		return false
	}
	if !progressUnchanged || a.idleSince.IsZero() || now.Before(a.idleSince) {
		a.idleSince = now
		return false
	}
	return now.Sub(a.idleSince) >= a.quietPeriod
}

func domainStatePrunePolicy(cfg *params.ChainConfig, targetReorgWindow uint64) statepruning.Policy {
	historyWindow := params.HistoryDefaultPruneWindow
	mode := params.HistoryModeFull
	if cfg != nil {
		historyWindow = cfg.EffectiveHistoryPruneWindow()
		mode = cfg.EffectiveHistoryMode()
	}
	reorgWindow := targetReorgWindow
	if reorgWindow == 0 || historyWindow < reorgWindow {
		reorgWindow = historyWindow
	}
	switch mode {
	case params.HistoryModeArchive:
		return statepruning.ArchiveColdPolicy(historyWindow, reorgWindow)
	case params.HistoryModeBlocks:
		return statepruning.BlocksPolicy(historyWindow, reorgWindow)
	case params.HistoryModeMinimal:
		return statepruning.MinimalPolicy(historyWindow, reorgWindow)
	case params.HistoryModeSnap:
		return statepruning.SnapPolicy(historyWindow, reorgWindow)
	default:
		return statepruning.FullPolicy(historyWindow, reorgWindow)
	}
}

// domainStatePrunerMaxSyncLag controls whether the background pruner pauses
// while the node is catching up. Full, blocks, and minimal modes delete hot
// history solely by their local retention window, so pruning old rows remains
// safe during sync and prevents a long replay from accumulating an unbounded
// hot history backlog. Snap mode is also safe because it only deletes history
// already covered by verified, published cold segments. Archive keeps the
// catch-up guard so cold construction does not compete with a far-behind
// importer unless the operator explicitly chooses snap mode.
func domainStatePrunerMaxSyncLag(cfg *params.ChainConfig, policy statepruning.Policy) uint64 {
	if cfg == nil {
		return policy.HistoryWindow
	}
	switch cfg.EffectiveHistoryMode() {
	case params.HistoryModeFull, params.HistoryModeBlocks, params.HistoryModeMinimal, params.HistoryModeSnap:
		return 0
	default:
		return policy.HistoryWindow
	}
}

func ensureHistoryPruneModeLocked(db ethdb.KeyValueStore, requested string) error {
	mode, err := normaliseHistoryMode(requested)
	if err != nil {
		return err
	}
	stored, ok, err := rawdb.ReadHistoryPruneMode(db)
	if err != nil {
		return fmt.Errorf("read persisted prune mode: %w", err)
	}
	if !ok {
		if err := ensureHistoryPruneModeStagesCompatible(db, mode); err != nil {
			return err
		}
		if err := rawdb.WriteHistoryPruneMode(db, mode); err != nil {
			return fmt.Errorf("persist prune mode: %w", err)
		}
		return nil
	}
	storedMode, err := normaliseHistoryMode(stored)
	if err != nil {
		return fmt.Errorf("persisted prune mode %q is invalid: %w", stored, err)
	}
	if storedMode != mode {
		return fmt.Errorf("datadir prune mode mismatch: stored %q, requested %q; use --prune.mode=%s or a fresh datadir", storedMode, mode, storedMode)
	}
	if stored != storedMode {
		if err := rawdb.WriteHistoryPruneMode(db, storedMode); err != nil {
			return fmt.Errorf("canonicalise persisted prune mode: %w", err)
		}
	}
	if err := ensureHistoryPruneModeStagesCompatible(db, storedMode); err != nil {
		return err
	}
	return nil
}

type historyPruneModeStageConflict struct {
	kind   string
	detail string
}

func ensureHistoryPruneModeStagesCompatible(db ethdb.KeyValueReader, mode string) error {
	conflicts, err := historyPruneModeStageConflicts(db, mode)
	if err != nil {
		return err
	}
	if len(conflicts) == 0 {
		return nil
	}
	return fmt.Errorf("datadir prune mode %q conflicts with stage progress: %s", mode, historyPruneModeStageConflictSummary(conflicts))
}

func historyPruneModeStageConflicts(db ethdb.KeyValueReader, mode string) ([]historyPruneModeStageConflict, error) {
	var conflicts []historyPruneModeStageConflict
	for _, stage := range historyPruneModeConflictStages(mode) {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			return nil, fmt.Errorf("read %s stage progress for prune mode %s: %w", stage, mode, err)
		}
		if !ok {
			continue
		}
		kind, detail, ok := historyPruneModeStageConflictFor(mode, stage, row.BlockNum)
		if ok {
			conflicts = append(conflicts, historyPruneModeStageConflict{kind: kind, detail: detail})
		}
	}
	return conflicts, nil
}

func historyPruneModeConflictStages(mode string) []rawdb.StageID {
	switch mode {
	case params.HistoryModeArchive:
		return historyPruneModeArchiveForbiddenStages()
	case params.HistoryModeMinimal:
		return nil
	default:
		return []rawdb.StageID{rawdb.StageSnapshotChainFreezerTailPrune}
	}
}

func historyPruneModeArchiveForbiddenStages() []rawdb.StageID {
	return []rawdb.StageID{
		rawdb.StageSnapshotChainLookupPrune,
		rawdb.StageSnapshotSectionBloomPrune,
		rawdb.StageSnapshotBalanceTracePrune,
		rawdb.StageSnapshotChainFreezerTailPrune,
	}
}

func historyPruneModeStageConflictFor(mode string, stage rawdb.StageID, block uint64) (string, string, bool) {
	if mode == params.HistoryModeArchive {
		for _, forbidden := range historyPruneModeArchiveForbiddenStages() {
			if stage == forbidden {
				return "archive-prune-stage", fmt.Sprintf("archive mode must not have %s progress at block %d", stage, block), true
			}
		}
	}
	if mode != params.HistoryModeMinimal && mode != params.HistoryModeArchive && stage == rawdb.StageSnapshotChainFreezerTailPrune {
		return "tail-prune-mode-mismatch", fmt.Sprintf("mode %s must not have minimal-only %s progress at block %d", mode, rawdb.StageSnapshotChainFreezerTailPrune, block), true
	}
	return "", "", false
}

func historyPruneModeStageConflictSummary(conflicts []historyPruneModeStageConflict) string {
	if len(conflicts) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(conflicts))
	for _, conflict := range conflicts {
		parts = append(parts, conflict.kind+": "+conflict.detail)
	}
	return strings.Join(parts, "; ")
}

// normaliseHistoryMode validates a user-supplied --prune.mode value.
// Unknown values are a hard error rather than a silent fallback so a typo
// doesn't degrade an archive node to full mode without warning.
func normaliseHistoryMode(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", params.HistoryModeFull:
		return params.HistoryModeFull, nil
	case params.HistoryModeBlocks:
		return params.HistoryModeBlocks, nil
	case params.HistoryModeMinimal:
		return params.HistoryModeMinimal, nil
	case params.HistoryModeSnap:
		return params.HistoryModeSnap, nil
	case params.HistoryModeArchive:
		return params.HistoryModeArchive, nil
	default:
		return "", fmt.Errorf("unknown prune mode %q (want full|blocks|minimal|snap|archive)", s)
	}
}

// loadHistoryTOML reads the [history] section out of the operator's
// config file. The parser is intentionally narrow — it understands a
// single [history] section header, key=value lines with bare strings or
// integers, comments starting with '#', and blank lines. Anything else
// is ignored (not an error) so a richer TOML in the same file (added by
// a future slice) doesn't break this loader.
//
// Returns (mode, window, enabled, present, err):
//   - present=false when path is empty or the file has no [history]
//     section
//   - mode is the literal value before normalisation; the caller runs
//     normaliseHistoryMode after applying CLI precedence
//   - window is the parsed prune_window (uint64); 0 means "absent"
//   - enabled is a tri-state *bool: nil means the key was absent (leave
//     cfg.HistoryEnabled untouched), non-nil carries the explicit value
//   - a non-empty path must exist; an explicit --config typo is a hard
//     startup error rather than a silent fallback to defaults
//
// The narrow contract avoids pulling in a TOML library for three scalars.
// A future slice that needs deeply-nested config can swap this for a
// real parser without changing the call site.
func loadHistoryTOML(path string) (string, uint64, *bool, bool, error) {
	if path == "" {
		return "", 0, nil, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return "", 0, nil, false, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	inSection := false
	sawSection := false
	var mode string
	var window uint64
	var enabled *bool
	for lineNum := 1; scanner.Scan(); lineNum++ {
		line := strings.TrimSpace(scanner.Text())
		// Strip trailing comments. Quotes within keys are not supported
		// — slice 5's TOML schema is scalars, no string values
		// containing '#'.
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section := strings.TrimSpace(line[1 : len(line)-1])
			inSection = (section == "history")
			if inSection {
				sawSection = true
			}
			continue
		}
		if !inSection {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			return "", 0, nil, false, fmt.Errorf("config %s:%d: expected key = value in [history]", path, lineNum)
		}
		key := strings.TrimSpace(line[:eq])
		value := strings.TrimSpace(line[eq+1:])
		// Strip surrounding quotes — TOML allows "string" or 'string'.
		value = trimMatching(value, '"')
		value = trimMatching(value, '\'')
		switch key {
		case "mode":
			mode = value
		case "prune_window":
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return "", 0, nil, false, fmt.Errorf("config %s:%d: prune_window: %w", path, lineNum, err)
			}
			window = n
		case "enabled":
			b, err := strconv.ParseBool(value)
			if err != nil {
				return "", 0, nil, false, fmt.Errorf("config %s:%d: enabled: %w", path, lineNum, err)
			}
			enabled = &b
		default:
			// Unknown keys in [history] are ignored rather than
			// rejected so a forward-compatible TOML written by a
			// newer gtron doesn't break old binaries.
		}
	}
	if err := scanner.Err(); err != nil {
		return "", 0, nil, false, fmt.Errorf("config %s: %w", path, err)
	}
	if mode != "" {
		normalised, err := normaliseHistoryMode(mode)
		if err != nil {
			return "", 0, nil, false, fmt.Errorf("config %s: %w", path, err)
		}
		mode = normalised
	}
	return mode, window, enabled, sawSection, nil
}

// trimMatching removes a matching pair of surrounding quote runes. Used
// so [history] mode = "archive" parses the same as mode = archive.
func trimMatching(s string, q byte) string {
	if len(s) >= 2 && s[0] == q && s[len(s)-1] == q {
		return s[1 : len(s)-1]
	}
	return s
}
