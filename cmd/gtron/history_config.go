package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

// applyHistoryConfig wires the operator-level State History Index
// retention settings into a chain config. Precedence (highest first):
//
//  1. --gcmode CLI flag
//  2. [history] section in the TOML file (when --config is set)
//  3. params.ChainConfig defaults
//
// Slice 5 deliberately keeps this surface tiny: two scalars (mode,
// prune_window) and no nesting. A future "wider node config TOML" slice
// can hoist this into a richer loader; until then a focused
// section-parser keeps the dep tree clean.
//
// applyHistoryConfig also turns HistoryEnabled on whenever the operator
// has explicitly asked for archive mode (an archive node with the capture
// path disabled would silently produce an empty index) OR has explicitly
// opted in via --history.enabled / [history] enabled. Full mode is inert
// without one of those: the pruner Lifecycle only registers when
// HistoryEnabled && mode==full, so `--gcmode=full --history.enabled` is
// the canonical way to run a pruned-history node. Plain full mode with no
// opt-in stays the zero-cost default (no capture, no pruning).
//
// Precedence for the enable toggle: --history.enabled CLI flag (when set)
// overrides [history] enabled TOML, which overrides the archive-implied
// default. The function returns an error only when the TOML file exists
// but is malformed.
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

	// Step 2: CLI flag overrides the TOML. cli/v2 treats flags with a
	// default value as "set" even when the user didn't pass them; we
	// detect explicit setting via IsSet so the TOML's value isn't
	// trampled by the flag default.
	if ctx.IsSet("gcmode") {
		mode, err := normaliseHistoryMode(ctx.String("gcmode"))
		if err != nil {
			return err
		}
		cfg.HistoryMode = mode
	}
	if ctx.IsSet("history.enabled") {
		cfg.HistoryEnabled = ctx.Bool("history.enabled")
	}

	// Step 3: archive mode implicitly turns on the capture path even when
	// the operator didn't pass --history.enabled. Without HistoryEnabled
	// the on-disk index stays empty and an archive-query RPC would
	// silently return live state for every blockNum — operationally
	// broken. An explicit --history.enabled=false in archive mode is
	// contradictory; archive wins (the index the operator asked to keep
	// forever must actually be captured).
	if cfg.EffectiveHistoryMode() == params.HistoryModeArchive {
		cfg.HistoryEnabled = true
	}
	return nil
}

// normaliseHistoryMode validates a user-supplied --gcmode value. The
// canonical strings are "full" and "archive"; anything else is a hard
// error rather than a silent fallback so a typo doesn't degrade an
// archive node to full mode without warning.
func normaliseHistoryMode(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", params.HistoryModeFull:
		return params.HistoryModeFull, nil
	case params.HistoryModeArchive:
		return params.HistoryModeArchive, nil
	default:
		return "", fmt.Errorf("--gcmode: unknown value %q (want full|archive)", s)
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
