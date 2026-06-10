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

func applyStatePrefetchConfig(ctx *cli.Context, cfg *params.ChainConfig) error {
	if cfg == nil {
		return nil
	}

	tomlEnabled, tomlWorkers, tomlLookahead, tomlPresent, err := loadStatePrefetchTOML(ctx.String("config"))
	if err != nil {
		return err
	}
	if tomlPresent {
		if tomlEnabled != nil {
			cfg.StatePrefetchEnabled = *tomlEnabled
		}
		if tomlWorkers != nil {
			cfg.StatePrefetchWorkers = *tomlWorkers
		}
		if tomlLookahead != nil {
			cfg.StatePrefetchLookahead = *tomlLookahead
		}
	}

	if ctx.IsSet("state.prefetch.enabled") {
		cfg.StatePrefetchEnabled = ctx.Bool("state.prefetch.enabled")
	}
	if ctx.IsSet("state.prefetch.workers") {
		cfg.StatePrefetchWorkers = ctx.Int("state.prefetch.workers")
	}
	if ctx.IsSet("state.prefetch.lookahead") {
		cfg.StatePrefetchLookahead = ctx.Int("state.prefetch.lookahead")
	}
	if cfg.StatePrefetchWorkers < 0 {
		return fmt.Errorf("state.prefetch.workers must be >= 0")
	}
	if cfg.StatePrefetchLookahead < 0 {
		return fmt.Errorf("state.prefetch.lookahead must be >= 0")
	}
	return nil
}

func loadStatePrefetchTOML(path string) (enabled *bool, workers *int, lookahead *int, present bool, err error) {
	if path == "" {
		return nil, nil, nil, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	inSection := false
	for lineNum := 1; scanner.Scan(); lineNum++ {
		line := strings.TrimSpace(scanner.Text())
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section := strings.TrimSpace(line[1 : len(line)-1])
			inSection = (section == "state.prefetch")
			if inSection {
				present = true
			}
			continue
		}
		if !inSection {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			return nil, nil, nil, false, fmt.Errorf("config %s:%d: expected key = value in [state.prefetch]", path, lineNum)
		}
		key := strings.TrimSpace(line[:eq])
		value := strings.TrimSpace(line[eq+1:])
		value = trimMatching(value, '"')
		value = trimMatching(value, '\'')
		switch key {
		case "enabled":
			b, err := strconv.ParseBool(value)
			if err != nil {
				return nil, nil, nil, false, fmt.Errorf("config %s:%d: state.prefetch.enabled: %w", path, lineNum, err)
			}
			enabled = &b
		case "workers":
			n, err := parseNonNegativeStatePrefetchInt(value)
			if err != nil {
				return nil, nil, nil, false, fmt.Errorf("config %s:%d: state.prefetch.workers: %w", path, lineNum, err)
			}
			workers = &n
		case "lookahead":
			n, err := parseNonNegativeStatePrefetchInt(value)
			if err != nil {
				return nil, nil, nil, false, fmt.Errorf("config %s:%d: state.prefetch.lookahead: %w", path, lineNum, err)
			}
			lookahead = &n
		default:
			// Unknown keys are ignored for forward-compatible operator configs.
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, nil, false, fmt.Errorf("config %s: %w", path, err)
	}
	return enabled, workers, lookahead, present, nil
}

func parseNonNegativeStatePrefetchInt(value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("must be >= 0")
	}
	return n, nil
}
