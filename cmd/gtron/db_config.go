package main

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func openPebbleDB(ctx *cli.Context, path string) (ethdb.KeyValueStore, error) {
	cache, handles, tune, err := makePebbleConfig(ctx)
	if err != nil {
		return nil, err
	}
	return rawdb.NewPebbleDBWithOptions(path, cache, handles, tune)
}

func makePebbleConfig(ctx *cli.Context) (int, int, rawdb.PebbleOptions, error) {
	cache := intFlagOrDefault(ctx, "db.cache", dbCacheFlag.Value)
	if cache <= 0 {
		return 0, 0, rawdb.PebbleOptions{}, fmt.Errorf("--db.cache must be positive")
	}
	handles := intFlagOrDefault(ctx, "db.handles", dbHandlesFlag.Value)
	if handles <= 0 {
		return 0, 0, rawdb.PebbleOptions{}, fmt.Errorf("--db.handles must be positive")
	}
	memtableMiB := uint64FlagOrDefault(ctx, "db.memtable", dbMemtableFlag.Value)
	if memtableMiB == 0 {
		return 0, 0, rawdb.PebbleOptions{}, fmt.Errorf("--db.memtable must be positive")
	}
	l0Compact := intFlagOrDefault(ctx, "db.l0.compact", dbL0CompactionFlag.Value)
	if l0Compact <= 0 {
		return 0, 0, rawdb.PebbleOptions{}, fmt.Errorf("--db.l0.compact must be positive")
	}
	l0Stop := intFlagOrDefault(ctx, "db.l0.stop", dbL0StopFlag.Value)
	if l0Stop < l0Compact {
		return 0, 0, rawdb.PebbleOptions{}, fmt.Errorf("--db.l0.stop must be >= --db.l0.compact")
	}
	tune := rawdb.DefaultPebbleOptions()
	tune.MemTableSizeBytes = memtableMiB * 1024 * 1024
	tune.L0CompactionThreshold = l0Compact
	tune.L0StopWritesThreshold = l0Stop
	return cache, handles, tune, nil
}

func intFlagOrDefault(ctx *cli.Context, name string, fallback int) int {
	if ctx.IsSet(name) {
		return ctx.Int(name)
	}
	value := ctx.Int(name)
	if value == 0 {
		return fallback
	}
	return value
}

func uint64FlagOrDefault(ctx *cli.Context, name string, fallback uint64) uint64 {
	if ctx.IsSet(name) {
		return ctx.Uint64(name)
	}
	value := ctx.Uint64(name)
	if value == 0 {
		return fallback
	}
	return value
}
