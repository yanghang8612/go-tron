package main

import (
	"fmt"
	"path/filepath"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/urfave/cli/v2"
)

func openPebbleDB(ctx *cli.Context, path string) (ethdb.KeyValueStore, error) {
	cache, handles, tune, err := makePebbleConfig(ctx)
	if err != nil {
		return nil, err
	}
	hot, err := rawdb.NewPebbleDBWithOptions(path, cache, handles, tune)
	if err != nil {
		return nil, err
	}
	marked, err := rawdb.DirectHistoryLayoutInitialized(hot)
	if err != nil {
		hot.Close()
		return nil, err
	}
	if !marked {
		_, paired, err := rawdb.ReadDirectHistoryPair(hot)
		if err != nil || paired {
			hot.Close()
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("history conversion is incomplete; continue db move-history first")
		}
		return hot, nil
	}
	historyPath := ctx.String(historyStoreFlag.Name)
	if historyPath == "" {
		historyPath = filepath.Join(filepath.Dir(path), "history")
	}
	history, err := rawdb.NewDirectHistoryPebbleDB(historyPath, max(16, cache/8), max(16, handles/8), false)
	if err != nil {
		hot.Close()
		return nil, err
	}
	if err := rawdb.EnsureDirectHistoryPair(hot, history, false); err != nil {
		history.Close()
		hot.Close()
		return nil, err
	}
	historyMarked, err := rawdb.DirectHistoryLayoutInitialized(history)
	if err != nil || !historyMarked {
		history.Close()
		hot.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("history database is missing its completed layout marker")
	}
	return rawdb.NewDirectHistoryStore(hot, history), nil
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
	targetFileMiB := uint64FlagOrDefault(ctx, "db.target-file-size", dbTargetFileSizeFlag.Value)
	if targetFileMiB == 0 || targetFileMiB > uint64(^uint64(0)>>1)/(1024*1024*64) {
		return 0, 0, rawdb.PebbleOptions{}, fmt.Errorf("--db.target-file-size must be a positive int64 MiB value")
	}
	lbaseMaxMiB := uint64FlagOrDefault(ctx, "db.lbase-max-size", dbLBaseMaxSizeFlag.Value)
	if lbaseMaxMiB == 0 || lbaseMaxMiB > uint64(^uint64(0)>>1)/(1024*1024) {
		return 0, 0, rawdb.PebbleOptions{}, fmt.Errorf("--db.lbase-max-size must be a positive int64 MiB value")
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
	tune.TargetFileSizeBytes = int64(targetFileMiB * 1024 * 1024)
	tune.LBaseMaxBytes = int64(lbaseMaxMiB * 1024 * 1024)
	tune.L0CompactionThreshold = l0Compact
	tune.L0StopWritesThreshold = l0Stop
	return cache, handles, tune, nil
}

func makeStateDatabaseConfig(ctx *cli.Context) (state.DatabaseConfig, error) {
	trieCacheMiB := intFlagOrDefault(ctx, "state.trie.cache", stateTrieCacheFlag.Value)
	if trieCacheMiB < -1 {
		return state.DatabaseConfig{}, fmt.Errorf("--state.trie.cache must be >= -1")
	}
	if trieCacheMiB == -1 {
		dbCacheMiB := intFlagOrDefault(ctx, "db.cache", dbCacheFlag.Value)
		trieCacheMiB = dbCacheMiB / 8
		if trieCacheMiB < 32 {
			trieCacheMiB = 32
		}
		if trieCacheMiB > 1024 {
			trieCacheMiB = 1024
		}
	}
	codeCacheMiB := intFlagOrDefault(ctx, "state.code.cache", stateCodeCacheFlag.Value)
	if codeCacheMiB < 0 {
		return state.DatabaseConfig{}, fmt.Errorf("--state.code.cache must be >= 0")
	}
	return state.DatabaseConfig{
		CleanTrieCacheSizeBytes: trieCacheMiB * 1024 * 1024,
		CodeCacheSizeBytes:      codeCacheMiB * 1024 * 1024,
	}, nil
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
