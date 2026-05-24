package main

import (
	"flag"
	"testing"

	"github.com/urfave/cli/v2"
)

func makeDBFlagSet(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{dbCacheFlag, dbHandlesFlag, dbMemtableFlag, dbL0CompactionFlag, dbL0StopFlag, stateTrieCacheFlag}
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	for _, f := range app.Flags {
		if err := f.Apply(set); err != nil {
			t.Fatalf("apply flag: %v", err)
		}
	}
	if err := set.Parse(argv); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return cli.NewContext(app, set, nil)
}

func TestMakePebbleConfigDefaults(t *testing.T) {
	ctx := makeDBFlagSet(t, nil)

	cache, handles, tune, err := makePebbleConfig(ctx)
	if err != nil {
		t.Fatalf("makePebbleConfig: %v", err)
	}
	if cache != 256 || handles != 500 {
		t.Fatalf("defaults: cache=%d handles=%d", cache, handles)
	}
	if tune.MemTableSizeBytes != 256*1024*1024 {
		t.Fatalf("memtable=%d", tune.MemTableSizeBytes)
	}
	if tune.L0CompactionThreshold != 8 || tune.L0StopWritesThreshold != 64 {
		t.Fatalf("l0 compact=%d stop=%d", tune.L0CompactionThreshold, tune.L0StopWritesThreshold)
	}
}

func TestMakePebbleConfigOverrides(t *testing.T) {
	ctx := makeDBFlagSet(t, []string{
		"--db.cache", "2048",
		"--db.handles", "4096",
		"--db.memtable", "192",
		"--db.l0.compact", "6",
		"--db.l0.stop", "48",
	})

	cache, handles, tune, err := makePebbleConfig(ctx)
	if err != nil {
		t.Fatalf("makePebbleConfig: %v", err)
	}
	if cache != 2048 || handles != 4096 {
		t.Fatalf("overrides: cache=%d handles=%d", cache, handles)
	}
	if tune.MemTableSizeBytes != 192*1024*1024 {
		t.Fatalf("memtable=%d", tune.MemTableSizeBytes)
	}
	if tune.L0CompactionThreshold != 6 || tune.L0StopWritesThreshold != 48 {
		t.Fatalf("l0 compact=%d stop=%d", tune.L0CompactionThreshold, tune.L0StopWritesThreshold)
	}
}

func TestMakePebbleConfigRejectsInvalidL0Stop(t *testing.T) {
	ctx := makeDBFlagSet(t, []string{"--db.l0.compact", "8", "--db.l0.stop", "4"})

	if _, _, _, err := makePebbleConfig(ctx); err == nil {
		t.Fatal("expected invalid l0 stop to fail")
	}
}

func TestMakePebbleConfigRejectsExplicitZero(t *testing.T) {
	ctx := makeDBFlagSet(t, []string{"--db.cache", "0"})

	if _, _, _, err := makePebbleConfig(ctx); err == nil {
		t.Fatal("expected explicit zero cache to fail")
	}
}

func TestMakeStateDatabaseConfigAutoScalesFromDBCache(t *testing.T) {
	ctx := makeDBFlagSet(t, []string{"--db.cache", "8192"})

	cfg, err := makeStateDatabaseConfig(ctx)
	if err != nil {
		t.Fatalf("makeStateDatabaseConfig: %v", err)
	}
	if cfg.CleanTrieCacheSizeBytes != 1024*1024*1024 {
		t.Fatalf("clean trie cache=%d, want 1GiB", cfg.CleanTrieCacheSizeBytes)
	}
}

func TestMakeStateDatabaseConfigOverrideAndDisable(t *testing.T) {
	ctx := makeDBFlagSet(t, []string{"--state.trie.cache", "256"})
	cfg, err := makeStateDatabaseConfig(ctx)
	if err != nil {
		t.Fatalf("override: %v", err)
	}
	if cfg.CleanTrieCacheSizeBytes != 256*1024*1024 {
		t.Fatalf("override clean trie cache=%d, want 256MiB", cfg.CleanTrieCacheSizeBytes)
	}

	ctx = makeDBFlagSet(t, []string{"--state.trie.cache", "0"})
	cfg, err = makeStateDatabaseConfig(ctx)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if cfg.CleanTrieCacheSizeBytes != 0 {
		t.Fatalf("disabled clean trie cache=%d, want 0", cfg.CleanTrieCacheSizeBytes)
	}
}

func TestMakeStateDatabaseConfigRejectsInvalidValue(t *testing.T) {
	ctx := makeDBFlagSet(t, []string{"--state.trie.cache", "-2"})

	if _, err := makeStateDatabaseConfig(ctx); err == nil {
		t.Fatal("expected invalid trie cache to fail")
	}
}
