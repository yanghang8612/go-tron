package main

import (
	"flag"
	"testing"

	"github.com/urfave/cli/v2"
)

func makeDBFlagSet(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{dbCacheFlag, dbHandlesFlag, dbMemtableFlag, dbL0CompactionFlag, dbL0StopFlag}
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
	if tune.MemTableSizeBytes != 64*1024*1024 {
		t.Fatalf("memtable=%d", tune.MemTableSizeBytes)
	}
	if tune.L0CompactionThreshold != 4 || tune.L0StopWritesThreshold != 24 {
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
