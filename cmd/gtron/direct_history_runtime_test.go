package main

import (
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func TestDirectHistoryOfflineRejectsPartialMove(t *testing.T) {
	dir := t.TempDir()
	hot, err := rawdb.NewPebbleDB(filepath.Join(dir, "chaindata"), 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	history, err := rawdb.NewDirectHistoryPebbleDB(filepath.Join(dir, "history"), 16, 32, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.EnsureDirectHistoryPair(hot, history, true); err != nil {
		t.Fatal(err)
	}
	hot.Close()
	history.Close()
	_, err = openPebbleDB(makeDBFlagSet(t, nil), filepath.Join(dir, "chaindata"))
	if err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("partial move opened: %v", err)
	}
}

func TestDirectHistoryOfflineRequiresTargetCompletion(t *testing.T) {
	dir := t.TempDir()
	hot, err := rawdb.NewPebbleDB(filepath.Join(dir, "chaindata"), 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	history, err := rawdb.NewDirectHistoryPebbleDB(filepath.Join(dir, "history"), 16, 32, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.EnsureDirectHistoryPair(hot, history, true); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.MarkDirectHistoryLayout(hot); err != nil {
		t.Fatal(err)
	}
	hot.Close()
	history.Close()
	_, err = openPebbleDB(makeDBFlagSet(t, nil), filepath.Join(dir, "chaindata"))
	if err == nil || !strings.Contains(err.Error(), "completed layout") {
		t.Fatalf("partial target opened: %v", err)
	}
}

func TestDirectHistoryCompletedRerunLeavesUnrelatedLegacy(t *testing.T) {
	datadir := t.TempDir()
	root := filepath.Dir(chainDataDir(datadir))
	hot, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	history, err := rawdb.NewDirectHistoryPebbleDB(filepath.Join(root, "history"), 16, 32, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.EnsureDirectHistoryPair(hot, history, true); err != nil {
		t.Fatal(err)
	}
	for _, db := range []ethdb.KeyValueWriter{hot, history} {
		if err := rawdb.MarkDirectHistoryLayout(db); err != nil {
			t.Fatal(err)
		}
	}
	hot.Close()
	history.Close()
	legacyPath := filepath.Join(root, "unrelated-legacy")
	legacy, err := rawdb.NewHistoryStagingPebbleDB(legacyPath, 16, 32, false)
	if err != nil {
		t.Fatal(err)
	}
	key := append(rawdb.DirectHistoryLegacyStagingMetadataRange().Start, []byte("unrelated-test")...)
	if err := legacy.Put(key, []byte("keep")); err != nil {
		t.Fatal(err)
	}
	legacy.Close()
	app := cli.NewApp()
	app.Writer, app.ErrWriter = io.Discard, io.Discard
	app.Commands = []*cli.Command{dbMoveHistoryCommand()}
	if err := app.Run([]string{"gtron", "move-history", "--datadir", datadir, "--legacy-staging-dir", legacyPath, "--compact=false", "--db.cache", "32", "--db.handles", "64"}); err != nil {
		t.Fatal(err)
	}
	legacy, err = rawdb.NewHistoryStagingPebbleDB(legacyPath, 16, 32, false)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	got, err := legacy.Get(key)
	if err != nil || string(got) != "keep" {
		t.Fatalf("unrelated target modified: %q %v", got, err)
	}
}
