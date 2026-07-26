package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func TestDBPruneTxInfoCommandDeletesOnlyLegacyRows(t *testing.T) {
	datadir := t.TempDir()
	path := chainDataDir(datadir)
	db, err := rawdb.NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	txID := bytes.Repeat([]byte{0x61}, 32)
	legacyKey := append([]byte("ti-"), txID...)
	blockRetKey := append([]byte("tib-"), make([]byte, 8)...)
	txIndexKey := append([]byte("tx-"), txID...)
	for key, value := range map[string][]byte{
		string(legacyKey):   []byte("legacy-info"),
		string(blockRetKey): []byte("block-ret"),
		string(txIndexKey):  make([]byte, 8),
	} {
		if err := db.Put([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "prune-tx-info",
		"--datadir", datadir,
		"--db.cache", "16",
		"--db.handles", "16",
		"--yes",
		"--compact",
	}); err != nil {
		t.Fatalf("prune command: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Deleted logical ti-* keyspace") {
		t.Fatalf("unexpected output:\n%s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "Compacted ti-* range") {
		t.Fatalf("compaction not reported:\n%s", stdout.String())
	}

	db, err = rawdb.NewPebbleDBReadOnly(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if has, err := db.Has(legacyKey); err != nil || has {
		t.Fatalf("legacy row exists=%v err=%v", has, err)
	}
	for _, key := range [][]byte{blockRetKey, txIndexKey} {
		if has, err := db.Has(key); err != nil || !has {
			t.Fatalf("adjacent row %x exists=%v err=%v", key, has, err)
		}
	}
}

func TestDBPruneTxInfoCommandRequiresConfirmation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	err := app.Run([]string{"gtron", "db", "prune-tx-info", "--datadir", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "without --yes") {
		t.Fatalf("error = %v", err)
	}
}
