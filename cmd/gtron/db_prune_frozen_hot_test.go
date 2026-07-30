package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

func TestDBPruneFrozenHotDeletesOnlyAncientCoveredRows(t *testing.T) {
	datadir := t.TempDir()
	ancient, err := rawdbfreezer.NewFreezer(ancientDataDir(datadir), "", false, 2049, chainfreezer.FreezerTableSet())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ancient.ModifyAncients(func(op rawdbfreezer.AncientWriteOp) error {
		for number := uint64(0); number < 3; number++ {
			if err := op.AppendRaw("bodies", number, []byte{byte(number + 1)}); err != nil {
				return err
			}
			if err := op.AppendRaw("tx_infos", number, []byte{byte(number + 2)}); err != nil {
				return err
			}
			if err := op.AppendRaw("state_roots", number, bytes.Repeat([]byte{byte(number)}, 32)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := ancient.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := ancient.Close(); err != nil {
		t.Fatal(err)
	}

	path := chainDataDir(datadir)
	db, err := rawdb.NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	for number := uint64(0); number < 4; number++ {
		if err := db.Put(testNumberedKey("b-", number), []byte("hot-body")); err != nil {
			t.Fatal(err)
		}
		if err := db.Put(testNumberedKey("tib-", number), []byte("hot-receipts")); err != nil {
			t.Fatal(err)
		}
	}
	txKey := append([]byte("tx-"), bytes.Repeat([]byte{0x44}, 32)...)
	if err := db.Put(txKey, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "prune-frozen-hot",
		"--datadir", datadir,
		"--db.cache", "16",
		"--db.handles", "16",
		"--yes",
		"--compact",
	}); err != nil {
		t.Fatalf("prune command: %v\nstderr: %s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "frozen range [0,3)") ||
		!strings.Contains(stdout.String(), "Compacted frozen b-* and tib-* ranges") {
		t.Fatalf("unexpected output:\n%s", stdout.String())
	}

	db, err = rawdb.NewPebbleDBReadOnly(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for number := uint64(0); number < 3; number++ {
		for _, prefix := range []string{"b-", "tib-"} {
			if has, err := db.Has(testNumberedKey(prefix, number)); err != nil || has {
				t.Fatalf("covered %s%d exists=%v err=%v", prefix, number, has, err)
			}
		}
	}
	for _, key := range [][]byte{testNumberedKey("b-", 3), testNumberedKey("tib-", 3), txKey} {
		if has, err := db.Has(key); err != nil || !has {
			t.Fatalf("uncovered/adjacent key %x exists=%v err=%v", key, has, err)
		}
	}
	if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageFreezerHotPrune); err != nil || !ok || progress != 3 {
		t.Fatalf("freezer prune progress=%d ok=%v err=%v", progress, ok, err)
	}
}

func TestDBPruneFrozenHotRequiresConfirmation(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{dbCommand()}}
	err := app.Run([]string{"gtron", "db", "prune-frozen-hot", "--datadir", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "without --yes") {
		t.Fatalf("error = %v", err)
	}
}

func testNumberedKey(prefix string, number uint64) []byte {
	key := make([]byte, len(prefix)+8)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], number)
	return key
}
