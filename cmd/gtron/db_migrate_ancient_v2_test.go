package main

import (
	"bytes"
	"encoding/json"
	"testing"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

func TestDBMigrateAncientV2CommandJSON(t *testing.T) {
	datadir := t.TempDir()
	path := ancientDataDir(datadir)
	tables := chainfreezer.FreezerTableSet()
	freezer, err := rawdbfreezer.NewFreezer(path, "", false, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := freezer.ModifyAncients(func(op rawdbfreezer.AncientWriteOp) error {
		for number := uint64(0); number < 130; number++ {
			if err := op.AppendRaw("bodies", number, bytes.Repeat([]byte("body"), 100)); err != nil {
				return err
			}
			if err := op.AppendRaw("tx_infos", number, bytes.Repeat([]byte("receipt"), 120)); err != nil {
				return err
			}
			if err := op.AppendRaw("state_roots", number, make([]byte, 32)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := freezer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := freezer.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "migrate-ancient-v2",
		"--datadir", datadir,
		"--yes",
		"--frame-blocks", "8",
		"--segment-blocks", "64",
		"--max-segments", "1",
		"--json",
	}); err != nil {
		t.Fatalf("migrate: %v\nstderr: %s", err, stderr.String())
	}
	var output ancientV2MigrationOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout.String())
	}
	if output.Start != 0 || output.End != 64 || output.Segments != 1 || output.FrameBlocks != 8 {
		t.Fatalf("unexpected output: %+v", output)
	}

	freezer, err = rawdbfreezer.NewFreezer(path, "", true, 2049, tables)
	if err != nil {
		t.Fatal(err)
	}
	defer freezer.Close()
	for _, number := range []uint64{0, 63, 64, 129} {
		body, err := freezer.Ancient("bodies", number)
		if err != nil || len(body) != 400 {
			t.Fatalf("body[%d] len=%d err=%v", number, len(body), err)
		}
	}
}

func TestDBMigrateAncientV2RequiresYes(t *testing.T) {
	app := &cli.App{Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{"gtron", "db", "migrate-ancient-v2"}); err == nil {
		t.Fatal("migration without --yes succeeded")
	}
}
