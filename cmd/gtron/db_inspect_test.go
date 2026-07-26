package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/urfave/cli/v2"
)

func TestDBInspectCommandJSON(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatalf("open seed database: %v", err)
	}
	if err := db.Put(append([]byte("ti-"), bytes.Repeat([]byte{0x11}, 32)...), bytes.Repeat([]byte{0xaa}, 128)); err != nil {
		t.Fatalf("seed transaction info: %v", err)
	}
	if err := db.Put([]byte("LastBlock"), []byte("head")); err != nil {
		t.Fatalf("seed head: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed database: %v", err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{
		Writer:    &stdout,
		ErrWriter: &stderr,
		Commands:  []*cli.Command{dbCommand()},
	}
	if err := app.Run([]string{
		"gtron", "db", "inspect",
		"--datadir", datadir,
		"--db.cache", "16",
		"--db.handles", "16",
		"--progress", "0s",
		"--json",
	}); err != nil {
		t.Fatalf("db inspect: %v\nstderr: %s", err, stderr.String())
	}
	var report dbInspectionOutput
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\noutput: %s", err, stdout.String())
	}
	if report.Chaindata.Rows != 2 {
		t.Fatalf("rows = %d, want 2", report.Chaindata.Rows)
	}
	if report.ChaindataPhysicalBytes == 0 {
		t.Fatal("chaindata physical bytes not reported")
	}
	if len(report.Chaindata.Keyspaces) != 2 || report.Chaindata.Keyspaces[0].Name != "transaction-info" {
		t.Fatalf("keyspaces = %+v", report.Chaindata.Keyspaces)
	}
}

func TestInspectAncientReportsPhysicalTables(t *testing.T) {
	dir := t.TempDir()
	tables := chainfreezer.FreezerTableSet()
	freezer, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, tables)
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	if _, err := freezer.ModifyAncients(func(op rawdbfreezer.AncientWriteOp) error {
		for table := range tables {
			if err := op.AppendRaw(table, 0, bytes.Repeat([]byte(table), 20)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := freezer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := freezer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	report, err := inspectAncient(dir)
	if err != nil {
		t.Fatalf("inspectAncient: %v", err)
	}
	if len(report.Tables) != len(tables) {
		t.Fatalf("table count = %d, want %d", len(report.Tables), len(tables))
	}
	for _, stat := range report.Tables {
		if stat.Rows != 1 {
			t.Errorf("%s rows = %d, want 1", stat.Name, stat.Rows)
		}
		if stat.PhysicalBytes == 0 {
			t.Errorf("%s physical bytes = 0", stat.Name)
		}
	}
}

func TestWriteDBInspectionTextExplainsLogicalBytes(t *testing.T) {
	var out bytes.Buffer
	writeDBInspectionText(&out, dbInspectionOutput{
		ChaindataPath: "/tmp/chaindata",
		Chaindata: rawdb.DatabaseInspection{
			Rows:         1,
			LogicalBytes: 10,
			Keyspaces: []rawdb.KeyspaceStat{
				{Name: "transaction-info", KeyPattern: "ti-*", Rows: 1, LogicalBytes: 10, Percent: 100},
			},
		},
	}, 0)
	text := out.String()
	for _, want := range []string{"transaction-info", "uncompressed live key/value bytes", "not per-keyspace SST disk usage"} {
		if !strings.Contains(text, want) {
			t.Errorf("output missing %q:\n%s", want, text)
		}
	}
}
