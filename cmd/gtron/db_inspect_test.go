package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func TestDBInspectCommandJSON(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatalf("open seed database: %v", err)
	}
	if err := db.Put(append([]byte("sync-staged-block-v1-"), bytes.Repeat([]byte{0x11}, 8)...), bytes.Repeat([]byte{0xaa}, 128)); err != nil {
		t.Fatalf("seed staged block: %v", err)
	}
	if err := db.Put([]byte("LastBlock"), []byte("head")); err != nil {
		t.Fatalf("seed head: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close seed database: %v", err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
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
	if got := report.Chaindata.Keyspaces[0].Name; got != "sync-staged-block" {
		t.Fatalf("largest keyspace = %q, want sync-staged-block", got)
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
				{Name: "state-changeset", KeyPattern: "state-changeset-v2-*", Rows: 1, LogicalBytes: 10, Percent: 100},
			},
		},
	}, 0)
	text := out.String()
	for _, want := range []string{"state-changeset", "uncompressed live key/value bytes", "not per-keyspace SST disk usage"} {
		if !strings.Contains(text, want) {
			t.Errorf("output missing %q:\n%s", want, text)
		}
	}
}
