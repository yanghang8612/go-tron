package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/urfave/cli/v2"
)

func TestDBHistoryCodecBenchmarkExportRoundTripAndTamper(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	row := &rawdb.StateDomainChange{BlockNum: 7, Seq: 1, TxNum: 101, FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner: common.Address{0x41, 1}, Generation: 1, Domain: kvdomains.SystemDelegation, Key: []byte("drax-fixture"),
		PrevExists: true, Prev: bytes.Repeat([]byte("whole history value"), 500)}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{row}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	exported := filepath.Join(t.TempDir(), "packs")
	if _, output, err := runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "7", "--to-block", "7", "--export-packs", exported); err != nil {
		t.Fatalf("%s: %v", output, err)
	}
	// Hold the real database lock while the offline artifact benchmark runs.
	db, err = rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var output bytes.Buffer
	app := &cli.App{Writer: &output, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{"gtron", "db", "benchmark-history-codecs", "--export-packs", exported}); err != nil {
		t.Fatal(err)
	}
	var report historyCodecBenchmarkReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Complete || report.Stats.Completed != 1 || len(report.Totals) != 4 || len(report.Samples) != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	for _, candidate := range report.Samples[0].Candidates {
		if !candidate.ByteExact {
			t.Fatal("nonexact codec")
		}
	}
	pack := filepath.Join(exported, "00000000000000000007.pack")
	contents, err := os.ReadFile(pack)
	if err != nil {
		t.Fatal(err)
	}
	contents[len(contents)-1] ^= 0x01
	if err := os.WriteFile(pack, contents, 0600); err != nil {
		t.Fatal(err)
	}
	report, err = benchmarkHistoryExport(context.Background(), exported)
	if err == nil || report.Complete || !strings.Contains(err.Error(), "checksum") || report.Stats.Attempts != 0 {
		t.Fatalf("tampered pack accepted: %+v, %v", report, err)
	}
	if err := os.Remove(pack); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(exported, "manifest.json"), pack); err != nil {
		t.Fatal(err)
	}
	if _, err := benchmarkHistoryExport(context.Background(), exported); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestDBHistoryCodecBenchmarkRejectsEscapingManifest(t *testing.T) {
	directory := t.TempDir()
	manifest := historyPackExportManifest{Version: 1, Entries: []historyPackExportEntry{{Block: 7, File: "../outside", EncodedBytes: 1, DecodedBytes: 1}}}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	report, err := benchmarkHistoryExport(context.Background(), directory)
	if err == nil || report.Complete || report.Stats.Attempts != 0 {
		t.Fatalf("escaping manifest accepted: %+v %v", report, err)
	}
}
