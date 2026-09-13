package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/urfave/cli/v2"
)

func TestDBHistorySharingBenchmarkExportReopenAndTamper(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	prev := make([]byte, 512<<10)
	_, _ = rand.New(rand.NewSource(6)).Read(prev)
	for block := uint64(1022); block < 1026; block++ {
		prev[block] ^= 1
		row := &rawdb.StateDomainChange{BlockNum: block, Seq: 1, TxNum: block * 4, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 1}, Generation: 1, Domain: kvdomains.SystemDelegation, Key: []byte("drax-fixture"), PrevExists: true, Prev: prev}
		if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	exported := filepath.Join(t.TempDir(), "packs")
	if _, output, err := runDBInspectHistoryTest(context.Background(), datadir, "--from-block", "1022", "--to-block", "1025", "--samples", "4", "--export-packs", exported); err != nil {
		t.Fatalf("%s: %v", output, err)
	}
	// A production-like DB lock remains held: replay cannot open that DB.
	db, err = rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var output bytes.Buffer
	app := &cli.App{Writer: &output, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{"gtron", "db", "benchmark-history-sharing", "--export-packs", exported}); err != nil {
		t.Fatal(err)
	}
	var report historySharingBenchmarkReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Complete || !report.Consecutive || report.Buckets != 2 || report.SharedPacks != 4 || report.SavedKVFraction < 0.35 {
		t.Fatalf("unexpected report %+v", report)
	}
	for _, sample := range report.Samples {
		if !sample.ReopenExact || !sample.ByteExact {
			t.Fatal("replay was not exact")
		}
	}
	pack := filepath.Join(exported, "00000000000000001022.pack")
	data, err := os.ReadFile(pack)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(pack, data, 0600); err != nil {
		t.Fatal(err)
	}
	if report, err := benchmarkHistorySharingExport(context.Background(), exported); err == nil || report.Complete || len(report.Samples) != 0 {
		t.Fatal("tampered input replayed")
	}
}
