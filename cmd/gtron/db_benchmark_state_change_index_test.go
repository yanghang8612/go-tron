package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/urfave/cli/v2"
)

func TestParseStateChangePostingRows(t *testing.T) {
	rows, err := parseStateChangePostingRows("256,32,256,64")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0] != 32 || rows[1] != 64 || rows[2] != 256 {
		t.Fatalf("rows = %v", rows)
	}
	for _, value := range []string{"", "0", "nope", "1048577"} {
		if _, err := parseStateChangePostingRows(value); err == nil {
			t.Fatalf("accepted invalid posting rows %q", value)
		}
	}
}

func TestPrefixEncodedDataBlockEstimatorUsesRestartPrefixCompression(t *testing.T) {
	estimator := newPrefixEncodedDataBlockEstimator()
	estimator.Add(100, 0, 0)
	estimator.Add(100, 0, 99)
	estimator.Finish()
	// Entry one: 3 one-byte varints + 100 unshared bytes. Entry two:
	// 3 one-byte varints + 1 unshared byte. One restart offset plus count: 8.
	if got := estimator.Total(); got != 115 {
		t.Fatalf("estimated bytes = %d, want 115", got)
	}
}

func TestDBBenchmarkStateChangeIndexCommandJSON(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	owner := common.Address{common.AddressPrefixMainnet, 0x44}
	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		if err := rawdb.WriteStateDomainChangeInverseIndex(db, &rawdb.StateDomainChange{
			BlockNum:   blockNum,
			Seq:        1,
			TxNum:      blockNum,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Domain:     kvdomains.ContractStorage,
			Key:        []byte("slot-a"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateDomainChangeInverseIndex(db, &rawdb.StateDomainChange{
		BlockNum:   11,
		Seq:        1,
		TxNum:      11,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot-b"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChangeInverseIndex(db, &rawdb.StateDomainChange{
		BlockNum:   12,
		Seq:        1,
		TxNum:      12,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "benchmark-state-change-index",
		"--datadir", datadir,
		"--db.cache", "16",
		"--db.handles", "16",
		"--family", "kv-latest",
		"--posting-rows", "2,4",
		"--progress", "0s",
		"--json",
	}); err != nil {
		t.Fatalf("benchmark state change index: %v\nstderr: %s", err, stderr.String())
	}
	var report stateChangeIndexBenchmarkOutput
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode JSON: %v\noutput: %s", err, stdout.String())
	}
	if !report.Complete || report.Rows != 11 || report.UniqueLatestKeys != 2 || report.MaxBlocksPerKey != 10 {
		t.Fatalf("report summary = %+v", report)
	}
	if len(report.Candidates) != 4 {
		t.Fatalf("candidates = %+v", report.Candidates)
	}
	if report.Candidates[0].Name != "current-row-v2" || report.Candidates[0].EstimatedDataBlockBytes == 0 {
		t.Fatalf("current candidate = %+v", report.Candidates[0])
	}
	if report.Candidates[1].SupportsLogicalPrefixHistory || !report.Candidates[1].RequiresChangesetCollisionCheck {
		t.Fatalf("hash row capabilities = %+v", report.Candidates[1])
	}
	if report.Candidates[2].PostingRows != 2 || report.Candidates[2].PhysicalRows != 6 {
		t.Fatalf("posting-2 candidate = %+v", report.Candidates[2])
	}
	if report.Candidates[3].PostingRows != 4 || report.Candidates[3].PhysicalRows != 4 {
		t.Fatalf("posting-4 candidate = %+v", report.Candidates[3])
	}
	if report.Candidates[2].LogicalSavingsPercent <= 0 || report.ExpectedHashCollisionPairs <= 0 {
		t.Fatalf("benchmark projection = %+v", report)
	}
}
