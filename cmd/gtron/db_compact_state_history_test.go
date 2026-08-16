package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func TestDBCompactStateHistoryCommandPreservesLiveRows(t *testing.T) {
	datadir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	owner := common.BytesToAddress([]byte{1, 2, 3})
	change := &rawdb.StateDomainChange{
		BlockNum:   7,
		TxNum:      9,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/1"),
		PrevExists: true,
		Prev:       []byte("before"),
		NextExists: true,
		Next:       []byte("live"),
	}
	if err := rawdb.WriteStateDomainChange(db, change); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("unrelated"), []byte("preserved")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest := statesnapshots.NewManifest(0, 0, nil)
	manifest.Progress = &statesnapshots.Progress{HotPruneBlockNum: 6}
	if err := statesnapshots.PublishManifest(stateSnapshotsDir(datadir), manifest); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{
		"gtron", "db", "compact-state-history",
		"--datadir", datadir,
		"--db.cache", "16",
		"--db.handles", "16",
		"--yes",
		"--progress", "0s",
		"--json",
	}); err != nil {
		t.Fatalf("compact state history: %v\nstderr: %s", err, stderr.String())
	}
	var report compactStateHistoryOutput
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\noutput: %s", err, stdout.String())
	}
	if !report.CompactedChangeSets || !report.CompactedPostingIndex {
		t.Fatalf("unexpected report: %+v", report)
	}
	if !report.UsedPruneWatermark || report.PrunedThroughBlock != 6 || report.PostingRowsScanned != 1 || report.DirectoryRowsScanned != 1 {
		t.Fatalf("watermark report: %+v", report)
	}
	updatedManifest, err := statesnapshots.LoadProductionManifest(stateSnapshotsDir(datadir))
	if err != nil {
		t.Fatal(err)
	}
	if updatedManifest.Progress == nil || updatedManifest.Progress.StateChangeIndexPruneBlockNum != 6 {
		t.Fatalf("state-change index prune progress = %+v", updatedManifest.Progress)
	}

	reopened, err := rawdb.NewPebbleDB(chainDataDir(datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok, err := rawdb.ReadStateDomainChange(reopened, 7, 1); err != nil || !ok || !bytes.Equal(got.Prev, []byte("before")) || got.NextExists {
		t.Fatalf("state change after compaction = (%+v, %t, %v)", got, ok, err)
	}
	if value, err := reopened.Get([]byte("unrelated")); err != nil || !bytes.Equal(value, []byte("preserved")) {
		t.Fatalf("unrelated row after compaction = (%q, %v)", value, err)
	}
}

func TestDBCompactStateHistoryRequiresConfirmation(t *testing.T) {
	ctx := makeDBFlagSet(t, nil)
	if err := dbCompactStateHistoryCmd(ctx); err == nil {
		t.Fatal("compact state history accepted missing --yes")
	}
}
