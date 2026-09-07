package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func TestReclaimEmptyHistoryDryRunDoesNotMutate(t *testing.T) {
	dir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(dir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	seedOfflineBoundary(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest := statesnapshots.NewManifest(0, 0, nil)
	manifest.Progress = &statesnapshots.Progress{HotPruneBlockNum: 8}
	if err := statesnapshots.PublishManifest(stateSnapshotsDir(dir), manifest); err != nil {
		t.Fatal(err)
	}
	before := offlineFixtureFiles(t, chainDataDir(dir))
	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	if err := app.Run([]string{"gtron", "db", "reclaim-empty-history", "--datadir", dir, "--through-block", "8"}); err != nil {
		t.Fatal(err)
	}
	var report reclaimEmptyHistoryReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.DryRun || !report.Inspection.Empty || report.Result != nil || report.Boundary.HeadBlock != 10 {
		t.Fatalf("unexpected report: %+v", report)
	}
	after := offlineFixtureFiles(t, chainDataDir(dir))
	if len(before) != len(after) {
		t.Fatal("dry run changed files")
	}
	for name, data := range before {
		if !bytes.Equal(data, after[name]) {
			t.Fatalf("dry run changed %s", name)
		}
	}
}

func offlineFixtureFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = data
	}
	return out
}

func TestReclaimEmptyHistoryRequiresExplicitTarget(t *testing.T) {
	var stdout bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stdout, Commands: []*cli.Command{dbCommand()}}
	err := app.Run([]string{"gtron", "db", "reclaim-empty-history"})
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("unexpected %v", err)
	}
}

func TestReclaimEmptyHistoryFailedAdmissionReopensAndReportsJSON(t *testing.T) {
	dir := t.TempDir()
	db, err := rawdb.NewPebbleDB(chainDataDir(dir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	seedOfflineBoundary(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest := statesnapshots.NewManifest(0, 0, nil)
	manifest.Progress = &statesnapshots.Progress{HotPruneBlockNum: 8}
	if err := statesnapshots.PublishManifest(stateSnapshotsDir(dir), manifest); err != nil {
		t.Fatal(err)
	}
	before := offlineFixtureFiles(t, chainDataDir(dir))
	var stdout, stderr bytes.Buffer
	app := &cli.App{Writer: &stdout, ErrWriter: &stderr, Commands: []*cli.Command{dbCommand()}}
	err = app.Run([]string{"gtron", "db", "reclaim-empty-history", "--datadir", dir, "--through-block", "8", "--yes"})
	if !errors.Is(err, pebbledb.ErrMaintenanceWALReplay) {
		t.Fatalf("want refused pending WAL, got %v", err)
	}
	var report reclaimEmptyHistoryReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid failure JSON: %v: %s", err, stdout.String())
	}
	if report.DryRun || report.Error == "" || !report.VerifiedAfterReopen || report.Result == nil || report.Inspection.MemTableCount == 0 {
		t.Fatalf("incorrect failed-admission report: %+v", report)
	}
	after := offlineFixtureFiles(t, chainDataDir(dir))
	if len(before) != len(after) {
		t.Fatal("refused replay changed database files")
	}
	for name, data := range before {
		if !bytes.Equal(data, after[name]) {
			t.Fatalf("refused replay changed %s", name)
		}
	}
}
