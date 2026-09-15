package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func referenceMigrationCLIFixture(t *testing.T) (string, string, []snapshots.SegmentRef) {
	t.Helper()
	source, _, _ := coldBenchmarkFixture(t, false)
	db, err := rawdb.NewPebbleDBReadOnly(chainDataDir(source), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(source, "gtron", "state-snapshots")
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeReadContext(context.Background(), db, dir, 2, 5, 2, 4, "history/state-domain-change-2-5.seg", snapshots.HistoryReadOptions{})
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if _, err := snapshots.NewAggregator(dir).Integrate(2, 5, refs); err != nil {
		t.Fatal(err)
	}
	return source, dir, refs
}

func runReferenceMigrationCLI(t *testing.T, args ...string) (historyReferenceMigrationReport, error) {
	t.Helper()
	var out bytes.Buffer
	a := &cli.App{Writer: &out, ErrWriter: &out, Commands: []*cli.Command{dbMigrateHistoryReferenceCommand()}}
	err := a.Run(append([]string{"gtron", "migrate-history-reference"}, args...))
	var report historyReferenceMigrationReport
	if e := json.Unmarshal(out.Bytes(), &report); e != nil {
		t.Fatalf("report %q: %v", out.String(), e)
	}
	return report, err
}

func TestDBHistoryReferenceMigrationInPlaceAndResume(t *testing.T) {
	source, dir, refs := referenceMigrationCLIFixture(t)
	before := historyInspectionFileHashes(t, dir)
	report, err := runReferenceMigrationCLI(t, "--datadir", source, "--min-free-gib=0")
	if err != nil || !report.DryRun || report.Result == nil || report.Result.TotalTrios != 1 || report.LargestAdmittedWorkBytes == 0 {
		t.Fatalf("plan %+v %v", report, err)
	}
	if after := historyInspectionFileHashes(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatal("plan changed original snapshot directory")
	}
	report, err = runReferenceMigrationCLI(t, "--datadir", source, "--min-free-gib=0", "--yes")
	if err != nil || report.DryRun || report.Result == nil || report.Result.MigratedTrios != 1 || report.Result.DeletedSourceFiles != 3 {
		t.Fatalf("execute %+v %v", report, err)
	}
	for _, ref := range refs {
		if _, e := os.Stat(filepath.Join(dir, ref.Path)); !os.IsNotExist(e) {
			t.Fatalf("old file not reclaimed: %s %v", ref.Path, e)
		}
	}
	manifest, err := snapshots.LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.VisibleTxStart != 2 || manifest.VisibleTxEnd != 5 || len(manifest.Segments) != 3 {
		t.Fatalf("manifest %+v", manifest)
	}
	report, err = runReferenceMigrationCLI(t, "--datadir", source, "--min-free-gib=0", "--yes")
	if err != nil || report.Result.AlreadyCurrent != 1 || report.Result.MigratedTrios != 0 {
		t.Fatalf("resume %+v %v", report, err)
	}
}

func TestDBHistoryReferenceMigrationRequiresOfflineLock(t *testing.T) {
	source, dir, _ := referenceMigrationCLIFixture(t)
	before := historyInspectionFileHashes(t, dir)
	db, err := rawdb.NewPebbleDBReadOnly(chainDataDir(source), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	report, err := runReferenceMigrationCLI(t, "--datadir", source, "--min-free-gib=0", "--yes")
	if err == nil || !strings.Contains(report.Error, "exclusive database lock") {
		t.Fatalf("running source admitted %+v %v", report, err)
	}
	if after := historyInspectionFileHashes(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatal("locked source mutated")
	}
}

func TestDBHistoryReferenceMigrationRejectsForeignDirectoryAndBudget(t *testing.T) {
	source, dir, _ := referenceMigrationCLIFixture(t)
	foreign, foreignDir, _ := referenceMigrationCLIFixture(t)
	_ = foreign
	before, foreignBefore := historyInspectionFileHashes(t, dir), historyInspectionFileHashes(t, foreignDir)
	for _, args := range [][]string{
		{"--snapshot.dir", foreignDir, "--yes"},
		{"--max-work-gib=0", "--yes"},
		{"--max-work-gib=18446744073709551615", "--yes"},
		{"--max-work-gib=1", "--yes"},
	} {
		report, err := runReferenceMigrationCLI(t, append([]string{"--datadir", source, "--min-free-gib=0"}, args...)...)
		if err == nil || report.Error == "" {
			t.Fatalf("unsafe migration accepted %+v %v", report, err)
		}
		if after := historyInspectionFileHashes(t, dir); !reflect.DeepEqual(before, after) {
			// An admitted executor may create its empty tool lock before budget checks;
			// it must not create output, journal, or change any source/control contents.
			delete(after, ".history-reference-migration.lock")
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rejected migration changed source", args)
			}
		}
		if after := historyInspectionFileHashes(t, foreignDir); !reflect.DeepEqual(foreignBefore, after) {
			t.Fatal("foreign source changed")
		}
	}
}
