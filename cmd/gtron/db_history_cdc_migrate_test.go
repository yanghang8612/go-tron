package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/urfave/cli/v2"
)

func cdcMigrationCLIFixture(t *testing.T) (string, string, []snapshots.SegmentRef) {
	return cdcMigrationCLIFixtureMode(t, false)
}

func cdcMigrationCLIFixtureMode(t *testing.T, reference bool) (string, string, []snapshots.SegmentRef) {
	t.Helper()
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
	source, _, _ := coldBenchmarkFixture(t, false)
	db, err := rawdb.NewPebbleDBReadOnly(chainDataDir(source), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(source, "gtron", "state-snapshots")
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeReadContext(context.Background(), db, dir, 2, 5, 2, 4, "history/state-domain-change-2-5.seg", snapshots.HistoryReadOptions{ReferenceContainer: reference})
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if _, err := snapshots.NewAggregator(dir).Integrate(2, 5, refs); err != nil {
		t.Fatal(err)
	}
	return source, dir, refs
}

func runCDCMigrationCLI(t *testing.T, args ...string) (historyReferenceMigrationReport, error) {
	t.Helper()
	var out, progress bytes.Buffer
	a := &cli.App{Writer: &out, ErrWriter: &progress, Commands: []*cli.Command{dbMigrateHistoryCDCCommand()}}
	err := a.Run(append([]string{"gtron", "migrate-history-cdc"}, args...))
	var report historyReferenceMigrationReport
	if e := json.Unmarshal(out.Bytes(), &report); e != nil {
		t.Fatalf("report %q: %v", out.String(), e)
	}
	return report, err
}

func TestDBHistoryCDCMigrationDryRun(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
	source, dir, _ := cdcMigrationCLIFixture(t)
	before := historyInspectionFileHashes(t, dir)
	report, err := runCDCMigrationCLI(t, "--datadir", source, "--min-free-gib=0")
	if err != nil || !report.DryRun || report.Result.TotalTrios != 1 || report.LargestAdmittedWorkBytes == 0 {
		t.Fatal(report, err)
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, dir)) {
		t.Fatal("plan changed snapshot files")
	}
}

func TestDBHistoryCDCMigrationRequiresOfflineLock(t *testing.T) {
	source, dir, _ := cdcMigrationCLIFixture(t)
	before := historyInspectionFileHashes(t, dir)
	db, err := rawdb.NewPebbleDBReadOnly(chainDataDir(source), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	report, err := runCDCMigrationCLI(t, "--datadir", source, "--min-free-gib=0", "--yes")
	if err == nil || !strings.Contains(report.Error, "exclusive database lock") {
		t.Fatalf("running source admitted %+v %v", report, err)
	}
	if after := historyInspectionFileHashes(t, dir); !reflect.DeepEqual(before, after) {
		t.Fatal("locked source mutated")
	}
}

func TestDBHistoryCDCMigrationRejectsForeignDirectoryAndBudget(t *testing.T) {
	source, dir, _ := cdcMigrationCLIFixture(t)
	foreign, foreignDir, _ := cdcMigrationCLIFixture(t)
	_ = foreign
	before, foreignBefore := historyInspectionFileHashes(t, dir), historyInspectionFileHashes(t, foreignDir)
	for _, args := range [][]string{
		{"--snapshot.dir", foreignDir, "--yes"},
		{"--max-work-gib=0", "--yes"},
		{"--max-work-gib=18446744073709551615", "--yes"},
		{"--max-work-gib=1", "--yes"},
	} {
		report, err := runCDCMigrationCLI(t, append([]string{"--datadir", source, "--min-free-gib=0"}, args...)...)
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

func TestDBHistoryCDCMigrationR1OnlyAndAllowGrowth(t *testing.T) {
	source, dir, refs := cdcMigrationCLIFixture(t)
	before := historyInspectionFileHashes(t, dir)
	report, err := runCDCMigrationCLI(t, "--datadir", source, "--min-free-gib=0", "--r1-only", "--yes")
	if err != nil || report.Result.TotalTrios != 0 || !reflect.DeepEqual(before, historyInspectionFileHashes(t, dir)) {
		t.Fatal(report, err)
	}
	source, dir, refs = cdcMigrationCLIFixtureMode(t, true)
	want, err := snapshots.DigestColdStateHistoryContext(context.Background(), dir, refs)
	if err != nil {
		t.Fatal(err)
	}
	report, err = runCDCMigrationCLI(t, "--datadir", source, "--min-free-gib=0", "--r1-only", "--allow-growth", "--min-savings-percent=100", "--yes")
	if err != nil || report.Result.MigratedTrios != 1 || report.Result.SkippedNoGain != 0 {
		t.Fatal(report, err)
	}
	manifest, err := snapshots.LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := snapshots.DigestColdStateHistoryContext(context.Background(), dir, manifest.Segments)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("CLI R1 rewrite changed logical data", err)
	}
}
