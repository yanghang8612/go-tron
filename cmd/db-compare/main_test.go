package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/tronprotocol/go-tron/internal/dbcompare"
)

func TestLiveJSONOutputReplacesSnapshot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "report-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	output, err := newLiveJSONOutput(file, true)
	if err != nil {
		t.Fatal(err)
	}
	first := &dbcompare.Report{
		Height:   10,
		Stores:   []dbcompare.StoreResult{{Name: "account", Equal: 123}},
		Progress: &dbcompare.ReportProgress{Phase: "progress", Store: "account", Rows: 123},
	}
	if err := output.Write(first); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	var live dbcompare.Report
	if err := json.Unmarshal(data, &live); err != nil {
		t.Fatalf("live JSON is invalid: %v\n%s", err, data)
	}
	if live.Progress == nil || live.Progress.Store != "account" || live.Progress.Rows != 123 {
		t.Fatalf("live progress = %+v", live.Progress)
	}

	final := &dbcompare.Report{Height: 10, Stores: []dbcompare.StoreResult{}}
	if err := output.Write(final); err != nil {
		t.Fatal(err)
	}

	data, err = os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	var got dbcompare.Report
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("final JSON is invalid: %v\n%s", err, data)
	}
	if got.Height != 10 || got.Progress != nil || len(got.Stores) != 0 {
		t.Fatalf("final report = %+v", got)
	}
}

func TestLiveReportSnapshotCapsDifferencesWithoutMutatingFinalReport(t *testing.T) {
	report := &dbcompare.Report{
		Differences: []dbcompare.Difference{{Key: "1"}, {Key: "2"}, {Key: "3"}},
	}
	snapshot := liveReportSnapshot(report, 2)
	if len(snapshot.Differences) != 2 || snapshot.Differences[1].Key != "2" {
		t.Fatalf("snapshot differences = %+v", snapshot.Differences)
	}
	if len(report.Differences) != 3 {
		t.Fatalf("source report was mutated: %+v", report.Differences)
	}
}
