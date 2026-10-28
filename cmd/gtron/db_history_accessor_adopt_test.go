package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/urfave/cli/v2"
)

func runAccessorAdoptionCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	a := &cli.App{Writer: &out, ErrWriter: &out, Commands: []*cli.Command{dbAdoptHistoryAccessorsCommand()}}
	err := a.Run(append([]string{"gtron", "adopt-history-accessors-v11"}, args...))
	var value any
	if e := json.Unmarshal(out.Bytes(), &value); e != nil {
		t.Fatalf("invalid adoption report %q: %v", out.String(), e)
	}
	return out.String(), err
}

func TestDBAccessorAdoptionRequiresOfflineLock(t *testing.T) {
	source, dir, _ := referenceMigrationCLIFixture(t)
	before := historyInspectionFileHashes(t, dir)
	db, err := rawdb.NewPebbleDBReadOnly(chainDataDir(source), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	receipt := filepath.Join(source, "adoption-receipt")
	out, err := runAccessorAdoptionCLI(t, "--datadir", source, "--receipt-dir", receipt, "--yes")
	if err == nil || !strings.Contains(out, "exclusive database lock") {
		t.Fatal(out, err)
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, dir)) {
		t.Fatal("busy executor changed source")
	}
	if _, e := os.Stat(receipt); !os.IsNotExist(e) {
		t.Fatal("busy executor created receipt", e)
	}
	// A read-only plan is allowed while the writer owns LOCK. Missing entries
	// fail at plan validation rather than attempting the offline lock.
	out, err = runAccessorAdoptionCLI(t, "--datadir", source, "--receipt-dir", receipt)
	if err == nil || strings.Contains(out, "exclusive database lock") || !strings.Contains(out, "one to four explicit trios") {
		t.Fatal(out, err)
	}
}

func TestDBAccessorAdoptionRejectsForeignDirectory(t *testing.T) {
	source, dir, _ := referenceMigrationCLIFixture(t)
	other, otherDir, _ := referenceMigrationCLIFixture(t)
	_ = other
	before := historyInspectionFileHashes(t, dir)
	out, err := runAccessorAdoptionCLI(t, "--datadir", source, "--snapshot.dir", otherDir, "--receipt-dir", filepath.Join(source, "receipt"), "--yes")
	if err == nil || !strings.Contains(out, "default snapshot directory") {
		t.Fatal(out, err)
	}
	if !reflect.DeepEqual(before, historyInspectionFileHashes(t, dir)) {
		t.Fatal("foreign directory changed source")
	}
}
