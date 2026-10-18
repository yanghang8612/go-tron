package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestCensusCLIReadOnlyAndExclusiveReport(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "history")
	db, err := rawdb.NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("unrelated-history-row"), []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "report.json")
	args := []string{"--db-dir", dir, "--cutoffs", "1,10", "--out", out}
	if err := runArgs(args); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var report rawdb.PostingCensus
	if err := json.Unmarshal(data, &report); err != nil || !report.Complete || len(report.Cutoffs) != 2 || report.All.Frames != 0 {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	if err := runArgs(args); err == nil {
		t.Fatal("overwrote existing receipt")
	}
	after, _ := os.ReadFile(out)
	if string(after) != string(data) {
		t.Fatal("existing receipt changed")
	}
	db, err = rawdb.NewPebbleDBReadOnly(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, err := db.Get([]byte("unrelated-history-row"))
	if err != nil || string(v) != "preserve" {
		t.Fatal("source row changed", err)
	}
	// An active Pebble lock must produce an incomplete report, never a second
	// reader pretending it scanned a coherent image of a running database.
	lockedOut := filepath.Join(root, "locked.json")
	child := exec.Command(os.Args[0], "-test.run=^TestCensusCLILockHelper$")
	child.Env = append(os.Environ(), "POSTING_CENSUS_LOCK_TEST_DB="+dir, "POSTING_CENSUS_LOCK_TEST_OUT="+lockedOut)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("external lock check failed: %v %s", err, output)
	}
	data, err = os.ReadFile(lockedOut)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &report); err != nil || report.Complete || report.Error == "" {
		t.Fatalf("lock failure lost incomplete receipt: %+v %v", report, err)
	}
}

func TestCensusCLILockHelper(t *testing.T) {
	dir := os.Getenv("POSTING_CENSUS_LOCK_TEST_DB")
	if dir == "" {
		return
	}
	if err := runArgs([]string{"--db-dir", dir, "--cutoffs", "10", "--out", os.Getenv("POSTING_CENSUS_LOCK_TEST_OUT")}); err == nil {
		t.Fatal("opened another process's locked database")
	}
}

func TestCensusCLIRejectsUnsafeArguments(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "history")
	db, err := rawdb.NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, cutoffs := range []string{"", "0", "2,1", "1,1", "-1", "1,2,3,4,5,6,7,8,9"} {
		if err := runArgs([]string{"--db-dir", dir, "--cutoffs", cutoffs, "--out", filepath.Join(root, "invalid.json")}); err == nil {
			t.Fatalf("accepted invalid cutoffs %q", cutoffs)
		}
	}
	// Resolve parent symlinks before checking the output is outside the DB.
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{filepath.Join(dir, "unsafe.json"), filepath.Join(alias, "unsafe.json")} {
		if err := runArgs([]string{"--db-dir", dir, "--cutoffs", "10", "--out", out}); err == nil {
			t.Fatal("accepted report inside source", out)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "unsafe.json")); !os.IsNotExist(err) {
		t.Fatal("wrote unsafe source output", err)
	}
}
