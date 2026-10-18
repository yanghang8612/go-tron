package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestGenerationCLIReadOnlyAndExclusiveReport(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "history")
	db, err := rawdb.NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("unrelated-history-row"), []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.InitializeStateCommitment(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "report.json")
	args := []string{"--db-dir", dir, "--groups", filepath.Join(root, "groups.jsonl"), "--out", out}
	if err := runArgs(args); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var report rawdb.GenerationCensus
	if err := json.Unmarshal(data, &report); err != nil || !report.Complete || !report.MuHashVerified || report.Total.Rows != 0 {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	if err := runArgs(args); err == nil {
		t.Fatal("overwrote existing receipt")
	}
	pointerOutput := filepath.Join(root, "account-pointers.json")
	if err := runArgs([]string{"verify-account-pointers", "--db-dir", dir, "--out", pointerOutput}); err != nil {
		t.Fatal(err)
	}
	var pointerReceipt struct {
		Complete bool                 `json:"complete"`
		Pointers accountPointerCensus `json:"account_generation_pointers"`
	}
	pointerData, err := os.ReadFile(pointerOutput)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(pointerData, &pointerReceipt); err != nil || !pointerReceipt.Complete || pointerReceipt.Pointers.Mismatches != 0 {
		t.Fatalf("pointer receipt=%+v error=%v", pointerReceipt, err)
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
	child := exec.Command(os.Args[0], "-test.run=^TestGenerationCLILockHelper$")
	child.Env = append(os.Environ(), "GENERATION_CENSUS_LOCK_TEST_DB="+dir, "GENERATION_CENSUS_LOCK_TEST_OUT="+lockedOut)
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

func TestGenerationCLILockHelper(t *testing.T) {
	dir := os.Getenv("GENERATION_CENSUS_LOCK_TEST_DB")
	if dir == "" {
		return
	}
	if err := runArgs([]string{"--db-dir", dir, "--groups", filepath.Join(filepath.Dir(os.Getenv("GENERATION_CENSUS_LOCK_TEST_OUT")), "locked-groups.jsonl"), "--out", os.Getenv("GENERATION_CENSUS_LOCK_TEST_OUT")}); err == nil {
		t.Fatal("opened another process's locked database")
	}
}

func TestGenerationCLIRejectsUnsafeArguments(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "db")
	db, err := rawdb.NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err = os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{filepath.Join(dir, "bad.json"), filepath.Join(alias, "bad.json")} {
		if err := runArgs([]string{"--db-dir", dir, "--out", bad, "--groups", filepath.Join(root, "groups.jsonl")}); err == nil {
			t.Fatal("source output allowed")
		}
	}
}
