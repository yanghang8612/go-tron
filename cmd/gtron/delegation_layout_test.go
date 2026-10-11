package main

import (
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPagedDelegationLayoutFreshAndRestart(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	if err := ensurePagedDelegationLayout(db); err != nil {
		t.Fatal(err)
	}
	marked, err := rawdb.ReadPagedDelegationLayout(db)
	if err != nil || !marked {
		t.Fatalf("fresh layout marker = %v, %v", marked, err)
	}
	rawdb.WriteHeadBlockHash(db, common.Hash{1})
	if err := ensurePagedDelegationLayout(db); err != nil {
		t.Fatalf("restart marked chain: %v", err)
	}
}

func TestPagedDelegationLayoutRejectsExistingChain(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	rawdb.WriteHeadBlockHash(db, common.Hash{1})
	if err := ensurePagedDelegationLayout(db); err == nil || !strings.Contains(err.Error(), "sync from genesis") {
		t.Fatalf("old chain accepted: %v", err)
	}
	marked, err := rawdb.ReadPagedDelegationLayout(db)
	if err != nil || marked {
		t.Fatalf("rejected chain changed layout = %v, %v", marked, err)
	}
}

func TestPagedDelegationLayoutRejectsHeadlessImportedState(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	if err := rawdb.MarkPagedDelegationLayout(db); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.InvalidatePagedDelegationLayout(db); err != nil {
		t.Fatal(err)
	}
	if err := ensurePagedDelegationLayout(db); err == nil || !strings.Contains(err.Error(), "uncertified imported") {
		t.Fatalf("headless imported state relabeled fresh: %v", err)
	}
}
