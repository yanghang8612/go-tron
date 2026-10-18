package main

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestGenerationAccountPointerOracle(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	for i := byte(0); i < 4; i++ {
		owner := common.Address{0x41, i + 1}
		generation := uint64(i)
		encoded, err := (&state.StateAccountV3{Version: state.StateAccountVersion, AccountKVRoot: state.EmptyKVRoot, AccountKVGeneration: generation}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		if err = rawdb.WriteStateAccountLatest(db, owner, encoded); err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			if err = rawdb.WriteStateKVGeneration(db, owner, generation); err != nil {
				t.Fatal(err)
			}
		}
	}
	r, err := inspectAccountPointers(context.Background(), db)
	if err != nil || r.Accounts != 4 || r.Explicit != 3 || r.ImplicitZero != 1 || r.Mismatches != 0 {
		t.Fatalf("report=%+v error=%v", r, err)
	}
	if err = rawdb.DeleteStateKVGeneration(db, common.Address{0x41, 2}); err != nil {
		t.Fatal(err)
	}
	r, err = inspectAccountPointers(context.Background(), db)
	if err == nil || r.Mismatches != 1 || len(r.Examples) != 1 {
		t.Fatalf("missing pointer accepted: %+v %v", r, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = inspectAccountPointers(ctx, db); err == nil {
		t.Fatal("cancelled account scan accepted")
	}
}
