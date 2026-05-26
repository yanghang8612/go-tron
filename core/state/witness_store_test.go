package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestWitnessBrokerageAnchorAndFlatLatest(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x01}

	sdb.GetOrCreateAccount(addr)
	if got := sdb.ReadWitnessBrokerage(addr); got != int64(rawdb.DefaultBrokerage) {
		t.Fatalf("default brokerage = %d, want %d", got, rawdb.DefaultBrokerage)
	}
	if err := sdb.WriteWitnessBrokerage(addr, 45); err != nil {
		t.Fatal(err)
	}
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	atR1, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR1.ReadWitnessBrokerage(addr); got != 45 {
		t.Fatalf("root1 brokerage = %d, want 45", got)
	}

	if err := atR1.WriteWitnessBrokerage(addr, 70); err != nil {
		t.Fatal(err)
	}
	root2, err := atR1.Commit()
	if err != nil {
		t.Fatal(err)
	}

	root1Open, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := root1Open.ReadWitnessBrokerage(addr); got != 70 {
		t.Fatalf("root1-open latest brokerage = %d, want 70", got)
	}
	atR2, err := New(root2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR2.ReadWitnessBrokerage(addr); got != 70 {
		t.Fatalf("root2 brokerage = %d, want 70", got)
	}
}
