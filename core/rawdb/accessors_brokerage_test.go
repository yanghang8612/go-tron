package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestBrokerageWriteRead(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := common.Address{0x41, 0x01}
	if err := WriteWitnessBrokerage(db, addr, 30); err != nil {
		t.Fatal(err)
	}
	if got := ReadWitnessBrokerage(db, addr); got != 30 {
		t.Fatalf("expected 30, got %d", got)
	}
}

func TestBrokerageDefault(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := common.Address{0x41, 0x01}
	if got := ReadWitnessBrokerage(db, addr); got != 20 {
		t.Fatalf("expected default 20, got %d", got)
	}
}
