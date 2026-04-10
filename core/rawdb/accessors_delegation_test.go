package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestDelegatedResourceWriteRead(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	dr := &DelegatedResource{
		From:                      from,
		To:                        to,
		FrozenBalanceForBandwidth: 1000000,
		FrozenBalanceForEnergy:    500000,
	}
	if err := WriteDelegatedResource(db, from, to, dr); err != nil {
		t.Fatal(err)
	}
	got := ReadDelegatedResource(db, from, to)
	if got == nil {
		t.Fatal("expected delegation record")
	}
	if got.FrozenBalanceForBandwidth != 1000000 || got.FrozenBalanceForEnergy != 500000 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestDelegatedResourceDelete(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	dr := &DelegatedResource{From: from, To: to, FrozenBalanceForBandwidth: 100}
	WriteDelegatedResource(db, from, to, dr)
	DeleteDelegatedResource(db, from, to)
	if ReadDelegatedResource(db, from, to) != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestDelegationIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	receivers := []common.Address{{0x41, 0x02}, {0x41, 0x03}}
	if err := WriteDelegationIndex(db, from, receivers); err != nil {
		t.Fatal(err)
	}
	got := ReadDelegationIndex(db, from)
	if len(got) != 2 {
		t.Fatalf("expected 2 receivers, got %d", len(got))
	}
	if got[0] != receivers[0] || got[1] != receivers[1] {
		t.Fatalf("unexpected receivers: %v", got)
	}
}

func TestDelegationNotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	if ReadDelegatedResource(db, from, to) != nil {
		t.Fatal("expected nil")
	}
	if ReadDelegationIndex(db, from) != nil {
		t.Fatal("expected nil")
	}
}
