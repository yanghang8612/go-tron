package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestReadWriteExchange(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	ex := &corepb.Exchange{
		ExchangeId:         1,
		CreatorAddress:     []byte{0x41, 0x01},
		CreateTime:         1000,
		FirstTokenId:       []byte("_"),
		FirstTokenBalance:  1000000,
		SecondTokenId:      []byte("1000001"),
		SecondTokenBalance: 500000,
	}

	if err := WriteExchange(db, ex); err != nil {
		t.Fatalf("WriteExchange: %v", err)
	}
	got := ReadExchange(db, 1)
	if got == nil {
		t.Fatal("ReadExchange returned nil")
	}
	if got.ExchangeId != 1 || got.FirstTokenBalance != 1000000 {
		t.Fatalf("mismatch: %+v", got)
	}

	DeleteExchange(db, 1)
	if ReadExchange(db, 1) != nil {
		t.Fatal("expected nil after delete")
	}
}
