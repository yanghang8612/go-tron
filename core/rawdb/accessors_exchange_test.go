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

func TestReadWriteExchangeV2SeparateBucket(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	legacy := &corepb.Exchange{
		ExchangeId:         1,
		FirstTokenId:       []byte("TOKEN"),
		FirstTokenBalance:  100,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 200,
	}
	v2 := &corepb.Exchange{
		ExchangeId:         1,
		FirstTokenId:       []byte("1000001"),
		FirstTokenBalance:  100,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 200,
	}
	if err := WriteExchange(db, legacy); err != nil {
		t.Fatalf("WriteExchange: %v", err)
	}
	if err := WriteExchangeV2(db, v2); err != nil {
		t.Fatalf("WriteExchangeV2: %v", err)
	}

	if got := ReadExchange(db, 1); got == nil || string(got.FirstTokenId) != "TOKEN" {
		t.Fatalf("legacy exchange mismatch: %+v", got)
	}
	if got := ReadExchangeV2(db, 1); got == nil || string(got.FirstTokenId) != "1000001" {
		t.Fatalf("v2 exchange mismatch: %+v", got)
	}

	DeleteExchangeV2(db, 1)
	if ReadExchangeV2(db, 1) != nil {
		t.Fatal("expected nil after v2 delete")
	}
	if ReadExchange(db, 1) == nil {
		t.Fatal("legacy bucket should remain after v2 delete")
	}
}
