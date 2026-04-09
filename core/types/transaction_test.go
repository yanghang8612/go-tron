package types

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestTransactionHash(t *testing.T) {
	transfer := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       1000000,
	}
	anyParam, _ := anypb.New(transfer)

	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
			Timestamp:  12345,
			Expiration: 99999,
		},
	}

	tx := NewTransactionFromPB(pb)
	h := tx.Hash()
	if h.IsEmpty() {
		t.Fatal("tx hash should not be empty")
	}
	h2 := tx.Hash()
	if h != h2 {
		t.Fatal("tx hash not deterministic")
	}
}

func TestTransactionContractType(t *testing.T) {
	transfer := &contractpb.TransferContract{
		OwnerAddress: []byte{0x41, 0x01},
		ToAddress:    []byte{0x41, 0x02},
		Amount:       100,
	}
	anyParam, _ := anypb.New(transfer)

	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferContract,
					Parameter: anyParam,
				},
			},
		},
	}

	tx := NewTransactionFromPB(pb)
	ct := tx.ContractType()
	if ct != corepb.Transaction_Contract_TransferContract {
		t.Fatalf("expected TransferContract, got %v", ct)
	}
}

func TestTransactionMarshalRoundTrip(t *testing.T) {
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  42,
			Expiration: 100,
		},
	}
	tx := NewTransactionFromPB(pb)
	data, err := tx.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := UnmarshalTransaction(data)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(tx.Proto(), tx2.Proto()) {
		t.Fatal("round trip failed")
	}
}
