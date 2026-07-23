package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestDecodedContractRejectsUnexpectedParameterType(t *testing.T) {
	parameter, err := anypb.New(&contractpb.TransferAssetContract{})
	if err != nil {
		t.Fatal(err)
	}
	tx := types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TransferContract,
			Parameter: parameter,
		}},
	}})

	if _, err := (&TransferActuator{}).getContract(&Context{Tx: tx}); err == nil || err.Error() != "failed to unmarshal TransferContract" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodedContractReportsMissingContract(t *testing.T) {
	tx := types.NewTransactionFromPB(&corepb.Transaction{RawData: &corepb.TransactionRaw{}})
	if _, err := (&TransferActuator{}).getContract(&Context{Tx: tx}); err == nil || err.Error() != "no contract in transaction" {
		t.Fatalf("unexpected error: %v", err)
	}
}
