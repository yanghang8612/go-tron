package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// TestBlockContainsShieldedTransfer pins the helper used by applyBlock to
// gate the Sapling commitment-tree reset/save lifecycle hooks. False on
// transparent-only blocks and on empty blocks; true as soon as a single
// ShieldedTransferContract is present.
func TestBlockContainsShieldedTransfer(t *testing.T) {
	transferAny, err := anypb.New(&contractpb.TransferContract{})
	if err != nil {
		t.Fatal(err)
	}
	shieldedAny, err := anypb.New(&contractpb.ShieldedTransferContract{})
	if err != nil {
		t.Fatal(err)
	}

	makeBlock := func(types_ []corepb.Transaction_Contract_ContractType) *types.Block {
		var txs []*corepb.Transaction
		for _, ty := range types_ {
			param := transferAny
			if ty == corepb.Transaction_Contract_ShieldedTransferContract {
				param = shieldedAny
			}
			txs = append(txs, &corepb.Transaction{
				RawData: &corepb.TransactionRaw{
					Contract: []*corepb.Transaction_Contract{{Type: ty, Parameter: param}},
				},
			})
		}
		return types.NewBlockFromPB(&corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1}},
			Transactions: txs,
		})
	}

	cases := []struct {
		name string
		txs  []corepb.Transaction_Contract_ContractType
		want bool
	}{
		{"empty block", nil, false},
		{"transparent only", []corepb.Transaction_Contract_ContractType{
			corepb.Transaction_Contract_TransferContract,
			corepb.Transaction_Contract_TransferContract,
		}, false},
		{"single shielded", []corepb.Transaction_Contract_ContractType{
			corepb.Transaction_Contract_ShieldedTransferContract,
		}, true},
		{"shielded among transparent", []corepb.Transaction_Contract_ContractType{
			corepb.Transaction_Contract_TransferContract,
			corepb.Transaction_Contract_ShieldedTransferContract,
			corepb.Transaction_Contract_TransferContract,
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := blockContainsShieldedTransfer(makeBlock(tc.txs)); got != tc.want {
				t.Fatalf("blockContainsShieldedTransfer: got %v, want %v", got, tc.want)
			}
		})
	}
}
