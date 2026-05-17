package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeVoteAssetTx(ownerByte byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.VoteAssetContract{
		OwnerAddress: owner.Bytes(),
		Count:        1,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_VoteAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestCreateActuatorRejectsVoteAsset(t *testing.T) {
	tx := makeVoteAssetTx(1)
	if act, err := CreateActuator(tx); err == nil || act != nil {
		t.Fatalf("VoteAssetContract should be unsupported, actuator=%T err=%v", act, err)
	}
}
