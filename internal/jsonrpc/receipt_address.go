package jsonrpc

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// receiptAddress matches java-tron's ByteArray.toJsonHexAddress. A missing
// recipient is JSON null. Present TRON addresses omit their network prefix.
func receiptAddress(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	if raw[0] == common.AddressPrefixMainnet {
		raw = raw[1:]
	}
	return hexBytes(raw)
}

// receiptRecipient follows JsonRpcApiUtil.getToAddress, which selects the
// first address for voting contracts and uses null for calls without a target.
func receiptRecipient(tx *types.Transaction) []byte {
	message, err := tx.DecodedContract()
	if err != nil {
		return nil
	}
	switch contract := message.(type) {
	case *contractpb.AccountCreateContract:
		return contract.GetAccountAddress()
	case *contractpb.TransferContract:
		return contract.GetToAddress()
	case *contractpb.TransferAssetContract:
		return contract.GetToAddress()
	case *contractpb.VoteAssetContract:
		addresses := contract.GetVoteAddress()
		if len(addresses) > 0 {
			return addresses[0]
		}
	case *contractpb.VoteWitnessContract:
		votes := contract.GetVotes()
		if len(votes) > 0 {
			return votes[0].GetVoteAddress()
		}
	case *contractpb.ParticipateAssetIssueContract:
		return contract.GetToAddress()
	case *contractpb.FreezeBalanceContract:
		return contract.GetReceiverAddress()
	case *contractpb.UnfreezeBalanceContract:
		return contract.GetReceiverAddress()
	case *contractpb.DelegateResourceContract:
		return contract.GetReceiverAddress()
	case *contractpb.UnDelegateResourceContract:
		return contract.GetReceiverAddress()
	case *contractpb.TriggerSmartContract:
		return contract.GetContractAddress()
	case *contractpb.UpdateSettingContract:
		return contract.GetContractAddress()
	case *contractpb.UpdateEnergyLimitContract:
		return contract.GetContractAddress()
	case *contractpb.ClearABIContract:
		return contract.GetContractAddress()
	case *contractpb.ShieldedTransferContract:
		return contract.GetTransparentToAddress()
	}
	return nil
}
