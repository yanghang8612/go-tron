package actuator

import (
	"bytes"
	"fmt"
	"strconv"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type ownerAddressMessage interface {
	proto.Message
	GetOwnerAddress() []byte
}

type ownerToAddressMessage interface {
	ownerAddressMessage
	GetToAddress() []byte
}

type ownerContractAddressMessage interface {
	ownerAddressMessage
	GetContractAddress() []byte
}

// PrefetchKeysFor returns deterministic raw latest-domain reads that can be
// warmed before tx validation/execution reaches the serial hot path. It is a
// best-effort performance hint: malformed payloads or invalid addresses simply
// produce fewer keys and must not change actuator validation behaviour.
func PrefetchKeysFor(tx *types.Transaction) []state.PrefetchKey {
	if tx == nil {
		return nil
	}
	c := tx.Contract()
	if c == nil {
		return nil
	}
	var b prefetchKeyBuilder

	switch c.Type {
	case corepb.Transaction_Contract_TransferContract:
		var m contractpb.TransferContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addOwnerTo(&m, true)

	case corepb.Transaction_Contract_TransferAssetContract:
		var m contractpb.TransferAssetContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addOwnerTo(&m, true)
		b.addTRC10AssetKeys(m.GetAssetName())

	case corepb.Transaction_Contract_ParticipateAssetIssueContract:
		var m contractpb.ParticipateAssetIssueContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addOwnerTo(&m, false)
		b.addTRC10AssetKeys(m.GetAssetName())

	case corepb.Transaction_Contract_TriggerSmartContract:
		var m contractpb.TriggerSmartContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		if contract, ok := b.addOwnerAndContract(&m); ok {
			b.add(state.ContractCodePrefetchKey(contract))
			b.add(state.ContractOriginAccountPrefetchKey(contract))
		}

	case corepb.Transaction_Contract_CreateSmartContract:
		var m contractpb.CreateSmartContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ok := b.addAccountBytes(m.GetOwnerAddress())
		if !ok {
			break
		}
		if nc := m.GetNewContract(); nc != nil {
			b.addAccountBytes(nc.GetOriginAddress())
		}
		created := generateContractAddress(tx, owner)
		b.add(state.AccountPrefetchKey(created))
		b.add(state.ContractMetadataPrefetchKey(created))

	case corepb.Transaction_Contract_UpdateSettingContract:
		var m contractpb.UpdateSettingContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		if contract, ok := b.addOwnerAndContract(&m); ok {
			b.add(state.ContractOriginAccountPrefetchKey(contract))
		}

	case corepb.Transaction_Contract_UpdateEnergyLimitContract:
		var m contractpb.UpdateEnergyLimitContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		if contract, ok := b.addOwnerAndContract(&m); ok {
			b.add(state.ContractOriginAccountPrefetchKey(contract))
		}

	case corepb.Transaction_Contract_ClearABIContract:
		var m contractpb.ClearABIContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		if contract, ok := b.addOwnerAndContract(&m); ok {
			b.add(state.ContractOriginAccountPrefetchKey(contract))
		}

	case corepb.Transaction_Contract_VoteWitnessContract:
		var m contractpb.VoteWitnessContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ownerOK := b.addAccountBytes(m.GetOwnerAddress())
		if ownerOK {
			b.add(state.PendingVotesPrefetchKey(owner))
			b.add(state.PendingVotesIndexPrefetchKey())
		}
		for _, vote := range m.GetVotes() {
			if vote != nil {
				if target, ok := b.addAccountBytes(vote.GetVoteAddress()); ok {
					b.add(state.WitnessCapsulePrefetchKey(target))
				}
			}
		}

	case corepb.Transaction_Contract_FreezeBalanceContract:
		var m contractpb.FreezeBalanceContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ownerOK := b.addAccountBytes(m.GetOwnerAddress())
		receiver, receiverOK := b.addAccountBytes(m.GetReceiverAddress())
		if ownerOK && receiverOK {
			b.addLegacyDelegationKeys(owner, receiver)
		}

	case corepb.Transaction_Contract_UnfreezeBalanceContract:
		var m contractpb.UnfreezeBalanceContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ownerOK := b.addAccountBytes(m.GetOwnerAddress())
		receiver, receiverOK := b.addAccountBytes(m.GetReceiverAddress())
		if ownerOK && receiverOK {
			b.addLegacyDelegationKeys(owner, receiver)
		}

	case corepb.Transaction_Contract_DelegateResourceContract:
		var m contractpb.DelegateResourceContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ownerOK := b.addAccountBytes(m.GetOwnerAddress())
		receiver, receiverOK := b.addAccountBytes(m.GetReceiverAddress())
		if ownerOK && receiverOK {
			b.addV2DelegationKeys(owner, receiver)
		}

	case corepb.Transaction_Contract_UnDelegateResourceContract:
		var m contractpb.UnDelegateResourceContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ownerOK := b.addAccountBytes(m.GetOwnerAddress())
		receiver, receiverOK := b.addAccountBytes(m.GetReceiverAddress())
		if ownerOK && receiverOK {
			b.addV2DelegationKeys(owner, receiver)
		}

	case corepb.Transaction_Contract_ShieldedTransferContract:
		var m contractpb.ShieldedTransferContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addAccountBytes(m.GetTransparentFromAddress())
		b.addAccountBytes(m.GetTransparentToAddress())

	case corepb.Transaction_Contract_AccountCreateContract:
		var m contractpb.AccountCreateContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addAccountBytes(m.GetOwnerAddress())
		b.addAccountBytes(m.GetAccountAddress())

	case corepb.Transaction_Contract_AssetIssueContract:
		var m contractpb.AssetIssueContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ok := b.addAccountBytes(m.GetOwnerAddress())
		if ok {
			b.add(state.AssetOwnerIndexPrefetchKey(owner.Bytes()))
		}
		b.add(state.AssetIssueByNamePrefetchKey(m.GetName()))
		b.add(state.AssetNameIndexPrefetchKey(m.GetName()))
	case corepb.Transaction_Contract_WitnessCreateContract:
		if owner, ok := prefetchOwnerOnly(c, &b, &contractpb.WitnessCreateContract{}); ok {
			b.add(state.WitnessCapsulePrefetchKey(owner))
			b.add(state.WitnessIndexPrefetchKey())
		}
	case corepb.Transaction_Contract_WitnessUpdateContract:
		if owner, ok := prefetchOwnerOnly(c, &b, &contractpb.WitnessUpdateContract{}); ok {
			b.add(state.WitnessCapsulePrefetchKey(owner))
		}
	case corepb.Transaction_Contract_AccountUpdateContract:
		prefetchOwnerOnly(c, &b, &contractpb.AccountUpdateContract{})
	case corepb.Transaction_Contract_SetAccountIdContract:
		prefetchOwnerOnly(c, &b, &contractpb.SetAccountIdContract{})
	case corepb.Transaction_Contract_AccountPermissionUpdateContract:
		prefetchOwnerOnly(c, &b, &contractpb.AccountPermissionUpdateContract{})
	case corepb.Transaction_Contract_UpdateBrokerageContract:
		if owner, ok := prefetchOwnerOnly(c, &b, &contractpb.UpdateBrokerageContract{}); ok {
			b.add(state.WitnessCapsulePrefetchKey(owner))
			b.add(state.WitnessBrokeragePrefetchKey(owner))
		}
	case corepb.Transaction_Contract_ProposalCreateContract:
		if owner, ok := prefetchOwnerOnly(c, &b, &contractpb.ProposalCreateContract{}); ok {
			b.add(state.WitnessCapsulePrefetchKey(owner))
			b.add(state.ProposalIndexPrefetchKey())
		}
	case corepb.Transaction_Contract_ProposalApproveContract:
		var m contractpb.ProposalApproveContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		if owner, ok := b.addAccountBytes(m.GetOwnerAddress()); ok {
			b.add(state.WitnessCapsulePrefetchKey(owner))
		}
		b.addProposal(m.GetProposalId())
	case corepb.Transaction_Contract_ProposalDeleteContract:
		var m contractpb.ProposalDeleteContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addAccountBytes(m.GetOwnerAddress())
		b.addProposal(m.GetProposalId())
	case corepb.Transaction_Contract_FreezeBalanceV2Contract:
		prefetchOwnerOnly(c, &b, &contractpb.FreezeBalanceV2Contract{})
	case corepb.Transaction_Contract_UnfreezeBalanceV2Contract:
		prefetchOwnerOnly(c, &b, &contractpb.UnfreezeBalanceV2Contract{})
	case corepb.Transaction_Contract_WithdrawBalanceContract:
		prefetchOwnerOnly(c, &b, &contractpb.WithdrawBalanceContract{})
	case corepb.Transaction_Contract_WithdrawExpireUnfreezeContract:
		prefetchOwnerOnly(c, &b, &contractpb.WithdrawExpireUnfreezeContract{})
	case corepb.Transaction_Contract_CancelAllUnfreezeV2Contract:
		prefetchOwnerOnly(c, &b, &contractpb.CancelAllUnfreezeV2Contract{})
	case corepb.Transaction_Contract_UpdateAssetContract:
		prefetchOwnerOnly(c, &b, &contractpb.UpdateAssetContract{})
	case corepb.Transaction_Contract_UnfreezeAssetContract:
		prefetchOwnerOnly(c, &b, &contractpb.UnfreezeAssetContract{})
	case corepb.Transaction_Contract_MarketSellAssetContract:
		var m contractpb.MarketSellAssetContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ok := b.addAccountBytes(m.GetOwnerAddress())
		if ok {
			b.add(state.MarketAccountOrderPrefetchKey(owner.Bytes()))
		}
		b.addTRC10AssetKeys(m.GetSellTokenId())
		b.addTRC10AssetKeys(m.GetBuyTokenId())
		b.addMarketPairKeys(m.GetSellTokenId(), m.GetBuyTokenId(), m.GetSellTokenQuantity(), m.GetBuyTokenQuantity())
		b.add(state.MarketPriceListPrefetchKey(m.GetBuyTokenId(), m.GetSellTokenId()))
	case corepb.Transaction_Contract_MarketCancelOrderContract:
		var m contractpb.MarketCancelOrderContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		owner, ok := b.addAccountBytes(m.GetOwnerAddress())
		if ok {
			b.add(state.MarketAccountOrderPrefetchKey(owner.Bytes()))
		}
		b.add(state.MarketOrderPrefetchKey(m.GetOrderId()))
	case corepb.Transaction_Contract_ExchangeCreateContract:
		var m contractpb.ExchangeCreateContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addAccountBytes(m.GetOwnerAddress())
		b.addTRC10AssetKeys(m.GetFirstTokenId())
		b.addTRC10AssetKeys(m.GetSecondTokenId())
	case corepb.Transaction_Contract_ExchangeInjectContract:
		var m contractpb.ExchangeInjectContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addAccountBytes(m.GetOwnerAddress())
		b.addExchangeKeys(m.GetExchangeId())
		b.addTRC10AssetKeys(m.GetTokenId())
	case corepb.Transaction_Contract_ExchangeWithdrawContract:
		var m contractpb.ExchangeWithdrawContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addAccountBytes(m.GetOwnerAddress())
		b.addExchangeKeys(m.GetExchangeId())
		b.addTRC10AssetKeys(m.GetTokenId())
	case corepb.Transaction_Contract_ExchangeTransactionContract:
		var m contractpb.ExchangeTransactionContract
		if !prefetchDecode(c, &m) {
			return nil
		}
		b.addAccountBytes(m.GetOwnerAddress())
		b.addExchangeKeys(m.GetExchangeId())
		b.addTRC10AssetKeys(m.GetTokenId())
	}

	return b.keys
}

type prefetchKeyBuilder struct {
	keys []state.PrefetchKey
	seen map[string]struct{}
}

func prefetchDecode(c *corepb.Transaction_Contract, msg proto.Message) bool {
	if c == nil || c.Parameter == nil {
		return false
	}
	return c.Parameter.UnmarshalTo(msg) == nil
}

func prefetchOwnerOnly(c *corepb.Transaction_Contract, b *prefetchKeyBuilder, msg ownerAddressMessage) (tcommon.Address, bool) {
	if !prefetchDecode(c, msg) {
		return tcommon.Address{}, false
	}
	return b.addAccountBytes(msg.GetOwnerAddress())
}

func (b *prefetchKeyBuilder) addOwnerTo(msg ownerToAddressMessage, toContractMetadata bool) {
	b.addAccountBytes(msg.GetOwnerAddress())
	to, ok := b.addAccountBytes(msg.GetToAddress())
	if ok && toContractMetadata {
		b.add(state.ContractMetadataPrefetchKey(to))
	}
}

func (b *prefetchKeyBuilder) addOwnerAndContract(msg ownerContractAddressMessage) (tcommon.Address, bool) {
	b.addAccountBytes(msg.GetOwnerAddress())
	contract, ok := b.addAccountBytes(msg.GetContractAddress())
	if ok {
		b.add(state.ContractMetadataPrefetchKey(contract))
	}
	return contract, ok
}

func (b *prefetchKeyBuilder) addAccountBytes(raw []byte) (tcommon.Address, bool) {
	if !validAddressBytes(raw) {
		return tcommon.Address{}, false
	}
	addr := tcommon.BytesToAddress(raw)
	b.add(state.AccountPrefetchKey(addr))
	return addr, true
}

func (b *prefetchKeyBuilder) addLegacyDelegationKeys(owner, receiver tcommon.Address) {
	b.addSystemDelegationKey(rawdb.DelegatedResourceStateKey(owner, receiver))
	b.addSystemDelegationKey(rawdb.DrAccountIndexLegacyStateKey(owner.Bytes()))
	b.addSystemDelegationKey(rawdb.DrAccountIndexLegacyStateKey(receiver.Bytes()))
	b.addSystemDelegationKey(rawdb.DrAccountIndexStateKey(rawdb.DrAccIdxV1From, owner.Bytes(), receiver.Bytes()))
	b.addSystemDelegationKey(rawdb.DrAccountIndexStateKey(rawdb.DrAccIdxV1To, receiver.Bytes(), owner.Bytes()))
}

func (b *prefetchKeyBuilder) addV2DelegationKeys(owner, receiver tcommon.Address) {
	b.addSystemDelegationKey(rawdb.DelegatedResourceV2StateKey(owner, receiver, false))
	b.addSystemDelegationKey(rawdb.DelegatedResourceV2StateKey(owner, receiver, true))
	b.addSystemDelegationKey(rawdb.DelegationIndexStateKey(owner))
}

func (b *prefetchKeyBuilder) addSystemDelegationKey(key []byte) {
	b.add(state.AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key))
}

func (b *prefetchKeyBuilder) addProposal(id int64) {
	if id <= 0 {
		return
	}
	b.add(state.ProposalPrefetchKey(id))
}

func (b *prefetchKeyBuilder) addTRC10AssetKeys(token []byte) {
	if len(token) == 0 || bytes.Equal(token, []byte("_")) {
		return
	}
	b.add(state.AssetIssueByNamePrefetchKey(token))
	b.add(state.AssetNameIndexPrefetchKey(token))
	if isNumericBytes(token) {
		if tokenID, err := strconv.ParseInt(string(token), 10, 64); err == nil {
			b.add(state.AssetIssuePrefetchKey(tokenID))
		}
	}
}

func (b *prefetchKeyBuilder) addMarketPairKeys(sellToken, buyToken []byte, sellQty, buyQty int64) {
	b.add(state.MarketPriceListPrefetchKey(sellToken, buyToken))
	b.add(state.MarketPairPriceCountPrefetchKey(sellToken, buyToken))
	if sellQty <= 0 || buyQty <= 0 {
		return
	}
	b.add(state.MarketOrderBookPrefetchKey(sellToken, buyToken, rawdb.PriceKey(sellQty, buyQty)))
}

func (b *prefetchKeyBuilder) addExchangeKeys(exchangeID int64) {
	if exchangeID <= 0 {
		return
	}
	b.add(state.ExchangePrefetchKey(exchangeID))
	b.add(state.ExchangeV2PrefetchKey(exchangeID))
}

func (b *prefetchKeyBuilder) add(key state.PrefetchKey) {
	if key.Kind == 0 {
		return
	}
	if b.seen == nil {
		b.seen = make(map[string]struct{})
	}
	id := prefetchKeyID(key)
	if _, ok := b.seen[id]; ok {
		return
	}
	b.seen[id] = struct{}{}
	b.keys = append(b.keys, key)
}

func prefetchKeyID(key state.PrefetchKey) string {
	return fmt.Sprintf("%d:%x:%04x:%x:%x:%d:%t",
		key.Kind,
		key.Owner[:],
		uint16(key.Domain),
		key.Key,
		key.Slot[:],
		key.Generation,
		key.HasGeneration,
	)
}
