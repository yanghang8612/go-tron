package conformance

import (
	"fmt"
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ComputeClosure walks every tx in every block and returns the sorted
// deduplicated set of addresses a replay might touch: the block witness,
// every address field referenced by each contract type, and any extra
// addresses the caller supplies via `extras`.
//
// The standby top-127 witnesses MUST be passed through `extras` — once
// `change_delegation` is active (mainnet default since 2021),
// payStandbyWitness touches their allowance/cycle-reward on every block,
// and none of those addresses appear in any transaction. Missing them
// would silently drop state changes from the digest.
//
// Contract types we don't yet extract are logged to `unhandled` (key =
// type, value = count) so the operator can decide whether to hand-extend
// the closure. The closure is a best-effort upper bound; the operator
// can still extend it after-the-fact via seed.json if replay surfaces a
// missed address.
func ComputeClosure(blocks []*types.Block, extras []tcommon.Address) (addrs []tcommon.Address, unhandled map[corepb.Transaction_Contract_ContractType]int, err error) {
	seen := make(map[tcommon.Address]struct{})
	unhandled = make(map[corepb.Transaction_Contract_ContractType]int)

	add := func(b []byte) {
		if len(b) == 0 {
			return
		}
		var a tcommon.Address
		copy(a[:], b)
		seen[a] = struct{}{}
	}

	for _, a := range extras {
		seen[a] = struct{}{}
	}

	for _, blk := range blocks {
		add(blk.WitnessAddress().Bytes())
		for _, tx := range blk.Transactions() {
			c := tx.Contract()
			if c == nil {
				continue
			}
			if err := extractContractAddrs(c, add, unhandled); err != nil {
				return nil, nil, fmt.Errorf("block %d: %w", blk.Number(), err)
			}
		}
	}

	addrs = make([]tcommon.Address, 0, len(seen))
	for a := range seen {
		addrs = append(addrs, a)
	}
	sort.Slice(addrs, func(i, j int) bool {
		for k := 0; k < len(addrs[i]); k++ {
			if addrs[i][k] != addrs[j][k] {
				return addrs[i][k] < addrs[j][k]
			}
		}
		return false
	})
	return addrs, unhandled, nil
}

// extractContractAddrs pulls every known address field out of one Contract.
func extractContractAddrs(
	c *corepb.Transaction_Contract,
	add func([]byte),
	unhandled map[corepb.Transaction_Contract_ContractType]int,
) error {
	switch c.Type {
	case corepb.Transaction_Contract_TransferContract:
		var m contractpb.TransferContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ToAddress)

	case corepb.Transaction_Contract_TransferAssetContract:
		var m contractpb.TransferAssetContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ToAddress)

	case corepb.Transaction_Contract_TriggerSmartContract:
		var m contractpb.TriggerSmartContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ContractAddress)

	case corepb.Transaction_Contract_CreateSmartContract:
		var m contractpb.CreateSmartContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		// The new contract's address is deterministic from (owner, nonce)
		// but not yet determinable here; operator extends closure after
		// first replay pass if needed.

	case corepb.Transaction_Contract_VoteWitnessContract:
		var m contractpb.VoteWitnessContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		for _, v := range m.Votes {
			add(v.VoteAddress)
		}

	case corepb.Transaction_Contract_FreezeBalanceContract:
		var m contractpb.FreezeBalanceContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ReceiverAddress)

	case corepb.Transaction_Contract_UnfreezeBalanceContract:
		var m contractpb.UnfreezeBalanceContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ReceiverAddress)

	case corepb.Transaction_Contract_FreezeBalanceV2Contract:
		var m contractpb.FreezeBalanceV2Contract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_UnfreezeBalanceV2Contract:
		var m contractpb.UnfreezeBalanceV2Contract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_WithdrawExpireUnfreezeContract:
		var m contractpb.WithdrawExpireUnfreezeContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_DelegateResourceContract:
		var m contractpb.DelegateResourceContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ReceiverAddress)

	case corepb.Transaction_Contract_UnDelegateResourceContract:
		var m contractpb.UnDelegateResourceContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ReceiverAddress)

	case corepb.Transaction_Contract_WithdrawBalanceContract:
		var m contractpb.WithdrawBalanceContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_AccountUpdateContract:
		var m contractpb.AccountUpdateContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_AccountPermissionUpdateContract:
		var m contractpb.AccountPermissionUpdateContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_UpdateBrokerageContract:
		var m contractpb.UpdateBrokerageContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_AccountCreateContract:
		var m contractpb.AccountCreateContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.AccountAddress)

	case corepb.Transaction_Contract_WitnessCreateContract:
		var m contractpb.WitnessCreateContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_WitnessUpdateContract:
		var m contractpb.WitnessUpdateContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_ProposalCreateContract:
		var m contractpb.ProposalCreateContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_ProposalApproveContract:
		var m contractpb.ProposalApproveContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_ProposalDeleteContract:
		var m contractpb.ProposalDeleteContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_AssetIssueContract:
		var m contractpb.AssetIssueContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_UpdateAssetContract:
		var m contractpb.UpdateAssetContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_UnfreezeAssetContract:
		var m contractpb.UnfreezeAssetContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_ParticipateAssetIssueContract:
		var m contractpb.ParticipateAssetIssueContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ToAddress)

	case corepb.Transaction_Contract_VoteAssetContract:
		var m contractpb.VoteAssetContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_SetAccountIdContract:
		var m contractpb.SetAccountIdContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_ExchangeCreateContract:
		var m contractpb.ExchangeCreateContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_ExchangeInjectContract:
		var m contractpb.ExchangeInjectContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_ExchangeWithdrawContract:
		var m contractpb.ExchangeWithdrawContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_ExchangeTransactionContract:
		var m contractpb.ExchangeTransactionContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_MarketSellAssetContract:
		var m contractpb.MarketSellAssetContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_MarketCancelOrderContract:
		var m contractpb.MarketCancelOrderContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	case corepb.Transaction_Contract_UpdateSettingContract:
		var m contractpb.UpdateSettingContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ContractAddress)

	case corepb.Transaction_Contract_UpdateEnergyLimitContract:
		var m contractpb.UpdateEnergyLimitContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ContractAddress)

	case corepb.Transaction_Contract_ClearABIContract:
		var m contractpb.ClearABIContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)
		add(m.ContractAddress)

	case corepb.Transaction_Contract_ShieldedTransferContract:
		var m contractpb.ShieldedTransferContract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.TransparentFromAddress)
		add(m.TransparentToAddress)

	case corepb.Transaction_Contract_CancelAllUnfreezeV2Contract:
		var m contractpb.CancelAllUnfreezeV2Contract
		if err := c.Parameter.UnmarshalTo(&m); err != nil {
			return err
		}
		add(m.OwnerAddress)

	default:
		unhandled[c.Type]++
	}
	return nil
}
