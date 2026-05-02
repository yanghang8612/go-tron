package tronapi

import (
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// wireSortFrozenV2 returns a clone of acc with its FrozenV2 list rewritten
// to match java-tron's Wallet.getAccount output: exactly one entry per
// ResourceCode (BANDWIDTH, ENERGY, TRON_POWER) in enum order, with
// 0-amount placeholders for resources the account hasn't frozen.
//
// Mirrors java-tron's Wallet.sortFrozenV2List
// (framework/src/main/java/org/tron/core/Wallet.java) so that
// /wallet/getaccount responses are byte-equivalent across implementations.
//
// The actual on-disk account is unchanged.
func wireSortFrozenV2(acc *corepb.Account) *corepb.Account {
	if acc == nil {
		return nil
	}
	out := proto.Clone(acc).(*corepb.Account)
	old := out.FrozenV2
	resources := []corepb.ResourceCode{
		corepb.ResourceCode_BANDWIDTH,
		corepb.ResourceCode_ENERGY,
		corepb.ResourceCode_TRON_POWER,
	}
	out.FrozenV2 = make([]*corepb.Account_FreezeV2, 0, len(resources))
	for _, code := range resources {
		entry := &corepb.Account_FreezeV2{Type: code}
		for _, prev := range old {
			if prev.GetType() == code {
				entry = prev
				break
			}
		}
		out.FrozenV2 = append(out.FrozenV2, entry)
	}
	return out
}
