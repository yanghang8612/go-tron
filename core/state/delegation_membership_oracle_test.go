package state

import corepb "github.com/tronprotocol/go-tron/proto/core"

// Frozen from 5915d3cb: only these two function names were replaced. This is
// the pre-arena membership/complete-entry control, not the older generic codec.
func frozenLegacyDelegationEntry(key []byte, rec *corepb.DelegatedResourceAccountIndex) *legacyDelegationEntry {
	e := &legacyDelegationEntry{key: string(key), record: rec}
	// Conservative accounting includes message/entry/cache-map overhead, the
	// decoder's complete byte/header arenas, and separately owned membership
	// keys plus map buckets. Oversized rows do not allocate membership tables.
	e.charge = 512 + len(key) + len(rec.Account) + len(rec.ProtoReflect().GetUnknown())
	for _, list := range [][][]byte{rec.FromAccounts, rec.ToAccounts} {
		e.charge += cap(list) * 24
		for _, addr := range list {
			e.charge += len(addr)
		}
		if len(list) >= legacyDelegationSetMinimum {
			for _, addr := range list {
				e.charge += 96 + len(addr)
			}
		}
	}
	if e.charge <= legacyDelegationCacheBytes {
		e.fromSet = frozenLegacyDelegationMembership(rec.FromAccounts)
		e.toSet = frozenLegacyDelegationMembership(rec.ToAccounts)
	}
	return e
}

func frozenLegacyDelegationMembership(list [][]byte) map[string]struct{} {
	if len(list) < legacyDelegationSetMinimum {
		return nil
	}
	set := make(map[string]struct{}, len(list))
	for _, addr := range list {
		set[string(addr)] = struct{}{}
	}
	return set
}
