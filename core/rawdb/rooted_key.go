package rawdb

import (
	"bytes"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// RootedStateKey describes how a legacy flat rawdb key is represented in the
// rooted generic account-KV state. The logical key deliberately keeps the full
// legacy physical key so every old store preserves its exact namespace while
// the owner/domain pair pulls it under the full state root.
type RootedStateKey struct {
	Owner  common.Address
	Domain kvdomains.KVDomain
	Key    []byte
}

// LookupRootedStateKey maps consensus-relevant mutable flat-store keys into the
// rooted account-KV model. Immutable chain data, replay-derived indexes,
// runtime/finality metadata, archive rows, and caches intentionally stay flat.
func LookupRootedStateKey(key []byte) (RootedStateKey, bool) {
	if len(key) == 0 {
		return RootedStateKey{}, false
	}
	if owner, _, ok := ownerAndRestAfterPrefix(key, codePrefix); ok {
		return rooted(owner, kvdomains.ContractMetadata, []byte("code")), true
	}
	if owner, _, ok := ownerAndRestAfterPrefix(key, contractPrefix); ok {
		return rooted(owner, kvdomains.ContractMetadata, []byte("meta")), true
	}
	if owner, rest, ok := ownerAndRestAfterPrefix(key, storagePrefix); ok {
		return rooted(owner, kvdomains.ContractStorage, rest), true
	}
	if owner, _, ok := ownerAndRestAfterPrefix(key, abiPrefix); ok {
		return rooted(owner, kvdomains.ContractABI, []byte("abi")), true
	}
	if owner, _, ok := ownerAndRestAfterPrefix(key, contractStatePrefix); ok {
		return rooted(owner, kvdomains.ContractRuntimeState, []byte("state")), true
	}
	if owner, ok := ownerAfterPrefix(key, witnessPrefix); ok {
		return rooted(owner, kvdomains.WitnessCapsule, key), true
	}
	if owner, ok := ownerAfterPrefix(key, witnessLatestBlockPrefix); ok {
		return rooted(owner, kvdomains.WitnessCapsule, key), true
	}
	if owner, ok := ownerAfterPrefix(key, brokeragePrefix); ok {
		return rooted(owner, kvdomains.WitnessCapsule, key), true
	}
	if owner, ok := ownerAfterPrefix(key, accountAssetPrefix); ok {
		return rooted(owner, kvdomains.AccountLocalIndex, key), true
	}

	switch {
	case bytes.HasPrefix(key, dynPropPrefix):
		return rootedSystem(kvdomains.SystemDynamicProperty, key), true
	case bytes.Equal(key, witnessScheduleKey):
		return rootedSystem(kvdomains.SystemWitnessSchedule, key), true
	case bytes.HasPrefix(key, forkStatsPrefix):
		return rootedSystem(kvdomains.SystemForkVote, key), true
	case bytes.HasPrefix(key, delegationPrefix),
		bytes.HasPrefix(key, delegationIndexPrefix),
		bytes.HasPrefix(key, drAccIdxPrefix):
		return rootedSystem(kvdomains.SystemDelegation, key), true
	case bytes.HasPrefix(key, delegRewardPrefix),
		bytes.HasPrefix(key, rewardViPrefix):
		return rootedSystem(kvdomains.SystemReward, key), true
	case bytes.HasPrefix(key, nullifierPrefix),
		bytes.HasPrefix(key, noteCommitmentPrefix),
		bytes.Equal(key, noteCommitmentCountKey),
		bytes.HasPrefix(key, zkProofPrefix),
		bytes.HasPrefix(key, incrMerkleTreePrefix),
		bytes.Equal(key, incrMerkleLastTreeKey),
		bytes.Equal(key, incrMerkleCurrentTreeKey),
		bytes.HasPrefix(key, merkleTreeIndexPrefix):
		return rootedSystem(kvdomains.SystemShielded, key), true
	}
	return RootedStateKey{}, false
}

func rooted(owner common.Address, domain kvdomains.KVDomain, key []byte) RootedStateKey {
	return RootedStateKey{Owner: owner, Domain: domain, Key: append([]byte(nil), key...)}
}

func rootedSystem(domain kvdomains.KVDomain, key []byte) RootedStateKey {
	return rooted(common.SystemAccountAddress, domain, key)
}

func ownerAfterPrefix(key, prefix []byte) (common.Address, bool) {
	if len(key) < len(prefix)+common.AddressLength || !bytes.HasPrefix(key, prefix) {
		return common.Address{}, false
	}
	return common.BytesToAddress(key[len(prefix) : len(prefix)+common.AddressLength]), true
}

func ownerAndRestAfterPrefix(key, prefix []byte) (common.Address, []byte, bool) {
	if len(key) < len(prefix)+common.AddressLength || !bytes.HasPrefix(key, prefix) {
		return common.Address{}, nil, false
	}
	ownerStart := len(prefix)
	restStart := ownerStart + common.AddressLength
	return common.BytesToAddress(key[ownerStart:restStart]), append([]byte(nil), key[restStart:]...), true
}
