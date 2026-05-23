package rawdb

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestLookupRootedStateKeyConsensusMappings(t *testing.T) {
	owner := common.BytesToAddress([]byte{
		0x41, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
		0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
		0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
	})
	slot := common.BytesToHash([]byte{0x99})

	tests := []struct {
		name   string
		key    []byte
		owner  common.Address
		domain kvdomains.KVDomain
	}{
		{
			name:   "dynamic property",
			key:    dynPropKey("next_maintenance_time"),
			owner:  common.SystemAccountAddress,
			domain: kvdomains.SystemDynamicProperty,
		},
		{
			name:   "witness capsule",
			key:    witnessKey(owner.Bytes()),
			owner:  owner,
			domain: kvdomains.WitnessCapsule,
		},
		{
			name:   "witness latest block",
			key:    witnessLatestBlockKey(owner.Bytes()),
			owner:  owner,
			domain: kvdomains.WitnessCapsule,
		},
		{
			name:   "contract storage",
			key:    storageKey(owner.Bytes(), slot.Bytes()),
			owner:  owner,
			domain: kvdomains.ContractStorage,
		},
		{
			name:   "fork vote",
			key:    forkStatsKey(27),
			owner:  common.SystemAccountAddress,
			domain: kvdomains.SystemForkVote,
		},
		{
			name:   "delegation",
			key:    delegationKeyV2(owner.Bytes(), common.SystemAccountAddress.Bytes(), false),
			owner:  common.SystemAccountAddress,
			domain: kvdomains.SystemDelegation,
		},
		{
			name:   "shielded nullifier",
			key:    nullifierKey([]byte("nullifier")),
			owner:  common.SystemAccountAddress,
			domain: kvdomains.SystemShielded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rk, ok := LookupRootedStateKey(tt.key)
			if !ok {
				t.Fatal("key was not rooted")
			}
			if rk.Owner != tt.owner || rk.Domain != tt.domain {
				t.Fatalf("mapping = (%s,%s), want (%s,%s)",
					rk.Owner.Hex(), kvdomains.Name(rk.Domain),
					tt.owner.Hex(), kvdomains.Name(tt.domain))
			}
		})
	}
}

func TestLookupRootedStateKeyRuntimeDerivedAndHistoryStayFlat(t *testing.T) {
	owner := common.BytesToAddress([]byte{
		0x41, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26,
		0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
		0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34,
	})

	tests := []struct {
		name string
		key  []byte
	}{
		{name: "pbft block sign data", key: pbftBlockSignKey(123)},
		{name: "pbft sr sign data", key: pbftSrSignKey(7)},
		{name: "latest pbft cursor", key: latestPbftBlockNumKey},
		{name: "shuffled witnesses", key: shuffledWitnessesKey},
		{name: "previous shuffled witnesses", key: previousShuffledWitnessesKey},
		{name: "tapos recent block", key: taposKey([]byte{0x00, 0x01})},
		{name: "total transaction count", key: totalTransactionCountKey},
		{name: "legacy address code mirror", key: codeKey(owner.Bytes())},
		{name: "state kv latest physical index", key: stateKVLatestKey(owner, 0, kvdomains.SystemDelegation, []byte("k"))},
		{name: "state kv generation physical index", key: stateKVGenerationKey(owner)},
		{name: "account trace", key: accountTraceKey(owner.Bytes(), 12)},
		{name: "balance trace", key: balanceTraceKey(12)},
		{name: "section bloom", key: sectionBloomKey(3, 42)},
		{name: "tree block index", key: treeBlockIndexKey(12)},
		{name: "checkpoint v2", key: append(append([]byte{}, checkPointV2Prefix...), []byte("row")...)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rk, ok := LookupRootedStateKey(tt.key); ok {
				t.Fatalf("runtime/derived/history key rooted as owner=%s domain=%s",
					rk.Owner.Hex(), kvdomains.Name(rk.Domain))
			}
		})
	}
}
