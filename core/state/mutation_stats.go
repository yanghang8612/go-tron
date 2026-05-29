package state

import (
	"fmt"
	"sort"
	"strings"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const kvDomainStatCount = 25

var kvDomainStatOrder = [kvDomainStatCount]kvdomains.KVDomain{
	kvdomains.SystemDynamicProperty,
	kvdomains.SystemWitnessSchedule,
	kvdomains.SystemProposal,
	kvdomains.SystemForkVote,
	kvdomains.SystemAsset,
	kvdomains.SystemExchange,
	kvdomains.SystemDelegation,
	kvdomains.SystemAccountIndex,
	kvdomains.SystemMarket,
	kvdomains.SystemReward,
	kvdomains.SystemShielded,
	kvdomains.SystemForkAux,
	kvdomains.SystemPBFT,
	kvdomains.SystemTapos,
	kvdomains.SystemTrace,
	kvdomains.SystemBloom,
	kvdomains.SystemCheckpoint,
	kvdomains.ContractStorage,
	kvdomains.ContractMetadata,
	kvdomains.ContractABI,
	kvdomains.ContractRuntimeState,
	kvdomains.AccountLocalIndex,
	kvdomains.AccountPermissionAux,
	kvdomains.WitnessCapsule,
	kvdomains.WitnessVoteState,
}

// KVDomainMutationStats counts final committed KV mutations for one domain.
type KVDomainMutationStats struct {
	Puts    int
	Deletes int
	Noops   int
}

func (s KVDomainMutationStats) total() int {
	return s.Puts + s.Deletes + s.Noops
}

// CommitMutationStats captures the shape of state writes in one Commit. Counts
// are based on the final commit plan after same-block overwrite collapse, so
// they describe what the block is about to persist rather than every setter
// call that happened during execution.
type CommitMutationStats struct {
	AccountCreates int
	AccountUpdates int
	AccountDeletes int

	CodeUpdates         int
	CodeDeletes         int
	ContractMetaUpdates int
	ContractMetaDeletes int

	StoragePuts    int
	StorageDeletes int
	StorageNoops   int

	KVPutItems    int
	KVDeleteItems int
	KVNoopItems   int
	KVDomains     [kvDomainStatCount]KVDomainMutationStats
}

func (s *CommitMutationStats) Add(o CommitMutationStats) {
	s.AccountCreates += o.AccountCreates
	s.AccountUpdates += o.AccountUpdates
	s.AccountDeletes += o.AccountDeletes
	s.CodeUpdates += o.CodeUpdates
	s.CodeDeletes += o.CodeDeletes
	s.ContractMetaUpdates += o.ContractMetaUpdates
	s.ContractMetaDeletes += o.ContractMetaDeletes
	s.StoragePuts += o.StoragePuts
	s.StorageDeletes += o.StorageDeletes
	s.StorageNoops += o.StorageNoops
	s.KVPutItems += o.KVPutItems
	s.KVDeleteItems += o.KVDeleteItems
	s.KVNoopItems += o.KVNoopItems
	for i := range s.KVDomains {
		s.KVDomains[i].Puts += o.KVDomains[i].Puts
		s.KVDomains[i].Deletes += o.KVDomains[i].Deletes
		s.KVDomains[i].Noops += o.KVDomains[i].Noops
	}
}

// KVDomain returns the mutation counts for a registered account-KV domain.
func (s CommitMutationStats) KVDomain(domain kvdomains.KVDomain) KVDomainMutationStats {
	if idx, ok := kvDomainStatIndex(domain); ok {
		return s.KVDomains[idx]
	}
	return KVDomainMutationStats{}
}

// TopKindsString returns the most frequent broad mutation classes in a compact
// log-friendly form, e.g. "kvPut=42,storagePut=40,accountUpdate=27".
func (s CommitMutationStats) TopKindsString(limit int) string {
	type entry struct {
		name  string
		count int
	}
	entries := []entry{
		{"accountCreate", s.AccountCreates},
		{"accountUpdate", s.AccountUpdates},
		{"accountDelete", s.AccountDeletes},
		{"codeUpdate", s.CodeUpdates},
		{"codeDelete", s.CodeDeletes},
		{"contractMetaUpdate", s.ContractMetaUpdates},
		{"contractMetaDelete", s.ContractMetaDeletes},
		{"storagePut", s.StoragePuts},
		{"storageDelete", s.StorageDeletes},
		{"storageNoop", s.StorageNoops},
		{"kvPut", s.KVPutItems},
		{"kvDelete", s.KVDeleteItems},
		{"kvNoop", s.KVNoopItems},
	}
	filtered := entries[:0]
	for _, e := range entries {
		if e.count > 0 {
			filtered = append(filtered, e)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].count != filtered[j].count {
			return filtered[i].count > filtered[j].count
		}
		return filtered[i].name < filtered[j].name
	})
	if limit <= 0 || limit > len(filtered) {
		limit = len(filtered)
	}
	parts := make([]string, 0, limit)
	for _, e := range filtered[:limit] {
		parts = append(parts, fmt.Sprintf("%s=%d", e.name, e.count))
	}
	return strings.Join(parts, ",")
}

// TopKVDomainsString returns the most frequent account-KV domains in a compact
// log-friendly form, e.g. "ContractStorage:p40/d1/n0,SystemReward:p2/d0/n1".
func (s CommitMutationStats) TopKVDomainsString(limit int) string {
	type entry struct {
		name  string
		stats KVDomainMutationStats
	}
	entries := make([]entry, 0, len(kvDomainStatOrder))
	for i, domain := range kvDomainStatOrder {
		stats := s.KVDomains[i]
		if stats.total() == 0 {
			continue
		}
		entries = append(entries, entry{name: kvdomains.Name(domain), stats: stats})
	}
	sort.Slice(entries, func(i, j int) bool {
		ti, tj := entries[i].stats.total(), entries[j].stats.total()
		if ti != tj {
			return ti > tj
		}
		return entries[i].name < entries[j].name
	})
	if limit <= 0 || limit > len(entries) {
		limit = len(entries)
	}
	parts := make([]string, 0, limit)
	for _, e := range entries[:limit] {
		parts = append(parts, fmt.Sprintf("%s:p%d/d%d/n%d", e.name, e.stats.Puts, e.stats.Deletes, e.stats.Noops))
	}
	return strings.Join(parts, ",")
}

func (s *CommitMutationStats) addKV(domain kvdomains.KVDomain, deleted bool) {
	if deleted {
		s.KVDeleteItems++
	} else {
		s.KVPutItems++
	}
	idx, ok := kvDomainStatIndex(domain)
	if !ok {
		return
	}
	if deleted {
		s.KVDomains[idx].Deletes++
	} else {
		s.KVDomains[idx].Puts++
	}
}

func kvDomainStatIndex(domain kvdomains.KVDomain) (int, bool) {
	switch domain {
	case kvdomains.SystemDynamicProperty:
		return 0, true
	case kvdomains.SystemWitnessSchedule:
		return 1, true
	case kvdomains.SystemProposal:
		return 2, true
	case kvdomains.SystemForkVote:
		return 3, true
	case kvdomains.SystemAsset:
		return 4, true
	case kvdomains.SystemExchange:
		return 5, true
	case kvdomains.SystemDelegation:
		return 6, true
	case kvdomains.SystemAccountIndex:
		return 7, true
	case kvdomains.SystemMarket:
		return 8, true
	case kvdomains.SystemReward:
		return 9, true
	case kvdomains.SystemShielded:
		return 10, true
	case kvdomains.SystemForkAux:
		return 11, true
	case kvdomains.SystemPBFT:
		return 12, true
	case kvdomains.SystemTapos:
		return 13, true
	case kvdomains.SystemTrace:
		return 14, true
	case kvdomains.SystemBloom:
		return 15, true
	case kvdomains.SystemCheckpoint:
		return 16, true
	case kvdomains.ContractStorage:
		return 17, true
	case kvdomains.ContractMetadata:
		return 18, true
	case kvdomains.ContractABI:
		return 19, true
	case kvdomains.ContractRuntimeState:
		return 20, true
	case kvdomains.AccountLocalIndex:
		return 21, true
	case kvdomains.AccountPermissionAux:
		return 22, true
	case kvdomains.WitnessCapsule:
		return 23, true
	case kvdomains.WitnessVoteState:
		return 24, true
	default:
		return 0, false
	}
}

func summarizeCommitMutations(plans []*accountCommitPlan) CommitMutationStats {
	var stats CommitMutationStats
	for _, plan := range plans {
		if plan == nil || plan.obj == nil {
			continue
		}
		obj := plan.obj
		if plan.deleteAccount {
			stats.AccountDeletes++
			continue
		}
		if obj.created {
			stats.AccountCreates++
		} else if obj.accountDirty {
			stats.AccountUpdates++
		}
		if obj.codeDirty {
			if len(obj.code) == 0 || obj.codeHash == (tcommon.Hash{}) {
				stats.CodeDeletes++
			} else {
				stats.CodeUpdates++
			}
		}
		if obj.contractMetaDirty {
			if obj.contractMeta == nil {
				stats.ContractMetaDeletes++
			} else {
				stats.ContractMetaUpdates++
			}
		}
		for _, op := range plan.storageOps {
			if !op.staged {
				stats.StorageNoops++
				continue
			}
			if op.delete {
				stats.StorageDeletes++
			} else {
				stats.StoragePuts++
			}
		}
		if plan.kvPlan == nil {
			continue
		}
		stats.KVNoopItems += plan.kvPlan.noopItems
		for i, n := range plan.kvPlan.noopByDomain {
			stats.KVDomains[i].Noops += n
		}
		for _, item := range plan.kvPlan.items {
			stats.addKV(item.domain, item.entry.deleted)
			// Push to the per-key hotspot tracker for /debug/state-hotspots.
			// The tracker is a thin atomic-gated path when enabled; it bails
			// out immediately when disabled, so the hot commit loop pays no
			// cost in the disabled state.
			recordHotspot(item.domain, item.logicalKey, item.entry.deleted, len(item.entry.val))
		}
	}
	return stats
}

// recordHotspot forwards one final-plan KV mutation to the process-wide
// KVHotspotTracker. Pulled out to keep summarizeCommitMutations readable;
// the function is inlined by the compiler.
func recordHotspot(domain kvdomains.KVDomain, key []byte, deleted bool, valueLen int) {
	defaultKVHotspotTracker.Record(domain, key, deleted, valueLen)
}
