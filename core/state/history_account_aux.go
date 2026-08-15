package state

import (
	"sort"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// StateKVColdAsOf exposes an exact historical prefix view when a cold backend
// can materialize it directly.
type StateKVColdAsOf interface {
	IterateKVAsOfPrefix(domain kvdomains.KVDomain, owner tcommon.Address, generation uint64, logicalPrefix []byte, txNum uint64, fn func(logicalKey, value []byte) (bool, error)) error
}

// StateKVColdBoundary enumerates keys from an immutable latest snapshot. The
// values seed historical reconstruction; changesets still select the as-of values.
type StateKVColdBoundary interface {
	IterateKVLatestPrefix(domain kvdomains.KVDomain, owner tcommon.Address, generation uint64, logicalPrefix []byte, txNum uint64, fn func(logicalKey, value []byte) (bool, error)) error
}

func (r *PersistentHistoryReader) materializeHistoricalAccountAux(pb *corepb.Account, owner tcommon.Address, generation, blockNum uint64) error {
	if pb == nil {
		return nil
	}
	clearAccountAuxProto(pb)
	targetTxNum, err := r.stateTxNumAtBlockEnd(blockNum)
	if err != nil {
		return err
	}
	headTxNum, err := r.stateTxNumAtBlockEnd(r.headNum)
	if err != nil {
		return err
	}
	candidates := make(map[kvdomains.KVDomain]map[string]struct{}, len(accountSplitDomains))
	for _, domain := range accountSplitDomains {
		candidates[domain] = make(map[string]struct{})
	}
	if r.coldHistory != nil && targetTxNum < headTxNum {
		if keyed, ok := r.coldHistory.(StateDomainChangeColdPrefixHistory); ok {
			// Production cold history is key-oriented. Enumerate only this
			// account/generation/domain prefix instead of decoding every state
			// mutation between the requested block and head.
			for _, domain := range accountSplitDomains {
				if err := keyed.IterateStateDomainChangesByPrefix(targetTxNum+1, headTxNum, owner, generation, domain, nil, func(change *rawdb.StateDomainChange) (bool, error) {
					if change != nil && change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Owner == owner && change.Generation == generation && change.Domain == domain {
						candidates[domain][string(change.Key)] = struct{}{}
					}
					return true, nil
				}); err != nil {
					return err
				}
			}
		} else {
			// Preserve the generic compatibility path for test or external cold
			// readers that have not implemented owner-prefix history seeks.
			if err := r.coldHistory.IterateStateDomainChanges(targetTxNum+1, headTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
				if change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Owner == owner && isAccountSplitDomain(change.Domain) {
					candidates[change.Domain][string(change.Key)] = struct{}{}
				}
				return true, nil
			}); err != nil {
				return err
			}
		}
	}
	if r.coldHistory != nil {
		currentGeneration, exists, err := r.hotLatest().KVGeneration(owner)
		if err != nil {
			return err
		}
		if exists {
			for _, domain := range accountSplitDomains {
				if err := rawdb.IterateStateKVLatest(r.db, owner, currentGeneration, domain, nil, func(key, _ []byte) (bool, error) {
					candidates[domain][string(key)] = struct{}{}
					return true, nil
				}); err != nil {
					return err
				}
			}
		}
	}
	if boundary, ok := r.coldHistory.(StateKVColdBoundary); ok {
		for _, domain := range accountSplitDomains {
			if err := boundary.IterateKVLatestPrefix(domain, owner, generation, nil, targetTxNum, func(key, _ []byte) (bool, error) {
				candidates[domain][string(key)] = struct{}{}
				return true, nil
			}); err != nil {
				return err
			}
		}
	}
	loadDomain := func(domain kvdomains.KVDomain, load func(key, value []byte) (bool, error)) error {
		if cold, ok := r.coldHistory.(StateKVColdAsOf); ok {
			return cold.IterateKVAsOfPrefix(domain, owner, generation, nil, targetTxNum, load)
		}
		if r.coldHistory != nil {
			for key := range candidates[domain] {
				value, exists, err := r.readStateAccountKVAsOf(owner, domain, []byte(key), blockNum, r.headNum)
				if err != nil {
					return err
				}
				if exists {
					if _, err := load([]byte(key), value); err != nil {
						return err
					}
				}
			}
			return nil
		}
		cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
		if !ok || cfg.IterateHotAccountKVPrefixAsOf == nil {
			return ErrStateDomainHistoryUnavailable
		}
		return cfg.IterateHotAccountKVPrefixAsOf(r.db, owner, domain, nil, targetTxNum, headTxNum, load)
	}

	for _, domain := range accountAuxDomains {
		values := accountAuxMap(pb, domain, true)
		load := func(key, value []byte) (bool, error) {
			decoded, err := decodeAccountAuxInt64(value)
			if err != nil {
				return false, err
			}
			values[string(key)] = decoded
			return true, nil
		}
		if err := loadDomain(domain, load); err != nil {
			return err
		}
	}

	clearAccountPermissionProto(pb)
	loadPermission := func(key, value []byte) (bool, error) {
		permission, kind, err := decodeAccountPermissionRow(key, value)
		if err != nil {
			return false, err
		}
		switch kind {
		case accountOwnerPermissionKey[0]:
			pb.OwnerPermission = permission
		case accountWitnessPermissionKey[0]:
			pb.WitnessPermission = permission
		case accountActivePermissionRoot[0]:
			pb.ActivePermission = append(pb.ActivePermission, permission)
		}
		return true, nil
	}
	if err := loadDomain(kvdomains.AccountPermissionAux, loadPermission); err != nil {
		return err
	}
	sort.Slice(pb.ActivePermission, func(i, j int) bool {
		return pb.ActivePermission[i].GetId() < pb.ActivePermission[j].GetId()
	})

	type indexedVote struct {
		index uint32
		vote  *corepb.Vote
	}
	votes := make([]indexedVote, 0)
	loadVote := func(key, value []byte) (bool, error) {
		index, vote, err := decodeAccountVoteRow(key, value)
		if err != nil {
			return false, err
		}
		votes = append(votes, indexedVote{index: index, vote: vote})
		return true, nil
	}
	if err := loadDomain(kvdomains.AccountVotesAux, loadVote); err != nil {
		return err
	}
	sort.Slice(votes, func(i, j int) bool { return votes[i].index < votes[j].index })
	clearAccountVotesProto(pb)
	for _, row := range votes {
		pb.Votes = append(pb.Votes, row.vote)
	}

	frozenBandwidth := make([]accountFrozenBandwidthRow, 0, 2)
	if err := loadDomain(kvdomains.AccountFrozenBandwidthAux, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenBandwidthRow(key, value)
		if err != nil {
			return false, err
		}
		frozenBandwidth = append(frozenBandwidth, row)
		return true, nil
	}); err != nil {
		return err
	}
	sort.Slice(frozenBandwidth, func(i, j int) bool { return frozenBandwidth[i].index < frozenBandwidth[j].index })
	clearAccountStakeV1Proto(pb)
	for _, row := range frozenBandwidth {
		pb.Frozen = append(pb.Frozen, row.entry)
	}
	if err := loadDomain(kvdomains.AccountTronPowerAux, func(key, value []byte) (bool, error) {
		tronPower, err := decodeAccountTronPower(key, value)
		if err != nil {
			return false, err
		}
		pb.TronPower = tronPower
		return true, nil
	}); err != nil {
		return err
	}

	frozen := make([]accountFrozenV2Row, 0, 3)
	if err := loadDomain(kvdomains.AccountFrozenV2Aux, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenV2Row(key, value)
		if err != nil {
			return false, err
		}
		frozen = append(frozen, row)
		return true, nil
	}); err != nil {
		return err
	}
	unfrozen := make([]accountUnfrozenV2Row, 0, 32)
	if err := loadDomain(kvdomains.AccountUnfrozenV2Aux, func(key, value []byte) (bool, error) {
		row, err := decodeAccountUnfrozenV2Row(key, value)
		if err != nil {
			return false, err
		}
		unfrozen = append(unfrozen, row)
		return true, nil
	}); err != nil {
		return err
	}
	sort.Slice(frozen, func(i, j int) bool { return frozen[i].ordinal < frozen[j].ordinal })
	sort.Slice(unfrozen, func(i, j int) bool { return unfrozen[i].seq < unfrozen[j].seq })
	clearAccountStakeV2Proto(pb)
	for _, row := range frozen {
		pb.FrozenV2 = append(pb.FrozenV2, &corepb.Account_FreezeV2{Type: row.resource, Amount: row.amount})
	}
	for _, row := range unfrozen {
		pb.UnfrozenV2 = append(pb.UnfrozenV2, row.entry)
	}

	frozenSupply := make([]accountFrozenSupplyRow, 0, 10)
	if err := loadDomain(kvdomains.AccountFrozenSupplyAux, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenSupplyRow(key, value)
		if err != nil {
			return false, err
		}
		frozenSupply = append(frozenSupply, row)
		return true, nil
	}); err != nil {
		return err
	}
	sort.Slice(frozenSupply, func(i, j int) bool { return frozenSupply[i].index < frozenSupply[j].index })
	clearAccountFrozenSupplyProto(pb)
	for _, row := range frozenSupply {
		pb.FrozenSupply = append(pb.FrozenSupply, row.entry)
	}
	clearAccountResourceProto(pb)
	if err := loadDomain(kvdomains.AccountResourceAux, func(key, value []byte) (bool, error) {
		resource, err := decodeHistoricalAccountResource(key, value)
		if err != nil {
			return false, err
		}
		pb.AccountResource = resource
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}
