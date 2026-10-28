package rawdb

import (
	"context"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// Posting seeks return candidates, never values. Merge equal candidate blocks
// before decoding canonical repaired rows, so a batch of properties does not
// decompress and scan the same block separately for every key. False positives
// continue from the following block; keys with no posting remain eligible for
// the caller's independent unindexed-tail scan.
func readFirstIndexedStateKVBatch(ctx context.Context, db StateKVHistoryReader, from, to, targetTx, headTx uint64, owner common.Address, generation uint64, domain kvdomains.KVDomain, wanted map[string][]byte, first map[string]*StateDomainChange) error {
	pending := make(map[string]uint64, len(wanted))
	for key := range wanted {
		pending[key] = from
	}
	for len(pending) > 0 {
		groups := make(map[uint64][]string)
		for key, start := range pending {
			if err := ctx.Err(); err != nil {
				return err
			}
			var block uint64
			ok := false
			latestKey := StateKVLatestCommitmentKey(owner, generation, domain, wanted[key])
			err := iterateStateChangePostingCandidatesContext(ctx, db, latestKey, start, to, func(candidate uint64) (bool, error) {
				block, ok = candidate, true
				return false, nil
			})
			if err != nil {
				return err
			}
			if !ok {
				delete(pending, key)
				continue
			}
			groups[block] = append(groups[block], key)
		}
		blocks := make([]uint64, 0, len(groups))
		for block := range groups {
			blocks = append(blocks, block)
		}
		sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
		for _, block := range blocks {
			keys := make(map[string]bool, len(groups[block]))
			for _, key := range groups[block] {
				keys[key] = true
			}
			if err := IterateStateDomainChangesContext(ctx, db, block, func(row *StateDomainChange) (bool, error) {
				if !stateDomainChangeInTxWindow(row, targetTx, headTx) || row.FlatDomain != StateFlatDomainKVLatest || row.Owner != owner || row.Generation != generation || row.Domain != domain {
					return true, nil
				}
				key := string(row.Key)
				if !keys[key] {
					return true, nil
				}
				first[key] = cloneStateDomainChange(row)
				delete(keys, key)
				delete(wanted, key)
				delete(pending, key)
				return len(keys) > 0, nil
			}); err != nil {
				return err
			}
			for key := range keys {
				if block == ^uint64(0) {
					delete(pending, key)
				} else {
					pending[key] = block + 1
				}
			}
		}
	}
	return ctx.Err()
}
