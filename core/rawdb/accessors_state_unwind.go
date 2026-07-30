package rawdb

import (
	"fmt"
)

// StateUnwindStore is the database surface required by CollectStateUnwind:
// it must support latest-domain row reads/writes (to restore pre-images) and
// changeset iteration (to discover which rows were touched). StateKVLatestStore
// (= stateKVLatestStore) already embeds ethdb.KeyValueReader/Writer/Iteratee,
// so it satisfies both requirements; CollectStateUnwind accepts that interface
// directly.
type StateUnwindStore = StateKVLatestStore

// CollectStateUnwind rolls back the three latest-domain tables (account-latest,
// KV-latest, KV-generation) from the current tip down to the END of toBlock, by
// restoring each touched row to the pre-image (StateDomainChange.Prev) of the
// EARLIEST change to that row in (toBlock, fromBlock], and returns the matching
// commitment inverse-delta. It does NOT delete changeset rows and does NOT touch
// the commitment branch keyspace — the caller feeds the returned updates to the
// commitment store. fromBlock must be >= toBlock (equal => no-op, returns nil).
//
// Completeness is NOT checked here: if the range's changesets are partially
// pruned the returned delta is incomplete, which the caller detects when the
// re-folded root fails to match the target block's persisted anchor.
//
// Memory aliasing: IterateStateDomainChanges already calls cloneStateDomainChange
// on every yielded row, so each *StateDomainChange callback argument is a freshly
// allocated struct with owned Key/Prev/Next slices. CollectStateUnwind stores the
// pointer directly without additional copying.
func CollectStateUnwind(db StateUnwindStore, fromBlock, toBlock uint64) ([]StateCommitmentUpdate, error) {
	if fromBlock < toBlock {
		return nil, fmt.Errorf("rawdb: unwind fromBlock %d < toBlock %d", fromBlock, toBlock)
	}
	if fromBlock == toBlock {
		return nil, nil
	}

	// For each commitment key, record the chronologically earliest change in
	// (toBlock, fromBlock] (ascending blockNum, then ascending Seq within a block).
	// Its Prev field is the value the row held at the end of toBlock.
	seen := make(map[string]*StateDomainChange)
	order := make([]string, 0)

	for blockNum := toBlock + 1; blockNum <= fromBlock; blockNum++ {
		if err := IterateStateDomainChanges(db, blockNum, func(c *StateDomainChange) (bool, error) {
			key, err := stateDomainChangeLatestKey(c)
			if err != nil {
				return false, err
			}
			sk := string(key)
			if _, ok := seen[sk]; ok {
				// Earliest already recorded; ascending block+seq means we saw it first.
				return true, nil
			}
			seen[sk] = c
			order = append(order, sk)
			return true, nil
		}); err != nil {
			return nil, err
		}
	}

	updates := make([]StateCommitmentUpdate, 0, len(order))
	for _, sk := range order {
		c := seen[sk]
		key, err := stateDomainChangeLatestKey(c)
		if err != nil {
			return nil, err
		}
		if c.PrevExists {
			if err := writeStateDomainLatestRow(db, c); err != nil {
				return nil, err
			}
			val, err := stateDomainChangeCommitmentValue(c, c.Prev)
			if err != nil {
				return nil, err
			}
			updates = append(updates, NewStateCommitmentPut(key, val))
		} else {
			if err := deleteStateDomainLatestRow(db, c); err != nil {
				return nil, err
			}
			updates = append(updates, NewStateCommitmentDelete(key))
		}
	}
	return CoalesceStateCommitmentUpdates(updates), nil
}
