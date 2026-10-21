package rawdb

import (
	"bytes"
	"context"
	"sort"

	"github.com/tronprotocol/go-tron/common"
)

// PlanStateHistoryQueryTailOwners is a no-I/O hint for one pinned request.
// The first necessary complete tail scan can retain all of its known owners,
// instead of decoding the same shared packs separately for each account.
// Neither planning nor an incomplete scan proves an owner/key absent.
func PlanStateHistoryQueryTailOwners(db StateKVHistoryReader, owners []common.Address) {
	v, ok := db.(*stateHistoryQueryTailView)
	if !ok {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.planned == nil {
		v.planned = make(map[common.Address]bool)
	}
	for _, owner := range owners {
		if v.planned[owner] {
			continue
		}
		known := make(map[common.Address]bool, stateHistoryQueryTailOwners)
		for candidate := range v.entries {
			known[candidate] = true
		}
		for candidate := range v.rejected {
			known[candidate] = true
		}
		for candidate := range v.planned {
			known[candidate] = true
		}
		if known[owner] || len(known) < stateHistoryQueryTailOwners {
			v.planned[owner] = true
		}
	}
}

type stateHistoryPlannedOwner struct {
	next     *stateHistoryQueryTailEntry
	old      *stateHistoryQueryTailEntry
	overflow bool
}

// Caller holds v.mu. All publications below require a complete successful
// repaired-row scan; early point hits retain the ordinary early-return behavior.
func (v *stateHistoryQueryTailView) iteratePlannedOwnersLocked(ctx context.Context, from, to uint64, owner common.Address, fn func(*StateDomainChange) (bool, error)) error {
	owners := []common.Address{owner}
	for candidate := range v.planned {
		entry := v.entries[candidate]
		if candidate != owner && !v.rejected[candidate] && (entry == nil || entry.from > from || entry.to < to) {
			owners = append(owners, candidate)
		}
	}
	// Prioritize the requested owner; optional owners have deterministic budgets.
	sort.Slice(owners[1:], func(i, j int) bool { return bytes.Compare(owners[i+1][:], owners[j+1][:]) < 0 })
	available := stateHistoryQueryTailOwners - len(v.entries) - len(v.rejected)
	kept := owners[:0]
	for _, candidate := range owners {
		if v.entries[candidate] == nil {
			if available == 0 {
				continue
			}
			available--
		}
		kept = append(kept, candidate)
	}
	owners = kept
	collect := make(map[common.Address]*stateHistoryPlannedOwner, len(owners))
	baseBytes, baseRows := v.used, v.rows
	for _, candidate := range owners {
		old := v.entries[candidate]
		if old != nil {
			baseBytes -= old.bytes
			baseRows -= len(old.rows)
		}
		collect[candidate] = &stateHistoryPlannedOwner{old: old, next: &stateHistoryQueryTailEntry{from: from, to: to, bytes: 128}}
	}
	newBytes, newRows := 0, 0
	for _, candidate := range owners {
		c := collect[candidate]
		if baseBytes+newBytes+c.next.bytes > v.budget {
			c.overflow = true
			continue
		}
		newBytes += c.next.bytes
	}
	stopped := false
	err := iterateStateHistorySelectedOwnersTail(ctx, v.StateKVHistoryReader, from, to, func(candidate common.Address) bool {
		return collect[candidate] != nil
	}, func(row *StateDomainChange) (bool, error) {
		c := collect[row.Owner]
		if !c.overflow {
			size := 256 + len(row.Key) + len(row.Prev) + len(row.Next)
			if baseBytes+newBytes+size > v.budget || baseRows+newRows+1 > v.maxRows {
				c.overflow = true
				newBytes -= c.next.bytes
				newRows -= len(c.next.rows)
				c.next.rows = nil
			} else {
				c.next.rows = append(c.next.rows, cloneStateDomainChange(row))
				c.next.bytes += size
				newBytes += size
				newRows++
			}
		}
		if row.Owner != owner {
			return true, nil
		}
		cont, err := fn(row)
		if !cont {
			stopped = true
		}
		return cont, err
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if stopped {
		return nil
	}
	for _, candidate := range owners {
		c := collect[candidate]
		if c.old != nil {
			v.used -= c.old.bytes
			v.rows -= len(c.old.rows)
		}
		if c.overflow {
			delete(v.entries, candidate)
			v.rejected[candidate] = true
		} else {
			v.entries[candidate] = c.next
			v.used += c.next.bytes
			v.rows += len(c.next.rows)
		}
	}
	return nil
}
