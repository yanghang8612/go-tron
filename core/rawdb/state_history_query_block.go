package rawdb

import "context"

const historyQueryBlockBytes = 16 << 20
const historyQueryBlockRows = 32768
const historyQueryBlockEntries = 64

type historyQueryBlock struct {
	rows []*StateDomainChange
	err  error
}

// A pinned request may validate a posting and then fetch the same block again,
// or request several keys in it. Retain the canonical repaired block once.
// Callbacks run outside the mutex and receive owned rows. A terminal decoder
// error is replayed at its original position, after earlier callbacks, so an
// ordinary early stop still hides only errors it would never have encountered.
func (v *stateHistoryQueryTailView) iterateBlock(ctx context.Context, block uint64, fn func(*StateDomainChange) (bool, error)) error {
	v.mu.Lock()
	entry := v.blocks[block]
	if entry == nil {
		if v.blockRejected[block] || len(v.blocks)+len(v.blockRejected) >= historyQueryBlockEntries {
			v.mu.Unlock()
			return IterateStateDomainChangesContext(ctx, v.StateKVHistoryReader, block, fn)
		}
		next := &historyQueryBlock{}
		size := 128
		overflow := false
		next.err = IterateStateDomainChangesContext(ctx, v.StateKVHistoryReader, block, func(row *StateDomainChange) (bool, error) {
			if !overflow {
				charge := 256 + len(row.Key) + len(row.Prev) + len(row.Next)
				if v.blockBytes+size+charge > historyQueryBlockBytes || v.blockRows+len(next.rows)+1 > historyQueryBlockRows {
					overflow = true
					next.rows = nil
				} else {
					// The ordinary owning decoder gives every callback its own row. No
					// borrowed iterator/chunk buffer is retained by this cache.
					next.rows = append(next.rows, row)
					size += charge
				}
			}
			return true, nil
		})
		if err := ctx.Err(); err != nil {
			v.mu.Unlock()
			return err
		}
		if overflow {
			if v.blockRejected == nil {
				v.blockRejected = make(map[uint64]bool)
			}
			v.blockRejected[block] = true
			v.mu.Unlock()
			return IterateStateDomainChangesContext(ctx, v.StateKVHistoryReader, block, fn)
		}
		if v.blocks == nil {
			v.blocks = make(map[uint64]*historyQueryBlock)
		}
		v.blocks[block] = next
		v.blockBytes += size
		v.blockRows += len(next.rows)
		entry = next
	}
	v.mu.Unlock()
	for _, row := range entry.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		cont, err := fn(cloneStateDomainChange(row))
		if err != nil || !cont {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return entry.err
}
