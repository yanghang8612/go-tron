package rawdb

import (
	"context"

	"github.com/tronprotocol/go-tron/common"
)

// Only first-change accessors may use this summary. Generic iterators must
// continue to expose every repaired row. All four query bounds are exact:
// an earlier first row outside a narrower Tx window cannot prove absence when
// legacy rows have nonmonotone TxNums.
type stateHistoryQueryTailFirst struct {
	from, to, targetTx, headTx uint64
	rows                       []*StateDomainChange
	bytes                      int
}

func iterateFirstStateHistoryTail(ctx context.Context, db StateKVHistoryReader, from, to, targetTx, headTx uint64, owner common.Address, fn func(*StateDomainChange) (bool, error)) error {
	v, ok := db.(*stateHistoryQueryTailView)
	if !ok {
		return iterateStateHistoryOwnerTail(ctx, db, from, to, owner, fn)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	old := v.first[owner]
	if old != nil && old.from == from && old.to == to && old.targetTx == targetTx && old.headTx == headTx {
		for _, row := range old.rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			cont, err := fn(cloneStateDomainChange(row))
			if err != nil || !cont {
				return err
			}
		}
		return nil
	}
	oldBytes, oldRows := 0, 0
	if old != nil {
		oldBytes, oldRows = old.bytes, len(old.rows)
	}
	next := &stateHistoryQueryTailFirst{from: from, to: to, targetTx: targetTx, headTx: headTx, bytes: 128}
	seen := make(map[string]struct{})
	var scratch []byte
	stopped, overflow := false, false
	abandon := func() { overflow = true; next.rows = nil; seen = nil; scratch = nil }
	if v.used-oldBytes+next.bytes > v.budget {
		abandon()
	}
	err := v.iterateOwnerLocked(ctx, from, to, owner, func(row *StateDomainChange) (bool, error) {
		if !overflow && stateDomainChangeInTxWindow(row, targetTx, headTx) {
			// Avoid allocating an oversized canonical key just to reject its cache row.
			if len(row.Key) > v.budget {
				abandon()
			} else {
				var keyErr error
				scratch, keyErr = appendStateDomainChangeLatestKey(scratch[:0], row)
				if keyErr != nil {
					abandon()
				} else if _, exists := seen[string(scratch)]; !exists {
					size := 320 + len(scratch) + len(row.Key) + len(row.Prev) + len(row.Next)
					if v.used-oldBytes+next.bytes+size > v.budget || v.rows-oldRows+len(next.rows)+1 > v.maxRows {
						abandon()
					} else {
						seen[string(scratch)] = struct{}{}
						next.rows = append(next.rows, cloneStateDomainChange(row))
						next.bytes += size
					}
				}
			}
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
	if err = ctx.Err(); err != nil {
		return err
	}
	// An overflowing full-row owner is the only case that needs this additional
	// summary. Small owners already reuse the existing complete-row cache.
	if stopped || overflow || !v.rejected[owner] {
		return nil
	}
	if v.used-oldBytes+next.bytes > v.budget || v.rows-oldRows+len(next.rows) > v.maxRows {
		return nil
	}
	if v.first == nil {
		v.first = make(map[common.Address]*stateHistoryQueryTailFirst)
	}
	v.first[owner] = next
	v.used += next.bytes - oldBytes
	v.rows += len(next.rows) - oldRows
	return nil
}
