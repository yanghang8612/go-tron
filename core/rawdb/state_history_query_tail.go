package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
)

const (
	stateHistoryQueryTailBytes  = 8 << 20
	stateHistoryQueryTailRows   = 16384
	stateHistoryQueryTailOwners = 16
)

// WithStateHistoryQueryTailCache reuses only this query's pinned hot view. It
// retains rows for requested owners, never the entire tail or shared mutable
// process state. A moving/legacy reader is left unchanged. Closing the source
// remains the caller's responsibility; the cache owns no snapshots or files.
func WithStateHistoryQueryTailCache(source StateKVHistoryReader) StateKVHistoryReader {
	marker, ok := source.(pointread.PinnedKeyValueView)
	if !ok || !marker.IsPinnedKeyValueView() {
		return source
	}
	if _, ok := source.(*stateHistoryQueryTailView); ok {
		return source
	}
	return &stateHistoryQueryTailView{StateKVHistoryReader: source, entries: make(map[common.Address]*stateHistoryQueryTailEntry), rejected: make(map[common.Address]bool), first: make(map[common.Address]*stateHistoryQueryTailFirst), budget: stateHistoryQueryTailBytes, maxRows: stateHistoryQueryTailRows}
}

type stateHistoryQueryTailEntry struct {
	from, to uint64
	rows     []*StateDomainChange
	bytes    int
}

type stateHistoryQueryTailView struct {
	StateKVHistoryReader
	mu                          sync.Mutex
	entries                     map[common.Address]*stateHistoryQueryTailEntry
	rejected                    map[common.Address]bool
	first                       map[common.Address]*stateHistoryQueryTailFirst
	planned                     map[common.Address]bool
	used, rows, budget, maxRows int
	packFilters                 map[uint64]*historyTailPackFilters
	packFilterBytes             int
}

func (*stateHistoryQueryTailView) IsPinnedKeyValueView() bool { return true }

// AcquireStateHistoryReadView preserves the wrapper on nested reads. A cache
// entry is published only after the entire owner/range scan succeeds. Empty
// entries therefore prove absence only in this pinned sequence and range.
func (v *stateHistoryQueryTailView) iterateOwner(ctx context.Context, from, to uint64, owner common.Address, fn func(*StateDomainChange) (bool, error)) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.iterateOwnerLocked(ctx, from, to, owner, fn)
}

func (v *stateHistoryQueryTailView) iterateOwnerLocked(ctx context.Context, from, to uint64, owner common.Address, fn func(*StateDomainChange) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if from > to {
		return nil
	}
	if v.rejected[owner] {
		return iterateStateHistoryOwnerTail(ctx, v.StateKVHistoryReader, from, to, owner, fn)
	}
	entry := v.entries[owner]
	if entry == nil || entry.from > from || entry.to < to {
		if len(v.entries)+len(v.rejected) >= stateHistoryQueryTailOwners && entry == nil {
			return iterateStateHistoryOwnerTail(ctx, v.StateKVHistoryReader, from, to, owner, fn)
		}
		if len(v.planned) > 1 && v.planned[owner] {
			return v.iteratePlannedOwnersLocked(ctx, from, to, owner, fn)
		}
		oldBytes, oldRows := 0, 0
		if entry != nil {
			oldBytes, oldRows = entry.bytes, len(entry.rows)
		}
		next := &stateHistoryQueryTailEntry{from: from, to: to, bytes: 128}
		stopped, overflow := false, false
		reject := func() {
			overflow = true
			next.rows = nil
			v.rejected[owner] = true
			if entry != nil {
				delete(v.entries, owner)
				v.used -= entry.bytes
				v.rows -= len(entry.rows)
			}
		}
		if v.used-oldBytes+next.bytes > v.budget {
			reject()
		}

		err := iterateStateHistoryOwnerTail(ctx, v.StateKVHistoryReader, from, to, owner, func(row *StateDomainChange) (bool, error) {
			if !overflow {
				size := 256 + len(row.Key) + len(row.Prev) + len(row.Next)
				if v.used-oldBytes+next.bytes+size > v.budget || v.rows-oldRows+len(next.rows)+1 > v.maxRows {
					// Continue this same scan without a cache. Never restart
					// an over-budget owner or retry it on subsequent reads.
					reject()
				} else {
					next.rows = append(next.rows, cloneStateDomainChange(row))
					next.bytes += size
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
		// A point/batch hit returns immediately, just like the uncached
		// iterator. Only a completed range proves coverage for later keys.
		if stopped || overflow {
			return nil
		}

		v.used += next.bytes - oldBytes
		v.rows += len(next.rows) - oldRows
		v.entries[owner] = next
		return nil
	}
	for _, row := range entry.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if row.BlockNum < from || row.BlockNum > to {
			continue
		}
		// Accessor callbacks may retain or modify their row. Do not expose
		// cache-owned slices to them.
		cont, err := fn(cloneStateDomainChange(row))
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func iterateStateHistoryTailByOwner(ctx context.Context, db StateKVHistoryReader, from, to uint64, owner common.Address, fn func(*StateDomainChange) (bool, error)) error {
	if view, ok := db.(*stateHistoryQueryTailView); ok {
		return view.iterateOwner(ctx, from, to, owner, fn)
	}
	return iterateStateHistoryOwnerTail(ctx, db, from, to, owner, fn)
}

// Decode packs with the existing validating borrowed decoder, retaining only
// the requested owner's rows. Positive sequence repairs are applied before
// callbacks, including repairs that change the owner and shadow a match.
// Unrelated values are validated but never copied into millions of objects.
func iterateStateHistoryOwnerTail(ctx context.Context, db StateKVHistoryReader, from, to uint64, owner common.Address, fn func(*StateDomainChange) (bool, error)) error {
	return iterateStateHistorySelectedOwnersTail(ctx, db, from, to, func(candidate common.Address) bool { return candidate == owner }, fn)
}

func iterateStateHistorySelectedOwnersTail(ctx context.Context, db StateKVHistoryReader, from, to uint64, selected func(common.Address) bool, fn func(*StateDomainChange) (bool, error)) error {
	return iterateStateHistorySelectedOwnersTailFiltered(ctx, db, from, to, selected, nil, fn)
}

func iterateStateHistorySelectedOwnersTailFiltered(ctx context.Context, db StateKVHistoryReader, from, to uint64, selected func(common.Address) bool, skipPack func(context.Context, StateKVHistoryReader, []byte, uint64) (bool, error), fn func(*StateDomainChange) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if from > to {
		return nil
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], from)
	it := db.NewIterator(stateChangeSetPrefix, start[:])
	defer it.Release()
	var block uint64
	haveBlock := false
	rows := make(map[uint64]*StateDomainChange)
	flush := func() (bool, error) {
		seqs := make([]uint64, 0, len(rows))
		for seq := range rows {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for _, seq := range seqs {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			cont, err := fn(rows[seq])
			if err != nil || !cont {
				return cont, err
			}
		}
		clear(rows)
		return true, nil
	}
	var scratch StateDomainChange
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !it.Next() {
			break
		}
		key := it.Key()
		if !bytes.HasPrefix(key, stateChangeSetPrefix) || len(key) != len(stateChangeSetPrefix)+16 {
			continue
		}
		num := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix):])
		if num < from {
			continue
		}
		if num > to {
			break
		}
		if haveBlock && num != block {
			cont, err := flush()
			if err != nil || !cont {
				return err
			}
		}
		block = num
		haveBlock = true
		seq := binary.BigEndian.Uint64(key[len(stateChangeSetPrefix)+8:])
		if seq == 0 {
			value := it.Value()
			if skipPack != nil {
				skip, err := skipPack(ctx, db, value, num)
				if err != nil {
					return err
				}
				if skip {
					continue
				} // Still consume every positive-seq repair.
			}
			used, err := iterateHistoryOwnerSpans(ctx, db, value, num, selected, &scratch, func(row *StateDomainChange) (bool, error) {
				rows[row.Seq] = cloneStateDomainChange(row)
				return true, nil
			})
			if err != nil {
				return err
			}
			if used {
				continue
			}
			_, err = iteratePersistedStateDomainChangeBlockBorrowedWithScratch(value, num, &scratch, func(row *StateDomainChange) (bool, error) {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				if selected(row.Owner) {
					rows[row.Seq] = cloneStateDomainChange(row)
				}
				return true, nil
			}, db)
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if isStateHistorySharedPack(value) {
				return err
			}
			clear(rows)
			row, legacyErr := decodePersistedStateDomainChange(value, num, 0)
			if legacyErr != nil {
				return err
			}
			if selected(row.Owner) {
				rows[0] = row
			}
			continue
		}
		row, err := decodePersistedStateDomainChange(it.Value(), num, seq)
		if err != nil {
			return err
		}
		if selected(row.Owner) {
			rows[seq] = row
		} else {
			delete(rows, seq)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := it.Error(); err != nil {
		return err
	}
	_, err := flush()
	return err
}
