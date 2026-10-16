// Copyright 2026 The go-tron Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package pebble

import (
	"context"
	"errors"

	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

const getCursorTableLimit = 4

// GetCursorStats counts cursor-local table iterator construction and reuse.
// TableOpens counts successful newIters calls, not physical file opens or IO.
type GetCursorStats struct {
	TableRequests, TableOpens, TableReuses, TableFallbacks, TableEvictions uint64
	MaxRetained                                                            uint64
}

// GetCursor performs snapshot-scoped, lazy point lookups. It is owned by one
// reader and is not safe for concurrent use. Get returns read-only borrowed
// bytes, valid until ReleaseValue, the next Get, or Close. It cannot hold values
// from multiple calls simultaneously. Independent cursors may run concurrently.
//
// Close every cursor before closing its Snapshot or DB. Like an Iterator, a
// cursor pins a readState (including memtables and obsolete SSTs) for its short
// lifetime. At most four point/range-deletion iterator pairs are retained; this
// bounds table handles, not their variable-size blocks or zombie bytes.
// An operation error poisons subsequent Get calls. The failing operation
// cleans up all table resources and joins their errors with the operation error.
// Upstream Iterator/individual Close methods retain their first-error precedence;
// errors suppressed within those methods cannot be recovered here. ReleaseValue
// and Close do not re-report an error already delivered by an earlier operation;
// Close reports fresh cleanup failures and releases every resource.
type GetCursor struct {
	db           *DB
	seqNum       uint64
	readState    *readState
	tableIters   tableNewIters
	iter         *Iterator
	active       bool
	closed       bool
	err          error
	clock        uint64
	tables       [getCursorTableLimit]getCursorTable
	stats        GetCursorStats
	useL6Filters bool
}

type getCursorTable struct {
	file     *manifest.FileMetadata
	level    manifest.Level
	lastUsed uint64
	// A lease lasts through the entire Get and its borrowed value. In particular,
	// getIter may retain a tombstone Span after logically closing its iterator.
	leased     bool
	point      getCursorPointIter
	rangeDel   getCursorRangeDelIter
	filtersBuf [1]BlockPropertyFilter
}

type getCursorPointIter struct{ internalIterator }

func (i *getCursorPointIter) Close() error { return i.Error() }
func (i *getCursorPointIter) MaybeFilteredKeys() bool {
	if f, ok := i.internalIterator.(filteredIter); ok {
		return f.MaybeFilteredKeys()
	}
	return false
}

type getCursorRangeDelIter struct{ keyspan.FragmentIterator }

func (i *getCursorRangeDelIter) Close() error { return i.Error() }

// GetCursorOptions fixes optional read policy for one snapshot-scoped cursor.
// Zero values preserve the default Get behavior.
type GetCursorOptions struct {
	// UseL6Filters checks existing L6 point-key Bloom filters. Missing filters
	// fall back to ordinary lookup. Range deletions remain independently read.
	UseL6Filters bool
}

// NewGetCursor opens a cursor with the default Get read policy, without SST IO.
// Ordinary DB.Get and Batch.Get remain unchanged.
func (s *Snapshot) NewGetCursor() (*GetCursor, error) {
	return s.NewGetCursorWithOptions(GetCursorOptions{})
}

// NewGetCursorWithOptions opens a cursor with immutable read options. It does
// not read an SST until the first Get.
func (s *Snapshot) NewGetCursorWithOptions(opts GetCursorOptions) (*GetCursor, error) {
	if s.db == nil {
		return nil, ErrClosed
	}
	if err := s.db.closed.Load(); err != nil {
		return nil, ErrClosed
	}
	c := &GetCursor{db: s.db, seqNum: s.seqNum, readState: s.db.loadReadState(), useL6Filters: opts.UseL6Filters}
	c.tableIters = c.newIters
	return c, nil
}

// Get returns the value or ErrNotFound. It releases the previous value first,
// propagating any release error before beginning another lookup. Missing keys
// are not errors that poison the cursor. Call ReleaseValue after consuming a
// successful value to observe merge/value-close errors promptly.
func (c *GetCursor) Get(key []byte) ([]byte, error) {
	if c == nil || c.closed {
		return nil, ErrClosed
	}
	if err := c.ReleaseValue(); err != nil {
		return nil, err
	}
	if c.err != nil {
		return nil, c.err
	}
	buf := getIterAllocPool.Get().(*getIterAlloc)
	get := &buf.get
	*get = getIter{
		logger: c.db.opts.Logger, comparer: c.db.opts.Comparer, newIters: c.tableIters,
		snapshot: c.seqNum, key: key, mem: c.readState.memtables,
		l0: c.readState.current.L0SublevelFiles, version: c.readState.current, useL6Filters: c.useL6Filters,
	}
	// Mutable memtables may receive writes after cursor construction. Preserve
	// Get's sequence filtering rather than treating their contents as immutable.
	for len(get.mem) > 0 {
		n := len(get.mem)
		if get.mem[n-1].logSeqNum < c.seqNum {
			break
		}
		get.mem = get.mem[:n-1]
	}
	c.readState.ref()
	c.iter = &buf.dbi
	*c.iter = Iterator{ctx: context.Background(), getIterAlloc: buf, iter: get, pointIter: get,
		merge: c.db.merge, comparer: *c.db.opts.Comparer, readState: c.readState, keyBuf: buf.keyBuf}
	c.active = true
	if !c.iter.First() {
		if err := c.ReleaseValue(); err != nil {
			return nil, err
		}
		return nil, ErrNotFound
	}
	value, err := c.iter.ValueAndErr()
	if err != nil {
		return nil, c.ReleaseValue()
	}
	return value, nil
}

// ReleaseValue invalidates the borrowed value and closes its merge/value closer.
// It preserves only the bounded SST iterator pairs for subsequent lookups.
// It is idempotent and must also be called when a consumer callback fails/panics.
func (c *GetCursor) ReleaseValue() error {
	if c == nil || !c.active {
		return nil
	}
	err := c.iter.Close()
	c.active = false
	// Iterator.Close returns its allocation to the upstream pool and clears
	// all borrowed state except the existing bounded key buffer. Do not access
	// the allocation after Close: another independent cursor may already own it.
	c.iter = nil
	for i := range c.tables {
		c.tables[i].leased = false
	}
	if err != nil {
		// Do all cleanup in the failing operation, so a speculative read error
		// cannot reappear later as an unrelated session-close error.
		err = errors.Join(err, c.closeTables())
		c.err = err
	}
	return err
}

// Stats returns cumulative local counters and does not reset them.
func (c *GetCursor) Stats() GetCursorStats { return c.stats }

// Close invalidates values, closes all retained iterators, then releases the
// readState. It attempts every close even when an earlier close fails.
func (c *GetCursor) Close() error {
	if c == nil || c.closed {
		return nil
	}
	err := c.ReleaseValue()
	err = errors.Join(err, c.closeTables())
	if c.readState != nil {
		c.readState.unref()
		c.readState = nil
	}
	c.db = nil
	c.tableIters = nil
	c.iter = nil
	c.closed = true
	return err
}

func (c *GetCursor) closeTable(t *getCursorTable) error {
	var err error
	if t.point.internalIterator != nil {
		err = t.point.internalIterator.Close()
	}
	if t.rangeDel.FragmentIterator != nil {
		err = errors.Join(err, t.rangeDel.FragmentIterator.Close())
	}
	*t = getCursorTable{}
	return err
}
func (c *GetCursor) closeTables() error {
	var err error
	for i := range c.tables {
		err = errors.Join(err, c.closeTable(&c.tables[i]))
	}
	return err
}

func (c *GetCursor) newIters(ctx context.Context, file *manifest.FileMetadata,
	opts *IterOptions, internalOpts internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
	c.stats.TableRequests++
	// The private Get driver uses only these default, unbounded table options.
	// Anything else must retain upstream construction/ownership, not reuse a
	// cached iterator configured for different bounds, filters or statistics.
	compatible := opts != nil && opts.snapshotForHideObsoletePoints == c.seqNum &&
		opts.LowerBound == nil && opts.UpperBound == nil && opts.TableFilter == nil && opts.SkipPoint == nil &&
		len(opts.PointKeyFilters) == 0 && len(opts.RangeKeyFilters) == 0 && opts.UseL6Filters == (c.useL6Filters && manifest.LevelToInt(opts.level) == numLevels-1) &&
		opts.KeyTypes == IterKeyTypePointsOnly && !opts.OnlyReadGuaranteedDurable && !opts.disableLazyCombinedIteration &&
		internalOpts.bytesIterated == nil && internalOpts.bufferPool == nil && internalOpts.stats == nil && internalOpts.boundLimitedFilter == nil
	if compatible {
		for i := range c.tables {
			t := &c.tables[i]
			if t.file == file && t.level == opts.level && !t.leased {
				c.clock++
				t.lastUsed = c.clock
				t.leased = true
				c.stats.TableReuses++
				var rd keyspan.FragmentIterator
				if t.rangeDel.FragmentIterator != nil {
					rd = &t.rangeDel
				}
				return &t.point, rd, nil
			}
		}
	}
	var slot *getCursorTable
	if compatible {
		for i := range c.tables {
			t := &c.tables[i]
			if t.leased {
				continue
			}
			if t.file == nil {
				slot = t
				break
			}
			if slot == nil || t.lastUsed < slot.lastUsed {
				slot = t
			}
		}
	}
	if slot == nil {
		c.stats.TableFallbacks++
		point, rd, err := c.db.newIters(ctx, file, opts, internalOpts)
		if err == nil {
			c.stats.TableOpens++
		}
		return point, rd, err
	}
	if slot.file != nil {
		c.stats.TableEvictions++
		if err := c.closeTable(slot); err != nil {
			return nil, nil, err
		}
	}
	// levelIter normally lends its filtersBuf to the SST filterer. That
	// buffer is overwritten on the next Get, so retained SSTs need slot-owned
	// backing even for the implicitly appended obsolete-key filter.
	stableOpts := *opts
	stableOpts.PointKeyFilters = slot.filtersBuf[:0:1]
	point, rd, err := c.db.newIters(ctx, file, &stableOpts, internalOpts)
	if err != nil {
		return nil, nil, err
	}
	c.stats.TableOpens++
	c.clock++
	*slot = getCursorTable{file: file, level: opts.level, lastUsed: c.clock, leased: true,
		point: getCursorPointIter{point}, rangeDel: getCursorRangeDelIter{rd}, filtersBuf: slot.filtersBuf}
	var retained uint64
	for i := range c.tables {
		if c.tables[i].file != nil {
			retained++
		}
	}
	if retained > c.stats.MaxRetained {
		c.stats.MaxRetained = retained
	}
	var rangeDel keyspan.FragmentIterator
	if rd != nil {
		rangeDel = &slot.rangeDel
	}
	return &slot.point, rangeDel, nil
}
