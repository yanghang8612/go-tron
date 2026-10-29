package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"golang.org/x/crypto/blake2b"
)

const historyQueryKeyGroups = 3
const historyQueryKeysMaxBytes = 16 << 10
const historyQueryKeysMaxItems = 1024

type historyQueryKeysBuilder struct {
	keys    [historyQueryKeyGroups]map[string]struct{}
	bytes   [historyQueryKeyGroups]int
	unknown [historyQueryKeyGroups]bool
}

func historyQueryKeyGroup(flat StateFlatDomain, owner common.Address, domain kvdomains.KVDomain) (int, bool) {
	if flat == StateFlatDomainAccountLatest || flat == StateFlatDomainKVGeneration {
		return 0, true
	}
	if flat != StateFlatDomainKVLatest || owner != common.SystemAccountAddress {
		return 0, false
	}
	if domain == kvdomains.SystemDynamicProperty {
		return 1, true
	}
	if domain == kvdomains.SystemForkVote {
		return 2, true
	}
	return 0, false
}

func (b *historyQueryKeysBuilder) add(row *StateDomainChange) {
	group, ok := historyQueryKeyGroup(row.FlatDomain, row.Owner, row.Domain)
	if !ok || b.unknown[group] {
		return
	}
	// Reject oversized rows before allocating a full commitment key.
	if len(row.Key) > historyQueryKeysMaxBytes {
		b.unknown[group] = true
		b.keys[group] = nil
		return
	}
	key, ok := stateDomainChangeLatestKeyByKey(row.FlatDomain, row.Owner, row.Generation, row.Domain, row.Key)
	if !ok {
		return
	}
	if _, exists := b.keys[group][string(key)]; exists {
		return
	}
	if len(b.keys[group]) == historyQueryKeysMaxItems || len(key) > historyQueryKeysMaxBytes-b.bytes[group] {
		b.unknown[group] = true
		b.keys[group] = nil
		return
	}
	if b.keys[group] == nil {
		b.keys[group] = make(map[string]struct{})
	}
	b.keys[group][string(key)] = struct{}{}
	b.bytes[group] += len(key)
}

func (b *historyQueryKeysBuilder) appendTo(out []byte) []byte {
	var flags uint64
	for group := range b.keys {
		if !b.unknown[group] {
			flags |= 1 << group
		}
	}
	out = binary.AppendUvarint(out, flags)
	for group, keys := range b.keys {
		if b.unknown[group] {
			continue
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		out = binary.AppendUvarint(out, uint64(len(ordered)))
		for _, key := range ordered {
			out = binary.AppendUvarint(out, uint64(len(key)))
			out = append(out, key...)
		}
	}
	return out
}

func decodeHistoryQueryKeys(raw []byte, r *historyOwnerSpans) error {
	return decodeHistoryQueryKeysVisit(raw, r, nil)
}

// A query validates every directory, but only needs borrowed key membership.
// Do not allocate strings for the unrelated account and generation keys.
func decodeHistoryQueryKeysVisit(raw []byte, r *historyOwnerSpans, visit func(int, []byte)) error {
	bad := errors.New("rawdb: invalid optional history query keys")
	flags, n := binary.Uvarint(raw)
	if n <= 0 || flags >= 1<<historyQueryKeyGroups {
		return bad
	}
	raw = raw[n:]
	r.queryKnown = uint8(flags)
	for group := range r.queryKeys {
		if flags&(1<<group) == 0 {
			continue
		}
		count, n := binary.Uvarint(raw)
		if n <= 0 || count > historyQueryKeysMaxItems {
			return bad
		}
		raw = raw[n:]
		var used uint64
		var previous []byte
		for i := uint64(0); i < count; i++ {
			size, n := binary.Uvarint(raw)
			if n <= 0 || size == 0 || size > uint64(len(raw)-n) || size > historyQueryKeysMaxBytes-used {
				return bad
			}
			raw = raw[n:]
			key := raw[:size]
			raw = raw[size:]
			used += size
			if i > 0 && bytes.Compare(key, previous) <= 0 {
				return bad
			}
			previous = key
			if visit == nil {
				r.queryKeys[group] = append(r.queryKeys[group], string(key))
			} else {
				visit(group, key)
			}
		}
	}
	if len(raw) != 0 {
		return bad
	}
	return nil
}

type historyTailKeyQuery struct {
	group     int
	fullNames map[string][]string // full canonical key -> original query names
	hashes    map[string]uint64
	wanted    map[string][]byte // shrinks after complete repaired blocks
}

func newHistoryTailKeyQuery(flat StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, wanted map[string][]byte) *historyTailKeyQuery {
	group, ok := historyQueryKeyGroup(flat, owner, domain)
	if !ok {
		return nil
	}
	q := &historyTailKeyQuery{group: group, fullNames: make(map[string][]string, len(wanted)), hashes: make(map[string]uint64, len(wanted)), wanted: wanted}
	for name, key := range wanted {
		full, ok := stateDomainChangeLatestKeyByKey(flat, owner, generation, domain, key)
		if !ok {
			return nil
		}
		q.fullNames[string(full)] = append(q.fullNames[string(full)], name)
		q.hashes[name] = historyQueryKeyHash(full)
	}
	return q
}

func (q *historyTailKeyQuery) skipPack(ctx context.Context, db StateKVHistoryReader, pack []byte, block uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	marker, ok := db.(pointread.PinnedKeyValueView)
	if !ok || !marker.IsPinnedKeyValueView() {
		return false, nil
	}
	raw, present, err := readPresentValue(db, stateChangeOwnerSpansKey(block), "history query keys")
	if err != nil || !present {
		return false, err
	}
	if len(raw) < 8 || (!bytes.Equal(raw[:8], historyOwnerSpansMagic[:]) && !bytes.Equal(raw[:8], historyOwnerSpansMagicV3[:])) {
		return false, nil
	}
	work := historyOwnerSpansWorkPool.Get().(*historyOwnerSpansWork)
	defer func() {
		work.index = historyOwnerSpans{}
		if cap(work.payload) <= 256<<10 && cap(work.owners)*common.AddressLength <= 256<<10 && cap(work.spans)*12 <= 256<<10 {
			historyOwnerSpansWorkPool.Put(work)
		}
	}()
	matched := false
	r, err := decodeHistoryOwnerSpansIntoVisit(raw, block, work, func(group int, key []byte) {
		if group != q.group || matched {
			return
		}
		for _, name := range q.fullNames[string(key)] {
			if _, active := q.wanted[name]; active {
				matched = true
				return
			}
		}
	})
	if err != nil || r.queryKnown&(1<<q.group) == 0 || blake2b.Sum256(pack) != r.digest {
		return false, nil
	}
	if filter := r.queryFilters[q.group]; filter != nil {
		matched = false
		for name := range q.wanted {
			if historyQueryFilterContains(filter, q.hashes[name]) {
				matched = true
				break
			}
		}
	}
	return !matched, ctx.Err()
}

// Filtered rows cannot populate either complete owner cache. Reuse an already
// complete cache when available; otherwise stream only this key query and let
// the high-level request's exact-key memo own completed results.
func iterateFirstStateHistoryKeyTail(ctx context.Context, db StateKVHistoryReader, from, to, target, head uint64, owner common.Address, q *historyTailKeyQuery, fn func(*StateDomainChange) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if from > to {
		return nil
	}
	if q == nil {
		return iterateFirstStateHistoryTail(ctx, db, from, to, target, head, owner, fn)
	}
	if v, ok := db.(*stateHistoryQueryTailView); ok {
		v.mu.Lock()
		entry, first := v.entries[owner], v.first[owner]
		cached := entry != nil && entry.from <= from && entry.to >= to || first != nil && first.from == from && first.to == to && first.targetTx == target && first.headTx == head
		v.mu.Unlock()
		if cached {
			return iterateFirstStateHistoryTail(ctx, db, from, to, target, head, owner, fn)
		}
	}
	// Cost hint only: an old/unindexed first pack keeps the existing planned
	// owner cache path. Avoid rescanning a wholly legacy tail once per key
	// family merely because the new optional directory is not deployed yet.
	available, err := historyQueryDirectoryHint(ctx, db, from, to)
	if err != nil {
		return err
	}
	if !available {
		return iterateFirstStateHistoryTail(ctx, db, from, to, target, head, owner, fn)
	}
	return iterateStateHistorySelectedOwnersTailFiltered(ctx, db, from, to, func(candidate common.Address) bool { return candidate == owner }, q.skipPack, fn)
}

func historyQueryDirectoryHint(ctx context.Context, db StateKVHistoryReader, from, to uint64) (bool, error) {
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], from)
	it := db.NewIterator(stateChangeOwnerSpansPrefix, start[:])
	defer it.Release()
	for checked := 0; checked < 8 && it.Next(); checked++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		key := it.Key()
		if len(key) != len(stateChangeOwnerSpansPrefix)+8 || !bytes.HasPrefix(key, stateChangeOwnerSpansPrefix) {
			continue
		}
		if binary.BigEndian.Uint64(key[len(stateChangeOwnerSpansPrefix):]) > to {
			break
		}
		value := it.Value()
		if len(value) >= 8 && (bytes.Equal(value[:8], historyOwnerSpansMagic[:]) || bytes.Equal(value[:8], historyOwnerSpansMagicV3[:])) {
			return true, nil
		}
	}
	return false, it.Error()
}
