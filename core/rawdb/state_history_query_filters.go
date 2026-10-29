package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/cespare/xxhash/v2"
)

// Four probes in power-of-two bitsets. A collision only requires canonical
// fallback; a missing bit proves that the exact canonical key was not inserted.
var historyQueryFilterBytes = [historyQueryKeyGroups]int{2048, 512, 128}

// The representation marker is inside the payload checksum. Changing only an
// outer V3/V4 version byte cannot reinterpret exact-key bytes as Bloom bits.
var historyQueryFilterTag = [8]byte{'G', 'T', 'H', 'Q', 'B', 'F', '0', '4'}

type historyQueryFiltersBuilder struct {
	filters [historyQueryKeyGroups][]byte
	unknown [historyQueryKeyGroups]bool
}

func historyQueryKeyHash(key []byte) uint64 { return xxhash.Sum64(key) }

func historyQueryFilterContains(filter []byte, hash uint64) bool {
	if len(filter) == 0 {
		return true
	}
	mask := uint64(len(filter)*8 - 1)
	step := ((hash >> 32) ^ (hash >> 16)) | 1
	for i := uint64(0); i < 4; i++ {
		bit := (hash + i*step) & mask
		if filter[bit>>3]&(1<<(bit&7)) == 0 {
			return false
		}
	}
	return true
}

func (b *historyQueryFiltersBuilder) add(row *StateDomainChange) {
	group, ok := historyQueryKeyGroup(row.FlatDomain, row.Owner, row.Domain)
	if !ok || b.unknown[group] {
		return
	}
	if len(row.Key) > historyQueryKeysMaxBytes {
		b.unknown[group] = true
		b.filters[group] = nil
		return
	}
	key, ok := stateDomainChangeLatestKeyByKey(row.FlatDomain, row.Owner, row.Generation, row.Domain, row.Key)
	if !ok {
		b.unknown[group] = true
		b.filters[group] = nil
		return
	}
	if b.filters[group] == nil {
		b.filters[group] = make([]byte, historyQueryFilterBytes[group])
	}
	hash := historyQueryKeyHash(key)
	mask := uint64(len(b.filters[group])*8 - 1)
	step := ((hash >> 32) ^ (hash >> 16)) | 1
	for i := uint64(0); i < 4; i++ {
		bit := (hash + i*step) & mask
		b.filters[group][bit>>3] |= 1 << (bit & 7)
	}
}

func (b *historyQueryFiltersBuilder) appendTo(out []byte) []byte {
	var flags uint64
	for group := range b.filters {
		if !b.unknown[group] {
			flags |= 1 << group
		}
	}
	out = append(out, historyQueryFilterTag[:]...)
	out = binary.AppendUvarint(out, flags)
	var empty [2048]byte
	for group, filter := range b.filters {
		if b.unknown[group] {
			continue
		}
		if filter == nil {
			filter = empty[:historyQueryFilterBytes[group]]
		}
		out = append(out, filter...)
	}
	return out
}

func decodeHistoryQueryFilters(raw []byte, r *historyOwnerSpans) error {
	bad := errors.New("rawdb: invalid optional history query filters")
	if len(raw) < len(historyQueryFilterTag) || !bytes.Equal(raw[:len(historyQueryFilterTag)], historyQueryFilterTag[:]) {
		return bad
	}
	raw = raw[len(historyQueryFilterTag):]
	flags, n := binary.Uvarint(raw)
	if n <= 0 || flags >= 1<<historyQueryKeyGroups {
		return bad
	}
	raw = raw[n:]
	r.queryKnown = uint8(flags)
	for group, size := range historyQueryFilterBytes {
		if flags&(1<<group) == 0 {
			continue
		}
		if len(raw) < size {
			return bad
		}
		r.queryFilters[group] = raw[:size]
		raw = raw[size:]
	}
	if len(raw) != 0 {
		return bad
	}
	return nil
}
