package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	stateDomainChangeBinaryAccessorV3HashSize       = 16
	stateDomainChangeBinaryAccessorV3ExactEntrySize = stateDomainChangeBinaryAccessorV3HashSize + 8 + 4
	stateDomainChangeBinaryAccessorV3GroupKeySize   = common.AccountIDLength + 8 + 2
	stateDomainChangeBinaryAccessorV3GroupEntrySize = 8 + 4
	stateDomainChangeBinaryAccessorV3HeaderExtra    = 8 // group count
)

type stateDomainChangeBinaryAccessorV3ExactEntry struct {
	hash        [stateDomainChangeBinaryAccessorV3HashSize]byte
	offset      uint64
	recordIndex uint32
}

type stateDomainChangeBinaryAccessorV3Group struct {
	key     [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	records []stateDomainChangeBinaryAccessorV3GroupIndexEntry
}

type stateDomainChangeBinaryAccessorV3GroupRecord struct {
	key         []byte
	offset      uint64
	recordIndex uint32
}

type stateDomainChangeBinaryAccessorV3GroupIndexEntry struct {
	offset      uint64
	recordIndex uint32
}

type stateDomainChangeBinaryAccessorV3Layout struct {
	groupCount        uint64
	exactOffset       uint64
	groupOffsetsStart uint64
	groupPayloadStart uint64
}

type stateDomainChangeBinaryAccessorV3GroupMeta struct {
	key          [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	offsetsStart uint64
	count        uint64
	next         uint64
}

func stateDomainChangeBinaryAccessorV3Hash(key []byte) [stateDomainChangeBinaryAccessorV3HashSize]byte {
	sum := sha256.Sum256(key)
	var out [stateDomainChangeBinaryAccessorV3HashSize]byte
	copy(out[:], sum[:len(out)])
	return out
}

func stateDomainChangeBinaryAccessorV3GroupKey(change *rawdb.StateDomainChange) ([stateDomainChangeBinaryAccessorV3GroupKeySize]byte, bool) {
	var key [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	if change == nil || change.FlatDomain != rawdb.StateFlatDomainKVLatest {
		return key, false
	}
	id := change.Owner.AccountID()
	copy(key[0:common.AccountIDLength], id[:])
	binary.BigEndian.PutUint64(key[common.AccountIDLength:common.AccountIDLength+8], change.Generation)
	binary.BigEndian.PutUint16(key[common.AccountIDLength+8:common.AccountIDLength+10], uint16(change.Domain))
	return key, true
}

func stateDomainChangeBinaryAccessorV3GroupLookupKey(owner common.Address, generation uint64, domain kvdomains.KVDomain) [stateDomainChangeBinaryAccessorV3GroupKeySize]byte {
	var key [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	id := owner.AccountID()
	copy(key[0:common.AccountIDLength], id[:])
	binary.BigEndian.PutUint64(key[common.AccountIDLength:common.AccountIDLength+8], generation)
	binary.BigEndian.PutUint16(key[common.AccountIDLength+8:common.AccountIDLength+10], uint16(domain))
	return key
}

func stateDomainChangeBinaryAccessorV3GroupKeyFromAccessorKey(accessorKey []byte) ([stateDomainChangeBinaryAccessorV3GroupKeySize]byte, bool) {
	var groupKey [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	if len(accessorKey) < 1+stateDomainChangeBinaryAccessorV3GroupKeySize || rawdb.StateFlatDomain(accessorKey[0]) != rawdb.StateFlatDomainKVLatest {
		return groupKey, false
	}
	copy(groupKey[:], accessorKey[1:1+stateDomainChangeBinaryAccessorV3GroupKeySize])
	return groupKey, true
}

func encodeStateDomainChangeBinaryAccessorV3(fromTxNum, toTxNum uint64, entries []stateDomainChangeBinaryAccessorEntry) ([]byte, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	exact := make([]stateDomainChangeBinaryAccessorV3ExactEntry, 0, len(entries))
	groups := make(map[[stateDomainChangeBinaryAccessorV3GroupKeySize]byte][]stateDomainChangeBinaryAccessorV3GroupRecord)
	for i, entry := range entries {
		if entry.recordIndex > math.MaxUint32 {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor v3 record index %d exceeds uint32", entry.recordIndex)
		}
		if err := validateStateDomainChangeBinaryAccessorEntry(SegmentRef{Path: "v3 accessor", FromTxNum: fromTxNum, ToTxNum: toTxNum}, entry, uint64(i)); err != nil {
			return nil, err
		}
		exact = append(exact, stateDomainChangeBinaryAccessorV3ExactEntry{
			hash:        stateDomainChangeBinaryAccessorV3Hash(entry.key),
			offset:      entry.offset,
			recordIndex: uint32(entry.recordIndex),
		})
		if groupKey, ok := stateDomainChangeBinaryAccessorV3GroupKeyFromAccessorKey(entry.key); ok {
			groups[groupKey] = append(groups[groupKey], stateDomainChangeBinaryAccessorV3GroupRecord{
				key:         append([]byte(nil), entry.key...),
				offset:      entry.offset,
				recordIndex: uint32(entry.recordIndex),
			})
		}
	}
	sort.Slice(exact, func(i, j int) bool {
		if cmp := bytes.Compare(exact[i].hash[:], exact[j].hash[:]); cmp != 0 {
			return cmp < 0
		}
		if exact[i].offset != exact[j].offset {
			return exact[i].offset < exact[j].offset
		}
		return exact[i].recordIndex < exact[j].recordIndex
	})
	orderedGroups := make([]stateDomainChangeBinaryAccessorV3Group, 0, len(groups))
	for key, records := range groups {
		sort.Slice(records, func(i, j int) bool {
			if cmp := bytes.Compare(records[i].key, records[j].key); cmp != 0 {
				return cmp < 0
			}
			return records[i].offset < records[j].offset
		})
		groupEntries := make([]stateDomainChangeBinaryAccessorV3GroupIndexEntry, len(records))
		for i := range records {
			groupEntries[i] = stateDomainChangeBinaryAccessorV3GroupIndexEntry{offset: records[i].offset, recordIndex: records[i].recordIndex}
		}
		orderedGroups = append(orderedGroups, stateDomainChangeBinaryAccessorV3Group{key: key, records: groupEntries})
	}
	sort.Slice(orderedGroups, func(i, j int) bool {
		return bytes.Compare(orderedGroups[i].key[:], orderedGroups[j].key[:]) < 0
	})

	if uint64(len(exact)) > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize)-stateDomainChangeBinaryAccessorV3HeaderExtra)/stateDomainChangeBinaryAccessorV3ExactEntrySize {
		return nil, errors.New("snapshots: state-domain-change accessor v3 exact table overflows size")
	}
	if uint64(len(orderedGroups)) > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize)-stateDomainChangeBinaryAccessorV3HeaderExtra-uint64(len(exact))*stateDomainChangeBinaryAccessorV3ExactEntrySize)/8 {
		return nil, errors.New("snapshots: state-domain-change accessor v3 group directory overflows size")
	}

	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&buf, stateDomainChangeBinaryAccessorMagic, fromTxNum, toTxNum, uint64(len(exact)), stateDomainChangeBinaryVersionV3)
	writeUint64(&buf, uint64(len(orderedGroups)))
	for _, entry := range exact {
		buf.Write(entry.hash[:])
		writeUint64(&buf, entry.offset)
		writeUint32(&buf, entry.recordIndex)
	}
	payloadStart := uint64(buf.Len()) + uint64(len(orderedGroups))*8
	var payload bytes.Buffer
	for _, group := range orderedGroups {
		writeUint64(&buf, payloadStart+uint64(payload.Len()))
		payload.Write(group.key[:])
		writeUint64(&payload, uint64(len(group.records)))
		for _, record := range group.records {
			writeUint64(&payload, record.offset)
			writeUint32(&payload, record.recordIndex)
		}
	}
	buf.Write(payload.Bytes())
	return buf.Bytes(), nil
}

func stateDomainChangeBinaryAccessorV3LayoutAt(r io.ReaderAt, fileSize uint64, header stateDomainChangeBinaryHeader) (stateDomainChangeBinaryAccessorV3Layout, error) {
	if header.version != stateDomainChangeBinaryVersionV3 {
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: accessor v3 layout requested for version %d", header.version)
	}
	// recordIndex is part of every v3 index entry. Reject impossible metadata
	// before any query converts a count to an in-memory search bound.
	if header.count > math.MaxUint32 {
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: state-domain-change accessor v3 count %d exceeds uint32 record index", header.count)
	}
	if fileSize < stateDomainChangeBinaryHeaderSize+stateDomainChangeBinaryAccessorV3HeaderExtra {
		return stateDomainChangeBinaryAccessorV3Layout{}, io.ErrUnexpectedEOF
	}
	var groupCountRaw [8]byte
	if _, err := r.ReadAt(groupCountRaw[:], stateDomainChangeBinaryHeaderSize); err != nil {
		return stateDomainChangeBinaryAccessorV3Layout{}, err
	}
	groupCount := binary.BigEndian.Uint64(groupCountRaw[:])
	if groupCount > header.count {
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: state-domain-change accessor v3 group count %d exceeds record count %d", groupCount, header.count)
	}
	exactOffset := uint64(stateDomainChangeBinaryHeaderSize + stateDomainChangeBinaryAccessorV3HeaderExtra)
	if header.count > (math.MaxUint64-exactOffset)/stateDomainChangeBinaryAccessorV3ExactEntrySize {
		return stateDomainChangeBinaryAccessorV3Layout{}, errors.New("snapshots: state-domain-change accessor v3 exact table overflows size")
	}
	groupOffsetsStart := exactOffset + header.count*stateDomainChangeBinaryAccessorV3ExactEntrySize
	if groupCount > (math.MaxUint64-groupOffsetsStart)/8 {
		return stateDomainChangeBinaryAccessorV3Layout{}, errors.New("snapshots: state-domain-change accessor v3 group directory overflows size")
	}
	groupPayloadStart := groupOffsetsStart + groupCount*8
	if groupPayloadStart > fileSize {
		return stateDomainChangeBinaryAccessorV3Layout{}, io.ErrUnexpectedEOF
	}
	return stateDomainChangeBinaryAccessorV3Layout{
		groupCount:        groupCount,
		exactOffset:       exactOffset,
		groupOffsetsStart: groupOffsetsStart,
		groupPayloadStart: groupPayloadStart,
	}, nil
}

func readStateDomainChangeBinaryAccessorV3ExactEntryAt(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, index uint64) (stateDomainChangeBinaryAccessorV3ExactEntry, error) {
	if index > (math.MaxInt64-layout.exactOffset)/stateDomainChangeBinaryAccessorV3ExactEntrySize {
		return stateDomainChangeBinaryAccessorV3ExactEntry{}, fmt.Errorf("snapshots: state-domain-change accessor v3 exact index too large: %d", index)
	}
	var raw [stateDomainChangeBinaryAccessorV3ExactEntrySize]byte
	if _, err := r.ReadAt(raw[:], int64(layout.exactOffset+index*stateDomainChangeBinaryAccessorV3ExactEntrySize)); err != nil {
		return stateDomainChangeBinaryAccessorV3ExactEntry{}, err
	}
	var entry stateDomainChangeBinaryAccessorV3ExactEntry
	copy(entry.hash[:], raw[:stateDomainChangeBinaryAccessorV3HashSize])
	entry.offset = binary.BigEndian.Uint64(raw[stateDomainChangeBinaryAccessorV3HashSize : stateDomainChangeBinaryAccessorV3HashSize+8])
	entry.recordIndex = binary.BigEndian.Uint32(raw[stateDomainChangeBinaryAccessorV3HashSize+8:])
	return entry, nil
}

func readStateDomainChangeBinaryAccessorV3GroupOffsetAt(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, index uint64) (uint64, error) {
	if index > (math.MaxInt64-layout.groupOffsetsStart)/8 {
		return 0, fmt.Errorf("snapshots: state-domain-change accessor v3 group index too large: %d", index)
	}
	var raw [8]byte
	if _, err := r.ReadAt(raw[:], int64(layout.groupOffsetsStart+index*8)); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(raw[:]), nil
}

func readStateDomainChangeBinaryAccessorV3GroupMetaAt(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, index, fileSize uint64) (stateDomainChangeBinaryAccessorV3GroupMeta, error) {
	if index >= layout.groupCount {
		return stateDomainChangeBinaryAccessorV3GroupMeta{}, io.EOF
	}
	offset, err := readStateDomainChangeBinaryAccessorV3GroupOffsetAt(r, layout, index)
	if err != nil {
		return stateDomainChangeBinaryAccessorV3GroupMeta{}, err
	}
	next := fileSize
	if index+1 < layout.groupCount {
		next, err = readStateDomainChangeBinaryAccessorV3GroupOffsetAt(r, layout, index+1)
		if err != nil {
			return stateDomainChangeBinaryAccessorV3GroupMeta{}, err
		}
	}
	if offset < layout.groupPayloadStart || offset >= next || next > fileSize || next-offset < stateDomainChangeBinaryAccessorV3GroupKeySize+8 {
		return stateDomainChangeBinaryAccessorV3GroupMeta{}, io.ErrUnexpectedEOF
	}
	var fixed [stateDomainChangeBinaryAccessorV3GroupKeySize + 8]byte
	if _, err := r.ReadAt(fixed[:], int64(offset)); err != nil {
		return stateDomainChangeBinaryAccessorV3GroupMeta{}, err
	}
	count := binary.BigEndian.Uint64(fixed[stateDomainChangeBinaryAccessorV3GroupKeySize:])
	if count > (next-offset-uint64(len(fixed)))/stateDomainChangeBinaryAccessorV3GroupEntrySize {
		return stateDomainChangeBinaryAccessorV3GroupMeta{}, io.ErrUnexpectedEOF
	}
	if want := uint64(len(fixed)) + count*stateDomainChangeBinaryAccessorV3GroupEntrySize; want != next-offset {
		return stateDomainChangeBinaryAccessorV3GroupMeta{}, fmt.Errorf("snapshots: state-domain-change accessor v3 group %d has %d trailing bytes", index, next-offset-want)
	}
	if count == 0 {
		return stateDomainChangeBinaryAccessorV3GroupMeta{}, errors.New("snapshots: state-domain-change accessor v3 group has no record offsets")
	}
	var meta stateDomainChangeBinaryAccessorV3GroupMeta
	copy(meta.key[:], fixed[:stateDomainChangeBinaryAccessorV3GroupKeySize])
	meta.offsetsStart = offset + uint64(len(fixed))
	meta.count = count
	meta.next = next
	return meta, nil
}

func readStateDomainChangeBinaryAccessorV3GroupRecordAt(r io.ReaderAt, meta stateDomainChangeBinaryAccessorV3GroupMeta, index uint64) (stateDomainChangeBinaryAccessorV3GroupIndexEntry, error) {
	if index >= meta.count || index > (math.MaxInt64-meta.offsetsStart)/stateDomainChangeBinaryAccessorV3GroupEntrySize {
		return stateDomainChangeBinaryAccessorV3GroupIndexEntry{}, fmt.Errorf("snapshots: state-domain-change accessor v3 group record index %d outside count %d", index, meta.count)
	}
	var raw [stateDomainChangeBinaryAccessorV3GroupEntrySize]byte
	if _, err := r.ReadAt(raw[:], int64(meta.offsetsStart+index*stateDomainChangeBinaryAccessorV3GroupEntrySize)); err != nil {
		return stateDomainChangeBinaryAccessorV3GroupIndexEntry{}, err
	}
	return stateDomainChangeBinaryAccessorV3GroupIndexEntry{
		offset:      binary.BigEndian.Uint64(raw[0:8]),
		recordIndex: binary.BigEndian.Uint32(raw[8:]),
	}, nil
}

func stateDomainChangeBinaryAccessorV3ExactLowerBound(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, count uint64, target [stateDomainChangeBinaryAccessorV3HashSize]byte) (uint64, bool, error) {
	low, high := uint64(0), count
	for low < high {
		mid := low + (high-low)/2
		entry, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(r, layout, mid)
		if err != nil {
			return 0, false, err
		}
		if bytes.Compare(entry.hash[:], target[:]) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, low < count, nil
}

func stateDomainChangeBinaryAccessorV3ExactUpperBound(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, count uint64, target [stateDomainChangeBinaryAccessorV3HashSize]byte) (uint64, error) {
	low, high := uint64(0), count
	for low < high {
		mid := low + (high-low)/2
		entry, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(r, layout, mid)
		if err != nil {
			return 0, err
		}
		if bytes.Compare(entry.hash[:], target[:]) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, nil
}

// stateDomainChangeBinaryAccessorV3TxLowerBound uses the fact that exact
// accessor entries with the same hash are ordered by segment offset and the
// segment itself is ordered by txNum/seq. Hash collisions remain in that same
// monotonic order, so the requested tx lower bound can be found without
// decoding every older history record for a frequently-mutated key.
func stateDomainChangeBinaryAccessorV3TxLowerBound(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, low, high, fromTxNum uint64) (uint64, error) {
	segmentHeader, err := stateDomainChangeBinaryHeaderForReader(segment)
	if err != nil {
		return 0, err
	}
	for low < high {
		mid := low + (high-low)/2
		entry, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(accessor, layout, mid)
		if err != nil {
			return 0, err
		}
		change, _, err := readStateDomainChangeBinaryRecordFrame(segment, entry.offset, segmentSize, segmentHeader.version, uint64(entry.recordIndex), false)
		if err != nil {
			return 0, err
		}
		if change.TxNum < fromTxNum {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, nil
}

func stateDomainChangeBinaryAccessorV3GroupLowerBound(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, fileSize uint64, target [stateDomainChangeBinaryAccessorV3GroupKeySize]byte) (uint64, bool, error) {
	low, high := uint64(0), layout.groupCount
	for low < high {
		mid := low + (high-low)/2
		group, err := readStateDomainChangeBinaryAccessorV3GroupMetaAt(r, layout, mid, fileSize)
		if err != nil {
			return 0, false, err
		}
		if bytes.Compare(group.key[:], target[:]) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, low < layout.groupCount, nil
}

func iterateStateDomainChangeBinarySegmentByAccessorV3Key(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader, lookupKey []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	layout, err := stateDomainChangeBinaryAccessorV3LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	target := stateDomainChangeBinaryAccessorV3Hash(lookupKey)
	start, ok, err := stateDomainChangeBinaryAccessorV3ExactLowerBound(accessor, layout, header.count, target)
	if err != nil || !ok {
		return err
	}
	first, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(accessor, layout, start)
	if err != nil {
		return err
	}
	if !bytes.Equal(first.hash[:], target[:]) {
		return nil
	}
	end, err := stateDomainChangeBinaryAccessorV3ExactUpperBound(accessor, layout, header.count, target)
	if err != nil {
		return err
	}
	if fromTxNum > header.fromTxNum {
		start, err = stateDomainChangeBinaryAccessorV3TxLowerBound(segment, segmentSize, accessor, layout, start, end, fromTxNum)
		if err != nil {
			return err
		}
	}
	for i := start; i < end; i++ {
		entry, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(accessor, layout, i)
		if err != nil {
			return err
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, entry.offset, segmentSize, uint64(entry.recordIndex))
		if err != nil {
			return err
		}
		if !bytes.Equal(stateDomainChangeBinaryAccessorKey(change), lookupKey) || change.TxNum < fromTxNum || change.TxNum > toTxNum {
			continue
		}
		cont, err := fn(change)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func iterateStateDomainChangeBinarySegmentByAccessorV3Prefix(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader, lookupPrefix []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if len(lookupPrefix) < 1+stateDomainChangeBinaryAccessorV3GroupKeySize || rawdb.StateFlatDomain(lookupPrefix[0]) != rawdb.StateFlatDomainKVLatest {
		return errors.New("snapshots: invalid state-domain-change accessor v3 prefix lookup key")
	}
	layout, err := stateDomainChangeBinaryAccessorV3LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	var target [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	copy(target[:], lookupPrefix[1:1+stateDomainChangeBinaryAccessorV3GroupKeySize])
	start, ok, err := stateDomainChangeBinaryAccessorV3GroupLowerBound(accessor, layout, accessorSize, target)
	if err != nil || !ok {
		return err
	}
	group, err := readStateDomainChangeBinaryAccessorV3GroupMetaAt(accessor, layout, start, accessorSize)
	if err != nil {
		return err
	}
	if !bytes.Equal(group.key[:], target[:]) {
		return nil
	}
	for i := uint64(0); i < group.count; i++ {
		record, err := readStateDomainChangeBinaryAccessorV3GroupRecordAt(accessor, group, i)
		if err != nil {
			return err
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, record.offset, segmentSize, uint64(record.recordIndex))
		if err != nil {
			return err
		}
		accessorKey := stateDomainChangeBinaryAccessorKey(change)
		if !bytes.HasPrefix(accessorKey, lookupPrefix) {
			if bytes.Compare(accessorKey, lookupPrefix) > 0 {
				return nil
			}
			continue
		}
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			continue
		}
		cont, err := fn(change)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func readStateDomainChangeBinaryAccessorV3Debug(dir string, ref SegmentRef, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader) ([]stateDomainChangeBinaryAccessorEntry, error) {
	layout, err := stateDomainChangeBinaryAccessorV3LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return nil, err
	}
	segmentRef := SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      ref.Path[:len(ref.Path)-len(filepath.Ext(ref.Path))] + ".seg",
	}
	segment, segmentSize, _, err := openHistorySegmentForRead(dir, segmentRef)
	if err != nil {
		return nil, err
	}
	defer segment.Close()
	entries := make([]stateDomainChangeBinaryAccessorEntry, 0, header.count)
	for i := uint64(0); i < header.count; i++ {
		exact, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(accessor, layout, i)
		if err != nil {
			return nil, err
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, exact.offset, segmentSize, uint64(exact.recordIndex))
		if err != nil {
			return nil, err
		}
		if got := stateDomainChangeBinaryAccessorV3Hash(stateDomainChangeBinaryAccessorKey(change)); !bytes.Equal(got[:], exact.hash[:]) {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor v3 hash mismatch at offset %d", exact.offset)
		}
		entries = append(entries, stateDomainChangeBinaryAccessorEntry{
			key:         stateDomainChangeBinaryAccessorKey(change),
			txNum:       change.TxNum,
			seq:         change.Seq,
			offset:      exact.offset,
			recordIndex: uint64(exact.recordIndex),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return compareStateDomainChangeBinaryAccessorEntry(entries[i], entries[j]) < 0
	})
	return entries, nil
}

func checkStateDomainChangeBinaryAccessorV3(accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader) error {
	if header.count > math.MaxUint32 {
		return fmt.Errorf("snapshots: state-domain-change accessor v3 count %d exceeds uint32 record index", header.count)
	}
	layout, err := stateDomainChangeBinaryAccessorV3LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	var previous stateDomainChangeBinaryAccessorV3ExactEntry
	for i := uint64(0); i < header.count; i++ {
		entry, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(accessor, layout, i)
		if err != nil {
			return err
		}
		if entry.offset < stateDomainChangeBinaryHeaderSize {
			return fmt.Errorf("snapshots: state-domain-change accessor v3 exact entry %d has invalid record offset %d", i, entry.offset)
		}
		if uint64(entry.recordIndex) >= header.count {
			return fmt.Errorf("snapshots: state-domain-change accessor v3 exact entry %d record index %d outside count %d", i, entry.recordIndex, header.count)
		}
		if i > 0 {
			if cmp := bytes.Compare(previous.hash[:], entry.hash[:]); cmp > 0 ||
				(cmp == 0 && (previous.offset > entry.offset || (previous.offset == entry.offset && previous.recordIndex >= entry.recordIndex))) {
				return fmt.Errorf("snapshots: state-domain-change accessor v3 exact entries are not strictly sorted at %d", i)
			}
		}
		previous = entry
	}
	var previousGroup [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	for i := uint64(0); i < layout.groupCount; i++ {
		group, err := readStateDomainChangeBinaryAccessorV3GroupMetaAt(accessor, layout, i, accessorSize)
		if err != nil {
			return err
		}
		if i > 0 && bytes.Compare(previousGroup[:], group.key[:]) >= 0 {
			return fmt.Errorf("snapshots: state-domain-change accessor v3 groups are not strictly sorted at %d", i)
		}
		for j := uint64(0); j < group.count; j++ {
			record, err := readStateDomainChangeBinaryAccessorV3GroupRecordAt(accessor, group, j)
			if err != nil {
				return err
			}
			if record.offset < stateDomainChangeBinaryHeaderSize {
				return fmt.Errorf("snapshots: state-domain-change accessor v3 group %d has invalid record offset %d", i, record.offset)
			}
			if uint64(record.recordIndex) >= header.count {
				return fmt.Errorf("snapshots: state-domain-change accessor v3 group %d record index %d outside count %d", i, record.recordIndex, header.count)
			}
		}
		previousGroup = group.key
	}
	return nil
}

func readStateDomainChangeBinaryRecordAtIndex(segment io.ReaderAt, segmentSize uint64, index io.ReaderAt, indexCount, recordIndex uint64) (*rawdb.StateDomainChange, uint64, error) {
	low, high := uint64(0), indexCount
	for low < high {
		mid := low + (high-low)/2
		entry, err := readStateDomainChangeBinaryIndexEntryAt(index, mid)
		if err != nil {
			return nil, 0, err
		}
		if entry.count > math.MaxUint64-entry.recordIndex {
			return nil, 0, fmt.Errorf("snapshots: state-domain-change index entry %d record range overflows", mid)
		}
		if entry.recordIndex+entry.count <= recordIndex {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low >= indexCount {
		return nil, 0, fmt.Errorf("snapshots: state-domain-change record index %d is not covered by index", recordIndex)
	}
	entry, err := readStateDomainChangeBinaryIndexEntryAt(index, low)
	if err != nil {
		return nil, 0, err
	}
	if recordIndex < entry.recordIndex || recordIndex-entry.recordIndex >= entry.count {
		return nil, 0, fmt.Errorf("snapshots: state-domain-change index entry does not cover record %d", recordIndex)
	}
	offset := entry.offset
	for i := entry.recordIndex; i <= recordIndex; i++ {
		change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, offset, segmentSize, i)
		if err != nil {
			return nil, 0, err
		}
		if i == recordIndex {
			return change, offset, nil
		}
		offset = next
	}
	return nil, 0, errors.New("snapshots: state-domain-change record index scan did not reach target")
}

func verifyStateDomainChangeBinaryAccessorV3Coverage(historyRef, accessorRef SegmentRef, segment io.ReaderAt, segmentSize, recordCount uint64, index io.ReaderAt, indexCount uint64, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader) error {
	if err := checkStateDomainChangeBinaryAccessorV3(accessor, accessorSize, header); err != nil {
		return err
	}
	if header.count != recordCount {
		return fmt.Errorf("snapshots: state-domain-change accessor %q count %d, want segment count %d", accessorRef.Path, header.count, recordCount)
	}
	layout, err := stateDomainChangeBinaryAccessorV3LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	seenWords := recordCount / 64
	if recordCount%64 != 0 {
		seenWords++
	}
	exactSeen := make([]uint64, seenWords)
	groupSeen := make([]uint64, seenWords)
	mark := func(seen []uint64, recordIndex uint32, kind string) error {
		index := uint64(recordIndex)
		if index >= recordCount {
			return fmt.Errorf("snapshots: state-domain-change accessor %q %s record index %d outside segment count %d", accessorRef.Path, kind, index, recordCount)
		}
		word := index / 64
		mask := uint64(1) << (index % 64)
		if seen[word]&mask != 0 {
			return fmt.Errorf("snapshots: state-domain-change accessor %q covers %s record %d more than once", accessorRef.Path, kind, index)
		}
		seen[word] |= mask
		return nil
	}
	for i := uint64(0); i < header.count; i++ {
		exact, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(accessor, layout, i)
		if err != nil {
			return err
		}
		if err := mark(exactSeen, exact.recordIndex, "exact"); err != nil {
			return err
		}
		change, expectedOffset, err := readStateDomainChangeBinaryRecordAtIndex(segment, segmentSize, index, indexCount, uint64(exact.recordIndex))
		if err != nil {
			return err
		}
		if exact.offset != expectedOffset {
			return fmt.Errorf("snapshots: state-domain-change accessor %q exact entry %d offset %d, want record %d offset %d", accessorRef.Path, i, exact.offset, exact.recordIndex, expectedOffset)
		}
		if got := stateDomainChangeBinaryAccessorV3Hash(stateDomainChangeBinaryAccessorKey(change)); !bytes.Equal(got[:], exact.hash[:]) {
			return fmt.Errorf("snapshots: state-domain-change accessor %q exact hash mismatch at record %d", accessorRef.Path, exact.recordIndex)
		}
	}
	for i := uint64(0); i < layout.groupCount; i++ {
		group, err := readStateDomainChangeBinaryAccessorV3GroupMetaAt(accessor, layout, i, accessorSize)
		if err != nil {
			return err
		}
		var previousKey []byte
		var previousOffset uint64
		for j := uint64(0); j < group.count; j++ {
			record, err := readStateDomainChangeBinaryAccessorV3GroupRecordAt(accessor, group, j)
			if err != nil {
				return err
			}
			if err := mark(groupSeen, record.recordIndex, "group"); err != nil {
				return err
			}
			change, expectedOffset, err := readStateDomainChangeBinaryRecordAtIndex(segment, segmentSize, index, indexCount, uint64(record.recordIndex))
			if err != nil {
				return err
			}
			if record.offset != expectedOffset {
				return fmt.Errorf("snapshots: state-domain-change accessor %q group %d record %d offset %d, want %d", accessorRef.Path, i, record.recordIndex, record.offset, expectedOffset)
			}
			groupKey, ok := stateDomainChangeBinaryAccessorV3GroupKey(change)
			if !ok || !bytes.Equal(group.key[:], groupKey[:]) {
				return fmt.Errorf("snapshots: state-domain-change accessor %q group %d does not match record %d", accessorRef.Path, i, record.recordIndex)
			}
			accessorKey := stateDomainChangeBinaryAccessorKey(change)
			if j > 0 {
				if cmp := bytes.Compare(previousKey, accessorKey); cmp > 0 || (cmp == 0 && previousOffset >= record.offset) {
					return fmt.Errorf("snapshots: state-domain-change accessor %q group %d records are not in lookup order", accessorRef.Path, i)
				}
			}
			previousKey = append(previousKey[:0], accessorKey...)
			previousOffset = record.offset
		}
	}
	return iterateStateDomainChangeBinaryRecords(segment, segmentSize, func(recordIndex uint64, change *rawdb.StateDomainChange) error {
		if change.FlatDomain != rawdb.StateFlatDomainKVLatest {
			return nil
		}
		word := recordIndex / 64
		mask := uint64(1) << (recordIndex % 64)
		if groupSeen[word]&mask == 0 {
			return fmt.Errorf("snapshots: state-domain-change accessor %q is missing KV-latest group record %d", accessorRef.Path, recordIndex)
		}
		return nil
	})
}

func iterateStateDomainChangeBinaryRecords(segment io.ReaderAt, segmentSize uint64, fn func(uint64, *rawdb.StateDomainChange) error) error {
	return iterateStateDomainChangeBinaryRecordsWithOffset(segment, segmentSize, func(recordIndex, _ uint64, change *rawdb.StateDomainChange) error {
		return fn(recordIndex, change)
	})
}

func iterateStateDomainChangeBinaryRecordsWithOffset(segment io.ReaderAt, segmentSize uint64, fn func(uint64, uint64, *rawdb.StateDomainChange) error) error {
	header, err := readStateDomainChangeBinaryHeaderAt(segment, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		return err
	}
	_, offset, err := stateDomainChangeBinaryTxRangeTableBoundsAt(segment, segmentSize, SegmentRef{}, header)
	if err != nil {
		return err
	}
	for index := uint64(0); index < header.count; index++ {
		recordOffset := offset
		change, next, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, offset, segmentSize, index)
		if err != nil {
			return err
		}
		if err := fn(index, recordOffset, change); err != nil {
			return err
		}
		offset = next
	}
	if offset != segmentSize {
		return fmt.Errorf("snapshots: state-domain-change record stream has %d trailing bytes", segmentSize-offset)
	}
	return nil
}
