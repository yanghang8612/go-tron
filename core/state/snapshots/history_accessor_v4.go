package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

const stateDomainChangeBinaryAccessorV4GroupEntrySize = 4 + 8 + 4

type stateDomainChangeBinaryAccessorV4GroupRecord struct {
	logicalKey  []byte
	offset      uint64
	recordIndex uint32
}

type stateDomainChangeBinaryAccessorV4GroupMeta struct {
	key          [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	recordsStart uint64
	count        uint64
	next         uint64
}

type stateDomainChangeBinaryAccessorV4GroupIndexEntry struct {
	prefix      uint32
	offset      uint64
	recordIndex uint32
}

func stateDomainChangeBinaryAccessorV4LogicalPrefix(key []byte) uint32 {
	var prefix [4]byte
	copy(prefix[:], key)
	return binary.BigEndian.Uint32(prefix[:])
}

func encodeStateDomainChangeBinaryAccessorV4(fromTxNum, toTxNum uint64, entries []stateDomainChangeBinaryAccessorEntry) ([]byte, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	exact := make([]stateDomainChangeBinaryAccessorV3ExactEntry, 0, len(entries))
	groups := make(map[[stateDomainChangeBinaryAccessorV3GroupKeySize]byte][]stateDomainChangeBinaryAccessorV4GroupRecord)
	for i, entry := range entries {
		if entry.recordIndex > math.MaxUint32 {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor v4 record index %d exceeds uint32", entry.recordIndex)
		}
		if err := validateStateDomainChangeBinaryAccessorEntry(SegmentRef{Path: "v4 accessor", FromTxNum: fromTxNum, ToTxNum: toTxNum}, entry, uint64(i)); err != nil {
			return nil, err
		}
		exact = append(exact, stateDomainChangeBinaryAccessorV3ExactEntry{
			hash:        stateDomainChangeBinaryAccessorV3Hash(entry.key),
			offset:      entry.offset,
			recordIndex: uint32(entry.recordIndex),
		})
		if groupKey, ok := stateDomainChangeBinaryAccessorV3GroupKeyFromAccessorKey(entry.key); ok {
			groups[groupKey] = append(groups[groupKey], stateDomainChangeBinaryAccessorV4GroupRecord{
				logicalKey:  append([]byte(nil), entry.key[1+stateDomainChangeBinaryAccessorV3GroupKeySize:]...),
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
	keys := make([][stateDomainChangeBinaryAccessorV3GroupKeySize]byte, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	for _, key := range keys {
		sort.Slice(groups[key], func(i, j int) bool {
			if cmp := bytes.Compare(groups[key][i].logicalKey, groups[key][j].logicalKey); cmp != 0 {
				return cmp < 0
			}
			return groups[key][i].offset < groups[key][j].offset
		})
	}
	if uint64(len(exact)) > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize)-stateDomainChangeBinaryAccessorV3HeaderExtra)/stateDomainChangeBinaryAccessorV3ExactEntrySize {
		return nil, errors.New("snapshots: state-domain-change accessor v4 exact table overflows size")
	}
	if uint64(len(keys)) > (math.MaxUint64-uint64(stateDomainChangeBinaryHeaderSize)-stateDomainChangeBinaryAccessorV3HeaderExtra-uint64(len(exact))*stateDomainChangeBinaryAccessorV3ExactEntrySize)/8 {
		return nil, errors.New("snapshots: state-domain-change accessor v4 group directory overflows size")
	}

	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&buf, stateDomainChangeBinaryAccessorMagic, fromTxNum, toTxNum, uint64(len(exact)), stateDomainChangeBinaryVersionV4)
	writeUint64(&buf, uint64(len(keys)))
	for _, entry := range exact {
		buf.Write(entry.hash[:])
		writeUint64(&buf, entry.offset)
		writeUint32(&buf, entry.recordIndex)
	}
	payloadStart := uint64(buf.Len()) + uint64(len(keys))*8
	var payload bytes.Buffer
	for _, key := range keys {
		writeUint64(&buf, payloadStart+uint64(payload.Len()))
		payload.Write(key[:])
		writeUint64(&payload, uint64(len(groups[key])))
		for _, record := range groups[key] {
			writeUint32(&payload, stateDomainChangeBinaryAccessorV4LogicalPrefix(record.logicalKey))
			writeUint64(&payload, record.offset)
			writeUint32(&payload, record.recordIndex)
		}
	}
	buf.Write(payload.Bytes())
	return buf.Bytes(), nil
}

func stateDomainChangeBinaryAccessorV4LayoutAt(r io.ReaderAt, fileSize uint64, header stateDomainChangeBinaryHeader) (stateDomainChangeBinaryAccessorV3Layout, error) {
	if header.version != stateDomainChangeBinaryVersionV4 {
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: accessor v4 layout requested for version %d", header.version)
	}
	if header.count > math.MaxUint32 {
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: state-domain-change accessor v4 count %d exceeds uint32 record index", header.count)
	}
	if fileSize < stateDomainChangeBinaryHeaderSize+stateDomainChangeBinaryAccessorV3HeaderExtra {
		return stateDomainChangeBinaryAccessorV3Layout{}, io.ErrUnexpectedEOF
	}
	var raw [8]byte
	if _, err := r.ReadAt(raw[:], stateDomainChangeBinaryHeaderSize); err != nil {
		return stateDomainChangeBinaryAccessorV3Layout{}, err
	}
	groupCount := binary.BigEndian.Uint64(raw[:])
	if groupCount > header.count {
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: state-domain-change accessor v4 group count %d exceeds record count %d", groupCount, header.count)
	}
	exactOffset := uint64(stateDomainChangeBinaryHeaderSize + stateDomainChangeBinaryAccessorV3HeaderExtra)
	if header.count > (math.MaxUint64-exactOffset)/stateDomainChangeBinaryAccessorV3ExactEntrySize {
		return stateDomainChangeBinaryAccessorV3Layout{}, errors.New("snapshots: state-domain-change accessor v4 exact table overflows size")
	}
	groupOffsetsStart := exactOffset + header.count*stateDomainChangeBinaryAccessorV3ExactEntrySize
	if groupCount > (math.MaxUint64-groupOffsetsStart)/8 {
		return stateDomainChangeBinaryAccessorV3Layout{}, errors.New("snapshots: state-domain-change accessor v4 group directory overflows size")
	}
	groupPayloadStart := groupOffsetsStart + groupCount*8
	if groupPayloadStart > fileSize {
		return stateDomainChangeBinaryAccessorV3Layout{}, io.ErrUnexpectedEOF
	}
	return stateDomainChangeBinaryAccessorV3Layout{groupCount: groupCount, exactOffset: exactOffset, groupOffsetsStart: groupOffsetsStart, groupPayloadStart: groupPayloadStart}, nil
}

func readStateDomainChangeBinaryAccessorV4GroupMetaAt(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, index, fileSize uint64) (stateDomainChangeBinaryAccessorV4GroupMeta, error) {
	if index >= layout.groupCount {
		return stateDomainChangeBinaryAccessorV4GroupMeta{}, io.EOF
	}
	offset, err := readStateDomainChangeBinaryAccessorV3GroupOffsetAt(r, layout, index)
	if err != nil {
		return stateDomainChangeBinaryAccessorV4GroupMeta{}, err
	}
	next := fileSize
	if index+1 < layout.groupCount {
		next, err = readStateDomainChangeBinaryAccessorV3GroupOffsetAt(r, layout, index+1)
		if err != nil {
			return stateDomainChangeBinaryAccessorV4GroupMeta{}, err
		}
	}
	const fixedSize = stateDomainChangeBinaryAccessorV3GroupKeySize + 8
	if offset < layout.groupPayloadStart || offset >= next || next > fileSize || next-offset < fixedSize {
		return stateDomainChangeBinaryAccessorV4GroupMeta{}, io.ErrUnexpectedEOF
	}
	var fixed [fixedSize]byte
	if _, err := r.ReadAt(fixed[:], int64(offset)); err != nil {
		return stateDomainChangeBinaryAccessorV4GroupMeta{}, err
	}
	count := binary.BigEndian.Uint64(fixed[stateDomainChangeBinaryAccessorV3GroupKeySize:])
	if count == 0 || count > (next-offset-fixedSize)/stateDomainChangeBinaryAccessorV4GroupEntrySize {
		return stateDomainChangeBinaryAccessorV4GroupMeta{}, io.ErrUnexpectedEOF
	}
	if want := uint64(fixedSize) + count*stateDomainChangeBinaryAccessorV4GroupEntrySize; want != next-offset {
		return stateDomainChangeBinaryAccessorV4GroupMeta{}, fmt.Errorf("snapshots: state-domain-change accessor v4 group %d has %d trailing bytes", index, next-offset-want)
	}
	var meta stateDomainChangeBinaryAccessorV4GroupMeta
	copy(meta.key[:], fixed[:stateDomainChangeBinaryAccessorV3GroupKeySize])
	meta.recordsStart = offset + fixedSize
	meta.count = count
	meta.next = next
	return meta, nil
}

func readStateDomainChangeBinaryAccessorV4GroupRecordAt(r io.ReaderAt, meta stateDomainChangeBinaryAccessorV4GroupMeta, index uint64) (stateDomainChangeBinaryAccessorV4GroupIndexEntry, error) {
	if index >= meta.count || index > (math.MaxInt64-meta.recordsStart)/stateDomainChangeBinaryAccessorV4GroupEntrySize {
		return stateDomainChangeBinaryAccessorV4GroupIndexEntry{}, fmt.Errorf("snapshots: state-domain-change accessor v4 group record index %d outside count %d", index, meta.count)
	}
	var raw [stateDomainChangeBinaryAccessorV4GroupEntrySize]byte
	if _, err := r.ReadAt(raw[:], int64(meta.recordsStart+index*stateDomainChangeBinaryAccessorV4GroupEntrySize)); err != nil {
		return stateDomainChangeBinaryAccessorV4GroupIndexEntry{}, err
	}
	return stateDomainChangeBinaryAccessorV4GroupIndexEntry{prefix: binary.BigEndian.Uint32(raw[0:4]), offset: binary.BigEndian.Uint64(raw[4:12]), recordIndex: binary.BigEndian.Uint32(raw[12:])}, nil
}

func stateDomainChangeBinaryAccessorV4GroupLowerBound(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, fileSize uint64, target [stateDomainChangeBinaryAccessorV3GroupKeySize]byte) (uint64, bool, error) {
	low, high := uint64(0), layout.groupCount
	for low < high {
		mid := low + (high-low)/2
		group, err := readStateDomainChangeBinaryAccessorV4GroupMetaAt(r, layout, mid, fileSize)
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

func iterateStateDomainChangeBinarySegmentByAccessorV4Key(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader, lookupKey []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	layout, err := stateDomainChangeBinaryAccessorV4LayoutAt(accessor, accessorSize, header)
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
		change, _, err := readStateDomainChangeBinaryRecordAtBounded(segment, entry.offset, segmentSize)
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

func iterateStateDomainChangeBinarySegmentByAccessorV4Prefix(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader, lookupPrefix []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if len(lookupPrefix) < 1+stateDomainChangeBinaryAccessorV3GroupKeySize || rawdb.StateFlatDomain(lookupPrefix[0]) != rawdb.StateFlatDomainKVLatest {
		return errors.New("snapshots: invalid state-domain-change accessor v4 prefix lookup key")
	}
	layout, err := stateDomainChangeBinaryAccessorV4LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	var target [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	copy(target[:], lookupPrefix[1:1+stateDomainChangeBinaryAccessorV3GroupKeySize])
	groupIndex, ok, err := stateDomainChangeBinaryAccessorV4GroupLowerBound(accessor, layout, accessorSize, target)
	if err != nil || !ok {
		return err
	}
	group, err := readStateDomainChangeBinaryAccessorV4GroupMetaAt(accessor, layout, groupIndex, accessorSize)
	if err != nil {
		return err
	}
	if !bytes.Equal(group.key[:], target[:]) {
		return nil
	}
	wantPrefix := stateDomainChangeBinaryAccessorV4LogicalPrefix(lookupPrefix[1+stateDomainChangeBinaryAccessorV3GroupKeySize:])
	low, high := uint64(0), group.count
	for low < high {
		mid := low + (high-low)/2
		record, err := readStateDomainChangeBinaryAccessorV4GroupRecordAt(accessor, group, mid)
		if err != nil {
			return err
		}
		if record.prefix < wantPrefix {
			low = mid + 1
		} else {
			high = mid
		}
	}
	for i := low; i < group.count; i++ {
		record, err := readStateDomainChangeBinaryAccessorV4GroupRecordAt(accessor, group, i)
		if err != nil {
			return err
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBounded(segment, record.offset, segmentSize)
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

func readStateDomainChangeBinaryAccessorV4Debug(dir string, ref SegmentRef, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader) ([]stateDomainChangeBinaryAccessorEntry, error) {
	layout, err := stateDomainChangeBinaryAccessorV4LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return nil, err
	}
	segmentRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: ref.FromTxNum, ToTxNum: ref.ToTxNum, Path: ref.Path[:len(ref.Path)-len(filepath.Ext(ref.Path))] + ".seg"}
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
		change, _, err := readStateDomainChangeBinaryRecordAtBounded(segment, exact.offset, segmentSize)
		if err != nil {
			return nil, err
		}
		if got := stateDomainChangeBinaryAccessorV3Hash(stateDomainChangeBinaryAccessorKey(change)); !bytes.Equal(got[:], exact.hash[:]) {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor v4 hash mismatch at offset %d", exact.offset)
		}
		entries = append(entries, stateDomainChangeBinaryAccessorEntry{key: stateDomainChangeBinaryAccessorKey(change), txNum: change.TxNum, seq: change.Seq, offset: exact.offset, recordIndex: uint64(exact.recordIndex)})
	}
	sort.Slice(entries, func(i, j int) bool { return compareStateDomainChangeBinaryAccessorEntry(entries[i], entries[j]) < 0 })
	return entries, nil
}

func checkStateDomainChangeBinaryAccessorV4(accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader) error {
	layout, err := stateDomainChangeBinaryAccessorV4LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	var previous stateDomainChangeBinaryAccessorV3ExactEntry
	for i := uint64(0); i < header.count; i++ {
		entry, err := readStateDomainChangeBinaryAccessorV3ExactEntryAt(accessor, layout, i)
		if err != nil {
			return err
		}
		if entry.offset < stateDomainChangeBinaryHeaderSize || uint64(entry.recordIndex) >= header.count {
			return fmt.Errorf("snapshots: state-domain-change accessor v4 exact entry %d is invalid", i)
		}
		if i > 0 && (bytes.Compare(previous.hash[:], entry.hash[:]) > 0 || (bytes.Equal(previous.hash[:], entry.hash[:]) && (previous.offset > entry.offset || (previous.offset == entry.offset && previous.recordIndex >= entry.recordIndex)))) {
			return fmt.Errorf("snapshots: state-domain-change accessor v4 exact entries are not strictly sorted at %d", i)
		}
		previous = entry
	}
	var previousGroup [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	for i := uint64(0); i < layout.groupCount; i++ {
		group, err := readStateDomainChangeBinaryAccessorV4GroupMetaAt(accessor, layout, i, accessorSize)
		if err != nil {
			return err
		}
		if i > 0 && bytes.Compare(previousGroup[:], group.key[:]) >= 0 {
			return fmt.Errorf("snapshots: state-domain-change accessor v4 groups are not strictly sorted at %d", i)
		}
		var previousPrefix uint32
		for j := uint64(0); j < group.count; j++ {
			record, err := readStateDomainChangeBinaryAccessorV4GroupRecordAt(accessor, group, j)
			if err != nil {
				return err
			}
			if record.offset < stateDomainChangeBinaryHeaderSize || uint64(record.recordIndex) >= header.count || (j > 0 && previousPrefix > record.prefix) {
				return fmt.Errorf("snapshots: state-domain-change accessor v4 group %d record %d is invalid", i, j)
			}
			previousPrefix = record.prefix
		}
		previousGroup = group.key
	}
	return nil
}

func verifyStateDomainChangeBinaryAccessorV4Coverage(historyRef, accessorRef SegmentRef, segment io.ReaderAt, segmentSize, recordCount uint64, index io.ReaderAt, indexCount uint64, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader) error {
	if err := checkStateDomainChangeBinaryAccessorV4(accessor, accessorSize, header); err != nil {
		return err
	}
	if header.count != recordCount {
		return fmt.Errorf("snapshots: state-domain-change accessor %q count %d, want segment count %d", accessorRef.Path, header.count, recordCount)
	}
	layout, err := stateDomainChangeBinaryAccessorV4LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	words := (recordCount + 63) / 64
	exactSeen, groupSeen := make([]uint64, words), make([]uint64, words)
	mark := func(seen []uint64, recordIndex uint32, kind string) error {
		n := uint64(recordIndex)
		if n >= recordCount || seen[n/64]&(uint64(1)<<(n%64)) != 0 {
			return fmt.Errorf("snapshots: state-domain-change accessor %q invalid %s record %d", accessorRef.Path, kind, n)
		}
		seen[n/64] |= uint64(1) << (n % 64)
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
		change, offset, err := readStateDomainChangeBinaryRecordAtIndex(segment, segmentSize, index, indexCount, uint64(exact.recordIndex))
		if err != nil {
			return err
		}
		hash := stateDomainChangeBinaryAccessorV3Hash(stateDomainChangeBinaryAccessorKey(change))
		if exact.offset != offset || !bytes.Equal(hash[:], exact.hash[:]) {
			return fmt.Errorf("snapshots: state-domain-change accessor %q exact entry %d does not match segment", accessorRef.Path, i)
		}
	}
	for i := uint64(0); i < layout.groupCount; i++ {
		group, err := readStateDomainChangeBinaryAccessorV4GroupMetaAt(accessor, layout, i, accessorSize)
		if err != nil {
			return err
		}
		var previousKey []byte
		var previousOffset uint64
		for j := uint64(0); j < group.count; j++ {
			record, err := readStateDomainChangeBinaryAccessorV4GroupRecordAt(accessor, group, j)
			if err != nil {
				return err
			}
			if err := mark(groupSeen, record.recordIndex, "group"); err != nil {
				return err
			}
			change, offset, err := readStateDomainChangeBinaryRecordAtIndex(segment, segmentSize, index, indexCount, uint64(record.recordIndex))
			if err != nil {
				return err
			}
			groupKey, ok := stateDomainChangeBinaryAccessorV3GroupKey(change)
			accessorKey := stateDomainChangeBinaryAccessorKey(change)
			if !ok || !bytes.Equal(group.key[:], groupKey[:]) || record.offset != offset || record.prefix != stateDomainChangeBinaryAccessorV4LogicalPrefix(change.Key) || (j > 0 && (bytes.Compare(previousKey, accessorKey) > 0 || (bytes.Equal(previousKey, accessorKey) && previousOffset >= record.offset))) {
				return fmt.Errorf("snapshots: state-domain-change accessor %q group %d record %d does not match segment", accessorRef.Path, i, j)
			}
			previousKey = append(previousKey[:0], accessorKey...)
			previousOffset = record.offset
		}
	}
	return iterateStateDomainChangeBinaryRecords(segment, segmentSize, func(recordIndex uint64, change *rawdb.StateDomainChange) error {
		if change.FlatDomain == rawdb.StateFlatDomainKVLatest && groupSeen[recordIndex/64]&(uint64(1)<<(recordIndex%64)) == 0 {
			return fmt.Errorf("snapshots: state-domain-change accessor %q is missing KV-latest group record %d", accessorRef.Path, recordIndex)
		}
		return nil
	})
}
