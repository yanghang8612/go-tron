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

	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	// V5 keeps the accessor as a candidate index: every hit is verified against
	// the immutable history record before it is returned. A 64-bit fingerprint
	// therefore cannot create a false result; collisions only add candidates.
	stateDomainChangeBinaryAccessorV5FingerprintSize = 8
	// Immutable history files are capped at 256 TiB of logical bytes. Production
	// segments are many orders of magnitude smaller, and rejecting an oversized
	// build is safer than silently truncating a random-read position.
	stateDomainChangeBinaryAccessorV5OffsetSize     = 6
	stateDomainChangeBinaryAccessorV5ExactEntrySize = stateDomainChangeBinaryAccessorV5FingerprintSize + stateDomainChangeBinaryAccessorV5OffsetSize + 4
	stateDomainChangeBinaryAccessorV5GroupEntrySize = 4 + stateDomainChangeBinaryAccessorV5OffsetSize + 4
	stateDomainChangeBinaryAccessorV5MaxOffset      = uint64(1)<<48 - 1
)

type stateDomainChangeBinaryAccessorV5ExactEntry struct {
	fingerprint [stateDomainChangeBinaryAccessorV5FingerprintSize]byte
	offset      uint64
	recordIndex uint32
}

type stateDomainChangeBinaryAccessorV5GroupIndexEntry struct {
	prefix      uint32
	offset      uint64
	recordIndex uint32
}

func stateDomainChangeBinaryAccessorV5Fingerprint(key []byte) [stateDomainChangeBinaryAccessorV5FingerprintSize]byte {
	sum := sha256.Sum256(key)
	var out [stateDomainChangeBinaryAccessorV5FingerprintSize]byte
	copy(out[:], sum[:len(out)])
	return out
}

func putStateDomainChangeBinaryAccessorV5Offset(dst []byte, offset uint64) error {
	if len(dst) < stateDomainChangeBinaryAccessorV5OffsetSize {
		return io.ErrShortBuffer
	}
	if offset > stateDomainChangeBinaryAccessorV5MaxOffset {
		return fmt.Errorf("snapshots: state-domain-change accessor v5 offset %d exceeds 48 bits", offset)
	}
	dst[0] = byte(offset >> 40)
	dst[1] = byte(offset >> 32)
	dst[2] = byte(offset >> 24)
	dst[3] = byte(offset >> 16)
	dst[4] = byte(offset >> 8)
	dst[5] = byte(offset)
	return nil
}

func stateDomainChangeBinaryAccessorV5Offset(src []byte) (uint64, error) {
	if len(src) < stateDomainChangeBinaryAccessorV5OffsetSize {
		return 0, io.ErrUnexpectedEOF
	}
	return uint64(src[0])<<40 |
		uint64(src[1])<<32 |
		uint64(src[2])<<24 |
		uint64(src[3])<<16 |
		uint64(src[4])<<8 |
		uint64(src[5]), nil
}

func encodeStateDomainChangeBinaryAccessorV5(fromTxNum, toTxNum uint64, entries []stateDomainChangeBinaryAccessorEntry) ([]byte, error) {
	if toTxNum < fromTxNum {
		return nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	exact := make([]stateDomainChangeBinaryAccessorV5ExactEntry, 0, len(entries))
	groups := make(map[[stateDomainChangeBinaryAccessorV3GroupKeySize]byte][]stateDomainChangeBinaryAccessorV4GroupRecord)
	for i, entry := range entries {
		if entry.recordIndex > math.MaxUint32 {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor v5 record index %d exceeds uint32", entry.recordIndex)
		}
		if entry.offset > stateDomainChangeBinaryAccessorV5MaxOffset {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor v5 offset %d exceeds 48 bits", entry.offset)
		}
		if err := validateStateDomainChangeBinaryAccessorEntry(SegmentRef{Path: "v5 accessor", FromTxNum: fromTxNum, ToTxNum: toTxNum}, entry, uint64(i)); err != nil {
			return nil, err
		}
		exact = append(exact, stateDomainChangeBinaryAccessorV5ExactEntry{
			fingerprint: stateDomainChangeBinaryAccessorV5Fingerprint(entry.key),
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
		if cmp := bytes.Compare(exact[i].fingerprint[:], exact[j].fingerprint[:]); cmp != 0 {
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
			if groups[key][i].offset != groups[key][j].offset {
				return groups[key][i].offset < groups[key][j].offset
			}
			return groups[key][i].recordIndex < groups[key][j].recordIndex
		})
	}

	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&buf, stateDomainChangeBinaryAccessorMagic, fromTxNum, toTxNum, uint64(len(exact)), stateDomainChangeBinaryVersionV5)
	writeUint64(&buf, uint64(len(keys)))
	var exactRaw [stateDomainChangeBinaryAccessorV5ExactEntrySize]byte
	for _, entry := range exact {
		copy(exactRaw[:stateDomainChangeBinaryAccessorV5FingerprintSize], entry.fingerprint[:])
		if err := putStateDomainChangeBinaryAccessorV5Offset(exactRaw[stateDomainChangeBinaryAccessorV5FingerprintSize:], entry.offset); err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(exactRaw[stateDomainChangeBinaryAccessorV5FingerprintSize+stateDomainChangeBinaryAccessorV5OffsetSize:], entry.recordIndex)
		buf.Write(exactRaw[:])
	}
	payloadStart := uint64(buf.Len()) + uint64(len(keys))*8
	var payload bytes.Buffer
	var groupRaw [stateDomainChangeBinaryAccessorV5GroupEntrySize]byte
	for _, key := range keys {
		writeUint64(&buf, payloadStart+uint64(payload.Len()))
		payload.Write(key[:])
		writeUint64(&payload, uint64(len(groups[key])))
		for _, record := range groups[key] {
			binary.BigEndian.PutUint32(groupRaw[:4], stateDomainChangeBinaryAccessorV4LogicalPrefix(record.logicalKey))
			if err := putStateDomainChangeBinaryAccessorV5Offset(groupRaw[4:], record.offset); err != nil {
				return nil, err
			}
			binary.BigEndian.PutUint32(groupRaw[4+stateDomainChangeBinaryAccessorV5OffsetSize:], record.recordIndex)
			payload.Write(groupRaw[:])
		}
	}
	buf.Write(payload.Bytes())
	return buf.Bytes(), nil
}

func stateDomainChangeBinaryAccessorV5LayoutAt(r io.ReaderAt, fileSize uint64, header stateDomainChangeBinaryHeader) (stateDomainChangeBinaryAccessorV3Layout, error) {
	if header.version != stateDomainChangeBinaryVersionV5 {
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: accessor v5 layout requested for version %d", header.version)
	}
	if header.count > math.MaxUint32 {
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: state-domain-change accessor v5 count %d exceeds uint32 record index", header.count)
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
		return stateDomainChangeBinaryAccessorV3Layout{}, fmt.Errorf("snapshots: state-domain-change accessor v5 group count %d exceeds record count %d", groupCount, header.count)
	}
	exactOffset := uint64(stateDomainChangeBinaryHeaderSize + stateDomainChangeBinaryAccessorV3HeaderExtra)
	if header.count > (math.MaxUint64-exactOffset)/stateDomainChangeBinaryAccessorV5ExactEntrySize {
		return stateDomainChangeBinaryAccessorV3Layout{}, errors.New("snapshots: state-domain-change accessor v5 exact table overflows size")
	}
	groupOffsetsStart := exactOffset + header.count*stateDomainChangeBinaryAccessorV5ExactEntrySize
	if groupCount > (math.MaxUint64-groupOffsetsStart)/8 {
		return stateDomainChangeBinaryAccessorV3Layout{}, errors.New("snapshots: state-domain-change accessor v5 group directory overflows size")
	}
	groupPayloadStart := groupOffsetsStart + groupCount*8
	if groupPayloadStart > fileSize {
		return stateDomainChangeBinaryAccessorV3Layout{}, io.ErrUnexpectedEOF
	}
	return stateDomainChangeBinaryAccessorV3Layout{groupCount: groupCount, exactOffset: exactOffset, groupOffsetsStart: groupOffsetsStart, groupPayloadStart: groupPayloadStart}, nil
}

func readStateDomainChangeBinaryAccessorV5ExactEntryAt(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, index uint64) (stateDomainChangeBinaryAccessorV5ExactEntry, error) {
	if index > (math.MaxInt64-layout.exactOffset)/stateDomainChangeBinaryAccessorV5ExactEntrySize {
		return stateDomainChangeBinaryAccessorV5ExactEntry{}, fmt.Errorf("snapshots: state-domain-change accessor v5 exact index too large: %d", index)
	}
	var raw [stateDomainChangeBinaryAccessorV5ExactEntrySize]byte
	if _, err := r.ReadAt(raw[:], int64(layout.exactOffset+index*stateDomainChangeBinaryAccessorV5ExactEntrySize)); err != nil {
		return stateDomainChangeBinaryAccessorV5ExactEntry{}, err
	}
	var entry stateDomainChangeBinaryAccessorV5ExactEntry
	copy(entry.fingerprint[:], raw[:stateDomainChangeBinaryAccessorV5FingerprintSize])
	offset, err := stateDomainChangeBinaryAccessorV5Offset(raw[stateDomainChangeBinaryAccessorV5FingerprintSize:])
	if err != nil {
		return stateDomainChangeBinaryAccessorV5ExactEntry{}, err
	}
	entry.offset = offset
	entry.recordIndex = binary.BigEndian.Uint32(raw[stateDomainChangeBinaryAccessorV5FingerprintSize+stateDomainChangeBinaryAccessorV5OffsetSize:])
	return entry, nil
}

func readStateDomainChangeBinaryAccessorV5GroupMetaAt(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, index, fileSize uint64) (stateDomainChangeBinaryAccessorV4GroupMeta, error) {
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
	if count == 0 || count > (next-offset-fixedSize)/stateDomainChangeBinaryAccessorV5GroupEntrySize {
		return stateDomainChangeBinaryAccessorV4GroupMeta{}, io.ErrUnexpectedEOF
	}
	if want := uint64(fixedSize) + count*stateDomainChangeBinaryAccessorV5GroupEntrySize; want != next-offset {
		return stateDomainChangeBinaryAccessorV4GroupMeta{}, fmt.Errorf("snapshots: state-domain-change accessor v5 group %d has %d trailing bytes", index, next-offset-want)
	}
	var meta stateDomainChangeBinaryAccessorV4GroupMeta
	copy(meta.key[:], fixed[:stateDomainChangeBinaryAccessorV3GroupKeySize])
	meta.recordsStart = offset + fixedSize
	meta.count = count
	meta.next = next
	return meta, nil
}

func readStateDomainChangeBinaryAccessorV5GroupRecordAt(r io.ReaderAt, meta stateDomainChangeBinaryAccessorV4GroupMeta, index uint64) (stateDomainChangeBinaryAccessorV5GroupIndexEntry, error) {
	if index >= meta.count || index > (math.MaxInt64-meta.recordsStart)/stateDomainChangeBinaryAccessorV5GroupEntrySize {
		return stateDomainChangeBinaryAccessorV5GroupIndexEntry{}, fmt.Errorf("snapshots: state-domain-change accessor v5 group record index %d outside count %d", index, meta.count)
	}
	var raw [stateDomainChangeBinaryAccessorV5GroupEntrySize]byte
	if _, err := r.ReadAt(raw[:], int64(meta.recordsStart+index*stateDomainChangeBinaryAccessorV5GroupEntrySize)); err != nil {
		return stateDomainChangeBinaryAccessorV5GroupIndexEntry{}, err
	}
	offset, err := stateDomainChangeBinaryAccessorV5Offset(raw[4:])
	if err != nil {
		return stateDomainChangeBinaryAccessorV5GroupIndexEntry{}, err
	}
	return stateDomainChangeBinaryAccessorV5GroupIndexEntry{
		prefix:      binary.BigEndian.Uint32(raw[:4]),
		offset:      offset,
		recordIndex: binary.BigEndian.Uint32(raw[4+stateDomainChangeBinaryAccessorV5OffsetSize:]),
	}, nil
}

func stateDomainChangeBinaryAccessorV5ExactLowerBound(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, count uint64, target [stateDomainChangeBinaryAccessorV5FingerprintSize]byte) (uint64, bool, error) {
	low, high := uint64(0), count
	for low < high {
		mid := low + (high-low)/2
		entry, err := readStateDomainChangeBinaryAccessorV5ExactEntryAt(r, layout, mid)
		if err != nil {
			return 0, false, err
		}
		if bytes.Compare(entry.fingerprint[:], target[:]) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, low < count, nil
}

func stateDomainChangeBinaryAccessorV5ExactUpperBound(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, count uint64, target [stateDomainChangeBinaryAccessorV5FingerprintSize]byte) (uint64, error) {
	low, high := uint64(0), count
	for low < high {
		mid := low + (high-low)/2
		entry, err := readStateDomainChangeBinaryAccessorV5ExactEntryAt(r, layout, mid)
		if err != nil {
			return 0, err
		}
		if bytes.Compare(entry.fingerprint[:], target[:]) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, nil
}

func stateDomainChangeBinaryAccessorV5TxLowerBound(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, low, high, fromTxNum uint64) (uint64, error) {
	segmentHeader, err := stateDomainChangeBinaryHeaderForReader(segment)
	if err != nil {
		return 0, err
	}
	for low < high {
		mid := low + (high-low)/2
		entry, err := readStateDomainChangeBinaryAccessorV5ExactEntryAt(accessor, layout, mid)
		if err != nil {
			return 0, err
		}
		// The lower bound only needs TxNum. Deferring StateTxRange hydration
		// avoids nesting a second binary search into every search probe.
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

func stateDomainChangeBinaryAccessorV5GroupLowerBound(r io.ReaderAt, layout stateDomainChangeBinaryAccessorV3Layout, fileSize uint64, target [stateDomainChangeBinaryAccessorV3GroupKeySize]byte) (uint64, bool, error) {
	low, high := uint64(0), layout.groupCount
	for low < high {
		mid := low + (high-low)/2
		group, err := readStateDomainChangeBinaryAccessorV5GroupMetaAt(r, layout, mid, fileSize)
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

func iterateStateDomainChangeBinarySegmentByAccessorV5Key(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader, lookupKey []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	layout, err := stateDomainChangeBinaryAccessorV5LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	target := stateDomainChangeBinaryAccessorV5Fingerprint(lookupKey)
	start, ok, err := stateDomainChangeBinaryAccessorV5ExactLowerBound(accessor, layout, header.count, target)
	if err != nil || !ok {
		return err
	}
	first, err := readStateDomainChangeBinaryAccessorV5ExactEntryAt(accessor, layout, start)
	if err != nil {
		return err
	}
	if !bytes.Equal(first.fingerprint[:], target[:]) {
		return nil
	}
	end, err := stateDomainChangeBinaryAccessorV5ExactUpperBound(accessor, layout, header.count, target)
	if err != nil {
		return err
	}
	if fromTxNum > header.fromTxNum {
		start, err = stateDomainChangeBinaryAccessorV5TxLowerBound(segment, segmentSize, accessor, layout, start, end, fromTxNum)
		if err != nil {
			return err
		}
	}
	for i := start; i < end; i++ {
		entry, err := readStateDomainChangeBinaryAccessorV5ExactEntryAt(accessor, layout, i)
		if err != nil {
			return err
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, entry.offset, segmentSize, uint64(entry.recordIndex))
		if err != nil {
			return err
		}
		// The full-key check makes fingerprint collisions fail closed.
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

func iterateStateDomainChangeBinarySegmentByAccessorV5Prefix(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader, lookupPrefix []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if len(lookupPrefix) < 1+stateDomainChangeBinaryAccessorV3GroupKeySize || rawdb.StateFlatDomain(lookupPrefix[0]) != rawdb.StateFlatDomainKVLatest {
		return errors.New("snapshots: invalid state-domain-change accessor v5 prefix lookup key")
	}
	layout, err := stateDomainChangeBinaryAccessorV5LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	var target [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	copy(target[:], lookupPrefix[1:1+stateDomainChangeBinaryAccessorV3GroupKeySize])
	groupIndex, ok, err := stateDomainChangeBinaryAccessorV5GroupLowerBound(accessor, layout, accessorSize, target)
	if err != nil || !ok {
		return err
	}
	group, err := readStateDomainChangeBinaryAccessorV5GroupMetaAt(accessor, layout, groupIndex, accessorSize)
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
		record, err := readStateDomainChangeBinaryAccessorV5GroupRecordAt(accessor, group, mid)
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
		record, err := readStateDomainChangeBinaryAccessorV5GroupRecordAt(accessor, group, i)
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

func readStateDomainChangeBinaryAccessorV5Debug(dir string, ref SegmentRef, accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader) ([]stateDomainChangeBinaryAccessorEntry, error) {
	layout, err := stateDomainChangeBinaryAccessorV5LayoutAt(accessor, accessorSize, header)
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
		exact, err := readStateDomainChangeBinaryAccessorV5ExactEntryAt(accessor, layout, i)
		if err != nil {
			return nil, err
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, exact.offset, segmentSize, uint64(exact.recordIndex))
		if err != nil {
			return nil, err
		}
		if got := stateDomainChangeBinaryAccessorV5Fingerprint(stateDomainChangeBinaryAccessorKey(change)); !bytes.Equal(got[:], exact.fingerprint[:]) {
			return nil, fmt.Errorf("snapshots: state-domain-change accessor v5 fingerprint mismatch at offset %d", exact.offset)
		}
		entries = append(entries, stateDomainChangeBinaryAccessorEntry{key: stateDomainChangeBinaryAccessorKey(change), txNum: change.TxNum, seq: change.Seq, offset: exact.offset, recordIndex: uint64(exact.recordIndex)})
	}
	sort.Slice(entries, func(i, j int) bool { return compareStateDomainChangeBinaryAccessorEntry(entries[i], entries[j]) < 0 })
	return entries, nil
}

func checkStateDomainChangeBinaryAccessorV5(accessor io.ReaderAt, accessorSize uint64, header stateDomainChangeBinaryHeader) error {
	layout, err := stateDomainChangeBinaryAccessorV5LayoutAt(accessor, accessorSize, header)
	if err != nil {
		return err
	}
	var previous stateDomainChangeBinaryAccessorV5ExactEntry
	for i := uint64(0); i < header.count; i++ {
		entry, err := readStateDomainChangeBinaryAccessorV5ExactEntryAt(accessor, layout, i)
		if err != nil {
			return err
		}
		if entry.offset < stateDomainChangeBinaryHeaderSize || uint64(entry.recordIndex) >= header.count {
			return fmt.Errorf("snapshots: state-domain-change accessor v5 exact entry %d is invalid", i)
		}
		if i > 0 && (bytes.Compare(previous.fingerprint[:], entry.fingerprint[:]) > 0 || (bytes.Equal(previous.fingerprint[:], entry.fingerprint[:]) && (previous.offset > entry.offset || (previous.offset == entry.offset && previous.recordIndex >= entry.recordIndex)))) {
			return fmt.Errorf("snapshots: state-domain-change accessor v5 exact entries are not strictly sorted at %d", i)
		}
		previous = entry
	}
	var previousGroup [stateDomainChangeBinaryAccessorV3GroupKeySize]byte
	for i := uint64(0); i < layout.groupCount; i++ {
		group, err := readStateDomainChangeBinaryAccessorV5GroupMetaAt(accessor, layout, i, accessorSize)
		if err != nil {
			return err
		}
		if i > 0 && bytes.Compare(previousGroup[:], group.key[:]) >= 0 {
			return fmt.Errorf("snapshots: state-domain-change accessor v5 groups are not strictly sorted at %d", i)
		}
		var previousPrefix uint32
		for j := uint64(0); j < group.count; j++ {
			record, err := readStateDomainChangeBinaryAccessorV5GroupRecordAt(accessor, group, j)
			if err != nil {
				return err
			}
			if record.offset < stateDomainChangeBinaryHeaderSize || uint64(record.recordIndex) >= header.count || (j > 0 && previousPrefix > record.prefix) {
				return fmt.Errorf("snapshots: state-domain-change accessor v5 group %d record %d is invalid", i, j)
			}
			previousPrefix = record.prefix
		}
		previousGroup = group.key
	}
	return nil
}
