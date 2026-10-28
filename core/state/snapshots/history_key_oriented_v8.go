package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"sort"
)

// V8 changes only accessors; history and inverted-index versions are independent.
const stateDomainChangeBinaryAccessorVersionV8 = uint32(8)
const historyAccessorSparseStride = uint64(32)
const historyAccessorDirectoryReadLimit = uint64(64 << 10)

func isStateDomainChangeFramedAccessor(version uint32) bool {
	return version == stateDomainChangeBinaryVersionV7 || isHistoryInlineAccessor(version)
}
func isStateDomainChangeKeyAccessor(version uint32) bool {
	return version == stateDomainChangeBinaryVersionV6 || isStateDomainChangeFramedAccessor(version)
}
func stateDomainChangeAccessorHeaderCRC(raw []byte, version uint32) uint32 {
	sum := crc32.ChecksumIEEE(raw[:112])
	if isHistoryInlineAccessor(version) {
		sum = crc32.Update(sum, crc32.IEEETable, raw[116:120])
	}
	return sum
}

func encodeStateDomainChangeBinaryAccessorV8KeyBlock(keys []stateDomainChangeBinaryAccessorV6Key, from uint64) ([]byte, error) {
	var raw, previous []byte
	for _, key := range keys {
		prefix := commonPrefixLength(previous, key.key)
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = binary.AppendUvarint(raw, uint64(len(key.key)-prefix))
		raw = append(raw, key.key[prefix:]...)
		count := key.postingCount
		if count == 0 {
			count = uint32(len(key.postings))
		}
		if count == 1 {
			p := key.inlinePosting
			if len(key.postings) == 1 {
				p = key.postings[0]
			}
			if p.txNum < from || p.offset > stateDomainChangeBinaryAccessorV5MaxOffset {
				return nil, errors.New("snapshots: invalid V8 singleton")
			}
			raw = binary.AppendUvarint(raw, 0)
			raw = binary.AppendUvarint(raw, p.txNum-from)
			raw = binary.AppendUvarint(raw, p.offset)
			raw = binary.AppendUvarint(raw, uint64(p.recordIndex))
		} else {
			if count < 2 {
				return nil, errors.New("snapshots: empty V8 posting list")
			}
			raw = binary.AppendUvarint(raw, uint64(count))
			raw = binary.AppendUvarint(raw, key.postingOff)
		}
		previous = key.key
	}
	return raw, nil
}

func readStateDomainChangeAccessorV8Meta(br io.ByteReader, h stateDomainChangeBinaryAccessorV6Header) (stateDomainChangeBinaryAccessorV6Record, error) {
	var rec stateDomainChangeBinaryAccessorV6Record
	token, err := binary.ReadUvarint(br)
	if err != nil {
		return rec, err
	}
	if token == 0 {
		a, e := binary.ReadUvarint(br)
		if e != nil {
			return rec, e
		}
		off, e := binary.ReadUvarint(br)
		if e != nil {
			return rec, e
		}
		idx, e := binary.ReadUvarint(br)
		if e != nil {
			return rec, e
		}
		if a > h.toTxNum-h.fromTxNum || off > stateDomainChangeBinaryAccessorV5MaxOffset || idx >= h.recordCount || idx > math.MaxUint32 {
			return rec, errors.New("snapshots: V8 singleton outside segment bounds")
		}
		rec.inline = true
		rec.postings = 1
		rec.inlinePosting = stateDomainChangeBinaryAccessorV6Posting{txNum: h.fromTxNum + a, offset: off, recordIndex: uint32(idx)}
	} else {
		if token < 2 || token > math.MaxUint32 {
			return rec, errors.New("snapshots: invalid V8 posting count")
		}
		off, e := binary.ReadUvarint(br)
		if e != nil {
			return rec, e
		}
		if off >= h.postingLen {
			return rec, errors.New("snapshots: V8 posting offset outside section")
		}
		rec.postings = uint32(token)
		rec.postingOff = off
	}
	return rec, nil
}

type historyAccessorSparseGroup struct {
	key                   []byte
	offset, length, index uint64
}

func encodeHistoryAccessorSparseDirectory(blocks []stateDomainChangeBinaryAccessorV6Block) []byte {
	if uint64(len(blocks)) <= historyAccessorSparseStride {
		return nil
	}
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, uint32(ceilDiv(uint64(len(blocks)), historyAccessorSparseStride)))
	off := uint64(stateDomainChangeBinaryAccessorV7HeaderSize) + uint64(len(blocks))*8
	for start := 0; start < len(blocks); start += int(historyAccessorSparseStride) {
		end := min(start+int(historyAccessorSparseStride), len(blocks))
		var length uint64
		for _, b := range blocks[start:end] {
			length += 2 + uint64(len(b.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
		}
		key := blocks[start].firstKey
		if len(key) == 0 || len(key) > math.MaxUint16 {
			return nil
		}
		raw = binary.BigEndian.AppendUint16(raw, uint16(len(key)))
		raw = append(raw, key...)
		raw = binary.BigEndian.AppendUint64(raw, off)
		raw = binary.BigEndian.AppendUint32(raw, uint32(length))
		off += length
		if uint64(len(raw)+4) > historyAccessorDirectoryReadLimit {
			return nil
		}
	}
	return binary.BigEndian.AppendUint32(raw, crc32.ChecksumIEEE(raw))
}
func readHistoryAccessorSparseDirectory(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header) ([]historyAccessorSparseGroup, error) {
	if h.sparseLen == 0 {
		return nil, nil
	}
	if !isHistoryInlineAccessor(h.version) || h.sparseLen < 8 || h.sparseLen > historyAccessorDirectoryReadLimit || h.sparseLen > h.blockDirLen {
		return nil, errors.New("snapshots: invalid V8 sparse directory length")
	}
	end := h.headerSize + h.blockDirLen - h.sparseLen
	raw := make([]byte, h.sparseLen)
	if _, err := r.ReadAt(raw, int64(end)); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(raw[len(raw)-4:]) != crc32.ChecksumIEEE(raw[:len(raw)-4]) {
		return nil, errors.New("snapshots: V8 sparse directory checksum mismatch")
	}
	count := uint64(binary.BigEndian.Uint32(raw))
	if count != ceilDiv(h.blockCount, historyAccessorSparseStride) || count > uint64(len(raw)-8)/15 {
		return nil, errors.New("snapshots: V8 sparse directory count mismatch")
	}
	pos := 4
	next := h.headerSize + h.blockCount*8
	groups := make([]historyAccessorSparseGroup, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(raw)-4-pos < 14 {
			return nil, io.ErrUnexpectedEOF
		}
		n := int(binary.BigEndian.Uint16(raw[pos:]))
		pos += 2
		if n == 0 || len(raw)-4-pos < n+12 {
			return nil, errors.New("snapshots: invalid V8 sparse key")
		}
		key := raw[pos : pos+n]
		pos += n
		off := binary.BigEndian.Uint64(raw[pos:])
		length := uint64(binary.BigEndian.Uint32(raw[pos+8:]))
		pos += 12
		if off != next || off > end || length == 0 || length > end-off || i > 0 && bytes.Compare(groups[i-1].key, key) >= 0 {
			return nil, errors.New("snapshots: invalid V8 sparse directory coverage/order")
		}
		groups = append(groups, historyAccessorSparseGroup{key, off, length, i * historyAccessorSparseStride})
		next = off + length
	}
	if pos != len(raw)-4 || next != end {
		return nil, errors.New("snapshots: V8 sparse directory trailing bytes or coverage gap")
	}
	return groups, nil
}
func decodeHistoryAccessorDirectoryEntry(raw []byte) (stateDomainChangeBinaryAccessorV6Block, error) {
	var block stateDomainChangeBinaryAccessorV6Block
	if len(raw) < 2+stateDomainChangeBinaryAccessorV6BlockTail {
		return block, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(raw))
	if n == 0 || len(raw) != 2+n+stateDomainChangeBinaryAccessorV6BlockTail {
		return block, errors.New("snapshots: invalid accessor directory entry length")
	}
	key := raw[2 : 2+n]
	tail := raw[2+n:]
	sum := crc32.Update(crc32.ChecksumIEEE(key), crc32.IEEETable, tail[:24])
	if sum != binary.BigEndian.Uint32(tail[24:]) {
		return block, errors.New("snapshots: accessor directory checksum mismatch")
	}
	// The entry owns an immutable group buffer; retain its key view instead of
	// allocating a separate copy for every entry in the selected group.
	block = stateDomainChangeBinaryAccessorV6Block{firstKey: key, dataOff: binary.BigEndian.Uint64(tail), dataLen: binary.BigEndian.Uint32(tail[8:]), rawLen: binary.BigEndian.Uint32(tail[12:]), checksum: binary.BigEndian.Uint32(tail[16:]), keyCount: binary.BigEndian.Uint32(tail[20:])}
	if block.keyCount == 0 || block.keyCount > stateDomainChangeBinaryAccessorV6BlockKeys {
		return block, errors.New("snapshots: invalid accessor directory key count")
	}
	return block, nil
}
func readHistoryAccessorDirectoryGroup(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, g historyAccessorSparseGroup) ([]stateDomainChangeBinaryAccessorV6Block, error) {
	if g.length > historyAccessorDirectoryReadLimit {
		return nil, errors.New("snapshots: accessor directory group exceeds read budget")
	}
	raw := make([]byte, g.length)
	if _, err := r.ReadAt(raw, int64(g.offset)); err != nil {
		return nil, err
	}
	count := min(historyAccessorSparseStride, h.blockCount-g.index)
	blocks := make([]stateDomainChangeBinaryAccessorV6Block, 0, count)
	pos := 0
	for i := uint64(0); i < count; i++ {
		if len(raw)-pos < 2 {
			return nil, io.ErrUnexpectedEOF
		}
		n := 2 + int(binary.BigEndian.Uint16(raw[pos:])) + stateDomainChangeBinaryAccessorV6BlockTail
		if n > len(raw)-pos {
			return nil, io.ErrUnexpectedEOF
		}
		b, e := decodeHistoryAccessorDirectoryEntry(raw[pos : pos+n])
		if e != nil {
			return nil, e
		}
		pos += n
		if i == 0 && !bytes.Equal(g.key, b.firstKey) || i > 0 && bytes.Compare(blocks[i-1].firstKey, b.firstKey) >= 0 {
			return nil, errors.New("snapshots: V8 directory group key mismatch/order")
		}
		blocks = append(blocks, b)
	}
	if pos != len(raw) {
		return nil, errors.New("snapshots: V8 directory group trailing bytes")
	}
	return blocks, nil
}

// This search belongs to one open descriptor and one batch. Reuse decoded
// fences as well as physical pages; a batch of neighboring dynamic-property
// keys otherwise decodes the same sparse directory and group for every key.
type historyAccessorKeySearch struct {
	r                         io.ReaderAt
	h                         stateDomainChangeBinaryAccessorV6Header
	smallLoaded, sparseLoaded bool
	small                     []stateDomainChangeBinaryAccessorV6Block
	groups                    []historyAccessorSparseGroup
	groupLoaded               bool
	groupIndex                uint64
	group                     []stateDomainChangeBinaryAccessorV6Block
}

func findHistoryAccessorKeyBlock(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, key []byte) (stateDomainChangeBinaryAccessorV6Block, uint64, bool, error) {
	return (&historyAccessorKeySearch{r: r, h: h}).find(key)
}

func (s *historyAccessorKeySearch) find(key []byte) (stateDomainChangeBinaryAccessorV6Block, uint64, bool, error) {
	r, h := s.r, s.h
	var zero stateDomainChangeBinaryAccessorV6Block
	if h.blockCount == 0 {
		return zero, 0, false, nil
	}
	if h.blockCount <= historyAccessorSparseStride && h.blockDirLen <= historyAccessorDirectoryReadLimit {
		if !s.smallLoaded {
			blocks, err := readSmallHistoryAccessorDirectory(r, h)
			if err != nil {
				return zero, 0, false, err
			}
			s.small, s.smallLoaded = blocks, true
		}
		blocks := s.small
		at := sort.Search(len(blocks), func(i int) bool { return bytes.Compare(blocks[i].firstKey, key) > 0 }) - 1
		if at < 0 {
			return zero, 0, false, nil
		}
		return blocks[at], uint64(at), true, nil
	}
	if !s.sparseLoaded {
		groups, err := readHistoryAccessorSparseDirectory(r, h)
		if err != nil {
			return zero, 0, false, err
		}
		s.groups, s.sparseLoaded = groups, true
	}
	groups := s.groups
	if len(groups) > 0 {
		at := sort.Search(len(groups), func(i int) bool { return bytes.Compare(groups[i].key, key) > 0 }) - 1
		if at < 0 {
			return zero, 0, false, nil
		}
		g := groups[at]
		if g.length <= historyAccessorDirectoryReadLimit {
			if !s.groupLoaded || s.groupIndex != g.index {
				blocks, e := readHistoryAccessorDirectoryGroup(r, h, g)
				if e != nil {
					return zero, 0, false, e
				}
				s.group, s.groupIndex, s.groupLoaded = blocks, g.index, true
			}
			blocks := s.group
			at := sort.Search(len(blocks), func(i int) bool { return bytes.Compare(blocks[i].firstKey, key) > 0 }) - 1
			if at < 0 {
				return zero, 0, false, errors.New("snapshots: sparse directory does not cover key")
			}
			return blocks[at], g.index + uint64(at), true, nil
		}
	}
	low, high := uint64(0), h.blockCount
	for low < high {
		mid := low + (high-low)/2
		b, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, mid)
		if e != nil {
			return zero, 0, false, e
		}
		if bytes.Compare(b.firstKey, key) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low == 0 {
		return zero, 0, false, nil
	}
	b, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, low-1)
	return b, low - 1, e == nil, e
}

func verifyHistoryAccessorSparseDirectory(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, blocks []stateDomainChangeBinaryAccessorV6Block) error {
	groups, err := readHistoryAccessorSparseDirectory(r, h)
	if err != nil {
		return err
	}
	off := h.headerSize + h.blockCount*8
	for i, b := range blocks {
		if len(groups) > 0 && uint64(i)%historyAccessorSparseStride == 0 {
			g := groups[uint64(i)/historyAccessorSparseStride]
			if g.offset != off || !bytes.Equal(g.key, b.firstKey) {
				return errors.New("snapshots: sparse directory differs from authoritative keys")
			}
		}
		off += 2 + uint64(len(b.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	return nil
}

func readSmallHistoryAccessorDirectory(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header) ([]stateDomainChangeBinaryAccessorV6Block, error) {
	size := h.blockDirLen - h.sparseLen
	if size > historyAccessorDirectoryReadLimit || size < h.blockCount*8 {
		return nil, errors.New("snapshots: invalid small directory bounds")
	}
	raw := make([]byte, size)
	if _, err := r.ReadAt(raw, int64(h.headerSize)); err != nil {
		return nil, err
	}
	blocks := make([]stateDomainChangeBinaryAccessorV6Block, 0, h.blockCount)
	next := h.blockCount * 8
	for i := uint64(0); i < h.blockCount; i++ {
		off := binary.BigEndian.Uint64(raw[i*8:])
		end := h.headerSize + size
		if i+1 < h.blockCount {
			end = binary.BigEndian.Uint64(raw[(i+1)*8:])
		}
		if off < h.headerSize || off-h.headerSize != next || end <= off || end > h.headerSize+size {
			return nil, errors.New("snapshots: invalid small directory offsets")
		}
		b, err := decodeHistoryAccessorDirectoryEntry(raw[off-h.headerSize : end-h.headerSize])
		if err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(blocks[i-1].firstKey, b.firstKey) >= 0 {
			return nil, errors.New("snapshots: small directory is not sorted")
		}
		blocks = append(blocks, b)
		next = end - h.headerSize
	}
	if next != size {
		return nil, errors.New("snapshots: small directory trailing bytes")
	}
	return blocks, nil
}
