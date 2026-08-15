package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	stateDomainChangeBinaryAccessorV7HeaderSize    = uint64(120)
	stateDomainChangeBinaryAccessorV7FramePostings = uint32(128)
	stateDomainChangeBinaryAccessorV7FrameDirSize  = uint64(24)
	stateDomainChangeBinaryAccessorV7Single        = byte(0)
	stateDomainChangeBinaryAccessorV7Framed        = byte(1)
)

type stateDomainChangeBinaryAccessorV7Frame struct {
	firstTx uint64
	dataOff uint32
	dataLen uint32
	count   uint16
	crc     uint32
}

func decodeStateDomainChangeBinaryAccessorV7Header(r io.ReaderAt, fileSize uint64) (stateDomainChangeBinaryAccessorV6Header, error) {
	if fileSize < stateDomainChangeBinaryAccessorV7HeaderSize {
		return stateDomainChangeBinaryAccessorV6Header{}, io.ErrUnexpectedEOF
	}
	var raw [stateDomainChangeBinaryAccessorV7HeaderSize]byte
	if _, err := r.ReadAt(raw[:], 0); err != nil {
		return stateDomainChangeBinaryAccessorV6Header{}, err
	}
	if !bytes.Equal(raw[:8], stateDomainChangeBinaryAccessorMagic[:]) || binary.BigEndian.Uint32(raw[8:12]) != stateDomainChangeBinaryVersionV7 {
		return stateDomainChangeBinaryAccessorV6Header{}, errors.New("snapshots: invalid state-domain-change V7 accessor magic/version")
	}
	if binary.BigEndian.Uint32(raw[36:40]) != stateDomainChangeBinaryAccessorV6BlockKeys || binary.BigEndian.Uint32(raw[112:116]) != crc32.ChecksumIEEE(raw[:112]) || !bytes.Equal(raw[116:120], []byte{0, 0, 0, 0}) {
		return stateDomainChangeBinaryAccessorV6Header{}, errors.New("snapshots: invalid state-domain-change V7 accessor header")
	}
	h := stateDomainChangeBinaryAccessorV6Header{
		version: stateDomainChangeBinaryVersionV7, headerSize: stateDomainChangeBinaryAccessorV7HeaderSize,
		fromTxNum: binary.BigEndian.Uint64(raw[12:20]), toTxNum: binary.BigEndian.Uint64(raw[20:28]),
		recordCount: binary.BigEndian.Uint64(raw[28:36]), keyCount: binary.BigEndian.Uint64(raw[40:48]),
		blockCount: binary.BigEndian.Uint64(raw[48:56]), blockDirLen: binary.BigEndian.Uint64(raw[56:64]),
		keyDataLen: binary.BigEndian.Uint64(raw[64:72]), postingLen: binary.BigEndian.Uint64(raw[104:112]),
	}
	copy(h.dictionaryDigest[:], raw[72:104])
	if h.recordCount > math.MaxUint32 || h.keyCount > math.MaxUint32 || h.blockCount != ceilDiv(h.keyCount, stateDomainChangeBinaryAccessorV6BlockKeys) || h.blockCount > math.MaxUint64/8 || h.blockDirLen < h.blockCount*8 {
		return h, errors.New("snapshots: invalid state-domain-change V7 accessor counts")
	}
	total, overflow := checkedAdd(h.headerSize, h.blockDirLen)
	if !overflow {
		total, overflow = checkedAdd(total, h.keyDataLen)
	}
	if !overflow {
		total, overflow = checkedAdd(total, h.postingLen)
	}
	if overflow || total != fileSize {
		return h, errors.New("snapshots: invalid state-domain-change V7 accessor length")
	}
	return h, nil
}

func decodeStateDomainChangeBinaryAccessorKeyHeader(r io.ReaderAt, fileSize uint64) (stateDomainChangeBinaryAccessorV6Header, error) {
	var raw [12]byte
	if _, err := r.ReadAt(raw[:], 0); err != nil {
		return stateDomainChangeBinaryAccessorV6Header{}, err
	}
	switch binary.BigEndian.Uint32(raw[8:12]) {
	case stateDomainChangeBinaryVersionV6:
		return decodeStateDomainChangeBinaryAccessorV6Header(r, fileSize)
	case stateDomainChangeBinaryVersionV7:
		return decodeStateDomainChangeBinaryAccessorV7Header(r, fileSize)
	default:
		return stateDomainChangeBinaryAccessorV6Header{}, errors.New("snapshots: unsupported key-oriented accessor version")
	}
}

func stateDomainChangeBinaryAccessorV7Lookup(r io.ReaderAt, fileSize uint64, lookup []byte) (stateDomainChangeBinaryAccessorV6Header, stateDomainChangeBinaryAccessorV6Record, bool, error) {
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(r, fileSize)
	if err != nil {
		return h, stateDomainChangeBinaryAccessorV6Record{}, false, err
	}
	low, high := uint64(0), h.blockCount
	for low < high {
		mid := low + (high-low)/2
		block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, mid)
		if err != nil {
			return h, stateDomainChangeBinaryAccessorV6Record{}, false, err
		}
		if bytes.Compare(block.firstKey, lookup) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low == 0 {
		return h, stateDomainChangeBinaryAccessorV6Record{}, false, nil
	}
	blockIndex := low - 1
	block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, blockIndex)
	if err != nil {
		return h, stateDomainChangeBinaryAccessorV6Record{}, false, err
	}
	records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, fileSize, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
	if err != nil {
		return h, stateDomainChangeBinaryAccessorV6Record{}, false, err
	}
	pos := sort.Search(len(records), func(i int) bool { return bytes.Compare(records[i].key, lookup) >= 0 })
	if pos == len(records) || !bytes.Equal(records[pos].key, lookup) {
		return h, stateDomainChangeBinaryAccessorV6Record{}, false, nil
	}
	return h, records[pos], true, nil
}

func encodeStateDomainChangeBinaryAccessorV7PostingList(fromTxNum uint64, postings []stateDomainChangeBinaryAccessorV6Posting) ([]byte, error) {
	if len(postings) == 0 {
		return nil, errors.New("snapshots: empty V7 accessor posting list")
	}
	frames := make([][]byte, 0, ceilDiv(uint64(len(postings)), uint64(stateDomainChangeBinaryAccessorV7FramePostings)))
	firstTx := make([]uint64, 0, cap(frames))
	counts := make([]uint16, 0, cap(frames))
	for start := 0; start < len(postings); start += int(stateDomainChangeBinaryAccessorV7FramePostings) {
		end := min(start+int(stateDomainChangeBinaryAccessorV7FramePostings), len(postings))
		var encoded []byte
		var previous stateDomainChangeBinaryAccessorV6Posting
		for i := start; i < end; i++ {
			posting := postings[i]
			if posting.txNum < fromTxNum || posting.offset > stateDomainChangeBinaryAccessorV5MaxOffset || i > start && (posting.txNum < previous.txNum || posting.offset <= previous.offset || posting.recordIndex <= previous.recordIndex) {
				return nil, errors.New("snapshots: invalid V7 accessor posting order")
			}
			if i == start {
				encoded = binary.AppendUvarint(encoded, posting.txNum-fromTxNum)
				encoded = binary.AppendUvarint(encoded, posting.offset)
				encoded = binary.AppendUvarint(encoded, uint64(posting.recordIndex))
			} else {
				encoded = binary.AppendUvarint(encoded, posting.txNum-previous.txNum)
				encoded = binary.AppendUvarint(encoded, posting.offset-previous.offset)
				encoded = binary.AppendUvarint(encoded, uint64(posting.recordIndex-previous.recordIndex))
			}
			previous = posting
		}
		frames = append(frames, encoded)
		firstTx = append(firstTx, postings[start].txNum)
		counts = append(counts, uint16(end-start))
	}
	if len(frames) == 1 {
		out := append([]byte{stateDomainChangeBinaryAccessorV7Single}, frames[0]...)
		var sum [4]byte
		binary.BigEndian.PutUint32(sum[:], crc32.ChecksumIEEE(frames[0]))
		return append(out, sum[:]...), nil
	}
	var out []byte
	out = append(out, stateDomainChangeBinaryAccessorV7Framed)
	out = binary.AppendUvarint(out, uint64(len(frames)))
	directoryStart := len(out)
	directoryBytes := len(frames) * int(stateDomainChangeBinaryAccessorV7FrameDirSize)
	out = append(out, make([]byte, directoryBytes)...)
	for i, frame := range frames {
		if len(out) > math.MaxUint32 || len(frame) > math.MaxUint32 {
			return nil, errors.New("snapshots: V7 accessor posting list exceeds uint32 offsets")
		}
		dir := out[directoryStart+i*int(stateDomainChangeBinaryAccessorV7FrameDirSize):]
		binary.BigEndian.PutUint64(dir[0:8], firstTx[i])
		binary.BigEndian.PutUint32(dir[8:12], uint32(len(out)))
		binary.BigEndian.PutUint32(dir[12:16], uint32(len(frame)))
		binary.BigEndian.PutUint16(dir[16:18], counts[i])
		binary.BigEndian.PutUint32(dir[20:24], crc32.ChecksumIEEE(frame))
		out = append(out, frame...)
	}
	return out, nil
}

// writeStateDomainChangeBinaryAccessorV7PostingFrames publishes one key's
// already encoded frames from bounded scratch storage. Only the small frame
// directory remains resident even for an exceptionally hot key.
func writeStateDomainChangeBinaryAccessorV7PostingFrames(dst io.Writer, scratch *os.File, frames []stateDomainChangeBinaryAccessorV7Frame) (uint64, uint32, error) {
	if dst == nil || scratch == nil || len(frames) == 0 {
		return 0, 0, errors.New("snapshots: invalid V7 posting frame writer")
	}
	var count uint64
	var dataLen uint64
	for i, frame := range frames {
		if frame.count == 0 || frame.dataLen == 0 || uint64(frame.dataOff) != dataLen || i > 0 && frame.firstTx < frames[i-1].firstTx {
			return 0, 0, errors.New("snapshots: invalid V7 posting scratch frames")
		}
		count += uint64(frame.count)
		dataLen += uint64(frame.dataLen)
	}
	if count > math.MaxUint32 {
		return 0, 0, errors.New("snapshots: V7 key posting count exceeds uint32")
	}
	if _, err := scratch.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}
	if len(frames) == 1 {
		if _, err := dst.Write([]byte{stateDomainChangeBinaryAccessorV7Single}); err != nil {
			return 0, 0, err
		}
		if _, err := io.CopyN(dst, scratch, int64(dataLen)); err != nil {
			return 0, 0, err
		}
		var sum [4]byte
		binary.BigEndian.PutUint32(sum[:], frames[0].crc)
		if _, err := dst.Write(sum[:]); err != nil {
			return 0, 0, err
		}
		return 1 + dataLen + 4, uint32(count), nil
	}
	var prefix []byte
	prefix = append(prefix, stateDomainChangeBinaryAccessorV7Framed)
	prefix = binary.AppendUvarint(prefix, uint64(len(frames)))
	directoryStart := len(prefix)
	prefix = append(prefix, make([]byte, len(frames)*int(stateDomainChangeBinaryAccessorV7FrameDirSize))...)
	dataStart := uint64(len(prefix))
	if dataStart+dataLen > math.MaxUint32 {
		return 0, 0, errors.New("snapshots: V7 key posting stream exceeds uint32")
	}
	for i, frame := range frames {
		dir := prefix[directoryStart+i*int(stateDomainChangeBinaryAccessorV7FrameDirSize):]
		binary.BigEndian.PutUint64(dir[0:8], frame.firstTx)
		binary.BigEndian.PutUint32(dir[8:12], uint32(dataStart+uint64(frame.dataOff)))
		binary.BigEndian.PutUint32(dir[12:16], frame.dataLen)
		binary.BigEndian.PutUint16(dir[16:18], frame.count)
		binary.BigEndian.PutUint32(dir[20:24], frame.crc)
	}
	if _, err := dst.Write(prefix); err != nil {
		return 0, 0, err
	}
	if _, err := io.CopyN(dst, scratch, int64(dataLen)); err != nil {
		return 0, 0, err
	}
	return dataStart + dataLen, uint32(count), nil
}

func decodeStateDomainChangeBinaryAccessorV7Frame(raw []byte, fromTxNum uint64, count uint16) ([]stateDomainChangeBinaryAccessorV6Posting, error) {
	postings := make([]stateDomainChangeBinaryAccessorV6Posting, 0, count)
	pos := 0
	read := func() (uint64, error) {
		if pos >= len(raw) {
			return 0, io.ErrUnexpectedEOF
		}
		value, n := binary.Uvarint(raw[pos:])
		if n <= 0 {
			return 0, errors.New("snapshots: invalid V7 accessor posting varint")
		}
		pos += n
		return value, nil
	}
	var previous stateDomainChangeBinaryAccessorV6Posting
	for i := uint16(0); i < count; i++ {
		a, err := read()
		if err != nil {
			return nil, err
		}
		b, err := read()
		if err != nil {
			return nil, err
		}
		c, err := read()
		if err != nil {
			return nil, err
		}
		var posting stateDomainChangeBinaryAccessorV6Posting
		if i == 0 {
			if a > math.MaxUint64-fromTxNum || c > math.MaxUint32 {
				return nil, errors.New("snapshots: V7 accessor posting base overflows")
			}
			posting = stateDomainChangeBinaryAccessorV6Posting{txNum: fromTxNum + a, offset: b, recordIndex: uint32(c)}
		} else {
			if previous.txNum > math.MaxUint64-a || previous.offset > math.MaxUint64-b || c > math.MaxUint32-uint64(previous.recordIndex) || b == 0 || c == 0 {
				return nil, errors.New("snapshots: V7 accessor posting delta overflows")
			}
			posting = stateDomainChangeBinaryAccessorV6Posting{txNum: previous.txNum + a, offset: previous.offset + b, recordIndex: previous.recordIndex + uint32(c)}
		}
		if posting.offset > stateDomainChangeBinaryAccessorV5MaxOffset || i > 0 && (posting.txNum < previous.txNum || posting.offset <= previous.offset || posting.recordIndex <= previous.recordIndex) {
			return nil, errors.New("snapshots: invalid V7 accessor posting")
		}
		postings = append(postings, posting)
		previous = posting
	}
	if pos != len(raw) {
		return nil, errors.New("snapshots: V7 accessor posting frame trailing bytes")
	}
	return postings, nil
}

func stateDomainChangeBinaryAccessorV7PostingFrames(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record) ([]stateDomainChangeBinaryAccessorV7Frame, error) {
	base := h.headerSize + h.blockDirLen + h.keyDataLen + record.postingOff
	if record.postingOff >= h.postingLen {
		return nil, errors.New("snapshots: V7 accessor posting offset outside section")
	}
	var marker [1]byte
	if _, err := r.ReadAt(marker[:], int64(base)); err != nil {
		return nil, err
	}
	if marker[0] == stateDomainChangeBinaryAccessorV7Single {
		if record.postings > stateDomainChangeBinaryAccessorV7FramePostings {
			return nil, errors.New("snapshots: oversized V7 single posting frame")
		}
		// Single frames are decoded by the bounded streaming helper below.
		return []stateDomainChangeBinaryAccessorV7Frame{{count: uint16(record.postings), dataOff: 1}}, nil
	}
	if marker[0] != stateDomainChangeBinaryAccessorV7Framed {
		return nil, errors.New("snapshots: invalid V7 accessor posting marker")
	}
	reader := io.NewSectionReader(r, int64(base+1), int64(h.postingLen-record.postingOff-1))
	frameCount, err := binary.ReadUvarint(&readerByteReader{r: reader})
	if err != nil || frameCount < 2 || frameCount != ceilDiv(uint64(record.postings), uint64(stateDomainChangeBinaryAccessorV7FramePostings)) || frameCount > math.MaxUint32 {
		return nil, errors.New("snapshots: invalid V7 accessor posting frame count")
	}
	prefixLen := uint64(1 + uvarintLen(frameCount))
	if frameCount > (h.postingLen-record.postingOff-prefixLen)/stateDomainChangeBinaryAccessorV7FrameDirSize {
		return nil, errors.New("snapshots: V7 accessor posting directory exceeds section")
	}
	frames := make([]stateDomainChangeBinaryAccessorV7Frame, frameCount)
	var raw [stateDomainChangeBinaryAccessorV7FrameDirSize]byte
	var total uint64
	var previousTx uint64
	dataStart := prefixLen + frameCount*stateDomainChangeBinaryAccessorV7FrameDirSize
	dataEnd := dataStart
	for i := uint64(0); i < frameCount; i++ {
		if _, err := r.ReadAt(raw[:], int64(base+prefixLen+i*stateDomainChangeBinaryAccessorV7FrameDirSize)); err != nil {
			return nil, err
		}
		if raw[18] != 0 || raw[19] != 0 {
			return nil, errors.New("snapshots: non-zero V7 accessor frame reserved bytes")
		}
		frame := stateDomainChangeBinaryAccessorV7Frame{firstTx: binary.BigEndian.Uint64(raw[0:8]), dataOff: binary.BigEndian.Uint32(raw[8:12]), dataLen: binary.BigEndian.Uint32(raw[12:16]), count: binary.BigEndian.Uint16(raw[16:18]), crc: binary.BigEndian.Uint32(raw[20:24])}
		wantCount := min(uint64(stateDomainChangeBinaryAccessorV7FramePostings), uint64(record.postings)-total)
		if uint64(frame.count) != wantCount || uint64(frame.dataOff) != dataEnd || uint64(frame.dataLen) > h.postingLen-record.postingOff-dataEnd || i > 0 && frame.firstTx < previousTx {
			return nil, errors.New("snapshots: invalid V7 accessor posting frame directory")
		}
		frames[i] = frame
		total += uint64(frame.count)
		dataEnd += uint64(frame.dataLen)
		previousTx = frame.firstTx
	}
	if total != uint64(record.postings) {
		return nil, errors.New("snapshots: V7 accessor posting frame count mismatch")
	}
	return frames, nil
}

type readerByteReader struct{ r io.Reader }

func (r *readerByteReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r.r, b[:])
	return b[0], err
}

func stateDomainChangeBinaryAccessorV7ReadFrame(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record, frame stateDomainChangeBinaryAccessorV7Frame) ([]stateDomainChangeBinaryAccessorV6Posting, error) {
	base := h.headerSize + h.blockDirLen + h.keyDataLen + record.postingOff
	if frame.dataOff == 1 { // single frame: decode at most 128 triples, then checksum
		cursor := &stateDomainChangeBinaryAccessorV7Cursor{r: r, off: base + 1, limit: h.headerSize + h.blockDirLen + h.keyDataLen + h.postingLen}
		var raw []byte
		for i := uint16(0); i < frame.count; i++ {
			for j := 0; j < 3; j++ {
				_, encoded, err := cursor.uvarint()
				if err != nil {
					return nil, err
				}
				raw = append(raw, encoded...)
			}
		}
		var sum [4]byte
		if _, err := r.ReadAt(sum[:], int64(cursor.off)); err != nil {
			return nil, err
		}
		if binary.BigEndian.Uint32(sum[:]) != crc32.ChecksumIEEE(raw) {
			return nil, errors.New("snapshots: V7 accessor single frame checksum mismatch")
		}
		return decodeStateDomainChangeBinaryAccessorV7Frame(raw, h.fromTxNum, frame.count)
	}
	raw := make([]byte, frame.dataLen)
	if _, err := r.ReadAt(raw, int64(base+uint64(frame.dataOff))); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(raw) != frame.crc {
		return nil, errors.New("snapshots: V7 accessor posting frame checksum mismatch")
	}
	postings, err := decodeStateDomainChangeBinaryAccessorV7Frame(raw, h.fromTxNum, frame.count)
	if err == nil && len(postings) > 0 && postings[0].txNum != frame.firstTx {
		return nil, errors.New("snapshots: V7 accessor posting frame first tx mismatch")
	}
	return postings, err
}

type stateDomainChangeBinaryAccessorV7Cursor struct {
	r          io.ReaderAt
	off, limit uint64
}

func (c *stateDomainChangeBinaryAccessorV7Cursor) uvarint() (uint64, []byte, error) {
	var raw []byte
	for i := 0; i < binary.MaxVarintLen64; i++ {
		if c.off >= c.limit {
			return 0, raw, io.ErrUnexpectedEOF
		}
		var b [1]byte
		if _, err := c.r.ReadAt(b[:], int64(c.off)); err != nil {
			return 0, raw, err
		}
		c.off++
		raw = append(raw, b[0])
		if b[0] < 0x80 {
			value, n := binary.Uvarint(raw)
			if n <= 0 {
				return 0, raw, errors.New("snapshots: invalid V7 accessor varint")
			}
			return value, raw, nil
		}
	}
	return 0, raw, errors.New("snapshots: oversized V7 accessor varint")
}

func iterateStateDomainChangeBinarySegmentByAccessorV7Record(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(accessor, h, record)
	if err != nil {
		return err
	}
	start := sort.Search(len(frames), func(i int) bool { return frames[i].firstTx >= fromTxNum })
	if start > 0 {
		start--
	}
	for i := start; i < len(frames); i++ {
		postings, err := stateDomainChangeBinaryAccessorV7ReadFrame(accessor, h, record, frames[i])
		if err != nil {
			return err
		}
		for _, posting := range postings {
			if posting.txNum < fromTxNum {
				continue
			}
			if posting.txNum > toTxNum {
				return nil
			}
			change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, posting.offset, segmentSize, uint64(posting.recordIndex))
			if err != nil {
				return err
			}
			if change.TxNum != posting.txNum || !bytes.Equal(stateDomainChangeBinaryAccessorKey(change), record.key) {
				return errors.New("snapshots: V7 accessor posting does not match history record")
			}
			cont, err := fn(change)
			if err != nil || !cont {
				return err
			}
		}
	}
	return nil
}

func iterateStateDomainChangeBinarySegmentByAccessorV7Key(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, lookupKey []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if err := verifyStateDomainChangeBinaryKeyDictionaryPair(segment, accessor, accessorSize); err != nil {
		return err
	}
	h, record, ok, err := stateDomainChangeBinaryAccessorV7Lookup(accessor, accessorSize, lookupKey)
	if err != nil || !ok {
		return err
	}
	return iterateStateDomainChangeBinarySegmentByAccessorV7Record(segment, segmentSize, accessor, h, record, fromTxNum, toTxNum, fn)
}

func iterateStateDomainChangeBinarySegmentByAccessorV7Prefix(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, lookupPrefix []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if err := verifyStateDomainChangeBinaryKeyDictionaryPair(segment, accessor, accessorSize); err != nil {
		return err
	}
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(accessor, accessorSize)
	if err != nil {
		return err
	}
	low, high := uint64(0), h.blockCount
	for low < high {
		mid := low + (high-low)/2
		block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(accessor, h, mid)
		if err != nil {
			return err
		}
		if bytes.Compare(block.firstKey, lookupPrefix) <= 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	start := uint64(0)
	if low != 0 {
		start = low - 1
	}
	for blockIndex := start; blockIndex < h.blockCount; blockIndex++ {
		block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(accessor, h, blockIndex)
		if err != nil {
			return err
		}
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(accessor, accessorSize, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return err
		}
		for _, record := range records {
			if bytes.Compare(record.key, lookupPrefix) < 0 {
				continue
			}
			if !bytes.HasPrefix(record.key, lookupPrefix) {
				return nil
			}
			if err := iterateStateDomainChangeBinarySegmentByAccessorV7Record(segment, segmentSize, accessor, h, record, fromTxNum, toTxNum, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyStateDomainChangeBinaryKeyDictionaryPair(segment, accessor io.ReaderAt, accessorSize uint64) error {
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(accessor, accessorSize)
	if err != nil {
		return err
	}
	digest, err := readStateDomainChangeBinaryV6DictionaryCommitment(segment)
	if err != nil {
		return err
	}
	if digest != h.dictionaryDigest {
		return errors.New("snapshots: history/accessor dictionary commitment mismatch")
	}
	return nil
}

func checkStateDomainChangeBinaryAccessorV7(r io.ReaderAt, fileSize uint64) error {
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(r, fileSize)
	if err != nil {
		return err
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
	if err != nil {
		return err
	}
	var keys, postings uint64
	var previous []byte
	var expectedOff uint64
	for blockIndex, block := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, fileSize, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return err
		}
		for _, record := range records {
			if previous != nil && bytes.Compare(previous, record.key) >= 0 {
				return errors.New("snapshots: V7 accessor dictionary is not sorted")
			}
			if record.postingOff != expectedOff {
				return errors.New("snapshots: V7 accessor postings are not contiguous")
			}
			frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, record)
			if err != nil {
				return err
			}
			var last stateDomainChangeBinaryAccessorV6Posting
			var seen uint64
			for _, frame := range frames {
				rows, err := stateDomainChangeBinaryAccessorV7ReadFrame(r, h, record, frame)
				if err != nil {
					return err
				}
				for _, p := range rows {
					if p.txNum < h.fromTxNum || p.txNum > h.toTxNum || uint64(p.recordIndex) >= h.recordCount {
						return errors.New("snapshots: V7 accessor posting outside segment bounds")
					}
					if seen > 0 && (p.txNum < last.txNum || p.offset <= last.offset || p.recordIndex <= last.recordIndex) {
						return errors.New("snapshots: V7 accessor postings are not ordered")
					}
					last = p
					seen++
				}
			}
			if seen != uint64(record.postings) {
				return errors.New("snapshots: V7 accessor posting count mismatch")
			}
			length, err := stateDomainChangeBinaryAccessorV7PostingListLength(r, h, record)
			if err != nil {
				return err
			}
			expectedOff += length
			postings += seen
			keys++
			previous = record.key
		}
	}
	if keys != h.keyCount || postings != h.recordCount || expectedOff != h.postingLen {
		return fmt.Errorf("snapshots: V7 accessor counts keys=%d/%d postings=%d/%d bytes=%d/%d", keys, h.keyCount, postings, h.recordCount, expectedOff, h.postingLen)
	}
	return nil
}

func stateDomainChangeBinaryAccessorV7PostingListLength(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record) (uint64, error) {
	frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(r, h, record)
	if err != nil {
		return 0, err
	}
	if len(frames) == 1 && frames[0].dataOff == 1 {
		base := h.headerSize + h.blockDirLen + h.keyDataLen + record.postingOff
		cursor := &stateDomainChangeBinaryAccessorV7Cursor{r: r, off: base + 1, limit: h.headerSize + h.blockDirLen + h.keyDataLen + h.postingLen}
		for i := uint32(0); i < record.postings; i++ {
			for j := 0; j < 3; j++ {
				if _, _, err := cursor.uvarint(); err != nil {
					return 0, err
				}
			}
		}
		return cursor.off - (base + 1) + 5, nil
	}
	last := frames[len(frames)-1]
	return uint64(last.dataOff) + uint64(last.dataLen), nil
}

func verifyStateDomainChangeBinaryAccessorV7Coverage(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64) error {
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(accessor, accessorSize)
	if err != nil {
		return err
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(accessor, h)
	if err != nil {
		return err
	}
	seen := make([]uint64, (h.recordCount+63)/64)
	var covered uint64
	for bi, b := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(accessor, accessorSize, h, b, uint32(bi*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return err
		}
		for _, record := range records {
			frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(accessor, h, record)
			if err != nil {
				return err
			}
			for _, frame := range frames {
				rows, err := stateDomainChangeBinaryAccessorV7ReadFrame(accessor, h, record, frame)
				if err != nil {
					return err
				}
				for _, p := range rows {
					word, bit := uint64(p.recordIndex)/64, uint(p.recordIndex)%64
					if seen[word]&(uint64(1)<<bit) != 0 {
						return errors.New("snapshots: V7 accessor covers a history record twice")
					}
					change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, p.offset, segmentSize, uint64(p.recordIndex))
					if err != nil {
						return err
					}
					if change.TxNum != p.txNum || !bytes.Equal(stateDomainChangeBinaryAccessorKey(change), record.key) {
						return errors.New("snapshots: V7 accessor posting/history mismatch")
					}
					seen[word] |= uint64(1) << bit
					covered++
				}
			}
		}
	}
	if covered != h.recordCount {
		return fmt.Errorf("snapshots: V7 accessor covers %d records, want %d", covered, h.recordCount)
	}
	return nil
}

func readStateDomainChangeBinaryAccessorV7Debug(accessor io.ReaderAt, accessorSize uint64) ([]stateDomainChangeBinaryAccessorEntry, error) {
	h, err := decodeStateDomainChangeBinaryAccessorV7Header(accessor, accessorSize)
	if err != nil {
		return nil, err
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(accessor, h)
	if err != nil {
		return nil, err
	}
	entries := make([]stateDomainChangeBinaryAccessorEntry, 0, h.recordCount)
	for bi, b := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(accessor, accessorSize, h, b, uint32(bi*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			frames, err := stateDomainChangeBinaryAccessorV7PostingFrames(accessor, h, record)
			if err != nil {
				return nil, err
			}
			for _, frame := range frames {
				rows, err := stateDomainChangeBinaryAccessorV7ReadFrame(accessor, h, record, frame)
				if err != nil {
					return nil, err
				}
				for _, p := range rows {
					entries = append(entries, stateDomainChangeBinaryAccessorEntry{key: append([]byte(nil), record.key...), txNum: p.txNum, seq: uint64(p.recordIndex) + 1, offset: p.offset, recordIndex: uint64(p.recordIndex)})
				}
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return compareStateDomainChangeBinaryAccessorEntry(entries[i], entries[j]) < 0 })
	return entries, nil
}
