package snapshots

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	stateDomainChangeBinaryIndexV7HeaderSize   = uint64(80)
	stateDomainChangeBinaryIndexV7FrameEntries = uint64(256)
	stateDomainChangeBinaryIndexV7FrameSize    = uint64(32)
)

type stateDomainChangeBinaryIndexV7Frame struct {
	firstTx uint64
	dataOff uint64
	dataLen uint32
	count   uint32
	crc     uint32
}

// rewriteStateDomainChangeBinaryIndexV7 turns the bounded fixed-width staging
// index into the published delta-framed representation. Keeping staging simple
// lets the history writer stay single-pass; the rewrite is sequential and does
// not retain more than one 256-entry frame.
func rewriteStateDomainChangeBinaryIndexV7(file *os.File, name string) (*os.File, string, error) {
	if file == nil {
		return nil, "", errors.New("snapshots: nil state-domain-change index staging file")
	}
	header, err := readStateDomainChangeBinaryHeaderAt(file, stateDomainChangeBinaryIndexMagic)
	if err != nil {
		return nil, "", err
	}
	if header.version != stateDomainChangeBinaryIndexVersion {
		return nil, "", fmt.Errorf("snapshots: staging index version %d, want %d", header.version, stateDomainChangeBinaryIndexVersion)
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, "", err
	}
	want := uint64(stateDomainChangeBinaryHeaderSize) + header.count*stateDomainChangeBinaryIndexEntrySize
	if header.count > (math.MaxUint64-stateDomainChangeBinaryHeaderSize)/stateDomainChangeBinaryIndexEntrySize || uint64(stat.Size()) != want {
		return nil, "", fmt.Errorf("snapshots: staging index size %d, want %d", stat.Size(), want)
	}
	frameCount := ceilDiv(header.count, stateDomainChangeBinaryIndexV7FrameEntries)
	if frameCount > (math.MaxUint64-stateDomainChangeBinaryIndexV7HeaderSize)/stateDomainChangeBinaryIndexV7FrameSize {
		return nil, "", errors.New("snapshots: V7 index frame directory overflows")
	}
	dirLen := frameCount * stateDomainChangeBinaryIndexV7FrameSize
	out, outName, err := createStateDomainChangeBinaryTempFileInDir(filepath.Dir(name), filepath.Base(name)+".v7")
	if err != nil {
		return nil, "", err
	}
	ok := false
	defer func() {
		if !ok {
			_ = out.Close()
			_ = os.Remove(outName)
		}
	}()
	if err := writeZeroes(out, stateDomainChangeBinaryIndexV7HeaderSize+dirLen); err != nil {
		return nil, "", err
	}
	frames := make([]stateDomainChangeBinaryIndexV7Frame, 0, frameCount)
	dataOff := stateDomainChangeBinaryIndexV7HeaderSize + dirLen
	var previousGlobal stateDomainChangeBinaryTxOffset
	for frameIndex := uint64(0); frameIndex < frameCount; frameIndex++ {
		start := frameIndex * stateDomainChangeBinaryIndexV7FrameEntries
		count := min(stateDomainChangeBinaryIndexV7FrameEntries, header.count-start)
		var encoded []byte
		var previous stateDomainChangeBinaryTxOffset
		for i := uint64(0); i < count; i++ {
			entry, err := readStateDomainChangeBinaryIndexEntryAt(file, start+i)
			if err != nil {
				return nil, "", err
			}
			if entry.count == 0 || entry.txNum < header.fromTxNum || entry.txNum > header.toTxNum {
				return nil, "", errors.New("snapshots: invalid staging index entry")
			}
			if start+i > 0 && (entry.txNum <= previousGlobal.txNum || entry.offset <= previousGlobal.offset || entry.recordIndex <= previousGlobal.recordIndex) {
				return nil, "", errors.New("snapshots: staging index entries are not strictly ordered")
			}
			if i == 0 {
				encoded = binary.AppendUvarint(encoded, entry.offset)
				encoded = binary.AppendUvarint(encoded, entry.recordIndex)
				encoded = binary.AppendUvarint(encoded, entry.count)
			} else {
				encoded = binary.AppendUvarint(encoded, entry.txNum-previous.txNum)
				encoded = binary.AppendUvarint(encoded, entry.offset-previous.offset)
				encoded = binary.AppendUvarint(encoded, entry.recordIndex-previous.recordIndex)
				encoded = binary.AppendUvarint(encoded, entry.count)
			}
			previous = entry
			previousGlobal = entry
		}
		if uint64(len(encoded)) > math.MaxUint32 {
			return nil, "", errors.New("snapshots: V7 index frame exceeds uint32")
		}
		if _, err := out.Write(encoded); err != nil {
			return nil, "", err
		}
		frames = append(frames, stateDomainChangeBinaryIndexV7Frame{
			firstTx: previous.txNum,
			dataOff: dataOff,
			dataLen: uint32(len(encoded)),
			count:   uint32(count),
			crc:     crc32.ChecksumIEEE(encoded),
		})
		// Store the first tx, not the last one retained in previous.
		first, err := readStateDomainChangeBinaryIndexEntryAt(file, start)
		if err != nil {
			return nil, "", err
		}
		frames[len(frames)-1].firstTx = first.txNum
		dataOff += uint64(len(encoded))
	}
	var rawHeader [stateDomainChangeBinaryIndexV7HeaderSize]byte
	copy(rawHeader[:8], stateDomainChangeBinaryIndexMagic[:])
	binary.BigEndian.PutUint32(rawHeader[8:12], stateDomainChangeBinaryIndexCurrentVersion)
	binary.BigEndian.PutUint64(rawHeader[12:20], header.fromTxNum)
	binary.BigEndian.PutUint64(rawHeader[20:28], header.toTxNum)
	binary.BigEndian.PutUint64(rawHeader[28:36], header.count)
	binary.BigEndian.PutUint32(rawHeader[36:40], uint32(stateDomainChangeBinaryIndexV7FrameEntries))
	binary.BigEndian.PutUint64(rawHeader[40:48], frameCount)
	binary.BigEndian.PutUint64(rawHeader[48:56], dirLen)
	binary.BigEndian.PutUint64(rawHeader[56:64], dataOff-stateDomainChangeBinaryIndexV7HeaderSize-dirLen)
	binary.BigEndian.PutUint32(rawHeader[64:68], crc32.ChecksumIEEE(rawHeader[:64]))
	if _, err := out.WriteAt(rawHeader[:], 0); err != nil {
		return nil, "", err
	}
	var rawFrame [stateDomainChangeBinaryIndexV7FrameSize]byte
	for i, frame := range frames {
		clear(rawFrame[:])
		binary.BigEndian.PutUint64(rawFrame[0:8], frame.firstTx)
		binary.BigEndian.PutUint64(rawFrame[8:16], frame.dataOff)
		binary.BigEndian.PutUint32(rawFrame[16:20], frame.dataLen)
		binary.BigEndian.PutUint32(rawFrame[20:24], frame.count)
		binary.BigEndian.PutUint32(rawFrame[24:28], frame.crc)
		if _, err := out.WriteAt(rawFrame[:], int64(stateDomainChangeBinaryIndexV7HeaderSize)+int64(i)*int64(stateDomainChangeBinaryIndexV7FrameSize)); err != nil {
			return nil, "", err
		}
	}
	if err := out.Sync(); err != nil {
		return nil, "", err
	}
	if err := file.Close(); err != nil {
		return nil, "", err
	}
	if err := os.Remove(name); err != nil {
		return nil, "", err
	}
	ok = true
	return out, outName, nil
}

type stateDomainChangeBinaryIndexV7Reader struct {
	file   *os.File
	header stateDomainChangeBinaryHeader
	frames []stateDomainChangeBinaryIndexV7Frame

	mu         sync.Mutex
	cacheIndex uint64
	cache      []stateDomainChangeBinaryTxOffset
	cacheValid bool
}

func openStateDomainChangeBinaryIndexV7Reader(file *os.File, size uint64, header stateDomainChangeBinaryHeader) (*stateDomainChangeBinaryIndexV7Reader, error) {
	if size < stateDomainChangeBinaryIndexV7HeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	var rawHeader [stateDomainChangeBinaryIndexV7HeaderSize]byte
	if _, err := file.ReadAt(rawHeader[:], 0); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(rawHeader[36:40]) != uint32(stateDomainChangeBinaryIndexV7FrameEntries) || binary.BigEndian.Uint32(rawHeader[64:68]) != crc32.ChecksumIEEE(rawHeader[:64]) {
		return nil, errors.New("snapshots: invalid V7 state history index header")
	}
	for _, b := range rawHeader[68:] {
		if b != 0 {
			return nil, errors.New("snapshots: non-zero V7 state history index reserved bytes")
		}
	}
	frameCount := binary.BigEndian.Uint64(rawHeader[40:48])
	dirLen := binary.BigEndian.Uint64(rawHeader[48:56])
	dataLen := binary.BigEndian.Uint64(rawHeader[56:64])
	if frameCount != ceilDiv(header.count, stateDomainChangeBinaryIndexV7FrameEntries) || dirLen != frameCount*stateDomainChangeBinaryIndexV7FrameSize || stateDomainChangeBinaryIndexV7HeaderSize+dirLen+dataLen != size {
		return nil, errors.New("snapshots: invalid V7 state history index layout")
	}
	frames := make([]stateDomainChangeBinaryIndexV7Frame, frameCount)
	var previousTx uint64
	var dataEnd = stateDomainChangeBinaryIndexV7HeaderSize + dirLen
	var rawFrame [stateDomainChangeBinaryIndexV7FrameSize]byte
	for i := uint64(0); i < frameCount; i++ {
		if _, err := file.ReadAt(rawFrame[:], int64(stateDomainChangeBinaryIndexV7HeaderSize+i*stateDomainChangeBinaryIndexV7FrameSize)); err != nil {
			return nil, err
		}
		for _, b := range rawFrame[28:] {
			if b != 0 {
				return nil, errors.New("snapshots: non-zero V7 index frame reserved bytes")
			}
		}
		frame := stateDomainChangeBinaryIndexV7Frame{
			firstTx: binary.BigEndian.Uint64(rawFrame[0:8]), dataOff: binary.BigEndian.Uint64(rawFrame[8:16]),
			dataLen: binary.BigEndian.Uint32(rawFrame[16:20]), count: binary.BigEndian.Uint32(rawFrame[20:24]), crc: binary.BigEndian.Uint32(rawFrame[24:28]),
		}
		wantCount := min(stateDomainChangeBinaryIndexV7FrameEntries, header.count-i*stateDomainChangeBinaryIndexV7FrameEntries)
		if uint64(frame.count) != wantCount || frame.dataOff != dataEnd || uint64(frame.dataLen) > size-dataEnd || frame.firstTx < header.fromTxNum || frame.firstTx > header.toTxNum || i > 0 && frame.firstTx <= previousTx {
			return nil, errors.New("snapshots: invalid V7 state history index frame directory")
		}
		frames[i] = frame
		previousTx = frame.firstTx
		dataEnd += uint64(frame.dataLen)
	}
	if dataEnd != size {
		return nil, errors.New("snapshots: V7 state history index data length mismatch")
	}
	return &stateDomainChangeBinaryIndexV7Reader{file: file, header: header, frames: frames}, nil
}

func (r *stateDomainChangeBinaryIndexV7Reader) Close() error { return r.file.Close() }

func (r *stateDomainChangeBinaryIndexV7Reader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("snapshots: negative V7 index logical offset")
	}
	if len(p) == stateDomainChangeBinaryIndexEntrySize && off >= stateDomainChangeBinaryHeaderSize && (off-stateDomainChangeBinaryHeaderSize)%stateDomainChangeBinaryIndexEntrySize == 0 {
		index := uint64((off - stateDomainChangeBinaryHeaderSize) / stateDomainChangeBinaryIndexEntrySize)
		entry, err := r.entryAt(index)
		if err != nil {
			return 0, err
		}
		var raw [stateDomainChangeBinaryIndexEntrySize]byte
		putStateDomainChangeBinaryIndexEntry(&raw, entry)
		copy(p, raw[:])
		return len(p), nil
	}
	return 0, fmt.Errorf("snapshots: unsupported V7 index logical read offset=%d len=%d", off, len(p))
}

func (r *stateDomainChangeBinaryIndexV7Reader) entryAt(index uint64) (stateDomainChangeBinaryTxOffset, error) {
	if index >= r.header.count {
		return stateDomainChangeBinaryTxOffset{}, io.EOF
	}
	frameIndex := index / stateDomainChangeBinaryIndexV7FrameEntries
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.cacheValid || r.cacheIndex != frameIndex {
		entries, err := r.decodeFrame(frameIndex)
		if err != nil {
			return stateDomainChangeBinaryTxOffset{}, err
		}
		r.cacheIndex, r.cache, r.cacheValid = frameIndex, entries, true
	}
	return r.cache[index%stateDomainChangeBinaryIndexV7FrameEntries], nil
}

func (r *stateDomainChangeBinaryIndexV7Reader) decodeFrame(index uint64) ([]stateDomainChangeBinaryTxOffset, error) {
	frame := r.frames[index]
	raw := make([]byte, frame.dataLen)
	if _, err := r.file.ReadAt(raw, int64(frame.dataOff)); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(raw) != frame.crc {
		return nil, errors.New("snapshots: V7 state history index frame checksum mismatch")
	}
	entries := make([]stateDomainChangeBinaryTxOffset, 0, frame.count)
	pos := 0
	read := func() (uint64, error) {
		value, n := binary.Uvarint(raw[pos:])
		if n <= 0 {
			return 0, errors.New("snapshots: invalid V7 state history index varint")
		}
		pos += n
		return value, nil
	}
	var previous stateDomainChangeBinaryTxOffset
	for i := uint32(0); i < frame.count; i++ {
		entry := stateDomainChangeBinaryTxOffset{}
		if i == 0 {
			entry.txNum = frame.firstTx
			var err error
			if entry.offset, err = read(); err != nil {
				return nil, err
			}
			if entry.recordIndex, err = read(); err != nil {
				return nil, err
			}
			if entry.count, err = read(); err != nil {
				return nil, err
			}
		} else {
			txDelta, err := read()
			if err != nil {
				return nil, err
			}
			offDelta, err := read()
			if err != nil {
				return nil, err
			}
			recordDelta, err := read()
			if err != nil {
				return nil, err
			}
			count, err := read()
			if err != nil {
				return nil, err
			}
			if txDelta == 0 || offDelta == 0 || recordDelta == 0 || previous.txNum > math.MaxUint64-txDelta || previous.offset > math.MaxUint64-offDelta || previous.recordIndex > math.MaxUint64-recordDelta {
				return nil, errors.New("snapshots: invalid V7 state history index delta")
			}
			entry = stateDomainChangeBinaryTxOffset{txNum: previous.txNum + txDelta, offset: previous.offset + offDelta, recordIndex: previous.recordIndex + recordDelta, count: count}
		}
		if entry.count == 0 || entry.txNum < r.header.fromTxNum || entry.txNum > r.header.toTxNum {
			return nil, errors.New("snapshots: V7 state history index entry outside range")
		}
		entries = append(entries, entry)
		previous = entry
	}
	if pos != len(raw) {
		return nil, errors.New("snapshots: V7 state history index frame trailing bytes")
	}
	return entries, nil
}

var _ historySegmentReader = (*stateDomainChangeBinaryIndexV7Reader)(nil)
