package snapshots

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
)

const (
	eventLogLookupV3Format = 3
	eventLogLookupV4Format = 4

	eventLogLookupV4HeaderSize = 64
	eventLogLookupV4BlockKeys  = 256
	eventLogLookupV4BlockTail  = 36 // dataOff(8)+dataLen(4)+rawLen(4)+checksum(4)+keyCount(4)+basePostingOff(8)+entryCRC(4)
	eventLogLookupV4StoredRaw  = uint32(1 << 31)
)

var eventLogLookupV4Magic = [8]byte{'g', 't', 'e', 'v', 'l', 'i', '4', '\n'}

type eventLogLookupV4Header struct {
	keySize, blockKeys      uint32
	keyCount, blockCount    uint64
	blockDirLen, keyDataLen uint64
	postingDataLen          uint64
}

type eventLogLookupV4Block struct {
	firstKey       []byte
	dataOff        uint64
	dataLen        uint32
	rawLen         uint32
	checksum       uint32
	keyCount       uint32
	basePostingOff uint64
	entryCRC       uint32
}

type eventLogLookupV4Record struct {
	key          []byte
	inlineSingle bool
	inlineRow    uint64
	postingOff   uint64
	postingLen   uint64
	postingCount uint64
	checksum     uint32
}

type eventLogLookupV4Build struct {
	keySize uint64
	keys    []eventLogV3LookupKey
	data    *os.File
	name    string
	dataLen uint64
	keyData *os.File
	keyName string
	keyLen  uint64
	blocks  []eventLogLookupV4Block
}

func (b *eventLogLookupV4Build) close() {
	if b == nil {
		return
	}
	if b.data != nil {
		_ = b.data.Close()
	}
	if b.name != "" {
		_ = os.Remove(b.name)
	}
	if b.keyData != nil {
		_ = b.keyData.Close()
	}
	if b.keyName != "" {
		_ = os.Remove(b.keyName)
	}
}

func (b *eventLogLookupV4Build) length() uint64 {
	if b == nil {
		return 0
	}
	return eventLogLookupV4HeaderSize + uint64(len(b.blocks))*(b.keySize+eventLogLookupV4BlockTail) + b.keyLen + b.dataLen
}

func detectEventLogCompactLookupFormat(file io.ReaderAt, offset, length, size uint64) (int, error) {
	if length < 8 || offset > size || length > size-offset || offset > math.MaxInt64 {
		return 0, errors.New("snapshots: invalid V3 lookup bounds")
	}
	var magic [8]byte
	if _, err := file.ReadAt(magic[:], int64(offset)); err != nil {
		return 0, err
	}
	switch magic {
	case eventLogV3LookupV2Magic:
		return eventLogLookupV3Format, nil
	case eventLogLookupV4Magic:
		return eventLogLookupV4Format, nil
	default:
		return 0, errors.New("snapshots: unsupported V3 lookup format; compact lookup magic required")
	}
}

func buildEventLogLookupV4(dir, base string, keySize int, postings map[string][]uint64) (*eventLogLookupV4Build, error) {
	data, name, err := createStateDomainChangeBinaryTempFileInDir(dir, base)
	if err != nil {
		return nil, err
	}
	keyData, keyName, err := createStateDomainChangeBinaryTempFileInDir(dir, base+"-keys")
	if err != nil {
		_ = data.Close()
		_ = os.Remove(name)
		return nil, err
	}
	b := &eventLogLookupV4Build{keySize: uint64(keySize), data: data, name: name, keyData: keyData, keyName: keyName}
	postingWriter := bufio.NewWriterSize(data, 1<<20)
	var postingOffset uint64
	keys := make([]string, 0, len(postings))
	for key := range postings {
		if len(key) != keySize {
			b.close()
			return nil, fmt.Errorf("snapshots: V5 lookup key length %d, want %d", len(key), keySize)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := postings[key]
		if len(values) == 0 {
			b.close()
			return nil, fmt.Errorf("snapshots: V5 lookup key %x has empty postings", []byte(key))
		}
		if len(values) == 1 {
			if values[0] > math.MaxUint64>>1 {
				b.close()
				return nil, errors.New("snapshots: V5 lookup inline row exceeds inline encoding limit")
			}
			b.keys = append(b.keys, eventLogV3LookupKey{
				key:          []byte(key),
				firstFrame:   values[0],
				frameCount:   0,
				postingCount: 1,
			})
			continue
		}
		raw := make([]byte, 0, len(values)*2)
		for i, value := range values {
			if i == 0 {
				raw = binary.AppendUvarint(raw, value)
			} else {
				if value <= values[i-1] {
					b.close()
					return nil, fmt.Errorf("snapshots: V5 lookup postings for %x are not strictly ordered", []byte(key))
				}
				raw = binary.AppendUvarint(raw, value-values[i-1])
			}
		}
		if uint64(len(raw)) > math.MaxUint32 {
			b.close()
			return nil, errors.New("snapshots: V5 lookup posting stream exceeds uint32 length limit")
		}
		if _, err := postingWriter.Write(raw); err != nil {
			b.close()
			return nil, err
		}
		b.keys = append(b.keys, eventLogV3LookupKey{
			key:          []byte(key),
			firstFrame:   postingOffset,
			frameCount:   uint32(len(raw)),
			postingCount: uint64(len(values)),
			checksum:     crc32.ChecksumIEEE(raw),
		})
		postingOffset += uint64(len(raw))
	}
	if err := postingWriter.Flush(); err != nil {
		b.close()
		return nil, err
	}
	stat, err := data.Stat()
	if err != nil {
		b.close()
		return nil, err
	}
	b.dataLen = uint64(stat.Size())
	if b.dataLen != postingOffset {
		b.close()
		return nil, errors.New("snapshots: V5 compact lookup posting length mismatch")
	}
	var blockBasePostingOff uint64
	for start := 0; start < len(b.keys); start += eventLogLookupV4BlockKeys {
		end := min(start+eventLogLookupV4BlockKeys, len(b.keys))
		blockKeys := b.keys[start:end]
		raw, nextPostingOff, err := encodeEventLogLookupV4KeyBlock(blockKeys, blockBasePostingOff)
		if err != nil {
			b.close()
			return nil, err
		}
		if len(raw) >= int(eventLogLookupV4StoredRaw) {
			b.close()
			return nil, errors.New("snapshots: V5 compact lookup key block exceeds uint32 length limit")
		}
		stored, storedLen, err := compressEventLogLookupV4KeyBlock(raw)
		if err != nil {
			b.close()
			return nil, err
		}
		off, err := keyData.Seek(0, io.SeekCurrent)
		if err != nil {
			b.close()
			return nil, err
		}
		if _, err := keyData.Write(stored); err != nil {
			b.close()
			return nil, err
		}
		b.blocks = append(b.blocks, eventLogLookupV4Block{
			firstKey:       append([]byte(nil), blockKeys[0].key...),
			dataOff:        uint64(off),
			dataLen:        storedLen,
			rawLen:         uint32(len(raw)),
			checksum:       crc32.ChecksumIEEE(raw),
			keyCount:       uint32(end - start),
			basePostingOff: blockBasePostingOff,
		})
		blockBasePostingOff = nextPostingOff
	}
	keyStat, err := keyData.Stat()
	if err != nil {
		b.close()
		return nil, err
	}
	b.keyLen = uint64(keyStat.Size())
	return b, nil
}

func encodeEventLogLookupV4KeyBlock(keys []eventLogV3LookupKey, basePostingOff uint64) ([]byte, uint64, error) {
	var raw []byte
	var previous []byte
	postingOff := basePostingOff
	for _, key := range keys {
		prefix := commonPrefixLength(previous, key.key)
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = append(raw, key.key[prefix:]...)
		if key.postingCount == 1 {
			if key.firstFrame > math.MaxUint64>>1 {
				return nil, 0, errors.New("snapshots: V5 lookup inline row overflows tag")
			}
			raw = binary.AppendUvarint(raw, key.firstFrame<<1)
		} else {
			if key.firstFrame != postingOff {
				return nil, 0, errors.New("snapshots: V5 lookup external postings are not contiguous")
			}
			if key.postingCount < 2 || key.postingCount-2 > (math.MaxUint64-1)>>1 {
				return nil, 0, errors.New("snapshots: V5 lookup external count overflows tag")
			}
			raw = binary.AppendUvarint(raw, ((key.postingCount-2)<<1)|1)
			raw = binary.AppendUvarint(raw, uint64(key.frameCount))
			raw = binary.BigEndian.AppendUint32(raw, key.checksum)
			postingOff += uint64(key.frameCount)
		}
		previous = key.key
	}
	return raw, postingOff, nil
}

// Lookup V4 tries Zstd unconditionally on every key block (no checksum-repetition
// heuristic needed) and keeps compressed bytes whenever strictly smaller than raw.
func compressEventLogLookupV4KeyBlock(raw []byte) ([]byte, uint32, error) {
	rawLength := uint32(len(raw)) | eventLogLookupV4StoredRaw
	if len(raw) == 0 {
		return raw, rawLength, nil
	}
	enc, _, err := cbCodec()
	if err != nil {
		return nil, 0, err
	}
	compressed := enc.EncodeAll(raw, nil)
	if len(compressed) >= len(raw) || len(compressed) >= int(eventLogLookupV4StoredRaw) {
		return raw, rawLength, nil
	}
	return compressed, uint32(len(compressed)), nil
}

func writeEventLogLookupV4(w *os.File, sectionOffset uint64, b *eventLogLookupV4Build) error {
	var header [eventLogLookupV4HeaderSize]byte
	copy(header[:8], eventLogLookupV4Magic[:])
	binary.BigEndian.PutUint32(header[8:12], uint32(b.keySize))
	binary.BigEndian.PutUint32(header[12:16], eventLogLookupV4BlockKeys)
	binary.BigEndian.PutUint64(header[16:24], uint64(len(b.keys)))
	binary.BigEndian.PutUint64(header[24:32], uint64(len(b.blocks)))
	blockDirLen := uint64(len(b.blocks)) * (b.keySize + eventLogLookupV4BlockTail)
	binary.BigEndian.PutUint64(header[32:40], blockDirLen)
	binary.BigEndian.PutUint64(header[40:48], b.keyLen)
	binary.BigEndian.PutUint64(header[48:56], b.dataLen)
	binary.BigEndian.PutUint32(header[56:60], crc32.ChecksumIEEE(header[:56]))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	keyDataBase := sectionOffset + eventLogLookupV4HeaderSize + blockDirLen
	for _, block := range b.blocks {
		if _, err := w.Write(block.firstKey); err != nil {
			return err
		}
		var tail [eventLogLookupV4BlockTail]byte
		binary.BigEndian.PutUint64(tail[0:8], keyDataBase+block.dataOff)
		binary.BigEndian.PutUint32(tail[8:12], block.dataLen)
		binary.BigEndian.PutUint32(tail[12:16], block.rawLen)
		binary.BigEndian.PutUint32(tail[16:20], block.checksum)
		binary.BigEndian.PutUint32(tail[20:24], block.keyCount)
		binary.BigEndian.PutUint64(tail[24:32], block.basePostingOff)
		entryCRC := crc32.NewIEEE()
		_, _ = entryCRC.Write(block.firstKey)
		_, _ = entryCRC.Write(tail[:32])
		binary.BigEndian.PutUint32(tail[32:36], entryCRC.Sum32())
		if _, err := w.Write(tail[:]); err != nil {
			return err
		}
	}
	if err := copyEventLogV3Temp(w, b.keyData); err != nil {
		return err
	}
	return copyEventLogV3Temp(w, b.data)
}

func readEventLogLookupV4Header(file io.ReaderAt, offset, length, size uint64, wantKeySize int) (eventLogLookupV4Header, error) {
	if length < eventLogLookupV4HeaderSize || offset > size || length > size-offset || offset > math.MaxInt64 {
		return eventLogLookupV4Header{}, errors.New("snapshots: invalid V5 compact lookup bounds")
	}
	var raw [eventLogLookupV4HeaderSize]byte
	if _, err := file.ReadAt(raw[:], int64(offset)); err != nil {
		return eventLogLookupV4Header{}, err
	}
	if !bytes.Equal(raw[:8], eventLogLookupV4Magic[:]) {
		return eventLogLookupV4Header{}, errors.New("snapshots: invalid V5 compact lookup magic")
	}
	if binary.BigEndian.Uint32(raw[56:60]) != crc32.ChecksumIEEE(raw[:56]) {
		return eventLogLookupV4Header{}, errors.New("snapshots: V5 compact lookup header checksum mismatch")
	}
	if !bytes.Equal(raw[60:64], []byte{0, 0, 0, 0}) {
		return eventLogLookupV4Header{}, errors.New("snapshots: V5 compact lookup header reserved bytes are non-zero")
	}
	h := eventLogLookupV4Header{
		keySize:        binary.BigEndian.Uint32(raw[8:12]),
		blockKeys:      binary.BigEndian.Uint32(raw[12:16]),
		keyCount:       binary.BigEndian.Uint64(raw[16:24]),
		blockCount:     binary.BigEndian.Uint64(raw[24:32]),
		blockDirLen:    binary.BigEndian.Uint64(raw[32:40]),
		keyDataLen:     binary.BigEndian.Uint64(raw[40:48]),
		postingDataLen: binary.BigEndian.Uint64(raw[48:56]),
	}
	if h.keySize != uint32(wantKeySize) || h.blockKeys != eventLogLookupV4BlockKeys {
		return eventLogLookupV4Header{}, errors.New("snapshots: unsupported V5 compact lookup framing")
	}
	wantBlocks := ceilDiv(h.keyCount, uint64(h.blockKeys))
	if h.blockCount != wantBlocks {
		return eventLogLookupV4Header{}, errors.New("snapshots: invalid V5 compact lookup block count")
	}
	dirLen, overflow := checkedMul(h.blockCount, uint64(h.keySize)+eventLogLookupV4BlockTail)
	if overflow || dirLen != h.blockDirLen {
		return eventLogLookupV4Header{}, errors.New("snapshots: invalid V5 compact lookup directory length")
	}
	total, overflow := checkedAdd(eventLogLookupV4HeaderSize, h.blockDirLen)
	if overflow {
		return eventLogLookupV4Header{}, errors.New("snapshots: V5 compact lookup length overflow")
	}
	total, overflow = checkedAdd(total, h.keyDataLen)
	if overflow {
		return eventLogLookupV4Header{}, errors.New("snapshots: V5 compact lookup length overflow")
	}
	total, overflow = checkedAdd(total, h.postingDataLen)
	if overflow || total != length {
		return eventLogLookupV4Header{}, errors.New("snapshots: invalid V5 compact lookup section length")
	}
	return h, nil
}

func readEventLogLookupV4Block(file io.ReaderAt, offset uint64, h eventLogLookupV4Header, index uint64) (eventLogLookupV4Block, error) {
	if index >= h.blockCount {
		return eventLogLookupV4Block{}, errors.New("snapshots: V5 compact lookup block outside directory")
	}
	entrySize := uint64(h.keySize) + eventLogLookupV4BlockTail
	entryOff := offset + eventLogLookupV4HeaderSize + index*entrySize
	raw := make([]byte, entrySize)
	if _, err := file.ReadAt(raw, int64(entryOff)); err != nil {
		return eventLogLookupV4Block{}, err
	}
	tail := raw[h.keySize:]
	block := eventLogLookupV4Block{
		firstKey:       append([]byte(nil), raw[:h.keySize]...),
		dataOff:        binary.BigEndian.Uint64(tail[0:8]),
		dataLen:        binary.BigEndian.Uint32(tail[8:12]),
		rawLen:         binary.BigEndian.Uint32(tail[12:16]),
		checksum:       binary.BigEndian.Uint32(tail[16:20]),
		keyCount:       binary.BigEndian.Uint32(tail[20:24]),
		basePostingOff: binary.BigEndian.Uint64(tail[24:32]),
		entryCRC:       binary.BigEndian.Uint32(tail[32:36]),
	}
	entryCRC := crc32.NewIEEE()
	_, _ = entryCRC.Write(raw[:h.keySize])
	_, _ = entryCRC.Write(tail[:32])
	if block.entryCRC != entryCRC.Sum32() {
		return eventLogLookupV4Block{}, errors.New("snapshots: V5 compact lookup directory entry checksum mismatch")
	}
	if block.keyCount == 0 || block.keyCount > h.blockKeys {
		return eventLogLookupV4Block{}, errors.New("snapshots: invalid V5 compact lookup key count")
	}
	if block.basePostingOff > h.postingDataLen {
		return eventLogLookupV4Block{}, errors.New("snapshots: V5 compact lookup base posting offset outside section")
	}
	maxRawLen := uint64(block.keyCount) * (uint64(h.keySize) + 3*binary.MaxVarintLen64 + 4)
	if uint64(block.rawLen) > maxRawLen {
		return eventLogLookupV4Block{}, errors.New("snapshots: V5 compact lookup key block decoded length is excessive")
	}
	return block, nil
}

func readEventLogLookupV4KeyBlockBytes(file io.ReaderAt, offset, length uint64, h eventLogLookupV4Header, block eventLogLookupV4Block, storedScratch, decodedScratch *[]byte) ([]byte, uint64, error) {
	keyDataStart := offset + eventLogLookupV4HeaderSize + h.blockDirLen
	postingStart := keyDataStart + h.keyDataLen
	storedLen := uint64(block.dataLen &^ eventLogLookupV4StoredRaw)
	if block.dataOff < keyDataStart || block.dataOff > postingStart || storedLen > postingStart-block.dataOff || postingStart > offset+length {
		return nil, 0, errors.New("snapshots: V5 compact lookup key block outside key data")
	}
	if storedLen > uint64(math.MaxInt) {
		return nil, 0, errors.New("snapshots: V5 compact lookup key block exceeds platform limits")
	}
	if cap(*storedScratch) < int(storedLen) {
		*storedScratch = make([]byte, int(storedLen))
	}
	stored := (*storedScratch)[:int(storedLen)]
	if _, err := file.ReadAt(stored, int64(block.dataOff)); err != nil {
		return nil, 0, err
	}
	decoded := stored
	if block.dataLen&eventLogLookupV4StoredRaw == 0 {
		_, decoder, err := cbCodec()
		if err != nil {
			return nil, 0, err
		}
		if cap(*decodedScratch) < int(block.rawLen) {
			*decodedScratch = make([]byte, 0, int(block.rawLen))
		}
		decoded, err = decoder.DecodeAll(stored, (*decodedScratch)[:0])
		if err != nil {
			return nil, 0, err
		}
		*decodedScratch = decoded
	}
	if uint64(len(decoded)) != uint64(block.rawLen) || crc32.ChecksumIEEE(decoded) != block.checksum {
		return nil, 0, errors.New("snapshots: V5 compact lookup key block checksum mismatch")
	}
	return decoded, storedLen, nil
}

func readEventLogLookupV4Records(file io.ReaderAt, offset, length uint64, h eventLogLookupV4Header, block eventLogLookupV4Block) ([]eventLogLookupV4Record, uint64, error) {
	var storedScratch, decodedScratch []byte
	decoded, storedLen, err := readEventLogLookupV4KeyBlockBytes(file, offset, length, h, block, &storedScratch, &decodedScratch)
	if err != nil {
		return nil, 0, err
	}
	keyDataStart := offset + eventLogLookupV4HeaderSize + h.blockDirLen
	postingStart := keyDataStart + h.keyDataLen
	cursor := 0
	postingCursor := block.basePostingOff
	records := make([]eventLogLookupV4Record, 0, block.keyCount)
	var previous []byte
	for i := uint32(0); i < block.keyCount; i++ {
		prefix, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
		if err != nil || prefix > uint64(len(previous)) || prefix > uint64(h.keySize) || (i == 0 && prefix != 0) {
			return nil, 0, errors.New("snapshots: invalid V5 compact lookup key prefix")
		}
		suffixLen := int(h.keySize) - int(prefix)
		if suffixLen > len(decoded)-cursor {
			return nil, 0, io.ErrUnexpectedEOF
		}
		key := make([]byte, h.keySize)
		copy(key, previous[:prefix])
		copy(key[prefix:], decoded[cursor:cursor+suffixLen])
		cursor += suffixLen

		tag, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
		if err != nil {
			return nil, 0, err
		}
		rec := eventLogLookupV4Record{key: key}
		if tag&1 == 0 {
			rec.inlineSingle = true
			rec.inlineRow = tag >> 1
			rec.postingCount = 1
		} else {
			countDelta := tag >> 1
			if countDelta > math.MaxUint64-2 {
				return nil, 0, errors.New("snapshots: V5 compact lookup posting count overflows")
			}
			postingCount := countDelta + 2
			postingLen, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
			if err != nil {
				return nil, 0, err
			}
			if len(decoded)-cursor < 4 {
				return nil, 0, io.ErrUnexpectedEOF
			}
			checksum := binary.BigEndian.Uint32(decoded[cursor : cursor+4])
			cursor += 4
			if postingCursor > h.postingDataLen || postingLen > h.postingDataLen-postingCursor || postingStart+postingCursor+postingLen > offset+length {
				return nil, 0, errors.New("snapshots: V5 compact lookup postings outside section")
			}
			if postingCount > postingLen {
				return nil, 0, errors.New("snapshots: V5 compact lookup posting count exceeds encoded bytes")
			}
			rec.postingOff = postingCursor
			rec.postingLen = postingLen
			rec.postingCount = postingCount
			rec.checksum = checksum
			postingCursor += postingLen
		}
		if i == 0 && !bytes.Equal(key, block.firstKey) {
			return nil, 0, errors.New("snapshots: V5 compact lookup first key mismatch")
		}
		if i > 0 && bytes.Compare(previous, key) >= 0 {
			return nil, 0, errors.New("snapshots: V5 compact lookup keys not sorted")
		}
		records = append(records, rec)
		previous = key
	}
	if cursor != len(decoded) {
		return nil, 0, errors.New("snapshots: V5 compact lookup key block trailing bytes")
	}
	return records, storedLen, nil
}

func readEventLogLookupV4RecordRows(file io.ReaderAt, offset, length uint64, h eventLogLookupV4Header, record eventLogLookupV4Record, maxRows uint64) ([]uint64, error) {
	if record.inlineSingle {
		if record.inlineRow >= maxRows {
			return nil, errors.New("snapshots: V5 compact lookup inline posting row outside segment")
		}
		return []uint64{record.inlineRow}, nil
	}
	if record.postingCount > maxRows || record.postingCount > uint64(math.MaxInt/8) {
		return nil, errors.New("snapshots: V5 compact lookup posting count exceeds safe row limit")
	}
	postingStart := offset + eventLogLookupV4HeaderSize + h.blockDirLen + h.keyDataLen
	raw, err := readEventLogPayloadAt(file, postingStart+record.postingOff, record.postingLen, offset+length)
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(raw) != record.checksum {
		return nil, errors.New("snapshots: V5 compact lookup posting checksum mismatch")
	}
	rows := make([]uint64, 0, record.postingCount)
	br := bytes.NewReader(raw)
	var previous uint64
	for i := uint64(0); i < record.postingCount; i++ {
		value, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			if value == 0 || math.MaxUint64-previous < value {
				return nil, errors.New("snapshots: V5 compact lookup posting delta invalid")
			}
			value += previous
		}
		if value >= maxRows {
			return nil, errors.New("snapshots: V5 compact lookup posting row outside segment")
		}
		rows = append(rows, value)
		previous = value
	}
	if br.Len() != 0 {
		return nil, errors.New("snapshots: V5 compact lookup posting trailing bytes")
	}
	return rows, nil
}

func readEventLogLookupV4Rows(file io.ReaderAt, offset, length, size uint64, keySize int, want []byte, maxRows uint64) ([]uint64, error) {
	h, err := readEventLogLookupV4Header(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	lo, hi := uint64(0), h.blockCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		block, err := readEventLogLookupV4Block(file, offset, h, mid)
		if err != nil {
			return nil, err
		}
		if bytes.Compare(block.firstKey, want) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return nil, nil
	}
	block, err := readEventLogLookupV4Block(file, offset, h, lo-1)
	if err != nil {
		return nil, err
	}
	records, _, err := readEventLogLookupV4Records(file, offset, length, h, block)
	if err != nil {
		return nil, err
	}
	idx := sort.Search(len(records), func(i int) bool { return bytes.Compare(records[i].key, want) >= 0 })
	if idx == len(records) || !bytes.Equal(records[idx].key, want) {
		return nil, nil
	}
	return readEventLogLookupV4RecordRows(file, offset, length, h, records[idx], maxRows)
}

func readAllEventLogLookupV4(file io.ReaderAt, offset, length, size uint64, keySize int, maxRows uint64) (map[string][]uint64, error) {
	h, err := readEventLogLookupV4Header(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]uint64, h.keyCount)
	var previous []byte
	expectedKeyOff := offset + eventLogLookupV4HeaderSize + h.blockDirLen
	var expectedPostingOff uint64
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogLookupV4Block(file, offset, h, i)
		if err != nil {
			return nil, err
		}
		storedLen := uint64(block.dataLen &^ eventLogLookupV4StoredRaw)
		if block.dataOff != expectedKeyOff {
			return nil, errors.New("snapshots: V5 compact lookup key blocks are not contiguous")
		}
		if block.basePostingOff != expectedPostingOff {
			return nil, errors.New("snapshots: V5 compact lookup base posting offset mismatch")
		}
		expectedKeyOff += storedLen
		records, _, err := readEventLogLookupV4Records(file, offset, length, h, block)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if len(previous) != 0 && bytes.Compare(previous, record.key) >= 0 {
				return nil, errors.New("snapshots: V5 compact lookup blocks not strictly sorted")
			}
			if !record.inlineSingle {
				if record.postingOff != expectedPostingOff {
					return nil, errors.New("snapshots: V5 compact lookup postings are not contiguous")
				}
				expectedPostingOff += record.postingLen
			}
			rows, err := readEventLogLookupV4RecordRows(file, offset, length, h, record, maxRows)
			if err != nil {
				return nil, err
			}
			out[string(record.key)] = rows
			previous = record.key
		}
	}
	if uint64(len(out)) != h.keyCount {
		return nil, errors.New("snapshots: V5 compact lookup key count mismatch")
	}
	if expectedKeyOff != offset+eventLogLookupV4HeaderSize+h.blockDirLen+h.keyDataLen || expectedPostingOff != h.postingDataLen {
		return nil, errors.New("snapshots: V5 compact lookup data coverage mismatch")
	}
	return out, nil
}

func readEventLogLookupV4Keys(file io.ReaderAt, offset, length, size uint64, keySize int) ([][]byte, error) {
	h, err := readEventLogLookupV4Header(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, h.keyCount)
	var previous []byte
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogLookupV4Block(file, offset, h, i)
		if err != nil {
			return nil, err
		}
		records, _, err := readEventLogLookupV4Records(file, offset, length, h, block)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if len(previous) != 0 && bytes.Compare(previous, record.key) >= 0 {
				return nil, errors.New("snapshots: V5 compact lookup blocks not strictly sorted")
			}
			keys = append(keys, record.key)
			previous = record.key
		}
	}
	if uint64(len(keys)) != h.keyCount {
		return nil, errors.New("snapshots: V5 compact lookup key count mismatch")
	}
	return keys, nil
}

func walkEventLogLookupV4(file io.ReaderAt, offset, length, size uint64, keySize int, maxRows uint64, visit func([]byte, []uint64) error) error {
	h, err := readEventLogLookupV4Header(file, offset, length, size, keySize)
	if err != nil {
		return err
	}
	expectedKeyOff := offset + eventLogLookupV4HeaderSize + h.blockDirLen
	var expectedPostingOff, visited uint64
	var previous []byte
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogLookupV4Block(file, offset, h, i)
		if err != nil {
			return err
		}
		storedLen := uint64(block.dataLen &^ eventLogLookupV4StoredRaw)
		if block.dataOff != expectedKeyOff {
			return errors.New("snapshots: V5 compact lookup key blocks are not contiguous")
		}
		if block.basePostingOff != expectedPostingOff {
			return errors.New("snapshots: V5 compact lookup base posting offset mismatch")
		}
		expectedKeyOff += storedLen
		records, _, err := readEventLogLookupV4Records(file, offset, length, h, block)
		if err != nil {
			return err
		}
		for _, record := range records {
			if len(previous) != 0 && bytes.Compare(previous, record.key) >= 0 {
				return errors.New("snapshots: V5 compact lookup keys are not strictly sorted")
			}
			if !record.inlineSingle {
				if record.postingOff != expectedPostingOff {
					return errors.New("snapshots: V5 compact lookup postings are not contiguous")
				}
				expectedPostingOff += record.postingLen
			}
			rows, err := readEventLogLookupV4RecordRows(file, offset, length, h, record, maxRows)
			if err != nil {
				return err
			}
			if visit != nil {
				if err := visit(record.key, rows); err != nil {
					return err
				}
			}
			previous = record.key
			visited++
		}
	}
	if visited != h.keyCount || expectedKeyOff != offset+eventLogLookupV4HeaderSize+h.blockDirLen+h.keyDataLen || expectedPostingOff != h.postingDataLen {
		return errors.New("snapshots: V5 compact lookup data coverage mismatch")
	}
	return nil
}

func checkEventLogLookupV4Postings(file io.ReaderAt, offset, length, size uint64, keySize int, maxRows uint64, name string, expected eventLogV4ExpectedPostings, topicPositions []uint64) error {
	h, err := readEventLogLookupV4Header(file, offset, length, size, keySize)
	if err != nil {
		return err
	}
	if h.keyCount != expected.keyCount() {
		return fmt.Errorf("snapshots: V5 %s lookup key count %d, want %d", name, h.keyCount, expected.keyCount())
	}
	expectedKeyOff := offset + eventLogLookupV4HeaderSize + h.blockDirLen
	var expectedPostingOff, keyID uint64
	var keyBlockStoredScratch, keyBlockDecodedScratch, postingScratch []byte
	previousKey := make([]byte, keySize)
	havePreviousKey := false
	postingDataStart := offset + eventLogLookupV4HeaderSize + h.blockDirLen + h.keyDataLen
	if postingDataStart > uint64(math.MaxInt64) || h.postingDataLen > uint64(math.MaxInt64) {
		return errors.New("snapshots: V5 compact lookup posting section exceeds int64 limits")
	}
	postingSource := eventLogV4ValidationPostingSource{source: io.NewSectionReader(file, int64(postingDataStart), int64(h.postingDataLen))}
	postingReader := acquireEventLogV4ValidationReader(&postingSource)
	defer releaseEventLogV4ValidationReader(&postingReader)
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogLookupV4Block(file, offset, h, i)
		if err != nil {
			return err
		}
		storedLen := uint64(block.dataLen &^ eventLogLookupV4StoredRaw)
		if block.dataOff != expectedKeyOff {
			return fmt.Errorf("snapshots: V5 %s lookup key blocks are not contiguous", name)
		}
		if block.basePostingOff != expectedPostingOff {
			return fmt.Errorf("snapshots: V5 %s lookup block base posting offset mismatch", name)
		}
		expectedKeyOff += storedLen
		decoded, _, err := readEventLogLookupV4KeyBlockBytes(file, offset, length, h, block, &keyBlockStoredScratch, &keyBlockDecodedScratch)
		if err != nil {
			return err
		}
		cursor := 0
		blockPreviousKey := make([]byte, keySize)
		blockKey := make([]byte, keySize)
		blockHasPrevious := false
		for within := uint32(0); within < block.keyCount; within++ {
			prefix, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
			if err != nil || prefix > uint64(keySize) || (!blockHasPrevious && prefix != 0) {
				return errors.New("snapshots: invalid V5 compact lookup key prefix")
			}
			if blockHasPrevious && prefix > uint64(len(blockPreviousKey)) {
				return errors.New("snapshots: invalid V5 compact lookup key prefix")
			}
			suffixLength := keySize - int(prefix)
			if suffixLength > len(decoded)-cursor {
				return io.ErrUnexpectedEOF
			}
			copy(blockKey[:prefix], blockPreviousKey[:prefix])
			copy(blockKey[prefix:], decoded[cursor:cursor+suffixLength])
			cursor += suffixLength

			tag, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
			if err != nil {
				return err
			}
			if within == 0 && !bytes.Equal(blockKey, block.firstKey) {
				return errors.New("snapshots: V5 compact lookup first key mismatch")
			}
			if blockHasPrevious && bytes.Compare(blockPreviousKey, blockKey) >= 0 {
				return errors.New("snapshots: V5 compact lookup keys not sorted")
			}
			if keyID >= expected.keyCount() {
				return fmt.Errorf("snapshots: V5 %s lookup contains too many keys", name)
			}
			if havePreviousKey && bytes.Compare(previousKey, blockKey) >= 0 {
				return fmt.Errorf("snapshots: V5 %s lookup blocks are not strictly sorted", name)
			}
			want := expected.rowsFor(keyID)
			if len(want) == 0 {
				return fmt.Errorf("snapshots: V5 %s lookup dictionary id %d is unused", name, keyID)
			}
			if topicPositions != nil {
				position := binary.BigEndian.Uint64(blockKey[:8])
				if keyID >= uint64(len(topicPositions)) || topicPositions[keyID] == math.MaxUint64 || position != topicPositions[keyID] {
					return fmt.Errorf("snapshots: V5 topic dictionary position mismatch at id %d", keyID)
				}
			}
			if tag&1 == 0 {
				inlineRow := tag >> 1
				if len(want) != 1 {
					return fmt.Errorf("snapshots: V5 %s lookup posting count for dictionary id %d is 1, want %d", name, keyID, len(want))
				}
				if inlineRow >= maxRows || inlineRow != want[0] {
					return fmt.Errorf("snapshots: V5 %s lookup dictionary id %d inline row %d, want %d", name, keyID, inlineRow, want[0])
				}
			} else {
				countDelta := tag >> 1
				if countDelta > math.MaxUint64-2 {
					return errors.New("snapshots: V5 compact lookup posting count overflows")
				}
				postingCount := countDelta + 2
				postingLen, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
				if err != nil {
					return err
				}
				if len(decoded)-cursor < 4 {
					return io.ErrUnexpectedEOF
				}
				postingChecksum := binary.BigEndian.Uint32(decoded[cursor : cursor+4])
				cursor += 4
				if expectedPostingOff > h.postingDataLen || postingLen > h.postingDataLen-expectedPostingOff || postingCount > postingLen {
					return errors.New("snapshots: V5 compact lookup postings outside section")
				}
				if postingCount != uint64(len(want)) {
					return fmt.Errorf("snapshots: V5 %s lookup posting count for dictionary id %d is %d, want %d", name, keyID, postingCount, len(want))
				}
				expectedPostingOff += postingLen
				if err := checkEventLogV4PostingRows(postingReader, postingLen, postingChecksum, maxRows, want, &postingScratch); err != nil {
					return fmt.Errorf("snapshots: V5 %s lookup dictionary id %d: %w", name, keyID, err)
				}
			}
			copy(previousKey, blockKey)
			havePreviousKey = true
			blockPreviousKey, blockKey = blockKey, blockPreviousKey
			blockHasPrevious = true
			keyID++
		}
		if cursor != len(decoded) {
			return errors.New("snapshots: V5 compact lookup key block trailing bytes")
		}
	}
	if keyID != h.keyCount {
		return fmt.Errorf("snapshots: V5 %s lookup key count mismatch", name)
	}
	if expectedKeyOff != offset+eventLogLookupV4HeaderSize+h.blockDirLen+h.keyDataLen || expectedPostingOff != h.postingDataLen {
		return fmt.Errorf("snapshots: V5 %s lookup data coverage mismatch", name)
	}
	return nil
}

func maxEventLogLookupV4KeyAtRead(file io.ReaderAt, offset, length, size uint64, keySize int) (uint64, error) {
	h, err := readEventLogLookupV4Header(file, offset, length, size, keySize)
	if err != nil {
		return 0, err
	}
	entrySize := uint64(keySize + eventLogLookupV4BlockTail)
	var maximum uint64
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogLookupV4Block(file, offset, h, i)
		if err != nil {
			return 0, err
		}
		maximum = max(maximum, entrySize+uint64(block.dataLen&^eventLogLookupV4StoredRaw))
	}
	return maximum, nil
}

func maxEventLogLookupV4Read(file io.ReaderAt, offset, length, size uint64, keySize int) (uint64, error) {
	h, err := readEventLogLookupV4Header(file, offset, length, size, keySize)
	if err != nil {
		return 0, err
	}
	entrySize := uint64(keySize + eventLogLookupV4BlockTail)
	var searchReads uint64
	for n := h.blockCount; n > 0; n >>= 1 {
		searchReads += entrySize
	}
	var maximum uint64
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogLookupV4Block(file, offset, h, i)
		if err != nil {
			return 0, err
		}
		records, storedBytes, err := readEventLogLookupV4Records(file, offset, length, h, block)
		if err != nil {
			return 0, err
		}
		for _, record := range records {
			maximum = max(maximum, searchReads+entrySize+storedBytes+record.postingLen)
		}
	}
	return maximum, nil
}
