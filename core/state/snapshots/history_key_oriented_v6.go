package snapshots

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	stateDomainChangeBinaryV6DictionaryCommitmentSize = sha256.Size
	stateDomainChangeBinaryAccessorV6HeaderSize       = 112
	stateDomainChangeBinaryAccessorV6BlockKeys        = 128
	stateDomainChangeBinaryAccessorV6BlockTail        = 28
	stateDomainChangeBinaryAccessorV6PostingSize      = 18
	stateDomainChangeBinaryAccessorV6StoredRaw        = uint32(1 << 31)
)

func writeStateDomainChangeBinaryV6DictionaryCommitment(w io.Writer, digest [sha256.Size]byte) error {
	_, err := w.Write(digest[:])
	return err
}

func readStateDomainChangeBinaryV6DictionaryCommitment(r io.ReaderAt) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	_, err := r.ReadAt(digest[:], stateDomainChangeBinaryHeaderSize)
	return digest, err
}

type stateDomainChangeBinaryAccessorV6Posting struct {
	txNum       uint64
	offset      uint64
	recordIndex uint32
}

type stateDomainChangeBinaryAccessorV6Key struct {
	key          []byte
	keyID        uint32
	postingOff   uint64
	postings     []stateDomainChangeBinaryAccessorV6Posting
	postingCount uint32
}

type stateDomainChangeBinaryAccessorV6Block struct {
	firstKey []byte
	dataOff  uint64
	dataLen  uint32
	rawLen   uint32
	checksum uint32
	keyCount uint32
}

type stateDomainChangeBinaryAccessorV6Header struct {
	version                             uint32
	headerSize                          uint64
	fromTxNum, toTxNum                  uint64
	recordCount, keyCount, blockCount   uint64
	blockDirLen, keyDataLen, postingLen uint64
	dictionaryDigest                    [sha256.Size]byte
}

type stateDomainChangeBinaryAccessorV6Record struct {
	key        []byte
	keyID      uint32
	postingOff uint64
	postings   uint32
}

func stateDomainChangeBinaryAccessorV6Keys(entries []stateDomainChangeBinaryAccessorEntry) ([]stateDomainChangeBinaryAccessorV6Key, []uint32, error) {
	if len(entries) > math.MaxUint32 {
		return nil, nil, fmt.Errorf("snapshots: state-domain-change V6 record count %d exceeds uint32", len(entries))
	}
	byKey := make(map[string][]stateDomainChangeBinaryAccessorV6Posting)
	for i, entry := range entries {
		if entry.offset > stateDomainChangeBinaryAccessorV5MaxOffset {
			return nil, nil, fmt.Errorf("snapshots: state-domain-change V6 offset %d exceeds 48 bits", entry.offset)
		}
		byKey[string(entry.key)] = append(byKey[string(entry.key)], stateDomainChangeBinaryAccessorV6Posting{
			txNum: entry.txNum, offset: entry.offset, recordIndex: uint32(entry.recordIndex),
		})
		_ = i
	}
	keyStrings := make([]string, 0, len(byKey))
	for key := range byKey {
		keyStrings = append(keyStrings, key)
	}
	sort.Strings(keyStrings)
	keys := make([]stateDomainChangeBinaryAccessorV6Key, len(keyStrings))
	ids := make(map[string]uint32, len(keyStrings))
	for i, key := range keyStrings {
		if len(key) > math.MaxUint16 {
			return nil, nil, fmt.Errorf("snapshots: state-domain-change V6 key length %d exceeds uint16", len(key))
		}
		postings := byKey[key]
		for j := 1; j < len(postings); j++ {
			if postings[j].txNum < postings[j-1].txNum || postings[j].recordIndex <= postings[j-1].recordIndex || postings[j].offset <= postings[j-1].offset {
				return nil, nil, fmt.Errorf("snapshots: state-domain-change V6 postings for key %x are not ordered", []byte(key))
			}
		}
		keys[i] = stateDomainChangeBinaryAccessorV6Key{key: []byte(key), keyID: uint32(i), postings: postings, postingCount: uint32(len(postings))}
		ids[key] = uint32(i)
	}
	recordIDs := make([]uint32, len(entries))
	for i, entry := range entries {
		recordIDs[i] = ids[string(entry.key)]
	}
	return keys, recordIDs, nil
}

func stateDomainChangeBinaryAccessorV6DictionaryDigest(keys []stateDomainChangeBinaryAccessorV6Key) [sha256.Size]byte {
	h := sha256.New()
	var length [4]byte
	for _, key := range keys {
		binary.BigEndian.PutUint32(length[:], uint32(len(key.key)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(key.key)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func encodeStateDomainChangeBinaryAccessorV6(fromTxNum, toTxNum uint64, entries []stateDomainChangeBinaryAccessorEntry) ([]byte, []uint32, error) {
	keys, recordIDs, err := stateDomainChangeBinaryAccessorV6Keys(entries)
	if err != nil {
		return nil, nil, err
	}
	dictionaryDigest := stateDomainChangeBinaryAccessorV6DictionaryDigest(keys)
	var postings bytes.Buffer
	var postingRaw [stateDomainChangeBinaryAccessorV6PostingSize]byte
	for i := range keys {
		keys[i].postingOff = uint64(postings.Len())
		for _, posting := range keys[i].postings {
			binary.BigEndian.PutUint64(postingRaw[:8], posting.txNum)
			if err := putStateDomainChangeBinaryAccessorV5Offset(postingRaw[8:14], posting.offset); err != nil {
				return nil, nil, err
			}
			binary.BigEndian.PutUint32(postingRaw[14:18], posting.recordIndex)
			postings.Write(postingRaw[:])
		}
	}
	var keyData bytes.Buffer
	blocks := make([]stateDomainChangeBinaryAccessorV6Block, 0, (len(keys)+stateDomainChangeBinaryAccessorV6BlockKeys-1)/stateDomainChangeBinaryAccessorV6BlockKeys)
	for start := 0; start < len(keys); start += stateDomainChangeBinaryAccessorV6BlockKeys {
		end := min(start+stateDomainChangeBinaryAccessorV6BlockKeys, len(keys))
		raw, err := encodeStateDomainChangeBinaryAccessorV6KeyBlock(keys[start:end])
		if err != nil {
			return nil, nil, err
		}
		if len(raw) >= int(stateDomainChangeBinaryAccessorV6StoredRaw) {
			return nil, nil, errors.New("snapshots: state-domain-change V6 key block exceeds uint32")
		}
		off := uint64(keyData.Len())
		keyData.Write(raw)
		blocks = append(blocks, stateDomainChangeBinaryAccessorV6Block{
			firstKey: append([]byte(nil), keys[start].key...), dataOff: off,
			dataLen: uint32(len(raw)) | stateDomainChangeBinaryAccessorV6StoredRaw,
			rawLen:  uint32(len(raw)), checksum: crc32.ChecksumIEEE(raw), keyCount: uint32(end - start),
		})
	}
	dirLen := uint64(0)
	for _, block := range blocks {
		dirLen += 2 + uint64(len(block.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	dirLen += uint64(len(blocks)) * 8
	var out bytes.Buffer
	var header [stateDomainChangeBinaryAccessorV6HeaderSize]byte
	copy(header[:8], stateDomainChangeBinaryAccessorMagic[:])
	binary.BigEndian.PutUint32(header[8:12], stateDomainChangeBinaryVersionV6)
	binary.BigEndian.PutUint64(header[12:20], fromTxNum)
	binary.BigEndian.PutUint64(header[20:28], toTxNum)
	binary.BigEndian.PutUint64(header[28:36], uint64(len(entries)))
	binary.BigEndian.PutUint32(header[36:40], stateDomainChangeBinaryAccessorV6BlockKeys)
	binary.BigEndian.PutUint64(header[40:48], uint64(len(keys)))
	binary.BigEndian.PutUint64(header[48:56], uint64(len(blocks)))
	binary.BigEndian.PutUint64(header[56:64], dirLen)
	binary.BigEndian.PutUint64(header[64:72], uint64(keyData.Len()))
	copy(header[72:104], dictionaryDigest[:])
	binary.BigEndian.PutUint32(header[104:108], crc32.ChecksumIEEE(header[:104]))
	out.Write(header[:])
	keyDataBase := uint64(stateDomainChangeBinaryAccessorV6HeaderSize) + dirLen
	entryOffset := uint64(stateDomainChangeBinaryAccessorV6HeaderSize) + uint64(len(blocks))*8
	for _, block := range blocks {
		writeUint64(&out, entryOffset)
		entryOffset += 2 + uint64(len(block.firstKey)) + stateDomainChangeBinaryAccessorV6BlockTail
	}
	var tail [stateDomainChangeBinaryAccessorV6BlockTail]byte
	for _, block := range blocks {
		if len(block.firstKey) > math.MaxUint16 {
			return nil, nil, fmt.Errorf("snapshots: state-domain-change V6 key length %d exceeds uint16", len(block.firstKey))
		}
		binary.Write(&out, binary.BigEndian, uint16(len(block.firstKey)))
		out.Write(block.firstKey)
		binary.BigEndian.PutUint64(tail[0:8], keyDataBase+block.dataOff)
		binary.BigEndian.PutUint32(tail[8:12], block.dataLen)
		binary.BigEndian.PutUint32(tail[12:16], block.rawLen)
		binary.BigEndian.PutUint32(tail[16:20], block.checksum)
		binary.BigEndian.PutUint32(tail[20:24], block.keyCount)
		entryCRC := crc32.NewIEEE()
		_, _ = entryCRC.Write(block.firstKey)
		_, _ = entryCRC.Write(tail[:24])
		binary.BigEndian.PutUint32(tail[24:28], entryCRC.Sum32())
		out.Write(tail[:])
	}
	out.Write(keyData.Bytes())
	out.Write(postings.Bytes())
	return out.Bytes(), recordIDs, nil
}

func encodeStateDomainChangeBinaryAccessorV6KeyBlock(keys []stateDomainChangeBinaryAccessorV6Key) ([]byte, error) {
	var raw []byte
	var previous []byte
	for _, key := range keys {
		prefix := commonPrefixLength(previous, key.key)
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = binary.AppendUvarint(raw, uint64(len(key.key)-prefix))
		raw = append(raw, key.key[prefix:]...)
		raw = binary.AppendUvarint(raw, key.postingOff)
		postingCount := key.postingCount
		if postingCount == 0 && len(key.postings) != 0 {
			postingCount = uint32(len(key.postings))
		}
		raw = binary.AppendUvarint(raw, uint64(postingCount))
		previous = key.key
	}
	return raw, nil
}

func decodeStateDomainChangeBinaryAccessorV6Header(r io.ReaderAt, fileSize uint64) (stateDomainChangeBinaryAccessorV6Header, error) {
	if fileSize < stateDomainChangeBinaryAccessorV6HeaderSize {
		return stateDomainChangeBinaryAccessorV6Header{}, io.ErrUnexpectedEOF
	}
	var raw [stateDomainChangeBinaryAccessorV6HeaderSize]byte
	if _, err := r.ReadAt(raw[:], 0); err != nil {
		return stateDomainChangeBinaryAccessorV6Header{}, err
	}
	if !bytes.Equal(raw[:8], stateDomainChangeBinaryAccessorMagic[:]) || binary.BigEndian.Uint32(raw[8:12]) != stateDomainChangeBinaryVersionV6 {
		return stateDomainChangeBinaryAccessorV6Header{}, errors.New("snapshots: invalid state-domain-change V6 accessor magic/version")
	}
	if binary.BigEndian.Uint32(raw[36:40]) != stateDomainChangeBinaryAccessorV6BlockKeys || binary.BigEndian.Uint32(raw[104:108]) != crc32.ChecksumIEEE(raw[:104]) || !bytes.Equal(raw[108:112], []byte{0, 0, 0, 0}) {
		return stateDomainChangeBinaryAccessorV6Header{}, errors.New("snapshots: invalid state-domain-change V6 accessor header")
	}
	h := stateDomainChangeBinaryAccessorV6Header{
		version: stateDomainChangeBinaryVersionV6, headerSize: stateDomainChangeBinaryAccessorV6HeaderSize,
		fromTxNum: binary.BigEndian.Uint64(raw[12:20]), toTxNum: binary.BigEndian.Uint64(raw[20:28]),
		recordCount: binary.BigEndian.Uint64(raw[28:36]), keyCount: binary.BigEndian.Uint64(raw[40:48]),
		blockCount: binary.BigEndian.Uint64(raw[48:56]), blockDirLen: binary.BigEndian.Uint64(raw[56:64]),
		keyDataLen: binary.BigEndian.Uint64(raw[64:72]),
	}
	copy(h.dictionaryDigest[:], raw[72:104])
	h.postingLen = h.recordCount * stateDomainChangeBinaryAccessorV6PostingSize
	if h.recordCount > math.MaxUint32 || h.keyCount > math.MaxUint32 || h.blockCount != ceilDiv(h.keyCount, stateDomainChangeBinaryAccessorV6BlockKeys) {
		return h, errors.New("snapshots: invalid state-domain-change V6 accessor counts")
	}
	total, overflow := checkedAdd(h.headerSize, h.blockDirLen)
	if overflow {
		return h, errors.New("snapshots: V6 accessor length overflow")
	}
	total, overflow = checkedAdd(total, h.keyDataLen)
	if overflow {
		return h, errors.New("snapshots: V6 accessor length overflow")
	}
	total, overflow = checkedAdd(total, h.postingLen)
	if overflow || total != fileSize {
		return h, errors.New("snapshots: invalid state-domain-change V6 accessor length")
	}
	if h.blockCount > math.MaxUint64/8 || h.blockDirLen < h.blockCount*8 {
		return h, errors.New("snapshots: invalid state-domain-change V6 accessor directory length")
	}
	return h, nil
}

func stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, index uint64) (stateDomainChangeBinaryAccessorV6Block, error) {
	if index >= h.blockCount {
		return stateDomainChangeBinaryAccessorV6Block{}, io.EOF
	}
	dirStart := h.headerSize
	entriesStart := dirStart + h.blockCount*8
	dirEnd := dirStart + h.blockDirLen
	var offRaw [8]byte
	if _, err := r.ReadAt(offRaw[:], int64(dirStart+index*8)); err != nil {
		return stateDomainChangeBinaryAccessorV6Block{}, err
	}
	off := binary.BigEndian.Uint64(offRaw[:])
	next := dirEnd
	if index+1 < h.blockCount {
		if _, err := r.ReadAt(offRaw[:], int64(dirStart+(index+1)*8)); err != nil {
			return stateDomainChangeBinaryAccessorV6Block{}, err
		}
		next = binary.BigEndian.Uint64(offRaw[:])
	}
	if off < entriesStart || off >= next || next > dirEnd || next-off < 2+stateDomainChangeBinaryAccessorV6BlockTail || index == 0 && off != entriesStart {
		return stateDomainChangeBinaryAccessorV6Block{}, errors.New("snapshots: invalid V6 accessor block directory offset")
	}
	var lenRaw [2]byte
	if _, err := r.ReadAt(lenRaw[:], int64(off)); err != nil {
		return stateDomainChangeBinaryAccessorV6Block{}, err
	}
	keyLen := uint64(binary.BigEndian.Uint16(lenRaw[:]))
	if keyLen == 0 || next-off != 2+keyLen+stateDomainChangeBinaryAccessorV6BlockTail {
		return stateDomainChangeBinaryAccessorV6Block{}, errors.New("snapshots: invalid V6 accessor block directory entry")
	}
	raw := make([]byte, keyLen+stateDomainChangeBinaryAccessorV6BlockTail)
	if _, err := r.ReadAt(raw, int64(off+2)); err != nil {
		return stateDomainChangeBinaryAccessorV6Block{}, err
	}
	key := append([]byte(nil), raw[:keyLen]...)
	tail := raw[keyLen:]
	entryCRC := crc32.NewIEEE()
	_, _ = entryCRC.Write(key)
	_, _ = entryCRC.Write(tail[:24])
	if binary.BigEndian.Uint32(tail[24:28]) != entryCRC.Sum32() {
		return stateDomainChangeBinaryAccessorV6Block{}, errors.New("snapshots: V6 accessor block directory checksum mismatch")
	}
	block := stateDomainChangeBinaryAccessorV6Block{
		firstKey: key, dataOff: binary.BigEndian.Uint64(tail[0:8]),
		dataLen: binary.BigEndian.Uint32(tail[8:12]), rawLen: binary.BigEndian.Uint32(tail[12:16]),
		checksum: binary.BigEndian.Uint32(tail[16:20]), keyCount: binary.BigEndian.Uint32(tail[20:24]),
	}
	if block.keyCount == 0 || block.keyCount > stateDomainChangeBinaryAccessorV6BlockKeys {
		return stateDomainChangeBinaryAccessorV6Block{}, errors.New("snapshots: invalid V6 accessor key block count")
	}
	return block, nil
}

func stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header) ([]stateDomainChangeBinaryAccessorV6Block, error) {
	blocks := make([]stateDomainChangeBinaryAccessorV6Block, 0, h.blockCount)
	var previous []byte
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, i)
		if err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(previous, block.firstKey) >= 0 {
			return nil, errors.New("snapshots: V6 accessor block keys not sorted")
		}
		blocks = append(blocks, block)
		previous = block.firstKey
	}
	return blocks, nil
}

func stateDomainChangeBinaryAccessorV6ReadBlock(r io.ReaderAt, fileSize uint64, h stateDomainChangeBinaryAccessorV6Header, block stateDomainChangeBinaryAccessorV6Block, firstKeyID uint32) ([]stateDomainChangeBinaryAccessorV6Record, error) {
	keyStart := h.headerSize + h.blockDirLen
	postingStart := keyStart + h.keyDataLen
	storedLen := uint64(block.dataLen &^ stateDomainChangeBinaryAccessorV6StoredRaw)
	if block.dataOff < keyStart || block.dataOff > postingStart || storedLen > postingStart-block.dataOff || postingStart+h.postingLen != fileSize {
		return nil, errors.New("snapshots: V6 accessor key block outside key data")
	}
	maxRaw := uint64(block.keyCount) * (math.MaxUint16 + 2*binary.MaxVarintLen64 + 2*binary.MaxVarintLen32)
	if uint64(block.rawLen) > h.keyDataLen || uint64(block.rawLen) > maxRaw {
		return nil, errors.New("snapshots: V6 accessor key block decoded length is excessive")
	}
	stored := make([]byte, storedLen)
	if _, err := r.ReadAt(stored, int64(block.dataOff)); err != nil {
		return nil, err
	}
	decoded := stored
	if block.dataLen&stateDomainChangeBinaryAccessorV6StoredRaw == 0 {
		_, dec, err := cbCodec()
		if err != nil {
			return nil, err
		}
		decoded, err = dec.DecodeAll(stored, make([]byte, 0, block.rawLen))
		if err != nil {
			return nil, err
		}
	}
	if len(decoded) != int(block.rawLen) || crc32.ChecksumIEEE(decoded) != block.checksum {
		return nil, errors.New("snapshots: V6 accessor key block checksum mismatch")
	}
	br := bytes.NewReader(decoded)
	records := make([]stateDomainChangeBinaryAccessorV6Record, 0, block.keyCount)
	var previous []byte
	for i := uint32(0); i < block.keyCount; i++ {
		prefix, err := binary.ReadUvarint(br)
		if err != nil || prefix > uint64(len(previous)) {
			return nil, errors.New("snapshots: invalid V6 accessor key prefix")
		}
		suffix, err := binary.ReadUvarint(br)
		if err != nil || suffix > uint64(br.Len()) {
			return nil, errors.New("snapshots: invalid V6 accessor key suffix")
		}
		key := make([]byte, prefix+suffix)
		copy(key, previous[:prefix])
		if _, err := io.ReadFull(br, key[prefix:]); err != nil {
			return nil, err
		}
		postingOff, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, err
		}
		postingCount, err := binary.ReadUvarint(br)
		if err != nil || postingCount > math.MaxUint32 {
			return nil, errors.New("snapshots: invalid V6 accessor posting count")
		}
		if postingOff > h.postingLen {
			return nil, errors.New("snapshots: key-oriented accessor posting offset outside section")
		}
		if h.version == stateDomainChangeBinaryVersionV6 {
			postingBytes := postingCount * stateDomainChangeBinaryAccessorV6PostingSize
			if postingBytes > h.postingLen-postingOff {
				return nil, errors.New("snapshots: V6 accessor postings outside section")
			}
		} else if postingCount != 0 && postingOff == h.postingLen {
			return nil, errors.New("snapshots: V7 accessor posting offset at section end")
		}
		if i == 0 && !bytes.Equal(key, block.firstKey) {
			return nil, errors.New("snapshots: V6 accessor first key mismatch")
		}
		if i > 0 && bytes.Compare(previous, key) >= 0 {
			return nil, errors.New("snapshots: V6 accessor keys not sorted")
		}
		records = append(records, stateDomainChangeBinaryAccessorV6Record{key: key, keyID: firstKeyID + i, postingOff: postingOff, postings: uint32(postingCount)})
		previous = key
	}
	if br.Len() != 0 {
		return nil, errors.New("snapshots: V6 accessor key block trailing bytes")
	}
	return records, nil
}

func stateDomainChangeBinaryAccessorV6Lookup(r io.ReaderAt, fileSize uint64, lookup []byte) (stateDomainChangeBinaryAccessorV6Header, stateDomainChangeBinaryAccessorV6Record, bool, error) {
	h, err := decodeStateDomainChangeBinaryAccessorV6Header(r, fileSize)
	if err != nil {
		return h, stateDomainChangeBinaryAccessorV6Record{}, false, err
	}
	low, high := uint64(0), h.blockCount
	for low < high {
		mid := low + (high-low)/2
		block, readErr := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, mid)
		if readErr != nil {
			return h, stateDomainChangeBinaryAccessorV6Record{}, false, readErr
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
	firstID := uint32(blockIndex * stateDomainChangeBinaryAccessorV6BlockKeys)
	records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, fileSize, h, block, firstID)
	if err != nil {
		return h, stateDomainChangeBinaryAccessorV6Record{}, false, err
	}
	pos := sort.Search(len(records), func(i int) bool { return bytes.Compare(records[i].key, lookup) >= 0 })
	if pos == len(records) || !bytes.Equal(records[pos].key, lookup) {
		return h, stateDomainChangeBinaryAccessorV6Record{}, false, nil
	}
	return h, records[pos], true, nil
}

func stateDomainChangeBinaryAccessorV6KeyByID(r io.ReaderAt, fileSize uint64, keyID uint32) ([]byte, error) {
	h, err := decodeStateDomainChangeBinaryAccessorV6Header(r, fileSize)
	if err != nil {
		return nil, err
	}
	if uint64(keyID) >= h.keyCount {
		return nil, errors.New("snapshots: V6 accessor key id outside dictionary")
	}
	blockIndex := uint64(keyID / stateDomainChangeBinaryAccessorV6BlockKeys)
	block, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(r, h, blockIndex)
	if err != nil {
		return nil, err
	}
	records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, fileSize, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
	if err != nil {
		return nil, err
	}
	return records[int(keyID%stateDomainChangeBinaryAccessorV6BlockKeys)].key, nil
}

func stateDomainChangeBinaryAccessorV6PostingAt(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record, index uint32) (stateDomainChangeBinaryAccessorV6Posting, error) {
	if index >= record.postings {
		return stateDomainChangeBinaryAccessorV6Posting{}, errors.New("snapshots: V6 accessor posting index outside key")
	}
	off := h.headerSize + h.blockDirLen + h.keyDataLen + record.postingOff + uint64(index)*stateDomainChangeBinaryAccessorV6PostingSize
	var raw [stateDomainChangeBinaryAccessorV6PostingSize]byte
	if _, err := r.ReadAt(raw[:], int64(off)); err != nil {
		return stateDomainChangeBinaryAccessorV6Posting{}, err
	}
	return decodeStateDomainChangeBinaryAccessorV6Posting(raw[:])
}

func decodeStateDomainChangeBinaryAccessorV6Posting(raw []byte) (stateDomainChangeBinaryAccessorV6Posting, error) {
	if len(raw) != stateDomainChangeBinaryAccessorV6PostingSize {
		return stateDomainChangeBinaryAccessorV6Posting{}, errors.New("snapshots: invalid V6 accessor posting size")
	}
	offset, err := stateDomainChangeBinaryAccessorV5Offset(raw[8:14])
	if err != nil {
		return stateDomainChangeBinaryAccessorV6Posting{}, err
	}
	return stateDomainChangeBinaryAccessorV6Posting{txNum: binary.BigEndian.Uint64(raw[:8]), offset: offset, recordIndex: binary.BigEndian.Uint32(raw[14:18])}, nil
}

func stateDomainChangeBinaryAccessorV6PostingLowerBound(r io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record, txNum uint64) (uint32, error) {
	low, high := uint32(0), record.postings
	for low < high {
		mid := low + (high-low)/2
		posting, err := stateDomainChangeBinaryAccessorV6PostingAt(r, h, record, mid)
		if err != nil {
			return 0, err
		}
		if posting.txNum < txNum {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, nil
}

func iterateStateDomainChangeBinarySegmentByAccessorV6Record(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, h stateDomainChangeBinaryAccessorV6Header, record stateDomainChangeBinaryAccessorV6Record, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	start, err := stateDomainChangeBinaryAccessorV6PostingLowerBound(accessor, h, record, fromTxNum)
	if err != nil {
		return err
	}
	for i := start; i < record.postings; i++ {
		posting, err := stateDomainChangeBinaryAccessorV6PostingAt(accessor, h, record, i)
		if err != nil {
			return err
		}
		if posting.txNum > toTxNum {
			return nil
		}
		change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, posting.offset, segmentSize, uint64(posting.recordIndex))
		if err != nil {
			return err
		}
		if change.TxNum != posting.txNum || !bytes.Equal(stateDomainChangeBinaryAccessorKey(change), record.key) {
			return errors.New("snapshots: V6 accessor posting does not match history record")
		}
		cont, err := fn(change)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func iterateStateDomainChangeBinarySegmentByAccessorV6Key(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, lookupKey []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if err := verifyStateDomainChangeBinaryV6DictionaryPair(segment, accessor, accessorSize); err != nil {
		return err
	}
	h, record, ok, err := stateDomainChangeBinaryAccessorV6Lookup(accessor, accessorSize, lookupKey)
	if err != nil || !ok {
		return err
	}
	return iterateStateDomainChangeBinarySegmentByAccessorV6Record(segment, segmentSize, accessor, h, record, fromTxNum, toTxNum, fn)
}

func iterateStateDomainChangeBinarySegmentByAccessorV6Prefix(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64, lookupPrefix []byte, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if err := verifyStateDomainChangeBinaryV6DictionaryPair(segment, accessor, accessorSize); err != nil {
		return err
	}
	h, err := decodeStateDomainChangeBinaryAccessorV6Header(accessor, accessorSize)
	if err != nil {
		return err
	}
	low, high := uint64(0), h.blockCount
	for low < high {
		mid := low + (high-low)/2
		block, readErr := stateDomainChangeBinaryAccessorV6ReadBlockDirectoryEntry(accessor, h, mid)
		if readErr != nil {
			return readErr
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
			cmp := bytes.Compare(record.key, lookupPrefix)
			if cmp < 0 {
				continue
			}
			if !bytes.HasPrefix(record.key, lookupPrefix) {
				return nil
			}
			if err := iterateStateDomainChangeBinarySegmentByAccessorV6Record(segment, segmentSize, accessor, h, record, fromTxNum, toTxNum, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyStateDomainChangeBinaryV6DictionaryPair(segment, accessor io.ReaderAt, accessorSize uint64) error {
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(accessor, accessorSize)
	if err != nil {
		return err
	}
	digest, err := readStateDomainChangeBinaryV6DictionaryCommitment(segment)
	if err != nil {
		return err
	}
	if digest != h.dictionaryDigest {
		return errors.New("snapshots: V6 history/accessor dictionary commitment mismatch")
	}
	return nil
}

// iterateStateDomainChangeBinaryAccessorV6Keys walks the immutable dictionary
// in keyID order. Compaction uses this once per source segment to collect the
// union dictionary and to build a dense source-ID to destination-ID remap.
// Unlike KeyByID, this performs one sequential block-directory pass and one
// decode per unique key block, independent of the number and order of postings.
func iterateStateDomainChangeBinaryAccessorV6Keys(r io.ReaderAt, fileSize uint64, fn func(uint32, []byte) error) (stateDomainChangeBinaryAccessorV6Header, error) {
	h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(r, fileSize)
	if err != nil {
		return h, err
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
	if err != nil {
		return h, err
	}
	var seen uint64
	for blockIndex, block := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, fileSize, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return h, err
		}
		for _, record := range records {
			if uint64(record.keyID) != seen {
				return h, errors.New("snapshots: V6 accessor dictionary key IDs are not contiguous")
			}
			if err := fn(record.keyID, record.key); err != nil {
				return h, err
			}
			seen++
		}
	}
	if seen != h.keyCount {
		return h, fmt.Errorf("snapshots: V6 accessor dictionary has %d keys, want %d", seen, h.keyCount)
	}
	return h, nil
}

func checkStateDomainChangeBinaryAccessorV6(r io.ReaderAt, fileSize uint64) error {
	h, err := decodeStateDomainChangeBinaryAccessorV6Header(r, fileSize)
	if err != nil {
		return err
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(r, h)
	if err != nil {
		return err
	}
	var keyCount, postingCount uint64
	var previous []byte
	postingBase := h.headerSize + h.blockDirLen + h.keyDataLen
	postingReader := bufio.NewReaderSize(io.NewSectionReader(r, int64(postingBase), int64(h.postingLen)), 1<<20)
	var postingRaw [stateDomainChangeBinaryAccessorV6PostingSize]byte
	var expectedPostingOff uint64
	for blockIndex, block := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(r, fileSize, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return err
		}
		for _, record := range records {
			if previous != nil && bytes.Compare(previous, record.key) >= 0 {
				return errors.New("snapshots: V6 accessor dictionary is not sorted")
			}
			if record.postingOff != expectedPostingOff {
				return errors.New("snapshots: V6 accessor postings are not contiguous")
			}
			var last stateDomainChangeBinaryAccessorV6Posting
			for i := uint32(0); i < record.postings; i++ {
				if _, err := io.ReadFull(postingReader, postingRaw[:]); err != nil {
					return err
				}
				posting, err := decodeStateDomainChangeBinaryAccessorV6Posting(postingRaw[:])
				if err != nil {
					return err
				}
				if posting.txNum < h.fromTxNum || posting.txNum > h.toTxNum || uint64(posting.recordIndex) >= h.recordCount || posting.offset > stateDomainChangeBinaryAccessorV5MaxOffset {
					return errors.New("snapshots: V6 accessor posting outside segment bounds")
				}
				if i > 0 && (posting.txNum < last.txNum || posting.recordIndex <= last.recordIndex || posting.offset <= last.offset) {
					return errors.New("snapshots: V6 accessor postings are not ordered")
				}
				last = posting
			}
			postingCount += uint64(record.postings)
			expectedPostingOff += uint64(record.postings) * stateDomainChangeBinaryAccessorV6PostingSize
			keyCount++
			previous = record.key
		}
	}
	if keyCount != h.keyCount || postingCount != h.recordCount {
		return fmt.Errorf("snapshots: V6 accessor counts keys=%d/%d postings=%d/%d", keyCount, h.keyCount, postingCount, h.recordCount)
	}
	if expectedPostingOff != h.postingLen {
		return errors.New("snapshots: V6 accessor posting length mismatch")
	}
	return nil
}

func verifyStateDomainChangeBinaryAccessorV6Coverage(segment io.ReaderAt, segmentSize uint64, accessor io.ReaderAt, accessorSize uint64) error {
	h, err := decodeStateDomainChangeBinaryAccessorV6Header(accessor, accessorSize)
	if err != nil {
		return err
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(accessor, h)
	if err != nil {
		return err
	}
	seen := make([]uint64, (h.recordCount+63)/64)
	var covered uint64
	for blockIndex, block := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(accessor, accessorSize, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return err
		}
		for _, record := range records {
			for i := uint32(0); i < record.postings; i++ {
				posting, err := stateDomainChangeBinaryAccessorV6PostingAt(accessor, h, record, i)
				if err != nil {
					return err
				}
				word, bit := uint64(posting.recordIndex)/64, uint(posting.recordIndex)%64
				if seen[word]&(uint64(1)<<bit) != 0 {
					return errors.New("snapshots: V6 accessor covers a history record twice")
				}
				change, _, err := readStateDomainChangeBinaryRecordAtBoundedIndex(segment, posting.offset, segmentSize, uint64(posting.recordIndex))
				if err != nil {
					return err
				}
				if change.TxNum != posting.txNum || !bytes.Equal(stateDomainChangeBinaryAccessorKey(change), record.key) {
					return errors.New("snapshots: V6 accessor posting/history mismatch")
				}
				seen[word] |= uint64(1) << bit
				covered++
			}
		}
	}
	if covered != h.recordCount {
		return fmt.Errorf("snapshots: V6 accessor covers %d records, want %d", covered, h.recordCount)
	}
	return nil
}

func readStateDomainChangeBinaryAccessorV6Debug(accessor io.ReaderAt, accessorSize uint64) ([]stateDomainChangeBinaryAccessorEntry, error) {
	h, err := decodeStateDomainChangeBinaryAccessorV6Header(accessor, accessorSize)
	if err != nil {
		return nil, err
	}
	blocks, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(accessor, h)
	if err != nil {
		return nil, err
	}
	entries := make([]stateDomainChangeBinaryAccessorEntry, 0, h.recordCount)
	for blockIndex, block := range blocks {
		records, err := stateDomainChangeBinaryAccessorV6ReadBlock(accessor, accessorSize, h, block, uint32(blockIndex*stateDomainChangeBinaryAccessorV6BlockKeys))
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			for i := uint32(0); i < record.postings; i++ {
				posting, err := stateDomainChangeBinaryAccessorV6PostingAt(accessor, h, record, i)
				if err != nil {
					return nil, err
				}
				entries = append(entries, stateDomainChangeBinaryAccessorEntry{key: append([]byte(nil), record.key...), txNum: posting.txNum, seq: uint64(posting.recordIndex) + 1, offset: posting.offset, recordIndex: uint64(posting.recordIndex)})
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return compareStateDomainChangeBinaryAccessorEntry(entries[i], entries[j]) < 0 })
	return entries, nil
}

func decodeStateDomainChangeBinaryAccessorKey(key []byte, change *rawdb.StateDomainChange) error {
	if change == nil || len(key) < 1+common.AccountIDLength {
		return errors.New("snapshots: invalid V6 logical key")
	}
	flat := rawdb.StateFlatDomain(key[0])
	if flat != rawdb.StateFlatDomainAccountLatest && flat != rawdb.StateFlatDomainKVLatest && flat != rawdb.StateFlatDomainKVGeneration {
		return errors.New("snapshots: invalid V6 flat domain")
	}
	change.FlatDomain = flat
	var id common.AccountID
	copy(id[:], key[1:1+common.AccountIDLength])
	change.Owner = id.Address(common.AddressPrefixMainnet)
	rest := key[1+common.AccountIDLength:]
	if flat == rawdb.StateFlatDomainKVLatest {
		if len(rest) < 10 {
			return errors.New("snapshots: invalid V6 KV logical key")
		}
		change.Generation = binary.BigEndian.Uint64(rest[:8])
		change.Domain = kvdomains.KVDomain(binary.BigEndian.Uint16(rest[8:10]))
		change.Key = append(change.Key[:0], rest[10:]...)
	} else if len(rest) != 0 {
		return errors.New("snapshots: V6 account/generation key has trailing bytes")
	}
	return nil
}

func encodeStateDomainChangeRecordV6(change *rawdb.StateDomainChange, keyID uint32) ([]byte, error) {
	payloadSize, err := stateDomainChangeRecordV6PayloadSize(change)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, payloadSize)
	putStateDomainChangeRecordV6(payload, change, keyID)
	return payload, nil
}

func stateDomainChangeRecordV6PayloadSize(change *rawdb.StateDomainChange) (int, error) {
	if change == nil || uint64(len(change.Prev)) > math.MaxUint32 {
		return 0, errors.New("snapshots: invalid V6 state-domain-change record")
	}
	return 4 + 8 + 1 + 4 + len(change.Prev), nil
}

// putStateDomainChangeRecordV6 encodes into exact-sized caller-owned storage.
// Callers validate the change and reserve stateDomainChangeRecordV6PayloadSize
// bytes first. Keeping this primitive allocation-free lets streaming snapshot
// writers reuse their record scratch buffer without a payload copy.
func putStateDomainChangeRecordV6(payload []byte, change *rawdb.StateDomainChange, keyID uint32) {
	binary.BigEndian.PutUint32(payload[:4], keyID)
	binary.BigEndian.PutUint64(payload[4:12], change.TxNum)
	if change.PrevExists {
		payload[12] = 1
	} else {
		payload[12] = 0
	}
	binary.BigEndian.PutUint32(payload[13:17], uint32(len(change.Prev)))
	copy(payload[17:], change.Prev)
}

func encodeStateDomainChangeBinarySegmentV6(fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange, txRangeSets ...[]*rawdb.StateTxRange) ([]byte, []stateDomainChangeBinaryTxOffset, []byte, error) {
	if toTxNum < fromTxNum {
		return nil, nil, nil, fmt.Errorf("snapshots: state-domain-change range [%d,%d] is inverted", fromTxNum, toTxNum)
	}
	txRanges, err := normalizeStateTxRangesForBinary(fromTxNum, toTxNum, changes, firstStateTxRangeSet(txRangeSets))
	if err != nil {
		return nil, nil, nil, err
	}
	keySet := make(map[string]struct{})
	for _, change := range changes {
		keySet[string(stateDomainChangeBinaryAccessorKey(change))] = struct{}{}
	}
	keyStrings := make([]string, 0, len(keySet))
	for key := range keySet {
		keyStrings = append(keyStrings, key)
	}
	sort.Strings(keyStrings)
	keyIDs := make(map[string]uint32, len(keyStrings))
	dictionaryKeys := make([]stateDomainChangeBinaryAccessorV6Key, len(keyStrings))
	for i, key := range keyStrings {
		keyIDs[key] = uint32(i)
		dictionaryKeys[i] = stateDomainChangeBinaryAccessorV6Key{key: []byte(key)}
	}
	var buf bytes.Buffer
	writeStateDomainChangeBinaryHeaderVersion(&buf, stateDomainChangeBinarySegmentMagic, fromTxNum, toTxNum, uint64(len(changes)), stateDomainChangeBinaryVersionV6)
	dictionaryDigest := stateDomainChangeBinaryAccessorV6DictionaryDigest(dictionaryKeys)
	if err := writeStateDomainChangeBinaryV6DictionaryCommitment(&buf, dictionaryDigest); err != nil {
		return nil, nil, nil, err
	}
	if err := writeStateDomainChangeBinaryTxRangeTable(&buf, txRanges); err != nil {
		return nil, nil, nil, err
	}
	index := make([]stateDomainChangeBinaryTxOffset, 0)
	entries := make([]stateDomainChangeBinaryAccessorEntry, 0, len(changes))
	for i, change := range changes {
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			return nil, nil, nil, fmt.Errorf("snapshots: V6 tx %d outside segment range", change.TxNum)
		}
		key := stateDomainChangeBinaryAccessorKey(change)
		payload, err := encodeStateDomainChangeRecordV6(change, keyIDs[string(key)])
		if err != nil {
			return nil, nil, nil, err
		}
		offset := uint64(buf.Len())
		writeUint32(&buf, uint32(len(payload)))
		buf.Write(payload)
		entries = append(entries, stateDomainChangeBinaryAccessorEntry{key: key, txNum: change.TxNum, seq: uint64(i) + 1, offset: offset, recordIndex: uint64(i)})
		if len(index) == 0 || index[len(index)-1].txNum != change.TxNum {
			index = append(index, stateDomainChangeBinaryTxOffset{txNum: change.TxNum, offset: offset, recordIndex: uint64(i), count: 1})
		} else {
			index[len(index)-1].count++
		}
	}
	accessor, recordIDs, err := encodeStateDomainChangeBinaryAccessorV6(fromTxNum, toTxNum, entries)
	if err != nil {
		return nil, nil, nil, err
	}
	for i, entry := range entries {
		if recordIDs[i] != keyIDs[string(entry.key)] {
			return nil, nil, nil, errors.New("snapshots: V6 key id assignment mismatch")
		}
	}
	return buf.Bytes(), index, accessor, nil
}

func decodeStateDomainChangeRecordV6(data []byte) (uint32, *rawdb.StateDomainChange, error) {
	change := new(rawdb.StateDomainChange)
	keyID, err := decodeStateDomainChangeRecordV6Into(data, change)
	if err != nil {
		return 0, nil, err
	}
	return keyID, change, nil
}

func decodeStateDomainChangeRecordV6Into(data []byte, change *rawdb.StateDomainChange) (uint32, error) {
	if change == nil {
		return 0, errors.New("snapshots: nil V6 state-domain-change destination")
	}
	if len(data) < 17 {
		return 0, io.ErrUnexpectedEOF
	}
	*change = rawdb.StateDomainChange{TxNum: binary.BigEndian.Uint64(data[4:12])}
	if data[12] > 1 {
		return 0, errors.New("snapshots: invalid V6 previous-value marker")
	}
	change.PrevExists = data[12] == 1
	length := uint64(binary.BigEndian.Uint32(data[13:17]))
	if length != uint64(len(data)-17) {
		return 0, errors.New("snapshots: invalid V6 previous-value length")
	}
	if length != 0 {
		change.Prev = data[17:]
	}
	return binary.BigEndian.Uint32(data[:4]), nil
}
