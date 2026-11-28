package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
)

const (
	EventLogIndexV3Version = 3

	eventLogIndexV3HeaderSize     = 80
	eventLogTopicBloomHeaderSize  = 48
	eventLogTopicBloomBitsPerKey  = 16
	eventLogTopicBloomPageBlocks  = 128
	eventLogTopicBloomBlockBytes  = 32
	eventLogTopicBloomBlockWords  = 8
	eventLogTopicBloomHashPrefix  = "gtevbf1\x00"
	eventLogTopicBloomCheckChunk  = 4096
	eventLogTopicBloomKeysPerBlk  = 16 // 256-bit block / 16 bits per key
)

var (
	eventLogIndexMagicV3     = [8]byte{'g', 't', 'e', 'v', 'l', 'x', '3', '\n'}
	eventLogTopicBloomMagicV1 = [8]byte{'g', 't', 'e', 'v', 'b', 'f', '1', '\n'}
)

type eventLogTopicBloomHeader struct {
	bitsPerKey uint32
	pageBlocks uint32
	keyCount   uint64
	blockCount uint64
	pageCount  uint64
}

func eventLogTopicBloomBlockCount(keyCount uint64) uint64 {
	if keyCount == 0 {
		return 0
	}
	return ceilDiv(keyCount, eventLogTopicBloomKeysPerBlk)
}

func eventLogTopicBloomExpectedLength(blockCount, pageCount uint64) (uint64, error) {
	crcBytes, overflow := checkedMul(pageCount, 4)
	if overflow {
		return 0, errors.New("snapshots: V3 topic bloom page CRC table overflows")
	}
	blockBytes, overflow := checkedMul(blockCount, eventLogTopicBloomBlockBytes)
	if overflow {
		return 0, errors.New("snapshots: V3 topic bloom filter body overflows")
	}
	total, overflow := checkedAdd(eventLogTopicBloomHeaderSize, crcBytes)
	if overflow {
		return 0, errors.New("snapshots: V3 topic bloom filter length overflows")
	}
	total, overflow = checkedAdd(total, blockBytes)
	if overflow {
		return 0, errors.New("snapshots: V3 topic bloom filter total length overflows")
	}
	return total, nil
}

func eventLogTopicBloomHash(key40 []byte, blockCount uint64) (blockIdx uint64, wordMasks [eventLogTopicBloomBlockWords]uint32) {
	var input [8 + eventLogTopicLookupKeySize]byte
	copy(input[:8], eventLogTopicBloomHashPrefix)
	copy(input[8:], key40)
	sum := sha256.Sum256(input[:])
	blockIdx, _ = bits.Mul64(binary.BigEndian.Uint64(sum[0:8]), blockCount)
	for w := 0; w < eventLogTopicBloomBlockWords; w++ {
		wordMasks[w] = uint32(1) << (sum[8+w] & 31)
	}
	return blockIdx, wordMasks
}

// encodeEventLogTopicBloomFilter builds a deterministic, page-CRC32-protected
// Split-Block Bloom Filter from strictly sorted unique 40-byte topic keys.
func encodeEventLogTopicBloomFilter(sortedTopicKeys []string) ([]byte, error) {
	keyCount := uint64(len(sortedTopicKeys))
	blockCount := eventLogTopicBloomBlockCount(keyCount)
	pageCount := ceilDiv(blockCount, eventLogTopicBloomPageBlocks)
	totalLen, err := eventLogTopicBloomExpectedLength(blockCount, pageCount)
	if err != nil {
		return nil, err
	}
	if totalLen > uint64(math.MaxInt) {
		return nil, errors.New("snapshots: V3 topic bloom filter exceeds platform limits")
	}
	out := make([]byte, int(totalLen))
	copy(out[:8], eventLogTopicBloomMagicV1[:])
	binary.BigEndian.PutUint32(out[8:12], eventLogTopicBloomBitsPerKey)
	binary.BigEndian.PutUint32(out[12:16], eventLogTopicBloomPageBlocks)
	binary.BigEndian.PutUint64(out[16:24], keyCount)
	binary.BigEndian.PutUint64(out[24:32], blockCount)
	binary.BigEndian.PutUint64(out[32:40], pageCount)
	binary.BigEndian.PutUint32(out[40:44], crc32.ChecksumIEEE(out[:40]))

	crcBase := eventLogTopicBloomHeaderSize
	bodyBase := crcBase + int(pageCount)*4
	var prev string
	for i, key := range sortedTopicKeys {
		if len(key) != eventLogTopicLookupKeySize {
			return nil, fmt.Errorf("snapshots: V3 topic bloom key length %d, want %d", len(key), eventLogTopicLookupKeySize)
		}
		if i > 0 && key <= prev {
			return nil, errors.New("snapshots: V3 topic bloom keys are not strictly sorted")
		}
		prev = key
		blockIdx, masks := eventLogTopicBloomHash([]byte(key), blockCount)
		blockOff := bodyBase + int(blockIdx)*eventLogTopicBloomBlockBytes
		for w := 0; w < eventLogTopicBloomBlockWords; w++ {
			wordOff := blockOff + w*4
			word := binary.BigEndian.Uint32(out[wordOff : wordOff+4])
			word |= masks[w]
			binary.BigEndian.PutUint32(out[wordOff:wordOff+4], word)
		}
	}
	for pageIdx := uint64(0); pageIdx < pageCount; pageIdx++ {
		pageStart := pageIdx * eventLogTopicBloomPageBlocks
		blocksInPage := uint64(eventLogTopicBloomPageBlocks)
		if rem := blockCount - pageStart; rem < blocksInPage {
			blocksInPage = rem
		}
		startByte := bodyBase + int(pageStart)*eventLogTopicBloomBlockBytes
		endByte := startByte + int(blocksInPage)*eventLogTopicBloomBlockBytes
		pageCRC := crc32.ChecksumIEEE(out[startByte:endByte])
		binary.BigEndian.PutUint32(out[crcBase+int(pageIdx)*4:crcBase+int(pageIdx+1)*4], pageCRC)
	}
	return out, nil
}

func readEventLogTopicBloomHeader(file io.ReaderAt, offset, length, size, wantKeyCount uint64) (eventLogTopicBloomHeader, error) {
	if length < eventLogTopicBloomHeaderSize || offset > size || length > size-offset || offset > math.MaxInt64 {
		return eventLogTopicBloomHeader{}, errors.New("snapshots: invalid V3 topic bloom bounds")
	}
	var raw [eventLogTopicBloomHeaderSize]byte
	if _, err := file.ReadAt(raw[:], int64(offset)); err != nil {
		if errors.Is(err, io.EOF) {
			return eventLogTopicBloomHeader{}, io.ErrUnexpectedEOF
		}
		return eventLogTopicBloomHeader{}, err
	}
	if !bytes.Equal(raw[:8], eventLogTopicBloomMagicV1[:]) {
		return eventLogTopicBloomHeader{}, errors.New("snapshots: invalid V3 topic bloom magic")
	}
	if binary.BigEndian.Uint32(raw[40:44]) != crc32.ChecksumIEEE(raw[:40]) {
		return eventLogTopicBloomHeader{}, errors.New("snapshots: V3 topic bloom header checksum mismatch")
	}
	if !bytes.Equal(raw[44:48], []byte{0, 0, 0, 0}) {
		return eventLogTopicBloomHeader{}, errors.New("snapshots: V3 topic bloom reserved bytes are non-zero")
	}
	h := eventLogTopicBloomHeader{
		bitsPerKey: binary.BigEndian.Uint32(raw[8:12]),
		pageBlocks: binary.BigEndian.Uint32(raw[12:16]),
		keyCount:   binary.BigEndian.Uint64(raw[16:24]),
		blockCount: binary.BigEndian.Uint64(raw[24:32]),
		pageCount:  binary.BigEndian.Uint64(raw[32:40]),
	}
	if h.bitsPerKey != eventLogTopicBloomBitsPerKey || h.pageBlocks != eventLogTopicBloomPageBlocks {
		return eventLogTopicBloomHeader{}, errors.New("snapshots: unsupported V3 topic bloom geometry")
	}
	if h.keyCount != wantKeyCount {
		return eventLogTopicBloomHeader{}, fmt.Errorf("snapshots: V3 topic bloom key count %d, want %d", h.keyCount, wantKeyCount)
	}
	if h.blockCount != eventLogTopicBloomBlockCount(h.keyCount) {
		return eventLogTopicBloomHeader{}, errors.New("snapshots: invalid V3 topic bloom block count")
	}
	if h.pageCount != ceilDiv(h.blockCount, uint64(h.pageBlocks)) {
		return eventLogTopicBloomHeader{}, errors.New("snapshots: invalid V3 topic bloom page count")
	}
	wantLen, err := eventLogTopicBloomExpectedLength(h.blockCount, h.pageCount)
	if err != nil {
		return eventLogTopicBloomHeader{}, err
	}
	if wantLen != length {
		return eventLogTopicBloomHeader{}, fmt.Errorf("snapshots: V3 topic bloom length %d, want %d", length, wantLen)
	}
	return h, nil
}

func checkEventLogTopicBloomSection(file io.ReaderAt, offset, length, size, wantKeyCount uint64) error {
	h, err := readEventLogTopicBloomHeader(file, offset, length, size, wantKeyCount)
	if err != nil {
		return err
	}
	if h.pageCount == 0 {
		return nil
	}
	crcBytes, err := readEventLogPayloadAt(file, offset+eventLogTopicBloomHeaderSize, h.pageCount*4, offset+length)
	if err != nil {
		return err
	}
	bodyStart := offset + eventLogTopicBloomHeaderSize + h.pageCount*4
	pageBuf := make([]byte, int(h.pageBlocks)*eventLogTopicBloomBlockBytes)
	for pageIdx := uint64(0); pageIdx < h.pageCount; pageIdx++ {
		pageStart := pageIdx * uint64(h.pageBlocks)
		blocksInPage := uint64(h.pageBlocks)
		if rem := h.blockCount - pageStart; rem < blocksInPage {
			blocksInPage = rem
		}
		pageLen := int(blocksInPage) * eventLogTopicBloomBlockBytes
		pageOff := bodyStart + pageStart*eventLogTopicBloomBlockBytes
		if _, err := file.ReadAt(pageBuf[:pageLen], int64(pageOff)); err != nil {
			if errors.Is(err, io.EOF) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		wantCRC := binary.BigEndian.Uint32(crcBytes[pageIdx*4 : pageIdx*4+4])
		if crc32.ChecksumIEEE(pageBuf[:pageLen]) != wantCRC {
			return fmt.Errorf("snapshots: V3 topic bloom page %d checksum mismatch", pageIdx)
		}
	}
	return nil
}

func eventLogTopicBloomContains(file io.ReaderAt, offset, length, size uint64, h eventLogTopicBloomHeader, key40 []byte) (bool, error) {
	if len(key40) != eventLogTopicLookupKeySize {
		return false, fmt.Errorf("snapshots: V3 topic bloom key length %d, want %d", len(key40), eventLogTopicLookupKeySize)
	}
	if h.keyCount == 0 || h.blockCount == 0 {
		return false, nil
	}
	blockIdx, masks := eventLogTopicBloomHash(key40, h.blockCount)
	pageIdx := blockIdx / uint64(h.pageBlocks)
	inPageBlock := blockIdx % uint64(h.pageBlocks)
	pageStartBlock := pageIdx * uint64(h.pageBlocks)
	blocksInPage := uint64(h.pageBlocks)
	if rem := h.blockCount - pageStartBlock; rem < blocksInPage {
		blocksInPage = rem
	}
	var crcRaw [4]byte
	crcOff := offset + eventLogTopicBloomHeaderSize + pageIdx*4
	if _, err := file.ReadAt(crcRaw[:], int64(crcOff)); err != nil {
		return false, err
	}
	wantCRC := binary.BigEndian.Uint32(crcRaw[:])

	bodyStart := offset + eventLogTopicBloomHeaderSize + h.pageCount*4
	pageOff := bodyStart + pageStartBlock*eventLogTopicBloomBlockBytes
	pageBytes, err := readEventLogPayloadAt(file, pageOff, blocksInPage*eventLogTopicBloomBlockBytes, offset+length)
	if err != nil {
		return false, err
	}
	if crc32.ChecksumIEEE(pageBytes) != wantCRC {
		return false, fmt.Errorf("snapshots: V3 topic bloom page %d checksum mismatch", pageIdx)
	}
	block := pageBytes[inPageBlock*eventLogTopicBloomBlockBytes : (inPageBlock+1)*eventLogTopicBloomBlockBytes]
	for w := 0; w < eventLogTopicBloomBlockWords; w++ {
		word := binary.BigEndian.Uint32(block[w*4 : w*4+4])
		if word&masks[w] != masks[w] {
			return false, nil
		}
	}
	return true, nil
}

func writeEventLogIndexV3Segment(dir string, ref SegmentRef, addressKeys, topicKeys []string) (SegmentRef, error) {
	if err := validateEventLogIndexRef(ref); err != nil {
		return SegmentRef{}, err
	}
	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	addressPostings := make(map[string][]uint64, len(addressKeys))
	for _, key := range addressKeys {
		addressPostings[key] = []uint64{ref.FromTxNum}
	}
	addressLookup, err := buildEventLogLookupV4(filepath.Dir(abs), "event-log-index-v3-address", eventLogAddressLookupKeySize, addressPostings)
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	defer addressLookup.close()

	topicBloom, err := encodeEventLogTopicBloomFilter(topicKeys)
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}

	if _, err := tmp.Write(make([]byte, eventLogIndexV3HeaderSize)); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offset := uint64(eventLogIndexV3HeaderSize)
	addressIndexOffset := offset
	addressIndexLength := addressLookup.length()
	if err := writeEventLogLookupV4(tmp, addressIndexOffset, addressLookup); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offset += addressIndexLength
	topicIndexOffset := offset
	topicIndexLength := uint64(len(topicBloom))
	if _, err := tmp.Write(topicBloom); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	header := eventLogIndexHeader{
		version:            EventLogIndexV3Version,
		fromBlock:          ref.FromTxNum,
		toBlock:            ref.ToTxNum,
		addressIndexOffset: addressIndexOffset,
		addressIndexLength: addressIndexLength,
		topicIndexOffset:   topicIndexOffset,
		topicIndexLength:   topicIndexLength,
		topicKeyCount:      uint64(len(topicKeys)),
	}
	if err := writeEventLogIndexV3HeaderAt(tmp, header); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	size, checksum, err := stateDomainChangeBinaryFileMetadata(tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	ref.Size = size
	ref.Checksum = checksum
	ref.Path = contentAddressedSnapshotPath(ref.Path, ref.Checksum)
	finalAbs := filepath.Join(dir, ref.Path)
	if err := os.Rename(tmpName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	if err := syncSnapshotDir(filepath.Dir(finalAbs)); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

func writeEventLogIndexV3HeaderAt(file *os.File, header eventLogIndexHeader) error {
	var raw [eventLogIndexV3HeaderSize]byte
	copy(raw[0:8], eventLogIndexMagicV3[:])
	binary.BigEndian.PutUint64(raw[8:16], header.fromBlock)
	binary.BigEndian.PutUint64(raw[16:24], header.toBlock)
	binary.BigEndian.PutUint64(raw[24:32], header.addressIndexOffset)
	binary.BigEndian.PutUint64(raw[32:40], header.addressIndexLength)
	binary.BigEndian.PutUint64(raw[40:48], header.topicIndexOffset)
	binary.BigEndian.PutUint64(raw[48:56], header.topicIndexLength)
	binary.BigEndian.PutUint64(raw[56:64], header.topicKeyCount)
	// [64:72] reserved = 0
	binary.BigEndian.PutUint32(raw[72:76], crc32.ChecksumIEEE(raw[:72]))
	// [76:80] reserved2 = 0
	_, err := file.WriteAt(raw[:], 0)
	return err
}
