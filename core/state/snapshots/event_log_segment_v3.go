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
	"path/filepath"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// V3 deliberately changes only the main event-log segment. The external
// event-log-index remains V1 and can index a mixed V1/V2/V3 manifest.
const (
	eventLogV3HeaderSize       = 176
	eventLogV3RowFrameRows     = 256
	eventLogV3PayloadTarget    = 32 << 10
	eventLogV3FrameDirEntry    = 32
	eventLogV3LookupHeaderSize = 16
	eventLogV3LookupFrameRows  = 1024
	eventLogV3LookupFrameEntry = 32
	eventLogV3LookupKeyTail    = 28
)

type eventLogV3Header struct {
	fromBlock, toBlock, rowCount           uint64
	rowFrameRows, payloadTarget            uint64
	blockDictOffset, blockDictLength       uint64
	txDictOffset, txDictLength             uint64
	rowDirOffset, rowDirLength             uint64
	rowDataOffset, rowDataLength           uint64
	payloadDirOffset, payloadDirLength     uint64
	payloadDataOffset, payloadDataLength   uint64
	addressIndexOffset, addressIndexLength uint64
	topicIndexOffset, topicIndexLength     uint64
}

type eventLogV3Reader struct {
	header            eventLogV3Header
	rowFrameCount     uint64
	payloadFrames     uint64
	blockCount        uint64
	txCount           uint64
	addressCount      uint64
	cacheFrame        uint64
	cacheRows         []eventLogV3Row
	cacheFrameValid   bool
	cachePayload      uint64
	cachePayloadBytes []byte
	cachePayloadValid bool
}

type eventLogV3Frame struct {
	firstRow uint64
	dataOff  uint64
	dataLen  uint32
	rowCount uint32
	checksum uint32
	rawLen   uint32
}

type eventLogV3Row struct {
	blockID, txIndex, logIndex, txID, addressID uint64
	payloadFrame, payloadOffset, payloadLength  uint64
}

type eventLogV3Block struct {
	number uint64
	hash   common.Hash
}

type eventLogV3LookupKey struct {
	key          []byte
	firstFrame   uint64
	frameCount   uint32
	postingCount uint64
}

type eventLogV3LookupFrame struct {
	dataOff  uint64
	dataLen  uint32
	count    uint32
	first    uint64
	checksum uint32
}

type eventLogV3LookupBuild struct {
	keySize uint64
	keys    []eventLogV3LookupKey
	frames  []eventLogV3LookupFrame
	data    *os.File
	name    string
	dataLen uint64
}

type EventLogV3PhysicalStats struct {
	HeaderBytes             uint64 `json:"headerBytes"`
	BlockDictionaryBytes    uint64 `json:"blockDictionaryBytes"`
	TxDictionaryBytes       uint64 `json:"txDictionaryBytes"`
	RowDirectoryBytes       uint64 `json:"rowDirectoryBytes"`
	RowDeltaBytes           uint64 `json:"rowDeltaBytes"`
	PayloadDirectoryBytes   uint64 `json:"payloadDirectoryBytes"`
	PayloadCompressedBytes  uint64 `json:"payloadCompressedBytes"`
	AddressLookupBytes      uint64 `json:"addressLookupBytes"`
	TopicLookupBytes        uint64 `json:"topicLookupBytes"`
	TotalBytes              uint64 `json:"totalBytes"`
	MaxRowFrameReadBytes    uint64 `json:"maxRowFrameReadBytes"`
	MaxPayloadFrameRead     uint64 `json:"maxPayloadFrameReadBytes"`
	MaxPointReadBytes       uint64 `json:"maxPointReadBytes"`
	MaxPointDecompressBytes uint64 `json:"maxPointDecompressBytes"`
	MaxAddressLookupRead    uint64 `json:"maxAddressLookupReadBytes"`
	MaxTopicLookupRead      uint64 `json:"maxTopicLookupReadBytes"`
	MaxFilterLookupRead     uint64 `json:"maxFilterLookupReadBytes"`
}

func InspectEventLogV3Physical(dir string, ref SegmentRef) (EventLogV3PhysicalStats, error) {
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		return EventLogV3PhysicalStats{}, err
	}
	defer seg.Close()
	if seg.header.version != EventLogSegmentV3Version {
		return EventLogV3PhysicalStats{}, fmt.Errorf("snapshots: event log %q is version %d, want V3", ref.Path, seg.header.version)
	}
	h := seg.v3.header
	out := EventLogV3PhysicalStats{
		HeaderBytes: eventLogV3HeaderSize, BlockDictionaryBytes: h.blockDictLength, TxDictionaryBytes: h.txDictLength,
		RowDirectoryBytes: h.rowDirLength, RowDeltaBytes: h.rowDataLength, PayloadDirectoryBytes: h.payloadDirLength,
		PayloadCompressedBytes: h.payloadDataLength, AddressLookupBytes: h.addressIndexLength, TopicLookupBytes: h.topicIndexLength, TotalBytes: seg.size,
	}
	for i := uint64(0); i < seg.v3.rowFrameCount; i++ {
		frame, err := readEventLogV3FrameAt(seg.file, h.rowDirOffset, i, seg.v3.rowFrameCount)
		if err != nil {
			return EventLogV3PhysicalStats{}, err
		}
		out.MaxRowFrameReadBytes = max(out.MaxRowFrameReadBytes, uint64(frame.dataLen))
	}
	for i := uint64(0); i < seg.v3.payloadFrames; i++ {
		frame, err := readEventLogV3FrameAt(seg.file, h.payloadDirOffset, i, seg.v3.payloadFrames)
		if err != nil {
			return EventLogV3PhysicalStats{}, err
		}
		out.MaxPayloadFrameRead = max(out.MaxPayloadFrameRead, uint64(frame.dataLen))
		out.MaxPointDecompressBytes = max(out.MaxPointDecompressBytes, uint64(frame.rawLen))
	}
	// One row frame and one payload frame plus their directory entries and
	// direct block/tx/address dictionary reads. Filesystem block amplification
	// is intentionally not guessed here.
	out.MaxPointReadBytes = eventLogV3FrameDirEntry + out.MaxRowFrameReadBytes + (8 + common.HashLength) + common.HashLength + (eventLogAddressLookupKeySize + eventLogV3LookupKeyTail) + eventLogV3FrameDirEntry + out.MaxPayloadFrameRead
	out.MaxAddressLookupRead, err = maxEventLogV3LookupRead(seg.file, h.addressIndexOffset, h.addressIndexLength, seg.size, eventLogAddressLookupKeySize)
	if err != nil {
		return EventLogV3PhysicalStats{}, err
	}
	out.MaxTopicLookupRead, err = maxEventLogV3LookupRead(seg.file, h.topicIndexOffset, h.topicIndexLength, seg.size, eventLogTopicLookupKeySize)
	if err != nil {
		return EventLogV3PhysicalStats{}, err
	}
	out.MaxFilterLookupRead = max(out.MaxAddressLookupRead, out.MaxTopicLookupRead)
	return out, nil
}

func maxEventLogV3LookupRead(file io.ReaderAt, offset, length, size uint64, keySize int) (uint64, error) {
	keys, frames, err := readEventLogV3LookupCounts(file, offset, length, size, keySize)
	if err != nil {
		return 0, err
	}
	entrySize := uint64(keySize + eventLogV3LookupKeyTail)
	frameDir := offset + eventLogV3LookupHeaderSize + keys*entrySize
	var searchReads uint64
	for n := keys; n > 0; n >>= 1 {
		searchReads += entrySize
	}
	var maximum uint64
	for i := uint64(0); i < keys; i++ {
		var tail [eventLogV3LookupKeyTail]byte
		if _, err := file.ReadAt(tail[:], int64(offset+eventLogV3LookupHeaderSize+i*entrySize+uint64(keySize))); err != nil {
			return 0, err
		}
		first := binary.BigEndian.Uint64(tail[0:8])
		count := uint64(binary.BigEndian.Uint32(tail[8:12]))
		if first+count > frames {
			return 0, errors.New("snapshots: V3 lookup frame range outside directory")
		}
		read := searchReads + count*eventLogV3LookupFrameEntry
		for j := uint64(0); j < count; j++ {
			var raw [eventLogV3LookupFrameEntry]byte
			if _, err := file.ReadAt(raw[:], int64(frameDir+(first+j)*eventLogV3LookupFrameEntry)); err != nil {
				return 0, err
			}
			read += uint64(binary.BigEndian.Uint32(raw[8:12]))
		}
		maximum = max(maximum, read)
	}
	return maximum, nil
}

func (b *eventLogV3LookupBuild) close() {
	if b == nil {
		return
	}
	if b.data != nil {
		_ = b.data.Close()
	}
	if b.name != "" {
		_ = os.Remove(b.name)
	}
}

func (b *eventLogV3LookupBuild) length() uint64 {
	return eventLogV3LookupHeaderSize + uint64(len(b.keys))*(b.keySize+eventLogV3LookupKeyTail) + uint64(len(b.frames))*eventLogV3LookupFrameEntry + b.dataLen
}

// BuildEventLogV3SegmentFromReader rewrites a continuously covered immutable
// range without opening chaindata. It performs two passes over the pinned
// reader so large protobuf payloads are never retained in memory.
func BuildEventLogV3SegmentFromReader(reader rawdb.EventLogReader, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	if reader == nil {
		return SegmentRef{}, errors.New("snapshots: nil V3 event log reader")
	}
	if dir == "" {
		return SegmentRef{}, errors.New("snapshots: V3 event log directory is empty")
	}
	if toBlock < fromBlock {
		return SegmentRef{}, fmt.Errorf("snapshots: V3 event log range [%d,%d] is inverted", fromBlock, toBlock)
	}
	covered, err := reader.EventLogRangeCovered(fromBlock, toBlock)
	if err != nil {
		return SegmentRef{}, err
	}
	if !covered {
		return SegmentRef{}, fmt.Errorf("snapshots: V3 event log reader does not cover [%d,%d]", fromBlock, toBlock)
	}
	if relPath == "" {
		relPath = EventLogSegmentPath(fromBlock, toBlock)
	}
	ref := SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: fromBlock, ToTxNum: toBlock, Path: filepath.ToSlash(relPath)}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}

	blocks, txHashes, addresses, rowCount, err := scanEventLogV3Dictionaries(reader, fromBlock, toBlock)
	if err != nil {
		return SegmentRef{}, err
	}
	blockIDs := make(map[uint64]uint64, len(blocks))
	for i, block := range blocks {
		blockIDs[block.number] = uint64(i)
	}
	addressKeys := make([]string, 0, len(addresses))
	for key := range addresses {
		addressKeys = append(addressKeys, key)
	}
	sort.Strings(addressKeys)
	addressIDs := make(map[string]uint64, len(addressKeys))
	for i, key := range addressKeys {
		addressIDs[key] = uint64(i)
	}

	leafDir := filepath.Join(dir, filepath.Dir(ref.Path))
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		return SegmentRef{}, err
	}
	rowData, rowName, err := createStateDomainChangeBinaryTempFileInDir(leafDir, "event-log-v3-row")
	if err != nil {
		return SegmentRef{}, err
	}
	defer func() { _ = rowData.Close(); _ = os.Remove(rowName) }()
	payloadData, payloadName, err := createStateDomainChangeBinaryTempFileInDir(leafDir, "event-log-v3-payload")
	if err != nil {
		return SegmentRef{}, err
	}
	defer func() { _ = payloadData.Close(); _ = os.Remove(payloadName) }()

	rowFrames, payloadFrames, addressPostings, topicPostings, err := writeEventLogV3Frames(reader, fromBlock, toBlock, rowCount, blockIDs, txHashes, addressIDs, rowData, payloadData)
	if err != nil {
		return SegmentRef{}, err
	}
	addressLookup, err := buildEventLogV3Lookup(leafDir, "event-log-v3-address", eventLogAddressLookupKeySize, addressPostings)
	if err != nil {
		return SegmentRef{}, err
	}
	defer addressLookup.close()
	topicLookup, err := buildEventLogV3Lookup(leafDir, "event-log-v3-topic", eventLogTopicLookupKeySize, topicPostings)
	if err != nil {
		return SegmentRef{}, err
	}
	defer topicLookup.close()

	blockDictLength := uint64(8 + len(blocks)*(8+common.HashLength))
	txDictLength := uint64(8 + len(txHashes)*common.HashLength)
	rowDirLength := uint64(8 + len(rowFrames)*eventLogV3FrameDirEntry)
	payloadDirLength := uint64(8 + len(payloadFrames)*eventLogV3FrameDirEntry)
	rowStat, err := rowData.Stat()
	if err != nil {
		return SegmentRef{}, err
	}
	payloadStat, err := payloadData.Stat()
	if err != nil {
		return SegmentRef{}, err
	}
	h := eventLogV3Header{fromBlock: fromBlock, toBlock: toBlock, rowCount: rowCount, rowFrameRows: eventLogV3RowFrameRows, payloadTarget: eventLogV3PayloadTarget}
	h.blockDictOffset, h.blockDictLength = eventLogV3HeaderSize, blockDictLength
	h.txDictOffset, h.txDictLength = h.blockDictOffset+h.blockDictLength, txDictLength
	h.rowDirOffset, h.rowDirLength = h.txDictOffset+h.txDictLength, rowDirLength
	h.rowDataOffset, h.rowDataLength = h.rowDirOffset+h.rowDirLength, uint64(rowStat.Size())
	h.payloadDirOffset, h.payloadDirLength = h.rowDataOffset+h.rowDataLength, payloadDirLength
	h.payloadDataOffset, h.payloadDataLength = h.payloadDirOffset+h.payloadDirLength, uint64(payloadStat.Size())
	h.addressIndexOffset, h.addressIndexLength = h.payloadDataOffset+h.payloadDataLength, addressLookup.length()
	h.topicIndexOffset, h.topicIndexLength = h.addressIndexOffset+h.addressIndexLength, topicLookup.length()

	finalTmp, finalName, err := createStateDomainChangeBinaryTempFileInDir(leafDir, filepath.Base(ref.Path))
	if err != nil {
		return SegmentRef{}, err
	}
	removeFinalTmp := true
	defer func() {
		if removeFinalTmp {
			if finalTmp != nil {
				_ = finalTmp.Close()
			}
			_ = os.Remove(finalName)
		}
	}()
	if err := writeEventLogV3File(finalTmp, h, blocks, txHashes, rowFrames, rowData, payloadFrames, payloadData, addressLookup, topicLookup); err != nil {
		return SegmentRef{}, err
	}
	size, checksum, err := closeAndHashStateDomainChangeBinaryTemp(finalTmp, finalName)
	if err != nil {
		return SegmentRef{}, err
	}
	finalTmp = nil
	finalAbs := contentAddressedSnapshotPath(filepath.Join(dir, ref.Path), checksum)
	if err := publishStateDomainChangeBinaryTemp(finalName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	removeFinalTmp = false
	if err := syncSnapshotDir(filepath.Dir(finalAbs)); err != nil {
		return SegmentRef{}, err
	}
	rel, err := filepath.Rel(dir, finalAbs)
	if err != nil {
		return SegmentRef{}, err
	}
	ref.Path, ref.Size, ref.Checksum = filepath.ToSlash(rel), size, checksum
	if err := CheckEventLogSegment(dir, ref); err != nil {
		return SegmentRef{}, fmt.Errorf("snapshots: verify newly written V3 event log: %w", err)
	}
	return ref, nil
}

func scanEventLogV3Dictionaries(reader rawdb.EventLogReader, fromBlock, toBlock uint64) ([]eventLogV3Block, []common.Hash, map[string]struct{}, uint64, error) {
	var blocks []eventLogV3Block
	var txHashes []common.Hash
	addresses := make(map[string]struct{})
	var count uint64
	var prev EventLog
	havePrev := false
	blockSeen := make(map[uint64]common.Hash)
	err := reader.IterateEventLogs(fromBlock, toBlock, EventLogFilter{}, func(row EventLog) (bool, error) {
		if err := validateEventLogV3SourceRow(row, fromBlock, toBlock); err != nil {
			return false, err
		}
		if havePrev && compareEventLogEntries(eventLogIndexEntry{blockNum: prev.BlockNum, txIndex: prev.TxIndex, logIndex: prev.LogIndex}, eventLogIndexEntry{blockNum: row.BlockNum, txIndex: row.TxIndex, logIndex: row.LogIndex}) >= 0 {
			return false, fmt.Errorf("snapshots: V3 source rows are not strictly ordered at block=%d tx=%d log=%d", row.BlockNum, row.TxIndex, row.LogIndex)
		}
		if hash, ok := blockSeen[row.BlockNum]; ok {
			if hash != row.BlockHash {
				return false, fmt.Errorf("snapshots: V3 source block %d has inconsistent hashes", row.BlockNum)
			}
		} else {
			blockSeen[row.BlockNum] = row.BlockHash
			blocks = append(blocks, eventLogV3Block{number: row.BlockNum, hash: row.BlockHash})
		}
		if !havePrev || prev.TxHash != row.TxHash {
			txHashes = append(txHashes, row.TxHash)
		}
		addresses[string(row.Address[:])] = struct{}{}
		prev, havePrev = row, true
		count++
		return true, nil
	})
	return blocks, txHashes, addresses, count, err
}

func validateEventLogV3SourceRow(row EventLog, fromBlock, toBlock uint64) error {
	if row.BlockNum < fromBlock || row.BlockNum > toBlock {
		return fmt.Errorf("snapshots: V3 source row block %d outside [%d,%d]", row.BlockNum, fromBlock, toBlock)
	}
	if row.Log == nil {
		return fmt.Errorf("snapshots: V3 source row block=%d tx=%d log=%d has nil protobuf", row.BlockNum, row.TxIndex, row.LogIndex)
	}
	if !bytes.Equal(row.Log.GetAddress(), row.Address[:]) {
		return fmt.Errorf("snapshots: V3 source row block=%d tx=%d log=%d address mismatch", row.BlockNum, row.TxIndex, row.LogIndex)
	}
	for _, topic := range row.Log.GetTopics() {
		if len(topic) != common.HashLength {
			return fmt.Errorf("snapshots: V3 source row block=%d tx=%d log=%d topic length %d", row.BlockNum, row.TxIndex, row.LogIndex, len(topic))
		}
	}
	return nil
}

type eventLogV3PayloadWriter struct {
	file   *os.File
	buf    []byte
	frames []eventLogV3Frame
	rows   uint32
}

func (w *eventLogV3PayloadWriter) add(firstRow uint64, raw []byte) (frame, offset uint64, err error) {
	if uint64(len(raw)) > math.MaxUint32 {
		return 0, 0, fmt.Errorf("snapshots: V3 protobuf payload length %d exceeds uint32 frame limit", len(raw))
	}
	if w.rows > 0 && len(w.buf)+len(raw) > eventLogV3PayloadTarget {
		if err := w.flush(firstRow - uint64(w.rows)); err != nil {
			return 0, 0, err
		}
	}
	frame, offset = uint64(len(w.frames)), uint64(len(w.buf))
	w.buf = append(w.buf, raw...)
	w.rows++
	return frame, offset, nil
}

func (w *eventLogV3PayloadWriter) flush(firstRow uint64) error {
	if w.rows == 0 {
		return nil
	}
	enc, _, err := cbCodec()
	if err != nil {
		return err
	}
	compressed := enc.EncodeAll(w.buf, nil)
	if uint64(len(w.buf)) > math.MaxUint32 || uint64(len(compressed)) > math.MaxUint32 {
		return errors.New("snapshots: V3 payload frame exceeds uint32 length limit")
	}
	off, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := w.file.Write(compressed); err != nil {
		return err
	}
	w.frames = append(w.frames, eventLogV3Frame{firstRow: firstRow, dataOff: uint64(off), dataLen: uint32(len(compressed)), rowCount: w.rows, checksum: crc32.ChecksumIEEE(w.buf), rawLen: uint32(len(w.buf))})
	w.buf = w.buf[:0]
	w.rows = 0
	return nil
}

func writeEventLogV3Frames(reader rawdb.EventLogReader, fromBlock, toBlock, wantRows uint64, blockIDs map[uint64]uint64, txHashes []common.Hash, addressIDs map[string]uint64, rowData, payloadData *os.File) ([]eventLogV3Frame, []eventLogV3Frame, map[string][]uint64, map[string][]uint64, error) {
	var rowFrames []eventLogV3Frame
	rowBuf := make([]eventLogV3Row, 0, eventLogV3RowFrameRows)
	payload := &eventLogV3PayloadWriter{file: payloadData, buf: make([]byte, 0, eventLogV3PayloadTarget)}
	addressPostings := make(map[string][]uint64, len(addressIDs))
	topicPostings := make(map[string][]uint64)
	var rowIndex uint64
	var txID uint64
	var previousTx common.Hash
	haveTx := false
	flushRows := func() error {
		if len(rowBuf) == 0 {
			return nil
		}
		raw := encodeEventLogV3Rows(rowBuf)
		off, err := rowData.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if _, err := rowData.Write(raw); err != nil {
			return err
		}
		rowFrames = append(rowFrames, eventLogV3Frame{firstRow: rowIndex - uint64(len(rowBuf)), dataOff: uint64(off), dataLen: uint32(len(raw)), rowCount: uint32(len(rowBuf)), checksum: crc32.ChecksumIEEE(raw), rawLen: uint32(len(raw))})
		rowBuf = rowBuf[:0]
		return nil
	}
	err := reader.IterateEventLogs(fromBlock, toBlock, EventLogFilter{}, func(row EventLog) (bool, error) {
		if err := validateEventLogV3SourceRow(row, fromBlock, toBlock); err != nil {
			return false, err
		}
		blockID, ok := blockIDs[row.BlockNum]
		if !ok {
			return false, fmt.Errorf("snapshots: missing V3 block dictionary entry %d", row.BlockNum)
		}
		if !haveTx || previousTx != row.TxHash {
			if haveTx {
				txID++
			}
			if txID >= uint64(len(txHashes)) || txHashes[txID] != row.TxHash {
				return false, fmt.Errorf("snapshots: V3 transaction dictionary changed at row %d", rowIndex)
			}
			previousTx, haveTx = row.TxHash, true
		}
		addressID, ok := addressIDs[string(row.Address[:])]
		if !ok {
			return false, fmt.Errorf("snapshots: missing V3 address dictionary entry %x", row.Address)
		}
		logCopy := proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
		logCopy.Address = nil
		raw, err := proto.Marshal(logCopy)
		if err != nil {
			return false, err
		}
		frame, offset, err := payload.add(rowIndex, raw)
		if err != nil {
			return false, err
		}
		rowBuf = append(rowBuf, eventLogV3Row{blockID: blockID, txIndex: row.TxIndex, logIndex: row.LogIndex, txID: txID, addressID: addressID, payloadFrame: frame, payloadOffset: offset, payloadLength: uint64(len(raw))})
		addressKey := string(eventLogAddressLookupKey(row.Address))
		addressPostings[addressKey] = append(addressPostings[addressKey], rowIndex)
		for position, rawTopic := range row.Log.GetTopics() {
			var topic common.Hash
			copy(topic[:], rawTopic)
			key := string(eventLogTopicLookupKey(uint64(position), topic))
			topicPostings[key] = append(topicPostings[key], rowIndex)
		}
		rowIndex++
		if len(rowBuf) == eventLogV3RowFrameRows {
			if err := flushRows(); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if rowIndex != wantRows {
		return nil, nil, nil, nil, fmt.Errorf("snapshots: V3 source changed between passes: got %d rows, want %d", rowIndex, wantRows)
	}
	if (haveTx && txID+1 != uint64(len(txHashes))) || (!haveTx && len(txHashes) != 0) {
		return nil, nil, nil, nil, errors.New("snapshots: V3 transaction dictionary changed between passes")
	}
	if err := flushRows(); err != nil {
		return nil, nil, nil, nil, err
	}
	if err := payload.flush(rowIndex - uint64(payload.rows)); err != nil {
		return nil, nil, nil, nil, err
	}
	return rowFrames, payload.frames, addressPostings, topicPostings, nil
}

func encodeEventLogV3Rows(rows []eventLogV3Row) []byte {
	raw := make([]byte, 0, len(rows)*16)
	var prev eventLogV3Row
	for i, row := range rows {
		blockDelta, txValue, logValue := row.blockID, row.txIndex, row.logIndex
		txIDDelta, payloadFrameDelta, payloadOffsetValue := row.txID, row.payloadFrame, row.payloadOffset
		if i > 0 {
			blockDelta = row.blockID - prev.blockID
			if blockDelta == 0 {
				txValue = row.txIndex - prev.txIndex
				logValue = row.logIndex - prev.logIndex
			}
			txIDDelta = row.txID - prev.txID
			payloadFrameDelta = row.payloadFrame - prev.payloadFrame
			if payloadFrameDelta == 0 {
				payloadOffsetValue = row.payloadOffset - prev.payloadOffset
			}
		}
		for _, value := range []uint64{blockDelta, txValue, logValue, txIDDelta, row.addressID, payloadFrameDelta, payloadOffsetValue, row.payloadLength} {
			raw = binary.AppendUvarint(raw, value)
		}
		prev = row
	}
	return raw
}

func buildEventLogV3Lookup(dir, base string, keySize int, postings map[string][]uint64) (*eventLogV3LookupBuild, error) {
	data, name, err := createStateDomainChangeBinaryTempFileInDir(dir, base)
	if err != nil {
		return nil, err
	}
	b := &eventLogV3LookupBuild{keySize: uint64(keySize), data: data, name: name}
	keys := make([]string, 0, len(postings))
	for key := range postings {
		if len(key) != keySize {
			b.close()
			return nil, fmt.Errorf("snapshots: V3 lookup key length %d, want %d", len(key), keySize)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := postings[key]
		entry := eventLogV3LookupKey{key: []byte(key), firstFrame: uint64(len(b.frames)), postingCount: uint64(len(values))}
		for start := 0; start < len(values); start += eventLogV3LookupFrameRows {
			end := min(start+eventLogV3LookupFrameRows, len(values))
			chunk := values[start:end]
			raw := make([]byte, 0, len(chunk)*2)
			for i, value := range chunk {
				if i == 0 {
					raw = binary.AppendUvarint(raw, value)
				} else {
					raw = binary.AppendUvarint(raw, value-chunk[i-1])
				}
			}
			off, err := data.Seek(0, io.SeekCurrent)
			if err != nil {
				b.close()
				return nil, err
			}
			if _, err := data.Write(raw); err != nil {
				b.close()
				return nil, err
			}
			b.frames = append(b.frames, eventLogV3LookupFrame{dataOff: uint64(off), dataLen: uint32(len(raw)), count: uint32(len(chunk)), first: chunk[0], checksum: crc32.ChecksumIEEE(raw)})
			entry.frameCount++
		}
		b.keys = append(b.keys, entry)
	}
	stat, err := data.Stat()
	if err != nil {
		b.close()
		return nil, err
	}
	b.dataLen = uint64(stat.Size())
	return b, nil
}

func writeEventLogV3File(dst *os.File, h eventLogV3Header, blocks []eventLogV3Block, txHashes []common.Hash, rowFrames []eventLogV3Frame, rowData *os.File, payloadFrames []eventLogV3Frame, payloadData *os.File, address, topic *eventLogV3LookupBuild) error {
	if err := writeEventLogV3Header(dst, h); err != nil {
		return err
	}
	if err := binary.Write(dst, binary.BigEndian, uint64(len(blocks))); err != nil {
		return err
	}
	for _, block := range blocks {
		if err := binary.Write(dst, binary.BigEndian, block.number); err != nil {
			return err
		}
		if _, err := dst.Write(block.hash[:]); err != nil {
			return err
		}
	}
	if err := binary.Write(dst, binary.BigEndian, uint64(len(txHashes))); err != nil {
		return err
	}
	for _, hash := range txHashes {
		if _, err := dst.Write(hash[:]); err != nil {
			return err
		}
	}
	if err := writeEventLogV3FrameDir(dst, rowFrames, h.rowDataOffset); err != nil {
		return err
	}
	if err := copyEventLogV3Temp(dst, rowData); err != nil {
		return err
	}
	if err := writeEventLogV3FrameDir(dst, payloadFrames, h.payloadDataOffset); err != nil {
		return err
	}
	if err := copyEventLogV3Temp(dst, payloadData); err != nil {
		return err
	}
	if err := writeEventLogV3Lookup(dst, h.addressIndexOffset, address); err != nil {
		return err
	}
	return writeEventLogV3Lookup(dst, h.topicIndexOffset, topic)
}

func copyEventLogV3Temp(dst *os.File, src *os.File) error {
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := io.Copy(dst, src)
	return err
}

func writeEventLogV3FrameDir(w io.Writer, frames []eventLogV3Frame, dataBase uint64) error {
	if err := binary.Write(w, binary.BigEndian, uint64(len(frames))); err != nil {
		return err
	}
	var raw [eventLogV3FrameDirEntry]byte
	for _, frame := range frames {
		binary.BigEndian.PutUint64(raw[0:8], frame.firstRow)
		binary.BigEndian.PutUint64(raw[8:16], dataBase+frame.dataOff)
		binary.BigEndian.PutUint32(raw[16:20], frame.dataLen)
		binary.BigEndian.PutUint32(raw[20:24], frame.rowCount)
		binary.BigEndian.PutUint32(raw[24:28], frame.checksum)
		binary.BigEndian.PutUint32(raw[28:32], frame.rawLen)
		if _, err := w.Write(raw[:]); err != nil {
			return err
		}
	}
	return nil
}

func writeEventLogV3Lookup(w *os.File, sectionOffset uint64, b *eventLogV3LookupBuild) error {
	if err := binary.Write(w, binary.BigEndian, uint64(len(b.keys))); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint64(len(b.frames))); err != nil {
		return err
	}
	for _, key := range b.keys {
		if _, err := w.Write(key.key); err != nil {
			return err
		}
		var tail [eventLogV3LookupKeyTail]byte
		binary.BigEndian.PutUint64(tail[0:8], key.firstFrame)
		binary.BigEndian.PutUint32(tail[8:12], key.frameCount)
		binary.BigEndian.PutUint64(tail[12:20], key.postingCount)
		if _, err := w.Write(tail[:]); err != nil {
			return err
		}
	}
	dataBase := sectionOffset + eventLogV3LookupHeaderSize + uint64(len(b.keys))*(b.keySize+eventLogV3LookupKeyTail) + uint64(len(b.frames))*eventLogV3LookupFrameEntry
	for _, frame := range b.frames {
		var raw [eventLogV3LookupFrameEntry]byte
		binary.BigEndian.PutUint64(raw[0:8], dataBase+frame.dataOff)
		binary.BigEndian.PutUint32(raw[8:12], frame.dataLen)
		binary.BigEndian.PutUint32(raw[12:16], frame.count)
		binary.BigEndian.PutUint64(raw[16:24], frame.first)
		binary.BigEndian.PutUint32(raw[24:28], frame.checksum)
		if _, err := w.Write(raw[:]); err != nil {
			return err
		}
	}
	return copyEventLogV3Temp(w, b.data)
}

func writeEventLogV3Header(w io.Writer, h eventLogV3Header) error {
	var raw [eventLogV3HeaderSize]byte
	copy(raw[:8], eventLogMagicV3[:])
	values := []uint64{h.fromBlock, h.toBlock, h.rowCount, h.rowFrameRows, h.payloadTarget, h.blockDictOffset, h.blockDictLength, h.txDictOffset, h.txDictLength, h.rowDirOffset, h.rowDirLength, h.rowDataOffset, h.rowDataLength, h.payloadDirOffset, h.payloadDirLength, h.payloadDataOffset, h.payloadDataLength, h.addressIndexOffset, h.addressIndexLength, h.topicIndexOffset, h.topicIndexLength}
	for i, value := range values {
		binary.BigEndian.PutUint64(raw[8+i*8:16+i*8], value)
	}
	_, err := w.Write(raw[:])
	return err
}

func readEventLogV3HeaderRest(r io.Reader) (eventLogV3Header, error) {
	var raw [eventLogV3HeaderSize - 8]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return eventLogV3Header{}, err
	}
	values := make([]uint64, 21)
	for i := range values {
		values[i] = binary.BigEndian.Uint64(raw[i*8 : i*8+8])
	}
	return eventLogV3Header{fromBlock: values[0], toBlock: values[1], rowCount: values[2], rowFrameRows: values[3], payloadTarget: values[4], blockDictOffset: values[5], blockDictLength: values[6], txDictOffset: values[7], txDictLength: values[8], rowDirOffset: values[9], rowDirLength: values[10], rowDataOffset: values[11], rowDataLength: values[12], payloadDirOffset: values[13], payloadDirLength: values[14], payloadDataOffset: values[15], payloadDataLength: values[16], addressIndexOffset: values[17], addressIndexLength: values[18], topicIndexOffset: values[19], topicIndexLength: values[20]}, nil
}

func validateEventLogV3Header(ref SegmentRef, h eventLogV3Header, fileSize uint64) error {
	if h.fromBlock != ref.FromTxNum || h.toBlock != ref.ToTxNum {
		return fmt.Errorf("snapshots: V3 event log %q range [%d,%d], want [%d,%d]", ref.Path, h.fromBlock, h.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	if h.toBlock < h.fromBlock || h.rowFrameRows != eventLogV3RowFrameRows || h.payloadTarget != eventLogV3PayloadTarget {
		return fmt.Errorf("snapshots: V3 event log %q has unsupported framing", ref.Path)
	}
	sections := [][2]uint64{{h.blockDictOffset, h.blockDictLength}, {h.txDictOffset, h.txDictLength}, {h.rowDirOffset, h.rowDirLength}, {h.rowDataOffset, h.rowDataLength}, {h.payloadDirOffset, h.payloadDirLength}, {h.payloadDataOffset, h.payloadDataLength}, {h.addressIndexOffset, h.addressIndexLength}, {h.topicIndexOffset, h.topicIndexLength}}
	next := uint64(eventLogV3HeaderSize)
	for i, section := range sections {
		if section[0] != next {
			return fmt.Errorf("snapshots: V3 event log %q section %d offset %d, want %d", ref.Path, i, section[0], next)
		}
		end, overflow := checkedAdd(section[0], section[1])
		if overflow || end > fileSize {
			return fmt.Errorf("snapshots: V3 event log %q section %d outside file", ref.Path, i)
		}
		next = end
	}
	if next != fileSize {
		return fmt.Errorf("snapshots: V3 event log %q ends at %d, file size %d", ref.Path, next, fileSize)
	}
	return nil
}

func openEventLogV3Reader(file io.ReaderAt, h eventLogV3Header, size uint64) (*eventLogV3Reader, error) {
	blockCount, err := readEventLogUint64At(file, h.blockDictOffset)
	if err != nil {
		return nil, err
	}
	blockBytes, overflow := checkedMul(blockCount, 8+common.HashLength)
	blockLength, addOverflow := checkedAdd(8, blockBytes)
	if overflow || addOverflow || h.blockDictLength != blockLength {
		return nil, errors.New("snapshots: invalid V3 block dictionary length")
	}
	txCount, err := readEventLogUint64At(file, h.txDictOffset)
	if err != nil {
		return nil, err
	}
	txBytes, overflow := checkedMul(txCount, common.HashLength)
	txLength, addOverflow := checkedAdd(8, txBytes)
	if overflow || addOverflow || h.txDictLength != txLength {
		return nil, errors.New("snapshots: invalid V3 transaction dictionary length")
	}
	rowFrames, err := readEventLogUint64At(file, h.rowDirOffset)
	if err != nil {
		return nil, err
	}
	rowDirBytes, overflow := checkedMul(rowFrames, eventLogV3FrameDirEntry)
	rowDirLength, addOverflow := checkedAdd(8, rowDirBytes)
	if overflow || addOverflow || h.rowDirLength != rowDirLength {
		return nil, errors.New("snapshots: invalid V3 row directory length")
	}
	if rowFrames != ceilDiv(h.rowCount, eventLogV3RowFrameRows) {
		return nil, errors.New("snapshots: invalid V3 row frame count")
	}
	payloadFrames, err := readEventLogUint64At(file, h.payloadDirOffset)
	if err != nil {
		return nil, err
	}
	payloadDirBytes, overflow := checkedMul(payloadFrames, eventLogV3FrameDirEntry)
	payloadDirLength, addOverflow := checkedAdd(8, payloadDirBytes)
	if overflow || addOverflow || h.payloadDirLength != payloadDirLength {
		return nil, errors.New("snapshots: invalid V3 payload directory length")
	}
	if (h.rowCount == 0 && payloadFrames != 0) || (h.rowCount != 0 && payloadFrames == 0) {
		return nil, errors.New("snapshots: invalid V3 payload frame count")
	}
	addressCount, _, err := readEventLogV3LookupCounts(file, h.addressIndexOffset, h.addressIndexLength, size, eventLogAddressLookupKeySize)
	if err != nil {
		return nil, err
	}
	return &eventLogV3Reader{header: h, rowFrameCount: rowFrames, payloadFrames: payloadFrames, blockCount: blockCount, txCount: txCount, addressCount: addressCount}, nil
}

func readEventLogV3FrameAt(file io.ReaderAt, dirOffset, index, count uint64) (eventLogV3Frame, error) {
	if index >= count {
		return eventLogV3Frame{}, fmt.Errorf("snapshots: V3 frame %d outside count %d", index, count)
	}
	var raw [eventLogV3FrameDirEntry]byte
	if _, err := file.ReadAt(raw[:], int64(dirOffset+8+index*eventLogV3FrameDirEntry)); err != nil {
		return eventLogV3Frame{}, err
	}
	return eventLogV3Frame{firstRow: binary.BigEndian.Uint64(raw[0:8]), dataOff: binary.BigEndian.Uint64(raw[8:16]), dataLen: binary.BigEndian.Uint32(raw[16:20]), rowCount: binary.BigEndian.Uint32(raw[20:24]), checksum: binary.BigEndian.Uint32(raw[24:28]), rawLen: binary.BigEndian.Uint32(raw[28:32])}, nil
}

func (s *EventLogSegment) readEventLogV3Row(rowIndex uint64) (eventLogV3Row, error) {
	if rowIndex >= s.header.rowCount {
		return eventLogV3Row{}, fmt.Errorf("snapshots: V3 row %d outside count %d", rowIndex, s.header.rowCount)
	}
	frameIndex := rowIndex / eventLogV3RowFrameRows
	if !s.v3.cacheFrameValid || s.v3.cacheFrame != frameIndex {
		frame, err := readEventLogV3FrameAt(s.file, s.v3.header.rowDirOffset, frameIndex, s.v3.rowFrameCount)
		if err != nil {
			return eventLogV3Row{}, err
		}
		expectedRows := uint32(eventLogV3RowFrameRows)
		if remaining := s.header.rowCount - frameIndex*eventLogV3RowFrameRows; remaining < uint64(expectedRows) {
			expectedRows = uint32(remaining)
		}
		if frame.firstRow != frameIndex*eventLogV3RowFrameRows || frame.rowCount != expectedRows || frame.rawLen != frame.dataLen || frame.dataOff < s.v3.header.rowDataOffset {
			return eventLogV3Row{}, fmt.Errorf("snapshots: V3 row frame %d metadata mismatch", frameIndex)
		}
		raw, err := readEventLogPayloadAt(s.file, frame.dataOff, uint64(frame.dataLen), s.v3.header.rowDataOffset+s.v3.header.rowDataLength)
		if err != nil {
			return eventLogV3Row{}, err
		}
		if crc32.ChecksumIEEE(raw) != frame.checksum {
			return eventLogV3Row{}, fmt.Errorf("snapshots: V3 row frame %d checksum mismatch", frameIndex)
		}
		rows, err := decodeEventLogV3Rows(raw, int(frame.rowCount))
		if err != nil {
			return eventLogV3Row{}, fmt.Errorf("snapshots: V3 row frame %d: %w", frameIndex, err)
		}
		s.v3.cacheFrame, s.v3.cacheRows, s.v3.cacheFrameValid = frameIndex, rows, true
	}
	return s.v3.cacheRows[rowIndex%eventLogV3RowFrameRows], nil
}

func decodeEventLogV3Rows(raw []byte, count int) ([]eventLogV3Row, error) {
	rows := make([]eventLogV3Row, 0, count)
	reader := bytes.NewReader(raw)
	var prev eventLogV3Row
	for i := 0; i < count; i++ {
		var values [8]uint64
		for j := range values {
			value, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, err
			}
			values[j] = value
		}
		row := eventLogV3Row{blockID: values[0], txIndex: values[1], logIndex: values[2], txID: values[3], addressID: values[4], payloadFrame: values[5], payloadOffset: values[6], payloadLength: values[7]}
		if i > 0 {
			row.blockID += prev.blockID
			if values[0] == 0 {
				row.txIndex += prev.txIndex
				row.logIndex += prev.logIndex
			}
			row.txID += prev.txID
			row.payloadFrame += prev.payloadFrame
			if values[5] == 0 {
				row.payloadOffset += prev.payloadOffset
			}
		}
		if row.blockID < prev.blockID || row.txID < prev.txID || row.payloadFrame < prev.payloadFrame {
			return nil, errors.New("dictionary id overflow")
		}
		rows = append(rows, row)
		prev = row
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("%d trailing row bytes", reader.Len())
	}
	return rows, nil
}

func (s *EventLogSegment) materializeEventLogV3(row eventLogV3Row) (EventLog, error) {
	if row.blockID >= s.v3.blockCount || row.txID >= s.v3.txCount || row.addressID >= s.v3.addressCount || row.payloadFrame >= s.v3.payloadFrames {
		return EventLog{}, errors.New("snapshots: V3 row dictionary id outside bounds")
	}
	var blockRaw [8 + common.HashLength]byte
	if _, err := s.file.ReadAt(blockRaw[:], int64(s.v3.header.blockDictOffset+8+row.blockID*uint64(len(blockRaw)))); err != nil {
		return EventLog{}, err
	}
	blockNum := binary.BigEndian.Uint64(blockRaw[:8])
	var blockHash common.Hash
	copy(blockHash[:], blockRaw[8:])
	var txHash common.Hash
	if _, err := s.file.ReadAt(txHash[:], int64(s.v3.header.txDictOffset+8+row.txID*common.HashLength)); err != nil {
		return EventLog{}, err
	}
	addressEntrySize := uint64(eventLogAddressLookupKeySize + eventLogV3LookupKeyTail)
	addressRaw := make([]byte, eventLogAddressLookupKeySize)
	if _, err := s.file.ReadAt(addressRaw, int64(s.v3.header.addressIndexOffset+eventLogV3LookupHeaderSize+row.addressID*addressEntrySize)); err != nil {
		return EventLog{}, err
	}
	address := common.BytesToAddress(addressRaw)
	if !s.v3.cachePayloadValid || s.v3.cachePayload != row.payloadFrame {
		payloadFrame, err := readEventLogV3FrameAt(s.file, s.v3.header.payloadDirOffset, row.payloadFrame, s.v3.payloadFrames)
		if err != nil {
			return EventLog{}, err
		}
		compressed, err := readEventLogPayloadAt(s.file, payloadFrame.dataOff, uint64(payloadFrame.dataLen), s.v3.header.payloadDataOffset+s.v3.header.payloadDataLength)
		if err != nil {
			return EventLog{}, err
		}
		_, dec, err := cbCodec()
		if err != nil {
			return EventLog{}, err
		}
		decoded, err := dec.DecodeAll(compressed, make([]byte, 0, int(payloadFrame.rawLen)))
		if err != nil {
			return EventLog{}, err
		}
		if len(decoded) != int(payloadFrame.rawLen) {
			return EventLog{}, errors.New("snapshots: V3 payload frame decoded length mismatch")
		}
		if crc32.ChecksumIEEE(decoded) != payloadFrame.checksum {
			return EventLog{}, errors.New("snapshots: V3 payload frame checksum mismatch")
		}
		s.v3.cachePayload, s.v3.cachePayloadBytes, s.v3.cachePayloadValid = row.payloadFrame, decoded, true
	}
	decoded := s.v3.cachePayloadBytes
	end, overflow := checkedAdd(row.payloadOffset, row.payloadLength)
	if overflow || end > uint64(len(decoded)) {
		return EventLog{}, errors.New("snapshots: V3 payload slice outside frame")
	}
	var log corepb.TransactionInfo_Log
	if err := proto.Unmarshal(decoded[row.payloadOffset:end], &log); err != nil {
		return EventLog{}, err
	}
	log.Address = append([]byte(nil), address[:]...)
	entry := eventLogIndexEntry{blockNum: blockNum, txIndex: row.txIndex, logIndex: row.logIndex, txHash: txHash, blockHash: blockHash, address: address}
	if err := validateEventLogPayload(entry, &log, "V3 event log read"); err != nil {
		return EventLog{}, err
	}
	return EventLog{BlockNum: blockNum, TxIndex: row.txIndex, LogIndex: row.logIndex, TxHash: txHash, BlockHash: blockHash, Address: address, Log: &log}, nil
}

func (s *EventLogSegment) iterateEventLogV3FullScan(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	for i := uint64(0); i < s.header.rowCount; i++ {
		row, err := s.readEventLogV3Row(i)
		if err != nil {
			return err
		}
		log, err := s.materializeEventLogV3(row)
		if err != nil {
			return err
		}
		if log.BlockNum < fromBlock || log.BlockNum > toBlock || !eventLogAddressMatches(filter, log.Address) || !eventLogTopicsMatch(filter.Topics, log.Log.GetTopics()) {
			continue
		}
		cont, err := fn(log)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (s *EventLogSegment) iterateEventLogV3ByLookup(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) (bool, error) {
	rows, ok, err := s.lookupEventLogV3CandidateRows(filter)
	if err != nil || !ok {
		return ok, err
	}
	for _, i := range rows {
		row, err := s.readEventLogV3Row(i)
		if err != nil {
			return true, err
		}
		log, err := s.materializeEventLogV3(row)
		if err != nil {
			return true, err
		}
		if log.BlockNum < fromBlock || log.BlockNum > toBlock {
			continue
		}
		if !eventLogAddressMatches(filter, log.Address) || !eventLogTopicsMatch(filter.Topics, log.Log.GetTopics()) {
			return true, fmt.Errorf("snapshots: V3 lookup row %d does not match filter", i)
		}
		cont, err := fn(log)
		if err != nil || !cont {
			return true, err
		}
	}
	return true, nil
}

func (s *EventLogSegment) lookupEventLogV3CandidateRows(filter EventLogFilter) ([]uint64, bool, error) {
	var candidates []uint64
	have := false
	if len(filter.Addresses) > 0 {
		var union []uint64
		for _, address := range filter.Addresses {
			rows, err := readEventLogV3LookupRows(s.file, s.v3.header.addressIndexOffset, s.v3.header.addressIndexLength, s.size, eventLogAddressLookupKeySize, eventLogAddressLookupKey(address))
			if err != nil {
				return nil, true, err
			}
			union = unionSortedUint64(union, rows)
		}
		candidates, have = union, true
	}
	for position, required := range filter.Topics {
		if len(required) == 0 {
			continue
		}
		var union []uint64
		for _, topic := range required {
			rows, err := readEventLogV3LookupRows(s.file, s.v3.header.topicIndexOffset, s.v3.header.topicIndexLength, s.size, eventLogTopicLookupKeySize, eventLogTopicLookupKey(uint64(position), topic))
			if err != nil {
				return nil, true, err
			}
			union = unionSortedUint64(union, rows)
		}
		if have {
			candidates = intersectSortedUint64(candidates, union)
		} else {
			candidates, have = union, true
		}
		if len(candidates) == 0 {
			return candidates, true, nil
		}
	}
	return candidates, have, nil
}

func readEventLogV3LookupCounts(file io.ReaderAt, offset, length, size uint64, keySize int) (uint64, uint64, error) {
	if length < eventLogV3LookupHeaderSize || offset+length > size {
		return 0, 0, errors.New("snapshots: invalid V3 lookup bounds")
	}
	keys, err := readEventLogUint64At(file, offset)
	if err != nil {
		return 0, 0, err
	}
	frames, err := readEventLogUint64At(file, offset+8)
	return keys, frames, err
}

func readEventLogV3LookupRows(file io.ReaderAt, offset, length, size uint64, keySize int, want []byte) ([]uint64, error) {
	keys, frames, err := readEventLogV3LookupCounts(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	entrySize := uint64(keySize + eventLogV3LookupKeyTail)
	frameDir := offset + eventLogV3LookupHeaderSize + keys*entrySize
	dataStart := frameDir + frames*eventLogV3LookupFrameEntry
	if dataStart > offset+length {
		return nil, errors.New("snapshots: V3 lookup directories outside section")
	}
	lo, hi := uint64(0), keys
	raw := make([]byte, keySize+eventLogV3LookupKeyTail)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if _, err := file.ReadAt(raw, int64(offset+eventLogV3LookupHeaderSize+mid*entrySize)); err != nil {
			return nil, err
		}
		cmp := bytes.Compare(raw[:keySize], want)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= keys {
		return nil, nil
	}
	if _, err := file.ReadAt(raw, int64(offset+eventLogV3LookupHeaderSize+lo*entrySize)); err != nil {
		return nil, err
	}
	if !bytes.Equal(raw[:keySize], want) {
		return nil, nil
	}
	tail := raw[keySize:]
	firstFrame := binary.BigEndian.Uint64(tail[0:8])
	frameCount := binary.BigEndian.Uint32(tail[8:12])
	postingCount := binary.BigEndian.Uint64(tail[12:20])
	if firstFrame+uint64(frameCount) > frames {
		return nil, errors.New("snapshots: V3 lookup frame range outside directory")
	}
	rows := make([]uint64, 0, postingCount)
	for i := uint64(0); i < uint64(frameCount); i++ {
		var fr [eventLogV3LookupFrameEntry]byte
		if _, err := file.ReadAt(fr[:], int64(frameDir+(firstFrame+i)*eventLogV3LookupFrameEntry)); err != nil {
			return nil, err
		}
		dataOff := binary.BigEndian.Uint64(fr[0:8])
		dataLen := binary.BigEndian.Uint32(fr[8:12])
		count := binary.BigEndian.Uint32(fr[12:16])
		first := binary.BigEndian.Uint64(fr[16:24])
		checksum := binary.BigEndian.Uint32(fr[24:28])
		if dataOff < uint64(dataStart) || dataOff+uint64(dataLen) > offset+length {
			return nil, errors.New("snapshots: V3 lookup data outside section")
		}
		data, err := readEventLogPayloadAt(file, dataOff, uint64(dataLen), offset+length)
		if err != nil {
			return nil, err
		}
		if crc32.ChecksumIEEE(data) != checksum {
			return nil, errors.New("snapshots: V3 lookup frame checksum mismatch")
		}
		br := bytes.NewReader(data)
		var prev uint64
		for j := uint32(0); j < count; j++ {
			value, err := binary.ReadUvarint(br)
			if err != nil {
				return nil, err
			}
			if j == 0 {
				prev = value
				if prev != first {
					return nil, errors.New("snapshots: V3 lookup first posting mismatch")
				}
			} else {
				if value == 0 || math.MaxUint64-prev < value {
					return nil, errors.New("snapshots: V3 lookup posting delta invalid")
				}
				prev += value
			}
			rows = append(rows, prev)
		}
		if br.Len() != 0 {
			return nil, errors.New("snapshots: V3 lookup trailing bytes")
		}
	}
	if uint64(len(rows)) != postingCount {
		return nil, errors.New("snapshots: V3 lookup posting count mismatch")
	}
	return rows, nil
}

func checkEventLogV3Segment(file *os.File, ref SegmentRef, header eventLogHeader, size uint64) error {
	reader, err := openEventLogV3Reader(file, *header.v3, size)
	if err != nil {
		return err
	}
	seg := &EventLogSegment{ref: ref, file: file, header: header, size: size, v3: reader}
	nextPayloadRow, nextPayloadOffset := uint64(0), reader.header.payloadDataOffset
	for i := uint64(0); i < reader.payloadFrames; i++ {
		frame, err := readEventLogV3FrameAt(file, reader.header.payloadDirOffset, i, reader.payloadFrames)
		if err != nil {
			return err
		}
		if frame.firstRow != nextPayloadRow || frame.rowCount == 0 || frame.dataOff != nextPayloadOffset {
			return fmt.Errorf("snapshots: V3 payload frame %d metadata mismatch", i)
		}
		nextPayloadRow += uint64(frame.rowCount)
		nextPayloadOffset += uint64(frame.dataLen)
	}
	if nextPayloadRow != header.rowCount || nextPayloadOffset != reader.header.payloadDataOffset+reader.header.payloadDataLength {
		return errors.New("snapshots: V3 payload frames do not cover payload section")
	}
	expectedAddress := make(map[string][]uint64)
	expectedTopic := make(map[string][]uint64)
	var prev EventLog
	have := false
	for i := uint64(0); i < header.rowCount; i++ {
		row, err := seg.readEventLogV3Row(i)
		if err != nil {
			return err
		}
		log, err := seg.materializeEventLogV3(row)
		if err != nil {
			return fmt.Errorf("snapshots: V3 event log %q row %d: %w", ref.Path, i, err)
		}
		if log.BlockNum < ref.FromTxNum || log.BlockNum > ref.ToTxNum {
			return errors.New("snapshots: V3 row block outside segment")
		}
		if have && compareEventLogEntries(eventLogIndexEntry{blockNum: prev.BlockNum, txIndex: prev.TxIndex, logIndex: prev.LogIndex}, eventLogIndexEntry{blockNum: log.BlockNum, txIndex: log.TxIndex, logIndex: log.LogIndex}) >= 0 {
			return errors.New("snapshots: V3 rows not strictly ordered")
		}
		have = true
		prev = log
		ak := string(eventLogAddressLookupKey(log.Address))
		expectedAddress[ak] = append(expectedAddress[ak], i)
		for p, t := range log.Log.GetTopics() {
			var topic common.Hash
			copy(topic[:], t)
			tk := string(eventLogTopicLookupKey(uint64(p), topic))
			expectedTopic[tk] = append(expectedTopic[tk], i)
		}
	}
	actualAddress, err := readAllEventLogV3Lookup(file, header.v3.addressIndexOffset, header.v3.addressIndexLength, size, eventLogAddressLookupKeySize)
	if err != nil {
		return err
	}
	actualTopic, err := readAllEventLogV3Lookup(file, header.v3.topicIndexOffset, header.v3.topicIndexLength, size, eventLogTopicLookupKeySize)
	if err != nil {
		return err
	}
	if err := compareEventLogSegmentLookupIndexMaps(ref, "V3 address", expectedAddress, actualAddress); err != nil {
		return err
	}
	return compareEventLogSegmentLookupIndexMaps(ref, "V3 topic", expectedTopic, actualTopic)
}

func readAllEventLogV3Lookup(file io.ReaderAt, offset, length, size uint64, keySize int) (map[string][]uint64, error) {
	keys, _, err := readEventLogV3LookupCounts(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]uint64, keys)
	entrySize := uint64(keySize + eventLogV3LookupKeyTail)
	var prev []byte
	for i := uint64(0); i < keys; i++ {
		key := make([]byte, keySize)
		if _, err := file.ReadAt(key, int64(offset+eventLogV3LookupHeaderSize+i*entrySize)); err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(prev, key) >= 0 {
			return nil, errors.New("snapshots: V3 lookup keys not sorted")
		}
		rows, err := readEventLogV3LookupRows(file, offset, length, size, keySize, key)
		if err != nil {
			return nil, err
		}
		out[string(key)] = rows
		prev = key
	}
	return out, nil
}

func readEventLogV3LookupKeys(file io.ReaderAt, offset, length, size uint64, keySize int) ([][]byte, error) {
	count, _, err := readEventLogV3LookupCounts(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	entrySize := uint64(keySize + eventLogV3LookupKeyTail)
	keys := make([][]byte, 0, count)
	var previous []byte
	for i := uint64(0); i < count; i++ {
		key := make([]byte, keySize)
		if _, err := file.ReadAt(key, int64(offset+eventLogV3LookupHeaderSize+i*entrySize)); err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(previous, key) >= 0 {
			return nil, errors.New("snapshots: V3 lookup keys not strictly sorted")
		}
		keys = append(keys, key)
		previous = key
	}
	return keys, nil
}

func writeFreshEventLogV3Index(dir string, eventRef SegmentRef, relPath string) (SegmentRef, error) {
	seg, err := OpenEventLogSegment(dir, eventRef)
	if err != nil {
		return SegmentRef{}, err
	}
	defer seg.Close()
	if seg.header.version != EventLogSegmentV3Version {
		return SegmentRef{}, fmt.Errorf("snapshots: event log %q is not V3", eventRef.Path)
	}
	addressKeys, err := readEventLogV3LookupKeys(seg.file, seg.v3.header.addressIndexOffset, seg.v3.header.addressIndexLength, seg.size, eventLogAddressLookupKeySize)
	if err != nil {
		return SegmentRef{}, err
	}
	topicKeys, err := readEventLogV3LookupKeys(seg.file, seg.v3.header.topicIndexOffset, seg.v3.header.topicIndexLength, seg.size, eventLogTopicLookupKeySize)
	if err != nil {
		return SegmentRef{}, err
	}
	addressPostings := make(map[string][]uint64, len(addressKeys))
	for _, key := range addressKeys {
		addressPostings[string(key)] = []uint64{eventRef.FromTxNum}
	}
	topicPostings := make(map[string][]uint64, len(topicKeys))
	for _, key := range topicKeys {
		topicPostings[string(key)] = []uint64{eventRef.FromTxNum}
	}
	if relPath == "" {
		relPath = EventLogIndexSegmentPath(eventRef.FromTxNum, eventRef.ToTxNum)
	}
	ref := SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: eventRef.FromTxNum, ToTxNum: eventRef.ToTxNum, Path: filepath.ToSlash(relPath)}
	return writeEventLogIndexSegment(dir, ref, addressPostings, topicPostings)
}
