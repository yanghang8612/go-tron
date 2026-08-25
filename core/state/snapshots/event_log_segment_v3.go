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
	"path/filepath"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// V4 removes address and topic bytes from each protobuf payload. The row keeps
// ordered topic dictionary IDs, while the compact lookup doubles as the topic
// dictionary. The external event-log-index uses the same checksummed compact
// lookup encoding.
const (
	eventLogV3HeaderSize       = 176
	eventLogV3RowFrameRows     = 256
	eventLogV3PayloadTarget    = 32 << 10
	eventLogV3FrameDirEntry    = 32
	eventLogV3LookupHeaderSize = 16
	eventLogV3LookupFrameRows  = 1024
	eventLogV3LookupFrameEntry = 32
	eventLogV3LookupKeyTail    = 28

	// The compact lookup keeps the V3 outer segment format while replacing the
	// fixed key and per-1024-posting directories with front-coded key blocks and
	// one delta-varint stream per key. Fresh-genesis readers require the v3
	// marker, whose header and directory entries are checksummed.
	eventLogV3LookupV2HeaderSize = 64
	eventLogV3LookupV2BlockKeys  = 128
	eventLogV3LookupV2BlockTail  = 28
	eventLogV3LookupV2StoredRaw  = uint32(1 << 31)
)

var eventLogV3LookupV2Magic = [8]byte{'g', 't', 'e', 'v', 'l', 'i', '3', '\n'}

var (
	eventLogV4ValidationRunsCounter        = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/runs", nil)
	eventLogV4ValidationRowsCounter        = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/rows", nil)
	eventLogV4ValidationBytesCounter       = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/bytes", nil)
	eventLogV4ValidationNanosCounter       = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/nanoseconds", nil)
	eventLogV4ValidationSemanticCounter    = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/semantic_nanoseconds", nil)
	eventLogV4ValidationPostingFillCounter = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/posting_fill_nanoseconds", nil)
	eventLogV4ValidationLookupCounter      = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/lookup_nanoseconds", nil)
	eventLogV4ValidationPostingReadCounter = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/posting_source_reads", nil)
	eventLogV4ValidationPostingByteCounter = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"event_log_v4/validation/posting_source_bytes", nil)
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
	topicCount        uint64
	cacheFrame        uint64
	cacheRows         []eventLogV3Row
	cacheFrameValid   bool
	cachePayload      uint64
	cachePayloadBytes []byte
	cachePayloadValid bool
	cacheAddressBlock uint64
	cacheAddressKeys  [][]byte
	cacheAddressValid bool
	topicLookup       eventLogV3LookupV2Header
	cacheTopicBlocks  map[uint64][][]byte
	cacheTopicOrder   []uint64
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
	topicIDs                                    []uint64
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
	checksum     uint32
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
	keyData *os.File
	keyName string
	keyLen  uint64
	blocks  []eventLogV3LookupV2Block
}

type eventLogV3LookupV2Block struct {
	firstKey []byte
	dataOff  uint64
	dataLen  uint32
	rawLen   uint32
	checksum uint32
	keyCount uint32
	entryCRC uint32
}

type eventLogV3LookupV2Header struct {
	keySize, blockKeys      uint32
	keyCount, blockCount    uint64
	blockDirLen, keyDataLen uint64
	postingDataLen          uint64
}

type eventLogV3LookupV2Record struct {
	key          []byte
	postingOff   uint64
	postingLen   uint64
	postingCount uint64
	checksum     uint32
}

type eventLogV3ChainReader struct {
	chain *rawdb.ChainDB
}

func (r eventLogV3ChainReader) EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	if r.chain == nil {
		return false, errors.New("snapshots: nil V3 event-log chain database")
	}
	return toBlock >= fromBlock, nil
}

func (r eventLogV3ChainReader) IterateEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	if r.chain == nil {
		return errors.New("snapshots: nil V3 event-log chain database")
	}
	if toBlock < fromBlock {
		return fmt.Errorf("snapshots: V4 chain event-log range [%d,%d] is inverted", fromBlock, toBlock)
	}
	for blockNum := fromBlock; ; blockNum++ {
		block, ok, err := rawdb.ReadBlockStrict(r.chain, blockNum)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshots: missing block %d during V3 event-log build", blockNum)
		}
		blockHash := block.Hash()
		txs := block.Transactions()
		infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(r.chain, blockNum)
		if err != nil {
			return err
		}
		if err := rawdb.ValidateTransactionInfosForBlock(blockNum, txs, infos, "V4 event-log segment build"); err != nil {
			return err
		}
		logIndex := uint64(0)
		for txIndex, info := range infos {
			txHash := common.Hash{}
			if txIndex < len(txs) {
				txHash = txs[txIndex].Hash()
			} else if len(info.Id) == common.HashLength {
				copy(txHash[:], info.Id)
			}
			for _, log := range info.GetLog() {
				if log == nil {
					continue
				}
				address := eventLogAddress(log.GetAddress())
				row := EventLog{BlockNum: blockNum, TxIndex: uint64(txIndex), LogIndex: logIndex, TxHash: txHash, BlockHash: blockHash, Address: address, Log: log}
				logIndex++
				if !eventLogAddressMatches(filter, address) || !eventLogTopicsMatch(filter.Topics, log.GetTopics()) {
					continue
				}
				cont, err := fn(row)
				if err != nil || !cont {
					return err
				}
			}
		}
		if blockNum == toBlock {
			return nil
		}
	}
}

func BuildEventLogV4SegmentFromChain(chain *rawdb.ChainDB, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	if chain == nil {
		return SegmentRef{}, errors.New("snapshots: nil chain database")
	}
	return BuildEventLogV4SegmentFromReader(eventLogV3ChainReader{chain: chain}, dir, relPath, fromBlock, toBlock)
}

type EventLogV4PhysicalStats struct {
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

func InspectEventLogV4Physical(dir string, ref SegmentRef) (EventLogV4PhysicalStats, error) {
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		return EventLogV4PhysicalStats{}, err
	}
	defer seg.Close()
	if seg.header.version != EventLogSegmentV4Version {
		return EventLogV4PhysicalStats{}, fmt.Errorf("snapshots: event log %q is version %d, want V4", ref.Path, seg.header.version)
	}
	h := seg.v3.header
	out := EventLogV4PhysicalStats{
		HeaderBytes: eventLogV3HeaderSize, BlockDictionaryBytes: h.blockDictLength, TxDictionaryBytes: h.txDictLength,
		RowDirectoryBytes: h.rowDirLength, RowDeltaBytes: h.rowDataLength, PayloadDirectoryBytes: h.payloadDirLength,
		PayloadCompressedBytes: h.payloadDataLength, AddressLookupBytes: h.addressIndexLength, TopicLookupBytes: h.topicIndexLength, TotalBytes: seg.size,
	}
	for i := uint64(0); i < seg.v3.rowFrameCount; i++ {
		frame, err := readEventLogV3FrameAt(seg.file, h.rowDirOffset, i, seg.v3.rowFrameCount)
		if err != nil {
			return EventLogV4PhysicalStats{}, err
		}
		out.MaxRowFrameReadBytes = max(out.MaxRowFrameReadBytes, uint64(frame.dataLen))
	}
	for i := uint64(0); i < seg.v3.payloadFrames; i++ {
		frame, err := readEventLogV3FrameAt(seg.file, h.payloadDirOffset, i, seg.v3.payloadFrames)
		if err != nil {
			return EventLogV4PhysicalStats{}, err
		}
		out.MaxPayloadFrameRead = max(out.MaxPayloadFrameRead, uint64(frame.dataLen))
		out.MaxPointDecompressBytes = max(out.MaxPointDecompressBytes, uint64(frame.rawLen))
	}
	// One row frame and one payload frame plus their directory entries and
	// direct block/tx/address dictionary reads. Filesystem block amplification
	// is intentionally not guessed here.
	addressDictionaryRead, err := maxEventLogV3LookupKeyAtRead(seg.file, h.addressIndexOffset, h.addressIndexLength, seg.size, eventLogAddressLookupKeySize)
	if err != nil {
		return EventLogV4PhysicalStats{}, err
	}
	out.MaxPointReadBytes = eventLogV3FrameDirEntry + out.MaxRowFrameReadBytes + (8 + common.HashLength) + common.HashLength + addressDictionaryRead + eventLogV3FrameDirEntry + out.MaxPayloadFrameRead
	out.MaxAddressLookupRead, err = maxEventLogV3LookupRead(seg.file, h.addressIndexOffset, h.addressIndexLength, seg.size, eventLogAddressLookupKeySize)
	if err != nil {
		return EventLogV4PhysicalStats{}, err
	}
	out.MaxTopicLookupRead, err = maxEventLogV3LookupRead(seg.file, h.topicIndexOffset, h.topicIndexLength, seg.size, eventLogTopicLookupKeySize)
	if err != nil {
		return EventLogV4PhysicalStats{}, err
	}
	out.MaxFilterLookupRead = max(out.MaxAddressLookupRead, out.MaxTopicLookupRead)
	return out, nil
}

func maxEventLogV3LookupKeyAtRead(file io.ReaderAt, offset, length, size uint64, keySize int) (uint64, error) {
	compact, err := isEventLogV3LookupV2(file, offset, length, size)
	if err != nil {
		return 0, err
	}
	if !compact {
		return uint64(keySize + eventLogV3LookupKeyTail), nil
	}
	h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
	if err != nil {
		return 0, err
	}
	entrySize := uint64(keySize + eventLogV3LookupV2BlockTail)
	var maximum uint64
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogV3LookupV2Block(file, offset, h, i)
		if err != nil {
			return 0, err
		}
		maximum = max(maximum, entrySize+uint64(block.dataLen&^eventLogV3LookupV2StoredRaw))
	}
	return maximum, nil
}

func maxEventLogV3LookupRead(file io.ReaderAt, offset, length, size uint64, keySize int) (uint64, error) {
	compact, err := isEventLogV3LookupV2(file, offset, length, size)
	if err != nil {
		return 0, err
	}
	if compact {
		return maxEventLogV3LookupV2Read(file, offset, length, size, keySize)
	}
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
			return 0, errors.New("snapshots: V4 lookup frame range outside directory")
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

func maxEventLogV3LookupV2Read(file io.ReaderAt, offset, length, size uint64, keySize int) (uint64, error) {
	h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
	if err != nil {
		return 0, err
	}
	entrySize := uint64(keySize + eventLogV3LookupV2BlockTail)
	var searchReads uint64
	for n := h.blockCount; n > 0; n >>= 1 {
		searchReads += entrySize
	}
	var maximum uint64
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogV3LookupV2Block(file, offset, h, i)
		if err != nil {
			return 0, err
		}
		records, storedBytes, err := readEventLogV3LookupV2Records(file, offset, length, h, block)
		if err != nil {
			return 0, err
		}
		for _, record := range records {
			maximum = max(maximum, searchReads+entrySize+storedBytes+record.postingLen)
		}
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
	if b.keyData != nil {
		_ = b.keyData.Close()
	}
	if b.keyName != "" {
		_ = os.Remove(b.keyName)
	}
}

func (b *eventLogV3LookupBuild) length() uint64 {
	if b.keyData != nil {
		return eventLogV3LookupV2HeaderSize + uint64(len(b.blocks))*(b.keySize+eventLogV3LookupV2BlockTail) + b.keyLen + b.dataLen
	}
	return eventLogV3LookupHeaderSize + uint64(len(b.keys))*(b.keySize+eventLogV3LookupKeyTail) + uint64(len(b.frames))*eventLogV3LookupFrameEntry + b.dataLen
}

// BuildEventLogV4SegmentFromReader rewrites a continuously covered immutable
// range without opening chaindata. It performs two passes over the pinned
// reader so large protobuf payloads are never retained in memory.
func BuildEventLogV4SegmentFromReader(reader rawdb.EventLogReader, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	if reader == nil {
		return SegmentRef{}, errors.New("snapshots: nil V3 event log reader")
	}
	if dir == "" {
		return SegmentRef{}, errors.New("snapshots: V4 event log directory is empty")
	}
	if toBlock < fromBlock {
		return SegmentRef{}, fmt.Errorf("snapshots: V4 event log range [%d,%d] is inverted", fromBlock, toBlock)
	}
	covered, err := reader.EventLogRangeCovered(fromBlock, toBlock)
	if err != nil {
		return SegmentRef{}, err
	}
	if !covered {
		return SegmentRef{}, fmt.Errorf("snapshots: V4 event log reader does not cover [%d,%d]", fromBlock, toBlock)
	}
	if relPath == "" {
		relPath = EventLogSegmentPath(fromBlock, toBlock)
	}
	ref := SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: fromBlock, ToTxNum: toBlock, Path: filepath.ToSlash(relPath)}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}

	blocks, txHashes, addresses, topics, rowCount, err := scanEventLogV3Dictionaries(reader, fromBlock, toBlock)
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
	topicKeys := make([]string, 0, len(topics))
	for key := range topics {
		topicKeys = append(topicKeys, key)
	}
	sort.Strings(topicKeys)
	topicIDs := make(map[string]uint64, len(topicKeys))
	for i, key := range topicKeys {
		topicIDs[key] = uint64(i)
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

	rowFrames, payloadFrames, addressPostings, topicPostings, err := writeEventLogV3Frames(reader, fromBlock, toBlock, rowCount, blockIDs, txHashes, addressIDs, topicIDs, rowData, payloadData)
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
		return SegmentRef{}, fmt.Errorf("snapshots: verify newly written V4 event log: %w", err)
	}
	return ref, nil
}

func scanEventLogV3Dictionaries(reader rawdb.EventLogReader, fromBlock, toBlock uint64) ([]eventLogV3Block, []common.Hash, map[string]struct{}, map[string]struct{}, uint64, error) {
	var blocks []eventLogV3Block
	var txHashes []common.Hash
	addresses := make(map[string]struct{})
	topics := make(map[string]struct{})
	var count uint64
	var prev EventLog
	havePrev := false
	blockSeen := make(map[uint64]common.Hash)
	err := reader.IterateEventLogs(fromBlock, toBlock, EventLogFilter{}, func(row EventLog) (bool, error) {
		if err := validateEventLogV3SourceRow(row, fromBlock, toBlock); err != nil {
			return false, err
		}
		if havePrev && compareEventLogEntries(eventLogIndexEntry{blockNum: prev.BlockNum, txIndex: prev.TxIndex, logIndex: prev.LogIndex}, eventLogIndexEntry{blockNum: row.BlockNum, txIndex: row.TxIndex, logIndex: row.LogIndex}) >= 0 {
			return false, fmt.Errorf("snapshots: V4 source rows are not strictly ordered at block=%d tx=%d log=%d", row.BlockNum, row.TxIndex, row.LogIndex)
		}
		if hash, ok := blockSeen[row.BlockNum]; ok {
			if hash != row.BlockHash {
				return false, fmt.Errorf("snapshots: V4 source block %d has inconsistent hashes", row.BlockNum)
			}
		} else {
			blockSeen[row.BlockNum] = row.BlockHash
			blocks = append(blocks, eventLogV3Block{number: row.BlockNum, hash: row.BlockHash})
		}
		if !havePrev || prev.TxHash != row.TxHash {
			txHashes = append(txHashes, row.TxHash)
		}
		addresses[string(row.Address[:])] = struct{}{}
		for position, rawTopic := range row.Log.GetTopics() {
			var topic common.Hash
			copy(topic[:], rawTopic)
			topics[string(eventLogTopicLookupKey(uint64(position), topic))] = struct{}{}
		}
		prev, havePrev = row, true
		count++
		return true, nil
	})
	return blocks, txHashes, addresses, topics, count, err
}

func validateEventLogV3SourceRow(row EventLog, fromBlock, toBlock uint64) error {
	if row.BlockNum < fromBlock || row.BlockNum > toBlock {
		return fmt.Errorf("snapshots: V4 source row block %d outside [%d,%d]", row.BlockNum, fromBlock, toBlock)
	}
	if row.Log == nil {
		return fmt.Errorf("snapshots: V4 source row block=%d tx=%d log=%d has nil protobuf", row.BlockNum, row.TxIndex, row.LogIndex)
	}
	if eventLogAddress(row.Log.GetAddress()) != row.Address {
		return fmt.Errorf("snapshots: V4 source row block=%d tx=%d log=%d address mismatch", row.BlockNum, row.TxIndex, row.LogIndex)
	}
	for _, topic := range row.Log.GetTopics() {
		if len(topic) != common.HashLength {
			return fmt.Errorf("snapshots: V4 source row block=%d tx=%d log=%d topic length %d", row.BlockNum, row.TxIndex, row.LogIndex, len(topic))
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
		return 0, 0, fmt.Errorf("snapshots: V4 protobuf payload length %d exceeds uint32 frame limit", len(raw))
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
		return errors.New("snapshots: V4 payload frame exceeds uint32 length limit")
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

func writeEventLogV3Frames(reader rawdb.EventLogReader, fromBlock, toBlock, wantRows uint64, blockIDs map[uint64]uint64, txHashes []common.Hash, addressIDs, topicIDs map[string]uint64, rowData, payloadData *os.File) ([]eventLogV3Frame, []eventLogV3Frame, map[string][]uint64, map[string][]uint64, error) {
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
				return false, fmt.Errorf("snapshots: V4 transaction dictionary changed at row %d", rowIndex)
			}
			previousTx, haveTx = row.TxHash, true
		}
		addressID, ok := addressIDs[string(row.Address[:])]
		if !ok {
			return false, fmt.Errorf("snapshots: missing V3 address dictionary entry %x", row.Address)
		}
		logCopy := proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
		logCopy.Address = nil
		logCopy.Topics = nil
		raw, err := proto.Marshal(logCopy)
		if err != nil {
			return false, err
		}
		frame, offset, err := payload.add(rowIndex, raw)
		if err != nil {
			return false, err
		}
		topicSequence := make([]uint64, 0, len(row.Log.GetTopics()))
		addressKey := string(eventLogAddressLookupKey(row.Address))
		addressPostings[addressKey] = append(addressPostings[addressKey], rowIndex)
		for position, rawTopic := range row.Log.GetTopics() {
			var topic common.Hash
			copy(topic[:], rawTopic)
			key := string(eventLogTopicLookupKey(uint64(position), topic))
			topicID, ok := topicIDs[key]
			if !ok {
				return false, fmt.Errorf("snapshots: missing V4 topic dictionary entry position=%d topic=%x", position, topic)
			}
			topicSequence = append(topicSequence, topicID)
			topicPostings[key] = append(topicPostings[key], rowIndex)
		}
		rowBuf = append(rowBuf, eventLogV3Row{blockID: blockID, txIndex: row.TxIndex, logIndex: row.LogIndex, txID: txID, addressID: addressID, payloadFrame: frame, payloadOffset: offset, payloadLength: uint64(len(raw)), topicIDs: topicSequence})
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
		return nil, nil, nil, nil, fmt.Errorf("snapshots: V4 source changed between passes: got %d rows, want %d", rowIndex, wantRows)
	}
	if (haveTx && txID+1 != uint64(len(txHashes))) || (!haveTx && len(txHashes) != 0) {
		return nil, nil, nil, nil, errors.New("snapshots: V4 transaction dictionary changed between passes")
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
		raw = binary.AppendUvarint(raw, uint64(len(row.topicIDs)))
		for position, topicID := range row.topicIDs {
			encoded := topicID
			if i > 0 && position < len(prev.topicIDs) {
				if topicID >= prev.topicIDs[position] {
					encoded = (topicID - prev.topicIDs[position]) << 1
				} else {
					encoded = ((prev.topicIDs[position] - topicID) << 1) | 1
				}
			}
			raw = binary.AppendUvarint(raw, encoded)
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
	keyData, keyName, err := createStateDomainChangeBinaryTempFileInDir(dir, base+"-keys")
	if err != nil {
		_ = data.Close()
		_ = os.Remove(name)
		return nil, err
	}
	b := &eventLogV3LookupBuild{keySize: uint64(keySize), data: data, name: name, keyData: keyData, keyName: keyName}
	postingWriter := bufio.NewWriterSize(data, 1<<20)
	var postingOffset uint64
	keys := make([]string, 0, len(postings))
	for key := range postings {
		if len(key) != keySize {
			b.close()
			return nil, fmt.Errorf("snapshots: V4 lookup key length %d, want %d", len(key), keySize)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := postings[key]
		raw := make([]byte, 0, len(values)*2)
		for i, value := range values {
			if i == 0 {
				raw = binary.AppendUvarint(raw, value)
			} else {
				if value <= values[i-1] {
					b.close()
					return nil, fmt.Errorf("snapshots: V4 lookup postings for %x are not strictly ordered", []byte(key))
				}
				raw = binary.AppendUvarint(raw, value-values[i-1])
			}
		}
		if uint64(len(raw)) > math.MaxUint32 {
			b.close()
			return nil, errors.New("snapshots: V4 lookup posting stream exceeds uint32 length limit")
		}
		if _, err := postingWriter.Write(raw); err != nil {
			b.close()
			return nil, err
		}
		b.keys = append(b.keys, eventLogV3LookupKey{key: []byte(key), firstFrame: postingOffset, frameCount: uint32(len(raw)), postingCount: uint64(len(values)), checksum: crc32.ChecksumIEEE(raw)})
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
		return nil, errors.New("snapshots: V4 compact lookup posting length mismatch")
	}
	for start := 0; start < len(b.keys); start += eventLogV3LookupV2BlockKeys {
		end := min(start+eventLogV3LookupV2BlockKeys, len(b.keys))
		raw := encodeEventLogV3LookupV2KeyBlock(b.keys[start:end])
		if len(raw) >= int(eventLogV3LookupV2StoredRaw) {
			b.close()
			return nil, errors.New("snapshots: V4 compact lookup key block exceeds uint32 length limit")
		}
		// Front coding removes the repeated position/prefix bytes without adding
		// one Zstd setup per small key block. The reader understands compressed
		// blocks too, leaving that option available for a measured future writer.
		stored := raw
		storedLen := uint32(len(raw)) | eventLogV3LookupV2StoredRaw
		off, err := keyData.Seek(0, io.SeekCurrent)
		if err != nil {
			b.close()
			return nil, err
		}
		if _, err := keyData.Write(stored); err != nil {
			b.close()
			return nil, err
		}
		b.blocks = append(b.blocks, eventLogV3LookupV2Block{firstKey: append([]byte(nil), b.keys[start].key...), dataOff: uint64(off), dataLen: storedLen, rawLen: uint32(len(raw)), checksum: crc32.ChecksumIEEE(raw), keyCount: uint32(end - start)})
	}
	keyStat, err := keyData.Stat()
	if err != nil {
		b.close()
		return nil, err
	}
	b.keyLen = uint64(keyStat.Size())
	return b, nil
}

func encodeEventLogV3LookupV2KeyBlock(keys []eventLogV3LookupKey) []byte {
	var raw []byte
	var previous []byte
	for _, key := range keys {
		prefix := commonPrefixLength(previous, key.key)
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = append(raw, key.key[prefix:]...)
		raw = binary.AppendUvarint(raw, key.firstFrame)
		raw = binary.AppendUvarint(raw, uint64(key.frameCount))
		raw = binary.AppendUvarint(raw, key.postingCount)
		raw = binary.BigEndian.AppendUint32(raw, key.checksum)
		previous = key.key
	}
	return raw
}

func commonPrefixLength(a, b []byte) int {
	limit := min(len(a), len(b))
	for i := 0; i < limit; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return limit
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
	if b.keyData != nil {
		return writeEventLogV3LookupV2(w, sectionOffset, b)
	}
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

func writeEventLogV3LookupV2(w *os.File, sectionOffset uint64, b *eventLogV3LookupBuild) error {
	var header [eventLogV3LookupV2HeaderSize]byte
	copy(header[:8], eventLogV3LookupV2Magic[:])
	binary.BigEndian.PutUint32(header[8:12], uint32(b.keySize))
	binary.BigEndian.PutUint32(header[12:16], eventLogV3LookupV2BlockKeys)
	binary.BigEndian.PutUint64(header[16:24], uint64(len(b.keys)))
	binary.BigEndian.PutUint64(header[24:32], uint64(len(b.blocks)))
	blockDirLen := uint64(len(b.blocks)) * (b.keySize + eventLogV3LookupV2BlockTail)
	binary.BigEndian.PutUint64(header[32:40], blockDirLen)
	binary.BigEndian.PutUint64(header[40:48], b.keyLen)
	binary.BigEndian.PutUint64(header[48:56], b.dataLen)
	binary.BigEndian.PutUint32(header[56:60], crc32.ChecksumIEEE(header[:56]))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	keyDataBase := sectionOffset + eventLogV3LookupV2HeaderSize + blockDirLen
	for _, block := range b.blocks {
		if _, err := w.Write(block.firstKey); err != nil {
			return err
		}
		var tail [eventLogV3LookupV2BlockTail]byte
		binary.BigEndian.PutUint64(tail[0:8], keyDataBase+block.dataOff)
		binary.BigEndian.PutUint32(tail[8:12], block.dataLen)
		binary.BigEndian.PutUint32(tail[12:16], block.rawLen)
		binary.BigEndian.PutUint32(tail[16:20], block.checksum)
		binary.BigEndian.PutUint32(tail[20:24], block.keyCount)
		entryCRC := crc32.NewIEEE()
		_, _ = entryCRC.Write(block.firstKey)
		_, _ = entryCRC.Write(tail[:24])
		binary.BigEndian.PutUint32(tail[24:28], entryCRC.Sum32())
		if _, err := w.Write(tail[:]); err != nil {
			return err
		}
	}
	if err := copyEventLogV3Temp(w, b.keyData); err != nil {
		return err
	}
	return copyEventLogV3Temp(w, b.data)
}

func writeEventLogV3Header(w io.Writer, h eventLogV3Header) error {
	var raw [eventLogV3HeaderSize]byte
	copy(raw[:8], eventLogMagicV4[:])
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
		return fmt.Errorf("snapshots: V4 event log %q range [%d,%d], want [%d,%d]", ref.Path, h.fromBlock, h.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	if h.toBlock < h.fromBlock || h.rowFrameRows != eventLogV3RowFrameRows || h.payloadTarget != eventLogV3PayloadTarget {
		return fmt.Errorf("snapshots: V4 event log %q has unsupported framing", ref.Path)
	}
	sections := [][2]uint64{{h.blockDictOffset, h.blockDictLength}, {h.txDictOffset, h.txDictLength}, {h.rowDirOffset, h.rowDirLength}, {h.rowDataOffset, h.rowDataLength}, {h.payloadDirOffset, h.payloadDirLength}, {h.payloadDataOffset, h.payloadDataLength}, {h.addressIndexOffset, h.addressIndexLength}, {h.topicIndexOffset, h.topicIndexLength}}
	next := uint64(eventLogV3HeaderSize)
	for i, section := range sections {
		if section[0] != next {
			return fmt.Errorf("snapshots: V4 event log %q section %d offset %d, want %d", ref.Path, i, section[0], next)
		}
		end, overflow := checkedAdd(section[0], section[1])
		if overflow || end > fileSize {
			return fmt.Errorf("snapshots: V4 event log %q section %d outside file", ref.Path, i)
		}
		next = end
	}
	if next != fileSize {
		return fmt.Errorf("snapshots: V4 event log %q ends at %d, file size %d", ref.Path, next, fileSize)
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
	topicLookup, err := readEventLogV3LookupV2Header(file, h.topicIndexOffset, h.topicIndexLength, size, eventLogTopicLookupKeySize)
	if err != nil {
		return nil, err
	}
	return &eventLogV3Reader{
		header: h, rowFrameCount: rowFrames, payloadFrames: payloadFrames, blockCount: blockCount, txCount: txCount,
		addressCount: addressCount, topicCount: topicLookup.keyCount, topicLookup: topicLookup,
		cacheTopicBlocks: make(map[uint64][][]byte),
	}, nil
}

func readEventLogV3FrameAt(file io.ReaderAt, dirOffset, index, count uint64) (eventLogV3Frame, error) {
	if index >= count {
		return eventLogV3Frame{}, fmt.Errorf("snapshots: V4 frame %d outside count %d", index, count)
	}
	var raw [eventLogV3FrameDirEntry]byte
	if _, err := file.ReadAt(raw[:], int64(dirOffset+8+index*eventLogV3FrameDirEntry)); err != nil {
		return eventLogV3Frame{}, err
	}
	return eventLogV3Frame{firstRow: binary.BigEndian.Uint64(raw[0:8]), dataOff: binary.BigEndian.Uint64(raw[8:16]), dataLen: binary.BigEndian.Uint32(raw[16:20]), rowCount: binary.BigEndian.Uint32(raw[20:24]), checksum: binary.BigEndian.Uint32(raw[24:28]), rawLen: binary.BigEndian.Uint32(raw[28:32])}, nil
}

func (s *EventLogSegment) readEventLogV3Row(rowIndex uint64) (eventLogV3Row, error) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if rowIndex >= s.header.rowCount {
		return eventLogV3Row{}, fmt.Errorf("snapshots: V4 row %d outside count %d", rowIndex, s.header.rowCount)
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
			return eventLogV3Row{}, fmt.Errorf("snapshots: V4 row frame %d metadata mismatch", frameIndex)
		}
		raw, err := readEventLogPayloadAt(s.file, frame.dataOff, uint64(frame.dataLen), s.v3.header.rowDataOffset+s.v3.header.rowDataLength)
		if err != nil {
			return eventLogV3Row{}, err
		}
		if crc32.ChecksumIEEE(raw) != frame.checksum {
			return eventLogV3Row{}, fmt.Errorf("snapshots: V4 row frame %d checksum mismatch", frameIndex)
		}
		rows, err := decodeEventLogV3Rows(raw, int(frame.rowCount))
		if err != nil {
			return eventLogV3Row{}, fmt.Errorf("snapshots: V4 row frame %d: %w", frameIndex, err)
		}
		s.v3.cacheFrame, s.v3.cacheRows, s.v3.cacheFrameValid = frameIndex, rows, true
	}
	return s.v3.cacheRows[rowIndex%eventLogV3RowFrameRows], nil
}

// eventLogV4BlockIDLowerBound returns the first block dictionary id whose
// block number is at least target. V4 writes the dictionary in canonical block
// order and rows reference it monotonically, so a narrow RPC range does not
// need to decode payloads belonging to the rest of the physical segment.
func (s *EventLogSegment) eventLogV4BlockIDLowerBound(target uint64) (uint64, error) {
	lo, hi := uint64(0), s.v3.blockCount
	var raw [8]byte
	for lo < hi {
		mid := lo + (hi-lo)/2
		offset := s.v3.header.blockDictOffset + 8 + mid*uint64(8+common.HashLength)
		if _, err := s.file.ReadAt(raw[:], int64(offset)); err != nil {
			return 0, err
		}
		if binary.BigEndian.Uint64(raw[:]) < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, nil
}

// eventLogV4RowLowerBound returns the first row whose block dictionary id is
// at least target. Reading a row validates the complete checksummed row frame;
// the binary search therefore remains fail-closed for every frame it uses.
func (s *EventLogSegment) eventLogV4RowLowerBound(target uint64) (uint64, error) {
	lo, hi := uint64(0), s.header.rowCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		row, err := s.readEventLogV3Row(mid)
		if err != nil {
			return 0, err
		}
		if row.blockID < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, nil
}

func (s *EventLogSegment) eventLogV4RowRange(fromBlock, toBlock uint64) (uint64, uint64, error) {
	if toBlock < fromBlock || s.header.rowCount == 0 || s.v3.blockCount == 0 {
		return 0, 0, nil
	}
	fromID, err := s.eventLogV4BlockIDLowerBound(fromBlock)
	if err != nil || fromID == s.v3.blockCount {
		return 0, 0, err
	}
	toID := s.v3.blockCount
	if toBlock != math.MaxUint64 {
		toID, err = s.eventLogV4BlockIDLowerBound(toBlock + 1)
		if err != nil {
			return 0, 0, err
		}
	}
	fromRow, err := s.eventLogV4RowLowerBound(fromID)
	if err != nil {
		return 0, 0, err
	}
	toRow, err := s.eventLogV4RowLowerBound(toID)
	if err != nil {
		return 0, 0, err
	}
	return fromRow, toRow, nil
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
		topicCount, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}
		if topicCount > uint64(reader.Len()) || topicCount > uint64(math.MaxInt) {
			return nil, errors.New("topic dictionary id count exceeds row frame")
		}
		row.topicIDs = make([]uint64, int(topicCount))
		for j := range row.topicIDs {
			encoded, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, err
			}
			row.topicIDs[j] = encoded
			if i > 0 && j < len(prev.topicIDs) {
				delta := encoded >> 1
				if encoded&1 != 0 {
					if delta > prev.topicIDs[j] {
						return nil, errors.New("topic dictionary id delta underflow")
					}
					row.topicIDs[j] = prev.topicIDs[j] - delta
				} else {
					if math.MaxUint64-prev.topicIDs[j] < delta {
						return nil, errors.New("topic dictionary id delta overflow")
					}
					row.topicIDs[j] = prev.topicIDs[j] + delta
				}
			}
		}
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
		return EventLog{}, errors.New("snapshots: V4 row dictionary id outside bounds")
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
	addressRaw, err := s.readEventLogV3Address(row.addressID)
	if err != nil {
		return EventLog{}, err
	}
	address := common.BytesToAddress(addressRaw)
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
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
			return EventLog{}, errors.New("snapshots: V4 payload frame decoded length mismatch")
		}
		if crc32.ChecksumIEEE(decoded) != payloadFrame.checksum {
			return EventLog{}, errors.New("snapshots: V4 payload frame checksum mismatch")
		}
		s.v3.cachePayload, s.v3.cachePayloadBytes, s.v3.cachePayloadValid = row.payloadFrame, decoded, true
	}
	decoded := s.v3.cachePayloadBytes
	end, overflow := checkedAdd(row.payloadOffset, row.payloadLength)
	if overflow || end > uint64(len(decoded)) {
		return EventLog{}, errors.New("snapshots: V4 payload slice outside frame")
	}
	var log corepb.TransactionInfo_Log
	if err := proto.Unmarshal(decoded[row.payloadOffset:end], &log); err != nil {
		return EventLog{}, err
	}
	log.Address = eventLogV3PayloadAddress(address)
	if len(log.Topics) != 0 {
		return EventLog{}, errors.New("snapshots: V4 payload unexpectedly contains topics")
	}
	log.Topics = make([][]byte, len(row.topicIDs))
	for position, topicID := range row.topicIDs {
		if topicID >= s.v3.topicCount {
			return EventLog{}, errors.New("snapshots: V4 row topic dictionary id outside bounds")
		}
		key, err := s.readEventLogV4TopicLocked(topicID)
		if err != nil {
			return EventLog{}, err
		}
		if binary.BigEndian.Uint64(key[:8]) != uint64(position) {
			return EventLog{}, errors.New("snapshots: V4 row topic dictionary position mismatch")
		}
		log.Topics[position] = append([]byte(nil), key[8:]...)
	}
	entry := eventLogIndexEntry{blockNum: blockNum, txIndex: row.txIndex, logIndex: row.logIndex, txHash: txHash, blockHash: blockHash, address: address}
	if err := validateEventLogPayload(entry, &log, "V3 event log read"); err != nil {
		return EventLog{}, err
	}
	return EventLog{BlockNum: blockNum, TxIndex: row.txIndex, LogIndex: row.logIndex, TxHash: txHash, BlockHash: blockHash, Address: address, Log: &log}, nil
}

const eventLogV4TopicCacheBlocks = 32

// readEventLogV4TopicLocked resolves one row topic while materializeEventLogV3
// holds cacheMu. Full scans interleave topic positions, so a one-block cache
// thrashes; a small FIFO keeps the current block for every common position and
// bounds memory independently of segment cardinality.
func (s *EventLogSegment) readEventLogV4TopicLocked(index uint64) ([]byte, error) {
	if index >= s.v3.topicLookup.keyCount {
		return nil, errors.New("snapshots: V4 topic index outside dictionary")
	}
	blockIndex := index / uint64(s.v3.topicLookup.blockKeys)
	keys, ok := s.v3.cacheTopicBlocks[blockIndex]
	if !ok {
		block, err := readEventLogV3LookupV2Block(s.file, s.v3.header.topicIndexOffset, s.v3.topicLookup, blockIndex)
		if err != nil {
			return nil, err
		}
		records, _, err := readEventLogV3LookupV2Records(s.file, s.v3.header.topicIndexOffset, s.v3.header.topicIndexLength, s.v3.topicLookup, block)
		if err != nil {
			return nil, err
		}
		keys = make([][]byte, len(records))
		for i := range records {
			keys[i] = records[i].key
		}
		if len(s.v3.cacheTopicOrder) == eventLogV4TopicCacheBlocks {
			delete(s.v3.cacheTopicBlocks, s.v3.cacheTopicOrder[0])
			copy(s.v3.cacheTopicOrder, s.v3.cacheTopicOrder[1:])
			s.v3.cacheTopicOrder = s.v3.cacheTopicOrder[:len(s.v3.cacheTopicOrder)-1]
		}
		s.v3.cacheTopicBlocks[blockIndex] = keys
		s.v3.cacheTopicOrder = append(s.v3.cacheTopicOrder, blockIndex)
	}
	within := index % uint64(s.v3.topicLookup.blockKeys)
	if within >= uint64(len(keys)) {
		return nil, errors.New("snapshots: V4 topic index outside dictionary block")
	}
	return keys[within], nil
}

// eventLogV3PayloadAddress restores the protobuf address width lost when V3
// moves the address into its fixed-width dictionary. TVM event logs carry a
// 20-byte EVM address, which eventLogAddress represents as a 21-byte common
// Address with a zero leading byte. Protocol-native TRON logs already carry a
// valid non-zero network prefix (0x41) and retain all 21 bytes. Restoring the
// original width keeps the cold protobuf byte-for-byte equal to the canonical
// TransactionInfo log while preserving one fixed-width lookup identity.
func eventLogV3PayloadAddress(address common.Address) []byte {
	raw := address[:]
	if address[0] == 0 {
		raw = raw[1:]
	}
	return append([]byte(nil), raw...)
}

func (s *EventLogSegment) readEventLogV3Address(index uint64) ([]byte, error) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	offset, length := s.v3.header.addressIndexOffset, s.v3.header.addressIndexLength
	compact, err := isEventLogV3LookupV2(s.file, offset, length, s.size)
	if err != nil {
		return nil, err
	}
	if !compact {
		return readEventLogV3LookupKeyAt(s.file, offset, length, s.size, eventLogAddressLookupKeySize, index)
	}
	h, err := readEventLogV3LookupV2Header(s.file, offset, length, s.size, eventLogAddressLookupKeySize)
	if err != nil {
		return nil, err
	}
	if index >= h.keyCount {
		return nil, errors.New("snapshots: V4 compact lookup key index outside directory")
	}
	blockIndex := index / uint64(h.blockKeys)
	if !s.v3.cacheAddressValid || s.v3.cacheAddressBlock != blockIndex {
		block, err := readEventLogV3LookupV2Block(s.file, offset, h, blockIndex)
		if err != nil {
			return nil, err
		}
		records, _, err := readEventLogV3LookupV2Records(s.file, offset, length, h, block)
		if err != nil {
			return nil, err
		}
		keys := make([][]byte, len(records))
		for i := range records {
			keys[i] = records[i].key
		}
		s.v3.cacheAddressBlock, s.v3.cacheAddressKeys, s.v3.cacheAddressValid = blockIndex, keys, true
	}
	within := index % uint64(h.blockKeys)
	if within >= uint64(len(s.v3.cacheAddressKeys)) {
		return nil, errors.New("snapshots: V4 compact lookup address index outside block")
	}
	return s.v3.cacheAddressKeys[within], nil
}

func readEventLogV3LookupKeyAt(file io.ReaderAt, offset, length, size uint64, keySize int, index uint64) ([]byte, error) {
	compact, err := isEventLogV3LookupV2(file, offset, length, size)
	if err != nil {
		return nil, err
	}
	if !compact {
		keys, _, err := readEventLogV3LookupCounts(file, offset, length, size, keySize)
		if err != nil {
			return nil, err
		}
		if index >= keys {
			return nil, errors.New("snapshots: V4 lookup key index outside directory")
		}
		entrySize := uint64(keySize + eventLogV3LookupKeyTail)
		key := make([]byte, keySize)
		if _, err := file.ReadAt(key, int64(offset+eventLogV3LookupHeaderSize+index*entrySize)); err != nil {
			return nil, err
		}
		return key, nil
	}
	h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	if index >= h.keyCount {
		return nil, errors.New("snapshots: V4 compact lookup key index outside directory")
	}
	blockIndex := index / uint64(h.blockKeys)
	block, err := readEventLogV3LookupV2Block(file, offset, h, blockIndex)
	if err != nil {
		return nil, err
	}
	records, _, err := readEventLogV3LookupV2Records(file, offset, length, h, block)
	if err != nil {
		return nil, err
	}
	within := index % uint64(h.blockKeys)
	if within >= uint64(len(records)) {
		return nil, errors.New("snapshots: V4 compact lookup key index outside block")
	}
	return records[within].key, nil
}

func (s *EventLogSegment) iterateEventLogV3FullScan(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	fromRow, toRow, err := s.eventLogV4RowRange(fromBlock, toBlock)
	if err != nil {
		return err
	}
	for i := fromRow; i < toRow; i++ {
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
			return true, fmt.Errorf("snapshots: V4 lookup row %d does not match filter", i)
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
			rows, err := readEventLogV3LookupRows(s.file, s.v3.header.addressIndexOffset, s.v3.header.addressIndexLength, s.size, eventLogAddressLookupKeySize, eventLogAddressLookupKey(address), s.header.rowCount)
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
			rows, err := readEventLogV3LookupRows(s.file, s.v3.header.topicIndexOffset, s.v3.header.topicIndexLength, s.size, eventLogTopicLookupKeySize, eventLogTopicLookupKey(uint64(position), topic), s.header.rowCount)
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

func isEventLogV3LookupV2(file io.ReaderAt, offset, length, size uint64) (bool, error) {
	if length < 8 || offset > size || length > size-offset || offset > math.MaxInt64 {
		return false, errors.New("snapshots: invalid V3 lookup bounds")
	}
	var magic [8]byte
	if _, err := file.ReadAt(magic[:], int64(offset)); err != nil {
		return false, err
	}
	if magic != eventLogV3LookupV2Magic {
		return false, errors.New("snapshots: unsupported V3 lookup format; compact lookup magic required")
	}
	return true, nil
}

func readEventLogV3LookupV2Header(file io.ReaderAt, offset, length, size uint64, wantKeySize int) (eventLogV3LookupV2Header, error) {
	if length < eventLogV3LookupV2HeaderSize || offset > size || length > size-offset || offset > math.MaxInt64 {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: invalid V3 compact lookup bounds")
	}
	var raw [eventLogV3LookupV2HeaderSize]byte
	if _, err := file.ReadAt(raw[:], int64(offset)); err != nil {
		return eventLogV3LookupV2Header{}, err
	}
	if !bytes.Equal(raw[:8], eventLogV3LookupV2Magic[:]) {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: invalid V3 compact lookup magic")
	}
	if binary.BigEndian.Uint32(raw[56:60]) != crc32.ChecksumIEEE(raw[:56]) {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: V4 compact lookup header checksum mismatch")
	}
	if !bytes.Equal(raw[60:64], []byte{0, 0, 0, 0}) {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: V4 compact lookup header reserved bytes are non-zero")
	}
	h := eventLogV3LookupV2Header{
		keySize: binary.BigEndian.Uint32(raw[8:12]), blockKeys: binary.BigEndian.Uint32(raw[12:16]),
		keyCount: binary.BigEndian.Uint64(raw[16:24]), blockCount: binary.BigEndian.Uint64(raw[24:32]),
		blockDirLen: binary.BigEndian.Uint64(raw[32:40]), keyDataLen: binary.BigEndian.Uint64(raw[40:48]),
		postingDataLen: binary.BigEndian.Uint64(raw[48:56]),
	}
	if h.keySize != uint32(wantKeySize) || h.blockKeys != eventLogV3LookupV2BlockKeys {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: unsupported V3 compact lookup framing")
	}
	wantBlocks := ceilDiv(h.keyCount, uint64(h.blockKeys))
	if h.blockCount != wantBlocks {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: invalid V3 compact lookup block count")
	}
	dirLen, overflow := checkedMul(h.blockCount, uint64(h.keySize)+eventLogV3LookupV2BlockTail)
	if overflow || dirLen != h.blockDirLen {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: invalid V3 compact lookup directory length")
	}
	total, overflow := checkedAdd(eventLogV3LookupV2HeaderSize, h.blockDirLen)
	if overflow {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: V4 compact lookup length overflow")
	}
	total, overflow = checkedAdd(total, h.keyDataLen)
	if overflow {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: V4 compact lookup length overflow")
	}
	total, overflow = checkedAdd(total, h.postingDataLen)
	if overflow || total != length {
		return eventLogV3LookupV2Header{}, errors.New("snapshots: invalid V3 compact lookup section length")
	}
	return h, nil
}

func readEventLogV3LookupV2Block(file io.ReaderAt, offset uint64, h eventLogV3LookupV2Header, index uint64) (eventLogV3LookupV2Block, error) {
	if index >= h.blockCount {
		return eventLogV3LookupV2Block{}, errors.New("snapshots: V4 compact lookup block outside directory")
	}
	entrySize := uint64(h.keySize) + eventLogV3LookupV2BlockTail
	entryOff := offset + eventLogV3LookupV2HeaderSize + index*entrySize
	raw := make([]byte, entrySize)
	if _, err := file.ReadAt(raw, int64(entryOff)); err != nil {
		return eventLogV3LookupV2Block{}, err
	}
	tail := raw[h.keySize:]
	block := eventLogV3LookupV2Block{
		firstKey: append([]byte(nil), raw[:h.keySize]...), dataOff: binary.BigEndian.Uint64(tail[0:8]),
		dataLen: binary.BigEndian.Uint32(tail[8:12]), rawLen: binary.BigEndian.Uint32(tail[12:16]),
		checksum: binary.BigEndian.Uint32(tail[16:20]), keyCount: binary.BigEndian.Uint32(tail[20:24]),
		entryCRC: binary.BigEndian.Uint32(tail[24:28]),
	}
	entryCRC := crc32.NewIEEE()
	_, _ = entryCRC.Write(raw[:h.keySize])
	_, _ = entryCRC.Write(tail[:24])
	if block.entryCRC != entryCRC.Sum32() {
		return eventLogV3LookupV2Block{}, errors.New("snapshots: V4 compact lookup directory entry checksum mismatch")
	}
	if block.keyCount == 0 || block.keyCount > h.blockKeys {
		return eventLogV3LookupV2Block{}, errors.New("snapshots: invalid V3 compact lookup key count")
	}
	maxRawLen := uint64(block.keyCount) * (uint64(h.keySize) + 4*binary.MaxVarintLen64 + 4)
	if uint64(block.rawLen) > maxRawLen {
		return eventLogV3LookupV2Block{}, errors.New("snapshots: V4 compact lookup key block decoded length is excessive")
	}
	return block, nil
}

func readEventLogV3LookupV2Records(file io.ReaderAt, offset, length uint64, h eventLogV3LookupV2Header, block eventLogV3LookupV2Block) ([]eventLogV3LookupV2Record, uint64, error) {
	keyDataStart := offset + eventLogV3LookupV2HeaderSize + h.blockDirLen
	postingStart := keyDataStart + h.keyDataLen
	storedLen := uint64(block.dataLen &^ eventLogV3LookupV2StoredRaw)
	if block.dataOff < keyDataStart || block.dataOff > postingStart || storedLen > postingStart-block.dataOff {
		return nil, 0, errors.New("snapshots: V4 compact lookup key block outside key data")
	}
	stored, err := readEventLogPayloadAt(file, block.dataOff, storedLen, postingStart)
	if err != nil {
		return nil, 0, err
	}
	decoded := stored
	if block.dataLen&eventLogV3LookupV2StoredRaw == 0 {
		_, dec, err := cbCodec()
		if err != nil {
			return nil, 0, err
		}
		decoded, err = dec.DecodeAll(stored, make([]byte, 0, int(block.rawLen)))
		if err != nil {
			return nil, 0, err
		}
	}
	if uint64(len(decoded)) != uint64(block.rawLen) || crc32.ChecksumIEEE(decoded) != block.checksum {
		return nil, 0, errors.New("snapshots: V4 compact lookup key block checksum mismatch")
	}
	br := bytes.NewReader(decoded)
	records := make([]eventLogV3LookupV2Record, 0, block.keyCount)
	var previous []byte
	for i := uint32(0); i < block.keyCount; i++ {
		prefix, err := binary.ReadUvarint(br)
		if err != nil || prefix > uint64(len(previous)) || prefix > uint64(h.keySize) {
			return nil, 0, errors.New("snapshots: invalid V3 compact lookup key prefix")
		}
		suffixLen := int(h.keySize) - int(prefix)
		key := make([]byte, h.keySize)
		copy(key, previous[:prefix])
		if _, err := io.ReadFull(br, key[prefix:prefix+uint64(suffixLen)]); err != nil {
			return nil, 0, err
		}
		postingOff, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, 0, err
		}
		postingLen, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, 0, err
		}
		postingCount, err := binary.ReadUvarint(br)
		if err != nil {
			return nil, 0, err
		}
		var checksumRaw [4]byte
		if _, err := io.ReadFull(br, checksumRaw[:]); err != nil {
			return nil, 0, err
		}
		if postingOff > h.postingDataLen || postingLen > h.postingDataLen-postingOff || postingStart+postingOff+postingLen > offset+length {
			return nil, 0, errors.New("snapshots: V4 compact lookup postings outside section")
		}
		if postingCount > postingLen {
			return nil, 0, errors.New("snapshots: V4 compact lookup posting count exceeds encoded bytes")
		}
		if i == 0 && !bytes.Equal(key, block.firstKey) {
			return nil, 0, errors.New("snapshots: V4 compact lookup first key mismatch")
		}
		if i > 0 && bytes.Compare(previous, key) >= 0 {
			return nil, 0, errors.New("snapshots: V4 compact lookup keys not sorted")
		}
		records = append(records, eventLogV3LookupV2Record{key: key, postingOff: postingOff, postingLen: postingLen, postingCount: postingCount, checksum: binary.BigEndian.Uint32(checksumRaw[:])})
		previous = key
	}
	if br.Len() != 0 {
		return nil, 0, errors.New("snapshots: V4 compact lookup key block trailing bytes")
	}
	return records, storedLen, nil
}

func readEventLogV3LookupV2Rows(file io.ReaderAt, offset, length, size uint64, keySize int, want []byte, maxRows uint64) ([]uint64, error) {
	h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	lo, hi := uint64(0), h.blockCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		block, err := readEventLogV3LookupV2Block(file, offset, h, mid)
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
	block, err := readEventLogV3LookupV2Block(file, offset, h, lo-1)
	if err != nil {
		return nil, err
	}
	records, _, err := readEventLogV3LookupV2Records(file, offset, length, h, block)
	if err != nil {
		return nil, err
	}
	idx := sort.Search(len(records), func(i int) bool { return bytes.Compare(records[i].key, want) >= 0 })
	if idx == len(records) || !bytes.Equal(records[idx].key, want) {
		return nil, nil
	}
	record := records[idx]
	return readEventLogV3LookupV2RecordRows(file, offset, length, h, record, maxRows)
}

func readEventLogV3LookupV2RecordRows(file io.ReaderAt, offset, length uint64, h eventLogV3LookupV2Header, record eventLogV3LookupV2Record, maxRows uint64) ([]uint64, error) {
	if record.postingCount > maxRows || record.postingCount > uint64(math.MaxInt/8) {
		return nil, errors.New("snapshots: V4 compact lookup posting count exceeds safe row limit")
	}
	postingStart := offset + eventLogV3LookupV2HeaderSize + h.blockDirLen + h.keyDataLen
	raw, err := readEventLogPayloadAt(file, postingStart+record.postingOff, record.postingLen, offset+length)
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(raw) != record.checksum {
		return nil, errors.New("snapshots: V4 compact lookup posting checksum mismatch")
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
				return nil, errors.New("snapshots: V4 compact lookup posting delta invalid")
			}
			value += previous
		}
		if value >= maxRows {
			return nil, errors.New("snapshots: V4 compact lookup posting row outside segment")
		}
		rows = append(rows, value)
		previous = value
	}
	if br.Len() != 0 {
		return nil, errors.New("snapshots: V4 compact lookup posting trailing bytes")
	}
	return rows, nil
}

func readEventLogV3LookupCounts(file io.ReaderAt, offset, length, size uint64, keySize int) (uint64, uint64, error) {
	compact, err := isEventLogV3LookupV2(file, offset, length, size)
	if err != nil {
		return 0, 0, err
	}
	if compact {
		h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
		return h.keyCount, h.blockCount, err
	}
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

func readEventLogV3LookupRows(file io.ReaderAt, offset, length, size uint64, keySize int, want []byte, maxRows uint64) ([]uint64, error) {
	compact, err := isEventLogV3LookupV2(file, offset, length, size)
	if err != nil {
		return nil, err
	}
	if compact {
		return readEventLogV3LookupV2Rows(file, offset, length, size, keySize, want, maxRows)
	}
	keys, frames, err := readEventLogV3LookupCounts(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	entrySize := uint64(keySize + eventLogV3LookupKeyTail)
	frameDir := offset + eventLogV3LookupHeaderSize + keys*entrySize
	dataStart := frameDir + frames*eventLogV3LookupFrameEntry
	if dataStart > offset+length {
		return nil, errors.New("snapshots: V4 lookup directories outside section")
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
		return nil, errors.New("snapshots: V4 lookup frame range outside directory")
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
			return nil, errors.New("snapshots: V4 lookup data outside section")
		}
		data, err := readEventLogPayloadAt(file, dataOff, uint64(dataLen), offset+length)
		if err != nil {
			return nil, err
		}
		if crc32.ChecksumIEEE(data) != checksum {
			return nil, errors.New("snapshots: V4 lookup frame checksum mismatch")
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
					return nil, errors.New("snapshots: V4 lookup first posting mismatch")
				}
			} else {
				if value == 0 || math.MaxUint64-prev < value {
					return nil, errors.New("snapshots: V4 lookup posting delta invalid")
				}
				prev += value
			}
			rows = append(rows, prev)
		}
		if br.Len() != 0 {
			return nil, errors.New("snapshots: V4 lookup trailing bytes")
		}
	}
	if uint64(len(rows)) != postingCount {
		return nil, errors.New("snapshots: V4 lookup posting count mismatch")
	}
	return rows, nil
}

func checkEventLogV3Segment(file *os.File, ref SegmentRef, header eventLogHeader, size uint64) error {
	validationStarted := time.Now()
	reader, err := openEventLogV3Reader(file, *header.v3, size)
	if err != nil {
		return err
	}
	seg := &EventLogSegment{ref: ref, file: file, header: header, size: size, v3: reader}
	nextRow, nextRowOffset := uint64(0), reader.header.rowDataOffset
	for i := uint64(0); i < reader.rowFrameCount; i++ {
		frame, err := readEventLogV3FrameAt(file, reader.header.rowDirOffset, i, reader.rowFrameCount)
		if err != nil {
			return err
		}
		expectedRows := uint32(eventLogV3RowFrameRows)
		if remaining := header.rowCount - nextRow; remaining < uint64(expectedRows) {
			expectedRows = uint32(remaining)
		}
		if frame.firstRow != nextRow || frame.rowCount != expectedRows || frame.dataOff != nextRowOffset || frame.rawLen != frame.dataLen {
			return fmt.Errorf("snapshots: V4 row frame %d metadata mismatch", i)
		}
		nextRow += uint64(frame.rowCount)
		nextRowOffset += uint64(frame.dataLen)
	}
	if nextRow != header.rowCount || nextRowOffset != reader.header.rowDataOffset+reader.header.rowDataLength {
		return errors.New("snapshots: V4 row frames do not cover row section")
	}
	nextPayloadRow, nextPayloadOffset := uint64(0), reader.header.payloadDataOffset
	for i := uint64(0); i < reader.payloadFrames; i++ {
		frame, err := readEventLogV3FrameAt(file, reader.header.payloadDirOffset, i, reader.payloadFrames)
		if err != nil {
			return err
		}
		if frame.firstRow != nextPayloadRow || frame.rowCount == 0 || frame.dataOff != nextPayloadOffset {
			return fmt.Errorf("snapshots: V4 payload frame %d metadata mismatch", i)
		}
		nextPayloadRow += uint64(frame.rowCount)
		nextPayloadOffset += uint64(frame.dataLen)
	}
	if nextPayloadRow != header.rowCount || nextPayloadOffset != reader.header.payloadDataOffset+reader.header.payloadDataLength {
		return errors.New("snapshots: V4 payload frames do not cover payload section")
	}
	blockNumbers, err := readEventLogV4BlockNumbers(file, reader, ref)
	if err != nil {
		return err
	}
	if reader.addressCount > uint64(math.MaxInt) || reader.topicCount > uint64(math.MaxInt) {
		return errors.New("snapshots: V4 lookup dictionary exceeds platform limits")
	}
	addressCounts := make([]uint64, int(reader.addressCount))
	topicCounts := make([]uint64, int(reader.topicCount))
	topicPositions := make([]uint64, int(reader.topicCount))
	for i := range topicPositions {
		topicPositions[i] = math.MaxUint64
	}
	payloads := eventLogV4PayloadVerifier{file: file, reader: reader}
	var previous eventLogIndexEntry
	var previousRow eventLogV3Row
	havePrevious := false
	for i := uint64(0); i < header.rowCount; i++ {
		row, err := seg.readEventLogV3Row(i)
		if err != nil {
			return err
		}
		if row.blockID >= reader.blockCount || row.txID >= reader.txCount || row.addressID >= reader.addressCount || row.payloadFrame >= reader.payloadFrames {
			return fmt.Errorf("snapshots: V4 event log %q row %d dictionary id outside bounds", ref.Path, i)
		}
		entry := eventLogIndexEntry{blockNum: blockNumbers[row.blockID], txIndex: row.txIndex, logIndex: row.logIndex}
		if havePrevious && compareEventLogEntries(previous, entry) >= 0 {
			return errors.New("snapshots: V4 rows not strictly ordered")
		}
		if !havePrevious {
			if row.blockID != 0 || row.txID != 0 {
				return errors.New("snapshots: V4 first row does not start at dictionary id zero")
			}
		} else {
			if row.blockID > previousRow.blockID+1 || row.txID > previousRow.txID+1 {
				return errors.New("snapshots: V4 row dictionary ids contain a gap")
			}
		}
		havePrevious, previous = true, entry
		previousRow = row
		if addressCounts[row.addressID] == math.MaxUint64 {
			return errors.New("snapshots: V4 address posting count overflow")
		}
		addressCounts[row.addressID]++
		for position, topicID := range row.topicIDs {
			if topicID >= reader.topicCount {
				return fmt.Errorf("snapshots: V4 event log %q row %d topic dictionary id outside bounds", ref.Path, i)
			}
			if topicPositions[topicID] == math.MaxUint64 {
				topicPositions[topicID] = uint64(position)
			} else if topicPositions[topicID] != uint64(position) {
				return fmt.Errorf("snapshots: V4 event log %q row %d topic dictionary position mismatch", ref.Path, i)
			}
			if topicCounts[topicID] == math.MaxUint64 {
				return errors.New("snapshots: V4 topic posting count overflow")
			}
			topicCounts[topicID]++
		}
		if err := payloads.verify(i, row); err != nil {
			return fmt.Errorf("snapshots: V4 event log %q row %d: %w", ref.Path, i, err)
		}
	}
	if err := payloads.finish(); err != nil {
		return fmt.Errorf("snapshots: V4 event log %q payload coverage: %w", ref.Path, err)
	}
	if header.rowCount == 0 {
		if reader.blockCount != 0 || reader.txCount != 0 || reader.addressCount != 0 || reader.topicCount != 0 {
			return errors.New("snapshots: V4 empty event-log segment has non-empty dictionaries")
		}
	} else if previousRow.blockID+1 != reader.blockCount || previousRow.txID+1 != reader.txCount {
		return errors.New("snapshots: V4 row dictionary ids do not cover block/transaction dictionaries")
	}
	semanticDuration := time.Since(validationStarted)
	postingFillStarted := time.Now()
	expectedAddress, err := allocateEventLogV4Postings(addressCounts)
	if err != nil {
		return err
	}
	expectedTopic, err := allocateEventLogV4Postings(topicCounts)
	if err != nil {
		return err
	}
	// A second row-frame pass fills exact posting lists into one flat allocation
	// per lookup. The expensive payload and dictionary checks above are not
	// repeated. This trades a small sequential row read for eliminating the old
	// per-row address/topic materialization and random compact-dictionary reads.
	for i := uint64(0); i < header.rowCount; i++ {
		row, err := seg.readEventLogV3Row(i)
		if err != nil {
			return err
		}
		if row.addressID >= expectedAddress.keyCount() {
			return errors.New("snapshots: V4 address dictionary changed between validation passes")
		}
		addressCursor := addressCounts[row.addressID]
		if addressCursor >= expectedAddress.offsets[row.addressID+1] {
			return errors.New("snapshots: V4 address posting count changed between validation passes")
		}
		expectedAddress.rows[addressCursor] = i
		addressCounts[row.addressID]++
		for _, topicID := range row.topicIDs {
			if topicID >= expectedTopic.keyCount() {
				return errors.New("snapshots: V4 topic dictionary changed between validation passes")
			}
			topicCursor := topicCounts[topicID]
			if topicCursor >= expectedTopic.offsets[topicID+1] {
				return errors.New("snapshots: V4 topic posting count changed between validation passes")
			}
			expectedTopic.rows[topicCursor] = i
			topicCounts[topicID]++
		}
	}
	if !expectedAddress.filled(addressCounts) || !expectedTopic.filled(topicCounts) {
		return errors.New("snapshots: V4 posting counts changed between validation passes")
	}
	postingFillDuration := time.Since(postingFillStarted)
	lookupStarted := time.Now()
	if err := checkEventLogV4LookupPostings(file, header.v3.addressIndexOffset, header.v3.addressIndexLength, size, eventLogAddressLookupKeySize, header.rowCount, "address", expectedAddress, nil); err != nil {
		return fmt.Errorf("snapshots: V4 event log %q: %w", ref.Path, err)
	}
	if err := checkEventLogV4LookupPostings(file, header.v3.topicIndexOffset, header.v3.topicIndexLength, size, eventLogTopicLookupKeySize, header.rowCount, "topic", expectedTopic, topicPositions); err != nil {
		return err
	}
	recordEventLogV4Validation(header.rowCount, size, time.Since(validationStarted), semanticDuration, postingFillDuration, time.Since(lookupStarted))
	return nil
}

func recordEventLogV4Validation(rows, bytes uint64, total, semantic, postingFill, lookup time.Duration) {
	eventLogV4ValidationRunsCounter.Inc(1)
	eventLogV4ValidationRowsCounter.Inc(coldSnapshotUintGauge(rows))
	eventLogV4ValidationBytesCounter.Inc(coldSnapshotUintGauge(bytes))
	eventLogV4ValidationNanosCounter.Inc(total.Nanoseconds())
	eventLogV4ValidationSemanticCounter.Inc(semantic.Nanoseconds())
	eventLogV4ValidationPostingFillCounter.Inc(postingFill.Nanoseconds())
	eventLogV4ValidationLookupCounter.Inc(lookup.Nanoseconds())
}

func readEventLogV4BlockNumbers(file io.ReaderAt, reader *eventLogV3Reader, ref SegmentRef) ([]uint64, error) {
	if reader.blockCount > uint64(math.MaxInt) {
		return nil, errors.New("snapshots: V4 block dictionary exceeds platform limits")
	}
	rawLength, overflow := checkedMul(reader.blockCount, 8+common.HashLength)
	if overflow || rawLength > uint64(math.MaxInt) {
		return nil, errors.New("snapshots: V4 block dictionary length overflow")
	}
	raw, err := readEventLogPayloadAt(file, reader.header.blockDictOffset+8, rawLength, reader.header.blockDictOffset+reader.header.blockDictLength)
	if err != nil {
		return nil, err
	}
	numbers := make([]uint64, int(reader.blockCount))
	entrySize := 8 + common.HashLength
	for i := range numbers {
		numbers[i] = binary.BigEndian.Uint64(raw[i*entrySize : i*entrySize+8])
		if numbers[i] < ref.FromTxNum || numbers[i] > ref.ToTxNum {
			return nil, errors.New("snapshots: V4 block dictionary entry outside segment")
		}
		if i > 0 && numbers[i] <= numbers[i-1] {
			return nil, errors.New("snapshots: V4 block dictionary is not strictly ordered")
		}
	}
	return numbers, nil
}

type eventLogV4PayloadVerifier struct {
	file       io.ReaderAt
	reader     *eventLogV3Reader
	loaded     bool
	frameIndex uint64
	frame      eventLogV3Frame
	decoded    []byte
	nextOffset uint64
	rows       uint64
}

func (v *eventLogV4PayloadVerifier) verify(rowIndex uint64, row eventLogV3Row) error {
	if !v.loaded || row.payloadFrame != v.frameIndex {
		if err := v.finishFrame(); err != nil {
			return err
		}
		expectedFrame := uint64(0)
		if v.loaded {
			expectedFrame = v.frameIndex + 1
		}
		if row.payloadFrame != expectedFrame {
			return errors.New("snapshots: V4 payload frame sequence is not contiguous")
		}
		frame, err := readEventLogV3FrameAt(v.file, v.reader.header.payloadDirOffset, row.payloadFrame, v.reader.payloadFrames)
		if err != nil {
			return err
		}
		compressed, err := readEventLogPayloadAt(v.file, frame.dataOff, uint64(frame.dataLen), v.reader.header.payloadDataOffset+v.reader.header.payloadDataLength)
		if err != nil {
			return err
		}
		_, decoder, err := cbCodec()
		if err != nil {
			return err
		}
		decoded, err := decoder.DecodeAll(compressed, make([]byte, 0, int(frame.rawLen)))
		if err != nil {
			return err
		}
		if len(decoded) != int(frame.rawLen) || crc32.ChecksumIEEE(decoded) != frame.checksum {
			return errors.New("snapshots: V4 payload frame content mismatch")
		}
		if frame.firstRow != rowIndex {
			return errors.New("snapshots: V4 payload frame first row mismatch")
		}
		v.loaded, v.frameIndex, v.frame, v.decoded = true, row.payloadFrame, frame, decoded
		v.nextOffset, v.rows = 0, 0
	}
	if rowIndex != v.frame.firstRow+v.rows || v.rows >= uint64(v.frame.rowCount) {
		return errors.New("snapshots: V4 payload frame row coverage mismatch")
	}
	if row.payloadOffset != v.nextOffset {
		return errors.New("snapshots: V4 payload slices are not contiguous")
	}
	end, overflow := checkedAdd(row.payloadOffset, row.payloadLength)
	if overflow || end > uint64(len(v.decoded)) {
		return errors.New("snapshots: V4 payload slice outside frame")
	}
	var log corepb.TransactionInfo_Log
	if err := proto.Unmarshal(v.decoded[row.payloadOffset:end], &log); err != nil {
		return err
	}
	if len(log.GetAddress()) != 0 || len(log.GetTopics()) != 0 {
		return errors.New("snapshots: V4 stripped payload contains address or topics")
	}
	v.nextOffset, v.rows = end, v.rows+1
	return nil
}

func (v *eventLogV4PayloadVerifier) finishFrame() error {
	if !v.loaded {
		return nil
	}
	if v.rows != uint64(v.frame.rowCount) || v.nextOffset != uint64(len(v.decoded)) {
		return errors.New("snapshots: V4 payload frame is not exactly covered by rows")
	}
	return nil
}

func (v *eventLogV4PayloadVerifier) finish() error {
	if err := v.finishFrame(); err != nil {
		return err
	}
	if v.reader.header.rowCount == 0 {
		if v.loaded {
			return errors.New("snapshots: V4 empty segment has payload data")
		}
		return nil
	}
	if !v.loaded || v.frameIndex+1 != v.reader.payloadFrames {
		return errors.New("snapshots: V4 payload frames are not exactly covered")
	}
	return nil
}

type eventLogV4ExpectedPostings struct {
	offsets []uint64
	rows    []uint64
}

func (p eventLogV4ExpectedPostings) keyCount() uint64 {
	if len(p.offsets) == 0 {
		return 0
	}
	return uint64(len(p.offsets) - 1)
}

func (p eventLogV4ExpectedPostings) rowsFor(id uint64) []uint64 {
	return p.rows[p.offsets[id]:p.offsets[id+1]]
}

func (p eventLogV4ExpectedPostings) filled(cursors []uint64) bool {
	if uint64(len(cursors)) != p.keyCount() {
		return false
	}
	for i, cursor := range cursors {
		if cursor != p.offsets[i+1] {
			return false
		}
	}
	return true
}

func allocateEventLogV4Postings(counts []uint64) (eventLogV4ExpectedPostings, error) {
	var total uint64
	offsets := make([]uint64, len(counts)+1)
	for i, count := range counts {
		offsets[i] = total
		var overflow bool
		total, overflow = checkedAdd(total, count)
		if overflow {
			return eventLogV4ExpectedPostings{}, errors.New("snapshots: V4 posting count overflow")
		}
		counts[i] = offsets[i]
	}
	offsets[len(counts)] = total
	if total > uint64(math.MaxInt) {
		return eventLogV4ExpectedPostings{}, errors.New("snapshots: V4 postings exceed platform limits")
	}
	return eventLogV4ExpectedPostings{offsets: offsets, rows: make([]uint64, int(total))}, nil
}

func checkEventLogV4LookupPostings(file io.ReaderAt, offset, length, size uint64, keySize int, maxRows uint64, name string, expected eventLogV4ExpectedPostings, topicPositions []uint64) error {
	h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
	if err != nil {
		return err
	}
	if h.keyCount != expected.keyCount() {
		return fmt.Errorf("snapshots: V4 %s lookup key count %d, want %d", name, h.keyCount, expected.keyCount())
	}
	expectedKeyOff := offset + eventLogV3LookupV2HeaderSize + h.blockDirLen
	var expectedPostingOff, keyID uint64
	var keyBlockStoredScratch, keyBlockDecodedScratch, postingScratch []byte
	previousKey := make([]byte, keySize)
	havePreviousKey := false
	postingDataStart := offset + eventLogV3LookupV2HeaderSize + h.blockDirLen + h.keyDataLen
	if postingDataStart > uint64(math.MaxInt64) || h.postingDataLen > uint64(math.MaxInt64) {
		return errors.New("snapshots: V4 compact lookup posting section exceeds int64 limits")
	}
	postingSource := eventLogV4ValidationPostingSource{source: io.NewSectionReader(file, int64(postingDataStart), int64(h.postingDataLen))}
	postingReader := acquireEventLogV4ValidationReader(&postingSource)
	defer releaseEventLogV4ValidationReader(&postingReader)
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogV3LookupV2Block(file, offset, h, i)
		if err != nil {
			return err
		}
		storedLen := uint64(block.dataLen &^ eventLogV3LookupV2StoredRaw)
		if block.dataOff != expectedKeyOff {
			return fmt.Errorf("snapshots: V4 %s lookup key blocks are not contiguous", name)
		}
		expectedKeyOff += storedLen
		decoded, err := readEventLogV4LookupKeyBlock(file, offset, length, h, block, &keyBlockStoredScratch, &keyBlockDecodedScratch)
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
				return errors.New("snapshots: invalid V3 compact lookup key prefix")
			}
			if blockHasPrevious && prefix > uint64(len(blockPreviousKey)) {
				return errors.New("snapshots: invalid V3 compact lookup key prefix")
			}
			suffixLength := keySize - int(prefix)
			if suffixLength > len(decoded)-cursor {
				return io.ErrUnexpectedEOF
			}
			copy(blockKey[:prefix], blockPreviousKey[:prefix])
			copy(blockKey[prefix:], decoded[cursor:cursor+suffixLength])
			cursor += suffixLength
			postingOff, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
			if err != nil {
				return err
			}
			postingLen, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
			if err != nil {
				return err
			}
			postingCount, err := consumeEventLogV4LookupUvarint(decoded, &cursor)
			if err != nil {
				return err
			}
			if len(decoded)-cursor < 4 {
				return io.ErrUnexpectedEOF
			}
			postingChecksum := binary.BigEndian.Uint32(decoded[cursor : cursor+4])
			cursor += 4
			if postingOff > h.postingDataLen || postingLen > h.postingDataLen-postingOff || postingCount > postingLen {
				return errors.New("snapshots: V4 compact lookup postings outside section")
			}
			if within == 0 && !bytes.Equal(blockKey, block.firstKey) {
				return errors.New("snapshots: V4 compact lookup first key mismatch")
			}
			if blockHasPrevious && bytes.Compare(blockPreviousKey, blockKey) >= 0 {
				return errors.New("snapshots: V4 compact lookup keys not sorted")
			}
			if keyID >= expected.keyCount() {
				return fmt.Errorf("snapshots: V4 %s lookup contains too many keys", name)
			}
			if havePreviousKey && bytes.Compare(previousKey, blockKey) >= 0 {
				return fmt.Errorf("snapshots: V4 %s lookup blocks are not strictly sorted", name)
			}
			if postingOff != expectedPostingOff {
				return fmt.Errorf("snapshots: V4 %s lookup postings are not contiguous", name)
			}
			expectedPostingOff += postingLen
			want := expected.rowsFor(keyID)
			if len(want) == 0 {
				return fmt.Errorf("snapshots: V4 %s lookup dictionary id %d is unused", name, keyID)
			}
			if postingCount != uint64(len(want)) {
				return fmt.Errorf("snapshots: V4 %s lookup posting count for dictionary id %d is %d, want %d", name, keyID, postingCount, len(want))
			}
			if topicPositions != nil {
				position := binary.BigEndian.Uint64(blockKey[:8])
				if keyID >= uint64(len(topicPositions)) || topicPositions[keyID] == math.MaxUint64 || position != topicPositions[keyID] {
					return fmt.Errorf("snapshots: V4 topic dictionary position mismatch at id %d", keyID)
				}
			}
			if err := checkEventLogV4PostingRows(postingReader, postingLen, postingChecksum, maxRows, want, &postingScratch); err != nil {
				return fmt.Errorf("snapshots: V4 %s lookup dictionary id %d: %w", name, keyID, err)
			}
			copy(previousKey, blockKey)
			havePreviousKey = true
			blockPreviousKey, blockKey = blockKey, blockPreviousKey
			blockHasPrevious = true
			keyID++
		}
		if cursor != len(decoded) {
			return errors.New("snapshots: V4 compact lookup key block trailing bytes")
		}
	}
	if keyID != h.keyCount {
		return fmt.Errorf("snapshots: V4 %s lookup key count mismatch", name)
	}
	if expectedKeyOff != offset+eventLogV3LookupV2HeaderSize+h.blockDirLen+h.keyDataLen || expectedPostingOff != h.postingDataLen {
		return fmt.Errorf("snapshots: V4 %s lookup data coverage mismatch", name)
	}
	return nil
}

type eventLogV4ValidationPostingSource struct {
	source io.Reader
}

func (r *eventLogV4ValidationPostingSource) Read(p []byte) (int, error) {
	n, err := r.source.Read(p)
	eventLogV4ValidationPostingReadCounter.Inc(1)
	if n > 0 {
		eventLogV4ValidationPostingByteCounter.Inc(int64(n))
	}
	return n, err
}

func readEventLogV4LookupKeyBlock(file io.ReaderAt, offset, length uint64, h eventLogV3LookupV2Header, block eventLogV3LookupV2Block, storedScratch, decodedScratch *[]byte) ([]byte, error) {
	keyDataStart := offset + eventLogV3LookupV2HeaderSize + h.blockDirLen
	postingStart := keyDataStart + h.keyDataLen
	storedLength := uint64(block.dataLen &^ eventLogV3LookupV2StoredRaw)
	if block.dataOff < keyDataStart || block.dataOff > postingStart || storedLength > postingStart-block.dataOff || postingStart > offset+length {
		return nil, errors.New("snapshots: V4 compact lookup key block outside key data")
	}
	if storedLength > uint64(math.MaxInt) {
		return nil, errors.New("snapshots: V4 compact lookup key block exceeds platform limits")
	}
	if cap(*storedScratch) < int(storedLength) {
		*storedScratch = make([]byte, int(storedLength))
	}
	stored := (*storedScratch)[:int(storedLength)]
	if _, err := file.ReadAt(stored, int64(block.dataOff)); err != nil {
		return nil, err
	}
	decoded := stored
	if block.dataLen&eventLogV3LookupV2StoredRaw == 0 {
		_, decoder, err := cbCodec()
		if err != nil {
			return nil, err
		}
		if cap(*decodedScratch) < int(block.rawLen) {
			*decodedScratch = make([]byte, 0, int(block.rawLen))
		}
		decoded, err = decoder.DecodeAll(stored, (*decodedScratch)[:0])
		if err != nil {
			return nil, err
		}
		*decodedScratch = decoded
	}
	if len(decoded) != int(block.rawLen) || crc32.ChecksumIEEE(decoded) != block.checksum {
		return nil, errors.New("snapshots: V4 compact lookup key block checksum mismatch")
	}
	return decoded, nil
}

func consumeEventLogV4LookupUvarint(raw []byte, cursor *int) (uint64, error) {
	if *cursor >= len(raw) {
		return 0, io.ErrUnexpectedEOF
	}
	value, width := binary.Uvarint(raw[*cursor:])
	if width == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if width < 0 {
		return 0, errors.New("snapshots: V4 compact lookup uvarint overflows")
	}
	*cursor += width
	return value, nil
}

func checkEventLogV4PostingRows(reader io.Reader, postingLen uint64, checksum uint32, maxRows uint64, want []uint64, scratch *[]byte) error {
	if postingLen > uint64(math.MaxInt) {
		return errors.New("posting stream exceeds platform limits")
	}
	if cap(*scratch) < int(postingLen) {
		*scratch = make([]byte, int(postingLen))
	}
	raw := (*scratch)[:int(postingLen)]
	if _, err := io.ReadFull(reader, raw); err != nil {
		return err
	}
	if crc32.ChecksumIEEE(raw) != checksum {
		return errors.New("posting checksum mismatch")
	}
	cursor := 0
	var previous uint64
	for i, expectedRow := range want {
		value, err := consumeEventLogV4LookupUvarint(raw, &cursor)
		if err != nil {
			return err
		}
		if i > 0 {
			if value == 0 || math.MaxUint64-previous < value {
				return errors.New("posting delta invalid")
			}
			value += previous
		}
		if value >= maxRows {
			return errors.New("posting row outside segment")
		}
		if value != expectedRow {
			return fmt.Errorf("posting row %d is %d, want %d", i, value, expectedRow)
		}
		previous = value
	}
	if cursor != len(raw) {
		return errors.New("posting stream has trailing bytes")
	}
	return nil
}

func readAllEventLogV3Lookup(file io.ReaderAt, offset, length, size uint64, keySize int, maxRows uint64) (map[string][]uint64, error) {
	compact, err := isEventLogV3LookupV2(file, offset, length, size)
	if err != nil {
		return nil, err
	}
	if compact {
		h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
		if err != nil {
			return nil, err
		}
		out := make(map[string][]uint64, h.keyCount)
		var previous []byte
		expectedKeyOff := offset + eventLogV3LookupV2HeaderSize + h.blockDirLen
		var expectedPostingOff uint64
		for i := uint64(0); i < h.blockCount; i++ {
			block, err := readEventLogV3LookupV2Block(file, offset, h, i)
			if err != nil {
				return nil, err
			}
			storedLen := uint64(block.dataLen &^ eventLogV3LookupV2StoredRaw)
			if block.dataOff != expectedKeyOff {
				return nil, errors.New("snapshots: V4 compact lookup key blocks are not contiguous")
			}
			expectedKeyOff += storedLen
			records, _, err := readEventLogV3LookupV2Records(file, offset, length, h, block)
			if err != nil {
				return nil, err
			}
			for _, record := range records {
				if len(previous) != 0 && bytes.Compare(previous, record.key) >= 0 {
					return nil, errors.New("snapshots: V4 compact lookup blocks not strictly sorted")
				}
				if record.postingOff != expectedPostingOff {
					return nil, errors.New("snapshots: V4 compact lookup postings are not contiguous")
				}
				expectedPostingOff += record.postingLen
				rows, err := readEventLogV3LookupV2RecordRows(file, offset, length, h, record, maxRows)
				if err != nil {
					return nil, err
				}
				out[string(record.key)] = rows
				previous = record.key
			}
		}
		if uint64(len(out)) != h.keyCount {
			return nil, errors.New("snapshots: V4 compact lookup key count mismatch")
		}
		if expectedKeyOff != offset+eventLogV3LookupV2HeaderSize+h.blockDirLen+h.keyDataLen || expectedPostingOff != h.postingDataLen {
			return nil, errors.New("snapshots: V4 compact lookup data coverage mismatch")
		}
		return out, nil
	}
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
			return nil, errors.New("snapshots: V4 lookup keys not sorted")
		}
		rows, err := readEventLogV3LookupRows(file, offset, length, size, keySize, key, maxRows)
		if err != nil {
			return nil, err
		}
		out[string(key)] = rows
		prev = key
	}
	return out, nil
}

func readEventLogV3LookupKeys(file io.ReaderAt, offset, length, size uint64, keySize int) ([][]byte, error) {
	compact, err := isEventLogV3LookupV2(file, offset, length, size)
	if err != nil {
		return nil, err
	}
	if compact {
		return readEventLogV3LookupV2Keys(file, offset, length, size, keySize)
	}
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
			return nil, errors.New("snapshots: V4 lookup keys not strictly sorted")
		}
		keys = append(keys, key)
		previous = key
	}
	return keys, nil
}

func readEventLogV3LookupV2Keys(file io.ReaderAt, offset, length, size uint64, keySize int) ([][]byte, error) {
	h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, h.keyCount)
	var previous []byte
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogV3LookupV2Block(file, offset, h, i)
		if err != nil {
			return nil, err
		}
		records, _, err := readEventLogV3LookupV2Records(file, offset, length, h, block)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if len(previous) != 0 && bytes.Compare(previous, record.key) >= 0 {
				return nil, errors.New("snapshots: V4 compact lookup blocks not strictly sorted")
			}
			keys = append(keys, record.key)
			previous = record.key
		}
	}
	if uint64(len(keys)) != h.keyCount {
		return nil, errors.New("snapshots: V4 compact lookup key count mismatch")
	}
	return keys, nil
}

func writeFreshEventLogV4Index(dir string, eventRef SegmentRef, relPath string) (SegmentRef, error) {
	seg, err := OpenEventLogSegment(dir, eventRef)
	if err != nil {
		return SegmentRef{}, err
	}
	defer seg.Close()
	if seg.header.version != EventLogSegmentV4Version {
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
