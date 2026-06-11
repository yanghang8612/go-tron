package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

const (
	EventLogSegmentVersion = 2

	eventLogHeaderV1Size = 8 + 8 + 8 + 8 + 8 + 8
	eventLogHeaderV2Size = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8
	eventLogHeaderSize   = eventLogHeaderV2Size

	eventLogIndexEntrySize       = 8 + 8 + 8 + common.HashLength + common.HashLength + common.AddressLength + 8 + 8
	eventLogLookupHeaderSize     = 8
	eventLogAddressLookupKeySize = common.AddressLength
	eventLogTopicLookupKeySize   = 8 + common.HashLength
)

var (
	eventLogMagicV1 = [8]byte{'g', 't', 'e', 'v', 'l', 'g', '1', '\n'}
	eventLogMagicV2 = [8]byte{'g', 't', 'e', 'v', 'l', 'g', '2', '\n'}
)

type EventLogSegment struct {
	ref    SegmentRef
	file   *os.File
	header eventLogHeader
}

type eventLogHeader struct {
	version            uint32
	headerSize         uint64
	fromBlock          uint64
	toBlock            uint64
	rowCount           uint64
	indexOffset        uint64
	payloadOffset      uint64
	payloadEnd         uint64
	addressIndexOffset uint64
	addressIndexLength uint64
	topicIndexOffset   uint64
	topicIndexLength   uint64
}

type eventLogIndexEntry struct {
	blockNum  uint64
	txIndex   uint64
	logIndex  uint64
	txHash    common.Hash
	blockHash common.Hash
	address   common.Address
	offset    uint64
	length    uint64
}

type eventLogRow struct {
	entry eventLogIndexEntry
	log   *corepb.TransactionInfo_Log
}

type EventLogFilter = rawdb.EventLogFilter
type EventLog = rawdb.EventLog

func EventLogSegmentPath(fromBlock, toBlock uint64) string {
	return fmt.Sprintf("log/event-log-%d-%d.seg", fromBlock, toBlock)
}

func BuildEventLogSegmentFromChain(chain *rawdb.ChainDB, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	if chain == nil {
		return SegmentRef{}, errors.New("snapshots: nil chain database")
	}
	if dir == "" {
		return SegmentRef{}, errors.New("snapshots: event log segment directory is empty")
	}
	if toBlock < fromBlock {
		return SegmentRef{}, fmt.Errorf("snapshots: event log range [%d,%d] is inverted", fromBlock, toBlock)
	}
	if relPath == "" {
		relPath = EventLogSegmentPath(fromBlock, toBlock)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetEventLog,
		Kind:      SegmentEventLog,
		FromTxNum: fromBlock,
		ToTxNum:   toBlock,
		Path:      filepath.ToSlash(relPath),
	}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}
	rows, err := collectEventLogRows(chain, fromBlock, toBlock)
	if err != nil {
		return SegmentRef{}, err
	}
	return writeEventLogSegmentRows(dir, ref, rows)
}

func CheckEventLogSegment(dir string, ref SegmentRef) error {
	if err := validateEventLogRef(ref); err != nil {
		return err
	}
	if err := checkSegmentFileMetadata(dir, ref, false); err != nil {
		return err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	header, err := readEventLogHeader(file)
	if err != nil {
		return err
	}
	fileSize := uint64(stat.Size())
	if err := validateEventLogHeader(ref, header, fileSize); err != nil {
		return err
	}
	return checkEventLogIndex(file, ref, header, fileSize)
}

func OpenEventLogSegment(dir string, ref SegmentRef) (*EventLogSegment, error) {
	if err := validateEventLogRef(ref); err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	header, err := readEventLogHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateEventLogHeader(ref, header, uint64(stat.Size())); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &EventLogSegment{ref: ref, file: file, header: header}, nil
}

func (s *EventLogSegment) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *EventLogSegment) IterateLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	if s == nil || s.file == nil {
		return errors.New("snapshots: nil event log segment")
	}
	if toBlock < fromBlock {
		return fmt.Errorf("snapshots: event log iterate range [%d,%d] is inverted", fromBlock, toBlock)
	}
	fromBlock = max(fromBlock, s.header.fromBlock)
	toBlock = min(toBlock, s.header.toBlock)
	if toBlock < fromBlock {
		return nil
	}
	if used, err := s.iterateLogsByLookupIndexes(fromBlock, toBlock, filter, fn); err != nil || used {
		return err
	}
	for i := uint64(0); i < s.header.rowCount; i++ {
		entry, err := readEventLogIndexEntryAt(s.file, eventLogIndexEntryOffset(s.header, i))
		if err != nil {
			return err
		}
		if entry.blockNum < fromBlock || entry.blockNum > toBlock {
			continue
		}
		if !eventLogAddressMatches(filter, entry.address) {
			continue
		}
		log, err := s.readLog(entry)
		if err != nil {
			return err
		}
		if !eventLogTopicsMatch(filter.Topics, log.GetTopics()) {
			continue
		}
		cont, err := fn(EventLog{
			BlockNum:  entry.blockNum,
			TxIndex:   entry.txIndex,
			LogIndex:  entry.logIndex,
			TxHash:    entry.txHash,
			BlockHash: entry.blockHash,
			Address:   entry.address,
			Log:       log,
		})
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (s *EventLogSegment) iterateLogsByLookupIndexes(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) (bool, error) {
	candidates, ok, err := s.lookupEventLogCandidateRows(filter)
	if err != nil || !ok {
		return ok, err
	}
	for _, rowIndex := range candidates {
		if rowIndex >= s.header.rowCount {
			return true, fmt.Errorf("snapshots: event log lookup row %d outside row count %d", rowIndex, s.header.rowCount)
		}
		entry, err := readEventLogIndexEntryAt(s.file, eventLogIndexEntryOffset(s.header, rowIndex))
		if err != nil {
			return true, err
		}
		if entry.blockNum < fromBlock || entry.blockNum > toBlock {
			continue
		}
		if !eventLogAddressMatches(filter, entry.address) {
			continue
		}
		log, err := s.readLog(entry)
		if err != nil {
			return true, err
		}
		if !eventLogTopicsMatch(filter.Topics, log.GetTopics()) {
			continue
		}
		cont, err := fn(EventLog{
			BlockNum:  entry.blockNum,
			TxIndex:   entry.txIndex,
			LogIndex:  entry.logIndex,
			TxHash:    entry.txHash,
			BlockHash: entry.blockHash,
			Address:   entry.address,
			Log:       log,
		})
		if err != nil || !cont {
			return true, err
		}
	}
	return true, nil
}

func (s *EventLogSegment) lookupEventLogCandidateRows(filter EventLogFilter) ([]uint64, bool, error) {
	if s.header.addressIndexOffset == 0 && s.header.topicIndexOffset == 0 {
		return nil, false, nil
	}
	var candidates []uint64
	haveCandidates := false
	if len(filter.Addresses) > 0 {
		if s.header.addressIndexOffset == 0 {
			return nil, false, nil
		}
		var union []uint64
		for _, address := range filter.Addresses {
			rows, err := readEventLogLookupRows(s.file, s.header.addressIndexOffset, s.header.addressIndexLength, eventLogAddressLookupKey(address))
			if err != nil {
				return nil, true, err
			}
			union = unionSortedUint64(union, rows)
		}
		candidates = union
		haveCandidates = true
	}
	for position, required := range filter.Topics {
		if len(required) == 0 {
			continue
		}
		if s.header.topicIndexOffset == 0 {
			return nil, false, nil
		}
		var union []uint64
		for _, topic := range required {
			rows, err := readEventLogLookupRows(s.file, s.header.topicIndexOffset, s.header.topicIndexLength, eventLogTopicLookupKey(uint64(position), topic))
			if err != nil {
				return nil, true, err
			}
			union = unionSortedUint64(union, rows)
		}
		if haveCandidates {
			candidates = intersectSortedUint64(candidates, union)
		} else {
			candidates = union
			haveCandidates = true
		}
		if len(candidates) == 0 {
			return candidates, true, nil
		}
	}
	if !haveCandidates {
		return nil, false, nil
	}
	return candidates, true, nil
}

func (s *EventLogSegment) readLog(entry eventLogIndexEntry) (*corepb.TransactionInfo_Log, error) {
	raw, err := readEventLogPayloadAt(s.file, entry.offset, entry.length)
	if err != nil {
		return nil, err
	}
	var log corepb.TransactionInfo_Log
	if err := proto.Unmarshal(raw, &log); err != nil {
		return nil, err
	}
	return &log, nil
}

func (m *Manager) IterateEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
	if m == nil {
		return nil
	}
	if toBlock < fromBlock {
		return fmt.Errorf("snapshots: event log manager range [%d,%d] is inverted", fromBlock, toBlock)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return err
	}
	for _, ref := range eventLogRefs(manifest) {
		if ref.ToTxNum < fromBlock || ref.FromTxNum > toBlock {
			continue
		}
		seg, err := OpenEventLogSegment(m.dir, ref)
		if err != nil {
			return err
		}
		stopped := false
		err = seg.IterateLogs(fromBlock, toBlock, filter, func(row EventLog) (bool, error) {
			cont, err := fn(row)
			if err != nil {
				return false, err
			}
			if !cont {
				stopped = true
			}
			return cont, nil
		})
		closeErr := seg.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if stopped {
			return nil
		}
	}
	return nil
}

func (m *Manager) EventLogRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	if m == nil {
		return false, nil
	}
	if toBlock < fromBlock {
		return false, fmt.Errorf("snapshots: event log coverage range [%d,%d] is inverted", fromBlock, toBlock)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	next := fromBlock
	for _, ref := range eventLogRefs(manifest) {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			return false, nil
		}
		if ref.ToTxNum >= toBlock {
			return true, nil
		}
		if ref.ToTxNum == ^uint64(0) {
			return false, nil
		}
		next = ref.ToTxNum + 1
	}
	return false, nil
}

func collectEventLogRows(chain *rawdb.ChainDB, fromBlock, toBlock uint64) ([]eventLogRow, error) {
	var rows []eventLogRow
	for blockNum := fromBlock; ; blockNum++ {
		block := rawdb.ReadBlock(chain, blockNum)
		if block == nil {
			return nil, fmt.Errorf("snapshots: missing block %d during event log segment build", blockNum)
		}
		blockHash := block.Hash()
		txs := block.Transactions()
		infos := rawdb.ReadTransactionInfosByBlock(chain, blockNum)
		logIndex := uint64(0)
		for txIndex, info := range infos {
			if info == nil {
				continue
			}
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
				rows = append(rows, eventLogRow{
					entry: eventLogIndexEntry{
						blockNum:  blockNum,
						txIndex:   uint64(txIndex),
						logIndex:  logIndex,
						txHash:    txHash,
						blockHash: blockHash,
						address:   eventLogAddress(log.GetAddress()),
					},
					log: proto.Clone(log).(*corepb.TransactionInfo_Log),
				})
				logIndex++
			}
		}
		if blockNum == toBlock {
			break
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		return compareEventLogEntries(rows[i].entry, rows[j].entry) < 0
	})
	return rows, nil
}

func writeEventLogSegmentRows(dir string, ref SegmentRef, rows []eventLogRow) (SegmentRef, error) {
	rowCount := uint64(len(rows))
	indexBytes, overflow := checkedMul(rowCount, eventLogIndexEntrySize)
	if overflow {
		return SegmentRef{}, fmt.Errorf("snapshots: event log index entries %d overflow size", rowCount)
	}
	payloadOffset, overflow := checkedAdd(eventLogHeaderSize, indexBytes)
	if overflow {
		return SegmentRef{}, fmt.Errorf("snapshots: event log payload offset overflow")
	}
	if payloadOffset > math.MaxInt64 {
		return SegmentRef{}, fmt.Errorf("snapshots: event log payload offset %d overflows int64", payloadOffset)
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

	if _, err := tmp.Write(make([]byte, eventLogHeaderSize)); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if _, err := tmp.Seek(int64(payloadOffset), io.SeekStart); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offset := payloadOffset
	for i, row := range rows {
		raw, err := proto.Marshal(row.log)
		if err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		if _, err := tmp.Write(raw); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		entry := row.entry
		entry.offset = offset
		entry.length = uint64(len(raw))
		if err := writeEventLogIndexEntryAt(tmp, int64(eventLogHeaderSize+i*eventLogIndexEntrySize), entry); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		offset += uint64(len(raw))
	}
	payloadEnd := offset
	addressIndexOffset := offset
	addressIndexLength, err := writeEventLogLookupIndexAt(tmp, addressIndexOffset, eventLogAddressLookupKeySize, eventLogAddressLookupPostings(rows))
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offset += addressIndexLength
	topicIndexOffset := offset
	topicIndexLength, err := writeEventLogLookupIndexAt(tmp, topicIndexOffset, eventLogTopicLookupKeySize, eventLogTopicLookupPostings(rows))
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	header := eventLogHeader{
		version:            EventLogSegmentVersion,
		headerSize:         uint64(eventLogHeaderSize),
		fromBlock:          ref.FromTxNum,
		toBlock:            ref.ToTxNum,
		rowCount:           rowCount,
		indexOffset:        uint64(eventLogHeaderSize),
		payloadOffset:      payloadOffset,
		payloadEnd:         payloadEnd,
		addressIndexOffset: addressIndexOffset,
		addressIndexLength: addressIndexLength,
		topicIndexOffset:   topicIndexOffset,
		topicIndexLength:   topicIndexLength,
	}
	if err := writeEventLogHeaderAt(tmp, header); err != nil {
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
	return ref, nil
}

func eventLogRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	var refs []SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentEventLog && ref.normalizedDataset() == SegmentDatasetEventLog {
			refs = append(refs, ref)
		}
	}
	sortSegments(refs)
	return refs
}

func validateEventLogRef(ref SegmentRef) error {
	if ref.Kind != SegmentEventLog || ref.normalizedDataset() != SegmentDatasetEventLog {
		return fmt.Errorf("snapshots: segment %q is %s/%s, want event-log/event-log", ref.Path, ref.Dataset, ref.Kind)
	}
	return validateSegmentRef(ref)
}

func validateEventLogHeader(ref SegmentRef, header eventLogHeader, fileSize uint64) error {
	if header.fromBlock != ref.FromTxNum || header.toBlock != ref.ToTxNum {
		return fmt.Errorf("snapshots: event log segment %q range [%d,%d], want [%d,%d]", ref.Path, header.fromBlock, header.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	if header.toBlock < header.fromBlock {
		return fmt.Errorf("snapshots: event log segment %q inverted header range [%d,%d]", ref.Path, header.fromBlock, header.toBlock)
	}
	if header.headerSize != eventLogHeaderV1Size && header.headerSize != eventLogHeaderV2Size {
		return fmt.Errorf("snapshots: event log segment %q header size %d is unsupported", ref.Path, header.headerSize)
	}
	if header.indexOffset != header.headerSize {
		return fmt.Errorf("snapshots: event log segment %q index offset %d, want %d", ref.Path, header.indexOffset, header.headerSize)
	}
	indexBytes, overflow := checkedMul(header.rowCount, eventLogIndexEntrySize)
	if overflow {
		return fmt.Errorf("snapshots: event log segment %q index size overflow", ref.Path)
	}
	payloadOffset, overflow := checkedAdd(header.indexOffset, indexBytes)
	if overflow || header.payloadOffset != payloadOffset {
		return fmt.Errorf("snapshots: event log segment %q payload offset %d, want %d", ref.Path, header.payloadOffset, payloadOffset)
	}
	if header.payloadOffset > fileSize {
		return fmt.Errorf("snapshots: event log segment %q payload offset %d after file size %d", ref.Path, header.payloadOffset, fileSize)
	}
	if header.version == 1 {
		if header.payloadEnd != 0 || header.addressIndexOffset != 0 || header.addressIndexLength != 0 || header.topicIndexOffset != 0 || header.topicIndexLength != 0 {
			return fmt.Errorf("snapshots: event log segment %q v1 header has lookup offsets", ref.Path)
		}
		return nil
	}
	if header.version != EventLogSegmentVersion {
		return fmt.Errorf("snapshots: event log segment %q version %d is unsupported", ref.Path, header.version)
	}
	if header.payloadEnd < header.payloadOffset || header.payloadEnd > fileSize {
		return fmt.Errorf("snapshots: event log segment %q payload end %d outside [%d,%d]", ref.Path, header.payloadEnd, header.payloadOffset, fileSize)
	}
	if header.addressIndexOffset != header.payloadEnd {
		return fmt.Errorf("snapshots: event log segment %q address index offset %d, want payload end %d", ref.Path, header.addressIndexOffset, header.payloadEnd)
	}
	addressEnd, overflow := checkedAdd(header.addressIndexOffset, header.addressIndexLength)
	if overflow || addressEnd > fileSize {
		return fmt.Errorf("snapshots: event log segment %q address index [%d,%d] outside file size %d", ref.Path, header.addressIndexOffset, addressEnd, fileSize)
	}
	if header.topicIndexOffset != addressEnd {
		return fmt.Errorf("snapshots: event log segment %q topic index offset %d, want address index end %d", ref.Path, header.topicIndexOffset, addressEnd)
	}
	topicEnd, overflow := checkedAdd(header.topicIndexOffset, header.topicIndexLength)
	if overflow || topicEnd != fileSize {
		return fmt.Errorf("snapshots: event log segment %q topic index end %d, want file size %d", ref.Path, topicEnd, fileSize)
	}
	return nil
}

func checkEventLogIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader, fileSize uint64) error {
	var prev eventLogIndexEntry
	for i := uint64(0); i < header.rowCount; i++ {
		entry, err := readEventLogIndexEntryAt(file, eventLogIndexEntryOffset(header, i))
		if err != nil {
			return err
		}
		if entry.blockNum < ref.FromTxNum || entry.blockNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: event log segment %q entry %d block %d outside [%d,%d]", ref.Path, i, entry.blockNum, ref.FromTxNum, ref.ToTxNum)
		}
		end, overflow := checkedAdd(entry.offset, entry.length)
		if overflow || entry.offset < header.payloadOffset || end > fileSize {
			return fmt.Errorf("snapshots: event log segment %q entry %d payload [%d,%d] outside file size %d", ref.Path, i, entry.offset, end, fileSize)
		}
		if i > 0 && compareEventLogEntries(prev, entry) >= 0 {
			return fmt.Errorf("snapshots: event log segment %q index entry %d is not strictly sorted", ref.Path, i)
		}
		raw, err := readEventLogPayloadAt(file, entry.offset, entry.length)
		if err != nil {
			return err
		}
		var log corepb.TransactionInfo_Log
		if err := proto.Unmarshal(raw, &log); err != nil {
			return fmt.Errorf("snapshots: event log segment %q entry %d payload: %w", ref.Path, i, err)
		}
		if eventLogAddress(log.GetAddress()) != entry.address {
			return fmt.Errorf("snapshots: event log segment %q entry %d address index mismatch", ref.Path, i)
		}
		prev = entry
	}
	if header.version >= 2 {
		if err := checkEventLogAddressLookupIndex(file, ref, header); err != nil {
			return err
		}
		if err := checkEventLogTopicLookupIndex(file, ref, header); err != nil {
			return err
		}
	}
	return nil
}

func eventLogAddress(raw []byte) common.Address {
	if len(raw) > common.AddressLength {
		raw = raw[len(raw)-common.AddressLength:]
	}
	return common.BytesToAddress(raw)
}

func eventLogAddressMatches(filter EventLogFilter, address common.Address) bool {
	if len(filter.Addresses) == 0 {
		return true
	}
	for _, candidate := range filter.Addresses {
		if candidate == address {
			return true
		}
	}
	return false
}

func eventLogTopicsMatch(filterTopics [][]common.Hash, logTopics [][]byte) bool {
	for i, required := range filterTopics {
		if len(required) == 0 {
			continue
		}
		if i >= len(logTopics) {
			return false
		}
		var got common.Hash
		copy(got[:], logTopics[i])
		matched := false
		for _, want := range required {
			if want == got {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func eventLogAddressLookupPostings(rows []eventLogRow) map[string][]uint64 {
	postings := make(map[string][]uint64)
	for i, row := range rows {
		key := string(eventLogAddressLookupKey(row.entry.address))
		postings[key] = append(postings[key], uint64(i))
	}
	return postings
}

func eventLogTopicLookupPostings(rows []eventLogRow) map[string][]uint64 {
	postings := make(map[string][]uint64)
	for i, row := range rows {
		for position, rawTopic := range row.log.GetTopics() {
			var topic common.Hash
			copy(topic[:], rawTopic)
			key := string(eventLogTopicLookupKey(uint64(position), topic))
			postings[key] = append(postings[key], uint64(i))
		}
	}
	return postings
}

func eventLogAddressLookupKey(address common.Address) []byte {
	return append([]byte(nil), address[:]...)
}

func eventLogTopicLookupKey(position uint64, topic common.Hash) []byte {
	key := make([]byte, eventLogTopicLookupKeySize)
	binary.BigEndian.PutUint64(key[0:8], position)
	copy(key[8:], topic[:])
	return key
}

func writeEventLogLookupIndexAt(file *os.File, offset uint64, keySize int, postings map[string][]uint64) (uint64, error) {
	keys := make([]string, 0, len(postings))
	for key := range postings {
		if len(key) != keySize {
			return 0, fmt.Errorf("snapshots: event log lookup key length %d, want %d", len(key), keySize)
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	entrySize := keySize + 16
	headerAndDir := eventLogLookupHeaderSize + len(keys)*entrySize
	if uint64(headerAndDir) > math.MaxInt64 {
		return 0, fmt.Errorf("snapshots: event log lookup directory size %d overflows int64", headerAndDir)
	}
	var buf bytes.Buffer
	var word [8]byte
	binary.BigEndian.PutUint64(word[:], uint64(len(keys)))
	buf.Write(word[:])
	postingsOffset, overflow := checkedAdd(offset, uint64(headerAndDir))
	if overflow {
		return 0, fmt.Errorf("snapshots: event log lookup postings start offset overflow")
	}
	for _, key := range keys {
		rows := postings[key]
		buf.WriteString(key)
		binary.BigEndian.PutUint64(word[:], postingsOffset)
		buf.Write(word[:])
		binary.BigEndian.PutUint64(word[:], uint64(len(rows)))
		buf.Write(word[:])
		bytesLen, overflow := checkedMul(uint64(len(rows)), 8)
		if overflow {
			return 0, fmt.Errorf("snapshots: event log lookup postings for key overflow")
		}
		var addOverflow bool
		postingsOffset, addOverflow = checkedAdd(postingsOffset, bytesLen)
		if addOverflow {
			return 0, fmt.Errorf("snapshots: event log lookup postings offset overflow")
		}
	}
	for _, key := range keys {
		for _, rowIndex := range postings[key] {
			binary.BigEndian.PutUint64(word[:], rowIndex)
			buf.Write(word[:])
		}
	}
	if _, err := file.WriteAt(buf.Bytes(), int64(offset)); err != nil {
		return 0, err
	}
	return uint64(buf.Len()), nil
}

func readEventLogLookupRows(file io.ReaderAt, offset, length uint64, key []byte) ([]uint64, error) {
	if offset == 0 || length == 0 {
		return nil, nil
	}
	if length < eventLogLookupHeaderSize {
		return nil, fmt.Errorf("snapshots: event log lookup length %d smaller than header", length)
	}
	indexEnd, overflow := checkedAdd(offset, length)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log lookup range [%d,+%d] overflows", offset, length)
	}
	count, err := readEventLogUint64At(file, offset)
	if err != nil {
		return nil, err
	}
	entrySize := uint64(len(key) + 16)
	dirBytes, overflow := checkedMul(count, entrySize)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log lookup directory overflows")
	}
	dirStart, overflow := checkedAdd(offset, eventLogLookupHeaderSize)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log lookup directory start overflows")
	}
	dirEnd, overflow := checkedAdd(dirStart, dirBytes)
	if overflow || dirEnd > indexEnd {
		return nil, fmt.Errorf("snapshots: event log lookup directory [%d,%d] outside [%d,%d]", dirStart, dirEnd, offset, indexEnd)
	}
	lo, hi := uint64(0), count
	entry := make([]byte, int(entrySize))
	for lo < hi {
		mid := lo + (hi-lo)/2
		entryOffset := dirStart + mid*entrySize
		if _, err := file.ReadAt(entry, int64(entryOffset)); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		cmp := bytes.Compare(entry[:len(key)], key)
		if cmp < 0 {
			lo = mid + 1
			continue
		}
		if cmp > 0 {
			hi = mid
			continue
		}
		postingsOffset := binary.BigEndian.Uint64(entry[len(key) : len(key)+8])
		postingsCount := binary.BigEndian.Uint64(entry[len(key)+8 : len(key)+16])
		return readEventLogLookupPostings(file, offset, length, dirEnd, postingsOffset, postingsCount)
	}
	return nil, nil
}

func readEventLogLookupPostings(file io.ReaderAt, indexOffset, indexLength, minPostingsOffset, postingsOffset, postingsCount uint64) ([]uint64, error) {
	indexEnd, overflow := checkedAdd(indexOffset, indexLength)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log lookup index range [%d,+%d] overflows", indexOffset, indexLength)
	}
	postingsBytes, overflow := checkedMul(postingsCount, 8)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log lookup postings overflow")
	}
	postingsEnd, overflow := checkedAdd(postingsOffset, postingsBytes)
	if overflow || postingsOffset < minPostingsOffset || postingsEnd > indexEnd {
		return nil, fmt.Errorf("snapshots: event log lookup postings [%d,%d] outside [%d,%d]", postingsOffset, postingsEnd, indexOffset, indexEnd)
	}
	rows := make([]uint64, postingsCount)
	var raw [8]byte
	for i := uint64(0); i < postingsCount; i++ {
		if _, err := file.ReadAt(raw[:], int64(postingsOffset+i*8)); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		rows[i] = binary.BigEndian.Uint64(raw[:])
	}
	return rows, nil
}

func unionSortedUint64(a, b []uint64) []uint64 {
	if len(a) == 0 {
		return append([]uint64(nil), b...)
	}
	if len(b) == 0 {
		return append([]uint64(nil), a...)
	}
	out := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		case b[j] < a[i]:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

func intersectSortedUint64(a, b []uint64) []uint64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	out := make([]uint64, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case b[j] < a[i]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

func checkEventLogAddressLookupIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader) error {
	return checkEventLogLookupIndex(file, ref, header, "address", header.addressIndexOffset, header.addressIndexLength, eventLogAddressLookupKeySize, func(rowIndex uint64, key []byte) error {
		entry, err := readEventLogIndexEntryAt(file, eventLogIndexEntryOffset(header, rowIndex))
		if err != nil {
			return err
		}
		if !bytes.Equal(key, entry.address[:]) {
			return fmt.Errorf("snapshots: event log segment %q address lookup row %d key mismatch", ref.Path, rowIndex)
		}
		return nil
	})
}

func checkEventLogTopicLookupIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader) error {
	return checkEventLogLookupIndex(file, ref, header, "topic", header.topicIndexOffset, header.topicIndexLength, eventLogTopicLookupKeySize, func(rowIndex uint64, key []byte) error {
		position := binary.BigEndian.Uint64(key[:8])
		want := key[8:]
		entry, err := readEventLogIndexEntryAt(file, eventLogIndexEntryOffset(header, rowIndex))
		if err != nil {
			return err
		}
		raw, err := readEventLogPayloadAt(file, entry.offset, entry.length)
		if err != nil {
			return err
		}
		var log corepb.TransactionInfo_Log
		if err := proto.Unmarshal(raw, &log); err != nil {
			return fmt.Errorf("snapshots: event log segment %q topic lookup row %d payload: %w", ref.Path, rowIndex, err)
		}
		if position >= uint64(len(log.GetTopics())) || !bytes.Equal(want, eventLogTopicBytes(log.GetTopics()[position])) {
			return fmt.Errorf("snapshots: event log segment %q topic lookup row %d key mismatch", ref.Path, rowIndex)
		}
		return nil
	})
}

func checkEventLogLookupIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader, name string, offset, length uint64, keySize int, verify func(rowIndex uint64, key []byte) error) error {
	if length < eventLogLookupHeaderSize {
		return fmt.Errorf("snapshots: event log segment %q %s lookup length %d smaller than header", ref.Path, name, length)
	}
	indexEnd, overflow := checkedAdd(offset, length)
	if overflow {
		return fmt.Errorf("snapshots: event log segment %q %s lookup range [%d,+%d] overflows", ref.Path, name, offset, length)
	}
	count, err := readEventLogUint64At(file, offset)
	if err != nil {
		return err
	}
	entrySize := uint64(keySize + 16)
	dirBytes, overflow := checkedMul(count, entrySize)
	if overflow {
		return fmt.Errorf("snapshots: event log segment %q %s lookup directory overflows", ref.Path, name)
	}
	dirStart, overflow := checkedAdd(offset, eventLogLookupHeaderSize)
	if overflow {
		return fmt.Errorf("snapshots: event log segment %q %s lookup directory start overflows", ref.Path, name)
	}
	dirEnd, overflow := checkedAdd(dirStart, dirBytes)
	if overflow || dirEnd > indexEnd {
		return fmt.Errorf("snapshots: event log segment %q %s lookup directory [%d,%d] outside [%d,%d]", ref.Path, name, dirStart, dirEnd, offset, indexEnd)
	}
	var prevKey []byte
	entryRaw := make([]byte, int(entrySize))
	for i := uint64(0); i < count; i++ {
		entryOffset := dirStart + i*entrySize
		if _, err := file.ReadAt(entryRaw, int64(entryOffset)); err != nil {
			if errors.Is(err, io.EOF) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		key := append([]byte(nil), entryRaw[:keySize]...)
		if i > 0 && bytes.Compare(prevKey, key) >= 0 {
			return fmt.Errorf("snapshots: event log segment %q %s lookup key %d is not strictly sorted", ref.Path, name, i)
		}
		prevKey = key
		postingsOffset := binary.BigEndian.Uint64(entryRaw[keySize : keySize+8])
		postingsCount := binary.BigEndian.Uint64(entryRaw[keySize+8 : keySize+16])
		rows, err := readEventLogLookupPostings(file, offset, length, dirEnd, postingsOffset, postingsCount)
		if err != nil {
			return err
		}
		var prevRow uint64
		for j, rowIndex := range rows {
			if rowIndex >= header.rowCount {
				return fmt.Errorf("snapshots: event log segment %q %s lookup row %d outside row count %d", ref.Path, name, rowIndex, header.rowCount)
			}
			if j > 0 && rowIndex <= prevRow {
				return fmt.Errorf("snapshots: event log segment %q %s lookup postings for key %d are not strictly sorted", ref.Path, name, i)
			}
			if err := verify(rowIndex, key); err != nil {
				return err
			}
			prevRow = rowIndex
		}
	}
	return nil
}

func eventLogTopicBytes(raw []byte) []byte {
	var topic common.Hash
	copy(topic[:], raw)
	return topic[:]
}

func compareEventLogEntries(a, b eventLogIndexEntry) int {
	if a.blockNum != b.blockNum {
		return compareEventLogUint64(a.blockNum, b.blockNum)
	}
	if a.txIndex != b.txIndex {
		return compareEventLogUint64(a.txIndex, b.txIndex)
	}
	return compareEventLogUint64(a.logIndex, b.logIndex)
}

func compareEventLogUint64(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func readEventLogHeader(r io.Reader) (eventLogHeader, error) {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return eventLogHeader{}, io.ErrUnexpectedEOF
		}
		return eventLogHeader{}, err
	}
	switch magic {
	case eventLogMagicV1:
		var raw [eventLogHeaderV1Size - 8]byte
		if _, err := io.ReadFull(r, raw[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return eventLogHeader{}, io.ErrUnexpectedEOF
			}
			return eventLogHeader{}, err
		}
		return eventLogHeader{
			version:       1,
			headerSize:    eventLogHeaderV1Size,
			fromBlock:     binary.BigEndian.Uint64(raw[0:8]),
			toBlock:       binary.BigEndian.Uint64(raw[8:16]),
			rowCount:      binary.BigEndian.Uint64(raw[16:24]),
			indexOffset:   binary.BigEndian.Uint64(raw[24:32]),
			payloadOffset: binary.BigEndian.Uint64(raw[32:40]),
		}, nil
	case eventLogMagicV2:
		var raw [eventLogHeaderV2Size - 8]byte
		if _, err := io.ReadFull(r, raw[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return eventLogHeader{}, io.ErrUnexpectedEOF
			}
			return eventLogHeader{}, err
		}
		return eventLogHeader{
			version:            2,
			headerSize:         eventLogHeaderV2Size,
			fromBlock:          binary.BigEndian.Uint64(raw[0:8]),
			toBlock:            binary.BigEndian.Uint64(raw[8:16]),
			rowCount:           binary.BigEndian.Uint64(raw[16:24]),
			indexOffset:        binary.BigEndian.Uint64(raw[24:32]),
			payloadOffset:      binary.BigEndian.Uint64(raw[32:40]),
			payloadEnd:         binary.BigEndian.Uint64(raw[40:48]),
			addressIndexOffset: binary.BigEndian.Uint64(raw[48:56]),
			addressIndexLength: binary.BigEndian.Uint64(raw[56:64]),
			topicIndexOffset:   binary.BigEndian.Uint64(raw[64:72]),
			topicIndexLength:   binary.BigEndian.Uint64(raw[72:80]),
		}, nil
	default:
		return eventLogHeader{}, errors.New("snapshots: invalid event log segment magic")
	}
}

func writeEventLogHeaderAt(file *os.File, header eventLogHeader) error {
	var raw [eventLogHeaderV2Size]byte
	copy(raw[0:8], eventLogMagicV2[:])
	binary.BigEndian.PutUint64(raw[8:16], header.fromBlock)
	binary.BigEndian.PutUint64(raw[16:24], header.toBlock)
	binary.BigEndian.PutUint64(raw[24:32], header.rowCount)
	binary.BigEndian.PutUint64(raw[32:40], header.indexOffset)
	binary.BigEndian.PutUint64(raw[40:48], header.payloadOffset)
	binary.BigEndian.PutUint64(raw[48:56], header.payloadEnd)
	binary.BigEndian.PutUint64(raw[56:64], header.addressIndexOffset)
	binary.BigEndian.PutUint64(raw[64:72], header.addressIndexLength)
	binary.BigEndian.PutUint64(raw[72:80], header.topicIndexOffset)
	binary.BigEndian.PutUint64(raw[80:88], header.topicIndexLength)
	_, err := file.WriteAt(raw[:], 0)
	return err
}

func writeEventLogIndexEntryAt(file *os.File, offset int64, entry eventLogIndexEntry) error {
	var raw [eventLogIndexEntrySize]byte
	binary.BigEndian.PutUint64(raw[0:8], entry.blockNum)
	binary.BigEndian.PutUint64(raw[8:16], entry.txIndex)
	binary.BigEndian.PutUint64(raw[16:24], entry.logIndex)
	copy(raw[24:56], entry.txHash[:])
	copy(raw[56:88], entry.blockHash[:])
	copy(raw[88:109], entry.address.Bytes())
	binary.BigEndian.PutUint64(raw[109:117], entry.offset)
	binary.BigEndian.PutUint64(raw[117:125], entry.length)
	if _, err := file.WriteAt(raw[:], offset); err != nil {
		return err
	}
	return nil
}

func readEventLogIndexEntryAt(file io.ReaderAt, offset int64) (eventLogIndexEntry, error) {
	var raw [eventLogIndexEntrySize]byte
	if _, err := file.ReadAt(raw[:], offset); err != nil {
		if errors.Is(err, io.EOF) {
			return eventLogIndexEntry{}, io.ErrUnexpectedEOF
		}
		return eventLogIndexEntry{}, err
	}
	var entry eventLogIndexEntry
	entry.blockNum = binary.BigEndian.Uint64(raw[0:8])
	entry.txIndex = binary.BigEndian.Uint64(raw[8:16])
	entry.logIndex = binary.BigEndian.Uint64(raw[16:24])
	copy(entry.txHash[:], raw[24:56])
	copy(entry.blockHash[:], raw[56:88])
	entry.address = common.BytesToAddress(raw[88:109])
	entry.offset = binary.BigEndian.Uint64(raw[109:117])
	entry.length = binary.BigEndian.Uint64(raw[117:125])
	return entry, nil
}

func readEventLogPayloadAt(file io.ReaderAt, offset, length uint64) ([]byte, error) {
	if length > math.MaxInt {
		return nil, fmt.Errorf("snapshots: event log payload length %d overflows int", length)
	}
	raw := make([]byte, int(length))
	if _, err := file.ReadAt(raw, int64(offset)); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return raw, nil
}

func eventLogIndexEntryOffset(header eventLogHeader, index uint64) int64 {
	return int64(header.indexOffset + index*eventLogIndexEntrySize)
}

func readEventLogUint64At(file io.ReaderAt, offset uint64) (uint64, error) {
	var raw [8]byte
	if _, err := file.ReadAt(raw[:], int64(offset)); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	return binary.BigEndian.Uint64(raw[:]), nil
}
