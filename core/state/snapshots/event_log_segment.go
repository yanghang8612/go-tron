package snapshots

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	EventLogSegmentVersion = 2
	EventLogIndexVersion   = 1

	eventLogHeaderV1Size    = 8 + 8 + 8 + 8 + 8 + 8
	eventLogHeaderV2Size    = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8
	eventLogHeaderSize      = eventLogHeaderV2Size
	eventLogIndexHeaderSize = 8 + 8 + 8 + 8 + 8 + 8 + 8

	eventLogIndexEntrySize       = 8 + 8 + 8 + common.HashLength + common.HashLength + common.AddressLength + 8 + 8
	eventLogLookupHeaderSize     = 8
	eventLogAddressLookupKeySize = common.AddressLength
	eventLogTopicLookupKeySize   = 8 + common.HashLength
	eventLogETLKeySize           = 8 + 8 + 8
	eventLogETLValueHeaderSize   = common.HashLength + common.HashLength + common.AddressLength
	eventLogIndexETLKindAddress  = byte(1)
	eventLogIndexETLKindTopic    = byte(2)
)

var (
	eventLogMagicV1    = [8]byte{'g', 't', 'e', 'v', 'l', 'g', '1', '\n'}
	eventLogMagicV2    = [8]byte{'g', 't', 'e', 'v', 'l', 'g', '2', '\n'}
	eventLogIndexMagic = [8]byte{'g', 't', 'e', 'v', 'l', 'x', '1', '\n'}
)

type EventLogSegment struct {
	ref    SegmentRef
	file   *os.File
	header eventLogHeader
}

type EventLogIndexSegment struct {
	ref    SegmentRef
	file   *os.File
	header eventLogIndexHeader
}

type EventLogIndexLookupStats struct {
	Keys                       uint64
	Postings                   uint64
	AveragePostingsPerKeyMilli uint64
	MaxPostingsPerKey          uint64
	SingletonKeys              uint64
	MultiPostingKeys           uint64
}

type EventLogIndexSegmentStats struct {
	Path      string
	FromBlock uint64
	ToBlock   uint64
	Size      uint64
	Address   EventLogIndexLookupStats
	Topic     EventLogIndexLookupStats
}

type EventLogIndexInspection struct {
	Segments []EventLogIndexSegmentStats
	Address  EventLogIndexLookupStats
	Topic    EventLogIndexLookupStats
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

type eventLogIndexHeader struct {
	fromBlock          uint64
	toBlock            uint64
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

type EventLogFilter = rawdb.EventLogFilter
type EventLog = rawdb.EventLog

func EventLogSegmentPath(fromBlock, toBlock uint64) string {
	return fmt.Sprintf("log/event-log-%d-%d.seg", fromBlock, toBlock)
}

func EventLogIndexSegmentPath(fromBlock, toBlock uint64) string {
	return fmt.Sprintf("log/event-log-index-%d-%d.idx", fromBlock, toBlock)
}

func BuildEventLogSegmentFromChain(chain *rawdb.ChainDB, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	return BuildEventLogSegmentFromChainWithOptions(chain, dir, relPath, fromBlock, toBlock, RestoreETLOptions{})
}

func BuildEventLogSegmentFromChainWithOptions(chain *rawdb.ChainDB, dir, relPath string, fromBlock, toBlock uint64, opts RestoreETLOptions) (SegmentRef, error) {
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
	collector, err := etl.NewCollector(opts.collectorOptions())
	if err != nil {
		return SegmentRef{}, err
	}
	defer collector.Close()
	rowCount, err := collectEventLogRowsToETL(chain, fromBlock, toBlock, collector)
	if err != nil {
		return SegmentRef{}, err
	}
	return writeEventLogSegmentFromETL(dir, ref, collector, rowCount)
}

func BuildEventLogIndexSegmentFromEventLogSegments(dir string, eventRefs []SegmentRef, relPath string) (SegmentRef, error) {
	return BuildEventLogIndexSegmentFromEventLogSegmentsWithOptions(dir, eventRefs, relPath, RestoreETLOptions{})
}

func BuildEventLogIndexSegmentFromEventLogSegmentsWithOptions(dir string, eventRefs []SegmentRef, relPath string, opts RestoreETLOptions) (SegmentRef, error) {
	if dir == "" {
		return SegmentRef{}, errors.New("snapshots: event log index directory is empty")
	}
	eventRefs = append([]SegmentRef(nil), eventRefs...)
	sortSegments(eventRefs)
	if len(eventRefs) == 0 {
		return SegmentRef{}, errors.New("snapshots: event log index requires event-log segments")
	}
	for _, ref := range eventRefs {
		if err := validateEventLogRef(ref); err != nil {
			return SegmentRef{}, err
		}
	}
	fromBlock := eventRefs[0].FromTxNum
	toBlock := eventRefs[len(eventRefs)-1].ToTxNum
	if !eventLogRangeCoveredByRefs(eventRefs, fromBlock, toBlock) {
		return SegmentRef{}, fmt.Errorf("snapshots: event log index range [%d,%d] is not continuously covered by event-log segments", fromBlock, toBlock)
	}
	if relPath == "" {
		relPath = EventLogIndexSegmentPath(fromBlock, toBlock)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetEventLog,
		Kind:      SegmentEventLogIndex,
		FromTxNum: fromBlock,
		ToTxNum:   toBlock,
		Path:      filepath.ToSlash(relPath),
	}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}
	collector, err := etl.NewCollector(opts.collectorOptions())
	if err != nil {
		return SegmentRef{}, err
	}
	defer collector.Close()
	for _, eventRef := range eventRefs {
		seg, err := OpenEventLogSegment(dir, eventRef)
		if err != nil {
			return SegmentRef{}, err
		}
		err = collectEventLogIndexPostingsToETL(seg, collector)
		closeErr := seg.Close()
		if err != nil {
			return SegmentRef{}, err
		}
		if closeErr != nil {
			return SegmentRef{}, closeErr
		}
	}
	postings := newEventLogIndexPostingWriter()
	if _, err := collector.Load(postings); err != nil {
		return SegmentRef{}, err
	}
	addressPostings, topicPostings := postings.postings()
	return writeEventLogIndexSegment(dir, ref, addressPostings, topicPostings)
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

func CheckEventLogIndexSegment(dir string, ref SegmentRef) error {
	if err := validateEventLogIndexRef(ref); err != nil {
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
	header, err := readEventLogIndexHeader(file)
	if err != nil {
		return err
	}
	if err := validateEventLogIndexHeader(ref, header, uint64(stat.Size())); err != nil {
		return err
	}
	if err := checkEventLogSegmentStartLookupIndex(file, ref, "address", header.addressIndexOffset, header.addressIndexLength, eventLogAddressLookupKeySize); err != nil {
		return err
	}
	if err := checkEventLogSegmentStartLookupIndex(file, ref, "topic", header.topicIndexOffset, header.topicIndexLength, eventLogTopicLookupKeySize); err != nil {
		return err
	}
	return nil
}

func verifyEventLogIndexSegmentAgainstEventLogs(dir string, indexRef SegmentRef, eventRefs []SegmentRef) error {
	if err := CheckEventLogIndexSegment(dir, indexRef); err != nil {
		return err
	}
	if !eventLogRangeCoveredByRefs(eventRefs, indexRef.FromTxNum, indexRef.ToTxNum) {
		return fmt.Errorf("snapshots: event-log-index segment %q has no continuous event-log coverage for block range [%d,%d]",
			indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
	}
	expectedAddress := make(map[string][]uint64)
	expectedTopic := make(map[string][]uint64)
	for _, ref := range eventRefs {
		if ref.ToTxNum < indexRef.FromTxNum || ref.FromTxNum > indexRef.ToTxNum {
			continue
		}
		if ref.FromTxNum < indexRef.FromTxNum || ref.ToTxNum > indexRef.ToTxNum {
			return fmt.Errorf("snapshots: event-log segment %q range [%d,%d] crosses event-log-index %q range [%d,%d]",
				ref.Path, ref.FromTxNum, ref.ToTxNum, indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
		}
		seg, err := OpenEventLogSegment(dir, ref)
		if err != nil {
			return err
		}
		collectErr := collectEventLogIndexPostings(seg, expectedAddress, expectedTopic)
		closeErr := seg.Close()
		if collectErr != nil {
			return collectErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	index, err := OpenEventLogIndexSegment(dir, indexRef)
	if err != nil {
		return err
	}
	actualAddress, err := readEventLogLookupIndexMap(index.file, index.header.addressIndexOffset, index.header.addressIndexLength, eventLogAddressLookupKeySize)
	if closeErr := index.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := compareEventLogLookupIndexMaps(indexRef, "address", expectedAddress, actualAddress); err != nil {
		return err
	}
	index, err = OpenEventLogIndexSegment(dir, indexRef)
	if err != nil {
		return err
	}
	actualTopic, err := readEventLogLookupIndexMap(index.file, index.header.topicIndexOffset, index.header.topicIndexLength, eventLogTopicLookupKeySize)
	if closeErr := index.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return compareEventLogLookupIndexMaps(indexRef, "topic", expectedTopic, actualTopic)
}

func verifyEventLogIndexCandidatesForFilter(dir string, indexRef SegmentRef, eventRefs []SegmentRef, fromBlock, toBlock uint64, filter EventLogFilter, candidateStarts []uint64) error {
	if !eventLogRangeCoveredByRefs(eventRefs, indexRef.FromTxNum, indexRef.ToTxNum) {
		return fmt.Errorf("snapshots: event-log-index segment %q has no continuous event-log coverage for block range [%d,%d]",
			indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
	}
	candidates := make(map[uint64]struct{}, len(candidateStarts))
	for _, start := range candidateStarts {
		candidates[start] = struct{}{}
	}
	refsByStart := make(map[uint64]SegmentRef, len(eventRefs))
	for _, ref := range eventRefs {
		refsByStart[ref.FromTxNum] = ref
	}
	for _, start := range candidateStarts {
		ref, ok := refsByStart[start]
		if ok {
			if ref.ToTxNum < fromBlock || ref.FromTxNum > toBlock {
				continue
			}
			if err := CheckEventLogSegment(dir, ref); err != nil {
				return err
			}
			continue
		}
		if start >= fromBlock && start <= toBlock {
			return fmt.Errorf("snapshots: event-log-index %q points to missing event-log segment starting at block %d", indexRef.Path, start)
		}
	}
	for _, ref := range eventRefs {
		if ref.ToTxNum < fromBlock || ref.FromTxNum > toBlock {
			continue
		}
		if ref.FromTxNum < indexRef.FromTxNum || ref.ToTxNum > indexRef.ToTxNum {
			return fmt.Errorf("snapshots: event-log segment %q range [%d,%d] crosses event-log-index %q range [%d,%d]",
				ref.Path, ref.FromTxNum, ref.ToTxNum, indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
		}
		if _, ok := candidates[ref.FromTxNum]; ok {
			continue
		}
		hasMatch, err := eventLogSegmentHasFilterMatch(dir, ref, max(fromBlock, ref.FromTxNum), min(toBlock, ref.ToTxNum), filter)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if hasMatch {
			return fmt.Errorf("snapshots: event-log-index %q missing candidate event-log segment %q for filtered range [%d,%d]",
				indexRef.Path, ref.Path, fromBlock, toBlock)
		}
	}
	return nil
}

func eventLogSegmentHasFilterMatch(dir string, ref SegmentRef, fromBlock, toBlock uint64, filter EventLogFilter) (bool, error) {
	if err := CheckEventLogSegment(dir, ref); err != nil {
		return false, err
	}
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		return false, err
	}
	hasMatch := false
	err = seg.IterateLogs(fromBlock, toBlock, filter, func(EventLog) (bool, error) {
		hasMatch = true
		return false, nil
	})
	closeErr := seg.Close()
	if err != nil {
		return false, err
	}
	if closeErr != nil {
		return false, closeErr
	}
	return hasMatch, nil
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

func OpenEventLogIndexSegment(dir string, ref SegmentRef) (*EventLogIndexSegment, error) {
	if err := validateEventLogIndexRef(ref); err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	header, err := readEventLogIndexHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateEventLogIndexHeader(ref, header, uint64(stat.Size())); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &EventLogIndexSegment{ref: ref, file: file, header: header}, nil
}

func (s *EventLogSegment) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *EventLogIndexSegment) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *EventLogIndexSegment) Stats() (EventLogIndexSegmentStats, error) {
	if s == nil || s.file == nil {
		return EventLogIndexSegmentStats{}, errors.New("snapshots: nil event log index segment")
	}
	stat, err := s.file.Stat()
	if err != nil {
		return EventLogIndexSegmentStats{}, err
	}
	address, err := readEventLogLookupStats(s.file, s.header.addressIndexOffset, s.header.addressIndexLength, eventLogAddressLookupKeySize)
	if err != nil {
		return EventLogIndexSegmentStats{}, err
	}
	topic, err := readEventLogLookupStats(s.file, s.header.topicIndexOffset, s.header.topicIndexLength, eventLogTopicLookupKeySize)
	if err != nil {
		return EventLogIndexSegmentStats{}, err
	}
	return EventLogIndexSegmentStats{
		Path:      s.ref.Path,
		FromBlock: s.ref.FromTxNum,
		ToBlock:   s.ref.ToTxNum,
		Size:      uint64(stat.Size()),
		Address:   address,
		Topic:     topic,
	}, nil
}

func InspectEventLogIndexes(dir string) (*EventLogIndexInspection, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		return nil, err
	}
	out := &EventLogIndexInspection{}
	for _, ref := range eventLogIndexRefs(manifest) {
		seg, err := OpenEventLogIndexSegment(dir, ref)
		if err != nil {
			return nil, err
		}
		stats, statsErr := seg.Stats()
		closeErr := seg.Close()
		if statsErr != nil {
			return nil, statsErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		out.Segments = append(out.Segments, stats)
		out.Address.add(stats.Address)
		out.Topic.add(stats.Topic)
	}
	return out, nil
}

func (s *EventLogIndexLookupStats) add(other EventLogIndexLookupStats) {
	if s == nil {
		return
	}
	if keys, overflow := checkedAdd(s.Keys, other.Keys); !overflow {
		s.Keys = keys
	} else {
		s.Keys = math.MaxUint64
	}
	if postings, overflow := checkedAdd(s.Postings, other.Postings); !overflow {
		s.Postings = postings
	} else {
		s.Postings = math.MaxUint64
	}
	if singleton, overflow := checkedAdd(s.SingletonKeys, other.SingletonKeys); !overflow {
		s.SingletonKeys = singleton
	} else {
		s.SingletonKeys = math.MaxUint64
	}
	if multi, overflow := checkedAdd(s.MultiPostingKeys, other.MultiPostingKeys); !overflow {
		s.MultiPostingKeys = multi
	} else {
		s.MultiPostingKeys = math.MaxUint64
	}
	if other.MaxPostingsPerKey > s.MaxPostingsPerKey {
		s.MaxPostingsPerKey = other.MaxPostingsPerKey
	}
	s.AveragePostingsPerKeyMilli = eventLogAveragePostingsPerKeyMilli(s.Keys, s.Postings)
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

func (s *EventLogIndexSegment) CandidateSegmentStarts(filter EventLogFilter) ([]uint64, bool, error) {
	if s == nil || s.file == nil {
		return nil, false, errors.New("snapshots: nil event log index segment")
	}
	var candidates []uint64
	haveCandidates := false
	if len(filter.Addresses) > 0 {
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
	refs, err := m.eventLogRefsForQuery(manifest, fromBlock, toBlock, filter)
	if err != nil {
		return err
	}
	nextBlock := fromBlock
	for _, ref := range refs {
		if ref.ToTxNum < nextBlock || ref.FromTxNum > toBlock {
			continue
		}
		iterFrom := max(nextBlock, ref.FromTxNum)
		iterTo := min(toBlock, ref.ToTxNum)
		if iterTo < iterFrom {
			continue
		}
		seg, err := OpenEventLogSegment(m.dir, ref)
		if err != nil {
			return err
		}
		stopped := false
		err = seg.IterateLogs(iterFrom, iterTo, filter, func(row EventLog) (bool, error) {
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
		if ref.ToTxNum >= toBlock {
			return nil
		}
		if ref.ToTxNum == ^uint64(0) {
			return nil
		}
		nextBlock = ref.ToTxNum + 1
	}
	return nil
}

func (m *Manager) eventLogRefsForQuery(manifest *Manifest, fromBlock, toBlock uint64, filter EventLogFilter) ([]SegmentRef, error) {
	refs := eventLogRefs(manifest)
	if !eventLogFilterHasLookupKey(filter) {
		return refs, nil
	}
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if indexRef.FromTxNum > fromBlock || indexRef.ToTxNum < toBlock {
			continue
		}
		index, err := OpenEventLogIndexSegment(m.dir, indexRef)
		if err != nil {
			return nil, err
		}
		starts, used, lookupErr := index.CandidateSegmentStarts(filter)
		closeErr := index.Close()
		if lookupErr != nil {
			return nil, lookupErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if !used {
			continue
		}
		if err := verifyEventLogIndexCandidatesForFilter(m.dir, indexRef, refs, fromBlock, toBlock, filter, starts); err != nil {
			return nil, err
		}
		if len(starts) == 0 {
			return nil, nil
		}
		candidates := make(map[uint64]struct{}, len(starts))
		for _, start := range starts {
			candidates[start] = struct{}{}
		}
		out := make([]SegmentRef, 0, len(starts))
		for _, ref := range refs {
			if ref.ToTxNum < fromBlock || ref.FromTxNum > toBlock {
				continue
			}
			if _, ok := candidates[ref.FromTxNum]; ok {
				out = append(out, ref)
			}
		}
		return out, nil
	}
	return refs, nil
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
		if err := CheckEventLogSegment(m.dir, ref); err != nil {
			return false, err
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

func (m *Manager) EventLogIndexedRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	if m == nil {
		return false, nil
	}
	if toBlock < fromBlock {
		return false, fmt.Errorf("snapshots: indexed event log coverage range [%d,%d] is inverted", fromBlock, toBlock)
	}
	eventsCovered, err := m.EventLogRangeCovered(fromBlock, toBlock)
	if err != nil || !eventsCovered {
		return eventsCovered, err
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if indexRef.FromTxNum > fromBlock || indexRef.ToTxNum < toBlock {
			continue
		}
		if err := verifyEventLogIndexSegmentAgainstEventLogs(m.dir, indexRef, eventLogRefs(manifest)); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (m *Manager) EventLogRangeCoveredForFilter(fromBlock, toBlock uint64, filter EventLogFilter) (bool, error) {
	if m == nil {
		return false, nil
	}
	if toBlock < fromBlock {
		return false, fmt.Errorf("snapshots: event log coverage range [%d,%d] is inverted", fromBlock, toBlock)
	}
	if !eventLogFilterHasLookupKey(filter) {
		return m.EventLogRangeCovered(fromBlock, toBlock)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if indexRef.FromTxNum > fromBlock || indexRef.ToTxNum < toBlock {
			continue
		}
		if err := CheckEventLogIndexSegment(m.dir, indexRef); err != nil {
			return false, err
		}
		index, err := OpenEventLogIndexSegment(m.dir, indexRef)
		if err != nil {
			return false, err
		}
		starts, used, lookupErr := index.CandidateSegmentStarts(filter)
		closeErr := index.Close()
		if lookupErr != nil {
			return false, lookupErr
		}
		if closeErr != nil {
			return false, closeErr
		}
		if !used {
			continue
		}
		if err := verifyEventLogIndexCandidatesForFilter(m.dir, indexRef, eventLogRefs(manifest), fromBlock, toBlock, filter, starts); err != nil {
			return false, err
		}
		if len(starts) == 0 {
			return true, nil
		}
		candidateStarts := make(map[uint64]struct{}, len(starts))
		for _, start := range starts {
			candidateStarts[start] = struct{}{}
		}
		seen := make(map[uint64]struct{}, len(starts))
		for _, ref := range eventLogRefs(manifest) {
			if ref.ToTxNum < fromBlock || ref.FromTxNum > toBlock {
				continue
			}
			if _, ok := candidateStarts[ref.FromTxNum]; !ok {
				continue
			}
			if err := CheckEventLogSegment(m.dir, ref); err != nil {
				return false, err
			}
			seen[ref.FromTxNum] = struct{}{}
		}
		for start := range candidateStarts {
			if start < fromBlock || start > toBlock {
				continue
			}
			if _, ok := seen[start]; !ok {
				return false, fmt.Errorf("snapshots: event-log-index %q points to missing event-log segment starting at block %d", indexRef.Path, start)
			}
		}
		return true, nil
	}
	return m.EventLogRangeCovered(fromBlock, toBlock)
}

func collectEventLogRowsToETL(chain *rawdb.ChainDB, fromBlock, toBlock uint64, collector *etl.Collector) (uint64, error) {
	var rowCount uint64
	for blockNum := fromBlock; ; blockNum++ {
		block := rawdb.ReadBlock(chain, blockNum)
		if block == nil {
			return 0, fmt.Errorf("snapshots: missing block %d during event log segment build", blockNum)
		}
		blockHash := block.Hash()
		txs := block.Transactions()
		infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(chain, blockNum)
		if err != nil {
			return 0, err
		}
		if err := rawdb.ValidateTransactionInfosForBlock(blockNum, txs, infos, "event log segment build"); err != nil {
			return 0, err
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
				entry := eventLogIndexEntry{
					blockNum:  blockNum,
					txIndex:   uint64(txIndex),
					logIndex:  logIndex,
					txHash:    txHash,
					blockHash: blockHash,
					address:   eventLogAddress(log.GetAddress()),
				}
				raw, err := proto.Marshal(log)
				if err != nil {
					return 0, err
				}
				if err := collector.Put(eventLogETLKey(entry), eventLogETLValue(entry, raw)); err != nil {
					return 0, err
				}
				rowCount++
				logIndex++
			}
		}
		if blockNum == toBlock {
			break
		}
	}
	return rowCount, nil
}

func writeEventLogSegmentFromETL(dir string, ref SegmentRef, collector *etl.Collector, rowCount uint64) (SegmentRef, error) {
	writer, err := newEventLogSegmentETLWriter(dir, ref, rowCount)
	if err != nil {
		return SegmentRef{}, err
	}
	finalized := false
	defer func() {
		if !finalized {
			writer.abort()
		}
	}()
	if _, err := collector.Load(writer); err != nil {
		return SegmentRef{}, err
	}
	out, err := writer.finalize()
	if err != nil {
		return SegmentRef{}, err
	}
	finalized = true
	return out, nil
}

type eventLogSegmentETLWriter struct {
	dir             string
	ref             SegmentRef
	tmp             *os.File
	tmpName         string
	rowCount        uint64
	rowIndex        uint64
	payloadOffset   uint64
	offset          uint64
	addressPostings map[string][]uint64
	topicPostings   map[string][]uint64
}

func newEventLogSegmentETLWriter(dir string, ref SegmentRef, rowCount uint64) (*eventLogSegmentETLWriter, error) {
	indexBytes, overflow := checkedMul(rowCount, eventLogIndexEntrySize)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log index entries %d overflow size", rowCount)
	}
	payloadOffset, overflow := checkedAdd(eventLogHeaderSize, indexBytes)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log payload offset overflow")
	}
	if payloadOffset > math.MaxInt64 {
		return nil, fmt.Errorf("snapshots: event log payload offset %d overflows int64", payloadOffset)
	}
	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return nil, err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(make([]byte, eventLogHeaderSize)); err != nil {
		return nil, err
	}
	if _, err := tmp.Seek(int64(payloadOffset), io.SeekStart); err != nil {
		return nil, err
	}
	ok = true
	return &eventLogSegmentETLWriter{
		dir:             dir,
		ref:             ref,
		tmp:             tmp,
		tmpName:         tmpName,
		rowCount:        rowCount,
		payloadOffset:   payloadOffset,
		offset:          payloadOffset,
		addressPostings: make(map[string][]uint64),
		topicPostings:   make(map[string][]uint64),
	}, nil
}

func (w *eventLogSegmentETLWriter) Put(key, value []byte) error {
	if w == nil || w.tmp == nil {
		return errors.New("snapshots: nil event log segment ETL writer")
	}
	if w.rowIndex >= w.rowCount {
		return fmt.Errorf("snapshots: event log ETL row %d exceeds expected row count %d", w.rowIndex, w.rowCount)
	}
	entry, err := eventLogEntryFromETLKeyValue(key, value)
	if err != nil {
		return err
	}
	raw := value[eventLogETLValueHeaderSize:]
	if _, err := w.tmp.Write(raw); err != nil {
		return err
	}
	entry.offset = w.offset
	entry.length = uint64(len(raw))
	if err := writeEventLogIndexEntryAt(w.tmp, int64(eventLogHeaderSize+w.rowIndex*eventLogIndexEntrySize), entry); err != nil {
		return err
	}
	addressKey := string(eventLogAddressLookupKey(entry.address))
	w.addressPostings[addressKey] = append(w.addressPostings[addressKey], w.rowIndex)
	var log corepb.TransactionInfo_Log
	if err := proto.Unmarshal(raw, &log); err != nil {
		return fmt.Errorf("snapshots: decode event log ETL row %d: %w", w.rowIndex, err)
	}
	for position, rawTopic := range log.GetTopics() {
		var topic common.Hash
		copy(topic[:], rawTopic)
		key := string(eventLogTopicLookupKey(uint64(position), topic))
		w.topicPostings[key] = append(w.topicPostings[key], w.rowIndex)
	}
	w.offset += uint64(len(raw))
	w.rowIndex++
	return nil
}

func (w *eventLogSegmentETLWriter) Delete(key []byte) error {
	return errors.New("snapshots: event log segment ETL writer does not support deletes")
}

func (w *eventLogSegmentETLWriter) finalize() (SegmentRef, error) {
	if w == nil || w.tmp == nil {
		return SegmentRef{}, errors.New("snapshots: nil event log segment ETL writer")
	}
	if w.rowIndex != w.rowCount {
		return SegmentRef{}, fmt.Errorf("snapshots: event log ETL wrote %d rows, want %d", w.rowIndex, w.rowCount)
	}
	payloadEnd := w.offset
	addressIndexOffset := w.offset
	addressIndexLength, err := writeEventLogLookupIndexAt(w.tmp, addressIndexOffset, eventLogAddressLookupKeySize, w.addressPostings)
	if err != nil {
		return SegmentRef{}, err
	}
	w.offset += addressIndexLength
	topicIndexOffset := w.offset
	topicIndexLength, err := writeEventLogLookupIndexAt(w.tmp, topicIndexOffset, eventLogTopicLookupKeySize, w.topicPostings)
	if err != nil {
		return SegmentRef{}, err
	}
	header := eventLogHeader{
		version:            EventLogSegmentVersion,
		headerSize:         uint64(eventLogHeaderSize),
		fromBlock:          w.ref.FromTxNum,
		toBlock:            w.ref.ToTxNum,
		rowCount:           w.rowCount,
		indexOffset:        uint64(eventLogHeaderSize),
		payloadOffset:      w.payloadOffset,
		payloadEnd:         payloadEnd,
		addressIndexOffset: addressIndexOffset,
		addressIndexLength: addressIndexLength,
		topicIndexOffset:   topicIndexOffset,
		topicIndexLength:   topicIndexLength,
	}
	if err := writeEventLogHeaderAt(w.tmp, header); err != nil {
		return SegmentRef{}, err
	}
	if err := w.tmp.Sync(); err != nil {
		return SegmentRef{}, err
	}
	if err := w.tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	w.tmp = nil
	size, checksum, err := stateDomainChangeBinaryFileMetadata(w.tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	ref := w.ref
	ref.Size = size
	ref.Checksum = checksum
	ref.Path = contentAddressedSnapshotPath(ref.Path, ref.Checksum)
	finalAbs := filepath.Join(w.dir, ref.Path)
	if err := os.Rename(w.tmpName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	w.tmpName = ""
	return ref, nil
}

func (w *eventLogSegmentETLWriter) abort() {
	if w == nil {
		return
	}
	if w.tmp != nil {
		_ = w.tmp.Close()
		w.tmp = nil
	}
	if w.tmpName != "" {
		_ = os.Remove(w.tmpName)
		w.tmpName = ""
	}
}

func eventLogETLKey(entry eventLogIndexEntry) []byte {
	key := make([]byte, eventLogETLKeySize)
	binary.BigEndian.PutUint64(key[0:8], entry.blockNum)
	binary.BigEndian.PutUint64(key[8:16], entry.txIndex)
	binary.BigEndian.PutUint64(key[16:24], entry.logIndex)
	return key
}

func eventLogETLValue(entry eventLogIndexEntry, rawLog []byte) []byte {
	value := make([]byte, eventLogETLValueHeaderSize+len(rawLog))
	copy(value[0:common.HashLength], entry.txHash[:])
	copy(value[common.HashLength:common.HashLength*2], entry.blockHash[:])
	copy(value[common.HashLength*2:eventLogETLValueHeaderSize], entry.address.Bytes())
	copy(value[eventLogETLValueHeaderSize:], rawLog)
	return value
}

func eventLogEntryFromETLKeyValue(key, value []byte) (eventLogIndexEntry, error) {
	if len(key) != eventLogETLKeySize {
		return eventLogIndexEntry{}, fmt.Errorf("snapshots: event log ETL key length %d, want %d", len(key), eventLogETLKeySize)
	}
	if len(value) < eventLogETLValueHeaderSize {
		return eventLogIndexEntry{}, fmt.Errorf("snapshots: event log ETL value length %d smaller than header %d", len(value), eventLogETLValueHeaderSize)
	}
	var entry eventLogIndexEntry
	entry.blockNum = binary.BigEndian.Uint64(key[0:8])
	entry.txIndex = binary.BigEndian.Uint64(key[8:16])
	entry.logIndex = binary.BigEndian.Uint64(key[16:24])
	copy(entry.txHash[:], value[0:common.HashLength])
	copy(entry.blockHash[:], value[common.HashLength:common.HashLength*2])
	entry.address = common.BytesToAddress(value[common.HashLength*2 : eventLogETLValueHeaderSize])
	return entry, nil
}

func collectEventLogIndexPostings(seg *EventLogSegment, addressPostings, topicPostings map[string][]uint64) error {
	if seg == nil || seg.file == nil {
		return errors.New("snapshots: nil event log segment for index build")
	}
	segmentStart := seg.ref.FromTxNum
	for i := uint64(0); i < seg.header.rowCount; i++ {
		entry, err := readEventLogIndexEntryAt(seg.file, eventLogIndexEntryOffset(seg.header, i))
		if err != nil {
			return err
		}
		addressKey := string(eventLogAddressLookupKey(entry.address))
		addressPostings[addressKey] = appendEventLogSegmentPosting(addressPostings[addressKey], segmentStart)
		log, err := seg.readLog(entry)
		if err != nil {
			return err
		}
		for position, rawTopic := range log.GetTopics() {
			var topic common.Hash
			copy(topic[:], rawTopic)
			topicKey := string(eventLogTopicLookupKey(uint64(position), topic))
			topicPostings[topicKey] = appendEventLogSegmentPosting(topicPostings[topicKey], segmentStart)
		}
	}
	return nil
}

func collectEventLogIndexPostingsToETL(seg *EventLogSegment, collector *etl.Collector) error {
	if seg == nil || seg.file == nil {
		return errors.New("snapshots: nil event log segment for index build")
	}
	segmentStart := seg.ref.FromTxNum
	for i := uint64(0); i < seg.header.rowCount; i++ {
		entry, err := readEventLogIndexEntryAt(seg.file, eventLogIndexEntryOffset(seg.header, i))
		if err != nil {
			return err
		}
		if err := collector.Put(eventLogIndexETLKey(eventLogIndexETLKindAddress, eventLogAddressLookupKey(entry.address), segmentStart), nil); err != nil {
			return err
		}
		log, err := seg.readLog(entry)
		if err != nil {
			return err
		}
		for position, rawTopic := range log.GetTopics() {
			var topic common.Hash
			copy(topic[:], rawTopic)
			if err := collector.Put(eventLogIndexETLKey(eventLogIndexETLKindTopic, eventLogTopicLookupKey(uint64(position), topic), segmentStart), nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func appendEventLogSegmentPosting(rows []uint64, segmentStart uint64) []uint64 {
	if len(rows) == 0 || rows[len(rows)-1] != segmentStart {
		return append(rows, segmentStart)
	}
	return rows
}

type eventLogIndexPostingWriter struct {
	addressPostings map[string][]uint64
	topicPostings   map[string][]uint64
}

func newEventLogIndexPostingWriter() *eventLogIndexPostingWriter {
	return &eventLogIndexPostingWriter{
		addressPostings: make(map[string][]uint64),
		topicPostings:   make(map[string][]uint64),
	}
}

func (w *eventLogIndexPostingWriter) Put(key, value []byte) error {
	kind, lookupKey, segmentStart, err := parseEventLogIndexETLKey(key)
	if err != nil {
		return err
	}
	switch kind {
	case eventLogIndexETLKindAddress:
		if len(lookupKey) != eventLogAddressLookupKeySize {
			return fmt.Errorf("snapshots: event-log-index ETL address key length %d, want %d", len(lookupKey), eventLogAddressLookupKeySize)
		}
		mapKey := string(lookupKey)
		w.addressPostings[mapKey] = appendEventLogSegmentPosting(w.addressPostings[mapKey], segmentStart)
	case eventLogIndexETLKindTopic:
		if len(lookupKey) != eventLogTopicLookupKeySize {
			return fmt.Errorf("snapshots: event-log-index ETL topic key length %d, want %d", len(lookupKey), eventLogTopicLookupKeySize)
		}
		mapKey := string(lookupKey)
		w.topicPostings[mapKey] = appendEventLogSegmentPosting(w.topicPostings[mapKey], segmentStart)
	default:
		return fmt.Errorf("snapshots: unknown event-log-index ETL key kind %d", kind)
	}
	return nil
}

func (w *eventLogIndexPostingWriter) Delete(key []byte) error {
	return errors.New("snapshots: event-log-index ETL writer does not support deletes")
}

func (w *eventLogIndexPostingWriter) postings() (map[string][]uint64, map[string][]uint64) {
	if w == nil {
		return nil, nil
	}
	return w.addressPostings, w.topicPostings
}

func eventLogIndexETLKey(kind byte, lookupKey []byte, segmentStart uint64) []byte {
	key := make([]byte, 1+len(lookupKey)+8)
	key[0] = kind
	copy(key[1:], lookupKey)
	binary.BigEndian.PutUint64(key[1+len(lookupKey):], segmentStart)
	return key
}

func parseEventLogIndexETLKey(key []byte) (byte, []byte, uint64, error) {
	if len(key) < 1+8 {
		return 0, nil, 0, fmt.Errorf("snapshots: event-log-index ETL key length %d is too short", len(key))
	}
	lookupLen := len(key) - 1 - 8
	lookupKey := append([]byte(nil), key[1:1+lookupLen]...)
	segmentStart := binary.BigEndian.Uint64(key[1+lookupLen:])
	return key[0], lookupKey, segmentStart, nil
}

func writeEventLogIndexSegment(dir string, ref SegmentRef, addressPostings, topicPostings map[string][]uint64) (SegmentRef, error) {
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

	if _, err := tmp.Write(make([]byte, eventLogIndexHeaderSize)); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offset := uint64(eventLogIndexHeaderSize)
	addressIndexOffset := offset
	addressIndexLength, err := writeEventLogLookupIndexAt(tmp, addressIndexOffset, eventLogAddressLookupKeySize, addressPostings)
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offset += addressIndexLength
	topicIndexOffset := offset
	topicIndexLength, err := writeEventLogLookupIndexAt(tmp, topicIndexOffset, eventLogTopicLookupKeySize, topicPostings)
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	header := eventLogIndexHeader{
		fromBlock:          ref.FromTxNum,
		toBlock:            ref.ToTxNum,
		addressIndexOffset: addressIndexOffset,
		addressIndexLength: addressIndexLength,
		topicIndexOffset:   topicIndexOffset,
		topicIndexLength:   topicIndexLength,
	}
	if err := writeEventLogIndexHeaderAt(tmp, header); err != nil {
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

func eventLogIndexRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	var refs []SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentEventLogIndex && ref.normalizedDataset() == SegmentDatasetEventLog {
			refs = append(refs, ref)
		}
	}
	sortSegments(refs)
	return refs
}

func validateEventLogIndexCompanions(manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	eventRefs := eventLogRefs(manifest)
	indexRefs := eventLogIndexRefs(manifest)
	for _, indexRef := range indexRefs {
		if !eventLogRangeCoveredByRefs(eventRefs, indexRef.FromTxNum, indexRef.ToTxNum) {
			return fmt.Errorf("snapshots: event-log-index segment %q has no continuous event-log coverage for block range [%d,%d]",
				indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
		}
	}
	return nil
}

func eventLogRangeCoveredByRefs(refs []SegmentRef, fromBlock, toBlock uint64) bool {
	if toBlock < fromBlock {
		return false
	}
	next := fromBlock
	for _, ref := range refs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			return false
		}
		if ref.ToTxNum >= toBlock {
			return true
		}
		if ref.ToTxNum == ^uint64(0) {
			return false
		}
		next = ref.ToTxNum + 1
	}
	return false
}

func validateEventLogRef(ref SegmentRef) error {
	if ref.Kind != SegmentEventLog || ref.normalizedDataset() != SegmentDatasetEventLog {
		return fmt.Errorf("snapshots: segment %q is %s/%s, want event-log/event-log", ref.Path, ref.Dataset, ref.Kind)
	}
	return validateSegmentRef(ref)
}

func validateEventLogIndexRef(ref SegmentRef) error {
	if ref.Kind != SegmentEventLogIndex || ref.normalizedDataset() != SegmentDatasetEventLog {
		return fmt.Errorf("snapshots: segment %q is %s/%s, want event-log/event-log-index", ref.Path, ref.Dataset, ref.Kind)
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

func validateEventLogIndexHeader(ref SegmentRef, header eventLogIndexHeader, fileSize uint64) error {
	if header.fromBlock != ref.FromTxNum || header.toBlock != ref.ToTxNum {
		return fmt.Errorf("snapshots: event log index %q range [%d,%d], want [%d,%d]", ref.Path, header.fromBlock, header.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	if header.toBlock < header.fromBlock {
		return fmt.Errorf("snapshots: event log index %q inverted header range [%d,%d]", ref.Path, header.fromBlock, header.toBlock)
	}
	if header.addressIndexOffset != eventLogIndexHeaderSize {
		return fmt.Errorf("snapshots: event log index %q address index offset %d, want %d", ref.Path, header.addressIndexOffset, eventLogIndexHeaderSize)
	}
	addressEnd, overflow := checkedAdd(header.addressIndexOffset, header.addressIndexLength)
	if overflow || addressEnd > fileSize {
		return fmt.Errorf("snapshots: event log index %q address index [%d,%d] outside file size %d", ref.Path, header.addressIndexOffset, addressEnd, fileSize)
	}
	if header.topicIndexOffset != addressEnd {
		return fmt.Errorf("snapshots: event log index %q topic index offset %d, want address index end %d", ref.Path, header.topicIndexOffset, addressEnd)
	}
	topicEnd, overflow := checkedAdd(header.topicIndexOffset, header.topicIndexLength)
	if overflow || topicEnd != fileSize {
		return fmt.Errorf("snapshots: event log index %q topic index end %d, want file size %d", ref.Path, topicEnd, fileSize)
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

func eventLogFilterHasLookupKey(filter EventLogFilter) bool {
	if len(filter.Addresses) > 0 {
		return true
	}
	for _, required := range filter.Topics {
		if len(required) > 0 {
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

func readEventLogLookupIndexMap(file io.ReaderAt, offset, length uint64, keySize int) (map[string][]uint64, error) {
	out := make(map[string][]uint64)
	if offset == 0 || length == 0 {
		return out, nil
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
	entrySize := uint64(keySize + 16)
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
	entryRaw := make([]byte, int(entrySize))
	for i := uint64(0); i < count; i++ {
		entryOffset := dirStart + i*entrySize
		if _, err := file.ReadAt(entryRaw, int64(entryOffset)); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		key := string(append([]byte(nil), entryRaw[:keySize]...))
		postingsOffset := binary.BigEndian.Uint64(entryRaw[keySize : keySize+8])
		postingsCount := binary.BigEndian.Uint64(entryRaw[keySize+8 : keySize+16])
		rows, err := readEventLogLookupPostings(file, offset, length, dirEnd, postingsOffset, postingsCount)
		if err != nil {
			return nil, err
		}
		out[key] = rows
	}
	return out, nil
}

func readEventLogLookupStats(file io.ReaderAt, offset, length uint64, keySize int) (EventLogIndexLookupStats, error) {
	var stats EventLogIndexLookupStats
	if offset == 0 || length == 0 {
		return stats, nil
	}
	if length < eventLogLookupHeaderSize {
		return stats, fmt.Errorf("snapshots: event log lookup length %d smaller than header", length)
	}
	indexEnd, overflow := checkedAdd(offset, length)
	if overflow {
		return stats, fmt.Errorf("snapshots: event log lookup range [%d,+%d] overflows", offset, length)
	}
	count, err := readEventLogUint64At(file, offset)
	if err != nil {
		return stats, err
	}
	entrySize := uint64(keySize + 16)
	dirBytes, overflow := checkedMul(count, entrySize)
	if overflow {
		return stats, fmt.Errorf("snapshots: event log lookup directory overflows")
	}
	dirStart, overflow := checkedAdd(offset, eventLogLookupHeaderSize)
	if overflow {
		return stats, fmt.Errorf("snapshots: event log lookup directory start overflows")
	}
	dirEnd, overflow := checkedAdd(dirStart, dirBytes)
	if overflow || dirEnd > indexEnd {
		return stats, fmt.Errorf("snapshots: event log lookup directory [%d,%d] outside [%d,%d]", dirStart, dirEnd, offset, indexEnd)
	}
	stats.Keys = count
	entryRaw := make([]byte, int(entrySize))
	for i := uint64(0); i < count; i++ {
		entryOffset := dirStart + i*entrySize
		if _, err := file.ReadAt(entryRaw, int64(entryOffset)); err != nil {
			if errors.Is(err, io.EOF) {
				return stats, io.ErrUnexpectedEOF
			}
			return stats, err
		}
		postingsOffset := binary.BigEndian.Uint64(entryRaw[keySize : keySize+8])
		postingsCount := binary.BigEndian.Uint64(entryRaw[keySize+8 : keySize+16])
		postingsBytes, overflow := checkedMul(postingsCount, 8)
		if overflow {
			return stats, fmt.Errorf("snapshots: event log lookup postings overflow")
		}
		postingsEnd, overflow := checkedAdd(postingsOffset, postingsBytes)
		if overflow || postingsOffset < dirEnd || postingsEnd > indexEnd {
			return stats, fmt.Errorf("snapshots: event log lookup postings [%d,%d] outside [%d,%d]", postingsOffset, postingsEnd, offset, indexEnd)
		}
		sum, overflow := checkedAdd(stats.Postings, postingsCount)
		if overflow {
			return stats, fmt.Errorf("snapshots: event log lookup postings count overflow")
		}
		stats.Postings = sum
		if postingsCount > stats.MaxPostingsPerKey {
			stats.MaxPostingsPerKey = postingsCount
		}
		switch {
		case postingsCount == 1:
			stats.SingletonKeys++
		case postingsCount > 1:
			stats.MultiPostingKeys++
		}
	}
	stats.AveragePostingsPerKeyMilli = eventLogAveragePostingsPerKeyMilli(stats.Keys, stats.Postings)
	return stats, nil
}

func eventLogAveragePostingsPerKeyMilli(keys, postings uint64) uint64 {
	if keys == 0 {
		return 0
	}
	hi, lo := bits.Mul64(postings, 1000)
	if hi >= keys {
		return math.MaxUint64
	}
	q, r := bits.Div64(hi, lo, keys)
	roundUpAt := keys/2 + keys%2
	if r >= roundUpAt && q < math.MaxUint64 {
		q++
	}
	return q
}

func compareEventLogLookupIndexMaps(ref SegmentRef, name string, expected, actual map[string][]uint64) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("snapshots: event-log-index %q %s lookup keys %d, want %d", ref.Path, name, len(actual), len(expected))
	}
	for key, want := range expected {
		got, ok := actual[key]
		if !ok {
			return fmt.Errorf("snapshots: event-log-index %q missing %s lookup key %x", ref.Path, name, []byte(key))
		}
		if !equalUint64Slices(got, want) {
			return fmt.Errorf("snapshots: event-log-index %q %s lookup key %x postings %v, want %v", ref.Path, name, []byte(key), got, want)
		}
	}
	return nil
}

func equalUint64Slices(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

func checkEventLogSegmentStartLookupIndex(file io.ReaderAt, ref SegmentRef, name string, offset, length uint64, keySize int) error {
	if length < eventLogLookupHeaderSize {
		return fmt.Errorf("snapshots: event log index %q %s lookup length %d smaller than header", ref.Path, name, length)
	}
	indexEnd, overflow := checkedAdd(offset, length)
	if overflow {
		return fmt.Errorf("snapshots: event log index %q %s lookup range [%d,+%d] overflows", ref.Path, name, offset, length)
	}
	count, err := readEventLogUint64At(file, offset)
	if err != nil {
		return err
	}
	entrySize := uint64(keySize + 16)
	dirBytes, overflow := checkedMul(count, entrySize)
	if overflow {
		return fmt.Errorf("snapshots: event log index %q %s lookup directory overflows", ref.Path, name)
	}
	dirStart, overflow := checkedAdd(offset, eventLogLookupHeaderSize)
	if overflow {
		return fmt.Errorf("snapshots: event log index %q %s lookup directory start overflows", ref.Path, name)
	}
	dirEnd, overflow := checkedAdd(dirStart, dirBytes)
	if overflow || dirEnd > indexEnd {
		return fmt.Errorf("snapshots: event log index %q %s lookup directory [%d,%d] outside [%d,%d]", ref.Path, name, dirStart, dirEnd, offset, indexEnd)
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
			return fmt.Errorf("snapshots: event log index %q %s lookup key %d is not strictly sorted", ref.Path, name, i)
		}
		prevKey = key
		postingsOffset := binary.BigEndian.Uint64(entryRaw[keySize : keySize+8])
		postingsCount := binary.BigEndian.Uint64(entryRaw[keySize+8 : keySize+16])
		rows, err := readEventLogLookupPostings(file, offset, length, dirEnd, postingsOffset, postingsCount)
		if err != nil {
			return err
		}
		var prevStart uint64
		for j, segmentStart := range rows {
			if segmentStart < ref.FromTxNum || segmentStart > ref.ToTxNum {
				return fmt.Errorf("snapshots: event log index %q %s lookup segment start %d outside [%d,%d]", ref.Path, name, segmentStart, ref.FromTxNum, ref.ToTxNum)
			}
			if j > 0 && segmentStart <= prevStart {
				return fmt.Errorf("snapshots: event log index %q %s lookup postings for key %d are not strictly sorted", ref.Path, name, i)
			}
			prevStart = segmentStart
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

func readEventLogIndexHeader(r io.Reader) (eventLogIndexHeader, error) {
	var raw [eventLogIndexHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return eventLogIndexHeader{}, io.ErrUnexpectedEOF
		}
		return eventLogIndexHeader{}, err
	}
	if !bytes.Equal(raw[0:8], eventLogIndexMagic[:]) {
		return eventLogIndexHeader{}, errors.New("snapshots: invalid event log index magic")
	}
	return eventLogIndexHeader{
		fromBlock:          binary.BigEndian.Uint64(raw[8:16]),
		toBlock:            binary.BigEndian.Uint64(raw[16:24]),
		addressIndexOffset: binary.BigEndian.Uint64(raw[24:32]),
		addressIndexLength: binary.BigEndian.Uint64(raw[32:40]),
		topicIndexOffset:   binary.BigEndian.Uint64(raw[40:48]),
		topicIndexLength:   binary.BigEndian.Uint64(raw[48:56]),
	}, nil
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

func writeEventLogIndexHeaderAt(file *os.File, header eventLogIndexHeader) error {
	var raw [eventLogIndexHeaderSize]byte
	copy(raw[0:8], eventLogIndexMagic[:])
	binary.BigEndian.PutUint64(raw[8:16], header.fromBlock)
	binary.BigEndian.PutUint64(raw[16:24], header.toBlock)
	binary.BigEndian.PutUint64(raw[24:32], header.addressIndexOffset)
	binary.BigEndian.PutUint64(raw[32:40], header.addressIndexLength)
	binary.BigEndian.PutUint64(raw[40:48], header.topicIndexOffset)
	binary.BigEndian.PutUint64(raw[48:56], header.topicIndexLength)
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
