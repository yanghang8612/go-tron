package snapshots

import (
	"bufio"
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
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	EventLogSegmentVersion   = 2
	EventLogSegmentV3Version = 3
	EventLogSegmentV4Version = 4
	EventLogIndexVersion     = 2

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
	eventLogMagicV3    = [8]byte{'g', 't', 'e', 'v', 'l', 'g', '3', '\n'}
	eventLogMagicV4    = [8]byte{'g', 't', 'e', 'v', 'l', 'g', '4', '\n'}
	eventLogIndexMagic = [8]byte{'g', 't', 'e', 'v', 'l', 'x', '2', '\n'}
)

type EventLogSegment struct {
	ref         SegmentRef
	file        *os.File
	header      eventLogHeader
	size        uint64
	v3          *eventLogV3Reader
	lifecycleMu sync.Mutex
	cacheMu     sync.Mutex
	closed      bool
	activeReads uint64
	closeErr    error
}

type EventLogIndexSegment struct {
	ref    SegmentRef
	file   *os.File
	header eventLogIndexHeader
	size   uint64
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
	v3                 *eventLogV3Header
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

// eventLogSegmentBuild retains the exact lookup postings already assembled by
// the trusted segment writer. A same-transaction segment-level sidecar can
// collapse those row postings to the new segment start without reopening,
// decoding, validating, and sorting the immutable file a second time.
type eventLogSegmentBuild struct {
	Ref             SegmentRef
	addressPostings map[string][]uint64
	topicPostings   map[string][]uint64
}

// orderedEventLogRows is the bounded in-memory handoff used when the source is
// the canonical ChainDB. That source is already strictly ordered by block,
// transaction, and log ordinal, so sending it through a sorting ETL collector
// only re-encodes, sorts, and decodes the same rows. Lookup postings are built
// from the validated protobuf while it is already live, avoiding a second
// unmarshal in the segment writer.
type orderedEventLogRows struct {
	rows            []orderedEventLogRow
	addressPostings map[string][]uint64
	topicPostings   map[string][]uint64
}

type orderedEventLogRow struct {
	entry eventLogIndexEntry
	raw   []byte
}

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
	build, err := buildEventLogSegmentFromChainWithOptions(chain, dir, relPath, fromBlock, toBlock, opts)
	return build.Ref, err
}

func buildEventLogSegmentFromChainWithOptions(chain *rawdb.ChainDB, dir, relPath string, fromBlock, toBlock uint64, _ RestoreETLOptions) (eventLogSegmentBuild, error) {
	if chain == nil {
		return eventLogSegmentBuild{}, errors.New("snapshots: nil chain database")
	}
	if dir == "" {
		return eventLogSegmentBuild{}, errors.New("snapshots: event log segment directory is empty")
	}
	if toBlock < fromBlock {
		return eventLogSegmentBuild{}, fmt.Errorf("snapshots: event log range [%d,%d] is inverted", fromBlock, toBlock)
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
		return eventLogSegmentBuild{}, err
	}
	rows, err := collectOrderedEventLogRows(chain, fromBlock, toBlock)
	if err != nil {
		return eventLogSegmentBuild{}, err
	}
	return writeEventLogSegmentBuildFromOrderedRows(dir, ref, rows)
}

func BuildEventLogSegmentFromReader(reader rawdb.EventLogReader, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	return BuildEventLogSegmentFromReaderWithOptions(reader, dir, relPath, fromBlock, toBlock, RestoreETLOptions{})
}

func BuildEventLogSegmentFromReaderWithOptions(reader rawdb.EventLogReader, dir, relPath string, fromBlock, toBlock uint64, opts RestoreETLOptions) (SegmentRef, error) {
	build, err := buildEventLogSegmentFromReaderWithOptions(reader, dir, relPath, fromBlock, toBlock, opts)
	return build.Ref, err
}

func buildEventLogSegmentFromReaderWithOptions(reader rawdb.EventLogReader, dir, relPath string, fromBlock, toBlock uint64, opts RestoreETLOptions) (eventLogSegmentBuild, error) {
	if reader == nil {
		return eventLogSegmentBuild{}, errors.New("snapshots: nil event log reader")
	}
	if dir == "" {
		return eventLogSegmentBuild{}, errors.New("snapshots: event log segment directory is empty")
	}
	if toBlock < fromBlock {
		return eventLogSegmentBuild{}, fmt.Errorf("snapshots: event log range [%d,%d] is inverted", fromBlock, toBlock)
	}
	covered, err := reader.EventLogRangeCovered(fromBlock, toBlock)
	if err != nil {
		return eventLogSegmentBuild{}, err
	}
	if !covered {
		return eventLogSegmentBuild{}, fmt.Errorf("snapshots: event log reader does not cover range [%d,%d]", fromBlock, toBlock)
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
		return eventLogSegmentBuild{}, err
	}
	collector, err := etl.NewCollector(opts.collectorOptions())
	if err != nil {
		return eventLogSegmentBuild{}, err
	}
	defer collector.Close()
	rowCount, err := collectEventLogRowsFromReaderToETL(reader, fromBlock, toBlock, collector)
	if err != nil {
		return eventLogSegmentBuild{}, err
	}
	return writeEventLogSegmentBuildFromETL(dir, ref, collector, rowCount)
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
		if err := CheckEventLogSegment(dir, ref); err != nil {
			return SegmentRef{}, fmt.Errorf("snapshots: verify event-log-index source: %w", err)
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
	if header.version == EventLogSegmentV4Version {
		return checkEventLogV3Segment(file, ref, header, fileSize)
	}
	return checkEventLogIndex(file, ref, header, fileSize)
}

func checkEventLogSegmentPayload(dir string, ref SegmentRef) error {
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
	if header.version == EventLogSegmentV4Version {
		return checkEventLogV3Segment(file, ref, header, fileSize)
	}
	_, _, err = checkEventLogPayloadIndex(file, ref, header, fileSize)
	return err
}

func checkCoveredEventLogPayloads(dir string, refs []SegmentRef, fromBlock, toBlock uint64) error {
	next := fromBlock
	for _, ref := range refs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			return fmt.Errorf("snapshots: event log range [%d,%d] is not continuously covered", fromBlock, toBlock)
		}
		if err := checkEventLogSegmentPayload(dir, ref); err != nil {
			return err
		}
		if ref.ToTxNum >= toBlock {
			return nil
		}
		if ref.ToTxNum == ^uint64(0) {
			break
		}
		next = ref.ToTxNum + 1
	}
	return fmt.Errorf("snapshots: event log range [%d,%d] is not continuously covered", fromBlock, toBlock)
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
	fileSize := uint64(stat.Size())
	if err := checkEventLogSegmentStartLookupIndex(file, ref, "address", header.addressIndexOffset, header.addressIndexLength, fileSize, eventLogAddressLookupKeySize); err != nil {
		return err
	}
	if err := checkEventLogSegmentStartLookupIndex(file, ref, "topic", header.topicIndexOffset, header.topicIndexLength, fileSize, eventLogTopicLookupKeySize); err != nil {
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
		if err := collectVerifiedEventLogSegmentPostings(dir, ref, expectedAddress, expectedTopic); err != nil {
			return err
		}
	}
	index, err := OpenEventLogIndexSegment(dir, indexRef)
	if err != nil {
		return err
	}
	actualAddress, err := readEventLogIndexLookupMap(index, index.header.addressIndexOffset, index.header.addressIndexLength, eventLogAddressLookupKeySize)
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
	actualTopic, err := readEventLogIndexLookupMap(index, index.header.topicIndexOffset, index.header.topicIndexLength, eventLogTopicLookupKeySize)
	if closeErr := index.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return compareEventLogLookupIndexMaps(indexRef, "topic", expectedTopic, actualTopic)
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
	segment := &EventLogSegment{ref: ref, file: file, header: header, size: uint64(stat.Size())}
	if header.version == EventLogSegmentV4Version {
		segment.v3, err = openEventLogV3Reader(file, *header.v3, segment.size)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return segment, nil
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
	return &EventLogIndexSegment{ref: ref, file: file, header: header, size: uint64(stat.Size())}, nil
}

func (s *EventLogSegment) Close() error {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	// A callback is allowed to close its own segment. Mark it closed so no
	// new iteration can start, but defer the physical close until the last
	// active reader exits; waiting here would self-deadlock that callback.
	if s.activeReads > 0 {
		return nil
	}
	return s.closeFileLocked()
}

func (s *EventLogSegment) closeFileLocked() error {
	if s.file == nil {
		return s.closeErr
	}
	s.closeErr = s.file.Close()
	s.file = nil
	return s.closeErr
}

func (s *EventLogSegment) beginRead() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.file == nil || s.closed {
		return errors.New("snapshots: closed event log segment")
	}
	s.activeReads++
	return nil
}

func (s *EventLogSegment) endRead() error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.activeReads == 0 {
		return errors.New("snapshots: event log segment reader underflow")
	}
	s.activeReads--
	if s.closed && s.activeReads == 0 {
		return s.closeFileLocked()
	}
	return nil
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
	address, err := readEventLogIndexLookupStats(s, s.header.addressIndexOffset, s.header.addressIndexLength, eventLogAddressLookupKeySize)
	if err != nil {
		return EventLogIndexSegmentStats{}, err
	}
	topic, err := readEventLogIndexLookupStats(s, s.header.topicIndexOffset, s.header.topicIndexLength, eventLogTopicLookupKeySize)
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

func (s *EventLogSegment) IterateLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) (err error) {
	if s == nil {
		return errors.New("snapshots: nil event log segment")
	}
	if err := s.beginRead(); err != nil {
		return err
	}
	defer func() {
		if closeErr := s.endRead(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
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
	return s.iterateLogsFullScan(fromBlock, toBlock, filter, fn)
}

func (s *EventLogSegment) iterateLogsFullScan(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) error {
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
	if s.header.version == EventLogSegmentV4Version {
		return s.iterateEventLogV3FullScan(fromBlock, toBlock, filter, fn)
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
			rows, err := readEventLogIndexLookupRows(s, s.header.addressIndexOffset, s.header.addressIndexLength, eventLogAddressLookupKeySize, eventLogAddressLookupKey(address))
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
			rows, err := readEventLogIndexLookupRows(s, s.header.topicIndexOffset, s.header.topicIndexLength, eventLogTopicLookupKeySize, eventLogTopicLookupKey(uint64(position), topic))
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
	if s.header.version == EventLogSegmentV4Version {
		return s.iterateEventLogV3ByLookup(fromBlock, toBlock, filter, fn)
	}
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
			return true, fmt.Errorf("snapshots: event log lookup row %d block=%d tx=%d log=%d address %x does not match address filter", rowIndex, entry.blockNum, entry.txIndex, entry.logIndex, entry.address)
		}
		log, err := s.readLog(entry)
		if err != nil {
			return true, err
		}
		if !eventLogTopicsMatch(filter.Topics, log.GetTopics()) {
			return true, fmt.Errorf("snapshots: event log lookup row %d block=%d tx=%d log=%d topics do not match topic filter", rowIndex, entry.blockNum, entry.txIndex, entry.logIndex)
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
	if s.header.version == EventLogSegmentV4Version {
		return s.lookupEventLogV3CandidateRows(filter)
	}
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
			rows, err := readEventLogLookupRows(s.file, s.header.addressIndexOffset, s.header.addressIndexLength, s.size, eventLogAddressLookupKey(address))
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
			rows, err := readEventLogLookupRows(s.file, s.header.topicIndexOffset, s.header.topicIndexLength, s.size, eventLogTopicLookupKey(uint64(position), topic))
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
	raw, err := s.readLogPayload(entry)
	if err != nil {
		return nil, err
	}
	var log corepb.TransactionInfo_Log
	if err := proto.Unmarshal(raw, &log); err != nil {
		return nil, err
	}
	if err := validateEventLogPayload(entry, &log, "event log segment read"); err != nil {
		return nil, err
	}
	return &log, nil
}

func (s *EventLogSegment) readLogPayload(entry eventLogIndexEntry) ([]byte, error) {
	if s == nil || s.file == nil {
		return nil, errors.New("snapshots: nil event log segment")
	}
	if err := validateEventLogPayloadEntry(s.ref, s.header, s.size, entry); err != nil {
		return nil, fmt.Errorf("snapshots: event log segment %q: %w", s.ref.Path, err)
	}
	return readEventLogPayloadAt(s.file, entry.offset, entry.length, eventLogPayloadEnd(s.header, s.size))
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
	return m.iterateEventLogRefs(refs, fromBlock, toBlock, filter, true, fn)
}

func (m *Manager) IterateCoveredEventLogs(fromBlock, toBlock uint64, filter EventLogFilter, fn func(EventLog) (bool, error)) (bool, error) {
	if m == nil {
		return false, nil
	}
	if toBlock < fromBlock {
		return false, fmt.Errorf("snapshots: covered event log manager range [%d,%d] is inverted", fromBlock, toBlock)
	}
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return false, err
	}
	refs, covered, forceFullScan, err := m.coveredEventLogRefsForQuery(manifest, fromBlock, toBlock, filter)
	if err != nil || !covered {
		return false, err
	}
	if len(refs) == 0 {
		return true, nil
	}
	if err := m.iterateEventLogRefs(refs, fromBlock, toBlock, filter, !forceFullScan, fn); err != nil {
		return true, err
	}
	return true, nil
}

func (m *Manager) iterateEventLogRefs(refs []SegmentRef, fromBlock, toBlock uint64, filter EventLogFilter, useLookup bool, fn func(EventLog) (bool, error)) error {
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
		iterate := seg.IterateLogs
		if !useLookup {
			iterate = seg.iterateLogsFullScan
		}
		err = iterate(iterFrom, iterTo, filter, func(row EventLog) (bool, error) {
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
	plans, indexed, err := m.eventLogIndexQueryPlans(manifest, refs, fromBlock, toBlock, filter)
	if err != nil {
		return nil, err
	}
	if !indexed {
		return refs, nil
	}
	candidates := eventLogIndexCandidateStartSet(plans)
	if len(candidates) == 0 {
		return nil, nil
	}
	out := make([]SegmentRef, 0, len(candidates))
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

func (m *Manager) coveredEventLogRefsForQuery(manifest *Manifest, fromBlock, toBlock uint64, filter EventLogFilter) ([]SegmentRef, bool, bool, error) {
	refs := eventLogRefs(manifest)
	plans, indexed, err := m.eventLogIndexQueryPlans(manifest, refs, fromBlock, toBlock, filter)
	if err != nil {
		return nil, false, false, err
	}
	if indexed {
		candidates := eventLogIndexCandidateStartSet(plans)
		if len(candidates) == 0 {
			return nil, true, false, nil
		}
		out := make([]SegmentRef, 0, len(candidates))
		for _, ref := range refs {
			if ref.ToTxNum < fromBlock || ref.FromTxNum > toBlock {
				continue
			}
			if _, ok := candidates[ref.FromTxNum]; !ok {
				continue
			}
			out = append(out, ref)
		}
		return out, true, false, nil
	}
	companionsVerified, err := m.verifyEventLogCompanionsForRange(manifest, refs, fromBlock, toBlock)
	if err != nil {
		return nil, false, false, err
	}

	next := fromBlock
	var out []SegmentRef
	forceFullScan := false
	for _, ref := range refs {
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			return nil, false, false, nil
		}
		if !companionsVerified {
			if err := CheckEventLogSegment(m.dir, ref); err != nil {
				if payloadErr := checkEventLogSegmentPayload(m.dir, ref); payloadErr != nil {
					return nil, false, false, payloadErr
				}
				forceFullScan = true
			}
		}
		out = append(out, ref)
		if ref.ToTxNum >= toBlock {
			return out, true, forceFullScan, nil
		}
		if ref.ToTxNum == ^uint64(0) {
			return nil, false, false, nil
		}
		next = ref.ToTxNum + 1
	}
	return nil, false, false, nil
}

type eventLogIndexQueryPlan struct {
	starts []uint64
}

func (m *Manager) eventLogIndexQueryPlans(manifest *Manifest, refs []SegmentRef, fromBlock, toBlock uint64, filter EventLogFilter) ([]eventLogIndexQueryPlan, bool, error) {
	if !eventLogFilterHasLookupKey(filter) {
		return nil, false, nil
	}
	nextBlock := fromBlock
	var plans []eventLogIndexQueryPlan
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if indexRef.ToTxNum < nextBlock {
			continue
		}
		if indexRef.FromTxNum > nextBlock {
			break
		}
		queryFrom := max(nextBlock, indexRef.FromTxNum)
		queryTo := min(toBlock, indexRef.ToTxNum)
		if queryTo < queryFrom {
			continue
		}
		companions := eventLogRefsForIndex(refs, indexRef)
		if _, _, err := m.chainVerificationCache.verifyEventLogIndex(m.dir, indexRef, companions); err != nil {
			return nil, false, err
		}
		m.chainVerificationCache.persistPendingAdvisory()
		index, err := OpenEventLogIndexSegment(m.dir, indexRef)
		if err != nil {
			return nil, false, err
		}
		starts, used, lookupErr := index.CandidateSegmentStarts(filter)
		closeErr := index.Close()
		if lookupErr != nil {
			return nil, false, lookupErr
		}
		if closeErr != nil {
			return nil, false, closeErr
		}
		if !used {
			return nil, false, nil
		}
		plans = append(plans, eventLogIndexQueryPlan{
			starts: starts,
		})
		if queryTo >= toBlock {
			return plans, true, nil
		}
		if queryTo == ^uint64(0) {
			break
		}
		nextBlock = queryTo + 1
	}
	return nil, false, nil
}

// verifyEventLogCompanionsForRange establishes one cached semantic proof for
// every immutable index/event companion set covering the requested range. It
// is also used by unfiltered queries: after the first proof, the query streams
// the selected payload once instead of running CheckEventLogSegment and then
// reopening the same segment for delivery on every request.
func (m *Manager) verifyEventLogCompanionsForRange(manifest *Manifest, refs []SegmentRef, fromBlock, toBlock uint64) (bool, error) {
	next := fromBlock
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if indexRef.ToTxNum < next {
			continue
		}
		if indexRef.FromTxNum > next {
			return false, nil
		}
		companions := eventLogRefsForIndex(refs, indexRef)
		if _, _, err := m.chainVerificationCache.verifyEventLogIndex(m.dir, indexRef, companions); err != nil {
			return false, err
		}
		m.chainVerificationCache.persistPendingAdvisory()
		if indexRef.ToTxNum >= toBlock {
			return true, nil
		}
		if indexRef.ToTxNum == ^uint64(0) {
			return false, nil
		}
		next = indexRef.ToTxNum + 1
	}
	return false, nil
}

func eventLogIndexCandidateStartSet(plans []eventLogIndexQueryPlan) map[uint64]struct{} {
	count := 0
	for _, plan := range plans {
		count += len(plan.starts)
	}
	if count == 0 {
		return nil
	}
	candidates := make(map[uint64]struct{}, count)
	for _, plan := range plans {
		for _, start := range plan.starts {
			candidates[start] = struct{}{}
		}
	}
	return candidates
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
	refs := eventLogRefs(manifest)
	next := fromBlock
	for _, indexRef := range eventLogIndexRefs(manifest) {
		if indexRef.ToTxNum < next {
			continue
		}
		if indexRef.FromTxNum > next {
			return false, nil
		}
		companions := eventLogRefsForIndex(refs, indexRef)
		if _, _, err := m.chainVerificationCache.verifyEventLogIndex(m.dir, indexRef, companions); err != nil {
			return false, err
		}
		m.chainVerificationCache.persistPendingAdvisory()
		if indexRef.ToTxNum >= toBlock {
			return true, nil
		}
		if indexRef.ToTxNum == ^uint64(0) {
			return false, nil
		}
		next = indexRef.ToTxNum + 1
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
	refs := eventLogRefs(manifest)
	_, indexed, err := m.eventLogIndexQueryPlans(manifest, refs, fromBlock, toBlock, filter)
	if err != nil {
		return false, err
	}
	if indexed {
		// The cached companion proof already established that every posting
		// resolves to the exact manifest event-segment set. Planning above only
		// performed immutable-index lookups; no candidate or non-candidate needs
		// another segment audit for a coverage answer.
		return true, nil
	}
	return m.EventLogRangeCovered(fromBlock, toBlock)
}

func collectEventLogRowsToETL(chain *rawdb.ChainDB, fromBlock, toBlock uint64, collector *etl.Collector) (uint64, error) {
	var rowCount uint64
	for blockNum := fromBlock; ; blockNum++ {
		block, ok, err := rawdb.ReadBlockStrict(chain, blockNum)
		if err != nil {
			return 0, err
		}
		if !ok {
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
				if err := putEventLogETLRow(collector, entry, raw); err != nil {
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

func collectOrderedEventLogRows(chain *rawdb.ChainDB, fromBlock, toBlock uint64) (*orderedEventLogRows, error) {
	if chain == nil {
		return nil, errors.New("snapshots: nil chain database")
	}
	out := &orderedEventLogRows{
		addressPostings: make(map[string][]uint64),
		topicPostings:   make(map[string][]uint64),
	}
	for blockNum := fromBlock; ; blockNum++ {
		block, ok, err := rawdb.ReadBlockStrict(chain, blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("snapshots: missing block %d during event log segment build", blockNum)
		}
		blockHash := block.Hash()
		txs := block.Transactions()
		infos, _, err := rawdb.ReadTransactionInfosByBlockStrict(chain, blockNum)
		if err != nil {
			return nil, err
		}
		if err := rawdb.ValidateTransactionInfosForBlock(blockNum, txs, infos, "event log segment build"); err != nil {
			return nil, err
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
				if err := validateEventLogPayload(entry, log, "ordered event log row"); err != nil {
					return nil, err
				}
				raw, err := proto.Marshal(log)
				if err != nil {
					return nil, err
				}
				rowIndex := uint64(len(out.rows))
				addressKey := string(eventLogAddressLookupKey(entry.address))
				out.addressPostings[addressKey] = append(out.addressPostings[addressKey], rowIndex)
				for position, rawTopic := range log.GetTopics() {
					var topic common.Hash
					copy(topic[:], rawTopic)
					key := string(eventLogTopicLookupKey(uint64(position), topic))
					out.topicPostings[key] = append(out.topicPostings[key], rowIndex)
				}
				out.rows = append(out.rows, orderedEventLogRow{entry: entry, raw: raw})
				logIndex++
			}
		}
		if blockNum == toBlock {
			break
		}
	}
	return out, nil
}

func collectEventLogRowsFromReaderToETL(reader rawdb.EventLogReader, fromBlock, toBlock uint64, collector *etl.Collector) (uint64, error) {
	var rowCount uint64
	if err := reader.IterateEventLogs(fromBlock, toBlock, EventLogFilter{}, func(row EventLog) (bool, error) {
		if row.BlockNum < fromBlock || row.BlockNum > toBlock {
			return false, fmt.Errorf("snapshots: event log reader returned block %d outside range [%d,%d]", row.BlockNum, fromBlock, toBlock)
		}
		if row.Log == nil {
			return false, fmt.Errorf("snapshots: event log reader returned nil log at block=%d tx=%d log=%d", row.BlockNum, row.TxIndex, row.LogIndex)
		}
		address, err := validateEventLogReaderRow(row)
		if err != nil {
			return false, err
		}
		entry := eventLogIndexEntry{
			blockNum:  row.BlockNum,
			txIndex:   row.TxIndex,
			logIndex:  row.LogIndex,
			txHash:    row.TxHash,
			blockHash: row.BlockHash,
			address:   address,
		}
		raw, err := proto.Marshal(row.Log)
		if err != nil {
			return false, err
		}
		if err := putEventLogETLRow(collector, entry, raw); err != nil {
			return false, err
		}
		rowCount++
		return true, nil
	}); err != nil {
		return 0, err
	}
	return rowCount, nil
}

func validateEventLogReaderRow(row EventLog) (common.Address, error) {
	entry := eventLogIndexEntry{
		blockNum: row.BlockNum,
		txIndex:  row.TxIndex,
		logIndex: row.LogIndex,
		address:  row.Address,
	}
	if err := validateEventLogPayload(entry, row.Log, "event log reader row"); err != nil {
		return common.Address{}, err
	}
	return row.Address, nil
}

func validateEventLogPayload(entry eventLogIndexEntry, log *corepb.TransactionInfo_Log, context string) error {
	if log == nil {
		return fmt.Errorf("snapshots: nil event log payload during %s at block=%d tx=%d log=%d", context, entry.blockNum, entry.txIndex, entry.logIndex)
	}
	address := eventLogAddress(log.GetAddress())
	if entry.address != address {
		return fmt.Errorf("snapshots: %s address %x does not match payload address %x at block=%d tx=%d log=%d", context, entry.address, address, entry.blockNum, entry.txIndex, entry.logIndex)
	}
	for i, topic := range log.GetTopics() {
		if len(topic) != common.HashLength {
			return fmt.Errorf("snapshots: %s topic %d length %d, want %d at block=%d tx=%d log=%d", context, i, len(topic), common.HashLength, entry.blockNum, entry.txIndex, entry.logIndex)
		}
	}
	return nil
}

func writeEventLogSegmentBuildFromETL(dir string, ref SegmentRef, collector *etl.Collector, rowCount uint64) (eventLogSegmentBuild, error) {
	writer, err := newEventLogSegmentETLWriter(dir, ref, rowCount)
	if err != nil {
		return eventLogSegmentBuild{}, err
	}
	finalized := false
	defer func() {
		if !finalized {
			writer.abort()
		}
	}()
	if _, err := collector.Load(writer); err != nil {
		return eventLogSegmentBuild{}, err
	}
	out, err := writer.finalize()
	if err != nil {
		return eventLogSegmentBuild{}, err
	}
	finalized = true
	return eventLogSegmentBuild{
		Ref:             out,
		addressPostings: writer.addressPostings,
		topicPostings:   writer.topicPostings,
	}, nil
}

func writeEventLogSegmentBuildFromOrderedRows(dir string, ref SegmentRef, rows *orderedEventLogRows) (eventLogSegmentBuild, error) {
	if rows == nil {
		return eventLogSegmentBuild{}, errors.New("snapshots: nil ordered event log rows")
	}
	writer, err := newEventLogSegmentETLWriter(dir, ref, uint64(len(rows.rows)))
	if err != nil {
		return eventLogSegmentBuild{}, err
	}
	writer.addressPostings = rows.addressPostings
	writer.topicPostings = rows.topicPostings
	finalized := false
	defer func() {
		if !finalized {
			writer.abort()
		}
	}()
	for _, row := range rows.rows {
		if err := writer.putValidatedRow(row.entry, row.raw); err != nil {
			return eventLogSegmentBuild{}, err
		}
	}
	out, err := writer.finalize()
	if err != nil {
		return eventLogSegmentBuild{}, err
	}
	finalized = true
	return eventLogSegmentBuild{
		Ref:             out,
		addressPostings: writer.addressPostings,
		topicPostings:   writer.topicPostings,
	}, nil
}

// writeFreshEventLogIndexSegment builds the segment-level lookup sidecar from
// the postings retained by the just-finished event-log writer. This trust is
// intentionally scoped to one build transaction; external/pre-existing event
// segments continue through BuildEventLogIndexSegmentFromEventLogSegments and
// its exhaustive source verification.
func writeFreshEventLogIndexSegment(dir string, build eventLogSegmentBuild, relPath string) (SegmentRef, error) {
	if err := validateEventLogRef(build.Ref); err != nil {
		return SegmentRef{}, err
	}
	if relPath == "" {
		relPath = EventLogIndexSegmentPath(build.Ref.FromTxNum, build.Ref.ToTxNum)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetEventLog,
		Kind:      SegmentEventLogIndex,
		FromTxNum: build.Ref.FromTxNum,
		ToTxNum:   build.Ref.ToTxNum,
		Path:      filepath.ToSlash(relPath),
	}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}
	if err := collapseEventLogPostingsToSegmentStart(build.addressPostings, build.Ref.FromTxNum); err != nil {
		return SegmentRef{}, err
	}
	if err := collapseEventLogPostingsToSegmentStart(build.topicPostings, build.Ref.FromTxNum); err != nil {
		return SegmentRef{}, err
	}
	return writeEventLogIndexSegment(dir, ref, build.addressPostings, build.topicPostings)
}

func collapseEventLogPostingsToSegmentStart(postings map[string][]uint64, segmentStart uint64) error {
	for key, rows := range postings {
		if len(rows) == 0 {
			return fmt.Errorf("snapshots: event log lookup key %x has no row postings", key)
		}
		rows[0] = segmentStart
		postings[key] = rows[:1]
	}
	return nil
}

type eventLogSegmentETLWriter struct {
	dir              string
	ref              SegmentRef
	tmp              *os.File
	payload          *bufio.Writer
	indexBuffer      []byte
	indexRowsFlushed uint64
	tmpName          string
	rowCount         uint64
	rowIndex         uint64
	payloadOffset    uint64
	offset           uint64
	addressPostings  map[string][]uint64
	topicPostings    map[string][]uint64
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
		payload:         bufio.NewWriterSize(tmp, 1<<20),
		indexBuffer:     make([]byte, 0, (1<<20/eventLogIndexEntrySize)*eventLogIndexEntrySize),
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
	var log corepb.TransactionInfo_Log
	if err := proto.Unmarshal(raw, &log); err != nil {
		return fmt.Errorf("snapshots: decode event log ETL row %d: %w", w.rowIndex, err)
	}
	if err := validateEventLogPayload(entry, &log, fmt.Sprintf("event log ETL row %d", w.rowIndex)); err != nil {
		return err
	}
	rowIndex := w.rowIndex
	if err := w.putValidatedRow(entry, raw); err != nil {
		return err
	}
	addressKey := string(eventLogAddressLookupKey(entry.address))
	w.addressPostings[addressKey] = append(w.addressPostings[addressKey], rowIndex)
	for position, rawTopic := range log.GetTopics() {
		var topic common.Hash
		copy(topic[:], rawTopic)
		key := string(eventLogTopicLookupKey(uint64(position), topic))
		w.topicPostings[key] = append(w.topicPostings[key], rowIndex)
	}
	return nil
}

func (w *eventLogSegmentETLWriter) putValidatedRow(entry eventLogIndexEntry, raw []byte) error {
	if w == nil || w.tmp == nil {
		return errors.New("snapshots: nil event log segment writer")
	}
	if w.rowIndex >= w.rowCount {
		return fmt.Errorf("snapshots: event log row %d exceeds expected row count %d", w.rowIndex, w.rowCount)
	}
	if _, err := w.payload.Write(raw); err != nil {
		return err
	}
	entry.offset = w.offset
	entry.length = uint64(len(raw))
	if err := w.bufferIndexEntry(entry); err != nil {
		return err
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
	if err := w.payload.Flush(); err != nil {
		return SegmentRef{}, err
	}
	if err := w.flushIndexEntries(); err != nil {
		return SegmentRef{}, err
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
	if err := syncSnapshotDir(filepath.Dir(finalAbs)); err != nil {
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

func (w *eventLogSegmentETLWriter) bufferIndexEntry(entry eventLogIndexEntry) error {
	if cap(w.indexBuffer)-len(w.indexBuffer) < eventLogIndexEntrySize {
		if err := w.flushIndexEntries(); err != nil {
			return err
		}
	}
	start := len(w.indexBuffer)
	w.indexBuffer = w.indexBuffer[:start+eventLogIndexEntrySize]
	encodeEventLogIndexEntry(w.indexBuffer[start:], entry)
	return nil
}

func (w *eventLogSegmentETLWriter) flushIndexEntries() error {
	if len(w.indexBuffer) == 0 {
		return nil
	}
	offset := int64(eventLogHeaderSize + w.indexRowsFlushed*eventLogIndexEntrySize)
	remaining := w.indexBuffer
	for len(remaining) > 0 {
		n, err := w.tmp.WriteAt(remaining, offset)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		offset += int64(n)
		remaining = remaining[n:]
	}
	w.indexRowsFlushed += uint64(len(w.indexBuffer) / eventLogIndexEntrySize)
	w.indexBuffer = w.indexBuffer[:0]
	return nil
}

func putEventLogETLRow(collector *etl.Collector, entry eventLogIndexEntry, rawLog []byte) error {
	if collector == nil {
		return errors.New("snapshots: nil event log ETL collector")
	}
	return collector.PutEncoded(eventLogETLKeySize, eventLogETLValueHeaderSize+len(rawLog), func(key, value []byte) {
		encodeEventLogETLKey(key, entry)
		encodeEventLogETLValue(value, entry, rawLog)
	})
}

func encodeEventLogETLKey(key []byte, entry eventLogIndexEntry) {
	binary.BigEndian.PutUint64(key[0:8], entry.blockNum)
	binary.BigEndian.PutUint64(key[8:16], entry.txIndex)
	binary.BigEndian.PutUint64(key[16:24], entry.logIndex)
}

func encodeEventLogETLValue(value []byte, entry eventLogIndexEntry, rawLog []byte) {
	copy(value[0:common.HashLength], entry.txHash[:])
	copy(value[common.HashLength:common.HashLength*2], entry.blockHash[:])
	copy(value[common.HashLength*2:eventLogETLValueHeaderSize], entry.address[:])
	copy(value[eventLogETLValueHeaderSize:], rawLog)
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
	if seg.header.version == EventLogSegmentV4Version {
		return seg.iterateLogsFullScan(seg.ref.FromTxNum, seg.ref.ToTxNum, EventLogFilter{}, func(row EventLog) (bool, error) {
			addressKey := string(eventLogAddressLookupKey(row.Address))
			addressPostings[addressKey] = appendEventLogSegmentPosting(addressPostings[addressKey], segmentStart)
			for position, rawTopic := range row.Log.GetTopics() {
				var topic common.Hash
				copy(topic[:], rawTopic)
				topicKey := string(eventLogTopicLookupKey(uint64(position), topic))
				topicPostings[topicKey] = appendEventLogSegmentPosting(topicPostings[topicKey], segmentStart)
			}
			return true, nil
		})
	}
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

// collectVerifiedEventLogSegmentPostings authenticates and validates the event
// segment while deriving the segment-start postings needed by its companion
// index. The former verifier called CheckEventLogSegment and then reopened and
// decoded every payload again through collectEventLogIndexPostings.
func collectVerifiedEventLogSegmentPostings(dir string, ref SegmentRef, addressPostings, topicPostings map[string][]uint64) error {
	if err := validateEventLogRef(ref); err != nil {
		return err
	}
	probeFile, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return err
	}
	probeHeader, headerErr := readEventLogHeader(probeFile)
	_ = probeFile.Close()
	if headerErr != nil {
		return headerErr
	}
	if probeHeader.version == EventLogSegmentV4Version {
		if err := CheckEventLogSegment(dir, ref); err != nil {
			return err
		}
		seg, err := OpenEventLogSegment(dir, ref)
		if err != nil {
			return err
		}
		addressKeys, err := readEventLogV3LookupKeys(seg.file, seg.v3.header.addressIndexOffset, seg.v3.header.addressIndexLength, seg.size, eventLogAddressLookupKeySize)
		if err == nil {
			var topicKeys [][]byte
			topicKeys, err = readEventLogV3LookupKeys(seg.file, seg.v3.header.topicIndexOffset, seg.v3.header.topicIndexLength, seg.size, eventLogTopicLookupKeySize)
			if err == nil {
				for _, key := range addressKeys {
					addressPostings[string(key)] = appendEventLogSegmentPosting(addressPostings[string(key)], ref.FromTxNum)
				}
				for _, key := range topicKeys {
					topicPostings[string(key)] = appendEventLogSegmentPosting(topicPostings[string(key)], ref.FromTxNum)
				}
			}
		}
		closeErr := seg.Close()
		if err != nil {
			return err
		}
		return closeErr
	}
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
	expectedAddress, expectedTopic, err := checkEventLogPayloadIndex(file, ref, header, fileSize)
	if err != nil {
		return err
	}
	if header.version >= 2 {
		if err := checkEventLogAddressLookupIndex(file, ref, header, fileSize); err != nil {
			return err
		}
		if err := checkEventLogTopicLookupIndex(file, ref, header, fileSize); err != nil {
			return err
		}
		if err := checkEventLogSegmentLookupCoverage(file, ref, header, fileSize, expectedAddress, expectedTopic); err != nil {
			return err
		}
	}
	segmentStart := ref.FromTxNum
	for key := range expectedAddress {
		addressPostings[key] = appendEventLogSegmentPosting(addressPostings[key], segmentStart)
	}
	for key := range expectedTopic {
		topicPostings[key] = appendEventLogSegmentPosting(topicPostings[key], segmentStart)
	}
	return nil
}

func collectEventLogIndexPostingsToETL(seg *EventLogSegment, collector *etl.Collector) error {
	if seg == nil || seg.file == nil {
		return errors.New("snapshots: nil event log segment for index build")
	}
	segmentStart := seg.ref.FromTxNum
	if seg.header.version == EventLogSegmentV4Version {
		addressKeys, err := readEventLogV3LookupKeys(seg.file, seg.v3.header.addressIndexOffset, seg.v3.header.addressIndexLength, seg.size, eventLogAddressLookupKeySize)
		if err != nil {
			return err
		}
		topicKeys, err := readEventLogV3LookupKeys(seg.file, seg.v3.header.topicIndexOffset, seg.v3.header.topicIndexLength, seg.size, eventLogTopicLookupKeySize)
		if err != nil {
			return err
		}
		for _, key := range addressKeys {
			if err := collector.PutOwned(eventLogIndexETLKey(eventLogIndexETLKindAddress, key, segmentStart), nil); err != nil {
				return err
			}
		}
		for _, key := range topicKeys {
			if err := collector.PutOwned(eventLogIndexETLKey(eventLogIndexETLKindTopic, key, segmentStart), nil); err != nil {
				return err
			}
		}
		return nil
	}
	for i := uint64(0); i < seg.header.rowCount; i++ {
		entry, err := readEventLogIndexEntryAt(seg.file, eventLogIndexEntryOffset(seg.header, i))
		if err != nil {
			return err
		}
		if err := collector.PutOwned(eventLogIndexETLKey(eventLogIndexETLKindAddress, eventLogAddressLookupKey(entry.address), segmentStart), nil); err != nil {
			return err
		}
		log, err := seg.readLog(entry)
		if err != nil {
			return err
		}
		for position, rawTopic := range log.GetTopics() {
			var topic common.Hash
			copy(topic[:], rawTopic)
			if err := collector.PutOwned(eventLogIndexETLKey(eventLogIndexETLKindTopic, eventLogTopicLookupKey(uint64(position), topic), segmentStart), nil); err != nil {
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
	addressLookup, err := buildEventLogV3Lookup(filepath.Dir(abs), "event-log-index-v2-address", eventLogAddressLookupKeySize, addressPostings)
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	defer addressLookup.close()
	topicLookup, err := buildEventLogV3Lookup(filepath.Dir(abs), "event-log-index-v2-topic", eventLogTopicLookupKeySize, topicPostings)
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	defer topicLookup.close()

	if _, err := tmp.Write(make([]byte, eventLogIndexHeaderSize)); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offset := uint64(eventLogIndexHeaderSize)
	addressIndexOffset := offset
	addressIndexLength := addressLookup.length()
	if err := writeEventLogV3Lookup(tmp, addressIndexOffset, addressLookup); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offset += addressIndexLength
	topicIndexOffset := offset
	topicIndexLength := topicLookup.length()
	if err := writeEventLogV3Lookup(tmp, topicIndexOffset, topicLookup); err != nil {
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
	if err := syncSnapshotDir(filepath.Dir(finalAbs)); err != nil {
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

// eventLogRefsForIndex selects the exact immutable companion set for one
// external index. eventLogRefs already returns range-sorted refs, so binary
// search avoids copying and sorting the entire snapshot catalog on every RPC.
func eventLogRefsForIndex(refs []SegmentRef, indexRef SegmentRef) []SegmentRef {
	start := sort.Search(len(refs), func(i int) bool {
		return refs[i].ToTxNum >= indexRef.FromTxNum
	})
	end := start
	for end < len(refs) && refs[end].FromTxNum <= indexRef.ToTxNum {
		end++
	}
	return refs[start:end]
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
	if header.version == EventLogSegmentV4Version {
		if header.v3 == nil || header.headerSize != eventLogV3HeaderSize {
			return fmt.Errorf("snapshots: event log segment %q has invalid V3 header", ref.Path)
		}
		return validateEventLogV3Header(ref, *header.v3, fileSize)
	}
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
	expectedAddress, expectedTopic, err := checkEventLogPayloadIndex(file, ref, header, fileSize)
	if err != nil {
		return err
	}
	if header.version >= 2 {
		if err := checkEventLogAddressLookupIndex(file, ref, header, fileSize); err != nil {
			return err
		}
		if err := checkEventLogTopicLookupIndex(file, ref, header, fileSize); err != nil {
			return err
		}
		if err := checkEventLogSegmentLookupCoverage(file, ref, header, fileSize, expectedAddress, expectedTopic); err != nil {
			return err
		}
	}
	return nil
}

func checkEventLogPayloadIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader, fileSize uint64) (map[string][]uint64, map[string][]uint64, error) {
	var prev eventLogIndexEntry
	expectedAddress := make(map[string][]uint64)
	expectedTopic := make(map[string][]uint64)
	for i := uint64(0); i < header.rowCount; i++ {
		entry, err := readEventLogIndexEntryAt(file, eventLogIndexEntryOffset(header, i))
		if err != nil {
			return nil, nil, err
		}
		if err := validateEventLogPayloadEntry(ref, header, fileSize, entry); err != nil {
			return nil, nil, fmt.Errorf("snapshots: event log segment %q entry %d: %w", ref.Path, i, err)
		}
		if i > 0 && compareEventLogEntries(prev, entry) >= 0 {
			return nil, nil, fmt.Errorf("snapshots: event log segment %q index entry %d is not strictly sorted", ref.Path, i)
		}
		raw, err := readEventLogPayloadAt(file, entry.offset, entry.length, eventLogPayloadEnd(header, fileSize))
		if err != nil {
			return nil, nil, err
		}
		var log corepb.TransactionInfo_Log
		if err := proto.Unmarshal(raw, &log); err != nil {
			return nil, nil, fmt.Errorf("snapshots: event log segment %q entry %d payload: %w", ref.Path, i, err)
		}
		if err := validateEventLogPayload(entry, &log, fmt.Sprintf("event log segment %q entry %d", ref.Path, i)); err != nil {
			return nil, nil, err
		}
		addressKey := string(eventLogAddressLookupKey(entry.address))
		expectedAddress[addressKey] = append(expectedAddress[addressKey], i)
		for position, rawTopic := range log.GetTopics() {
			var topic common.Hash
			copy(topic[:], rawTopic)
			key := string(eventLogTopicLookupKey(uint64(position), topic))
			expectedTopic[key] = append(expectedTopic[key], i)
		}
		prev = entry
	}
	return expectedAddress, expectedTopic, nil
}

func validateEventLogPayloadEntry(ref SegmentRef, header eventLogHeader, fileSize uint64, entry eventLogIndexEntry) error {
	if entry.blockNum < ref.FromTxNum || entry.blockNum > ref.ToTxNum {
		return fmt.Errorf("entry block %d outside [%d,%d]", entry.blockNum, ref.FromTxNum, ref.ToTxNum)
	}
	if entry.length == 0 {
		return fmt.Errorf("entry block=%d tx=%d log=%d has empty payload", entry.blockNum, entry.txIndex, entry.logIndex)
	}
	payloadEnd := eventLogPayloadEnd(header, fileSize)
	end, overflow := checkedAdd(entry.offset, entry.length)
	if overflow || entry.offset < header.payloadOffset || end > payloadEnd {
		return fmt.Errorf("entry block=%d tx=%d log=%d payload [%d,%d] outside payload section [%d,%d]",
			entry.blockNum, entry.txIndex, entry.logIndex, entry.offset, end, header.payloadOffset, payloadEnd)
	}
	return nil
}

func eventLogPayloadEnd(header eventLogHeader, fileSize uint64) uint64 {
	if header.version >= 2 {
		return header.payloadEnd
	}
	return fileSize
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

func readEventLogLookupRows(file io.ReaderAt, offset, length, fileSize uint64, key []byte) ([]uint64, error) {
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
	if indexEnd > fileSize {
		return nil, fmt.Errorf("snapshots: event log lookup range [%d,%d] outside file size %d", offset, indexEnd, fileSize)
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
		return readEventLogLookupPostings(file, offset, length, fileSize, dirEnd, postingsOffset, postingsCount)
	}
	return nil, nil
}

func readEventLogLookupIndexMap(file io.ReaderAt, offset, length, fileSize uint64, keySize int) (map[string][]uint64, error) {
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
	if indexEnd > fileSize {
		return nil, fmt.Errorf("snapshots: event log lookup range [%d,%d] outside file size %d", offset, indexEnd, fileSize)
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
		rows, err := readEventLogLookupPostings(file, offset, length, fileSize, dirEnd, postingsOffset, postingsCount)
		if err != nil {
			return nil, err
		}
		out[key] = rows
	}
	return out, nil
}

func readEventLogLookupStats(file io.ReaderAt, offset, length, fileSize uint64, keySize int) (EventLogIndexLookupStats, error) {
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
	if indexEnd > fileSize {
		return stats, fmt.Errorf("snapshots: event log lookup range [%d,%d] outside file size %d", offset, indexEnd, fileSize)
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

func readEventLogLookupPostings(file io.ReaderAt, indexOffset, indexLength, fileSize, minPostingsOffset, postingsOffset, postingsCount uint64) ([]uint64, error) {
	indexEnd, overflow := checkedAdd(indexOffset, indexLength)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log lookup index range [%d,+%d] overflows", indexOffset, indexLength)
	}
	if indexEnd > fileSize {
		return nil, fmt.Errorf("snapshots: event log lookup index range [%d,%d] outside file size %d", indexOffset, indexEnd, fileSize)
	}
	postingsBytes, overflow := checkedMul(postingsCount, 8)
	if overflow {
		return nil, fmt.Errorf("snapshots: event log lookup postings overflow")
	}
	postingsEnd, overflow := checkedAdd(postingsOffset, postingsBytes)
	if overflow || postingsOffset < minPostingsOffset || postingsEnd > indexEnd {
		return nil, fmt.Errorf("snapshots: event log lookup postings [%d,%d] outside [%d,%d]", postingsOffset, postingsEnd, indexOffset, indexEnd)
	}
	if postingsEnd > fileSize {
		return nil, fmt.Errorf("snapshots: event log lookup postings [%d,%d] outside file size %d", postingsOffset, postingsEnd, fileSize)
	}
	if postingsEnd > math.MaxInt64 {
		return nil, fmt.Errorf("snapshots: event log lookup postings end %d exceeds int64", postingsEnd)
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

func checkEventLogAddressLookupIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader, fileSize uint64) error {
	return checkEventLogLookupIndex(file, ref, header, "address", header.addressIndexOffset, header.addressIndexLength, fileSize, eventLogAddressLookupKeySize, func(rowIndex uint64, key []byte) error {
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

func checkEventLogTopicLookupIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader, fileSize uint64) error {
	return checkEventLogLookupIndex(file, ref, header, "topic", header.topicIndexOffset, header.topicIndexLength, fileSize, eventLogTopicLookupKeySize, func(rowIndex uint64, key []byte) error {
		position := binary.BigEndian.Uint64(key[:8])
		want := key[8:]
		entry, err := readEventLogIndexEntryAt(file, eventLogIndexEntryOffset(header, rowIndex))
		if err != nil {
			return err
		}
		raw, err := readEventLogPayloadAt(file, entry.offset, entry.length, eventLogPayloadEnd(header, fileSize))
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

func checkEventLogSegmentLookupCoverage(file io.ReaderAt, ref SegmentRef, header eventLogHeader, fileSize uint64, expectedAddress, expectedTopic map[string][]uint64) error {
	actualAddress, err := readEventLogLookupIndexMap(file, header.addressIndexOffset, header.addressIndexLength, fileSize, eventLogAddressLookupKeySize)
	if err != nil {
		return err
	}
	if err := compareEventLogSegmentLookupIndexMaps(ref, "address", expectedAddress, actualAddress); err != nil {
		return err
	}
	actualTopic, err := readEventLogLookupIndexMap(file, header.topicIndexOffset, header.topicIndexLength, fileSize, eventLogTopicLookupKeySize)
	if err != nil {
		return err
	}
	return compareEventLogSegmentLookupIndexMaps(ref, "topic", expectedTopic, actualTopic)
}

func compareEventLogSegmentLookupIndexMaps(ref SegmentRef, name string, expected, actual map[string][]uint64) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("snapshots: event log segment %q %s lookup keys %d, want %d", ref.Path, name, len(actual), len(expected))
	}
	for key, want := range expected {
		got, ok := actual[key]
		if !ok {
			return fmt.Errorf("snapshots: event log segment %q missing %s lookup key %x", ref.Path, name, []byte(key))
		}
		if !equalUint64Slices(got, want) {
			return fmt.Errorf("snapshots: event log segment %q %s lookup key %x postings %v, want %v", ref.Path, name, []byte(key), got, want)
		}
	}
	return nil
}

func checkEventLogLookupIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader, name string, offset, length, fileSize uint64, keySize int, verify func(rowIndex uint64, key []byte) error) error {
	if length < eventLogLookupHeaderSize {
		return fmt.Errorf("snapshots: event log segment %q %s lookup length %d smaller than header", ref.Path, name, length)
	}
	indexEnd, overflow := checkedAdd(offset, length)
	if overflow {
		return fmt.Errorf("snapshots: event log segment %q %s lookup range [%d,+%d] overflows", ref.Path, name, offset, length)
	}
	if indexEnd > fileSize {
		return fmt.Errorf("snapshots: event log segment %q %s lookup range [%d,%d] outside file size %d", ref.Path, name, offset, indexEnd, fileSize)
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
		rows, err := readEventLogLookupPostings(file, offset, length, fileSize, dirEnd, postingsOffset, postingsCount)
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

func checkEventLogSegmentStartLookupIndex(file io.ReaderAt, ref SegmentRef, name string, offset, length, fileSize uint64, keySize int) error {
	err := walkEventLogIndexLookup(file, offset, length, fileSize, keySize, eventLogIndexMaxPosting(ref), func(key []byte, rows []uint64) error {
		if len(key) != keySize {
			return fmt.Errorf("snapshots: event log index %q %s key length %d, want %d", ref.Path, name, len(key), keySize)
		}
		for _, segmentStart := range rows {
			if segmentStart < ref.FromTxNum || segmentStart > ref.ToTxNum {
				return fmt.Errorf("snapshots: event log index %q %s lookup segment start %d outside [%d,%d]", ref.Path, name, segmentStart, ref.FromTxNum, ref.ToTxNum)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("snapshots: event log index %q %s lookup: %w", ref.Path, name, err)
	}
	return nil
}

func eventLogIndexMaxPosting(ref SegmentRef) uint64 {
	if ref.ToTxNum == math.MaxUint64 {
		return math.MaxUint64
	}
	return ref.ToTxNum + 1
}

func readEventLogIndexLookupRows(s *EventLogIndexSegment, offset, length uint64, keySize int, key []byte) ([]uint64, error) {
	return readEventLogV3LookupRows(s.file, offset, length, s.size, keySize, key, eventLogIndexMaxPosting(s.ref))
}

func readEventLogIndexLookupMap(s *EventLogIndexSegment, offset, length uint64, keySize int) (map[string][]uint64, error) {
	return readAllEventLogV3Lookup(s.file, offset, length, s.size, keySize, eventLogIndexMaxPosting(s.ref))
}

func readEventLogIndexLookupStats(s *EventLogIndexSegment, offset, length uint64, keySize int) (EventLogIndexLookupStats, error) {
	var stats EventLogIndexLookupStats
	err := walkEventLogIndexLookup(s.file, offset, length, s.size, keySize, eventLogIndexMaxPosting(s.ref), func(_ []byte, rows []uint64) error {
		stats.Keys++
		count := uint64(len(rows))
		var overflow bool
		stats.Postings, overflow = checkedAdd(stats.Postings, count)
		if overflow {
			return errors.New("snapshots: event log index posting count overflow")
		}
		stats.MaxPostingsPerKey = max(stats.MaxPostingsPerKey, count)
		switch count {
		case 1:
			stats.SingletonKeys++
		case 0:
		default:
			stats.MultiPostingKeys++
		}
		return nil
	})
	if err != nil {
		return EventLogIndexLookupStats{}, err
	}
	stats.AveragePostingsPerKeyMilli = eventLogAveragePostingsPerKeyMilli(stats.Keys, stats.Postings)
	return stats, nil
}

// walkEventLogIndexLookup validates and visits one compact key/posting record
// at a time. It keeps index inspection and startup verification bounded by the
// largest posting list instead of retaining the whole sidecar in memory.
func walkEventLogIndexLookup(file io.ReaderAt, offset, length, size uint64, keySize int, maxRows uint64, visit func([]byte, []uint64) error) error {
	h, err := readEventLogV3LookupV2Header(file, offset, length, size, keySize)
	if err != nil {
		return err
	}
	expectedKeyOff := offset + eventLogV3LookupV2HeaderSize + h.blockDirLen
	var expectedPostingOff, visited uint64
	var previous []byte
	for i := uint64(0); i < h.blockCount; i++ {
		block, err := readEventLogV3LookupV2Block(file, offset, h, i)
		if err != nil {
			return err
		}
		storedLen := uint64(block.dataLen &^ eventLogV3LookupV2StoredRaw)
		if block.dataOff != expectedKeyOff {
			return errors.New("snapshots: compact event log index key blocks are not contiguous")
		}
		expectedKeyOff += storedLen
		records, _, err := readEventLogV3LookupV2Records(file, offset, length, h, block)
		if err != nil {
			return err
		}
		for _, record := range records {
			if len(previous) != 0 && bytes.Compare(previous, record.key) >= 0 {
				return errors.New("snapshots: compact event log index keys are not strictly sorted")
			}
			if record.postingOff != expectedPostingOff {
				return errors.New("snapshots: compact event log index postings are not contiguous")
			}
			expectedPostingOff += record.postingLen
			rows, err := readEventLogV3LookupV2RecordRows(file, offset, length, h, record, maxRows)
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
	if visited != h.keyCount || expectedKeyOff != offset+eventLogV3LookupV2HeaderSize+h.blockDirLen+h.keyDataLen || expectedPostingOff != h.postingDataLen {
		return errors.New("snapshots: compact event log index data coverage mismatch")
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
	case eventLogMagicV3:
		return eventLogHeader{}, errors.New("snapshots: event log V3 is unsupported by the fresh-genesis V4 reader")
	case eventLogMagicV4:
		v3, err := readEventLogV3HeaderRest(r)
		if err != nil {
			return eventLogHeader{}, err
		}
		return eventLogHeader{
			version:    EventLogSegmentV4Version,
			headerSize: eventLogV3HeaderSize,
			fromBlock:  v3.fromBlock,
			toBlock:    v3.toBlock,
			rowCount:   v3.rowCount,
			v3:         &v3,
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
	encodeEventLogIndexEntry(raw[:], entry)
	if _, err := file.WriteAt(raw[:], offset); err != nil {
		return err
	}
	return nil
}

func encodeEventLogIndexEntry(raw []byte, entry eventLogIndexEntry) {
	binary.BigEndian.PutUint64(raw[0:8], entry.blockNum)
	binary.BigEndian.PutUint64(raw[8:16], entry.txIndex)
	binary.BigEndian.PutUint64(raw[16:24], entry.logIndex)
	copy(raw[24:56], entry.txHash[:])
	copy(raw[56:88], entry.blockHash[:])
	copy(raw[88:109], entry.address.Bytes())
	binary.BigEndian.PutUint64(raw[109:117], entry.offset)
	binary.BigEndian.PutUint64(raw[117:125], entry.length)
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

func readEventLogPayloadAt(file io.ReaderAt, offset, length, maxEnd uint64) ([]byte, error) {
	end, overflow := checkedAdd(offset, length)
	if overflow || end > maxEnd {
		return nil, fmt.Errorf("snapshots: event log payload [%d,%d] exceeds segment bound %d", offset, end, maxEnd)
	}
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
