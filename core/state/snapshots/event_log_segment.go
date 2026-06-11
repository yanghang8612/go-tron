package snapshots

import (
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
	EventLogSegmentVersion = 1
	eventLogHeaderSize     = 8 + 8 + 8 + 8 + 8 + 8
	eventLogIndexEntrySize = 8 + 8 + 8 + common.HashLength + common.HashLength + common.AddressLength + 8 + 8
)

var eventLogMagic = [8]byte{'g', 't', 'e', 'v', 'l', 'g', '1', '\n'}

type EventLogSegment struct {
	ref    SegmentRef
	file   *os.File
	header eventLogHeader
}

type eventLogHeader struct {
	fromBlock     uint64
	toBlock       uint64
	rowCount      uint64
	indexOffset   uint64
	payloadOffset uint64
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

type EventLogFilter struct {
	Addresses []common.Address
	Topics    [][]common.Hash
}

type EventLog struct {
	BlockNum  uint64
	TxIndex   uint64
	LogIndex  uint64
	TxHash    common.Hash
	BlockHash common.Hash
	Address   common.Address
	Log       *corepb.TransactionInfo_Log
}

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
	for i := uint64(0); i < s.header.rowCount; i++ {
		entry, err := readEventLogIndexEntryAt(s.file, eventLogIndexEntryOffset(i))
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

	header := eventLogHeader{
		fromBlock:     ref.FromTxNum,
		toBlock:       ref.ToTxNum,
		rowCount:      rowCount,
		indexOffset:   uint64(eventLogHeaderSize),
		payloadOffset: payloadOffset,
	}
	if err := writeEventLogHeader(tmp, header); err != nil {
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
	if header.indexOffset != eventLogHeaderSize {
		return fmt.Errorf("snapshots: event log segment %q index offset %d, want %d", ref.Path, header.indexOffset, eventLogHeaderSize)
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
	return nil
}

func checkEventLogIndex(file io.ReaderAt, ref SegmentRef, header eventLogHeader, fileSize uint64) error {
	var prev eventLogIndexEntry
	for i := uint64(0); i < header.rowCount; i++ {
		entry, err := readEventLogIndexEntryAt(file, eventLogIndexEntryOffset(i))
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

func writeEventLogHeader(w io.Writer, header eventLogHeader) error {
	var raw [eventLogHeaderSize]byte
	copy(raw[0:8], eventLogMagic[:])
	binary.BigEndian.PutUint64(raw[8:16], header.fromBlock)
	binary.BigEndian.PutUint64(raw[16:24], header.toBlock)
	binary.BigEndian.PutUint64(raw[24:32], header.rowCount)
	binary.BigEndian.PutUint64(raw[32:40], header.indexOffset)
	binary.BigEndian.PutUint64(raw[40:48], header.payloadOffset)
	_, err := w.Write(raw[:])
	return err
}

func readEventLogHeader(r io.Reader) (eventLogHeader, error) {
	var raw [eventLogHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return eventLogHeader{}, io.ErrUnexpectedEOF
		}
		return eventLogHeader{}, err
	}
	if got := [8]byte(raw[0:8]); got != eventLogMagic {
		return eventLogHeader{}, errors.New("snapshots: invalid event log segment magic")
	}
	return eventLogHeader{
		fromBlock:     binary.BigEndian.Uint64(raw[8:16]),
		toBlock:       binary.BigEndian.Uint64(raw[16:24]),
		rowCount:      binary.BigEndian.Uint64(raw[24:32]),
		indexOffset:   binary.BigEndian.Uint64(raw[32:40]),
		payloadOffset: binary.BigEndian.Uint64(raw[40:48]),
	}, nil
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

func eventLogIndexEntryOffset(index uint64) int64 {
	return int64(eventLogHeaderSize + index*eventLogIndexEntrySize)
}
