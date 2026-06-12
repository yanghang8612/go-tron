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

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

const (
	BalanceTraceSegmentVersion        = 1
	balanceTraceHeaderSize            = 8 + 8 + 8 + 8 + 8 + 8 + 8
	balanceTraceBlockIndexEntrySize   = 8 + 8 + 8
	balanceTraceAccountIndexEntrySize = common.AddressLength + 8 + 8
)

var balanceTraceMagic = [8]byte{'g', 't', 'b', 't', 'r', 'c', '1', '\n'}

type BalanceTraceSegment struct {
	ref    SegmentRef
	file   *os.File
	header balanceTraceHeader
}

type balanceTraceHeader struct {
	fromBlock          uint64
	toBlock            uint64
	blockCount         uint64
	accountCount       uint64
	blockIndexOffset   uint64
	accountIndexOffset uint64
}

type balanceTraceBlockIndexEntry struct {
	blockNum uint64
	offset   uint64
	length   uint64
}

type balanceTraceAccountIndexEntry struct {
	owner    common.Address
	blockNum uint64
	balance  int64
}

func BalanceTraceSegmentPath(fromBlock, toBlock uint64) string {
	return fmt.Sprintf("trace/balance-trace-%d-%d.seg", fromBlock, toBlock)
}

func BuildBalanceTraceSegmentFromDB(db ethdb.Iteratee, dir, relPath string, fromBlock, toBlock uint64) (SegmentRef, error) {
	if db == nil {
		return SegmentRef{}, errors.New("snapshots: nil database")
	}
	if dir == "" {
		return SegmentRef{}, errors.New("snapshots: balance trace segment directory is empty")
	}
	if toBlock < fromBlock {
		return SegmentRef{}, fmt.Errorf("snapshots: balance trace range [%d,%d] is inverted", fromBlock, toBlock)
	}
	if toBlock > math.MaxInt64 {
		return SegmentRef{}, fmt.Errorf("snapshots: balance trace to-block %d exceeds int64 block number range", toBlock)
	}
	if relPath == "" {
		relPath = BalanceTraceSegmentPath(fromBlock, toBlock)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetBalanceTrace,
		Kind:      SegmentBalanceTrace,
		FromTxNum: fromBlock,
		ToTxNum:   toBlock,
		Path:      filepath.ToSlash(relPath),
	}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}

	blockCount, accountCount, err := countBalanceTraceRows(db, fromBlock, toBlock)
	if err != nil {
		return SegmentRef{}, err
	}
	return writeBalanceTraceSegmentFromDB(db, dir, ref, blockCount, accountCount)
}

func CheckBalanceTraceSegment(dir string, ref SegmentRef) error {
	if err := validateBalanceTraceRef(ref); err != nil {
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
	header, err := readBalanceTraceHeader(file)
	if err != nil {
		return err
	}
	if err := validateBalanceTraceHeader(ref, header, uint64(stat.Size())); err != nil {
		return err
	}
	if err := checkBalanceTraceBlockIndex(file, ref, header); err != nil {
		return err
	}
	return checkBalanceTraceAccountIndex(file, ref, header)
}

func OpenBalanceTraceSegment(dir string, ref SegmentRef) (*BalanceTraceSegment, error) {
	if err := validateBalanceTraceRef(ref); err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	header, err := readBalanceTraceHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateBalanceTraceHeader(ref, header, uint64(stat.Size())); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &BalanceTraceSegment{ref: ref, file: file, header: header}, nil
}

func (s *BalanceTraceSegment) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *BalanceTraceSegment) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	raw, ok, err := s.blockBalanceTraceRaw(blockNum)
	if err != nil || !ok {
		return nil, ok, err
	}
	var trace contractpb.BlockBalanceTrace
	if err := proto.Unmarshal(raw, &trace); err != nil {
		return nil, false, fmt.Errorf("snapshots: decode balance trace block %d: %w", blockNum, err)
	}
	return &trace, true, nil
}

func (s *BalanceTraceSegment) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	if s == nil || s.file == nil || blockNum < 0 || len(owner) != common.AddressLength {
		return 0, 0, false, nil
	}
	queryBlock := uint64(blockNum)
	if queryBlock < s.header.fromBlock {
		return 0, 0, false, nil
	}
	lo, hi := uint64(0), s.header.accountCount
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry, err := readBalanceTraceAccountIndexEntryAt(s.file, balanceTraceAccountEntryOffset(s.header, mid))
		if err != nil {
			return 0, 0, false, err
		}
		if compareBalanceTraceAccountEntry(entry, owner, queryBlock) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= s.header.accountCount {
		return 0, 0, false, nil
	}
	entry, err := readBalanceTraceAccountIndexEntryAt(s.file, balanceTraceAccountEntryOffset(s.header, lo))
	if err != nil {
		return 0, 0, false, err
	}
	if !bytes.Equal(entry.owner[:], owner) || entry.blockNum > queryBlock {
		return 0, 0, false, nil
	}
	return int64(entry.blockNum), entry.balance, true, nil
}

func (m *Manager) BlockBalanceTrace(blockNum int64) (*contractpb.BlockBalanceTrace, bool, error) {
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return nil, false, err
	}
	if blockNum < 0 {
		return nil, false, nil
	}
	queryBlock := uint64(blockNum)
	for _, ref := range balanceTraceRefs(manifest) {
		if queryBlock < ref.FromTxNum || queryBlock > ref.ToTxNum {
			continue
		}
		seg, err := OpenBalanceTraceSegment(m.dir, ref)
		if err != nil {
			return nil, false, err
		}
		trace, ok, lookupErr := seg.BlockBalanceTrace(blockNum)
		closeErr := seg.Close()
		if lookupErr != nil {
			return nil, false, lookupErr
		}
		if closeErr != nil {
			return nil, false, closeErr
		}
		if ok {
			return trace, true, nil
		}
	}
	return nil, false, nil
}

func (m *Manager) AccountTraceAtOrBefore(owner []byte, blockNum int64) (int64, int64, bool, error) {
	manifest, err := m.currentManifest()
	if err != nil || manifest == nil {
		return 0, 0, false, err
	}
	for _, ref := range balanceTraceRefs(manifest) {
		if blockNum >= 0 && uint64(blockNum) < ref.FromTxNum {
			continue
		}
		seg, err := OpenBalanceTraceSegment(m.dir, ref)
		if err != nil {
			return 0, 0, false, err
		}
		traceBlock, balance, ok, lookupErr := seg.AccountTraceAtOrBefore(owner, blockNum)
		closeErr := seg.Close()
		if lookupErr != nil {
			return 0, 0, false, lookupErr
		}
		if closeErr != nil {
			return 0, 0, false, closeErr
		}
		if ok {
			return traceBlock, balance, true, nil
		}
	}
	return 0, 0, false, nil
}

func balanceTraceRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	refs := make([]SegmentRef, 0)
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentBalanceTrace || ref.normalizedDataset() != SegmentDatasetBalanceTrace {
			continue
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].ToTxNum != refs[j].ToTxNum {
			return refs[i].ToTxNum > refs[j].ToTxNum
		}
		if refs[i].FromTxNum != refs[j].FromTxNum {
			return refs[i].FromTxNum > refs[j].FromTxNum
		}
		return refs[i].Path < refs[j].Path
	})
	return refs
}

func countBalanceTraceRows(db ethdb.Iteratee, fromBlock, toBlock uint64) (uint64, uint64, error) {
	var blockCount uint64
	if err := rawdb.IterateBlockBalanceTraceRows(db, int64(fromBlock), int64(toBlock), func(_ int64, _ []byte) (bool, error) {
		blockCount++
		return true, nil
	}); err != nil {
		return 0, 0, err
	}
	var accountCount uint64
	if err := rawdb.IterateAccountTraceRows(db, int64(fromBlock), int64(toBlock), func(owner []byte, _ int64, _ int64) (bool, error) {
		if len(owner) != common.AddressLength {
			return false, fmt.Errorf("snapshots: account trace owner length %d, want %d", len(owner), common.AddressLength)
		}
		accountCount++
		return true, nil
	}); err != nil {
		return 0, 0, err
	}
	return blockCount, accountCount, nil
}

func writeBalanceTraceSegmentFromDB(db ethdb.Iteratee, dir string, ref SegmentRef, blockCount, accountCount uint64) (SegmentRef, error) {
	blockIndexBytes, overflow := checkedMul(blockCount, balanceTraceBlockIndexEntrySize)
	if overflow {
		return SegmentRef{}, fmt.Errorf("snapshots: balance trace block index entries %d overflow size", blockCount)
	}
	blockPayloadOffset, overflow := checkedAdd(balanceTraceHeaderSize, blockIndexBytes)
	if overflow {
		return SegmentRef{}, fmt.Errorf("snapshots: balance trace block payload offset overflow")
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

	header := balanceTraceHeader{
		fromBlock:        ref.FromTxNum,
		toBlock:          ref.ToTxNum,
		blockCount:       blockCount,
		accountCount:     accountCount,
		blockIndexOffset: uint64(balanceTraceHeaderSize),
	}
	if err := writeBalanceTraceHeader(tmp, header); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if _, err := tmp.Seek(int64(blockPayloadOffset), io.SeekStart); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	var blockOrdinal uint64
	var prevBlock uint64
	if err := rawdb.IterateBlockBalanceTraceRows(db, int64(ref.FromTxNum), int64(ref.ToTxNum), func(blockNum int64, raw []byte) (bool, error) {
		if blockNum < 0 {
			return false, fmt.Errorf("snapshots: negative balance trace block %d", blockNum)
		}
		num := uint64(blockNum)
		if blockOrdinal > 0 && num <= prevBlock {
			return false, fmt.Errorf("snapshots: balance trace blocks are not strictly increasing")
		}
		offset, err := tmp.Seek(0, io.SeekCurrent)
		if err != nil {
			return false, err
		}
		if _, err := tmp.Write(raw); err != nil {
			return false, err
		}
		entry := balanceTraceBlockIndexEntry{
			blockNum: num,
			offset:   uint64(offset),
			length:   uint64(len(raw)),
		}
		if err := writeBalanceTraceBlockIndexEntryAt(tmp, header.blockIndexOffset+blockOrdinal*balanceTraceBlockIndexEntrySize, entry); err != nil {
			return false, err
		}
		prevBlock = num
		blockOrdinal++
		return true, nil
	}); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if blockOrdinal != blockCount {
		_ = tmp.Close()
		return SegmentRef{}, fmt.Errorf("snapshots: balance trace block rows %d, want %d", blockOrdinal, blockCount)
	}
	accountOffset, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	header.accountIndexOffset = uint64(accountOffset)
	var accountOrdinal uint64
	var prevAccount *balanceTraceAccountIndexEntry
	if err := rawdb.IterateAccountTraceRows(db, int64(ref.FromTxNum), int64(ref.ToTxNum), func(owner []byte, blockNum int64, balance int64) (bool, error) {
		if len(owner) != common.AddressLength {
			return false, fmt.Errorf("snapshots: account trace owner length %d, want %d", len(owner), common.AddressLength)
		}
		if blockNum < 0 {
			return false, fmt.Errorf("snapshots: negative account trace block %d", blockNum)
		}
		var addr common.Address
		copy(addr[:], owner)
		entry := balanceTraceAccountIndexEntry{
			owner:    addr,
			blockNum: uint64(blockNum),
			balance:  balance,
		}
		if prevAccount != nil && compareBalanceTraceAccountEntries(*prevAccount, entry) >= 0 {
			return false, fmt.Errorf("snapshots: balance trace account entries are not strictly sorted")
		}
		if err := writeBalanceTraceAccountIndexEntry(tmp, entry); err != nil {
			return false, err
		}
		prev := entry
		prevAccount = &prev
		accountOrdinal++
		return true, nil
	}); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if accountOrdinal != accountCount {
		_ = tmp.Close()
		return SegmentRef{}, fmt.Errorf("snapshots: balance trace account rows %d, want %d", accountOrdinal, accountCount)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := writeBalanceTraceHeader(tmp, header); err != nil {
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
	if err := os.MkdirAll(filepath.Dir(finalAbs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	if err := os.Rename(tmpName, finalAbs); err != nil {
		return SegmentRef{}, err
	}
	return ref, nil
}

func validateBalanceTraceRef(ref SegmentRef) error {
	if err := validateSegmentRef(ref); err != nil {
		return err
	}
	if ref.Kind != SegmentBalanceTrace {
		return fmt.Errorf("snapshots: expected %s segment, got %s", SegmentBalanceTrace, ref.Kind)
	}
	if ref.normalizedDataset() != SegmentDatasetBalanceTrace {
		return fmt.Errorf("snapshots: balance trace segment %q dataset %q, want %q", ref.Path, ref.Dataset, SegmentDatasetBalanceTrace)
	}
	return nil
}

func validateBalanceTraceHeader(ref SegmentRef, header balanceTraceHeader, fileSize uint64) error {
	if header.fromBlock != ref.FromTxNum || header.toBlock != ref.ToTxNum {
		return fmt.Errorf("snapshots: balance trace segment %q range [%d,%d], want [%d,%d]",
			ref.Path, header.fromBlock, header.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	if header.blockIndexOffset != balanceTraceHeaderSize {
		return fmt.Errorf("snapshots: balance trace segment %q block index offset %d, want %d", ref.Path, header.blockIndexOffset, balanceTraceHeaderSize)
	}
	blockIndexBytes, overflow := checkedMul(header.blockCount, balanceTraceBlockIndexEntrySize)
	if overflow {
		return fmt.Errorf("snapshots: balance trace segment %q block index size overflow", ref.Path)
	}
	minAccountOffset, overflow := checkedAdd(header.blockIndexOffset, blockIndexBytes)
	if overflow {
		return fmt.Errorf("snapshots: balance trace segment %q account offset overflow", ref.Path)
	}
	if header.accountIndexOffset < minAccountOffset || header.accountIndexOffset > fileSize {
		return fmt.Errorf("snapshots: balance trace segment %q account index offset %d outside [%d,%d]",
			ref.Path, header.accountIndexOffset, minAccountOffset, fileSize)
	}
	accountBytes, overflow := checkedMul(header.accountCount, balanceTraceAccountIndexEntrySize)
	if overflow {
		return fmt.Errorf("snapshots: balance trace segment %q account index size overflow", ref.Path)
	}
	expectedSize, overflow := checkedAdd(header.accountIndexOffset, accountBytes)
	if overflow {
		return fmt.Errorf("snapshots: balance trace segment %q size overflow", ref.Path)
	}
	if expectedSize != fileSize {
		return fmt.Errorf("snapshots: balance trace segment %q size %d, want %d", ref.Path, fileSize, expectedSize)
	}
	return nil
}

func checkBalanceTraceBlockIndex(file io.ReaderAt, ref SegmentRef, header balanceTraceHeader) error {
	var prev uint64
	for i := uint64(0); i < header.blockCount; i++ {
		entry, err := readBalanceTraceBlockIndexEntryAt(file, header.blockIndexOffset+i*balanceTraceBlockIndexEntrySize)
		if err != nil {
			return err
		}
		if entry.blockNum < ref.FromTxNum || entry.blockNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: balance trace segment %q block index entry %d points to block %d outside [%d,%d]",
				ref.Path, i, entry.blockNum, ref.FromTxNum, ref.ToTxNum)
		}
		if i > 0 && entry.blockNum <= prev {
			return fmt.Errorf("snapshots: balance trace segment %q block index is not sorted", ref.Path)
		}
		if err := validateBalanceTracePayloadBounds(header, entry); err != nil {
			return fmt.Errorf("snapshots: balance trace segment %q block %d: %w", ref.Path, entry.blockNum, err)
		}
		raw, err := readBalanceTracePayloadAt(file, entry.offset, entry.length)
		if err != nil {
			return err
		}
		var trace contractpb.BlockBalanceTrace
		if err := proto.Unmarshal(raw, &trace); err != nil {
			return fmt.Errorf("snapshots: decode balance trace block %d: %w", entry.blockNum, err)
		}
		if id := trace.GetBlockIdentifier(); id != nil && id.GetNumber() != int64(entry.blockNum) {
			return fmt.Errorf("snapshots: balance trace segment %q block payload number %d does not match index %d",
				ref.Path, id.GetNumber(), entry.blockNum)
		}
		prev = entry.blockNum
	}
	return nil
}

func checkBalanceTraceAccountIndex(file io.ReaderAt, ref SegmentRef, header balanceTraceHeader) error {
	var prev *balanceTraceAccountIndexEntry
	for i := uint64(0); i < header.accountCount; i++ {
		entry, err := readBalanceTraceAccountIndexEntryAt(file, balanceTraceAccountEntryOffset(header, i))
		if err != nil {
			return err
		}
		if entry.blockNum < ref.FromTxNum || entry.blockNum > ref.ToTxNum {
			return fmt.Errorf("snapshots: balance trace segment %q account entry %d points to block %d outside [%d,%d]",
				ref.Path, i, entry.blockNum, ref.FromTxNum, ref.ToTxNum)
		}
		if prev != nil && compareBalanceTraceAccountEntries(*prev, entry) >= 0 {
			return fmt.Errorf("snapshots: balance trace segment %q account index is not sorted", ref.Path)
		}
		cp := entry
		prev = &cp
	}
	return nil
}

func validateBalanceTracePayloadBounds(header balanceTraceHeader, entry balanceTraceBlockIndexEntry) error {
	blockIndexBytes, overflow := checkedMul(header.blockCount, balanceTraceBlockIndexEntrySize)
	if overflow {
		return fmt.Errorf("block index size overflow")
	}
	payloadStart, overflow := checkedAdd(header.blockIndexOffset, blockIndexBytes)
	if overflow {
		return fmt.Errorf("payload start overflow")
	}
	end, overflow := checkedAdd(entry.offset, entry.length)
	if overflow || entry.offset < payloadStart || end > header.accountIndexOffset {
		return fmt.Errorf("payload [%d,%d] outside payload section [%d,%d]", entry.offset, end, payloadStart, header.accountIndexOffset)
	}
	return nil
}

func writeBalanceTraceHeader(w io.Writer, header balanceTraceHeader) error {
	var raw [balanceTraceHeaderSize]byte
	copy(raw[0:8], balanceTraceMagic[:])
	binary.BigEndian.PutUint64(raw[8:16], header.fromBlock)
	binary.BigEndian.PutUint64(raw[16:24], header.toBlock)
	binary.BigEndian.PutUint64(raw[24:32], header.blockCount)
	binary.BigEndian.PutUint64(raw[32:40], header.accountCount)
	binary.BigEndian.PutUint64(raw[40:48], header.blockIndexOffset)
	binary.BigEndian.PutUint64(raw[48:56], header.accountIndexOffset)
	_, err := w.Write(raw[:])
	return err
}

func readBalanceTraceHeader(r io.Reader) (balanceTraceHeader, error) {
	var raw [balanceTraceHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return balanceTraceHeader{}, io.ErrUnexpectedEOF
		}
		return balanceTraceHeader{}, err
	}
	if headerMagic := [8]byte(raw[0:8]); headerMagic != balanceTraceMagic {
		return balanceTraceHeader{}, errors.New("snapshots: invalid balance trace segment magic")
	}
	return balanceTraceHeader{
		fromBlock:          binary.BigEndian.Uint64(raw[8:16]),
		toBlock:            binary.BigEndian.Uint64(raw[16:24]),
		blockCount:         binary.BigEndian.Uint64(raw[24:32]),
		accountCount:       binary.BigEndian.Uint64(raw[32:40]),
		blockIndexOffset:   binary.BigEndian.Uint64(raw[40:48]),
		accountIndexOffset: binary.BigEndian.Uint64(raw[48:56]),
	}, nil
}

func writeBalanceTraceBlockIndexEntryAt(file *os.File, offset uint64, entry balanceTraceBlockIndexEntry) error {
	var raw [balanceTraceBlockIndexEntrySize]byte
	binary.BigEndian.PutUint64(raw[0:8], entry.blockNum)
	binary.BigEndian.PutUint64(raw[8:16], entry.offset)
	binary.BigEndian.PutUint64(raw[16:24], entry.length)
	_, err := file.WriteAt(raw[:], int64(offset))
	return err
}

func readBalanceTraceBlockIndexEntryAt(file io.ReaderAt, offset uint64) (balanceTraceBlockIndexEntry, error) {
	var raw [balanceTraceBlockIndexEntrySize]byte
	if _, err := file.ReadAt(raw[:], int64(offset)); err != nil {
		return balanceTraceBlockIndexEntry{}, err
	}
	return balanceTraceBlockIndexEntry{
		blockNum: binary.BigEndian.Uint64(raw[0:8]),
		offset:   binary.BigEndian.Uint64(raw[8:16]),
		length:   binary.BigEndian.Uint64(raw[16:24]),
	}, nil
}

func readBalanceTracePayloadAt(file io.ReaderAt, offset, length uint64) ([]byte, error) {
	if length > uint64(^uint(0)>>1) || offset > math.MaxInt64 {
		return nil, fmt.Errorf("snapshots: balance trace payload offset=%d length=%d overflows", offset, length)
	}
	out := make([]byte, int(length))
	if len(out) == 0 {
		return out, nil
	}
	if _, err := file.ReadAt(out, int64(offset)); err != nil {
		return nil, err
	}
	return out, nil
}

func writeBalanceTraceAccountIndexEntry(w io.Writer, entry balanceTraceAccountIndexEntry) error {
	var raw [balanceTraceAccountIndexEntrySize]byte
	copy(raw[0:common.AddressLength], entry.owner[:])
	binary.BigEndian.PutUint64(raw[common.AddressLength:common.AddressLength+8], entry.blockNum)
	binary.BigEndian.PutUint64(raw[common.AddressLength+8:], uint64(entry.balance))
	_, err := w.Write(raw[:])
	return err
}

func readBalanceTraceAccountIndexEntryAt(file io.ReaderAt, offset uint64) (balanceTraceAccountIndexEntry, error) {
	var raw [balanceTraceAccountIndexEntrySize]byte
	if _, err := file.ReadAt(raw[:], int64(offset)); err != nil {
		return balanceTraceAccountIndexEntry{}, err
	}
	var owner common.Address
	copy(owner[:], raw[0:common.AddressLength])
	return balanceTraceAccountIndexEntry{
		owner:    owner,
		blockNum: binary.BigEndian.Uint64(raw[common.AddressLength : common.AddressLength+8]),
		balance:  int64(binary.BigEndian.Uint64(raw[common.AddressLength+8:])),
	}, nil
}

func balanceTraceAccountEntryOffset(header balanceTraceHeader, ordinal uint64) uint64 {
	return header.accountIndexOffset + ordinal*balanceTraceAccountIndexEntrySize
}

func compareBalanceTraceAccountEntries(a, b balanceTraceAccountIndexEntry) int {
	if cmp := bytes.Compare(a.owner[:], b.owner[:]); cmp != 0 {
		return cmp
	}
	return compareBalanceTraceUint64(balanceTraceAccountBlockSuffix(a.blockNum), balanceTraceAccountBlockSuffix(b.blockNum))
}

func compareBalanceTraceAccountEntry(entry balanceTraceAccountIndexEntry, owner []byte, blockNum uint64) int {
	if cmp := bytes.Compare(entry.owner[:], owner); cmp != 0 {
		return cmp
	}
	return compareBalanceTraceUint64(balanceTraceAccountBlockSuffix(entry.blockNum), balanceTraceAccountBlockSuffix(blockNum))
}

func compareBalanceTraceUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func balanceTraceAccountBlockSuffix(blockNum uint64) uint64 {
	const longMax uint64 = 0x7FFFFFFFFFFFFFFF
	return blockNum ^ longMax
}
