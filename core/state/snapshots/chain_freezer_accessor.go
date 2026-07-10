package snapshots

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/golang/snappy"
)

const (
	ChainFreezerAccessorSegmentVersion = 1
	chainFreezerAccessorHeaderSize     = 8 + 8 + 8 + 8
	chainFreezerAccessorEntrySize      = 8
)

var chainFreezerAccessorMagic = [8]byte{'g', 't', 'c', 'f', 'a', 'x', '1', '\n'}

type ChainFreezerAccessorSegment struct {
	ref    SegmentRef
	file   *os.File
	header chainFreezerAccessorHeader
}

type chainFreezerAccessorHeader struct {
	fromBlock uint64
	toBlock   uint64
	count     uint64
}

func ChainFreezerAccessorSegmentPath(fromBlock, toBlock uint64) string {
	return fmt.Sprintf("chain/freezer-accessor-%d-%d.idx", fromBlock, toBlock)
}

func BuildChainFreezerAccessorSegmentFromChainFreezerSegment(dir string, freezerRef SegmentRef, relPath string) (SegmentRef, error) {
	if err := validateSegmentRef(freezerRef); err != nil {
		return SegmentRef{}, err
	}
	if freezerRef.Kind != SegmentChainFreezer {
		return SegmentRef{}, fmt.Errorf("snapshots: chain-freezer accessor requires %s segment, got %s", SegmentChainFreezer, freezerRef.Kind)
	}
	// The offsets are only useful when they describe a semantically valid
	// freezer segment. Do this before publishing a derived sidecar.
	if err := CheckChainFreezerSegment(dir, freezerRef); err != nil {
		return SegmentRef{}, fmt.Errorf("snapshots: verify chain-freezer accessor source: %w", err)
	}
	if relPath == "" {
		relPath = ChainFreezerAccessorSegmentPath(freezerRef.FromTxNum, freezerRef.ToTxNum)
	}
	ref := SegmentRef{
		Dataset:   SegmentDatasetChainFreezer,
		Kind:      SegmentChainFreezerAccessor,
		FromTxNum: freezerRef.FromTxNum,
		ToTxNum:   freezerRef.ToTxNum,
		Path:      relPath,
	}
	if err := validateSegmentRef(ref); err != nil {
		return SegmentRef{}, err
	}
	offsets, err := chainFreezerRowOffsets(dir, freezerRef)
	if err != nil {
		return SegmentRef{}, err
	}
	expectedRows, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		return SegmentRef{}, err
	}
	if uint64(len(offsets)) != expectedRows {
		return SegmentRef{}, fmt.Errorf("snapshots: chain-freezer accessor offsets %d, want %d", len(offsets), expectedRows)
	}
	return writeChainFreezerAccessorSegment(dir, ref, offsets)
}

func CheckChainFreezerAccessorSegment(dir string, ref SegmentRef) error {
	if err := validateSegmentRef(ref); err != nil {
		return err
	}
	if ref.Kind != SegmentChainFreezerAccessor {
		return fmt.Errorf("snapshots: chain-freezer accessor checker got %s segment %q", ref.Kind, ref.Path)
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
	header, err := readChainFreezerAccessorHeader(file)
	if err != nil {
		return err
	}
	if header.fromBlock != ref.FromTxNum || header.toBlock != ref.ToTxNum {
		return fmt.Errorf("snapshots: chain-freezer accessor segment %q range [%d,%d], want [%d,%d]",
			ref.Path, header.fromBlock, header.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	expectedRows, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		return err
	}
	if header.count != expectedRows {
		return fmt.Errorf("snapshots: chain-freezer accessor segment %q entries %d, want %d", ref.Path, header.count, expectedRows)
	}
	expectedSize, err := chainFreezerAccessorExpectedSize(header)
	if err != nil {
		return err
	}
	if uint64(stat.Size()) != expectedSize {
		return fmt.Errorf("snapshots: chain-freezer accessor segment %q size %d, want %d", ref.Path, stat.Size(), expectedSize)
	}
	return checkChainFreezerAccessorOffsets(file, header, ref)
}

func OpenChainFreezerAccessorSegment(dir string, ref SegmentRef) (*ChainFreezerAccessorSegment, error) {
	if err := validateSegmentRef(ref); err != nil {
		return nil, err
	}
	if ref.Kind != SegmentChainFreezerAccessor {
		return nil, fmt.Errorf("snapshots: expected %s segment, got %s", SegmentChainFreezerAccessor, ref.Kind)
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	header, err := readChainFreezerAccessorHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if header.fromBlock != ref.FromTxNum || header.toBlock != ref.ToTxNum {
		_ = file.Close()
		return nil, fmt.Errorf("snapshots: chain-freezer accessor segment %q range [%d,%d], want [%d,%d]",
			ref.Path, header.fromBlock, header.toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	expectedRows, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if header.count != expectedRows {
		_ = file.Close()
		return nil, fmt.Errorf("snapshots: chain-freezer accessor segment %q entries %d, want %d", ref.Path, header.count, expectedRows)
	}
	expectedSize, err := chainFreezerAccessorExpectedSize(header)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if uint64(stat.Size()) != expectedSize {
		_ = file.Close()
		return nil, fmt.Errorf("snapshots: chain-freezer accessor segment %q size %d, want %d", ref.Path, stat.Size(), expectedSize)
	}
	return &ChainFreezerAccessorSegment{ref: ref, file: file, header: header}, nil
}

func (s *ChainFreezerAccessorSegment) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *ChainFreezerAccessorSegment) RowOffset(blockNum uint64) (uint64, bool, error) {
	if s == nil || s.file == nil || blockNum < s.header.fromBlock || blockNum > s.header.toBlock {
		return 0, false, nil
	}
	ordinal := blockNum - s.header.fromBlock
	var raw [chainFreezerAccessorEntrySize]byte
	if _, err := s.file.ReadAt(raw[:], int64(chainFreezerAccessorEntryOffset(ordinal))); err != nil {
		return 0, false, err
	}
	return binary.BigEndian.Uint64(raw[:]), true, nil
}

func readChainFreezerSegmentRowWithAccessor(dir string, freezerRef, accessorRef SegmentRef, blockNum uint64) (chainFreezerRow, bool, error) {
	accessor, err := OpenChainFreezerAccessorSegment(dir, accessorRef)
	if err != nil {
		return chainFreezerRow{}, false, err
	}
	offset, ok, lookupErr := accessor.RowOffset(blockNum)
	closeErr := accessor.Close()
	if lookupErr != nil {
		return chainFreezerRow{}, false, lookupErr
	}
	if closeErr != nil {
		return chainFreezerRow{}, false, closeErr
	}
	if !ok {
		return chainFreezerRow{}, false, nil
	}
	file, err := os.Open(filepath.Join(dir, freezerRef.Path))
	if err != nil {
		return chainFreezerRow{}, false, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return chainFreezerRow{}, false, err
	}
	row, err := readChainFreezerSegmentRowAt(file, offset, blockNum, uint64(stat.Size()))
	if err != nil {
		return chainFreezerRow{}, false, err
	}
	if _, err := validateChainFreezerRowPayload(row, "chain-freezer accessor read"); err != nil {
		return chainFreezerRow{}, false, err
	}
	return row, true, nil
}

func VerifyChainFreezerAccessorSegmentAgainstChainFreezer(dir string, accessorRef, freezerRef SegmentRef) error {
	if accessorRef.Kind != SegmentChainFreezerAccessor || freezerRef.Kind != SegmentChainFreezer {
		return fmt.Errorf("snapshots: chain-freezer accessor verification requires %s against %s, got %s against %s",
			SegmentChainFreezerAccessor, SegmentChainFreezer, accessorRef.Kind, freezerRef.Kind)
	}
	if accessorRef.FromTxNum != freezerRef.FromTxNum || accessorRef.ToTxNum != freezerRef.ToTxNum {
		return fmt.Errorf("snapshots: chain-freezer accessor range [%d,%d] does not match chain-freezer range [%d,%d]",
			accessorRef.FromTxNum, accessorRef.ToTxNum, freezerRef.FromTxNum, freezerRef.ToTxNum)
	}
	if err := CheckChainFreezerAccessorSegment(dir, accessorRef); err != nil {
		return err
	}
	if err := CheckChainFreezerSegment(dir, freezerRef); err != nil {
		return err
	}
	accessor, err := OpenChainFreezerAccessorSegment(dir, accessorRef)
	if err != nil {
		return err
	}
	defer accessor.Close()
	file, err := os.Open(filepath.Join(dir, freezerRef.Path))
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := uint64(stat.Size())
	return iterateChainFreezerSegmentRows(dir, freezerRef, func(row chainFreezerRow) error {
		if _, err := validateChainFreezerRowPayload(row, "chain-freezer accessor verification"); err != nil {
			return err
		}
		offset, ok, err := accessor.RowOffset(row.blockNum)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("snapshots: chain-freezer accessor segment %q missing block %d", accessorRef.Path, row.blockNum)
		}
		got, err := readChainFreezerSegmentRowAt(file, offset, row.blockNum, fileSize)
		if err != nil {
			return err
		}
		if string(got.blockRaw) != string(row.blockRaw) ||
			string(got.txInfosRaw) != string(row.txInfosRaw) ||
			string(got.stateRootRaw) != string(row.stateRootRaw) {
			return fmt.Errorf("snapshots: chain-freezer accessor segment %q offset for block %d points at different row", accessorRef.Path, row.blockNum)
		}
		return nil
	})
}

func chainFreezerAccessorRefForFreezer(manifest *Manifest, freezerRef SegmentRef) (SegmentRef, bool) {
	if manifest == nil || freezerRef.Kind != SegmentChainFreezer {
		return SegmentRef{}, false
	}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainFreezerAccessor ||
			ref.normalizedDataset() != SegmentDatasetChainFreezer ||
			ref.Domain != freezerRef.Domain ||
			ref.FromTxNum != freezerRef.FromTxNum ||
			ref.ToTxNum != freezerRef.ToTxNum {
			continue
		}
		return ref, true
	}
	return SegmentRef{}, false
}

func chainFreezerAccessorRefs(manifest *Manifest) []SegmentRef {
	if manifest == nil {
		return nil
	}
	refs := make([]SegmentRef, 0)
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentChainFreezerAccessor || ref.normalizedDataset() != SegmentDatasetChainFreezer {
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

func writeChainFreezerAccessorSegment(dir string, ref SegmentRef, offsets []uint64) (SegmentRef, error) {
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

	hash := sha256.New()
	counter := &countingWriter{w: io.MultiWriter(tmp, hash)}
	buf := bufio.NewWriter(counter)
	if err := writeChainFreezerAccessorHeader(buf, chainFreezerAccessorHeader{
		fromBlock: ref.FromTxNum,
		toBlock:   ref.ToTxNum,
		count:     uint64(len(offsets)),
	}); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	for _, offset := range offsets {
		if err := writeChainFreezerAccessorOffset(buf, offset); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
	}
	if err := buf.Flush(); err != nil {
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
	ref.Size = counter.n
	ref.Checksum = "sha256:" + hex.EncodeToString(hash.Sum(nil))
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

func chainFreezerRowOffsets(dir string, ref SegmentRef) ([]uint64, error) {
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := uint64(stat.Size())
	fromBlock, toBlock, count, err := readChainFreezerHeader(file)
	if err != nil {
		return nil, err
	}
	if fromBlock != ref.FromTxNum || toBlock != ref.ToTxNum {
		return nil, fmt.Errorf("snapshots: chain-freezer segment %q range [%d,%d], want [%d,%d]", ref.Path, fromBlock, toBlock, ref.FromTxNum, ref.ToTxNum)
	}
	expectedRows, err := chainFreezerRowCount(ref.FromTxNum, ref.ToTxNum)
	if err != nil {
		return nil, err
	}
	if count != expectedRows {
		return nil, fmt.Errorf("snapshots: chain-freezer segment %q rows %d, want %d", ref.Path, count, expectedRows)
	}
	offsets := make([]uint64, 0, count)
	offset := uint64(chainFreezerHeaderSize)
	for i := uint64(0); i < count; i++ {
		offsets = append(offsets, offset)
		var next uint64
		if next, err = skipChainFreezerBytesAt(file, offset, fileSize); err != nil {
			return nil, fmt.Errorf("snapshots: read chain-freezer block %d body for accessor: %w", fromBlock+i, err)
		}
		if next, err = skipChainFreezerBytesAt(file, next, fileSize); err != nil {
			return nil, fmt.Errorf("snapshots: read chain-freezer block %d tx infos for accessor: %w", fromBlock+i, err)
		}
		if next, err = skipChainFreezerBytesAt(file, next, fileSize); err != nil {
			return nil, fmt.Errorf("snapshots: read chain-freezer block %d state root for accessor: %w", fromBlock+i, err)
		}
		offset = next
	}
	if offset != uint64(stat.Size()) {
		return nil, fmt.Errorf("snapshots: chain-freezer segment %q has trailing bytes", ref.Path)
	}
	return offsets, nil
}

func readChainFreezerSegmentRowAt(file io.ReaderAt, offset, blockNum, fileSize uint64) (chainFreezerRow, error) {
	row, _, err := readChainFreezerSegmentRowAtWithNext(file, offset, blockNum, fileSize)
	return row, err
}

func readChainFreezerSegmentRowAtWithNext(file io.ReaderAt, offset, blockNum, fileSize uint64) (chainFreezerRow, uint64, error) {
	blockRaw, next, err := readChainFreezerBytesAt(file, offset, fileSize)
	if err != nil {
		return chainFreezerRow{}, 0, fmt.Errorf("snapshots: read chain-freezer block %d body at offset %d: %w", blockNum, offset, err)
	}
	txInfosRaw, next, err := readChainFreezerBytesAt(file, next, fileSize)
	if err != nil {
		return chainFreezerRow{}, 0, fmt.Errorf("snapshots: read chain-freezer block %d tx infos at offset %d: %w", blockNum, next, err)
	}
	stateRootRaw, next, err := readChainFreezerBytesAt(file, next, fileSize)
	if err != nil {
		return chainFreezerRow{}, 0, fmt.Errorf("snapshots: read chain-freezer block %d state root at offset %d: %w", blockNum, next, err)
	}
	return chainFreezerRow{
		blockNum:     blockNum,
		blockRaw:     blockRaw,
		txInfosRaw:   txInfosRaw,
		stateRootRaw: stateRootRaw,
	}, next, nil
}

func readChainFreezerBytesAt(file io.ReaderAt, offset, fileSize uint64) ([]byte, uint64, error) {
	encoded, compressed, next, err := readChainFreezerEncodedBytesAt(file, offset, fileSize)
	if err != nil {
		return nil, 0, err
	}
	if !compressed {
		return encoded, next, nil
	}
	decodedLen, err := snappy.DecodedLen(encoded)
	if err != nil {
		return nil, 0, fmt.Errorf("decode compressed chain-freezer blob length: %w", err)
	}
	if decodedLen > chainFreezerMaxDecodedBlobSize {
		return nil, 0, fmt.Errorf("compressed chain-freezer blob decoded length %d exceeds limit %d", decodedLen, chainFreezerMaxDecodedBlobSize)
	}
	decoded, err := snappy.Decode(nil, encoded)
	if err != nil {
		return nil, 0, fmt.Errorf("decode compressed chain-freezer blob: %w", err)
	}
	return decoded, next, nil
}

func skipChainFreezerBytesAt(file io.ReaderAt, offset, fileSize uint64) (uint64, error) {
	_, _, next, err := chainFreezerBlobBoundsAt(file, offset, fileSize)
	return next, err
}

func readChainFreezerEncodedBytesAt(file io.ReaderAt, offset, fileSize uint64) ([]byte, bool, uint64, error) {
	length, compressed, next, err := chainFreezerBlobBoundsAt(file, offset, fileSize)
	if err != nil {
		return nil, false, 0, err
	}
	if length == 0 {
		return nil, false, next, nil
	}
	out := make([]byte, int(length))
	if _, err := file.ReadAt(out, int64(offset+4)); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, 0, io.ErrUnexpectedEOF
		}
		return nil, false, 0, err
	}
	return out, compressed, next, nil
}

func chainFreezerBlobBoundsAt(file io.ReaderAt, offset, fileSize uint64) (uint64, bool, uint64, error) {
	if offset > uint64(^uint(0)>>1) {
		return 0, false, 0, fmt.Errorf("offset %d overflows int64", offset)
	}
	if offset > fileSize || fileSize-offset < 4 {
		return 0, false, 0, io.ErrUnexpectedEOF
	}
	var rawLen [4]byte
	if _, err := file.ReadAt(rawLen[:], int64(offset)); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, false, 0, io.ErrUnexpectedEOF
		}
		return 0, false, 0, err
	}
	storedLen := binary.BigEndian.Uint32(rawLen[:])
	compressed := storedLen&chainFreezerBlobCompressedFlag != 0
	length := uint64(storedLen & chainFreezerBlobLengthMask)
	dataOffset := offset + 4
	next := dataOffset + length
	if next < dataOffset {
		return 0, false, 0, fmt.Errorf("length %d at offset %d overflows", length, offset)
	}
	if length > fileSize-dataOffset {
		return 0, false, 0, fmt.Errorf("declared length %d at offset %d exceeds remaining %d", length, offset, fileSize-dataOffset)
	}
	if length > chainFreezerMaxDecodedBlobSize {
		return 0, false, 0, fmt.Errorf("chain-freezer blob encoded length %d exceeds limit %d", length, chainFreezerMaxDecodedBlobSize)
	}
	if length == 0 {
		if compressed {
			return 0, false, 0, fmt.Errorf("compressed chain-freezer blob at offset %d has zero encoded length", offset)
		}
		return 0, false, next, nil
	}
	if next > uint64(^uint(0)>>1) {
		return 0, false, 0, fmt.Errorf("offset %d length %d overflows int64", offset, length)
	}
	return length, compressed, next, nil
}

func writeChainFreezerAccessorHeader(w io.Writer, header chainFreezerAccessorHeader) error {
	var raw [chainFreezerAccessorHeaderSize]byte
	copy(raw[0:8], chainFreezerAccessorMagic[:])
	binary.BigEndian.PutUint64(raw[8:16], header.fromBlock)
	binary.BigEndian.PutUint64(raw[16:24], header.toBlock)
	binary.BigEndian.PutUint64(raw[24:32], header.count)
	_, err := w.Write(raw[:])
	return err
}

func readChainFreezerAccessorHeader(r io.Reader) (chainFreezerAccessorHeader, error) {
	var raw [chainFreezerAccessorHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return chainFreezerAccessorHeader{}, io.ErrUnexpectedEOF
		}
		return chainFreezerAccessorHeader{}, err
	}
	if headerMagic := [8]byte(raw[0:8]); headerMagic != chainFreezerAccessorMagic {
		return chainFreezerAccessorHeader{}, errors.New("snapshots: invalid chain-freezer accessor segment magic")
	}
	return chainFreezerAccessorHeader{
		fromBlock: binary.BigEndian.Uint64(raw[8:16]),
		toBlock:   binary.BigEndian.Uint64(raw[16:24]),
		count:     binary.BigEndian.Uint64(raw[24:32]),
	}, nil
}

func writeChainFreezerAccessorOffset(w io.Writer, offset uint64) error {
	var raw [chainFreezerAccessorEntrySize]byte
	binary.BigEndian.PutUint64(raw[:], offset)
	_, err := w.Write(raw[:])
	return err
}

func chainFreezerAccessorExpectedSize(header chainFreezerAccessorHeader) (uint64, error) {
	entries, overflow := checkedMul(header.count, chainFreezerAccessorEntrySize)
	if overflow {
		return 0, fmt.Errorf("snapshots: chain-freezer accessor entries %d overflow size", header.count)
	}
	size, overflow := checkedAdd(chainFreezerAccessorHeaderSize, entries)
	if overflow {
		return 0, fmt.Errorf("snapshots: chain-freezer accessor size overflow")
	}
	return size, nil
}

func chainFreezerAccessorEntryOffset(ordinal uint64) uint64 {
	return chainFreezerAccessorHeaderSize + ordinal*chainFreezerAccessorEntrySize
}

func checkChainFreezerAccessorOffsets(file io.ReaderAt, header chainFreezerAccessorHeader, ref SegmentRef) error {
	var prev uint64
	for i := uint64(0); i < header.count; i++ {
		var raw [chainFreezerAccessorEntrySize]byte
		if _, err := file.ReadAt(raw[:], int64(chainFreezerAccessorEntryOffset(i))); err != nil {
			return err
		}
		offset := binary.BigEndian.Uint64(raw[:])
		if i == 0 {
			if offset != chainFreezerHeaderSize {
				return fmt.Errorf("snapshots: chain-freezer accessor segment %q first offset %d, want %d", ref.Path, offset, chainFreezerHeaderSize)
			}
		} else if offset <= prev {
			return fmt.Errorf("snapshots: chain-freezer accessor segment %q offset %d after %d is not increasing", ref.Path, offset, prev)
		}
		prev = offset
	}
	return nil
}
