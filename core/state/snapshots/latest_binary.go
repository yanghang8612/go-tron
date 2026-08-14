package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const (
	latestBinarySegmentVersion  = uint32(1)
	latestBinaryAccessorVersion = uint32(1)
	latestBinaryBTreeVersion    = uint32(1)

	latestBinaryHeaderSize         = 8 + 4 + 4 + 2 + 2 + 2 + 2 + 8 + 8 + 8
	latestBinaryAccessorHeaderSize = 8 + 4 + 4 + 2 + 2 + 2 + 2 + 8 + 8 + 8 + 8 + sha256.Size
	latestBinaryBTreeHeaderSize    = latestBinaryAccessorHeaderSize + 8
	latestBinaryBTreeBlockSize     = uint64(128)
	latestBinaryCompressedValues   = uint16(1 << 0)
	latestBinaryVerifyReadWindow   = 256 << 10

	// Within a segment whose header has latestBinaryCompressedValues set, the
	// high bit of an entry's uint32 value length marks a Snappy-compressed
	// payload. Legacy segments have no header flag and always treat all 32 bits
	// as the raw value length.
	latestBinaryValueCompressedFlag = uint32(1 << 31)
	latestBinaryMaxDecodedValueSize = 256 << 20
	latestBinaryCompressMinValue    = 64
)

var (
	latestBinarySegmentMagic  = [8]byte{'g', 't', 'l', 'a', 't', 's', 'e', 'g'}
	latestBinaryAccessorMagic = [8]byte{'g', 't', 'l', 'a', 't', 'i', 'd', 'x'}
	latestBinaryBTreeMagic    = [8]byte{'g', 't', 'l', 'a', 't', 'b', 't', '1'}
)

type latestBinaryHeader struct {
	dataset          SegmentDataset
	domain           kvdomains.KVDomain
	kind             SegmentKind
	fromTxNum        uint64
	toTxNum          uint64
	count            uint64
	compressedValues bool
	fileSize         uint64
}

type latestBinaryAccessorHeader struct {
	dataset         SegmentDataset
	domain          kvdomains.KVDomain
	kind            SegmentKind
	fromTxNum       uint64
	toTxNum         uint64
	count           uint64
	segmentSize     uint64
	segmentChecksum [sha256.Size]byte
	fileSize        uint64
}

type latestBinaryAccessor struct {
	header  latestBinaryAccessorHeader
	offsets []uint64
}

type latestBinaryBTreeHeader struct {
	latestBinaryAccessorHeader
	blockSize uint64
}

type latestBinaryBTreeEntry struct {
	key           []byte
	ordinal       uint64
	segmentOffset uint64
}

type latestBinaryValueFrame struct {
	encodedLen uint32
	compressed bool
}

// latestBinaryWindowReader amortizes sparse, monotonically increasing
// verification reads over a bounded window. Latest-segment verification only
// needs each record header and every Nth key, so advancing the logical offset
// over values avoids both reading large skipped values and issuing several
// Seek/Read syscalls per record.
type latestBinaryWindowReader struct {
	source io.ReaderAt
	size   uint64
	buf    []byte
	start  uint64
	end    uint64
}

func newLatestBinaryWindowReader(source io.ReaderAt, size uint64) *latestBinaryWindowReader {
	window := uint64(latestBinaryVerifyReadWindow)
	if size < window {
		window = size
	}
	return &latestBinaryWindowReader{
		source: source,
		size:   size,
		buf:    make([]byte, int(window)),
	}
}

func (r *latestBinaryWindowReader) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r == nil || r.source == nil {
		return 0, io.ErrUnexpectedEOF
	}
	if off < 0 {
		return 0, fmt.Errorf("snapshots: negative latest binary verification offset %d", off)
	}
	offset := uint64(off)
	requestEnd, err := latestBinaryBoundedRange("verification read", offset, uint64(len(p)), r.size)
	if err != nil {
		return 0, err
	}
	if len(p) > len(r.buf) || len(r.buf) == 0 {
		return r.source.ReadAt(p, off)
	}
	if offset < r.start || requestEnd > r.end {
		want := uint64(len(r.buf))
		if remaining := r.size - offset; want > remaining {
			want = remaining
		}
		n, readErr := r.source.ReadAt(r.buf[:int(want)], off)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return 0, readErr
		}
		if uint64(n) < want {
			return 0, io.ErrUnexpectedEOF
		}
		r.start = offset
		r.end = offset + uint64(n)
	}
	return copy(p, r.buf[offset-r.start:requestEnd-r.start]), nil
}

// CompressLatestSegments gates whether new cold latest-state records use
// per-value Snappy compression. Readers always accept both compressed and
// legacy raw records, so this only affects future segment emission.
var CompressLatestSegments = true

type latestEntryIterator func(func(LatestEntry) error) error

func writeLatestBinarySegment(path string, seg *LatestSegment) (string, uint64, string, error) {
	if seg == nil {
		return "", 0, "", errors.New("snapshots: nil latest segment")
	}
	normalized := &LatestSegment{
		Version:   seg.Version,
		Dataset:   seg.normalizedDataset(),
		Domain:    seg.Domain,
		FromTxNum: seg.FromTxNum,
		ToTxNum:   seg.ToTxNum,
		Entries:   normalizeLatestEntries(seg.Entries),
	}
	if err := normalized.Validate(); err != nil {
		return "", 0, "", err
	}
	data, err := encodeLatestBinarySegment(normalized)
	if err != nil {
		return "", 0, "", err
	}
	size, checksum := latestBinaryMetadata(data)
	path = contentAddressedSnapshotPath(path, checksum)
	if err := writeLatestBinaryFile(path, data); err != nil {
		return "", 0, "", err
	}
	return path, size, checksum, nil
}

func writeLatestBinarySegmentAndAccessor(dir string, ref SegmentRef, iter latestEntryIterator) (SegmentRef, SegmentRef, SegmentRef, error) {
	return writeLatestBinarySegmentWithCompanions(dir, ref, iter, true)
}

func writeLatestBinarySegmentWithCompanions(dir string, ref SegmentRef, iter latestEntryIterator, writeAccessor bool) (SegmentRef, SegmentRef, SegmentRef, error) {
	return writeLatestBinarySegmentWithCompanionsContext(context.Background(), dir, ref, iter, writeAccessor)
}

func writeLatestBinarySegmentWithCompanionsContext(ctx context.Context, dir string, ref SegmentRef, iter latestEntryIterator, writeAccessor bool) (SegmentRef, SegmentRef, SegmentRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if iter == nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, errors.New("snapshots: nil latest entry iterator")
	}
	if ref.Kind == "" {
		ref.Kind = SegmentLatest
	}
	if ref.Dataset == "" {
		ref.Dataset = SegmentDatasetKVLatest
	}
	if ref.Kind != SegmentLatest || !isLatestBinarySegmentPath(ref.Path) {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: streaming latest writer requires latest .seg ref, got %s/%s %q", ref.normalizedDataset(), ref.Kind, ref.Path)
	}
	if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	abs := filepath.Join(dir, ref.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	var offsets, btreePayload, btreeOffsets *os.File
	closeTemps := func() {
		for _, file := range []*os.File{tmp, offsets, btreePayload, btreeOffsets} {
			if file != nil {
				_ = file.Close()
			}
		}
	}
	defer closeTemps()

	var offsetsName string
	if writeAccessor {
		offsets, err = os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".offsets-*.tmp")
		if err != nil {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
		}
		offsetsName = offsets.Name()
		defer os.Remove(offsetsName)
	}
	btreePayload, err = os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".btree-*.tmp")
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	btreePayloadName := btreePayload.Name()
	defer os.Remove(btreePayloadName)
	btreeOffsets, err = os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".btree-offsets-*.tmp")
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	btreeOffsetsName := btreeOffsets.Name()
	defer os.Remove(btreeOffsetsName)

	if err := writeLatestBinaryHeaderTo(tmp, ref.normalizedDataset(), ref.Domain, SegmentLatest, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	var count uint64
	var btreeCount uint64
	var prev []byte
	err = iter(func(entry LatestEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry = LatestEntry{
			Key:   append([]byte(nil), entry.Key...),
			Value: append([]byte(nil), entry.Value...),
		}
		if len(entry.Key) == 0 {
			return errors.New("snapshots: latest segment contains empty key")
		}
		if len(prev) > 0 && bytes.Compare(prev, entry.Key) >= 0 {
			return errors.New("snapshots: latest stream entries are not strictly sorted")
		}
		if err := validateLatestEntry(ref.normalizedDataset(), entry); err != nil {
			return err
		}
		pos, err := tmp.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos < 0 {
			return fmt.Errorf("snapshots: latest stream negative offset %d", pos)
		}
		if writeAccessor {
			var off [8]byte
			binary.BigEndian.PutUint64(off[:], uint64(pos))
			if _, err := offsets.Write(off[:]); err != nil {
				return err
			}
		}
		if count%latestBinaryBTreeBlockSize == 0 {
			if err := writeLatestBinaryBTreeTempEntry(btreePayload, btreeOffsets, latestBinaryBTreeEntry{
				key:           entry.Key,
				ordinal:       count,
				segmentOffset: uint64(pos),
			}); err != nil {
				return err
			}
			btreeCount++
		}
		if err := writeLatestBinaryEntry(tmp, int(count), entry); err != nil {
			return err
		}
		prev = entry.Key
		count++
		return nil
	})
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := ctx.Err(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if ref.normalizedDataset() == SegmentDatasetCommitmentRoot && count != 1 {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: commitment root segment entries = %d, want 1", count)
	}
	var countBuf [8]byte
	binary.BigEndian.PutUint64(countBuf[:], count)
	if _, err := tmp.WriteAt(countBuf[:], 40); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if writeAccessor {
		if err := offsets.Sync(); err != nil {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
		}
		if err := offsets.Close(); err != nil {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
		}
	}
	if err := btreePayload.Sync(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := btreePayload.Close(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := btreeOffsets.Sync(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := btreeOffsets.Close(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	size, checksum, checksumBytes, err := latestBinaryFileMetadata(tmpName)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	// The rename is the first durable publication point. Honor shutdown before
	// crossing it so canceled builds leave only deferred-cleaned temp files.
	if err := ctx.Err(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	finalAbs := contentAddressedSnapshotPath(abs, checksum)
	if err := os.Rename(tmpName, finalAbs); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	rel, err := filepath.Rel(dir, finalAbs)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	segRef := ref
	segRef.Path = filepath.ToSlash(rel)
	segRef.Size = size
	segRef.Checksum = checksum

	var accessorRef SegmentRef
	if writeAccessor {
		accessorRef, err = writeLatestBinaryAccessorFromOffsetsFile(dir, segRef, checksumBytes, offsetsName, count)
		if err != nil {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
		}
	}
	btreeRef, err := writeLatestBinaryBTreeFromTempFiles(dir, segRef, checksumBytes, btreePayloadName, btreeOffsetsName, btreeCount)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	return segRef, accessorRef, btreeRef, nil
}

func readLatestBinarySegment(path string, ref SegmentRef) (*LatestSegment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := verifyLatestBinaryRef(path, ref, data); err != nil {
		return nil, err
	}
	seg, err := decodeLatestBinarySegment(data)
	if err != nil {
		return nil, err
	}
	if err := validateLatestBinaryRefMetadata(path, ref, seg); err != nil {
		return nil, err
	}
	if err := seg.Validate(); err != nil {
		return nil, err
	}
	return seg, nil
}

func restoreLatestBinarySegmentToStore(dir string, ref SegmentRef, store latestHotStore) error {
	if store == nil {
		return errors.New("snapshots: nil latest hot store")
	}
	path := filepath.Join(dir, ref.Path)
	if err := verifyLatestBinaryFileRef(path, ref); err != nil {
		return err
	}
	file, header, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return err
	}
	defer file.Close()
	if header.kind != SegmentLatest {
		return fmt.Errorf("snapshots: latest binary segment %q kind %q, want %q", path, header.kind, SegmentLatest)
	}
	var prev []byte
	for i := uint64(0); i < header.count; i++ {
		key, valueLen, err := readLatestBinaryEntryKey(file, header.fileSize, header.compressedValues)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, key) >= 0 {
			return errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		value, err := readLatestBinaryValueBytes(file, valueLen, header.fileSize)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary value %d: %w", i, err)
		}
		if err := restoreLatestEntryToStore(header.dataset, header.domain, store, LatestEntry{Key: key, Value: value}); err != nil {
			return err
		}
		prev = key
	}
	return nil
}

func checkLatestBinarySegment(dir string, ref SegmentRef) error {
	path := filepath.Join(dir, ref.Path)
	if err := verifyLatestBinaryFileRef(path, ref); err != nil {
		return err
	}
	file, header, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if header.kind != SegmentLatest {
		return fmt.Errorf("snapshots: latest binary segment %q kind %q, want %q", path, header.kind, SegmentLatest)
	}
	if header.dataset == SegmentDatasetCommitmentRoot && header.count != 1 {
		return fmt.Errorf("snapshots: commitment root segment entries = %d, want 1", header.count)
	}
	var prev []byte
	for i := uint64(0); i < header.count; i++ {
		key, valueLen, err := readLatestBinaryEntryKey(file, header.fileSize, header.compressedValues)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, key) >= 0 {
			return errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		value, err := readLatestBinaryValueBytes(file, valueLen, header.fileSize)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary value %d: %w", i, err)
		}
		if err := validateLatestEntry(header.dataset, LatestEntry{Key: key, Value: value}); err != nil {
			return err
		}
		prev = key
	}
	pos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if pos < 0 {
		return fmt.Errorf("snapshots: latest binary segment %q negative offset %d", path, pos)
	}
	size := uint64(stat.Size())
	if uint64(pos) > size {
		return fmt.Errorf("snapshots: latest binary segment %q read offset %d beyond size %d", path, pos, size)
	}
	if uint64(pos) != size {
		return fmt.Errorf("snapshots: latest binary segment %q has %d trailing bytes", path, size-uint64(pos))
	}
	return nil
}

func checkLatestBinaryAccessor(dir string, ref SegmentRef) error {
	path := filepath.Join(dir, ref.Path)
	if err := verifyLatestBinaryFileRef(path, ref); err != nil {
		return err
	}
	file, header, err := openLatestBinaryAccessorReader(dir, ref)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if header.kind != SegmentAccessor {
		return fmt.Errorf("snapshots: latest binary accessor %q kind %q, want %q", path, header.kind, SegmentAccessor)
	}
	if header.segmentSize < latestBinaryHeaderSize {
		return fmt.Errorf("snapshots: latest binary accessor %q segment size %d below header size", path, header.segmentSize)
	}
	if header.count > (^uint64(0)-uint64(latestBinaryAccessorHeaderSize))/8 {
		return fmt.Errorf("snapshots: latest binary accessor %q count %d overflows payload size", path, header.count)
	}
	expectedSize := uint64(latestBinaryAccessorHeaderSize) + header.count*8
	if uint64(stat.Size()) != expectedSize {
		return fmt.Errorf("snapshots: latest binary accessor %q size %d, want %d from count", path, stat.Size(), expectedSize)
	}
	var prev uint64
	for i := uint64(0); i < header.count; i++ {
		offset, err := readLatestBinaryAccessorOffsetAt(file, i)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary accessor offset %d: %w", i, err)
		}
		if offset < latestBinaryHeaderSize || offset >= header.segmentSize {
			return fmt.Errorf("snapshots: latest binary accessor offset %d out of bounds: %d", i, offset)
		}
		if i > 0 && offset <= prev {
			return errors.New("snapshots: latest binary accessor offsets are not strictly increasing")
		}
		prev = offset
	}
	return nil
}

func readLatestBinaryValue(path string, ref SegmentRef, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, nil
	}
	file, header, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	var prev []byte
	for i := uint64(0); i < header.count; i++ {
		entryKey, valueLen, err := readLatestBinaryEntryKey(file, header.fileSize, header.compressedValues)
		if err != nil {
			return nil, false, fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, entryKey) >= 0 {
			return nil, false, errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		cmp := bytes.Compare(entryKey, key)
		switch {
		case cmp == 0:
			value, err := readLatestBinaryValueBytes(file, valueLen, header.fileSize)
			if err != nil {
				return nil, false, fmt.Errorf("snapshots: decode latest binary value %d: %w", i, err)
			}
			if err := validateLatestBinaryEntry(header.dataset, entryKey, value); err != nil {
				return nil, false, fmt.Errorf("snapshots: latest binary entry %d: %w", i, err)
			}
			return value, true, nil
		case cmp > 0:
			return nil, false, nil
		default:
			if err := skipLatestBinaryValue(file, valueLen, header.fileSize); err != nil {
				return nil, false, fmt.Errorf("snapshots: skip latest binary value %d: %w", i, err)
			}
		}
		prev = entryKey
	}
	return nil, false, nil
}

func readLatestBinaryValueByAccessor(path string, ref SegmentRef, accessor latestBinaryAccessor, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, nil
	}
	file, header, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	if err := validateLatestBinaryAccessorMatchesSegment(path, ref, header, accessor.header); err != nil {
		return nil, false, err
	}
	i, ok, err := latestBinaryAccessorLowerBound(file, accessor.offsets, key, header.fileSize, header.compressedValues)
	if err != nil || !ok {
		return nil, false, err
	}
	entryKey, value, err := readLatestBinaryEntryAt(file, accessor.offsets[i], header.fileSize, header.compressedValues)
	if err != nil {
		return nil, false, err
	}
	if !bytes.Equal(entryKey, key) {
		return nil, false, nil
	}
	if err := validateLatestBinaryEntry(header.dataset, entryKey, value); err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func iterateLatestBinaryPrefix(path string, ref SegmentRef, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	file, header, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return err
	}
	defer file.Close()
	var prev []byte
	for i := uint64(0); i < header.count; i++ {
		key, valueLen, err := readLatestBinaryEntryKey(file, header.fileSize, header.compressedValues)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, key) >= 0 {
			return errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		if len(prefix) > 0 && !bytes.HasPrefix(key, prefix) {
			if bytes.Compare(key, prefix) > 0 {
				return nil
			}
			if err := skipLatestBinaryValue(file, valueLen, header.fileSize); err != nil {
				return fmt.Errorf("snapshots: skip latest binary value %d: %w", i, err)
			}
			prev = key
			continue
		}
		value, err := readLatestBinaryValueBytes(file, valueLen, header.fileSize)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary value %d: %w", i, err)
		}
		if err := validateLatestBinaryEntry(header.dataset, key, value); err != nil {
			return fmt.Errorf("snapshots: latest binary entry %d: %w", i, err)
		}
		cont, err := fn(append([]byte(nil), key...), value)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
		prev = key
	}
	return nil
}

func iterateLatestBinaryPrefixByAccessor(path string, ref SegmentRef, accessor latestBinaryAccessor, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	file, header, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := validateLatestBinaryAccessorMatchesSegment(path, ref, header, accessor.header); err != nil {
		return err
	}
	start := 0
	if len(prefix) > 0 {
		i, _, err := latestBinaryAccessorLowerBound(file, accessor.offsets, prefix, header.fileSize, header.compressedValues)
		if err != nil {
			return err
		}
		start = i
	}
	for i := start; i < len(accessor.offsets); i++ {
		key, value, err := readLatestBinaryEntryAt(file, accessor.offsets[i], header.fileSize, header.compressedValues)
		if err != nil {
			return err
		}
		if len(prefix) > 0 && !bytes.HasPrefix(key, prefix) {
			if bytes.Compare(key, prefix) > 0 {
				return nil
			}
			continue
		}
		if err := validateLatestBinaryEntry(header.dataset, key, value); err != nil {
			return err
		}
		cont, err := fn(key, value)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func readLatestBinaryValueByAccessorFile(dir, path string, ref SegmentRef, accessorRef SegmentRef, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, nil
	}
	segFile, segHeader, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return nil, false, err
	}
	defer segFile.Close()
	accessorFile, accessorHeader, err := openLatestBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return nil, false, err
	}
	defer accessorFile.Close()
	if err := validateLatestBinaryAccessorMatchesSegment(path, ref, segHeader, accessorHeader); err != nil {
		return nil, false, err
	}
	i, ok, err := latestBinaryAccessorLowerBoundFile(segFile, accessorFile, accessorHeader.count, key, segHeader.fileSize, segHeader.compressedValues)
	if err != nil || !ok {
		return nil, false, err
	}
	offset, err := readLatestBinaryAccessorOffsetAt(accessorFile, uint64(i))
	if err != nil {
		return nil, false, err
	}
	entryKey, value, err := readLatestBinaryEntryAt(segFile, offset, segHeader.fileSize, segHeader.compressedValues)
	if err != nil {
		return nil, false, err
	}
	if !bytes.Equal(entryKey, key) {
		return nil, false, nil
	}
	if err := validateLatestBinaryEntry(segHeader.dataset, entryKey, value); err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func iterateLatestBinaryPrefixByAccessorFile(dir, path string, ref SegmentRef, accessorRef SegmentRef, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	segFile, segHeader, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return err
	}
	defer segFile.Close()
	accessorFile, accessorHeader, err := openLatestBinaryAccessorReader(dir, accessorRef)
	if err != nil {
		return err
	}
	defer accessorFile.Close()
	if err := validateLatestBinaryAccessorMatchesSegment(path, ref, segHeader, accessorHeader); err != nil {
		return err
	}
	start := 0
	if len(prefix) > 0 {
		i, _, err := latestBinaryAccessorLowerBoundFile(segFile, accessorFile, accessorHeader.count, prefix, segHeader.fileSize, segHeader.compressedValues)
		if err != nil {
			return err
		}
		start = i
	}
	for i := start; uint64(i) < accessorHeader.count; i++ {
		offset, err := readLatestBinaryAccessorOffsetAt(accessorFile, uint64(i))
		if err != nil {
			return err
		}
		key, value, err := readLatestBinaryEntryAt(segFile, offset, segHeader.fileSize, segHeader.compressedValues)
		if err != nil {
			return err
		}
		if len(prefix) > 0 && !bytes.HasPrefix(key, prefix) {
			if bytes.Compare(key, prefix) > 0 {
				return nil
			}
			continue
		}
		if err := validateLatestBinaryEntry(segHeader.dataset, key, value); err != nil {
			return err
		}
		cont, err := fn(key, value)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

func readLatestBinaryValueByBTreeFile(dir, path string, ref SegmentRef, btreeRef SegmentRef, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, nil
	}
	segFile, segHeader, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return nil, false, err
	}
	defer segFile.Close()
	btreeFile, btreeHeader, err := openLatestBinaryBTreeReader(dir, btreeRef)
	if err != nil {
		return nil, false, err
	}
	defer btreeFile.Close()
	if err := validateLatestBinaryCompanionMatchesSegment(path, ref, segHeader, btreeHeader.latestBinaryAccessorHeader, SegmentBTree); err != nil {
		return nil, false, err
	}
	return readLatestBinaryValueByBTreeReaders(segFile, segHeader, btreeFile, btreeHeader, key)
}

// readLatestBinaryValueByBTreeReaders is the descriptor-owning counterpart of
// readLatestBinaryValueByBTreeFile. It performs only concurrent-safe ReadAt
// calls and therefore supports one persistently opened immutable view shared by
// all commitment lanes.
func readLatestBinaryValueByBTreeReaders(segFile io.ReaderAt, segHeader latestBinaryHeader, btreeFile io.ReaderAt, btreeHeader latestBinaryBTreeHeader, key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, nil
	}
	entry, ok, err := latestBinaryBTreeFloor(segFile, btreeFile, btreeHeader.count, key, btreeHeader.fileSize)
	if err != nil || !ok {
		return nil, false, err
	}
	limit := min(segHeader.count, entry.ordinal+btreeHeader.blockSize)
	offset := entry.segmentOffset
	for ordinal := entry.ordinal; ordinal < limit; ordinal++ {
		entryKey, value, next, err := readLatestBinaryEntryAtWithNext(segFile, offset, segHeader.fileSize, segHeader.compressedValues)
		if err != nil {
			return nil, false, err
		}
		cmp := bytes.Compare(entryKey, key)
		if cmp == 0 {
			if err := validateLatestBinaryEntry(segHeader.dataset, entryKey, value); err != nil {
				return nil, false, err
			}
			return value, true, nil
		}
		if cmp > 0 {
			return nil, false, nil
		}
		offset = next
	}
	return nil, false, nil
}

func iterateLatestBinaryPrefixByBTreeFile(dir, path string, ref SegmentRef, btreeRef SegmentRef, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	segFile, segHeader, err := openLatestBinaryReader(path, ref)
	if err != nil {
		return err
	}
	defer segFile.Close()
	btreeFile, btreeHeader, err := openLatestBinaryBTreeReader(dir, btreeRef)
	if err != nil {
		return err
	}
	defer btreeFile.Close()
	if err := validateLatestBinaryCompanionMatchesSegment(path, ref, segHeader, btreeHeader.latestBinaryAccessorHeader, SegmentBTree); err != nil {
		return err
	}
	var entry latestBinaryBTreeEntry
	if len(prefix) == 0 {
		var ok bool
		entry, ok, err = readLatestBinaryBTreeEntryAt(btreeFile, 0, btreeHeader.fileSize)
		if err != nil || !ok {
			return err
		}
	} else {
		var ok bool
		entry, ok, err = latestBinaryBTreeFloor(segFile, btreeFile, btreeHeader.count, prefix, btreeHeader.fileSize)
		if err != nil {
			return err
		}
		if !ok {
			entry, ok, err = readLatestBinaryBTreeEntryAt(btreeFile, 0, btreeHeader.fileSize)
			if err != nil || !ok {
				return err
			}
		}
	}
	offset := entry.segmentOffset
	for ordinal := entry.ordinal; ordinal < segHeader.count; ordinal++ {
		key, value, next, err := readLatestBinaryEntryAtWithNext(segFile, offset, segHeader.fileSize, segHeader.compressedValues)
		if err != nil {
			return err
		}
		if len(prefix) > 0 && !bytes.HasPrefix(key, prefix) {
			if bytes.Compare(key, prefix) > 0 {
				return nil
			}
			offset = next
			continue
		}
		if err := validateLatestBinaryEntry(segHeader.dataset, key, value); err != nil {
			return err
		}
		cont, err := fn(key, value)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
		offset = next
	}
	return nil
}

func openLatestBinaryReader(path string, ref SegmentRef) (*os.File, latestBinaryHeader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, latestBinaryHeader{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryHeader{}, err
	}
	fileSize := uint64(stat.Size())
	if ref.Size != 0 {
		if fileSize != ref.Size {
			_ = file.Close()
			return nil, latestBinaryHeader{}, fmt.Errorf("snapshots: segment %q size %d, want %d", path, stat.Size(), ref.Size)
		}
	}
	header, err := readLatestBinaryHeader(file, fileSize)
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryHeader{}, err
	}
	header.fileSize = fileSize
	if err := validateLatestBinaryHeaderRefMetadata(path, ref, header); err != nil {
		_ = file.Close()
		return nil, latestBinaryHeader{}, err
	}
	return file, header, nil
}

func isLatestBinarySegmentPath(path string) bool {
	return strings.EqualFold(filepath.Ext(path), ".seg")
}

func latestBinaryAccessorPath(segmentPath string) string {
	ext := filepath.Ext(segmentPath)
	if ext == "" {
		return segmentPath + ".lidx"
	}
	return segmentPath[:len(segmentPath)-len(ext)] + ".lidx"
}

func latestBinaryBTreePath(segmentPath string) string {
	ext := filepath.Ext(segmentPath)
	if ext == "" {
		return segmentPath + ".bt"
	}
	return segmentPath[:len(segmentPath)-len(ext)] + ".bt"
}

func writeLatestBinaryAccessorForSegment(dir string, ref SegmentRef) (SegmentRef, error) {
	if ref.Kind != SegmentLatest || !isLatestBinarySegmentPath(ref.Path) {
		return SegmentRef{}, fmt.Errorf("snapshots: latest accessor requires binary latest segment, got %s/%s %q", ref.normalizedDataset(), ref.Kind, ref.Path)
	}
	abs := filepath.Join(dir, ref.Path)
	file, header, err := openLatestBinaryReader(abs, ref)
	if err != nil {
		return SegmentRef{}, err
	}
	defer file.Close()
	offsets, err := collectLatestBinaryOffsets(file, header)
	if err != nil {
		return SegmentRef{}, err
	}
	checksum, err := latestBinaryChecksumBytes(ref.Checksum)
	if err != nil {
		return SegmentRef{}, err
	}
	data, err := encodeLatestBinaryAccessor(latestBinaryAccessor{
		header: latestBinaryAccessorHeader{
			dataset:         ref.normalizedDataset(),
			domain:          ref.Domain,
			kind:            SegmentAccessor,
			fromTxNum:       ref.FromTxNum,
			toTxNum:         ref.ToTxNum,
			count:           uint64(len(offsets)),
			segmentSize:     ref.Size,
			segmentChecksum: checksum,
		},
		offsets: offsets,
	})
	if err != nil {
		return SegmentRef{}, err
	}
	accessorRef := SegmentRef{
		Dataset:   ref.normalizedDataset(),
		Domain:    ref.Domain,
		Kind:      SegmentAccessor,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      latestBinaryAccessorPath(ref.Path),
	}
	size, checksumText := latestBinaryMetadata(data)
	accessorRef.Size = size
	accessorRef.Checksum = checksumText
	if err := writeLatestBinaryFile(filepath.Join(dir, accessorRef.Path), data); err != nil {
		return SegmentRef{}, err
	}
	return accessorRef, nil
}

func writeLatestBinaryAccessorFromOffsetsFile(dir string, ref SegmentRef, segmentChecksum [sha256.Size]byte, offsetsPath string, count uint64) (SegmentRef, error) {
	accessorRef := SegmentRef{
		Dataset:   ref.normalizedDataset(),
		Domain:    ref.Domain,
		Kind:      SegmentAccessor,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      latestBinaryAccessorPath(ref.Path),
	}
	if err := validateSegment(accessorRef, accessorRef.FromTxNum, accessorRef.ToTxNum); err != nil {
		return SegmentRef{}, err
	}
	offsets, err := os.Open(offsetsPath)
	if err != nil {
		return SegmentRef{}, err
	}
	defer offsets.Close()

	abs := filepath.Join(dir, accessorRef.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := writeLatestBinaryAccessorHeaderTo(tmp, latestBinaryAccessorHeader{
		dataset:         ref.normalizedDataset(),
		domain:          ref.Domain,
		kind:            SegmentAccessor,
		fromTxNum:       ref.FromTxNum,
		toTxNum:         ref.ToTxNum,
		count:           count,
		segmentSize:     ref.Size,
		segmentChecksum: segmentChecksum,
	}); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if n, err := io.CopyN(tmp, offsets, int64(count*8)); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	} else if uint64(n) != count*8 {
		_ = tmp.Close()
		return SegmentRef{}, io.ErrUnexpectedEOF
	}
	if extra := make([]byte, 1); true {
		n, err := offsets.Read(extra)
		if err != nil && err != io.EOF {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		if n != 0 {
			_ = tmp.Close()
			return SegmentRef{}, fmt.Errorf("snapshots: latest binary offsets file has trailing bytes")
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	size, checksum, _, err := latestBinaryFileMetadata(tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	accessorRef.Size = size
	accessorRef.Checksum = checksum
	if err := os.Rename(tmpName, abs); err != nil {
		return SegmentRef{}, err
	}
	return accessorRef, nil
}

func writeLatestBinaryBTreeTempEntry(payload, offsets io.Writer, entry latestBinaryBTreeEntry) error {
	if len(entry.key) == 0 {
		return errors.New("snapshots: latest btree entry has empty key")
	}
	if seeker, ok := payload.(io.Seeker); ok {
		pos, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos < 0 {
			return fmt.Errorf("snapshots: latest btree negative temp offset %d", pos)
		}
		var off [8]byte
		binary.BigEndian.PutUint64(off[:], uint64(pos))
		if _, err := offsets.Write(off[:]); err != nil {
			return err
		}
	} else {
		return errors.New("snapshots: latest btree payload writer is not seekable")
	}
	if len(entry.key) > math.MaxUint32 {
		return fmt.Errorf("snapshots: latest btree key is too large: %d bytes", len(entry.key))
	}
	var header [20]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(entry.key)))
	binary.BigEndian.PutUint64(header[4:12], entry.ordinal)
	binary.BigEndian.PutUint64(header[12:20], entry.segmentOffset)
	if _, err := payload.Write(header[:]); err != nil {
		return err
	}
	_, err := payload.Write(entry.key)
	return err
}

func writeLatestBinaryBTreeFromTempFiles(dir string, ref SegmentRef, segmentChecksum [sha256.Size]byte, payloadPath, offsetsPath string, count uint64) (SegmentRef, error) {
	btreeRef := SegmentRef{
		Dataset:   ref.normalizedDataset(),
		Domain:    ref.Domain,
		Kind:      SegmentBTree,
		FromTxNum: ref.FromTxNum,
		ToTxNum:   ref.ToTxNum,
		Path:      latestBinaryBTreePath(ref.Path),
	}
	if err := validateSegment(btreeRef, btreeRef.FromTxNum, btreeRef.ToTxNum); err != nil {
		return SegmentRef{}, err
	}
	payload, err := os.Open(payloadPath)
	if err != nil {
		return SegmentRef{}, err
	}
	defer payload.Close()
	offsets, err := os.Open(offsetsPath)
	if err != nil {
		return SegmentRef{}, err
	}
	defer offsets.Close()

	abs := filepath.Join(dir, btreeRef.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return SegmentRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".*.tmp")
	if err != nil {
		return SegmentRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := writeLatestBinaryBTreeHeaderTo(tmp, latestBinaryBTreeHeader{
		latestBinaryAccessorHeader: latestBinaryAccessorHeader{
			dataset:         ref.normalizedDataset(),
			domain:          ref.Domain,
			kind:            SegmentBTree,
			fromTxNum:       ref.FromTxNum,
			toTxNum:         ref.ToTxNum,
			count:           count,
			segmentSize:     ref.Size,
			segmentChecksum: segmentChecksum,
		},
		blockSize: latestBinaryBTreeBlockSize,
	}); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	offsetBase := latestBinaryBTreeHeaderSize + count*8
	for i := uint64(0); i < count; i++ {
		var raw [8]byte
		if _, err := io.ReadFull(offsets, raw[:]); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
		relative := binary.BigEndian.Uint64(raw[:])
		binary.BigEndian.PutUint64(raw[:], offsetBase+relative)
		if _, err := tmp.Write(raw[:]); err != nil {
			_ = tmp.Close()
			return SegmentRef{}, err
		}
	}
	if n, err := io.Copy(tmp, payload); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	} else if n == 0 && count != 0 {
		_ = tmp.Close()
		return SegmentRef{}, io.ErrUnexpectedEOF
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return SegmentRef{}, err
	}
	size, checksum, _, err := latestBinaryFileMetadata(tmpName)
	if err != nil {
		return SegmentRef{}, err
	}
	btreeRef.Size = size
	btreeRef.Checksum = checksum
	if err := os.Rename(tmpName, abs); err != nil {
		return SegmentRef{}, err
	}
	return btreeRef, nil
}

func encodeLatestBinarySegment(seg *LatestSegment) ([]byte, error) {
	if seg.ToTxNum < seg.FromTxNum {
		return nil, fmt.Errorf("snapshots: latest binary segment range [%d,%d] is inverted", seg.FromTxNum, seg.ToTxNum)
	}
	datasetCode, err := latestBinaryDatasetCode(seg.normalizedDataset())
	if err != nil {
		return nil, err
	}
	kindCode, err := latestBinaryKindCode(SegmentLatest)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Write(latestBinarySegmentMagic[:])
	writeUint32(&buf, latestBinarySegmentVersion)
	writeUint32(&buf, latestBinaryHeaderSize)
	writeUint16(&buf, datasetCode)
	writeUint16(&buf, uint16(seg.Domain))
	writeUint16(&buf, kindCode)
	if CompressLatestSegments {
		writeUint16(&buf, latestBinaryCompressedValues)
	} else {
		writeUint16(&buf, 0)
	}
	writeUint64(&buf, seg.FromTxNum)
	writeUint64(&buf, seg.ToTxNum)
	writeUint64(&buf, uint64(len(seg.Entries)))
	for i, entry := range seg.Entries {
		if err := encodeLatestBinaryEntry(&buf, i, entry); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func encodeLatestBinaryAccessor(accessor latestBinaryAccessor) ([]byte, error) {
	if accessor.header.toTxNum < accessor.header.fromTxNum {
		return nil, fmt.Errorf("snapshots: latest binary accessor range [%d,%d] is inverted", accessor.header.fromTxNum, accessor.header.toTxNum)
	}
	if accessor.header.kind != SegmentAccessor {
		return nil, fmt.Errorf("snapshots: latest binary accessor kind %q, want %q", accessor.header.kind, SegmentAccessor)
	}
	if accessor.header.count != uint64(len(accessor.offsets)) {
		return nil, fmt.Errorf("snapshots: latest binary accessor count %d, want %d", accessor.header.count, len(accessor.offsets))
	}
	datasetCode, err := latestBinaryDatasetCode(accessor.header.dataset)
	if err != nil {
		return nil, err
	}
	kindCode, err := latestBinaryKindCode(accessor.header.kind)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Write(latestBinaryAccessorMagic[:])
	writeUint32(&buf, latestBinaryAccessorVersion)
	writeUint32(&buf, latestBinaryAccessorHeaderSize)
	writeUint16(&buf, datasetCode)
	writeUint16(&buf, uint16(accessor.header.domain))
	writeUint16(&buf, kindCode)
	writeUint16(&buf, 0)
	writeUint64(&buf, accessor.header.fromTxNum)
	writeUint64(&buf, accessor.header.toTxNum)
	writeUint64(&buf, accessor.header.count)
	writeUint64(&buf, accessor.header.segmentSize)
	buf.Write(accessor.header.segmentChecksum[:])
	for i, offset := range accessor.offsets {
		if offset < latestBinaryHeaderSize || offset >= accessor.header.segmentSize {
			return nil, fmt.Errorf("snapshots: latest binary accessor offset %d out of bounds: %d", i, offset)
		}
		if i > 0 && offset <= accessor.offsets[i-1] {
			return nil, errors.New("snapshots: latest binary accessor offsets are not strictly increasing")
		}
		writeUint64(&buf, offset)
	}
	return buf.Bytes(), nil
}

func decodeLatestBinarySegment(data []byte) (*LatestSegment, error) {
	header, rest, err := decodeLatestBinaryHeader(data)
	if err != nil {
		return nil, err
	}
	if header.count > uint64(len(rest))/8 {
		return nil, fmt.Errorf("snapshots: latest binary segment record count %d exceeds payload size %d", header.count, len(rest))
	}
	entries := make([]LatestEntry, 0, header.count)
	for i := uint64(0); i < header.count; i++ {
		entry, next, err := decodeLatestBinaryEntry(rest, header.compressedValues)
		if err != nil {
			return nil, fmt.Errorf("snapshots: decode latest binary record %d: %w", i, err)
		}
		entries = append(entries, entry)
		rest = next
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("snapshots: latest binary segment has %d trailing bytes", len(rest))
	}
	return &LatestSegment{
		Version:   LatestSegmentVersion,
		Dataset:   header.dataset,
		Domain:    header.domain,
		FromTxNum: header.fromTxNum,
		ToTxNum:   header.toTxNum,
		Entries:   entries,
	}, nil
}

func decodeLatestBinaryAccessor(data []byte) (latestBinaryAccessor, error) {
	header, rest, err := decodeLatestBinaryAccessorHeader(data)
	if err != nil {
		return latestBinaryAccessor{}, err
	}
	if header.count > uint64(len(rest))/8 {
		return latestBinaryAccessor{}, fmt.Errorf("snapshots: latest binary accessor count %d exceeds payload size %d", header.count, len(rest))
	}
	if uint64(len(rest)) != header.count*8 {
		return latestBinaryAccessor{}, fmt.Errorf("snapshots: latest binary accessor payload size %d, want %d", len(rest), header.count*8)
	}
	offsets := make([]uint64, 0, header.count)
	for i := uint64(0); i < header.count; i++ {
		offset := binary.BigEndian.Uint64(rest[:8])
		if offset < latestBinaryHeaderSize || offset >= header.segmentSize {
			return latestBinaryAccessor{}, fmt.Errorf("snapshots: latest binary accessor offset %d out of bounds: %d", i, offset)
		}
		if i > 0 && offset <= offsets[i-1] {
			return latestBinaryAccessor{}, errors.New("snapshots: latest binary accessor offsets are not strictly increasing")
		}
		offsets = append(offsets, offset)
		rest = rest[8:]
	}
	return latestBinaryAccessor{header: header, offsets: offsets}, nil
}

func decodeLatestBinaryHeader(data []byte) (latestBinaryHeader, []byte, error) {
	if len(data) < latestBinaryHeaderSize {
		return latestBinaryHeader{}, nil, fmt.Errorf("snapshots: latest binary segment is too small: %d bytes", len(data))
	}
	if !bytes.Equal(data[:8], latestBinarySegmentMagic[:]) {
		return latestBinaryHeader{}, nil, errors.New("snapshots: invalid latest binary magic")
	}
	version := binary.BigEndian.Uint32(data[8:12])
	if version != latestBinarySegmentVersion {
		return latestBinaryHeader{}, nil, fmt.Errorf("snapshots: unsupported latest binary version %d", version)
	}
	headerSize := binary.BigEndian.Uint32(data[12:16])
	if headerSize < latestBinaryHeaderSize {
		return latestBinaryHeader{}, nil, fmt.Errorf("snapshots: latest binary header size %d, want at least %d", headerSize, latestBinaryHeaderSize)
	}
	if uint64(headerSize) > uint64(len(data)) {
		return latestBinaryHeader{}, nil, io.ErrUnexpectedEOF
	}
	dataset, err := latestBinaryDataset(binary.BigEndian.Uint16(data[16:18]))
	if err != nil {
		return latestBinaryHeader{}, nil, err
	}
	kind, err := latestBinaryKind(binary.BigEndian.Uint16(data[20:22]))
	if err != nil {
		return latestBinaryHeader{}, nil, err
	}
	flags := binary.BigEndian.Uint16(data[22:24])
	if flags&^latestBinaryCompressedValues != 0 {
		return latestBinaryHeader{}, nil, fmt.Errorf("snapshots: unsupported latest binary flags %#04x", flags)
	}
	return latestBinaryHeader{
		dataset:          dataset,
		domain:           kvdomains.KVDomain(binary.BigEndian.Uint16(data[18:20])),
		kind:             kind,
		fromTxNum:        binary.BigEndian.Uint64(data[24:32]),
		toTxNum:          binary.BigEndian.Uint64(data[32:40]),
		count:            binary.BigEndian.Uint64(data[40:48]),
		compressedValues: flags&latestBinaryCompressedValues != 0,
	}, data[headerSize:], nil
}

func decodeLatestBinaryAccessorHeader(data []byte) (latestBinaryAccessorHeader, []byte, error) {
	return decodeLatestBinaryAccessorHeaderWithMagic(data, latestBinaryAccessorMagic, latestBinaryAccessorVersion)
}

func decodeLatestBinaryAccessorHeaderWithMagic(data []byte, magic [8]byte, versionWant uint32) (latestBinaryAccessorHeader, []byte, error) {
	if len(data) < latestBinaryAccessorHeaderSize {
		return latestBinaryAccessorHeader{}, nil, fmt.Errorf("snapshots: latest binary accessor is too small: %d bytes", len(data))
	}
	if !bytes.Equal(data[:8], magic[:]) {
		return latestBinaryAccessorHeader{}, nil, errors.New("snapshots: invalid latest binary accessor magic")
	}
	version := binary.BigEndian.Uint32(data[8:12])
	if version != versionWant {
		return latestBinaryAccessorHeader{}, nil, fmt.Errorf("snapshots: unsupported latest binary accessor version %d", version)
	}
	headerSize := binary.BigEndian.Uint32(data[12:16])
	if headerSize < latestBinaryAccessorHeaderSize {
		return latestBinaryAccessorHeader{}, nil, fmt.Errorf("snapshots: latest binary accessor header size %d, want at least %d", headerSize, latestBinaryAccessorHeaderSize)
	}
	if uint64(headerSize) > uint64(len(data)) {
		return latestBinaryAccessorHeader{}, nil, io.ErrUnexpectedEOF
	}
	dataset, err := latestBinaryDataset(binary.BigEndian.Uint16(data[16:18]))
	if err != nil {
		return latestBinaryAccessorHeader{}, nil, err
	}
	kind, err := latestBinaryKind(binary.BigEndian.Uint16(data[20:22]))
	if err != nil {
		return latestBinaryAccessorHeader{}, nil, err
	}
	flags := binary.BigEndian.Uint16(data[22:24])
	if flags != 0 {
		return latestBinaryAccessorHeader{}, nil, fmt.Errorf("snapshots: unsupported latest binary accessor flags %#04x", flags)
	}
	var checksum [sha256.Size]byte
	copy(checksum[:], data[56:56+sha256.Size])
	return latestBinaryAccessorHeader{
		dataset:         dataset,
		domain:          kvdomains.KVDomain(binary.BigEndian.Uint16(data[18:20])),
		kind:            kind,
		fromTxNum:       binary.BigEndian.Uint64(data[24:32]),
		toTxNum:         binary.BigEndian.Uint64(data[32:40]),
		count:           binary.BigEndian.Uint64(data[40:48]),
		segmentSize:     binary.BigEndian.Uint64(data[48:56]),
		segmentChecksum: checksum,
	}, data[headerSize:], nil
}

func readLatestBinaryHeader(r io.Reader, fileSize uint64) (latestBinaryHeader, error) {
	fixed := make([]byte, latestBinaryHeaderSize)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return latestBinaryHeader{}, err
	}
	if !bytes.Equal(fixed[:8], latestBinarySegmentMagic[:]) {
		return latestBinaryHeader{}, errors.New("snapshots: invalid latest binary magic")
	}
	headerSize := binary.BigEndian.Uint32(fixed[12:16])
	if headerSize < latestBinaryHeaderSize {
		return latestBinaryHeader{}, fmt.Errorf("snapshots: latest binary header size %d, want at least %d", headerSize, latestBinaryHeaderSize)
	}
	if uint64(headerSize) > fileSize {
		return latestBinaryHeader{}, fmt.Errorf("snapshots: latest binary header size %d exceeds file size %d", headerSize, fileSize)
	}
	headerBytes := fixed
	if headerSize > latestBinaryHeaderSize {
		extra := make([]byte, int(headerSize)-latestBinaryHeaderSize)
		if _, err := io.ReadFull(r, extra); err != nil {
			return latestBinaryHeader{}, err
		}
		headerBytes = append(headerBytes, extra...)
	}
	header, rest, err := decodeLatestBinaryHeader(headerBytes)
	if err != nil {
		return latestBinaryHeader{}, err
	}
	if len(rest) != 0 {
		return latestBinaryHeader{}, fmt.Errorf("snapshots: latest binary header has %d trailing bytes", len(rest))
	}
	return header, nil
}

func openLatestBinaryAccessorReader(dir string, ref SegmentRef) (*os.File, latestBinaryAccessorHeader, error) {
	if ref.Kind != SegmentAccessor {
		return nil, latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor %q kind %q, want %q", ref.Path, ref.Kind, SegmentAccessor)
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, latestBinaryAccessorHeader{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryAccessorHeader{}, err
	}
	fileSize := uint64(stat.Size())
	if ref.Size != 0 {
		if fileSize != ref.Size {
			_ = file.Close()
			return nil, latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: accessor %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
		}
	}
	header, err := readLatestBinaryAccessorHeader(file, fileSize)
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryAccessorHeader{}, err
	}
	header.fileSize = fileSize
	if ref.Dataset != "" && header.dataset != ref.normalizedDataset() {
		_ = file.Close()
		return nil, latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor %q dataset %q, want %q", ref.Path, header.dataset, ref.normalizedDataset())
	}
	if header.domain != ref.Domain {
		_ = file.Close()
		return nil, latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor %q domain %#04x, want %#04x", ref.Path, uint16(header.domain), uint16(ref.Domain))
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		_ = file.Close()
		return nil, latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if ref.Size != 0 && ref.Size != latestBinaryAccessorHeaderSize+header.count*8 {
		_ = file.Close()
		return nil, latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor %q size %d, want %d from count", ref.Path, ref.Size, latestBinaryAccessorHeaderSize+header.count*8)
	}
	return file, header, nil
}

func openLatestBinaryBTreeReader(dir string, ref SegmentRef) (*os.File, latestBinaryBTreeHeader, error) {
	if ref.Kind != SegmentBTree {
		return nil, latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree %q kind %q, want %q", ref.Path, ref.Kind, SegmentBTree)
	}
	file, err := os.Open(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, latestBinaryBTreeHeader{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryBTreeHeader{}, err
	}
	fileSize := uint64(stat.Size())
	if ref.Size != 0 {
		if fileSize != ref.Size {
			_ = file.Close()
			return nil, latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
		}
	}
	header, err := readLatestBinaryBTreeHeader(file, fileSize)
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryBTreeHeader{}, err
	}
	header.fileSize = fileSize
	if ref.Dataset != "" && header.dataset != ref.normalizedDataset() {
		_ = file.Close()
		return nil, latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree %q dataset %q, want %q", ref.Path, header.dataset, ref.normalizedDataset())
	}
	if header.domain != ref.Domain {
		_ = file.Close()
		return nil, latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree %q domain %#04x, want %#04x", ref.Path, uint16(header.domain), uint16(ref.Domain))
	}
	if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
		_ = file.Close()
		return nil, latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree %q range [%d,%d], want [%d,%d]", ref.Path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	if header.blockSize == 0 {
		_ = file.Close()
		return nil, latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree %q has zero block size", ref.Path)
	}
	if ref.Size != 0 && ref.Size < latestBinaryBTreeHeaderSize+header.count*8 {
		_ = file.Close()
		return nil, latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree %q size %d below offset table size", ref.Path, ref.Size)
	}
	return file, header, nil
}

func readLatestBinaryAccessorHeader(r io.Reader, fileSize uint64) (latestBinaryAccessorHeader, error) {
	fixed := make([]byte, latestBinaryAccessorHeaderSize)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return latestBinaryAccessorHeader{}, err
	}
	headerSize := binary.BigEndian.Uint32(fixed[12:16])
	if headerSize < latestBinaryAccessorHeaderSize {
		return latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor header size %d, want at least %d", headerSize, latestBinaryAccessorHeaderSize)
	}
	if uint64(headerSize) > fileSize {
		return latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor header size %d exceeds file size %d", headerSize, fileSize)
	}
	headerBytes := fixed
	if headerSize > latestBinaryAccessorHeaderSize {
		extra := make([]byte, int(headerSize)-latestBinaryAccessorHeaderSize)
		if _, err := io.ReadFull(r, extra); err != nil {
			return latestBinaryAccessorHeader{}, err
		}
		headerBytes = append(headerBytes, extra...)
	}
	header, rest, err := decodeLatestBinaryAccessorHeader(headerBytes)
	if err != nil {
		return latestBinaryAccessorHeader{}, err
	}
	if len(rest) != 0 {
		return latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor header has %d trailing bytes", len(rest))
	}
	return header, nil
}

func readLatestBinaryBTreeHeader(r io.Reader, fileSize uint64) (latestBinaryBTreeHeader, error) {
	fixed := make([]byte, latestBinaryBTreeHeaderSize)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return latestBinaryBTreeHeader{}, err
	}
	if !bytes.Equal(fixed[:8], latestBinaryBTreeMagic[:]) {
		return latestBinaryBTreeHeader{}, errors.New("snapshots: invalid latest binary btree magic")
	}
	version := binary.BigEndian.Uint32(fixed[8:12])
	if version != latestBinaryBTreeVersion {
		return latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: unsupported latest binary btree version %d", version)
	}
	headerSize := binary.BigEndian.Uint32(fixed[12:16])
	if headerSize < latestBinaryBTreeHeaderSize {
		return latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree header size %d, want at least %d", headerSize, latestBinaryBTreeHeaderSize)
	}
	if uint64(headerSize) > fileSize {
		return latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree header size %d exceeds file size %d", headerSize, fileSize)
	}
	headerBytes := fixed
	if headerSize > latestBinaryBTreeHeaderSize {
		extra := make([]byte, int(headerSize)-latestBinaryBTreeHeaderSize)
		if _, err := io.ReadFull(r, extra); err != nil {
			return latestBinaryBTreeHeader{}, err
		}
		headerBytes = append(headerBytes, extra...)
	}
	dataset, err := latestBinaryDataset(binary.BigEndian.Uint16(headerBytes[16:18]))
	if err != nil {
		return latestBinaryBTreeHeader{}, err
	}
	kind, err := latestBinaryKind(binary.BigEndian.Uint16(headerBytes[20:22]))
	if err != nil {
		return latestBinaryBTreeHeader{}, err
	}
	flags := binary.BigEndian.Uint16(headerBytes[22:24])
	if flags != 0 {
		return latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: unsupported latest binary btree flags %#04x", flags)
	}
	var checksum [sha256.Size]byte
	copy(checksum[:], headerBytes[56:56+sha256.Size])
	return latestBinaryBTreeHeader{
		latestBinaryAccessorHeader: latestBinaryAccessorHeader{
			dataset:         dataset,
			domain:          kvdomains.KVDomain(binary.BigEndian.Uint16(headerBytes[18:20])),
			kind:            kind,
			fromTxNum:       binary.BigEndian.Uint64(headerBytes[24:32]),
			toTxNum:         binary.BigEndian.Uint64(headerBytes[32:40]),
			count:           binary.BigEndian.Uint64(headerBytes[40:48]),
			segmentSize:     binary.BigEndian.Uint64(headerBytes[48:56]),
			segmentChecksum: checksum,
		},
		blockSize: binary.BigEndian.Uint64(headerBytes[latestBinaryAccessorHeaderSize:latestBinaryBTreeHeaderSize]),
	}, nil
}

func readLatestBinaryAccessor(dir string, ref SegmentRef) (latestBinaryAccessor, error) {
	if ref.Kind != SegmentAccessor {
		return latestBinaryAccessor{}, fmt.Errorf("snapshots: latest binary accessor %q kind %q, want %q", ref.Path, ref.Kind, SegmentAccessor)
	}
	data, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return latestBinaryAccessor{}, err
	}
	if err := verifyLatestBinaryRef(filepath.Join(dir, ref.Path), ref, data); err != nil {
		return latestBinaryAccessor{}, err
	}
	accessor, err := decodeLatestBinaryAccessor(data)
	if err != nil {
		return latestBinaryAccessor{}, err
	}
	if ref.Dataset != "" && accessor.header.dataset != ref.normalizedDataset() {
		return latestBinaryAccessor{}, fmt.Errorf("snapshots: latest binary accessor %q dataset %q, want %q", ref.Path, accessor.header.dataset, ref.normalizedDataset())
	}
	if accessor.header.domain != ref.Domain {
		return latestBinaryAccessor{}, fmt.Errorf("snapshots: latest binary accessor %q domain %#04x, want %#04x", ref.Path, uint16(accessor.header.domain), uint16(ref.Domain))
	}
	if accessor.header.fromTxNum != ref.FromTxNum || accessor.header.toTxNum != ref.ToTxNum {
		return latestBinaryAccessor{}, fmt.Errorf("snapshots: latest binary accessor %q range [%d,%d], want [%d,%d]", ref.Path, accessor.header.fromTxNum, accessor.header.toTxNum, ref.FromTxNum, ref.ToTxNum)
	}
	return accessor, nil
}

func encodeLatestBinaryEntry(buf *bytes.Buffer, index int, entry LatestEntry) error {
	return writeLatestBinaryEntry(buf, index, entry)
}

func writeLatestBinaryHeaderTo(w io.Writer, dataset SegmentDataset, domain kvdomains.KVDomain, kind SegmentKind, fromTxNum, toTxNum, count uint64) error {
	datasetCode, err := latestBinaryDatasetCode(dataset)
	if err != nil {
		return err
	}
	kindCode, err := latestBinaryKindCode(kind)
	if err != nil {
		return err
	}
	var header [latestBinaryHeaderSize]byte
	copy(header[:8], latestBinarySegmentMagic[:])
	binary.BigEndian.PutUint32(header[8:12], latestBinarySegmentVersion)
	binary.BigEndian.PutUint32(header[12:16], latestBinaryHeaderSize)
	binary.BigEndian.PutUint16(header[16:18], datasetCode)
	binary.BigEndian.PutUint16(header[18:20], uint16(domain))
	binary.BigEndian.PutUint16(header[20:22], kindCode)
	if CompressLatestSegments {
		binary.BigEndian.PutUint16(header[22:24], latestBinaryCompressedValues)
	}
	binary.BigEndian.PutUint64(header[24:32], fromTxNum)
	binary.BigEndian.PutUint64(header[32:40], toTxNum)
	binary.BigEndian.PutUint64(header[40:48], count)
	_, err = w.Write(header[:])
	return err
}

func writeLatestBinaryAccessorHeaderTo(w io.Writer, header latestBinaryAccessorHeader) error {
	datasetCode, err := latestBinaryDatasetCode(header.dataset)
	if err != nil {
		return err
	}
	kindCode, err := latestBinaryKindCode(header.kind)
	if err != nil {
		return err
	}
	var out [latestBinaryAccessorHeaderSize]byte
	copy(out[:8], latestBinaryAccessorMagic[:])
	binary.BigEndian.PutUint32(out[8:12], latestBinaryAccessorVersion)
	binary.BigEndian.PutUint32(out[12:16], latestBinaryAccessorHeaderSize)
	binary.BigEndian.PutUint16(out[16:18], datasetCode)
	binary.BigEndian.PutUint16(out[18:20], uint16(header.domain))
	binary.BigEndian.PutUint16(out[20:22], kindCode)
	binary.BigEndian.PutUint64(out[24:32], header.fromTxNum)
	binary.BigEndian.PutUint64(out[32:40], header.toTxNum)
	binary.BigEndian.PutUint64(out[40:48], header.count)
	binary.BigEndian.PutUint64(out[48:56], header.segmentSize)
	copy(out[56:56+sha256.Size], header.segmentChecksum[:])
	_, err = w.Write(out[:])
	return err
}

func writeLatestBinaryBTreeHeaderTo(w io.Writer, header latestBinaryBTreeHeader) error {
	datasetCode, err := latestBinaryDatasetCode(header.dataset)
	if err != nil {
		return err
	}
	kindCode, err := latestBinaryKindCode(header.kind)
	if err != nil {
		return err
	}
	var out [latestBinaryBTreeHeaderSize]byte
	copy(out[:8], latestBinaryBTreeMagic[:])
	binary.BigEndian.PutUint32(out[8:12], latestBinaryBTreeVersion)
	binary.BigEndian.PutUint32(out[12:16], latestBinaryBTreeHeaderSize)
	binary.BigEndian.PutUint16(out[16:18], datasetCode)
	binary.BigEndian.PutUint16(out[18:20], uint16(header.domain))
	binary.BigEndian.PutUint16(out[20:22], kindCode)
	binary.BigEndian.PutUint64(out[24:32], header.fromTxNum)
	binary.BigEndian.PutUint64(out[32:40], header.toTxNum)
	binary.BigEndian.PutUint64(out[40:48], header.count)
	binary.BigEndian.PutUint64(out[48:56], header.segmentSize)
	copy(out[56:56+sha256.Size], header.segmentChecksum[:])
	binary.BigEndian.PutUint64(out[latestBinaryAccessorHeaderSize:latestBinaryBTreeHeaderSize], header.blockSize)
	_, err = w.Write(out[:])
	return err
}

func writeLatestBinaryEntry(w io.Writer, index int, entry LatestEntry) error {
	if len(entry.Key) > math.MaxUint32 {
		return fmt.Errorf("snapshots: latest binary record %d key is too large: %d bytes", index, len(entry.Key))
	}
	if len(entry.Value) > latestBinaryMaxDecodedValueSize {
		return fmt.Errorf("snapshots: latest binary record %d value is too large: %d bytes", index, len(entry.Value))
	}
	encodedValue, frame, err := encodeLatestBinaryValue(entry.Value)
	if err != nil {
		return fmt.Errorf("snapshots: latest binary record %d value: %w", index, err)
	}
	var lens [8]byte
	binary.BigEndian.PutUint32(lens[:4], uint32(len(entry.Key)))
	storedLen := frame.encodedLen
	if frame.compressed {
		storedLen |= latestBinaryValueCompressedFlag
	}
	binary.BigEndian.PutUint32(lens[4:], storedLen)
	if _, err := w.Write(lens[:]); err != nil {
		return err
	}
	if _, err := w.Write(entry.Key); err != nil {
		return err
	}
	_, err = w.Write(encodedValue)
	return err
}

func encodeLatestBinaryValue(value []byte) ([]byte, latestBinaryValueFrame, error) {
	if len(value) > latestBinaryMaxDecodedValueSize {
		return nil, latestBinaryValueFrame{}, fmt.Errorf("decoded length %d exceeds limit %d", len(value), latestBinaryMaxDecodedValueSize)
	}
	if len(value) > int(latestBinaryValueCompressedFlag-1) {
		return nil, latestBinaryValueFrame{}, fmt.Errorf("encoded length %d exceeds limit %d", len(value), latestBinaryValueCompressedFlag-1)
	}
	frame := latestBinaryValueFrame{encodedLen: uint32(len(value))}
	if !CompressLatestSegments || len(value) < latestBinaryCompressMinValue {
		return value, frame, nil
	}
	compressed := snappy.Encode(nil, value)
	if len(compressed) >= len(value) {
		return value, frame, nil
	}
	if len(compressed) > int(latestBinaryValueCompressedFlag-1) {
		return nil, latestBinaryValueFrame{}, fmt.Errorf("compressed length %d exceeds limit %d", len(compressed), latestBinaryValueCompressedFlag-1)
	}
	return compressed, latestBinaryValueFrame{encodedLen: uint32(len(compressed)), compressed: true}, nil
}

func latestBinaryValueFrameFromStoredLength(stored uint32, compressedValues bool) (latestBinaryValueFrame, error) {
	if !compressedValues {
		return latestBinaryValueFrame{encodedLen: stored}, nil
	}
	frame := latestBinaryValueFrame{
		encodedLen: stored &^ latestBinaryValueCompressedFlag,
		compressed: stored&latestBinaryValueCompressedFlag != 0,
	}
	if frame.compressed && frame.encodedLen == 0 {
		return latestBinaryValueFrame{}, errors.New("compressed latest binary value has zero encoded length")
	}
	return frame, nil
}

func validateLatestBinaryValueFrame(frame latestBinaryValueFrame) error {
	if frame.encodedLen > latestBinaryMaxDecodedValueSize {
		return fmt.Errorf("encoded length %d exceeds limit %d", frame.encodedLen, latestBinaryMaxDecodedValueSize)
	}
	return nil
}

func decodeLatestBinaryValue(encoded []byte, frame latestBinaryValueFrame) ([]byte, error) {
	if err := validateLatestBinaryValueFrame(frame); err != nil {
		return nil, err
	}
	if uint64(len(encoded)) != uint64(frame.encodedLen) {
		return nil, fmt.Errorf("encoded length %d, want %d", len(encoded), frame.encodedLen)
	}
	if !frame.compressed {
		if len(encoded) > latestBinaryMaxDecodedValueSize {
			return nil, fmt.Errorf("decoded length %d exceeds limit %d", len(encoded), latestBinaryMaxDecodedValueSize)
		}
		return encoded, nil
	}
	decodedLen, err := snappy.DecodedLen(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode compressed latest binary value length: %w", err)
	}
	if decodedLen > latestBinaryMaxDecodedValueSize {
		return nil, fmt.Errorf("compressed latest binary value decoded length %d exceeds limit %d", decodedLen, latestBinaryMaxDecodedValueSize)
	}
	decoded, err := snappy.Decode(nil, encoded)
	if err != nil {
		return nil, fmt.Errorf("decode compressed latest binary value: %w", err)
	}
	return decoded, nil
}

func latestBinaryBoundedRange(kind string, offset, length, maxEnd uint64) (uint64, error) {
	if offset > maxEnd {
		return 0, fmt.Errorf("snapshots: latest binary %s offset %d exceeds segment bound %d", kind, offset, maxEnd)
	}
	if length > maxEnd-offset {
		return 0, fmt.Errorf("snapshots: latest binary %s length %d at offset %d exceeds segment bound %d", kind, length, offset, maxEnd)
	}
	return offset + length, nil
}

func readLatestBinaryEntryKey(file *os.File, maxEnd uint64, compressedValues bool) ([]byte, latestBinaryValueFrame, error) {
	pos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, latestBinaryValueFrame{}, err
	}
	if pos < 0 {
		return nil, latestBinaryValueFrame{}, fmt.Errorf("snapshots: latest binary negative offset %d", pos)
	}
	offset := uint64(pos)
	if _, err := latestBinaryBoundedRange("entry header", offset, 8, maxEnd); err != nil {
		return nil, latestBinaryValueFrame{}, err
	}
	var lens [8]byte
	if _, err := io.ReadFull(file, lens[:]); err != nil {
		return nil, latestBinaryValueFrame{}, err
	}
	keyLen := binary.BigEndian.Uint32(lens[:4])
	frame, err := latestBinaryValueFrameFromStoredLength(binary.BigEndian.Uint32(lens[4:]), compressedValues)
	if err != nil {
		return nil, latestBinaryValueFrame{}, err
	}
	keyOffset := offset + 8
	if _, err := latestBinaryBoundedRange("entry key", keyOffset, uint64(keyLen), maxEnd); err != nil {
		return nil, latestBinaryValueFrame{}, err
	}
	key := make([]byte, keyLen)
	if keyLen > 0 {
		if _, err := io.ReadFull(file, key); err != nil {
			return nil, latestBinaryValueFrame{}, err
		}
	}
	return key, frame, nil
}

func readLatestBinaryEntryKeyAt(r io.ReaderAt, offset, maxEnd uint64, compressedValues bool) ([]byte, latestBinaryValueFrame, error) {
	keyLen, frame, err := readLatestBinaryEntryHeaderAt(r, offset, maxEnd, compressedValues)
	if err != nil {
		return nil, latestBinaryValueFrame{}, err
	}
	keyOffset := offset + 8
	key := make([]byte, keyLen)
	if keyLen > 0 {
		if _, err := r.ReadAt(key, int64(keyOffset)); err != nil {
			return nil, latestBinaryValueFrame{}, err
		}
	}
	return key, frame, nil
}

func readLatestBinaryEntryHeaderAt(r io.ReaderAt, offset, maxEnd uint64, compressedValues bool) (uint32, latestBinaryValueFrame, error) {
	if offset > math.MaxInt64 {
		return 0, latestBinaryValueFrame{}, fmt.Errorf("snapshots: latest binary offset too large: %d", offset)
	}
	if _, err := latestBinaryBoundedRange("entry header", offset, 8, maxEnd); err != nil {
		return 0, latestBinaryValueFrame{}, err
	}
	var lens [8]byte
	if _, err := r.ReadAt(lens[:], int64(offset)); err != nil {
		return 0, latestBinaryValueFrame{}, err
	}
	keyLen := binary.BigEndian.Uint32(lens[:4])
	frame, err := latestBinaryValueFrameFromStoredLength(binary.BigEndian.Uint32(lens[4:]), compressedValues)
	if err != nil {
		return 0, latestBinaryValueFrame{}, err
	}
	keyOffset := offset + 8
	if _, err := latestBinaryBoundedRange("entry key", keyOffset, uint64(keyLen), maxEnd); err != nil {
		return 0, latestBinaryValueFrame{}, err
	}
	if keyOffset > math.MaxInt64 {
		return 0, latestBinaryValueFrame{}, fmt.Errorf("snapshots: latest binary key offset too large: %d", keyOffset)
	}
	return keyLen, frame, nil
}

func readLatestBinaryEntryAt(r io.ReaderAt, offset, maxEnd uint64, compressedValues bool) ([]byte, []byte, error) {
	key, frame, err := readLatestBinaryEntryKeyAt(r, offset, maxEnd, compressedValues)
	if err != nil {
		return nil, nil, err
	}
	valueOffset := offset + 8 + uint64(len(key))
	if _, err := latestBinaryBoundedRange("entry value", valueOffset, uint64(frame.encodedLen), maxEnd); err != nil {
		return nil, nil, err
	}
	if valueOffset > math.MaxInt64 {
		return nil, nil, fmt.Errorf("snapshots: latest binary value offset too large: %d", valueOffset)
	}
	if err := validateLatestBinaryValueFrame(frame); err != nil {
		return nil, nil, err
	}
	encoded := make([]byte, frame.encodedLen)
	if frame.encodedLen > 0 {
		if _, err := r.ReadAt(encoded, int64(valueOffset)); err != nil {
			return nil, nil, err
		}
	}
	value, err := decodeLatestBinaryValue(encoded, frame)
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

func readLatestBinaryEntryAtWithNext(r io.ReaderAt, offset, maxEnd uint64, compressedValues bool) ([]byte, []byte, uint64, error) {
	key, frame, err := readLatestBinaryEntryKeyAt(r, offset, maxEnd, compressedValues)
	if err != nil {
		return nil, nil, 0, err
	}
	valueOffset := offset + 8 + uint64(len(key))
	next, err := latestBinaryBoundedRange("entry value", valueOffset, uint64(frame.encodedLen), maxEnd)
	if err != nil {
		return nil, nil, 0, err
	}
	if valueOffset > math.MaxInt64 {
		return nil, nil, 0, fmt.Errorf("snapshots: latest binary value offset too large: %d", valueOffset)
	}
	if err := validateLatestBinaryValueFrame(frame); err != nil {
		return nil, nil, 0, err
	}
	encoded := make([]byte, frame.encodedLen)
	if frame.encodedLen > 0 {
		if _, err := r.ReadAt(encoded, int64(valueOffset)); err != nil {
			return nil, nil, 0, err
		}
	}
	value, err := decodeLatestBinaryValue(encoded, frame)
	if err != nil {
		return nil, nil, 0, err
	}
	return key, value, next, nil
}

func validateLatestBinaryEntry(dataset SegmentDataset, key, value []byte) error {
	return validateLatestEntry(dataset, LatestEntry{
		Key:   key,
		Value: value,
	})
}

func latestBinaryAccessorLowerBound(r io.ReaderAt, offsets []uint64, key []byte, maxEnd uint64, compressedValues bool) (int, bool, error) {
	var foundErr error
	i := sort.Search(len(offsets), func(i int) bool {
		if foundErr != nil {
			return true
		}
		entryKey, _, err := readLatestBinaryEntryKeyAt(r, offsets[i], maxEnd, compressedValues)
		if err != nil {
			foundErr = err
			return true
		}
		return bytes.Compare(entryKey, key) >= 0
	})
	if foundErr != nil {
		return 0, false, foundErr
	}
	return i, i < len(offsets), nil
}

func readLatestBinaryBTreeEntryAt(r io.ReaderAt, index, fileSize uint64) (latestBinaryBTreeEntry, bool, error) {
	offset, err := readLatestBinaryBTreeEntryOffsetAt(r, index)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return latestBinaryBTreeEntry{}, false, nil
		}
		return latestBinaryBTreeEntry{}, false, err
	}
	entry, err := readLatestBinaryBTreeEntryAtOffset(r, offset, fileSize)
	return entry, true, err
}

func readLatestBinaryBTreeEntryOffsetAt(r io.ReaderAt, index uint64) (uint64, error) {
	if index > (math.MaxInt64-latestBinaryBTreeHeaderSize)/8 {
		return 0, fmt.Errorf("snapshots: latest btree index too large: %d", index)
	}
	var raw [8]byte
	if _, err := r.ReadAt(raw[:], int64(latestBinaryBTreeHeaderSize+index*8)); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(raw[:]), nil
}

func readLatestBinaryBTreeEntryAtOffset(r io.ReaderAt, offset, fileSize uint64) (latestBinaryBTreeEntry, error) {
	if offset > math.MaxInt64 {
		return latestBinaryBTreeEntry{}, fmt.Errorf("snapshots: latest btree offset too large: %d", offset)
	}
	if _, err := latestBinaryBoundedRange("btree entry header", offset, 20, fileSize); err != nil {
		return latestBinaryBTreeEntry{}, err
	}
	var head [20]byte
	if _, err := r.ReadAt(head[:], int64(offset)); err != nil {
		return latestBinaryBTreeEntry{}, err
	}
	keyLen := binary.BigEndian.Uint32(head[:4])
	keyOffset := offset + uint64(len(head))
	if _, err := latestBinaryBoundedRange("btree entry key", keyOffset, uint64(keyLen), fileSize); err != nil {
		return latestBinaryBTreeEntry{}, err
	}
	if keyOffset > math.MaxInt64 {
		return latestBinaryBTreeEntry{}, fmt.Errorf("snapshots: latest btree key offset too large: %d", keyOffset)
	}
	key := make([]byte, keyLen)
	if keyLen > 0 {
		if _, err := r.ReadAt(key, int64(keyOffset)); err != nil {
			return latestBinaryBTreeEntry{}, err
		}
	}
	return latestBinaryBTreeEntry{
		key:           key,
		ordinal:       binary.BigEndian.Uint64(head[4:12]),
		segmentOffset: binary.BigEndian.Uint64(head[12:20]),
	}, nil
}

func latestBinaryBTreeFloor(segment io.ReaderAt, btree io.ReaderAt, count uint64, key []byte, btreeFileSize uint64) (latestBinaryBTreeEntry, bool, error) {
	if count == 0 {
		return latestBinaryBTreeEntry{}, false, nil
	}
	var foundErr error
	i := sort.Search(int(count), func(i int) bool {
		if foundErr != nil {
			return true
		}
		entry, ok, err := readLatestBinaryBTreeEntryAt(btree, uint64(i), btreeFileSize)
		if err != nil {
			foundErr = err
			return true
		}
		if !ok {
			foundErr = io.ErrUnexpectedEOF
			return true
		}
		return bytes.Compare(entry.key, key) > 0
	})
	if foundErr != nil {
		return latestBinaryBTreeEntry{}, false, foundErr
	}
	if i == 0 {
		first, ok, err := readLatestBinaryBTreeEntryAt(btree, 0, btreeFileSize)
		if err != nil || !ok {
			return latestBinaryBTreeEntry{}, false, err
		}
		if bytes.Compare(first.key, key) > 0 {
			return latestBinaryBTreeEntry{}, false, nil
		}
		return first, true, nil
	}
	entry, ok, err := readLatestBinaryBTreeEntryAt(btree, uint64(i-1), btreeFileSize)
	if err != nil || !ok {
		return latestBinaryBTreeEntry{}, false, err
	}
	if entry.segmentOffset >= latestBinaryHeaderSize {
		return entry, true, nil
	}
	return latestBinaryBTreeEntry{}, false, fmt.Errorf("snapshots: latest btree entry has invalid segment offset %d", entry.segmentOffset)
}

func latestBinaryAccessorLowerBoundFile(segment io.ReaderAt, accessor io.ReaderAt, count uint64, key []byte, maxEnd uint64, compressedValues bool) (int, bool, error) {
	var foundErr error
	i := sort.Search(int(count), func(i int) bool {
		if foundErr != nil {
			return true
		}
		offset, err := readLatestBinaryAccessorOffsetAt(accessor, uint64(i))
		if err != nil {
			foundErr = err
			return true
		}
		entryKey, _, err := readLatestBinaryEntryKeyAt(segment, offset, maxEnd, compressedValues)
		if err != nil {
			foundErr = err
			return true
		}
		return bytes.Compare(entryKey, key) >= 0
	})
	if foundErr != nil {
		return 0, false, foundErr
	}
	return i, uint64(i) < count, nil
}

func readLatestBinaryAccessorOffsetAt(r io.ReaderAt, i uint64) (uint64, error) {
	if i > (math.MaxInt64-latestBinaryAccessorHeaderSize)/8 {
		return 0, fmt.Errorf("snapshots: latest binary accessor index too large: %d", i)
	}
	var raw [8]byte
	if _, err := r.ReadAt(raw[:], int64(latestBinaryAccessorHeaderSize+i*8)); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(raw[:]), nil
}

func readLatestBinaryValueBytes(file *os.File, frame latestBinaryValueFrame, maxEnd uint64) ([]byte, error) {
	pos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	if pos < 0 {
		return nil, fmt.Errorf("snapshots: latest binary negative offset %d", pos)
	}
	if _, err := latestBinaryBoundedRange("entry value", uint64(pos), uint64(frame.encodedLen), maxEnd); err != nil {
		return nil, err
	}
	if err := validateLatestBinaryValueFrame(frame); err != nil {
		return nil, err
	}
	encoded := make([]byte, frame.encodedLen)
	if frame.encodedLen > 0 {
		if _, err := io.ReadFull(file, encoded); err != nil {
			return nil, err
		}
	}
	return decodeLatestBinaryValue(encoded, frame)
}

func skipLatestBinaryValue(file *os.File, frame latestBinaryValueFrame, maxEnd uint64) error {
	if frame.encodedLen == 0 {
		return nil
	}
	pos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if pos < 0 {
		return fmt.Errorf("snapshots: latest binary negative offset %d", pos)
	}
	if _, err := latestBinaryBoundedRange("entry value", uint64(pos), uint64(frame.encodedLen), maxEnd); err != nil {
		return err
	}
	_, err = file.Seek(int64(frame.encodedLen), io.SeekCurrent)
	return err
}

func collectLatestBinaryOffsets(file *os.File, header latestBinaryHeader) ([]uint64, error) {
	offsets := make([]uint64, 0, header.count)
	var prev []byte
	for i := uint64(0); i < header.count; i++ {
		pos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		if pos < 0 {
			return nil, fmt.Errorf("snapshots: latest binary negative offset %d", pos)
		}
		key, valueLen, err := readLatestBinaryEntryKey(file, header.fileSize, header.compressedValues)
		if err != nil {
			return nil, fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, key) >= 0 {
			return nil, errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		offsets = append(offsets, uint64(pos))
		if err := skipLatestBinaryValue(file, valueLen, header.fileSize); err != nil {
			return nil, err
		}
		prev = key
	}
	return offsets, nil
}

func decodeLatestBinaryEntry(data []byte, compressedValues bool) (LatestEntry, []byte, error) {
	if len(data) < 8 {
		return LatestEntry{}, nil, io.ErrUnexpectedEOF
	}
	keyLen := binary.BigEndian.Uint32(data[:4])
	frame, err := latestBinaryValueFrameFromStoredLength(binary.BigEndian.Uint32(data[4:8]), compressedValues)
	if err != nil {
		return LatestEntry{}, nil, err
	}
	end := 8 + uint64(keyLen) + uint64(frame.encodedLen)
	if uint64(len(data)) < end {
		return LatestEntry{}, nil, io.ErrUnexpectedEOF
	}
	value, err := decodeLatestBinaryValue(data[8+keyLen:end], frame)
	if err != nil {
		return LatestEntry{}, nil, err
	}
	entry := LatestEntry{
		Key:   append([]byte(nil), data[8:8+keyLen]...),
		Value: append([]byte(nil), value...),
	}
	return entry, data[end:], nil
}

func validateLatestBinaryRefMetadata(path string, ref SegmentRef, seg *LatestSegment) error {
	return validateLatestBinaryHeaderRefMetadata(path, ref, latestBinaryHeader{
		dataset:   seg.normalizedDataset(),
		domain:    seg.Domain,
		kind:      SegmentLatest,
		fromTxNum: seg.FromTxNum,
		toTxNum:   seg.ToTxNum,
		count:     uint64(len(seg.Entries)),
	})
}

func validateLatestBinaryHeaderRefMetadata(path string, ref SegmentRef, header latestBinaryHeader) error {
	if ref.Dataset != "" && header.dataset != ref.normalizedDataset() {
		return fmt.Errorf("snapshots: latest binary segment %q dataset %q, want %q", path, header.dataset, ref.normalizedDataset())
	}
	if ref.Domain != 0 && header.domain != ref.Domain {
		return fmt.Errorf("snapshots: latest binary segment %q domain %#04x, want %#04x", path, uint16(header.domain), uint16(ref.Domain))
	}
	if ref.Kind != "" && header.kind != ref.Kind {
		return fmt.Errorf("snapshots: latest binary segment %q kind %q, want %q", path, header.kind, ref.Kind)
	}
	if ref.FromTxNum != 0 || ref.ToTxNum != 0 {
		if header.fromTxNum != ref.FromTxNum || header.toTxNum != ref.ToTxNum {
			return fmt.Errorf("snapshots: latest binary segment %q range [%d,%d], want [%d,%d]", path, header.fromTxNum, header.toTxNum, ref.FromTxNum, ref.ToTxNum)
		}
	}
	return nil
}

func validateLatestBinaryAccessorMatchesSegment(path string, ref SegmentRef, segment latestBinaryHeader, accessor latestBinaryAccessorHeader) error {
	if err := validateLatestBinaryCompanionMatchesSegment(path, ref, segment, accessor, SegmentAccessor); err != nil {
		return err
	}
	if accessor.count != segment.count {
		return fmt.Errorf("snapshots: latest binary accessor for %q count %d, want %d", path, accessor.count, segment.count)
	}
	return nil
}

func verifyLatestBinaryAccessorOffsetsAgainstSegment(path string, segment io.ReaderAt, segmentHeader latestBinaryHeader, accessor io.ReaderAt, accessorHeader latestBinaryAccessorHeader) error {
	return verifyLatestBinaryAccessorOffsetsAgainstSegmentContext(context.Background(), path, segment, segmentHeader, accessor, accessorHeader)
}

func verifyLatestBinaryAccessorOffsetsAgainstSegmentContext(ctx context.Context, path string, segment io.ReaderAt, segmentHeader latestBinaryHeader, accessor io.ReaderAt, accessorHeader latestBinaryAccessorHeader) error {
	if err := validateLatestBinaryAccessorMatchesSegment(path, SegmentRef{
		Dataset:   segmentHeader.dataset,
		Domain:    segmentHeader.domain,
		Kind:      SegmentLatest,
		FromTxNum: segmentHeader.fromTxNum,
		ToTxNum:   segmentHeader.toTxNum,
	}, segmentHeader, accessorHeader); err != nil {
		return err
	}
	segmentReader := newLatestBinaryWindowReader(segment, segmentHeader.fileSize)
	accessorReader := newLatestBinaryWindowReader(accessor, accessorHeader.fileSize)
	segmentOffset := uint64(latestBinaryHeaderSize)
	for i := uint64(0); i < segmentHeader.count; i++ {
		if i%latestBinaryBTreeBlockSize == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		got, err := readLatestBinaryAccessorOffsetAt(accessorReader, i)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary accessor offset %d: %w", i, err)
		}
		if got != segmentOffset {
			return fmt.Errorf("snapshots: latest binary accessor for %q offset %d=%d, want segment record offset %d", path, i, got, segmentOffset)
		}
		keyLen, valueFrame, err := readLatestBinaryEntryHeaderAt(segmentReader, segmentOffset, segmentHeader.fileSize, segmentHeader.compressedValues)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		valueOffset := segmentOffset + 8 + uint64(keyLen)
		nextOffset, err := latestBinaryBoundedRange("entry value", valueOffset, uint64(valueFrame.encodedLen), segmentHeader.fileSize)
		if err != nil {
			return fmt.Errorf("snapshots: skip latest binary value %d: %w", i, err)
		}
		segmentOffset = nextOffset
	}
	return nil
}

func verifyLatestBinaryBTreeAgainstSegment(path string, segment io.ReaderAt, segmentHeader latestBinaryHeader, btree io.ReaderAt, btreeHeader latestBinaryBTreeHeader) error {
	return verifyLatestBinaryBTreeAgainstSegmentContext(context.Background(), path, segment, segmentHeader, btree, btreeHeader)
}

func verifyLatestBinaryBTreeAgainstSegmentContext(ctx context.Context, path string, segment io.ReaderAt, segmentHeader latestBinaryHeader, btree io.ReaderAt, btreeHeader latestBinaryBTreeHeader) error {
	if err := validateLatestBinaryCompanionMatchesSegment(path, SegmentRef{
		Dataset:   segmentHeader.dataset,
		Domain:    segmentHeader.domain,
		Kind:      SegmentLatest,
		FromTxNum: segmentHeader.fromTxNum,
		ToTxNum:   segmentHeader.toTxNum,
	}, segmentHeader, btreeHeader.latestBinaryAccessorHeader, SegmentBTree); err != nil {
		return err
	}
	var expectedEntries uint64
	if segmentHeader.count > 0 {
		expectedEntries = (segmentHeader.count + btreeHeader.blockSize - 1) / btreeHeader.blockSize
	}
	if btreeHeader.count != expectedEntries {
		return fmt.Errorf("snapshots: latest binary btree for %q entries=%d, want %d for %d segment records and block size %d", path, btreeHeader.count, expectedEntries, segmentHeader.count, btreeHeader.blockSize)
	}
	segmentReader := newLatestBinaryWindowReader(segment, segmentHeader.fileSize)
	segmentOffset := uint64(latestBinaryHeaderSize)
	var btreeIndex uint64
	for ordinal := uint64(0); ordinal < segmentHeader.count; ordinal++ {
		if ordinal%latestBinaryBTreeBlockSize == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		keyLen, valueFrame, err := readLatestBinaryEntryHeaderAt(segmentReader, segmentOffset, segmentHeader.fileSize, segmentHeader.compressedValues)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary key %d: %w", ordinal, err)
		}
		if ordinal%btreeHeader.blockSize == 0 {
			key := make([]byte, keyLen)
			if keyLen > 0 {
				if _, err := segmentReader.ReadAt(key, int64(segmentOffset+8)); err != nil {
					return fmt.Errorf("snapshots: decode latest binary key %d: %w", ordinal, err)
				}
			}
			entry, ok, err := readLatestBinaryBTreeEntryAt(btree, btreeIndex, btreeHeader.fileSize)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("snapshots: latest binary btree for %q missing entry %d", path, btreeIndex)
			}
			if entry.ordinal != ordinal {
				return fmt.Errorf("snapshots: latest binary btree for %q entry %d ordinal=%d, want %d", path, btreeIndex, entry.ordinal, ordinal)
			}
			if entry.segmentOffset != segmentOffset {
				return fmt.Errorf("snapshots: latest binary btree for %q entry %d offset=%d, want segment record offset %d", path, btreeIndex, entry.segmentOffset, segmentOffset)
			}
			if !bytes.Equal(entry.key, key) {
				return fmt.Errorf("snapshots: latest binary btree for %q entry %d key mismatch", path, btreeIndex)
			}
			btreeIndex++
		}
		valueOffset := segmentOffset + 8 + uint64(keyLen)
		nextOffset, err := latestBinaryBoundedRange("entry value", valueOffset, uint64(valueFrame.encodedLen), segmentHeader.fileSize)
		if err != nil {
			return fmt.Errorf("snapshots: skip latest binary value %d: %w", ordinal, err)
		}
		segmentOffset = nextOffset
	}
	return nil
}

func validateLatestBinaryCompanionMatchesSegment(path string, ref SegmentRef, segment latestBinaryHeader, accessor latestBinaryAccessorHeader, wantKind SegmentKind) error {
	if accessor.kind != wantKind {
		return fmt.Errorf("snapshots: latest binary accessor for %q kind %q, want %q", path, accessor.kind, wantKind)
	}
	if accessor.dataset != segment.dataset || accessor.domain != segment.domain {
		return fmt.Errorf("snapshots: latest binary accessor for %q domain mismatch: %s/%#04x vs %s/%#04x", path, accessor.dataset, uint16(accessor.domain), segment.dataset, uint16(segment.domain))
	}
	if accessor.fromTxNum != segment.fromTxNum || accessor.toTxNum != segment.toTxNum {
		return fmt.Errorf("snapshots: latest binary accessor for %q range mismatch", path)
	}
	if ref.Size != 0 && accessor.segmentSize != ref.Size {
		return fmt.Errorf("snapshots: latest binary accessor for %q segment size %d, want %d", path, accessor.segmentSize, ref.Size)
	}
	if ref.Checksum != "" {
		checksum, err := latestBinaryChecksumBytes(ref.Checksum)
		if err != nil {
			return err
		}
		if accessor.segmentChecksum != checksum {
			return fmt.Errorf("snapshots: latest binary accessor for %q segment checksum mismatch", path)
		}
	}
	return nil
}

func verifyLatestBinaryRef(path string, ref SegmentRef, data []byte) error {
	if ref.Size != 0 && uint64(len(data)) != ref.Size {
		return fmt.Errorf("snapshots: segment %q size %d, want %d", path, len(data), ref.Size)
	}
	if ref.Checksum != "" {
		_, got := latestBinaryMetadata(data)
		if !strings.EqualFold(got, ref.Checksum) {
			return fmt.Errorf("snapshots: segment %q checksum %s, want %s", path, got, ref.Checksum)
		}
	}
	return nil
}

func verifyLatestBinaryFileRef(path string, ref SegmentRef) error {
	if ref.Size == 0 && ref.Checksum == "" {
		return nil
	}
	size, checksum, _, err := latestBinaryFileMetadata(path)
	if err != nil {
		return err
	}
	if ref.Size != 0 && size != ref.Size {
		return fmt.Errorf("snapshots: segment %q size %d, want %d", path, size, ref.Size)
	}
	if ref.Checksum != "" && !strings.EqualFold(checksum, ref.Checksum) {
		return fmt.Errorf("snapshots: segment %q checksum %s, want %s", path, checksum, ref.Checksum)
	}
	return nil
}

func latestBinaryChecksumBytes(checksum string) ([sha256.Size]byte, error) {
	var out [sha256.Size]byte
	const prefix = "sha256:"
	if !strings.HasPrefix(strings.ToLower(checksum), prefix) {
		return out, fmt.Errorf("snapshots: unsupported checksum %q", checksum)
	}
	raw, err := hex.DecodeString(checksum[len(prefix):])
	if err != nil {
		return out, err
	}
	if len(raw) != sha256.Size {
		return out, fmt.Errorf("snapshots: checksum length %d, want %d", len(raw), sha256.Size)
	}
	copy(out[:], raw)
	return out, nil
}

func latestBinaryMetadata(data []byte) (uint64, string) {
	sum := sha256.Sum256(data)
	return uint64(len(data)), "sha256:" + hex.EncodeToString(sum[:])
}

func latestBinaryFileMetadata(path string) (uint64, string, [sha256.Size]byte, error) {
	var checksum [sha256.Size]byte
	file, err := os.Open(path)
	if err != nil {
		return 0, "", checksum, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return 0, "", checksum, err
	}
	sum := hash.Sum(nil)
	copy(checksum[:], sum)
	return uint64(size), "sha256:" + hex.EncodeToString(sum), checksum, nil
}

func writeLatestBinaryFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func latestBinaryDatasetCode(dataset SegmentDataset) (uint16, error) {
	switch dataset {
	case SegmentDatasetAccountLatest:
		return 1, nil
	case SegmentDatasetKVLatest:
		return 2, nil
	case SegmentDatasetKVGeneration:
		return 3, nil
	case SegmentDatasetCode:
		return 6, nil
	case SegmentDatasetCommitmentRoot:
		return 4, nil
	case SegmentDatasetCommitmentCheckpoint:
		return 7, nil
	case SegmentDatasetCommitmentBranch:
		return 8, nil
	default:
		return 0, fmt.Errorf("snapshots: unknown latest binary dataset %q", dataset)
	}
}

func latestBinaryDataset(code uint16) (SegmentDataset, error) {
	switch code {
	case 1:
		return SegmentDatasetAccountLatest, nil
	case 2:
		return SegmentDatasetKVLatest, nil
	case 3:
		return SegmentDatasetKVGeneration, nil
	case 6:
		return SegmentDatasetCode, nil
	case 4:
		return SegmentDatasetCommitmentRoot, nil
	case 7:
		return SegmentDatasetCommitmentCheckpoint, nil
	case 8:
		return SegmentDatasetCommitmentBranch, nil
	default:
		return "", fmt.Errorf("snapshots: unknown latest binary dataset code %d", code)
	}
}

func latestBinaryKindCode(kind SegmentKind) (uint16, error) {
	switch kind {
	case SegmentLatest:
		return 1, nil
	case SegmentAccessor:
		return 2, nil
	case SegmentBTree:
		return 3, nil
	default:
		return 0, fmt.Errorf("snapshots: unknown latest binary kind %q", kind)
	}
}

func latestBinaryKind(code uint16) (SegmentKind, error) {
	switch code {
	case 1:
		return SegmentLatest, nil
	case 2:
		return SegmentAccessor, nil
	case 3:
		return SegmentBTree, nil
	default:
		return "", fmt.Errorf("snapshots: unknown latest binary kind code %d", code)
	}
}
