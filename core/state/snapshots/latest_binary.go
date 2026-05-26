package snapshots

import (
	"bytes"
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
)

var (
	latestBinarySegmentMagic  = [8]byte{'g', 't', 'l', 'a', 't', 's', 'e', 'g'}
	latestBinaryAccessorMagic = [8]byte{'g', 't', 'l', 'a', 't', 'i', 'd', 'x'}
	latestBinaryBTreeMagic    = [8]byte{'g', 't', 'l', 'a', 't', 'b', 't', '1'}
)

type latestBinaryHeader struct {
	dataset   SegmentDataset
	domain    kvdomains.KVDomain
	kind      SegmentKind
	fromTxNum uint64
	toTxNum   uint64
	count     uint64
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
	offsets, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".offsets-*.tmp")
	if err != nil {
		_ = tmp.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	offsetsName := offsets.Name()
	defer os.Remove(offsetsName)
	btreePayload, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".btree-*.tmp")
	if err != nil {
		_ = tmp.Close()
		_ = offsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	btreePayloadName := btreePayload.Name()
	defer os.Remove(btreePayloadName)
	btreeOffsets, err := os.CreateTemp(filepath.Dir(abs), "."+filepath.Base(abs)+".btree-offsets-*.tmp")
	if err != nil {
		_ = tmp.Close()
		_ = offsets.Close()
		_ = btreePayload.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	btreeOffsetsName := btreeOffsets.Name()
	defer os.Remove(btreeOffsetsName)

	if err := writeLatestBinaryHeaderTo(tmp, ref.normalizedDataset(), ref.Domain, SegmentLatest, ref.FromTxNum, ref.ToTxNum, 0); err != nil {
		_ = tmp.Close()
		_ = offsets.Close()
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	var count uint64
	var btreeCount uint64
	var prev []byte
	err = iter(func(entry LatestEntry) error {
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
		var off [8]byte
		binary.BigEndian.PutUint64(off[:], uint64(pos))
		if _, err := offsets.Write(off[:]); err != nil {
			return err
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
		_ = tmp.Close()
		_ = offsets.Close()
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if ref.normalizedDataset() == SegmentDatasetCommitmentRoot && count != 1 {
		_ = tmp.Close()
		_ = offsets.Close()
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, fmt.Errorf("snapshots: commitment root segment entries = %d, want 1", count)
	}
	var countBuf [8]byte
	binary.BigEndian.PutUint64(countBuf[:], count)
	if _, err := tmp.WriteAt(countBuf[:], 40); err != nil {
		_ = tmp.Close()
		_ = offsets.Close()
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = offsets.Close()
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := tmp.Close(); err != nil {
		_ = offsets.Close()
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := offsets.Sync(); err != nil {
		_ = offsets.Close()
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := offsets.Close(); err != nil {
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := btreePayload.Sync(); err != nil {
		_ = btreePayload.Close()
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := btreePayload.Close(); err != nil {
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := btreeOffsets.Sync(); err != nil {
		_ = btreeOffsets.Close()
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}
	if err := btreeOffsets.Close(); err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
	}

	size, checksum, checksumBytes, err := latestBinaryFileMetadata(tmpName)
	if err != nil {
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

	accessorRef, err := writeLatestBinaryAccessorFromOffsetsFile(dir, segRef, checksumBytes, offsetsName, count)
	if err != nil {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, err
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
		key, valueLen, err := readLatestBinaryEntryKey(file)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, key) >= 0 {
			return errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		value, err := readLatestBinaryValueBytes(file, valueLen)
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
		key, valueLen, err := readLatestBinaryEntryKey(file)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, key) >= 0 {
			return errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		value, err := readLatestBinaryValueBytes(file, valueLen)
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
		entryKey, valueLen, err := readLatestBinaryEntryKey(file)
		if err != nil {
			return nil, false, fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, entryKey) >= 0 {
			return nil, false, errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		cmp := bytes.Compare(entryKey, key)
		switch {
		case cmp == 0:
			value, err := readLatestBinaryValueBytes(file, valueLen)
			if err != nil {
				return nil, false, fmt.Errorf("snapshots: decode latest binary value %d: %w", i, err)
			}
			return value, true, nil
		case cmp > 0:
			return nil, false, nil
		default:
			if err := skipLatestBinaryValue(file, valueLen); err != nil {
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
	i, ok, err := latestBinaryAccessorLowerBound(file, accessor.offsets, key)
	if err != nil || !ok {
		return nil, false, err
	}
	entryKey, value, err := readLatestBinaryEntryAt(file, accessor.offsets[i])
	if err != nil {
		return nil, false, err
	}
	if !bytes.Equal(entryKey, key) {
		return nil, false, nil
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
		key, valueLen, err := readLatestBinaryEntryKey(file)
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
			if err := skipLatestBinaryValue(file, valueLen); err != nil {
				return fmt.Errorf("snapshots: skip latest binary value %d: %w", i, err)
			}
			prev = key
			continue
		}
		value, err := readLatestBinaryValueBytes(file, valueLen)
		if err != nil {
			return fmt.Errorf("snapshots: decode latest binary value %d: %w", i, err)
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
		i, _, err := latestBinaryAccessorLowerBound(file, accessor.offsets, prefix)
		if err != nil {
			return err
		}
		start = i
	}
	for i := start; i < len(accessor.offsets); i++ {
		key, value, err := readLatestBinaryEntryAt(file, accessor.offsets[i])
		if err != nil {
			return err
		}
		if len(prefix) > 0 && !bytes.HasPrefix(key, prefix) {
			if bytes.Compare(key, prefix) > 0 {
				return nil
			}
			continue
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
	i, ok, err := latestBinaryAccessorLowerBoundFile(segFile, accessorFile, accessorHeader.count, key)
	if err != nil || !ok {
		return nil, false, err
	}
	offset, err := readLatestBinaryAccessorOffsetAt(accessorFile, uint64(i))
	if err != nil {
		return nil, false, err
	}
	entryKey, value, err := readLatestBinaryEntryAt(segFile, offset)
	if err != nil {
		return nil, false, err
	}
	if !bytes.Equal(entryKey, key) {
		return nil, false, nil
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
		i, _, err := latestBinaryAccessorLowerBoundFile(segFile, accessorFile, accessorHeader.count, prefix)
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
		key, value, err := readLatestBinaryEntryAt(segFile, offset)
		if err != nil {
			return err
		}
		if len(prefix) > 0 && !bytes.HasPrefix(key, prefix) {
			if bytes.Compare(key, prefix) > 0 {
				return nil
			}
			continue
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
	entry, ok, err := latestBinaryBTreeFloor(segFile, btreeFile, btreeHeader.count, key)
	if err != nil || !ok {
		return nil, false, err
	}
	limit := min(segHeader.count, entry.ordinal+btreeHeader.blockSize)
	offset := entry.segmentOffset
	for ordinal := entry.ordinal; ordinal < limit; ordinal++ {
		entryKey, value, next, err := readLatestBinaryEntryAtWithNext(segFile, offset)
		if err != nil {
			return nil, false, err
		}
		cmp := bytes.Compare(entryKey, key)
		if cmp == 0 {
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
		entry, ok, err = readLatestBinaryBTreeEntryAt(btreeFile, 0)
		if err != nil || !ok {
			return err
		}
	} else {
		var ok bool
		entry, ok, err = latestBinaryBTreeFloor(segFile, btreeFile, btreeHeader.count, prefix)
		if err != nil {
			return err
		}
		if !ok {
			entry, ok, err = readLatestBinaryBTreeEntryAt(btreeFile, 0)
			if err != nil || !ok {
				return err
			}
		}
	}
	offset := entry.segmentOffset
	for ordinal := entry.ordinal; ordinal < segHeader.count; ordinal++ {
		key, value, next, err := readLatestBinaryEntryAtWithNext(segFile, offset)
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
	if ref.Size != 0 {
		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, latestBinaryHeader{}, err
		}
		if uint64(stat.Size()) != ref.Size {
			_ = file.Close()
			return nil, latestBinaryHeader{}, fmt.Errorf("snapshots: segment %q size %d, want %d", path, stat.Size(), ref.Size)
		}
	}
	header, err := readLatestBinaryHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryHeader{}, err
	}
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
	writeUint16(&buf, 0)
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
		entry, next, err := decodeLatestBinaryEntry(rest)
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
	if flags != 0 {
		return latestBinaryHeader{}, nil, fmt.Errorf("snapshots: unsupported latest binary flags %#04x", flags)
	}
	return latestBinaryHeader{
		dataset:   dataset,
		domain:    kvdomains.KVDomain(binary.BigEndian.Uint16(data[18:20])),
		kind:      kind,
		fromTxNum: binary.BigEndian.Uint64(data[24:32]),
		toTxNum:   binary.BigEndian.Uint64(data[32:40]),
		count:     binary.BigEndian.Uint64(data[40:48]),
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

func readLatestBinaryHeader(r io.Reader) (latestBinaryHeader, error) {
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
	if ref.Size != 0 {
		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, latestBinaryAccessorHeader{}, err
		}
		if uint64(stat.Size()) != ref.Size {
			_ = file.Close()
			return nil, latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: accessor %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
		}
	}
	header, err := readLatestBinaryAccessorHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryAccessorHeader{}, err
	}
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
	if ref.Size != 0 {
		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, latestBinaryBTreeHeader{}, err
		}
		if uint64(stat.Size()) != ref.Size {
			_ = file.Close()
			return nil, latestBinaryBTreeHeader{}, fmt.Errorf("snapshots: latest binary btree %q size %d, want %d", ref.Path, stat.Size(), ref.Size)
		}
	}
	header, err := readLatestBinaryBTreeHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, latestBinaryBTreeHeader{}, err
	}
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

func readLatestBinaryAccessorHeader(r io.Reader) (latestBinaryAccessorHeader, error) {
	fixed := make([]byte, latestBinaryAccessorHeaderSize)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return latestBinaryAccessorHeader{}, err
	}
	headerSize := binary.BigEndian.Uint32(fixed[12:16])
	if headerSize < latestBinaryAccessorHeaderSize {
		return latestBinaryAccessorHeader{}, fmt.Errorf("snapshots: latest binary accessor header size %d, want at least %d", headerSize, latestBinaryAccessorHeaderSize)
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

func readLatestBinaryBTreeHeader(r io.Reader) (latestBinaryBTreeHeader, error) {
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
	if len(entry.Value) > math.MaxUint32 {
		return fmt.Errorf("snapshots: latest binary record %d value is too large: %d bytes", index, len(entry.Value))
	}
	var lens [8]byte
	binary.BigEndian.PutUint32(lens[:4], uint32(len(entry.Key)))
	binary.BigEndian.PutUint32(lens[4:], uint32(len(entry.Value)))
	if _, err := w.Write(lens[:]); err != nil {
		return err
	}
	if _, err := w.Write(entry.Key); err != nil {
		return err
	}
	_, err := w.Write(entry.Value)
	return err
}

func readLatestBinaryEntryKey(r io.Reader) ([]byte, uint32, error) {
	var lens [8]byte
	if _, err := io.ReadFull(r, lens[:]); err != nil {
		return nil, 0, err
	}
	keyLen := binary.BigEndian.Uint32(lens[:4])
	valueLen := binary.BigEndian.Uint32(lens[4:])
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, 0, err
	}
	return key, valueLen, nil
}

func readLatestBinaryEntryKeyAt(r io.ReaderAt, offset uint64) ([]byte, uint32, error) {
	if offset > math.MaxInt64 {
		return nil, 0, fmt.Errorf("snapshots: latest binary offset too large: %d", offset)
	}
	var lens [8]byte
	if _, err := r.ReadAt(lens[:], int64(offset)); err != nil {
		return nil, 0, err
	}
	keyLen := binary.BigEndian.Uint32(lens[:4])
	valueLen := binary.BigEndian.Uint32(lens[4:])
	key := make([]byte, keyLen)
	if _, err := r.ReadAt(key, int64(offset)+8); err != nil {
		return nil, 0, err
	}
	return key, valueLen, nil
}

func readLatestBinaryEntryAt(r io.ReaderAt, offset uint64) ([]byte, []byte, error) {
	key, valueLen, err := readLatestBinaryEntryKeyAt(r, offset)
	if err != nil {
		return nil, nil, err
	}
	valueOffset := offset + 8 + uint64(len(key))
	if valueOffset > math.MaxInt64 {
		return nil, nil, fmt.Errorf("snapshots: latest binary value offset too large: %d", valueOffset)
	}
	value := make([]byte, valueLen)
	if _, err := r.ReadAt(value, int64(valueOffset)); err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

func readLatestBinaryEntryAtWithNext(r io.ReaderAt, offset uint64) ([]byte, []byte, uint64, error) {
	key, valueLen, err := readLatestBinaryEntryKeyAt(r, offset)
	if err != nil {
		return nil, nil, 0, err
	}
	valueOffset := offset + 8 + uint64(len(key))
	if valueOffset > math.MaxInt64 {
		return nil, nil, 0, fmt.Errorf("snapshots: latest binary value offset too large: %d", valueOffset)
	}
	value := make([]byte, valueLen)
	if _, err := r.ReadAt(value, int64(valueOffset)); err != nil {
		return nil, nil, 0, err
	}
	return key, value, valueOffset + uint64(valueLen), nil
}

func latestBinaryAccessorLowerBound(r io.ReaderAt, offsets []uint64, key []byte) (int, bool, error) {
	var foundErr error
	i := sort.Search(len(offsets), func(i int) bool {
		if foundErr != nil {
			return true
		}
		entryKey, _, err := readLatestBinaryEntryKeyAt(r, offsets[i])
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

func readLatestBinaryBTreeEntryAt(r io.ReaderAt, index uint64) (latestBinaryBTreeEntry, bool, error) {
	offset, err := readLatestBinaryBTreeEntryOffsetAt(r, index)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return latestBinaryBTreeEntry{}, false, nil
		}
		return latestBinaryBTreeEntry{}, false, err
	}
	entry, err := readLatestBinaryBTreeEntryAtOffset(r, offset)
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

func readLatestBinaryBTreeEntryAtOffset(r io.ReaderAt, offset uint64) (latestBinaryBTreeEntry, error) {
	if offset > math.MaxInt64 {
		return latestBinaryBTreeEntry{}, fmt.Errorf("snapshots: latest btree offset too large: %d", offset)
	}
	var head [20]byte
	if _, err := r.ReadAt(head[:], int64(offset)); err != nil {
		return latestBinaryBTreeEntry{}, err
	}
	keyLen := binary.BigEndian.Uint32(head[:4])
	keyOffset := offset + uint64(len(head))
	if keyOffset > math.MaxInt64 {
		return latestBinaryBTreeEntry{}, fmt.Errorf("snapshots: latest btree key offset too large: %d", keyOffset)
	}
	key := make([]byte, keyLen)
	if _, err := r.ReadAt(key, int64(keyOffset)); err != nil {
		return latestBinaryBTreeEntry{}, err
	}
	return latestBinaryBTreeEntry{
		key:           key,
		ordinal:       binary.BigEndian.Uint64(head[4:12]),
		segmentOffset: binary.BigEndian.Uint64(head[12:20]),
	}, nil
}

func latestBinaryBTreeFloor(segment io.ReaderAt, btree io.ReaderAt, count uint64, key []byte) (latestBinaryBTreeEntry, bool, error) {
	if count == 0 {
		return latestBinaryBTreeEntry{}, false, nil
	}
	var foundErr error
	i := sort.Search(int(count), func(i int) bool {
		if foundErr != nil {
			return true
		}
		entry, ok, err := readLatestBinaryBTreeEntryAt(btree, uint64(i))
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
		first, ok, err := readLatestBinaryBTreeEntryAt(btree, 0)
		if err != nil || !ok {
			return latestBinaryBTreeEntry{}, false, err
		}
		if bytes.Compare(first.key, key) > 0 {
			return latestBinaryBTreeEntry{}, false, nil
		}
		return first, true, nil
	}
	entry, ok, err := readLatestBinaryBTreeEntryAt(btree, uint64(i-1))
	if err != nil || !ok {
		return latestBinaryBTreeEntry{}, false, err
	}
	if entry.segmentOffset >= latestBinaryHeaderSize {
		return entry, true, nil
	}
	return latestBinaryBTreeEntry{}, false, fmt.Errorf("snapshots: latest btree entry has invalid segment offset %d", entry.segmentOffset)
}

func latestBinaryAccessorLowerBoundFile(segment io.ReaderAt, accessor io.ReaderAt, count uint64, key []byte) (int, bool, error) {
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
		entryKey, _, err := readLatestBinaryEntryKeyAt(segment, offset)
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

func readLatestBinaryValueBytes(r io.Reader, valueLen uint32) ([]byte, error) {
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, err
	}
	return value, nil
}

func skipLatestBinaryValue(file *os.File, valueLen uint32) error {
	if valueLen == 0 {
		return nil
	}
	_, err := file.Seek(int64(valueLen), io.SeekCurrent)
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
		key, valueLen, err := readLatestBinaryEntryKey(file)
		if err != nil {
			return nil, fmt.Errorf("snapshots: decode latest binary key %d: %w", i, err)
		}
		if len(prev) > 0 && bytes.Compare(prev, key) >= 0 {
			return nil, errors.New("snapshots: latest binary entries are not strictly sorted")
		}
		offsets = append(offsets, uint64(pos))
		if err := skipLatestBinaryValue(file, valueLen); err != nil {
			return nil, err
		}
		prev = key
	}
	return offsets, nil
}

func decodeLatestBinaryEntry(data []byte) (LatestEntry, []byte, error) {
	if len(data) < 8 {
		return LatestEntry{}, nil, io.ErrUnexpectedEOF
	}
	keyLen := binary.BigEndian.Uint32(data[:4])
	valueLen := binary.BigEndian.Uint32(data[4:8])
	end := 8 + uint64(keyLen) + uint64(valueLen)
	if uint64(len(data)) < end {
		return LatestEntry{}, nil, io.ErrUnexpectedEOF
	}
	entry := LatestEntry{
		Key:   append([]byte(nil), data[8:8+keyLen]...),
		Value: append([]byte(nil), data[8+keyLen:end]...),
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
	case SegmentDatasetCommitmentNode:
		return 5, nil
	case SegmentDatasetCommitmentCheckpoint:
		return 7, nil
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
	case 5:
		return SegmentDatasetCommitmentNode, nil
	case 7:
		return SegmentDatasetCommitmentCheckpoint, nil
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
