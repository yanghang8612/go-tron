package freezer

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	v2Magic           = "gtanv201"
	v2Version         = uint32(1)
	v2HeaderSize      = 64
	v2FrameEntrySize  = 48
	v2DefaultFrames   = uint32(64)
	v2DefaultSegments = uint64(65_536)
	v2CacheFrames     = 16
)

var v2CRC = crc32.MakeTable(crc32.Castagnoli)

type v2FrameEntry struct {
	firstRecord     uint64
	records         uint32
	compressedStart uint64
	compressedLen   uint64
	uncompressedLen uint64
	checksum        uint32
}

type v2SegmentReader struct {
	kind        string
	path        string
	file        *os.File
	start       uint64
	count       uint64
	frameBlocks uint32
	frames      []v2FrameEntry
}

type v2Manifest struct {
	Version     uint32            `json:"version"`
	Start       uint64            `json:"start"`
	Count       uint64            `json:"count"`
	FrameBlocks uint32            `json:"frame_blocks"`
	Tables      map[string]string `json:"tables"`
}

type v2FrameKey struct {
	segment *v2SegmentReader
	frame   int
}

type v2CacheValue struct {
	key  v2FrameKey
	data []byte
}

// v2Store is the immutable segment tier below the still-appendable V1 freezer
// tail. A manifest is the commit marker: unreferenced segment files are ignored
// after a crash, while every loaded manifest must describe a contiguous prefix.
type v2Store struct {
	base       string
	segments   map[string][]*v2SegmentReader
	coverage   uint64
	decoder    *zstd.Decoder
	cacheMu    sync.Mutex
	cacheList  *list.List
	cacheItems map[v2FrameKey]*list.Element
}

func openV2Store(ancientDir string) (*v2Store, error) {
	base := filepath.Join(ancientDir, "v2")
	decoder, err := newV2Decoder()
	if err != nil {
		return nil, err
	}
	store := &v2Store{
		base:       base,
		segments:   make(map[string][]*v2SegmentReader),
		decoder:    decoder,
		cacheList:  list.New(),
		cacheItems: make(map[v2FrameKey]*list.Element),
	}
	manifestDir := filepath.Join(base, "manifests")
	entries, err := os.ReadDir(manifestDir)
	if os.IsNotExist(err) {
		return store, nil
	}
	if err != nil {
		store.Close()
		return nil, err
	}
	var manifests []v2Manifest
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(manifestDir, entry.Name()))
		if err != nil {
			store.Close()
			return nil, err
		}
		var manifest v2Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			store.Close()
			return nil, fmt.Errorf("decode ancient V2 manifest %s: %w", entry.Name(), err)
		}
		if manifest.Version != v2Version || manifest.Count == 0 || len(manifest.Tables) == 0 {
			store.Close()
			return nil, fmt.Errorf("invalid ancient V2 manifest %s", entry.Name())
		}
		manifests = append(manifests, manifest)
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].Start < manifests[j].Start })
	expected := uint64(0)
	var tableSet map[string]struct{}
	for _, manifest := range manifests {
		if manifest.Start != expected {
			store.Close()
			return nil, fmt.Errorf("ancient V2 manifest gap: start %d, want %d", manifest.Start, expected)
		}
		if tableSet == nil {
			tableSet = make(map[string]struct{}, len(manifest.Tables))
			for kind := range manifest.Tables {
				tableSet[kind] = struct{}{}
			}
		} else {
			if len(manifest.Tables) != len(tableSet) {
				store.Close()
				return nil, fmt.Errorf("ancient V2 manifest at %d changes table set", manifest.Start)
			}
			for kind := range tableSet {
				if _, ok := manifest.Tables[kind]; !ok {
					store.Close()
					return nil, fmt.Errorf("ancient V2 manifest at %d misses table %s", manifest.Start, kind)
				}
			}
		}
		for kind, name := range manifest.Tables {
			reader, err := openV2Segment(filepath.Join(base, kind, name), kind)
			if err != nil {
				store.Close()
				return nil, err
			}
			if reader.start != manifest.Start || reader.count != manifest.Count || reader.frameBlocks != manifest.FrameBlocks {
				reader.Close()
				store.Close()
				return nil, fmt.Errorf("ancient V2 segment %s does not match manifest", reader.path)
			}
			store.segments[kind] = append(store.segments[kind], reader)
		}
		expected += manifest.Count
	}
	store.coverage = expected
	return store, nil
}

func newV2Decoder() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
}

func (s *v2Store) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	for _, segments := range s.segments {
		for _, segment := range segments {
			if err := segment.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if s.decoder != nil {
		s.decoder.Close()
	}
	return errors.Join(errs...)
}

func (s *v2Store) has(kind string, number uint64) bool {
	_, ok := s.find(kind, number)
	return ok
}

func (s *v2Store) find(kind string, number uint64) (*v2SegmentReader, bool) {
	if s == nil {
		return nil, false
	}
	segments := s.segments[kind]
	i := sort.Search(len(segments), func(i int) bool { return segments[i].start+segments[i].count > number })
	if i == len(segments) || number < segments[i].start {
		return nil, false
	}
	return segments[i], true
}

func (s *v2Store) read(kind string, number uint64) ([]byte, error) {
	segment, ok := s.find(kind, number)
	if !ok {
		return nil, errOutOfBounds
	}
	relative := number - segment.start
	frameIndex := int(relative / uint64(segment.frameBlocks))
	data, err := s.readFrame(segment, frameIndex)
	if err != nil {
		return nil, err
	}
	ordinal := relative - segment.frames[frameIndex].firstRecord
	for i := uint64(0); i <= ordinal; i++ {
		length, consumed := binary.Uvarint(data)
		if consumed <= 0 {
			return nil, fmt.Errorf("ancient V2 malformed record length in %s frame %d", segment.path, frameIndex)
		}
		data = data[consumed:]
		if length > uint64(len(data)) {
			return nil, fmt.Errorf("ancient V2 record exceeds frame in %s frame %d", segment.path, frameIndex)
		}
		if i == ordinal {
			return append([]byte(nil), data[:length]...), nil
		}
		data = data[length:]
	}
	return nil, errOutOfBounds
}

func (s *v2Store) readFrame(segment *v2SegmentReader, frameIndex int) ([]byte, error) {
	key := v2FrameKey{segment: segment, frame: frameIndex}
	s.cacheMu.Lock()
	if element := s.cacheItems[key]; element != nil {
		s.cacheList.MoveToFront(element)
		data := element.Value.(v2CacheValue).data
		s.cacheMu.Unlock()
		return data, nil
	}
	s.cacheMu.Unlock()

	if frameIndex < 0 || frameIndex >= len(segment.frames) {
		return nil, errOutOfBounds
	}
	frame := segment.frames[frameIndex]
	if frame.compressedLen > uint64(^uint(0)>>1) || frame.uncompressedLen > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("ancient V2 frame too large in %s", segment.path)
	}
	compressed := make([]byte, int(frame.compressedLen))
	if _, err := segment.file.ReadAt(compressed, int64(frame.compressedStart)); err != nil {
		return nil, err
	}
	if crc32.Checksum(compressed, v2CRC) != frame.checksum {
		return nil, fmt.Errorf("ancient V2 checksum mismatch in %s frame %d", segment.path, frameIndex)
	}
	decoded, err := s.decoder.DecodeAll(compressed, make([]byte, 0, int(frame.uncompressedLen)))
	if err != nil {
		return nil, err
	}
	if uint64(len(decoded)) != frame.uncompressedLen {
		return nil, fmt.Errorf("ancient V2 decoded length %d, want %d", len(decoded), frame.uncompressedLen)
	}

	s.cacheMu.Lock()
	if existing := s.cacheItems[key]; existing != nil {
		data := existing.Value.(v2CacheValue).data
		s.cacheList.MoveToFront(existing)
		s.cacheMu.Unlock()
		return data, nil
	}
	element := s.cacheList.PushFront(v2CacheValue{key: key, data: decoded})
	s.cacheItems[key] = element
	for s.cacheList.Len() > v2CacheFrames {
		oldest := s.cacheList.Back()
		value := oldest.Value.(v2CacheValue)
		delete(s.cacheItems, value.key)
		s.cacheList.Remove(oldest)
	}
	s.cacheMu.Unlock()
	return decoded, nil
}

func (s *v2Store) size(kind string) uint64 {
	var total uint64
	for _, segment := range s.segments[kind] {
		if info, err := segment.file.Stat(); err == nil {
			total += uint64(info.Size())
		}
	}
	return total
}

func openV2Segment(path, kind string) (*v2SegmentReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*v2SegmentReader, error) {
		file.Close()
		return nil, err
	}
	header := make([]byte, v2HeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return fail(err)
	}
	if string(header[:8]) != v2Magic || binary.BigEndian.Uint32(header[8:12]) != v2Version {
		return fail(fmt.Errorf("invalid ancient V2 header %s", path))
	}
	if crc32.Checksum(header[:56], v2CRC) != binary.BigEndian.Uint32(header[56:60]) {
		return fail(fmt.Errorf("ancient V2 header checksum mismatch %s", path))
	}
	frameBlocks := binary.BigEndian.Uint32(header[12:16])
	start := binary.BigEndian.Uint64(header[16:24])
	count := binary.BigEndian.Uint64(header[24:32])
	frameCount := binary.BigEndian.Uint64(header[32:40])
	dataOffset := binary.BigEndian.Uint64(header[40:48])
	if frameBlocks == 0 || count == 0 || frameCount == 0 || frameCount > 1<<24 {
		return fail(fmt.Errorf("invalid ancient V2 dimensions %s", path))
	}
	if frameCount != (count+uint64(frameBlocks)-1)/uint64(frameBlocks) {
		return fail(fmt.Errorf("invalid ancient V2 frame count %s", path))
	}
	tableLen := frameCount * v2FrameEntrySize
	if dataOffset != v2HeaderSize+tableLen || tableLen > 1<<32 {
		return fail(fmt.Errorf("invalid ancient V2 frame table %s", path))
	}
	table := make([]byte, int(tableLen))
	if _, err := io.ReadFull(file, table); err != nil {
		return fail(err)
	}
	if crc32.Checksum(table, v2CRC) != binary.BigEndian.Uint32(header[48:52]) {
		return fail(fmt.Errorf("ancient V2 table checksum mismatch %s", path))
	}
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	reader := &v2SegmentReader{kind: kind, path: path, file: file, start: start, count: count, frameBlocks: frameBlocks, frames: make([]v2FrameEntry, frameCount)}
	var (
		expectedRecord uint64
		expectedData   = dataOffset
	)
	for i := range reader.frames {
		offset := i * v2FrameEntrySize
		entry := table[offset : offset+v2FrameEntrySize]
		frame := v2FrameEntry{
			firstRecord:     binary.BigEndian.Uint64(entry[0:8]),
			records:         binary.BigEndian.Uint32(entry[8:12]),
			compressedStart: binary.BigEndian.Uint64(entry[16:24]),
			compressedLen:   binary.BigEndian.Uint64(entry[24:32]),
			uncompressedLen: binary.BigEndian.Uint64(entry[32:40]),
			checksum:        binary.BigEndian.Uint32(entry[40:44]),
		}
		if frame.firstRecord != expectedRecord || frame.records == 0 || frame.records > frameBlocks || (i < len(reader.frames)-1 && frame.records != frameBlocks) || frame.compressedStart != expectedData || frame.compressedStart+frame.compressedLen > uint64(info.Size()) {
			return fail(fmt.Errorf("invalid ancient V2 frame %d in %s", i, path))
		}
		expectedRecord += uint64(frame.records)
		expectedData += frame.compressedLen
		reader.frames[i] = frame
	}
	if expectedRecord != count || expectedData != uint64(info.Size()) {
		return fail(fmt.Errorf("ancient V2 record count %d, want %d in %s", expectedRecord, count, path))
	}
	return reader, nil
}

func (r *v2SegmentReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func writeV2Segment(path string, start, count uint64, frameBlocks uint32, read func(uint64) ([]byte, error)) error {
	if count == 0 || frameBlocks == 0 {
		return errors.New("ancient V2: empty segment dimensions")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".v2-segment-*.tmp")
	if err != nil {
		return err
	}
	tempName := file.Name()
	defer func() {
		file.Close()
		os.Remove(tempName)
	}()
	frameCount := (count + uint64(frameBlocks) - 1) / uint64(frameBlocks)
	dataOffset := uint64(v2HeaderSize) + frameCount*v2FrameEntrySize
	if _, err := file.Seek(int64(dataOffset), io.SeekStart); err != nil {
		return err
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	defer encoder.Close()
	frames := make([]v2FrameEntry, 0, frameCount)
	buffer := make([]byte, 0, 4<<20)
	var written uint64
	for written < count {
		first := written
		records := uint32(0)
		buffer = buffer[:0]
		for records < frameBlocks && written < count {
			record, err := read(start + written)
			if err != nil {
				return err
			}
			buffer = binary.AppendUvarint(buffer, uint64(len(record)))
			buffer = append(buffer, record...)
			records++
			written++
		}
		compressed := encoder.EncodeAll(buffer, nil)
		position, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if _, err := file.Write(compressed); err != nil {
			return err
		}
		frames = append(frames, v2FrameEntry{
			firstRecord:     first,
			records:         records,
			compressedStart: uint64(position),
			compressedLen:   uint64(len(compressed)),
			uncompressedLen: uint64(len(buffer)),
			checksum:        crc32.Checksum(compressed, v2CRC),
		})
	}
	table := make([]byte, len(frames)*v2FrameEntrySize)
	for i, frame := range frames {
		entry := table[i*v2FrameEntrySize : (i+1)*v2FrameEntrySize]
		binary.BigEndian.PutUint64(entry[0:8], frame.firstRecord)
		binary.BigEndian.PutUint32(entry[8:12], frame.records)
		binary.BigEndian.PutUint64(entry[16:24], frame.compressedStart)
		binary.BigEndian.PutUint64(entry[24:32], frame.compressedLen)
		binary.BigEndian.PutUint64(entry[32:40], frame.uncompressedLen)
		binary.BigEndian.PutUint32(entry[40:44], frame.checksum)
	}
	header := make([]byte, v2HeaderSize)
	copy(header[:8], v2Magic)
	binary.BigEndian.PutUint32(header[8:12], v2Version)
	binary.BigEndian.PutUint32(header[12:16], frameBlocks)
	binary.BigEndian.PutUint64(header[16:24], start)
	binary.BigEndian.PutUint64(header[24:32], count)
	binary.BigEndian.PutUint64(header[32:40], uint64(len(frames)))
	binary.BigEndian.PutUint64(header[40:48], dataOffset)
	binary.BigEndian.PutUint32(header[48:52], crc32.Checksum(table, v2CRC))
	binary.BigEndian.PutUint32(header[56:60], crc32.Checksum(header[:56], v2CRC))
	if _, err := file.WriteAt(header, 0); err != nil {
		return err
	}
	if _, err := file.WriteAt(table, v2HeaderSize); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func v2SegmentName(start, count uint64) string {
	return fmt.Sprintf("%020d-%020d.gtv2", start, start+count)
}

func v2ManifestName(start, count uint64) string {
	return fmt.Sprintf("%020d-%020d.json", start, start+count)
}

func publishV2Manifest(base string, manifest v2Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(bytes.TrimSpace(data), '\n')
	dir := filepath.Join(base, "manifests")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return reset(filepath.Join(dir, v2ManifestName(manifest.Start, manifest.Count)), data)
}
