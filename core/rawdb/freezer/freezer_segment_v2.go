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
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	v2Magic              = "gtanv201"
	v2Version            = uint32(1)
	v2HeaderSize         = 64
	v2FrameEntrySize     = 48
	v2DefaultFrames      = uint32(64)
	v2DefaultSegments    = uint64(65_536)
	v2DefaultCacheBytes  = uint64(64 << 20)
	v2MaxFrameBytes      = uint64(512 << 20)
	v2MaxCompressedBytes = v2MaxFrameBytes + uint64(1<<20)
	v2DecoderConcurrency = 4
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
	Version            uint32            `json:"version"`
	Start              uint64            `json:"start"`
	Count              uint64            `json:"count"`
	FrameBlocks        uint32            `json:"frame_blocks"`
	Tables             map[string]string `json:"tables"`
	TxInfoIDsCompacted bool              `json:"tx_info_ids_compacted,omitempty"`
}

type v2FrameKey struct {
	segment *v2SegmentReader
	frame   int
}

type v2CacheValue struct {
	key   v2FrameKey
	frame *v2DecodedFrame
	size  uint64
}

type v2RecordBounds struct {
	start uint32
	end   uint32
}

type v2DecodedFrame struct {
	data    []byte
	records []v2RecordBounds
}

type v2FrameLoad struct {
	done  chan struct{}
	frame *v2DecodedFrame
	err   error
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
	cacheLoads map[v2FrameKey]*v2FrameLoad
	cacheBytes uint64
	cacheLimit uint64
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
		cacheLoads: make(map[v2FrameKey]*v2FrameLoad),
		cacheLimit: v2DefaultCacheBytes,
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
	concurrency := runtime.GOMAXPROCS(0)
	if concurrency > v2DecoderConcurrency {
		concurrency = v2DecoderConcurrency
	}
	return zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(concurrency),
		zstd.WithDecoderMaxMemory(v2MaxFrameBytes),
	)
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
	frame, err := s.readFrame(segment, frameIndex)
	if err != nil {
		return nil, err
	}
	ordinal := relative - segment.frames[frameIndex].firstRecord
	if ordinal >= uint64(len(frame.records)) {
		return nil, errOutOfBounds
	}
	bounds := frame.records[ordinal]
	return append([]byte(nil), frame.data[bounds.start:bounds.end]...), nil
}

func (s *v2Store) readRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if count == 0 {
		return nil, nil
	}
	if !s.has(kind, start) {
		return nil, errOutOfBounds
	}
	capacity := count
	if available := s.coverage - start; capacity > available {
		capacity = available
	}
	items := make([][]byte, 0, int(capacity))
	var total uint64
	for number := start; uint64(len(items)) < count && number < s.coverage; {
		segment, ok := s.find(kind, number)
		if !ok {
			break
		}
		relative := number - segment.start
		frameIndex := int(relative / uint64(segment.frameBlocks))
		frame, err := s.readFrame(segment, frameIndex)
		if err != nil {
			return nil, err
		}
		ordinal := relative - segment.frames[frameIndex].firstRecord
		for ordinal < uint64(len(frame.records)) && uint64(len(items)) < count {
			bounds := frame.records[ordinal]
			length := uint64(bounds.end - bounds.start)
			if len(items) > 0 && maxBytes != 0 && total+length > maxBytes {
				return items, nil
			}
			items = append(items, append([]byte(nil), frame.data[bounds.start:bounds.end]...))
			total += length
			number++
			ordinal++
		}
	}
	return items, nil
}

func (s *v2Store) readFrame(segment *v2SegmentReader, frameIndex int) (*v2DecodedFrame, error) {
	key := v2FrameKey{segment: segment, frame: frameIndex}
	s.cacheMu.Lock()
	if element := s.cacheItems[key]; element != nil {
		s.cacheList.MoveToFront(element)
		frame := element.Value.(v2CacheValue).frame
		s.cacheMu.Unlock()
		return frame, nil
	}
	if load := s.cacheLoads[key]; load != nil {
		s.cacheMu.Unlock()
		<-load.done
		return load.frame, load.err
	}
	load := &v2FrameLoad{done: make(chan struct{})}
	s.cacheLoads[key] = load
	s.cacheMu.Unlock()

	frame, err := s.loadFrame(segment, frameIndex)

	s.cacheMu.Lock()
	load.frame, load.err = frame, err
	delete(s.cacheLoads, key)
	if err == nil && uint64(len(frame.data)) <= s.cacheLimit {
		value := v2CacheValue{key: key, frame: frame, size: uint64(len(frame.data))}
		element := s.cacheList.PushFront(value)
		s.cacheItems[key] = element
		s.cacheBytes += value.size
		for s.cacheBytes > s.cacheLimit && s.cacheList.Len() > 0 {
			oldest := s.cacheList.Back()
			oldValue := oldest.Value.(v2CacheValue)
			delete(s.cacheItems, oldValue.key)
			s.cacheList.Remove(oldest)
			s.cacheBytes -= oldValue.size
		}
	}
	close(load.done)
	s.cacheMu.Unlock()
	return frame, err
}

func (s *v2Store) loadFrame(segment *v2SegmentReader, frameIndex int) (*v2DecodedFrame, error) {
	if frameIndex < 0 || frameIndex >= len(segment.frames) {
		return nil, errOutOfBounds
	}
	frame := segment.frames[frameIndex]
	if frame.compressedLen > v2MaxCompressedBytes || frame.uncompressedLen > v2MaxFrameBytes || frame.compressedLen > uint64(^uint(0)>>1) || frame.uncompressedLen > uint64(^uint(0)>>1) {
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
	records := make([]v2RecordBounds, 0, frame.records)
	remaining := decoded
	consumedBytes := 0
	for record := uint32(0); record < frame.records; record++ {
		length, consumed := binary.Uvarint(remaining)
		if consumed <= 0 {
			return nil, fmt.Errorf("ancient V2 malformed record length in %s frame %d", segment.path, frameIndex)
		}
		remaining = remaining[consumed:]
		consumedBytes += consumed
		if length > uint64(len(remaining)) {
			return nil, fmt.Errorf("ancient V2 record exceeds frame in %s frame %d", segment.path, frameIndex)
		}
		start := consumedBytes
		consumedBytes += int(length)
		records = append(records, v2RecordBounds{start: uint32(start), end: uint32(consumedBytes)})
		remaining = remaining[length:]
	}
	if len(remaining) != 0 {
		return nil, fmt.Errorf("ancient V2 trailing frame bytes in %s frame %d", segment.path, frameIndex)
	}
	return &v2DecodedFrame{data: decoded, records: records}, nil
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
			if uint64(len(buffer)) > v2MaxFrameBytes {
				return fmt.Errorf("ancient V2 frame exceeds %d bytes at record %d", v2MaxFrameBytes, start+written)
			}
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
