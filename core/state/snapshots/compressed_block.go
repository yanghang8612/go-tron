package snapshots

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Compressed block segment format ("gtcblk01"). A record stream is grouped into
// fixed-count blocks; each block is zstd-compressed independently. A block table
// maps an UNCOMPRESSED record offset to its (compressed block, position), so an
// external key/tx accessor can keep storing plain uncompressed offsets while the
// payload on disk is compressed. This is the seekable-compression primitive for
// cold history segments (see project_archive_compression_arc): block compression
// captures the heavy cross-record redundancy (repeated BlockHash/owner/etc.) that
// per-record compression cannot, measured ~2.7x on realistic history.
//
// Layout:
//
//	header (48 bytes): magic[8] version blockSize recordCount blockCount
//	                   uncompressedSize dataOffset
//	block table:       blockCount x { uncompressedStart, compressedStart,
//	                                  compressedLen, records }
//	data:              compressed blocks back to back (at dataOffset)
const (
	compressedBlockMagic               = "gtcblk01"
	compressedBlockVersion             = uint32(1)
	compressedBlockHeaderSize          = 8 + 4 + 4 + 8 + 8 + 8 + 8 // = 48
	compressedBlockTableEntry          = 8 + 8 + 8 + 4             // = 28
	CompressedBlockDefaultSize         = 128                       // matches the .bt block size
	compressedBlockMaxDecodedBlockSize = 256 << 20
	minParallelHistoryCompressionBytes = 1 << 20
	maxHistoryCompressionWorkers       = 4
)

// Shared zstd encoder/decoder. klauspost's EncodeAll/DecodeAll are documented
// safe for concurrent use; the decoder is built with concurrency 0 so DecodeAll
// stays single-allocation per call and goroutine-safe.
var (
	cbEnc     *zstd.Encoder
	cbDec     *zstd.Decoder
	cbInit    sync.Once
	cbInitErr error
)

func cbCodec() (*zstd.Encoder, *zstd.Decoder, error) {
	cbInit.Do(func() {
		cbEnc, cbInitErr = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if cbInitErr != nil {
			return
		}
		cbDec, cbInitErr = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecodeAllCapLimit(true))
	})
	return cbEnc, cbDec, cbInitErr
}

type cbBlock struct {
	uncompressedStart uint64
	compressedStart   uint64
	compressedLen     uint64
	records           uint32
}

// compressedBlockWriter accumulates opaque records, compressing them one block at
// a time into a temp file, and assembles the final file on Finish. Peak memory is
// one uncompressed block plus the in-memory block table (~28 bytes per block).
type compressedBlockWriter struct {
	enc       *zstd.Encoder
	blockSize int
	tmp       *os.File
	tmpWriter *bufio.Writer
	tmpName   string
	table     []cbBlock
	buf       []byte
	encoded   []byte
	bufRecs   int
	uncTotal  uint64
	compTotal uint64
	recCount  uint64
}

func newCompressedBlockWriter(dir string, blockSize int) (*compressedBlockWriter, error) {
	if blockSize <= 0 {
		blockSize = CompressedBlockDefaultSize
	}
	enc, _, err := cbCodec()
	if err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, ".cbw-*.tmp")
	if err != nil {
		return nil, err
	}
	return &compressedBlockWriter{
		enc:       enc,
		blockSize: blockSize,
		tmp:       tmp,
		tmpWriter: acquireStateDomainChangeHistoryWriter(tmp),
		tmpName:   tmp.Name(),
	}, nil
}

// Append adds one record and returns its uncompressed offset (the value an
// external accessor stores to address this record later).
func (w *compressedBlockWriter) Append(rec []byte) (uint64, error) {
	off := w.uncTotal
	w.buf = append(w.buf, rec...)
	w.uncTotal += uint64(len(rec))
	w.recCount++
	w.bufRecs++
	if w.bufRecs >= w.blockSize {
		if err := w.flushBlock(); err != nil {
			return 0, err
		}
	}
	return off, nil
}

func (w *compressedBlockWriter) flushBlock() error {
	if w.bufRecs == 0 {
		return nil
	}
	w.encoded = w.enc.EncodeAll(w.buf, w.encoded[:0])
	comp := w.encoded
	if _, err := w.tmpWriter.Write(comp); err != nil {
		return err
	}
	w.table = append(w.table, cbBlock{
		uncompressedStart: w.uncTotal - uint64(len(w.buf)),
		compressedStart:   w.compTotal,
		compressedLen:     uint64(len(comp)),
		records:           uint32(w.bufRecs),
	})
	w.compTotal += uint64(len(comp))
	w.buf = w.buf[:0]
	w.bufRecs = 0
	return nil
}

// appendEncodedBlock is the ordered reducer half of file compression. Encoding
// may happen out of order in workers, but the block table and temp data must be
// appended in source order so existing uncompressed accessor offsets remain
// valid and output stays deterministic.
func (w *compressedBlockWriter) appendEncodedBlock(uncompressedLen int, comp []byte) error {
	if uncompressedLen <= 0 {
		return errors.New("snapshots: cannot append empty encoded block")
	}
	if w.bufRecs != 0 {
		return errors.New("snapshots: cannot mix buffered and pre-encoded blocks")
	}
	if _, err := w.tmpWriter.Write(comp); err != nil {
		return err
	}
	w.table = append(w.table, cbBlock{
		uncompressedStart: w.uncTotal,
		compressedStart:   w.compTotal,
		compressedLen:     uint64(len(comp)),
		records:           1,
	})
	w.uncTotal += uint64(uncompressedLen)
	w.compTotal += uint64(len(comp))
	w.recCount++
	return nil
}

// Finish flushes the last partial block and writes the assembled file to path.
func (w *compressedBlockWriter) Finish(path string) (err error) {
	defer func() {
		releaseStateDomainChangeHistoryWriter(&w.tmpWriter)
		_ = w.tmp.Close()
		_ = os.Remove(w.tmpName)
	}()
	if e := w.flushBlock(); e != nil {
		return e
	}
	if err = w.tmpWriter.Flush(); err != nil {
		return err
	}
	if _, err = w.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); err == nil {
			err = cerr
		}
	}()

	dataOffset := uint64(compressedBlockHeaderSize) + uint64(len(w.table))*compressedBlockTableEntry
	var hdr bytes.Buffer
	hdr.WriteString(compressedBlockMagic)
	writeUint32(&hdr, compressedBlockVersion)
	writeUint32(&hdr, uint32(w.blockSize))
	writeUint64(&hdr, w.recCount)
	writeUint64(&hdr, uint64(len(w.table)))
	writeUint64(&hdr, w.uncTotal)
	writeUint64(&hdr, dataOffset)
	for _, b := range w.table {
		writeUint64(&hdr, b.uncompressedStart)
		writeUint64(&hdr, b.compressedStart)
		writeUint64(&hdr, b.compressedLen)
		writeUint32(&hdr, b.records)
	}
	if _, err = out.Write(hdr.Bytes()); err != nil {
		return err
	}
	if _, err = copyStateDomainChangeHistoryData(out, w.tmp); err != nil {
		return err
	}
	return out.Sync()
}

// finishWithPrefix assembles a stream whose first logical chunk stayed in
// memory so callers could backpatch fixed header fields. The body temp already
// contains independently compressed later chunks; its compressed offsets are
// shifted by the encoded prefix length while uncompressed offsets remain those
// of the original byte stream.
func (w *compressedBlockWriter) finishWithPrefix(path string, prefix []byte) (err error) {
	defer func() {
		releaseStateDomainChangeHistoryWriter(&w.tmpWriter)
		_ = w.tmp.Close()
		_ = os.Remove(w.tmpName)
	}()
	if w.bufRecs != 0 || len(w.buf) != 0 {
		return errors.New("snapshots: prefixed compressed writer has buffered records")
	}
	if err = w.tmpWriter.Flush(); err != nil {
		return err
	}
	if _, err = w.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var prefixComp []byte
	prefixRecords := uint64(0)
	uncTotal := w.uncTotal
	if len(prefix) != 0 {
		w.encoded = w.enc.EncodeAll(prefix, w.encoded[:0])
		prefixComp = w.encoded
		prefixRecords = 1
		if w.recCount == 0 {
			uncTotal = uint64(len(prefix))
		} else if len(w.table) == 0 || w.table[0].uncompressedStart != uint64(len(prefix)) {
			return errors.New("snapshots: compressed body does not follow retained prefix")
		}
	} else if w.recCount != 0 {
		return errors.New("snapshots: compressed body has no retained prefix")
	}

	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); err == nil {
			err = cerr
		}
	}()

	blockCount := uint64(len(w.table)) + prefixRecords
	dataOffset := uint64(compressedBlockHeaderSize) + blockCount*compressedBlockTableEntry
	var hdr bytes.Buffer
	hdr.WriteString(compressedBlockMagic)
	writeUint32(&hdr, compressedBlockVersion)
	writeUint32(&hdr, uint32(w.blockSize))
	writeUint64(&hdr, w.recCount+prefixRecords)
	writeUint64(&hdr, blockCount)
	writeUint64(&hdr, uncTotal)
	writeUint64(&hdr, dataOffset)
	if prefixRecords != 0 {
		writeUint64(&hdr, 0)
		writeUint64(&hdr, 0)
		writeUint64(&hdr, uint64(len(prefixComp)))
		writeUint32(&hdr, 1)
	}
	shift := uint64(len(prefixComp))
	for _, b := range w.table {
		writeUint64(&hdr, b.uncompressedStart)
		writeUint64(&hdr, b.compressedStart+shift)
		writeUint64(&hdr, b.compressedLen)
		writeUint32(&hdr, b.records)
	}
	if _, err = out.Write(hdr.Bytes()); err != nil {
		return err
	}
	if len(prefixComp) != 0 {
		if _, err = out.Write(prefixComp); err != nil {
			return err
		}
	}
	if _, err = copyStateDomainChangeHistoryData(out, w.tmp); err != nil {
		return err
	}
	return out.Sync()
}

// compressedBlockReader serves records by uncompressed offset, decompressing the
// containing block on demand with a one-block cache (so a sequential walk
// decompresses each block exactly once). Concurrent callers are serialized on mu
// and always receive a private copy, so the reader is safe to share.
type compressedBlockReader struct {
	f         *os.File
	dec       *zstd.Decoder
	blockSize int
	recCount  uint64
	uncSize   uint64
	dataOff   uint64
	fileSize  uint64
	table     []cbBlock

	mu         sync.Mutex
	cacheLimit int
	compressed []byte
	cache      []cbCacheEntry // small MRU-ordered decompressed-block cache (front = newest)
}

type cbCacheEntry struct {
	idx   int
	bytes []byte
}

// cbCacheBlocks bounds the decompressed-block cache. 1 is optimal for sequential
// scans (range reads); a keyed binary search probes ~2·log N random blocks and a
// 1-block cache thrashes, so a small MRU cache lets the search's narrowing phase
// re-hit recent blocks. ~16 × block-size of transient memory per open reader.
const cbCacheBlocks = 16

func openCompressedBlockReader(path string) (*compressedBlockReader, error) {
	return openCompressedBlockReaderWithCacheLimit(path, cbCacheBlocks)
}

func openCompressedBlockReaderWithCacheLimit(path string, cacheLimit int) (*compressedBlockReader, error) {
	if cacheLimit < 1 || cacheLimit > cbCacheBlocks {
		return nil, fmt.Errorf("snapshots: compressed-block cache limit %d outside [1,%d]", cacheLimit, cbCacheBlocks)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	_, dec, err := cbCodec()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	fileSize := uint64(stat.Size())
	if fileSize < compressedBlockHeaderSize {
		_ = f.Close()
		return nil, fmt.Errorf("snapshots: compressed-block file %q size %d below header size %d", path, fileSize, compressedBlockHeaderSize)
	}
	hdr := make([]byte, compressedBlockHeaderSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		_ = f.Close()
		return nil, err
	}
	if string(hdr[:8]) != compressedBlockMagic {
		_ = f.Close()
		return nil, errors.New("snapshots: bad compressed-block magic")
	}
	if ver := binary.BigEndian.Uint32(hdr[8:12]); ver != compressedBlockVersion {
		_ = f.Close()
		return nil, fmt.Errorf("snapshots: unsupported compressed-block version %d", ver)
	}
	r := &compressedBlockReader{
		f:          f,
		dec:        dec,
		blockSize:  int(binary.BigEndian.Uint32(hdr[12:16])),
		recCount:   binary.BigEndian.Uint64(hdr[16:24]),
		dataOff:    binary.BigEndian.Uint64(hdr[40:48]),
		fileSize:   fileSize,
		cacheLimit: cacheLimit,
	}
	blockCount := binary.BigEndian.Uint64(hdr[24:32])
	r.uncSize = binary.BigEndian.Uint64(hdr[32:40])
	if r.blockSize <= 0 {
		_ = f.Close()
		return nil, errors.New("snapshots: compressed-block has zero block size")
	}
	tableLen, err := compressedBlockTableLen(blockCount)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	tableEnd, overflow := checkedAdd(compressedBlockHeaderSize, tableLen)
	if overflow || tableEnd > fileSize {
		_ = f.Close()
		return nil, fmt.Errorf("snapshots: compressed-block table end %d outside file size %d", tableEnd, fileSize)
	}
	if r.dataOff != tableEnd {
		_ = f.Close()
		return nil, fmt.Errorf("snapshots: compressed-block data offset %d, want table end %d", r.dataOff, tableEnd)
	}
	tableBytes := make([]byte, int(tableLen))
	if _, err := io.ReadFull(f, tableBytes); err != nil {
		_ = f.Close()
		return nil, err
	}
	r.table = make([]cbBlock, blockCount)
	for i := uint64(0); i < blockCount; i++ {
		o := i * compressedBlockTableEntry
		r.table[i] = cbBlock{
			uncompressedStart: binary.BigEndian.Uint64(tableBytes[o : o+8]),
			compressedStart:   binary.BigEndian.Uint64(tableBytes[o+8 : o+16]),
			compressedLen:     binary.BigEndian.Uint64(tableBytes[o+16 : o+24]),
			records:           binary.BigEndian.Uint32(tableBytes[o+24 : o+28]),
		}
	}
	if err := validateCompressedBlockTable(r.table, r.recCount, r.uncSize, r.dataOff, fileSize); err != nil {
		_ = f.Close()
		return nil, err
	}
	return r, nil
}

func (r *compressedBlockReader) Close() error { return r.f.Close() }

func compressedBlockMaxAlloc() uint64 {
	return uint64(int(^uint(0) >> 1))
}

func compressedBlockTableLen(blockCount uint64) (uint64, error) {
	if blockCount > uint64(1)<<40 {
		return 0, fmt.Errorf("snapshots: implausible compressed-block count %d", blockCount)
	}
	tableLen, overflow := checkedMul(blockCount, compressedBlockTableEntry)
	if overflow || tableLen > compressedBlockMaxAlloc() {
		return 0, fmt.Errorf("snapshots: compressed-block table size overflows: count=%d", blockCount)
	}
	return tableLen, nil
}

func validateCompressedBlockTable(table []cbBlock, recCount, uncSize, dataOff, fileSize uint64) error {
	if len(table) == 0 {
		if recCount != 0 || uncSize != 0 {
			return fmt.Errorf("snapshots: empty compressed-block table with records=%d uncompressed=%d", recCount, uncSize)
		}
		if dataOff != fileSize {
			return fmt.Errorf("snapshots: empty compressed-block data offset %d, want file size %d", dataOff, fileSize)
		}
		return nil
	}
	if recCount == 0 || uncSize == 0 {
		return fmt.Errorf("snapshots: compressed-block table has %d blocks with records=%d uncompressed=%d", len(table), recCount, uncSize)
	}
	var expectedCompStart uint64
	var prevUncStart uint64
	var records uint64
	for i, block := range table {
		if block.records == 0 {
			return fmt.Errorf("snapshots: compressed-block table entry %d has zero records", i)
		}
		if i == 0 {
			if block.uncompressedStart != 0 {
				return fmt.Errorf("snapshots: compressed-block first uncompressed offset %d, want 0", block.uncompressedStart)
			}
		} else if block.uncompressedStart <= prevUncStart {
			return fmt.Errorf("snapshots: compressed-block uncompressed offsets are not increasing at entry %d", i)
		}
		if block.uncompressedStart >= uncSize {
			return fmt.Errorf("snapshots: compressed-block uncompressed offset %d outside size %d", block.uncompressedStart, uncSize)
		}
		if block.compressedStart != expectedCompStart {
			return fmt.Errorf("snapshots: compressed-block entry %d compressed offset %d, want %d", i, block.compressedStart, expectedCompStart)
		}
		if block.compressedLen == 0 {
			return fmt.Errorf("snapshots: compressed-block entry %d has zero compressed length", i)
		}
		compEnd, overflow := checkedAdd(block.compressedStart, block.compressedLen)
		if overflow {
			return fmt.Errorf("snapshots: compressed-block entry %d compressed range overflows", i)
		}
		physicalStart, overflow := checkedAdd(dataOff, block.compressedStart)
		if overflow {
			return fmt.Errorf("snapshots: compressed-block entry %d physical offset overflows", i)
		}
		physicalEnd, overflow := checkedAdd(dataOff, compEnd)
		if overflow || physicalEnd > fileSize {
			return fmt.Errorf("snapshots: compressed-block entry %d range [%d,%d] outside file size %d", i, physicalStart, physicalEnd, fileSize)
		}
		var recordsOverflow bool
		records, recordsOverflow = checkedAdd(records, uint64(block.records))
		if recordsOverflow {
			return fmt.Errorf("snapshots: compressed-block record count overflows")
		}
		expectedCompStart = compEnd
		prevUncStart = block.uncompressedStart
	}
	physicalEnd, overflow := checkedAdd(dataOff, expectedCompStart)
	if overflow || physicalEnd != fileSize {
		return fmt.Errorf("snapshots: compressed-block data end %d, want file size %d", physicalEnd, fileSize)
	}
	if records != recCount {
		return fmt.Errorf("snapshots: compressed-block records=%d, want %d", records, recCount)
	}
	return nil
}

func compressedBlockExpectedLen(table []cbBlock, uncSize uint64, index int) (uint64, error) {
	if index < 0 || index >= len(table) {
		return 0, fmt.Errorf("snapshots: compressed-block index %d outside table", index)
	}
	start := table[index].uncompressedStart
	end := uncSize
	if index+1 < len(table) {
		end = table[index+1].uncompressedStart
	}
	if end <= start {
		return 0, fmt.Errorf("snapshots: compressed-block entry %d has invalid uncompressed range [%d,%d]", index, start, end)
	}
	length := end - start
	if length > compressedBlockMaxAlloc() {
		return 0, fmt.Errorf("snapshots: compressed-block entry %d uncompressed length %d exceeds allocation limit", index, length)
	}
	if length > compressedBlockMaxDecodedBlockSize {
		return 0, fmt.Errorf("snapshots: compressed-block entry %d uncompressed length %d exceeds decoded block limit %d", index, length, compressedBlockMaxDecodedBlockSize)
	}
	return length, nil
}

// findBlock returns the index of the block whose uncompressed range contains
// offset, or -1.
func (r *compressedBlockReader) findBlock(offset uint64) int {
	i := sort.Search(len(r.table), func(k int) bool { return r.table[k].uncompressedStart > offset })
	return i - 1
}

// blockBytes returns the decompressed bytes of block i (caller holds r.mu). The
// returned slice aliases the cache and must not be retained past the unlock.
func (r *compressedBlockReader) blockBytes(i int) ([]byte, error) {
	for j := range r.cache {
		if r.cache[j].idx == i {
			e := r.cache[j]
			copy(r.cache[1:j+1], r.cache[:j]) // move to front (MRU)
			r.cache[0] = e
			return e.bytes, nil
		}
	}
	b := r.table[i]
	expectedLen, err := compressedBlockExpectedLen(r.table, r.uncSize, i)
	if err != nil {
		return nil, err
	}
	compStart, overflow := checkedAdd(r.dataOff, b.compressedStart)
	if overflow {
		return nil, fmt.Errorf("snapshots: compressed-block entry %d physical offset overflows", i)
	}
	compEnd, overflow := checkedAdd(compStart, b.compressedLen)
	if overflow || compEnd > r.fileSize {
		return nil, fmt.Errorf("snapshots: compressed-block entry %d range [%d,%d] outside file size %d", i, compStart, compEnd, r.fileSize)
	}
	if compEnd > uint64(1<<63-1) {
		return nil, fmt.Errorf("snapshots: compressed-block entry %d end %d exceeds int64", i, compEnd)
	}
	if b.compressedLen > compressedBlockMaxAlloc() {
		return nil, fmt.Errorf("snapshots: compressed-block entry %d compressed length %d exceeds allocation limit", i, b.compressedLen)
	}
	compressedLen := int(b.compressedLen)
	if cap(r.compressed) < compressedLen {
		r.compressed = make([]byte, compressedLen)
	}
	comp := r.compressed[:compressedLen]
	if _, err := r.f.ReadAt(comp, int64(compStart)); err != nil {
		return nil, err
	}
	var decoded []byte
	if len(r.cache) >= r.cacheLimit {
		decoded = r.cache[len(r.cache)-1].bytes[:0]
	}
	if uint64(cap(decoded)) < expectedLen {
		decoded = make([]byte, 0, int(expectedLen))
	}
	dst, err := r.dec.DecodeAll(comp, decoded)
	if err != nil {
		return nil, err
	}
	if uint64(len(dst)) != expectedLen {
		return nil, fmt.Errorf("snapshots: compressed-block entry %d decoded to %d bytes, want %d", i, len(dst), expectedLen)
	}
	if len(r.cache) < r.cacheLimit {
		r.cache = append(r.cache, cbCacheEntry{})
	}
	copy(r.cache[1:], r.cache[:len(r.cache)-1]) // shift down, evicting the tail
	r.cache[0] = cbCacheEntry{idx: i, bytes: dst}
	return dst, nil
}

// RecordTailAt returns a private copy of the decompressed block from the record
// at offset to the end of its block. The caller decodes one self-delimiting
// record from the head of the returned slice. Used for keyed point lookups.
func (r *compressedBlockReader) RecordTailAt(offset uint64) ([]byte, error) {
	if offset >= r.uncSize {
		return nil, fmt.Errorf("snapshots: compressed-block offset %d >= size %d", offset, r.uncSize)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.findBlock(offset)
	if i < 0 {
		return nil, fmt.Errorf("snapshots: no compressed block for offset %d", offset)
	}
	blk, err := r.blockBytes(i)
	if err != nil {
		return nil, err
	}
	intra := offset - r.table[i].uncompressedStart
	if intra > uint64(len(blk)) {
		return nil, fmt.Errorf("snapshots: compressed-block intra offset %d > block len %d", intra, len(blk))
	}
	return append([]byte(nil), blk[intra:]...), nil
}

// ReadAt implements io.ReaderAt over the uncompressed logical content: it fills p
// with bytes starting at uncompressed offset off, decompressing whatever blocks
// the range spans (one-block cache makes a sequential scan decompress each block
// once). This is the seam that lets existing offset/ReadAt-based readers operate
// over a compressed segment unchanged: store the segment's plain bytes through
// compressBlobToFile, then hand the reader this ReadAt and the logical size.
func (r *compressedBlockReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("snapshots: negative compressed-block read offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	uoff := uint64(off)
	if uoff >= r.uncSize {
		return 0, io.EOF
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for n < len(p) {
		cur := uoff + uint64(n)
		if cur >= r.uncSize {
			return n, io.EOF
		}
		i := r.findBlock(cur)
		if i < 0 {
			return n, fmt.Errorf("snapshots: no compressed block for offset %d", cur)
		}
		blk, err := r.blockBytes(i)
		if err != nil {
			return n, err
		}
		intra := cur - r.table[i].uncompressedStart
		if intra >= uint64(len(blk)) {
			return n, io.ErrUnexpectedEOF
		}
		n += copy(p[n:], blk[intra:])
	}
	return n, nil
}

// UncompressedSize returns the total uncompressed logical size, i.e. the value a
// caller passes as fileSize to a bounded reader operating over ReadAt.
func (r *compressedBlockReader) UncompressedSize() uint64 { return r.uncSize }

// isCompressedBlockBlob reports whether data begins with the compressed-block
// magic — i.e. whether a segment file is compressed.
func isCompressedBlockBlob(data []byte) bool {
	return len(data) >= len(compressedBlockMagic) && string(data[:len(compressedBlockMagic)]) == compressedBlockMagic
}

// decompressBlockBlob returns the full uncompressed logical content of an
// in-memory compressed-block blob (the inverse of compressBlobToFile). Used by
// the whole-segment readers/checkers that scan the entire segment in memory.
func decompressBlockBlob(data []byte) ([]byte, error) {
	if len(data) < compressedBlockHeaderSize || !isCompressedBlockBlob(data) {
		return nil, errors.New("snapshots: not a compressed-block blob")
	}
	_, dec, err := cbCodec()
	if err != nil {
		return nil, err
	}
	blockCount := binary.BigEndian.Uint64(data[24:32])
	uncSize := binary.BigEndian.Uint64(data[32:40])
	dataOff := binary.BigEndian.Uint64(data[40:48])
	tableLen, err := compressedBlockTableLen(blockCount)
	if err != nil {
		return nil, err
	}
	tableEnd, overflow := checkedAdd(compressedBlockHeaderSize, tableLen)
	if overflow || tableEnd > uint64(len(data)) {
		return nil, errors.New("snapshots: corrupt compressed-block blob header")
	}
	if dataOff != tableEnd {
		return nil, fmt.Errorf("snapshots: compressed-block data offset %d, want table end %d", dataOff, tableEnd)
	}
	table := make([]cbBlock, blockCount)
	for i := uint64(0); i < blockCount; i++ {
		o := uint64(compressedBlockHeaderSize) + i*compressedBlockTableEntry
		table[i] = cbBlock{
			uncompressedStart: binary.BigEndian.Uint64(data[o : o+8]),
			compressedStart:   binary.BigEndian.Uint64(data[o+8 : o+16]),
			compressedLen:     binary.BigEndian.Uint64(data[o+16 : o+24]),
			records:           binary.BigEndian.Uint32(data[o+24 : o+28]),
		}
	}
	if err := validateCompressedBlockTable(table, binary.BigEndian.Uint64(data[16:24]), uncSize, dataOff, uint64(len(data))); err != nil {
		return nil, err
	}
	if uncSize > compressedBlockMaxAlloc() {
		return nil, fmt.Errorf("snapshots: compressed-block uncompressed size %d exceeds allocation limit", uncSize)
	}
	out := make([]byte, 0)
	for i := range table {
		expectedLen, err := compressedBlockExpectedLen(table, uncSize, i)
		if err != nil {
			return nil, err
		}
		start, overflow := checkedAdd(dataOff, table[i].compressedStart)
		if overflow {
			return nil, fmt.Errorf("snapshots: compressed-block blob block %d offset overflows", i)
		}
		end, overflow := checkedAdd(start, table[i].compressedLen)
		if overflow || end > uint64(len(data)) {
			return nil, errors.New("snapshots: corrupt compressed-block blob block")
		}
		dst, err := dec.DecodeAll(data[start:end], make([]byte, 0, int(expectedLen)))
		if err != nil {
			return nil, err
		}
		if uint64(len(dst)) != expectedLen {
			return nil, fmt.Errorf("snapshots: compressed-block blob block %d decoded to %d bytes, want %d", i, len(dst), expectedLen)
		}
		out = append(out, dst...)
	}
	if uint64(len(out)) != uncSize {
		return nil, fmt.Errorf("snapshots: compressed-block blob decompressed to %d bytes, want %d", len(out), uncSize)
	}
	return out, nil
}

// compressBlobToFile stores blob block-compressed at path, chunked into chunkSize
// pieces (one block each). The result is byte-addressable via ReadAt at the same
// offsets as the original blob — so an existing segment's serialized bytes can be
// compressed on disk while its .idx/.kv accessor offsets stay valid.
func compressBlobToFile(dir, path string, blob []byte, chunkSize int) error {
	if chunkSize <= 0 {
		chunkSize = 16384
	}
	w, err := newCompressedBlockWriter(dir, 1)
	if err != nil {
		return err
	}
	for off := 0; off < len(blob); off += chunkSize {
		end := off + chunkSize
		if end > len(blob) {
			end = len(blob)
		}
		if _, err := w.Append(blob[off:end]); err != nil {
			_ = w.tmp.Close()
			_ = os.Remove(w.tmpName)
			return err
		}
	}
	return w.Finish(path)
}

// historyCompressionConcurrency follows Erigon's ordered page-compression
// worker model while reserving CPU for block execution and Pebble compaction.
// Four encoders are enough to hide this short stage without scaling memory or
// scheduler pressure with a large host-wide CPU count.
func historyCompressionConcurrency(procs int) int {
	if procs < 1 {
		return 1
	}
	if procs > maxHistoryCompressionWorkers {
		return maxHistoryCompressionWorkers
	}
	return procs
}

type compressedFileJob struct {
	seq  uint64
	data []byte
}

type compressedFileResult struct {
	seq          uint64
	uncompressed []byte
	compressed   []byte
}

// orderedCompressionPipeline accepts owned, fixed-size logical chunks from one
// writer, encodes them on bounded workers, and appends results to out in input
// order. The caller owns exactly one additional current chunk; the pool holds
// the other workers*2-1 chunks, keeping total uncompressed in-flight memory at
// workers*2 chunks. A matching bounded pool of encoded destinations follows
// each result through the ordered reducer and is reused only after its bytes
// have been written to the body file.
type orderedCompressionPipeline struct {
	out       *compressedBlockWriter
	jobs      chan compressedFileJob
	results   chan compressedFileResult
	pool      chan []byte
	encoded   chan []byte
	done      chan struct{}
	cancel    sync.Once
	closeJobs sync.Once
	workers   sync.WaitGroup
	reducer   sync.WaitGroup

	errMu sync.Mutex
	err   error
	seq   uint64
}

func newOrderedCompressionPipeline(out *compressedBlockWriter, chunkSize, workers int) *orderedCompressionPipeline {
	inFlight := workers * 2
	p := &orderedCompressionPipeline{
		out:     out,
		jobs:    make(chan compressedFileJob, inFlight),
		results: make(chan compressedFileResult, inFlight),
		pool:    make(chan []byte, inFlight),
		encoded: make(chan []byte, inFlight),
		done:    make(chan struct{}),
	}
	// The stream writer transfers its current chunk as the final pool member.
	for i := 1; i < inFlight; i++ {
		p.pool <- make([]byte, 0, chunkSize)
	}
	for range inFlight {
		p.encoded <- nil
	}

	p.workers.Add(workers)
	for range workers {
		go func() {
			defer p.workers.Done()
			for {
				select {
				case job, ok := <-p.jobs:
					if !ok {
						return
					}
					var encoded []byte
					select {
					case encoded = <-p.encoded:
					case <-p.done:
						return
					}
					result := compressedFileResult{
						seq:          job.seq,
						uncompressed: job.data,
						compressed:   p.out.enc.EncodeAll(job.data, encoded[:0]),
					}
					select {
					case p.results <- result:
					case <-p.done:
						return
					}
				case <-p.done:
					return
				}
			}
		}()
	}
	go func() {
		p.workers.Wait()
		close(p.results)
	}()
	p.reducer.Add(1)
	go p.reduce()
	return p
}

func (p *orderedCompressionPipeline) reduce() {
	defer p.reducer.Done()
	pending := make(map[uint64]compressedFileResult, cap(p.results))
	var next uint64
	for result := range p.results {
		if p.Err() != nil {
			continue
		}
		pending[result.seq] = result
		for {
			ready, ok := pending[next]
			if !ok {
				break
			}
			if err := p.out.appendEncodedBlock(len(ready.uncompressed), ready.compressed); err != nil {
				p.setError(err)
				break
			}
			delete(pending, next)
			select {
			case p.pool <- ready.uncompressed[:0]:
			case <-p.done:
			}
			select {
			case p.encoded <- ready.compressed[:0]:
			case <-p.done:
			}
			next++
		}
	}
	if p.Err() == nil && len(pending) != 0 {
		p.setError(fmt.Errorf("snapshots: streaming compression reducer stopped with %d pending blocks", len(pending)))
	}
}

func (p *orderedCompressionPipeline) setError(err error) {
	if err == nil {
		return
	}
	p.errMu.Lock()
	if p.err == nil {
		p.err = err
		p.cancel.Do(func() { close(p.done) })
	}
	p.errMu.Unlock()
}

func (p *orderedCompressionPipeline) Err() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.err
}

func (p *orderedCompressionPipeline) Submit(data []byte) error {
	if err := p.Err(); err != nil {
		return err
	}
	job := compressedFileJob{seq: p.seq, data: data}
	select {
	case p.jobs <- job:
		p.seq++
		return nil
	case <-p.done:
		return p.Err()
	}
}

func (p *orderedCompressionPipeline) Acquire() ([]byte, error) {
	select {
	case buf := <-p.pool:
		return buf, nil
	case <-p.done:
		return nil, p.Err()
	}
}

func (p *orderedCompressionPipeline) Close() error {
	p.closeJobs.Do(func() { close(p.jobs) })
	p.reducer.Wait()
	return p.Err()
}

func (p *orderedCompressionPipeline) Abort() {
	p.cancel.Do(func() { close(p.done) })
	p.closeJobs.Do(func() { close(p.jobs) })
	p.reducer.Wait()
}

// compressedBlockStreamWriter preserves only the first logical chunk for
// fixed-header WriteAt updates. Every later full chunk is immediately encoded;
// after the measured break-even size, encoding moves to the ordered worker
// pipeline above. No complete uncompressed segment is written to disk.
type compressedBlockStreamWriter struct {
	dir       string
	chunkSize int
	workers   int
	body      *compressedBlockWriter
	first     []byte
	chunk     []byte
	parallel  *orderedCompressionPipeline
	logical   uint64
	bodyStart bool
	closed    bool
}

func newCompressedBlockStreamWriter(dir string, chunkSize, workers int) (*compressedBlockStreamWriter, error) {
	if chunkSize <= 0 {
		chunkSize = 16384
	}
	if workers < 1 {
		workers = 1
	}
	body, err := newCompressedBlockWriter(dir, 1)
	if err != nil {
		return nil, err
	}
	return &compressedBlockStreamWriter{
		dir:       dir,
		chunkSize: chunkSize,
		workers:   workers,
		body:      body,
		first:     make([]byte, 0, chunkSize),
	}, nil
}

func (w *compressedBlockStreamWriter) Write(p []byte) (int, error) {
	if w == nil || w.closed || w.body == nil {
		return 0, errors.New("snapshots: compressed stream writer is closed")
	}
	written := 0
	if len(w.first) < w.chunkSize {
		n := w.chunkSize - len(w.first)
		if n > len(p) {
			n = len(p)
		}
		w.first = append(w.first, p[:n]...)
		p = p[n:]
		written += n
		w.logical += uint64(n)
		if len(p) == 0 {
			return written, nil
		}
	}
	if w.chunk == nil {
		w.chunk = make([]byte, 0, w.chunkSize)
	}
	for len(p) != 0 {
		n := w.chunkSize - len(w.chunk)
		if n > len(p) {
			n = len(p)
		}
		w.chunk = append(w.chunk, p[:n]...)
		p = p[n:]
		written += n
		w.logical += uint64(n)
		if len(w.chunk) == w.chunkSize {
			if err := w.flushChunk(false); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (w *compressedBlockStreamWriter) WriteAt(p []byte, off int64) (int, error) {
	if w == nil || w.closed || w.body == nil {
		return 0, errors.New("snapshots: compressed stream writer is closed")
	}
	if off < 0 || off > int64(len(w.first)) || int64(len(p)) > int64(len(w.first))-off {
		return 0, errors.New("snapshots: compressed stream WriteAt is outside retained prefix")
	}
	return copy(w.first[int(off):], p), nil
}

func (w *compressedBlockStreamWriter) flushChunk(final bool) error {
	if len(w.chunk) == 0 {
		return nil
	}
	if !w.bodyStart {
		w.body.uncTotal = uint64(len(w.first))
		w.bodyStart = true
	}
	if w.parallel == nil && w.workers > 1 && w.logical >= minParallelHistoryCompressionBytes {
		w.parallel = newOrderedCompressionPipeline(w.body, w.chunkSize, w.workers)
	}
	if w.parallel == nil {
		w.body.encoded = w.body.enc.EncodeAll(w.chunk, w.body.encoded[:0])
		comp := w.body.encoded
		if err := w.body.appendEncodedBlock(len(w.chunk), comp); err != nil {
			return err
		}
		w.chunk = w.chunk[:0]
		return nil
	}
	if err := w.parallel.Submit(w.chunk); err != nil {
		return err
	}
	w.chunk = nil
	if !final {
		chunk, err := w.parallel.Acquire()
		if err != nil {
			return err
		}
		w.chunk = chunk
	}
	return nil
}

func (w *compressedBlockStreamWriter) Finish(path string) error {
	if w == nil || w.closed || w.body == nil {
		return errors.New("snapshots: compressed stream writer is closed")
	}
	if err := w.flushChunk(true); err != nil {
		w.Abort()
		return err
	}
	if w.parallel != nil {
		if err := w.parallel.Close(); err != nil {
			w.Abort()
			return err
		}
		w.parallel = nil
	}
	body := w.body
	w.body = nil
	w.closed = true
	return body.finishWithPrefix(path, w.first)
}

func (w *compressedBlockStreamWriter) Reset() error {
	if w == nil || w.closed {
		return errors.New("snapshots: compressed stream writer is closed")
	}
	w.abortBody()
	body, err := newCompressedBlockWriter(w.dir, 1)
	if err != nil {
		w.closed = true
		return err
	}
	w.body = body
	w.first = w.first[:0]
	w.chunk = nil
	w.logical = 0
	w.bodyStart = false
	return nil
}

func (w *compressedBlockStreamWriter) Abort() {
	if w == nil || w.closed {
		return
	}
	w.abortBody()
	w.closed = true
}

func (w *compressedBlockStreamWriter) abortBody() {
	if w.parallel != nil {
		w.parallel.Abort()
		w.parallel = nil
	}
	if w.body != nil {
		releaseStateDomainChangeHistoryWriter(&w.body.tmpWriter)
		_ = w.body.tmp.Close()
		_ = os.Remove(w.body.tmpName)
		w.body = nil
	}
}

// BlockAt returns a private copy of the decompressed block containing offset plus
// that block's uncompressed start. Used for sequential range iteration: the
// caller walks records across blocks, calling BlockAt(start+len(block)) next.
func (r *compressedBlockReader) BlockAt(offset uint64) ([]byte, uint64, error) {
	if offset >= r.uncSize {
		return nil, 0, fmt.Errorf("snapshots: compressed-block offset %d >= size %d", offset, r.uncSize)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.findBlock(offset)
	if i < 0 {
		return nil, 0, fmt.Errorf("snapshots: no compressed block for offset %d", offset)
	}
	blk, err := r.blockBytes(i)
	if err != nil {
		return nil, 0, err
	}
	return append([]byte(nil), blk...), r.table[i].uncompressedStart, nil
}
