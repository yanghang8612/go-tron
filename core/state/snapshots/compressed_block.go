package snapshots

import (
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
	compressedBlockMagic       = "gtcblk01"
	compressedBlockVersion     = uint32(1)
	compressedBlockHeaderSize  = 8 + 4 + 4 + 8 + 8 + 8 + 8 // = 48
	compressedBlockTableEntry  = 8 + 8 + 8 + 4             // = 28
	CompressedBlockDefaultSize = 128                       // matches the .bt block size
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
		cbDec, cbInitErr = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
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
	tmpName   string
	table     []cbBlock
	buf       []byte
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
	return &compressedBlockWriter{enc: enc, blockSize: blockSize, tmp: tmp, tmpName: tmp.Name()}, nil
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
	comp := w.enc.EncodeAll(w.buf, nil)
	if _, err := w.tmp.Write(comp); err != nil {
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

// Finish flushes the last partial block and writes the assembled file to path.
func (w *compressedBlockWriter) Finish(path string) (err error) {
	defer func() {
		_ = w.tmp.Close()
		_ = os.Remove(w.tmpName)
	}()
	if e := w.flushBlock(); e != nil {
		return e
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
	if _, err = io.Copy(out, w.tmp); err != nil {
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
	table     []cbBlock

	mu       sync.Mutex
	cacheIdx int
	cache    []byte
}

func openCompressedBlockReader(path string) (*compressedBlockReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	_, dec, err := cbCodec()
	if err != nil {
		_ = f.Close()
		return nil, err
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
		f:         f,
		dec:       dec,
		blockSize: int(binary.BigEndian.Uint32(hdr[12:16])),
		recCount:  binary.BigEndian.Uint64(hdr[16:24]),
		dataOff:   binary.BigEndian.Uint64(hdr[40:48]),
		cacheIdx:  -1,
	}
	blockCount := binary.BigEndian.Uint64(hdr[24:32])
	r.uncSize = binary.BigEndian.Uint64(hdr[32:40])
	if blockCount > (uint64(1)<<40) || blockCount*compressedBlockTableEntry > uint64(1)<<40 {
		_ = f.Close()
		return nil, fmt.Errorf("snapshots: implausible compressed-block count %d", blockCount)
	}
	tableBytes := make([]byte, blockCount*compressedBlockTableEntry)
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
	return r, nil
}

func (r *compressedBlockReader) Close() error { return r.f.Close() }

// findBlock returns the index of the block whose uncompressed range contains
// offset, or -1.
func (r *compressedBlockReader) findBlock(offset uint64) int {
	i := sort.Search(len(r.table), func(k int) bool { return r.table[k].uncompressedStart > offset })
	return i - 1
}

// blockBytes returns the decompressed bytes of block i (caller holds r.mu). The
// returned slice aliases the cache and must not be retained past the unlock.
func (r *compressedBlockReader) blockBytes(i int) ([]byte, error) {
	if r.cacheIdx == i {
		return r.cache, nil
	}
	b := r.table[i]
	comp := make([]byte, b.compressedLen)
	if _, err := r.f.ReadAt(comp, int64(r.dataOff+b.compressedStart)); err != nil {
		return nil, err
	}
	dst, err := r.dec.DecodeAll(comp, nil)
	if err != nil {
		return nil, err
	}
	r.cacheIdx = i
	r.cache = dst
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
