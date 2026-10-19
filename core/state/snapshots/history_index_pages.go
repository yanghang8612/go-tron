package snapshots

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

const (
	historyIndexPageBytes      = 4 << 10
	historyIndexPageHeader     = 96
	historyIndexPageEntry      = 24
	historyIndexRawPage        = uint32(1 << 31)
	historyIndexSnappyPages    = uint32(1 << 31)
	historyIndexPageCacheBytes = 64 << 10
)

type historyIndexPage struct {
	offset               uint64
	stored, decoded, crc uint32
}
type cachedIndexPage struct {
	data []byte
	tick uint64
}
type historyIndexPages struct {
	snappy    bool
	encoded   []byte
	file      historySegmentReader
	logical   uint64
	pageBytes uint64
	pages     []historyIndexPage
	decoder   *zstd.Decoder
	mu        sync.Mutex
	tick      uint64
	cache     map[uint64]cachedIndexPage
}

// The outer physical file carries V8, while each page reproduces the unchanged
// V7 byte stream. All inner offsets, frame CRCs and query validation stay intact.
func rewriteHistoryIndexPagesContext(ctx context.Context, source *os.File, name string, header stateDomainChangeBinaryHeader) (*os.File, string, error) {
	return rewriteHistoryIndexPagesWithSize(ctx, source, name, header, historyIndexPageBytes)
}
func rewriteHistoryIndexPagesWithSize(ctx context.Context, source *os.File, name string, header stateDomainChangeBinaryHeader, pageBytes uint64) (*os.File, string, error) {
	return rewriteHistoryIndexPagesWithCodec(ctx, source, name, header, pageBytes, true)
}

func rewriteHistoryIndexPagesWithCodec(ctx context.Context, source *os.File, name string, header stateDomainChangeBinaryHeader, pageBytes uint64, useSnappy bool) (*os.File, string, error) {
	if pageBytes != 4096 && pageBytes != 16384 && pageBytes != 65536 {
		return nil, "", errors.New("snapshots: unsupported index page size")
	}
	stat, err := source.Stat()
	if err != nil {
		return nil, "", err
	}
	logical := uint64(stat.Size())
	count := ceilDiv(logical, pageBytes)
	if count > 64<<20/historyIndexPageEntry {
		return nil, "", errors.New("snapshots: paged index directory exceeds budget")
	}
	out, outName, err := createStateDomainChangeBinaryTempFileInDir(filepath.Dir(name), filepath.Base(name)+".pages")
	if err != nil {
		return nil, "", err
	}
	ok := false
	defer func() {
		if !ok {
			out.Close()
			os.Remove(outName)
		}
	}()
	var enc *zstd.Encoder
	if !useSnappy {
		enc, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderCRC(true))
		if err != nil {
			return nil, "", err
		}
		defer enc.Close()
	}
	directory := make([]byte, count*historyIndexPageEntry)
	offset := uint64(historyIndexPageHeader) + uint64(len(directory))
	if _, err = out.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, "", err
	}
	buf := make([]byte, pageBytes)
	for i := uint64(0); i < count; i++ {
		if err := contextError(ctx); err != nil {
			return nil, "", err
		}
		n := min(uint64(len(buf)), logical-i*pageBytes)
		if _, err = source.ReadAt(buf[:n], int64(i*pageBytes)); err != nil {
			return nil, "", err
		}
		var data []byte
		if useSnappy {
			data = snappy.Encode(nil, buf[:n])
		} else {
			data = enc.EncodeAll(buf[:n], nil)
		}
		stored := uint32(len(data))
		if len(data) >= int(n) {
			data = buf[:n]
			stored = uint32(n) | historyIndexRawPage
		}
		entry := directory[i*historyIndexPageEntry:]
		binary.BigEndian.PutUint64(entry, offset)
		binary.BigEndian.PutUint32(entry[8:], stored)
		binary.BigEndian.PutUint32(entry[12:], uint32(n))
		binary.BigEndian.PutUint32(entry[16:], crc32.ChecksumIEEE(data))
		if _, err = out.Write(data); err != nil {
			return nil, "", err
		}
		offset += uint64(len(data))
	}
	raw := make([]byte, historyIndexPageHeader)
	copy(raw, stateDomainChangeBinaryIndexMagic[:])
	binary.BigEndian.PutUint32(raw[8:], 8)
	binary.BigEndian.PutUint64(raw[12:], header.fromTxNum)
	binary.BigEndian.PutUint64(raw[20:], header.toTxNum)
	binary.BigEndian.PutUint64(raw[28:], header.count)
	pageCode := uint32(pageBytes)
	if useSnappy {
		pageCode |= historyIndexSnappyPages
	}
	binary.BigEndian.PutUint32(raw[36:], pageCode)
	binary.BigEndian.PutUint64(raw[40:], logical)
	binary.BigEndian.PutUint64(raw[48:], count)
	binary.BigEndian.PutUint64(raw[56:], uint64(len(directory)))
	binary.BigEndian.PutUint64(raw[64:], offset)
	binary.BigEndian.PutUint32(raw[72:], crc32.ChecksumIEEE(directory))
	binary.BigEndian.PutUint32(raw[76:], crc32.ChecksumIEEE(raw[:76]))
	if _, err = out.WriteAt(raw, 0); err != nil {
		return nil, "", err
	}
	if _, err = out.WriteAt(directory, historyIndexPageHeader); err != nil {
		return nil, "", err
	}
	if err = out.Sync(); err != nil {
		return nil, "", err
	}
	if err = contextError(ctx); err != nil {
		return nil, "", err
	}
	if err = source.Close(); err != nil {
		return nil, "", err
	}
	if err = os.Remove(name); err != nil {
		return nil, "", err
	}
	ok = true
	return out, outName, nil
}

func openHistoryIndexPages(file historySegmentReader, size uint64, outer stateDomainChangeBinaryHeader) (*historyIndexPages, error) {
	if size < historyIndexPageHeader {
		return nil, io.ErrUnexpectedEOF
	}
	var h [historyIndexPageHeader]byte
	if _, err := file.ReadAt(h[:], 0); err != nil {
		return nil, err
	}
	pageCode := binary.BigEndian.Uint32(h[36:])
	useSnappy := pageCode&historyIndexSnappyPages != 0
	pageBytes := uint64(pageCode &^ historyIndexSnappyPages)
	if (pageBytes != 4096 && pageBytes != 16384 && pageBytes != 65536) || crc32.ChecksumIEEE(h[:76]) != binary.BigEndian.Uint32(h[76:]) {
		return nil, errors.New("snapshots: paged index header checksum/size")
	}
	for _, b := range h[80:] {
		if b != 0 {
			return nil, errors.New("snapshots: paged index reserved bytes")
		}
	}
	logical, count, dirLen, total := binary.BigEndian.Uint64(h[40:]), binary.BigEndian.Uint64(h[48:]), binary.BigEndian.Uint64(h[56:]), binary.BigEndian.Uint64(h[64:])
	if logical < stateDomainChangeBinaryIndexV7HeaderSize || logical > math.MaxInt64 || count != ceilDiv(logical, pageBytes) || count > 64<<20/historyIndexPageEntry || dirLen != count*historyIndexPageEntry || total != size || dirLen > size-historyIndexPageHeader {
		return nil, errors.New("snapshots: paged index layout")
	}
	raw := make([]byte, dirLen)
	if _, err := file.ReadAt(raw, historyIndexPageHeader); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(raw) != binary.BigEndian.Uint32(h[72:]) {
		return nil, errors.New("snapshots: paged index directory checksum")
	}
	pages := make([]historyIndexPage, count)
	end := uint64(historyIndexPageHeader) + dirLen
	for i := range pages {
		b := raw[i*historyIndexPageEntry:]
		p := historyIndexPage{binary.BigEndian.Uint64(b), binary.BigEndian.Uint32(b[8:]), binary.BigEndian.Uint32(b[12:]), binary.BigEndian.Uint32(b[16:])}
		n := uint64(p.stored &^ historyIndexRawPage)
		expected := min(pageBytes, logical-uint64(i)*pageBytes)
		if p.offset != end || uint64(p.decoded) != expected || n == 0 || n > expected || n > size-end || p.stored&historyIndexRawPage != 0 && n != expected || binary.BigEndian.Uint32(b[20:]) != 0 {
			return nil, errors.New("snapshots: invalid paged index entry")
		}
		pages[i] = p
		end += n
	}
	if end != size {
		return nil, errors.New("snapshots: paged index trailing bytes")
	}
	var dec *zstd.Decoder
	var err error
	if !useSnappy {
		dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(1<<20), zstd.WithDecoderMaxWindow(1<<20))
		if err != nil {
			return nil, err
		}
	}
	return &historyIndexPages{snappy: useSnappy, file: file, logical: logical, pageBytes: pageBytes, pages: pages, decoder: dec, cache: make(map[uint64]cachedIndexPage)}, nil
}

func (r *historyIndexPages) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.decoder != nil {
		r.decoder.Close()
	}
	r.cache = nil
	r.encoded = nil
	return r.file.Close()
}
func (r *historyIndexPages) ReadAt(dst []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("snapshots: negative paged index offset")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(dst) == 0 {
		return 0, nil
	}
	if r.cache == nil {
		return 0, os.ErrClosed
	}
	n := 0
	for n < len(dst) && uint64(off) < r.logical {
		i := uint64(off) / r.pageBytes
		r.tick++
		entry, ok := r.cache[i]
		if !ok {
			p := r.pages[i]
			stored := int(p.stored &^ historyIndexRawPage)
			if cap(r.encoded) < stored {
				r.encoded = make([]byte, stored)
			}
			encoded := r.encoded[:stored]
			if _, err := r.file.ReadAt(encoded, int64(p.offset)); err != nil {
				return n, err
			}
			if crc32.ChecksumIEEE(encoded) != p.crc {
				return n, errors.New("snapshots: paged index page checksum")
			}
			var reuse []byte
			if uint64(len(r.cache)) >= historyIndexPageCacheBytes/r.pageBytes {
				var victim uint64
				oldest := uint64(math.MaxUint64)
				for key, e := range r.cache {
					if e.tick < oldest {
						victim, oldest = key, e.tick
					}
				}
				reuse = r.cache[victim].data[:0]
				delete(r.cache, victim)
			}
			if cap(reuse) < int(p.decoded) {
				reuse = make([]byte, 0, p.decoded)
			}
			var data []byte
			if p.stored&historyIndexRawPage != 0 {
				data = append(reuse, encoded...)
			} else {
				var err error
				if r.snappy {
					decodedLen, decodeErr := snappy.DecodedLen(encoded)
					if decodeErr != nil || decodedLen != int(p.decoded) {
						return n, errors.New("snapshots: paged index Snappy decoded length")
					}
					data, err = snappy.Decode(reuse[:cap(reuse)], encoded)
				} else {
					data, err = r.decoder.DecodeAll(encoded, reuse)
				}
				if err != nil {
					return n, err
				}
			}
			if len(data) != int(p.decoded) {
				return n, errors.New("snapshots: paged index decoded length")
			}
			entry.data = data
		}
		entry.tick = r.tick
		r.cache[i] = entry
		copied := copy(dst[n:], entry.data[uint64(off)%r.pageBytes:])
		n += copied
		off += int64(copied)
	}
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}
