package snapshots

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// V2 writes the body once. The retained first logical chunk is physically last,
// followed by the logical-order table and a fixed trailer. Header fields other
// than magic/version/blockSize are reserved zero: no on-disk backpatch changes
// bytes already included in the ordinary whole-file SHA-256.
const (
	compressedBlockFooterVersion = uint32(2)
	compressedBlockFooterMagic   = "gtcend02"
	compressedBlockTrailerSize   = 48
)

var (
	historyFooterFilesCounter       = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"compression_footer/files", nil)
	historyFooterCopyBytesCounter   = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"compression_footer/body_copy_avoided_bytes", nil)
	historyFooterFinishNanosCounter = metrics.NewRegisteredCounter(defaultColdSnapshotMetrics+"compression_footer/finish_nanos", nil)
)

func historyCompressionFooterEnabled() (bool, error) {
	switch value := os.Getenv("GTRON_HISTORY_COMPRESSION_FORMAT"); value {
	case "", "1":
		return false, nil
	case "2", "3", "auto":
		return true, nil
	default:
		return false, fmt.Errorf("snapshots: invalid GTRON_HISTORY_COMPRESSION_FORMAT %q (want auto, 1, 2 or 3)", value)
	}
}

func newCompressedBlockStreamBody(dir string, footer bool) (*compressedBlockWriter, error) {
	w, err := newCompressedBlockWriter(dir, 1)
	if err != nil || !footer {
		return w, err
	}
	w.footerMetadata = newSnapshotMetadataWriter(w.tmp)
	w.tmpWriter.Reset(w.footerMetadata)
	var header [compressedBlockHeaderSize]byte
	copy(header[:8], compressedBlockMagic)
	binary.BigEndian.PutUint32(header[8:12], compressedBlockFooterVersion)
	binary.BigEndian.PutUint32(header[12:16], uint32(w.blockSize))
	if _, err := w.tmpWriter.Write(header[:]); err != nil {
		releaseStateDomainChangeHistoryBlockTable(&w.table)
		releaseStateDomainChangeHistoryWriter(&w.tmpWriter)
		_ = w.tmp.Close()
		_ = os.Remove(w.tmpName)
		return nil, err
	}
	return w, nil
}

func (w *compressedBlockWriter) finishWithFooterContext(ctx context.Context, path string, prefix []byte, metadata *snapshotFileMetadata) (err error) {
	if metadata != nil {
		*metadata = snapshotFileMetadata{}
	}
	started := time.Now()
	defer func() {
		releaseStateDomainChangeHistoryBlockTable(&w.table)
		releaseStateDomainChangeHistoryWriter(&w.tmpWriter)
		_ = w.tmp.Close()
		if w.tmpName != "" {
			_ = os.Remove(w.tmpName)
		}
	}()
	if err := contextError(ctx); err != nil {
		return err
	}
	if w.footerMetadata == nil || w.bufRecs != 0 || len(w.buf) != 0 {
		return errors.New("snapshots: invalid footer writer state")
	}
	// All compression workers have joined. Subsequent buffered writes now obey
	// cancellation; the temporary inode is never published before Sync/Close.
	w.footerMetadata.dst = contextWriter{ctx: ctx, w: w.tmp}
	uncTotal, recCount := w.uncTotal, w.recCount
	var prefixComp []byte
	if len(prefix) != 0 {
		if recCount == 0 {
			uncTotal = uint64(len(prefix))
		} else if len(w.table) == 0 || w.table[0].uncompressedStart != uint64(len(prefix)) {
			return errors.New("snapshots: compressed body does not follow retained prefix")
		}
		w.encoded = w.enc.EncodeAll(prefix, w.encoded[:0])
		prefixComp = w.encoded
		recCount++
	} else if recCount != 0 {
		return errors.New("snapshots: compressed body has no retained prefix")
	}
	blockCount := uint64(len(w.table))
	if len(prefixComp) != 0 {
		blockCount++
	}
	tableLen, err := compressedBlockTableLen(blockCount)
	if err != nil {
		return err
	}
	tableOff, overflow := checkedAdd(compressedBlockHeaderSize, w.compTotal)
	if overflow {
		return errors.New("snapshots: footer body offset overflows")
	}
	tableOff, overflow = checkedAdd(tableOff, uint64(len(prefixComp)))
	if overflow {
		return errors.New("snapshots: footer table offset overflows")
	}
	if _, err := w.tmpWriter.Write(prefixComp); err != nil {
		return err
	}
	var entry [compressedBlockTableEntry]byte
	writeEntry := func(block cbBlock) error {
		binary.BigEndian.PutUint64(entry[0:8], block.uncompressedStart)
		binary.BigEndian.PutUint64(entry[8:16], block.compressedStart)
		binary.BigEndian.PutUint64(entry[16:24], block.compressedLen)
		binary.BigEndian.PutUint32(entry[24:28], block.records)
		_, err := w.tmpWriter.Write(entry[:])
		return err
	}
	if len(prefixComp) != 0 {
		if err := writeEntry(cbBlock{compressedStart: w.compTotal, compressedLen: uint64(len(prefixComp)), records: 1}); err != nil {
			return err
		}
	}
	for _, block := range w.table {
		if err := contextError(ctx); err != nil {
			return err
		}
		if err := writeEntry(block); err != nil {
			return err
		}
	}
	var trailer [compressedBlockTrailerSize]byte
	copy(trailer[:8], compressedBlockFooterMagic)
	binary.BigEndian.PutUint64(trailer[8:16], recCount)
	binary.BigEndian.PutUint64(trailer[16:24], blockCount)
	binary.BigEndian.PutUint64(trailer[24:32], uncTotal)
	binary.BigEndian.PutUint64(trailer[32:40], tableOff)
	binary.BigEndian.PutUint64(trailer[40:48], tableLen)
	if _, err := w.tmpWriter.Write(trailer[:]); err != nil {
		return err
	}
	if err := w.tmpWriter.Flush(); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := w.tmp.Sync(); err != nil {
		return err
	}
	if err := w.tmp.Close(); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := os.Rename(w.tmpName, path); err != nil {
		return err
	}
	w.tmpName = ""
	if metadata != nil {
		*metadata = w.footerMetadata.Metadata()
	}
	historyFooterFilesCounter.Inc(1)
	historyFooterCopyBytesCounter.Inc(int64(w.compTotal))
	historyFooterFinishNanosCounter.Inc(time.Since(started).Nanoseconds())
	return nil
}

type compressedBlockFooterInfo struct {
	blockSize                                         int
	recCount, blockCount, uncSize, tableOff, tableLen uint64
}

type compressedBlockFooterLayout struct {
	compressedBlockFooterInfo
	table []cbBlock
}

func readCompressedBlockFooterInfo(src io.ReaderAt, size uint64, header []byte) (compressedBlockFooterInfo, error) {
	var info compressedBlockFooterInfo
	if size < compressedBlockHeaderSize+compressedBlockTrailerSize || size > uint64(1<<63-1) || len(header) != compressedBlockHeaderSize {
		return info, errors.New("snapshots: invalid footer file size")
	}
	if string(header[:8]) != compressedBlockMagic || binary.BigEndian.Uint32(header[8:12]) != compressedBlockFooterVersion {
		return info, errors.New("snapshots: invalid footer header")
	}
	for _, b := range header[16:] {
		if b != 0 {
			return info, errors.New("snapshots: nonzero reserved footer header field")
		}
	}
	info.blockSize = int(binary.BigEndian.Uint32(header[12:16]))
	if info.blockSize <= 0 {
		return info, errors.New("snapshots: compressed-block has zero block size")
	}
	var trailer [compressedBlockTrailerSize]byte
	if _, err := src.ReadAt(trailer[:], int64(size-compressedBlockTrailerSize)); err != nil {
		return info, err
	}
	if string(trailer[:8]) != compressedBlockFooterMagic {
		return info, errors.New("snapshots: missing compressed-block footer trailer")
	}
	info.recCount = binary.BigEndian.Uint64(trailer[8:16])
	info.blockCount = binary.BigEndian.Uint64(trailer[16:24])
	info.uncSize = binary.BigEndian.Uint64(trailer[24:32])
	info.tableOff = binary.BigEndian.Uint64(trailer[32:40])
	info.tableLen = binary.BigEndian.Uint64(trailer[40:48])
	wantLen, err := compressedBlockTableLen(info.blockCount)
	if err != nil {
		return info, err
	}
	end, overflow := checkedAdd(info.tableOff, info.tableLen)
	if overflow || info.tableOff < compressedBlockHeaderSize || info.tableLen != wantLen || end != size-compressedBlockTrailerSize || info.blockCount > compressedBlockMaxAlloc()/32 {
		return info, errors.New("snapshots: invalid compressed-block footer table bounds")
	}
	return info, nil
}

func readCompressedBlockFooterLayout(src io.ReaderAt, size uint64, header []byte) (compressedBlockFooterLayout, error) {
	var layout compressedBlockFooterLayout
	info, err := readCompressedBlockFooterInfo(src, size, header)
	if err != nil {
		return layout, err
	}
	layout.compressedBlockFooterInfo = info
	layout.table = make([]cbBlock, info.blockCount)
	// Decode directly into the final table; do not retain a second full encoded
	// table allocation for large history files.
	reader := bufio.NewReaderSize(io.NewSectionReader(src, int64(info.tableOff), int64(info.tableLen)), 32<<10)
	var entry [compressedBlockTableEntry]byte
	for i := range layout.table {
		if _, err := io.ReadFull(reader, entry[:]); err != nil {
			return layout, err
		}
		layout.table[i] = cbBlock{
			uncompressedStart: binary.BigEndian.Uint64(entry[0:8]),
			compressedStart:   binary.BigEndian.Uint64(entry[8:16]),
			compressedLen:     binary.BigEndian.Uint64(entry[16:24]),
			records:           binary.BigEndian.Uint32(entry[24:28]),
		}
	}
	if err := validateCompressedBlockPhysicalTable(layout.table, info.recCount, info.uncSize, compressedBlockHeaderSize, info.tableOff, true); err != nil {
		return layout, err
	}
	return layout, nil
}
