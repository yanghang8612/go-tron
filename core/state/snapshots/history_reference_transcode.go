package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var ErrHistoryReferenceAlreadyEncoded = errors.New("snapshots: history trio already uses the reference container")

// HistoryReferenceTranscodeWorkBytesContext estimates additional local disk
// space before starting a trio; it is not a deletion/coverage proof. It counts
// CDC anchors once, two copies of bounded raw unique bytes (scratch + output),
// two companion copies, two maximal directories and 1GiB slack. V7 validation
// spills 8-byte keys + 18-byte postings + 17-byte ETL headers; 128 bytes/record
// conservatively covers two sequential verifiers and run headers. V6 has no
// verification ETL. Unsupported legacy accessors fail closed instead of
// inventing a bound. Other processes and existing files still require the
// caller's live free-space check; a header estimate cannot reserve disk space.
func HistoryReferenceTranscodeWorkBytesContext(ctx context.Context, dir string, refs []SegmentRef) (estimate uint64, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	history, index, accessor, _, err := historyReferenceTranscodeIdentity(refs)
	if err != nil {
		return 0, err
	}
	if index.Size > historyReferenceMaxPhysical || accessor.Size > historyReferenceMaxPhysical-index.Size {
		return 0, fmt.Errorf("%w: companion work bytes", errHistoryReferenceBudget)
	}
	companions := index.Size + accessor.Size
	path := filepath.Join(dir, history.Path)
	if err = historyReferenceTranscodePreflight(ctx, path, history.Size); err != nil {
		return 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	var magic [8]byte
	if err = historyReferenceReadAt(ctx, f, magic[:], 0); err != nil {
		return 0, err
	}
	if string(magic[:]) == historyReferenceMagic {
		return 0, ErrHistoryReferenceAlreadyEncoded
	}
	var virtual io.ReaderAt = f
	unique := history.Size
	if string(magic[:]) == compressedBlockMagic {
		reader, e := openCompressedBlockReaderWithCacheLimit(path, 1)
		if e != nil {
			return 0, e
		}
		defer func() { err = errors.Join(err, reader.Close()) }()
		virtual, unique = reader, reader.UncompressedSize()
		if reader.cdc != nil {
			unique = 0
			for i := uint64(0); i < reader.cdc.count; i++ {
				if e = ctx.Err(); e != nil {
					return 0, e
				}
				entry, length, e := reader.cdc.entry(i)
				if e != nil {
					return 0, e
				}
				if entry.anchor == cdcAnchor {
					if length > historyReferenceMaxPhysical-unique {
						return 0, fmt.Errorf("%w: CDC unique work bytes", errHistoryReferenceBudget)
					}
					unique += length
				}
			}
		}
	}
	if unique > historyReferenceMaxPhysical-historyReferenceMaxMetadata-historyReferenceHeaderSize {
		return 0, fmt.Errorf("%w: unique scratch bytes", errHistoryReferenceBudget)
	}
	header, e := readStateDomainChangeBinaryHeaderAt(contextReaderAt{ctx: ctx, r: virtual}, stateDomainChangeBinarySegmentMagic)
	if e != nil {
		return 0, e
	}
	if header.version != stateDomainChangeBinaryVersionV6 {
		return 0, errors.New("snapshots: work estimate requires V6 logical history")
	}
	accessorFile, e := os.Open(filepath.Join(dir, accessor.Path))
	if e != nil {
		return 0, e
	}
	accessorHeader, e := readStateDomainChangeBinaryHeaderAt(contextReaderAt{ctx: ctx, r: accessorFile}, stateDomainChangeBinaryAccessorMagic)
	e = errors.Join(e, accessorFile.Close())
	if e != nil {
		return 0, e
	}
	if accessorHeader.fromTxNum != header.fromTxNum || accessorHeader.toTxNum != header.toTxNum || accessorHeader.count != header.count {
		return 0, errors.New("snapshots: work estimate accessor header differs")
	}
	version := accessorHeader.version
	var etl uint64
	if version == stateDomainChangeBinaryVersionV7 {
		if header.count > historyReferenceMaxLogical/128 {
			return 0, fmt.Errorf("%w: verifier ETL record count", errHistoryReferenceBudget)
		}
		etl = header.count * 128
	} else if version != stateDomainChangeBinaryVersionV6 {
		return 0, errors.New("snapshots: work estimate requires V6/V7 accessor for bounded verification")
	}
	return 2*unique + 2*companions + 2*historyReferenceMaxMetadata + (1 << 30) + etl, ctx.Err()
}

// ReencodeHistoryReferenceTrioContext converts one immutable, fully verified V6
// trio in the existing directory. It does not publish a manifest, advance a
// stage, or remove a source. Companions retain every byte and logical offset,
// with paths rebound to the new content-addressed history stem. Source and
// destination coexist until the caller's separate migration protocol commits.
//
// CDC v3 references keep their original anchor boundaries. Only anchors are
// decoded/stored; the bounded mapping of old ordinal to new ID is a scratch
// file. V1/V2 preserve original page boundaries, splitting only a legacy page
// larger than 128KiB. Such a page may retain the existing <=256MiB decoded plus
// <=264MiB encoded buffers; directories are separately capped at 32MiB. There is
// no complete virtual stream or Prev materialization in the conversion stage.
func ReencodeHistoryReferenceTrioContext(ctx context.Context, dir string, refs []SegmentRef) (result []SegmentRef, stats HistoryReferenceContainerStats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	history, index, accessor, cfg, err := historyReferenceTranscodeIdentity(refs)
	if err != nil {
		return nil, stats, err
	}
	// Bound old table/codec allocations before the semantic verifier opens it.
	if err = historyReferenceTranscodePreflight(ctx, filepath.Join(dir, history.Path), history.Size); err != nil {
		return nil, stats, err
	}
	if err = verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, history, index, accessor); err != nil {
		return nil, stats, err
	}
	source, err := os.Open(filepath.Join(dir, history.Path))
	if err != nil {
		return nil, stats, err
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	var magic [8]byte
	if err = historyReferenceReadAt(ctx, source, magic[:], 0); err != nil {
		return nil, stats, err
	}
	if string(magic[:]) == historyReferenceMagic {
		return nil, stats, ErrHistoryReferenceAlreadyEncoded
	}
	var virtual io.ReaderAt = source
	logical := history.Size
	var compressed *compressedBlockReader
	if string(magic[:]) == compressedBlockMagic {
		compressed, err = openCompressedBlockReaderWithCacheLimit(filepath.Join(dir, history.Path), 1)
		if err != nil {
			return nil, stats, err
		}
		defer func() { err = errors.Join(err, compressed.Close()) }()
		virtual, logical = compressed, compressed.UncompressedSize()
	}
	header, err := readStateDomainChangeBinaryHeaderAt(virtual, stateDomainChangeBinarySegmentMagic)
	if err != nil {
		return nil, stats, err
	}
	if header.version != stateDomainChangeBinaryVersionV6 {
		return nil, stats, errors.New("snapshots: reference transcode requires the original V6 logical format")
	}
	w, err := newHistoryReferenceWriter(ctx, dir, 0)
	if err != nil {
		return nil, stats, err
	}
	defer func() { err = errors.Join(err, w.Release()) }()
	if compressed != nil && compressed.cdc != nil {
		err = historyReferenceTranscodeCDC(ctx, w, compressed.cdc)
	} else if compressed != nil {
		for i := range compressed.table {
			if err = ctx.Err(); err != nil {
				break
			}
			var raw []byte
			raw, err = compressed.blockBytes(i)
			if err != nil {
				break
			}
			if err = historyReferenceTranscodeBytes(w, raw); err != nil {
				break
			}
		}
	} else {
		var raw [historyReferenceMaxChunk]byte
		for off := uint64(0); off < logical; {
			n := min(uint64(len(raw)), logical-off)
			if err = historyReferenceReadAt(ctx, source, raw[:n], off); err != nil {
				break
			}
			if err = historyReferenceTranscodeBytes(w, raw[:n]); err != nil {
				break
			}
			off += n
		}
	}
	if err != nil {
		return nil, stats, err
	}
	if w.LogicalSize() != logical {
		return nil, stats, errors.New("snapshots: transcode logical byte count differs")
	}
	output := filepath.Join(w.dir, "output.seg")
	size, checksum, err := w.Finalize(output)
	if err != nil {
		return nil, stats, err
	}
	target, err := openHistoryReferenceReader(ctx, output, 2*historyReferenceMaxChunk)
	if err != nil {
		return nil, stats, err
	}
	err = historyReferenceCompareVirtual(ctx, virtual, target, logical)
	err = errors.Join(err, target.Close())
	if err != nil {
		return nil, stats, err
	}
	newHistory := history
	newHistory.Path = contentAddressedSnapshotPath(history.Path, checksum)
	newHistory.Size, newHistory.Checksum = size, checksum
	if newHistory.Path == history.Path {
		return nil, stats, errors.New("snapshots: reference target aliases source")
	}
	newIndex, newAccessor := index, accessor
	newIndex.Path, newAccessor.Path = cfg.HistoryIndexPathFor(newHistory.Path), cfg.HistoryAccessorPathFor(newHistory.Path)
	// Copy just the two companions, never the original database or virtual data.
	for _, pair := range [][2]SegmentRef{{index, newIndex}, {accessor, newAccessor}} {
		tmp := filepath.Join(w.dir, filepath.Base(pair[1].Path))
		if err = historyReferenceCopyCompanion(ctx, filepath.Join(dir, pair[0].Path), tmp, pair[0]); err != nil {
			return nil, stats, err
		}
		if err = historyReferenceInstallFile(ctx, tmp, filepath.Join(dir, pair[1].Path), pair[1]); err != nil {
			return nil, stats, err
		}
	}
	if err = historyReferenceInstallFile(ctx, output, filepath.Join(dir, newHistory.Path), newHistory); err != nil {
		return nil, stats, err
	}
	if err = verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, newHistory, newIndex, newAccessor); err != nil {
		return nil, stats, err
	}
	// Detect replacement or modification during conversion before returning any
	// actionable new refs. Final names already created on error remain orphans.
	if err = verifyStateDomainChangeBinaryCompanionChecksumsContext(ctx, dir, history, index, accessor); err != nil {
		return nil, stats, err
	}
	if err = ctx.Err(); err != nil {
		return nil, stats, err
	}
	newByKind := map[SegmentKind]SegmentRef{SegmentHistory: newHistory, SegmentInverted: newIndex, SegmentAccessor: newAccessor}
	for _, ref := range refs {
		result = append(result, newByKind[ref.Kind])
	}
	return result, w.Stats(), nil
}

// The work estimate uses the same exact trio identities as conversion so a
// duplicate or unrelated small companion cannot understate required disk space.
func historyReferenceTranscodeIdentity(refs []SegmentRef) (SegmentRef, SegmentRef, SegmentRef, DomainCfg, error) {
	if len(refs) != 3 {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, DomainCfg{}, errors.New("snapshots: reference transcode requires exactly one history trio")
	}
	byKind := make(map[SegmentKind]SegmentRef, 3)
	for _, ref := range refs {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Size == 0 || ref.Checksum == "" {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, DomainCfg{}, errors.New("snapshots: reference transcode requires complete source identities")
		}
		if _, ok := byKind[ref.Kind]; ok {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, DomainCfg{}, errors.New("snapshots: duplicate transcode companion kind")
		}
		if err := validateSegment(ref, ref.FromTxNum, ref.ToTxNum); err != nil {
			return SegmentRef{}, SegmentRef{}, SegmentRef{}, DomainCfg{}, err
		}
		byKind[ref.Kind] = ref
	}
	history, haveHistory := byKind[SegmentHistory]
	index, haveIndex := byKind[SegmentInverted]
	accessor, haveAccessor := byKind[SegmentAccessor]
	cfg, ok := DefaultDomainRegistry().ConfigForRef(history)
	if !haveHistory || !haveIndex || !haveAccessor || !ok || !cfg.IsHistoryBinarySegmentPath(history.Path) ||
		index.Path != cfg.HistoryIndexPathFor(history.Path) || accessor.Path != cfg.HistoryAccessorPathFor(history.Path) ||
		index.FromTxNum != history.FromTxNum || accessor.FromTxNum != history.FromTxNum ||
		index.ToTxNum != history.ToTxNum || accessor.ToTxNum != history.ToTxNum ||
		index.effectiveAggregationSteps() != history.effectiveAggregationSteps() || accessor.effectiveAggregationSteps() != history.effectiveAggregationSteps() {
		return SegmentRef{}, SegmentRef{}, SegmentRef{}, DomainCfg{}, errors.New("snapshots: reference transcode companion identity differs")
	}
	return history, index, accessor, cfg, nil
}

func historyReferenceTranscodeBytes(w *historyReferenceWriter, raw []byte) error {
	for len(raw) > 0 {
		n := min(len(raw), historyReferenceMaxChunk)
		id, err := w.StoreChunk(raw[:n])
		if err != nil {
			return err
		}
		if err = w.WriteSpan(id, 0, uint32(n)); err != nil {
			return err
		}
		raw = raw[n:]
	}
	return nil
}

func historyReferenceTranscodeCDC(ctx context.Context, w *historyReferenceWriter, r *cdcReader) (err error) {
	if r.count > historyReferenceMaxSpans {
		return fmt.Errorf("%w: CDC span count", errHistoryReferenceBudget)
	}
	mapFile, err := os.OpenFile(filepath.Join(w.dir, "cdc-ordinal-map"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, mapFile.Close()) }()
	for ordinal := uint64(0); ordinal < r.count; ordinal++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		entry, length, e := r.entry(ordinal)
		if e != nil {
			return e
		}
		if entry.logical != w.LogicalSize() {
			return errors.New("snapshots: CDC transcode logical gap")
		}
		var id uint32
		if entry.anchor == cdcAnchor {
			raw, e := r.bytes(ordinal, entry, length)
			if e != nil {
				return e
			}
			id, e = w.StoreChunk(raw)
			if e != nil {
				return e
			}
		} else {
			anchor, anchorLength, e := r.entry(uint64(entry.anchor))
			if e != nil {
				return e
			}
			if anchor.anchor != cdcAnchor || uint64(entry.anchor) >= ordinal || anchor.physical != entry.physical || anchor.stored != entry.stored || anchorLength != length {
				return errors.New("snapshots: CDC transcode reference is not a matching full anchor")
			}
			var b [4]byte
			if e = historyReferenceReadAt(ctx, mapFile, b[:], uint64(entry.anchor)*4); e != nil {
				return e
			}
			id = binary.BigEndian.Uint32(b[:])
		}
		if err = w.WriteSpan(id, 0, uint32(length)); err != nil {
			return err
		}
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], id)
		if err = historyReferenceWrite(mapFile, b[:]); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func historyReferenceCompareVirtual(ctx context.Context, a, b io.ReaderAt, size uint64) error {
	var left, right [historyReferenceMaxChunk]byte
	for off := uint64(0); off < size; {
		n := min(uint64(len(left)), size-off)
		if err := historyReferenceReadAt(ctx, a, left[:n], off); err != nil {
			return err
		}
		if err := historyReferenceReadAt(ctx, b, right[:n], off); err != nil {
			return err
		}
		if !bytes.Equal(left[:n], right[:n]) {
			return errors.New("snapshots: reference transcode virtual bytes differ")
		}
		off += n
	}
	return contextError(ctx)
}

func historyReferenceCopyCompanion(ctx context.Context, src, dst string, ref SegmentRef) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, in.Close()) }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, out.Close()) }()
	n, err := io.CopyBuffer(out, contextReader{ctx: ctx, r: in}, make([]byte, 64<<10))
	if err != nil {
		return err
	}
	if uint64(n) != ref.Size {
		return errors.New("snapshots: copied companion size differs")
	}
	return out.Sync()
}

// Link installs only a new immutable name. A resumed identical output is
// accepted only by full checksum/size; no existing object is overwritten.
func historyReferenceInstallFile(ctx context.Context, src, dst string, ref SegmentRef) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	size, checksum, err := stateDomainChangeBinaryFileMetadataContext(ctx, src)
	if err != nil {
		return err
	}
	if size != ref.Size || checksum != ref.Checksum {
		return errors.New("snapshots: transcode output identity differs")
	}
	if err = os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if err = os.Link(src, dst); err != nil {
		if !os.IsExist(err) {
			return err
		}
		info, e := os.Lstat(dst)
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return errors.New("snapshots: transcode destination is not a regular immutable file")
		}
		size, checksum, e = stateDomainChangeBinaryFileMetadataContext(ctx, dst)
		if e != nil {
			return e
		}
		if size != ref.Size || checksum != ref.Checksum {
			return errors.New("snapshots: conflicting transcode destination")
		}
	}
	return syncSnapshotDir(filepath.Dir(dst))
}

func historyReferenceTranscodePreflight(ctx context.Context, path string, size uint64) (err error) {
	if size < 8 || size > historyReferenceMaxPhysical {
		return fmt.Errorf("%w: source physical size", errHistoryReferenceBudget)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || uint64(info.Size()) != size {
		return errors.New("snapshots: transcode source file size/type differs")
	}
	var header [compressedBlockHeaderSize]byte
	if err = historyReferenceReadAt(ctx, f, header[:8], 0); err != nil {
		return err
	}
	if string(header[:8]) != compressedBlockMagic {
		return nil
	}
	if err = historyReferenceReadAt(ctx, f, header[:], 0); err != nil {
		return err
	}
	var count, tableOff, logical uint64
	switch binary.BigEndian.Uint32(header[8:12]) {
	case compressedBlockVersion:
		count, tableOff, logical = binary.BigEndian.Uint64(header[24:32]), compressedBlockHeaderSize, binary.BigEndian.Uint64(header[32:40])
	case compressedBlockFooterVersion:
		layout, e := readCompressedBlockFooterInfo(contextReaderAt{ctx: ctx, r: f}, size, header[:])
		if e != nil {
			return e
		}
		count, tableOff, logical = layout.blockCount, layout.tableOff, layout.uncSize
	case compressedBlockCDCVersion:
		r, e := openCDCReader(contextReaderAt{ctx: ctx, r: f}, size, header[:])
		if e != nil {
			return e
		}
		if r.count > historyReferenceMaxSpans || r.logical > historyReferenceMaxLogical {
			return fmt.Errorf("%w: source CDC spans=%d (limit %d), logical=%d (limit %d)", errHistoryReferenceBudget, r.count, historyReferenceMaxSpans, r.logical, historyReferenceMaxLogical)
		}
		return nil
	default:
		return errors.New("snapshots: unsupported compressed source version")
	}
	// V1 temporarily has both its encoded 28-byte and decoded 32-byte table.
	if count > historyReferenceMaxMetadata/64 || logical > historyReferenceMaxLogical || tableOff > size || count*compressedBlockTableEntry > size-tableOff {
		return fmt.Errorf("%w: source table entries=%d (limit %d), logical=%d (limit %d) or table bounds", errHistoryReferenceBudget, count, historyReferenceMaxMetadata/64, logical, historyReferenceMaxLogical)
	}
	var row [compressedBlockTableEntry]byte
	var previous uint64
	for i := uint64(0); i < count; i++ {
		if err = historyReferenceReadAt(ctx, f, row[:], tableOff+i*compressedBlockTableEntry); err != nil {
			return err
		}
		start := binary.BigEndian.Uint64(row[:8])
		stored := binary.BigEndian.Uint64(row[16:24])
		if start > logical || i > 0 && (start <= previous || start-previous > compressedBlockMaxDecodedBlockSize) || stored > compressedBlockMaxDecodedBlockSize+(8<<20) {
			return fmt.Errorf("%w: legacy page size", errHistoryReferenceBudget)
		}
		previous = start
	}
	if count > 0 && logical-previous > compressedBlockMaxDecodedBlockSize {
		return fmt.Errorf("%w: legacy final page size", errHistoryReferenceBudget)
	}
	return contextError(ctx)
}
