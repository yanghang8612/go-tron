package snapshots

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type HistoryRecordColumnStats struct {
	BeforeBytes, CandidateBytes, AfterBytes, LogicalBytes, Records uint64
	Converted                                                      bool
}

// ReencodeHistoryRecordColumnsTrioContext preserves every canonical V6 byte and
// immutable companion. It returns new refs only for a smaller, fully verified
// physical trio. No publication, routing mutation, source deletion or frontier
// advance occurs here; callers retain their existing authenticated protocol.
func ReencodeHistoryRecordColumnsTrioContext(ctx context.Context, dir string, refs []SegmentRef) (result []SegmentRef, stats HistoryRecordColumnStats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return
	}
	history, index, accessor, cfg, e := historyReferenceTranscodeIdentity(refs)
	if e != nil {
		return nil, stats, e
	}
	stats.BeforeBytes, stats.AfterBytes = history.Size, history.Size
	if err = historyReferenceTranscodePreflight(ctx, filepath.Join(dir, history.Path), history.Size); err != nil {
		return
	}
	if err = verifyStateDomainChangeBinaryCompanionChecksumsContext(ctx, dir, history, index, accessor); err != nil {
		return
	}
	r, h, size, e := openStateDomainChangeBinaryCompactionRecordReader(ctx, dir, history)
	if e != nil {
		return nil, stats, e
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	if h.version != stateDomainChangeBinaryVersionV6 {
		return nil, stats, fmt.Errorf("%w: V6 required", errHistoryRecordColumns)
	}
	stats.LogicalBytes, stats.Records = size, h.count
	_, off, e := readStateDomainChangeBinaryTxRangeTableAt(r, size, history, h)
	if e != nil {
		return nil, stats, e
	}
	w, e := newRecordColumnStreamWriter(dir, historyHeaderPrefixBytes)
	if e != nil {
		return nil, stats, e
	}
	defer w.Abort()
	buf := make([]byte, 64<<10)
	copyRange := func(start, n uint64) error {
		if start > size || n > size-start {
			return errHistoryRecordColumns
		}
		_, e := io.CopyBuffer(w, io.NewSectionReader(contextReaderAt{ctx: ctx, r: r}, int64(start), int64(n)), buf)
		return e
	}
	if err = copyRange(0, off); err != nil {
		return
	}
	var frame [21]byte
	for i := uint64(0); i < h.count; i++ {
		if err = ctx.Err(); err != nil {
			return
		}
		if off > size || size-off < 21 {
			return nil, stats, errHistoryRecordColumns
		}
		if err = historyReferenceReadAt(ctx, r, frame[:], off); err != nil {
			return
		}
		n := uint64(binary.BigEndian.Uint32(frame[17:21]))
		if uint64(binary.BigEndian.Uint32(frame[:4])) != 17+n || frame[16] > 1 || 21+n > size-off {
			return nil, stats, errHistoryRecordColumns
		}
		if err = w.NoteRecordHeader(off); err != nil {
			return
		}
		if err = copyRange(off, 21+n); err != nil {
			return
		}
		off += 21 + n
	}
	if off != size {
		return nil, stats, fmt.Errorf("%w: trailing V6 bytes", errHistoryRecordColumns)
	}
	file, e := os.CreateTemp(dir, ".record-columns-output-*.seg")
	if e != nil {
		return nil, stats, e
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err = file.Close(); err != nil {
		return
	}
	meta, e := w.FinishWithMetadataContext(ctx, tmp)
	if e != nil {
		return nil, stats, e
	}
	stats.CandidateBytes = meta.size
	target, e := openCompressedBlockReaderWithCacheLimit(tmp, 1)
	if e != nil {
		return nil, stats, e
	}
	err = historyReferenceCompareVirtual(ctx, r, target, size)
	err = errors.Join(err, target.Close())
	if err != nil {
		return
	}
	if meta.size >= history.Size {
		if err = verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, history, index, accessor); err != nil {
			return
		}
		if err = verifyStateDomainChangeBinaryCompanionChecksumsContext(ctx, dir, history, index, accessor); err != nil {
			return
		}
		return append([]SegmentRef(nil), refs...), stats, nil
	}
	nextHistory := history
	nextHistory.Checksum = "sha256:" + hex.EncodeToString(meta.checksum[:])
	nextHistory.Path = contentAddressedSnapshotPath(history.Path, nextHistory.Checksum)
	nextHistory.Size = meta.size
	nextIndex, nextAccessor := index, accessor
	nextIndex.Path = cfg.HistoryIndexPathFor(nextHistory.Path)
	nextAccessor.Path = cfg.HistoryAccessorPathFor(nextHistory.Path)
	for _, pair := range [][2]SegmentRef{{index, nextIndex}, {accessor, nextAccessor}} {
		f, e := os.CreateTemp(dir, ".record-columns-companion-*")
		if e != nil {
			return nil, stats, e
		}
		name := f.Name()
		e = f.Close()
		if e == nil {
			e = os.Remove(name)
		}
		if e == nil {
			e = historyReferenceCopyCompanion(ctx, filepath.Join(dir, pair[0].Path), name, pair[0])
		}
		if e == nil {
			e = historyReferenceInstallFile(ctx, name, filepath.Join(dir, pair[1].Path), pair[1])
		}
		os.Remove(name)
		if e != nil {
			return nil, stats, e
		}
	}
	if err = historyReferenceInstallFile(ctx, tmp, filepath.Join(dir, nextHistory.Path), nextHistory); err != nil {
		return
	}
	if err = verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, nextHistory, nextIndex, nextAccessor); err != nil {
		return
	}
	if err = verifyStateDomainChangeBinaryCompanionChecksumsContext(ctx, dir, history, index, accessor); err != nil {
		return
	}
	byKind := map[SegmentKind]SegmentRef{SegmentHistory: nextHistory, SegmentInverted: nextIndex, SegmentAccessor: nextAccessor}
	for _, ref := range refs {
		result = append(result, byKind[ref.Kind])
	}
	stats.Converted = true
	stats.AfterBytes = meta.size
	return
}
