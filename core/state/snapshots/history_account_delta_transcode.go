package snapshots

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"io"
	"os"
	"path/filepath"
)

// HistoryAccountDeltaStats counts complete physical files, including the page
// directory, checksums and mode markers. AfterBytes retains the source size
// when conversion is unhelpful. Companions are byte-identical and counted by
// the caller once in its final manifest closure.
type HistoryAccountDeltaStats struct {
	BeforeBytes, CandidateBytes, AfterBytes, LogicalBytes, AccountRecords uint64
	Converted                                                             bool
}

// ReencodeHistoryAccountDeltaTrioContext builds an independently verifiable
// immutable trio. It never publishes a manifest, removes a source or changes
// coverage. All canonical offsets and both companion byte streams survive.
// The caller must separately publish/rebind routing under its own protocol.
func ReencodeHistoryAccountDeltaTrioContext(ctx context.Context, dir string, refs []SegmentRef) (result []SegmentRef, stats HistoryAccountDeltaStats, err error) {
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
	stats.BeforeBytes = history.Size
	stats.AfterBytes = history.Size
	if err = historyReferenceTranscodePreflight(ctx, filepath.Join(dir, history.Path), history.Size); err != nil {
		return
	}
	if err = verifyStateDomainChangeBinaryCompanionChecksumsContext(ctx, dir, history, index, accessor); err != nil {
		return
	}
	af, e := os.Open(filepath.Join(dir, accessor.Path))
	if e != nil {
		return nil, stats, e
	}
	defer func() { err = errors.Join(err, af.Close()) }()
	ah, e := decodeStateDomainChangeBinaryAccessorKeyHeader(contextReaderAt{ctx: ctx, r: af}, accessor.Size)
	if e != nil {
		return nil, stats, e
	}
	// The existing accessor table is bounded independently of account payloads.
	if ah.keyCount > 1<<20 {
		return nil, stats, fmt.Errorf("%w: accessor key budget", errAccountDelta)
	}
	dirs, e := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(af, ah)
	if e != nil {
		return nil, stats, e
	}
	accounts := make([]bool, ah.keyCount)
	for i, b := range dirs {
		if err = ctx.Err(); err != nil {
			return
		}
		ks, e := stateDomainChangeBinaryAccessorV6ReadBlock(af, accessor.Size, ah, b, uint32(i*128))
		if e != nil {
			return nil, stats, e
		}
		for _, k := range ks {
			if len(k.key) == 0 || int(k.keyID) >= len(accounts) {
				return nil, stats, errAccountDelta
			}
			accounts[k.keyID] = k.key[0] == byte(rawdb.StateFlatDomainAccountLatest)
		}
	}
	r, h, size, e := openStateDomainChangeBinaryCompactionRecordReader(ctx, dir, history)
	if e != nil {
		return nil, stats, e
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	stats.LogicalBytes = size
	if h.version != stateDomainChangeBinaryVersionV6 {
		return nil, stats, fmt.Errorf("%w: V6 required", errAccountDelta)
	}
	_, off, e := readStateDomainChangeBinaryTxRangeTableAt(r, size, history, h)
	if e != nil {
		return nil, stats, e
	}
	w, e := newAccountDeltaStreamWriter(dir, accountDeltaPageMax, historyHeaderPrefixBytes, 4)
	if e != nil {
		return nil, stats, e
	}
	defer w.Abort()
	buf := make([]byte, 64<<10)
	copyRange := func(start, n uint64) error {
		if n > size-start {
			return errAccountDelta
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
		if size-off < 21 {
			return nil, stats, errAccountDelta
		}
		if err = historyReferenceReadAt(ctx, r, frame[:], off); err != nil {
			return
		}
		n := uint64(binary.BigEndian.Uint32(frame[17:21]))
		id := binary.BigEndian.Uint32(frame[4:8])
		if int(id) >= len(accounts) || uint64(binary.BigEndian.Uint32(frame[:4])) != 17+n || frame[16] > 1 || 21+n > size-off {
			return nil, stats, errAccountDelta
		}
		if accounts[id] && frame[16] == 1 {
			stats.AccountRecords++
			if err = w.NoteAccountValue(id, off+21, uint32(n)); err != nil {
				return
			}
		}
		if err = copyRange(off, 21+n); err != nil {
			return
		}
		off += 21 + n
	}
	if off != size {
		return nil, stats, fmt.Errorf("%w: trailing records", errAccountDelta)
	}
	scratch, e := os.CreateTemp(dir, ".account-delta-output-*.seg")
	if e != nil {
		return nil, stats, e
	}
	tmp := scratch.Name()
	if e = scratch.Close(); e != nil {
		return nil, stats, e
	}
	defer os.Remove(tmp)
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
		return append([]SegmentRef(nil), refs...), stats, nil
	}
	newHistory := history
	newHistory.Path = contentAddressedSnapshotPath(history.Path, "sha256:"+hex.EncodeToString(meta.checksum[:]))
	newHistory.Size = meta.size
	newHistory.Checksum = "sha256:" + hex.EncodeToString(meta.checksum[:])
	newIndex, newAccessor := index, accessor
	newIndex.Path = cfg.HistoryIndexPathFor(newHistory.Path)
	newAccessor.Path = cfg.HistoryAccessorPathFor(newHistory.Path)
	// Companions are immutable copies with complete source checksums. No inode
	// used by the baseline is opened writable or replaced.
	for _, pair := range [][2]SegmentRef{{index, newIndex}, {accessor, newAccessor}} {
		f, e := os.CreateTemp(dir, ".account-delta-companion-*")
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
	if err = historyReferenceInstallFile(ctx, tmp, filepath.Join(dir, newHistory.Path), newHistory); err != nil {
		return
	}
	if err = verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, newHistory, newIndex, newAccessor); err != nil {
		return
	}
	if err = verifyStateDomainChangeBinaryCompanionChecksumsContext(ctx, dir, history, index, accessor); err != nil {
		return
	}
	byKind := map[SegmentKind]SegmentRef{SegmentHistory: newHistory, SegmentInverted: newIndex, SegmentAccessor: newAccessor}
	for _, ref := range refs {
		result = append(result, byKind[ref.Kind])
	}
	stats.AfterBytes = meta.size
	stats.Converted = true
	return
}
