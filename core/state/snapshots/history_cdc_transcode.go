package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// HistoryCDCTranscodeWorkBytesContext bounds the additional disk needed for a
// streaming R1/V2 -> CDC rewrite. No logical stream or verification ETL is
// retained. Two logical sizes cover worst-case frames, both directory copies,
// and finalization; companions are copied once, with 1 GiB filesystem slack.
func HistoryCDCTranscodeWorkBytesContext(ctx context.Context, dir string, refs []SegmentRef) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	h, i, a, _, err := historyReferenceTranscodeIdentity(refs)
	if err != nil {
		return 0, err
	}
	current, err := historyCDCIsCurrent(dir, h)
	if err != nil {
		return 0, err
	}
	if current {
		return 0, errors.New("snapshots: CDC source already current")
	}
	if err = historyReferenceTranscodePreflight(ctx, filepath.Join(dir, h.Path), h.Size); err != nil {
		return 0, err
	}
	logical, err := readHistoryCompactionLogicalBytes(ctx, dir, h)
	if err != nil {
		return 0, err
	}
	// Existing format bounds also keep arithmetic below uint64 and CDC's
	// ordinal/sparse-directory limits, even at the minimum chunk size.
	maxEntries := logical/historychunk.MinSize + 2
	if logical > historyReferenceMaxLogical || maxEntries > uint64(cdcAnchor) || (maxEntries+cdcEntriesPerPage-1)/cdcEntriesPerPage > cdcMaxSparseBytes/8 || i.Size > historyReferenceMaxPhysical || a.Size > historyReferenceMaxPhysical {
		return 0, errors.New("snapshots: CDC migration exceeds container bounds")
	}
	return 2*logical + 2*(i.Size+a.Size) + (1 << 30), ctx.Err()
}

// A nil result means the measured size gain did not meet the requested floor.
// Only private scratch is removed in that case. Installed immutable outputs
// are never removed on errors; the migration journal owns their publication.
func reencodeHistoryCDC(ctx context.Context, dir string, refs []SegmentRef, minSavings uint64, allowGrowth bool) (result []SegmentRef, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if minSavings > 100 {
		return nil, errors.New("snapshots: invalid CDC savings")
	}
	if _, err = HistoryCDCTranscodeWorkBytesContext(ctx, dir, refs); err != nil {
		return nil, err
	}
	h, i, a, cfg, err := historyReferenceTranscodeIdentity(refs)
	if err != nil {
		return nil, err
	}
	if err = verifyStateDomainChangeBinaryCompanionChecksumsContext(ctx, dir, h, i, a); err != nil {
		return nil, err
	}
	r, logical, header, err := openHistorySegmentForReadWithCacheLimitContext(ctx, dir, h, 1)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	// R1 logical neighbors can reference distant physical chunks. Keep its
	// existing bounded cache to avoid repeatedly decoding the same values.
	if contextual, ok := r.(*stateDomainChangeHistoryReader); ok {
		if reference, ok := contextual.historySegmentReader.(*historyReferenceReader); ok {
			reference.cacheLimit = historyReferenceMaxCache
		}
	}
	if header.version != stateDomainChangeBinaryVersionV6 {
		return nil, errors.New("snapshots: CDC migration requires V6 logical history")
	}
	scratch, err := os.MkdirTemp(dir, ".history-cdc-work-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(scratch)
	w, err := newHistoryCompressedStreamFormat(ctx, scratch, historyCompressChunkSize, 1, "3")
	if err != nil {
		return nil, err
	}
	defer w.Abort()
	digest := sha256.New()
	buf := make([]byte, 1<<20)
	for off := uint64(0); off < logical; {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		part := buf[:min(uint64(len(buf)), logical-off)]
		if _, err = r.ReadAt(part, int64(off)); err != nil {
			return nil, err
		}
		digest.Write(part)
		if _, err = w.Write(part); err != nil {
			return nil, err
		}
		off += uint64(len(part))
	}
	output := filepath.Join(scratch, "output.seg")
	meta, err := w.FinishWithMetadataContext(ctx, output)
	if err != nil {
		return nil, err
	}
	// Source size is bounded above; multiplication cannot overflow uint64.
	if !allowGrowth && (meta.size >= h.Size || (h.Size-meta.size)*100 < h.Size*minSavings) {
		return nil, nil
	}
	encoded, err := openCompressedBlockReaderWithCacheLimit(output, cbCacheBlocks)
	if err != nil {
		return nil, err
	}
	if encoded.uncSize != logical {
		encoded.Close()
		return nil, errors.New("snapshots: CDC logical size changed")
	}
	check := sha256.New()
	for off := uint64(0); off < logical; {
		part := buf[:min(uint64(len(buf)), logical-off)]
		if err = historyReferenceReadAt(ctx, encoded, part, off); err != nil {
			break
		}
		check.Write(part)
		off += uint64(len(part))
	}
	if err == nil && !bytes.Equal(digest.Sum(nil), check.Sum(nil)) {
		err = errors.New("snapshots: CDC logical bytes changed")
	}
	err = errors.Join(err, encoded.Close())
	if err != nil {
		return nil, err
	}
	// The source is offline and immutable; detect an unexpected replacement
	// during the stream before publishing any output identity.
	if err = checkStateDomainChangeBinarySegmentChecksumContext(ctx, dir, h); err != nil {
		return nil, err
	}

	newH, newI, newA := h, i, a
	newH.Checksum = "sha256:" + hex.EncodeToString(meta.checksum[:])
	newH.Size = meta.size
	newH.Path = contentAddressedSnapshotPath(h.Path, newH.Checksum)
	if newH.Path == h.Path {
		return nil, errors.New("snapshots: CDC output aliases source")
	}
	newI.Path, newA.Path = cfg.HistoryIndexPathFor(newH.Path), cfg.HistoryAccessorPathFor(newH.Path)
	for _, pair := range [][2]SegmentRef{{i, newI}, {a, newA}} {
		tmp := filepath.Join(scratch, filepath.Base(pair[1].Path))
		if err = historyReferenceCopyCompanion(ctx, filepath.Join(dir, pair[0].Path), tmp, pair[0]); err != nil {
			return nil, err
		}
		if err = historyCDCInstallVerifiedFile(ctx, tmp, filepath.Join(dir, pair[1].Path), pair[1]); err != nil {
			return nil, err
		}
	}
	if err = historyCDCInstallVerifiedFile(ctx, output, filepath.Join(dir, newH.Path), newH); err != nil {
		return nil, err
	}
	// All file contents were synced before linking. Persist the installed
	// names once per parent before the caller may publish its journal.
	installedDirs := make(map[string]bool, 3)
	for _, ref := range []SegmentRef{newH, newI, newA} {
		parent := filepath.Dir(filepath.Join(dir, ref.Path))
		if !installedDirs[parent] {
			if err = syncSnapshotDir(parent); err != nil {
				return nil, err
			}
			installedDirs[parent] = true
		}
	}
	byKind := map[SegmentKind]SegmentRef{SegmentHistory: newH, SegmentInverted: newI, SegmentAccessor: newA}
	for _, ref := range refs {
		result = append(result, byKind[ref.Kind])
	}
	return result, ctx.Err()
}

// A fresh output already has the writer's complete physical digest and a
// decoded logical digest, or the companion copy's physical digest. Only a
// pre-existing immutable name needs rereading. The caller syncs parent dirs
// together after the complete trio has been installed, before journaling.
func historyCDCInstallVerifiedFile(ctx context.Context, src, dst string, ref SegmentRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	if err := os.Link(src, dst); err != nil {
		if !os.IsExist(err) {
			return err
		}
		if err = historyReferenceMigrationFile(filepath.Dir(dst), filepath.Base(dst), false); err != nil {
			return err
		}
		size, checksum, err := stateDomainChangeBinaryFileMetadataContext(ctx, dst)
		if err != nil {
			return err
		}
		if size != ref.Size || checksum != ref.Checksum {
			return errors.New("snapshots: conflicting CDC destination")
		}
	}
	return nil
}
