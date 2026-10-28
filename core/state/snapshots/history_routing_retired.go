package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const historyRoutingRetiredScanLimit = 256
const historyRoutingRetiredFileLimit = 16
const historyRoutingPublishedBudget = 128 << 20

type historyRoutingRetiredResult struct {
	Next         retiredMetadataSweepCursor
	Scanned      int
	FilesDeleted int
	BytesDeleted uint64
	Deferred     bool
}

// These objects contain no before-images or canonical state. Deleting an
// unpublished retired routing object never removes a fallback source. Keep
// canonical retirement's exhaustive authentication gate entirely separate.
func historyRoutingRetiredObject(ref SegmentRef) bool {
	if ref.normalizedDataset() != SegmentDatasetStateDomainChange {
		return false
	}
	digest, ok := snapshotChecksumDigest(ref.Checksum)
	if !ok || len(digest) != 64 || !strings.HasPrefix(ref.Checksum, "sha256:") {
		return false
	}
	prefix := ""
	switch ref.Kind {
	case SegmentMetadataHistoryRouting:
		prefix = "metadata-routing"
	case SegmentSmallSystemHistoryRouting:
		prefix = "small-system-routing"
	case SegmentSystemHistoryRouting:
		prefix = "system-routing"
	case SegmentAccountHistoryRouting:
		prefix = "account-routing"
	default:
		return false
	}
	return ref.Path == fmt.Sprintf("history/%s-%d-%d-%s.route", prefix, ref.FromTxNum, ref.ToTxNum, digest)
}

// Caller holds Runner.passMu and the heavy-work lease. A cursor bounds metadata
// work; at most sixteen files are unlinked. Missing rows remain in Retired for
// the existing bounded metadata sweep. No manifest/frontier/commitment changes.
func pruneRetiredHistoryRoutingContext(ctx context.Context, dir string, cursor retiredMetadataSweepCursor) (result historyRoutingRetiredResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	result.Next = cursor
	if err = contextError(ctx); err != nil {
		return result, err
	}
	release, acquired := tryHistoryRetirement()
	if !acquired {
		result.Deferred = true
		return result, nil
	}
	defer release()
	if !manifestPublicationMu.TryLock() {
		result.Deferred = true
		return result, nil
	}
	defer manifestPublicationMu.Unlock()
	root, e := os.Lstat(dir)
	parent, pe := os.Lstat(filepath.Join(dir, "history"))
	if e != nil || pe != nil || !root.IsDir() || !parent.IsDir() || root.Mode()&os.ModeSymlink != 0 || parent.Mode()&os.ModeSymlink != 0 {
		result.Deferred = true
		return result, nil
	}
	for _, name := range []string{historyReferenceMigrationJournalFile, historyCDCMigrationJournalFile} {
		if _, e := os.Lstat(filepath.Join(dir, name)); !os.IsNotExist(e) {
			result.Deferred = true
			return result, nil
		}
	}
	manifest, err := loadProductionManifestReadOnly(dir)
	if os.IsNotExist(err) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	chunks, next := retiredMetadataSweepChunks(manifest.Retired, cursor, historyRoutingRetiredScanLimit)
	candidates := make(map[string]SegmentRef)
	for _, chunk := range chunks {
		result.Scanned += chunk.end - chunk.start
		for _, ref := range manifest.Retired[chunk.start:chunk.end] {
			if historyRoutingRetiredObject(ref) {
				candidates[ref.Path] = ref
			}
		}
	}
	for _, ref := range manifest.Segments {
		delete(candidates, ref.Path)
	}
	// Even a malformed duplicate retired identity cannot authorize deletion of
	// a canonical recovery object that happens to use the same path.
	for _, ref := range manifest.Retired {
		if !historyRoutingRetiredObject(ref) {
			delete(candidates, ref.Path)
		}
	}
	if len(candidates) == 0 {
		result.Next = next
		return result, nil
	}
	if ok, e := protectPublishedRoutingObjects(ctx, dir, candidates); e != nil || !ok {
		result.Deferred = true
		return result, e
	}
	for path, ref := range candidates {
		if result.FilesDeleted >= historyRoutingRetiredFileLimit {
			break
		}
		if err := contextError(ctx); err != nil {
			return result, err
		}
		full := filepath.Join(dir, path)
		before, e := os.Lstat(full)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return result, e
		}
		if !before.Mode().IsRegular() || before.Size() < 8 || uint64(before.Size()) != ref.Size {
			continue
		}
		f, e := os.Open(full)
		if e != nil {
			return result, e
		}
		var magic [8]byte
		_, readErr := io.ReadFull(f, magic[:])
		inspected, statErr := f.Stat()
		closeErr := f.Close()
		want := systemHistoryRoutingMagic
		if ref.Kind == SegmentMetadataHistoryRouting {
			want = metadataRoutingMagic
		}
		if ref.Kind == SegmentSmallSystemHistoryRouting {
			want = smallSystemHistoryRoutingMagic
		}
		if ref.Kind == SegmentAccountHistoryRouting {
			want = accountRoutingMagic
		}
		if readErr != nil || statErr != nil || closeErr != nil || !os.SameFile(before, inspected) || !bytes.Equal(magic[:], want[:]) {
			continue
		}
		after, e := os.Lstat(full)
		currentRoot, re := os.Lstat(dir)
		currentParent, de := os.Lstat(filepath.Join(dir, "history"))
		if e != nil || re != nil || de != nil || !os.SameFile(before, after) || !os.SameFile(root, currentRoot) || !os.SameFile(parent, currentParent) {
			result.Deferred = true
			return result, nil
		}
		if err := contextError(ctx); err != nil {
			return result, err
		}
		if e := os.Remove(full); e != nil {
			return result, e
		}
		result.FilesDeleted++
		result.BytesDeleted += ref.Size
	}
	if result.FilesDeleted > 0 {
		if err := syncSnapshotDir(filepath.Join(dir, "history")); err != nil {
			return result, err
		}
	}
	result.Next = next
	return result, nil
}

// Decode one bounded immutable generation at a time. Validate its entire JSON
// and content-addressed name before trusting absence. A legacy mutable catalog
// or an oversized collection defers this optional cleanup without weakening
// the ordinary snapshot/retirement verifier.
func protectPublishedRoutingObjects(ctx context.Context, dir string, candidates map[string]SegmentRef) (bool, error) {
	catalogPath, catalogChecksum := "", ""
	if data, within, err := readRoutingRetiredMetadata(ctx, filepath.Join(dir, SnapshotCatalogFile), 1<<20); err == nil {
		if !within {
			return false, nil
		}
		var catalog SnapshotCatalog
		if err := json.Unmarshal(data, &catalog); err != nil {
			return false, err
		}
		if err := catalog.Validate(); err != nil {
			return false, err
		}
		if catalog.ManifestPath == ManifestFile {
			return false, nil
		}
		catalogPath, catalogChecksum = catalog.ManifestPath, catalog.ManifestChecksum
	} else if !os.IsNotExist(err) {
		return false, err
	}
	root := filepath.Join(dir, SnapshotPublishedDir)
	rootInfo, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return catalogPath == "", nil
	}
	if err != nil {
		return false, err
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return false, nil
	}
	directory, err := os.Open(root)
	if os.IsNotExist(err) {
		return catalogPath == "", nil
	}
	if err != nil {
		return false, err
	}
	openedInfo, statErr := directory.Stat()
	if statErr != nil || !os.SameFile(rootInfo, openedInfo) {
		directory.Close()
		return false, statErr
	}
	entries, err := directory.ReadDir(65)
	closeErr := directory.Close()
	if err != nil && err != io.EOF {
		return false, err
	}
	if closeErr != nil || len(entries) > 64 {
		return false, closeErr
	}
	remaining := int64(historyRoutingPublishedBudget)
	catalogFound := catalogPath == ""
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "manifest-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if err := contextError(ctx); err != nil {
			return false, err
		}
		info, err := entry.Info()
		if err != nil {
			return false, err
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > remaining {
			return false, nil
		}
		remaining -= info.Size()
		data, within, err := readRoutingRetiredMetadata(ctx, filepath.Join(root, entry.Name()), info.Size())
		if err != nil {
			return false, err
		}
		if !within || int64(len(data)) != info.Size() {
			return false, nil
		}
		m, err := decodeProductionManifest(data)
		if err != nil {
			return false, err
		}
		want, err := publishedSnapshotManifestPath(m.Generation, checksumBytes(data))
		if err != nil || want != filepath.ToSlash(filepath.Join(SnapshotPublishedDir, entry.Name())) {
			return false, fmt.Errorf("snapshots: routing retirement encountered an invalid published generation")
		}
		if want == catalogPath {
			if checksumBytes(data) != catalogChecksum {
				return false, fmt.Errorf("snapshots: routing retirement catalog checksum mismatch")
			}
			catalogFound = true
		}
		for _, ref := range m.Segments {
			delete(candidates, ref.Path)
		}
	}
	return catalogFound, nil
}

func readRoutingRetiredMetadata(ctx context.Context, path string, limit int64) ([]byte, bool, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, false, err
	}
	if !before.Mode().IsRegular() {
		return nil, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, false, err
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, r: f}, limit+1))
	return data, int64(len(data)) <= limit, err
}
