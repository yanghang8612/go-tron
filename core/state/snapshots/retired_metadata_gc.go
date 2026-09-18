package snapshots

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const retiredMetadataSweepLimit = 1024

const (
	retiredMetadataDeferredSnapshotRoot = "snapshot-root"
	retiredMetadataDeferredMigration    = "migration-journal"
	retiredMetadataDeferredCatalog      = "snapshot-catalog"
	retiredMetadataDeferredPublished    = "published-manifest"
)

// retiredMetadataSweepCursor is deliberately process-local. A restart begins
// at the front again, which is safe because a missing retired ref is forgotten
// only by a successful ordinary manifest publication.
//
// Offset counts already inspected rows within Key's sortSegments equivalence
// group. sortSegments has no identity tie-break after Path, so using an exact
// SegmentRef as the cursor would be unsafe for a group containing multiple
// metadata identities for the same immutable path. All rows in one group have
// the same path and therefore the same fresh filesystem and active-lease
// classification; an offset remains meaningful even if equal rows reorder.
type retiredMetadataSweepCursor struct {
	Key    retiredMetadataSortKey
	Offset int
	Valid  bool
}

type retiredMetadataSortKey struct {
	Dataset SegmentDataset
	Domain  uint16
	Kind    SegmentKind
	From    uint64
	To      uint64
	Path    string
}

type retiredMetadataSweepResult struct {
	Manifest *Manifest
	Next     retiredMetadataSweepCursor
	Scanned  int
	Removed  int
	Deferred string
	root     os.FileInfo
}

type retiredMetadataSweepFS struct {
	lstat   func(string) (os.FileInfo, error)
	readDir func(string) ([]os.DirEntry, error)
}

var retiredMetadataOS = retiredMetadataSweepFS{
	lstat:   os.Lstat,
	readDir: os.ReadDir,
}

// prepareRetiredMetadataSweep removes no files and publishes nothing. It
// returns a detached manifest view only when bounded, fresh checks prove that
// one or more old retired refs name absent objects. The caller must advance to
// Next only after its normal manifest publication succeeds.
func prepareRetiredMetadataSweep(ctx context.Context, dir string, old *Manifest, cursor retiredMetadataSweepCursor) (retiredMetadataSweepResult, error) {
	return prepareRetiredMetadataSweepWithFS(ctx, dir, old, cursor, retiredMetadataSweepLimit, retiredMetadataOS)
}

func (r *Runner) integrateWithRetiredMetadataSweep(ctx context.Context, aggregator *Aggregator, visibleStart, visibleEnd uint64, refs []SegmentRef, old *Manifest) (*Manifest, retiredMetadataSweepResult, error) {
	return r.integrateWithRetiredMetadataSweepFS(ctx, aggregator, visibleStart, visibleEnd, refs, old, retiredMetadataOS)
}

func (r *Runner) integrateWithRetiredMetadataSweepFS(ctx context.Context, aggregator *Aggregator, visibleStart, visibleEnd uint64, refs []SegmentRef, old *Manifest, fs retiredMetadataSweepFS) (*Manifest, retiredMetadataSweepResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sweep, err := prepareRetiredMetadataSweepWithFS(ctx, r.cfg.Dir, old, r.retiredMetadataCursor, retiredMetadataSweepLimit, fs)
	if err != nil {
		return nil, sweep, err
	}
	if err := ctx.Err(); err != nil {
		return nil, sweep, err
	}
	// Recheck the cheap guards immediately before the existing publication.
	// Runtime cold work is serialized by Runner.passMu and offline reference
	// migration requires the node to be stopped; this is a fail-closed boundary,
	// not a cross-process compare-and-swap protocol.
	root, deferred := sweep.root, ""
	if sweep.Scanned > 0 {
		root, deferred = retiredMetadataSweepGuard(r.cfg.Dir, fs)
	}
	if sweep.Scanned > 0 && (deferred != "" || sweep.root == nil || root == nil || !os.SameFile(sweep.root, root)) {
		sweep.Manifest = old
		sweep.Next = r.retiredMetadataCursor
		sweep.Removed = 0
		if deferred == "" {
			deferred = retiredMetadataDeferredSnapshotRoot
		}
		sweep.Deferred = deferred
	}
	if err := ctx.Err(); err != nil {
		return nil, sweep, err
	}
	manifest, err := aggregator.integrateWithManifest(visibleStart, visibleEnd, refs, sweep.Manifest)
	if err != nil {
		return nil, sweep, err
	}
	r.retiredMetadataCursor = sweep.Next
	return manifest, sweep, nil
}

func prepareRetiredMetadataSweepWithFS(ctx context.Context, dir string, old *Manifest, cursor retiredMetadataSweepCursor, limit int, fs retiredMetadataSweepFS) (retiredMetadataSweepResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	result := retiredMetadataSweepResult{Manifest: old, Next: cursor}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if old == nil || len(old.Retired) == 0 || limit <= 0 || fs.lstat == nil || fs.readDir == nil {
		if old != nil && len(old.Retired) == 0 {
			result.Next = retiredMetadataSweepCursor{}
		}
		return result, nil
	}
	rootInfo, deferred := retiredMetadataSweepGuard(dir, fs)
	if deferred != "" {
		result.Deferred = deferred
		return result, nil
	}
	result.root = rootInfo

	chunks, next := retiredMetadataSweepChunks(old.Retired, cursor, limit)
	result.Next = next
	for _, chunk := range chunks {
		result.Scanned += chunk.end - chunk.start
	}
	if result.Scanned == 0 {
		return result, nil
	}

	missingByPath := make(map[string]bool, len(chunks))
	parentCache := make(map[string]os.FileInfo, len(chunks))
	requiredParents := make(map[string]os.FileInfo, len(chunks))
	for _, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return retiredMetadataSweepResult{Manifest: old, Next: cursor, Scanned: result.Scanned}, err
		}
		path := old.Retired[chunk.start].Path
		abs := filepath.Join(dir, filepath.FromSlash(path))
		info, err := fs.lstat(abs)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return retiredMetadataSweepResult{Manifest: old, Next: cursor, Scanned: result.Scanned}, ctxErr
		}
		if err == nil {
			// Regular files, symlinks (including dangling ones), directories and
			// every other existing object remain represented in retired metadata.
			_ = info
			continue
		}
		if !os.IsNotExist(err) {
			continue
		}
		parents, ok := retiredMetadataParentsPresent(ctx, dir, path, fs.lstat, parentCache)
		if err := ctx.Err(); err != nil {
			return retiredMetadataSweepResult{Manifest: old, Next: cursor, Scanned: result.Scanned}, err
		}
		if !ok {
			continue
		}
		for _, parent := range parents {
			requiredParents[parent] = parentCache[parent]
		}
		missingByPath[path] = true
	}
	if len(missingByPath) == 0 {
		return result, nil
	}
	// Retired and active path uniqueness is not a Manifest.Validate invariant.
	// Protect any bounded missing candidate that is still active, without
	// allocating an unbounded active-path map.
	for _, ref := range old.Segments {
		if err := ctx.Err(); err != nil {
			return retiredMetadataSweepResult{Manifest: old, Next: cursor, Scanned: result.Scanned}, err
		}
		if missingByPath[ref.Path] {
			delete(missingByPath, ref.Path)
			if len(missingByPath) == 0 {
				return result, nil
			}
		}
	}
	// Detect a root replacement/unmount between the leaf checks and forgetting
	// metadata. ENOENT caused by a disappearing snapshot tree is not proof that
	// each immutable object is gone.
	finalRoot, err := fs.lstat(dir)
	if err != nil || finalRoot == nil || !finalRoot.IsDir() || finalRoot.Mode()&os.ModeSymlink != 0 || !os.SameFile(rootInfo, finalRoot) {
		result.Next = cursor
		result.Deferred = retiredMetadataDeferredSnapshotRoot
		return result, nil
	}
	for parent, before := range requiredParents {
		if err := ctx.Err(); err != nil {
			return retiredMetadataSweepResult{Manifest: old, Next: cursor, Scanned: result.Scanned}, err
		}
		after, err := fs.lstat(parent)
		if err != nil || after == nil || !after.IsDir() || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, after) {
			result.Next = cursor
			result.Deferred = retiredMetadataDeferredSnapshotRoot
			return result, nil
		}
	}

	remove := make(map[int]struct{}, result.Scanned)
	for _, chunk := range chunks {
		if !missingByPath[old.Retired[chunk.start].Path] {
			continue
		}
		for i := chunk.start; i < chunk.end; i++ {
			remove[i] = struct{}{}
		}
	}
	if len(remove) == 0 {
		return result, nil
	}
	filtered := make([]SegmentRef, 0, len(old.Retired)-len(remove))
	for i, ref := range old.Retired {
		if i%retiredMetadataSweepLimit == 0 {
			if err := ctx.Err(); err != nil {
				return retiredMetadataSweepResult{Manifest: old, Next: cursor, Scanned: result.Scanned}, err
			}
		}
		if _, drop := remove[i]; !drop {
			filtered = append(filtered, ref)
		}
	}
	detached := *old
	detached.lookup = nil
	detached.Retired = filtered
	result.Manifest = &detached
	result.Removed = len(remove)

	last := chunks[len(chunks)-1]
	if last.end < last.groupEnd {
		// Removed rows shift the remaining same-path group to its beginning.
		// Recheck it next time rather than applying an offset to the new slice.
		result.Next = retiredMetadataSweepCursor{Key: last.key, Valid: true}
	}
	return result, nil
}

func retiredMetadataSweepGuard(dir string, fs retiredMetadataSweepFS) (os.FileInfo, string) {
	rootInfo, err := fs.lstat(dir)
	if err != nil || rootInfo == nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, retiredMetadataDeferredSnapshotRoot
	}
	// A pending journal binds exact before/after manifest bytes and old retired
	// refs. Its mere presence, including a dangling symlink, forbids metadata GC.
	if _, err := fs.lstat(filepath.Join(dir, historyReferenceMigrationJournalFile)); err == nil || !os.IsNotExist(err) {
		return rootInfo, retiredMetadataDeferredMigration
	}
	// A legacy catalog leases mutable manifest.json. Immutable catalogs also
	// have a published generation below, but treating any catalog presence as a
	// conservative defer closes incomplete-upgrade and catalog-check-error gaps.
	if _, err := fs.lstat(filepath.Join(dir, SnapshotCatalogFile)); err == nil || !os.IsNotExist(err) {
		return rootInfo, retiredMetadataDeferredCatalog
	}
	if publishedRetiredMetadataLeasePresent(dir, fs) {
		return rootInfo, retiredMetadataDeferredPublished
	}
	return rootInfo, ""
}

func publishedRetiredMetadataLeasePresent(dir string, fs retiredMetadataSweepFS) bool {
	root := filepath.Join(dir, SnapshotPublishedDir)
	info, err := fs.lstat(root)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil || info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return true
	}
	entries, err := fs.readDir(root)
	if err != nil {
		return true
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "manifest-") && strings.HasSuffix(name, ".json") {
			// Do not decode every retained generation on each cold pass. Any
			// possible immutable generation conservatively pauses metadata GC.
			return true
		}
	}
	return false
}

func retiredMetadataParentsPresent(ctx context.Context, root, rel string, lstat func(string) (os.FileInfo, error), cache map[string]os.FileInfo) ([]string, bool) {
	var checked []string
	parent := filepath.Dir(filepath.FromSlash(rel))
	for parent != "." && parent != string(filepath.Separator) {
		if ctx.Err() != nil {
			return nil, false
		}
		abs := filepath.Join(root, parent)
		if info, ok := cache[abs]; ok {
			if info == nil {
				return nil, false
			}
			checked = append(checked, abs)
			parent = filepath.Dir(parent)
			continue
		}
		info, err := lstat(abs)
		if err != nil || info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, false
		}
		cache[abs] = info
		checked = append(checked, abs)
		next := filepath.Dir(parent)
		if next == parent {
			return nil, false
		}
		parent = next
	}
	return checked, true
}

type retiredMetadataSweepChunk struct {
	key                              retiredMetadataSortKey
	start, end, groupStart, groupEnd int
}

func retiredMetadataSweepChunks(retired []SegmentRef, cursor retiredMetadataSweepCursor, limit int) ([]retiredMetadataSweepChunk, retiredMetadataSweepCursor) {
	if len(retired) == 0 || limit <= 0 {
		return nil, retiredMetadataSweepCursor{}
	}
	start := 0
	if cursor.Valid {
		start = sort.Search(len(retired), func(i int) bool {
			return compareRetiredMetadataKey(retiredMetadataKey(retired[i]), cursor.Key) >= 0
		})
		if start < len(retired) && compareRetiredMetadataKey(retiredMetadataKey(retired[start]), cursor.Key) == 0 {
			groupEnd := sort.Search(len(retired), func(i int) bool {
				return compareRetiredMetadataKey(retiredMetadataKey(retired[i]), cursor.Key) > 0
			})
			start += min(cursor.Offset, groupEnd-start)
			if start >= groupEnd {
				start = groupEnd
			}
		}
		if start >= len(retired) {
			start = 0
		}
	}
	remaining := min(limit, len(retired)-start)
	if remaining == 0 {
		return nil, cursor
	}
	chunks := make([]retiredMetadataSweepChunk, 0, min(remaining, 16))
	pos := start
	for remaining > 0 {
		key := retiredMetadataKey(retired[pos])
		groupStart := sort.Search(len(retired), func(i int) bool {
			return compareRetiredMetadataKey(retiredMetadataKey(retired[i]), key) >= 0
		})
		groupEnd := sort.Search(len(retired), func(i int) bool {
			return compareRetiredMetadataKey(retiredMetadataKey(retired[i]), key) > 0
		})
		end := min(groupEnd, pos+remaining)
		chunks = append(chunks, retiredMetadataSweepChunk{key: key, start: pos, end: end, groupStart: groupStart, groupEnd: groupEnd})
		remaining -= end - pos
		pos = end
		if pos >= len(retired) {
			break
		}
	}
	last := chunks[len(chunks)-1]
	next := retiredMetadataSweepCursor{Key: last.key, Offset: last.end - last.groupStart, Valid: true}
	return chunks, next
}

func retiredMetadataKey(ref SegmentRef) retiredMetadataSortKey {
	return retiredMetadataSortKey{
		Dataset: ref.normalizedDataset(),
		Domain:  uint16(ref.Domain),
		Kind:    ref.Kind,
		From:    ref.FromTxNum,
		To:      ref.ToTxNum,
		Path:    ref.Path,
	}
}

func compareRetiredMetadataKey(a, b retiredMetadataSortKey) int {
	if a.Dataset != b.Dataset {
		return strings.Compare(string(a.Dataset), string(b.Dataset))
	}
	if a.Domain != b.Domain {
		return int(a.Domain) - int(b.Domain)
	}
	if a.Kind != b.Kind {
		return strings.Compare(string(a.Kind), string(b.Kind))
	}
	if a.From < b.From {
		return -1
	}
	if a.From > b.From {
		return 1
	}
	if a.To < b.To {
		return -1
	}
	if a.To > b.To {
		return 1
	}
	return strings.Compare(a.Path, b.Path)
}
