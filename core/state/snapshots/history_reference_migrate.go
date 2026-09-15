package snapshots

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

const historyReferenceMigrationJournalFile = ".history-reference-migration.json"
const historyReferenceMigrationLockFile = ".history-reference-migration.lock"

// HistoryReferenceMigrationOptions controls offline, in-place conversion. Zero
// MaxTrios means all remaining trios; completing a pending journal consumes one
// of the limit even if its manifest publication preceded this invocation.
type HistoryReferenceMigrationOptions struct {
	MaxTrios   uint64
	DryRun     bool
	BeforeTrio func(refs []SegmentRef) error
	OnProgress func(HistoryReferenceMigrationProgress)
}

type HistoryReferenceMigrationProgress struct {
	OldRefs            []SegmentRef `json:"oldRefs"`
	NewRefs            []SegmentRef `json:"newRefs"`
	CompletedTrios     uint64       `json:"completedTrios"`
	MigratedTrios      uint64       `json:"migratedTrios"`
	DeletedFiles       uint64       `json:"deletedFiles"`
	DeletedBytes       uint64       `json:"deletedBytes"`
	ManifestGeneration uint64       `json:"manifestGeneration"`
}

type HistoryReferenceMigrationResult struct {
	SnapshotDir        string  `json:"snapshotDir"`
	DryRun             bool    `json:"dryRun"`
	TotalTrios         uint64  `json:"totalTrios"`
	AlreadyCurrent     uint64  `json:"alreadyCurrentTrios"`
	MigratedTrios      uint64  `json:"migratedTrios"`
	CompletedTrios     uint64  `json:"completedTrios"`
	RemainingTrios     uint64  `json:"remainingTrios"`
	ResumedJournal     bool    `json:"resumedJournal"`
	JournalPending     bool    `json:"journalPending"`
	ActiveBytesBefore  uint64  `json:"activeBytesBefore"`
	ActiveBytesAfter   uint64  `json:"activeBytesAfter"`
	DeletedSourceFiles uint64  `json:"deletedSourceFiles"`
	DeletedSourceBytes uint64  `json:"deletedSourceBytes"`
	ElapsedSeconds     float64 `json:"elapsedSeconds"`
}

// The journal is deliberately bounded to six refs and two complete manifest
// byte identities, not a second copy of the (potentially large) manifest. It is
// written only after transcode has proved full virtual-byte equivalence.
type historyReferenceMigrationJournal struct {
	Version            uint32       `json:"version"`
	Old                []SegmentRef `json:"old"`
	New                []SegmentRef `json:"new"`
	BeforeManifestSHA  string       `json:"beforeManifestSHA"`
	AfterManifestSHA   string       `json:"afterManifestSHA"`
	AfterPublishedUnix int64        `json:"afterPublishedUnix"`
}

type historyReferenceMigration struct {
	ctx      context.Context
	dir      string
	opts     HistoryReferenceMigrationOptions
	manifest *Manifest
	checksum string
	result   *HistoryReferenceMigrationResult
}

// MigrateHistoryReferenceContext owns offline publication in dir. The caller
// MUST stop the node and hold its database's exclusive lock throughout: this
// migration's flock excludes other migrations, not arbitrary node publishers.
// Manifest byte identities are checked at every commit/cleanup boundary; they
// are conflict detection, not a compare-and-swap against a running node.
//
// Only one trio's new files and scratch coexist with its source. Every new trio
// retains exact V6 bytes and companion offsets. Chain identity, visible range,
// and all progress fields are preserved. No leases are expired and no global
// retired-file scan or whole-manifest file verification is performed per trio.
// DryRun never creates a lock, journal, output, or manifest. BeforeTrio must
// perform read-only work/space admission; DryRun invokes it for selected trios.
// It is not called for a journal whose new manifest is already active and only
// needs cleanup.
func MigrateHistoryReferenceContext(ctx context.Context, dir string, opts HistoryReferenceMigrationOptions) (result *HistoryReferenceMigrationResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dir == "" {
		return nil, errors.New("snapshots: empty reference migration directory")
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err = historyReferenceMigrationDirectory(dir); err != nil {
		return nil, err
	}
	if err = historyReferenceMigrationFile(dir, ManifestFile, false); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	manifest, err := decodeProductionManifest(data)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	result = &HistoryReferenceMigrationResult{SnapshotDir: dir, DryRun: opts.DryRun}
	defer func() { result.ElapsedSeconds = time.Since(started).Seconds() }()
	m := &historyReferenceMigration{ctx: ctx, dir: dir, opts: opts, manifest: manifest, checksum: checksumBytes(data), result: result}
	data = nil
	journal, err := historyReferenceMigrationLoadJournal(dir)
	if err != nil {
		return result, err
	}
	result.JournalPending = journal != nil
	if journal != nil {
		if err = m.journalState(journal); err != nil {
			return result, err
		}
	}
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		return result, errors.New("snapshots: missing history dataset")
	}
	candidates := historyCompactionCandidates(manifest, cfg)
	if len(candidates) != activeBinaryHistoryCount(manifest, cfg) {
		return result, errors.New("snapshots: incomplete active history trios")
	}
	var pending [][]SegmentRef
	for _, candidate := range candidates {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		refs := append([]SegmentRef{candidate.history}, candidate.companions...)
		if err = historyReferenceMigrationValidateTrio(refs); err != nil {
			return result, err
		}
		n, e := historyReferenceMigrationBytes(refs)
		if e != nil || math.MaxUint64-result.ActiveBytesBefore < n {
			return result, errors.New("snapshots: history migration byte count overflow")
		}
		result.ActiveBytesBefore += n
		result.TotalTrios++
		current, e := historyReferenceMigrationIsCurrent(dir, candidate.history)
		if e != nil {
			return result, e
		}
		if current {
			result.AlreadyCurrent++ // Header classification, not full content verification.
		} else {
			result.RemainingTrios++
			pending = append(pending, refs)
		}
	}
	result.ActiveBytesAfter = result.ActiveBytesBefore
	// Refuse a pinned source before even creating the private lock file. Recheck
	// under the lock and immediately before publication and unlink as well.
	var selection []SegmentRef
	var selectedTrios [][]SegmentRef
	if journal != nil {
		selection = append(selection, journal.Old...)
		if m.checksum == journal.BeforeManifestSHA {
			selectedTrios = append(selectedTrios, journal.Old)
		}
	}
	budget := opts.MaxTrios
	selected := uint64(0)
	if journal != nil {
		selected++
	}
	for _, refs := range pending {
		if journal != nil && historyReferenceMigrationSameTrio(refs, journal.Old) {
			continue
		}
		if budget != 0 && selected >= budget {
			break
		}
		selection = append(selection, refs...)
		selectedTrios = append(selectedTrios, refs)
		selected++
	}
	if err = historyReferenceMigrationNoLeases(dir, selection); err != nil {
		return result, err
	}
	if opts.DryRun {
		for _, refs := range selectedTrios {
			if err = m.beforeTrio(refs); err != nil {
				return result, err
			}
		}
		return result, nil
	}
	if len(selection) == 0 {
		return result, nil
	}
	if err = historyReferenceMigrationFile(dir, historyReferenceMigrationLockFile, true); err != nil {
		return result, err
	}
	lock := flock.New(filepath.Join(dir, historyReferenceMigrationLockFile))
	locked, err := lock.TryLock()
	if err != nil {
		return result, err
	}
	if !locked {
		return result, errors.New("snapshots: another reference migration holds the lock")
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err = m.checkManifest(); err != nil {
		return result, err
	}
	// A previous invocation may have created/finished a journal between the
	// read-only admission and acquisition of the lock, even without publication.
	lockedJournal, e := historyReferenceMigrationLoadJournal(dir)
	if e != nil || !reflect.DeepEqual(lockedJournal, journal) {
		return result, errors.Join(e, errors.New("snapshots: migration journal changed during admission"))
	}
	if journal != nil {
		result.ResumedJournal = true
		if err = m.finish(journal, true); err != nil {
			return result, err
		}
	}
	for _, refs := range pending {
		if journal != nil && historyReferenceMigrationSameTrio(refs, journal.Old) {
			continue
		}
		if opts.MaxTrios != 0 && result.CompletedTrios >= opts.MaxTrios {
			break
		}
		if err = m.beforeTrio(refs); err != nil {
			return result, err
		}
		newRefs, _, e := ReencodeHistoryReferenceTrioContext(ctx, dir, refs)
		if e != nil {
			return result, e
		}
		j, _, e := historyReferenceMigrationPrepare(m.manifest, m.checksum, refs, newRefs, time.Now().Unix())
		if e != nil {
			return result, e
		}
		if err = m.checkManifest(); err != nil {
			return result, err // Outputs retained on every uncertain path.
		}
		if err = historyReferenceMigrationNoLeases(dir, refs); err != nil {
			return result, err
		}
		if err = historyReferenceMigrationWriteJournal(dir, j); err != nil {
			// Rename may have succeeded before a directory-sync error.
			result.JournalPending = true
			return result, err
		}
		result.JournalPending = true
		if err = m.finish(j, false); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (m *historyReferenceMigration) beforeTrio(refs []SegmentRef) error {
	if err := m.checkManifest(); err != nil {
		return err
	}
	if err := historyReferenceMigrationNoLeases(m.dir, refs); err != nil {
		return err
	}
	for _, ref := range refs {
		if err := historyReferenceMigrationFile(m.dir, ref.Path, false); err != nil {
			return err
		}
	}
	if m.opts.BeforeTrio != nil {
		if err := m.opts.BeforeTrio(append([]SegmentRef(nil), refs...)); err != nil {
			return err
		}
	}
	if err := m.checkManifest(); err != nil {
		return err
	}
	return historyReferenceMigrationNoLeases(m.dir, refs)
}

func (m *historyReferenceMigration) finish(j *historyReferenceMigrationJournal, resumed bool) error {
	if err := m.journalState(j); err != nil {
		return err
	}
	if m.checksum == j.BeforeManifestSHA {
		if resumed {
			if err := m.beforeTrio(j.Old); err != nil {
				return err
			}
			// A pre-publication journal never substitutes for a new proof. Reuse
			// immutable output names, but reprove every source/output byte.
			refs, _, err := ReencodeHistoryReferenceTrioContext(m.ctx, m.dir, j.Old)
			if err != nil {
				return err
			}
			if !historyReferenceMigrationSameTrio(refs, j.New) {
				return errors.New("snapshots: resumed transcode output identity differs")
			}
		}
		_, next, err := historyReferenceMigrationPrepare(m.manifest, m.checksum, j.Old, j.New, j.AfterPublishedUnix)
		if err != nil {
			return err
		}
		if err = m.checkManifest(); err != nil {
			return err
		}
		if err = historyReferenceMigrationNoLeases(m.dir, j.Old); err != nil {
			return err
		}
		if err = PublishManifest(m.dir, next); err != nil {
			return fmt.Errorf("snapshots: reference migration publication uncertain; retain journal and both trios: %w", err)
		}
		m.manifest, m.checksum = next, j.AfterManifestSHA
		if err = m.checkManifest(); err != nil {
			return err
		}
		oldBytes, _ := historyReferenceMigrationBytes(j.Old)
		newBytes, _ := historyReferenceMigrationBytes(j.New)
		if oldBytes > m.result.ActiveBytesAfter || math.MaxUint64-(m.result.ActiveBytesAfter-oldBytes) < newBytes {
			return errors.New("snapshots: migrated active byte count overflow")
		}
		m.result.ActiveBytesAfter = m.result.ActiveBytesAfter - oldBytes + newBytes
		m.result.MigratedTrios++
		m.result.RemainingTrios--
	}
	if err := m.journalState(j); err != nil {
		return err
	}
	// This also covers resume after any subset of old files has been removed.
	// Only the new trio is needed for verification once its manifest is active.
	if err := historyReferenceMigrationVerifyNew(m.ctx, m.dir, j.New); err != nil {
		return err
	}
	if err := m.checkManifest(); err != nil {
		return err
	}
	if err := historyReferenceMigrationNoLeases(m.dir, j.Old); err != nil {
		return err
	}
	keep := make(map[string]bool, len(m.manifest.Segments))
	for _, ref := range m.manifest.Segments {
		keep[ref.Path] = true
	}
	deletedDirs := make(map[string]bool, 3)
	for _, ref := range j.Old {
		if keep[ref.Path] { // A reused companion remains active.
			continue
		}
		if err := m.ctx.Err(); err != nil {
			return err
		}
		if err := historyReferenceMigrationFile(m.dir, ref.Path, true); err != nil {
			return err
		}
		path := filepath.Join(m.dir, ref.Path)
		deletedDirs[filepath.Dir(path)] = true
		if _, err := os.Lstat(path); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		// An unexpected file at an old path is not an authorized source unlink.
		size, sha, err := stateDomainChangeBinaryFileMetadataContext(m.ctx, path)
		if err != nil {
			return err
		}
		if size != ref.Size || sha != ref.Checksum {
			return fmt.Errorf("snapshots: retired source identity changed: %s", ref.Path)
		}
		if err = os.Remove(path); err != nil {
			return err
		}
		m.result.DeletedSourceFiles++
		m.result.DeletedSourceBytes += size
		if err = syncSnapshotDir(filepath.Dir(path)); err != nil {
			return err
		}
	}
	// Include already-missing files on resume: an earlier unlink may have
	// completed just before its directory fsync failed.
	for dir := range deletedDirs {
		if err := syncSnapshotDir(dir); err != nil {
			return err
		}
	}
	if err := m.checkManifest(); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(m.dir, historyReferenceMigrationJournalFile)); err != nil {
		return err
	}
	if err := syncSnapshotDir(m.dir); err != nil {
		return err
	}
	m.result.JournalPending = false
	m.result.CompletedTrios++
	if m.opts.OnProgress != nil {
		m.opts.OnProgress(HistoryReferenceMigrationProgress{OldRefs: append([]SegmentRef(nil), j.Old...), NewRefs: append([]SegmentRef(nil), j.New...),
			CompletedTrios: m.result.CompletedTrios, MigratedTrios: m.result.MigratedTrios, DeletedFiles: m.result.DeletedSourceFiles,
			DeletedBytes: m.result.DeletedSourceBytes, ManifestGeneration: m.manifest.Generation})
	}
	return nil
}

func (m *historyReferenceMigration) checkManifest() error {
	if err := m.ctx.Err(); err != nil {
		return err
	}
	if err := historyReferenceMigrationFile(m.dir, ManifestFile, false); err != nil {
		return err
	}
	_, sha, err := stateDomainChangeBinaryFileMetadataContext(m.ctx, filepath.Join(m.dir, ManifestFile))
	if err != nil {
		return err
	}
	if sha != m.checksum {
		return errors.New("snapshots: concurrent manifest mutation during reference migration")
	}
	return nil
}

func (m *historyReferenceMigration) journalState(j *historyReferenceMigrationJournal) error {
	if err := historyReferenceMigrationValidateJournal(j); err != nil {
		return err
	}
	switch m.checksum {
	case j.BeforeManifestSHA:
		prepared, _, err := historyReferenceMigrationPrepare(m.manifest, m.checksum, j.Old, j.New, j.AfterPublishedUnix)
		if err != nil {
			return err
		}
		if prepared.AfterManifestSHA != j.AfterManifestSHA {
			return errors.New("snapshots: journal target manifest identity differs")
		}
	case j.AfterManifestSHA:
		active := make(map[string]SegmentRef, len(m.manifest.Segments))
		retired := make(map[string]SegmentRef, len(m.manifest.Retired))
		for _, ref := range m.manifest.Segments {
			active[ref.Path] = ref
		}
		for _, ref := range m.manifest.Retired {
			retired[ref.Path] = ref
		}
		newPaths := make(map[string]bool, 3)
		for _, ref := range j.New {
			if active[ref.Path] != ref {
				return errors.New("snapshots: journal new trio is not exactly active")
			}
			newPaths[ref.Path] = true
		}
		for _, ref := range j.Old {
			if newPaths[ref.Path] {
				continue
			}
			if _, ok := active[ref.Path]; ok || retired[ref.Path] != ref {
				return errors.New("snapshots: journal source is active or not exactly retired")
			}
		}
	default:
		return errors.New("snapshots: pending journal does not match the complete active manifest")
	}
	return nil
}

func historyReferenceMigrationPrepare(m *Manifest, checksum string, old, newRefs []SegmentRef, published int64) (*historyReferenceMigrationJournal, *Manifest, error) {
	j := &historyReferenceMigrationJournal{Version: 1, Old: append([]SegmentRef(nil), old...), New: append([]SegmentRef(nil), newRefs...), BeforeManifestSHA: checksum, AfterManifestSHA: "sha256:" + strings.Repeat("0", 64), AfterPublishedUnix: published}
	if err := historyReferenceMigrationValidateJournal(j); err != nil {
		return nil, nil, err
	}
	if m.Generation == math.MaxUint64 {
		return nil, nil, errors.New("snapshots: manifest generation overflow")
	}
	next := cloneManifest(m)
	next.Generation++
	next.PublishedUnix = published
	oldByPath := make(map[string]SegmentRef, 3)
	newByPath := make(map[string]SegmentRef, 3)
	for _, ref := range old {
		oldByPath[ref.Path] = ref
	}
	for _, ref := range newRefs {
		newByPath[ref.Path] = ref
	}
	next.Segments = nil
	matched := 0
	for _, ref := range m.Segments {
		if expected, ok := oldByPath[ref.Path]; ok {
			if expected != ref {
				return nil, nil, errors.New("snapshots: migration source active identity differs")
			}
			matched++
			continue
		}
		if _, collision := newByPath[ref.Path]; collision {
			return nil, nil, errors.New("snapshots: migration output collides with unrelated active ref")
		}
		next.Segments = append(next.Segments, ref)
	}
	if matched != 3 {
		return nil, nil, errors.New("snapshots: migration requires exactly the complete active source trio")
	}
	next.Segments = append(next.Segments, newRefs...)
	for _, ref := range old {
		if _, reused := newByPath[ref.Path]; reused {
			continue
		}
		found := false
		for _, retired := range next.Retired {
			if retired.Path == ref.Path {
				if retired != ref {
					return nil, nil, errors.New("snapshots: migration retired identity differs")
				}
				found = true
			}
		}
		if !found {
			next.Retired = append(next.Retired, ref)
		}
	}
	sortSegments(next.Segments)
	sortSegments(next.Retired)
	normalizeChainIdentity(next.Chain)
	data, err := json.Marshal(next)
	if err != nil {
		return nil, nil, err
	}
	j.AfterManifestSHA = checksumBytes(data)
	return j, next, nil
}

func historyReferenceMigrationValidateJournal(j *historyReferenceMigrationJournal) error {
	if j == nil || j.Version != 1 || j.AfterPublishedUnix <= 0 || !historyReferenceMigrationSHA(j.BeforeManifestSHA) || !historyReferenceMigrationSHA(j.AfterManifestSHA) || j.BeforeManifestSHA == j.AfterManifestSHA {
		return errors.New("snapshots: invalid reference migration journal identity")
	}
	if err := historyReferenceMigrationValidateTrio(j.Old); err != nil {
		return err
	}
	if err := historyReferenceMigrationValidateTrio(j.New); err != nil {
		return err
	}
	old := make(map[SegmentKind]SegmentRef, 3)
	for _, ref := range j.Old {
		old[ref.Kind] = ref
	}
	for _, ref := range j.New {
		prior := old[ref.Kind]
		if prior.FromTxNum != ref.FromTxNum || prior.ToTxNum != ref.ToTxNum || prior.AggregationSteps != ref.AggregationSteps || prior.Domain != ref.Domain ||
			(ref.Kind != SegmentHistory && (prior.Size != ref.Size || prior.Checksum != ref.Checksum)) ||
			(ref.Kind == SegmentHistory && prior.Path == ref.Path) {
			return errors.New("snapshots: migration changes trio coverage or companion content")
		}
	}
	return nil
}

func historyReferenceMigrationValidateTrio(refs []SegmentRef) error {
	if len(refs) != 3 {
		return errors.New("snapshots: reference migration requires exactly three refs")
	}
	byKind := make(map[SegmentKind]SegmentRef, 3)
	for _, ref := range refs {
		if ref.Dataset != SegmentDatasetStateDomainChange || ref.Size == 0 || !historyReferenceMigrationSHA(ref.Checksum) || len(ref.Path) > 4096 {
			return errors.New("snapshots: incomplete migration ref identity")
		}
		if err := validateSegmentRef(ref); err != nil {
			return err
		}
		if _, ok := byKind[ref.Kind]; ok {
			return errors.New("snapshots: duplicate migration ref kind")
		}
		byKind[ref.Kind] = ref
	}
	h, hOK := byKind[SegmentHistory]
	i, iOK := byKind[SegmentInverted]
	a, aOK := byKind[SegmentAccessor]
	cfg, ok := DefaultDomainRegistry().ConfigForRef(h)
	if !hOK || !iOK || !aOK || !ok || !cfg.IsHistoryBinarySegmentPath(h.Path) || i.Path != cfg.HistoryIndexPathFor(h.Path) || a.Path != cfg.HistoryAccessorPathFor(h.Path) ||
		i.FromTxNum != h.FromTxNum || a.FromTxNum != h.FromTxNum || i.ToTxNum != h.ToTxNum || a.ToTxNum != h.ToTxNum ||
		i.AggregationSteps != h.AggregationSteps || a.AggregationSteps != h.AggregationSteps {
		return errors.New("snapshots: migration companion coverage differs")
	}
	_, err := historyReferenceMigrationBytes(refs)
	return err
}

func historyReferenceMigrationSHA(s string) bool {
	if len(s) != 71 || !strings.HasPrefix(s, "sha256:") || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s[len("sha256:"):])
	return err == nil
}

func historyReferenceMigrationSameTrio(a, b []SegmentRef) bool {
	if len(a) != 3 || len(b) != 3 {
		return false
	}
	for _, ref := range a {
		found := false
		for _, other := range b {
			found = found || ref == other
		}
		if !found {
			return false
		}
	}
	return true
}

func historyReferenceMigrationBytes(refs []SegmentRef) (uint64, error) {
	var n uint64
	for _, ref := range refs {
		if math.MaxUint64-n < ref.Size {
			return 0, errors.New("snapshots: migration trio byte overflow")
		}
		n += ref.Size
	}
	return n, nil
}

func historyReferenceMigrationNoLeases(dir string, refs []SegmentRef) error {
	wanted := make(map[string]bool, len(refs))
	for _, ref := range refs {
		wanted[ref.Path] = true
	}
	leases, err := LoadPublishedSnapshotManifests(dir)
	if err != nil {
		return err
	}
	if catalog, err := LoadSnapshotCatalog(dir); err == nil {
		found := false
		for _, lease := range leases {
			if lease.Path == catalog.ManifestPath {
				found = true
			}
		}
		if !found {
			return errors.New("snapshots: current snapshot catalog has no complete published lease")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	for _, lease := range leases {
		for _, ref := range lease.Manifest.Segments {
			if wanted[ref.Path] {
				return fmt.Errorf("snapshots: migration source %s is pinned by published lease %s", ref.Path, lease.Path)
			}
		}
	}
	return nil
}

func historyReferenceMigrationDirectory(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("snapshots: migration requires a real directory: %s", dir)
	}
	return nil
}

// Do not follow a segment/journal symlink, including any component below dir.
// The caller supplies and exclusively owns the directory; no hostile concurrent
// filesystem writer is supported by this offline protocol.
func historyReferenceMigrationFile(dir, rel string, missingOK bool) error {
	if rel == "" || filepath.IsAbs(rel) || filepath.Clean(rel) != rel || rel == "." || hasParentDir(rel) {
		return errors.New("snapshots: invalid migration relative path")
	}
	parts := strings.Split(rel, string(filepath.Separator))
	path := dir
	for i, part := range parts {
		path = filepath.Join(path, part)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) && missingOK && i == len(parts)-1 {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (i < len(parts)-1 && !info.IsDir()) || (i == len(parts)-1 && !info.Mode().IsRegular()) {
			return fmt.Errorf("snapshots: migration path is not a regular file under real directories: %s", rel)
		}
	}
	return nil
}

func historyReferenceMigrationIsCurrent(dir string, history SegmentRef) (current bool, err error) {
	if err = historyReferenceMigrationFile(dir, history.Path, false); err != nil {
		return false, err
	}
	f, err := os.Open(filepath.Join(dir, history.Path))
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	var magic [8]byte
	if _, err = io.ReadFull(f, magic[:]); err != nil {
		return false, err
	}
	return string(magic[:]) == historyReferenceMagic, nil
}

func historyReferenceMigrationVerifyNew(ctx context.Context, dir string, refs []SegmentRef) error {
	if err := historyReferenceMigrationValidateTrio(refs); err != nil {
		return err
	}
	byKind := make(map[SegmentKind]SegmentRef, 3)
	for _, ref := range refs {
		if err := historyReferenceMigrationFile(dir, ref.Path, false); err != nil {
			return err
		}
		byKind[ref.Kind] = ref
	}
	current, err := historyReferenceMigrationIsCurrent(dir, byKind[SegmentHistory])
	if err != nil {
		return err
	}
	if !current {
		return errors.New("snapshots: journal output is not a reference container")
	}
	return verifyStateDomainChangeBinaryCompanionsAgainstSegmentContext(ctx, dir, byKind[SegmentHistory], byKind[SegmentInverted], byKind[SegmentAccessor])
}

func historyReferenceMigrationLoadJournal(dir string) (*historyReferenceMigrationJournal, error) {
	if err := historyReferenceMigrationFile(dir, historyReferenceMigrationJournalFile, true); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, historyReferenceMigrationJournalFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	err = errors.Join(err, f.Close())
	if err != nil {
		return nil, err
	}
	if len(data) > 64*1024 {
		return nil, errors.New("snapshots: oversized migration journal")
	}
	var j historyReferenceMigrationJournal
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&j); err != nil {
		return nil, err
	}
	if err = dec.Decode(new(any)); err != io.EOF {
		return nil, errors.New("snapshots: trailing migration journal data")
	}
	if err = historyReferenceMigrationValidateJournal(&j); err != nil {
		return nil, err
	}
	return &j, nil
}

func historyReferenceMigrationWriteJournal(dir string, j *historyReferenceMigrationJournal) (err error) {
	if err = historyReferenceMigrationValidateJournal(j); err != nil {
		return err
	}
	if _, e := os.Lstat(filepath.Join(dir, historyReferenceMigrationJournalFile)); !os.IsNotExist(e) {
		return errors.New("snapshots: refusing to replace an existing migration journal")
	}
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if len(data) > 64*1024 {
		return errors.New("snapshots: oversized migration journal")
	}
	f, err := os.CreateTemp(dir, ".reference-journal-*.tmp")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(dir, historyReferenceMigrationJournalFile)); err != nil {
		return err
	}
	return syncSnapshotDir(dir)
}
