package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

// HistoryAccessorAdoption names one exact active trio and an independently
// produced V11 accessor. The candidate lives under CandidateDir, never at an
// active source path. Apply verifies every dictionary entry and posting tuple.
type HistoryAccessorAdoption struct {
	History, Accessor, Inverted, Candidate SegmentRef
}

type HistoryAccessorAdoptionOptions struct {
	Entries      []HistoryAccessorAdoption
	CandidateDir string
	ReceiptDir   string
	Apply        bool
	Rollback     bool
}

type HistoryAccessorAdoptionReport struct {
	DryRun, AlreadyApplied, RolledBack  bool
	Trios                               int
	TuplesVerified                      uint64
	SourceAccessorBytes                 uint64
	CandidateAccessorBytes              uint64
	BeforeGeneration, AfterGeneration   uint64
	BeforeManifestSHA, AfterManifestSHA string
	ReceiptDir                          string
}

type accessorAdoptionPair struct {
	Old, New  []SegmentRef
	Candidate SegmentRef
}

type accessorAdoptionJournal struct {
	Version                             uint32
	Status                              string
	CandidateDir                        string
	Pairs                               []accessorAdoptionPair
	BeforeManifestSHA, AfterManifestSHA string
	TuplesVerified                      uint64
}

const accessorAdoptionFileLimit = 32 << 20
const accessorAdoptionRecordLimit = 4_000_000

func adoptionRead(ctx context.Context, dir string, ref SegmentRef) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ref.Size == 0 || ref.Size > accessorAdoptionFileLimit {
		return nil, errors.New("snapshots: accessor adoption file cap")
	}
	if err := historyReferenceMigrationFile(dir, ref.Path, false); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return nil, err
	}
	if uint64(len(b)) != ref.Size || checksumBytes(b) != ref.Checksum {
		return nil, fmt.Errorf("snapshots: adoption SHA/size mismatch: %s", ref.Path)
	}
	return b, ctx.Err()
}

func compareAdoptedAccessor(ctx context.Context, old, candidate []byte) (uint64, error) {
	a, b := bytes.NewReader(old), bytes.NewReader(candidate)
	ah, err := decodeStateDomainChangeBinaryAccessorKeyHeader(a, uint64(len(old)))
	if err != nil {
		return 0, err
	}
	bh, err := decodeStateDomainChangeBinaryAccessorKeyHeader(b, uint64(len(candidate)))
	if err != nil {
		return 0, err
	}
	if ah.version != 7 || bh.version != 11 || ah.recordCount > accessorAdoptionRecordLimit || ah.keyCount > 1<<20 ||
		ah.recordCount != bh.recordCount || ah.keyCount != bh.keyCount || ah.dictionaryDigest != bh.dictionaryDigest ||
		ah.fromTxNum != bh.fromTxNum || ah.toTxNum != bh.toTxNum {
		return 0, errors.New("snapshots: adoption version/dictionary/range/count differs")
	}
	ad, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(a, ah)
	if err != nil {
		return 0, err
	}
	bd, err := stateDomainChangeBinaryAccessorV6ReadBlockDirectory(b, bh)
	if err != nil {
		return 0, err
	}
	if len(ad) != len(bd) {
		return 0, errors.New("snapshots: adoption dictionary block count differs")
	}
	for i := range ad {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		ar, err := stateDomainChangeBinaryAccessorV6ReadBlock(a, uint64(len(old)), ah, ad[i], uint32(i*128))
		if err != nil {
			return 0, err
		}
		br, err := stateDomainChangeBinaryAccessorV6ReadBlock(b, uint64(len(candidate)), bh, bd[i], uint32(i*128))
		if err != nil {
			return 0, err
		}
		if len(ar) != len(br) {
			return 0, errors.New("snapshots: adoption dictionary count differs")
		}
		for j := range ar {
			if !bytes.Equal(ar[j].key, br[j].key) || ar[j].keyID != br[j].keyID || ar[j].postings != br[j].postings {
				return 0, errors.New("snapshots: adoption key/ID/posting count differs")
			}
		}
	}
	ac, err := newStateDomainChangeBinaryAccessorV7PostingCursor(a, uint64(len(old)))
	if err != nil {
		return 0, err
	}
	bc, err := newStateDomainChangeBinaryAccessorV7PostingCursor(b, uint64(len(candidate)))
	if err != nil {
		return 0, err
	}
	var n uint64
	for {
		if n%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return n, err
			}
		}
		ak, ap, ao, err := ac.Next()
		if err != nil {
			return n, err
		}
		bk, bp, bo, err := bc.Next()
		if err != nil {
			return n, err
		}
		if ao != bo || ak != bk || ap != bp {
			return n, fmt.Errorf("snapshots: adoption tuple differs at %d", n)
		}
		if !ao {
			break
		}
		n++
	}
	if n != ah.recordCount {
		return n, errors.New("snapshots: adoption posting coverage differs")
	}
	return n, nil
}

func adoptionNext(m *Manifest, pairs []accessorAdoptionPair, reverse bool) (*Manifest, error) {
	next := cloneManifest(m)
	remove := make(map[string]SegmentRef)
	var add []SegmentRef
	for _, pair := range pairs {
		old, newRefs := pair.Old, pair.New
		if reverse {
			old, newRefs = newRefs, old
		}
		for _, ref := range old {
			if _, exists := remove[ref.Path]; exists {
				return nil, errors.New("snapshots: duplicate adoption source")
			}
			remove[ref.Path] = ref
		}
		add = append(add, newRefs...)
	}
	next.Segments = nil
	matched := 0
	for _, ref := range m.Segments {
		if want, ok := remove[ref.Path]; ok {
			if want != ref {
				return nil, errors.New("snapshots: adoption active source identity changed")
			}
			matched++
		} else {
			next.Segments = append(next.Segments, ref)
		}
	}
	if matched != len(remove) {
		return nil, errors.New("snapshots: adoption source is no longer active")
	}
	for _, ref := range add {
		for _, existing := range next.Segments {
			if existing.Path == ref.Path {
				return nil, errors.New("snapshots: adoption target already active")
			}
		}
		for _, retired := range next.Retired {
			if retired.Path == ref.Path {
				return nil, errors.New("snapshots: adoption target already retired")
			}
		}
	}
	// Retention is deliberate: removed refs remain physical files and are held
	// by the external receipt/backup, not enrolled in automatic retired GC.
	for _, retired := range next.Retired {
		if _, ok := remove[retired.Path]; ok {
			return nil, errors.New("snapshots: adoption source already queued for retirement")
		}
	}
	next.Segments = append(next.Segments, add...)
	if next.Generation == ^uint64(0) {
		return nil, errors.New("snapshots: adoption generation overflow")
	}
	next.Generation++
	next.PublishedUnix = time.Now().Unix()
	normalizeManifest(next)
	sortSegments(next.Segments)
	sortSegments(next.Retired)
	if err := next.ValidateProduction(); err != nil {
		return nil, err
	}
	return next, nil
}

func adoptionWriteNew(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".accessor-adopt-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // Only this invocation's unpublished scratch link.
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	// Link publishes a complete fsynced inode without overwriting an existing
	// destination. An interrupted copy never leaves a partial final alias.
	if err = os.Link(f.Name(), path); err != nil {
		return err
	}
	return syncSnapshotDir(filepath.Dir(path))
}

func adoptionWriteJournal(dir string, j *accessorAdoptionJournal) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".adoption-journal-*.tmp")
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(dir, "journal.json")); err != nil {
		return err
	}
	return syncSnapshotDir(dir)
}

func adoptionInstall(ctx context.Context, sourceDir, targetDir string, source, target SegmentRef, link bool) error {
	b, err := adoptionRead(ctx, sourceDir, source)
	if err != nil {
		return err
	}
	if source.Size != target.Size || source.Checksum != target.Checksum {
		return errors.New("snapshots: adoption install identity differs")
	}
	if err = os.MkdirAll(targetDir, 0700); err != nil {
		return err
	}
	if err = historyReferenceMigrationDirectory(targetDir); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(filepath.Join(targetDir, target.Path)), 0700); err != nil {
		return err
	}
	if err = historyReferenceMigrationFile(targetDir, target.Path, true); err != nil {
		return err
	}
	path := filepath.Join(targetDir, target.Path)
	if _, err = os.Lstat(path); err == nil {
		_, err = adoptionRead(ctx, targetDir, target)
		return err
	} else if !os.IsNotExist(err) {
		return err
	}
	if link {
		if err = os.Link(filepath.Join(sourceDir, source.Path), path); err != nil {
			return err
		}
		return syncSnapshotDir(filepath.Dir(path))
	}
	return adoptionWriteNew(path, b)
}

func adoptionPublish(ctx context.Context, dir, before string, next *Manifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return err
	}
	if checksumBytes(b) != before {
		return errors.New("snapshots: concurrent manifest change during adoption")
	}
	return PublishManifest(dir, next)
}

// AdoptHistoryAccessorsV11Context is an offline sealed-batch installer, not a
// transcode service. Apply and rollback require the caller to hold the normal
// datadir's exclusive database lock with the node stopped. DryRun is read-only.
// No source, retired, alias, or backup is deleted by this protocol.
func AdoptHistoryAccessorsV11Context(ctx context.Context, dir string, opts HistoryAccessorAdoptionOptions) (*HistoryAccessorAdoptionReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err = historyReferenceMigrationDirectory(dir); err != nil {
		return nil, err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	if err = historyReferenceMigrationFile(dir, ManifestFile, false); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	m, err := decodeProductionManifest(data)
	if err != nil {
		return nil, err
	}
	r := &HistoryAccessorAdoptionReport{DryRun: !opts.Apply, BeforeGeneration: m.Generation, BeforeManifestSHA: checksumBytes(data)}
	if opts.ReceiptDir == "" {
		return r, errors.New("snapshots: adoption receipt directory required")
	}
	receipt, err := filepath.Abs(opts.ReceiptDir)
	if err != nil {
		return r, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(receipt))
	if err != nil {
		return r, errors.New("snapshots: adoption receipt parent must be a real existing directory")
	}
	receipt = filepath.Join(parent, filepath.Base(receipt))
	r.ReceiptDir = receipt
	if rel, err := filepath.Rel(dir, receipt); err != nil || rel == "." || !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return r, errors.New("snapshots: adoption receipts must be outside snapshot directory")
	}
	var unlock func() error
	if opts.Apply {
		lock := flock.New(filepath.Join(dir, historyReferenceMigrationLockFile))
		ok, err := lock.TryLock()
		if err != nil || !ok {
			return r, fmt.Errorf("snapshots: adoption migration lock unavailable: %v", err)
		}
		unlock = lock.Unlock
		defer unlock()
		// Reload after acquiring the cross-process migration gate.
		current, e := os.ReadFile(filepath.Join(dir, ManifestFile))
		if e != nil || !bytes.Equal(data, current) {
			return r, errors.New("snapshots: manifest changed before adoption lock")
		}
	}
	var j accessorAdoptionJournal
	if _, e := os.Lstat(receipt); e == nil {
		if err = historyReferenceMigrationDirectory(receipt); err != nil {
			return r, err
		}
		if err = historyReferenceMigrationFile(receipt, "journal.json", true); err != nil {
			return r, err
		}
		if info, e := os.Stat(filepath.Join(receipt, "journal.json")); e == nil && info.Size() > 128<<10 {
			return r, errors.New("snapshots: adoption journal exceeds bound")
		}
	} else if !os.IsNotExist(e) {
		return r, e
	}
	journalData, journalErr := os.ReadFile(filepath.Join(receipt, "journal.json"))
	if journalErr == nil {
		if err = historyReferenceMigrationDirectory(receipt); err != nil {
			return r, err
		}
		if err = historyReferenceMigrationFile(receipt, "journal.json", false); err != nil {
			return r, err
		}
		if err = json.Unmarshal(journalData, &j); err != nil {
			return r, err
		}
		if j.Status != "prepared" && j.Status != "published" && j.Status != "rolled-back" {
			return r, errors.New("snapshots: invalid adoption journal status")
		}
		if j.Version != 1 || len(j.Pairs) == 0 || len(j.Pairs) > 4 || !historyReferenceMigrationSHA(j.BeforeManifestSHA) || !historyReferenceMigrationSHA(j.AfterManifestSHA) {
			return r, errors.New("snapshots: invalid adoption journal")
		}
		for _, pair := range j.Pairs {
			if len(pair.Old) != 3 || len(pair.New) != 3 || pair.Old[0].Kind != SegmentHistory || pair.Old[1].Kind != SegmentAccessor || pair.Old[2].Kind != SegmentInverted {
				return r, errors.New("snapshots: invalid adoption journal trio")
			}
			if err = historyReferenceMigrationValidateTrio(pair.Old); err != nil {
				return r, err
			}
			candidate := pair.Old[1]
			candidate.Path, candidate.Size, candidate.Checksum = pair.Candidate.Path, pair.Candidate.Size, pair.Candidate.Checksum
			if candidate != pair.Candidate || !historyReferenceMigrationSHA(candidate.Checksum) || candidate.Size >= pair.Old[1].Size {
				return r, errors.New("snapshots: invalid adoption journal candidate")
			}
			want := adoptionPair(HistoryAccessorAdoption{History: pair.Old[0], Accessor: pair.Old[1], Inverted: pair.Old[2], Candidate: pair.Candidate})
			for i := range pair.New {
				if pair.New[i] != want.New[i] {
					return r, errors.New("snapshots: adoption journal target identity differs")
				}
			}
		}
		if opts.Rollback {
			return rollbackAdoptedAccessors(ctx, dir, receipt, m, &j, opts.Apply, r)
		}
		if checksumBytes(data) == j.AfterManifestSHA || j.Status == "published" && adoptionPairsActive(m, j.Pairs, false) {
			for _, pair := range j.Pairs {
				for _, ref := range pair.New {
					if _, err = adoptionRead(ctx, dir, ref); err != nil {
						return r, err
					}
				}
			}
			r.AlreadyApplied, r.Trios, r.TuplesVerified = true, len(j.Pairs), j.TuplesVerified
			r.AfterGeneration, r.AfterManifestSHA = m.Generation, checksumBytes(data)
			if opts.Apply && j.Status != "published" {
				j.Status = "published"
				err = adoptionWriteJournal(receipt, &j)
			}
			return r, err
		}
		if checksumBytes(data) != j.BeforeManifestSHA {
			return r, errors.New("snapshots: adoption journal conflicts with current manifest")
		}
		if j.Status != "prepared" {
			return r, errors.New("snapshots: adoption journal phase conflicts with manifest")
		}
		if !opts.Apply {
			r.Trios = len(j.Pairs)
			return r, nil
		}
		return finishAccessorAdoption(ctx, dir, receipt, m, &j, r, false)
	}
	if !os.IsNotExist(journalErr) {
		return r, journalErr
	}
	if opts.Rollback {
		return r, errors.New("snapshots: adoption rollback requires its journal")
	}
	if len(opts.Entries) == 0 || len(opts.Entries) > 4 {
		return r, errors.New("snapshots: adoption requires one to four explicit trios")
	}
	opts.CandidateDir, err = filepath.Abs(opts.CandidateDir)
	if err != nil {
		return r, err
	}
	if err = historyReferenceMigrationDirectory(opts.CandidateDir); err != nil {
		return r, err
	}
	opts.CandidateDir, err = filepath.EvalSymlinks(opts.CandidateDir)
	if err != nil {
		return r, err
	}
	for _, entry := range opts.Entries {
		old := []SegmentRef{entry.History, entry.Accessor, entry.Inverted}
		if entry.History.Kind != SegmentHistory || entry.Accessor.Kind != SegmentAccessor || entry.Inverted.Kind != SegmentInverted {
			return r, errors.New("snapshots: adoption source roles differ")
		}
		if err = historyReferenceMigrationValidateTrio(old); err != nil {
			return r, err
		}
		want := entry.Accessor
		want.Size, want.Checksum, want.Path = entry.Candidate.Size, entry.Candidate.Checksum, entry.Candidate.Path
		if entry.Candidate != want || !historyReferenceMigrationSHA(entry.Candidate.Checksum) || entry.Candidate.Size >= entry.Accessor.Size {
			return r, errors.New("snapshots: adoption candidate identity or measured saving invalid")
		}
		for _, ref := range old {
			if _, err = adoptionRead(ctx, dir, ref); err != nil {
				return r, err
			}
		}
		source, err := adoptionRead(ctx, dir, entry.Accessor)
		if err != nil {
			return r, err
		}
		candidate, err := adoptionRead(ctx, opts.CandidateDir, entry.Candidate)
		if err != nil {
			return r, err
		}
		n, err := compareAdoptedAccessor(ctx, source, candidate)
		if err != nil {
			return r, err
		}
		h, err := decodeStateDomainChangeBinaryAccessorKeyHeader(bytes.NewReader(source), uint64(len(source)))
		if err != nil || h.fromTxNum != entry.History.FromTxNum || h.toTxNum != entry.History.ToTxNum {
			return r, errors.New("snapshots: adoption accessor/ref range differs")
		}
		hr, _, hh, err := openHistorySegmentForRead(dir, entry.History)
		if err != nil {
			return r, err
		}
		digest, readErr := readStateDomainChangeBinaryV6DictionaryCommitment(hr)
		closeErr := hr.Close()
		if readErr != nil || closeErr != nil || hh.version != 6 || hh.count != n || digest != h.dictionaryDigest {
			return r, errors.New("snapshots: adoption history/dictionary binding differs")
		}
		j.Pairs = append(j.Pairs, adoptionPair(entry))
		r.TuplesVerified += n
		r.SourceAccessorBytes += entry.Accessor.Size
		r.CandidateAccessorBytes += entry.Candidate.Size
	}
	next, err := adoptionNext(m, j.Pairs, false)
	if err != nil {
		return r, err
	}
	nextData, err := json.Marshal(next)
	if err != nil {
		return r, err
	}
	r.Trios, r.AfterGeneration, r.AfterManifestSHA = len(j.Pairs), next.Generation, checksumBytes(nextData)
	if !opts.Apply {
		return r, nil
	}
	if err = os.Mkdir(receipt, 0700); err != nil {
		return r, err
	}
	if err = syncSnapshotDir(filepath.Dir(receipt)); err != nil {
		return r, err
	}
	j.Version, j.Status, j.CandidateDir = 1, "prepared", opts.CandidateDir
	j.BeforeManifestSHA, j.AfterManifestSHA, j.TuplesVerified = checksumBytes(data), checksumBytes(nextData), r.TuplesVerified
	if err = adoptionWriteNew(filepath.Join(receipt, "before-manifest.json"), data); err != nil {
		return r, err
	}
	if err = adoptionWriteNew(filepath.Join(receipt, "after-manifest.json"), nextData); err != nil {
		return r, err
	}
	if err = adoptionWriteJournal(receipt, &j); err != nil {
		return r, err
	}
	return finishAccessorAdoption(ctx, dir, receipt, m, &j, r, true)
}

func adoptionPair(entry HistoryAccessorAdoption) accessorAdoptionPair {
	old := []SegmentRef{entry.History, entry.Accessor, entry.Inverted}
	// Both callers validate the complete candidate checksum before deriving aliases.
	digest := strings.TrimPrefix(entry.Candidate.Checksum, "sha256:")
	base := strings.TrimSuffix(entry.History.Path, ".seg") + "-v11-" + digest[:16]
	newRefs := append([]SegmentRef(nil), old...)
	newRefs[0].Path, newRefs[1].Path, newRefs[2].Path = base+".seg", base+".kv", base+".idx"
	newRefs[1].Size, newRefs[1].Checksum = entry.Candidate.Size, entry.Candidate.Checksum
	return accessorAdoptionPair{Old: old, New: newRefs, Candidate: entry.Candidate}
}

func adoptionPairsActive(m *Manifest, pairs []accessorAdoptionPair, reverse bool) bool {
	active := make(map[string]SegmentRef, len(m.Segments))
	for _, ref := range m.Segments {
		active[ref.Path] = ref
	}
	for _, pair := range pairs {
		refs := pair.New
		if reverse {
			refs = pair.Old
		}
		for _, ref := range refs {
			if active[ref.Path] != ref {
				return false
			}
		}
	}
	return true
}

func finishAccessorAdoption(ctx context.Context, dir, receipt string, m *Manifest, j *accessorAdoptionJournal, r *HistoryAccessorAdoptionReport, verified bool) (*HistoryAccessorAdoptionReport, error) {
	for _, pair := range j.Pairs {
		if !verified {
			a, err := adoptionRead(ctx, dir, pair.Old[1])
			if err != nil {
				return r, err
			}
			b, err := adoptionRead(ctx, j.CandidateDir, pair.Candidate)
			if err != nil {
				return r, err
			}
			if _, err = compareAdoptedAccessor(ctx, a, b); err != nil {
				return r, err
			}
		}
		for i, ref := range pair.Old {
			if err := adoptionInstall(ctx, dir, filepath.Join(receipt, "old"), ref, ref, true); err != nil {
				return r, err
			}
			sourceDir, source := dir, ref
			if i == 1 {
				sourceDir, source = j.CandidateDir, pair.Candidate
			}
			if err := adoptionInstall(ctx, sourceDir, dir, source, pair.New[i], i != 1); err != nil {
				return r, err
			}
		}
	}
	data, err := os.ReadFile(filepath.Join(receipt, "after-manifest.json"))
	if err != nil || checksumBytes(data) != j.AfterManifestSHA {
		return r, errors.New("snapshots: adoption target manifest receipt differs")
	}
	next, err := decodeProductionManifest(data)
	if err != nil {
		return r, err
	}
	expected, err := adoptionNext(m, j.Pairs, false)
	if err != nil {
		return r, err
	}
	// PublishedUnix is fixed by the prepared target, rather than resume time.
	expected.PublishedUnix = next.PublishedUnix
	b, err := json.Marshal(expected)
	if err != nil || !bytes.Equal(b, data) {
		return r, errors.New("snapshots: adoption target contains unrelated changes")
	}
	if err = adoptionPublish(ctx, dir, j.BeforeManifestSHA, next); err != nil {
		return r, err
	}
	j.Status = "published"
	if err = adoptionWriteJournal(receipt, j); err != nil {
		return r, err
	}
	r.Trios, r.TuplesVerified, r.AfterGeneration, r.AfterManifestSHA = len(j.Pairs), j.TuplesVerified, next.Generation, j.AfterManifestSHA
	return r, nil
}

func rollbackAdoptedAccessors(ctx context.Context, dir, receipt string, m *Manifest, j *accessorAdoptionJournal, apply bool, r *HistoryAccessorAdoptionReport) (*HistoryAccessorAdoptionReport, error) {
	for _, pair := range j.Pairs {
		for _, ref := range pair.Old {
			if _, err := adoptionRead(ctx, filepath.Join(receipt, "old"), ref); err != nil {
				return r, err
			}
			if _, err := adoptionRead(ctx, dir, ref); err != nil {
				return r, err
			}
		}
	}
	if adoptionPairsActive(m, j.Pairs, true) {
		r.RolledBack, r.AlreadyApplied, r.Trios = true, true, len(j.Pairs)
		r.AfterGeneration, r.AfterManifestSHA = m.Generation, r.BeforeManifestSHA
		if apply && j.Status != "rolled-back" {
			j.Status = "rolled-back"
			return r, adoptionWriteJournal(receipt, j)
		}
		return r, nil
	}
	next, err := adoptionNext(m, j.Pairs, true)
	if err != nil {
		return r, err
	}
	b, err := json.Marshal(next)
	if err != nil {
		return r, err
	}
	r.Trios, r.AfterGeneration, r.AfterManifestSHA = len(j.Pairs), next.Generation, checksumBytes(b)
	if !apply {
		return r, nil
	}
	if err = adoptionPublish(ctx, dir, r.BeforeManifestSHA, next); err != nil {
		return r, err
	}
	j.Status = "rolled-back"
	if err = adoptionWriteJournal(receipt, j); err != nil {
		return r, err
	}
	r.RolledBack = true
	return r, nil
}
