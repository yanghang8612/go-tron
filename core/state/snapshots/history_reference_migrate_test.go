package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gofrs/flock"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func referenceMigrationFixture(t *testing.T, count int) (string, [][]SegmentRef, *Manifest) {
	t.Helper()
	dir := t.TempDir()
	var trios [][]SegmentRef
	var all []SegmentRef
	for i := 0; i < count; i++ {
		from := uint64(i*2 + 1)
		changes := []*rawdb.StateDomainChange{
			{BlockNum: from, TxNum: from, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDelegation, Key: []byte("repeat"), PrevExists: true, Prev: bytes.Repeat([]byte{byte(i + 3)}, 64<<10)},
			{BlockNum: from + 1, TxNum: from + 1, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDelegation, Key: []byte("empty"), PrevExists: true},
		}
		refs := writeV6StateDomainHistorySegmentForTest(t, dir, from, from+1, changes)
		trios = append(trios, refs)
		all = append(all, refs...)
	}
	m := NewManifestForChain(1, uint64(count*2), all, ChainIdentity{ChainID: 1, NetworkID: 2, GenesisHash: strings.Repeat("a1", 32)})
	m.Generation, m.PublishedUnix = 81, 1789450000
	m.Progress = &Progress{HistoryBuildTxNum: 1, AccessorBuildTxNum: 1, LatestBuildTxNum: 1, HotPruneTxNum: 1, HotPruneBlockNum: 1, StateChangeIndexPruneBlockNum: 1}
	// Existing, missing retired files must remain metadata, never a prerequisite
	// to verifying or converting the active trio.
	retired := all[0]
	retired.Path = "unrelated-retired.seg"
	m.Retired = []SegmentRef{retired}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	return dir, trios, cloneManifest(m)
}

func referenceMigrationFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[rel] = checksumBytes(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func referenceMigrationAssertOldPresent(t *testing.T, dir string, refs []SegmentRef) {
	t.Helper()
	for _, ref := range refs {
		n, sha, err := stateDomainChangeBinaryFileMetadata(filepath.Join(dir, ref.Path))
		if err != nil || n != ref.Size || sha != ref.Checksum {
			t.Fatalf("source %s changed: %d/%s/%v", ref.Path, n, sha, err)
		}
	}
}

func referenceMigrationPending(t *testing.T, dir string, refs []SegmentRef) (*historyReferenceMigrationJournal, *Manifest) {
	t.Helper()
	newRefs, _, err := ReencodeHistoryReferenceTrioContext(context.Background(), dir, refs)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeProductionManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	j, next, err := historyReferenceMigrationPrepare(m, checksumBytes(data), refs, newRefs, 1789451234)
	if err != nil {
		t.Fatal(err)
	}
	if err := historyReferenceMigrationWriteJournal(dir, j); err != nil {
		t.Fatal(err)
	}
	return j, next
}

func TestHistoryReferenceMigrationTwoTriosBoundedResume(t *testing.T) {
	dir, trios, original := referenceMigrationFixture(t, 2)
	var progress []HistoryReferenceMigrationProgress
	admitted := 0
	opts := HistoryReferenceMigrationOptions{MaxTrios: 1,
		BeforeTrio: func(refs []SegmentRef) error {
			if !historyReferenceMigrationSameTrio(refs, trios[admitted]) {
				t.Fatal("wrong admission trio")
			}
			// Admission gets owned refs; mutating them cannot rewrite the source.
			refs[0].Path = "mutated-callback-copy"
			admitted++
			return nil
		}, OnProgress: func(p HistoryReferenceMigrationProgress) { progress = append(progress, p) }}
	for run := 0; run < 2; run++ {
		result, err := MigrateHistoryReferenceContext(context.Background(), dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		if result.CompletedTrios != 1 || result.MigratedTrios != 1 || result.RemainingTrios != uint64(1-run) || result.AlreadyCurrent != uint64(run) || result.JournalPending || result.ResumedJournal || result.DeletedSourceFiles != 3 {
			t.Fatalf("run %d: %+v", run, result)
		}
		oldSize, _ := historyReferenceMigrationBytes(trios[run])
		if result.DeletedSourceBytes != oldSize {
			t.Fatalf("deleted bytes %d want %d", result.DeletedSourceBytes, oldSize)
		}
		m, err := LoadProductionManifest(dir)
		if err != nil {
			t.Fatal(err)
		}
		if m.Generation != original.Generation+uint64(run+1) || !reflect.DeepEqual(m.Chain, original.Chain) || !reflect.DeepEqual(m.Progress, original.Progress) || m.VisibleTxStart != original.VisibleTxStart || m.VisibleTxEnd != original.VisibleTxEnd {
			t.Fatal("migration changed chain, coverage or progress")
		}
		for _, ref := range trios[run] {
			if _, err := os.Stat(filepath.Join(dir, ref.Path)); !os.IsNotExist(err) {
				t.Fatal("old source still present", ref.Path, err)
			}
		}
		if run == 0 {
			referenceMigrationAssertOldPresent(t, dir, trios[1])
			for _, ref := range trios[1] {
				found := false
				for _, active := range m.Segments {
					found = found || active == ref
				}
				if !found {
					t.Fatal("unrelated active trio changed")
				}
			}
		}
	}
	if len(progress) != 2 || admitted != 2 || progress[1].ManifestGeneration != original.Generation+2 {
		t.Fatal("progress not delivered", progress)
	}
	before := referenceMigrationFiles(t, dir)
	result, err := MigrateHistoryReferenceContext(context.Background(), dir, HistoryReferenceMigrationOptions{})
	if err != nil || result.MigratedTrios != 0 || result.AlreadyCurrent != 2 || result.RemainingTrios != 0 || !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
		t.Fatalf("already current invocation wrote data: %+v, %v", result, err)
	}
	// A complete independent logical and companion verification still succeeds
	// after both source trios have actually been unlinked.
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, candidate := range historyCompactionCandidates(m, cfg) {
		if err := historyReferenceMigrationVerifyNew(context.Background(), dir, append([]SegmentRef{candidate.history}, candidate.companions...)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHistoryReferenceMigrationCrashResume(t *testing.T) {
	for _, stage := range []string{"journal-before-publication", "published", "one-source-unlinked", "all-sources-unlinked"} {
		t.Run(stage, func(t *testing.T) {
			dir, trios, oldManifest := referenceMigrationFixture(t, 2)
			j, next := referenceMigrationPending(t, dir, trios[0])
			deleted := 0
			if stage != "journal-before-publication" {
				if err := PublishManifest(dir, next); err != nil {
					t.Fatal(err)
				}
			}
			switch stage {
			case "one-source-unlinked":
				deleted = 1
			case "all-sources-unlinked":
				deleted = 3
			}
			for _, ref := range j.Old[:deleted] {
				if err := os.Remove(filepath.Join(dir, ref.Path)); err != nil {
					t.Fatal(err)
				}
			}
			called := 0
			result, err := MigrateHistoryReferenceContext(context.Background(), dir, HistoryReferenceMigrationOptions{MaxTrios: 1, BeforeTrio: func([]SegmentRef) error { called++; return nil }})
			if err != nil {
				t.Fatal(err)
			}
			wantPublished := uint64(0)
			if stage == "journal-before-publication" {
				wantPublished = 1
			}
			if !result.ResumedJournal || result.JournalPending || result.CompletedTrios != 1 || result.MigratedTrios != wantPublished || result.RemainingTrios != 1 || result.DeletedSourceFiles != uint64(3-deleted) || called != int(wantPublished) {
				t.Fatalf("resume: %+v admission%d", result, called)
			}
			m, err := LoadProductionManifest(dir)
			if err != nil || m.Generation != oldManifest.Generation+1 || !reflect.DeepEqual(m.Progress, oldManifest.Progress) {
				t.Fatal("unexpected extra publication", m, err)
			}
			if _, err := os.Stat(filepath.Join(dir, historyReferenceMigrationJournalFile)); !os.IsNotExist(err) {
				t.Fatal("journal not removed", err)
			}
			referenceMigrationAssertOldPresent(t, dir, trios[1])
			if err := historyReferenceMigrationVerifyNew(context.Background(), dir, j.New); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHistoryReferenceMigrationNewCorruptionNeverDeletesSource(t *testing.T) {
	for _, published := range []bool{false, true} {
		for _, kind := range []SegmentKind{SegmentHistory, SegmentInverted, SegmentAccessor} {
			t.Run(string(kind)+map[bool]string{false: "/before", true: "/after"}[published], func(t *testing.T) {
				dir, trios, _ := referenceMigrationFixture(t, 1)
				j, next := referenceMigrationPending(t, dir, trios[0])
				if published {
					if err := PublishManifest(dir, next); err != nil {
						t.Fatal(err)
					}
				}
				for _, ref := range j.New {
					if ref.Kind == kind {
						data, err := os.ReadFile(filepath.Join(dir, ref.Path))
						if err != nil {
							t.Fatal(err)
						}
						data[len(data)-1] ^= 1
						if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0o600); err != nil {
							t.Fatal(err)
						}
					}
				}
				manifestBefore, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
				result, err := MigrateHistoryReferenceContext(context.Background(), dir, HistoryReferenceMigrationOptions{})
				if err == nil || result.DeletedSourceFiles != 0 || !result.JournalPending {
					t.Fatalf("corrupt output accepted: %+v %v", result, err)
				}
				referenceMigrationAssertOldPresent(t, dir, j.Old)
				manifestAfter, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
				if !bytes.Equal(manifestBefore, manifestAfter) {
					t.Fatal("manifest changed on corrupted resume")
				}
			})
		}
	}
}

func referenceMigrationPublishLease(t *testing.T, dir string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeProductionManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := publishedSnapshotManifestPath(m.Generation, checksumBytes(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := publishImmutableSnapshotManifest(dir, rel, data); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryReferenceMigrationPublishedLeaseNoWrites(t *testing.T) {
	for _, stage := range []string{"fresh", "pending-published", "lease-during-admission"} {
		t.Run(stage, func(t *testing.T) {
			dir, trios, _ := referenceMigrationFixture(t, 1)
			var next *Manifest
			if stage == "pending-published" {
				_, next = referenceMigrationPending(t, dir, trios[0])
			}
			if stage != "lease-during-admission" {
				referenceMigrationPublishLease(t, dir)
			}
			if next != nil {
				if err := PublishManifest(dir, next); err != nil {
					t.Fatal(err)
				}
			}
			before := referenceMigrationFiles(t, dir)
			opts := HistoryReferenceMigrationOptions{}
			if stage == "lease-during-admission" {
				opts.BeforeTrio = func([]SegmentRef) error { referenceMigrationPublishLease(t, dir); return nil }
			}
			_, err := MigrateHistoryReferenceContext(context.Background(), dir, opts)
			if err == nil || !strings.Contains(err.Error(), "pinned by published lease") {
				t.Fatal("source lease ignored", err)
			}
			referenceMigrationAssertOldPresent(t, dir, trios[0])
			if stage != "lease-during-admission" && !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
				t.Fatal("pinned migration wrote files")
			}
			if stage == "lease-during-admission" {
				if _, err := os.Stat(filepath.Join(dir, historyReferenceMigrationJournalFile)); !os.IsNotExist(err) {
					t.Fatal("journal written after lease was introduced")
				}
			}
		})
	}
}

func TestHistoryReferenceMigrationDryRunNoWrites(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "pending"}[pending], func(t *testing.T) {
			dir, trios, _ := referenceMigrationFixture(t, 2)
			if pending {
				referenceMigrationPending(t, dir, trios[0])
			}
			before := referenceMigrationFiles(t, dir)
			admitted := 0
			result, err := MigrateHistoryReferenceContext(context.Background(), dir, HistoryReferenceMigrationOptions{DryRun: true, MaxTrios: 1, BeforeTrio: func(refs []SegmentRef) error {
				admitted++
				if !historyReferenceMigrationSameTrio(refs, trios[0]) {
					t.Fatal("dry run admitted unselected trio")
				}
				return nil
			}})
			if err != nil || result.TotalTrios != 2 || result.RemainingTrios != 2 || result.CompletedTrios != 0 || result.JournalPending != pending || !result.DryRun || admitted != 1 {
				t.Fatalf("dry run: %+v %v", result, err)
			}
			if !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
				t.Fatal("dry run wrote files")
			}
		})
	}
}

func TestHistoryReferenceMigrationRejectsConcurrentManifestChanges(t *testing.T) {
	for _, change := range []string{"before-trio", "journal-old-progress", "journal-new-progress", "mixed-active", "source-not-retired"} {
		t.Run(change, func(t *testing.T) {
			dir, trios, old := referenceMigrationFixture(t, 2)
			opts := HistoryReferenceMigrationOptions{}
			if change == "before-trio" {
				opts.BeforeTrio = func([]SegmentRef) error {
					old.Progress.HistoryBuildTxNum++
					return PublishManifest(dir, old)
				}
			} else {
				j, next := referenceMigrationPending(t, dir, trios[0])
				m := next
				switch change {
				case "journal-old-progress":
					m = old
					m.Progress.HistoryBuildTxNum++
				case "journal-new-progress":
					m.Progress.HistoryBuildTxNum++
				case "source-not-retired":
					m.Retired = old.Retired
				case "mixed-active":
					// Build a malformed manifest directly: publication validation
					// itself normally rejects a mixed history/companion generation.
					for i, ref := range m.Segments {
						if ref == j.New[0] {
							m.Segments[i] = j.Old[0]
						}
					}
				}
				if change == "mixed-active" {
					data, _ := json.Marshal(m)
					if err := os.WriteFile(filepath.Join(dir, ManifestFile), data, 0o600); err != nil {
						t.Fatal(err)
					}
				} else if err := PublishManifest(dir, m); err != nil {
					t.Fatal(err)
				}
			}
			_, err := MigrateHistoryReferenceContext(context.Background(), dir, opts)
			if err == nil {
				t.Fatal("accepted changed manifest")
			}
			for _, refs := range trios {
				referenceMigrationAssertOldPresent(t, dir, refs)
			}
		})
	}
}

func TestHistoryReferenceMigrationJournalValidation(t *testing.T) {
	for _, corruption := range []string{"path", "checksum", "range", "kind", "companion-content", "unknown-field", "oversized", "trailing"} {
		t.Run(corruption, func(t *testing.T) {
			dir, trios, _ := referenceMigrationFixture(t, 1)
			j, _ := referenceMigrationPending(t, dir, trios[0])
			switch corruption {
			case "path":
				j.New[0].Path = "../outside.seg"
			case "checksum":
				j.New[0].Checksum = strings.Repeat("x", 64)
			case "range":
				j.New[0].ToTxNum++
			case "kind":
				j.New[1].Kind = j.New[0].Kind
			case "companion-content":
				j.New[1].Checksum = "sha256:" + strings.Repeat("0", 64)
			}
			data, _ := json.Marshal(j)
			switch corruption {
			case "unknown-field":
				data = append(data[:len(data)-1], []byte(",\"unrecognized\":true}")...)
			case "oversized":
				data = bytes.Repeat([]byte{' '}, 64*1024+1)
			case "trailing":
				data = append(data, []byte(" {}")...)
			}
			if err := os.WriteFile(filepath.Join(dir, historyReferenceMigrationJournalFile), data, 0o600); err != nil {
				t.Fatal(err)
			}
			before := referenceMigrationFiles(t, dir)
			if _, err := MigrateHistoryReferenceContext(context.Background(), dir, HistoryReferenceMigrationOptions{}); err == nil {
				t.Fatal("invalid journal accepted")
			}
			if !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
				t.Fatal("invalid journal changed files")
			}
		})
	}
}

func TestHistoryReferenceMigrationCancellationAdmissionAndLock(t *testing.T) {
	for _, fault := range []string{"canceled", "admission-error", "admission-cancel", "lock-held"} {
		t.Run(fault, func(t *testing.T) {
			dir, trios, _ := referenceMigrationFixture(t, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("space admission rejected")
			opts := HistoryReferenceMigrationOptions{}
			switch fault {
			case "canceled":
				cancel()
			case "admission-error":
				opts.BeforeTrio = func([]SegmentRef) error { return sentinel }
			case "admission-cancel":
				opts.BeforeTrio = func([]SegmentRef) error { cancel(); return nil }
			case "lock-held":
				lock := flock.New(filepath.Join(dir, historyReferenceMigrationLockFile))
				if ok, err := lock.TryLock(); err != nil || !ok {
					t.Fatal("fixture lock", err)
				}
				defer lock.Close()
			}
			_, err := MigrateHistoryReferenceContext(ctx, dir, opts)
			if err == nil {
				t.Fatal("expected failure")
			}
			if fault == "admission-error" && !errors.Is(err, sentinel) {
				t.Fatal("admission cause lost", err)
			}
			if (fault == "canceled" || fault == "admission-cancel") && !errors.Is(err, context.Canceled) {
				t.Fatal("context cause lost", err)
			}
			referenceMigrationAssertOldPresent(t, dir, trios[0])
			if _, err := os.Stat(filepath.Join(dir, historyReferenceMigrationJournalFile)); !os.IsNotExist(err) {
				t.Fatal("journal written on admission failure")
			}
		})
	}
}
