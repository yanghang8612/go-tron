package snapshots

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func cdcMigrationFixture(t *testing.T, count int) (string, [][]SegmentRef, *Manifest) {
	t.Helper()
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	base := make([]byte, 256<<10)
	rand.New(rand.NewSource(718)).Read(base)
	var trios [][]SegmentRef
	var all []SegmentRef
	for n := 0; n < count; n++ {
		from, to := uint64(n*12+1), uint64((n+1)*12)
		for tx := from; tx <= to; tx++ {
			value := append([]byte{byte(tx)}, base...)
			row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDelegation, Key: []byte("drax-test"), PrevExists: true, Prev: value}
			if err := rawdb.WriteStateTxRange(db, tx, row.BlockHash, tx, tx); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{row}); err != nil {
				t.Fatal(err)
			}
		}
		refs, err := BuildDiagnosticStateHistoryTrioContext(context.Background(), db, dir, from, to, from, to, fmt.Sprintf("history/state-domain-change-%d-%d.seg", from, to), "2")
		if err != nil {
			t.Fatal(err)
		}
		trios = append(trios, refs)
		all = append(all, refs...)
	}
	m := NewManifest(1, uint64(count*12), all)
	m.Generation = 81
	m.Progress = &Progress{HistoryBuildTxNum: 12, HotPruneTxNum: 12}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	return dir, trios, cloneManifest(m)
}
func cdcPending(t *testing.T, dir string, refs []SegmentRef) (*historyReferenceMigrationJournal, *Manifest) {
	t.Helper()
	out, err := reencodeHistoryCDC(context.Background(), dir, refs, 5, false)
	if err != nil || out == nil {
		t.Fatal(out, err)
	}
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeProductionManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	j, next, err := historyReferenceMigrationPrepare(m, checksumBytes(data), refs, out, 1789451234)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeHistoryMigrationJournal(dir, historyCDCMigrationJournalFile, j); err != nil {
		t.Fatal(err)
	}
	return j, next
}
func TestHistoryCDCMigrationExactQueriesMixedAndResume(t *testing.T) {
	dir, trios, original := cdcMigrationFixture(t, 2)
	ctx := context.Background()
	before, err := DigestColdStateHistoryContext(ctx, dir, trios[1])
	if err != nil {
		t.Fatal(err)
	}
	h, i, _, _, _ := historyReferenceTranscodeIdentity(trios[1])
	want, err := readStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, h, i, 17, 19)
	if err != nil {
		t.Fatal(err)
	}
	beforeFiles := referenceMigrationFiles(t, dir)
	result, err := MigrateHistoryCDCContext(ctx, dir, HistoryCDCMigrationOptions{DryRun: true, MaxTrios: 1, MinSavingsPercent: 5})
	if err != nil || result.TotalTrios != 2 || !reflect.DeepEqual(beforeFiles, referenceMigrationFiles(t, dir)) {
		t.Fatal(result, err)
	}
	result, err = MigrateHistoryCDCContext(ctx, dir, HistoryCDCMigrationOptions{MaxTrios: 1, MinSavingsPercent: 5})
	if err != nil || result.MigratedTrios != 1 || result.DeletedSourceFiles != 3 || result.ActiveBytesAfter >= result.ActiveBytesBefore {
		t.Fatal(result, err)
	}
	referenceMigrationAssertOldPresent(t, dir, trios[0])
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Generation != 82 || !reflect.DeepEqual(original.Progress, m.Progress) || m.VisibleTxEnd != original.VisibleTxEnd {
		t.Fatal("progress changed")
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, c := range historyCompactionCandidates(m, cfg) {
		if c.history.FromTxNum == 13 {
			after, e := DigestColdStateHistoryContext(ctx, dir, append([]SegmentRef{c.history}, c.companions...))
			if e != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("mixed format digest differs", before, after, e)
			}
			idx, _ := historyCompactionCompanion(c, SegmentInverted)
			got, e := readStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, c.history, idx, 17, 19)
			if e != nil || !reflect.DeepEqual(want, got) {
				t.Fatal("query differs", e)
			}
		}
	}
	result, err = MigrateHistoryCDCContext(ctx, dir, HistoryCDCMigrationOptions{MaxTrios: 0, MinSavingsPercent: 5})
	if err != nil || result.AlreadyCurrent != 1 || result.MigratedTrios != 1 {
		t.Fatal(result, err)
	}
	beforeFiles = referenceMigrationFiles(t, dir)
	result, err = MigrateHistoryCDCContext(ctx, dir, HistoryCDCMigrationOptions{})
	if err != nil || result.AlreadyCurrent != 2 || !reflect.DeepEqual(beforeFiles, referenceMigrationFiles(t, dir)) {
		t.Fatal(result, err)
	}
}
func TestHistoryCDCMigrationCrashResume(t *testing.T) {
	for _, stage := range []int{0, 1, 2, 4} {
		t.Run(fmt.Sprint(stage), func(t *testing.T) {
			dir, trios, original := cdcMigrationFixture(t, 1)
			j, next := cdcPending(t, dir, trios[0])
			if stage > 0 {
				if err := PublishManifest(dir, next); err != nil {
					t.Fatal(err)
				}
			}
			for n := 1; n < stage; n++ {
				if err := os.Remove(filepath.Join(dir, j.Old[n-1].Path)); err != nil {
					t.Fatal(err)
				}
			}
			result, err := MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{MaxTrios: 1, MinSavingsPercent: 5})
			if err != nil || !result.ResumedJournal || result.JournalPending || result.CompletedTrios != 1 || result.RemainingTrios != 0 {
				t.Fatal(result, err)
			}
			m, err := LoadProductionManifest(dir)
			if err != nil || m.Generation != original.Generation+1 || !reflect.DeepEqual(m.Progress, original.Progress) {
				t.Fatal(m, err)
			}
			if _, err = DigestColdStateHistoryContext(context.Background(), dir, m.Segments); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestHistoryCDCMigrationRejectsCorruptionAndPins(t *testing.T) {
	for _, fault := range []string{"source", "new-history", "new-accessor", "new-index", "lease", "conflict", "other-target", "bounds"} {
		t.Run(fault, func(t *testing.T) {
			dir, trios, _ := cdcMigrationFixture(t, 1)
			opts := HistoryCDCMigrationOptions{MinSavingsPercent: 5}
			if fault == "lease" {
				referenceMigrationPublishLease(t, dir)
			} else if fault == "source" {
				path := filepath.Join(dir, trios[0][0].Path)
				b, _ := os.ReadFile(path)
				b[len(b)-1] ^= 1
				os.WriteFile(path, b, 0600)
			} else {
				j, next := cdcPending(t, dir, trios[0])
				if fault == "other-target" {
					before := referenceMigrationFiles(t, dir)
					if _, err := MigrateHistoryReferenceContext(context.Background(), dir, HistoryReferenceMigrationOptions{}); err == nil {
						t.Fatal("R1 accepted CDC journal")
					}
					if !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
						t.Fatal("other target wrote")
					}
					return
				}
				if fault == "bounds" {
					opts.FromTxNum = 13
				} else if fault == "conflict" {
					next.Progress.HotPruneTxNum++
					if err := PublishManifest(dir, next); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := PublishManifest(dir, next); err != nil {
						t.Fatal(err)
					}
					kind := map[string]SegmentKind{"new-history": SegmentHistory, "new-accessor": SegmentAccessor, "new-index": SegmentInverted}[fault]
					for _, ref := range j.New {
						if ref.Kind == kind {
							path := filepath.Join(dir, ref.Path)
							b, _ := os.ReadFile(path)
							b[len(b)-1] ^= 1
							os.WriteFile(path, b, 0600)
						}
					}
				}
			}
			before := referenceMigrationFiles(t, dir)
			if _, err := MigrateHistoryCDCContext(context.Background(), dir, opts); err == nil {
				t.Fatal("accepted", fault)
			}
			after := referenceMigrationFiles(t, dir)
			delete(before, historyReferenceMigrationLockFile)
			delete(after, historyReferenceMigrationLockFile)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("failure changed files", fault)
			}
		})
	}
}
func TestHistoryCDCMigrationNoGainAndSelection(t *testing.T) {
	dir, trios, _ := cdcMigrationFixture(t, 2)
	before := referenceMigrationFiles(t, dir)
	result, err := MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{MaxTrios: 1, MinSavingsPercent: 100, FromTxNum: 13})
	if err != nil || result.SkippedNoGain != 1 || result.MigratedTrios != 0 || result.TotalTrios != 1 {
		t.Fatal(result, err)
	}
	after := referenceMigrationFiles(t, dir)
	delete(after, historyReferenceMigrationLockFile)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("rejected size wrote output")
	}
	referenceMigrationAssertOldPresent(t, dir, trios[1])
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = MigrateHistoryCDCContext(ctx, dir, HistoryCDCMigrationOptions{}); err == nil {
		t.Fatal("accepted canceled migration")
	}
	// Control documents must remain byte-identical when there is no publication.
	if !bytes.Equal([]byte(before[ManifestFile]), []byte(after[ManifestFile])) {
		t.Fatal("manifest differs")
	}
}

func TestHistoryCDCMigrationJournalBlocksMetadataGC(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, historyCDCMigrationJournalFile), []byte("pending"), 0600); err != nil {
		t.Fatal(err)
	}
	_, reason := retiredMetadataSweepGuard(dir, retiredMetadataOS)
	if reason != retiredMetadataDeferredMigration {
		t.Fatal(reason)
	}
}

// Deliberately use small R1 spans over the exact existing V6 bytes: the span
// directory is physical overhead, never part of the virtual history stream.
func cdcR1MigrationFixture(t *testing.T) (string, []SegmentRef, *Manifest) {
	t.Helper()
	dir, trios, _ := cdcMigrationFixture(t, 1)
	refs := trios[0]
	h, _, _, _, _ := historyReferenceTranscodeIdentity(refs)
	r, size, _, err := openHistorySegmentForRead(dir, h)
	if err != nil {
		t.Fatal(err)
	}
	w, err := newHistoryReferenceWriter(context.Background(), dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	buf := make([]byte, historyReferenceMaxChunk-1)
	for off := uint64(0); off < size; {
		part := buf[:min(uint64(len(buf)), size-off)]
		if err = historyReferenceReadAt(context.Background(), r, part, off); err != nil {
			t.Fatal(err)
		}
		id, err := w.StoreChunk(part)
		if err != nil {
			t.Fatal(err)
		}
		paddedID, err := w.StoreChunk(append([]byte{0}, part...))
		if err != nil {
			t.Fatal(err)
		}
		for start := 0; start < len(part); start += 64 {
			chunk, offset := id, uint32(start)
			// Alternate equivalent slices of distinct chunks so the writer cannot
			// coalesce adjacent spans; virtual bytes remain exactly the source.
			if start/64%2 == 1 {
				chunk, offset = paddedID, offset+1
			}
			if err = w.WriteSpan(chunk, offset, uint32(min(64, len(part)-start))); err != nil {
				t.Fatal(err)
			}
		}
		off += uint64(len(part))
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, h.Path)
	size, checksum, err := w.Finalize(path + ".r1")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path+".r1", path); err != nil {
		t.Fatal(err)
	}
	for n := range refs {
		if refs[n].Kind == SegmentHistory {
			refs[n].Size, refs[n].Checksum = size, checksum
		}
	}
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	m.Segments = refs
	if err = PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	return dir, refs, cloneManifest(m)
}

func TestHistoryCDCMigrationR1SameInputSizeAndQueries(t *testing.T) {
	dir, refs, original := cdcR1MigrationFixture(t)
	ctx := context.Background()
	h, i, a, _, _ := historyReferenceTranscodeIdentity(refs)
	before, err := DigestColdStateHistoryContext(ctx, dir, refs)
	if err != nil {
		t.Fatal(err)
	}
	want, err := readStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, h, i, 3, 7)
	if err != nil {
		t.Fatal(err)
	}
	accessorBefore, err := os.ReadFile(filepath.Join(dir, a.Path))
	if err != nil {
		t.Fatal(err)
	}
	results, err := RecompressHistoryDiagnostic(ctx, filepath.Join(dir, h.Path), filepath.Join(t.TempDir(), "same-input"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].LogicalSHA256 != results[1].LogicalSHA256 || results[1].OutputBytes >= h.Size {
		t.Fatalf("same-input sizes: %+v", results)
	}
	for _, result := range results {
		info, err := os.Stat(result.Output)
		if err != nil || uint64(info.Size()) != result.OutputBytes {
			t.Fatal(info, err)
		}
		t.Logf("R1=%d bytes, V%s=%d bytes, logical=%d, encode=%.3fs, verify=%.3fs", h.Size, result.Format, result.OutputBytes, result.LogicalBytes, result.EncodeSeconds, result.VerifySeconds)
	}
	result, err := MigrateHistoryCDCContext(ctx, dir, HistoryCDCMigrationOptions{MinSavingsPercent: 5})
	if err != nil || result.MigratedTrios != 1 || result.DeletedSourceFiles != 3 {
		t.Fatal(result, err)
	}
	m, err := LoadProductionManifest(dir)
	if err != nil || m.Generation != original.Generation+1 || !reflect.DeepEqual(m.Progress, original.Progress) {
		t.Fatal(m, err)
	}
	after, err := DigestColdStateHistoryContext(ctx, dir, m.Segments)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("R1 -> CDC digest changed", before, after, err)
	}
	newH, newI, newA, _, _ := historyReferenceTranscodeIdentity(m.Segments)
	got, err := readStateDomainChangeBinarySegmentTxRangeByIndexFile(dir, newH, newI, 3, 7)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("indexed query changed", err)
	}
	accessorAfter, err := os.ReadFile(filepath.Join(dir, newA.Path))
	if err != nil || !bytes.Equal(accessorBefore, accessorAfter) {
		t.Fatal("accessor bytes changed", err)
	}
	if newH.Size != results[1].OutputBytes {
		t.Fatal("diagnostic and actual rewrite disagree", newH.Size, results[1].OutputBytes)
	}
}

func TestHistoryCDCMigrationR1Resume(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprint(published), func(t *testing.T) {
			dir, refs, original := cdcR1MigrationFixture(t)
			want, err := DigestColdStateHistoryContext(context.Background(), dir, refs)
			if err != nil {
				t.Fatal(err)
			}
			j, next := cdcPending(t, dir, refs)
			if published {
				if err = PublishManifest(dir, next); err != nil {
					t.Fatal(err)
				}
				if err = os.Remove(filepath.Join(dir, j.Old[0].Path)); err != nil {
					t.Fatal(err)
				}
			}
			result, err := MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{MaxTrios: 1, MinSavingsPercent: 5})
			if err != nil || !result.ResumedJournal || result.JournalPending || result.CompletedTrios != 1 {
				t.Fatal(result, err)
			}
			m, err := LoadProductionManifest(dir)
			if err != nil || m.Generation != original.Generation+1 {
				t.Fatal(m, err)
			}
			got, err := DigestColdStateHistoryContext(context.Background(), dir, m.Segments)
			if err != nil || !reflect.DeepEqual(want, got) {
				t.Fatal("resumed R1 digest changed", err)
			}
		})
	}
}

func TestHistoryCDCMigrationKeepsSmallerR1(t *testing.T) {
	db, _, closeDB := referenceBuildSource(t, "shared")
	dir := t.TempDir()
	refs, err := BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeReadContext(context.Background(), db, dir, 1, 12, 1, 4, "history/state-domain-change-1-12.seg", HistoryReadOptions{ReferenceContainer: true})
	closeDB()
	if err != nil {
		t.Fatal(err)
	}
	if err = PublishManifest(dir, NewManifest(1, 12, refs)); err != nil {
		t.Fatal(err)
	}
	h, _, _, _, _ := historyReferenceTranscodeIdentity(refs)
	results, err := RecompressHistoryDiagnostic(context.Background(), filepath.Join(dir, h.Path), filepath.Join(t.TempDir(), "comparison"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("same-input R1=%d bytes, CDC=%d bytes", h.Size, results[1].OutputBytes)
	if results[1].OutputBytes <= h.Size {
		t.Fatal("fixture must exercise genuinely larger CDC output")
	}
	want, err := DigestColdStateHistoryContext(context.Background(), dir, refs)
	if err != nil {
		t.Fatal(err)
	}

	before := referenceMigrationFiles(t, dir)
	result, err := MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{})
	if err != nil || result.SkippedNoGain != 1 || result.MigratedTrios != 0 {
		t.Fatal(result, err)
	}
	after := referenceMigrationFiles(t, dir)
	delete(after, historyReferenceMigrationLockFile)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("larger CDC output changed published files")
	}
	result, err = MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{R1Only: true, AllowGrowth: true, MinSavingsPercent: 100})
	if err != nil || result.MigratedTrios != 1 || result.ActiveBytesAfter <= result.ActiveBytesBefore {
		t.Fatal("explicit growth conversion failed", result, err)
	}
	m, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	newH, _, _, _, _ := historyReferenceTranscodeIdentity(m.Segments)
	current, err := historyCDCIsCurrent(dir, newH)
	if err != nil || !current || newH.Size != results[1].OutputBytes {
		t.Fatal("growth output is not expected CDC", newH, err)
	}
	got, err := DigestColdStateHistoryContext(context.Background(), dir, m.Segments)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("growth conversion changed logical data", err)
	}

}

func TestHistoryCDCMigrationR1OnlySkipsV2AndResumes(t *testing.T) {
	dir, trios, original := cdcMigrationFixture(t, 2)
	r1, _, err := ReencodeHistoryReferenceTrioContext(context.Background(), dir, trios[1])
	if err != nil {
		t.Fatal(err)
	}
	original.Segments = append(trios[0], r1...)
	if err = PublishManifest(dir, original); err != nil {
		t.Fatal(err)
	}
	result, err := MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{R1Only: true, DryRun: true})
	if err != nil || result.TotalTrios != 1 || result.RemainingTrios != 1 {
		t.Fatal(result, err)
	}
	j, next := cdcPending(t, dir, r1)
	if err = PublishManifest(dir, next); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, j.Old[0].Path)); err != nil {
		t.Fatal(err)
	}
	result, err = MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{R1Only: true})
	if err != nil || !result.ResumedJournal || result.CompletedTrios != 1 || result.TotalTrios != 0 {
		t.Fatal(result, err)
	}
	referenceMigrationAssertOldPresent(t, dir, trios[0])
}

func TestHistoryCDCPendingV2ResumeR1OnlySkipsRewriteAdmission(t *testing.T) {
	dir, trios, _ := cdcMigrationFixture(t, 1)
	_, _ = cdcPending(t, dir, trios[0])
	calls := 0
	opts := HistoryCDCMigrationOptions{R1Only: true, MaxTrios: 1, BeforeTrio: func([]SegmentRef) error {
		calls++
		return errors.New("disk space already consumed by completed output")
	}}
	before := referenceMigrationFiles(t, dir)
	opts.DryRun = true
	result, err := MigrateHistoryCDCContext(context.Background(), dir, opts)
	if err != nil || calls != 0 || !result.JournalPending || result.TotalTrios != 1 || result.RemainingTrios != 1 || !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
		t.Fatal(result, calls, err)
	}
	opts.DryRun = false
	result, err = MigrateHistoryCDCContext(context.Background(), dir, opts)
	if err != nil || calls != 0 || !result.ResumedJournal || result.JournalPending || result.CompletedTrios != 1 || result.RemainingTrios != 0 || result.TotalTrios != 1 || result.ActiveBytesAfter >= result.ActiveBytesBefore {
		t.Fatal(result, calls, err)
	}
}

func TestHistoryCDCFreshRewriteRequiresAdmission(t *testing.T) {
	for _, dry := range []bool{false, true} {
		t.Run(fmt.Sprint(dry), func(t *testing.T) {
			dir, _, _ := cdcMigrationFixture(t, 1)
			diskError := errors.New("not enough rewrite workspace")
			calls := 0
			result, err := MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{DryRun: dry, BeforeTrio: func([]SegmentRef) error { calls++; return diskError }})
			if !errors.Is(err, diskError) || calls != 1 || result.CompletedTrios != 0 || result.JournalPending {
				t.Fatal(result, calls, err)
			}
		})
	}
}
