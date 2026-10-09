package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

func TestOfflineHistoryRepairCopyPreservesColdRange(t *testing.T) {
	for _, mode := range []string{"raw", "codec3", "r1"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			changes := v6StreamChanges(1, 12, 3, 100)
			changes[3].Prev = bytes.Repeat([]byte("large streamed previous image"), 100000)
			changes[5].TxNum = changes[4].TxNum
			refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 12, changes)
			if mode == "codec3" {
				t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "3")
				compressV6StreamFixture(t, dir, refs, 4096)
			}
			if mode == "r1" {
				var err error
				refs, _, err = ReencodeHistoryReferenceTrioContext(context.Background(), dir, refs)
				if err != nil {
					t.Fatal(err)
				}
			}
			oldHistory := referenceBuildHistoryRef(t, refs)
			old, err := OpenStateDomainChangeSegment(dir, oldHistory)
			if err != nil {
				t.Fatal(err)
			}
			before := make(map[string][32]byte)
			for _, ref := range refs {
				raw, err := os.ReadFile(filepath.Join(dir, ref.Path))
				if err != nil {
					t.Fatal(err)
				}
				before[ref.Path] = sha256.Sum256(raw)
			}
			out, err := CopyStateHistoryReferenceTrioRangeContext(context.Background(), dir, refs, 4, 9, "history/state-domain-change-repair-slice-4-9.seg", etl.Options{})
			if err != nil {
				t.Fatal(err)
			}
			h := referenceBuildHistoryRef(t, out)
			raw, err := os.ReadFile(filepath.Join(dir, h.Path))
			if err != nil || string(raw[:8]) != historyReferenceMagic {
				t.Fatal("output is not self-contained reference container", err)
			}
			got, err := OpenStateDomainChangeSegment(dir, h)
			if err != nil {
				t.Fatal(err)
			}
			var row int
			for _, want := range old.Changes {
				if want.TxNum < 4 || want.TxNum > 9 {
					continue
				}
				if row >= len(got.Changes) {
					t.Fatal("lost row")
				}
				actual := got.Changes[row]
				wantCopy := *want
				actualCopy := *actual
				wantCopy.Seq, actualCopy.Seq = 0, 0
				if !reflect.DeepEqual(wantCopy, actualCopy) {
					t.Fatalf("row %d changed metadata or Prev", row)
				}
				row++
			}
			if row != len(got.Changes) {
				t.Fatal("extra rows")
			}
			var expectedRanges int
			for _, want := range old.TxRanges {
				if want.EndTxNum < 4 || want.BeginTxNum > 9 {
					continue
				}
				if expectedRanges >= len(got.TxRanges) || *got.TxRanges[expectedRanges] != *want {
					t.Fatal("canonical block range changed")
				}
				expectedRanges++
			}
			if expectedRanges != len(got.TxRanges) {
				t.Fatal("extra canonical block ranges")
			}
			for path, hash := range before {
				raw, err := os.ReadFile(filepath.Join(dir, path))
				if err != nil || sha256.Sum256(raw) != hash {
					t.Fatal("old source changed", path, err)
				}
			}
			// Removing the complete old trio cannot break the new immutable output.
			for path := range before {
				if err := os.Remove(filepath.Join(dir, path)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := OpenStateDomainChangeSegment(dir, h); err != nil {
				t.Fatal("output depends on source", err)
			}
		})
	}
}

func TestOfflineHistoryRepairBoundaryPartitionAndResumedDigest(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "3")
	dir := t.TempDir()
	changes := v6StreamChanges(1, 300, 1, 50)
	for _, change := range changes {
		change.BlockHash[1] = 1 // The shared fixture's one-byte hash wraps at block 256.
	}
	refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 300, changes)
	compressV6StreamFixture(t, dir, refs, 4096)
	ctx := context.Background()
	parts, err := PlanOfflineHistoryRepairBoundaryBlockSlices(ctx, dir, refs, 1, 300, 128)
	if err != nil {
		t.Fatal(err)
	}
	want := []OfflineHistoryRepairBlockSlice{
		{FromTxNum: 1, ToTxNum: 128, FromBlock: 1, ToBlock: 128},
		{FromTxNum: 129, ToTxNum: 256, FromBlock: 129, ToBlock: 256},
		{FromTxNum: 257, ToTxNum: 300, FromBlock: 257, ToBlock: 300},
	}
	if !reflect.DeepEqual(parts, want) {
		t.Fatalf("boundary slices=%+v, want %+v", parts, want)
	}
	var replacements []SegmentRef
	for i, part := range parts {
		copied, err := CopyStateHistoryReferenceTrioRangeContext(ctx, dir, refs, part.FromTxNum, part.ToTxNum, fmtRepairTestPath(i), etl.Options{})
		if err != nil {
			t.Fatal(err)
		}
		replacements = append(replacements, copied...)
	}
	plan := &OfflineHistoryRepairPlan{Left: &OfflineHistoryRepairSlice{SourceRefs: refs, FromTxNum: 1, ToTxNum: 300}}
	if err := VerifyOfflineHistoryRepairBoundaryCopies(ctx, dir, plan, replacements); err != nil {
		t.Fatal("multi-trio resume digest rejected exact copy", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := PlanOfflineHistoryRepairBoundaryBlockSlices(canceled, dir, refs, 1, 300, 128); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled partition returned %v", err)
	}
	path := filepath.Join(dir, replacements[0].Path)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyOfflineHistoryRepairBoundaryCopies(ctx, dir, plan, replacements); err == nil {
		t.Fatal("tampered boundary candidate passed resumed digest")
	}
}

func TestOfflineHistoryRepairCopyRejectsUnsafeInput(t *testing.T) {
	dir := t.TempDir()
	refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 9, v6StreamChanges(1, 9, 3, 100))
	for _, r := range [][2]uint64{{0, 4}, {2, 10}, {6, 5}, {2, 9}, {1, 8}} {
		if _, err := CopyStateHistoryReferenceTrioRangeContext(context.Background(), dir, refs, r[0], r[1], "history/state-domain-change-unsafe.seg", etl.Options{}); err == nil {
			t.Fatal("bad range accepted", r)
		}
	}
	if _, err := CopyStateHistoryReferenceTrioRangeContext(context.Background(), dir, refs, 2, 4, refs[0].Path, etl.Options{}); err == nil {
		t.Fatal("source path overwrite accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CopyStateHistoryReferenceTrioRangeContext(ctx, dir, refs, 2, 4, "history/state-domain-change-cancel.seg", etl.Options{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	path := filepath.Join(dir, refs[0].Path)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := CopyStateHistoryReferenceTrioRangeContext(context.Background(), dir, refs, 2, 4, "history/state-domain-change-corrupt.seg", etl.Options{}); err == nil {
		t.Fatal("corrupt source accepted")
	}
}

func TestOfflineHistoryRepairPlanExactReplacement(t *testing.T) {
	dir := t.TempDir()
	var old []SegmentRef
	for _, r := range [][2]uint64{{1, 6}, {7, 12}, {13, 18}} {
		old = append(old, writeV6StateDomainHistorySegmentForTest(t, dir, r[0], r[1], v6StreamChanges(r[0], r[1]-r[0]+1, 1, 50))...)
	}
	m := NewManifest(1, 18, old)
	m.Generation = 8
	m.PublishedUnix = 100
	m.Progress = &Progress{HistoryBuildTxNum: 18, HotPruneTxNum: 17, HotPruneBlockNum: 5}
	m.HistoryStagingResetEpoch = 3
	plan, err := PlanOfflineHistoryRepair(m, 8, 11)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.SourceRefs) != 3 || plan.ReplaceFromTxNum != 7 || plan.ReplaceToTxNum != 12 || plan.Left.FromTxNum != 7 || plan.Left.ToTxNum != 7 || plan.Right.FromTxNum != 12 || plan.Right.ToTxNum != 12 {
		t.Fatalf("bad bounded plan %+v", plan)
	}
	var replacement []SegmentRef
	for i, r := range [][2]uint64{{7, 7}, {8, 9}, {10, 11}, {12, 12}} {
		refs, err := CopyStateHistoryReferenceTrioRangeContext(context.Background(), dir, plan.SourceRefs, r[0], r[1], fmtRepairTestPath(i), etl.Options{})
		if err != nil {
			t.Fatal(err)
		}
		replacement = append(replacement, refs...)
	}
	before := cloneManifest(m)
	next, err := PrepareOfflineHistoryRepairManifest(m, plan, replacement, 101)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, m) {
		t.Fatal("prepare mutated current manifest")
	}
	if next.Generation != 9 || next.PublishedUnix != 101 || !reflect.DeepEqual(next.Progress, m.Progress) || next.HistoryStagingResetEpoch != m.HistoryStagingResetEpoch || len(next.Retired) != 3 {
		t.Fatal("publication changed protected metadata")
	}
	for _, ref := range append(append([]SegmentRef(nil), old[:3]...), old[6:]...) {
		found := false
		for _, got := range next.Segments {
			found = found || got == ref
		}
		if !found {
			t.Fatal("unrelated active ref changed")
		}
	}
	for _, bad := range [][]SegmentRef{replacement[3:], replacement[:len(replacement)-3], append(append([]SegmentRef(nil), replacement...), replacement[:3]...)} {
		if _, err := PrepareOfflineHistoryRepairManifest(m, plan, bad, 101); err == nil {
			t.Fatal("gap/overlap accepted")
		}
	}
	changed := *plan
	changed.ReplaceToTxNum--
	if _, err := PrepareOfflineHistoryRepairManifest(m, &changed, replacement, 101); err == nil {
		t.Fatal("stale plan accepted")
	}
	canonical, err := json.Marshal(next)
	if err != nil {
		t.Fatal(err)
	}
	if err = PublishManifest(dir, next); err != nil {
		t.Fatal(err)
	}
	published, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil || !bytes.Equal(published, canonical) {
		t.Fatal("candidate SHA cannot be fixed before publication", err)
	}
}

func TestOfflineHistoryRepairCopyEmptyCanonicalBlock(t *testing.T) {
	dir := t.TempDir()
	from, to := uint64(1), uint64(6)
	ranges := []*rawdb.StateTxRange{{BlockNum: 10, BlockHash: common.HexToHash("01"), BeginTxNum: 1, EndTxNum: 3}, {BlockNum: 11, BlockHash: common.HexToHash("02"), BeginTxNum: 4, EndTxNum: 6}}
	segData, index, accData, err := encodeStateDomainChangeBinarySegmentV6(from, to, nil, ranges)
	if err != nil {
		t.Fatal(err)
	}
	idxData, err := encodeStateDomainChangeBinaryIndex(from, to, index)
	if err != nil {
		t.Fatal(err)
	}
	h := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: from, ToTxNum: to, Path: "history/state-domain-change-empty.seg"}
	setStateDomainChangeBinaryRefMetadata(&h, segData)
	h.Path = contentAddressedSnapshotPath(h.Path, h.Checksum)
	i, a := h, h
	i.Kind, i.Path = SegmentInverted, stateDomainChangeBinaryIndexPath(h.Path)
	a.Kind, a.Path = SegmentAccessor, stateDomainChangeBinaryAccessorPath(h.Path)
	setStateDomainChangeBinaryRefMetadata(&i, idxData)
	setStateDomainChangeBinaryRefMetadata(&a, accData)
	refs := []SegmentRef{h, i, a}
	for n, data := range [][]byte{segData, idxData, accData} {
		if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, refs[n].Path), data); err != nil {
			t.Fatal(err)
		}
	}
	out, err := CopyStateHistoryReferenceTrioRangeContext(context.Background(), dir, refs, 4, 6, "history/state-domain-change-empty-copy.seg", etl.Options{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := OpenStateDomainChangeSegment(dir, referenceBuildHistoryRef(t, out))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Changes) != 0 || len(got.TxRanges) != 1 || *got.TxRanges[0] != *ranges[1] {
		t.Fatal("empty canonical block lost")
	}
}

func fmtRepairTestPath(i int) string {
	return "history/state-domain-change-repair-part-" + string(rune('a'+i)) + ".seg"
}
