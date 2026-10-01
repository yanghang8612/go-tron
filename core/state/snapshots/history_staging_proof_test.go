package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func historyStagingProofFixture(t *testing.T) (string, *Manifest, []rawdb.HistoryStagingBlockProof) {
	t.Helper()
	dir := t.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range blocks {
		number := uint64(i + 1024)
		hash := common.Hash{byte(number >> 8), byte(number)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash,
			BeginTxNum: number, EndTxNum: number}
		ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash,
			BeginTxNum: number, EndTxNum: number}
	}
	change := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "cold-prev")
	change.BlockHash = blocks[5].Hash
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum,
		Path: stateDomainChangeHistorySegmentPath(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum)}
	history, index, accessor, err := writeHistorySegmentFiles(dir, ref,
		[]*rawdb.StateDomainChange{change}, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifestForChain(ref.FromTxNum, ref.ToTxNum,
		[]SegmentRef{history, index, accessor}, ChainIdentity{
			ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	if err := manifest.ValidateProduction(); err != nil {
		t.Fatal(err)
	}
	return dir, manifest, blocks
}

func TestHistoryStagingTargetColdEquivalenceChecksPrevAndZeroBlocks(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, block := range blocks {
		if err := rawdb.WriteStateTxRange(db, block.Number, block.Hash, block.BeginTxNum, block.EndTxNum); err != nil {
			t.Fatal(err)
		}
	}
	check := func() error {
		view, release, err := rawdb.AcquireStateHistoryReadView(db)
		if err != nil {
			return err
		}
		defer release()
		_, err = VerifyHistoryStagingTargetColdEquivalence(context.Background(), view, dir, manifest, blocks)
		return err
	}
	if err := check(); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("missing target changeset = %v, want semantic mismatch", err)
	}
	change := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "cold-prev")
	change.BlockHash = blocks[5].Hash
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
		t.Fatal(err)
	}
	if err := check(); err != nil {
		t.Fatalf("matching target and 1023 zero-change blocks: %v", err)
	}
	changed := *change
	changed.Prev = []byte("different-prev")
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&changed}); err != nil {
		t.Fatal(err)
	}
	if err := check(); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("changed Prev = %v, want semantic mismatch", err)
	}
}

func TestHistoryStagingMixedTargetColdEquivalence(t *testing.T) {
	dir := t.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range blocks {
		number := uint64(i + 1024)
		hash := common.Hash{byte(number >> 8), byte(number)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash,
			BeginTxNum: number, EndTxNum: number}
		ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash,
			BeginTxNum: number, EndTxNum: number}
	}
	oldChange := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "old-cold")
	newChange := binaryStateDomainChange(blocks[600].Number, blocks[600].BeginTxNum, 1, "new-cold")
	oldChange.BlockHash, newChange.BlockHash = blocks[5].Hash, blocks[600].Hash
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum,
		Path: stateDomainChangeHistorySegmentPath(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum)}
	history, index, accessor, err := writeHistorySegmentFiles(dir, ref,
		[]*rawdb.StateDomainChange{oldChange, newChange}, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifestForChain(ref.FromTxNum, ref.ToTxNum,
		[]SegmentRef{history, index, accessor}, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	oldMask := make([]bool, len(blocks))
	for i := 0; i <= 10; i++ {
		oldMask[i] = true
	}
	oldSpans, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, oldMask)
	if err != nil {
		t.Fatal(err)
	}
	old := rawdb.HistoryStagingColdBinding{Version: 1, Bucket: 1, Epoch: 1, BindingEpoch: 1,
		ManifestEpoch: manifest.Generation, Spans: oldSpans}
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, block := range blocks {
		if err := rawdb.WriteStateTxRange(db, block.Number, block.Hash, block.BeginTxNum, block.EndTxNum); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{newChange}); err != nil {
		t.Fatal(err)
	}
	check := func(binding rawdb.HistoryStagingColdBinding) ([]rawdb.HistoryStagingColdSpan, error) {
		view, release, err := rawdb.AcquireStateHistoryReadView(db)
		if err != nil {
			return nil, err
		}
		defer release()
		return VerifyHistoryStagingMixedTargetColdEquivalence(context.Background(), view, dir, manifest, blocks, binding)
	}
	full, err := check(old)
	if err != nil || len(full) != 1 || full[0].From != blocks[0].Number || full[0].To != blocks[len(blocks)-1].Number || full[0].RowCount != 2 {
		t.Fatalf("mixed extension = %+v, %v", full, err)
	}
	wrongOld := old
	wrongOld.Spans = append([]rawdb.HistoryStagingColdSpan(nil), old.Spans...)
	wrongOld.Spans[0].SemanticHash[0] ^= 1
	if _, err := check(wrongOld); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("wrong pruned prefix = %v, want semantic mismatch", err)
	}
	changed := *newChange
	changed.Prev = []byte("changed-Prev")
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&changed}); err != nil {
		t.Fatal(err)
	}
	if _, err := check(old); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("changed hot suffix = %v, want semantic mismatch", err)
	}
	if err := rawdb.DeleteStateDomainChanges(db, blocks[600].Number); err != nil {
		t.Fatal(err)
	}
	if _, err := check(old); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("missing hot suffix = %v, want semantic mismatch", err)
	}
}

func TestHistoryStagingSemanticDigestBindsSameTxRepairOrder(t *testing.T) {
	dir, _, blocks := historyStagingProofFixture(t)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i, block := range blocks {
		ranges[i] = &rawdb.StateTxRange{BlockNum: block.Number, BlockHash: block.Hash,
			BeginTxNum: block.BeginTxNum, EndTxNum: block.EndTxNum}
	}
	a := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "same-key")
	a.BlockHash = blocks[5].Hash
	b := *a
	b.Seq = 2
	b.Prev = []byte("second-prev")
	c := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 3, "other-key")
	c.BlockHash = blocks[5].Hash
	c.Domain = kvdomains.SystemDelegation
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: blocks[0].BeginTxNum, ToTxNum: blocks[len(blocks)-1].EndTxNum,
		Path: stateDomainChangeHistorySegmentPath(blocks[0].BeginTxNum, blocks[len(blocks)-1].EndTxNum)}
	history, index, accessor, err := writeHistorySegmentFiles(dir, ref,
		[]*rawdb.StateDomainChange{a, &b, c}, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifestForChain(ref.FromTxNum, ref.ToTxNum,
		[]SegmentRef{history, index, accessor}, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"})
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, block := range blocks {
		if err := rawdb.WriteStateTxRange(db, block.Number, block.Hash, block.BeginTxNum, block.EndTxNum); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{a, &b, c}); err != nil {
		t.Fatal(err)
	}
	check := func() error {
		view, release, err := rawdb.AcquireStateHistoryReadView(db)
		if err != nil {
			return err
		}
		defer release()
		_, err = VerifyHistoryStagingTargetColdEquivalence(context.Background(), view, dir, manifest, blocks)
		return err
	}
	if err := check(); err != nil {
		t.Fatalf("same TxNum/Key ordered repairs: %v", err)
	}
	swappedA, swappedB := *a, b
	swappedA.Prev, swappedB.Prev = b.Prev, a.Prev
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&swappedA, &swappedB, c}); err != nil {
		t.Fatal(err)
	}
	if err := check(); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("same TxNum/Key swapped repair order = %v, want mismatch", err)
	}
}

func TestHistoryStagingColdProofMissingSubrangeAndZeroChange(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	needed := make([]bool, len(blocks))
	needed[4], needed[5], needed[6] = true, true, true
	prover, err := NewHistoryStagingColdProver(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	spans, err := prover.Build(context.Background(), blocks, needed)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0].From != blocks[4].Number ||
		spans[0].To != blocks[6].Number || spans[0].RowCount != 1 ||
		spans[0].SemanticHash == ([32]byte{}) || spans[0].ContentID == ([32]byte{}) {
		t.Fatalf("invalid certified cold span: %+v", spans)
	}
	spansAgain, err := prover.Build(context.Background(), blocks, needed)
	if err != nil || len(spansAgain) != 1 || spansAgain[0] != spans[0] {
		t.Fatalf("cached proof changed: %+v, %v", spansAgain, err)
	}
	binding := rawdb.HistoryStagingColdBinding{Bucket: 1, Spans: spans}
	if err := VerifyHistoryStagingColdBinding(context.Background(), dir, manifest, binding, blocks); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryStagingPinnedBindingCacheHitAndFileReplacement(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	needed := make([]bool, len(blocks))
	needed[4], needed[5], needed[6] = true, true, true
	spans, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, needed)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := OpenPinnedManager(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion,
		Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: manifest.Generation, Spans: spans}
	if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	loads := 0
	load := func() ([]rawdb.HistoryStagingBlockProof, error) { loads++; return blocks, nil }
	for i := 0; i < 2; i++ {
		if err := pinned.VerifyHistoryStagingPinnedBinding(context.Background(), binding, load); err != nil {
			t.Fatal(err)
		}
	}
	if loads != 1 {
		t.Fatalf("cached proof loaded canonical txranges %d times", loads)
	}
	// A later manifest can append an unrelated immutable trio without forcing
	// every older binding to rebind or redo semantic scans.
	nextRanges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range nextRanges {
		number := uint64(2048 + i)
		nextRanges[i] = &rawdb.StateTxRange{BlockNum: number,
			BlockHash:  common.Hash{byte(number >> 8), byte(number)},
			BeginTxNum: number, EndTxNum: number}
	}
	ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
		FromTxNum: 2048, ToTxNum: 3071, Path: stateDomainChangeHistorySegmentPath(2048, 3071)}
	newHistory, newIndex, newAccessor, err := writeHistorySegmentFiles(dir, ref, nil, nextRanges)
	if err != nil {
		t.Fatal(err)
	}
	later := *manifest
	later.Generation++
	later.Segments = append(append([]SegmentRef(nil), manifest.Segments...), newHistory, newIndex, newAccessor)
	later.VisibleTxEnd = 3071
	laterPinned, err := OpenPinnedManager(dir, &later)
	if err != nil {
		t.Fatal(err)
	}
	if err := laterPinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
	if err := laterPinned.VerifyHistoryStagingPinnedBinding(context.Background(), binding, load); err != nil {
		t.Fatal(err)
	}
	if loads != 1 {
		t.Fatalf("unrelated trio append reloaded old canonical proofs %d times", loads)
	}
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentHistory {
			continue
		}
		path := filepath.Join(dir, ref.Path)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		data[8] ^= 0xff
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		break
	}
	if err := pinned.VerifyHistoryStagingPinnedBinding(context.Background(), binding, load); err == nil {
		t.Fatal("cached semantic proof accepted replaced/corrupt trio")
	}
	if err := pinned.VerifyHistoryStagingPinnedBindingReceipt(context.Background(), binding); err == nil {
		t.Fatal("receipt checksum cache accepted replaced/corrupt trio")
	}
}

func TestHistoryStagingColdProofRejectsWrongCanonicalAndIncompleteMask(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	needed := make([]bool, len(blocks))
	needed[5] = true
	blocks[5].Hash = common.Hash{0xff}
	if _, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, needed); err == nil {
		t.Fatal("mismatched canonical hash accepted")
	}
	blocks[5].Hash = common.Hash{byte(blocks[5].Number >> 8), byte(blocks[5].Number)}
	if _, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, needed[:10]); err == nil {
		t.Fatal("short cold mask accepted")
	}
}

func TestHistoryStagingColdProofRejectsTamperedTrio(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	needed := make([]bool, len(blocks))
	needed[5] = true
	for _, ref := range manifest.Segments {
		if ref.Kind != SegmentHistory {
			continue
		}
		path := filepath.Join(dir, ref.Path)
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteAt([]byte{0xff}, 8); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		break
	}
	if _, err := BuildHistoryStagingColdSpans(context.Background(), dir, manifest, blocks, needed); err == nil {
		t.Fatal("tampered history file was accepted")
	}
}

func TestHistoryStagingColdRebindTwoTriosToOne(t *testing.T) {
	dir := t.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range blocks {
		number := uint64(i + 1024)
		hash := common.Hash{byte(number >> 8), byte(number)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: number, Hash: hash,
			BeginTxNum: number, EndTxNum: number}
		ranges[i] = &rawdb.StateTxRange{BlockNum: number, BlockHash: hash,
			BeginTxNum: number, EndTxNum: number}
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "early"),
		binaryStateDomainChange(blocks[600].Number, blocks[600].BeginTxNum, 1, "late"),
	}
	for _, change := range changes {
		change.BlockHash = blocks[int(change.BlockNum-1024)].Hash
	}
	write := func(start, end int, changes []*rawdb.StateDomainChange) []SegmentRef {
		t.Helper()
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: blocks[start].BeginTxNum, ToTxNum: blocks[end-1].EndTxNum,
			Path: stateDomainChangeHistorySegmentPath(blocks[start].BeginTxNum, blocks[end-1].EndTxNum)}
		history, index, accessor, err := writeHistorySegmentFiles(dir, ref, changes, ranges[start:end])
		if err != nil {
			t.Fatal(err)
		}
		return []SegmentRef{history, index, accessor}
	}
	oldRefs := append(write(0, 512, changes[:1]), write(512, 1024, changes[1:])...)
	chain := ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: "0x01"}
	oldManifest := NewManifestForChain(blocks[0].BeginTxNum, blocks[1023].EndTxNum, oldRefs, chain)
	mask := make([]bool, len(blocks))
	for i := range mask {
		mask[i] = true
	}
	oldSpans, err := BuildHistoryStagingColdSpans(context.Background(), dir, oldManifest, blocks, mask)
	if err != nil || len(oldSpans) != 2 {
		t.Fatalf("old spans = %+v, %v", oldSpans, err)
	}
	mergedRefs := write(0, 1024, changes)
	merged := NewManifestForChain(blocks[0].BeginTxNum, blocks[1023].EndTxNum, mergedRefs, chain)
	binding := rawdb.HistoryStagingColdBinding{Version: 1, Bucket: 1, Epoch: 1,
		BindingEpoch: 1, Spans: oldSpans}
	updated, err := RebindHistoryStagingColdBinding(context.Background(), dir, merged, binding, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if updated.BindingEpoch != 2 || len(updated.Spans) != 2 ||
		updated.Spans[0].ContentID != updated.Spans[1].ContentID ||
		updated.Spans[0].ContentID == oldSpans[0].ContentID {
		t.Fatalf("merged binding incorrect: %+v", updated)
	}
	if err := VerifyHistoryStagingColdBinding(context.Background(), dir, merged, updated, blocks); err != nil {
		t.Fatalf("merged binding kept certified subrange boundaries: %v", err)
	}
	changed := *changes[1]
	changed.Prev = []byte("different logical history")
	badRefs := write(0, 1024, []*rawdb.StateDomainChange{changes[0], &changed})
	badManifest := NewManifestForChain(blocks[0].BeginTxNum, blocks[1023].EndTxNum, badRefs, chain)
	if _, err := RebindHistoryStagingColdBinding(context.Background(), dir, badManifest, binding, blocks); err == nil {
		t.Fatal("content-changed merge passed semantic rebind")
	}
}
