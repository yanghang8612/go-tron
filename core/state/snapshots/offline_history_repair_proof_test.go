package snapshots

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

func TestOfflineHistoryRepairPartialTargetProofExcludesUnrepairedTail(t *testing.T) {
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
	good := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "cold-prev")
	good.BlockHash = blocks[5].Hash
	// This genuine TARGET tail is absent from old cold. It must make full
	// retirement fail, while a completed first repair slice remains provable.
	tail := binaryStateDomainChange(blocks[600].Number, blocks[600].BeginTxNum, 1, "not-yet-rebuilt")
	tail.BlockHash = blocks[600].Hash
	for _, change := range []*rawdb.StateDomainChange{good, tail} {
		if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
			t.Fatal(err)
		}
	}
	mask := make([]bool, len(blocks))
	for i := 0; i <= 10; i++ {
		mask[i] = true
	}
	originalMask := append([]bool(nil), mask...)
	check := func(needed []bool) ([]rawdb.HistoryStagingColdSpan, error) {
		view, release, err := rawdb.AcquireStateHistoryReadView(db)
		if err != nil {
			return nil, err
		}
		defer release()
		return VerifyHistoryStagingTargetColdRange(context.Background(), view, dir, manifest, blocks, needed)
	}
	spans, err := check(mask)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0].From != blocks[0].Number || spans[0].To != blocks[10].Number || spans[0].RowCount != 1 {
		t.Fatalf("selected proof leaked beyond the repaired interval: %+v", spans)
	}
	if !reflect.DeepEqual(mask, originalMask) {
		t.Fatal("proof changed caller mask")
	}
	full := make([]bool, len(blocks))
	for i := range full {
		full[i] = true
	}
	if _, err := check(full); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("unrepaired tail accepted: %v", err)
	}
	wrong := *good
	wrong.Prev = []byte("wrong previous image")
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&wrong}); err != nil {
		t.Fatal(err)
	}
	if _, err := check(mask); !errors.Is(err, ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("selected Prev mismatch accepted: %v", err)
	}
	zero := make([]bool, len(blocks))
	zero[11] = true
	if spans, err := check(zero); err != nil || len(spans) != 1 || spans[0].From != blocks[11].Number || spans[0].To != blocks[11].Number || spans[0].RowCount != 0 {
		t.Fatalf("zero-change selected block not proved exactly: %+v %v", spans, err)
	}
}

func TestOfflineHistoryRepairResumeBoundaryRejectsChangedPrev(t *testing.T) {
	dir := t.TempDir()
	changes := v6StreamChanges(1, 6, 1, 50)
	old := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 6, changes)
	plan, err := PlanOfflineHistoryRepair(NewManifest(1, 6, old), 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	var refs []SegmentRef
	for _, span := range [][2]uint64{{1, 2}, {3, 4}, {5, 6}} {
		part, err := CopyStateHistoryReferenceTrioRangeContext(context.Background(), dir, old, span[0], span[1], fmtRepairTestPath(int(span[0])), etl.Options{})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, part...)
	}
	ctx, facts, err := WithHistoryStagingPhysicalFacts(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := AuthenticateOfflineHistoryRepairTrios(ctx, dir, refs); err != nil {
		t.Fatal(err)
	}
	if err := VerifyOfflineHistoryRepairBoundaryCopies(ctx, dir, plan, refs); err != nil {
		t.Fatal(err)
	}
	wrong := *changes[5]
	wrong.Prev = []byte("different valid right boundary bytes")
	badRight := writeV6StateDomainHistorySegmentForTest(t, dir, 5, 6, []*rawdb.StateDomainChange{changes[4], &wrong})
	badRefs := append(append([]SegmentRef(nil), refs[:6]...), badRight...)
	if err := VerifyOfflineHistoryRepairBoundaryCopies(ctx, dir, plan, badRefs); err == nil {
		t.Fatal("well-formed but changed boundary Prev accepted on resume")
	}
	// Full authentication enrolled the right boundary even without any route
	// receipt. Replacing it is detected by the final no-SHA metadata check.
	h := referenceBuildHistoryRef(t, refs[6:])
	path := filepath.Join(dir, h.Path)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := facts.RecheckAll(ctx); err == nil {
		t.Fatal("unbound right boundary replacement escaped physical collector")
	}
}

func TestOfflineHistoryRepairNarrowRebindSkipsOtherRetiredDependencies(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	changes := v6StreamChanges(1024, 1024, 1, 40)
	for _, change := range changes {
		change.BlockHash = common.Hash{1, byte(change.BlockNum >> 8), byte(change.BlockNum)}
	}
	oldRefs := writeV6StateDomainHistorySegmentForTest(t, dir, 1024, 2047, changes)
	oldManifest := NewManifest(1024, 2047, oldRefs)
	oldSegment, err := OpenStateDomainChangeSegment(dir, referenceBuildHistoryRef(t, oldRefs))
	if err != nil {
		t.Fatal(err)
	}
	blocks := make([]rawdb.HistoryStagingBlockProof, len(oldSegment.TxRanges))
	for i, row := range oldSegment.TxRanges {
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: row.BlockNum, Hash: row.BlockHash, BeginTxNum: row.BeginTxNum, EndTxNum: row.EndTxNum}
	}
	if len(blocks) != 1024 {
		t.Fatal("fixture does not cover complete bucket")
	}
	root := t.TempDir()
	hot, err := rawdb.NewPebbleDB(filepath.Join(root, "hot"), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(filepath.Join(root, "stage"), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{Version: 1, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.InitializeOfflineSourceRoutes(ctx, 1, 1, 2); err != nil {
		t.Fatal(err)
	}
	for _, row := range oldSegment.TxRanges {
		if err := rawdb.WriteStateTxRange(hot, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			t.Fatal(err)
		}
	}
	mask := make([]bool, len(blocks))
	for i := range mask {
		mask[i] = true
	}
	spans, err := BuildHistoryStagingColdSpans(ctx, dir, oldManifest, blocks, mask)
	if err != nil {
		t.Fatal(err)
	}
	oldBinding := rawdb.HistoryStagingColdBinding{Version: 1, Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1, Spans: spans}
	if err := manager.CertifyColdRange(ctx, oldBinding, func() error { return VerifyHistoryStagingColdBinding(ctx, dir, oldManifest, oldBinding, blocks) }); err != nil {
		t.Fatal(err)
	}
	newRefs, err := CopyStateHistoryReferenceTrioRangeContext(ctx, dir, oldRefs, 1024, 2047, "history/state-domain-change-rebound.seg", etl.Options{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanOfflineHistoryRepair(oldManifest, 1024, 2047)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := PrepareOfflineHistoryRepairManifest(oldManifest, plan, newRefs, 10)
	if err != nil {
		t.Fatal(err)
	}
	// An unrelated retired trio has a durable reverse reference but is absent
	// on disk. A global reconcile would attempt it and fail. This repair must
	// touch only the selected old trio ID, leaving the unrelated binding exact.
	fake := append([]SegmentRef(nil), plan.SourceRefs...)
	for i := range fake {
		fake[i].FromTxNum, fake[i].ToTxNum = 2048, 3071
		fake[i].Path = "history/state-domain-change-unrelated-retired" + filepath.Ext(fake[i].Path)
		fake[i].Checksum = "sha256:0101010101010101010101010101010101010101010101010101010101010101"
	}
	fh, fi, fa, _, err := historyReferenceTranscodeIdentity(fake)
	if err != nil {
		t.Fatal(err)
	}
	fakeID, err := historyStagingTrioID([3]SegmentRef{fh, fi, fa})
	if err != nil {
		t.Fatal(err)
	}
	unrelated := rawdb.HistoryStagingColdBinding{Version: 1, Bucket: 2, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1, Spans: []rawdb.HistoryStagingColdSpan{{From: 2048, To: 3071, ContentID: fakeID, SemanticHash: [32]byte{1}, TxRangeDigest: [32]byte{2}}}}
	if err := manager.CertifyColdRange(ctx, unrelated, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	candidate.Retired = append(candidate.Retired, fake...)
	if err := PublishManifest(dir, candidate); err != nil {
		t.Fatal(err)
	}
	proofCtx, facts, err := WithHistoryStagingPhysicalFacts(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	guardErr := errors.New("guard unavailable")
	if err := OfflineRebindHistoryStagingRepair(proofCtx, dir, manager, candidate, plan.SourceRefs, func() error { return guardErr }); !errors.Is(err, guardErr) {
		t.Fatal(err)
	}
	if actual, _, err := manager.ReadColdBindingAt(1, 1); err != nil || !reflect.DeepEqual(actual, oldBinding) {
		t.Fatal("failed guard modified binding", err)
	}
	before := make(map[string][32]byte)
	for _, ref := range oldRefs {
		raw, err := os.ReadFile(filepath.Join(dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		before[ref.Path] = sha256.Sum256(raw)
	}
	if err := OfflineRebindHistoryStagingRepair(proofCtx, dir, manager, candidate, plan.SourceRefs, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	actual, present, err := manager.ReadColdBindingAt(1, 1)
	if err != nil || !present || actual.BindingEpoch != 2 || actual.ManifestEpoch != candidate.Generation || actual.Spans[0].ContentID == oldBinding.Spans[0].ContentID {
		t.Fatalf("rebind not durable: %+v %v", actual, err)
	}
	if other, _, err := manager.ReadColdBindingAt(1, 2); err != nil || !reflect.DeepEqual(other, unrelated) {
		t.Fatal("unrelated retired dependency changed", err)
	}
	if err := OfflineRebindHistoryStagingRepair(proofCtx, dir, manager, candidate, plan.SourceRefs, func() error { return nil }); err != nil {
		t.Fatal("retry", err)
	}
	if retry, _, _ := manager.ReadColdBindingAt(1, 1); !reflect.DeepEqual(retry, actual) {
		t.Fatal("retry changed completed binding")
	}
	if err := facts.RecheckAll(ctx); err != nil {
		t.Fatal(err)
	}
	for path, hash := range before {
		raw, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil || sha256.Sum256(raw) != hash {
			t.Fatal("old cold file changed", err)
		}
	}
	if _, bound := historyStagingRetentionFor(dir); bound {
		t.Fatal("offline repair registered global retention")
	}
}

func TestOfflineHistoryRepairPartialTargetProofRejectsInvalidMask(t *testing.T) {
	dir, manifest, blocks := historyStagingProofFixture(t)
	for _, mask := range [][]bool{nil, make([]bool, len(blocks)-1), make([]bool, len(blocks))} {
		if _, err := VerifyHistoryStagingTargetColdRange(context.Background(), nil, dir, manifest, blocks, mask); err == nil {
			t.Fatal("invalid/empty mask accepted")
		}
	}
	mask := make([]bool, len(blocks))
	mask[0] = true
	if _, err := VerifyHistoryStagingTargetColdRange(context.Background(), nil, dir, manifest, blocks[:len(blocks)-1], mask); err == nil {
		t.Fatal("partial canonical bucket accepted")
	}
	if _, err := VerifyHistoryStagingTargetColdRange(context.Background(), nil, dir, manifest, blocks, mask); err == nil {
		t.Fatal("unpinned source accepted")
	}
}
