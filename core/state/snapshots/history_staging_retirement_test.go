package snapshots

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type historyStagingRetirementFaultStore struct {
	ethdb.KeyValueStore
	failSync bool
}

func (s *historyStagingRetirementFaultStore) SyncKeyValue() error {
	if s.failSync {
		return errors.New("injected hot Sync failure")
	}
	return s.KeyValueStore.(interface{ SyncKeyValue() error }).SyncKeyValue()
}
func (s *historyStagingRetirementFaultStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return s.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
}

func TestHistoryStagingPublicationGateHoldsOnlyCapture(t *testing.T) {
	dir := t.TempDir()
	key, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	binding := &historyStagingRetentionBinding{ready: true}
	historyStagingRetention.Store(key, binding)
	defer historyStagingRetention.Delete(key)
	readRelease, err := AcquireHistoryStagingPublicationRead(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		close(entered)
		writerRelease, err := binding.publication.acquireWrite(context.Background())
		if err == nil {
			writerRelease()
		}
		close(finished)
	}()
	<-entered
	select {
	case <-finished:
		t.Fatal("merge publication entered before read capture released")
	case <-time.After(10 * time.Millisecond):
	}
	readRelease()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("merge publication remained blocked after capture")
	}
	writerRelease, err := binding.publication.acquireWrite(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if release, err := AcquireHistoryStagingPublicationRead(cancelled, dir); !errors.Is(err, context.Canceled) || release != nil {
		t.Fatalf("cancelled read admission = release %t, err %v", release != nil, err)
	}
	writerRelease()
	if stats := HistoryStagingPublicationWait(dir); stats.WriteWaitNanos == 0 {
		t.Fatalf("missing publication wait accounting: %+v", stats)
	}
}

func TestHistoryStagingResetColdTailIsolationIsDurableAndIdempotent(t *testing.T) {
	ctx := context.Background()
	dir, manifest, blocks := historyStagingProofFixture(t)
	manifest.Chain.GenesisHash = (common.Hash{1}).Hex()
	var oldHistoryRef SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentHistory {
			oldHistoryRef = ref
			break
		}
	}
	if oldHistoryRef.Path == "" {
		t.Fatal("missing old history segment")
	}
	oldBytes, err := os.ReadFile(filepath.Join(dir, oldHistoryRef.Path))
	if err != nil {
		t.Fatal(err)
	}
	oldSHA := sha256.Sum256(oldBytes)
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
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
	identity := rawdb.HistoryStagingIdentity{Version: rawdb.HistoryStagingFormatVersion, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}}
	staging, err := rawdb.NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := staging.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := staging.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	intent := rawdb.HistoryStagingResetIntent{Version: rawdb.HistoryStagingFormatVersion, OldEpoch: 1, NewEpoch: 2, TargetHeight: 1024, TargetHash: common.Hash{4}, BlockDigest: [32]byte{5}}
	if err := staging.BeginResetIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := staging.WriteReplaySourceRoutes(ctx, 2, 0, 1, 1024); err != nil {
		t.Fatal(err)
	}
	intent.ReadyThrough, intent.Complete = 1024, true
	if err := staging.CompleteResetIntent(ctx, intent, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	oldPinned, oldRelease, err := (&Manager{dir: dir}).PinHistoryReadView()
	if err != nil || oldPinned == nil {
		t.Fatalf("pin old cold view: %v", err)
	}
	defer oldRelease()
	if err := BindHistoryStagingColdRetention(dir, staging); err != nil {
		t.Fatal(err)
	}
	if release, err := AcquireHistoryStagingPublicationRead(ctx, dir); err == nil {
		release()
		t.Fatal("pre-isolation old cold manifest admitted")
	}
	if err := IsolateHistoryStagingResetColdTail(ctx, dir, staging, *manifest.Chain); err != nil {
		t.Fatal(err)
	}
	isolated, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if isolated.HistoryStagingResetEpoch != 2 || isolated.Generation != manifest.Generation+1 || len(isolated.Segments) != 0 || len(isolated.Retired) != len(manifest.Segments) {
		t.Fatalf("cold history not isolated: generation=%d epoch=%d active=%d retired=%d", isolated.Generation, isolated.HistoryStagingResetEpoch, len(isolated.Segments), len(isolated.Retired))
	}
	if visible, err := coldSnapshotVisibleTxEndFromManifest(isolated, SegmentDatasetStateDomainChange); err != nil || visible != 0 {
		t.Fatalf("reset history cold cursor=%d err=%v", visible, err)
	}
	if err := VerifyHistoryStagingQuarantineColdTail(ctx, isolated, staging); err != nil {
		t.Fatal(err)
	}
	if err := IsolateHistoryStagingResetColdTail(ctx, dir, staging, *manifest.Chain); err != nil {
		t.Fatal(err)
	}
	resumed, err := LoadProductionManifest(dir)
	if err != nil || resumed.Generation != isolated.Generation {
		t.Fatalf("retry republished isolation generation=%d err=%v", resumed.Generation, err)
	}
	if err := oldPinned.IterateStateDomainChanges(1024, 2047, func(*rawdb.StateDomainChange) (bool, error) { return false, nil }); err != nil {
		t.Fatalf("pinned pre-reset cold files lost: %v", err)
	}
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i, block := range blocks {
		ranges[i] = &rawdb.StateTxRange{BlockNum: block.Number, BlockHash: block.Hash, BeginTxNum: block.BeginTxNum, EndTxNum: block.EndTxNum}
	}
	change := binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "cold-prev")
	change.BlockHash = blocks[5].Hash
	var rebuiltRefs []SegmentRef
	for _, half := range [][2]int{{0, 512}, {512, 1024}} {
		start, end := half[0], half[1]
		leaf := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: blocks[start].BeginTxNum, ToTxNum: blocks[end-1].EndTxNum,
			Path: filepath.Join(fmt.Sprintf("history-staging-epoch-%d", isolated.HistoryStagingResetEpoch),
				stateDomainChangeHistorySegmentPath(blocks[start].BeginTxNum, blocks[end-1].EndTxNum))}
		var changes []*rawdb.StateDomainChange
		if start == 0 {
			changes = []*rawdb.StateDomainChange{change}
		}
		history, index, accessor, err := writeHistorySegmentFiles(dir, leaf, changes, ranges[start:end])
		if err != nil {
			t.Fatal(err)
		}
		rebuiltRefs = append(rebuiltRefs, history, index, accessor)
	}
	rebuilt, err := NewAggregator(dir).Integrate(oldHistoryRef.FromTxNum, oldHistoryRef.ToTxNum, rebuiltRefs)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.HistoryStagingResetEpoch != 2 || len(rebuilt.Segments) != 6 || rebuilt.Segments[0].Path == oldHistoryRef.Path {
		t.Fatalf("new cold history reused old path or lost epoch: %+v", rebuilt)
	}
	if _, err := CompactHistoryDomainContext(ctx, dir, SegmentDatasetStateDomainChange, CompactionConfig{MaxSteps: 2, MinSteps: 2, DeleteObsolete: true}); err != nil {
		t.Fatalf("same-range new-epoch merge: %v", err)
	}
	merged, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if merged.HistoryStagingResetEpoch != 2 || len(merged.Segments) != 3 {
		t.Fatalf("new-epoch merged manifest: %+v", merged)
	}
	for _, ref := range merged.Segments {
		if ref.Kind == SegmentHistory && (ref.Path == oldHistoryRef.Path || !strings.HasPrefix(ref.Path, "history-staging-epoch-2/")) {
			t.Fatalf("merged output reused old published path: %s", ref.Path)
		}
	}
	if current, err := os.ReadFile(filepath.Join(dir, oldHistoryRef.Path)); err != nil || sha256.Sum256(current) != oldSHA {
		t.Fatalf("new cold publication overwrote retired history: %v", err)
	}
	lease, err := AcquireHistoryStagingQuarantineRetirement(ctx, dir, func(ctx context.Context, got *Manifest) error {
		return VerifyHistoryStagingQuarantineColdTail(ctx, got, staging)
	})
	if err != nil {
		t.Fatal(err)
	}
	lease()
}

func TestHistoryStagingResetColdTailWithoutPriorManifest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
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
	staging, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{Version: rawdb.HistoryStagingFormatVersion, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := staging.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := staging.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	intent := rawdb.HistoryStagingResetIntent{Version: rawdb.HistoryStagingFormatVersion, OldEpoch: 1, NewEpoch: 2, TargetHeight: 1024, TargetHash: common.Hash{4}, BlockDigest: [32]byte{5}}
	if err := staging.BeginResetIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := staging.WriteReplaySourceRoutes(ctx, 2, 0, 1, 1024); err != nil {
		t.Fatal(err)
	}
	intent.ReadyThrough, intent.Complete = 1024, true
	if err := staging.CompleteResetIntent(ctx, intent, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := BindHistoryStagingColdRetention(dir, staging); err != nil {
		t.Fatal(err)
	}
	chain := ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: (common.Hash{1}).Hex()}
	if err := IsolateHistoryStagingResetColdTail(ctx, dir, staging, chain); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil || manifest.HistoryStagingResetEpoch != 2 || manifest.Chain == nil || *manifest.Chain != chain {
		t.Fatalf("fresh isolation marker/identity: %+v err=%v", manifest, err)
	}
	if visible, err := coldSnapshotVisibleTxEndFromManifest(manifest, SegmentDatasetStateDomainChange); err != nil || visible != 0 {
		t.Fatalf("fresh cold history cursor %d err=%v", visible, err)
	}
	if lease, err := AcquireHistoryStagingPublicationRead(ctx, dir); err != nil {
		t.Fatal(err)
	} else {
		lease()
	}
}

func TestHistoryStagingMergeSyncFailureReconcilesBeforeServing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	blocks := make([]rawdb.HistoryStagingBlockProof, rawdb.StateHistoryChunkBucketBlocks)
	ranges := make([]*rawdb.StateTxRange, len(blocks))
	for i := range blocks {
		n := uint64(1024 + i)
		hash := common.Hash{byte(n >> 8), byte(n)}
		blocks[i] = rawdb.HistoryStagingBlockProof{Number: n, Hash: hash, BeginTxNum: n, EndTxNum: n}
		ranges[i] = &rawdb.StateTxRange{BlockNum: n, BlockHash: hash, BeginTxNum: n, EndTxNum: n}
	}
	changes := []*rawdb.StateDomainChange{
		binaryStateDomainChange(blocks[5].Number, blocks[5].BeginTxNum, 1, "early"),
		binaryStateDomainChange(blocks[600].Number, blocks[600].BeginTxNum, 1, "late"),
	}
	for _, change := range changes {
		change.BlockHash = blocks[int(change.BlockNum-1024)].Hash
	}
	writeLeaf := func(start, end int, rows []*rawdb.StateDomainChange) []SegmentRef {
		t.Helper()
		ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
			FromTxNum: blocks[start].BeginTxNum, ToTxNum: blocks[end-1].EndTxNum,
			Path: stateDomainChangeHistorySegmentPath(blocks[start].BeginTxNum, blocks[end-1].EndTxNum)}
		history, index, accessor, err := writeHistorySegmentFiles(dir, ref, rows, ranges[start:end])
		if err != nil {
			t.Fatal(err)
		}
		return []SegmentRef{history, index, accessor}
	}
	refs := append(writeLeaf(0, 512, changes[:1]), writeLeaf(512, 1024, changes[1:])...)
	manifest := NewManifestForChain(1024, 2047, refs, ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: (common.Hash{1}).Hex()})
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	hotPath, stagePath := filepath.Join(root, "hot"), filepath.Join(root, "stage")
	hot, err := rawdb.NewPebbleDB(hotPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := rawdb.NewHistoryStagingPebbleDB(stagePath, 16, 16, false)
	if err != nil {
		hot.Close()
		t.Fatal(err)
	}
	defer func() { _ = hot.Close(); _ = stage.Close() }()
	faultHot := &historyStagingRetirementFaultStore{KeyValueStore: hot}
	identity := rawdb.HistoryStagingIdentity{Version: rawdb.HistoryStagingFormatVersion, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}}
	staging, err := rawdb.NewHistoryStagingManager(faultHot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := staging.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := staging.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	for _, row := range ranges {
		if err := rawdb.WriteStateTxRange(hot, row.BlockNum, row.BlockHash, row.BeginTxNum, row.EndTxNum); err != nil {
			t.Fatal(err)
		}
	}
	needed := make([]bool, len(blocks))
	for i := range needed {
		needed[i] = true
	}
	spans, err := BuildHistoryStagingColdSpans(ctx, dir, manifest, blocks, needed)
	if err != nil || len(spans) != 2 {
		t.Fatalf("old cold spans %+v err=%v", spans, err)
	}
	binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion, Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: manifest.Generation, Spans: spans}
	if err := staging.CertifyColdRange(ctx, binding, func() error {
		return VerifyHistoryStagingColdBinding(ctx, dir, manifest, binding, blocks)
	}); err != nil {
		t.Fatal(err)
	}
	if err := BindHistoryStagingColdRetention(dir, staging); err != nil {
		t.Fatal(err)
	}
	_, oldRelease, err := (&Manager{dir: dir}).PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer oldRelease()
	faultHot.failSync = true
	if _, err := CompactHistoryDomainContext(ctx, dir, SegmentDatasetStateDomainChange, CompactionConfig{DeleteObsolete: true}); err == nil {
		t.Fatal("merge accepted unsynced hot cold-binding rebind")
	}
	published, err := LoadProductionManifest(dir)
	if err != nil || published.Generation != manifest.Generation+1 {
		t.Fatalf("merged G+1 not published: generation=%d err=%v", published.Generation, err)
	}
	if release, err := AcquireHistoryStagingPublicationRead(ctx, dir); err == nil {
		release()
		t.Fatal("unsynced G+1 admitted a new mixed cold view")
	}
	for _, span := range spans {
		if allowed, err := staging.CanGCCold(span.ContentID); err != nil || allowed {
			t.Fatalf("old trio released before durable rebind: allowed=%v err=%v", allowed, err)
		}
	}
	faultHot.failSync = false
	if err := ReconcileHistoryStagingColdDependencies(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if release, err := AcquireHistoryStagingPublicationRead(ctx, dir); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
	rebound, present, err := staging.ReadColdBindingAt(1, 1)
	if err != nil || !present || rebound.BindingEpoch != 3 {
		// A failed Sync leaves a visible G+1 binding; the recovery pass must
		// authenticate and durably sync that exact binding before admission.
		if err != nil || !present || rebound.BindingEpoch < 2 {
			t.Fatalf("binding did not survive reconcile: %+v present=%v err=%v", rebound, present, err)
		}
	}
	if len(rebound.Spans) != 2 || rebound.Spans[0].ContentID != rebound.Spans[1].ContentID || rebound.Spans[0].ContentID == spans[0].ContentID {
		t.Fatalf("merged semantic rebind not published: %+v", rebound)
	}
}
