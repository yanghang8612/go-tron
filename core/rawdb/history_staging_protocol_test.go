package rawdb

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
)

type historyStagingSyncFaultStore struct {
	ethdb.KeyValueStore
	failSync  bool
	syncCalls int
	failAfter int
}

func (s *historyStagingSyncFaultStore) SyncKeyValue() error {
	s.syncCalls++
	if s.failSync || (s.failAfter > 0 && s.syncCalls >= s.failAfter) {
		return errors.New("injected staging sync failure")
	}
	return s.KeyValueStore.(interface{ SyncKeyValue() error }).SyncKeyValue()
}
func (s *historyStagingSyncFaultStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return s.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
}

func stagingProtocolStores(t *testing.T) (*HistoryStagingManager, func(), string, string, HistoryStagingIdentity) {
	t.Helper()
	root := t.TempDir()
	hotPath, stagePath := filepath.Join(root, "hot"), filepath.Join(root, "stage")
	hot, err := NewPebbleDB(hotPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := NewHistoryStagingPebbleDB(stagePath, 16, 16, false)
	if err != nil {
		_ = hot.Close()
		t.Fatal(err)
	}
	identity := HistoryStagingIdentity{Version: HistoryStagingFormatVersion, GenesisHash: common.Hash{1}, NetworkID: 1, SourceID: [32]byte{2}, TargetID: [32]byte{3}}
	m, err := NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return m, func() { _ = hot.Close(); _ = stage.Close() }, hotPath, stagePath, identity
}

func stagingProtocolProof(t *testing.T, m *HistoryStagingManager, bucket uint64) HistoryStagingProof {
	t.Helper()
	first, last, err := StateHistoryChunkBucketBounds(bucket)
	if err != nil {
		t.Fatal(err)
	}
	proof := HistoryStagingProof{Bucket: bucket, Epoch: 1, EligibleThrough: last, FinishBlock: last, FinishHash: common.Hash{9}, IndexBlock: last, IndexHash: common.Hash{8}, Blocks: make([]HistoryStagingBlockProof, 0, StateHistoryChunkBucketBlocks)}
	for block := first; block <= last; block++ {
		hash := common.Hash{byte(block >> 8), byte(block)}
		if err := WriteStateTxRange(m.hot, block, hash, block, block); err != nil {
			t.Fatal(err)
		}
		proof.Blocks = append(proof.Blocks, HistoryStagingBlockProof{Number: block, Hash: hash, BeginTxNum: block, EndTxNum: block})
	}
	return proof
}

func stagingProtocolLimits() HistoryStagingLimits {
	return HistoryStagingLimits{MaxRowBytes: 4 << 20, MaxBucketBytes: 16 << 20, MaxBatchBytes: 4 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 8 << 20, MinFreeBytes: 1 << 20, FreeBytes: func() (uint64, error) { return 1 << 30, nil }}
}

func stagingProtocolRows(t *testing.T, m *HistoryStagingManager, first uint64) [][]byte {
	t.Helper()
	enableSharedHistoryTest(t)
	rows := chunkHistoryRows(1<<20, 2)
	physical := make([][]byte, 0, 4)
	for i, row := range rows {
		row.BlockNum = first + uint64(i)
		row.TxNum = row.BlockNum
		row.Seq = 1
		batch := newSharedHistoryTestBatch(m.hot)
		if err := WriteStateDomainChangeBlockRows(batch, []*StateDomainChange{row}); err != nil {
			t.Fatal(err)
		}
		if err := batch.batch.Write(); err != nil {
			t.Fatal(err)
		}
		key := stateChangeSetKey(row.BlockNum, 0)
		value, err := m.hot.Get(key)
		if err != nil || !isStateHistorySharedPack(value) {
			t.Fatalf("shared pack %d: %v", row.BlockNum, err)
		}
		physical = append(physical, key)
	}
	for seq := uint64(10); seq <= 11; seq++ {
		repair := borrowedStateDomainChangeTestRow(first, seq, first)
		if err := WriteStateDomainChangeRow(m.hot, repair); err != nil {
			t.Fatal(err)
		}
		physical = append(physical, stateChangeSetKey(first, seq))
	}
	return physical
}

func stagingReadRows(t *testing.T, view StateHistoryReadView, first, last uint64) []StateDomainChange {
	t.Helper()
	var rows []StateDomainChange
	if err := IterateStateDomainChangesByBlockRangeContext(context.Background(), view, first, last, func(row *StateDomainChange) (bool, error) {
		copyRow := *row
		copyRow.Key = bytes.Clone(row.Key)
		copyRow.Prev = bytes.Clone(row.Prev)
		copyRow.Next = bytes.Clone(row.Next)
		rows = append(rows, copyRow)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestHistoryStagingDualPebbleCopyAdoptRecover(t *testing.T) {
	ctx := context.Background()
	m, closeStores, hotPath, stagePath, identity := stagingProtocolStores(t)
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, m, 1)
	first, _, _ := StateHistoryChunkBucketBounds(1)
	physicalKeys := stagingProtocolRows(t, m, first)
	var physicalValues [][]byte
	for _, key := range physicalKeys {
		value, err := m.hot.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		physicalValues = append(physicalValues, bytes.Clone(value))
	}
	claim, err := m.BeginClaim(ctx, proof, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := m.CopyClaim(ctx, claim, stagingProtocolLimits())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TxRangeRows != StateHistoryChunkBucketBlocks || receipt.PayloadRows != 4 || receipt.ChunkRows == 0 {
		t.Fatalf("unexpected receipt %+v", receipt)
	}
	closeStores()
	hot, err := NewPebbleDB(hotPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := NewHistoryStagingPebbleDB(stagePath, 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	m, err = NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.VerifyIdentity(); err != nil {
		t.Fatal(err)
	}
	targetView, err := m.AcquireTargetBucketView(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !targetView.IsPinnedKeyValueView() {
		t.Fatal("target view is not pinned")
	}
	for i, key := range physicalKeys {
		got, err := targetView.Get(key)
		if err != nil || !bytes.Equal(got, physicalValues[i]) {
			t.Fatalf("target view key %x differs from copied source: %v", key, err)
		}
	}
	if _, err := targetView.Get(stateChangeSetKey(2048, 0)); err == nil {
		t.Fatal("target view read outside its bucket")
	}
	iter := targetView.NewIterator(stateChangeSetBlockPrefix(first), nil)
	if !iter.Next() || !bytes.Equal(iter.Key(), stateChangeSetKey(first, 0)) {
		t.Fatal("target view did not expose first packed row")
	}
	iter.Release()
	var spanBlocks int
	if err := IterateStateHistorySpanBlocks(ctx, targetView, first, first+1, first, first+1, func(block *StateHistorySpanBlock) (bool, error) {
		spanBlocks++
		_, err := block.IterateRows(func(row *StateHistorySpanRow) (bool, error) {
			return true, block.WritePrevTo(row, &bytes.Buffer{})
		})
		return true, err
	}); err != nil || spanBlocks != 2 {
		t.Fatalf("target-only span source blocks=%d err=%v", spanBlocks, err)
	}
	if err := targetView.Close(); err != nil {
		t.Fatal(err)
	}
	stats, err := m.InspectTargetBucket(ctx, proof, stagingProtocolLimits())
	if err != nil || stats.Digest != receipt.DataDigest || stats.TxRangeRows != receipt.TxRangeRows || stats.ChangeRows != receipt.PayloadRows || stats.ChunkRows != receipt.ChunkRows {
		t.Fatalf("target reverify stats=%+v receipt=%+v err=%v", stats, receipt, err)
	}
	oldView, oldClose, err := AcquireStateHistoryReadView(hot)
	if err != nil {
		t.Fatal(err)
	}
	baseline := stagingReadRows(t, oldView, first, first+1)
	if _, err := m.AdoptClaim(ctx, claim, proof); err != nil {
		t.Fatal(err)
	}
	for i, key := range physicalKeys {
		got, err := stage.Get(historyStagingPayloadKey(1, key))
		if err != nil || !bytes.Equal(got, physicalValues[i]) {
			t.Fatalf("target physical key %x mismatch %v", key, err)
		}
	}
	if err := m.ClearSource(ctx, claim, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
	if got := stagingReadRows(t, oldView, first, first+1); !reflect.DeepEqual(got, baseline) {
		t.Fatal("old pinned source view changed after clear")
	}
	if err := oldClose(); err != nil {
		t.Fatal(err)
	}
	composite, err := m.AcquireView(func() (StateHistoryReadView, func() error, error) { return AcquireStateHistoryReadView(hot) }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := stagingReadRows(t, composite, first, first+1); !reflect.DeepEqual(got, baseline) {
		t.Fatal("composite read differs from source baseline")
	}
	if err := composite.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.ClearSource(ctx, claim, stagingProtocolLimits()); err != nil {
		t.Fatalf("idempotent clear: %v", err)
	}
	route, present, err := m.ReadRoute(1)
	if err != nil || !present || route.Owner != HistoryStagingOwnerTarget || !route.SourceCleared {
		t.Fatalf("route %+v present=%v err=%v", route, present, err)
	}
	if _, present, err := m.ReadClaim(1); err != nil || present {
		t.Fatalf("claim survives clear present=%v err=%v", present, err)
	}
	first, last := first, first+StateHistoryChunkBucketBlocks-1
	binding := HistoryStagingColdBinding{Version: HistoryStagingFormatVersion, Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1, Spans: []HistoryStagingColdSpan{{From: first, To: last, ContentID: [32]byte{11}, SemanticHash: [32]byte{12}, TxRangeDigest: [32]byte{13}}}}
	if err := m.CertifyColdRange(ctx, binding, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := m.ReleaseTargetToCold(ctx, 1, func(got HistoryStagingColdBinding) error {
		if got.BindingEpoch != 1 {
			t.Fatal(got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.ClearColdTarget(ctx, 1, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
	route, present, err = m.ReadRoute(1)
	if err != nil || !present || route.Owner != HistoryStagingOwnerCold || !route.TargetCleared {
		t.Fatalf("cold route %+v %v %v", route, present, err)
	}
	if ok, err := m.stage.Has(historyStagingPayloadKey(1, stateTxRangeKey(first))); err != nil || ok {
		t.Fatalf("target row not reclaimed: %v %v", ok, err)
	}
	if ok, err := m.CanGCCold([32]byte{11}); err != nil || ok {
		t.Fatalf("bound cold file GC=%v %v", ok, err)
	}
}

func TestHistoryStagingResetEpochAndColdRefGC(t *testing.T) {
	ctx := context.Background()
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	intent := HistoryStagingResetIntent{Version: HistoryStagingFormatVersion, OldEpoch: 1, NewEpoch: 2, TargetHeight: 1024, TargetHash: common.Hash{5}, BlockDigest: [32]byte{6}}
	if err := m.BeginResetIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := m.CheckHistoryStagingBucketWritable(1); err != ErrHistoryStagingResetting {
		t.Fatalf("ordinary writer during reset: %v", err)
	}
	if err := m.PrepareResetReplay(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := m.CheckReplayBucketWritable(2, 1); err != nil {
		t.Fatal(err)
	}
	if err := m.WriteReplaySourceRoutes(ctx, 2, 0, 1, 1024); err != nil {
		t.Fatal(err)
	}
	intent.ReadyThrough = 1024
	intent.Complete = true
	if err := m.CompleteResetIntent(ctx, intent, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if epoch, err := m.CurrentEpoch(); err != nil || epoch != 2 {
		t.Fatalf("epoch %d %v", epoch, err)
	}
	oldPayload := historyStagingPayloadKey(1, stateTxRangeKey(1024))
	if err := m.stage.Put(oldPayload, []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := m.stage.Put(historyStagingReceiptKey(1, 1), []byte{3}); err != nil {
		t.Fatal(err)
	}
	var gcDone bool
	for i := 0; i < 5 && !gcDone; i++ {
		_, done, err := m.ClearQuarantinedTargetEpoch(ctx, 1, 1, 1024, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		gcDone = done
	}
	if !gcDone {
		t.Fatal("old target GC did not finish")
	}
	if present, err := m.stage.Has(oldPayload); err != nil || present {
		t.Fatalf("old target payload present=%v err=%v", present, err)
	}
	oldID := [32]byte{91}
	if err := m.hot.Put(historyStagingColdRefKey(oldID, 1, 1), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := m.syncHot(); err != nil {
		t.Fatal(err)
	}
	if err := m.RebindCold(ctx, oldID, 2, func(old HistoryStagingColdBinding) (HistoryStagingColdBinding, error) {
		t.Fatal("old epoch should not be rebound")
		return old, nil
	}, func(old, new HistoryStagingColdBinding) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if allowed, err := m.CanGCCold(oldID); err != nil || allowed {
		t.Fatalf("old epoch ref lost %v %v", allowed, err)
	}
	if count, done, err := m.RetireQuarantinedColdRefs(ctx, 128, func() error { return nil }); err != nil || !done || count != 1 {
		t.Fatalf("retire old refs count=%d done=%v err=%v", count, done, err)
	}
	if allowed, err := m.CanGCCold(oldID); err != nil || !allowed {
		t.Fatalf("old epoch GC still blocked %v %v", allowed, err)
	}
	next := HistoryStagingResetIntent{Version: HistoryStagingFormatVersion, OldEpoch: 2, NewEpoch: 3, TargetHeight: 1024, TargetHash: common.Hash{7}, BlockDigest: [32]byte{8}}
	if err := m.BeginResetIntent(ctx, next); err != nil {
		t.Fatalf("second reset: %v", err)
	}
}

func TestHistoryStagingCancelledClaimPinsColdAndPurgesOrphan(t *testing.T) {
	ctx := context.Background()
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, m, 1)
	first, last, _ := StateHistoryChunkBucketBounds(1)
	oldID := [32]byte{77}
	proof.ColdSpans = []HistoryStagingColdSpan{{From: first, To: last, ContentID: oldID, SemanticHash: [32]byte{2}, TxRangeDigest: [32]byte{3}}}
	claim, err := m.BeginClaim(ctx, proof, [32]byte{4})
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := m.CanGCCold(oldID); err != nil || allowed {
		t.Fatalf("active claim cold GC=%v %v", allowed, err)
	}
	orphan := historyStagingPayloadKey(1, stateChangeSetKey(first, 0))
	if err := m.stage.Put(orphan, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := m.AbortClaim(ctx, claim, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
	if present, err := m.stage.Has(orphan); err != nil || present {
		t.Fatalf("orphan remains %v %v", present, err)
	}
	if _, present, err := m.ReadClaim(1); err != nil || present {
		t.Fatalf("claim remains %v %v", present, err)
	}
	if allowed, err := m.CanGCCold(oldID); err != nil || !allowed {
		t.Fatalf("released claim cold GC=%v %v", allowed, err)
	}
	if _, err := m.BeginClaim(ctx, proof, [32]byte{5}); err != nil {
		t.Fatalf("fresh claim after purge: %v", err)
	}
}

func TestHistoryStagingSyncFailureNeverDeletesUniqueSource(t *testing.T) {
	ctx := context.Background()
	base, closeStores, _, _, identity := stagingProtocolStores(t)
	defer closeStores()
	if err := base.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, base, 1)
	first, _, _ := StateHistoryChunkBucketBounds(1)
	if err := WriteStateDomainChangeRow(base.hot, borrowedStateDomainChangeTestRow(first, 1, first)); err != nil {
		t.Fatal(err)
	}
	hot := &historyStagingSyncFaultStore{KeyValueStore: base.hot}
	stage := &historyStagingSyncFaultStore{KeyValueStore: base.stage}
	m, err := NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := m.BeginClaim(ctx, proof, [32]byte{14})
	if err != nil {
		t.Fatal(err)
	}
	stage.failSync = true
	if _, err := m.CopyClaim(ctx, claim, stagingProtocolLimits()); err == nil {
		t.Fatal("copy succeeded without target sync")
	}
	if _, err := m.AdoptClaim(ctx, claim, proof); err == nil {
		t.Fatal("adopt accepted unsynced target")
	}
	if present, err := hot.Has(stateChangeSetKey(first, 1)); err != nil || !present {
		t.Fatalf("source lost after target sync failure %v %v", present, err)
	}
	stage.failSync = false
	if _, err := m.CopyClaim(ctx, claim, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
	hot.failSync = true
	if _, err := m.AdoptClaim(ctx, claim, proof); err == nil {
		t.Fatal("adopt reported success without hot sync")
	}
	if err := m.ClearSource(ctx, claim, stagingProtocolLimits()); err == nil {
		t.Fatal("clear accepted unsynced route")
	}
	if present, err := hot.Has(stateChangeSetKey(first, 1)); err != nil || !present {
		t.Fatalf("source lost after hot sync failure %v %v", present, err)
	}
	hot.failSync = false
	if _, err := m.AdoptClaim(ctx, claim, proof); err != nil {
		t.Fatal(err)
	}
	if err := m.ClearSource(ctx, claim, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryStagingPartialSourceDeleteReopenResume(t *testing.T) {
	ctx := context.Background()
	m, closeStores, hotPath, stagePath, identity := stagingProtocolStores(t)
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, m, 1)
	first, _, _ := StateHistoryChunkBucketBounds(1)
	for seq := uint64(1); seq <= 3; seq++ {
		if err := WriteStateDomainChangeRow(m.hot, borrowedStateDomainChangeTestRow(first, seq, first)); err != nil {
			t.Fatal(err)
		}
	}
	claim, err := m.BeginClaim(ctx, proof, [32]byte{24})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CopyClaim(ctx, claim, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AdoptClaim(ctx, claim, proof); err != nil {
		t.Fatal(err)
	}
	limits := stagingProtocolLimits()
	limits.MaxRowBytes = 80
	limits.MaxBatchBytes = 80
	var freeCalls int
	limits.FreeBytes = func() (uint64, error) {
		freeCalls++
		if freeCalls >= 2 {
			return 0, nil
		}
		return 1 << 30, nil
	}
	if err := m.ClearSource(ctx, claim, limits); err == nil {
		t.Fatal("source clear did not stop at injected space floor")
	}
	if freeCalls < 2 {
		t.Fatalf("expected multiple delete batches, got %d", freeCalls)
	}
	if ok, err := m.hot.Has(stateChangeSetKey(first, 3)); err != nil || !ok {
		t.Fatalf("unique remaining source row missing %v %v", ok, err)
	}
	closeStores()
	hot, err := NewPebbleDB(hotPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := NewHistoryStagingPebbleDB(stagePath, 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	m, err = NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ClearSource(ctx, claim, stagingProtocolLimits()); err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if ok, err := hot.Has(stateChangeSetKey(first, seq)); err != nil || ok {
			t.Fatalf("source row %d remains=%v err=%v", seq, ok, err)
		}
	}
	route, present, err := m.ReadRoute(1)
	if err != nil || !present || !route.SourceCleared {
		t.Fatalf("source not cleared %+v %v %v", route, present, err)
	}
}

func TestHistoryStagingColdRebindSplitAndSyncFence(t *testing.T) {
	ctx := context.Background()
	m, closeStores, _, _, identity := stagingProtocolStores(t)
	defer closeStores()
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	first, last, _ := StateHistoryChunkBucketBounds(1)
	oldID := [32]byte{31}
	newA := [32]byte{32}
	newB := [32]byte{33}
	old := HistoryStagingColdBinding{Version: HistoryStagingFormatVersion, Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1, Spans: []HistoryStagingColdSpan{{From: first, To: last, ContentID: oldID, SemanticHash: [32]byte{1}, TxRangeDigest: [32]byte{2}}}}
	if err := m.CertifyColdRange(ctx, old, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := m.CertifyColdRange(ctx, old, func() error { return nil }); err != nil {
		t.Fatalf("idempotent binding: %v", err)
	}
	build := func(prior HistoryStagingColdBinding) (HistoryStagingColdBinding, error) {
		next := prior
		next.BindingEpoch++
		next.ManifestEpoch = 2
		next.Spans = []HistoryStagingColdSpan{{From: first, To: first + 511, ContentID: newA, SemanticHash: [32]byte{3}, TxRangeDigest: [32]byte{4}}, {From: first + 512, To: last, ContentID: newB, SemanticHash: [32]byte{5}, TxRangeDigest: [32]byte{6}}}
		return next, nil
	}
	if err := m.RebindCold(ctx, oldID, 2, build, func(old, new HistoryStagingColdBinding) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if allowed, err := m.CanGCCold(oldID); err != nil || !allowed {
		t.Fatalf("old trio still referenced %v %v", allowed, err)
	}
	for _, id := range [][32]byte{newA, newB} {
		if allowed, err := m.CanGCCold(id); err != nil || allowed {
			t.Fatalf("new trio unpinned %v %v", allowed, err)
		}
	}
	faultHot := &historyStagingSyncFaultStore{KeyValueStore: m.hot, failSync: true}
	faultManager, err := NewHistoryStagingManager(faultHot, m.stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := faultManager.RebindCold(ctx, newA, 3, func(prior HistoryStagingColdBinding) (HistoryStagingColdBinding, error) {
		next := prior
		next.BindingEpoch++
		next.ManifestEpoch = 3
		next.Spans[0].ContentID = [32]byte{34}
		return next, nil
	}, func(old, new HistoryStagingColdBinding) error { return nil }); err == nil {
		t.Fatal("rebind accepted unsynced hot publication")
	}
	if allowed, err := faultManager.CanGCCold(newA); err != nil || allowed {
		t.Fatalf("old file GC passed failed sync fence %v %v", allowed, err)
	}
}

func TestHistoryStagingColdRebindBatchesDurabilityByPage(t *testing.T) {
	ctx := context.Background()
	m, closeStores, _, _, identity := stagingProtocolStores(t)
	defer closeStores()
	const buckets = uint64(129)
	for _, page := range [][2]uint64{{1, 128}, {129, buckets}} {
		if err := m.InitializeOfflineSourceRoutes(ctx, 1, page[0], page[1]); err != nil {
			t.Fatal(err)
		}
	}
	oldID, newID := [32]byte{41}, [32]byte{42}
	for bucket := uint64(1); bucket <= buckets; bucket++ {
		first, last, err := StateHistoryChunkBucketBounds(bucket)
		if err != nil {
			t.Fatal(err)
		}
		binding := HistoryStagingColdBinding{Version: HistoryStagingFormatVersion, Bucket: bucket, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1,
			Spans: []HistoryStagingColdSpan{{From: first, To: last, ContentID: oldID, SemanticHash: [32]byte{1}, TxRangeDigest: [32]byte{2}}}}
		if err := m.CertifyColdRange(ctx, binding, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	faultHot := &historyStagingSyncFaultStore{KeyValueStore: m.hot, failAfter: 2}
	rebinder, err := NewHistoryStagingManager(faultHot, m.stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	build := func(old HistoryStagingColdBinding) (HistoryStagingColdBinding, error) {
		next := old
		next.Spans = append([]HistoryStagingColdSpan(nil), old.Spans...)
		next.BindingEpoch++
		next.ManifestEpoch = 2
		next.Spans[0].ContentID = newID
		return next, nil
	}
	if err := rebinder.RebindCold(ctx, oldID, 2, build, func(_, _ HistoryStagingColdBinding) error { return nil }); err == nil {
		t.Fatal("second-page Sync failure was accepted")
	} else if faultHot.syncCalls == 0 {
		t.Fatalf("rebind rejected before first bounded page: %v", err)
	}
	if faultHot.syncCalls != 2 {
		t.Fatalf("129 bindings required %d Sync calls, want 2 bounded pages", faultHot.syncCalls)
	}
	if allowed, err := rebinder.CanGCCold(oldID); err != nil || allowed {
		t.Fatalf("failed second-page Sync released old trio: %v, %v", allowed, err)
	}
	for _, bucket := range []uint64{1, 128, 129} {
		binding, present, err := rebinder.ReadColdBindingAt(1, bucket)
		if err != nil || !present || binding.Spans[0].ContentID != newID {
			t.Fatalf("batch %d visible binding %+v, present=%t, err=%v", bucket, binding, present, err)
		}
	}
	// The failed Sync can leave page two visible in Pebble. A fresh Sync is
	// required even if a retry finds no old reverse refs to process.
	faultHot.failAfter = 0
	if err := rebinder.RebindCold(ctx, oldID, 2, build, func(_, _ HistoryStagingColdBinding) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := rebinder.SyncColdDependencyPublications(); err != nil {
		t.Fatal(err)
	}
	if allowed, err := rebinder.CanGCCold(oldID); err != nil || !allowed {
		t.Fatalf("durably rebound old trio remains pinned: %v, %v", allowed, err)
	}
}

func TestHistoryStagingScanBucketsMergesSparseRoutesAndClaims(t *testing.T) {
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	ctx := context.Background()
	for _, bucket := range []uint64{100, 300} {
		route := HistoryStagingRoute{Version: HistoryStagingFormatVersion, Bucket: bucket, Epoch: 1, WriteVersion: 1, Owner: HistoryStagingOwnerSource}
		if err := writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingRoutePrefix, bucket), route); err != nil {
			t.Fatal(err)
		}
	}
	for _, bucket := range []uint64{2, 100, 200} {
		proof := stagingProtocolProof(t, m, bucket)
		digest, err := proof.digest()
		if err != nil {
			t.Fatal(err)
		}
		claim := HistoryStagingClaim{Version: HistoryStagingFormatVersion, Bucket: bucket, Epoch: 1, ClaimID: [32]byte{1}, ProofDigest: digest, Proof: proof}
		if err := writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingClaimPrefix, bucket), claim); err != nil {
			t.Fatal(err)
		}
	}
	var got []uint64
	var after []byte
	for {
		states, next, done, err := m.ScanBuckets(ctx, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, state := range states {
			got = append(got, state.Bucket)
			if state.Bucket == 100 && (!state.HasRoute || !state.HasClaim) {
				t.Fatal("overlapping route and claim were not merged")
			}
		}
		if done {
			break
		}
		if len(next) != 8 {
			t.Fatal("missing exclusive page cursor")
		}
		after = next
	}
	if want := []uint64{0, 2, 100, 200, 300}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bucket order %v want %v", got, want)
	}
}

func TestHistoryStagingColdGCLeaseSerializesNewBinding(t *testing.T) {
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	ctx := context.Background()
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	first, last, _ := StateHistoryChunkBucketBounds(1)
	id := [32]byte{29}
	binding := HistoryStagingColdBinding{Version: HistoryStagingFormatVersion, Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1,
		Spans: []HistoryStagingColdSpan{{From: first, To: last, ContentID: id, SemanticHash: [32]byte{1}, TxRangeDigest: [32]byte{2}}}}
	started := make(chan struct{})
	finished := make(chan error, 1)
	removed, err := m.WithColdGCLease(id, func() error {
		go func() {
			close(started)
			finished <- m.CertifyColdRange(ctx, binding, func() error { return nil })
		}()
		<-started
		select {
		case err := <-finished:
			t.Fatalf("new binding published during physical GC lease: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		return nil
	})
	if err != nil || !removed {
		t.Fatalf("GC lease failed: removed=%v err=%v", removed, err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cold binding remained blocked after GC lease")
	}
	if allowed, err := m.CanGCCold(id); err != nil || allowed {
		t.Fatalf("new cold binding failed to pin file: allowed=%v err=%v", allowed, err)
	}
}

func TestHistoryStagingSemanticMismatchRetainsClaimColdRefs(t *testing.T) {
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	ctx := context.Background()
	if err := m.InitializeOfflineSourceRoutes(ctx, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	proof := stagingProtocolProof(t, m, 1)
	first, last, _ := StateHistoryChunkBucketBounds(1)
	oldID := [32]byte{74}
	span := HistoryStagingColdSpan{From: first, To: last, ContentID: oldID, SemanticHash: [32]byte{2}, TxRangeDigest: [32]byte{3}}
	proof.ColdSpans = []HistoryStagingColdSpan{span}
	claim, err := m.BeginClaim(ctx, proof, [32]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	binding := HistoryStagingColdBinding{Version: HistoryStagingFormatVersion, Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1, Spans: []HistoryStagingColdSpan{span}}
	if err := m.CertifyColdRange(ctx, binding, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	mismatch := errors.New("injected semantic mismatch")
	err = m.RebindCold(ctx, oldID, 2, func(HistoryStagingColdBinding) (HistoryStagingColdBinding, error) {
		return HistoryStagingColdBinding{}, mismatch
	}, func(HistoryStagingColdBinding, HistoryStagingColdBinding) error { return nil })
	if !errors.Is(err, mismatch) {
		t.Fatalf("semantic mismatch was not preserved: %v", err)
	}
	if got, present, err := m.ReadClaim(1); err != nil || !present || got.ClaimID != claim.ClaimID {
		t.Fatalf("semantic mismatch discarded frozen claim: present=%v err=%v", present, err)
	}
	if allowed, err := m.CanGCCold(oldID); err != nil || allowed {
		t.Fatalf("semantic mismatch released old cold file: allowed=%v err=%v", allowed, err)
	}
}

func TestHistoryStagingQuarantinedColdRefsDrainBoundedPages(t *testing.T) {
	m, closeStores, _, _, _ := stagingProtocolStores(t)
	defer closeStores()
	ctx := context.Background()
	intent := HistoryStagingResetIntent{Version: HistoryStagingFormatVersion, OldEpoch: 1, NewEpoch: 2, TargetHeight: 1024, TargetHash: common.Hash{5}, BlockDigest: [32]byte{6}}
	if err := m.BeginResetIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := m.WriteReplaySourceRoutes(ctx, 2, 0, 1, 1024); err != nil {
		t.Fatal(err)
	}
	intent.ReadyThrough, intent.Complete = 1024, true
	if err := m.CompleteResetIntent(ctx, intent, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	id := [32]byte{91}
	for bucket := uint64(1); bucket <= 301; bucket++ {
		if err := m.hot.Put(historyStagingColdRefKey(id, 1, bucket), []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	var total uint64
	var pages int
	for {
		count, done, err := m.RetireQuarantinedColdRefs(ctx, 128, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		total += count
		pages++
		if done {
			break
		}
		if pages > 4 {
			t.Fatal("quarantined refs failed to drain")
		}
	}
	if total != 301 || pages < 3 {
		t.Fatalf("unbounded/incomplete ref retirement total=%d pages=%d", total, pages)
	}
	if allowed, err := m.CanGCCold(id); err != nil || !allowed {
		t.Fatalf("old cold file remains pinned after full drain: %v %v", allowed, err)
	}
}

func TestHistoryStagingQuarantinedHotMetadataPagesAndReopen(t *testing.T) {
	ctx := context.Background()
	m, closeStores, hotPath, stagePath, identity := stagingProtocolStores(t)
	receiptSize := 0
	for block := uint64(1); block <= 301; block++ {
		row, err := NewHistoryStagingBlockHasher(block).Finish(common.Hash{1}, 1, block, block)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := encodeHistoryStaging(row)
		if err != nil {
			t.Fatal(err)
		}
		receiptSize = len(encoded) + len(historyStagingBlockCompleteKey(1, block))
		if err := WriteHistoryStagingBlockComplete(m.hot, row); err != nil {
			t.Fatal(err)
		}
	}
	if receiptSize < 100 || receiptSize > 256 {
		t.Fatalf("unexpected RLP receipt physical bytes per block: %d", receiptSize)
	}
	t.Logf("staging block-complete RLP key+value bytes: %d", receiptSize)
	oldBinding := HistoryStagingColdBinding{Version: HistoryStagingFormatVersion, Bucket: 3, Epoch: 1, BindingEpoch: 1, ManifestEpoch: 1,
		Spans: []HistoryStagingColdSpan{{From: 3072, To: 4095, ContentID: [32]byte{3}, SemanticHash: [32]byte{4}, TxRangeDigest: [32]byte{5}}}}
	if err := writeHistoryStagingValue(m.hot, historyStagingColdBindingKey(1, 3), oldBinding); err != nil {
		t.Fatal(err)
	}
	oldClaim := HistoryStagingClaim{Version: HistoryStagingFormatVersion, Bucket: 3, Epoch: 1}
	if err := writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingClaimPrefix, 3), oldClaim); err != nil {
		t.Fatal(err)
	}
	oldRoute := HistoryStagingRoute{Version: HistoryStagingFormatVersion, Bucket: 3, Epoch: 1, Owner: HistoryStagingOwnerTarget, ReceiptDigest: [32]byte{9}}
	if err := writeHistoryStagingValue(m.hot, historyStagingBucketKey(historyStagingRoutePrefix, 3), oldRoute); err != nil {
		t.Fatal(err)
	}
	if err := m.hot.Put(historyStagingColdRefKey([32]byte{3}, 1, 3), []byte{1}); err != nil {
		t.Fatal(err)
	}
	intent := HistoryStagingResetIntent{Version: HistoryStagingFormatVersion, OldEpoch: 1, NewEpoch: 2, TargetHeight: 1024, TargetHash: common.Hash{5}, BlockDigest: [32]byte{6}}
	if err := m.BeginResetIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := m.WriteReplaySourceRoutes(ctx, 2, 0, 1, 1024); err != nil {
		t.Fatal(err)
	}
	intent.ReadyThrough, intent.Complete = 1024, true
	if err := m.CompleteResetIntent(ctx, intent, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	current, err := NewHistoryStagingBlockHasher(1024).Finish(common.Hash{2}, 2, 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteHistoryStagingBlockComplete(m.hot, current); err != nil {
		t.Fatal(err)
	}
	if _, done, err := m.RetireQuarantinedHotMetadata(ctx, 128, 1<<20, func() error { return nil }); err == nil || done {
		t.Fatalf("old hot metadata removed before cold refs retired: %v done=%v", err, done)
	}
	if _, done, err := m.RetireQuarantinedColdRefs(ctx, 128, func() error { return nil }); err != nil || !done {
		t.Fatalf("old refs not durably retired: %v done=%v", err, done)
	}
	if count, done, err := m.RetireQuarantinedHotMetadata(ctx, 128, 1<<20, func() error { return nil }); err != nil || done || count == 0 {
		t.Fatalf("expected first bounded hot page count=%d done=%v err=%v", count, done, err)
	}
	closeStores()
	hot, err := NewPebbleDB(hotPath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := NewHistoryStagingPebbleDB(stagePath, 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	m, err = NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	var pages int
	for {
		count, done, err := m.RetireQuarantinedHotMetadata(ctx, 128, 1<<20, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		total += count
		pages++
		if done {
			break
		}
		if pages > 8 {
			t.Fatal("quarantined hot metadata did not drain")
		}
	}
	if total < 170 || pages < 2 {
		t.Fatalf("incomplete/bulk hot metadata GC total=%d pages=%d", total, pages)
	}
	if _, present, err := ReadHistoryStagingBlockComplete(hot, 2, 1024); err != nil || !present {
		t.Fatalf("current SOURCE completeness receipt lost: present=%v err=%v", present, err)
	}
	if _, present, err := ReadHistoryStagingBlockComplete(hot, 1, 301); err != nil || present {
		t.Fatalf("old receipt survived: present=%v err=%v", present, err)
	}
	for _, key := range [][]byte{historyStagingColdBindingKey(1, 3), historyStagingBucketKey(historyStagingClaimPrefix, 3), historyStagingBucketKey(historyStagingRoutePrefix, 3)} {
		if present, err := hot.Has(key); err != nil || present {
			t.Fatalf("old hot metadata %x remains: present=%v err=%v", key, present, err)
		}
	}
	for _, bucket := range []uint64{0, 1} {
		route, present, err := m.ReadRoute(bucket)
		if err != nil || !present || route.Epoch != 2 || route.Owner != HistoryStagingOwnerSource {
			t.Fatalf("current SOURCE route %d lost: %+v present=%v err=%v", bucket, route, present, err)
		}
	}
}
