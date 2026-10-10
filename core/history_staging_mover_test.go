package core

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/params"
)

type historyStagingRunnerChain struct {
	bc         *BlockChain
	solidified int64
}

func TestHistoryStagingLastCompleteBucket(t *testing.T) {
	for _, tc := range []struct {
		eligible, want uint64
	}{
		{0, 0}, {1022, 0}, {1023, 0}, {1024, 0},
		{2046, 0}, {2047, 1}, {2048, 1}, {3070, 1}, {3071, 2},
		{^uint64(0) - 1, ^uint64(0)/1024 - 1},
		{^uint64(0), ^uint64(0) / 1024},
	} {
		if got := historyStagingLastCompleteBucket(tc.eligible); got != tc.want {
			t.Errorf("eligible=%d: last complete bucket=%d want=%d", tc.eligible, got, tc.want)
		}
	}
}

func TestHistoryStagingMoverRevisitsMaturingBucketWithoutRestart(t *testing.T) {
	ctx := context.Background()
	hot, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(t.TempDir(), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{Config: cfg, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	_, genesisHash, err := SetupGenesisBlock(hot, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{
		Version: rawdb.HistoryStagingFormatVersion, GenesisHash: genesisHash, NetworkID: 1,
		SourceID: [32]byte{1}, TargetID: [32]byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureCanonicalSourceRoute(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	for height := uint64(1); height <= 2063; height++ {
		block := testRestartBlock(height)
		if err := rawdb.WriteBlock(hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(hot, height, block.Hash(), height, height); err != nil {
			t.Fatal(err)
		}
		if height >= 1024 && height <= 2047 {
			receipt, err := rawdb.NewHistoryStagingBlockHasher(height).Finish(block.Hash(), 1, height, height)
			if err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteHistoryStagingBlockComplete(hot, receipt); err != nil {
				t.Fatal(err)
			}
		}
	}
	setProgress := func(head, solid, finish, indexed uint64) {
		t.Helper()
		bc.currentBlock.Store(testRestartBlock(head))
		props := bc.DynProps().Copy()
		props.SetLatestSolidifiedBlockNum(int64(solid))
		bc.storeDynPropsCache(props)
		for stageID, height := range map[rawdb.StageID]uint64{rawdb.StageFinish: finish, rawdb.StageStateHistoryIndex: indexed} {
			if err := rawdb.WriteStageProgressWithHash(hot, stageID, height, testRestartBlock(height).Hash()); err != nil {
				t.Fatal(err)
			}
		}
	}
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	mover, err := NewHistoryStagingMover(bc, HistoryStagingMoverConfig{
		HistoryWindow: 16, Cadence: time.Second, HeavyWorkGate: maintenance.NewHeavyWorkGate(),
		HotPressure: pressure, StagePressure: pressure,
		Limits: rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20,
			MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20,
			MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                         string
		head, solid, finish, indexed uint64
	}{
		{"retention tail", 2050, 2050, 2050, 2050},
		{"Finish lag", 2063, 2063, 2046, 2063},
		{"Index lag", 2063, 2063, 2063, 2046},
		{"solid lag", 2063, 2050, 2063, 2063},
		{"head lag", 2046, 2063, 2063, 2063},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setProgress(tc.head, tc.solid, tc.finish, tc.indexed)
			if ready, err := mover.candidateHint(ctx); err != nil || ready {
				t.Fatalf("immature bucket hint: ready=%t err=%v", ready, err)
			}
			if err := mover.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if mover.nextBucket != 1 {
				t.Fatalf("immature bucket skipped: next=%d want=1", mover.nextBucket)
			}
		})
	}
	setProgress(2063, 2063, 2063, 2063) // retention cutoff reaches bucket one's last block.
	if ready, err := mover.candidateHint(ctx); err != nil || !ready {
		t.Fatalf("mature bucket hint: ready=%t err=%v", ready, err)
	}
	// A failed copy must retain its durable claim and retry the same bucket.
	mover.cfg.Limits.MaxRowBytes = 1
	if err := mover.RunOnce(ctx); err == nil {
		t.Fatal("undersized row budget accepted copy")
	}
	if mover.nextBucket != 1 {
		t.Fatalf("failed copy skipped bucket: next=%d", mover.nextBucket)
	}
	if _, present, err := manager.ReadClaim(1); err != nil || !present {
		t.Fatalf("failed copy lost claim: present=%t err=%v", present, err)
	}
	mover.cfg.Limits.MaxRowBytes = 1 << 20
	if err := mover.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	route, present, err := manager.ReadRoute(1)
	if err != nil || !present || route.Owner != rawdb.HistoryStagingOwnerTarget || !route.SourceCleared {
		t.Fatalf("mature bucket not handed off: route=%+v present=%t err=%v", route, present, err)
	}
	target, err := manager.AcquireTargetBucketView(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if row, present, err := rawdb.ReadStateTxRange(target, 2047); err != nil || !present || row.BlockHash != testRestartBlock(2047).Hash() {
		t.Fatalf("target missing copied range: row=%+v present=%t err=%v", row, present, err)
	}
	if route, present, err := manager.ReadRoute(0); err != nil || !present || route.Owner != rawdb.HistoryStagingOwnerSource {
		t.Fatalf("genesis route changed: route=%+v present=%t err=%v", route, present, err)
	}
	// Missing route metadata at the next eligible bucket must fail closed on
	// every attempt, rather than silently advancing beyond the unread bucket.
	setProgress(3087, 3087, 3087, 3087)
	for attempt := 0; attempt < 2; attempt++ {
		if err := mover.runAdmitted(ctx); !errors.Is(err, rawdb.ErrHistoryStagingIncomplete) {
			t.Fatalf("missing next route attempt %d: err=%v", attempt, err)
		}
		if mover.nextBucket != 2 {
			t.Fatalf("missing route skipped bucket: next=%d want=2", mover.nextBucket)
		}
	}
}

func (c historyStagingRunnerChain) DB() snapshots.AggregatorDB      { return c.bc.DB() }
func (c historyStagingRunnerChain) LatestSolidifiedBlockNum() int64 { return c.solidified }
func (c historyStagingRunnerChain) AcquireStateHistorySourceView(ctx context.Context) (snapshots.AggregatorDB, func() error, error) {
	return c.bc.AcquireStateHistorySourceView(ctx)
}
func (c historyStagingRunnerChain) CanonicalBlockHashStrict(number uint64) (common.Hash, bool, error) {
	hash, present := rawdb.ReadBlockHash(c.bc.chaindb, number)
	return hash, present, nil
}

func TestHistoryStagingMoverResumesClaimAfterStageAdvance(t *testing.T) {
	hot, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := rawdb.NewHistoryStagingPebbleDB(t.TempDir(), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	defer stage.Close()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{Config: cfg, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	_, genesisHash, err := SetupGenesisBlock(hot, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{Version: rawdb.HistoryStagingFormatVersion, GenesisHash: genesisHash, NetworkID: 1, SourceID: [32]byte{1}, TargetID: [32]byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureCanonicalSourceRoute(context.Background(), 1, 1); err != nil {
		t.Fatal(err)
	}
	for height := uint64(1); height <= 2049; height++ {
		block := testRestartBlock(height)
		if err := rawdb.WriteBlock(hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(hot, height, block.Hash(), height, height); err != nil {
			t.Fatal(err)
		}
		if height >= 1024 && height <= 2047 {
			receipt, err := rawdb.NewHistoryStagingBlockHasher(height).Finish(block.Hash(), 1, height, height)
			if err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteHistoryStagingBlockComplete(hot, receipt); err != nil {
				t.Fatal(err)
			}
		}
		if height == 2048 {
			bc.currentBlock.Store(block)
			props := bc.DynProps().Copy()
			props.SetLatestSolidifiedBlockNum(2048)
			bc.storeDynPropsCache(props)
			for _, stageID := range []rawdb.StageID{rawdb.StageFinish, rawdb.StageStateHistoryIndex} {
				if err := rawdb.WriteStageProgressWithHash(hot, stageID, height, block.Hash()); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	finish, _, _ := rawdb.ReadStageProgressRow(hot, rawdb.StageFinish)
	indexed, _, _ := rawdb.ReadStageProgressRow(hot, rawdb.StageStateHistoryIndex)
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now, DeviceQueueMilli: 4_000, DeviceAwait: time.Millisecond}
	}
	mover, err := NewHistoryStagingMover(bc, HistoryStagingMoverConfig{HistoryWindow: 1, Cadence: time.Second, HeavyWorkGate: maintenance.NewHeavyWorkGate(), HotPressure: pressure, StagePressure: pressure, Limits: rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20, MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20, MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	proof, needed, err := mover.collectProofLocked(1, 2047, finish, indexed)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range needed {
		if v {
			t.Fatal("receipt-backed hot bucket unexpectedly needs cold history")
		}
	}
	claim, err := manager.BeginClaim(context.Background(), proof, [32]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("reservation hint survives an unrelated empty page", func(t *testing.T) {
		bc.chainmu.Lock()
		ready, err := mover.candidateHint(context.Background())
		bc.chainmu.Unlock()
		if err != nil || !ready {
			t.Fatalf("route-only hint under writer lock: ready=%t err=%v", ready, err)
		}
		other, ok := mover.cfg.HeavyWorkGate.TryAcquire()
		if !ok {
			t.Fatal("could not occupy maintenance gate")
		}
		if _, ok := mover.acquireHeavy(true); ok || mover.reservation == nil {
			t.Fatal("candidate failed to reserve a turn")
		}
		var after [8]byte
		binary.BigEndian.PutUint64(after[:], 1000)
		mover.hintCursor = after[:]
		ready, err = mover.candidateHint(context.Background())
		if err != nil || !ready || mover.reservation == nil {
			t.Fatalf("empty page revoked eligible candidate: ready=%t err=%v", ready, err)
		}
		other()
		mover.cancelReservation()
		mover.hintCursor = nil
	})
	t.Run("canceled claim releases publication before checkpoint", func(t *testing.T) {
		dir := t.TempDir()
		manifest := snapshots.NewManifestForChain(0, 0, nil, snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: fmt.Sprintf("0x%x", genesisHash[:])})
		if err := snapshots.PublishManifest(dir, manifest); err != nil {
			t.Fatal(err)
		}
		cold, err := snapshots.OpenManager(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := snapshots.BindHistoryStagingColdRetention(dir, manager); err != nil {
			t.Fatal(err)
		}
		bc.SetStateCodeColdHistory(cold)
		limits := mover.cfg.Limits
		limits.Checkpoint = func(uint64) error { return context.Canceled }
		if err := manager.AbortClaim(context.Background(), claim, limits); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel claim: %v", err)
		}
		calls := 0
		mover.cfg.Limits.Checkpoint = func(uint64) error {
			calls++
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			// This takes publication Write admission. It must succeed while the
			// aborted payload copy yields instead of waiting behind its Read.
			return snapshots.ReconcileHistoryStagingColdDependencies(ctx, dir)
		}
		if err := mover.runAdmitted(context.Background()); err != nil {
			t.Fatal(err)
		}
		mover.cfg.Limits.Checkpoint = nil
		if calls == 0 {
			t.Fatal("abort never reached a checkpoint")
		}
		if _, present, err := manager.ReadClaim(1); err != nil || present {
			t.Fatalf("abort left claim: present=%t err=%v", present, err)
		}
		claim, err = manager.BeginClaim(context.Background(), proof, [32]byte{9})
		if err != nil {
			t.Fatal(err)
		}
	})
	// Simulate foreground import and index progress after the durable claim.
	newHead := testRestartBlock(2049)
	bc.currentBlock.Store(newHead)
	props := bc.DynProps().Copy()
	props.SetLatestSolidifiedBlockNum(2049)
	bc.storeDynPropsCache(props)
	for _, stageID := range []rawdb.StageID{rawdb.StageFinish, rawdb.StageStateHistoryIndex} {
		if err := rawdb.WriteStageProgressWithHash(hot, stageID, 2049, newHead.Hash()); err != nil {
			t.Fatal(err)
		}
	}
	if err := mover.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	route, present, err := manager.ReadRoute(1)
	if err != nil || !present || route.Owner != rawdb.HistoryStagingOwnerTarget || !route.SourceCleared {
		t.Fatalf("route after resume = %+v present=%t err=%v; claim=%+v", route, present, err, claim)
	}
	// The migration cursor has passed the only eligible bucket. A later cold
	// publication must still be found by the separate wrapping retirement
	// cursor and must release the target only after semantic equivalence.
	dir := t.TempDir()
	historyCfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("state history dataset missing")
	}
	ranges := make([]*rawdb.StateTxRange, 0, rawdb.StateHistoryChunkBucketBlocks)
	for number := uint64(1024); number <= 2047; number++ {
		row, present, err := rawdb.ReadStateTxRange(hot, number)
		if err != nil || !present {
			t.Fatalf("txrange %d: present=%t err=%v", number, present, err)
		}
		ranges = append(ranges, row)
	}
	ref := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory,
		FromTxNum: 1024, ToTxNum: 2047, Path: historyCfg.HistoryPath(1024, 2047)}
	unexpected := &rawdb.StateDomainChange{BlockNum: 1024, BlockHash: ranges[0].BlockHash,
		TxNum: 1024, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41}}
	history, index, accessor, err := historyCfg.WriteHistory(dir, ref, []*rawdb.StateDomainChange{unexpected}, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest := snapshots.NewManifestForChain(1024, 2047, []snapshots.SegmentRef{history, index, accessor},
		snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: fmt.Sprintf("0x%x", genesisHash[:])})
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	coldManager, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetStateCodeColdHistory(coldManager)
	var mismatch error
	for attempt := 0; attempt < 3; attempt++ {
		mismatch = mover.RunOnce(context.Background())
		if mismatch != nil {
			break
		}
	}
	if !errors.Is(mismatch, snapshots.ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("incomplete cold publication = %v, want semantic mismatch", mismatch)
	}
	if err := mover.RunOnce(context.Background()); !errors.Is(err, snapshots.ErrHistoryStagingColdSemanticMismatch) {
		t.Fatalf("repeated cold publication validation = %v, want persistent mismatch", err)
	}
	route, present, err = manager.ReadRoute(1)
	if err != nil || !present || route.Owner != rawdb.HistoryStagingOwnerTarget || route.ColdBindingEpoch != 0 {
		t.Fatalf("mismatched cold publication changed owner: %+v present=%t err=%v", route, present, err)
	}
	// A corrected cold trio can then certify the same durable TARGET rows.
	history, index, accessor, err = historyCfg.WriteHistory(dir, ref, nil, ranges)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Generation++
	manifest.PublishedUnix++
	manifest.Segments = []snapshots.SegmentRef{history, index, accessor}
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 4; attempt++ {
		if err := mover.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	route, present, err = manager.ReadRoute(1)
	if err != nil || !present || route.Owner != rawdb.HistoryStagingOwnerCold || !route.TargetCleared {
		t.Fatalf("route after later cold publication = %+v present=%t err=%v", route, present, err)
	}
}

func TestHistoryStagingMoverPressureFailClosed(t *testing.T) {
	stale := func() maintenance.StoragePressure { return maintenance.StoragePressure{} }
	m := &HistoryStagingMover{cfg: HistoryStagingMoverConfig{HeavyWorkGate: maintenance.NewHeavyWorkGate(), HotPressure: stale, StagePressure: stale}}
	if err := m.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	busyDevice := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now, DeviceBusyPPM: 950_000, DeviceAwait: 2 * time.Millisecond}
	}
	m.cfg.HotPressure, m.cfg.StagePressure = busyDevice, busyDevice
	if err := m.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryStagingMoverQuarantineRefsContinueAfterTargetsDone(t *testing.T) {
	ctx := context.Background()
	hot, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(t.TempDir(), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{Config: cfg, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	_, genesisHash, err := SetupGenesisBlock(hot, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{
		Version: rawdb.HistoryStagingFormatVersion, GenesisHash: genesisHash, NetworkID: 1,
		SourceID: [32]byte{1}, TargetID: [32]byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureCanonicalSourceRoute(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	proof := rawdb.HistoryStagingProof{Bucket: 1, Epoch: 1, EligibleThrough: 2047,
		FinishBlock: 2047, FinishHash: common.Hash{1}, IndexBlock: 2047, IndexHash: common.Hash{1}}
	for number := uint64(1024); number <= 2047; number++ {
		proof.Blocks = append(proof.Blocks, rawdb.HistoryStagingBlockProof{
			Number: number, Hash: common.Hash{1}, BeginTxNum: number, EndTxNum: number})
	}
	var lastID [32]byte
	for i := uint64(1); i <= 300; i++ {
		var id [32]byte
		binary.BigEndian.PutUint64(id[:8], i)
		proof.ColdSpans = append(proof.ColdSpans, rawdb.HistoryStagingColdSpan{
			From: 1022 + 2*i, To: 1022 + 2*i, ContentID: id,
			SemanticHash: [32]byte{1}, TxRangeDigest: [32]byte{2}})
		lastID = id
	}
	if _, err := manager.BeginClaim(ctx, proof, [32]byte{9}); err != nil {
		t.Fatal(err)
	}
	if can, err := manager.CanGCCold(lastID); err != nil || can {
		t.Fatalf("old claim ref absent before reset: canGC=%t err=%v", can, err)
	}
	intent := rawdb.HistoryStagingResetIntent{Version: rawdb.HistoryStagingFormatVersion,
		OldEpoch: 1, NewEpoch: 2, TargetHeight: 1024, TargetHash: common.Hash{4}, BlockDigest: [32]byte{5}}
	if err := manager.BeginResetIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := manager.WriteReplaySourceRoutes(ctx, 2, 0, 1, 1024); err != nil {
		t.Fatal(err)
	}
	intent.ReadyThrough, intent.Complete = 1024, true
	if err := manager.CompleteResetIntent(ctx, intent, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	manifest := snapshots.NewManifestForChain(0, 0, nil, snapshots.ChainIdentity{
		ChainID: cfg.ChainID, NetworkID: 1, GenesisHash: genesisHash.Hex()})
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	coldManager, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetStateCodeColdHistory(coldManager)
	if err := snapshots.BindHistoryStagingColdRetention(dir, manager); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.IsolateHistoryStagingResetColdTail(ctx, dir, manager, snapshots.ChainIdentity{
		ChainID: cfg.ChainID, NetworkID: 1, GenesisHash: genesisHash.Hex()}); err != nil {
		t.Fatal(err)
	}
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	mover, err := NewHistoryStagingMover(bc, HistoryStagingMoverConfig{
		HistoryWindow: 1, Cadence: time.Second, HeavyWorkGate: maintenance.NewHeavyWorkGate(),
		HotPressure: pressure, StagePressure: pressure,
		Limits: rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20,
			MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20,
			MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := mover.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if mover.quarantineEpoch < 2 {
		t.Fatalf("old target cursor did not complete: %d", mover.quarantineEpoch)
	}
	if can, err := manager.CanGCCold(lastID); err != nil || can {
		t.Fatalf("old cold refs retired before third page: canGC=%t err=%v", can, err)
	}
	for i := 0; i < 3 && !mover.quarantineRefsDone; i++ {
		if err := mover.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !mover.quarantineRefsDone {
		t.Fatal("cold refs did not finish after target cursor completed")
	}
	if can, err := manager.CanGCCold(lastID); err != nil || !can {
		t.Fatalf("old cold ref remains after bounded pages: canGC=%t err=%v", can, err)
	}
	// Another offline reset can leave a TARGET route above the new head. Its
	// canonical blocks are deliberately absent; live retirement must ignore
	// the old epoch and allow quarantine to do the bounded cleanup.
	if err := manager.EnsureCanonicalSourceRoute(ctx, 2, 2); err != nil {
		t.Fatal(err)
	}
	highProof := rawdb.HistoryStagingProof{Bucket: 2, Epoch: 2, EligibleThrough: 3071,
		FinishBlock: 3071, FinishHash: common.Hash{3}, IndexBlock: 3071, IndexHash: common.Hash{3}}
	for number := uint64(2048); number <= 3071; number++ {
		if err := rawdb.WriteStateTxRange(hot, number, common.Hash{3}, number, number); err != nil {
			t.Fatal(err)
		}
		highProof.Blocks = append(highProof.Blocks, rawdb.HistoryStagingBlockProof{
			Number: number, Hash: common.Hash{3}, BeginTxNum: number, EndTxNum: number})
	}
	highClaim, err := manager.BeginClaim(ctx, highProof, [32]byte{8})
	if err != nil {
		t.Fatal(err)
	}
	limits := rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20,
		MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20,
		MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}
	if _, err := manager.CopyClaim(ctx, highClaim, limits); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AdoptClaim(ctx, highClaim, highProof); err != nil {
		t.Fatal(err)
	}
	if err := manager.ClearSource(ctx, highClaim, limits); err != nil {
		t.Fatal(err)
	}
	secondReset := rawdb.HistoryStagingResetIntent{Version: rawdb.HistoryStagingFormatVersion,
		OldEpoch: 2, NewEpoch: 3, TargetHeight: 1024, TargetHash: common.Hash{4}, BlockDigest: [32]byte{6}}
	if err := manager.BeginResetIntent(ctx, secondReset); err != nil {
		t.Fatal(err)
	}
	if err := manager.WriteReplaySourceRoutes(ctx, 3, 0, 1, 1024); err != nil {
		t.Fatal(err)
	}
	secondReset.ReadyThrough, secondReset.Complete = 1024, true
	if err := manager.CompleteResetIntent(ctx, secondReset, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := snapshots.IsolateHistoryStagingResetColdTail(ctx, dir, manager, snapshots.ChainIdentity{
		ChainID: cfg.ChainID, NetworkID: 1, GenesisHash: genesisHash.Hex()}); err != nil {
		t.Fatal(err)
	}
	if err := mover.RunOnce(ctx); err != nil {
		t.Fatalf("old higher TARGET interrupted live pass: %v", err)
	}
	stale, present, err := manager.ReadRoute(2)
	if err != nil || !present || stale.Epoch != 2 || stale.Owner != rawdb.HistoryStagingOwnerTarget {
		t.Fatalf("old TARGET unexpectedly changed before bounded quarantine: %+v present=%t err=%v", stale, present, err)
	}
	for pass := 0; pass < 32 && !mover.quarantineHotDone; pass++ {
		if err := mover.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !mover.quarantineHotDone {
		t.Fatal("old epoch target/refs/metadata did not drain within bounded passes")
	}
	if _, present, err := manager.ReadRoute(2); err != nil || present {
		t.Fatalf("stale high TARGET metadata remains after quarantine: present=%t err=%v", present, err)
	}
	currentRoute, present, err := manager.ReadRoute(1)
	if err != nil || !present || currentRoute.Epoch != 3 || currentRoute.Owner != rawdb.HistoryStagingOwnerSource {
		t.Fatalf("current replay SOURCE route lost during quarantine: %+v present=%t err=%v", currentRoute, present, err)
	}
}

func TestHistoryStagingMoverExtendsPartialColdBinding(t *testing.T) {
	ctx := context.Background()
	hot, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(t.TempDir(), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{Config: cfg, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	_, genesisHash, err := SetupGenesisBlock(hot, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{
		Version: rawdb.HistoryStagingFormatVersion, GenesisHash: genesisHash, NetworkID: 1,
		SourceID: [32]byte{1}, TargetID: [32]byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureCanonicalSourceRoute(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	proof := rawdb.HistoryStagingProof{Bucket: 1, Epoch: 1, EligibleThrough: 2047,
		FinishBlock: 2047, FinishHash: common.Hash{1}, IndexBlock: 2047, IndexHash: common.Hash{1}}
	var firstRanges, secondRanges []*rawdb.StateTxRange
	for number := uint64(1024); number <= 2047; number++ {
		block := testRestartBlock(number)
		if err := rawdb.WriteBlock(hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(hot, number, block.Hash(), number, number); err != nil {
			t.Fatal(err)
		}
		proof.Blocks = append(proof.Blocks, rawdb.HistoryStagingBlockProof{
			Number: number, Hash: block.Hash(), BeginTxNum: number, EndTxNum: number})
		row := &rawdb.StateTxRange{BlockNum: number, BlockHash: block.Hash(), BeginTxNum: number, EndTxNum: number}
		if number < 1536 {
			firstRanges = append(firstRanges, row)
		} else {
			secondRanges = append(secondRanges, row)
		}
	}
	dir := t.TempDir()
	historyCfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("history dataset missing")
	}
	firstRef := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory,
		FromTxNum: 1024, ToTxNum: 1535, Path: historyCfg.HistoryPath(1024, 1535)}
	h1, i1, a1, err := historyCfg.WriteHistory(dir, firstRef, nil, firstRanges)
	if err != nil {
		t.Fatal(err)
	}
	// Production manifests are contiguous from tx 1. Seed the earlier cold
	// prefix so the Runner's visible frontier is the actual mixed boundary.
	prefixRanges := make([]*rawdb.StateTxRange, 0, 1023)
	for number := uint64(1); number <= 1023; number++ {
		prefixRanges = append(prefixRanges, &rawdb.StateTxRange{
			BlockNum: number, BlockHash: common.Hash{1}, BeginTxNum: number, EndTxNum: number})
	}
	prefixRef := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory,
		FromTxNum: 1, ToTxNum: 1023, Path: historyCfg.HistoryPath(1, 1023)}
	h0, i0, a0, err := historyCfg.WriteHistory(dir, prefixRef, nil, prefixRanges)
	if err != nil {
		t.Fatal(err)
	}
	identity := snapshots.ChainIdentity{ChainID: cfg.ChainID, NetworkID: 1, GenesisHash: genesisHash.Hex()}
	partialManifest := snapshots.NewManifestForChain(1, 1535, []snapshots.SegmentRef{h0, i0, a0, h1, i1, a1}, identity)
	if err := snapshots.PublishManifest(dir, partialManifest); err != nil {
		t.Fatal(err)
	}
	needed := make([]bool, rawdb.StateHistoryChunkBucketBlocks)
	for i := 0; i < 512; i++ {
		needed[i] = true
	}
	proof.ColdSpans, err = snapshots.BuildHistoryStagingColdSpans(ctx, dir, partialManifest, proof.Blocks, needed)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := manager.BeginClaim(ctx, proof, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	limits := rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20,
		MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20,
		MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}
	if _, err := manager.CopyClaim(ctx, claim, limits); err != nil {
		t.Fatal(err)
	}
	binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion,
		Bucket: 1, Epoch: 1, BindingEpoch: 1, ManifestEpoch: partialManifest.Generation, Spans: proof.ColdSpans}
	if err := manager.CertifyColdRange(ctx, binding, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AdoptClaim(ctx, claim, proof); err != nil {
		t.Fatal(err)
	}
	if err := manager.ClearSource(ctx, claim, limits); err != nil {
		t.Fatal(err)
	}
	_ = secondRanges // the real Runner reads these retained TARGET rows
	coldManager, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetStateCodeColdHistory(coldManager)
	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageFinish, 2047, testRestartBlock(2047).Hash()); err != nil {
		t.Fatal(err)
	}
	runner := snapshots.NewRunner(historyStagingRunnerChain{bc: bc, solidified: 2048}, snapshots.Config{
		Dir: dir, Enabled: true, Interval: time.Hour, HistoryWindow: 1, BatchBlocks: 512})
	build, err := runner.OnePass()
	if err != nil || !build.Built || build.FromTxNum != 1536 || build.ToTxNum != 2047 {
		t.Fatalf("routed cold Runner suffix build = %+v err=%v", build, err)
	}
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	mover, err := NewHistoryStagingMover(bc, HistoryStagingMoverConfig{
		HistoryWindow: 1, Cadence: time.Second, HeavyWorkGate: maintenance.NewHeavyWorkGate(),
		HotPressure: pressure, StagePressure: pressure, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	if err := mover.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	route, present, err := manager.ReadRoute(1)
	if err != nil || !present || route.Owner != rawdb.HistoryStagingOwnerCold || !route.TargetCleared {
		t.Fatalf("mixed binding was not completed and retired: route=%+v present=%t err=%v", route, present, err)
	}
	// The posting ETL frontier is already beyond the transferred bucket.
	// Advancing it over the next SOURCE bucket must remain a normal online
	// operation even while older history is COLD-owned.
	if err := manager.EnsureCanonicalSourceRoute(ctx, 1, 2); err != nil {
		t.Fatal(err)
	}
	for number := uint64(2048); number <= 2049; number++ {
		block := testRestartBlock(number)
		if err := rawdb.WriteBlock(hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(hot, number, block.Hash(), number, number); err != nil {
			t.Fatal(err)
		}
	}
	bc.currentBlock.Store(testRestartBlock(2049))
	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageStateHistoryIndex, 2047, testRestartBlock(2047).Hash()); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(hot, rawdb.StageFinish, 2049, testRestartBlock(2049).Hash()); err != nil {
		t.Fatal(err)
	}
	props := bc.DynProps().Copy()
	props.SetLatestSolidifiedBlockNum(2049)
	bc.storeDynPropsCache(props)
	indexed, err := bc.AdvanceStateHistoryIndexStageBatchedInterruptible(1, 2, nil)
	if err != nil || !indexed.Advanced {
		t.Fatalf("posting ETL after mixed cold handoff = %+v err=%v", indexed, err)
	}
}

func TestHistoryStagingMoverKeepsClaimOnColdSemanticMismatch(t *testing.T) {
	ctx := context.Background()
	hot, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(t.TempDir(), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{Config: cfg, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	_, genesisHash, err := SetupGenesisBlock(hot, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{
		Version: rawdb.HistoryStagingFormatVersion, GenesisHash: genesisHash, NetworkID: 1,
		SourceID: [32]byte{1}, TargetID: [32]byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureCanonicalSourceRoute(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	blocks := make([]rawdb.HistoryStagingBlockProof, 0, rawdb.StateHistoryChunkBucketBlocks)
	var firstRanges, secondRanges []*rawdb.StateTxRange
	for number := uint64(1024); number <= 2048; number++ {
		block := testRestartBlock(number)
		if err := rawdb.WriteBlock(hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(hot, number, block.Hash(), number, number); err != nil {
			t.Fatal(err)
		}
		if number <= 2047 {
			blocks = append(blocks, rawdb.HistoryStagingBlockProof{Number: number, Hash: block.Hash(), BeginTxNum: number, EndTxNum: number})
			row := &rawdb.StateTxRange{BlockNum: number, BlockHash: block.Hash(), BeginTxNum: number, EndTxNum: number}
			if number < 1536 {
				firstRanges = append(firstRanges, row)
			} else {
				secondRanges = append(secondRanges, row)
			}
		}
	}
	bc.currentBlock.Store(testRestartBlock(2048))
	props := bc.DynProps().Copy()
	props.SetLatestSolidifiedBlockNum(2048)
	bc.storeDynPropsCache(props)
	for _, stageID := range []rawdb.StageID{rawdb.StageFinish, rawdb.StageStateHistoryIndex} {
		if err := rawdb.WriteStageProgressWithHash(hot, stageID, 2048, testRestartBlock(2048).Hash()); err != nil {
			t.Fatal(err)
		}
	}
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	mover, err := NewHistoryStagingMover(bc, HistoryStagingMoverConfig{HistoryWindow: 1, Cadence: time.Second,
		HeavyWorkGate: maintenance.NewHeavyWorkGate(), HotPressure: pressure, StagePressure: pressure,
		Limits: rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20,
			MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20,
			MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	finish, _, _ := rawdb.ReadStageProgressRow(hot, rawdb.StageFinish)
	indexed, _, _ := rawdb.ReadStageProgressRow(hot, rawdb.StageStateHistoryIndex)
	proof, _, err := mover.collectProofLocked(1, 2047, finish, indexed)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	historyCfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	ref1 := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory,
		FromTxNum: 1024, ToTxNum: 1535, Path: historyCfg.HistoryPath(1024, 1535)}
	ref2 := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory,
		FromTxNum: 1536, ToTxNum: 2047, Path: historyCfg.HistoryPath(1536, 2047)}
	h1, i1, a1, err := historyCfg.WriteHistory(dir, ref1, nil, firstRanges)
	if err != nil {
		t.Fatal(err)
	}
	h2, i2, a2, err := historyCfg.WriteHistory(dir, ref2, nil, secondRanges)
	if err != nil {
		t.Fatal(err)
	}
	identity := snapshots.ChainIdentity{ChainID: cfg.ChainID, NetworkID: 1, GenesisHash: genesisHash.Hex()}
	oldManifest := snapshots.NewManifestForChain(1024, 2047, []snapshots.SegmentRef{h1, i1, a1, h2, i2, a2}, identity)
	needed := make([]bool, len(blocks))
	for i := range needed {
		needed[i] = true
	}
	proof.ColdSpans, err = snapshots.BuildHistoryStagingColdSpans(ctx, dir, oldManifest, blocks, needed)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := manager.BeginClaim(ctx, proof, [32]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	oldID := proof.ColdSpans[0].ContentID
	corrupt := &rawdb.StateDomainChange{BlockNum: 1024, BlockHash: blocks[0].Hash, TxNum: 1024,
		Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41}}
	mergedRef := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory,
		FromTxNum: 1024, ToTxNum: 2047, Path: historyCfg.HistoryPath(1024, 2047)}
	allRanges := append(append([]*rawdb.StateTxRange(nil), firstRanges...), secondRanges...)
	h3, i3, a3, err := historyCfg.WriteHistory(dir, mergedRef, []*rawdb.StateDomainChange{corrupt}, allRanges)
	if err != nil {
		t.Fatal(err)
	}
	currentManifest := snapshots.NewManifestForChain(1024, 2047, []snapshots.SegmentRef{h3, i3, a3}, identity)
	currentManifest.Generation = oldManifest.Generation + 1
	currentManifest.Retired = []snapshots.SegmentRef{h1, i1, a1, h2, i2, a2}
	if err := snapshots.PublishManifest(dir, currentManifest); err != nil {
		t.Fatal(err)
	}
	coldManager, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetStateCodeColdHistory(coldManager)
	for pass := 0; pass < 2; pass++ {
		if err := mover.RunOnce(ctx); !errors.Is(err, snapshots.ErrHistoryStagingColdSemanticMismatch) {
			t.Fatalf("pass %d accepted changed cold semantics: %v", pass, err)
		}
		got, present, err := manager.ReadClaim(1)
		if err != nil || !present || got.ClaimID != claim.ClaimID || got.Cancelled {
			t.Fatalf("claim lost after semantic mismatch: %+v present=%t err=%v", got, present, err)
		}
		if can, err := manager.CanGCCold(oldID); err != nil || can {
			t.Fatalf("old correct trio lost its persistent ref: canGC=%t err=%v", can, err)
		}
	}
}
