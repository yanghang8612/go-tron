package core

import (
	"context"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

// Offline SOURCE→COLD buckets never owned target payload. They must not
// monopolize the retirement cursor, while a later TARGET→COLD bucket still
// needs its staged payload and receipt retired.
func TestHistoryStagingMoverSkipsDirectColdAndRetiresTargetCold(t *testing.T) {
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
		SourceID: [32]byte{1}, TargetID: [32]byte{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := manager.InitializeOfflineSourceRoutes(ctx, 1, 1, 2); err != nil {
		t.Fatal(err)
	}
	limits := rawdb.HistoryStagingLimits{
		MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20, MaxBucketBytes: 64 << 20,
		MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20, MinFreeBytes: 1,
		FreeBytes: func() (uint64, error) { return 1 << 40, nil },
	}
	binding := func(bucket uint64) rawdb.HistoryStagingColdBinding {
		first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
		if err != nil {
			t.Fatal(err)
		}
		return rawdb.HistoryStagingColdBinding{
			Version: rawdb.HistoryStagingFormatVersion, Bucket: bucket, Epoch: 1,
			BindingEpoch: 1, ManifestEpoch: 1,
			Spans: []rawdb.HistoryStagingColdSpan{{From: first, To: last,
				ContentID: [32]byte{byte(bucket)}, SemanticHash: [32]byte{11}, TxRangeDigest: [32]byte{12}}},
		}
	}
	if err := manager.CertifyColdRange(ctx, binding(1), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := manager.ClearCertifiedSourceRange(ctx, 1, 1024, 2047, limits); err != nil {
		t.Fatal(err)
	}
	if route, present, err := manager.ReadRoute(1); err != nil || !present ||
		route.Owner != rawdb.HistoryStagingOwnerCold || route.SourceCleared || route.TargetCleared {
		t.Fatalf("direct cold route=%+v present=%t err=%v", route, present, err)
	}
	setProgress := func(height uint64) {
		t.Helper()
		block := testRestartBlock(height)
		bc.currentBlock.Store(block)
		props := bc.DynProps().Copy()
		props.SetLatestSolidifiedBlockNum(int64(height))
		bc.storeDynPropsCache(props)
		for _, stageID := range []rawdb.StageID{rawdb.StageFinish, rawdb.StageStateHistoryIndex} {
			if err := rawdb.WriteStageProgressWithHash(hot, stageID, height, block.Hash()); err != nil {
				t.Fatal(err)
			}
		}
	}
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	mover, err := NewHistoryStagingMover(bc, HistoryStagingMoverConfig{
		HistoryWindow: 1, Cadence: time.Second, HeavyWorkGate: maintenance.NewHeavyWorkGate(),
		HotPressure: pressure, StagePressure: pressure, Limits: limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	setProgress(2048) // bucket two is not eligible yet
	if ready, err := mover.candidateHint(ctx); err != nil || ready {
		t.Fatalf("direct cold candidate ready=%t err=%v", ready, err)
	}
	for i := 0; i < 2; i++ {
		if err := mover.RunOnce(ctx); err != nil {
			t.Fatalf("direct cold pass %d: %v", i, err)
		}
	}

	proof := rawdb.HistoryStagingProof{
		Bucket: 2, Epoch: 1, EligibleThrough: 3071,
		FinishBlock: 3071, FinishHash: common.Hash{9},
		IndexBlock: 3071, IndexHash: common.Hash{8},
		Blocks: make([]rawdb.HistoryStagingBlockProof, 0, rawdb.StateHistoryChunkBucketBlocks),
	}
	for number := uint64(2048); number <= 3071; number++ {
		hash := testRestartBlock(number).Hash()
		if err := rawdb.WriteStateTxRange(hot, number, hash, number, number); err != nil {
			t.Fatal(err)
		}
		proof.Blocks = append(proof.Blocks, rawdb.HistoryStagingBlockProof{
			Number: number, Hash: hash, BeginTxNum: number, EndTxNum: number,
		})
	}
	claim, err := manager.BeginClaim(ctx, proof, [32]byte{3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CopyClaim(ctx, claim, limits); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AdoptClaim(ctx, claim, proof); err != nil {
		t.Fatal(err)
	}
	if err := manager.ClearSource(ctx, claim, limits); err != nil {
		t.Fatal(err)
	}
	if err := manager.CertifyColdRange(ctx, binding(2), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleaseTargetToCold(ctx, 2, func(rawdb.HistoryStagingColdBinding) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if route, present, err := manager.ReadRoute(2); err != nil || !present ||
		route.Owner != rawdb.HistoryStagingOwnerCold || !route.SourceCleared || route.TargetCleared {
		t.Fatalf("pending target route=%+v present=%t err=%v", route, present, err)
	}
	setProgress(3072)
	if ready, err := mover.candidateHint(ctx); err != nil || !ready {
		t.Fatalf("pending target candidate ready=%t err=%v", ready, err)
	}
	if err := mover.RunOnce(ctx); err != nil {
		t.Fatalf("retirement pass: %v", err)
	}
	if route, present, err := manager.ReadRoute(2); err != nil || !present || !route.TargetCleared {
		t.Fatalf("pending target not retired: route=%+v present=%t err=%v", route, present, err)
	}
	for i := 0; i < 2; i++ {
		if err := mover.RunOnce(ctx); err != nil {
			t.Fatalf("post-retirement pass %d: %v", i, err)
		}
	}
}
