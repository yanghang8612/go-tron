package core

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type parallelMoverSharedWriter struct {
	ethdb.KeyValueStore
	batch ethdb.Batch
}

func (w parallelMoverSharedWriter) Put(k, v []byte) error               { return w.batch.Put(k, v) }
func (w parallelMoverSharedWriter) Delete(k []byte) error               { return w.batch.Delete(k) }
func (w parallelMoverSharedWriter) StateHistoryChunkWritesAtomic() bool { return true }

func TestHistoryStagingMoverLocatesCompletedPrefixWithoutHeavyLease(t *testing.T) {
	f := newStagingIndexGCFixture(t)
	for bucket := uint64(1); bucket <= 4097; bucket++ {
		f.cold(t, bucket)
	}
	if err := f.manager.EnsureCanonicalSourceRoute(context.Background(), 1, 4098); err != nil {
		t.Fatal(err)
	}
	gate := maintenance.NewHeavyWorkGate()
	release, _ := gate.TryAcquire()
	defer release()
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	mover, err := NewHistoryStagingMover(f.bc, HistoryStagingMoverConfig{HistoryWindow: 1, Cadence: time.Second, HeavyWorkGate: gate, HotPressure: pressure, StagePressure: pressure})
	if err != nil {
		t.Fatal(err)
	}
	if err := mover.RunOnce(context.Background()); err != nil || mover.nextBucket != 4097 {
		t.Fatalf("unbounded or gate-blocked prefix: next=%d err=%v", mover.nextBucket, err)
	}
	if err := mover.RunOnce(context.Background()); err != nil || mover.nextBucket != 4098 {
		t.Fatalf("prefix did not stop at SOURCE: next=%d err=%v", mover.nextBucket, err)
	}
	// A route gap is not permission to jump to a later row.
	mover.nextBucket = 5000
	if err := mover.advanceCompletedPrefix(context.Background()); err != nil || mover.nextBucket != 5000 {
		t.Fatalf("route gap skipped: %d %v", mover.nextBucket, err)
	}
	mover.prefixEpoch = 2 // simulate a cursor retained from another epoch
	if err := mover.advanceCompletedPrefix(context.Background()); err != nil || mover.nextBucket != 4097 {
		t.Fatalf("epoch failed to restart bounded scan: %d %v", mover.nextBucket, err)
	}
}

func TestHistoryStagingMoverRunsBesideForwardIndexETL(t *testing.T) {
	f := newStagingIndexGCFixture(t)
	ctx := context.Background()
	if err := f.manager.InitializeOfflineSourceRoutes(ctx, 1, 1, 2); err != nil {
		t.Fatal(err)
	}
	owner := common.Address{0x41, 9}
	rawdb.SetStateHistoryCrossBlockDedup(true)
	defer rawdb.SetStateHistoryCrossBlockDedup(false)
	for height := uint64(1024); height <= 2050; height++ {
		block := testRestartBlock(height)
		if err := rawdb.WriteBlock(f.hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(f.hot, height, block.Hash(), height, height); err != nil {
			t.Fatal(err)
		}
		if height == 1024 || height == 2048 || height == 2049 {
			batch := f.hot.NewBatch()
			err := rawdb.WriteStateDomainChangeBlockRows(parallelMoverSharedWriter{f.hot, batch}, []*rawdb.StateDomainChange{{
				BlockNum: height, BlockHash: block.Hash(), TxNum: height, Seq: 1,
				FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner,
				PrevExists: true, Prev: bytes.Repeat([]byte{7}, 256<<10),
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
		}
	}
	view, release, err := rawdb.AcquireStateHistoryReadView(f.hot)
	if err != nil {
		t.Fatal(err)
	}
	for height := uint64(1024); height <= 2047; height++ {
		row, err := rawdb.BuildHistoryStagingBlockComplete(ctx, view, 1, height, testRestartBlock(height).Hash())
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteHistoryStagingBlockComplete(f.hot, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(f.hot, rawdb.StageStateHistoryIndex, 2047, testRestartBlock(2047).Hash()); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(f.hot, rawdb.StageFinish, 2050, testRestartBlock(2050).Hash()); err != nil {
		t.Fatal(err)
	}
	f.bc.currentBlock.Store(testRestartBlock(2051))
	props := f.bc.cachedDynProps().Copy()
	props.SetLatestSolidifiedBlockNum(2051)
	f.bc.storeDynPropsCache(props)
	genesis, _ := rawdb.ReadBlockHash(f.bc.chaindb, 0)
	dir := t.TempDir()
	if err := snapshots.PublishManifest(dir, snapshots.NewManifestForChain(0, 0, nil, snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: fmt.Sprintf("0x%x", genesis[:])})); err != nil {
		t.Fatal(err)
	}
	cold, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.BindHistoryStagingColdRetention(dir, f.manager); err != nil {
		t.Fatal(err)
	}
	f.bc.SetStateCodeColdHistory(cold)
	old, err := f.manager.AcquireView(func() (rawdb.StateHistoryReadView, func() error, error) {
		return rawdb.AcquireStateHistoryReadView(f.hot)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	entered, resume := make(chan struct{}), make(chan struct{})
	var once, resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		_, err := f.bc.AdvanceStateHistoryIndexStageInterruptible(4096, func() bool { once.Do(func() { close(entered); <-resume }); return false })
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("ETL did not reach snapshot barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ETL capture timed out")
	}
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	mover, err := NewHistoryStagingMover(f.bc, HistoryStagingMoverConfig{HistoryWindow: 1, Cadence: time.Second, HeavyWorkGate: maintenance.NewHeavyWorkGate(), HotPressure: pressure, StagePressure: pressure,
		Limits: rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20, MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20, MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}})
	if err != nil {
		unblock()
		<-done
		t.Fatal(err)
	}
	moved := make(chan error, 1)
	go func() { moved <- mover.RunOnce(ctx) }()
	select {
	case err := <-moved:
		if err != nil {
			unblock()
			<-done
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		unblock()
		<-done
		<-moved
		t.Fatal("mover waited for unrelated forward ETL")
	}
	route, present, err := f.manager.ReadRoute(1)
	if err != nil || !present || route.Owner != rawdb.HistoryStagingOwnerTarget || !route.SourceCleared {
		t.Fatalf("handoff not completed beside ETL: %+v %v", route, err)
	}
	fresh, releaseFresh, err := f.bc.AcquireStateHistorySourceView(ctx)
	if err != nil {
		unblock()
		<-done
		t.Fatal(err)
	}
	defer releaseFresh()
	historyCfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if _, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeReadContext(ctx, fresh, t.TempDir(), 1024, 2047, 1024, 2047, historyCfg.HistoryPath(1024, 2047), snapshots.HistoryReadOptions{ChunkCache: true}); err != nil {
		unblock()
		<-done
		t.Fatal(err)
	}
	for _, reader := range []rawdb.StateKVHistoryReader{old, fresh} {
		rows := 0
		if err := rawdb.IterateStateDomainChanges(reader, 1024, func(c *rawdb.StateDomainChange) (bool, error) {
			rows++
			if !bytes.Equal(c.Prev, bytes.Repeat([]byte{7}, 256<<10)) {
				t.Error("old pinned reader changed")
			}
			return true, nil
		}); err != nil || rows != 1 {
			t.Fatalf("old read: rows=%d err=%v", rows, err)
		}
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	row, present, err := rawdb.ReadStageProgressRow(f.hot, rawdb.StageStateHistoryIndex)
	if err != nil || !present || row.BlockNum != 2050 || row.BlockHash != testRestartBlock(2050).Hash() {
		t.Fatalf("ETL watermark: %+v %v", row, err)
	}
	var indexed []uint64
	if err := rawdb.IterateStateAccountLatestChangeBlocks(f.hot, owner, func(block uint64) (bool, error) { indexed = append(indexed, block); return true, nil }); err != nil || len(indexed) != 2 || indexed[0] != 2048 || indexed[1] != 2049 {
		t.Fatalf("forward postings: %v err=%v", indexed, err)
	}
}
