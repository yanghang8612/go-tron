package core

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

type stagingIndexGCFixture struct {
	bc         *BlockChain
	hot, stage ethdb.KeyValueStore
	manager    *rawdb.HistoryStagingManager
	worker     *HistoryStagingIndexGC
}

func newStagingIndexGCFixture(t *testing.T) *stagingIndexGCFixture {
	t.Helper()
	hot, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := rawdb.NewHistoryStagingPebbleDB(t.TempDir(), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	_, genesis, err := SetupGenesisBlock(hot, &params.Genesis{Config: cfg, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}})
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bc.Close(); _ = stage.Close(); _ = hot.Close() })
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{Version: rawdb.HistoryStagingFormatVersion, GenesisHash: genesis, NetworkID: 1, SourceID: [32]byte{1}, TargetID: [32]byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	for _, height := range []uint64{1023, 2047, 3071, 4095, 4096} {
		if err = rawdb.WriteBlock(hot, testRestartBlock(height)); err != nil {
			t.Fatal(err)
		}
	}
	bc.currentBlock.Store(testRestartBlock(4096))
	props := bc.cachedDynProps()
	props.SetLatestSolidifiedBlockNum(4096)
	bc.storeDynPropsCache(props)
	for _, stageID := range []rawdb.StageID{rawdb.StageFinish, rawdb.StageStateHistoryIndex} {
		if err = rawdb.WriteStageProgressWithHash(hot, stageID, 4096, testRestartBlock(4096).Hash()); err != nil {
			t.Fatal(err)
		}
	}
	bc.historyStagingReady.Store(true)
	worker, err := NewHistoryStagingIndexGC(bc, HistoryStagingIndexGCConfig{HeavyWorkGate: maintenance.NewHeavyWorkGate(), Limits: postingPruneGuardLimits(), Pressure: func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}})
	if err != nil {
		t.Fatal(err)
	}
	return &stagingIndexGCFixture{bc, hot, stage, manager, worker}
}
func (f *stagingIndexGCFixture) cold(t *testing.T, bucket uint64) {
	t.Helper()
	ctx := context.Background()
	if err := f.manager.EnsureCanonicalSourceRoute(ctx, 1, bucket); err != nil {
		t.Fatal(err)
	}
	first, last, err := rawdb.StateHistoryChunkBucketBounds(bucket)
	if err != nil {
		t.Fatal(err)
	}
	binding := rawdb.HistoryStagingColdBinding{Version: rawdb.HistoryStagingFormatVersion, Epoch: 1, Bucket: bucket, BindingEpoch: 1, ManifestEpoch: 1, Spans: []rawdb.HistoryStagingColdSpan{{From: first, To: last, ContentID: [32]byte{1}, SemanticHash: [32]byte{2}, TxRangeDigest: [32]byte{3}}}}
	if err = f.manager.CertifyColdRange(ctx, binding, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	limits := rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20, MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20, MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}
	if err = f.manager.ClearCertifiedSourceRange(ctx, bucket, first, last, limits); err != nil {
		t.Fatal(err)
	}
}
func stagingGCWrite(t *testing.T, db ethdb.KeyValueWriter, owner byte, height uint64) {
	t.Helper()
	if err := rawdb.WriteStateDomainChangePostingIndex(db, &rawdb.StateDomainChange{BlockNum: height, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, owner}}); err != nil {
		t.Fatal(err)
	}
}
func stagingGCCount(t *testing.T, db ethdb.Iteratee, directory bool) int {
	t.Helper()
	p, _, d, _ := rawdb.StateHistoryPostingKeyspaceBounds()
	if directory {
		p = d
	}
	it := db.NewIterator(p, nil)
	defer it.Release()
	n := 0
	for it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return n
}
func (f *stagingIndexGCFixture) run(t *testing.T) {
	t.Helper()
	if err := f.worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryStagingIndexGCContinuousPrefixFixedSweepAndOldView(t *testing.T) {
	f := newStagingIndexGCFixture(t)
	f.cold(t, 1)
	if err := f.manager.EnsureCanonicalSourceRoute(context.Background(), 1, 2); err != nil {
		t.Fatal(err)
	}
	f.cold(t, 3) // the hole at bucket 2 stops certification
	stagingGCWrite(t, f.hot, 1, 1023)
	stagingGCWrite(t, f.hot, 2, 1024)
	stagingGCWrite(t, f.hot, 3, 3072)
	f.bc.chainmu.Lock()
	old, err := f.manager.AcquireView(func() (rawdb.StateHistoryReadView, func() error, error) {
		v, e := f.bc.buffer.NewReadSnapshot()
		if e != nil {
			return nil, nil, e
		}
		return v, v.Close, nil
	}, nil)
	f.bc.chainmu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	unchanged := stagingGCUnrelatedRows(t, f.hot)
	f.run(t)
	if after := stagingGCUnrelatedRows(t, f.hot); !reflect.DeepEqual(unchanged, after) {
		t.Fatal("GC changed non-index key families")
	}
	if f.worker.prefix.through != 2047 || f.worker.sweep.through != 2047 || !f.worker.directoryPhase || stagingGCCount(t, f.hot, false) != 2 {
		t.Fatalf("first pass prefix=%+v sweep=%+v directory=%v", f.worker.prefix, f.worker.sweep, f.worker.directoryPhase)
	}
	f.cold(t, 2)
	f.run(t) // growing prefix must not change this sweep's cutoff
	if f.worker.completed != 2047 || f.worker.prefix.through != 4095 || stagingGCCount(t, f.hot, true) != 2 {
		t.Fatalf("completion=%d prefix=%d", f.worker.completed, f.worker.prefix.through)
	}
	if stagingGCCount(t, old, false) != 3 || stagingGCCount(t, old, true) != 3 {
		t.Fatal("old routed snapshot lost MVCC indices")
	}
	f.run(t)
	f.run(t)
	if f.worker.completed != 4095 || stagingGCCount(t, f.hot, false) != 1 || stagingGCCount(t, f.hot, true) != 1 {
		t.Fatal("second sweep did not collect newly cold postings")
	}
	// A later canonical write republishes both the directory and its posting.
	f.bc.chainmu.Lock()
	f.bc.buffer.BeginBlock(testRestartBlock(4097).Hash(), 4097)
	stagingGCWrite(t, f.bc.buffer, 2, 4097)
	f.bc.buffer.CommitBlock()
	f.bc.chainmu.Unlock()
	view, err := f.bc.buffer.NewReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if stagingGCCount(t, view, false) != 2 || stagingGCCount(t, view, true) != 2 {
		t.Fatal("canonical republish did not restore pair")
	}
}

func TestHistoryStagingIndexGCDefersAndInvalidates(t *testing.T) {
	for _, name := range []string{"not-ready", "index-writer", "chain-writer", "gate", "pressure", "finish-lag", "index-lag", "solid-lag", "unsettled", "flush-error", "commit-error", "missing-manager", "replacement-manager", "anchor"} {
		t.Run(name, func(t *testing.T) {
			f := newStagingIndexGCFixture(t)
			f.cold(t, 1)
			stagingGCWrite(t, f.hot, 1, 1024)
			stagingGCWrite(t, f.hot, 2, 1025)
			f.worker.cfg.Limits.MaxScannedRows = 1
			f.run(t)
			if f.worker.prefix.through != 2047 || len(f.worker.postingCursor) == 0 || f.worker.directoryPhase {
				t.Fatal("fixture failed to establish authority and cursor")
			}
			before := bytes.Clone(f.worker.postingCursor)
			switch name {
			case "not-ready":
				f.bc.historyStagingReady.Store(false)
			case "index-writer":
				f.bc.stateHistoryIndexMu.Lock()
				defer f.bc.stateHistoryIndexMu.Unlock()
			case "chain-writer":
				f.bc.chainmu.Lock()
				defer f.bc.chainmu.Unlock()
			case "gate":
				release, ok := f.worker.cfg.HeavyWorkGate.TryAcquire()
				if !ok {
					t.Fatal("gate")
				}
				defer release()
			case "pressure":
				f.worker.cfg.Pressure = func() maintenance.StoragePressure { return maintenance.StoragePressure{} }
			case "finish-lag", "index-lag":
				stage := rawdb.StageFinish
				if name == "index-lag" {
					stage = rawdb.StageStateHistoryIndex
				}
				if err := rawdb.WriteStageProgressWithHash(f.hot, stage, 1023, testRestartBlock(1023).Hash()); err != nil {
					t.Fatal(err)
				}
			case "solid-lag":
				props := f.bc.cachedDynProps()
				props.SetLatestSolidifiedBlockNum(1023)
				f.bc.storeDynPropsCache(props)
			case "unsettled":
				f.bc.buffer.BeginBlock(testRestartBlock(1024).Hash(), 1024)
				f.bc.buffer.CommitBlock()
			case "flush-error":
				e := errors.New("flush fault")
				f.bc.flushErr.Store(&e)
				defer f.bc.flushErr.Store(nil)
			case "commit-error":
				e := errors.New("commit fault")
				f.bc.commitErr.Store(&e)
				defer f.bc.commitErr.Store(nil)
			case "missing-manager":
				f.bc.historyStaging.Store(nil)
			case "replacement-manager":
				m, err := rawdb.NewHistoryStagingManager(f.hot, f.stage, f.worker.identity)
				if err != nil {
					t.Fatal(err)
				}
				f.bc.historyStaging.Store(m)
			case "anchor":
				f.worker.prefix = stagingIndexAuthority{epoch: 1, through: 2047, anchor: common.Hash{99}}
			}
			err := f.worker.RunOnce(context.Background())
			if (name == "flush-error" || name == "commit-error") != (err != nil) {
				t.Fatalf("err=%v", err)
			}
			if stagingGCCount(t, f.hot, false) != 1 {
				t.Fatal("deferred pass changed index")
			}
			invalidated := name == "not-ready" || name == "missing-manager" || name == "replacement-manager" || name == "anchor" || name == "finish-lag" || name == "index-lag" || name == "solid-lag"
			if invalidated {
				if f.worker.prefix.through != 0 || len(f.worker.postingCursor) != 0 {
					t.Fatal("authority/cursor not invalidated")
				}
			} else if !bytes.Equal(before, f.worker.postingCursor) {
				t.Fatal("temporary deferral progressed cursor")
			}
		})
	}
}

type stagingGCFlushGate struct {
	ethdb.KeyValueStore
	entered, release chan struct{}
	once             sync.Once
}
type stagingGCFlushBatch struct {
	ethdb.Batch
	gate *stagingGCFlushGate
}

func (g *stagingGCFlushGate) NewBatch() ethdb.Batch {
	return &stagingGCFlushBatch{g.KeyValueStore.NewBatch(), g}
}
func (g *stagingGCFlushGate) NewBatchWithSize(size int) ethdb.Batch {
	return &stagingGCFlushBatch{g.KeyValueStore.NewBatchWithSize(size), g}
}
func (b *stagingGCFlushBatch) Write() error {
	b.gate.once.Do(func() { close(b.gate.entered) })
	<-b.gate.release
	return b.Batch.Write()
}
func TestHistoryStagingIndexGCBusySnapshotRetainsCursorAndInflightProtectsDirectory(t *testing.T) {
	f := newStagingIndexGCFixture(t)
	f.cold(t, 1)
	stagingGCWrite(t, f.hot, 1, 1024)
	f.run(t)
	if !f.worker.directoryPhase {
		t.Fatal("posting phase incomplete")
	}
	f.bc.chainmu.Lock()
	f.bc.buffer.BeginBlock(testRestartBlock(4097).Hash(), 4097)
	stagingGCWrite(t, f.bc.buffer, 1, 4097)
	f.bc.buffer.CommitBlock()
	f.bc.chainmu.Unlock()
	gate := &stagingGCFlushGate{KeyValueStore: f.hot, entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	var unblock sync.Once
	defer unblock.Do(func() { close(gate.release) })
	go func() { done <- f.bc.buffer.FlushUpTo(4097, gate) }()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}
	before := bytes.Clone(f.worker.directoryCursor)
	f.run(t)
	if !bytes.Equal(before, f.worker.directoryCursor) || !f.worker.directoryPhase || f.worker.counts["deferred/snapshot_busy"] != 1 || f.worker.counts["directory/chunks"] != 0 {
		t.Fatal("busy snapshot progressed")
	}
	if !f.bc.chainmu.TryLock() {
		t.Fatal("chain guard leaked")
	}
	f.bc.chainmu.Unlock()
	if !f.bc.stateHistoryIndexMu.TryLock() {
		t.Fatal("index guard leaked")
	}
	f.bc.stateHistoryIndexMu.Unlock()
	release, ok := f.worker.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		t.Fatal("heavy gate leaked")
	}
	release()
	unblock.Do(func() { close(gate.release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	f.run(t)
	if stagingGCCount(t, f.hot, true) != 1 {
		t.Fatal("newer posting did not protect directory")
	}
	// Also protect a directory while its new posting exists only in inflight.
	stagingGCWrite(t, f.hot, 2, 1024)
	f.bc.chainmu.Lock()
	f.bc.buffer.BeginBlock(testRestartBlock(4098).Hash(), 4098)
	stagingGCWrite(t, f.bc.buffer, 2, 4098)
	f.bc.chainmu.Unlock()
	f.worker.sweep = f.worker.prefix
	f.worker.directoryPhase = false
	f.run(t)
	f.run(t)
	if stagingGCCount(t, f.hot, true) != 2 {
		t.Fatal("inflight posting did not protect directory")
	}
}

func TestHistoryStagingIndexGCResetEpochAndErrorRetainTraversal(t *testing.T) {
	f := newStagingIndexGCFixture(t)
	f.cold(t, 1)
	stagingGCWrite(t, f.hot, 1, 1024)
	// A previous epoch's cursor must never hide current-epoch rows.
	f.worker.prefix = stagingIndexAuthority{epoch: 9, through: 2047, anchor: testRestartBlock(2047).Hash()}
	f.worker.postingCursor = []byte("invalid old cursor")
	f.worker.directoryCursor = []byte("old directory")
	f.run(t)
	if f.worker.prefix.epoch != 1 || stagingGCCount(t, f.hot, false) != 0 || !f.worker.directoryPhase {
		t.Fatal("old epoch traversal reused")
	}
	cursor := bytes.Clone(f.worker.postingCursor)
	fault := errors.New("canonical lookup fault")
	wrapper := &postingPruneGuardDB{KeyValueStore: f.hot, readErr: fault}
	f.bc.db = wrapper
	if err := f.worker.RunOnce(context.Background()); !errors.Is(err, fault) {
		t.Fatalf("lookup error=%v", err)
	}
	if !bytes.Equal(cursor, f.worker.postingCursor) || !f.worker.directoryPhase {
		t.Fatal("read error advanced traversal")
	}
	f.bc.db = f.hot
	intent := rawdb.HistoryStagingResetIntent{Version: rawdb.HistoryStagingFormatVersion, OldEpoch: 1, NewEpoch: 2, TargetHeight: 4096, TargetHash: testRestartBlock(4096).Hash(), BlockDigest: [32]byte{1}}
	if err := f.manager.BeginResetIntent(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	f.run(t)
	if f.worker.prefix.through != 0 || f.worker.sweep.through != 0 || len(f.worker.postingCursor) != 0 || len(f.worker.directoryCursor) != 0 {
		t.Fatal("reset retained authority")
	}
	// Complete a real manager epoch transition. Replay creates SOURCE routes,
	// which must be revisited rather than inheriting old COLD authorization.
	if err := f.manager.WriteReplaySourceRoutes(context.Background(), 2, 0, 4, 4096); err != nil {
		t.Fatal(err)
	}
	intent.ReadyThrough, intent.Complete = 4096, true
	if err := f.manager.CompleteResetIntent(context.Background(), intent, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	f.run(t)
	if f.worker.prefix.epoch != 2 || f.worker.prefix.through != 0 || f.worker.counts["deferred/prefix"] == 0 {
		t.Fatal("epoch transition inherited old certificate")
	}
}

// Replace a known encoded metadata row without duplicating private key prefixes.
// This deliberately constructs invalid protocol combinations to exercise the
// worker's defensive checks; legitimate publications use manager operations.
func stagingGCReplaceMetadata(t *testing.T, db ethdb.KeyValueStore, old, new any) {
	t.Helper()
	before, err := rlp.EncodeToBytes(old)
	if err != nil {
		t.Fatal(err)
	}
	after, err := rlp.EncodeToBytes(new)
	if err != nil {
		t.Fatal(err)
	}
	it := db.NewIterator(nil, nil)
	var key []byte
	for it.Next() {
		if bytes.Equal(it.Value(), before) {
			if key != nil {
				t.Fatal("ambiguous fixture row")
			}
			key = bytes.Clone(it.Key())
		}
	}
	err = it.Error()
	it.Release()
	if err != nil {
		t.Fatal(err)
	}
	if key == nil {
		t.Fatal("missing fixture metadata")
	}
	if err = db.Put(key, after); err != nil {
		t.Fatal(err)
	}
}
func TestHistoryStagingIndexGCRejectsIncompleteColdAuthority(t *testing.T) {
	for _, name := range []string{"SOURCE", "TARGET", "partial-binding", "binding-mismatch", "claim"} {
		t.Run(name, func(t *testing.T) {
			f := newStagingIndexGCFixture(t)
			f.cold(t, 1)
			stagingGCWrite(t, f.hot, 1, 1024)
			route, present, err := f.manager.ReadRoute(1)
			if err != nil || !present {
				t.Fatal(err)
			}
			switch name {
			case "SOURCE", "TARGET":
				next := route
				next.Owner = rawdb.HistoryStagingOwnerSource
				if name == "TARGET" {
					next.Owner = rawdb.HistoryStagingOwnerTarget
					next.ReceiptDigest = [32]byte{1}
				}
				stagingGCReplaceMetadata(t, f.hot, route, next)
			case "partial-binding", "binding-mismatch":
				binding, ok, err := f.manager.ReadColdBindingAt(1, 1)
				if err != nil || !ok {
					t.Fatal(err)
				}
				next := binding
				next.Spans = append([]rawdb.HistoryStagingColdSpan(nil), binding.Spans...)
				if name == "partial-binding" {
					next.Spans[0].To--
				} else {
					next.BindingEpoch++
				}
				stagingGCReplaceMetadata(t, f.hot, binding, next)
			case "claim":
				source := route
				source.Owner = rawdb.HistoryStagingOwnerSource
				stagingGCReplaceMetadata(t, f.hot, route, source)
				proof := rawdb.HistoryStagingProof{Epoch: 1, Bucket: 1, EligibleThrough: 2047, FinishBlock: 4096, FinishHash: testRestartBlock(4096).Hash(), IndexBlock: 4096, IndexHash: testRestartBlock(4096).Hash()}
				for height := uint64(1024); height <= 2047; height++ {
					proof.Blocks = append(proof.Blocks, rawdb.HistoryStagingBlockProof{Number: height, Hash: testRestartBlock(height).Hash(), BeginTxNum: height, EndTxNum: height})
				}
				if _, err := f.manager.BeginClaim(context.Background(), proof, [32]byte{1}); err != nil {
					t.Fatal(err)
				}
				stagingGCReplaceMetadata(t, f.hot, source, route)
			}
			f.run(t)
			f.run(t)
			if f.worker.prefix.through != 0 || len(f.worker.postingCursor) != 0 || stagingGCCount(t, f.hot, false) != 1 || f.worker.counts["deferred/prefix"] != 2 {
				t.Fatal("incomplete authority accepted or hole skipped")
			}
		})
	}
}

func stagingGCUnrelatedRows(t *testing.T, db ethdb.KeyValueStore) map[string]string {
	t.Helper()
	posting, _, directory, _ := rawdb.StateHistoryPostingKeyspaceBounds()
	it := db.NewIterator(nil, nil)
	defer it.Release()
	rows := make(map[string]string)
	for it.Next() {
		if !bytes.HasPrefix(it.Key(), posting) && !bytes.HasPrefix(it.Key(), directory) {
			rows[string(it.Key())] = string(it.Value())
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestHistoryStagingIndexGCSlowProofBudgetStillProgresses(t *testing.T) {
	f := newStagingIndexGCFixture(t)
	f.cold(t, 1)
	f.cold(t, 2)
	for i := byte(1); i <= 3; i++ {
		stagingGCWrite(t, f.hot, i, 1024+uint64(i))
	}
	// Every mandatory real Pebble proof/capture takes longer than this budget.
	// It must restrict work to one row, rather than permanently reject all work.
	f.worker.cfg.Limits.MaxDuration = time.Nanosecond
	for pass := 0; pass < 20 && f.worker.completed != 3071; pass++ {
		before := f.worker.counts["posting/scanned_rows"] + f.worker.counts["directory/scanned_rows"]
		f.run(t)
		after := f.worker.counts["posting/scanned_rows"] + f.worker.counts["directory/scanned_rows"]
		if after-before > 1 {
			t.Fatal("exhausted guard budget scanned more than one row")
		}
	}
	if f.worker.completed != 3071 || stagingGCCount(t, f.hot, false) != 0 || stagingGCCount(t, f.hot, true) != 0 || f.worker.counts["budget/first_row"] == 0 {
		t.Fatal("slow proofs starved bounded GC progress")
	}
}

func TestHistoryStagingIndexGCDelay(t *testing.T) {
	for _, tc := range []struct{ elapsed, want time.Duration }{{0, 100 * time.Millisecond}, {-time.Second, 100 * time.Millisecond}, {10 * time.Millisecond, 100 * time.Millisecond}, {50 * time.Millisecond, 450 * time.Millisecond}, {time.Second, 9 * time.Second}, {time.Duration(1<<63 - 1), time.Duration(1<<63 - 1)}} {
		if got := historyStagingIndexGCDelay(100*time.Millisecond, tc.elapsed); got != tc.want {
			t.Errorf("elapsed=%s got=%s want=%s", tc.elapsed, got, tc.want)
		}
	}
}
