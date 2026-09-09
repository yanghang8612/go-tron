package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func seedParallelHistoryEvent(t testing.TB, db ethdb.KeyValueStore, blocks, txs, changes, payload int) {
	t.Helper()
	random := rand.New(rand.NewSource(909))
	for number := 1; number <= blocks; number++ {
		txPBs := make([]*corepb.Transaction, txs)
		infos := make([]*corepb.TransactionInfo, txs)
		for tx := range txPBs {
			txPBs[tx] = &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: int64(number*1000 + tx), Data: []byte("parallel-source")}}
			hash := coretypes.NewTransactionFromPB(txPBs[tx]).Hash()
			data := make([]byte, payload)
			random.Read(data)
			log := &corepb.TransactionInfo_Log{Address: eventLogTestAddress(byte(tx%8 + 1)), Topics: [][]byte{common.Hash{byte(tx % 16)}.Bytes()}, Data: data}
			log.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			infos[tx] = &corepb.TransactionInfo{Id: hash.Bytes(), BlockNumber: int64(number), BlockTimeStamp: int64(number * 3000), Log: []*corepb.TransactionInfo_Log{log}}
		}
		block := coretypes.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number * 3000)}}, Transactions: txPBs})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, uint64(number), infos); err != nil {
			t.Fatal(err)
		}
		begin := uint64((number-1)*txs + 1)
		if err := rawdb.WriteStateTxRange(db, uint64(number), block.Hash(), begin, begin+uint64(txs)-1); err != nil {
			t.Fatal(err)
		}
		rows := make([]*rawdb.StateDomainChange, 0, txs*changes)
		for tx := 0; tx < txs; tx++ {
			for j := 0; j < changes; j++ {
				prev := make([]byte, payload)
				random.Read(prev)
				key := make([]byte, 16)
				binary.BigEndian.PutUint64(key, uint64(tx))
				binary.BigEndian.PutUint64(key[8:], uint64(j))
				rows = append(rows, &rawdb.StateDomainChange{BlockNum: uint64(number), BlockHash: block.Hash(), TxNum: begin + uint64(tx), Seq: uint64(tx*changes + j + 1), FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: coldBuilderOwner(byte(tx%8 + 1)), Domain: kvdomains.ContractStorage, Key: key, PrevExists: true, Prev: prev})
			}
		}
		if err := rawdb.WriteStateDomainChangeBlockRows(db, rows); err != nil {
			t.Fatal(err)
		}
	}
}

func parallelHistoryEventRunner(chain ChainSource, dir string, blocks, txs int) *Runner {
	r := NewRunner(chain, Config{
		Dir: dir, Enabled: true, HistoryWindow: 1, BatchBlocks: uint64(blocks) * 4,
		BatchTxNums: uint64(blocks*txs) * 4, MetricsNamespace: dir,
		HistoryCatchupMode: HistoryCatchupThroughput,
		BuildEventLogs:     true, EventLogVersion: EventLogSegmentV4Version,
		DeferDerivedSidecarsWhileSyncing: true, BuildEventLogsWhileSyncing: true,
		HeavyWorkGate:             maintenance.NewHeavyWorkGate(),
		HistoryLoadProbe:          func() maintenance.StoragePressure { return healthyHistoryLoad(time.Now()) },
		ParallelHistoryEventReady: func() bool { return true },
	})
	r.historyLoad.level, r.historyLoad.good = 3, 3
	r.historyLoad.sample = healthyHistoryLoad(time.Now())
	return r
}

func TestParallelHistoryEventAdmission(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	for _, tc := range []struct {
		name   string
		mutate func(*Runner)
	}{
		{"nil CPU probe", func(r *Runner) { r.cfg.ParallelHistoryEventReady = nil }},
		{"CPU busy", func(r *Runner) { r.cfg.ParallelHistoryEventReady = func() bool { return false } }},
		{"no lease", func(r *Runner) { r.cfg.HeavyWorkGate = nil }},
		{"balanced", func(r *Runner) { r.cfg.HistoryCatchupMode = HistoryCatchupBalanced }},
		{"idle", func(r *Runner) { r.chain.(*coldBuilderChain).syncRemainingOK = false; r.cfg.HistoryLoadProbe = nil }},
		{"unknown engine", func(r *Runner) { r.historyLoad.sample.Available = false }},
		{"stale engine", func(r *Runner) { r.historyLoad.sample.SampledAt = time.Now().Add(-16 * time.Second) }},
		{"unknown device", func(r *Runner) { r.historyLoad.sample.DeviceAvailable = false }},
		{"stale device", func(r *Runner) { r.historyLoad.sample.DeviceSampledAt = time.Now().Add(-16 * time.Second) }},
		{"high latency", func(r *Runner) { r.historyLoad.sample.DeviceAwait = 3 * time.Millisecond }},
		{"soft pressure", func(r *Runner) { r.historyLoad.level = 1 }},
		{"hard pressure", func(r *Runner) { r.historyLoad.sample.WriteStalled = true }},
		{"legacy event", func(r *Runner) { r.cfg.EventLogVersion = EventLogSegmentVersion }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := parallelHistoryEventRunner(&coldBuilderChain{syncRemainingOK: true, syncRemaining: 1000}, t.TempDir(), 8, 4)
			if !r.parallelHistoryEventReady(true, false, time.Now()) {
				t.Fatal("healthy fixture did not permit pair")
			}
			tc.mutate(r)
			if r.parallelHistoryEventReady(true, false, time.Now()) {
				t.Fatal("unsafe pair admitted")
			}
		})
	}
	r := parallelHistoryEventRunner(&coldBuilderChain{syncRemainingOK: true, syncRemaining: 1000}, t.TempDir(), 8, 4)
	if r.parallelHistoryEventReady(false, false, time.Now()) || r.parallelHistoryEventReady(true, true, time.Now()) {
		t.Fatal("unpaired or idle-derived path changed")
	}
	runtime.GOMAXPROCS(4)
	if r.parallelHistoryEventReady(true, false, time.Now()) {
		t.Fatal("small CPU pool admitted pair")
	}
}

// Barriers are activated only at the final CPU admission callback, after range
// selection and the common gate lease. Each facade owns one independent read
// operation over the same real Pebble, without assuming private DB prefixes.
type parallelReadBarrier struct {
	ethdb.KeyValueStore
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
}

func newParallelReadBarrier(db ethdb.KeyValueStore) *parallelReadBarrier {
	return &parallelReadBarrier{KeyValueStore: db, entered: make(chan struct{}), release: make(chan struct{})}
}
func (b *parallelReadBarrier) unblock() { b.once.Do(func() { close(b.release) }) }
func (b *parallelReadBarrier) before() error {
	if b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
		return b.err
	}
	return nil
}
func (b *parallelReadBarrier) Get(key []byte) ([]byte, error) {
	if err := b.before(); err != nil {
		return nil, err
	}
	return b.KeyValueStore.Get(key)
}
func (b *parallelReadBarrier) Has(key []byte) (bool, error) {
	if err := b.before(); err != nil {
		return false, err
	}
	return b.KeyValueStore.Has(key)
}
func (b *parallelReadBarrier) NewIterator(prefix, start []byte) ethdb.Iterator {
	if err := b.before(); err != nil {
		return &parallelErrorIterator{err: err}
	}
	return b.KeyValueStore.NewIterator(prefix, start)
}

type parallelErrorIterator struct{ err error }

func (*parallelErrorIterator) Next() bool     { return false }
func (i *parallelErrorIterator) Error() error { return i.err }
func (*parallelErrorIterator) Key() []byte    { return nil }
func (*parallelErrorIterator) Value() []byte  { return nil }
func (*parallelErrorIterator) Release()       {}

type parallelHistoryEventChain struct {
	*coldBuilderChain
	events *rawdb.ChainDB
}

func (c *parallelHistoryEventChain) EventLogDB() *rawdb.ChainDB { return c.events }

func waitParallelRead(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("builder did not enter while its sibling was blocked")
	}
}

func TestParallelHistoryEventRealSourcesJoinBeforePublication(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	for _, mode := range []string{"success", "history-error", "event-error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			seedParallelHistoryEvent(t, db, 8, 4, 2, 128)
			h, e := newParallelReadBarrier(db), newParallelReadBarrier(db)
			defer h.unblock()
			defer e.unblock()
			chain := &parallelHistoryEventChain{&coldBuilderChain{db: h, solidified: 9, syncRemaining: 1000, syncRemainingOK: true}, rawdb.NewChainDB(e, nil)}
			r := parallelHistoryEventRunner(chain, t.TempDir(), 8, 4)
			r.cfg.ParallelHistoryEventReady = func() bool { h.armed.Store(true); e.armed.Store(true); return true }
			// A prior failed attempt may already have produced identical files.
			historyCfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			preexisting, err := BuildStateDomainChangeHistorySegmentsFromDBByBlockRange(db, r.cfg.Dir, 1, 32, 1, 8, historyCfg.HistoryPath(1, 32))
			if err != nil {
				t.Fatal(err)
			}
			wantFiles := make(map[string][]byte)
			for _, ref := range preexisting {
				b, err := os.ReadFile(filepath.Join(r.cfg.Dir, ref.Path))
				if err != nil {
					t.Fatal(err)
				}
				wantFiles[ref.Path] = b
			}
			errSource := errors.New("injected source failure")
			if mode == "history-error" {
				h.err = errSource
			}
			if mode == "event-error" {
				e.err = errSource
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type outcome struct {
				result PassResult
				err    error
			}
			done := make(chan outcome, 1)
			var callbacks atomic.Int32
			go func() {
				res, err := r.OnePassWithMaintenanceContext(ctx, func(context.Context, PassResult) error { callbacks.Add(1); return nil })
				done <- outcome{res, err}
			}()
			// Cleanup always unblocks both real readers and joins before closing DB.
			joined := false
			defer func() {
				h.unblock()
				e.unblock()
				if !joined {
					<-done
				}
			}()
			waitParallelRead(t, h.entered)
			waitParallelRead(t, e.entered)
			if mode == "event-error" {
				e.unblock()
			} else {
				h.unblock()
			}
			if mode == "cancel" {
				cancel()
			}
			select {
			case got := <-done:
				joined = true
				t.Fatalf("returned before sibling exited: %+v", got)
			case <-time.After(20 * time.Millisecond):
			}
			if release, ok := r.cfg.HeavyWorkGate.TryAcquire(); ok {
				release()
				t.Fatal("lease released before join")
			}
			if _, err := LoadProductionManifest(r.cfg.Dir); !os.IsNotExist(err) {
				t.Fatalf("published before join: %v", err)
			}
			if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || ok {
				t.Fatalf("stage published early: %v %v", ok, err)
			}
			if callbacks.Load() != 0 {
				t.Fatal("prune callback ran before join")
			}
			h.unblock()
			e.unblock()
			got := <-done
			joined = true
			if !got.result.HistoryEventParallel {
				t.Fatal("real pair path was not used")
			}
			if mode == "success" {
				if got.err != nil || !got.result.Built || !got.result.EventLogBuilt || len(got.result.Segments) != 5 || callbacks.Load() != 1 {
					t.Fatalf("publication: %+v %v", got.result, got.err)
				}
				if got.result.BuildDuration < got.result.HistoryDuration || got.result.BuildDuration < got.result.EventLogDuration {
					t.Fatal("whole build wall shorter than a branch")
				}
				if r.historyEventMetrics.builds.Snapshot().Count() != 1 || r.historyEventMetrics.lastPaired.Snapshot().Value() != 1 {
					t.Fatal("parallel completion metrics missing")
				}
			} else {
				wantErr := errSource
				if mode == "cancel" {
					wantErr = context.Canceled
				}
				if !errors.Is(got.err, wantErr) || got.result.Built || callbacks.Load() != 0 {
					t.Fatalf("failed pair published: %+v %v", got.result, got.err)
				}
				if _, err := LoadProductionManifest(r.cfg.Dir); !os.IsNotExist(err) {
					t.Fatalf("failed manifest: %v", err)
				}
				if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || ok {
					t.Fatalf("failed stage: %v %v", ok, err)
				}
				if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
					t.Fatalf("failed source was pruned: %v %v", ok, err)
				}
				for path, want := range wantFiles {
					got, err := os.ReadFile(filepath.Join(r.cfg.Dir, path))
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("existing output changed/deleted %s: %v", path, err)
					}
				}
				// Releasing canceled/failed work must make a later retry possible.
				if release, ok := r.cfg.HeavyWorkGate.TryAcquire(); !ok {
					t.Fatal("joined pair leaked lease")
				} else {
					release()
				}
				h.err, e.err = nil, nil
				r.cfg.ParallelHistoryEventReady = func() bool { return true }
				r.historyNotBefore.Store(time.Now().Add(-time.Second).UnixNano())
				retry, err := r.OnePass()
				if err != nil || !retry.Built || !retry.EventLogBuilt {
					t.Fatalf("retry: %+v %v", retry, err)
				}
			}
		})
	}
}

func TestParallelHistoryEventSameFilesAndQueries(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	var wantRefs []SegmentRef
	var wantChanges []*rawdb.StateDomainChange
	var wantLogs []EventLog
	for _, parallel := range []bool{false, true} {
		db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		seedParallelHistoryEvent(t, db, 16, 8, 3, 512)
		r := parallelHistoryEventRunner(&coldBuilderChain{db: rawdb.NewChainDB(db, nil), solidified: 17, syncRemaining: 1000, syncRemainingOK: true}, t.TempDir(), 16, 8)
		r.cfg.ParallelHistoryEventReady = func() bool { return parallel }
		// The importer independently retires downloader staging rows. These are
		// not canonical body/receipt/history rows and must not invalidate either
		// live reader. Run the real write/delete accessors through the same Pebble.
		staged, ok, err := rawdb.ReadBlockStrict(rawdb.NewChainDB(db, nil), 16)
		if err != nil || !ok {
			t.Fatalf("staged fixture: %v %v", ok, err)
		}
		stopCleanup, cleanupDone := make(chan struct{}), make(chan error, 1)
		var stopOnce sync.Once
		stop := func() { stopOnce.Do(func() { close(stopCleanup) }) }
		go func() {
			for {
				select {
				case <-stopCleanup:
					cleanupDone <- nil
					return
				default:
				}
				if err := rawdb.WriteSyncStagedBlock(db, staged); err != nil {
					cleanupDone <- err
					return
				}
				if _, err := rawdb.DeleteSyncStagedBlocksThrough(db, 16); err != nil {
					cleanupDone <- err
					return
				}
			}
		}()
		result, err := r.OnePass()
		stop()
		if cleanupErr := <-cleanupDone; cleanupErr != nil {
			t.Fatal(cleanupErr)
		}
		if err != nil || !result.Built || result.HistoryEventParallel != parallel {
			t.Fatalf("pair=%v: %+v %v", parallel, result, err)
		}
		if _, err := VerifyManifestFiles(r.cfg.Dir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
			t.Fatal(err)
		}
		mgr, err := OpenManager(r.cfg.Dir)
		if err != nil {
			t.Fatal(err)
		}
		var changes []*rawdb.StateDomainChange
		if err := mgr.IterateStateDomainChanges(1, 128, func(change *rawdb.StateDomainChange) (bool, error) {
			copy := *change
			copy.Key = bytes.Clone(change.Key)
			copy.Prev = bytes.Clone(change.Prev)
			changes = append(changes, &copy)
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		var logs []EventLog
		if err := mgr.IterateEventLogs(1, 16, EventLogFilter{}, func(row EventLog) (bool, error) {
			row.Log = proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
			logs = append(logs, row)
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(changes) != 16*8*3 || len(logs) != 16*8 {
			t.Fatalf("incomplete queries: %d %d", len(changes), len(logs))
		}
		if !parallel {
			wantRefs, wantChanges, wantLogs = result.Segments, changes, logs
			continue
		}
		if !reflect.DeepEqual(result.Segments, wantRefs) || !reflect.DeepEqual(changes, wantChanges) {
			t.Fatal("parallel files/history differ")
		}
		for i, row := range logs {
			want := wantLogs[i]
			if row.BlockNum != want.BlockNum || row.TxIndex != want.TxIndex || row.LogIndex != want.LogIndex || row.TxHash != want.TxHash || row.BlockHash != want.BlockHash || row.Address != want.Address || !proto.Equal(row.Log, want.Log) {
				t.Fatalf("parallel log %d differs", i)
			}
		}
	}
}

func TestParallelHistoryEventPostPublicationFailureKeepsFiles(t *testing.T) {
	old := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(old)
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	seedParallelHistoryEvent(t, db, 8, 4, 2, 128)
	errStage := errors.New("stage write failed after publication")
	failing := &coldBuilderFailStageDB{store: db, failStage: rawdb.StageSnapshotBuild, err: errStage}
	failing.fail.Store(true)
	chain := &parallelHistoryEventChain{&coldBuilderChain{db: failing, solidified: 9, syncRemaining: 1000, syncRemainingOK: true}, rawdb.NewChainDB(db, nil)}
	r := parallelHistoryEventRunner(chain, t.TempDir(), 8, 4)
	result, err := r.OnePass()
	if !errors.Is(err, errStage) || !result.HistoryEventParallel || result.Built {
		t.Fatalf("post-publication result: %+v %v", result, err)
	}
	manifest, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil || len(manifest.Segments) != 5 {
		t.Fatalf("durable publication lost: %+v %v", manifest, err)
	}
	if _, err := VerifyManifestFiles(r.cfg.Dir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || ok {
		t.Fatalf("failed stage advanced: %v %v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("failed stage allowed hot prune: %v %v", ok, err)
	}
}

type serialHistoryTransitionStore struct {
	ethdb.KeyValueStore
	gate      *maintenance.HeavyWorkGate
	onHistory func()
	err       error
	once      sync.Once
}

func (s *serialHistoryTransitionStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	if release, admitted := s.gate.TryAcquire(); admitted {
		release() // planning reads precede the actual build lease
	} else {
		s.once.Do(s.onHistory)
		if s.err != nil {
			return &parallelErrorIterator{err: s.err}
		}
	}
	return s.KeyValueStore.NewIterator(prefix, start)
}

func TestParallelHistoryEventSerialPlanningRemainsAfterHistory(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("history-fails=%v", fail), func(t *testing.T) {
			db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			seedParallelHistoryEvent(t, db, 8, 4, 2, 128)
			gate := maintenance.NewHeavyWorkGate()
			store := &serialHistoryTransitionStore{KeyValueStore: db, gate: gate}
			// Intentionally no EventLogDB/ChainDB: a prematurely resolved derived
			// source would hide history failure or reject a newly deferred sidecar.
			chain := &coldBuilderChain{db: store, solidified: 9}
			store.onHistory = func() { chain.syncRemaining, chain.syncRemainingOK = 1000, true }
			errHistory := errors.New("history scan failed before derived planning")
			if fail {
				store.err = errHistory
			}
			r := parallelHistoryEventRunner(chain, t.TempDir(), 8, 4)
			r.cfg.HeavyWorkGate = gate
			r.cfg.HistoryLoadProbe = nil
			r.cfg.HistoryCatchupMode = HistoryCatchupBalanced
			r.cfg.ParallelHistoryEventReady = nil
			r.cfg.BuildEventLogsWhileSyncing = false
			result, err := r.OnePass()
			if fail {
				if !errors.Is(err, errHistory) || result.Built {
					t.Fatalf("history error priority changed: %+v %v", result, err)
				}
			} else if err != nil || !result.Built || result.EventLogBuilt || !result.DerivedSidecarsDeferred {
				t.Fatalf("serial history did not observe newly active sync: %+v %v", result, err)
			}
		})
	}
}

func BenchmarkParallelHistoryEventFullBuild(b *testing.B) {
	old := runtime.GOMAXPROCS(16)
	defer runtime.GOMAXPROCS(old)
	const blocks, txs = 256, 32
	var reference []SegmentRef
	for _, parallel := range []bool{false, true} {
		b.Run(fmt.Sprintf("paired=%v", parallel), func(b *testing.B) {
			previous := runtime.GOMAXPROCS(16)
			defer runtime.GOMAXPROCS(previous)
			db, err := rawdb.NewPebbleDB(b.TempDir(), 64, 64)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			seedParallelHistoryEvent(b, db, blocks, txs, 2, 2048)
			root := b.TempDir()
			var history, event, wall time.Duration
			var size uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r := parallelHistoryEventRunner(&coldBuilderChain{db: rawdb.NewChainDB(db, nil), solidified: blocks + 1, syncRemaining: 100000, syncRemainingOK: true}, filepath.Join(root, fmt.Sprint(i)), blocks, txs)
				r.cfg.ParallelHistoryEventReady = func() bool { return parallel }
				// Stage rows belong to the source, and each timed build uses a fresh manifest.
				for _, stage := range []rawdb.StageID{rawdb.StageSnapshotBuild, rawdb.StageSnapshotHistory, rawdb.StageSnapshotAccessor, rawdb.StageSnapshotEventLogBuild} {
					if err := rawdb.DeleteStageProgress(db, stage); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				result, err := r.OnePass()
				b.StopTimer()
				if err != nil || !result.Built || result.HistoryEventParallel != parallel {
					b.Fatalf("full build: %+v %v", result, err)
				}
				if reference == nil {
					reference = append([]SegmentRef(nil), result.Segments...)
				} else if !reflect.DeepEqual(reference, result.Segments) {
					b.Fatal("same-input benchmark changed output identity")
				}
				history += result.HistoryDuration
				event += result.EventLogDuration
				wall += result.BuildDuration
				size = segmentRefsSize(result.Segments)
				b.StartTimer()
			}
			b.ReportMetric(float64(history.Nanoseconds())/float64(b.N), "history-ns/op")
			b.ReportMetric(float64(event.Nanoseconds())/float64(b.N), "event-ns/op")
			b.ReportMetric(float64(wall.Nanoseconds())/float64(b.N), "build-wall-ns/op")
			b.ReportMetric(float64(size), "stored-B/op")
			b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
			b.ReportMetric(float64(eventLogPayloadWorkers()), "event-workers")
		})
	}
}
