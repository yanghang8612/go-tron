package main

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	chainfreezer "github.com/tronprotocol/go-tron/core/freezer"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	snapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type contentionFreezerChain struct {
	db              *rawdb.ChainDB
	ancient         *rawdbfreezer.Freezer
	entered, resume chan struct{}
	once            sync.Once
}

func (c *contentionFreezerChain) DB() ethdb.KeyValueStore         { return c.db }
func (c *contentionFreezerChain) LatestSolidifiedBlockNum() int64 { return 63 }
func (c *contentionFreezerChain) ReadBlockRawStrict(n uint64) ([]byte, bool, error) {
	c.once.Do(func() { close(c.entered); <-c.resume })
	return rawdb.ReadBlockRawStrict(c.db, n)
}
func (c *contentionFreezerChain) ReadTransactionInfosRawStrict(n uint64) ([]byte, bool, error) {
	return rawdb.ReadTransactionInfosRawStrict(c.db, n)
}
func (c *contentionFreezerChain) ReadBlockHashByNumberStrict(n uint64) (common.Hash, bool, error) {
	return rawdb.ReadBlockHashByNumberStrict(rawdb.NewChainDB(c.db, rawdb.NewFreezerReader(c.ancient)), n)
}
func (c *contentionFreezerChain) ReadBlockStateRootRaw(common.Hash) ([]byte, error) { return nil, nil }

type contentionColdChain struct{ db *rawdb.ChainDB }

func (c contentionColdChain) DB() snapshots.AggregatorDB          { return c.db }
func (c contentionColdChain) LatestSolidifiedBlockNum() int64     { return 65 }
func (c contentionColdChain) SyncRemainingBlocks() (uint64, bool) { return 1000, true }
func (c contentionColdChain) CanonicalBlockHashStrict(n uint64) (common.Hash, bool, error) {
	return rawdb.ReadBlockHashByNumberStrict(c.db, n)
}

// Exercises the real cmd wiring, not a synthetic TryAcquire failure: the
// nonblocking raw layout probe defers before sidecar gap/rate/gate admission.
// The actual freezer Runner Due/Yield must observe that pre-gate demand, stop
// all source work, release its raw locks, and let cold publish while coverage
// is still unpublished. No test code acquires a history gate lease.
func TestSyncEventLogTargetDeferredNotifiesActualDirectV2Job(t *testing.T) {
	for _, caughtUp := range []bool{false, true} {
		t.Run(fmt.Sprintf("caught_up_%t", caughtUp), func(t *testing.T) {
			db := rawdb.NewMemoryChainDB()
			defer db.Close()
			bodies := map[uint64][]byte{}
			receipts := map[uint64][]byte{}
			for n := uint64(0); n <= 64; n++ {
				tx := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: int64(n + 100), Data: []byte{byte(n)}}}
				block := coretypes.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(n), Timestamp: int64(n * 3000)}}, Transactions: []*corepb.Transaction{tx}})
				if err := rawdb.WriteBlock(db, block); err != nil {
					t.Fatal(err)
				}
				info := &corepb.TransactionInfo{Id: coretypes.NewTransactionFromPB(tx).Hash().Bytes(), BlockNumber: int64(n), Log: []*corepb.TransactionInfo_Log{{Address: bytes.Repeat([]byte{0x31}, 20), Data: []byte{byte(n)}}}}
				if err := rawdb.WriteTransactionInfosByBlock(db, n, []*corepb.TransactionInfo{info}); err != nil {
					t.Fatal(err)
				}
				bodies[n], _, _ = rawdb.ReadBlockRawStrict(db, n)
				receipts[n], _, _ = rawdb.ReadTransactionInfosRawStrict(db, n)
				if n == 0 {
					continue
				}
				if err := rawdb.WriteStateTxRange(db, n, block.Hash(), n, n); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{{BlockNum: n, BlockHash: block.Hash(), TxNum: n, Seq: 1, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 1}, Domain: kvdomains.SystemReward, Key: []byte{byte(n)}, PrevExists: true, Prev: []byte("before")}}); err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			coldChain := contentionColdChain{db: db}
			cfg := snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1, BatchBlocks: 64, BuildEventLogs: true, EventLogVersion: snapshots.EventLogSegmentV4Version, DeferDerivedSidecarsWhileSyncing: !caughtUp, MetricsNamespace: "test/target-notify/bootstrap/"}
			bootstrap := snapshots.NewRunner(coldChain, cfg)
			built, err := bootstrap.OnePass()
			if err != nil || !built.Built || built.EventLogBuilt != caughtUp {
				t.Fatalf("bootstrap=%+v err=%v", built, err)
			}
			f, err := rawdbfreezer.NewFreezer(t.TempDir(), "", false, 2049, chainfreezer.FreezerTableSet())
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			gate := maintenance.NewHeavyWorkGateWithCooldownAfter(15*time.Second, 250*time.Millisecond)
			probe := makeSyncEventLogTargetBlock(f, 64, nil)
			cfg.HistoryCatchupMode = snapshots.HistoryCatchupThroughput
			cfg.CatchupHeavyWorkCooldown = 3 * time.Second
			cfg.BuildEventLogsWhileSyncing = true
			cfg.SyncEventLogCatchupBlocks = 64
			cfg.SyncEventLogTargetBlock = probe
			cfg.HeavyWorkGate = gate
			cfg.MetricsNamespace = "test/target-notify/runtime/"
			cold := snapshots.NewRunner(coldChain, cfg)
			chain := &contentionFreezerChain{db: db, ancient: f, entered: make(chan struct{}), resume: make(chan struct{})}
			var resume sync.Once
			unblock := func() { resume.Do(func() { close(chain.resume) }) }
			defer unblock()
			freezer := chainfreezer.New(chain, newFreezerStore(f), chainfreezer.Config{Enabled: true, V2Enabled: true, DirectV2: true, V2SegmentBlocks: 64, V2FrameBlocks: 8, HeavyWorkGate: gate, TransactionIndexLoadProbe: func() maintenance.StoragePressure {
				now := time.Now()
				return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now, DeviceAwait: time.Millisecond, L0CompactionThreshold: 4, L0StopWritesThreshold: 12}
			}, SyncActive: func() bool { return true }, MetricsNamespace: "test/target-notify/freezer/"})
			done := make(chan error, 1)
			go func() {
				n, err := freezer.OnePass()
				if err == nil && n != 64 {
					err = fmt.Errorf("froze %d blocks", n)
				}
				done <- err
			}()
			<-chain.entered
			time.Sleep(11 * time.Second) // Make the real ten-second Due threshold mature.
			if _, ok, deferred := probe(); ok || !deferred {
				t.Fatal("actual raw migration did not defer the target")
			}
			before := gate.Observation()
			deferred, err := cold.OnePass()
			if err != nil || !deferred.HistoryGateDeferred || !deferred.DerivedSidecarsDeferred || deferred.EventLogBuilt {
				t.Fatalf("production target deferral=%+v err=%v", deferred, err)
			}
			after := gate.Observation()
			if after.Acquisitions != before.Acquisitions || after.RejectedHeld != before.RejectedHeld {
				t.Fatal("pre-gate path acquired/rejected a gate rather than just declaring demand")
			}
			// This assertion fails on 2b64: the production early return never made
			// history contention visible to the direct Runner's actual Due callback.
			if !gate.HistoryContended(time.Second) {
				unblock()
				<-done
				t.Fatal("pre-gate targetDeferred did not notify fresh history demand")
			}
			unblock()
			var resumed snapshots.PassResult
			for i := 0; i < 100; i++ {
				if _, ok, deferred := probe(); ok && !deferred && gate.CanTryAcquire() {
					if f.V2Coverage() != 0 {
						t.Fatal("freezer committed without making a pre-publication cold opportunity")
					}
					resumed, err = cold.OnePass()
					if err != nil {
						t.Fatal(err)
					}
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if caughtUp {
				if resumed.EventLogBuilt || gate.Observation().HistoryAcquisitions != 0 {
					t.Fatal("caught-up planner fabricated cold progress")
				}
			} else if !resumed.EventLogBuilt || gate.Observation().HistoryAcquisitions != 1 {
				t.Fatalf("cold did not really acquire/build during yield: %+v", resumed)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("direct job did not resume after handoff/timeout")
			}
			if f.V2Coverage() != 64 {
				t.Fatal("incomplete direct segment")
			}
			for n := uint64(0); n < 64; n++ {
				got, err := f.Ancient("bodies", n)
				if err != nil || !bytes.Equal(got, bodies[n]) {
					t.Fatalf("body%d: %v", n, err)
				}
				want, err := rawdb.CompactAncientV2Record("tx_infos", n, receipts[n], bodies[n])
				if err != nil {
					t.Fatal(err)
				}
				got, err = f.Ancient("tx_infos", n)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("receipts%d: %v", n, err)
				}
			}
			if gate.Observation().Owner != maintenance.HeavyWorkUnspecified {
				t.Fatal("direct job leaked shared lease")
			}
		})
	}
}
