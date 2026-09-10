package snapshots

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	busyResourceEquivalenceBlocks  = 12
	busyResourceEquivalenceChanges = 2
)

func newBusyResourceEquivalenceRunner(t *testing.T, ready bool, txs int, blockLimit, txLimit uint64) *Runner {
	t.Helper()
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close source database: %v", err)
		}
	})
	seedParallelHistoryEvent(t, db, busyResourceEquivalenceBlocks, txs, busyResourceEquivalenceChanges, 128)
	mode := "busy"
	if ready {
		mode = "ready"
	}
	namespace := normalizeColdSnapshotMetricNamespace("test/history-busy-resource/" + t.Name() + "/" + mode)
	t.Cleanup(func() { unregisterColdRunnerMetricNamespace(namespace) })
	r := NewRunner(&coldBuilderChain{
		db: rawdb.NewChainDB(db, nil), solidified: busyResourceEquivalenceBlocks + 1,
		syncRemaining: 1000, syncRemainingOK: true,
	}, Config{
		Dir: t.TempDir(), Enabled: true, HistoryWindow: 1,
		// An unmeasured throughput batch starts at one quarter of each bound.
		// Both admission paths therefore select the same deterministic range.
		BatchBlocks: blockLimit * 4, BatchTxNums: txLimit * 4,
		MetricsNamespace: namespace, HistoryCatchupMode: HistoryCatchupThroughput,
		DeferHistoryBuildWhileSyncing: true, MaxDeferredHistoryBlocks: 2, MaxBusyDeferredHistoryBlocks: 20,
		SyncBuildReady: func() bool { return ready }, BusyHistoryBuildReady: func() bool { return true },
		CatchupBuildMinInterval: time.Minute, CatchupUnthrottledLagBlocks: 1, CatchupHeavyWorkCooldown: 3 * time.Second,
		HeavyWorkGate:    maintenance.NewHeavyWorkGate(),
		HistoryLoadProbe: func() maintenance.StoragePressure { return healthyHistoryLoad(time.Now()) },
		BuildEventLogs:   true, EventLogVersion: EventLogSegmentV4Version,
		DeferDerivedSidecarsWhileSyncing: true, BuildEventLogsWhileSyncing: true,
		DeferLatestBuildWhileSyncing: true,
	})
	t.Cleanup(r.cancel)
	r.historyLoad.level, r.historyLoad.good = 3, 3
	r.historyLoad.sample = healthyHistoryLoad(time.Now())
	return r
}

type busyResourceColdOutput struct {
	refs    []SegmentRef
	files   map[string][]byte
	changes []*rawdb.StateDomainChange
	logs    []EventLog
}

func readBusyResourceColdOutput(t *testing.T, r *Runner, result PassResult, txs int) busyResourceColdOutput {
	t.Helper()
	if _, err := VerifyManifestFiles(r.cfg.Dir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("verify complete cold output: %v", err)
	}
	manifest, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if end := ContiguousHistoryVisibleTxEnd(manifest, SegmentDatasetStateDomainChange, 1); end != result.ToTxNum {
		t.Fatalf("contiguous history end = %d, want %d", end, result.ToTxNum)
	}
	output := busyResourceColdOutput{refs: result.Segments, files: make(map[string][]byte)}
	for _, ref := range result.Segments {
		if ref.Checksum == "" {
			t.Fatalf("published segment has no checksum: %+v", ref)
		}
		content, err := os.ReadFile(filepath.Join(r.cfg.Dir, ref.Path))
		if err != nil {
			t.Fatal(err)
		}
		output.files[ref.Path] = content
	}
	mgr, err := OpenManager(r.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.IterateStateDomainChanges(1, uint64(busyResourceEquivalenceBlocks*txs), func(change *rawdb.StateDomainChange) (bool, error) {
		copy := *change
		copy.Key, copy.Prev = bytes.Clone(change.Key), bytes.Clone(change.Prev)
		output.changes = append(output.changes, &copy)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(output.changes) != int(result.ToTxNum)*busyResourceEquivalenceChanges {
		t.Fatalf("cold history rows = %d, want %d", len(output.changes), result.ToTxNum*busyResourceEquivalenceChanges)
	}
	for _, change := range output.changes {
		if change.BlockNum < result.FromBlock || change.BlockNum > result.ToBlock || change.TxNum < result.FromTxNum || change.TxNum > result.ToTxNum {
			t.Fatalf("history row escaped the selected complete blocks: %+v", change)
		}
	}
	if covered, err := mgr.EventLogIndexedRangeCovered(1, result.ToBlock); err != nil || !covered {
		t.Fatalf("published event range is not covered: %v %v", covered, err)
	}
	if covered, err := mgr.EventLogIndexedRangeCovered(result.ToBlock+1, result.ToBlock+1); err != nil || covered {
		t.Fatalf("unpublished next event block is covered: %v %v", covered, err)
	}
	if err := mgr.IterateEventLogs(1, result.ToBlock, EventLogFilter{}, func(row EventLog) (bool, error) {
		row.Log = proto.Clone(row.Log).(*corepb.TransactionInfo_Log)
		output.logs = append(output.logs, row)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(output.logs) != int(result.ToTxNum) {
		t.Fatalf("cold event rows = %d, want %d", len(output.logs), result.ToTxNum)
	}
	return output
}

func TestHistoryBusyResourceReadySameFilesAndQueries(t *testing.T) {
	for _, tc := range []struct {
		name                string
		txs                 int
		blockLimit, txLimit uint64
		wantBlocks, wantTxs uint64
	}{
		{name: "block bound", txs: 3, blockLimit: 2, txLimit: 20, wantBlocks: 2, wantTxs: 6},
		{name: "txnum bound includes final block", txs: 2, blockLimit: 8, txLimit: 3, wantBlocks: 2, wantTxs: 4},
		{name: "single dense block exceeds txnum target", txs: 10, blockLimit: 8, txLimit: 3, wantBlocks: 1, wantTxs: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want busyResourceColdOutput
			for _, ready := range []bool{true, false} {
				r := newBusyResourceEquivalenceRunner(t, ready, tc.txs, tc.blockLimit, tc.txLimit)
				result, err := r.OnePass()
				if err != nil || !result.Built || !result.EventLogBuilt || result.HistoryDeferred {
					t.Fatalf("ready=%v publication: %+v err=%v", ready, result, err)
				}
				if !result.HistoryAdmissionChecked || result.HistoryAdmissionReady != ready || result.HistoryForcedBusy == ready || result.HistoryBusyResourceReady == ready || result.HistoryAccelerated != ready {
					t.Fatalf("ready=%v changed admission classification: %+v", ready, result)
				}
				if result.EligibleCutoffBlock != busyResourceEquivalenceBlocks || result.EligibleCutoffBlock <= r.cfg.MaxDeferredHistoryBlocks || result.EligibleCutoffBlock > r.cfg.MaxBusyDeferredHistoryBlocks {
					t.Fatalf("fixture did not exercise the soft-to-busy interval: %+v", result)
				}
				if result.FromBlock != 1 || result.ToBlock != tc.wantBlocks || result.FromTxNum != 1 || result.ToTxNum != tc.wantTxs || result.HistoryBatchBlocks != tc.wantBlocks || result.HistoryBatchTxNums != tc.wantTxs || !result.HistoryNeedsCatchup() {
					t.Fatalf("ready=%v bounded complete-block range: %+v", ready, result)
				}
				if result.ToTxNum != result.ToBlock*uint64(tc.txs) || result.HistoryBatchBlocks > tc.blockLimit {
					t.Fatalf("ready=%v split a block or exceeded the block bound: %+v", ready, result)
				}
				got := readBusyResourceColdOutput(t, r, result, tc.txs)
				if ready {
					want = got
					continue
				}
				if !reflect.DeepEqual(got.refs, want.refs) || !reflect.DeepEqual(got.files, want.files) || !reflect.DeepEqual(got.changes, want.changes) {
					t.Fatal("busy resource admission changed segment refs/checksums, bytes, or history queries")
				}
				for i, row := range got.logs {
					previous := want.logs[i]
					if row.BlockNum != previous.BlockNum || row.TxIndex != previous.TxIndex || row.LogIndex != previous.LogIndex || row.TxHash != previous.TxHash || row.BlockHash != previous.BlockHash || row.Address != previous.Address || !proto.Equal(row.Log, previous.Log) {
						t.Fatalf("busy resource admission changed event query row %d", i)
					}
				}
			}
		})
	}
}
