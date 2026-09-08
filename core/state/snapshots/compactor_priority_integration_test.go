package snapshots

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/maintenance"
)

type priorityCompactionTestChain struct {
	dbCalls int
}

func (c *priorityCompactionTestChain) DB() AggregatorDB {
	c.dbCalls++
	return nil
}

func (*priorityCompactionTestChain) LatestSolidifiedBlockNum() int64 { return 100 }
func (*priorityCompactionTestChain) SyncRemainingBlocks() (uint64, bool) {
	return 1_000_000, true
}

func TestRunnerPrioritizesPendingMergeAfterSafePrune(t *testing.T) {
	dir := t.TempDir()
	var refs []SegmentRef
	for block := uint64(1); block <= 16; block++ {
		refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, block, block,
			binaryStateDomainChange(block, block, 1, fmt.Sprintf("account-%d", block)))...)
	}
	if err := PublishManifest(dir, NewManifest(1, 16, refs)); err != nil {
		t.Fatal(err)
	}
	chain := &priorityCompactionTestChain{}
	gate := maintenance.NewHeavyWorkGate()
	r := NewRunner(chain, Config{
		Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange,
		HistoryCatchupMode: HistoryCatchupThroughput, CompactMaxSteps: 256,
		HeavyWorkGate: gate, BuildEventLogs: true, BuildEventLogsWhileSyncing: true,
		DeferLatestBuildWhileSyncing: true, MetricsNamespace: "test/" + t.Name(),
	})
	t.Cleanup(r.cancel)
	r.lastPublishedBlock.Store(16)
	r.lastEligibleCutoff.Store(100)
	historyDeadline := time.Now().Add(time.Minute)
	r.historyNotBefore.Store(historyDeadline.UnixNano())

	// Arm priority through a real refused gate acquisition and manifest-only
	// readiness check. Advancing this independent test deadline avoids sleeps.
	release, admitted := gate.TryAcquire()
	if !admitted {
		t.Fatal("could not reserve shared gate")
	}
	deferred, err := r.compactHistory(context.Background(), true)
	release()
	if err != nil || deferred.DeferReason != "heavy-work-gate" || !r.compactionBudget.pending || deferred.RetryDeadline.IsZero() {
		t.Fatalf("did not arm a ready pending merge: %+v err=%v", deferred, err)
	}
	r.compactionBudget.pendingAt = time.Now().Add(-time.Nanosecond)

	var order []string
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(historyCancelLogHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		onRecord: func(record slog.Record) {
			switch record.Message {
			case "History cold snapshot compaction started":
				order = append(order, "merge")
			case "History cold snapshot compaction completed":
				order = append(order, "merge-complete")
			}
		},
	}))
	result, err := r.OnePassWithMaintenanceContext(context.Background(), func(ctx context.Context, p PassResult) error {
		if len(order) != 0 || p.Compaction.Merged || p.HistoryBuildAttempted || p.HistoryEventAttempted || p.PublishedBlock != 16 || p.EligibleCutoffBlock != 100 {
			t.Fatalf("unsafe pre-merge callback: order=%v result=%+v", order, p)
		}
		manifest, err := LoadProductionManifest(dir)
		if err != nil {
			return err
		}
		leaves := 0
		for _, ref := range manifest.Segments {
			if ref.Kind == SegmentHistory {
				leaves++
				if err := VerifyHistorySegmentWithCompanions(dir, manifest, ref); err != nil {
					return err
				}
			}
		}
		if leaves != 16 {
			t.Fatalf("prune coverage changed before callback: %d leaves", leaves)
		}
		order = append(order, "verified-prune")
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"verified-prune", "merge", "merge-complete"}) {
		t.Fatalf("maintenance order=%v", order)
	}
	if !result.Compaction.Merged || result.Compaction.MergePasses != 1 || result.Compaction.InputSources != 16 || r.compactionBudget.pending {
		t.Fatalf("did not execute exactly one pending merge: %+v", result.Compaction)
	}
	if chain.dbCalls != 0 || result.Built || result.HistoryBuildAttempted || result.HistoryEventAttempted || result.DerivedSidecarCatchup || result.LatestBuilt {
		t.Fatalf("priority pass started history/event/latest work: DB calls=%d result=%+v", chain.dbCalls, result)
	}
	if r.historyNotBefore.Load() != historyDeadline.UnixNano() {
		t.Fatal("priority pass modified stored history admission deadline")
	}
	if !result.HistoryRetryDeadline.Equal(historyDeadline) {
		t.Fatalf("history retry deadline=%v, want its preserved admission deadline %v", result.HistoryRetryDeadline, historyDeadline)
	}
	now := time.Now()
	historyAfter := result.HistoryRetryRemaining(now)
	maintenanceAfter := result.MaintenanceRetryRemaining(now)
	// The stored deadline intentionally contains wall-clock nanoseconds only.
	// Compare in that same representation, rather than the original time's
	// extra monotonic component, whose precision can differ on Darwin.
	wantHistoryAfter := time.Unix(0, historyDeadline.UnixNano()).Sub(now)
	if historyAfter != wantHistoryAfter || maintenanceAfter != result.Compaction.RetryDeadline.Sub(now) || maintenanceAfter <= 0 || maintenanceAfter >= historyAfter {
		t.Fatalf("independent wakes: history=%v maintenance=%v merge deadline=%v", historyAfter, maintenanceAfter, result.Compaction.RetryDeadline)
	}
}
