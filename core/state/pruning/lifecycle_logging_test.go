package pruning

import (
	"bytes"
	"strings"
	"testing"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestSnapshotLifecycleFailureLogsAreStatefulAndRateLimited(t *testing.T) {
	var buf bytes.Buffer
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))

	lifecycle := &SnapshotLifecycle{}
	started := time.Now()
	lifecycle.logPassFailure("interval", &snapshots.HistoryCoverageError{
		Progress: 105, Dataset: snapshots.SegmentDatasetStateDomainChange, VisibleEnd: 100,
	}, started)
	// The changing numeric values remain the same failure class and should not
	// create one warning per maintenance interval.
	lifecycle.logPassFailure("interval", &snapshots.HistoryCoverageError{
		Progress: 106, Dataset: snapshots.SegmentDatasetStateDomainChange, VisibleEnd: 101,
	}, started.Add(time.Minute))
	lifecycle.logPassFailure("interval", &snapshots.HistoryCoverageError{
		Progress: 107, Dataset: snapshots.SegmentDatasetStateDomainChange, VisibleEnd: 102,
	}, started.Add(2*time.Minute))
	lifecycle.logPassFailure("interval", &snapshots.HistoryCoverageError{
		Progress: 108, Dataset: snapshots.SegmentDatasetStateDomainChange, VisibleEnd: 103,
	}, started.Add(snapshotLifecycleFailureSummaryInterval))
	lifecycle.logPassRecovery(started.Add(snapshotLifecycleFailureSummaryInterval + time.Minute))

	out := buf.String()
	if got := strings.Count(out, `msg="Domain state snapshot/prune pass failed"`); got != 1 {
		t.Fatalf("initial failure log count = %d, want 1:\n%s", got, out)
	}
	if got := strings.Count(out, `msg="Domain state snapshot/prune pass still failing"`); got != 1 {
		t.Fatalf("failure summary log count = %d, want 1:\n%s", got, out)
	}
	if got := strings.Count(out, `msg="Domain state snapshot/prune pass recovered"`); got != 1 {
		t.Fatalf("recovery log count = %d, want 1:\n%s", got, out)
	}
	for _, field := range []string{
		"failureKind=history_coverage_gap", "dataset=state-domain-change",
		"historyProgress=105", "visibleCoverage=100", "coverageGap=5",
		"failures=4", "suppressedSinceLastLog=2", "failedFor=", "previousErr=",
	} {
		if !strings.Contains(out, field) {
			t.Errorf("missing lifecycle log field %q:\n%s", field, out)
		}
	}
}
