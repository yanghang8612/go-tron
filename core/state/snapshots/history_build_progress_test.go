package snapshots

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
)

func TestColdSnapshotBuildProgressReportsLongPhase(t *testing.T) {
	var buf bytes.Buffer
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))

	progress := &coldSnapshotBuildProgress{
		dataset: SegmentDatasetStateDomainChange,
		fromTx:  10, toTx: 20,
		fromBlock: 30, toBlock: 40,
		eligible: 140,
		started:  time.Now().Add(-2 * time.Minute),
	}
	progress.phase.Store("event-log")
	progress.log()

	out := buf.String()
	for _, field := range []string{
		`msg="History cold snapshot build progress"`, "dataset=state-domain-change",
		"phase=event-log", "fromTx=10", "toTx=20", "fromBlock=30", "toBlock=40",
		"eligibleCutoffBlock=140", "backlogBlocks=100", "elapsed=",
	} {
		if !strings.Contains(out, field) {
			t.Errorf("missing build progress field %q:\n%s", field, out)
		}
	}
}

func TestColdSnapshotBuildProgressConcurrentStop(t *testing.T) {
	progress := startColdSnapshotBuildProgress(SegmentDatasetStateDomainChange, 10, 20, 30, 40, 140, time.Hour)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			progress.SetPhase("publish")
			progress.Stop()
		}()
	}
	wg.Wait()
}
