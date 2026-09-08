package snapshots

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/maintenance"
)

func budgetTestLeaves(costs ...historyCompactionInputCost) ([]historyCompactionCandidate, func(historyCompactionCandidate) (historyCompactionInputCost, error)) {
	candidates := make([]historyCompactionCandidate, len(costs))
	for i := range candidates {
		candidates[i].history = SegmentRef{FromTxNum: uint64(i + 1), ToTxNum: uint64(i + 1), AggregationSteps: 1}
	}
	return candidates, func(candidate historyCompactionCandidate) (historyCompactionInputCost, error) {
		return costs[candidate.history.FromTxNum-1], nil
	}
}

func TestBudgetedHistoryCompactionSelectsByActualWork(t *testing.T) {
	tests := []struct {
		name      string
		costs     []historyCompactionInputCost
		cfg       CompactionConfig
		wantFrom  uint64
		wantTo    uint64
		wantBytes uint64
		wantCount uint64
	}{
		{"wait for target", []historyCompactionInputCost{{10, 10, 0}, {10, 10, 0}}, CompactionConfig{MaxSources: 16}, 0, 0, 0, 0},
		{"source target", []historyCompactionInputCost{{10, 10, 0}, {10, 10, 0}, {10, 10, 0}}, CompactionConfig{MaxSources: 3}, 1, 3, 30, 30},
		{"next byte overflow", []historyCompactionInputCost{{20, 1, 0}, {20, 1, 0}, {70, 1, 0}}, CompactionConfig{MaxInputBytes: 100, MaxSources: 16}, 1, 2, 40, 2},
		{"dense records", []historyCompactionInputCost{{1, 3_000_000, 0}, {1, 3_000_000, 0}, {1, 3_000_000, 0}}, CompactionConfig{MaxInputRecords: 8_000_000, MaxSources: 16}, 1, 2, 2, 6_000_000},
		{"exact byte target", []historyCompactionInputCost{{50, 1, 0}, {50, 1, 0}}, CompactionConfig{MaxInputBytes: 100, MaxSources: 16}, 1, 2, 100, 2},
		{"skip oversized leaf", []historyCompactionInputCost{{200, 1, 0}, {50, 1, 0}, {50, 1, 0}}, CompactionConfig{MaxInputBytes: 100, MaxSources: 16}, 2, 3, 100, 2},
		{"skip record oversized leaf", []historyCompactionInputCost{{1, 10, 0}, {1, 2, 0}, {1, 2, 0}}, CompactionConfig{MaxInputRecords: 4, MaxSources: 16}, 2, 3, 2, 4},
		{"retain unmergeable single", []historyCompactionInputCost{{90, 1, 0}, {50, 1, 0}, {50, 1, 0}}, CompactionConfig{MaxInputBytes: 100, MaxSources: 16}, 2, 3, 100, 2},
		{"uint64 overflow is a boundary", []historyCompactionInputCost{{math.MaxUint64 - 2, 1, 0}, {1, 1, 0}, {5, 1, 0}}, CompactionConfig{MaxSources: 16}, 1, 2, math.MaxUint64 - 1, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidates, read := budgetTestLeaves(test.costs...)
			selection, ok, err := selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, test.cfg, read)
			if err != nil || ok != (test.wantFrom != 0) {
				t.Fatalf("selection=%+v ok=%v err=%v", selection, ok, err)
			}
			if selection.fromTxNum != test.wantFrom || selection.toTxNum != test.wantTo || selection.inputBytes != test.wantBytes || selection.inputRecords != test.wantCount {
				t.Fatalf("selection=%+v", selection)
			}
		})
	}
}

func TestBudgetedHistoryCompactionKeepsMergedSegmentsAndGaps(t *testing.T) {
	candidates, read := budgetTestLeaves(historyCompactionInputCost{10, 1, 0}, historyCompactionInputCost{10, 1, 0}, historyCompactionInputCost{10, 1, 0}, historyCompactionInputCost{10, 1, 0})
	candidates[0].history.AggregationSteps = 256
	selection, ok, err := selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, CompactionConfig{MaxSources: 3}, func(c historyCompactionCandidate) (historyCompactionInputCost, error) {
		if c.history.FromTxNum == 1 {
			t.Fatal("already merged prefix was read")
		}
		return read(c)
	})
	if err != nil || !ok || selection.fromTxNum != 2 || selection.toTxNum != 4 {
		t.Fatalf("selection=%+v ok=%v err=%v", selection, ok, err)
	}
	candidates[2].history.FromTxNum = 8
	candidates[2].history.ToTxNum = 8
	_, ok, err = selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, CompactionConfig{MaxSources: 2}, func(historyCompactionCandidate) (historyCompactionInputCost, error) {
		return historyCompactionInputCost{10, 1, 0}, nil
	})
	if err != nil || ok {
		t.Fatalf("merged across a gap: ok=%v err=%v", ok, err)
	}
}

func TestBudgetedHistoryCompactionRewritesEveryLeafOnce(t *testing.T) {
	const leaves = 1024
	var shape []historyCompactionCandidate
	rewrites := make([]uint64, leaves+1)
	for leaf := uint64(1); leaf <= leaves; leaf++ {
		shape = append(shape, historyCompactionCandidate{history: SegmentRef{FromTxNum: leaf, ToTxNum: leaf}})
		selection, ok, err := selectBudgetedHistoryCompactionLeaves(context.Background(), shape, CompactionConfig{MaxSources: 16}, func(historyCompactionCandidate) (historyCompactionInputCost, error) {
			return historyCompactionInputCost{10, 100, 0}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			continue
		}
		var next []historyCompactionCandidate
		for _, c := range shape {
			if c.history.FromTxNum >= selection.fromTxNum && c.history.ToTxNum <= selection.toTxNum {
				for i := c.history.FromTxNum; i <= c.history.ToTxNum; i++ {
					rewrites[i]++
				}
				continue
			}
			next = append(next, c)
		}
		next = append(next, historyCompactionCandidate{history: SegmentRef{FromTxNum: selection.fromTxNum, ToTxNum: selection.toTxNum, AggregationSteps: selection.aggregationSteps}})
		shape = next
	}
	if len(shape) != leaves/16 {
		t.Fatalf("active history files=%d, want %d", len(shape), leaves/16)
	}
	for leaf, n := range rewrites[1:] {
		if n != 1 {
			t.Fatalf("leaf %d rewritten %d times", leaf+1, n)
		}
	}
}

func TestBudgetedHistoryCompactionCancellationAndProbeErrors(t *testing.T) {
	candidates, _ := budgetTestLeaves(historyCompactionInputCost{1, 1, 0}, historyCompactionInputCost{1, 1, 0})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := selectBudgetedHistoryCompactionLeaves(ctx, candidates, CompactionConfig{MaxSources: 2}, func(historyCompactionCandidate) (historyCompactionInputCost, error) {
		t.Fatal("canceled selection performed input reads")
		return historyCompactionInputCost{}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	want := errors.New("cannot inspect immutable header")
	_, _, err = selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, CompactionConfig{MaxSources: 2}, func(historyCompactionCandidate) (historyCompactionInputCost, error) {
		return historyCompactionInputCost{}, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
}

func TestBudgetedHistoryCompactionRealFilesAndIntegrity(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "merge", true: "corrupt canonical input"}[corrupt], func(t *testing.T) {
			dir := t.TempDir()
			refs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))
			refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
			if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
				t.Fatal(err)
			}
			manifestBefore, err := os.ReadFile(filepath.Join(dir, ManifestFile))
			if err != nil {
				t.Fatal(err)
			}
			var inputBytes uint64
			for _, ref := range refs {
				inputBytes += ref.Size
			}
			if corrupt {
				path := filepath.Join(dir, refs[0].Path)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data[len(data)/2] ^= 1
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: true, MaxInputBytes: inputBytes, MaxInputRecords: 2, MaxSources: 16, DeleteObsolete: true})
			if corrupt {
				if err == nil || result.Merged {
					t.Fatalf("accepted corrupt input: result=%+v err=%v", result, err)
				}
				after, readErr := os.ReadFile(filepath.Join(dir, ManifestFile))
				if readErr != nil || !bytes.Equal(after, manifestBefore) {
					t.Fatalf("failed merge changed manifest: %v", readErr)
				}
				return
			}
			if err != nil || !result.Merged || result.InputBytes != inputBytes || result.InputRecords != 2 || result.InputSources != 2 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			manifest, err := LoadProductionManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := VerifyHistorySegmentWithCompanions(dir, manifest, compactionRefByKind(t, result, SegmentHistory)); err != nil {
				t.Fatal(err)
			}
			result, err = CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: true, MaxSources: 2})
			if err != nil || result.Merged {
				t.Fatalf("rewrote terminal output: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestHistoryCompactionIndependentGateAndRecovery(t *testing.T) {
	gate := maintenance.NewHeavyWorkGate()
	r := &Runner{cfg: Config{Enabled: true, Dir: filepath.Join(t.TempDir(), "not-created"), HeavyWorkGate: gate}}
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("could not reserve shared gate")
	}
	result, err := r.compactHistory(context.Background(), true)
	if err != nil || !result.Deferred || result.DeferReason != "heavy-work-gate" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	release()
	r.compactionBudget.notBefore = time.Now().Add(time.Minute)
	result, err = r.compactHistory(context.Background(), true)
	if err != nil || !result.Deferred || result.DeferReason != "merge-recovery" || result.RetryAfter <= 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if r.historyNotBefore.Load() != 0 {
		t.Fatal("merge recovery changed history admission")
	}
	for _, test := range []struct {
		work time.Duration
		fail bool
		want time.Duration
	}{{0, false, 3 * time.Second}, {2 * time.Second, false, 8 * time.Second}, {15 * time.Minute, false, time.Minute}, {time.Duration(math.MaxInt64), false, time.Minute}, {time.Millisecond, true, time.Minute}} {
		if got := historyCompactionRecovery(test.work, test.fail); got != test.want {
			t.Fatalf("work=%v fail=%v got=%v want=%v", test.work, test.fail, got, test.want)
		}
	}
}

func TestBusyHistoryCompactionOwnsGateUntilPublication(t *testing.T) {
	dir := t.TempDir()
	refs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}
	gate := maintenance.NewHeavyWorkGate()
	r := &Runner{
		chain: &coldBuilderChain{syncRemainingOK: true},
		cfg: Config{Enabled: true, Dir: dir, HistoryDataset: SegmentDatasetStateDomainChange,
			HistoryCatchupMode: HistoryCatchupThroughput, CompactMaxSteps: 2, HeavyWorkGate: gate},
	}
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	checked := 0
	gtronlog.SetDefault(gtronlog.NewLogger(historyCancelLogHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		onRecord: func(record slog.Record) {
			if record.Message != "History cold snapshot compaction started" && record.Message != "History cold snapshot compaction completed" {
				return
			}
			checked++
			if release, ok := gate.TryAcquire(); ok {
				release()
				t.Error("another heavy job acquired the gate during compaction")
			}
		},
	}))
	result, err := r.compactHistory(context.Background(), true)
	if err != nil || !result.Merged || result.InputSources != 2 || checked != 2 {
		t.Fatalf("result=%+v checked=%d err=%v", result, checked, err)
	}
	if !r.compactionBudget.notBefore.After(time.Now()) || result.Recovery < 3*time.Second || result.Recovery > time.Minute {
		t.Fatalf("missing bounded independent recovery: result=%+v until=%v", result, r.compactionBudget.notBefore)
	}
	if r.historyNotBefore.Load() != 0 {
		t.Fatal("completed merge delayed history admission")
	}
	result, err = r.compactHistory(context.Background(), true)
	if err != nil || !result.Deferred || result.DeferReason != "merge-recovery" {
		t.Fatalf("second pass=%+v err=%v", result, err)
	}
}
