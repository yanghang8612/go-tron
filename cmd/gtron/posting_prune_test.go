package main

import (
	"context"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/pruning"
)

type postingPruneAdmissionTestChain struct {
	beforeAdmission func()
	admitted        bool
}

func (c *postingPruneAdmissionTestChain) PruneStateChangePostingChunkWithAdmission(_ context.Context, _, _ uint64, _, anchor common.Hash, _ []byte, _ rawdb.StateChangePostingPruneLimits, admit core.StateChangePostingPruneAdmission) (rawdb.StateChangePostingPruneChunkResult, common.Hash, core.StateChangePostingPruneTimings, error) {
	c.beforeAdmission() // Represents state changing while the core queued its lock.
	release, ok := admit()
	if release != nil {
		defer release()
	}
	c.admitted = ok
	if !ok {
		return rawdb.StateChangePostingPruneChunkResult{}, anchor, core.StateChangePostingPruneTimings{ChainWait: 20, Admission: 3}, core.ErrStateChangePostingPruneDeferred
	}
	return rawdb.StateChangePostingPruneChunkResult{RowsScanned: 1, ScanDuration: 5, WriteDuration: 2}, anchor, core.StateChangePostingPruneTimings{ChainWait: 20, ChainHeld: 11, Admission: 3, Proof: 1, GateHeld: 8}, nil
}

func TestRuntimePostingPruneRechecksAfterCoreWait(t *testing.T) {
	for _, mode := range []string{"ready", "debt-relief", "debt-relief-late-latency", "debt-relief-late-queue", "debt-relief-gate-lost", "new-pressure", "stale-device", "unavailable", "gate-lost", "gate-hard-pressure"} {
		t.Run(mode, func(t *testing.T) {
			gate := maintenance.NewHeavyWorkGate()
			pressure := maintenance.StoragePressure{Available: true, SampledAt: time.Now(), DeviceAvailable: true, DeviceSampledAt: time.Now(), DeviceAwait: time.Millisecond}
			if !gate.CanTryAcquire() || !pruning.PostingPrunePressureReady(pressure, time.Now()) {
				t.Fatal("initial preflight must be ready")
			}
			var otherRelease func()
			defer func() {
				if otherRelease != nil {
					otherRelease()
				}
			}()
			chain := &postingPruneAdmissionTestChain{beforeAdmission: func() {
				switch mode {
				case "new-pressure":
					pressure.WriteStalled = true
				case "debt-relief":
					pressure.CompactionDebt = 40 << 30
				case "debt-relief-late-latency":
					pressure.CompactionDebt = 40 << 30
					pressure.DeviceAwait = 3 * time.Millisecond
				case "debt-relief-late-queue":
					pressure.CompactionDebt = 40 << 30
					pressure.DeviceQueueMilli = 8_001
				case "stale-device":
					pressure.DeviceSampledAt = time.Now().Add(-16 * time.Second)
				case "unavailable":
					pressure = maintenance.StoragePressure{}
				case "gate-lost", "debt-relief-gate-lost":
					if mode == "debt-relief-gate-lost" {
						pressure.CompactionDebt = 40 << 30
					}
					var ok bool
					otherRelease, ok = gate.TryAcquire()
					if !ok {
						t.Fatal("intervening owner could not acquire")
					}
				case "gate-hard-pressure":
					gate.SetAdmissionCheck(func() bool { return false })
				}
			}}
			out, err := runtimePostingPruneChunk(chain, gate, func() maintenance.StoragePressure { return pressure })(context.Background(), pruning.PostingPruneBoundary{}, common.Hash{}, nil, rawdb.StateChangePostingPruneLimits{})
			if err != nil || out.Timings.ChainWait != 20 || out.Timings.Admission != 3 {
				t.Fatalf("adapter dropped timings or leaked deferred error: %+v %v", out, err)
			}
			wantReady := mode == "ready" || mode == "debt-relief"
			wantGate := mode == "gate-lost" || mode == "gate-hard-pressure" || mode == "debt-relief-gate-lost"
			if chain.admitted != wantReady || out.GateDeferred != wantGate || out.PressureDeferred != (!wantReady && !wantGate) || out.Deferred == wantReady {
				t.Fatalf("late admission=%+v admitted=%v", out, chain.admitted)
			}
			if out.DebtRelief != (mode == "debt-relief") {
				t.Fatalf("wrong locked-admission debt tier: %+v", out)
			}
			if wantReady && (out.Timings.Proof != 1 || out.Timings.GateHeld != 8 || out.Result.ScanDuration != 5 || out.Result.WriteDuration != 2 || !gate.CanTryAcquire()) {
				t.Fatal("adapter lost phase values or lease release")
			}
		})
	}
}

func TestPostingPruneEnabled(t *testing.T) {
	for _, value := range []string{"", "0", "1", "true", " 1", "2"} {
		got, err := postingPruneEnabled(value)
		valid := value == "" || value == "0" || value == "1"
		if (err == nil) != valid || got != (value == "1") {
			t.Fatalf("%q: enabled=%v err=%v", value, got, err)
		}
	}
}
