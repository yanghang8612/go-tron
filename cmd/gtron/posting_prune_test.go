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
	for _, mode := range []string{"ready", "new-pressure", "stale-device", "unavailable", "gate-lost", "gate-hard-pressure"} {
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
				case "stale-device":
					pressure.DeviceSampledAt = time.Now().Add(-16 * time.Second)
				case "unavailable":
					pressure = maintenance.StoragePressure{}
				case "gate-lost":
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
			wantReady := mode == "ready"
			wantGate := mode == "gate-lost" || mode == "gate-hard-pressure"
			if chain.admitted != wantReady || out.GateDeferred != wantGate || out.PressureDeferred != (!wantReady && !wantGate) || out.Deferred == wantReady {
				t.Fatalf("late admission=%+v admitted=%v", out, chain.admitted)
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
