package pruning

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func postingWorkerTestBoundary(height uint64, proof byte) PostingPruneBoundary {
	return PostingPruneBoundary{PrunedThrough: height, ProofHead: height + 100, ProofHash: common.Hash{proof}}
}

func postingWorkerTestPressure(now time.Time) maintenance.StoragePressure {
	return maintenance.StoragePressure{Available: true, SampledAt: now,
		DeviceAvailable: true, DeviceSampledAt: now, DeviceBusyPPM: 500_000,
		DeviceAwait: time.Millisecond, DeviceQueueMilli: 1000,
		L0Sublevels: 1, L0StopWritesThreshold: 64, MemTableCount: 1, MemTableStopWritesThreshold: 8}
}

func postingWorkerTestConfig(t *testing.T, boundary func() PostingPruneBoundary, chunk PostingPruneChunkFunc) PostingPruneWorkerConfig {
	t.Helper()
	return PostingPruneWorkerConfig{Boundary: boundary, Chunk: chunk,
		LoadProbe:     func() maintenance.StoragePressure { return postingWorkerTestPressure(time.Now()) },
		HeavyWorkGate: maintenance.NewHeavyWorkGate(), MinAdvanceBlocks: 10,
		Interval: time.Millisecond, MetricsNamespace: "test/" + t.Name() + "/"}
}

func runPostingWorkerTestChunk(t *testing.T, w *PostingPruneWorker) {
	t.Helper()
	if err := w.runChunk(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPostingPruneWorkerKeepsFixedTargetCursorAndFailures(t *testing.T) {
	boundary := postingWorkerTestBoundary(100, 1)
	fixed := boundary
	anchor := common.Hash{3}
	boom := errors.New("posting write failed")
	calls := 0
	cfg := postingWorkerTestConfig(t, func() PostingPruneBoundary { return boundary },
		func(_ context.Context, got PostingPruneBoundary, gotAnchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
			calls++
			if got != fixed || limits.MaxScannedRows != 4096 || limits.MaxScannedBytes != 1<<20 || limits.MaxDeleteBytes != 256<<10 || limits.MaxDuration != 10*time.Millisecond {
				t.Errorf("target or budgets changed: %+v %+v", got, limits)
			}
			if calls == 1 {
				if len(cursor) != 0 || gotAnchor != (common.Hash{}) {
					t.Error("fresh sweep inherited cursor/anchor")
				}
				return PostingPruneChunkOutcome{Anchor: anchor, Result: rawdb.StateChangePostingPruneChunkResult{NextCursor: []byte{1}, RowsScanned: 3, BytesScanned: 30, RowsDeleted: 2, BytesDeleted: 20}}, nil
			}
			if !bytes.Equal(cursor, []byte{1}) || gotAnchor != anchor {
				t.Errorf("unconfirmed cursor/anchor advanced: %x %x", cursor, gotAnchor)
			}
			switch calls {
			case 2:
				return PostingPruneChunkOutcome{Anchor: common.Hash{9}, Result: rawdb.StateChangePostingPruneChunkResult{NextCursor: []byte{9}, Complete: true, RowsScanned: 4, BytesScanned: 40, RowsDeleted: 99, BytesDeleted: 990}}, boom
			case 3:
				return PostingPruneChunkOutcome{Deferred: true, Anchor: common.Hash{9}, Result: rawdb.StateChangePostingPruneChunkResult{NextCursor: []byte{9}, RowsScanned: 5, BytesScanned: 50, RowsDeleted: 99, BytesDeleted: 990}}, nil
			default:
				return PostingPruneChunkOutcome{Anchor: anchor, Result: rawdb.StateChangePostingPruneChunkResult{Complete: true, NextCursor: []byte{2}, RowsScanned: 1, BytesScanned: 10, RowsDeleted: 1, BytesDeleted: 10}}, nil
			}
		})
	w := NewPostingPruneWorker(cfg)
	t.Cleanup(func() { _ = w.Stop() })
	runPostingWorkerTestChunk(t, w)
	boundary = postingWorkerTestBoundary(200, 2)
	if err := w.runChunk(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("failure = %v", err)
	}
	if w.completed != 0 || !bytes.Equal(w.cursor, []byte{1}) || w.counts["deleted/rows"] != 2 || w.gauges["last/deleted_bytes"].Snapshot().Value() != 0 {
		t.Fatalf("failure published unconfirmed work: %+v", w)
	}
	runPostingWorkerTestChunk(t, w)
	if w.target != fixed || !bytes.Equal(w.cursor, []byte{1}) || w.counts["deferred/chain"] != 1 {
		t.Fatal("deferred chain chunk changed sweep")
	}
	runPostingWorkerTestChunk(t, w)
	if w.completed != 100 || w.target != (PostingPruneBoundary{}) || len(w.cursor) != 0 || w.anchor != (common.Hash{}) {
		t.Fatalf("completion retained sweep: %+v", w)
	}
	for name, want := range map[string]uint64{"chunks": 2, "sweeps": 1, "errors": 1, "scanned/rows": 13, "scanned/bytes": 130, "deleted/rows": 3, "deleted/bytes": 30} {
		if w.counts[name] != want || w.gauges[name].Snapshot().Value() != int64(want) {
			t.Errorf("%s count=%d gauge=%d, want %d", name, w.counts[name], w.gauges[name].Snapshot().Value(), want)
		}
	}
}

func TestPostingPruneWorkerCompletionWaitsForMinimumAdvance(t *testing.T) {
	boundary := postingWorkerTestBoundary(100, 1)
	var targets []uint64
	w := NewPostingPruneWorker(postingWorkerTestConfig(t, func() PostingPruneBoundary { return boundary },
		func(_ context.Context, got PostingPruneBoundary, anchor common.Hash, cursor []byte, _ rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
			if len(cursor) != 0 || anchor != (common.Hash{}) {
				t.Error("new target inherited old sweep cursor")
			}
			targets = append(targets, got.PrunedThrough)
			return PostingPruneChunkOutcome{Result: rawdb.StateChangePostingPruneChunkResult{Complete: true}}, nil
		}))
	t.Cleanup(func() { _ = w.Stop() })
	runPostingWorkerTestChunk(t, w)
	for _, height := range []uint64{100, 101, 109} {
		boundary = postingWorkerTestBoundary(height, 2)
		runPostingWorkerTestChunk(t, w)
	}
	if len(targets) != 1 {
		t.Fatalf("started before minimum advance: %v", targets)
	}
	boundary = postingWorkerTestBoundary(110, 3)
	runPostingWorkerTestChunk(t, w)
	if len(targets) != 2 || targets[1] != 110 || w.completed != 110 || w.counts["sweeps"] != 2 {
		t.Fatalf("minimum advance not honored: targets=%v completed=%d", targets, w.completed)
	}
}

func TestPostingPruneWorkerInvalidOrRegressedPermissionClearsSweep(t *testing.T) {
	for _, invalid := range []string{"regression", "zero", "proof-before-H", "zero-proof-hash"} {
		t.Run(invalid, func(t *testing.T) {
			boundary := postingWorkerTestBoundary(100, 1)
			calls := 0
			w := NewPostingPruneWorker(postingWorkerTestConfig(t, func() PostingPruneBoundary { return boundary },
				func(_ context.Context, _ PostingPruneBoundary, anchor common.Hash, cursor []byte, _ rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
					calls++
					if len(cursor) != 0 || anchor != (common.Hash{}) {
						t.Error("fresh permission reused rejected cursor")
					}
					return PostingPruneChunkOutcome{Anchor: common.Hash{4}, Result: rawdb.StateChangePostingPruneChunkResult{NextCursor: []byte{1}}}, nil
				}))
			t.Cleanup(func() { _ = w.Stop() })
			runPostingWorkerTestChunk(t, w)
			switch invalid {
			case "regression":
				boundary = postingWorkerTestBoundary(90, 2)
			case "zero":
				boundary = PostingPruneBoundary{}
			case "proof-before-H":
				boundary.ProofHead = 99
			case "zero-proof-hash":
				boundary.ProofHash = common.Hash{}
			}
			runPostingWorkerTestChunk(t, w)
			if calls != 1 || w.target != (PostingPruneBoundary{}) || len(w.cursor) != 0 || w.anchor != (common.Hash{}) || w.completed != 0 || w.counts["resets"] != 1 {
				t.Fatalf("invalid permission kept/restarted sweep: calls=%d worker=%+v", calls, w)
			}
			runPostingWorkerTestChunk(t, w)
			if calls != 1 || w.counts["resets"] != 1 {
				t.Fatalf("same rejected permission retried: calls=%d resets=%d", calls, w.counts["resets"])
			}
			boundary = postingWorkerTestBoundary(101, 3)
			runPostingWorkerTestChunk(t, w)
			if calls != 2 || w.target != boundary {
				t.Fatal("new successful permission did not restart sweep")
			}
		})
	}
}

func TestPostingPruneWorkerChainProofChangeRequiresNewPermission(t *testing.T) {
	boundary := postingWorkerTestBoundary(100, 1)
	calls := 0
	w := NewPostingPruneWorker(postingWorkerTestConfig(t, func() PostingPruneBoundary { return boundary },
		func(_ context.Context, _ PostingPruneBoundary, anchor common.Hash, cursor []byte, _ rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
			calls++
			if calls == 2 {
				return PostingPruneChunkOutcome{BoundaryChanged: true}, nil
			}
			if len(cursor) != 0 || anchor != (common.Hash{}) {
				t.Error("new permission kept old anchor/cursor")
			}
			return PostingPruneChunkOutcome{Anchor: common.Hash{4}, Result: rawdb.StateChangePostingPruneChunkResult{NextCursor: []byte{1}}}, nil
		}))
	t.Cleanup(func() { _ = w.Stop() })
	runPostingWorkerTestChunk(t, w)
	runPostingWorkerTestChunk(t, w)
	if w.target != (PostingPruneBoundary{}) || w.rejected != boundary || len(w.cursor) != 0 || w.completed != 0 {
		t.Fatal("chain proof failure retained sweep")
	}
	runPostingWorkerTestChunk(t, w)
	if calls != 2 {
		t.Fatal("same rejected chain permission was reused")
	}
	boundary = postingWorkerTestBoundary(100, 2)
	runPostingWorkerTestChunk(t, w)
	if calls != 3 || w.target != boundary {
		t.Fatal("new proof at the same height was not accepted")
	}
}

func TestPostingPruneWorkerCompletedHeightRegressionWaitsForFreshPermission(t *testing.T) {
	boundary := postingWorkerTestBoundary(100, 1)
	calls := 0
	w := NewPostingPruneWorker(postingWorkerTestConfig(t, func() PostingPruneBoundary { return boundary },
		func(_ context.Context, got PostingPruneBoundary, anchor common.Hash, cursor []byte, _ rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
			calls++
			if got != boundary || len(cursor) != 0 || anchor != (common.Hash{}) {
				t.Error("fresh sweep did not start from the new permission")
			}
			return PostingPruneChunkOutcome{Result: rawdb.StateChangePostingPruneChunkResult{Complete: true}}, nil
		}))
	t.Cleanup(func() { _ = w.Stop() })
	runPostingWorkerTestChunk(t, w)
	boundary = postingWorkerTestBoundary(90, 2)
	runPostingWorkerTestChunk(t, w)
	if calls != 1 || w.completed != 0 || w.gauges["completed/block"].Snapshot().Value() != 0 || w.rejected != boundary {
		t.Fatal("completed height regression did not revoke previous completion")
	}
	runPostingWorkerTestChunk(t, w)
	if calls != 1 || w.counts["resets"] != 1 {
		t.Fatal("regressed permission was reused or repeatedly reset")
	}
	boundary = postingWorkerTestBoundary(90, 3)
	runPostingWorkerTestChunk(t, w)
	if calls != 2 || w.completed != 90 {
		t.Fatal("fresh lower-height permission was incorrectly gated by old completion")
	}
}

func TestPostingPruneWorkerInvalidCompletedProofRevokesAdvanceThreshold(t *testing.T) {
	for _, invalid := range []string{"proof-before-H", "zero-proof-hash"} {
		t.Run(invalid, func(t *testing.T) {
			boundary := postingWorkerTestBoundary(100, 1)
			calls := 0
			w := NewPostingPruneWorker(postingWorkerTestConfig(t, func() PostingPruneBoundary { return boundary },
				func(context.Context, PostingPruneBoundary, common.Hash, []byte, rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
					calls++
					return PostingPruneChunkOutcome{Result: rawdb.StateChangePostingPruneChunkResult{Complete: true}}, nil
				}))
			t.Cleanup(func() { _ = w.Stop() })
			runPostingWorkerTestChunk(t, w)
			if invalid == "proof-before-H" {
				boundary.ProofHead = 99
			} else {
				boundary.ProofHash = common.Hash{}
			}
			runPostingWorkerTestChunk(t, w)
			if calls != 1 || w.completed != 0 || w.gauges["completed/block"].Snapshot().Value() != 0 || w.rejected != boundary || w.counts["resets"] != 1 {
				t.Fatal("invalid proof retained completed-height threshold")
			}
			runPostingWorkerTestChunk(t, w)
			if calls != 1 || w.counts["resets"] != 1 {
				t.Fatal("unchanged invalid proof retried or repeatedly reset")
			}
			boundary = postingWorkerTestBoundary(100, 2)
			runPostingWorkerTestChunk(t, w)
			if calls != 2 || w.completed != 100 {
				t.Fatal("fresh same-height proof was blocked by revoked completion")
			}
		})
	}
}

func TestPostingPruneWorkerPressureAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*maintenance.StoragePressure)
		ready  bool
	}{
		{"healthy", func(*maintenance.StoragePressure) {}, true},
		{"busy-low-latency", func(p *maintenance.StoragePressure) { p.DeviceBusyPPM = 1_000_000 }, true},
		{"idle-high-latency", func(p *maintenance.StoragePressure) { p.DeviceBusyPPM = 0; p.DeviceAwait = 6 * time.Millisecond }, false},
		{"busy-high-latency", func(p *maintenance.StoragePressure) {
			p.DeviceBusyPPM = 1_000_000
			p.DeviceAwait = 6 * time.Millisecond
		}, false},
		{"unknown-engine", func(p *maintenance.StoragePressure) { p.Available = false }, false},
		{"unknown-device", func(p *maintenance.StoragePressure) { p.DeviceAvailable = false }, false},
		{"stale-engine", func(p *maintenance.StoragePressure) { p.SampledAt = p.SampledAt.Add(-16 * time.Second) }, false},
		{"stale-device", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = p.DeviceSampledAt.Add(-16 * time.Second) }, false},
		{"zero-engine-time", func(p *maintenance.StoragePressure) { p.SampledAt = time.Time{} }, false},
		{"zero-device-time", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = time.Time{} }, false},
		{"future-engine", func(p *maintenance.StoragePressure) { p.SampledAt = p.SampledAt.Add(2 * time.Second) }, false},
		{"future-device", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = p.DeviceSampledAt.Add(2 * time.Second) }, false},
		{"write-stalled", func(p *maintenance.StoragePressure) { p.WriteStalled = true }, false},
		{"L0-limit", func(p *maintenance.StoragePressure) { p.L0Sublevels = 8 }, false},
		{"debt-limit", func(p *maintenance.StoragePressure) { p.CompactionDebt = 32 << 30 }, false},
		{"queue-limit", func(p *maintenance.StoragePressure) { p.DeviceQueueMilli = 32_001 }, false},
		{"memtable-near-stop", func(p *maintenance.StoragePressure) { p.MemTableCount = 7 }, false},
		{"engine-specific-L0-stop", func(p *maintenance.StoragePressure) { p.L0Sublevels = 3; p.L0StopWritesThreshold = 4 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			cfg := postingWorkerTestConfig(t, func() PostingPruneBoundary { return postingWorkerTestBoundary(100, 1) },
				func(context.Context, PostingPruneBoundary, common.Hash, []byte, rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
					calls++
					return PostingPruneChunkOutcome{}, nil
				})
			cfg.LoadProbe = func() maintenance.StoragePressure {
				p := postingWorkerTestPressure(time.Now())
				tc.change(&p)
				return p
			}
			w := NewPostingPruneWorker(cfg)
			t.Cleanup(func() { _ = w.Stop() })
			runPostingWorkerTestChunk(t, w)
			if (calls == 1) != tc.ready || (!tc.ready && w.counts["deferred/pressure"] != 1) {
				t.Fatalf("pressure admission calls=%d deferred=%d, want ready=%v", calls, w.counts["deferred/pressure"], tc.ready)
			}
		})
	}
}

func TestPostingPruneWorkerBusyGateDoesNotCallChunk(t *testing.T) {
	gate := maintenance.NewHeavyWorkGate()
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("cannot hold fresh gate")
	}
	defer release()
	calls := 0
	cfg := postingWorkerTestConfig(t, func() PostingPruneBoundary { return postingWorkerTestBoundary(100, 1) },
		func(context.Context, PostingPruneBoundary, common.Hash, []byte, rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
			calls++
			if unexpectedRelease, acquired := gate.TryAcquire(); acquired {
				unexpectedRelease()
				t.Error("chunk did not own maintenance gate")
			}
			return PostingPruneChunkOutcome{}, nil
		})
	cfg.HeavyWorkGate = gate
	w := NewPostingPruneWorker(cfg)
	t.Cleanup(func() { _ = w.Stop() })
	runPostingWorkerTestChunk(t, w)
	if calls != 0 || w.counts["deferred/gate"] != 1 {
		t.Fatal("busy gate admitted a chunk")
	}
	release()
	runPostingWorkerTestChunk(t, w)
	if calls != 1 {
		t.Fatal("released gate did not admit a chunk")
	}
	if release, acquired := gate.TryAcquire(); !acquired {
		t.Fatal("chunk leaked maintenance gate")
	} else {
		release()
	}
}

func TestPostingPruneWorkerStopCancelsAndJoinsInflight(t *testing.T) {
	entered, canceled, leave := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	cfg := postingWorkerTestConfig(t, func() PostingPruneBoundary { return postingWorkerTestBoundary(100, 1) },
		func(ctx context.Context, _ PostingPruneBoundary, _ common.Hash, _ []byte, _ rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
			if calls.Add(1) != 1 {
				return PostingPruneChunkOutcome{}, errors.New("unexpected second chunk")
			}
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-leave
			return PostingPruneChunkOutcome{Result: rawdb.StateChangePostingPruneChunkResult{RowsScanned: 7}}, ctx.Err()
		})
	w := NewPostingPruneWorker(cfg)
	defer func() {
		select {
		case <-leave:
		default:
			close(leave)
		}
		_ = w.Stop()
	}()
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	postingWorkerTestWait(t, entered, "first chunk")
	stopped := make(chan struct{})
	go func() { _ = w.Stop(); close(stopped) }()
	postingWorkerTestWait(t, canceled, "inflight cancellation")
	select {
	case <-stopped:
		t.Fatal("Stop returned before inflight exit")
	default:
	}
	if release, acquired := cfg.HeavyWorkGate.TryAcquire(); acquired {
		release()
		t.Fatal("inflight chunk released gate before return")
	}
	close(leave)
	postingWorkerTestWait(t, stopped, "worker join")
	if calls.Load() != 1 || w.gauges["enabled"].Snapshot().Value() != 0 || w.counts["errors"] != 0 || w.counts["scanned/rows"] != 7 {
		t.Fatalf("stopped worker state: calls=%d enabled=%d errors=%d scanned=%d", calls.Load(), w.gauges["enabled"].Snapshot().Value(), w.counts["errors"], w.counts["scanned/rows"])
	}
	if err := w.Start(); err == nil {
		t.Fatal("stopped worker restarted")
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * cfg.Interval)
	if calls.Load() != 1 {
		t.Fatal("worker called a chunk after Stop")
	}
	if release, acquired := cfg.HeavyWorkGate.TryAcquire(); !acquired {
		t.Fatal("Stop leaked maintenance gate")
	} else {
		release()
	}
}

func postingWorkerTestWait(t *testing.T, channel <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for " + label)
	}
}

func TestPostingPruneWorkerStartValidationAndStopBeforeStart(t *testing.T) {
	for _, missing := range []string{"boundary", "chunk", "pressure", "partial-limits"} {
		t.Run(missing, func(t *testing.T) {
			cfg := postingWorkerTestConfig(t, func() PostingPruneBoundary { return PostingPruneBoundary{} },
				func(context.Context, PostingPruneBoundary, common.Hash, []byte, rawdb.StateChangePostingPruneLimits) (PostingPruneChunkOutcome, error) {
					t.Error("invalid worker called chunk")
					return PostingPruneChunkOutcome{}, nil
				})
			switch missing {
			case "boundary":
				cfg.Boundary = nil
			case "chunk":
				cfg.Chunk = nil
			case "pressure":
				cfg.LoadProbe = nil
			case "partial-limits":
				cfg.Limits.MaxScannedRows = 1
			}
			w := NewPostingPruneWorker(cfg)
			if err := w.Start(); err == nil {
				t.Fatal("unsafe configuration started")
			}
			if err := w.Stop(); err != nil {
				t.Fatal(err)
			}
			if err := w.Stop(); err != nil {
				t.Fatal(err)
			}
			if err := w.Start(); err == nil {
				t.Fatal("stopped worker started")
			}
		})
	}
}
