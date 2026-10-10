package core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func stagingFinalProofFixture(t *testing.T) (*stagingIndexGCFixture, *HistoryStagingMover, string, *historyStagingFinalProof) {
	t.Helper()
	f := newStagingIndexGCFixture(t)
	ctx := context.Background()
	if err := f.manager.EnsureCanonicalSourceRoute(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	ranges := make([]*rawdb.StateTxRange, 0, 1024)
	for n := uint64(1024); n <= 2047; n++ {
		block := testRestartBlock(n)
		if err := rawdb.WriteBlock(f.hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(f.hot, n, block.Hash(), n, n); err != nil {
			t.Fatal(err)
		}
		ranges = append(ranges, &rawdb.StateTxRange{BlockNum: n, BlockHash: block.Hash(), BeginTxNum: n, EndTxNum: n})
		receipt, err := rawdb.NewHistoryStagingBlockHasher(n).Finish(block.Hash(), 1, n, n)
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteHistoryStagingBlockComplete(f.hot, receipt); err != nil {
			t.Fatal(err)
		}
	}
	pressure := func() maintenance.StoragePressure {
		now := time.Now()
		return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
	}
	m, err := NewHistoryStagingMover(f.bc, HistoryStagingMoverConfig{HistoryWindow: 1, Cadence: time.Second, HeavyWorkGate: maintenance.NewHeavyWorkGate(), HotPressure: pressure, StagePressure: pressure,
		Limits: rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20, MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20, MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.runAdmitted(ctx); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	ref := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory, FromTxNum: 1024, ToTxNum: 2047, Path: cfg.HistoryPath(1024, 2047)}
	h, i, a, err := cfg.WriteHistory(dir, ref, nil, ranges)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1024, 2047, []snapshots.SegmentRef{h, i, a})); err != nil {
		t.Fatal(err)
	}
	cold, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.bc.SetStateCodeColdHistory(cold)
	if certified, err := m.certifyNewColdTarget(ctx, 1); err != nil || !certified {
		t.Fatalf("certify: %v %v", certified, err)
	}
	binding, present, err := f.manager.ReadColdBindingAt(1, 1)
	if err != nil || !present {
		t.Fatalf("binding: %v %v", present, err)
	}
	blocks, err := m.canonicalBucketBlocks(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	release, err := protectHistoryStagingColdProof(dir, blocks)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	pinned, release, err := cold.PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	manifest := pinned.Manifest()
	proofCtx, facts, err := snapshots.WithHistoryStagingPhysicalFacts(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshots.RebindHistoryStagingColdBinding(proofCtx, dir, manifest, binding, blocks); err != nil {
		t.Fatal(err)
	}
	return f, m, dir, &historyStagingFinalProof{manager: f.manager, cold: cold, binding: binding, manifest: manifest, facts: facts, blocks: blocks}
}

func TestHistoryStagingMoverFinalProofReuse(t *testing.T) {
	for _, fault := range []string{"none", "append", "binding", "capsule-epoch", "canonical", "reset", "file-rewrite", "file-replace", "trio-retired", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			f, mover, dir, proof := stagingFinalProofFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			manifest := proof.cold.Manifest()
			switch fault {
			case "append":
				// A metadata-only publication after adoption must refresh the
				// generation without repeating the semantic row walk.
				manifest.Generation++
				if err := snapshots.PublishManifest(dir, manifest); err != nil {
					t.Fatal(err)
				}
			case "binding":
				updated := proof.binding
				updated.BindingEpoch++
				if err := f.manager.CertifyColdRange(ctx, updated, func() error { return nil }); err != nil {
					t.Fatal(err)
				}
			case "capsule-epoch":
				proof.binding.Epoch++
			case "canonical":
				block := testRestartBlock(1024)
				if err := rawdb.WriteStateTxRange(f.hot, 1024, block.Hash(), 1024, 1025); err != nil {
					t.Fatal(err)
				}
			case "reset":
				f.bc.historyStagingReplayEpoch.Store(1)
			case "file-rewrite", "file-replace":
				path := filepath.Join(dir, manifest.Segments[0].Path)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if fault == "file-rewrite" {
					data[len(data)-1] ^= 1
				} else if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "trio-retired":
				manifest.Generation++
				manifest.Segments = nil
				if err := snapshots.PublishManifest(dir, manifest); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			}
			walks := 0
			ctx = maintenance.WithWorkCheckpoint(ctx, func(uint64) error {
				walks++
				return errors.New("unexpected repeated cold semantic walk")
			})
			err := mover.finalizeColdBucketWithProof(ctx, 1, proof)
			route, present, routeErr := f.manager.ReadRoute(1)
			if routeErr != nil || !present || walks != 0 {
				t.Fatalf("route=%+v walks=%d err=%v", route, walks, routeErr)
			}
			if fault == "none" || fault == "append" {
				if err != nil || route.Owner != rawdb.HistoryStagingOwnerCold || !route.TargetCleared {
					t.Fatalf("valid handoff failed: route=%+v err=%v", route, err)
				}
				if fault == "append" {
					binding, _, err := f.manager.ReadColdBindingAt(1, 1)
					if err != nil || binding.ManifestEpoch != manifest.Generation || binding.BindingEpoch != proof.binding.BindingEpoch+1 {
						t.Fatalf("binding generation was not refreshed: %+v %v", binding, err)
					}
				}
			} else if err == nil || route.Owner != rawdb.HistoryStagingOwnerTarget || route.TargetCleared {
				t.Fatalf("invalid proof published: route=%+v err=%v", route, err)
			}
		})
	}
}

func TestHistoryStagingMoverFinalProofRecoveryAuthenticates(t *testing.T) {
	f, mover, _, _ := stagingFinalProofFixture(t)
	interrupted := errors.New("interrupted semantic recovery")
	ctx := maintenance.WithWorkCheckpoint(context.Background(), func(uint64) error { return interrupted })
	if err := mover.finalizeColdBucket(ctx, 1); !errors.Is(err, interrupted) {
		t.Fatalf("recovery skipped full authentication: %v", err)
	}
	route, _, _ := f.manager.ReadRoute(1)
	if route.Owner != rawdb.HistoryStagingOwnerTarget || route.TargetCleared {
		t.Fatalf("interrupted recovery cleared target: %+v", route)
	}
	if err := mover.finalizeColdBucket(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
}
