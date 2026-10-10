package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestHistoryStagingMoverPublishesAcrossUnrelatedManifestAppend(t *testing.T) {
	for _, chainBound := range []bool{true, false} {
		for _, path := range []string{"adopt", "adopt-resume", "adopt-file-replace", "certify", "finalize"} {
			t.Run(fmt.Sprintf("%s/chain-bound=%t", path, chainBound), func(t *testing.T) {
				isAdopt := path != "certify" && path != "finalize"
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
					if !isAdopt {
						receipt, err := rawdb.NewHistoryStagingBlockHasher(n).Finish(block.Hash(), 1, n, n)
						if err != nil {
							t.Fatal(err)
						}
						if err := rawdb.WriteHistoryStagingBlockComplete(f.hot, receipt); err != nil {
							t.Fatal(err)
						}
					}
				}
				pressure := func() maintenance.StoragePressure {
					now := time.Now()
					return maintenance.StoragePressure{Available: true, SampledAt: now, DeviceAvailable: true, DeviceSampledAt: now}
				}
				mover, err := NewHistoryStagingMover(f.bc, HistoryStagingMoverConfig{HistoryWindow: 1, Cadence: time.Second, HeavyWorkGate: maintenance.NewHeavyWorkGate(), HotPressure: pressure, StagePressure: pressure,
					Limits: rawdb.HistoryStagingLimits{MaxRowBytes: 1 << 20, MaxBatchBytes: 4 << 20, MaxBucketBytes: 64 << 20, MaxWorkBytes: 128 << 20, MaxDecodedBytes: 32 << 20, MinFreeBytes: 1, FreeBytes: func() (uint64, error) { return 1 << 40, nil }}})
				if err != nil {
					t.Fatal(err)
				}
				if !isAdopt {
					if err := mover.runAdmitted(ctx); err != nil {
						t.Fatal(err)
					}
				}
				dir := t.TempDir()
				cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
				ref := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory, FromTxNum: 1024, ToTxNum: 2047, Path: cfg.HistoryPath(1024, 2047)}
				h, i, a, err := cfg.WriteHistory(dir, ref, nil, ranges)
				if err != nil {
					t.Fatal(err)
				}
				genesis, _ := rawdb.ReadBlockHash(f.bc.chaindb, 0)
				old := snapshots.NewManifestForChain(1024, 2047, []snapshots.SegmentRef{h, i, a}, snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: fmt.Sprintf("0x%x", genesis[:])})
				if !chainBound {
					old.Chain = nil
				}
				if err := snapshots.PublishManifest(dir, old); err != nil {
					t.Fatal(err)
				}
				cold, err := snapshots.OpenManager(dir)
				if err != nil {
					t.Fatal(err)
				}
				if err := snapshots.BindHistoryStagingColdRetention(dir, f.manager); err != nil {
					t.Fatal(err)
				}
				f.bc.SetStateCodeColdHistory(cold)
				if path == "finalize" {
					if certified, err := mover.certifyNewColdTarget(ctx, 1); err != nil || !certified {
						t.Fatalf("initial target certification: %v %v", certified, err)
					}
				}
				extra := make([]*rawdb.StateTxRange, 0, 1024)
				for n := uint64(2048); n <= 3071; n++ {
					extra = append(extra, &rawdb.StateTxRange{BlockNum: n, BlockHash: testRestartBlock(n).Hash(), BeginTxNum: n, EndTxNum: n})
				}
				ref.FromTxNum, ref.ToTxNum, ref.Path = 2048, 3071, cfg.HistoryPath(2048, 3071)
				h, i, a, err = cfg.WriteHistory(dir, ref, nil, extra)
				if err != nil {
					t.Fatal(err)
				}
				live := snapshots.NewManifest(1024, 3071, append(append([]snapshots.SegmentRef(nil), old.Segments...), h, i, a))
				if old.Chain != nil {
					chain := *old.Chain
					live.Chain = &chain
				}
				live.Generation = old.Generation + 1
				appended := false
				replaced := false
				if path == "adopt-file-replace" {
					mover.cfg.Limits.Checkpoint = func(uint64) error {
						if replaced {
							return nil
						}
						file := filepath.Join(dir, old.Segments[0].Path)
						data, err := os.ReadFile(file)
						if err != nil {
							return err
						}
						if err := os.Rename(file, file+".original"); err != nil {
							return err
						}
						replaced = true
						return os.WriteFile(file, data, 0600)
					}
				}
				if path == "adopt-resume" {
					interrupted := errors.New("interrupt after initial proof and durable claim")
					mover.cfg.Limits.Checkpoint = func(uint64) error { return interrupted }
					if err := mover.runAdmitted(ctx); !errors.Is(err, interrupted) {
						t.Fatalf("claim interruption: %v", err)
					}
					mover.cfg.Limits.Checkpoint = nil
					if _, present, err := f.manager.ReadClaim(1); err != nil || !present {
						t.Fatalf("interrupted claim was not durable: %v %v", present, err)
					}
				}
				beforeRecheck := historyStagingMoverManifestRecheck.Snapshot().Count()
				beforeReuse := historyStagingMoverFinalizeReuse.Snapshot().Count()
				beforeAdoptReuse := historyStagingMoverAdoptReuse.Snapshot().Count()
				proofCtx := maintenance.WithWorkCheckpoint(ctx, func(uint64) error {
					if appended {
						return nil
					}
					appended = true
					if err := snapshots.PublishManifest(dir, live); err != nil {
						return err
					}
					merged, err := snapshots.CompactHistoryDomain(dir, snapshots.SegmentDatasetStateDomainChange, snapshots.CompactionConfig{MaxSteps: 2})
					if err != nil {
						return err
					}
					if merged.Merged {
						return errors.New("compaction replaced the protected proof dependency")
					}
					return nil
				})
				switch path {
				case "adopt", "adopt-resume", "adopt-file-replace":
					err = mover.runAdmitted(proofCtx)
				case "certify":
					var certified bool
					certified, err = mover.certifyNewColdTarget(proofCtx, 1)
					if err == nil && !certified {
						t.Fatal("target was not certified")
					}
				case "finalize":
					err = mover.finalizeColdBucket(proofCtx, 1)
				}
				if path == "adopt-file-replace" {
					route, present, routeErr := f.manager.ReadRoute(1)
					if !replaced || err == nil || routeErr != nil || !present || route.Owner != rawdb.HistoryStagingOwnerSource || route.SourceCleared ||
						historyStagingMoverAdoptReuse.Snapshot().Count() != beforeAdoptReuse || historyStagingMoverFinalizeReuse.Snapshot().Count() != beforeReuse {
						t.Fatalf("changed fresh-proof dependency adopted: replaced=%v route=%+v err=%v routeErr=%v", replaced, route, err, routeErr)
					}
					return
				}
				if err != nil || !appended || historyStagingMoverManifestRecheck.Snapshot().Count() <= beforeRecheck {
					t.Fatalf("publication did not accept unchanged dependencies: appended=%v revalidated=%d err=%v", appended, historyStagingMoverManifestRecheck.Snapshot().Count()-beforeRecheck, err)
				}
				if isAdopt && historyStagingMoverFinalizeReuse.Snapshot().Count() != beforeReuse+1 {
					t.Fatal("same-run adoption did not reuse its authenticated final proof")
				}
				if path == "adopt" && historyStagingMoverAdoptReuse.Snapshot().Count() != beforeAdoptReuse+1 {
					t.Fatal("same-run adoption repeated the authenticated initial proof")
				}
				if path == "adopt-resume" && historyStagingMoverAdoptReuse.Snapshot().Count() != beforeAdoptReuse {
					t.Fatal("resumed claim reused a proof from a previous run")
				}
				binding, present, err := f.manager.ReadColdBindingAt(1, 1)
				if err != nil || !present || binding.ManifestEpoch != live.Generation {
					t.Fatalf("binding not published against current manifest: %+v %v", binding, err)
				}
				route, present, err := f.manager.ReadRoute(1)
				if err != nil || !present || !route.SourceCleared {
					t.Fatalf("invalid route after publication: %+v %v", route, err)
				}
				if path != "certify" && (route.Owner != rawdb.HistoryStagingOwnerCold || !route.TargetCleared) {
					t.Fatalf("cold handoff not completed: %+v", route)
				}
			})
		}
	}
}

func TestHistoryStagingMoverFinalCanonicalPublicationGuard(t *testing.T) {
	f := newStagingIndexGCFixture(t)
	ctx := context.Background()
	if err := f.manager.EnsureCanonicalSourceRoute(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	for n := uint64(1024); n <= 2047; n++ {
		block := testRestartBlock(n)
		if err := rawdb.WriteBlock(f.hot, block); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateTxRange(f.hot, n, block.Hash(), n, n); err != nil {
			t.Fatal(err)
		}
	}
	genesis, _ := rawdb.ReadBlockHash(f.bc.chaindb, 0)
	dir := t.TempDir()
	manifest := snapshots.NewManifestForChain(0, 0, nil, snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: genesis.Hex()})
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	cold, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.bc.SetStateCodeColdHistory(cold)
	mover := &HistoryStagingMover{bc: f.bc}
	route, _, err := f.manager.ReadRoute(1)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := mover.canonicalBucketBlocks(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"none", "route", "canonical", "stage-manager", "cold-manager", "replay", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			expectedRoute := route
			expectedBlocks := append([]rawdb.HistoryStagingBlockProof(nil), blocks...)
			stageManager, coldManager := f.manager, cold
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			switch fault {
			case "route":
				expectedRoute.ColdBindingEpoch++
			case "canonical":
				expectedBlocks[0].Hash[0] ^= 1
			case "stage-manager":
				stageManager = nil
			case "cold-manager":
				coldManager = nil
			case "replay":
				f.bc.historyStagingReplayEpoch.Store(1)
				defer f.bc.historyStagingReplayEpoch.Store(0)
			case "cancel":
				cancel()
			}
			published := false
			err := mover.withCanonicalBucketPublication(ctx, stageManager, coldManager, expectedRoute, expectedBlocks, func() error { published = true; return nil })
			if fault == "none" {
				if err != nil || !published {
					t.Fatalf("valid proof rejected: %v", err)
				}
			} else if err == nil || published {
				t.Fatalf("changed final guard published: %v", err)
			} else if fault == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("wrong cancellation: %v", err)
			}
		})
	}
}
