package pruning

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func offlineLegacyHistoryFixture(t *testing.T) (ethdb.KeyValueStore, OfflineHistoryOptions) {
	t.Helper()
	db, opts := offlineHistoryFixture(t)
	_, err := offlineHistoryPassContext(context.Background(), db, opts, func(dir string, manifest *snapshots.Manifest) error {
		if err := snapshots.PublishManifest(dir, manifest); err != nil {
			return err
		}
		return errors.New("stop after fixture history publication")
	})
	if err == nil {
		t.Fatal("fixture publication did not stop")
	}
	m := offlineHistoryManifest(t, opts.SnapshotDir)
	if len(m.Segments) != 3 {
		t.Fatalf("fixture did not build history: %v", err)
	}
	m.Chain = nil
	if err := snapshots.PublishManifest(opts.SnapshotDir, m); err != nil {
		t.Fatal(err)
	}
	opts.LegacyManifestSHA256, err = offlineHistoryManifestDigest(opts.SnapshotDir)
	if err != nil {
		t.Fatal(err)
	}
	canonical := opts.CanonicalHash
	opts.CanonicalHash = func(block uint64) (common.Hash, bool, error) {
		if block == 0 {
			return common.HexToHash(opts.ExpectedChain.GenesisHash), true, nil
		}
		return canonical(block)
	}
	return db, opts
}

func TestOfflineHistoryLegacyPlanAndPassPreserveUnboundManifest(t *testing.T) {
	db, opts := offlineLegacyHistoryFixture(t)
	m := offlineHistoryManifest(t, opts.SnapshotDir)
	// These families are intentionally opaque to history maintenance. Their
	// files and references must survive without being read or assigned a chain.
	unrelated := []snapshots.SegmentRef{
		{Dataset: snapshots.SegmentDatasetEventLog, Kind: snapshots.SegmentEventLog, FromTxNum: 1, ToTxNum: 2, Path: "log/event-log-1-2.seg"},
		{Dataset: snapshots.SegmentDatasetEventLog, Kind: snapshots.SegmentEventLogIndex, FromTxNum: 1, ToTxNum: 2, Path: "log/event-log-index-1-2.idx"},
	}
	for _, ref := range unrelated {
		p := filepath.Join(opts.SnapshotDir, ref.Path)
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("unrelated family unchanged"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	m.Segments = append(m.Segments, unrelated...)
	if err := snapshots.PublishManifest(opts.SnapshotDir, m); err != nil {
		t.Fatal(err)
	}
	opts.LegacyManifestSHA256, _ = offlineHistoryManifestDigest(opts.SnapshotDir)
	before, err := os.ReadFile(filepath.Join(opts.SnapshotDir, snapshots.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanOfflineHistoryContext(context.Background(), db, opts)
	if err != nil || !plan.LegacyManifestVerified || plan.BuildNeeded || plan.FromBlock != 1 {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
	after, _ := os.ReadFile(filepath.Join(opts.SnapshotDir, snapshots.ManifestFile))
	if string(after) != string(before) {
		t.Fatal("legacy read-only proof rewrote manifest")
	}
	for _, block := range []uint64{1, 2} {
		offlineHistoryAssertHot(t, db, block, true)
	}
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err != nil || !result.Plan.LegacyManifestVerified || !result.ProgressDurable || result.ComparedRecords != 2 || result.Built {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if _, err := PlanOfflineHistoryContext(context.Background(), db, opts); err == nil || !strings.Contains(err.Error(), "SHA256") {
		t.Fatalf("stale pin was accepted: %v", err)
	}
	opts.LegacyManifestSHA256, _ = offlineHistoryManifestDigest(opts.SnapshotDir)
	result, err = OfflineHistoryPassContext(context.Background(), db, opts)
	if err != nil || !result.Built || !result.ProgressDurable || !result.Plan.LegacyManifestVerified {
		t.Fatalf("new range result=%+v error=%v", result, err)
	}
	m = offlineHistoryManifest(t, opts.SnapshotDir)
	if m.Chain != nil || m.Progress.HotPruneBlockNum != 4 {
		t.Fatalf("legacy pass relabeled manifest or lost cursor: %+v", m)
	}
	var retained []snapshots.SegmentRef
	for _, ref := range m.Segments {
		if ref.Dataset == snapshots.SegmentDatasetEventLog {
			retained = append(retained, ref)
			data, err := os.ReadFile(filepath.Join(opts.SnapshotDir, ref.Path))
			if err != nil || string(data) != "unrelated family unchanged" {
				t.Fatalf("unrelated file changed: %s %v", data, err)
			}
		}
	}
	if !reflect.DeepEqual(retained, unrelated) {
		t.Fatalf("unrelated refs changed: %+v", retained)
	}
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	var recovered uint64
	for _, ref := range offlineHistoryRefs(m) {
		if err := cfg.IterateHistoryRange(opts.SnapshotDir, m, ref, 1, 12, func(row *rawdb.StateDomainChange) (bool, error) {
			if row.BlockNum < 1 || row.BlockNum > 4 || !row.PrevExists || string(row.Prev) != "prev" {
				t.Fatalf("lost historical previous image: %+v", row)
			}
			recovered++
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if recovered != 4 {
		t.Fatalf("recovered %d previous images, want 4", recovered)
	}
	for block := uint64(1); block <= 8; block++ {
		offlineHistoryAssertHot(t, db, block, block > 4)
	}
}

func TestOfflineHistoryLegacyRejectsUnprovenInputsWithoutWrites(t *testing.T) {
	for _, failure := range []string{"no-pin", "wrong-pin", "wrong-genesis", "missing-genesis", "genesis-read-error", "retained-endpoint", "canonical-endpoint", "missing-companion", "missing-history-proof"} {
		t.Run(failure, func(t *testing.T) {
			db, opts := offlineLegacyHistoryFixture(t)
			switch failure {
			case "no-pin":
				opts.LegacyManifestSHA256 = ""
			case "wrong-pin":
				opts.LegacyManifestSHA256 = strings.Repeat("0", 64)
			case "wrong-genesis":
				canonical := opts.CanonicalHash
				opts.CanonicalHash = func(n uint64) (common.Hash, bool, error) {
					if n == 0 {
						return common.Hash{99}, true, nil
					}
					return canonical(n)
				}
			case "missing-genesis", "genesis-read-error":
				canonical := opts.CanonicalHash
				opts.CanonicalHash = func(n uint64) (common.Hash, bool, error) {
					if n == 0 {
						if failure == "genesis-read-error" {
							return common.Hash{}, false, errors.New("genesis storage failure")
						}
						return common.Hash{}, false, nil
					}
					return canonical(n)
				}
			case "retained-endpoint":
				if err := rawdb.WriteStateTxRange(db, 2, common.Hash{2}, 5, 6); err != nil {
					t.Fatal(err)
				}
			case "canonical-endpoint":
				canonical := opts.CanonicalHash
				opts.CanonicalHash = func(n uint64) (common.Hash, bool, error) {
					if n == 2 {
						return common.Hash{99}, true, nil
					}
					return canonical(n)
				}
			case "missing-companion":
				for _, ref := range offlineHistoryManifest(t, opts.SnapshotDir).Segments {
					if ref.Kind == snapshots.SegmentInverted {
						if err := os.Remove(filepath.Join(opts.SnapshotDir, ref.Path)); err != nil {
							t.Fatal(err)
						}
					}
				}
			case "missing-history-proof":
				m := snapshots.NewManifest(0, 0, nil)
				if err := snapshots.PublishManifest(opts.SnapshotDir, m); err != nil {
					t.Fatal(err)
				}
				opts.LegacyManifestSHA256, _ = offlineHistoryManifestDigest(opts.SnapshotDir)
			}
			before, _ := os.ReadFile(filepath.Join(opts.SnapshotDir, snapshots.ManifestFile))
			result, err := OfflineHistoryPassContext(context.Background(), db, opts)
			if err == nil || result.Built || result.PublicationAttempted || result.DeletedBlocks != 0 {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			after, _ := os.ReadFile(filepath.Join(opts.SnapshotDir, snapshots.ManifestFile))
			if string(after) != string(before) {
				t.Fatal("failed proof rewrote manifest")
			}
			offlineHistoryAssertHot(t, db, 1, true)
		})
	}
}

func TestOfflineHistoryLegacyPinCannotOverrideBoundIdentity(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	opts.LegacyManifestSHA256, _ = offlineHistoryManifestDigest(opts.SnapshotDir)
	opts.ExpectedChain.GenesisHash = strings.Repeat("22", 32)
	if _, err := PlanOfflineHistoryContext(context.Background(), db, opts); err == nil || !strings.Contains(err.Error(), "chain identity mismatch") {
		t.Fatalf("pin bypassed bound identity: %v", err)
	}
	opts.ExpectedChain.GenesisHash = strings.Repeat("11", 32)
	opts.LegacyManifestSHA256 = strings.Repeat("0", 64)
	if _, err := PlanOfflineHistoryContext(context.Background(), db, opts); err == nil || !strings.Contains(err.Error(), "SHA256") {
		t.Fatalf("incorrect pin ignored on bound manifest: %v", err)
	}
}

func TestOfflineHistoryLegacyBoundaryProofDoesNotReplacePreimageVerification(t *testing.T) {
	db, opts := offlineLegacyHistoryFixture(t)
	change, ok, err := rawdb.ReadStateDomainChange(db, 1, 1)
	if err != nil || !ok {
		t.Fatal(err)
	}
	change.Prev = []byte("different retained hot previous image")
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
		t.Fatal(err)
	}
	if plan, err := PlanOfflineHistoryContext(context.Background(), db, opts); err != nil || !plan.LegacyManifestVerified {
		t.Fatalf("boundary-only plan=%+v error=%v", plan, err)
	}
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err == nil || !strings.Contains(err.Error(), "previous image mismatch") || result.DeletedBlocks != 0 || result.ProgressPublicationAttempted {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	offlineHistoryAssertHot(t, db, 1, true)
	if digest, _ := offlineHistoryManifestDigest(opts.SnapshotDir); digest != opts.LegacyManifestSHA256 {
		t.Fatal("preimage failure mutated legacy manifest")
	}
}

func TestOfflineHistoryLegacyRejectsNonadjacentTrioBlocks(t *testing.T) {
	db, opts := offlineLegacyHistoryFixture(t)
	// The tx numbers meet, but the block identities omit block 3. Every
	// endpoint separately matches its retained/canonical row, so only the
	// inter-trio block-continuity proof can reject this gap.
	writeSnapPruningChange(t, db, 4, 7, 9)
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeContext(context.Background(), db, opts.SnapshotDir, 7, 9, 4, 4, cfg.HistoryPath(7, 9), opts.ETL)
	if err != nil {
		t.Fatal(err)
	}
	m := offlineHistoryManifest(t, opts.SnapshotDir)
	m.Segments = append(m.Segments, refs...)
	m.VisibleTxEnd = 9
	if err := snapshots.PublishManifest(opts.SnapshotDir, m); err != nil {
		t.Fatal(err)
	}
	opts.LegacyManifestSHA256, _ = offlineHistoryManifestDigest(opts.SnapshotDir)
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err == nil || !strings.Contains(err.Error(), "boundaries are not contiguous") || result.DeletedBlocks != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	offlineHistoryAssertHot(t, db, 1, true)
}
