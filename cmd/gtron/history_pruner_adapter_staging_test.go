package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	corestate "github.com/tronprotocol/go-tron/core/state"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/params"
)

// This is the same adapter passed to NewSnapshotLifecycle in main. A moved
// bucket has no hot changeset left, so a hot-only fallback would miss its row.
func TestHistorySnapshotLifecycleAdapterReadsStagedTarget(t *testing.T) {
	f := newHistoryStagingE2EFixture(t)
	plan, err := f.command(t, "migrate", "--max-buckets", "0")
	if err != nil || plan.PlanID == "" {
		t.Fatalf("migration plan: %+v %v", plan, err)
	}
	if _, err := f.command(t, "apply", "--plan-id", plan.PlanID); err != nil {
		t.Fatal(err)
	}

	hot, err := rawdb.NewPebbleDB(chainDataDir(f.datadir), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	stage, err := rawdb.NewHistoryStagingPebbleDB(defaultHistoryStagingDir(f.datadir), 16, 16, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	identity, present, err := rawdb.ReadHistoryStagingIdentity(hot)
	if err != nil || !present {
		t.Fatalf("staging identity: present=%v err=%v", present, err)
	}
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, identity)
	if err != nil {
		t.Fatal(err)
	}
	genesis := params.DefaultMainnetGenesis()
	bc, err := core.NewBlockChainWithAncient(hot, corestate.NewDatabase(rawdb.WrapKeyValueStore(hot)), genesis.Config, rawdb.NoopAncient{})
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	coldManager, err := statesnapshots.OpenManager(f.cold)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetStateCodeColdHistory(coldManager)
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}

	count := 0
	if err := rawdb.IterateStateDomainChangesContext(context.Background(), hot, 2500, func(_ *rawdb.StateDomainChange) (bool, error) {
		count++
		return true, nil
	}); err != nil || count != 0 {
		t.Fatalf("moved source remains hot: rows=%d err=%v", count, err)
	}
	for name, adapter := range map[string]any{
		"production lifecycle": newDomainPrunerChainSource(bc, nil),
		"direct snapshot":      newStateSnapshotChainSource(bc),
	} {
		source, ok := adapter.(interface {
			AcquireStateHistorySourceView(context.Context) (statesnapshots.AggregatorDB, func() error, error)
		})
		if !ok {
			t.Fatalf("%s does not forward the pinned history-source interface", name)
		}
		view, release, err := source.AcquireStateHistorySourceView(context.Background())
		if err != nil || view == nil || release == nil {
			t.Fatalf("%s routed view: %v", name, err)
		}
		count = 0
		err = rawdb.IterateStateDomainChangesContext(context.Background(), view, 2500, func(row *rawdb.StateDomainChange) (bool, error) {
			count++
			if !bytes.Equal(row.Prev, []byte("recent-repair")) {
				t.Errorf("%s returned wrong staged repair: %q", name, row.Prev)
			}
			return true, nil
		})
		err = joinHistoryAdapterTestErrors(err, release())
		if err != nil || count != 1 {
			t.Fatalf("%s staged bucket rows=%d err=%v, want one", name, count, err)
		}
	}

	// Build a real immutable cold trio from the adapter's routed view. The
	// normal packed row at block 1600 was copied to TARGET and removed from hot.
	adapter := newDomainPrunerChainSource(bc, nil).(interface {
		AcquireStateHistorySourceView(context.Context) (statesnapshots.AggregatorDB, func() error, error)
	})
	view, release, err := adapter.AcquireStateHistorySourceView(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var expected []byte
	err = rawdb.IterateStateDomainChangesContext(context.Background(), view, 1600, func(row *rawdb.StateDomainChange) (bool, error) {
		expected = bytes.Clone(row.Prev)
		return true, nil
	})
	if err != nil || len(expected) < 512<<10 {
		t.Fatalf("routed source block 1600: prev=%d err=%v", len(expected), err)
	}
	outputDir := t.TempDir()
	refs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDBByBlockRangeContext(
		context.Background(), view, outputDir, 1600, 1600, 1600, 1600,
		filepath.Join("history", "state-domain-change-1600-1600.seg"), etl.Options{})
	err = joinHistoryAdapterTestErrors(err, release())
	if err != nil {
		t.Fatalf("build routed cold history: %v", err)
	}
	if err := statesnapshots.PublishManifest(outputDir, statesnapshots.NewManifest(1600, 1600, refs)); err != nil {
		t.Fatal(err)
	}
	reader, err := statesnapshots.OpenManager(outputDir)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	err = reader.IterateStateDomainChanges(1600, 1600, func(row *rawdb.StateDomainChange) (bool, error) {
		count++
		if row.BlockNum != 1600 || !bytes.Equal(row.Prev, expected) {
			t.Errorf("cold row mismatch: block=%d prev=%d", row.BlockNum, len(row.Prev))
		}
		return true, nil
	})
	if err != nil || count != 1 {
		t.Fatalf("cold history rows=%d err=%v, want one staged row", count, err)
	}
}

func joinHistoryAdapterTestErrors(first, second error) error {
	if first != nil {
		return first
	}
	return second
}
