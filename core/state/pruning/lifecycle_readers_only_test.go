package pruning

import (
	"bytes"
	"context"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestSnapshotLifecycleReadersOnlyHoldsAllMaintenance(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	contents := func() [][]byte {
		it := db.NewIterator(nil, nil)
		defer it.Release()
		var out [][]byte
		for it.Next() {
			out = append(out, bytes.Clone(it.Key()), bytes.Clone(it.Value()))
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := contents()
	var calls atomic.Int64
	l := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 2}, SnapshotLifecycleConfig{
		ReadersOnly: true,
		Snapshot:    snapshots.Config{Dir: dir, Enabled: true, Interval: time.Nanosecond, HistoryWindow: 1},
		Pruner:      PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir},
		ChainFreezerBuild: func() (snapshots.ChainFreezerSnapshotPassResult, error) {
			calls.Add(1)
			return snapshots.ChainFreezerSnapshotPassResult{}, nil
		},
		ChainLookupPrune:      func() (*snapshots.PruneHotChainLookupResult, error) { calls.Add(1); return nil, nil },
		SectionBloomPrune:     func() (*snapshots.PruneHotSectionBloomResult, error) { calls.Add(1); return nil, nil },
		BalanceTracePrune:     func() (*snapshots.PruneHotBalanceTraceResult, error) { calls.Add(1); return nil, nil },
		StateChangeIndexPrune: func(context.Context) (*rawdb.StateChangePostingPruneResult, error) { calls.Add(1); return nil, nil },
		RetiredPrune: func(context.Context, snapshots.ActiveManifestVerifier) (*snapshots.PruneRetiredSegmentFilesResult, error) {
			calls.Add(1)
			return nil, nil
		},
	})
	l.AddPassCompleteHook(func() { calls.Add(1) })
	if err := l.Start(); err != nil {
		t.Fatal(err)
	}
	defer l.Stop()
	select {
	case <-l.done:
	default:
		t.Fatal("reader-only start launched a maintenance worker")
	}
	for i := 0; i < 20; i++ {
		l.RequestPass()
		result, err := l.OnePass()
		if err != nil || !result.PruneDeferred || result.Snapshot.Built {
			t.Fatalf("held pass: %+v %v", result, err)
		}
	}
	if calls.Load() != 0 || !reflect.DeepEqual(contents(), before) || !reflect.DeepEqual(l.PostingPruneBoundary(), PostingPruneBoundary{}) {
		t.Fatal("reader-only maintenance changed data, invoked a callback or granted pruning")
	}
	if files, err := os.ReadDir(dir); err != nil || len(files) != 0 {
		t.Fatal("reader-only maintenance created snapshot files", files, err)
	}
	if err := l.Stop(); err != nil {
		t.Fatal(err)
	}
	l.RequestPass()
}
