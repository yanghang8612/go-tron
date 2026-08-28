package pruning

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type lifecycleCompactionStopHandler struct {
	slog.Handler
	onCopy func()
}

func (h lifecycleCompactionStopHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "History cold snapshot compaction phase started" {
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "phase" && attr.Value.String() == "copy-records" {
				h.onCopy()
			}
			return true
		})
	}
	return h.Handler.Handle(ctx, record)
}

func (h lifecycleCompactionStopHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.Handler = h.Handler.WithAttrs(attrs)
	return h
}

func (h lifecycleCompactionStopHandler) WithGroup(name string) slog.Handler {
	h.Handler = h.Handler.WithGroup(name)
	return h
}

func TestSnapshotLifecycleStopCancelsHistoryCompaction(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	var refs []snapshots.SegmentRef
	for block := uint64(1); block <= 2; block++ {
		from, to := 2*block-1, 2*block
		writeSnapPruningChange(t, db, block, from, to)
		built, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, from, to, fmt.Sprintf("history/state-domain-change-%d-%d.seg", from, to))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 4, refs)); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, snapshots.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 3}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{Dir: dir, Enabled: true, Interval: time.Hour, HistoryWindow: 1},
		Pruner:   PrunerConfig{Policy: SnapPolicy(1, 1), Interval: time.Hour, SnapshotDir: dir},
	})
	entered := make(chan struct{})
	var once sync.Once
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(lifecycleCompactionStopHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		onCopy: func() {
			once.Do(func() { close(entered) })
			<-lifecycle.ctx.Done()
		},
	}))
	if err := lifecycle.Start(); err != nil {
		t.Fatal(err)
	}
	defer lifecycle.Stop()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle did not enter history compaction")
	}
	stopped := make(chan error, 1)
	started := time.Now()
	go func() { stopped <- lifecycle.Stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel and join compaction within one second")
	}
	t.Logf("Stop canceled and joined history compaction in %s", time.Since(started))
	after, err := os.ReadFile(filepath.Join(dir, snapshots.ManifestFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("shutdown published compaction/prune progress: %v", err)
	}
	for _, ref := range refs {
		if _, err := os.Stat(filepath.Join(dir, ref.Path)); err != nil {
			t.Fatalf("shutdown removed old snapshot %s: %v", ref.Path, err)
		}
	}
	for _, block := range []uint64{1, 2} {
		if _, ok, err := rawdb.ReadStateDomainChange(db, block, 1); err != nil || !ok {
			t.Fatalf("shutdown pruned hot row block=%d: ok=%v err=%v", block, ok, err)
		}
	}
}
