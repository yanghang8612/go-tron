package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// A staging-enabled cold directory retains old immutable trios until the
// source's durable ContentID reverse refs have been rebound to the replacement
// manifest and synced. Active query leases alone cannot protect this across a
// crash. These process-local registrations deliberately fail closed for every
// physical retirement path until that rebind protocol completes.
type historyStagingRetentionBinding struct {
	manager     *rawdb.HistoryStagingManager
	mu          sync.Mutex // serializes publication/rebind/CanGC/unlink for this cold dir
	ready       bool
	publication historyStagingPublicationGate
}

// This admission gate protects only capture of a mutually consistent route,
// hot snapshot and cold manifest. A query releases admission immediately
// after capture; its existing immutable-file lease protects the longer read.
// Writers are favored so a steady query stream cannot starve a merge.
type historyStagingPublicationGate struct {
	mu             sync.Mutex
	wake           chan struct{}
	readers        uint64
	writer         bool
	waitingWriters uint64
	readWaitNanos  atomic.Uint64
	writeWaitNanos atomic.Uint64
}

func (g *historyStagingPublicationGate) signal() {
	if g.wake != nil {
		close(g.wake)
	}
	g.wake = make(chan struct{})
}

func (g *historyStagingPublicationGate) acquireRead(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("snapshots: missing publication context")
	}
	started := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g.mu.Lock()
		if !g.writer && g.waitingWriters == 0 {
			g.readers++
			g.readWaitNanos.Add(uint64(time.Since(started).Nanoseconds()))
			g.mu.Unlock()
			return sync.OnceFunc(func() {
				g.mu.Lock()
				g.readers--
				g.signal()
				g.mu.Unlock()
			}), nil
		}
		if g.wake == nil {
			g.wake = make(chan struct{})
		}
		wake := g.wake
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		}
	}
}

func (g *historyStagingPublicationGate) acquireWrite(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("snapshots: missing publication context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	started := time.Now()
	g.mu.Lock()
	g.waitingWriters++
	g.signal()
	for {
		if !g.writer && g.readers == 0 {
			g.waitingWriters--
			g.writer = true
			g.writeWaitNanos.Add(uint64(time.Since(started).Nanoseconds()))
			g.mu.Unlock()
			return sync.OnceFunc(func() {
				g.mu.Lock()
				g.writer = false
				g.signal()
				g.mu.Unlock()
			}), nil
		}
		wake := g.wake
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.waitingWriters--
			g.signal()
			g.mu.Unlock()
			return nil, ctx.Err()
		case <-wake:
			g.mu.Lock()
		}
	}
}

// AcquireHistoryStagingPublicationRead must precede chain/index/route locks.
// Release it immediately after the paired route/hot/cold snapshots are pinned.
func AcquireHistoryStagingPublicationRead(ctx context.Context, dir string) (func(), error) {
	binding, present := historyStagingRetentionFor(dir)
	if !present {
		return func() {}, nil
	}
	release, err := binding.publication.acquireRead(ctx)
	if err != nil {
		return nil, err
	}
	binding.mu.Lock()
	ready := binding.ready
	binding.mu.Unlock()
	if !ready {
		release()
		return nil, errors.New("snapshots: history staging cold publication reconciliation pending")
	}
	return release, nil
}

// AcquireHistoryStagingQuarantineRetirement fixes a reconciled cold manifest
// while the caller retires old reset-epoch references and target payload. The
// caller supplies the canonical cold-tail proof; this package cannot infer it
// from a manifest alone. Rawdb separately requires all old route views closed.
func AcquireHistoryStagingQuarantineRetirement(ctx context.Context, dir string, verifyTail func(context.Context, *Manifest) error) (func(), error) {
	if verifyTail == nil {
		return nil, errors.New("snapshots: missing quarantine cold-tail proof")
	}
	release, err := AcquireHistoryStagingPublicationRead(ctx, dir)
	if err != nil {
		return nil, err
	}
	manifest, err := LoadProductionManifest(dir)
	if err == nil {
		err = verifyTail(ctx, manifest)
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}

type HistoryStagingPublicationWaitStats struct{ ReadWaitNanos, WriteWaitNanos uint64 }

func HistoryStagingPublicationWait(dir string) HistoryStagingPublicationWaitStats {
	binding, present := historyStagingRetentionFor(dir)
	if !present {
		return HistoryStagingPublicationWaitStats{}
	}
	return HistoryStagingPublicationWaitStats{ReadWaitNanos: binding.publication.readWaitNanos.Load(), WriteWaitNanos: binding.publication.writeWaitNanos.Load()}
}

func (binding *historyStagingRetentionBinding) isReady() bool {
	binding.mu.Lock()
	defer binding.mu.Unlock()
	return binding.ready
}

func (binding *historyStagingRetentionBinding) resetColdIsolated(manifest *Manifest) (bool, error) {
	intent, present, err := binding.manager.ReadResetIntent()
	if err != nil {
		return false, err
	}
	if !present {
		return true, nil
	}
	if !intent.Complete {
		return false, nil
	}
	epoch, err := binding.manager.CurrentEpoch()
	if err != nil || epoch != intent.NewEpoch {
		return false, rawdb.ErrHistoryStagingConflict
	}
	return manifest != nil && manifest.HistoryStagingResetEpoch == epoch, nil
}

var historyStagingRetention sync.Map // absolute cold dir -> *historyStagingRetentionBinding

func BindHistoryStagingColdRetention(dir string, manager *rawdb.HistoryStagingManager) error {
	if dir == "" || manager == nil {
		return errors.New("snapshots: missing staging cold directory or manager")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	key := filepath.Clean(abs)
	candidate := &historyStagingRetentionBinding{manager: manager}
	actual, loaded := historyStagingRetention.LoadOrStore(key, candidate)
	binding := actual.(*historyStagingRetentionBinding)
	if loaded && binding.manager != manager {
		return errors.New("snapshots: cold directory bound to another staging manager")
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	if binding.ready {
		return nil
	}
	manifest, err := LoadProductionManifest(dir)
	if os.IsNotExist(err) {
		binding.ready, err = binding.resetColdIsolated(nil)
		return err
	}
	if err != nil {
		return err
	}
	if err := reconcileHistoryStagingColdDependencies(context.Background(), dir, manifest, manager); err != nil {
		return err
	}
	binding.ready, err = binding.resetColdIsolated(manifest)
	if err != nil {
		return err
	}
	return nil
}

func historyStagingRetentionFor(dir string) (*historyStagingRetentionBinding, bool) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, false
	}
	value, present := historyStagingRetention.Load(filepath.Clean(abs))
	if !present {
		return nil, false
	}
	return value.(*historyStagingRetentionBinding), true
}

func historyStagingColdRetentionActive(dir string) bool {
	_, active := historyStagingRetentionFor(dir)
	return active
}
