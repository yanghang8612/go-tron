package blockbuffer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// TestBuffer_ConcurrentPipelineRaceFree drives the buffer the way the async
// commit pipeline does — foreground writes the newest in-flight layer while a
// worker writes an OLDER in-flight layer, a flush worker drains committed layers
// to base, and many readers resolve keys concurrently — and asserts that every
// key, once its block is committed, stays resolvable with the correct value for
// the rest of the run (it must live in either a layer or base, never neither),
// while -race watches for unsynchronized map/slice access.
//
// On the single-lock buffer this passes (every method is mutually exclusive
// under b.mu). It is the regression net for the sharded-layer-lock change, which
// must keep it green under -race.
func TestBuffer_ConcurrentPipelineRaceFree(t *testing.T) {
	const (
		blocks       = 200
		keysPerBlock = 16
		maxInflight  = 4
		readers      = 6
	)
	keyFor := func(n uint64, i int) []byte { return []byte(fmt.Sprintf("k-%03d-%02d", n, i)) }
	valFor := func(n uint64, i int) []byte { return []byte(fmt.Sprintf("v-%03d-%02d", n, i)) }

	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.SetMaxInflight(maxInflight)

	var chainmu sync.Mutex // serializes foreground structural ops, as core.chainmu does
	var committedUpTo atomic.Uint64
	sem := make(chan struct{}, maxInflight) // backpressure: bound in-flight layers
	handoff := make(chan InflightHandle, maxInflight)
	errCh := make(chan error, 8)
	fail := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	stop := make(chan struct{})
	var readWG sync.WaitGroup
	for r := 0; r < readers; r++ {
		readWG.Add(1)
		go func() {
			defer readWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				hi := committedUpTo.Load()
				if hi == 0 {
					continue
				}
				n := (hi+7)%hi + 1 // some committed block in [1,hi]
				for i := 0; i < keysPerBlock; i++ {
					want := valFor(n, i)
					got, err := b.Get(keyFor(n, i))
					if err != nil || string(got) != string(want) {
						fail(fmt.Errorf("Get(k %d/%d) = (%q,%v), want %q (committedUpTo=%d)", n, i, got, err, want, hi))
						return
					}
					if ok, err := b.Has(keyFor(n, i)); err != nil || !ok {
						fail(fmt.Errorf("Has(k %d/%d) = (%v,%v), want true", n, i, ok, err))
						return
					}
					nc, err := b.GetNoCopy(keyFor(n, i))
					if err != nil || string(nc) != string(want) {
						fail(fmt.Errorf("GetNoCopy(k %d/%d) = (%q,%v), want %q", n, i, nc, err, want))
						return
					}
				}
			}
		}()
	}

	// Flush worker: continuously drain committed layers to base.
	var flushWG sync.WaitGroup
	flushWG.Add(1)
	go func() {
		defer flushWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if hi := committedUpTo.Load(); hi > 1 {
				if err := b.FlushUpTo(hi-1, base); err != nil {
					fail(fmt.Errorf("FlushUpTo(%d): %w", hi-1, err))
					return
				}
			}
		}
	}()

	// Worker: write the OLDER in-flight layer (via its handle) then commit FIFO.
	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		for h := range handoff {
			view := b.ViewLayer(h)
			n := h.Number()
			for i := keysPerBlock / 2; i < keysPerBlock; i++ {
				if err := view.Put(keyFor(n, i), valFor(n, i)); err != nil {
					fail(fmt.Errorf("worker Put: %w", err))
					return
				}
			}
			if err := b.CommitInflight(h); err != nil {
				fail(fmt.Errorf("CommitInflight(%d): %w", n, err))
				return
			}
			committedUpTo.Store(n)
			<-sem
		}
	}()

	// Foreground: begin each block + write the newer half of its keys to the
	// active layer, hand the layer to the worker. Backpressure via sem.
	for n := uint64(1); n <= blocks; n++ {
		sem <- struct{}{}
		chainmu.Lock()
		b.BeginBlock(bufHash(byte(n)), n)
		for i := 0; i < keysPerBlock/2; i++ {
			if err := b.Put(keyFor(n, i), valFor(n, i)); err != nil {
				chainmu.Unlock()
				t.Fatalf("foreground Put: %v", err)
			}
		}
		h, ok := b.NewestInflight()
		chainmu.Unlock()
		if !ok {
			t.Fatalf("NewestInflight not ok at block %d", n)
		}
		handoff <- h
		select {
		case err := <-errCh:
			t.Fatalf("concurrent failure: %v", err)
		default:
		}
	}
	close(handoff)
	workerWG.Wait()
	close(stop)
	readWG.Wait()
	flushWG.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("concurrent failure: %v", err)
	default:
	}

	// Final: every key of every block resolves (in base after full drain, or a layer).
	if err := b.FlushUpTo(blocks, base); err != nil {
		t.Fatalf("final FlushUpTo: %v", err)
	}
	for n := uint64(1); n <= blocks; n++ {
		for i := 0; i < keysPerBlock; i++ {
			got, err := b.Get(keyFor(n, i))
			if err != nil || string(got) != string(valFor(n, i)) {
				t.Fatalf("final Get(k %d/%d) = (%q,%v), want %q", n, i, got, err, valFor(n, i))
			}
		}
	}
}
