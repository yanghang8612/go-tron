package domains

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type blockedPrefetchLayer struct {
	*blockbuffer.LayerView
	started, release, foreground chan struct{}
	closed                       atomic.Bool
}

func (l *blockedPrefetchLayer) NewCommitmentParentReadSession(readers int) (pointread.CommitmentParentSession, error) {
	s, err := l.LayerView.NewCommitmentParentReadSession(readers)
	if err != nil {
		return nil, err
	}
	return &blockedPrefetchSession{CommitmentParentSession: s, layer: l}, nil
}

type blockedPrefetchSession struct {
	pointread.CommitmentParentSession
	layer      *blockedPrefetchLayer
	started    sync.Once
	foreground sync.Once
}

func (s *blockedPrefetchSession) PrefetchKeyParts(reader int, first, second []byte) (bool, error) {
	s.started.Do(func() { close(s.layer.started) })
	<-s.layer.release
	return s.CommitmentParentSession.(pointread.CommitmentParentPrefetchSession).PrefetchKeyParts(reader, first, second)
}

func (s *blockedPrefetchSession) ViewKeyParts(reader int, first, second []byte, fn func([]byte, bool) error) (bool, error) {
	<-s.layer.started
	s.foreground.Do(func() { close(s.layer.foreground) })
	return s.CommitmentParentSession.ViewKeyParts(reader, first, second, fn)
}

func (s *blockedPrefetchSession) Close() error {
	s.layer.closed.Store(true)
	return s.CommitmentParentSession.Close()
}

// A blocked speculative read must neither stop an unrelated authoritative
// read nor allow publication/session close before the speculative owner exits.
// The real Pebble-backed parent session supplies cache/version/singleflight
// semantics; only the scheduling interleaving is controlled by this wrapper.
func TestOrderedCommitmentPrefetchOverlapPreservesSessionLifetime(t *testing.T) {
	t.Setenv("GTRON_COMMITMENT_PREFETCH_OVERLAP", "1")
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	reference := rawdb.NewMemoryDatabase()
	seed := buildRandomPuts(rand.New(rand.NewSource(12091)), 512)
	for _, store := range []CommitmentDB{db, reference} {
		if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(store), seed); err != nil {
			t.Fatal(err)
		}
	}
	buf := blockbuffer.New(db)
	pipeline, err := NewOrderedCommitmentPipeline(buf)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()
	buf.BeginBlock(common.Hash{1}, 1)
	handle, _ := buf.NewestInflight()
	layer := &blockedPrefetchLayer{
		LayerView: buf.ViewLayer(handle), started: make(chan struct{}),
		release: make(chan struct{}), foreground: make(chan struct{}),
	}
	var release sync.Once
	defer release.Do(func() { close(layer.release) })
	updates := []rawdb.StateCommitmentUpdate{rawdb.NewStateCommitmentPut(seed[0].Key, []byte("updated"))}
	want, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(reference), updates)
	if err != nil {
		t.Fatal(err)
	}
	result := pipeline.Submit(layer, updates)
	select {
	case <-layer.foreground:
	case <-time.After(5 * time.Second):
		t.Fatal("authoritative read waited for unrelated prefetch completion")
	}
	if layer.closed.Load() {
		t.Fatal("session closed with prefetch still running")
	}
	select {
	case <-result:
		t.Fatal("job published with prefetch still running")
	default:
	}
	release.Do(func() { close(layer.release) })
	select {
	case got := <-result:
		if got.Err != nil || got.Root != want {
			t.Fatalf("root=%x err=%v want=%x", got.Root, got.Err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job did not finish after prefetch release")
	}
	if !layer.closed.Load() {
		t.Fatal("completed job retained session")
	}
}

func TestOrderedCommitmentPrefetchOverlapPebbleInflightParity(t *testing.T) {
	for _, overlap := range []string{"0", "1"} {
		t.Run(overlap, func(t *testing.T) {
			t.Setenv("GTRON_COMMITMENT_PREFETCH_OVERLAP", overlap)
			db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 32)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			reference := rawdb.NewMemoryDatabase()
			seed := buildRandomPuts(rand.New(rand.NewSource(23019)), 2048)
			for _, store := range []CommitmentDB{db, reference} {
				if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(store), seed); err != nil {
					t.Fatal(err)
				}
			}
			buf := blockbuffer.New(db)
			buf.SetMaxInflight(4)
			buf.SetBaseReadCacheSizeWithTrunk(1<<20, 1, rawdb.CommitmentBranchKeyPrefix)
			oldDepth := CommitmentParentPrefetchDepth
			CommitmentParentPrefetchDepth = 2
			defer func() { CommitmentParentPrefetchDepth = oldDepth }()
			pipeline, err := NewOrderedCommitmentPipeline(buf)
			if err != nil {
				t.Fatal(err)
			}
			defer pipeline.Close()
			for batch := 0; batch < 6; batch++ {
				var handles [4]blockbuffer.InflightHandle
				var results [4]<-chan OrderedCommitmentResult
				var roots [4]common.Hash
				for j := 0; j < 4; j++ {
					block := batch*4 + j + 1
					updates := make([]rawdb.StateCommitmentUpdate, 0, 128)
					for i := 0; i < 128; i++ {
						key := seed[(i*13+block*7)%len(seed)].Key
						if (i+block)%5 == 0 {
							updates = append(updates, rawdb.NewStateCommitmentDelete(key))
						} else {
							updates = append(updates, rawdb.NewStateCommitmentPut(key, []byte{byte(block), byte(i)}))
						}
					}
					roots[j], err = ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(reference), updates)
					if err != nil {
						t.Fatal(err)
					}
					buf.BeginBlock(common.Hash{byte(block)}, uint64(block))
					handles[j], _ = buf.NewestInflight()
					results[j] = pipeline.Submit(buf.ViewLayer(handles[j]), updates)
				}
				for j := range results {
					got := <-results[j]
					if got.Err != nil || got.Root != roots[j] {
						t.Fatalf("block %d root=%x err=%v want=%x", batch*4+j+1, got.Root, got.Err, roots[j])
					}
					if err := buf.CommitInflight(handles[j]); err != nil {
						t.Fatal(err)
					}
				}
				want, got := collectCommitmentRows(t, reference), collectCommitmentRows(t, buf)
				if len(want) != len(got) {
					t.Fatalf("branch count %d want %d", len(got), len(want))
				}
				for key, value := range want {
					if !bytes.Equal(got[key], value) {
						t.Fatalf("branch %x differs", key)
					}
				}
				// Force the next batch through a fresh durable cut, not only retained overlays.
				if err := buf.Flush(db); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
