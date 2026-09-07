package domains

import (
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type partitionFailureLayer struct {
	*blockbuffer.LayerView
	entered, release chan struct{}
	once             sync.Once
	err              error
}

func (l *partitionFailureLayer) NewCommitmentParentReadSession(readers int) (pointread.CommitmentParentSession, error) {
	s, err := l.LayerView.NewCommitmentParentReadSession(readers)
	if err != nil {
		return nil, err
	}
	return &partitionFailureSession{CommitmentParentSession: s, layer: l}, nil
}

type partitionFailureSession struct {
	pointread.CommitmentParentSession
	layer *partitionFailureLayer
}

func (s *partitionFailureSession) ViewKeyParts(reader int, first, second []byte, fn func([]byte, bool) error) (bool, error) {
	s.layer.once.Do(func() { close(s.layer.entered) })
	<-s.layer.release
	return false, s.layer.err
}

// An empty block queued while its predecessor is in a failed durable read
// must receive that failure, never publish a zero/success root or hang waiting
// on the publisher's separate result channel. All partition owners must join.
func TestPartitionedCommitmentFailureReleasesEmptySuccessor(t *testing.T) {
	t.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	seed := buildRandomPuts(rand.New(rand.NewSource(9897)), 4096)
	if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), seed); err != nil {
		t.Fatal(err)
	}
	buf := blockbuffer.New(db)
	buf.SetMaxInflight(4)
	p, err := NewOrderedCommitmentPipeline(buf)
	if err != nil {
		t.Fatal(err)
	}
	buf.BeginBlock(common.Hash{1}, 1)
	h, _ := buf.NewestInflight()
	injected := errors.New("injected partition parent read failure")
	layer := &partitionFailureLayer{LayerView: buf.ViewLayer(h), entered: make(chan struct{}), release: make(chan struct{}), err: injected}
	first := p.Submit(layer, []rawdb.StateCommitmentUpdate{rawdb.NewStateCommitmentPut(seed[0].Key, []byte("changed"))})
	select {
	case <-layer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("parent read never started")
	}
	buf.BeginBlock(common.Hash{2}, 2)
	h, _ = buf.NewestInflight()
	second := p.Submit(buf.ViewLayer(h), nil)
	close(layer.release)
	for _, result := range []<-chan OrderedCommitmentResult{first, second} {
		select {
		case got := <-result:
			if !errors.Is(got.Err, injected) {
				t.Fatalf("completion error %v, want injected failure", got.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("failure left successor blocked")
		}
	}
	p.Close()
	if got := p.inflight.Load(); got != 0 {
		t.Fatalf("retained %d inflight jobs", got)
	}
}
