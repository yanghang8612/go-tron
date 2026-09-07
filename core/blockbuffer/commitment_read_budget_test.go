package blockbuffer

import (
	"errors"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestCommitmentReadBudgetOnlyBlocksDurableLeaders(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, key := range []string{"p/cached", "p/durable"} {
		if err := db.Put([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	b := New(db)
	b.SetBaseReadCacheSize(1 << 20)
	if _, err := b.Prefetch([]byte("p/cached")); err != nil {
		t.Fatal(err)
	}
	b.BeginBlock(bufHash(1), 1)
	if err := b.Put([]byte("p/overlay"), []byte("overlay")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
	h, _ := b.NewestInflight()
	s, err := b.ViewLayer(h).NewCommitmentParentReadSession(3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	permits := make(chan struct{}, 1)
	s.(pointread.CommitmentParentReadBudget).SetCommitmentParentReadBudget(permits)
	permits <- struct{}{} // another session owns the only durable read permit
	done := make(chan error, 1)
	go func() {
		found, err := s.ViewKeyParts(0, []byte("p/"), []byte("durable"), func([]byte, bool) error { return nil })
		if err == nil && !found {
			err = errors.New("durable row missing")
		}
		done <- err
	}()
	bypass := make(chan error, 1)
	go func() {
		for _, key := range []string{"cached", "overlay"} {
			found, err := s.ViewKeyParts(1, []byte("p/"), []byte(key), func([]byte, bool) error { return nil })
			if err != nil || !found {
				bypass <- errors.New("cache/overlay read failed")
				return
			}
			found, err = s.(pointread.CommitmentParentPrefetchSession).PrefetchKeyParts(2, []byte("p/"), []byte(key))
			if err != nil || !found {
				bypass <- errors.New("cache/overlay prefetch failed")
				return
			}
		}
		bypass <- nil
	}()
	select {
	case err := <-bypass:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("budget blocked an overlay/cache hit")
	}
	select {
	case err := <-done:
		t.Fatalf("durable read bypassed occupied budget: %v", err)
	default:
	}
	<-permits
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("permit release did not unblock durable read")
	}
	if len(permits) != 0 {
		t.Fatal("durable read leaked a permit")
	}
}

type failingBudgetCursor struct{ panicRead bool }

func (c failingBudgetCursor) View([]byte, func([]byte) error) (bool, error) {
	if c.panicRead {
		panic("injected read panic")
	}
	return false, errors.New("injected read failure")
}
func (failingBudgetCursor) Close() error { return nil }

func TestCommitmentReadBudgetReleasesOnFailureAndPanic(t *testing.T) {
	for _, panicRead := range []bool{false, true} {
		s := &commitmentParentReadSession{durableReadPermits: make(chan struct{}, 1)}
		func() {
			defer func() {
				if got := recover(); (got != nil) != panicRead {
					t.Errorf("panic=%v want=%v", got, panicRead)
				}
			}()
			if _, err := s.readDurable(failingBudgetCursor{panicRead: panicRead}, nil, nil); err == nil {
				t.Error("lost durable error")
			}
		}()
		if len(s.durableReadPermits) != 0 {
			t.Fatal("failure leaked a permit")
		}
	}
}
