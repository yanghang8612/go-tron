package blockbuffer

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type historySnapshotCountedStore struct {
	ethdb.KeyValueStore
	calls int
}

type historySnapshotSeekingView struct {
	pointread.KeyValueSnapshot
	seeks, iterators int
}

func (*historySnapshotSeekingView) IsPinnedKeyValueView() bool { return true }
func (s *historySnapshotSeekingView) SeekPrefix(prefix, start []byte) ([]byte, []byte, bool, error) {
	s.seeks++
	return s.KeyValueSnapshot.(pointread.PrefixSeeker).SeekPrefix(prefix, start)
}
func (s *historySnapshotSeekingView) NewIterator(prefix, start []byte) ethdb.Iterator {
	s.iterators++
	return s.KeyValueSnapshot.NewIterator(prefix, start)
}

func TestStateHistoryReadSnapshotDurableSeekPreservesBaseSeeker(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 3, 4); err != nil {
		t.Fatal(err)
	}
	pinned, err := db.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	view := &historySnapshotSeekingView{KeyValueSnapshot: pinned}
	snapshot := &ReadSnapshot{view: &bufferReadView{}, base: &stateHistoryReadBase{StateHistoryReadView: view, release: view.Close}, historyOnly: true}
	defer snapshot.Close()
	var prefix []byte
	for _, r := range rawdb.DirectHistoryRanges() {
		if r.Name == "state_tx_range" {
			prefix = r.Start
		}
	}
	if _, _, ok, err := snapshot.SeekDurablePrefix(prefix, nil); err != nil || !ok {
		t.Fatalf("durable seek present=%v err=%v", ok, err)
	}
	if view.seeks != 1 || view.iterators != 0 {
		t.Fatalf("native seeker lost: seeks=%d iterators=%d", view.seeks, view.iterators)
	}
}

func (s *historySnapshotCountedStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	s.calls++
	return s.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
}

func TestStateHistoryReadSnapshotThroughScopeAndFlush(t *testing.T) {
	dir := t.TempDir()
	open := func(name string) *historySnapshotCountedStore {
		db, err := rawdb.NewPebbleDB(filepath.Join(dir, name), 16, 16)
		if err != nil {
			t.Fatal(err)
		}
		return &historySnapshotCountedStore{KeyValueStore: db}
	}
	hot, history := open("hot"), open("history")
	db := rawdb.NewDirectHistoryStore(hot, history)
	t.Cleanup(func() { _ = db.Close() })
	b := New(db)
	write := func(block, begin uint64) {
		if err := rawdb.WriteStateTxRange(b, block, common.Hash{byte(block + 1)}, begin, begin+1); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateTxRange(db, 0, common.Hash{1}, 1, 2); err != nil {
		t.Fatal(err)
	}
	b.BeginBlock(bufHash(1), 1)
	write(1, 3)
	if err := b.Put([]byte("hot-overlay"), []byte("must not escape")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
	write(2, 5)
	b.CommitBlock()
	b.BeginBlock(bufHash(3), 3)
	write(3, 7)
	snapshot, err := b.NewStateHistoryReadSnapshotThrough(1)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if hot.calls != 0 || history.calls != 1 {
		t.Fatalf("snapshot factories hot=%d history=%d", hot.calls, history.calls)
	}
	b.CommitBlock()
	if err := b.FlushUpTo(3, db); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 0, common.Hash{1}, 99, 100); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	for block, begin := range map[uint64]uint64{0: 1, 1: 3} {
		row, ok, err := rawdb.ReadStateTxRange(snapshot, block)
		if err != nil || !ok || row.BeginTxNum != begin {
			t.Fatalf("block %d old row=%+v present=%v err=%v", block, row, ok, err)
		}
	}
	for _, block := range []uint64{2, 3} {
		if row, ok, err := rawdb.ReadStateTxRange(snapshot, block); err != nil || ok {
			t.Fatalf("included excluded block %d: %+v/%v/%v", block, row, ok, err)
		}
	}
	for _, key := range [][]byte{nil, []byte("hot-overlay"), rawdb.StateAccountLatestCommitmentKey(common.Address{1})} {
		if _, _, err := snapshot.GetWithPresence(key); !errors.Is(err, rawdb.ErrStateHistorySnapshotScope) {
			t.Fatalf("out-of-scope overlay read accepted: %q/%v", key, err)
		}
		if _, err := snapshot.Get(key); !errors.Is(err, rawdb.ErrStateHistorySnapshotScope) {
			t.Fatal(err)
		}
		if _, err := snapshot.Has(key); !errors.Is(err, rawdb.ErrStateHistorySnapshotScope) {
			t.Fatal(err)
		}
	}
	for _, prefix := range [][]byte{nil, []byte("state-"), []byte("hot-overlay"), []byte("stage-progress-v1-StateHistoryIndex")} {
		it := snapshot.NewIterator(prefix, nil)
		if it.Next() || !errors.Is(it.Error(), rawdb.ErrStateHistorySnapshotScope) {
			t.Fatalf("out-of-scope iterator accepted %q: %v", prefix, it.Error())
		}
		it.Release()
		if _, _, _, err := snapshot.SeekPrefix(prefix, nil); !errors.Is(err, rawdb.ErrStateHistorySnapshotScope) {
			t.Fatal(err)
		}
		if _, _, _, err := snapshot.SeekDurablePrefix(prefix, nil); !errors.Is(err, rawdb.ErrStateHistorySnapshotScope) {
			t.Fatal(err)
		}
	}
	latest := snapshot.NewStateKVLatestIterator(nil, common.AccountID{}, nil)
	if latest.Next() || !errors.Is(latest.Error(), rawdb.ErrStateHistorySnapshotScope) {
		t.Fatalf("out-of-scope latest iterator accepted: %v", latest.Error())
	}
	latest.Release()
	var prefix []byte
	for _, r := range rawdb.DirectHistoryRanges() {
		if r.Name == "state_tx_range" {
			prefix = r.Start
		}
	}
	if prefix == nil {
		t.Fatal("missing tx-range family")
	}
	it := snapshot.NewIterator(prefix, nil)
	count := 0
	for it.Next() {
		count++
	}
	if it.Error() != nil || count != 2 {
		t.Fatalf("history iterator rows=%d err=%v", count, it.Error())
	}
	it.Release()
	if _, _, ok, err := snapshot.SeekPrefix(prefix, nil); err != nil || !ok {
		t.Fatalf("history prefix seek present=%v err=%v", ok, err)
	}
	if _, _, ok, err := snapshot.SeekDurablePrefix(prefix, nil); err != nil || !ok {
		t.Fatalf("history durable seek present=%v err=%v", ok, err)
	}
	general, err := b.NewReadSnapshotThrough(1)
	if err != nil {
		t.Fatal(err)
	}
	defer general.Close()
	if value, err := general.Get([]byte("hot-overlay")); err != nil || string(value) != "must not escape" {
		t.Fatalf("general read changed: %q/%v", value, err)
	}
	if hot.calls != 1 || history.calls != 2 {
		t.Fatal("general snapshot stopped pinning both engines")
	}
}

func TestStateHistoryReadSnapshotThroughUnsupportedDoesNotMove(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	defer base.Close()
	for name, source := range map[string]ethdb.KeyValueStore{
		"memory":  base,
		"wrapped": rawdb.WrapKeyValueStore(base),
		"chain":   rawdb.NewChainDB(base, rawdb.NoopAncient{}),
	} {
		t.Run(name, func(t *testing.T) {
			snapshot, err := New(source).NewStateHistoryReadSnapshotThrough(1)
			if snapshot != nil || !errors.Is(err, ErrReadSnapshotUnsupported) {
				t.Fatalf("unpinned base snapshot=%v err=%v", snapshot, err)
			}
		})
	}
}

func TestStateHistoryReadSnapshotThroughWaitsForFlushHandoff(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failed"}[fail], func(t *testing.T) {
			db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			b := New(db)
			b.BeginBlock(bufHash(1), 1)
			if err := rawdb.WriteStateTxRange(b, 1, common.Hash{1}, 3, 4); err != nil {
				t.Fatal(err)
			}
			b.CommitBlock()
			var writeErr error
			if fail {
				writeErr = errors.New("flush failed")
			}
			gate := newGatedPebbleFlush(db, writeErr)
			defer gate.unblock()
			flushed := make(chan error, 1)
			go func() { flushed <- b.FlushUpTo(1, gate) }()
			<-gate.entered
			snapshots := make(chan *ReadSnapshot, 1)
			failed := make(chan error, 1)
			go func() {
				snapshot, err := b.NewStateHistoryReadSnapshotThrough(1)
				if err != nil {
					failed <- err
					return
				}
				snapshots <- snapshot
			}()
			select {
			case snapshot := <-snapshots:
				snapshot.Close()
				t.Fatal("captured a partially completed flush handoff")
			case err := <-failed:
				t.Fatal(err)
			case <-time.After(20 * time.Millisecond):
			}
			gate.unblock()
			if err := <-flushed; !errors.Is(err, writeErr) {
				t.Fatalf("flush returned %v want %v", err, writeErr)
			}
			select {
			case snapshot := <-snapshots:
				defer snapshot.Close()
				row, ok, err := rawdb.ReadStateTxRange(snapshot, 1)
				if err != nil || !ok || row.BeginTxNum != 3 || row.EndTxNum != 4 {
					t.Fatalf("lost history across handoff %+v/%v/%v", row, ok, err)
				}
			case err := <-failed:
				t.Fatal(err)
			case <-time.After(time.Second):
				t.Fatal("snapshot stayed blocked after flush handoff")
			}
		})
	}
}
