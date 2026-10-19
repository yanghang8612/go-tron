package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestPredecessorDirectHistoryRoutingAndSnapshot(t *testing.T) {
	hot, err := NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer hot.Close()
	history, err := NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	d := NewDirectHistoryStore(hot, history)
	owner := common.Address{0x41, 3}
	for i := byte(1); i <= 3; i++ {
		word := make([]byte, 32)
		word[31] = i
		if err := WriteStateKVLatest(d, owner, 0, kvdomains.SystemShielded, []byte{'p', i}, word); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := d.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if err := DeleteStateKVLatest(d, owner, 0, kvdomains.SystemShielded, []byte{'p', 3}); err != nil {
		t.Fatal(err)
	}
	for _, source := range []struct {
		db   pointread.KeyValueSnapshot
		want byte
	}{{snap, 3}} {
		key, v, ok, err := ReadStateKVLatestBefore(source.db, owner, 0, kvdomains.SystemShielded, []byte{'p'}, nil)
		if err != nil || !ok || key[1] != source.want || v[31] != source.want {
			t.Fatal(key, v, ok, err)
		}
	}
	key, _, ok, err := ReadStateKVLatestBefore(d, owner, 0, kvdomains.SystemShielded, []byte{'p'}, nil)
	if err != nil || !ok || !bytes.Equal(key, []byte{'p', 2}) {
		t.Fatal(key, ok, err)
	}
	// Ambiguous merged prefix must neither recurse nor deadlock.
	if _, _, ok, err := d.SeekPrefixBefore(nil, nil); err != nil || !ok {
		t.Fatal(ok, err)
	}
	hs, err := d.NewStateHistoryReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer hs.Close()
	if _, _, _, err := pointread.SeekBefore(hs, stateKVLatestPrefix, nil); err != ErrStateHistorySnapshotScope {
		t.Fatal("scope not enforced", err)
	}
}

func TestCompactRejectsResidualLegacyKV(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	if err := db.Put(append([]byte("state-kv-latest-v2-"), make([]byte, 40)...), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := InitializeStateCommitment(db); err == nil {
		t.Fatal("legacy-only database silently initialized")
	}
}

type nativePredecessorProbe struct {
	ethdb.KeyValueStore
	seeks int
}

func (p *nativePredecessorProbe) SeekPrefixBefore(prefix, upper []byte) ([]byte, []byte, bool, error) {
	p.seeks++
	return []byte("p/key"), []byte("value"), true, nil
}
func (p *nativePredecessorProbe) NewIterator([]byte, []byte) ethdb.Iterator {
	panic("native predecessor hidden behind wrapper")
}
func TestPredecessorWrappersPreserveNativeSeek(t *testing.T) {
	probe := &nativePredecessorProbe{KeyValueStore: NewMemoryDatabase()}
	defer probe.Close()
	for _, wrapper := range []ethdb.Iteratee{NewChainDB(probe, NoopAncient{}), NewChainDBReadView(probe, NoopAncient{}), WrapKeyValueStore(probe)} {
		k, v, ok, err := pointread.SeekBefore(wrapper, []byte("p/"), nil)
		if err != nil || !ok || string(k) != "p/key" || string(v) != "value" {
			t.Fatal(k, v, ok, err)
		}
	}
	if probe.seeks != 3 {
		t.Fatal("native seeks", probe.seeks)
	}
}
