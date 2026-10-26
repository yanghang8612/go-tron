package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// predecessorMaterialized is deliberately independent of all blockbuffer
// iterators, overlay walks and candidate helpers. Apply the public operation
// history directly to a complete map, then find the answer by exhaustive scan.
type predecessorMaterialized map[string][]byte

func (m predecessorMaterialized) put(k, v []byte) { m[string(k)] = bytes.Clone(v) }
func (m predecessorMaterialized) del(k []byte)    { delete(m, string(k)) }
func (m predecessorMaterialized) delRange(start, end []byte) {
	for k := range m {
		if bytes.Compare([]byte(k), start) >= 0 && bytes.Compare([]byte(k), end) < 0 {
			delete(m, k)
		}
	}
}
func (m predecessorMaterialized) clone() predecessorMaterialized {
	n := make(predecessorMaterialized, len(m))
	for k, v := range m {
		n[k] = bytes.Clone(v)
	}
	return n
}
func (m predecessorMaterialized) before(prefix, upper []byte) ([]byte, []byte, bool) {
	var best string
	var value []byte
	found := false
	for k, v := range m {
		if bytes.HasPrefix([]byte(k), prefix) && (upper == nil || bytes.Compare([]byte(k), upper) < 0) && (!found || k > best) {
			best, value, found = k, v, true
		}
	}
	if !found {
		return nil, nil, false
	}
	return []byte(best), bytes.Clone(value), true
}

func predecessorTyped(logical []byte) []byte {
	return rawdb.StateKVLatestCommitmentKey(common.SystemAccountAddress, 0, kvdomains.SystemShielded, logical)
}

func assertPredecessorMaterialized(t *testing.T, source pointread.PrefixPredecessor, want predecessorMaterialized, prefixes [][]byte) {
	t.Helper()
	bounds := [][]byte{nil, {}, {0}, {0xff}, {0xff, 0xff}, predecessorTyped(nil), predecessorTyped([]byte("root/")), predecessorTyped([]byte("root/05")), predecessorTyped([]byte("root/99"))}
	for k := range want {
		bounds = append(bounds, []byte(k))
	}
	for pi, prefix := range prefixes {
		for bi, upper := range bounds {
			wk, wv, wok := want.before(prefix, upper)
			k, v, ok, err := source.SeekPrefixBefore(prefix, upper)
			if err != nil || ok != wok || !bytes.Equal(k, wk) || !bytes.Equal(v, wv) {
				t.Fatalf("source=%T prefix[%d]=%x bound[%d]=%x (nil=%t): got %x/%x/%t/%v want %x/%x/%t", source, pi, prefix, bi, upper, upper == nil, k, v, ok, err, wk, wv, wok)
			}
			if !ok {
				continue
			}
			for i := range k {
				k[i] ^= 0xff
			}
			for i := range v {
				v[i] ^= 0xff
			}
			k, v, ok, err = source.SeekPrefixBefore(prefix, upper)
			if err != nil || !ok || !bytes.Equal(k, wk) || !bytes.Equal(v, wv) {
				t.Fatalf("source=%T returned borrowed bytes", source)
			}
		}
	}
}

func TestPredecessorCandidatesMaterializedLifecycle(t *testing.T) {
	base, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	m := make(predecessorMaterialized)
	keys := [][]byte{nil, {0xff}, {0xff, 0xff}, []byte("generic"), predecessorTyped(nil), predecessorTyped([]byte{0xff}), predecessorTyped([]byte{0xff, 0xff})}
	for i := 0; i < 30; i++ {
		keys = append(keys, predecessorTyped([]byte(fmt.Sprintf("root/%02d", i))))
	}
	for i, k := range keys {
		v := []byte{byte(i), 1}
		if err := base.Put(k, v); err != nil {
			t.Fatal(err)
		}
		m.put(k, v)
	}
	prefixes := [][]byte{nil, {}, {0xff}, {0xff, 0xff}, []byte("generic"), predecessorTyped(nil), predecessorTyped([]byte("root/")), predecessorTyped([]byte{0xff}), predecessorTyped([]byte{0xff, 0xff}), predecessorTyped([]byte("missing"))}
	b := New(base)
	b.SetMaxInflight(4)
	b.BeginBlock(bufHash(1), 1)
	for _, k := range keys[len(keys)-8:] {
		b.Delete(k)
		m.del(k)
	}
	b.Put(predecessorTyped([]byte("root/03")), []byte("first"))
	m.put(predecessorTyped([]byte("root/03")), []byte("first"))
	b.CommitBlock()
	pinned, err := b.NewReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	pinnedWant := m.clone()
	assertPredecessorMaterialized(t, b, m, prefixes)
	assertPredecessorMaterialized(t, pinned, pinnedWant, prefixes)

	b.BeginBlock(bufHash(2), 2)
	h, _ := b.NewestInflight()
	view := b.ViewLayer(h)
	start, end := predecessorTyped([]byte("root/02")), predecessorTyped([]byte("root/20"))
	if err := b.DeleteRange(start, end); err != nil {
		t.Fatal(err)
	}
	m.delRange(start, end)
	b.Put(predecessorTyped([]byte("root/17")), []byte("revived"))
	m.put(predecessorTyped([]byte("root/17")), []byte("revived"))
	// Exercise writes/deletes appended after lazy bucket construction, then
	// range removal of points already represented in that bucket.
	assertPredecessorMaterialized(t, view, m, prefixes)
	b.Put(predecessorTyped([]byte("root/18")), []byte("later"))
	m.put(predecessorTyped([]byte("root/18")), []byte("later"))
	b.Delete(predecessorTyped([]byte("root/18")))
	m.del(predecessorTyped([]byte("root/18")))
	if err := b.DeleteRange(predecessorTyped([]byte("root/16")), predecessorTyped([]byte("root/19"))); err != nil {
		t.Fatal(err)
	}
	m.delRange(predecessorTyped([]byte("root/16")), predecessorTyped([]byte("root/19")))
	b.Put(predecessorTyped([]byte("root/16")), []byte("after-range"))
	m.put(predecessorTyped([]byte("root/16")), []byte("after-range"))
	viewWant := m.clone()
	assertPredecessorMaterialized(t, b, m, prefixes)
	assertPredecessorMaterialized(t, view, viewWant, prefixes)
	b.BeginBlock(bufHash(3), 3)
	b.Put(predecessorTyped([]byte("root/99")), []byte("future"))
	m.put(predecessorTyped([]byte("root/99")), []byte("future"))
	assertPredecessorMaterialized(t, b, m, prefixes)
	assertPredecessorMaterialized(t, view, viewWant, prefixes)
	b.DiscardActive()
	m = viewWant.clone()
	b.CommitBlock()
	if err := b.FlushUpTo(2, base); err != nil {
		t.Fatal(err)
	}
	assertPredecessorMaterialized(t, b, m, prefixes)
	assertPredecessorMaterialized(t, pinned, pinnedWant, prefixes)
	fresh, err := b.NewReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertPredecessorMaterialized(t, fresh, m, prefixes)
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := fresh.SeekPrefixBefore(predecessorTyped(nil), nil); err == nil {
		t.Fatal("closed snapshot accepted predecessor")
	}
	var absent *ReadSnapshot
	if _, _, _, err := absent.SeekPrefixBefore(predecessorTyped(nil), nil); err == nil {
		t.Fatal("nil snapshot accepted predecessor")
	}
	scoped := &ReadSnapshot{historyOnly: true}
	if _, _, _, err := scoped.SeekPrefixBefore(predecessorTyped(nil), nil); !errors.Is(err, rawdb.ErrStateHistorySnapshotScope) {
		t.Fatalf("scope error: %v", err)
	}
}

func TestPredecessorCandidatesMaterializedRandom(t *testing.T) {
	for _, seed := range []int64{1, 938, 98172} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			base := rawdb.NewMemoryDatabase()
			defer base.Close()
			b := New(base)
			m := make(predecessorMaterialized)
			keys := make([][]byte, 40)
			for i := range keys {
				keys[i] = predecessorTyped([]byte{byte(i / 10), byte(i % 10)})
			}
			prefixes := [][]byte{predecessorTyped(nil), predecessorTyped([]byte{0}), predecessorTyped([]byte{1}), predecessorTyped([]byte{2}), predecessorTyped([]byte{3}), predecessorTyped([]byte{0xff}), {0xff}}
			rng := rand.New(rand.NewSource(seed))
			for n := 1; n <= 12; n++ {
				b.BeginBlock(bufHash(byte(n)), uint64(n))
				for j := 0; j < 18; j++ {
					k := keys[rng.Intn(len(keys))]
					switch rng.Intn(5) {
					case 0:
						b.Delete(k)
						m.del(k)
					case 1:
						end := bytes.Clone(k)
						end[len(end)-1] += 4
						if err := b.DeleteRange(k, end); err != nil {
							t.Fatal(err)
						}
						m.delRange(k, end)
					default:
						v := []byte{byte(n), byte(j)}
						b.Put(k, v)
						m.put(k, v)
					}
				}
				b.CommitBlock()
				assertPredecessorMaterialized(t, b, m, prefixes)
				// Deliberately build the independent generic index only after
				// checking the cold bucket path, then validate the reused-index leg.
				it := b.NewIterator(predecessorTyped(nil), nil)
				it.Release()
				assertPredecessorMaterialized(t, b, m, prefixes)
			}
		})
	}
}

type predecessorErrorBase struct {
	ethdb.KeyValueStore
	iterators, releases int
	before, after       error
}

func (b *predecessorErrorBase) NewIterator(_, _ []byte) ethdb.Iterator {
	b.iterators++
	return &predecessorErrorIterator{base: b}
}

type predecessorErrorIterator struct {
	base     *predecessorErrorBase
	released bool
}

func (*predecessorErrorIterator) Next() bool    { return false }
func (*predecessorErrorIterator) Key() []byte   { return nil }
func (*predecessorErrorIterator) Value() []byte { return nil }
func (i *predecessorErrorIterator) Error() error {
	if i.released {
		return i.base.after
	}
	return i.base.before
}
func (i *predecessorErrorIterator) Release() { i.released = true; i.base.releases++ }

func TestPredecessorCandidatesBaseErrorsRelease(t *testing.T) {
	before, after := errors.New("predecessor read failed"), errors.New("predecessor release failed")
	for _, stage := range []string{"read", "release", "both"} {
		t.Run(stage, func(t *testing.T) {
			base := &predecessorErrorBase{KeyValueStore: rawdb.NewMemoryDatabase()}
			defer base.Close()
			if stage != "release" {
				base.before = before
			}
			if stage != "read" {
				base.after = after
			}
			b := New(base)
			b.BeginBlock(bufHash(1), 1)
			b.Put(predecessorTyped([]byte("root/99")), []byte("overlay"))
			k, v, ok, err := b.SeekPrefixBefore(predecessorTyped([]byte("root/")), nil)
			if k != nil || v != nil || ok || base.iterators != 1 || base.releases != 1 || base.before != nil && !errors.Is(err, before) || base.after != nil && !errors.Is(err, after) {
				t.Fatalf("got %x/%x/%t/%v calls=%d releases=%d", k, v, ok, err, base.iterators, base.releases)
			}
		})
	}
}

func TestPredecessorCandidatesBucketExpansionAndFallback(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	defer base.Close()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	m := make(predecessorMaterialized)
	var prefixes [][]byte
	for _, first := range []byte{0, 1, 2, 0xff} {
		owner := common.Address{0x41, first, 7}
		prefix := rawdb.StateKVLatestCommitmentKey(owner, 9, kvdomains.ContractStorage, []byte{0xff})
		prefixes = append(prefixes, prefix)
		for _, suffix := range []byte{0, 1, 0xfe, 0xff} {
			key := append(bytes.Clone(prefix), suffix)
			value := []byte{first, suffix}
			b.Put(key, value)
			m.put(key, value)
		}
	}
	assertPredecessorMaterialized(t, b, m, prefixes)
	// A second owner bucket expands the same existing index into all buckets.
	// Later raw writes must append using the existing generic mutation hooks.
	for i, prefix := range prefixes {
		key := append(bytes.Clone(prefix), 0xff, 1)
		value := []byte{byte(i), 99}
		b.Put(key, value)
		m.put(key, value)
	}
	assertPredecessorMaterialized(t, b, m, prefixes)
	l := b.loadReadView().inflight[0]
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		s.prefixBucketIndex = newLayerPrefixBucketIndex("unrelated-schema/")
		s.mu.Unlock()
	}
	assertPredecessorMaterialized(t, b, m, prefixes)
	// A concurrent flush can hold a shard RLock during durable I/O. A missing
	// bucket must use the map fallback instead of waiting for an upgraded lock.
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		s.prefixBucketIndex = nil
		s.mu.Unlock()
		s.mu.RLock()
	}
	assertPredecessorMaterialized(t, b, m, prefixes)
	for i := range l.shards {
		l.shards[i].mu.RUnlock()
	}
}

func TestPredecessorCandidatesConcurrentMutation(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	defer base.Close()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	key := predecessorTyped([]byte("root/01"))
	b.Put(key, []byte{1})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; n < 500; n++ {
			b.Put(key, []byte{byte(n)})
			other := predecessorTyped([]byte(fmt.Sprintf("unrelated/%04d", n)))
			b.Put(other, []byte{2})
			b.Delete(other)
		}
	}()
	for n := 0; n < 500; n++ {
		got, _, ok, err := b.SeekPrefixBefore(predecessorTyped([]byte("root/")), nil)
		if err != nil || !ok || !bytes.Equal(got, key) {
			t.Fatalf("concurrent predecessor: %x/%t/%v", got, ok, err)
		}
	}
	<-done
}
