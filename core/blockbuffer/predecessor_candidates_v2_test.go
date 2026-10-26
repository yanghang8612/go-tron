package blockbuffer

import (
	"bytes"
	"sort"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestPredecessorCandidatesV2SortedBucketLifecycle(t *testing.T) {
	s := &layerShard{writes: map[string][]byte{"p/\x01z": {1}, "p/\x01a": {2}, "p/\x02m": {3}}, deletes: make(map[string]struct{}), prefixBucketIndex: newLayerPrefixBucketIndex("p/")}
	i := s.prefixBucketIndex
	i.ensureBucketSorted(1, s.writes, s.deletes)
	if !i.bucketSorted(1) || !sort.StringsAreSorted(i.buckets[1]) {
		t.Fatal("initial bucket not sorted")
	}
	backing := &i.buckets[1][0]
	for _, op := range []string{"replace", "delete", "revive"} {
		key := "p/\x01z"
		s.trackPrefixBucketKeyBeforeMutation(key)
		if op == "delete" {
			delete(s.writes, key)
			s.deletes[key] = struct{}{}
		} else {
			delete(s.deletes, key)
			s.writes[key] = []byte(op)
		}
		if !i.bucketSorted(1) {
			t.Fatalf("%s invalidated stable membership", op)
		}
		i.ensureBucketSorted(1, s.writes, s.deletes)
		if &i.buckets[1][0] != backing {
			t.Fatal("stable bucket rebuilt its key storage")
		}
	}
	for _, key := range []string{"p/\x01c", "p/\x01b"} {
		s.trackPrefixBucketKeyBeforeMutation(key)
		s.writes[key] = []byte(key)
		if i.bucketSorted(1) {
			t.Fatal("new write left sorted bit set")
		}
		i.ensureBucketSorted(1, s.writes, s.deletes)
		if !i.bucketSorted(1) || !sort.StringsAreSorted(i.buckets[1]) {
			t.Fatal("new write missing from resort")
		}
	}
	key := "p/\x01y"
	s.trackPrefixBucketKeyBeforeMutation(key)
	s.deletes[key] = struct{}{}
	if i.bucketSorted(1) {
		t.Fatal("new tombstone left sorted bit set")
	}
	i.ensureBucketSorted(1, s.writes, s.deletes)
	// A second first-byte request repacks all buckets; old sorted bits cannot
	// survive the arbitrary map iteration order of the packed arena.
	i.ensureBucketSorted(2, s.writes, s.deletes)
	if i.bucketSorted(1) || !i.bucketSorted(2) {
		t.Fatal("packed build retained stale sorted bits")
	}
	i.ensureBucketSorted(1, s.writes, s.deletes)
	if !sort.StringsAreSorted(i.buckets[1]) {
		t.Fatal("packed bucket not resorted")
	}
	for bucket, keys := range i.buckets {
		if cap(keys) != len(keys) {
			t.Fatalf("bucket %d lost packed arena bounds", bucket)
		}
	}
}

func TestPredecessorCandidatesV2SchemaOwnershipAndMutations(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	defer base.Close()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	m := make(predecessorMaterialized)
	owner := common.SystemAccountAddress
	prefix := predecessorTyped([]byte("root/"))
	upper := predecessorTyped([]byte("root/99"))
	key := predecessorTyped([]byte("root/01"))
	value := []byte("initial")
	b.Put(key, value)
	m.put(key, value)
	k, v, ok, err := b.SeekPrefixBefore(prefix, upper)
	if err != nil || !ok {
		t.Fatal(err)
	}
	for n := range prefix {
		prefix[n] ^= 0xff
	}
	for n := range upper {
		upper[n] ^= 0xff
	}
	if !bytes.Equal(k, key) || !bytes.Equal(v, value) {
		t.Fatal("caller reuse changed owned result")
	}
	schemaSize := rawdb.StateKVLatestCommitmentKeySize(0) - common.AccountIDLength - 8 - 2
	wantSchema := string(key[:schemaSize])
	l := b.loadReadView().inflight[0]
	for n := range l.shards {
		s := &l.shards[n]
		s.mu.RLock()
		i := s.prefixBucketIndex
		if i != nil && i.prefix != wantSchema {
			s.mu.RUnlock()
			t.Fatal("caller prefix escaped into persistent schema")
		}
		s.mu.RUnlock()
	}
	prefix = predecessorTyped([]byte("root/"))
	// The structured and owned batch APIs all use the same first-new-key hook.
	for n, logical := range [][]byte{[]byte("root/02"), []byte("root/03"), []byte("root/04")} {
		encoded := rawdb.EncodeStateKVLatestValue([]byte{byte(n)})
		batch := b.NewBatch()
		if err := rawdb.WriteStateKVLatestEncodedOwned(batch, owner, 0, kvdomains.SystemShielded, logical, encoded); err != nil {
			t.Fatal(err)
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}
		m.put(predecessorTyped(logical), encoded)
		assertPredecessorMaterialized(t, b, m, [][]byte{prefix})
	}
	batch := b.NewBatch()
	logical := []byte("root/04")
	if err := rawdb.DeleteStateKVLatest(batch, owner, 0, kvdomains.SystemShielded, logical); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	m.del(predecessorTyped(logical))
	assertPredecessorMaterialized(t, b, m, [][]byte{prefix})
	start, end := predecessorTyped([]byte("root/02")), predecessorTyped([]byte("root/90"))
	if err := b.DeleteRange(start, end); err != nil {
		t.Fatal(err)
	}
	m.delRange(start, end)
	for n := range l.shards {
		s := &l.shards[n]
		s.mu.RLock()
		i := s.prefixBucketIndex
		if i != nil && i.sorted != [4]uint64{} {
			s.mu.RUnlock()
			t.Fatal("range reset retained sorted state")
		}
		s.mu.RUnlock()
	}
	assertPredecessorMaterialized(t, b, m, [][]byte{prefix})
	// Generic index creation can share the holder and supersedes bucket seeks.
	it := b.NewIterator(nil, nil)
	it.Release()
	b.Put(predecessorTyped([]byte("root/80")), []byte("after-generic"))
	m.put(predecessorTyped([]byte("root/80")), []byte("after-generic"))
	assertPredecessorMaterialized(t, b, m, [][]byte{prefix})
}
