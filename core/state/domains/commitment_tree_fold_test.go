package domains

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

// mapBranchStore is an in-memory branchStore test double keyed by the packed
// prefix (one byte per nibble; empty prefix == "").
type mapBranchStore struct {
	m map[string]BranchData
}

func newMapBranchStore() *mapBranchStore {
	return &mapBranchStore{m: make(map[string]BranchData)}
}

func (s *mapBranchStore) GetBranch(prefix []byte) (BranchData, bool, error) {
	b, ok := s.m[string(prefix)]
	if !ok {
		return BranchData{}, false, nil
	}
	// Return a decoded copy so callers can mutate freely without aliasing.
	cp, err := DecodeBranchData(b.Encode())
	if err != nil {
		return BranchData{}, false, err
	}
	return cp, true, nil
}

func (s *mapBranchStore) PutBranch(prefix []byte, b BranchData) error {
	// Store a decoded copy to defend against later caller mutation.
	cp, err := DecodeBranchData(b.Encode())
	if err != nil {
		return err
	}
	s.m[string(prefix)] = cp
	return nil
}

func (s *mapBranchStore) DelBranch(prefix []byte) error {
	delete(s.m, string(prefix))
	return nil
}

// rowSet returns the prefix→encoded-branch map for equality comparisons.
func (s *mapBranchStore) rowSet() map[string][]byte {
	out := make(map[string][]byte, len(s.m))
	for k, v := range s.m {
		out[k] = v.Encode()
	}
	return out
}

func assertRowSetsEqual(t *testing.T, a, b map[string][]byte) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("row-set size mismatch: %d vs %d\n  a=%v\n  b=%v",
			len(a), len(b), prefixKeys(a), prefixKeys(b))
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			t.Fatalf("prefix %x present in a but not b", k)
		}
		if !bytes.Equal(av, bv) {
			t.Fatalf("branch at prefix %x differs:\n  a=%x\n  b=%x", k, av, bv)
		}
	}
}

func prefixKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func put(key, value []byte) Update {
	return Update{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)}
}

func del(key []byte) Update {
	return Update{Key: append([]byte(nil), key...), Delete: true}
}

// emptyRoot is the documented empty-trie root.
var emptyRoot = common.Hash{}

func TestCommitmentTrie_SingleKeyRoot(t *testing.T) {
	store := newMapBranchStore()
	trie := newCommitmentTrie(store)

	// Empty trie → zero hash, no rows.
	root, err := trie.Fold(nil)
	if err != nil {
		t.Fatalf("Fold(nil): %v", err)
	}
	if root != emptyRoot {
		t.Fatalf("empty trie root = %x, want zero", root)
	}
	if len(store.m) != 0 {
		t.Fatalf("empty trie left %d branch rows", len(store.m))
	}

	// Single key → root == that key's leaf value-hash.
	key := []byte("hello")
	val := []byte("world")
	root, err = trie.Fold([]Update{put(key, val)})
	if err != nil {
		t.Fatalf("Fold(put): %v", err)
	}
	want := leafValueHash(key, val)
	if root != want {
		t.Fatalf("single-key root = %x, want leaf value-hash %x", root, want)
	}
}

func TestCommitmentTrie_DeterministicAcrossOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const n = 200
	updates := make([]Update, n)
	for i := range updates {
		klen := rng.Intn(40) + 1
		vlen := rng.Intn(60) + 1
		key := make([]byte, klen)
		val := make([]byte, vlen)
		rng.Read(key)
		rng.Read(val)
		updates[i] = put(key, val)
	}

	store1 := newMapBranchStore()
	root1, err := newCommitmentTrie(store1).Fold(updates)
	if err != nil {
		t.Fatalf("Fold #1: %v", err)
	}

	// Shuffle the SAME updates and fold into a fresh store.
	shuffled := append([]Update(nil), updates...)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	store2 := newMapBranchStore()
	root2, err := newCommitmentTrie(store2).Fold(shuffled)
	if err != nil {
		t.Fatalf("Fold #2: %v", err)
	}

	if root1 != root2 {
		t.Fatalf("root differs across input order: %x vs %x", root1, root2)
	}
	assertRowSetsEqual(t, store1.rowSet(), store2.rowSet())
}

func TestCommitmentTrie_IncrementalEqualsBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const n = 150
	updates := make([]Update, n)
	for i := range updates {
		key := make([]byte, rng.Intn(30)+1)
		val := make([]byte, rng.Intn(30)+1)
		rng.Read(key)
		rng.Read(val)
		updates[i] = put(key, val)
	}

	// Batch: all at once.
	batchStore := newMapBranchStore()
	batchRoot, err := newCommitmentTrie(batchStore).Fold(updates)
	if err != nil {
		t.Fatalf("batch Fold: %v", err)
	}

	// Incremental: N separate Fold(one) calls on a fresh store.
	incStore := newMapBranchStore()
	var incRoot common.Hash
	for i, u := range updates {
		incRoot, err = newCommitmentTrie(incStore).Fold([]Update{u})
		if err != nil {
			t.Fatalf("incremental Fold #%d: %v", i, err)
		}
	}

	if batchRoot != incRoot {
		t.Fatalf("incremental root %x != batch root %x", incRoot, batchRoot)
	}
	assertRowSetsEqual(t, batchStore.rowSet(), incStore.rowSet())
}

func TestCommitmentTrie_InsertDeleteIdentity(t *testing.T) {
	store := newMapBranchStore()

	key := []byte("some-commitment-key")
	val := []byte("some-value")

	if _, err := newCommitmentTrie(store).Fold([]Update{put(key, val)}); err != nil {
		t.Fatalf("Fold(put): %v", err)
	}

	root, err := newCommitmentTrie(store).Fold([]Update{del(key)})
	if err != nil {
		t.Fatalf("Fold(del): %v", err)
	}
	if root != emptyRoot {
		t.Fatalf("root after delete = %x, want empty-trie root", root)
	}
	if len(store.m) != 0 {
		t.Fatalf("store has %d branch rows after delete, want 0: %v",
			len(store.m), prefixKeys(store.rowSet()))
	}

	// A larger insert/delete identity: insert many, delete all → zero rows, zero root.
	rng := rand.New(rand.NewSource(99))
	const n = 120
	keys := make([][]byte, n)
	puts := make([]Update, n)
	dels := make([]Update, n)
	for i := 0; i < n; i++ {
		k := make([]byte, rng.Intn(20)+1)
		v := make([]byte, rng.Intn(20)+1)
		rng.Read(k)
		rng.Read(v)
		keys[i] = k
		puts[i] = put(k, v)
		dels[i] = del(k)
	}
	if _, err := newCommitmentTrie(store).Fold(puts); err != nil {
		t.Fatalf("bulk Fold(put): %v", err)
	}
	root, err = newCommitmentTrie(store).Fold(dels)
	if err != nil {
		t.Fatalf("bulk Fold(del): %v", err)
	}
	if root != emptyRoot {
		t.Fatalf("bulk root after delete = %x, want empty", root)
	}
	if len(store.m) != 0 {
		t.Fatalf("store has %d branch rows after bulk delete, want 0", len(store.m))
	}
}

// TestCommitmentTrie_ExtensionLikeChain exercises the non-root single-HASH-child
// shape: two keys whose hashed paths share a long common nibble prefix force a
// chain of single-HASH branches down to the divergence point. It checks that
// (a) the chain round-trips through insert, (b) batch == incremental over that
// chain, and (c) deleting both keys removes every branch row (no orphan chain).
func TestCommitmentTrie_ExtensionLikeChain(t *testing.T) {
	ka, kb, share := findSharedPrefixPair(t, 6)
	t.Logf("found pair sharing %d leading nibbles: %x / %x", share, ka, kb)

	va := []byte("va")
	vb := []byte("vb")

	// Batch insert both.
	batchStore := newMapBranchStore()
	batchRoot, err := newCommitmentTrie(batchStore).Fold([]Update{put(ka, va), put(kb, vb)})
	if err != nil {
		t.Fatalf("batch Fold: %v", err)
	}
	// There must be at least `share` intermediate single-HASH branches plus the
	// divergence branch, i.e. more than one row.
	if len(batchStore.m) < 2 {
		t.Fatalf("expected a multi-row chain, got %d rows", len(batchStore.m))
	}

	// Incremental insert: one at a time.
	incStore := newMapBranchStore()
	if _, err := newCommitmentTrie(incStore).Fold([]Update{put(ka, va)}); err != nil {
		t.Fatalf("inc Fold a: %v", err)
	}
	incRoot, err := newCommitmentTrie(incStore).Fold([]Update{put(kb, vb)})
	if err != nil {
		t.Fatalf("inc Fold b: %v", err)
	}
	if incRoot != batchRoot {
		t.Fatalf("incremental root %x != batch root %x over extension chain", incRoot, batchRoot)
	}
	assertRowSetsEqual(t, batchStore.rowSet(), incStore.rowSet())

	// Delete both → empty trie, zero rows (no orphan chain left behind).
	root, err := newCommitmentTrie(batchStore).Fold([]Update{del(ka), del(kb)})
	if err != nil {
		t.Fatalf("delete both: %v", err)
	}
	if root != emptyRoot {
		t.Fatalf("root after deleting both = %x, want empty", root)
	}
	if len(batchStore.m) != 0 {
		t.Fatalf("orphan chain: %d rows remain after deleting both: %v",
			len(batchStore.m), prefixKeys(batchStore.rowSet()))
	}

	// Deleting only one of the two collapses the chain back to a single-key
	// trie whose root is the survivor's leaf value-hash.
	store := newMapBranchStore()
	if _, err := newCommitmentTrie(store).Fold([]Update{put(ka, va), put(kb, vb)}); err != nil {
		t.Fatalf("re-insert pair: %v", err)
	}
	root, err = newCommitmentTrie(store).Fold([]Update{del(kb)})
	if err != nil {
		t.Fatalf("delete one: %v", err)
	}
	if want := leafValueHash(ka, va); root != want {
		t.Fatalf("root after deleting one = %x, want survivor leaf %x", root, want)
	}
	// Collapsed to the singleton: exactly one root row holding a single leaf.
	if len(store.m) != 1 {
		t.Fatalf("expected exactly 1 row after collapse, got %d", len(store.m))
	}
}

// findSharedPrefixPair grinds keys until it finds two whose hashed paths share at
// least minShared leading nibbles.
func findSharedPrefixPair(t *testing.T, minShared int) (ka, kb []byte, shared int) {
	t.Helper()
	seen := make(map[[6]byte][]byte) // first 6 nibbles → key, for a quick bucket
	for i := 0; i < 1<<24; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		p := keyPath(k)
		var bucket [6]byte
		copy(bucket[:], p[:6])
		if prev, ok := seen[bucket]; ok {
			pp := keyPath(prev)
			s := commonNibbles(pp[:], p[:])
			if s >= minShared {
				return append([]byte(nil), prev...), k, s
			}
		}
		seen[bucket] = append([]byte(nil), k...)
	}
	t.Fatalf("could not find a key pair sharing %d nibbles", minShared)
	return nil, nil, 0
}

func commonNibbles(a, b []byte) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n
}

// referenceRoot builds the trie from scratch over all live entries and returns
// the root hash, using a throwaway store. This is the naive oracle.
func referenceRoot(t *testing.T, live map[string][]byte) common.Hash {
	t.Helper()
	if len(live) == 0 {
		return emptyRoot
	}
	updates := make([]Update, 0, len(live))
	for k, v := range live {
		updates = append(updates, put([]byte(k), v))
	}
	root, err := newCommitmentTrie(newMapBranchStore()).Fold(updates)
	if err != nil {
		t.Fatalf("referenceRoot Fold: %v", err)
	}
	return root
}

func FuzzCommitmentTrie(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06})
	f.Add([]byte{0xff, 0x00, 0xff, 0x00})
	f.Add([]byte("the quick brown fox"))

	f.Fuzz(func(t *testing.T, ops []byte) {
		store := newMapBranchStore()
		live := make(map[string][]byte)

		// Interpret the fuzz bytes as a sequence of operations. Each op uses a
		// few bytes: [opcode][keyByte][valByte]. Keep a small key space so
		// collisions, splits and collapses are exercised.
		i := 0
		applyOne := func(u Update) {
			root, err := newCommitmentTrie(store).Fold([]Update{u})
			if err != nil {
				t.Fatalf("Fold: %v", err)
			}
			// Update the naive reference set.
			if u.Delete {
				delete(live, string(u.Key))
			} else {
				live[string(u.Key)] = append([]byte(nil), u.Value...)
			}
			// Recompute reference root from scratch over all live entries.
			want := referenceRoot(t, live)
			if root != want {
				t.Fatalf("incremental root %x != reference root %x (live=%d)", root, want, len(live))
			}
		}

		// Cap the number of ops per execution. libFuzzer grows inputs into the
		// multi-KB range; without a cap a single execution would fold hundreds of
		// times AND rebuild the reference from scratch each step (O(n²)), which
		// stalls the fuzzer's per-execution budget. The cap keeps each execution
		// bounded so the fuzzer can actually explore structural churn.
		const maxFuzzOps = 64
		for n := 0; i+2 < len(ops) && n < maxFuzzOps; n++ {
			opcode := ops[i]
			// Constrain the key to a small alphabet to force structural churn.
			key := []byte{ops[i+1] & 0x07}
			val := []byte{ops[i+2]}
			i += 3
			if opcode&0x01 == 0 {
				applyOne(Update{Key: key, Value: val})
			} else {
				applyOne(Update{Key: key, Delete: true})
			}
		}

		// Final cross-check: a from-scratch rebuild of live entries must equal
		// the incremental store's current root.
		root, _, err := readRootForTest(store)
		if err != nil {
			t.Fatalf("readRoot: %v", err)
		}
		if want := referenceRoot(t, live); root != want {
			t.Fatalf("final root %x != reference %x", root, want)
		}
	})
}

// readRootForTest re-derives the current root from a store without applying any
// update, by folding an empty update set.
func readRootForTest(store *mapBranchStore) (common.Hash, bool, error) {
	root, err := newCommitmentTrie(store).Fold(nil)
	return root, true, err
}
