package state

import (
	"bytes"
	"errors"
	"fmt"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

func legacyPendingPrefix(w *accountKVLatestBatch, owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(logicalKey, value []byte) (bool, error)) error {
	if w == nil || w.latestStore == nil {
		return nil
	}
	entries := make(map[string][]byte)
	if err := w.latestStore.IterateKVLatest(owner, generation, domain, prefix, func(key, value []byte) (bool, error) {
		entries[string(key)] = append([]byte(nil), value...)
		return true, nil
	}); err != nil {
		return err
	}
	ownerID := owner.AccountID()
	logicalPrefix := string(prefix)
	for key, pending := range w.pending {
		if key.owner != ownerID || key.generation != generation || key.domain != domain || !strings.HasPrefix(key.logicalKey, logicalPrefix) {
			continue
		}
		mapKey := key.logicalKey
		if pending.deleted {
			delete(entries, mapKey)
			continue
		}
		entries[mapKey] = append([]byte(nil), pending.value...)
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		cont, err := fn([]byte(key), append([]byte(nil), entries[key]...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

type prefixRow struct{ k, v string }

func prefixResults(t *testing.T, w *accountKVLatestBatch, owner tcommon.Address, gen uint64, domain kvdomains.KVDomain, prefix []byte, legacy bool) []prefixRow {
	t.Helper()
	var rows []prefixRow
	cb := func(k, v []byte) (bool, error) {
		rows = append(rows, prefixRow{string(k), string(v)})
		if len(k) > 0 {
			k[0] ^= 0xff
		}
		if len(v) > 0 {
			v[0] ^= 0xff
		}
		return true, nil
	}
	var err error
	if legacy {
		err = legacyPendingPrefix(w, owner, gen, domain, prefix, cb)
	} else {
		err = w.iterateLatestPrefix(owner, gen, domain, prefix, cb)
	}
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestPendingPrefixIndexDifferential(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	defer db.Close()
	w := newAccountKVLatestDomainBatch(db, zeroGeneration, nil, nil)
	owners := []tcommon.Address{pruneTestOwner(1), pruneTestOwner(2)}
	domains := []kvdomains.KVDomain{kvdomains.AccountVotesAux, kvdomains.SystemReward}
	keys := [][]byte{nil, []byte("a"), []byte("ab"), []byte("abc"), {0, 255}, bytes.Repeat([]byte("long"), 100)}
	w.commitBlock = 2
	for i := 0; i < 300; i++ {
		w.rememberPut(pruneTestOwner(100+i), 0, kvdomains.ContractStorage, []byte("padding"), []byte("v"))
	}
	w.commitBlock = 2
	for _, o := range owners {
		for gen := uint64(0); gen < 2; gen++ {
			for _, d := range domains {
				for i, k := range keys {
					if err := rawdb.WriteStateKVLatest(db, o, gen, d, k, []byte("base")); err != nil {
						t.Fatal(err)
					}
					if i%3 == 0 {
						w.rememberDelete(o, gen, d, k)
					} else {
						w.rememberPut(o, gen, d, k, []byte(fmt.Sprint(i)))
					}
				}
			}
		}
	}
	check := func() {
		t.Helper()
		for _, o := range owners {
			alias := o
			alias[0] = 0xa0
			for gen := uint64(0); gen < 3; gen++ {
				for _, d := range domains {
					for _, pre := range [][]byte{nil, []byte("a"), {0}, []byte("missing")} {
						want := prefixResults(t, w, o, gen, d, pre, true)
						got := prefixResults(t, w, alias, gen, d, pre, false)
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("%x/%d/%d/%x got=%v want=%v", o, gen, d, pre, got, want)
						}
					}
				}
			}
		}
	}
	check()
	if w.pendingPrefixes == nil {
		t.Fatal("repeated requests did not build index")
	}
	// Both the copying and owned put/delete entry points maintain new and old buckets.
	for _, o := range owners {
		for _, d := range domains {
			w.rememberPut(o, 0, d, []byte("new"), []byte("one"))
			w.rememberPutOwned(o, 0, d, []byte("new"), []byte("two"))
			w.rememberDelete(o, 0, d, []byte("new"))
			w.rememberDeleteOwned(o, 1, d, []byte("new-delete"))
			w.rememberPut(o, 0, d, []byte("new"), nil)
			w.rememberPutOwned(o, 8, d, []byte("new-generation"), []byte("eight"))
		}
	}
	check()
	for g, list := range w.pendingPrefixes {
		seen := map[accountKVPendingPrefixKey]bool{}
		for _, k := range list {
			if seen[k] {
				t.Fatal("duplicate bucket member", g, k)
			}
			seen[k] = true
		}
	}
	w.prunePending(1)
	if w.pendingPrefixes == nil {
		t.Fatal("prune with no eligible entries invalidated index")
	}
	check()
	w.prunePending(2)
	if w.pendingPrefixes != nil || w.pendingPrefixScans != 0 {
		t.Fatal("prune retained stale index")
	}
	check()
	w.rememberPut(owners[0], 0, domains[0], []byte("after-prune"), []byte("v"))
	check()
	w.reset()
	if w.pendingPrefixes != nil || w.pendingPrefixScans != 0 || len(w.pending) != 0 {
		t.Fatal("reset retained index or pending")
	}
}

func TestPendingPrefixIndexAdmissionAndDeepPrune(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	defer db.Close()
	w := newAccountKVLatestDomainBatch(db, zeroGeneration, nil, nil)
	owner := pruneTestOwner(9)
	d := kvdomains.SystemReward
	for i := 0; i < 300; i++ {
		w.commitBlock = 10
		w.rememberPut(pruneTestOwner(100+i), 0, kvdomains.ContractStorage, []byte("padding"), []byte("v"))
	}
	w.commitBlock = 4
	w.rememberPut(owner, 0, d, []byte("a"), []byte("pending"))
	prefixResults(t, w, owner, 0, d, nil, false)
	if w.pendingPrefixes != nil {
		t.Fatal("single read built index")
	}
	for i := 0; i < 4; i++ {
		prefixResults(t, w, owner, 0, d, nil, false)
	}
	if w.pendingPrefixes == nil {
		t.Fatal("fifth read did not build index")
	}
	w.deepAsync = true
	originalStore := w.latestStore
	failure := errors.New("durable check failed")
	w.latestStore = &pendingPrefixErrorStore{originalStore, failure}
	w.prunePending(4)
	if w.pendingPrefixes == nil || len(w.pending) != 301 {
		t.Fatal("durability error removed entry or invalidated index")
	}
	w.latestStore = originalStore
	w.prunePending(4)
	if w.pendingPrefixes == nil || len(w.pending) != 301 {
		t.Fatal("non-durable deep entry was removed or invalidated")
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 0, d, []byte("a"), []byte("pending")); err != nil {
		t.Fatal(err)
	}
	w.prunePending(4)
	if w.pendingPrefixes != nil || w.pendingPrefixScans != 0 || len(w.pending) != 300 {
		t.Fatal("durable deep prune did not invalidate index")
	}
}

type pendingPrefixErrorStore struct {
	accountKVPhysicalLatestStore
	err error
}

func (s *pendingPrefixErrorStore) IterateKVLatest(tcommon.Address, uint64, kvdomains.KVDomain, []byte, func([]byte, []byte) (bool, error)) error {
	return s.err
}
func (s *pendingPrefixErrorStore) ReadKVLatest(tcommon.Address, uint64, kvdomains.KVDomain, []byte) ([]byte, bool, error) {
	return nil, false, s.err
}
func TestPendingPrefixIndexErrorAndCallback(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	defer db.Close()
	w := newAccountKVLatestDomainBatch(db, zeroGeneration, nil, nil)
	owner := pruneTestOwner(8)
	d := kvdomains.SystemReward
	w.rememberPut(owner, 0, d, []byte("a"), []byte("v"))
	w.rememberPut(owner, 0, d, []byte("b"), []byte("v"))
	for i := 0; i < 300; i++ {
		w.rememberPut(pruneTestOwner(100+i), 0, d, []byte("padding"), []byte("v"))
	}
	original := w.latestStore
	failure := errors.New("iterate failed")
	w.latestStore = &pendingPrefixErrorStore{original, failure}
	calls := 0
	if err := w.iterateLatestPrefix(owner, 0, d, nil, func([]byte, []byte) (bool, error) { calls++; return true, nil }); !errors.Is(err, failure) || calls != 0 || w.pendingPrefixScans != 0 || w.pendingPrefixes != nil {
		t.Fatal("durable error changed callback/index order", err)
	}
	w.latestStore = original
	for i := 0; i < 6; i++ {
		calls = 0
		err := w.iterateLatestPrefix(owner, 0, d, nil, func([]byte, []byte) (bool, error) { calls++; return false, nil })
		if err != nil || calls != 1 {
			t.Fatal("stop", err, calls)
		}
	}
	if w.pendingPrefixes == nil {
		t.Fatal("callback stop path never built index")
	}
	calls = 0
	if err := w.iterateLatestPrefix(owner, 0, d, nil, func([]byte, []byte) (bool, error) { calls++; return true, failure }); !errors.Is(err, failure) || calls != 1 {
		t.Fatal("callback error", err, calls)
	}
}

func TestPendingPrefixIndexConcurrentStableReaders(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprint(warm), func(t *testing.T) {
			db := ethrawdb.NewMemoryDatabase()
			defer db.Close()
			w := newAccountKVLatestDomainBatch(db, zeroGeneration, nil, nil)
			o := pruneTestOwner(5)
			d := kvdomains.AccountVotesAux
			for i := 0; i < 512; i++ {
				w.rememberPut(o, 7, d, []byte(fmt.Sprintf("%03d", i)), []byte("value"))
				w.rememberPut(pruneTestOwner(6), 7, d, []byte(fmt.Sprintf("%03d", i)), []byte("other"))
			}
			if warm {
				for i := 0; i < 5; i++ {
					prefixResults(t, w, o, 7, d, nil, false)
				}
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			errs := make(chan error, 8)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for j := 0; j < 20; j++ {
						n := 0
						err := w.iterateLatestPrefix(o, 7, d, nil, func(k, v []byte) (bool, error) {
							if len(k) != 3 || string(v) != "value" {
								return false, fmt.Errorf("bad row")
							}
							n++
							return true, nil
						})
						if err != nil || n != 512 {
							errs <- fmt.Errorf("rows=%d err=%v", n, err)
							return
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
		})
	}
}

type legacyPendingPrefixIterator struct{ w *accountKVLatestBatch }

func (r legacyPendingPrefixIterator) KVLatestPrefix(o tcommon.Address, g uint64, d kvdomains.KVDomain, p []byte, fn func([]byte, []byte) (bool, error)) error {
	return legacyPendingPrefix(r.w, o, g, d, p, fn)
}

func (r legacyPendingPrefixIterator) DomainIterate(o tcommon.Address, d kvdomains.KVDomain, p []byte, fn statedomains.IterateFunc) error {
	g, err := r.w.resolveGeneration(o)
	if err != nil {
		return err
	}
	return legacyPendingPrefix(r.w, o, g, d, p, fn)
}

func TestPendingPrefixStateCommitAndRevertDifferential(t *testing.T) {
	type result struct {
		roots []tcommon.Hash
		reads [][]prefixRow
	}
	run := func(legacy bool) result {
		s := newTestStateDB(t)
		owner := pruneTestOwner(11)
		domain := kvdomains.AccountAssetV2
		s.CreateAccount(owner, 0)
		otherOwner := pruneTestOwner(12)
		s.CreateAccount(otherOwner, 0)
		if _, err := s.Commit(); err != nil {
			t.Fatal(err)
		}
		scope := s.NewCommitScope()
		defer scope.Discard()
		var out result
		commit := func(n uint64) {
			root, _, err := s.CommitWithStatsOptionsInScope(scope, CommitOptions{BlockNumber: n, FlushLatestDomain: func() error { return nil }})
			if err != nil {
				t.Fatal(err)
			}
			out.roots = append(out.roots, root)
		}
		for i := 0; i < 512; i++ {
			if err := s.SetAccountKV(owner, domain, []byte(fmt.Sprintf("prefix/%04d", i)), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := s.SetAccountKV(otherOwner, domain, []byte(fmt.Sprintf("prefix/%04d", i)), []byte("other")); err != nil {
				t.Fatal(err)
			}
		}
		commit(1)
		if legacy {
			s.accountKVLatestIterator = legacyPendingPrefixIterator{scope.latestWriter}
		}
		read := func() {
			var got []prefixRow
			if err := s.IterateAccountKV(owner, domain, []byte("prefix/"), func(k, v []byte) (bool, error) { got = append(got, prefixRow{string(k), string(v)}); return true, nil }); err != nil {
				t.Fatal(err)
			}
			out.reads = append(out.reads, got)
		}
		for i := 0; i < 6; i++ {
			read()
		}
		if !legacy && scope.latestWriter.pendingPrefixes == nil {
			t.Fatal("StateDB differential did not build index")
		}
		rev := s.Snapshot()
		if err := s.DeleteAccountKVPrefix(owner, domain, []byte("prefix/00")); err != nil {
			t.Fatal(err)
		}
		read()
		s.RevertToSnapshot(rev)
		read()
		if err := s.SetAccountKV(owner, domain, []byte("prefix/0001"), []byte("changed")); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteAccountKV(owner, domain, []byte("prefix/0002")); err != nil {
			t.Fatal(err)
		}
		commit(2)
		if legacy {
			s.accountKVLatestIterator = legacyPendingPrefixIterator{scope.latestWriter}
		}
		for i := 0; i < 6; i++ {
			read()
		}
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
		read()
		return out
	}
	want, got := run(true), run(false)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("state roots, prefix/revert results or published reads differ from legacy scan")
	}
}

func TestPendingPrefixIndexWorkReductionAndSmallFallback(t *testing.T) {
	for _, n := range []int{64, 4096} {
		db := ethrawdb.NewMemoryDatabase()
		w := newAccountKVLatestDomainBatch(db, zeroGeneration, nil, nil)
		rows := pendingPrefixTrace(n)
		for _, r := range rows {
			w.rememberPut(r.owner, r.generation, r.domain, r.key, r.value)
		}
		r := rows[0]
		for i := 0; i < 6; i++ {
			prefixResults(t, w, r.owner, r.generation, r.domain, nil, false)
		}
		if n <= 256 {
			if w.pendingPrefixes != nil {
				t.Fatal("small overlay allocated index")
			}
		} else {
			keys, indexed := w.pendingPrefixKeys(accountKVLatestPendingGroup{r.owner.AccountID(), r.generation, r.domain})
			if !indexed || len(keys) != 16 {
				t.Fatalf("indexed=%v candidates=%d, want 16 of %d", indexed, len(keys), n)
			}
			matching := 0
			for _, key := range keys {
				if key.generation == r.generation && key.domain == r.domain {
					matching++
				}
			}
			if matching != 2 {
				t.Fatalf("matching=%d, want 2", matching)
			}
		}
		db.Close()
	}
}

// Building exact-capacity adjacent owner buckets must isolate later appends.
func TestPendingPrefixOwnerArenaAppendIsolation(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	defer db.Close()
	w := newAccountKVLatestDomainBatch(db, zeroGeneration, nil, nil)
	for i := 0; i < 300; i++ {
		w.rememberPut(pruneTestOwner(i+1), 0, kvdomains.SystemReward, []byte("same"), []byte("v"))
	}
	owner := pruneTestOwner(1)
	other := pruneTestOwner(2)
	group := accountKVLatestPendingGroup{owner.AccountID(), 0, kvdomains.SystemReward}
	for i := 0; i < 5; i++ {
		w.pendingPrefixKeys(group)
	}
	before := append([]accountKVPendingPrefixKey(nil), w.pendingPrefixes[other.AccountID()]...)
	if cap(w.pendingPrefixes[owner.AccountID()]) != len(w.pendingPrefixes[owner.AccountID()]) {
		t.Fatal("owner bucket has spare arena capacity")
	}
	for gen := uint64(0); gen < 3; gen++ {
		for _, domain := range []kvdomains.KVDomain{kvdomains.SystemReward, kvdomains.AccountVotesAux} {
			w.rememberPut(owner, gen, domain, []byte("same"), []byte("next"))
			w.rememberDelete(owner, gen, domain, []byte("same"))
			w.rememberPut(owner, gen, domain, []byte("same"), []byte("restored"))
		}
	}
	if !reflect.DeepEqual(before, w.pendingPrefixes[other.AccountID()]) {
		t.Fatal("appending corrupted adjacent owner")
	}
	if len(w.pendingPrefixes[owner.AccountID()]) != 6 {
		t.Fatal("full tuple insert/update membership mismatch")
	}
	for gen := uint64(0); gen < 3; gen++ {
		for _, domain := range []kvdomains.KVDomain{kvdomains.SystemReward, kvdomains.AccountVotesAux} {
			if got, want := prefixResults(t, w, owner, gen, domain, nil, false), prefixResults(t, w, owner, gen, domain, nil, true); !reflect.DeepEqual(got, want) {
				t.Fatal("same logical key crossed generation/domain", gen, domain, got, want)
			}
		}
	}
}
