package state

import (
	"bytes"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type delegationReadCounter struct {
	store                   accountKVPhysicalLatestStore
	calls, pageCalls, bytes int
	pageKeys                map[string]int
}

func (r *delegationReadCounter) GetLatest(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	return r.KVLatest(owner, 0, domain, key)
}
func (r *delegationReadCounter) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	value, exists, err := r.store.ReadKVLatest(owner, generation, domain, key)
	if domain == kvdomains.SystemDelegation {
		r.calls++
		r.bytes += len(value)
		if _, ok := rawdb.DrAccountIndexLegacyPageAccount(key); ok {
			r.pageCalls++
			if r.pageKeys == nil {
				r.pageKeys = make(map[string]int)
			}
			r.pageKeys[string(key)]++
		}
	}
	return value, exists, err
}

func TestLegacyDelegationOptionalMembershipKeepsRepeatedReadsResident(t *testing.T) {
	f := seedLegacyBenchmark(t, 250_000)
	s := f.open(t)
	reads := &delegationReadCounter{store: s.accountKVPhysicalLatestStore()}
	s.accountKVLatestReader = reads
	if err := s.WriteDrAccountIndexLegacyDelegate(f.from, f.existing); err != nil {
		t.Fatal(err)
	}
	cache := s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation
	entry := cache.entries[string(rawdb.DrAccountIndexLegacyStateKey(f.from))]
	if entry == nil || entry.toSet != nil || entry.charge > legacyDelegationCacheBytes {
		t.Fatal("owned list was not admitted without optional map")
	}
	if reads.pageCalls < 1950 {
		t.Fatal("fixture did not read the full large paged index")
	}
	before := reads.calls
	decoded := legacyDelegationDecoded.Snapshot().Count()
	for range 4 {
		if err := s.WriteDrAccountIndexLegacyDelegate(f.from, f.existing); err != nil {
			t.Fatal(err)
		}
	}
	if reads.calls != before || legacyDelegationDecoded.Snapshot().Count() != decoded {
		t.Fatal("resident duplicate re-read or decoded pages")
	}
	// Multiple new peers grow the aggregate after its one spare header, change
	// the final page, and commit actual encoded versions. Map allocation failures
	// must not inflate charge and immediately bypass this otherwise fitting row.
	for i := 0; i < 64; i++ {
		peer := legacyBenchmarkAddress(uint64(300_000 + i))
		if err := s.WriteDrAccountIndexLegacyDelegate(f.from, peer); err != nil {
			t.Fatal(err)
		}
		if cache.entries[entry.key] != entry || entry.toSet != nil {
			t.Fatal("append discarded resident owned pages")
		}
		if i%8 == 7 {
			if _, err := s.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		assertDelegationCacheBounds(t, cache)
	}
	// Touched-page writes may fetch an encoded preimage; they must not fetch
	// every untouched owner page again. Count actual backing-store requests by
	// key rather than confusing required preimage reads with aggregate reloads.
	lastPage := string(rawdb.DrAccountIndexLegacyPageStateKey(f.from, false, 1953))
	for key, count := range reads.pageKeys {
		account, ok := rawdb.DrAccountIndexLegacyPageAccount([]byte(key))
		if ok && bytes.Equal(account, f.from) && key != lastPage && count != 1 {
			t.Fatalf("untouched owner page read %d times", count)
		}
	}
	if legacyDelegationDecoded.Snapshot().Count() != decoded {
		t.Fatal("append or commit re-decoded resident list")
	}

	want, ok, err := s.ReadDrAccountIndexLegacyStrict(f.from)
	if err != nil || !ok || len(want.ToAccounts) != 250_064 {
		t.Fatalf("final index: %v/%v", ok, err)
	}
	for i := 0; i < 64; i++ {
		if !bytes.Equal(want.ToAccounts[250_000+i], legacyBenchmarkAddress(uint64(300_000+i))) {
			t.Fatal("append order changed")
		}
	}

	recorder := new(TransactionAccessRecorder)
	recorder.Reset(4096)
	s.SetTransactionAccessRecorder(recorder)
	if _, _, err := s.loadLegacyDelegation(f.from); err != nil {
		t.Fatal(err)
	}
	dependencies := 0
	recorder.Visit(func(key TransactionAccessKey, mode TransactionAccessMode) bool {
		if mode&TransactionAccessRead != 0 && key.Kind == TransactionAccessAccountKV && key.KVDomain == kvdomains.SystemDelegation {
			if account, ok := rawdb.DrAccountIndexLegacyPageAccount([]byte(key.LogicalKey)); ok && bytes.Equal(account, f.from) {
				dependencies++
			}
		}
		return true
	})
	s.SetTransactionAccessRecorder(nil)
	if dependencies != len(entry.pages.from)+len(entry.pages.to) {
		t.Fatalf("map-free hit omitted page dependencies: %d", dependencies)
	}
	copied, err := s.Copy()
	if err != nil {
		t.Fatal(err)
	}
	if obj := copied.stateObjects[tcommon.SystemAccountAddress]; obj != nil && obj.legacyDelegation != nil {
		t.Fatal("copy shared map-free cache")
	}
	if err := copied.WriteDrAccountIndexLegacyUnDelegate(f.from, f.existing); err != nil {
		t.Fatal(err)
	}
	parent, _, err := s.ReadDrAccountIndexLegacyStrict(f.from)
	if err != nil || len(parent.ToAccounts) != len(want.ToAccounts) {
		t.Fatal("copy mutation changed parent")
	}
	// Generic page mutation/revert must invalidate even the map-free resident.
	snapshot := s.Snapshot()
	key := rawdb.DrAccountIndexLegacyPageStateKey(f.from, false, 0)
	if err := s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, []byte("bad page")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.loadLegacyDelegation(f.from); err == nil {
		t.Fatal("resident entry hid corrupt page")
	}
	s.RevertToSnapshot(snapshot)
	got, ok, err := s.ReadDrAccountIndexLegacyStrict(f.from)
	if err != nil || !ok || len(got.ToAccounts) != len(want.ToAccounts) {
		t.Fatal("page revert changed logical index")
	}
}

func TestLegacyDelegationOptionalMembershipChargeBoundaries(t *testing.T) {
	list := make([][]byte, 16)
	for i := range list {
		list[i] = []byte{byte(i)}
	}
	if charge, ok := legacyDelegationMembershipCharge(list, 16*97); !ok || charge != 16*97 {
		t.Fatal("exact membership charge rejected")
	}
	if charge, ok := legacyDelegationMembershipCharge(list, 16*97-1); ok || charge != 0 {
		t.Fatal("failed optional map was charged")
	}
	if _, ok := legacyDelegationMembershipCharge(list, -1); ok {
		t.Fatal("negative remaining budget accepted")
	}
	// True oversized base bytes remain bypassed without constructing maps.
	peer := bytes.Repeat([]byte{0x41}, 1024)
	count := legacyDelegationCacheBytes/(1024+58) + 1
	large := make([][]byte, count)
	for i := range large {
		large[i] = peer
	}
	entry := newLegacyDelegationEntry([]byte("oversized-base"), &corepb.DelegatedResourceAccountIndex{ToAccounts: large})
	if entry.charge <= legacyDelegationCacheBytes || entry.toSet != nil {
		t.Fatal("true oversized base admitted or built map")
	}
}

// The budget applies to the complete two-direction entry. Neither optional
// map may consume space needed by the other direction's owned pages.
func TestLegacyDelegationOptionalMembershipCombinedAndExactBase(t *testing.T) {
	list := make([][]byte, 100_000)
	for i := range list {
		list[i] = legacyBenchmarkAddress(uint64(i + 1))
	}
	e := newLegacyDelegationEntry([]byte("both"), &corepb.DelegatedResourceAccountIndex{FromAccounts: list, ToAccounts: list})
	if e.charge >= legacyDelegationCacheBytes || e.fromSet != nil || e.toSet != nil {
		t.Fatalf("combined base/map policy charge%d", e.charge)
	}
	c := &legacyDelegationCache{entries: make(map[string]*legacyDelegationEntry)}
	c.admit(e)
	if c.entries[e.key] != e {
		t.Fatal("combined base was bypassed")
	}
	// Empty rows isolate the actual fixed/key charge, including its exact bound.
	key := make([]byte, legacyDelegationCacheBytes-512)
	exact := newLegacyDelegationEntry(key, new(corepb.DelegatedResourceAccountIndex))
	if exact.charge != legacyDelegationCacheBytes {
		t.Fatalf("exact base charge%d", exact.charge)
	}
	c.admit(exact)
	if c.entries[exact.key] != exact || len(c.entries) != 1 {
		t.Fatal("exact fit/LRU admission failed")
	}
	over := newLegacyDelegationEntry(append(key, 1), new(corepb.DelegatedResourceAccountIndex))
	c.admit(over)
	if c.entries[over.key] != nil {
		t.Fatal("base one byte over budget admitted")
	}
}

func TestLegacyDelegationOptionalMembershipGrowthReloadAndRemoval(t *testing.T) {
	for _, degree := range []int{300_000, 400_000, 1_000_000} {
		t.Run(fmt.Sprint(degree), func(t *testing.T) {
			f := seedLegacyBenchmark(t, degree)
			s := f.open(t)
			reads := &delegationReadCounter{store: s.accountKVPhysicalLatestStore()}
			s.accountKVLatestReader = reads
			e, ok, err := s.loadLegacyDelegation(f.from)
			if err != nil || !ok {
				t.Fatal(err)
			}
			cache := s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation
			if degree == 1_000_000 && cache.entries[e.key] != nil {
				t.Fatal("genuine oversized row retained")
			}
			for i := 0; i < 4; i++ {
				peer := legacyBenchmarkAddress(uint64(degree + 200 + i))
				if err := s.WriteDrAccountIndexLegacyDelegate(f.from, peer); err != nil {
					t.Fatal(err)
				}
				before := reads.pageCalls
				wasResident := cache.entries[e.key] != nil
				current, ok, err := s.loadLegacyDelegation(f.from)
				if err != nil || !ok {
					t.Fatal(err)
				}
				resident := cache.entries[current.key] == current
				if wasResident && reads.pageCalls != before {
					t.Fatal("resident follow-up reloaded pages")
				}
				if !wasResident && reads.pageCalls == before {
					t.Fatal("bypassed row did not reload")
				}
				assertDelegationCacheBounds(t, cache)
				t.Logf("degree%d append%d charge%d resident%v pageGets%d", degree, i, current.charge, resident, reads.pageCalls-before)
			}
			// A removal must retain the conservative charge for byte arenas and header
			// allocations still owned by this entry, even when no map was constructed.
			e, _, err = s.loadLegacyDelegation(f.from)
			if err != nil {
				t.Fatal(err)
			}
			charge := e.charge
			if err := s.changeLegacyDelegation(e, f.existing, false, true); err != nil {
				t.Fatal(err)
			}
			if e.charge < charge {
				t.Fatal("removal forgave retained owned allocation")
			}
			got, ok, err := s.ReadDrAccountIndexLegacyStrict(f.from)
			if err != nil || !ok || len(got.ToAccounts) != degree+3 {
				t.Fatalf("growth/removal oracle %v/%v", ok, err)
			}
			for _, peer := range got.ToAccounts {
				if bytes.Equal(peer, f.existing) {
					t.Fatal("remove-all left existing peer")
				}
			}
		})
	}
}
