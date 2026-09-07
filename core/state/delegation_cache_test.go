package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Seed both directions above the membership threshold. These tests therefore
// exercise resident hash membership rather than only the small-list fallback.
func delegationCacheFixture(t *testing.T) (*StateDB, []byte, []byte) {
	t.Helper()
	s := newTestStateDB(t)
	fromAddr, toAddr := testAddr(0x11), testAddr(0x12)
	from, to := bytes.Clone(fromAddr[:]), bytes.Clone(toAddr[:])
	fill := func(start byte) [][]byte {
		out := make([][]byte, 24)
		for i := range out {
			addr := testAddr(start + byte(i))
			out[i] = bytes.Clone(addr[:])
		}
		return out
	}
	delegationCachePut(t, s, from, &corepb.DelegatedResourceAccountIndex{
		Account: bytes.Clone(from), FromAccounts: fill(80), ToAccounts: append(fill(120), bytes.Clone(to)),
	})
	delegationCachePut(t, s, to, &corepb.DelegatedResourceAccountIndex{
		Account: bytes.Clone(to), FromAccounts: append(fill(160), bytes.Clone(from)), ToAccounts: fill(200),
	})
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	delegationCacheWarm(t, s, from, to)
	return s, from, to
}

func delegationCacheEncode(t *testing.T, rec *corepb.DelegatedResourceAccountIndex) []byte {
	t.Helper()
	// The generic codec is the independent byte-format oracle.
	data, err := statecodec.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func delegationCachePut(t *testing.T, s *StateDB, account []byte, rec *corepb.DelegatedResourceAccountIndex) {
	t.Helper()
	if err := s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation,
		rawdb.DrAccountIndexLegacyStateKey(account), delegationCacheEncode(t, rec)); err != nil {
		t.Fatal(err)
	}
}

func delegationCacheRead(t *testing.T, s *StateDB, account []byte) *corepb.DelegatedResourceAccountIndex {
	t.Helper()
	rec, ok, err := s.ReadDrAccountIndexLegacyStrict(account)
	if err != nil || !ok {
		t.Fatalf("read legacy index %x: exists=%v err=%v", account, ok, err)
	}
	return rec
}

func delegationCacheAssertRow(t *testing.T, s *StateDB, account []byte, want *corepb.DelegatedResourceAccountIndex) {
	t.Helper()
	got, ok, err := s.GetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation,
		rawdb.DrAccountIndexLegacyStateKey(account))
	if err != nil || !ok || !bytes.Equal(got, delegationCacheEncode(t, want)) {
		t.Fatalf("legacy row %x differs from generic-codec oracle: exists=%v err=%v", account, ok, err)
	}
}

func delegationCacheWarm(t *testing.T, s *StateDB, from, to []byte) {
	t.Helper()
	if err := s.WriteDrAccountIndexLegacyDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	cache := s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation
	if cache == nil {
		t.Fatal("legacy cache was not populated")
	}
	for _, account := range [][]byte{from, to} {
		entry := cache.entries[string(rawdb.DrAccountIndexLegacyStateKey(account))]
		if entry == nil || entry.fromSet == nil || entry.toSet == nil {
			t.Fatalf("fixture %x did not exercise both membership indexes", account)
		}
	}
}

func TestLegacyDelegationCacheGenericMutations(t *testing.T) {
	for _, operation := range []string{"put", "put-final", "delete", "delete-prefix"} {
		t.Run(operation, func(t *testing.T) {
			s, from, to := delegationCacheFixture(t)
			want := delegationCacheRead(t, s, from)
			toWant := delegationCacheRead(t, s, to)
			key := rawdb.DrAccountIndexLegacyStateKey(from)
			var err error
			switch operation {
			case "put", "put-final":
				want.Timestamp = 777
				want.ToAccounts = want.ToAccounts[:len(want.ToAccounts)-1]
				data := delegationCacheEncode(t, want)
				if operation == "put" {
					err = s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, data)
				} else {
					err = s.SetAccountKVFinal(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, data)
				}
			case "delete":
				err = s.DeleteAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key)
				want = &corepb.DelegatedResourceAccountIndex{Account: bytes.Clone(from)}
			case "delete-prefix":
				err = s.DeleteAccountKVPrefix(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key)
				want = &corepb.DelegatedResourceAccountIndex{Account: bytes.Clone(from)}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := s.WriteDrAccountIndexLegacyDelegate(from, to); err != nil {
				t.Fatal(err)
			}
			want.ToAccounts = append(want.ToAccounts, bytes.Clone(to))
			delegationCacheAssertRow(t, s, from, want)
			delegationCacheAssertRow(t, s, to, toWant)
		})
	}
}

func TestLegacyDelegationCacheNestedRevert(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	originalFrom, originalTo := delegationCacheRead(t, s, from), delegationCacheRead(t, s, to)
	outer := s.Snapshot()
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	middleFrom, middleTo := delegationCacheRead(t, s, from), delegationCacheRead(t, s, to)
	inner := s.Snapshot()
	changed := proto.Clone(middleFrom).(*corepb.DelegatedResourceAccountIndex)
	changed.Timestamp = 999
	delegationCachePut(t, s, from, changed)
	if err := s.WriteDrAccountIndexLegacyDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	s.RevertToSnapshot(inner)
	// Re-reading through the mutator proves rollback cannot reuse its future
	// membership, even when the rollback restored an earlier dirty entry.
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	delegationCacheAssertRow(t, s, from, middleFrom)
	delegationCacheAssertRow(t, s, to, middleTo)
	s.RevertToSnapshot(outer)
	// This outer rollback removes dirty entries and exposes the durable rows.
	delegationCacheWarm(t, s, from, to)
	delegationCacheAssertRow(t, s, from, originalFrom)
	delegationCacheAssertRow(t, s, to, originalTo)
}

func TestLegacyDelegationCacheResetAndRollback(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	originalFrom, originalTo := delegationCacheRead(t, s, from), delegationCacheRead(t, s, to)
	snapshot := s.Snapshot()
	if err := s.ResetAccountKV(tcommon.SystemAccountAddress); err != nil {
		t.Fatal(err)
	}
	changedFrom := proto.Clone(originalFrom).(*corepb.DelegatedResourceAccountIndex)
	changedTo := proto.Clone(originalTo).(*corepb.DelegatedResourceAccountIndex)
	changedFrom.Timestamp, changedTo.Timestamp = 10, 20
	changedFrom.ToAccounts = changedFrom.ToAccounts[:len(changedFrom.ToAccounts)-1]
	delegationCachePut(t, s, from, changedFrom)
	delegationCachePut(t, s, to, changedTo)
	delegationCacheWarm(t, s, from, to)
	changedFrom.ToAccounts = append(changedFrom.ToAccounts, bytes.Clone(to))
	delegationCacheAssertRow(t, s, from, changedFrom)
	s.RevertToSnapshot(snapshot)
	delegationCacheWarm(t, s, from, to)
	delegationCacheAssertRow(t, s, from, originalFrom)
	delegationCacheAssertRow(t, s, to, originalTo)
}

func TestLegacyDelegationCacheSurvivesCommit(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	cache := s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation
	fromEntry := cache.entries[string(rawdb.DrAccountIndexLegacyStateKey(from))]
	toEntry := cache.entries[string(rawdb.DrAccountIndexLegacyStateKey(to))]
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != cache {
		t.Fatal("successful commit discarded resident delegation cache")
	}
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	if cache.entries[fromEntry.key] != fromEntry || cache.entries[toEntry.key] != toEntry {
		t.Fatal("unchanged cross-block reads did not reuse resident entries")
	}
	if len(s.stateObjects[tcommon.SystemAccountAddress].kvDirty) != 0 {
		t.Fatal("unchanged cross-block un-delegation created dirty rows")
	}
}

func TestLegacyDelegationCacheCopyIsolation(t *testing.T) {
	for _, executionCopy := range []bool{false, true} {
		t.Run(map[bool]string{false: "full", true: "execution"}[executionCopy], func(t *testing.T) {
			s, from, to := delegationCacheFixture(t)
			originalFrom, originalTo := delegationCacheRead(t, s, from), delegationCacheRead(t, s, to)
			var copied *StateDB
			var err error
			if executionCopy {
				copied, err = s.CopyBlockExecutionBase()
			} else {
				copied, err = s.Copy()
			}
			if err != nil {
				t.Fatal(err)
			}
			if obj := copied.stateObjects[tcommon.SystemAccountAddress]; obj != nil && obj.legacyDelegation != nil {
				t.Fatal("copy inherited parent's mutable cache")
			}
			delegationCacheWarm(t, copied, from, to)
			if err := copied.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
				t.Fatal(err)
			}
			delegationCacheWarm(t, s, from, to)
			delegationCacheAssertRow(t, s, from, originalFrom)
			delegationCacheAssertRow(t, s, to, originalTo)
			originalFrom.ToAccounts = originalFrom.ToAccounts[:len(originalFrom.ToAccounts)-1]
			originalTo.FromAccounts = originalTo.FromAccounts[:len(originalTo.FromAccounts)-1]
			delegationCacheAssertRow(t, copied, from, originalFrom)
			delegationCacheAssertRow(t, copied, to, originalTo)
		})
	}
}

func TestLegacyDelegationCacheReaderChanges(t *testing.T) {
	for _, change := range []string{"latest-reader", "index-store", "scope-detach"} {
		t.Run(change, func(t *testing.T) {
			s, from, to := delegationCacheFixture(t)
			fromRec, toRec := delegationCacheRead(t, s, from), delegationCacheRead(t, s, to)
			fromKey, toKey := rawdb.DrAccountIndexLegacyStateKey(from), rawdb.DrAccountIndexLegacyStateKey(to)
			fromRec.Timestamp = 123
			switch change {
			case "latest-reader":
				reader := &countingGenerationBatchReader{values: map[string][]byte{
					string(fromKey): delegationCacheEncode(t, fromRec), string(toKey): delegationCacheEncode(t, toRec),
				}}
				s.setAccountKVLatestView(reader, nil)
			case "index-store":
				store := ethrawdb.NewMemoryDatabase()
				t.Cleanup(func() { _ = store.Close() })
				for _, row := range []struct {
					key []byte
					rec *corepb.DelegatedResourceAccountIndex
				}{{fromKey, fromRec}, {toKey, toRec}} {
					if err := rawdb.WriteStateKVLatest(store, tcommon.SystemAccountAddress, 0, kvdomains.SystemDelegation, row.key, delegationCacheEncode(t, row.rec)); err != nil {
						t.Fatal(err)
					}
				}
				s.SetAccountKVIndexStore(store)
			case "scope-detach":
				scope := s.NewCommitScope()
				scope.latestWriter.rememberPut(tcommon.SystemAccountAddress, 0, kvdomains.SystemDelegation, fromKey, delegationCacheEncode(t, fromRec))
				delegationCacheWarm(t, s, from, to)
				scope.Discard()
				fromRec.Timestamp = 0 // The discarded pending image was never durable.
			}
			if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
				t.Fatal(err)
			}
			fromRec.ToAccounts = fromRec.ToAccounts[:len(fromRec.ToAccounts)-1]
			toRec.FromAccounts = toRec.FromAccounts[:len(toRec.FromAccounts)-1]
			delegationCacheAssertRow(t, s, from, fromRec)
			delegationCacheAssertRow(t, s, to, toRec)
		})
	}
}

func TestLegacyDelegationCacheStickyError(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	want := delegationCacheRead(t, s, from)
	sentinel := errors.New("storage failed before cached no-op")
	s.setError(sentinel)
	if err := s.WriteDrAccountIndexLegacyDelegate(from, to); !errors.Is(err, sentinel) {
		t.Fatalf("cached delegate ignored sticky error: %v", err)
	}
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); !errors.Is(err, sentinel) {
		t.Fatalf("cached un-delegate ignored sticky error: %v", err)
	}
	delegationCacheAssertRow(t, s, from, want)
}

func TestLegacyDelegationCacheSelfEdgeSemantics(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	want := delegationCacheRead(t, s, from)
	if err := s.WriteDrAccountIndexLegacyDelegate(from, from); err != nil {
		t.Fatal(err)
	}
	// Both old reads precede both writes: the second record gains FromAccounts
	// and overwrites the first record's addition to ToAccounts.
	want.FromAccounts = append(want.FromAccounts, bytes.Clone(from))
	delegationCacheAssertRow(t, s, from, want)
	delegationCacheWarm(t, s, from, to)
	want.ToAccounts = append(want.ToAccounts, bytes.Clone(from))
	delegationCachePut(t, s, from, want)
	delegationCacheWarm(t, s, from, to)
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, from); err != nil {
		t.Fatal(err)
	}
	want.FromAccounts = want.FromAccounts[:len(want.FromAccounts)-1]
	// The second independent write similarly restores the self ToAccounts edge.
	delegationCacheAssertRow(t, s, from, want)
}

func TestLegacyDelegationCachePublicReaderOwnership(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	wantFrom, wantTo := delegationCacheRead(t, s, from), delegationCacheRead(t, s, to)
	for _, strict := range []bool{false, true} {
		var rec *corepb.DelegatedResourceAccountIndex
		if strict {
			rec = delegationCacheRead(t, s, from)
		} else {
			rec = s.ReadDrAccountIndexLegacy(from)
		}
		rec.Account[0] ^= 0xff
		rec.ToAccounts[len(rec.ToAccounts)-1][0] ^= 0xff
		rec.FromAccounts[0] = []byte("caller mutation")
		rec.Timestamp = 987
	}
	delegationCacheWarm(t, s, from, to)
	delegationCacheAssertRow(t, s, from, wantFrom)
	delegationCacheAssertRow(t, s, to, wantTo)
}

func TestLegacyDelegationCacheHitRecordsLogicalReads(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	recorder := new(TransactionAccessRecorder)
	s.SetTransactionAccessRecorder(recorder)
	if err := s.WriteDrAccountIndexLegacyDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	reads := recorder.CaptureReadSet()
	want := map[TransactionAccessKey]bool{
		{Kind: TransactionAccessAccountField, Address: tcommon.SystemAccountAddress, AccountField: TransactionAccountFieldExistence}:                                                  false,
		{Kind: TransactionAccessAccountKVGeneration, Address: tcommon.SystemAccountAddress}:                                                                                           false,
		{Kind: TransactionAccessAccountKV, Address: tcommon.SystemAccountAddress, KVDomain: kvdomains.SystemDelegation, LogicalKey: string(rawdb.DrAccountIndexLegacyStateKey(from))}: false,
		{Kind: TransactionAccessAccountKV, Address: tcommon.SystemAccountAddress, KVDomain: kvdomains.SystemDelegation, LogicalKey: string(rawdb.DrAccountIndexLegacyStateKey(to))}:   false,
	}
	for _, read := range reads.Reads {
		if _, ok := want[read.Key]; ok && read.Mode&TransactionAccessRead != 0 {
			want[read.Key] = true
		}
	}
	for key, seen := range want {
		if !seen {
			t.Errorf("resident hit omitted logical read: %+v", key)
		}
	}
	if reads.Unsupported {
		t.Fatal("exact resident point reads became unsupported")
	}
}

func TestLegacyDelegationCacheVersionedReaderBypass(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	want := delegationCacheRead(t, s, from)
	want.Timestamp = 456
	key := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: tcommon.SystemAccountAddress,
		KVDomain: kvdomains.SystemDelegation, LogicalKey: string(rawdb.DrAccountIndexLegacyStateKey(from))}
	s.SetTransactionVersionedValueReader(testTransactionVersionedReader{
		key: {{txIndex: 1, value: TransactionWriteValue{Exists: true, Value: delegationCacheEncode(t, want)}}},
	}, 2)
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	want.ToAccounts = want.ToAccounts[:len(want.ToAccounts)-1]
	delegationCacheAssertRow(t, s, from, want)
}

func TestLegacyDelegationCacheSecondReadErrorPrecedesWrites(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	want := delegationCacheRead(t, s, from)
	want.ToAccounts = want.ToAccounts[:len(want.ToAccounts)-1]
	delegationCachePut(t, s, from, want)
	if err := s.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemDelegation,
		rawdb.DrAccountIndexLegacyStateKey(to), []byte("invalid native row")); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteDrAccountIndexLegacyDelegate(from, to); err == nil {
		t.Fatal("cached to-side hid corrupted replacement")
	}
	delegationCacheAssertRow(t, s, from, want)
}

func TestLegacyDelegationCacheMembershipThreshold(t *testing.T) {
	for _, incoming := range []bool{false, true} {
		t.Run(map[bool]string{false: "outgoing", true: "incoming"}[incoming], func(t *testing.T) {
			s := newTestStateDB(t)
			anchorAddr := testAddr(70)
			anchor := bytes.Clone(anchorAddr[:])
			want := &corepb.DelegatedResourceAccountIndex{Account: bytes.Clone(anchor)}
			peers := make([][]byte, legacyDelegationSetMinimum+1)
			for i := range peers {
				addr := testAddr(byte(i + 1))
				peers[i] = bytes.Clone(addr[:])
			}
			list := &want.ToAccounts
			if incoming {
				list = &want.FromAccounts
			}
			mutate := func(peer []byte, remove bool) {
				t.Helper()
				from, to := anchor, peer
				if incoming {
					from, to = peer, anchor
				}
				var err error
				if remove {
					err = s.WriteDrAccountIndexLegacyUnDelegate(from, to)
				} else {
					err = s.WriteDrAccountIndexLegacyDelegate(from, to)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			assertMembership := func(expectSet bool) {
				t.Helper()
				cache := s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation
				entry := cache.entries[string(rawdb.DrAccountIndexLegacyStateKey(anchor))]
				if entry == nil {
					t.Fatal("anchor is not resident")
				}
				set := entry.toSet
				if incoming {
					set = entry.fromSet
				}
				if (set != nil) != expectSet {
					t.Fatalf("degree %d: membership allocated=%v, want %v", len(*list), set != nil, expectSet)
				}
				delegationCacheAssertRow(t, s, anchor, want)
			}
			for _, peer := range peers[:legacyDelegationSetMinimum-1] {
				mutate(peer, false)
				*list = append(*list, bytes.Clone(peer))
			}
			assertMembership(false)
			beforeThreshold := proto.Clone(want).(*corepb.DelegatedResourceAccountIndex)
			snapshot := s.Snapshot()
			for _, peer := range peers[legacyDelegationSetMinimum-1:] {
				mutate(peer, false)
				*list = append(*list, bytes.Clone(peer))
				assertMembership(true)
			}
			beforeRepeat := s.DomainChangeJournalMark()
			mutate(peers[legacyDelegationSetMinimum-1], false)
			assertMembership(true)
			if s.DomainChangeJournalMark() != beforeRepeat {
				t.Fatal("duplicate after threshold appended state journal entries")
			}
			// Delete a middle entry and re-add it. It must move to the tail,
			// while every surviving peer keeps its original relative order.
			removed := peers[3]
			mutate(removed, true)
			*list = append((*list)[:3], (*list)[4:]...)
			assertMembership(true)
			mutate(removed, false)
			*list = append(*list, bytes.Clone(removed))
			assertMembership(true)
			s.RevertToSnapshot(snapshot)
			want = beforeThreshold
			if incoming {
				list = &want.FromAccounts
			} else {
				list = &want.ToAccounts
			}
			mutate(peers[0], false) // Rebuild from the authoritative reverted row.
			assertMembership(false)
			for _, peer := range peers[legacyDelegationSetMinimum-1:] {
				if rec, exists, err := s.ReadDrAccountIndexLegacyStrict(peer); err != nil || exists || rec != nil {
					t.Fatalf("rollback retained new counterparty: rec=%v exists=%v err=%v", rec, exists, err)
				}
			}
		})
	}
}

func assertDelegationCacheBounds(t *testing.T, cache *legacyDelegationCache) {
	t.Helper()
	if cache.bytes < 0 || cache.bytes > legacyDelegationCacheBytes || len(cache.entries) > legacyDelegationCacheRows {
		t.Fatalf("cache exceeded bounds: bytes=%d rows=%d", cache.bytes, len(cache.entries))
	}
	seen := make(map[string]bool, len(cache.entries))
	charge := 0
	var previous *legacyDelegationEntry
	for entry := cache.oldest; entry != nil; entry = entry.newer {
		if seen[entry.key] || cache.entries[entry.key] != entry || entry.older != previous {
			t.Fatal("LRU chain disagrees with resident entries")
		}
		seen[entry.key] = true
		charge += entry.charge
		previous = entry
	}
	if len(seen) != len(cache.entries) || previous != cache.newest || charge != cache.bytes {
		t.Fatalf("LRU/accounting mismatch: chain=%d map=%d charge=%d recorded=%d", len(seen), len(cache.entries), charge, cache.bytes)
	}
}

func TestLegacyDelegationCacheRowLimitAndLRUThroughAPI(t *testing.T) {
	s := newTestStateDB(t)
	address := func(index int) []byte {
		addr := make([]byte, tcommon.AddressLength)
		addr[0] = 0x41
		binary.BigEndian.PutUint64(addr[len(addr)-8:], uint64(index+1000))
		return addr
	}
	key := func(index int) string { return string(rawdb.DrAccountIndexLegacyStateKey(address(index))) }
	// Independent two-account delegations fill the actual resident-row bound
	// without building a quadratic history of ever-growing central rows.
	for i := 0; i < legacyDelegationCacheRows; i += 2 {
		if err := s.WriteDrAccountIndexLegacyDelegate(address(i), address(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	cache := s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation
	assertDelegationCacheBounds(t, cache)
	if len(cache.entries) != legacyDelegationCacheRows {
		t.Fatalf("resident rows=%d, want %d", len(cache.entries), legacyDelegationCacheRows)
	}
	// Refresh the oldest pair, then force exactly two evictions. Its former
	// successor must be evicted, while the freshly used pair remains resident.
	if err := s.WriteDrAccountIndexLegacyDelegate(address(0), address(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteDrAccountIndexLegacyDelegate(address(legacyDelegationCacheRows), address(legacyDelegationCacheRows+1)); err != nil {
		t.Fatal(err)
	}
	assertDelegationCacheBounds(t, cache)
	if cache.entries[key(0)] == nil || cache.entries[key(1)] == nil || cache.entries[key(2)] != nil || cache.entries[key(3)] != nil {
		t.Fatal("row-limit eviction did not respect recent API reads")
	}
	wantFrom, wantTo := delegationCacheRead(t, s, address(2)), delegationCacheRead(t, s, address(3))
	beforeReload := s.DomainChangeJournalMark()
	if err := s.WriteDrAccountIndexLegacyDelegate(address(2), address(3)); err != nil {
		t.Fatal(err)
	}
	assertDelegationCacheBounds(t, cache)
	if cache.entries[key(2)] == nil || cache.entries[key(3)] == nil {
		t.Fatal("evicted rows were not reloaded")
	}
	if s.DomainChangeJournalMark() != beforeReload {
		t.Fatal("reloading evicted dirty rows rewrote unchanged state")
	}
	delegationCacheAssertRow(t, s, address(2), wantFrom)
	delegationCacheAssertRow(t, s, address(3), wantTo)
}

func TestLegacyDelegationCacheByteBudgetAndOversizedBypass(t *testing.T) {
	cache := &legacyDelegationCache{entries: make(map[string]*legacyDelegationEntry)}
	// Synthetic charges exercise exact byte boundaries without allocating tens
	// of MiB merely to make an otherwise tiny LRU test reach the configured cap.
	a := &legacyDelegationEntry{key: "a", charge: legacyDelegationCacheBytes / 2}
	b := &legacyDelegationEntry{key: "b", charge: legacyDelegationCacheBytes / 2}
	c := &legacyDelegationEntry{key: "c", charge: legacyDelegationCacheBytes / 2}
	cache.admit(a)
	cache.admit(b)
	assertDelegationCacheBounds(t, cache)
	if cache.bytes != legacyDelegationCacheBytes {
		t.Fatal("an exact-fit entry was rejected")
	}
	cache.get(a.key)
	cache.admit(c)
	assertDelegationCacheBounds(t, cache)
	if cache.entries[a.key] != a || cache.entries[b.key] != nil || cache.entries[c.key] != c {
		t.Fatal("byte-bound eviction ignored LRU order")
	}
	oversized := &legacyDelegationEntry{key: "oversized", charge: legacyDelegationCacheBytes + 1}
	cache.admit(oversized)
	assertDelegationCacheBounds(t, cache)
	if len(cache.entries) != 2 || cache.entries[oversized.key] != nil {
		t.Fatal("oversized admission retained its entry or evicted unrelated residents")
	}
	// A same-key oversized replacement cannot leave the previous cached image
	// readable, even though the replacement itself is deliberately bypassed.
	cache.admit(&legacyDelegationEntry{key: a.key, charge: legacyDelegationCacheBytes + 1})
	assertDelegationCacheBounds(t, cache)
	if len(cache.entries) != 1 || cache.entries[a.key] != nil || cache.entries[c.key] != c {
		t.Fatal("oversized replacement left its old version resident")
	}

	// Repeated aliases keep the test allocation small; the entry constructor
	// must budget the complete decoded-row shape before allocating membership.
	const addressBytes = 1024
	peer := bytes.Repeat([]byte{0x41}, addressBytes)
	count := legacyDelegationCacheBytes/(2*addressBytes+120) + 1
	list := make([][]byte, count)
	for i := range list {
		list[i] = peer
	}
	entry := newLegacyDelegationEntry([]byte("large-native-row"), &corepb.DelegatedResourceAccountIndex{ToAccounts: list})
	if entry.charge <= legacyDelegationCacheBytes || entry.fromSet != nil || entry.toSet != nil {
		t.Fatal("oversized decoded shape allocated a membership table")
	}
	cache.admit(entry)
	assertDelegationCacheBounds(t, cache)
	if cache.entries[entry.key] != nil || cache.entries[c.key] != c {
		t.Fatal("oversized decoded row changed the resident working set")
	}
}

func TestLegacyDelegationCacheRepeatedIndexBindingAcrossCommit(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	store := s.db.DiskDB()
	s.SetAccountKVIndexStore(store)
	delegationCacheWarm(t, s, from, to)
	cache := s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation
	// This is the binding that BlockChain.prepareOpenState repeats on its
	// canonical range StateDB before every block.
	s.SetAccountKVIndexStore(store)
	if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != cache {
		t.Fatal("rebinding the same live store discarded resident entries")
	}
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	fromEntry := cache.entries[string(rawdb.DrAccountIndexLegacyStateKey(from))]
	toEntry := cache.entries[string(rawdb.DrAccountIndexLegacyStateKey(to))]
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	s.SetAccountKVIndexStore(store)
	if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != cache ||
		cache.entries[fromEntry.key] != fromEntry || cache.entries[toEntry.key] != toEntry {
		t.Fatal("next-block binding discarded committed resident images")
	}
	beforeNoop := s.DomainChangeJournalMark()
	if err := s.WriteDrAccountIndexLegacyUnDelegate(from, to); err != nil {
		t.Fatal(err)
	}
	if s.DomainChangeJournalMark() != beforeNoop || cache.entries[fromEntry.key] != fromEntry {
		t.Fatal("cross-block duplicate did not reuse its exact resident image")
	}
	// A new wrapper pointer is a different view even when its underlying store
	// happens to match. This preserves explicit source-switch invalidation.
	otherView := &delegationCacheStoreWrapper{KeyValueStore: store}
	s.SetAccountKVIndexStore(otherView)
	if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != nil {
		t.Fatal("a different store pointer retained the old resident view")
	}
	delegationCacheWarm(t, s, from, to)
	s.SetAccountKVIndexStore(nil)
	if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != nil {
		t.Fatal("switching to the default store retained the previous cache")
	}
}

type delegationCacheStoreWrapper struct {
	ethdb.KeyValueStore
	nonComparable []byte
}

func TestLegacyDelegationCacheNonComparableIndexStore(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	// A value wrapper with a slice implements the full store interface but
	// panics if compared directly as an interface. Such wrappers always clear
	// residency conservatively; correctness never depends on their equality.
	store := delegationCacheStoreWrapper{KeyValueStore: s.db.DiskDB(), nonComparable: []byte{1}}
	s.SetAccountKVIndexStore(store)
	delegationCacheWarm(t, s, from, to)
	s.SetAccountKVIndexStore(store)
	if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != nil {
		t.Fatal("value-wrapper rebinding did not invalidate its cached view")
	}
	delegationCacheWarm(t, s, from, to)
	store.nonComparable = []byte{2}
	s.SetAccountKVIndexStore(store)
	if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != nil {
		t.Fatal("changed non-comparable view did not invalidate residency")
	}
}

func TestLegacyDelegationCachePhysicalRebindWithinCommitScope(t *testing.T) {
	s, from, to := delegationCacheFixture(t)
	store := s.db.DiskDB()
	s.SetAccountKVIndexStore(store)
	scope := s.NewCommitScope()
	t.Cleanup(scope.Discard)
	delegationCacheWarm(t, s, from, to)
	cache := s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation
	for i := 0; i < 3; i++ {
		s.SetAccountKVIndexStore(store)
		delegationCacheWarm(t, s, from, to)
		if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != cache {
			t.Fatal("physical rebind discarded cache behind the unchanged scope reader")
		}
	}
	// A genuinely new latest reader remains an unconditional invalidation
	// boundary. The normal scope only binds once, at NewCommitScope.
	s.setAccountKVLatestView(nil, nil)
	if s.stateObjects[tcommon.SystemAccountAddress].legacyDelegation != nil {
		t.Fatal("latest-reader replacement retained the old scope cache")
	}
}
