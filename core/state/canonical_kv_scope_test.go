package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Preserve all production store capabilities while observing the two routes.
type canonicalKVStore struct {
	*pebbledb.Database
	live, cursors, points int
	sink                  ethdb.KeyValueStore
	fault                 error
}

func (s *canonicalKVStore) View(key []byte, fn func([]byte) error) error {
	s.live++
	return s.Database.View(key, fn)
}
func (s *canonicalKVStore) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	v, err := s.Database.NewKeyValueSnapshot()
	if err != nil {
		return nil, err
	}
	return &canonicalKVSnapshot{KeyValueSnapshot: v, store: s}, nil
}

type canonicalKVSnapshot struct {
	pointread.KeyValueSnapshot
	store *canonicalKVStore
}

func (s *canonicalKVSnapshot) NewPointReadCursor() (pointread.Cursor, error) {
	c, err := s.KeyValueSnapshot.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
	if err != nil {
		return nil, err
	}
	s.store.cursors++
	return &canonicalKVCursor{Cursor: c, store: s.store}, nil
}

type canonicalKVCursor struct {
	pointread.Cursor
	store *canonicalKVStore
}

func (c *canonicalKVCursor) View(k []byte, fn func([]byte) error) (bool, error) {
	c.store.points++
	if c.store.fault != nil {
		return false, c.store.fault
	}
	return c.Cursor.View(k, fn)
}

func newCanonicalKVFixture(t testing.TB, cache int) (*StateDB, *blockbuffer.Buffer, *CommitScope, *canonicalKVStore) {
	t.Helper()
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { disk.Close() })
	store := &canonicalKVStore{Database: disk.(*pebbledb.Database)}
	history, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { history.Close() })
	store.sink = rawdb.NewDirectHistoryStore(store, history)
	s, err := New(tcommon.Hash{}, NewDatabase(rawdb.WrapKeyValueStore(store.sink)))
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0xa7)
	s.CreateAccount(addr, corepb.AccountType_Normal)
	if _, err = s.Commit(); err != nil {
		t.Fatal(err)
	}
	buf := blockbuffer.New(store.sink)
	buf.SetBaseReadCacheSize(cache)
	s.SetAccountKVIndexStore(buf)
	scope := s.NewCommitScope()
	t.Cleanup(scope.Discard)
	t.Cleanup(func() {
		if s.storageReadScope != nil {
			s.storageReadScope.Close()
			s.storageReadScope = nil
			s.storageReadScopeReader = nil
		}
	})
	return s, buf, scope, store
}

var canonicalKVDomains = []kvdomains.KVDomain{kvdomains.SystemReward, kvdomains.SystemDelegation, kvdomains.ContractStorage, kvdomains.AccountAssetV2}

func TestCanonicalKVScopeRoutesAndOwnership(t *testing.T) {
	s, buf, commits, store := newCanonicalKVFixture(t, 0)
	addr := testAddr(0xa7)
	for d, domain := range canonicalKVDomains {
		for gen := uint64(0); gen < 2; gen++ {
			for _, size := range []int{1, 32, 600} {
				key := bytes.Repeat([]byte{byte(size)}, size)
				if err := rawdb.WriteStateKVLatest(store, addr, gen, domain, key, []byte{byte(d), byte(gen)}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := rawdb.WriteStateKVLatest(store, addr, 0, kvdomains.SystemReward, []byte("empty"), []byte{}); err != nil {
		t.Fatal(err)
	}
	buf.BeginBlock(tcommon.Hash{1}, 1)
	closeScope, err := s.BeginCanonicalStorageReadScope(1)
	if err != nil || s.storageReadScope == nil {
		t.Fatalf("open %v", err)
	}
	live := store.live
	points := store.points
	for d, domain := range canonicalKVDomains {
		for gen := uint64(0); gen < 2; gen++ {
			for _, size := range []int{1, 32, 600} {
				key := bytes.Repeat([]byte{byte(size)}, size)
				want := []byte{byte(d), byte(gen)}
				got, ok, err := s.readAccountKVLatest(addr, gen, domain, key)
				if err != nil || !ok || !bytes.Equal(got, want) {
					t.Fatalf("own %d/%d/%d: %x %v %v", domain, gen, size, got, ok, err)
				}
				got[0] = 99
				got, ok, err = s.readAccountKVLatestForDecoding(addr, gen, domain, key)
				if err != nil || !ok || !bytes.Equal(got, want) {
					t.Fatalf("borrow %x %v %v", got, ok, err)
				}
			}
		}
	}
	if store.live != live || store.points-points != 24 || store.cursors != 1 {
		t.Fatalf("route live=%d points=%d cursors=%d", store.live-live, store.points-points, store.cursors)
	}
	if got, ok, err := s.readAccountKVLatest(addr, 0, kvdomains.SystemReward, []byte("empty")); err != nil || !ok || got != nil {
		t.Fatalf("empty shape %v/%v/%v", got, ok, err)
	}
	if _, ok, err := s.readAccountKVLatestForDecoding(addr, 0, kvdomains.SystemReward, []byte("missing")); err != nil || ok {
		t.Fatalf("miss %v/%v", ok, err)
	}
	// Pending preimages, unlike dirty entries, precede the fixed physical cut.
	key := []byte("pending")
	commits.latestWriter.pending = make(map[accountKVLatestPendingMapKey]accountKVLatestPending)
	commits.latestWriter.pending[accountKVLatestPendingLookupKey(addr, 0, kvdomains.SystemReward, key)] = accountKVLatestPending{value: []byte("new")}
	if got, ok, err := s.readAccountKVLatest(addr, 0, kvdomains.SystemReward, key); err != nil || !ok || string(got) != "new" {
		t.Fatalf("pending %q/%v/%v", got, ok, err)
	}
	commits.latestWriter.pending[accountKVLatestPendingLookupKey(addr, 0, kvdomains.SystemReward, key)] = accountKVLatestPending{deleted: true}
	if _, ok, err := s.readAccountKVLatest(addr, 0, kvdomains.SystemReward, key); err != nil || ok {
		t.Fatalf("pending delete %v/%v", ok, err)
	}
	failure := errors.New("cursor failure")
	store.fault = failure
	if _, _, err := s.getAccountKVForDecoding(addr, kvdomains.SystemReward, []byte("fault")); !errors.Is(err, failure) || !errors.Is(s.Error(), failure) {
		t.Fatalf("failure not sticky: %v/%v", err, s.Error())
	}
	if err := closeScope(); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalKVScopeLifecycleDifferential(t *testing.T) {
	var roots []tcommon.Hash
	var persisted [][]byte
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			s, buf, commits, store := newCanonicalKVFixture(t, 1<<20)
			addr := testAddr(0xa7)
			keys := [][]byte{[]byte("present"), []byte("empty"), []byte("absent")}
			for _, domain := range canonicalKVDomains {
				if err := rawdb.WriteStateKVLatest(store, addr, 0, domain, keys[0], []byte{3}); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateKVLatest(store, addr, 0, domain, keys[1], []byte{}); err != nil {
					t.Fatal(err)
				}
			}
			for block := uint64(1); block <= 8; block++ {
				buf.BeginBlock(tcommon.Hash{31: byte(block)}, block)
				closeScope := func() error { return nil }
				var err error
				if enabled {
					closeScope, err = s.BeginCanonicalStorageReadScope(block)
					if err != nil || s.storageReadScope == nil {
						t.Fatalf("open %v", err)
					}
				}
				if block > 1 {
					if err := buf.FlushUpTo(block-1, store.sink); err != nil {
						t.Fatal(err)
					}
					commits.latestWriter.prunePending(block - 1)
				}
				for _, domain := range canonicalKVDomains {
					before, exists, err := s.GetAccountKV(addr, domain, keys[0])
					if err != nil || !exists {
						t.Fatalf("before %v/%v", exists, err)
					}
					rev := s.Snapshot()
					if err := s.SetAccountKV(addr, domain, keys[0], []byte{byte(block), 9}); err != nil {
						t.Fatal(err)
					}
					entry, _ := lookupKVEntry(s.stateObjects[addr].kvDirty, domain, keys[0])
					if !entry.prevLoaded || !entry.prevExists || !bytes.Equal(entry.prev, before) {
						t.Fatalf("preimage %x != %x", entry.prev, before)
					}
					// A direct preimage read must never substitute the new dirty value.
					if got, ok, err := s.readAccountKVLatest(addr, 0, domain, keys[0]); err != nil || !ok || !bytes.Equal(got, before) {
						t.Fatalf("preimage route %x/%v/%v", got, ok, err)
					}
					s.RevertToSnapshot(rev)
					if got, ok, err := s.GetAccountKV(addr, domain, keys[0]); err != nil || !ok || !bytes.Equal(got, before) {
						t.Fatalf("revert %x/%v/%v", got, ok, err)
					}
					rev = s.Snapshot()
					if err := s.ResetAccountKV(addr); err != nil {
						t.Fatal(err)
					}
					if _, ok, err := s.GetAccountKV(addr, domain, keys[0]); err != nil || ok {
						t.Fatalf("reset %v/%v", ok, err)
					}
					s.RevertToSnapshot(rev)
					rev = s.Snapshot()
					s.DeleteAccount(addr)
					s.CreateAccount(addr, corepb.AccountType_Normal)
					if _, ok, err := s.GetAccountKV(addr, domain, keys[0]); err != nil || ok {
						t.Fatalf("recreate %v/%v", ok, err)
					}
					s.RevertToSnapshot(rev)
					if err := s.SetAccountKV(addr, domain, keys[0], []byte{byte(block)}); err != nil {
						t.Fatal(err)
					}
					if block%2 == 0 {
						if err := s.DeleteAccountKV(addr, domain, keys[1]); err != nil {
							t.Fatal(err)
						}
					} else {
						if err := s.SetAccountKV(addr, domain, keys[1], []byte{}); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := closeScope(); err != nil {
					t.Fatal(err)
				}
				root, _, err := s.CommitWithStatsOptionsInScope(commits, CommitOptions{BlockNumber: block})
				if err != nil {
					t.Fatal(err)
				}
				if enabled {
					if root != roots[block-1] {
						t.Fatalf("root block %d %x != %x", block, root, roots[block-1])
					}
				} else {
					roots = append(roots, root)
				}
				if err := commits.FlushLatest(); err != nil {
					t.Fatal(err)
				}
				buf.CommitBlock()
			}
			if err := buf.Flush(store.sink); err != nil {
				t.Fatal(err)
			}
			reopened, err := New(roots[len(roots)-1], s.db)
			if err != nil {
				t.Fatal(err)
			}
			var rows [][]byte
			for _, domain := range canonicalKVDomains {
				for _, key := range keys {
					v, ok, err := reopened.GetAccountKV(addr, domain, key)
					if err != nil {
						t.Fatal(err)
					}
					rows = append(rows, append([]byte{byte(domain), byte(map[bool]int{true: 1}[ok])}, v...))
				}
			}
			if enabled {
				if !reflect.DeepEqual(rows, persisted) {
					t.Fatalf("reopened rows differ %x/%x", rows, persisted)
				}
			} else {
				persisted = rows
			}
		})
	}
}

// Every operation performs one canonical block's 256 fixed point reads, with
// equal setup/scope lifetime in parent and candidate. No data changes while timed.
func BenchmarkCanonicalKVPointScope(b *testing.B) {
	for _, shape := range []string{"durable", "resident", "pending", "legacy"} {
		b.Run(shape, func(b *testing.B) {
			cache := 0
			if shape == "resident" {
				cache = 1 << 20
			}
			s, buf, commits, store := newCanonicalKVFixture(b, cache)
			addr := testAddr(0xa7)
			var keys [256][32]byte
			for i := range keys {
				binary.BigEndian.PutUint64(keys[i][24:], uint64(i))
				if i%4 != 0 {
					if err := rawdb.WriteStateKVLatest(store, addr, 0, canonicalKVDomains[i%len(canonicalKVDomains)], keys[i][:], bytes.Repeat([]byte{byte(i)}, 32)); err != nil {
						b.Fatal(err)
					}
				}
			}
			if err := store.Compact(nil, nil); err != nil {
				b.Fatal(err)
			}
			if shape == "pending" {
				commits.latestWriter.pending = make(map[accountKVLatestPendingMapKey]accountKVLatestPending)
				for i := range keys {
					commits.latestWriter.pending[accountKVLatestPendingLookupKey(addr, 0, canonicalKVDomains[i%len(canonicalKVDomains)], keys[i][:])] = accountKVLatestPending{value: []byte{byte(i)}}
				}
			}
			if shape == "resident" {
				for n := 0; n < 3; n++ {
					for i := range keys {
						if _, _, err := s.readAccountKVLatestForDecoding(addr, 0, canonicalKVDomains[i%len(canonicalKVDomains)], keys[i][:]); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			store.live, store.cursors, store.points = 0, 0, 0
			b.ReportAllocs()
			b.ResetTimer()
			for block := 0; block < b.N; block++ {
				number := uint64(block + 1)
				buf.BeginBlock(tcommon.Hash{31: byte(number)}, number)
				closeScope := func() error { return nil }
				var err error
				if shape != "legacy" {
					closeScope, err = s.BeginCanonicalStorageReadScope(number)
					if err != nil || s.storageReadScope == nil {
						b.Fatalf("open %v", err)
					}
				}
				for i := range keys {
					var err error
					if i%2 == 0 {
						_, _, err = s.readAccountKVLatest(addr, 0, canonicalKVDomains[i%len(canonicalKVDomains)], keys[i][:])
					} else {
						_, _, err = s.readAccountKVLatestForDecoding(addr, 0, canonicalKVDomains[i%len(canonicalKVDomains)], keys[i][:])
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := closeScope(); err != nil {
					b.Fatal(err)
				}
				buf.DiscardActive()
			}
			b.StopTimer()
			b.ReportMetric(float64(store.live)/float64(b.N), "live-reads/op")
			b.ReportMetric(float64(store.points)/float64(b.N), "cursor-reads/op")
			b.ReportMetric(float64(store.cursors)/float64(b.N), "cursors/op")
		})
	}
}

func TestCanonicalKVScopeReaderAndPendingAccounting(t *testing.T) {
	s, buf, commits, _ := newCanonicalKVFixture(t, 0)
	addr := testAddr(0xa7)
	key := []byte("row")
	buf.BeginBlock(tcommon.Hash{1}, 1)
	closeScope, err := s.BeginCanonicalStorageReadScope(1)
	if err != nil || s.storageReadScope == nil {
		t.Fatalf("open %v", err)
	}
	defer closeScope()
	var resolutions int64
	commits.latestWriter.storagePendingResolved = &resolutions
	commits.latestWriter.pending = make(map[accountKVLatestPendingMapKey]accountKVLatestPending)
	for _, domain := range []kvdomains.KVDomain{kvdomains.ContractStorage, kvdomains.SystemReward} {
		commits.latestWriter.pending[accountKVLatestPendingLookupKey(addr, 0, domain, key)] = accountKVLatestPending{value: []byte("pending")}
		if _, _, err := s.readAccountKVLatest(addr, 0, domain, key); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.readAccountKVLatestForDecoding(addr, 0, domain, key); err != nil {
			t.Fatal(err)
		}
	}
	if resolutions != 1 {
		t.Fatalf("pending SLOAD accounting %d", resolutions)
	}
	s.accountKVLatestReader = &commitScopeLatestReader{state: s, writer: commits.latestWriter}
	for _, domain := range []kvdomains.KVDomain{kvdomains.ContractStorage, kvdomains.SystemReward} {
		if _, _, err := s.readAccountKVLatestForDecoding(addr, 0, domain, key); err == nil {
			t.Fatal("borrowed accepted replaced reader")
		}
	}
}

func TestCanonicalPermissionObjectReuseReadSet(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(0xa9)
	s.CreateAccount(addr, corepb.AccountType_Normal)
	first := splitTestPermission(corepb.Permission_Owner, 0, "first", 1)
	second := splitTestPermission(corepb.Permission_Owner, 0, "second", 2)
	s.SetPermissions(addr, first, nil, nil)
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	check := func(want string) {
		t.Helper()
		var r TransactionAccessRecorder
		r.Reset(8)
		s.SetTransactionAccessRecorder(&r)
		permission, err := s.AccountPermissionByID(addr, 0)
		s.SetTransactionAccessRecorder(nil)
		if err != nil || permission.GetPermissionName() != want {
			t.Fatalf("permission %v/%v", permission, err)
		}
		reads := r.CaptureReadSet()
		expected := map[TransactionAccessKey]bool{{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldExistence}: true, {Kind: TransactionAccessAccountKVGeneration, Address: addr}: true, {Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.AccountPermissionAux, LogicalKey: string(accountOwnerPermissionKey)}: true}
		if reads.Unsupported || len(reads.Reads) != len(expected) {
			t.Fatalf("read set %+v", reads)
		}
		for _, read := range reads.Reads {
			if !expected[read.Key] || read.Mode != TransactionAccessRead {
				t.Fatalf("unexpected read %+v", read)
			}
		}
	}
	check("first")
	check("first")
	rev := s.Snapshot()
	s.SetPermissions(addr, second, nil, nil)
	check("second")
	s.RevertToSnapshot(rev)
	check("first")
}

func TestCanonicalKVScopePendingLatePublication(t *testing.T) {
	for _, cache := range []int{0, 1 << 20} {
		t.Run(fmt.Sprint(cache), func(t *testing.T) {
			s, buf, commits, store := newCanonicalKVFixture(t, cache)
			addr := testAddr(0xa7)
			for _, domain := range canonicalKVDomains {
				for _, key := range []string{"put", "delete"} {
					if err := rawdb.WriteStateKVLatest(store, addr, 0, domain, []byte(key), []byte("old")); err != nil {
						t.Fatal(err)
					}
				}
			}
			buf.BeginBlock(tcommon.Hash{1}, 1)
			for _, domain := range canonicalKVDomains {
				if err := s.SetAccountKV(addr, domain, []byte("put"), []byte("new")); err != nil {
					t.Fatal(err)
				}
				if err := s.DeleteAccountKV(addr, domain, []byte("delete")); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := s.CommitWithStatsOptionsInScope(commits, CommitOptions{BlockNumber: 1, FlushLatestDomain: func() error { return nil }}); err != nil {
				t.Fatal(err)
			}
			if len(commits.latestWriter.pending) < 8 {
				t.Fatalf("pending fixture not staged: %d", len(commits.latestWriter.pending))
			}
			// Publish the block before applying its queued batch, as deep async does.
			buf.CommitBlock()
			buf.BeginBlock(tcommon.Hash{2}, 2)
			closeScope, err := s.BeginCanonicalStorageReadScope(2)
			if err != nil || s.storageReadScope == nil {
				t.Fatalf("open %v", err)
			}
			defer closeScope()
			check := func() {
				t.Helper()
				for _, domain := range canonicalKVDomains {
					for _, borrow := range []bool{false, true} {
						read := s.readAccountKVLatest
						if borrow {
							read = s.readAccountKVLatestForDecoding
						}
						if v, ok, err := read(addr, 0, domain, []byte("put")); err != nil || !ok || string(v) != "new" {
							t.Fatalf("put %d/%v %q/%v/%v", domain, borrow, v, ok, err)
						}
						if _, ok, err := read(addr, 0, domain, []byte("delete")); err != nil || ok {
							t.Fatalf("delete %d/%v %v/%v", domain, borrow, ok, err)
						}
					}
				}
			}
			check()
			if err := commits.FlushLatest(); err != nil {
				t.Fatal(err)
			}
			if err := buf.FlushUpTo(1, store.sink); err != nil {
				t.Fatal(err)
			}
			commits.latestWriter.prunePending(1)
			for _, domain := range canonicalKVDomains {
				for _, key := range []string{"put", "delete"} {
					if _, ok := commits.latestWriter.pending[accountKVLatestPendingLookupKey(addr, 0, domain, []byte(key))]; ok {
						t.Fatal("pending survived prune")
					}
				}
			}
			check()
		})
	}
}

// This complete block trace opens the same scope in both revisions, reads four
// domains and a valid permission, writes/reverts, closes, commits and flushes.
func BenchmarkCanonicalKVScopeLifecycle(b *testing.B) {
	s, buf, commits, store := newCanonicalKVFixture(b, 1<<20)
	addr := testAddr(0xa7)
	var keys [64][32]byte
	for i := range keys {
		binary.BigEndian.PutUint64(keys[i][24:], uint64(i))
		if err := rawdb.WriteStateKVLatest(store, addr, 0, canonicalKVDomains[i%4], keys[i][:], []byte{byte(i + 1)}); err != nil {
			b.Fatal(err)
		}
	}
	permission := splitTestPermission(corepb.Permission_Owner, 0, "owner", 1)
	encoded, err := appendAccountPermission(nil, permission)
	if err != nil {
		b.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(store, addr, 0, kvdomains.AccountPermissionAux, accountOwnerPermissionKey, encoded); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		number := uint64(i + 1)
		buf.BeginBlock(tcommon.Hash{31: byte(number)}, number)
		closeScope, err := s.BeginCanonicalStorageReadScope(number)
		if err != nil || s.storageReadScope == nil {
			b.Fatalf("open %v", err)
		}
		for j := range keys {
			if _, ok, err := s.GetAccountKV(addr, canonicalKVDomains[j%4], keys[j][:]); err != nil || !ok {
				b.Fatalf("row %d %v/%v", j, ok, err)
			}
		}
		s.stateObjects[addr].accountPermissionPointLoaded = false
		if p, err := s.AccountPermissionByID(addr, 0); err != nil || p.GetPermissionName() != "owner" {
			b.Fatalf("permission %v/%v", p, err)
		}
		rev := s.Snapshot()
		if err := s.SetAccountKV(addr, canonicalKVDomains[0], keys[0][:], []byte("reverted")); err != nil {
			b.Fatal(err)
		}
		s.RevertToSnapshot(rev)
		for j := 0; j < 4; j++ {
			if err := s.SetAccountKV(addr, canonicalKVDomains[j], keys[j][:], []byte{byte(i % 251), byte(j)}); err != nil {
				b.Fatal(err)
			}
		}
		s.AddBalance(addr, 1)
		if err := closeScope(); err != nil {
			b.Fatal(err)
		}
		if _, _, err := s.CommitWithStatsOptionsInScope(commits, CommitOptions{BlockNumber: number}); err != nil {
			b.Fatal(err)
		}
		if err := commits.FlushLatest(); err != nil {
			b.Fatal(err)
		}
		buf.CommitBlock()
		if err := buf.Flush(store.sink); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if got := s.GetBalance(addr); got != int64(b.N) {
		b.Fatalf("balance %d", got)
	}
}
