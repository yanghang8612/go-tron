package domains

import (
	"bytes"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestFlatStorePutGetDeleteRawDBLatest(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddress(0x31)
	alias := owner
	alias[0] = 0xa0
	store := NewFlatStore(db, 7)

	value := []byte("data")
	if err := store.DomainPut(owner, kvdomains.ContractStorage, []byte("slot"), value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'x'

	got, ok, err := rawdb.ReadStateKVLatest(db, alias, 7, kvdomains.ContractStorage, []byte("slot"))
	if err != nil || !ok || string(got) != "data" {
		t.Fatalf("rawdb latest = %q ok=%v err=%v, want data,true,nil", got, ok, err)
	}
	got, ok, err = store.GetLatest(alias, kvdomains.ContractStorage, []byte("slot"))
	if err != nil || !ok || string(got) != "data" {
		t.Fatalf("store latest = %q ok=%v err=%v, want data,true,nil", got, ok, err)
	}
	got[0] = 'x'
	got, ok, err = store.GetLatest(owner, kvdomains.ContractStorage, []byte("slot"))
	if err != nil || !ok || string(got) != "data" {
		t.Fatalf("store latest after caller mutation = %q ok=%v err=%v", got, ok, err)
	}

	if _, ok, err = NewFlatStore(db, 8).GetLatest(owner, kvdomains.ContractStorage, []byte("slot")); err != nil || ok {
		t.Fatalf("wrong generation visible: ok=%v err=%v", ok, err)
	}
	if err := store.DomainDel(owner, kvdomains.ContractStorage, []byte("slot")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = rawdb.ReadStateKVLatest(db, owner, 7, kvdomains.ContractStorage, []byte("slot")); err != nil || ok {
		t.Fatalf("rawdb latest after delete ok=%v err=%v", ok, err)
	}
}

func TestFlatStoreDeletePrefixScopesToOwnerDomainAndGeneration(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddress(0x32)
	other := testAddress(0x33)
	store := NewFlatStore(db, 2)
	nextGeneration := NewFlatStore(db, 3)

	mustFlatPut(t, store, owner, kvdomains.SystemMarket, "book/1", "one")
	mustFlatPut(t, store, owner, kvdomains.SystemMarket, "book/2", "two")
	mustFlatPut(t, store, owner, kvdomains.SystemMarket, "price/1", "price")
	mustFlatPut(t, store, owner, kvdomains.SystemReward, "book/reward", "reward")
	mustFlatPut(t, store, other, kvdomains.SystemMarket, "book/other", "other")
	mustFlatPut(t, nextGeneration, owner, kvdomains.SystemMarket, "book/new-generation", "new")

	if err := store.DomainDelPrefix(owner, kvdomains.SystemMarket, []byte("book/")); err != nil {
		t.Fatal(err)
	}

	assertFlatMissing(t, store, owner, kvdomains.SystemMarket, "book/1")
	assertFlatMissing(t, store, owner, kvdomains.SystemMarket, "book/2")
	assertFlatValue(t, store, owner, kvdomains.SystemMarket, "price/1", "price")
	assertFlatValue(t, store, owner, kvdomains.SystemReward, "book/reward", "reward")
	assertFlatValue(t, store, other, kvdomains.SystemMarket, "book/other", "other")
	assertFlatValue(t, nextGeneration, owner, kvdomains.SystemMarket, "book/new-generation", "new")
}

func TestFlatStoreIterateScopesSortsAndStops(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddress(0x34)
	other := testAddress(0x35)
	store := NewFlatStore(db, 4)
	wrongGeneration := NewFlatStore(db, 5)

	mustFlatPut(t, store, owner, kvdomains.SystemDelegation, "aa/2", "two")
	mustFlatPut(t, store, owner, kvdomains.SystemDelegation, "aa/1", "one")
	mustFlatPut(t, store, owner, kvdomains.SystemDelegation, "bb/1", "skip")
	mustFlatPut(t, store, owner, kvdomains.SystemReward, "aa/reward", "skip")
	mustFlatPut(t, store, other, kvdomains.SystemDelegation, "aa/other", "skip")
	mustFlatPut(t, wrongGeneration, owner, kvdomains.SystemDelegation, "aa/new-generation", "skip")

	var rows []string
	err := store.DomainIterate(owner, kvdomains.SystemDelegation, []byte("aa/"), func(key, value []byte) (bool, error) {
		rows = append(rows, string(key)+"="+string(value))
		key[0] = 'x'
		value[0] = 'x'
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"aa/1=one", "aa/2=two"}
	if !sameFlatRows(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	assertFlatValue(t, store, owner, kvdomains.SystemDelegation, "aa/1", "one")

	var stopped []string
	if err := store.DomainIterate(owner, kvdomains.SystemDelegation, []byte("aa/"), func(key, value []byte) (bool, error) {
		stopped = append(stopped, string(key)+"="+string(value))
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !sameFlatRows(stopped, []string{"aa/1=one"}) {
		t.Fatalf("stopped rows = %v", stopped)
	}
}

func TestFlatStoreGenerationResolver(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddress(0x36)
	wantErr := errors.New("generation unavailable")
	var calls int
	store := NewFlatStoreWithGenerationResolver(db, 99, func(got common.Address) (uint64, error) {
		calls++
		if got != owner {
			return 0, wantErr
		}
		return 11, nil
	})

	if err := store.DomainPut(owner, kvdomains.SystemReward, []byte("cycle"), []byte("reward")); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", calls)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(db, owner, 11, kvdomains.SystemReward, []byte("cycle")); err != nil || !ok {
		t.Fatalf("rawdb latest at resolved generation ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(db, owner, 99, kvdomains.SystemReward, []byte("cycle")); err != nil || ok {
		t.Fatalf("rawdb latest at default generation ok=%v err=%v", ok, err)
	}

	if err := store.DomainPut(testAddress(0x37), kvdomains.SystemReward, []byte("cycle"), []byte("reward")); !errors.Is(err, wantErr) {
		t.Fatalf("resolver error = %v, want %v", err, wantErr)
	}
}

func TestFlatStoreUsesTypedLatestKVStore(t *testing.T) {
	owner := testAddress(0x40)
	store := &FlatStore{
		latest:            newRecordingFlatLatestStore(),
		defaultGeneration: 5,
	}
	latest := store.latest.(*recordingFlatLatestStore)
	value := []byte("one")
	if err := store.DomainPut(owner, kvdomains.SystemReward, []byte("prefix/1"), value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'x'
	if err := store.DomainPut(owner, kvdomains.SystemReward, []byte("prefix/2"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := store.DomainPut(owner, kvdomains.SystemReward, []byte("keep"), []byte("keep")); err != nil {
		t.Fatal(err)
	}
	if latest.generations[len(latest.generations)-1] != 5 {
		t.Fatalf("latest store generation = %d, want 5", latest.generations[len(latest.generations)-1])
	}

	got, ok, err := store.GetLatest(owner, kvdomains.SystemReward, []byte("prefix/1"))
	if err != nil || !ok || string(got) != "one" {
		t.Fatalf("typed latest get = %q ok=%v err=%v, want one,true,nil", got, ok, err)
	}
	got[0] = 'x'
	got, ok, err = store.GetLatest(owner, kvdomains.SystemReward, []byte("prefix/1"))
	if err != nil || !ok || string(got) != "one" {
		t.Fatalf("typed latest cloned get = %q ok=%v err=%v, want one,true,nil", got, ok, err)
	}
	var rows []string
	if err := store.DomainIterate(owner, kvdomains.SystemReward, []byte("prefix/"), func(key, value []byte) (bool, error) {
		rows = append(rows, string(key)+"="+string(value))
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !sameFlatRows(rows, []string{"prefix/1=one", "prefix/2=two"}) {
		t.Fatalf("typed latest rows = %v", rows)
	}
	if err := store.DomainDelPrefix(owner, kvdomains.SystemReward, []byte("prefix/")); err != nil {
		t.Fatal(err)
	}
	assertFlatMissing(t, store, owner, kvdomains.SystemReward, "prefix/1")
	assertFlatMissing(t, store, owner, kvdomains.SystemReward, "prefix/2")
	assertFlatValue(t, store, owner, kvdomains.SystemReward, "keep", "keep")
}

func TestFlatStoreWithCommitmentTracksPutDeletePrefix(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddress(0x38)
	store := NewFlatStoreWithCommitment(db, 1)

	mustFlatPut(t, store, owner, kvdomains.SystemMarket, "book/1", "one")
	mustFlatPut(t, store, owner, kvdomains.SystemMarket, "book/2", "two")
	root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok || root == (common.Hash{}) {
		t.Fatalf("commitment root after puts = %x ok=%v err=%v", root, ok, err)
	}
	want, err := NewStagedCommitmentStore(db).Rebuild()
	if err != nil {
		t.Fatalf("rebuild after puts: %v", err)
	}
	if root != want {
		t.Fatalf("commitment root after puts = %x, rebuild = %x", root, want)
	}

	if err := store.DomainDelPrefix(owner, kvdomains.SystemMarket, []byte("book/")); err != nil {
		t.Fatal(err)
	}
	root, ok, err = rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok {
		t.Fatalf("commitment root after delete prefix ok=%v err=%v", ok, err)
	}
	want, err = NewStagedCommitmentStore(db).Rebuild()
	if err != nil {
		t.Fatalf("rebuild after delete prefix: %v", err)
	}
	if root != want {
		t.Fatalf("commitment root after delete prefix = %x, rebuild = %x", root, want)
	}
}

func TestApplyLatestCommitmentRestoresMissingRootFromNodes(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := testAddress(0x39)
	store := NewFlatStoreWithCommitment(db, 1)
	mustFlatPut(t, store, owner, kvdomains.SystemReward, "cycle", "value")
	root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok {
		t.Fatalf("commitment root after put ok=%v err=%v", ok, err)
	}
	if err := rawdb.DeleteStateCommitmentDomain(db, rawdb.LatestDomainCommitmentRootLogicalKey()); err != nil {
		t.Fatalf("delete root row: %v", err)
	}
	if _, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db); err != nil || ok {
		t.Fatalf("root before restore ok=%v err=%v", ok, err)
	}

	restored, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(db), nil)
	if err != nil {
		t.Fatalf("restore via commitment helper: %v", err)
	}
	if restored != root {
		t.Fatalf("restored root = %x, want %x", restored, root)
	}
	if stored, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db); err != nil || !ok || stored != root {
		t.Fatalf("stored restored root = %x ok=%v err=%v, want %x", stored, ok, err, root)
	}
}

func mustFlatPut(t *testing.T, store *FlatStore, owner common.Address, domain kvdomains.KVDomain, key, value string) {
	t.Helper()
	if err := store.DomainPut(owner, domain, []byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func assertFlatMissing(t *testing.T, store *FlatStore, owner common.Address, domain kvdomains.KVDomain, key string) {
	t.Helper()
	if got, ok, err := store.GetLatest(owner, domain, []byte(key)); err != nil || ok {
		t.Fatalf("%s = %q ok=%v err=%v, want missing", key, got, ok, err)
	}
}

func assertFlatValue(t *testing.T, store *FlatStore, owner common.Address, domain kvdomains.KVDomain, key, want string) {
	t.Helper()
	got, ok, err := store.GetLatest(owner, domain, []byte(key))
	if err != nil || !ok || string(got) != want {
		t.Fatalf("%s = %q ok=%v err=%v, want %q,true,nil", key, got, ok, err, want)
	}
}

func sameFlatRows(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal([]byte(a[i]), []byte(b[i])) {
			return false
		}
	}
	return true
}

type recordingFlatLatestStore struct {
	rows        map[string][]byte
	generations []uint64
}

func newRecordingFlatLatestStore() *recordingFlatLatestStore {
	return &recordingFlatLatestStore{rows: make(map[string][]byte)}
}

func (s *recordingFlatLatestStore) GetLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	s.generations = append(s.generations, generation)
	value, ok := s.rows[recordingFlatLatestKey(owner, generation, domain, key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *recordingFlatLatestStore) PutLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) error {
	s.generations = append(s.generations, generation)
	s.rows[recordingFlatLatestKey(owner, generation, domain, key)] = append([]byte(nil), value...)
	return nil
}

func (s *recordingFlatLatestStore) DeleteLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) error {
	s.generations = append(s.generations, generation)
	delete(s.rows, recordingFlatLatestKey(owner, generation, domain, key))
	return nil
}

func (s *recordingFlatLatestStore) DeleteLatestPrefix(owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) error {
	s.generations = append(s.generations, generation)
	for key := range s.rows {
		if recordingFlatLatestKeyMatches(key, owner, generation, domain, prefix) {
			delete(s.rows, key)
		}
	}
	return nil
}

func (s *recordingFlatLatestStore) IterateLatest(owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn IterateFunc) error {
	s.generations = append(s.generations, generation)
	keys := make([]string, 0, len(s.rows))
	for key := range s.rows {
		if recordingFlatLatestKeyMatches(key, owner, generation, domain, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		logicalKey := recordingFlatLatestLogicalKey(key)
		cont, err := fn([]byte(logicalKey), append([]byte(nil), s.rows[key]...))
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func recordingFlatLatestKey(owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) string {
	return owner.Hex() + "/" + strconv.FormatUint(generation, 10) + "/" + strconv.Itoa(int(domain)) + "/" + string(key)
}

func recordingFlatLatestKeyMatches(rowKey string, owner common.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte) bool {
	prefixKey := recordingFlatLatestKey(owner, generation, domain, prefix)
	return strings.HasPrefix(rowKey, prefixKey)
}

func recordingFlatLatestLogicalKey(rowKey string) string {
	parts := strings.SplitN(rowKey, "/", 4)
	if len(parts) != 4 {
		return ""
	}
	return parts[3]
}
