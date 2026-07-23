package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var benchmarkDomainCommitmentTouchCount int

func BenchmarkDomainCommitmentRecordTouches(b *testing.B) {
	const count = 1024
	owner := testAddr(0x7e)
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = make([]byte, 32)
		binary.BigEndian.PutUint64(keys[i][24:], uint64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		commitment := NewDomainCommitmentState(&StateDB{})
		for _, key := range keys {
			commitment.recordKVLatestTouch(owner, 7, kvdomains.ContractStorage, key)
		}
		benchmarkDomainCommitmentTouchCount = len(commitment.touches)
	}
}

func BenchmarkDomainCommitmentRecordRepeatedTouch(b *testing.B) {
	owner := testAddr(0x7e)
	key := make([]byte, 32)
	commitment := NewDomainCommitmentState(&StateDB{})
	commitment.recordKVLatestTouch(owner, 7, kvdomains.ContractStorage, key)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		commitment.recordKVLatestTouch(owner, 7, kvdomains.ContractStorage, key)
	}
}

func TestDomainCommitmentTouchesPreserveRootedIdentityAndCallerKey(t *testing.T) {
	commitment := NewDomainCommitmentState(&StateDB{})
	owner := testAddr(0x7e)
	alias := owner
	alias[0] = 0xa0
	key := []byte("slot/original")
	original := append([]byte(nil), key...)

	commitment.recordKVLatestTouch(owner, 7, kvdomains.ContractStorage, key)
	key[0] = 'X'
	commitment.recordKVLatestTouch(alias, 7, kvdomains.ContractStorage, original)
	if len(commitment.touches) != 1 {
		t.Fatalf("AccountID alias duplicated original touch: got %d touches", len(commitment.touches))
	}
	commitment.recordKVLatestTouch(owner, 7, kvdomains.ContractStorage, key)
	if len(commitment.touches) != 2 {
		t.Fatalf("caller key mutation changed retained touch: got %d touches, want 2 distinct keys", len(commitment.touches))
	}

	commitment.recordAccountLatestTouch(owner)
	commitment.recordAccountLatestTouch(alias)
	commitment.recordKVGenerationTouch(owner)
	commitment.recordKVGenerationTouch(alias)
	if len(commitment.touches) != 4 {
		t.Fatalf("AccountID alias duplicated account/generation touches: got %d, want 4 total", len(commitment.touches))
	}
}

func TestDomainStateAdaptsAccountKV(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x77)
	dom := sdb.Domains()

	if err := dom.DomainPut(owner, kvdomains.ContractABI, []byte("abi"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := sdb.GetAccountKV(owner, kvdomains.ContractABI, []byte("abi"))
	if err != nil || !ok || string(got) != "data" {
		t.Fatalf("StateDB account KV = %q ok=%v err=%v", got, ok, err)
	}

	if err := dom.DomainDel(owner, kvdomains.ContractABI, []byte("abi")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = dom.GetLatest(owner, kvdomains.ContractABI, []byte("abi")); err != nil || ok {
		t.Fatalf("domain delete still visible: ok=%v err=%v", ok, err)
	}
}

func TestDomainOverlayFlushesToStateDBAdapter(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x78)
	if err := sdb.SetAccountKV(owner, kvdomains.SystemDelegation, []byte("parent"), []byte("p")); err != nil {
		t.Fatal(err)
	}
	overlay := statedomains.NewOverlay(sdb.Domains())

	got, ok, err := overlay.GetLatest(owner, kvdomains.SystemDelegation, []byte("parent"))
	if err != nil || !ok || string(got) != "p" {
		t.Fatalf("overlay parent read-through = %q ok=%v err=%v", got, ok, err)
	}
	if err := overlay.DomainPut(owner, kvdomains.SystemDelegation, []byte("child"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if err := overlay.FlushTo(sdb.Domains()); err != nil {
		t.Fatal(err)
	}

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err = reopened.GetAccountKV(owner, kvdomains.SystemDelegation, []byte("child"))
	if err != nil || !ok || string(got) != "c" {
		t.Fatalf("flushed domain value = %q ok=%v err=%v", got, ok, err)
	}
}

func TestDomainStatePrefixDeleteUsesLatestIndex(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x79)
	dom := sdb.Domains()
	if err := dom.DomainPut(owner, kvdomains.SystemDelegation, []byte("prefix/1"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := dom.DomainPut(owner, kvdomains.SystemDelegation, []byte("prefix/2"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := dom.DomainPut(owner, kvdomains.SystemDelegation, []byte("other"), []byte("keep")); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Domains().DomainDelPrefix(owner, kvdomains.SystemDelegation, []byte("prefix/")); err != nil {
		t.Fatal(err)
	}
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := reopened.Domains().GetLatest(owner, kvdomains.SystemDelegation, []byte("prefix/1")); err != nil || ok {
		t.Fatalf("prefix/1 visible after prefix delete: ok=%v err=%v", ok, err)
	}
	if got, ok, err := reopened.Domains().GetLatest(owner, kvdomains.SystemDelegation, []byte("other")); err != nil || !ok || string(got) != "keep" {
		t.Fatalf("other = %q ok=%v err=%v", got, ok, err)
	}
}

func TestStateDBTemporalDomainsReadHistoryCommitmentAndFlush(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x7a)
	domain := kvdomains.SystemReward
	key := []byte("temporal/reward")
	disk := sdb.db.DiskDB()

	if err := sdb.SetAccountKV(owner, domain, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	begin1, end1, err := rawdb.NextStateTxRange(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sdb.SetDomainChangeSetWriterRange(disk, 1, tcommon.Hash{0x01}, begin1, end1)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit block 1: %v", err)
	}

	sdb, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(owner, domain, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	begin2, end2, err := rawdb.NextStateTxRange(end1, 0)
	if err != nil {
		t.Fatal(err)
	}
	sdb.SetDomainChangeSetWriterRange(disk, 2, tcommon.Hash{0x02}, begin2, end2)
	root, err = sdb.Commit()
	if err != nil {
		t.Fatalf("commit block 2: %v", err)
	}

	head, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	tx := head.TemporalDomains(end2)
	got, ok, err := tx.GetLatest(owner, domain, key)
	if err != nil || !ok || string(got) != "v2" {
		t.Fatalf("latest = %q ok=%v err=%v", got, ok, err)
	}
	got, ok, err = tx.GetAsOf(owner, domain, key, end1)
	if err != nil || !ok || string(got) != "v1" {
		t.Fatalf("as-of block 1 = %q ok=%v err=%v", got, ok, err)
	}
	txNum, blockNum, err := tx.SeekCommitment(context.Background())
	if err != nil {
		t.Fatalf("seek commitment: %v", err)
	}
	if txNum != 0 || blockNum != 0 {
		t.Fatalf("unexpected checkpoint tx=%d block=%d", txNum, blockNum)
	}
	computedRoot, err := tx.ComputeCommitment(context.Background(), 2, end2)
	if err != nil {
		t.Fatalf("compute commitment: %v", err)
	}
	if computedRoot != root {
		t.Fatalf("computed root = %x, want committed root %x", computedRoot, root)
	}

	if err := tx.DomainPut(owner, domain, key, []byte("v3")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	root, err = head.Commit()
	if err != nil {
		t.Fatalf("commit flushed temporal write: %v", err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err = reopened.GetAccountKV(owner, domain, key)
	if err != nil || !ok || string(got) != "v3" {
		t.Fatalf("flushed temporal latest = %q ok=%v err=%v", got, ok, err)
	}
}

func TestStateDBCommitRepairsMissingCommitmentRootFromNodes(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x7b)
	domain := kvdomains.SystemReward
	key := []byte("repair/root")
	disk := sdb.db.DiskDB()

	if err := sdb.SetAccountKV(owner, domain, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if err := rawdb.DeleteStateCommitmentDomain(disk, rawdb.LatestDomainCommitmentRootLogicalKey()); err != nil {
		t.Fatalf("delete commitment root row: %v", err)
	}
	if _, ok, err := rawdb.ReadLatestDomainCommitmentRoot(disk); err != nil || ok {
		t.Fatalf("root before repair ok=%v err=%v", ok, err)
	}

	head, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := head.SetAccountKV(owner, domain, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	root2, err := head.Commit()
	if err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	rebuilt, err := statedomains.NewStagedCommitmentStore(disk).Rebuild()
	if err != nil {
		t.Fatalf("rebuild latest commitment: %v", err)
	}
	if root2 != rebuilt {
		t.Fatalf("commitment root after repair/update = %x, rebuilt = %x", root2, rebuilt)
	}
	if stored, ok, err := rawdb.ReadLatestDomainCommitmentRoot(disk); err != nil || !ok || stored != root2 {
		t.Fatalf("stored root after repair = %x ok=%v err=%v, want %x", stored, ok, err, root2)
	}
}

func TestDomainCommitmentStateRecordsLogicalDomainTouches(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	owner := testAddr(0x7b)
	domain := kvdomains.SystemReward
	for key, value := range map[string]string{
		"prefix/1": "one",
		"prefix/2": "two",
		"keep":     "keep",
	} {
		if err := rawdb.WriteStateKVLatest(disk, owner, 0, domain, []byte(key), []byte(value)); err != nil {
			t.Fatalf("write latest %s: %v", key, err)
		}
	}
	initialRoot, err := statedomains.NewStagedCommitmentStore(disk).Rebuild()
	if err != nil {
		t.Fatalf("initial commitment: %v", err)
	}

	commitment := NewDomainCommitmentState(sdb)
	flat := statedomains.NewFlatStore(disk, 0)
	tx := statedomains.NewSharedDomainTx(statedomains.SharedDomainTxConfig{
		Latest:     flat,
		Writer:     flat,
		Commitment: commitment,
	})
	if err := tx.DomainDelPrefix(owner, domain, []byte("prefix/")); err != nil {
		t.Fatal(err)
	}
	if err := tx.DomainPut(owner, domain, []byte("prefix/3"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Flush(context.Background()); err != nil {
		t.Fatalf("flush temporal domain tx: %v", err)
	}
	updates, err := commitment.latestUpdatesFromTouches()
	if err != nil {
		t.Fatalf("commitment updates from touches: %v", err)
	}
	if len(updates) != 3 {
		t.Fatalf("touch-derived updates = %+v, want deletes for prefix/1,prefix/2 and put for prefix/3", updates)
	}
	updatedRoot, err := statedomains.NewStagedCommitmentStore(disk).Update(updates)
	if err != nil {
		t.Fatalf("update commitment from touches: %v", err)
	}
	rebuiltRoot, err := statedomains.NewStagedCommitmentStore(disk).Rebuild()
	if err != nil {
		t.Fatalf("rebuild commitment: %v", err)
	}
	if updatedRoot != rebuiltRoot {
		t.Fatalf("touch-derived root = %x, rebuilt root = %x", updatedRoot, rebuiltRoot)
	}
	if updatedRoot == initialRoot {
		t.Fatalf("commitment root did not change after prefix delete/put: %x", updatedRoot)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(disk, owner, 0, domain, []byte("prefix/1")); err != nil || ok {
		t.Fatalf("prefix/1 after flush ok=%v err=%v", ok, err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(disk, owner, 0, domain, []byte("prefix/3")); err != nil || !ok || string(got) != "three" {
		t.Fatalf("prefix/3 after flush = %q ok=%v err=%v", got, ok, err)
	}
}

func TestDomainCommitmentStateRecordsAccountAndGenerationTouches(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	owner := testAddr(0x7c)
	if err := rawdb.WriteStateAccountLatest(disk, owner, []byte("account-v1")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(disk, owner, 1); err != nil {
		t.Fatal(err)
	}
	initialRoot, err := statedomains.NewStagedCommitmentStore(disk).Rebuild()
	if err != nil {
		t.Fatalf("initial commitment: %v", err)
	}

	commitment := NewDomainCommitmentState(sdb)
	if err := rawdb.WriteStateAccountLatest(disk, owner, []byte("account-v2")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.DeleteStateKVGeneration(disk, owner); err != nil {
		t.Fatal(err)
	}
	commitment.recordAccountLatestTouch(owner)
	commitment.recordKVGenerationTouch(owner)

	updates, err := commitment.latestUpdatesFromTouches()
	if err != nil {
		t.Fatalf("commitment updates from touches: %v", err)
	}
	if len(updates) != 2 {
		t.Fatalf("touch-derived updates = %+v, want account latest put and KV generation delete", updates)
	}
	updatedRoot, err := statedomains.NewStagedCommitmentStore(disk).Update(updates)
	if err != nil {
		t.Fatalf("update commitment from touches: %v", err)
	}
	rebuiltRoot, err := statedomains.NewStagedCommitmentStore(disk).Rebuild()
	if err != nil {
		t.Fatalf("rebuild commitment: %v", err)
	}
	if updatedRoot != rebuiltRoot {
		t.Fatalf("touch-derived root = %x, rebuilt root = %x", updatedRoot, rebuiltRoot)
	}
	if updatedRoot == initialRoot {
		t.Fatalf("commitment root did not change after account/generation touch: %x", updatedRoot)
	}
}

func TestDomainCommitmentStateUsesStateLatestViewForTouches(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	owner := testAddr(0x7d)
	domain := kvdomains.SystemReward
	view := &commitmentLatestView{
		t:          t,
		owner:      owner,
		domain:     domain,
		account:    []byte("account-v2"),
		generation: 7,
		kv: map[string][]byte{
			"prefix/3": []byte("three"),
		},
		prefixKeys: []string{"prefix/1", "prefix/2"},
	}
	sdb.flatLatestReader = view
	sdb.setAccountKVLatestView(view, view)

	commitment := NewDomainCommitmentState(sdb)
	commitment.recordAccountLatestTouch(owner)
	commitment.recordKVGenerationTouch(owner)
	if err := commitment.RecordCommitmentMutations(context.Background(), []statedomains.Mutation{
		{Kind: statedomains.MutationDelPrefix, Owner: owner, Domain: domain, Key: []byte("prefix/")},
		{Kind: statedomains.MutationPut, Owner: owner, Domain: domain, Key: []byte("prefix/3"), Value: []byte("three")},
	}); err != nil {
		t.Fatal(err)
	}

	updates, err := commitment.latestUpdatesFromTouches()
	if err != nil {
		t.Fatalf("commitment updates from typed latest view: %v", err)
	}
	if len(updates) != 5 {
		t.Fatalf("typed latest-view updates = %+v, want account, generation, two prefix deletes, one put", updates)
	}
	for i := 1; i < len(updates); i++ {
		if bytes.Compare(updates[i-1].Key, updates[i].Key) >= 0 {
			t.Fatalf("touch-derived updates not strictly sorted at %d: %x >= %x", i, updates[i-1].Key, updates[i].Key)
		}
	}
	byKey := stateCommitmentUpdatesByKey(updates)
	assertCommitmentPut(t, byKey, rawdb.StateAccountLatestCommitmentKey(owner), []byte("account-v2"))
	assertCommitmentPut(t, byKey, rawdb.StateKVGenerationCommitmentKey(owner), rawdb.EncodeStateKVGenerationValue(7))
	assertCommitmentDelete(t, byKey, rawdb.StateKVLatestCommitmentKey(owner, 7, domain, []byte("prefix/1")))
	assertCommitmentDelete(t, byKey, rawdb.StateKVLatestCommitmentKey(owner, 7, domain, []byte("prefix/2")))
	assertCommitmentPut(t, byKey, rawdb.StateKVLatestCommitmentKey(owner, 7, domain, []byte("prefix/3")), rawdb.EncodeStateKVLatestValue([]byte("three")))
	if len(view.prefixGenerations) != 1 || view.prefixGenerations[0] != 7 {
		t.Fatalf("prefix generations = %v, want [7]", view.prefixGenerations)
	}
	for _, generation := range view.kvGenerations {
		if generation != 7 {
			t.Fatalf("kv latest generation = %d, want 7 in %v", generation, view.kvGenerations)
		}
	}
}

func TestDomainCommitmentStateUsesCapturedFinalKVMutations(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	owner := testAddr(0x7e)
	domain := kvdomains.ContractStorage
	view := &commitmentLatestView{
		t:            t,
		owner:        owner,
		domain:       domain,
		generation:   9,
		failKVLatest: true,
	}
	sdb.flatLatestReader = view
	sdb.setAccountKVLatestView(view, view)

	finalValue := []byte("final")
	commitment := NewDomainCommitmentState(sdb)
	if err := commitment.RecordCommitmentMutations(context.Background(), []statedomains.Mutation{
		{Kind: statedomains.MutationPut, Owner: owner, Domain: domain, Key: []byte("prefix/final"), Value: []byte("first")},
		{Kind: statedomains.MutationPut, Owner: owner, Domain: domain, Key: []byte("prefix/gone"), Value: []byte("gone")},
		{Kind: statedomains.MutationDelPrefix, Owner: owner, Domain: domain, Key: []byte("prefix/")},
		{Kind: statedomains.MutationPut, Owner: owner, Domain: domain, Key: []byte("prefix/final"), Value: finalValue},
		{Kind: statedomains.MutationPut, Owner: owner, Domain: domain, Key: []byte("empty"), Value: nil},
	}); err != nil {
		t.Fatal(err)
	}
	// The recorder must own retained bytes because the temporal overlay releases
	// its mutation storage immediately after Flush.
	finalValue[0] = 'X'

	updates, err := commitment.latestUpdatesFromTouches()
	if err != nil {
		t.Fatalf("commitment updates from captured mutations: %v", err)
	}
	if len(updates) != 3 {
		t.Fatalf("captured updates = %+v, want final put, prefix delete, and empty put", updates)
	}
	byKey := stateCommitmentUpdatesByKey(updates)
	assertCommitmentPut(t, byKey, rawdb.StateKVLatestCommitmentKey(owner, 9, domain, []byte("prefix/final")), rawdb.EncodeStateKVLatestValue([]byte("final")))
	assertCommitmentDelete(t, byKey, rawdb.StateKVLatestCommitmentKey(owner, 9, domain, []byte("prefix/gone")))
	assertCommitmentPut(t, byKey, rawdb.StateKVLatestCommitmentKey(owner, 9, domain, []byte("empty")), rawdb.EncodeStateKVLatestValue(nil))
}

type commitmentLatestView struct {
	t                 *testing.T
	owner             tcommon.Address
	domain            kvdomains.KVDomain
	account           []byte
	generation        uint64
	kv                map[string][]byte
	prefixKeys        []string
	kvGenerations     []uint64
	prefixGenerations []uint64
	failKVLatest      bool
}

func (v *commitmentLatestView) AccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	v.checkOwner(owner)
	return append([]byte(nil), v.account...), true, nil
}

func (v *commitmentLatestView) KVGeneration(owner tcommon.Address) (uint64, bool, error) {
	v.checkOwner(owner)
	return v.generation, true, nil
}

func (v *commitmentLatestView) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	v.checkOwner(owner)
	v.checkDomain(domain)
	if v.failKVLatest {
		v.t.Fatalf("unexpected KVLatest read for captured mutation %q", key)
	}
	v.kvGenerations = append(v.kvGenerations, generation)
	value, ok := v.kv[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (v *commitmentLatestView) KVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	v.checkOwner(owner)
	v.checkDomain(domain)
	v.prefixGenerations = append(v.prefixGenerations, generation)
	for _, key := range v.prefixKeys {
		if len(prefix) > len(key) || string(prefix) != key[:len(prefix)] {
			continue
		}
		cont, err := fn([]byte(key), []byte("old"))
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (v *commitmentLatestView) GetLatest(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	v.t.Helper()
	v.t.Fatalf("unexpected generation-less GetLatest for %s %#04x %q", owner.Hex(), uint16(domain), key)
	return nil, false, nil
}

func (v *commitmentLatestView) DomainIterate(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte, fn statedomains.IterateFunc) error {
	v.t.Helper()
	v.t.Fatalf("unexpected generation-less DomainIterate for %s %#04x %q", owner.Hex(), uint16(domain), prefix)
	return nil
}

func (v *commitmentLatestView) checkOwner(owner tcommon.Address) {
	v.t.Helper()
	if owner != v.owner {
		v.t.Fatalf("owner = %s, want %s", owner.Hex(), v.owner.Hex())
	}
}

func (v *commitmentLatestView) checkDomain(domain kvdomains.KVDomain) {
	v.t.Helper()
	if domain != v.domain {
		v.t.Fatalf("domain = %#04x, want %#04x", uint16(domain), uint16(v.domain))
	}
}

func stateCommitmentUpdatesByKey(updates []rawdb.StateCommitmentUpdate) map[string]rawdb.StateCommitmentUpdate {
	out := make(map[string]rawdb.StateCommitmentUpdate, len(updates))
	for _, update := range updates {
		out[string(update.Key)] = update
	}
	return out
}

func assertCommitmentPut(t *testing.T, updates map[string]rawdb.StateCommitmentUpdate, key, value []byte) {
	t.Helper()
	update, ok := updates[string(key)]
	if !ok {
		t.Fatalf("missing commitment put key %x", key)
	}
	if update.Delete || string(update.Value) != string(value) {
		t.Fatalf("commitment update %x = %+v, want put %x", key, update, value)
	}
}

func assertCommitmentDelete(t *testing.T, updates map[string]rawdb.StateCommitmentUpdate, key []byte) {
	t.Helper()
	update, ok := updates[string(key)]
	if !ok {
		t.Fatalf("missing commitment delete key %x", key)
	}
	if !update.Delete {
		t.Fatalf("commitment update %x = %+v, want delete", key, update)
	}
}
