package state

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type frozenBandwidthPointReadStore struct {
	ethdb.Database
	iteratorCalls int
}

func (s *frozenBandwidthPointReadStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	s.iteratorCalls++
	return s.Database.NewIterator(prefix, start)
}

func assertFrozenBandwidth(t *testing.T, got []*corepb.Account_Frozen, want ...*corepb.Account_Frozen) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("frozen-bandwidth length = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("frozen-bandwidth[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAccountStakeV1PersistsOutsideAccountEnvelopeAndPreservesOrder(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa4)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	first := &corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 30}
	second := &corepb.Account_Frozen{FrozenBalance: 22, ExpireTime: 10}
	duplicate := &corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 30}
	if err := sdb.writeAccountFrozenBandwidth(sdb.getStateObject(addr), []*corepb.Account_Frozen{first, second, duplicate}); err != nil {
		t.Fatal(err)
	}
	tronPower := &corepb.Account_Frozen{FrozenBalance: 44, ExpireTime: 50}
	if err := sdb.writeAccountTronPower(sdb.getStateObject(addr), tronPower); err != nil {
		t.Fatal(err)
	}

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	raw, ok, err := rawdb.ReadStateAccountLatest(sdb.db.DiskDB(), addr)
	if err != nil || !ok {
		t.Fatalf("read account latest: ok=%v err=%v", ok, err)
	}
	envelope, err := DecodeStateAccountV3(raw)
	if err != nil {
		t.Fatal(err)
	}
	var stored corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Frozen) != 0 || stored.TronPower != nil {
		t.Fatalf("split Stake V1 leaked into account envelope: frozen=%+v tronPower=%+v", stored.Frozen, stored.TronPower)
	}
	for index, want := range []*corepb.Account_Frozen{first, second, duplicate} {
		key := accountFrozenBandwidthKey(uint32(index))
		value, exists, readErr := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountFrozenBandwidthAux, key)
		if readErr != nil || !exists {
			t.Fatalf("read frozen-bandwidth row %d: exists=%v err=%v", index, exists, readErr)
		}
		row, decodeErr := decodeAccountFrozenBandwidthRow(key, value)
		if decodeErr != nil || !proto.Equal(row.entry, want) {
			t.Fatalf("frozen-bandwidth row %d = %+v err=%v, want %+v", index, row.entry, decodeErr, want)
		}
	}
	value, exists, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.AccountTronPowerAux, accountTronPowerKey)
	if err != nil || !exists {
		t.Fatalf("read tron-power row: exists=%v err=%v", exists, err)
	}
	storedTronPower, err := decodeAccountTronPower(accountTronPowerKey, value)
	if err != nil || !proto.Equal(storedTronPower, tronPower) {
		t.Fatalf("stored tron-power = %+v err=%v, want %+v", storedTronPower, err, tronPower)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.GetBalance(addr); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
	obj := reopened.stateObjects[addr]
	if obj == nil || obj.accountFrozenBandwidthLoaded || obj.accountTronPowerLoaded || len(obj.account.Proto().Frozen) != 0 || obj.account.Proto().TronPower != nil {
		t.Fatalf("balance read eagerly loaded Stake V1: %+v", obj)
	}
	if got := reopened.GetFreezeV1ExpireTime(addr, 0); got != 30 {
		t.Fatalf("typed bandwidth expiry = %d, want 30", got)
	}
	if obj.accountFrozenBandwidthLoaded || obj.accountTronPowerLoaded {
		t.Fatal("typed bandwidth expiry unnecessarily materialized full Stake V1")
	}
	account := reopened.GetAccount(addr)
	if account == nil {
		t.Fatal("materialized account missing")
	}
	assertFrozenBandwidth(t, account.FrozenBandwidthList(), first, second, duplicate)
	if !proto.Equal(account.Proto().TronPower, tronPower) {
		t.Fatalf("materialized tron-power = %+v, want %+v", account.Proto().TronPower, tronPower)
	}
	copy := reopened.CopyAccount(addr)
	assertFrozenBandwidth(t, copy.FrozenBandwidthList(), first, second, duplicate)
}

func TestAccountFrozenBandwidthRowsUseDensePointReads(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xab)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	want := []*corepb.Account_Frozen{
		{FrozenBalance: 11, ExpireTime: 30},
		{FrozenBalance: 22, ExpireTime: 20},
		{FrozenBalance: 33, ExpireTime: 10},
	}
	if err := sdb.writeAccountFrozenBandwidth(sdb.getStateObject(addr), want); err != nil {
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
	store := &frozenBandwidthPointReadStore{Database: sdb.db.DiskDB()}
	reopened.SetAccountKVIndexStore(store)
	rows, err := reopened.accountFrozenBandwidthRows(reopened.getStateObject(addr))
	if err != nil {
		t.Fatal(err)
	}
	if store.iteratorCalls != 0 {
		t.Fatalf("frozen-bandwidth point read opened %d iterators", store.iteratorCalls)
	}
	if len(rows) != len(want) {
		t.Fatalf("frozen-bandwidth rows = %d, want %d", len(rows), len(want))
	}
	for i := range rows {
		if rows[i].index != uint32(i) || !proto.Equal(rows[i].entry, want[i]) {
			t.Fatalf("frozen-bandwidth row %d = index %d %+v, want %+v", i, rows[i].index, rows[i].entry, want[i])
		}
	}
	if got := reopened.FrozenV1BandwidthCount(addr); got != len(want) {
		t.Fatalf("frozen-bandwidth count = %d, want %d", got, len(want))
	}
	if store.iteratorCalls != 0 {
		t.Fatalf("frozen-bandwidth count opened %d iterators", store.iteratorCalls)
	}
	if got := reopened.FrozenV1ResourceAmount(addr, corepb.ResourceCode_BANDWIDTH); got != 66 {
		t.Fatalf("frozen-bandwidth amount = %d, want 66", got)
	}
	obj := reopened.getStateObject(addr)
	if obj.accountMapsLoaded || obj.accountPermissionsLoaded || obj.accountVotesLoaded || obj.accountStakeV2Loaded || obj.accountFrozenSupplyLoaded || obj.accountResourceLoaded || obj.accountFrozenBandwidthLoaded || obj.accountTronPowerLoaded {
		t.Fatalf("point reads materialized split account domains: %+v", obj)
	}
}

func TestAccountStakeV1PreservesPresentEmptyMessages(t *testing.T) {
	sdb := newTestStateDB(t)
	withEmpty := testAddr(0xa5)
	withoutTronPower := testAddr(0xa6)
	for _, addr := range []byte{0xa5, 0xa6} {
		sdb.CreateAccount(testAddr(addr), corepb.AccountType_Normal)
	}
	if err := sdb.setAccountFrozenBandwidth(sdb.getStateObject(withEmpty), 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := sdb.writeAccountTronPower(sdb.getStateObject(withEmpty), &corepb.Account_Frozen{}); err != nil {
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
	account := reopened.GetAccount(withEmpty)
	if account == nil {
		t.Fatal("account with empty Stake V1 missing")
	}
	assertFrozenBandwidth(t, account.FrozenBandwidthList(), &corepb.Account_Frozen{})
	if account.Proto().TronPower == nil || !proto.Equal(account.Proto().TronPower, &corepb.Account_Frozen{}) {
		t.Fatalf("present-empty tron-power was lost: %+v", account.Proto().TronPower)
	}
	if account := reopened.GetAccount(withoutTronPower); account == nil || account.Proto().TronPower != nil {
		t.Fatalf("absent tron-power became present: %+v", account)
	}
}

func TestAccountStakeV1TypedMutationSemantics(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa7)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.FreezeV1Bandwidth(addr, 10, 30)
	sdb.FreezeV1Bandwidth(addr, 5, 20)
	account := sdb.GetAccount(addr)
	assertFrozenBandwidth(t, account.FrozenBandwidthList(), &corepb.Account_Frozen{FrozenBalance: 15, ExpireTime: 20})
	if got := sdb.UnfreezeV1Bandwidth(addr, 19); got != 0 {
		t.Fatalf("premature bandwidth refund = %d, want 0", got)
	}
	if got := sdb.UnfreezeV1Bandwidth(addr, 20); got != 15 {
		t.Fatalf("bandwidth refund = %d, want 15", got)
	}
	if got := sdb.GetAccount(addr); got == nil || len(got.FrozenBandwidthList()) != 0 {
		t.Fatalf("bandwidth rows remained after unfreeze: %+v", got)
	}

	sdb.FreezeV1TronPower(addr, 10, 30)
	sdb.FreezeV1TronPower(addr, 5, 20)
	account = sdb.GetAccount(addr)
	if account.V1TronPowerFrozen() != 15 || account.V1TronPowerExpireTime() != 30 {
		t.Fatalf("merged tron-power = amount %d expiry %d, want 15/30", account.V1TronPowerFrozen(), account.V1TronPowerExpireTime())
	}
	if got := sdb.UnfreezeV1TronPower(addr, 29); got != 0 {
		t.Fatalf("premature tron-power refund = %d, want 0", got)
	}
	if got := sdb.UnfreezeV1TronPower(addr, 30); got != 15 {
		t.Fatalf("tron-power refund = %d, want 15", got)
	}
	if got := sdb.GetAccount(addr); got == nil || got.Proto().TronPower != nil {
		t.Fatalf("tron-power row remained after unfreeze: %+v", got)
	}
}

func TestAccountStakeV1SnapshotRevertInvalidatesMaterializedCache(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xa8)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	first := &corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 10}
	second := &corepb.Account_Frozen{FrozenBalance: 22, ExpireTime: 30}
	if err := sdb.writeAccountFrozenBandwidth(sdb.getStateObject(addr), []*corepb.Account_Frozen{first, second}); err != nil {
		t.Fatal(err)
	}
	sdb.FreezeV1TronPower(addr, 44, 50)
	if got := sdb.GetAccount(addr); got == nil {
		t.Fatal("initial account missing")
	}

	snapshot := sdb.Snapshot()
	if amount := sdb.UnfreezeV1Bandwidth(addr, 20); amount != 11 {
		t.Fatalf("removed bandwidth = %d, want 11", amount)
	}
	sdb.FreezeV1TronPower(addr, 6, 70)
	updated := sdb.GetAccount(addr)
	assertFrozenBandwidth(t, updated.FrozenBandwidthList(), second)
	if updated.V1TronPowerFrozen() != 50 || updated.V1TronPowerExpireTime() != 70 {
		t.Fatalf("updated tron-power = %+v", updated.Proto().TronPower)
	}

	sdb.RevertToSnapshot(snapshot)
	reverted := sdb.GetAccount(addr)
	assertFrozenBandwidth(t, reverted.FrozenBandwidthList(), first, second)
	if reverted.V1TronPowerFrozen() != 44 || reverted.V1TronPowerExpireTime() != 50 {
		t.Fatalf("tron-power after revert = %+v", reverted.Proto().TronPower)
	}
}

func TestAccountStakeV1DirectKVWriteInvalidatesMaterializedField(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xaa)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.FreezeV1Bandwidth(addr, 11, 22)
	if got := sdb.GetAccount(addr); got == nil || got.TotalFrozenBandwidth() != 11 {
		t.Fatalf("initial materialized bandwidth = %+v", got)
	}
	replacement := &corepb.Account_Frozen{FrozenBalance: 33, ExpireTime: 44}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(replacement)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(addr, kvdomains.AccountFrozenBandwidthAux, accountFrozenBandwidthKey(0), value); err != nil {
		t.Fatal(err)
	}
	got := sdb.GetAccount(addr)
	assertFrozenBandwidth(t, got.FrozenBandwidthList(), replacement)
}

func TestAccountStakeV1HistoryUsesOnlyChangedRows(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0xa9)
	first := &corepb.Account_Frozen{FrozenBalance: 11, ExpireTime: 10}
	second := &corepb.Account_Frozen{FrozenBalance: 22, ExpireTime: 30}
	f.applyBlock([32]byte{0x81}, func(s *StateDB) {
		s.AddBalance(addr, 1)
		if err := s.writeAccountFrozenBandwidth(s.getStateObject(addr), []*corepb.Account_Frozen{first, second}); err != nil {
			t.Fatal(err)
		}
		s.FreezeV1TronPower(addr, 44, 50)
	})
	f.applyBlock([32]byte{0x82}, func(s *StateDB) {
		if amount := s.UnfreezeV1Bandwidth(addr, 20); amount != 11 {
			t.Fatalf("removed bandwidth = %d, want 11", amount)
		}
		s.FreezeV1TronPower(addr, 6, 70)
	})

	changes := collectStateDomainChanges(t, f.disk, 2)
	var bandwidthChanges, tronPowerChanges []*rawdb.StateDomainChange
	for _, change := range changes {
		if change.Owner != addr {
			continue
		}
		if change.FlatDomain == rawdb.StateFlatDomainAccountLatest {
			t.Fatalf("Stake V1-only update rewrote account envelope: %+v", change)
		}
		if change.FlatDomain != rawdb.StateFlatDomainKVLatest {
			continue
		}
		switch change.Domain {
		case kvdomains.AccountFrozenBandwidthAux:
			bandwidthChanges = append(bandwidthChanges, change)
		case kvdomains.AccountTronPowerAux:
			tronPowerChanges = append(tronPowerChanges, change)
		}
	}
	if len(bandwidthChanges) != 1 || !bytes.Equal(bandwidthChanges[0].Key, accountFrozenBandwidthKey(0)) || bandwidthChanges[0].NextExists {
		t.Fatalf("frozen-bandwidth history changes = %+v, want one index-0 delete", bandwidthChanges)
	}
	if len(tronPowerChanges) != 1 || !bytes.Equal(tronPowerChanges[0].Key, accountTronPowerKey) || !tronPowerChanges[0].PrevExists || !tronPowerChanges[0].NextExists {
		t.Fatalf("tron-power history changes = %+v, want one update", tronPowerChanges)
	}

	at1, err := f.reader().AccountAt(addr, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertFrozenBandwidth(t, at1.FrozenBandwidthList(), first, second)
	if at1.V1TronPowerFrozen() != 44 || at1.V1TronPowerExpireTime() != 50 {
		t.Fatalf("block 1 tron-power = %+v", at1.Proto().TronPower)
	}
	at2, err := f.reader().AccountAt(addr, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertFrozenBandwidth(t, at2.FrozenBandwidthList(), second)
	if at2.V1TronPowerFrozen() != 50 || at2.V1TronPowerExpireTime() != 70 {
		t.Fatalf("block 2 tron-power = %+v", at2.Proto().TronPower)
	}
}
