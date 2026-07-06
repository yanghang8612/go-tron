package state

import (
	"reflect"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestDelegationStoreRoundTripAcrossRoot(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	statedb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	from := testAddr(0x11)
	to := testAddr(0x12)
	if err := statedb.WriteDelegatedResourceLegacy(from, to, &rawdb.DelegatedResource{
		From:                      from,
		To:                        to,
		FrozenBalanceForBandwidth: 10,
		ExpireTimeForBandwidth:    100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteDelegatedResourceV2(from, to, false, &rawdb.DelegatedResource{
		From:                   from,
		To:                     to,
		FrozenBalanceForEnergy: 20,
		ExpireTimeForEnergy:    200,
	}); err != nil {
		t.Fatal(err)
	}
	root, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}

	agg := reopened.ReadDelegatedResource(from, to)
	if agg == nil || agg.FrozenBalanceForBandwidth != 10 || agg.FrozenBalanceForEnergy != 20 ||
		agg.ExpireTimeForBandwidth != 100 || agg.ExpireTimeForEnergy != 200 {
		t.Fatalf("aggregate delegation mismatch: %+v", agg)
	}
	strict, ok, err := reopened.ReadDelegatedResourceStrict(from, to)
	if err != nil || !ok || strict == nil || strict.FrozenBalanceForBandwidth != 10 || strict.FrozenBalanceForEnergy != 20 ||
		strict.ExpireTimeForBandwidth != 100 || strict.ExpireTimeForEnergy != 200 {
		t.Fatalf("strict aggregate delegation mismatch: %+v/%v/%v", strict, ok, err)
	}
	unlocked, ok, err := reopened.ReadDelegatedResourceV2Strict(from, to, false)
	if err != nil || !ok || unlocked == nil || unlocked.FrozenBalanceForEnergy != 20 {
		t.Fatalf("strict v2 delegation mismatch: %+v/%v/%v", unlocked, ok, err)
	}
	locked, ok, err := reopened.ReadDelegatedResourceV2Strict(from, to, true)
	if err != nil || ok || locked != nil {
		t.Fatalf("strict missing locked delegation = %+v/%v/%v, want nil/false/nil", locked, ok, err)
	}
}

func TestDelegationStoreUnlockExpired(t *testing.T) {
	statedb := newTestStateDB(t)
	from := testAddr(0x21)
	to := testAddr(0x22)
	if err := statedb.WriteDelegatedResourceV2(from, to, true, &rawdb.DelegatedResource{
		From:                      from,
		To:                        to,
		FrozenBalanceForBandwidth: 10,
		FrozenBalanceForEnergy:    20,
		ExpireTimeForBandwidth:    999,
		ExpireTimeForEnergy:       2_000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := statedb.UnlockExpiredDelegatedResource(from, to, 1_000); err != nil {
		t.Fatal(err)
	}
	unlocked := statedb.ReadDelegatedResourceV2(from, to, false)
	if unlocked == nil || unlocked.FrozenBalanceForBandwidth != 10 || unlocked.FrozenBalanceForEnergy != 0 {
		t.Fatalf("unlocked bucket mismatch: %+v", unlocked)
	}
	locked := statedb.ReadDelegatedResourceV2(from, to, true)
	if locked == nil || locked.FrozenBalanceForBandwidth != 0 || locked.FrozenBalanceForEnergy != 20 {
		t.Fatalf("locked bucket mismatch: %+v", locked)
	}
}

func TestDelegationStoreIndexes(t *testing.T) {
	statedb := newTestStateDB(t)
	from := testAddr(0x31)
	to1 := testAddr(0x32)
	to2 := testAddr(0x33)

	if err := statedb.WriteDelegationIndex(from, []tcommon.Address{to1, to2}); err != nil {
		t.Fatal(err)
	}
	if got := statedb.ReadDelegationIndex(from); !reflect.DeepEqual(got, []tcommon.Address{to1, to2}) {
		t.Fatalf("delegation index = %v, want [%s %s]", got, to1.Hex(), to2.Hex())
	}
	strict, err := statedb.ReadDelegationIndexStrict(from)
	if err != nil {
		t.Fatalf("ReadDelegationIndexStrict: %v", err)
	}
	if !reflect.DeepEqual(strict, []tcommon.Address{to1, to2}) {
		t.Fatalf("strict delegation index = %v, want [%s %s]", strict, to1.Hex(), to2.Hex())
	}

	if err := statedb.WriteDrAccountIndexLegacyDelegate(from.Bytes(), to1.Bytes()); err != nil {
		t.Fatal(err)
	}
	legacy := statedb.ReadDrAccountIndexLegacy(from.Bytes())
	if legacy == nil || len(legacy.ToAccounts) != 1 || string(legacy.ToAccounts[0]) != string(to1.Bytes()) {
		t.Fatalf("legacy index mismatch: %+v", legacy)
	}
	if err := statedb.ConvertDrAccountIndexLegacy(from.Bytes()); err != nil {
		t.Fatal(err)
	}
	if legacy := statedb.ReadDrAccountIndexLegacy(from.Bytes()); legacy != nil {
		t.Fatalf("legacy index should be removed after conversion: %+v", legacy)
	}
	entry := statedb.ReadDrAccountIndexEntry(rawdb.DrAccIdxV1From, from.Bytes(), to1.Bytes())
	if entry == nil || string(entry.Account) != string(to1.Bytes()) || entry.Timestamp != 1 {
		t.Fatalf("directional index mismatch: %+v", entry)
	}
}

func TestDelegationIndexStrictRejectsMalformedBytes(t *testing.T) {
	statedb := newTestStateDB(t)
	from := testAddr(0x41)
	if err := statedb.SystemKVPut(kvdomains.SystemDelegation, rawdb.DelegationIndexStateKey(from), []byte("short")); err != nil {
		t.Fatalf("write malformed delegation index: %v", err)
	}

	if got := statedb.ReadDelegationIndex(from); got != nil {
		t.Fatalf("legacy delegation index = %v, want nil for malformed bytes", got)
	}
	_, err := statedb.ReadDelegationIndexStrict(from)
	if err == nil || !strings.Contains(err.Error(), "malformed length") {
		t.Fatalf("strict delegation index error = %v, want malformed length", err)
	}
}

func TestDelegatedResourceStrictRejectsMalformedJSON(t *testing.T) {
	statedb := newTestStateDB(t)
	from := testAddr(0x51)
	to := testAddr(0x52)

	if err := statedb.SystemKVPut(kvdomains.SystemDelegation, rawdb.DelegatedResourceStateKey(from, to), []byte("{")); err != nil {
		t.Fatalf("write malformed legacy delegation: %v", err)
	}
	if got := statedb.ReadDelegatedResourceLegacy(from, to); got != nil {
		t.Fatalf("compat legacy delegation = %+v, want nil for malformed JSON", got)
	}
	if got, ok, err := statedb.ReadDelegatedResourceLegacyStrict(from, to); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode delegated resource legacy") {
		t.Fatalf("strict legacy delegation = %+v/%v/%v, want decode error", got, ok, err)
	}
	if got, ok, err := statedb.ReadDelegatedResourceStrict(from, to); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode delegated resource legacy") {
		t.Fatalf("strict aggregate delegation = %+v/%v/%v, want decode error", got, ok, err)
	}

	if err := statedb.DeleteDelegatedResourceLegacy(from, to); err != nil {
		t.Fatal(err)
	}
	if err := statedb.SystemKVPut(kvdomains.SystemDelegation, rawdb.DelegatedResourceV2StateKey(from, to, true), []byte("{")); err != nil {
		t.Fatalf("write malformed v2 delegation: %v", err)
	}
	if got := statedb.ReadDelegatedResourceV2(from, to, true); got != nil {
		t.Fatalf("compat v2 delegation = %+v, want nil for malformed JSON", got)
	}
	if got, ok, err := statedb.ReadDelegatedResourceV2Strict(from, to, true); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode delegated resource v2") {
		t.Fatalf("strict v2 delegation = %+v/%v/%v, want decode error", got, ok, err)
	}
}
