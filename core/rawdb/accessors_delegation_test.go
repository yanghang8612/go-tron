package rawdb

import (
	"errors"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestDelegatedResourceWriteRead(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	dr := &DelegatedResource{
		From:                      from,
		To:                        to,
		FrozenBalanceForBandwidth: 1000000,
		FrozenBalanceForEnergy:    500000,
	}
	if err := WriteDelegatedResource(db, from, to, dr); err != nil {
		t.Fatal(err)
	}
	got := ReadDelegatedResource(db, from, to)
	if got == nil {
		t.Fatal("expected delegation record")
	}
	if got.FrozenBalanceForBandwidth != 1000000 || got.FrozenBalanceForEnergy != 500000 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestDelegatedResourceDelete(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	dr := &DelegatedResource{From: from, To: to, FrozenBalanceForBandwidth: 100}
	WriteDelegatedResource(db, from, to, dr)
	WriteDelegatedResourceV2(db, from, to, true, &DelegatedResource{
		From: from, To: to, FrozenBalanceForEnergy: 200, ExpireTimeForEnergy: 10,
	})
	DeleteDelegatedResource(db, from, to)
	if ReadDelegatedResource(db, from, to) != nil {
		t.Fatal("expected nil after delete")
	}
	if ReadDelegatedResourceV2(db, from, to, false) != nil || ReadDelegatedResourceV2(db, from, to, true) != nil {
		t.Fatal("expected both V2 buckets deleted")
	}
}

func TestDelegatedResourceV2BucketsAndAggregate(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}

	WriteDelegatedResourceV2(db, from, to, false, &DelegatedResource{
		From: from, To: to, FrozenBalanceForBandwidth: 100,
	})
	WriteDelegatedResourceV2(db, from, to, true, &DelegatedResource{
		From: from, To: to, FrozenBalanceForBandwidth: 200, ExpireTimeForBandwidth: 5000,
	})

	if got := ReadDelegatedResourceV2(db, from, to, false); got == nil || got.FrozenBalanceForBandwidth != 100 || got.ExpireTimeForBandwidth != 0 {
		t.Fatalf("unexpected unlocked bucket: %+v", got)
	}
	if got := ReadDelegatedResourceV2(db, from, to, true); got == nil || got.FrozenBalanceForBandwidth != 200 || got.ExpireTimeForBandwidth != 5000 {
		t.Fatalf("unexpected locked bucket: %+v", got)
	}
	agg := ReadDelegatedResource(db, from, to)
	if agg == nil || agg.FrozenBalanceForBandwidth != 300 || agg.ExpireTimeForBandwidth != 5000 {
		t.Fatalf("unexpected aggregate: %+v", agg)
	}
	strict, ok, err := ReadDelegatedResourceStrict(db, from, to)
	if err != nil || !ok || strict == nil || strict.FrozenBalanceForBandwidth != 300 || strict.ExpireTimeForBandwidth != 5000 {
		t.Fatalf("strict aggregate = %+v/%v/%v, want bandwidth 300 expiry 5000", strict, ok, err)
	}
	unlocked, ok, err := ReadDelegatedResourceV2Strict(db, from, to, false)
	if err != nil || !ok || unlocked == nil || unlocked.FrozenBalanceForBandwidth != 100 {
		t.Fatalf("strict unlocked = %+v/%v/%v, want bandwidth 100", unlocked, ok, err)
	}
	if legacy, ok, err := ReadDelegatedResourceLegacyStrict(db, from, to); err != nil || ok || legacy != nil {
		t.Fatalf("strict legacy absent = %+v/%v/%v, want nil/false/nil", legacy, ok, err)
	}
}

func TestUnlockExpiredDelegatedResource(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}

	WriteDelegatedResourceV2(db, from, to, true, &DelegatedResource{
		From: from, To: to,
		FrozenBalanceForBandwidth: 100,
		ExpireTimeForBandwidth:    999,
		FrozenBalanceForEnergy:    200,
		ExpireTimeForEnergy:       2000,
	})

	if err := UnlockExpiredDelegatedResource(db, db, from, to, 1000); err != nil {
		t.Fatal(err)
	}
	unlocked := ReadDelegatedResourceV2(db, from, to, false)
	if unlocked == nil || unlocked.FrozenBalanceForBandwidth != 100 || unlocked.FrozenBalanceForEnergy != 0 {
		t.Fatalf("unexpected unlocked after expiry: %+v", unlocked)
	}
	locked := ReadDelegatedResourceV2(db, from, to, true)
	if locked == nil || locked.FrozenBalanceForBandwidth != 0 || locked.FrozenBalanceForEnergy != 200 {
		t.Fatalf("unexpected locked after partial expiry: %+v", locked)
	}
}

func TestDelegationIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	receivers := []common.Address{{0x41, 0x02}, {0x41, 0x03}}
	if err := WriteDelegationIndex(db, from, receivers); err != nil {
		t.Fatal(err)
	}
	got := ReadDelegationIndex(db, from)
	if len(got) != 2 {
		t.Fatalf("expected 2 receivers, got %d", len(got))
	}
	if got[0] != receivers[0] || got[1] != receivers[1] {
		t.Fatalf("unexpected receivers: %v", got)
	}
	strict, ok, err := ReadDelegationIndexStrict(db, from)
	if err != nil || !ok || len(strict) != 2 || strict[0] != receivers[0] || strict[1] != receivers[1] {
		t.Fatalf("strict delegation index = %v/%v/%v, want receivers", strict, ok, err)
	}
	if err := db.Put(delegationIndexKey(from[:]), nil); err != nil {
		t.Fatal(err)
	}
	if strict, ok, err := ReadDelegationIndexStrict(db, from); err != nil || !ok || len(strict) != 0 {
		t.Fatalf("strict empty delegation index = %v/%v/%v, want empty/true/nil", strict, ok, err)
	}
}

func TestDelegationIndexRejectsMalformedBytes(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	if err := db.Put(delegationIndexKey(from[:]), []byte("short")); err != nil {
		t.Fatal(err)
	}
	if got := ReadDelegationIndex(db, from); got != nil {
		t.Fatalf("delegation index = %v, want nil for malformed bytes", got)
	}
	if got, ok, err := ReadDelegationIndexStrict(db, from); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "length 5") {
		t.Fatalf("strict delegation index malformed = %v/%v/%v, want length error", got, ok, err)
	}
}

func TestDelegationNotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	if ReadDelegatedResource(db, from, to) != nil {
		t.Fatal("expected nil")
	}
	if ReadDelegationIndex(db, from) != nil {
		t.Fatal("expected nil")
	}
	if got, ok, err := ReadDelegatedResourceStrict(db, from, to); err != nil || ok || got != nil {
		t.Fatalf("strict delegation absent = %+v/%v/%v, want nil/false/nil", got, ok, err)
	}
	if got, ok, err := ReadDelegationIndexStrict(db, from); err != nil || ok || got != nil {
		t.Fatalf("strict delegation index absent = %v/%v/%v, want nil/false/nil", got, ok, err)
	}
}

func TestDelegatedResourceStrictSurfacesStorageErrors(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	if err := WriteDelegatedResource(db, from, to, &DelegatedResource{From: from, To: to, FrozenBalanceForEnergy: 5}); err != nil {
		t.Fatal(err)
	}

	readers := []struct {
		name string
		read func(ethdb.KeyValueReader) (bool, error)
	}{
		{
			name: "legacy",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadDelegatedResourceLegacyStrict(r, from, to)
				return ok, err
			},
		},
		{
			name: "aggregate",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadDelegatedResourceStrict(r, from, to)
				return ok, err
			},
		},
		{
			name: "index",
			read: func(r ethdb.KeyValueReader) (bool, error) {
				_, ok, err := ReadDelegationIndexStrict(r, from)
				return ok, err
			},
		},
	}

	if err := WriteDelegationIndex(db, from, []common.Address{to}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range readers {
		t.Run(tc.name+"/has", func(t *testing.T) {
			ok, err := tc.read(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")})
			if err == nil || ok || !strings.Contains(err.Error(), "presence") {
				t.Fatalf("has error: ok=%v err=%v, want presence error", ok, err)
			}
		})
		t.Run(tc.name+"/get", func(t *testing.T) {
			ok, err := tc.read(failingStateDomainReader{reader: db, getErr: errors.New("get boom")})
			if err == nil || ok || !strings.Contains(err.Error(), "get boom") {
				t.Fatalf("get error: ok=%v err=%v, want get error", ok, err)
			}
		})
	}
}

func TestDelegatedResourceStrictSurfacesCorruptRows(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}

	if err := db.Put(delegationKey(from[:], to[:]), []byte("{")); err != nil {
		t.Fatal(err)
	}
	if got := ReadDelegatedResourceLegacy(db, from, to); got != nil {
		t.Fatalf("compat corrupt legacy = %+v, want nil", got)
	}
	if got, ok, err := ReadDelegatedResourceLegacyStrict(db, from, to); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode delegated resource legacy") {
		t.Fatalf("strict corrupt legacy = %+v/%v/%v, want decode error", got, ok, err)
	}
	if got, ok, err := ReadDelegatedResourceStrict(db, from, to); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode delegated resource legacy") {
		t.Fatalf("strict corrupt aggregate = %+v/%v/%v, want decode error", got, ok, err)
	}

	if err := db.Delete(delegationKey(from[:], to[:])); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(delegationKeyV2(from[:], to[:], true), []byte("{")); err != nil {
		t.Fatal(err)
	}
	if got := ReadDelegatedResourceV2(db, from, to, true); got != nil {
		t.Fatalf("compat corrupt v2 = %+v, want nil", got)
	}
	if got, ok, err := ReadDelegatedResourceV2Strict(db, from, to, true); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode delegated resource v2") {
		t.Fatalf("strict corrupt v2 = %+v/%v/%v, want decode error", got, ok, err)
	}
}
