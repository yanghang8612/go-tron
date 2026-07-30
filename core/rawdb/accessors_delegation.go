package rawdb

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type DelegatedResource struct {
	From                      common.Address `json:"from"`
	To                        common.Address `json:"to"`
	FrozenBalanceForBandwidth int64          `json:"frozen_balance_for_bandwidth"`
	FrozenBalanceForEnergy    int64          `json:"frozen_balance_for_energy"`
	ExpireTimeForBandwidth    int64          `json:"expire_time_for_bandwidth"`
	ExpireTimeForEnergy       int64          `json:"expire_time_for_energy"`
}

func WriteDelegatedResource(db ethdb.KeyValueWriter, from, to common.Address, dr *DelegatedResource) error {
	data, err := json.Marshal(dr)
	if err != nil {
		return err
	}
	return db.Put(delegationKey(from[:], to[:]), data)
}

func WriteDelegatedResourceV2(db ethdb.KeyValueWriter, from, to common.Address, locked bool, dr *DelegatedResource) error {
	data, err := json.Marshal(dr)
	if err != nil {
		return err
	}
	return db.Put(delegationKeyV2(from[:], to[:], locked), data)
}

func ReadDelegatedResource(db ethdb.KeyValueReader, from, to common.Address) *DelegatedResource {
	var out *DelegatedResource
	merge := func(dr *DelegatedResource) {
		if dr == nil {
			return
		}
		if out == nil {
			out = &DelegatedResource{From: from, To: to}
		}
		out.FrozenBalanceForBandwidth += dr.FrozenBalanceForBandwidth
		out.FrozenBalanceForEnergy += dr.FrozenBalanceForEnergy
		if dr.ExpireTimeForBandwidth > out.ExpireTimeForBandwidth {
			out.ExpireTimeForBandwidth = dr.ExpireTimeForBandwidth
		}
		if dr.ExpireTimeForEnergy > out.ExpireTimeForEnergy {
			out.ExpireTimeForEnergy = dr.ExpireTimeForEnergy
		}
	}
	merge(readDelegatedResourceByKey(db, delegationKey(from[:], to[:])))
	merge(ReadDelegatedResourceV2(db, from, to, false))
	merge(ReadDelegatedResourceV2(db, from, to, true))
	return out
}

// ReadDelegatedResourceStrict returns the aggregate legacy+V2 delegation row
// for (from,to) and surfaces storage/corruption errors. Missing rows return
// (nil, false, nil).
func ReadDelegatedResourceStrict(db ethdb.KeyValueReader, from, to common.Address) (*DelegatedResource, bool, error) {
	var out *DelegatedResource
	seen := false
	merge := func(dr *DelegatedResource, ok bool, err error) (bool, error) {
		if err != nil {
			return ok, err
		}
		if !ok || dr == nil {
			return false, nil
		}
		seen = true
		if out == nil {
			out = &DelegatedResource{From: from, To: to}
		}
		out.FrozenBalanceForBandwidth += dr.FrozenBalanceForBandwidth
		out.FrozenBalanceForEnergy += dr.FrozenBalanceForEnergy
		if dr.ExpireTimeForBandwidth > out.ExpireTimeForBandwidth {
			out.ExpireTimeForBandwidth = dr.ExpireTimeForBandwidth
		}
		if dr.ExpireTimeForEnergy > out.ExpireTimeForEnergy {
			out.ExpireTimeForEnergy = dr.ExpireTimeForEnergy
		}
		return true, nil
	}
	if rowOK, err := merge(readDelegatedResourceByKeyStrict(db, delegationKey(from[:], to[:]), "delegated resource legacy")); err != nil {
		return nil, seen || rowOK, err
	}
	if rowOK, err := merge(ReadDelegatedResourceV2Strict(db, from, to, false)); err != nil {
		return nil, seen || rowOK, err
	}
	if rowOK, err := merge(ReadDelegatedResourceV2Strict(db, from, to, true)); err != nil {
		return nil, seen || rowOK, err
	}
	return out, seen, nil
}

func ReadDelegatedResourceLegacy(db ethdb.KeyValueReader, from, to common.Address) *DelegatedResource {
	return readDelegatedResourceByKey(db, delegationKey(from[:], to[:]))
}

// ReadDelegatedResourceLegacyStrict returns the legacy delegation row for
// (from,to) and surfaces storage/corruption errors. Missing rows return
// (nil, false, nil).
func ReadDelegatedResourceLegacyStrict(db ethdb.KeyValueReader, from, to common.Address) (*DelegatedResource, bool, error) {
	return readDelegatedResourceByKeyStrict(db, delegationKey(from[:], to[:]), "delegated resource legacy")
}

func ReadDelegatedResourceV2(db ethdb.KeyValueReader, from, to common.Address, locked bool) *DelegatedResource {
	return readDelegatedResourceByKey(db, delegationKeyV2(from[:], to[:], locked))
}

// ReadDelegatedResourceV2Strict returns the V2 delegation row for (from,to)
// and the locked bucket and surfaces storage/corruption errors. Missing rows
// return (nil, false, nil).
func ReadDelegatedResourceV2Strict(db ethdb.KeyValueReader, from, to common.Address, locked bool) (*DelegatedResource, bool, error) {
	return readDelegatedResourceByKeyStrict(db, delegationKeyV2(from[:], to[:], locked), fmt.Sprintf("delegated resource v2 locked=%v", locked))
}

func readDelegatedResourceByKey(db ethdb.KeyValueReader, key []byte) *DelegatedResource {
	data, err := db.Get(key)
	if err != nil || len(data) == 0 {
		return nil
	}
	dr := &DelegatedResource{}
	if err := json.Unmarshal(data, dr); err != nil {
		return nil
	}
	return dr
}

func readDelegatedResourceByKeyStrict(db ethdb.KeyValueReader, key []byte, context string) (*DelegatedResource, bool, error) {
	data, ok, err := readPresentValue(db, key, context)
	if err != nil || !ok {
		return nil, ok, err
	}
	dr := &DelegatedResource{}
	if err := json.Unmarshal(data, dr); err != nil {
		return nil, true, fmt.Errorf("rawdb: decode %s: %w", context, err)
	}
	return dr, true, nil
}

func DeleteDelegatedResource(db ethdb.KeyValueWriter, from, to common.Address) error {
	if err := db.Delete(delegationKey(from[:], to[:])); err != nil {
		return err
	}
	if err := DeleteDelegatedResourceV2(db, from, to, false); err != nil {
		return err
	}
	return DeleteDelegatedResourceV2(db, from, to, true)
}

func DeleteDelegatedResourceLegacy(db ethdb.KeyValueWriter, from, to common.Address) error {
	return db.Delete(delegationKey(from[:], to[:]))
}

func DeleteDelegatedResourceV2(db ethdb.KeyValueWriter, from, to common.Address, locked bool) error {
	return db.Delete(delegationKeyV2(from[:], to[:], locked))
}

func UnlockExpiredDelegatedResource(db ethdb.KeyValueReader, writer ethdb.KeyValueWriter, from, to common.Address, now int64) error {
	lockResource := ReadDelegatedResourceV2(db, from, to, true)
	if lockResource == nil {
		return nil
	}
	if lockResource.ExpireTimeForEnergy >= now && lockResource.ExpireTimeForBandwidth >= now {
		return nil
	}

	unlockResource := ReadDelegatedResourceV2(db, from, to, false)
	if unlockResource == nil {
		unlockResource = &DelegatedResource{From: from, To: to}
	}
	if lockResource.ExpireTimeForEnergy < now {
		unlockResource.FrozenBalanceForEnergy += lockResource.FrozenBalanceForEnergy
		lockResource.FrozenBalanceForEnergy = 0
		lockResource.ExpireTimeForEnergy = 0
	}
	if lockResource.ExpireTimeForBandwidth < now {
		unlockResource.FrozenBalanceForBandwidth += lockResource.FrozenBalanceForBandwidth
		lockResource.FrozenBalanceForBandwidth = 0
		lockResource.ExpireTimeForBandwidth = 0
	}
	if lockResource.FrozenBalanceForBandwidth == 0 && lockResource.FrozenBalanceForEnergy == 0 {
		if err := DeleteDelegatedResourceV2(writer, from, to, true); err != nil {
			return err
		}
	} else if err := WriteDelegatedResourceV2(writer, from, to, true, lockResource); err != nil {
		return err
	}
	return WriteDelegatedResourceV2(writer, from, to, false, unlockResource)
}

func WriteDelegationIndex(db ethdb.KeyValueWriter, from common.Address, receivers []common.Address) error {
	buf := make([]byte, common.AddressLength*len(receivers))
	for i, r := range receivers {
		copy(buf[i*common.AddressLength:], r[:])
	}
	return db.Put(delegationIndexKey(from[:]), buf)
}

func ReadDelegationIndex(db ethdb.KeyValueReader, from common.Address) []common.Address {
	data, err := db.Get(delegationIndexKey(from[:]))
	if err != nil || len(data) == 0 {
		return nil
	}
	addrs, err := decodeDelegationIndex(data)
	if err != nil {
		return nil
	}
	return addrs
}

// ReadDelegationIndexStrict returns the flat receiver index for a delegating
// account and surfaces storage/corruption errors. Missing rows return
// (nil, false, nil). A present empty index returns an empty slice with ok=true.
func ReadDelegationIndexStrict(db ethdb.KeyValueReader, from common.Address) ([]common.Address, bool, error) {
	data, ok, err := readPresentValue(db, delegationIndexKey(from[:]), fmt.Sprintf("delegation index %s", from.Hex()))
	if err != nil || !ok {
		return nil, ok, err
	}
	addrs, err := decodeDelegationIndex(data)
	if err != nil {
		return nil, true, err
	}
	return addrs, true, nil
}

func decodeDelegationIndex(data []byte) ([]common.Address, error) {
	if len(data)%common.AddressLength != 0 {
		return nil, fmt.Errorf("rawdb: decode delegation index: length %d, want multiple of %d", len(data), common.AddressLength)
	}
	count := len(data) / common.AddressLength
	addrs := make([]common.Address, count)
	for i := range addrs {
		copy(addrs[i][:], data[i*common.AddressLength:])
	}
	return addrs, nil
}

func DelegatedResourceStateKey(from, to common.Address) []byte {
	return delegationKey(from[:], to[:])
}

func DelegatedResourceV2StateKey(from, to common.Address, locked bool) []byte {
	return delegationKeyV2(from[:], to[:], locked)
}

func DelegationIndexStateKey(from common.Address) []byte {
	return delegationIndexKey(from[:])
}
