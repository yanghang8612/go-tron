package rawdb

import (
	"encoding/json"

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

func ReadDelegatedResourceLegacy(db ethdb.KeyValueReader, from, to common.Address) *DelegatedResource {
	return readDelegatedResourceByKey(db, delegationKey(from[:], to[:]))
}

func ReadDelegatedResourceV2(db ethdb.KeyValueReader, from, to common.Address, locked bool) *DelegatedResource {
	return readDelegatedResourceByKey(db, delegationKeyV2(from[:], to[:], locked))
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
	count := len(data) / common.AddressLength
	addrs := make([]common.Address, count)
	for i := range addrs {
		copy(addrs[i][:], data[i*common.AddressLength:])
	}
	return addrs
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
