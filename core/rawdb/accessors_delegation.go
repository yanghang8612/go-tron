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

func ReadDelegatedResource(db ethdb.KeyValueReader, from, to common.Address) *DelegatedResource {
	data, err := db.Get(delegationKey(from[:], to[:]))
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
	return db.Delete(delegationKey(from[:], to[:]))
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
