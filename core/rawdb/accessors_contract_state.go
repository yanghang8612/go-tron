package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// ReadContractState loads a per-contract dynamic-energy state. Returns
// nil if no record exists yet (caller should bootstrap with
// types.NewContractState(currentCycle)).
func ReadContractState(db ethdb.KeyValueReader, addr tcommon.Address) *types.ContractState {
	data, _ := db.Get(contractStateKey(addr.Bytes()))
	if len(data) == 0 {
		return nil
	}
	cs, err := types.NewContractStateFromBytes(data)
	if err != nil {
		return nil
	}
	return cs
}

// WriteContractState persists a per-contract dynamic-energy state.
func WriteContractState(db ethdb.KeyValueWriter, addr tcommon.Address, cs *types.ContractState) error {
	if cs == nil {
		return nil
	}
	data, err := cs.Bytes()
	if err != nil {
		return err
	}
	return db.Put(contractStateKey(addr.Bytes()), data)
}
