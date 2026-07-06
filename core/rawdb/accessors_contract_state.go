package rawdb

import (
	"fmt"

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

// ReadContractStateStrict loads a per-contract dynamic-energy state and
// surfaces storage/corruption errors. Missing rows return (nil, false, nil).
// A present zero-byte protobuf is decoded as an empty ContractState with
// ok=true.
func ReadContractStateStrict(db ethdb.KeyValueReader, addr tcommon.Address) (*types.ContractState, bool, error) {
	data, ok, err := readPresentValue(db, contractStateKey(addr.Bytes()), fmt.Sprintf("contract state %s", addr.Hex()))
	if err != nil || !ok {
		return nil, ok, err
	}
	cs, err := types.NewContractStateFromBytes(data)
	if err != nil {
		return nil, true, fmt.Errorf("rawdb: decode contract state %s: %w", addr.Hex(), err)
	}
	return cs, true, nil
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
