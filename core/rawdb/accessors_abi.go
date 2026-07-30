package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// WriteContractABI stores the ABI for a contract address in the dedicated
// ABI store. Mirrors java-tron AbiStore.put, activated when
// allow_account_asset_optimization is on. Callers must check the fork gate
// before writing; when the feature is off, ABI stays inline in the
// SmartContract proto (see contractPrefix "ct-").
func WriteContractABI(db ethdb.KeyValueWriter, contractAddr []byte, abi *contractpb.SmartContract_ABI) error {
	data, err := proto.Marshal(abi)
	if err != nil {
		return err
	}
	return db.Put(abiKey(contractAddr), data)
}

// ReadContractABI returns the ABI stored for a contract, or nil if absent.
func ReadContractABI(db ethdb.KeyValueReader, contractAddr []byte) *contractpb.SmartContract_ABI {
	data, err := db.Get(abiKey(contractAddr))
	if err != nil || len(data) == 0 {
		return nil
	}
	var abi contractpb.SmartContract_ABI
	if err := proto.Unmarshal(data, &abi); err != nil {
		return nil
	}
	return &abi
}

// ReadContractABIStrict returns the dedicated ABI entry for a contract and
// surfaces storage/corruption errors. Missing rows return (nil, false, nil).
// A present zero-byte protobuf is decoded as an empty ABI with ok=true.
func ReadContractABIStrict(db ethdb.KeyValueReader, contractAddr []byte) (*contractpb.SmartContract_ABI, bool, error) {
	data, ok, err := readPresentValue(db, abiKey(contractAddr), fmt.Sprintf("contract abi %x", contractAddr))
	if err != nil || !ok {
		return nil, ok, err
	}
	var abi contractpb.SmartContract_ABI
	if err := proto.Unmarshal(data, &abi); err != nil {
		return nil, true, fmt.Errorf("rawdb: decode contract abi %x: %w", contractAddr, err)
	}
	return &abi, true, nil
}

// HasContractABI reports whether a dedicated ABI entry exists for
// contractAddr. When false, the ABI (if any) is still inline in the
// SmartContract proto.
func HasContractABI(db ethdb.KeyValueReader, contractAddr []byte) bool {
	ok, _ := db.Has(abiKey(contractAddr))
	return ok
}

// DeleteContractABI removes the dedicated ABI entry (used when reverting a
// contract creation or clearing an ABI during state rollback).
func DeleteContractABI(db ethdb.KeyValueWriter, contractAddr []byte) error {
	return db.Delete(abiKey(contractAddr))
}
