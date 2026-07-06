package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func WriteAccount(db ethdb.KeyValueWriter, addr common.Address, acc *types.Account) {
	data, err := acc.Marshal()
	if err != nil {
		return
	}
	db.Put(accountKey(addr.Bytes()), data)
}

func ReadAccount(db ethdb.KeyValueReader, addr common.Address) *types.Account {
	data, err := db.Get(accountKey(addr.Bytes()))
	if err != nil {
		return nil
	}
	acc, err := types.UnmarshalAccount(data)
	if err != nil {
		return nil
	}
	return acc
}

// ReadAccountStrict returns the account row for addr and surfaces
// storage/corruption errors. Missing rows return (nil, false, nil). A present
// zero-byte protobuf decodes as an empty account with ok=true.
func ReadAccountStrict(db ethdb.KeyValueReader, addr common.Address) (*types.Account, bool, error) {
	data, ok, err := readPresentValue(db, accountKey(addr.Bytes()), fmt.Sprintf("account %s", addr.Hex()))
	if err != nil || !ok {
		return nil, ok, err
	}
	acc, err := types.UnmarshalAccount(data)
	if err != nil {
		return nil, true, fmt.Errorf("rawdb: decode account %s: %w", addr.Hex(), err)
	}
	return acc, true, nil
}

func DeleteAccount(db ethdb.KeyValueWriter, addr common.Address) {
	db.Delete(accountKey(addr.Bytes()))
}

func HasAccount(db ethdb.KeyValueReader, addr common.Address) bool {
	has, _ := db.Has(accountKey(addr.Bytes()))
	return has
}

// HasAccountStrict reports whether an account row exists and surfaces storage
// errors.
func HasAccountStrict(db ethdb.KeyValueReader, addr common.Address) (bool, error) {
	return readKeyPresence(db, accountKey(addr.Bytes()), fmt.Sprintf("account %s", addr.Hex()))
}

func WriteWitness(db ethdb.KeyValueWriter, addr common.Address, w *types.Witness) {
	data, err := w.Marshal()
	if err != nil {
		return
	}
	db.Put(witnessKey(addr.Bytes()), data)
}

func ReadWitness(db ethdb.KeyValueReader, addr common.Address) *types.Witness {
	data, err := db.Get(witnessKey(addr.Bytes()))
	if err != nil {
		return nil
	}
	w, err := types.UnmarshalWitness(data)
	if err != nil {
		return nil
	}
	return w
}

// ReadWitnessStrict returns the witness row for addr and surfaces
// storage/corruption errors. Missing rows return (nil, false, nil). A present
// zero-byte protobuf decodes as an empty witness with ok=true.
func ReadWitnessStrict(db ethdb.KeyValueReader, addr common.Address) (*types.Witness, bool, error) {
	data, ok, err := readPresentValue(db, witnessKey(addr.Bytes()), fmt.Sprintf("witness %s", addr.Hex()))
	if err != nil || !ok {
		return nil, ok, err
	}
	w, err := types.UnmarshalWitness(data)
	if err != nil {
		return nil, true, fmt.Errorf("rawdb: decode witness %s: %w", addr.Hex(), err)
	}
	return w, true, nil
}

// WitnessCapsuleStateKey exposes the legacy witness key bytes for the native
// typed StateDB witness store. The key shape stays centralized in rawdb/schema.
func WitnessCapsuleStateKey(addr common.Address) []byte {
	return witnessKey(addr.Bytes())
}
