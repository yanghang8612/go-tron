package rawdb

import (
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

func DeleteAccount(db ethdb.KeyValueWriter, addr common.Address) {
	db.Delete(accountKey(addr.Bytes()))
}

func HasAccount(db ethdb.KeyValueReader, addr common.Address) bool {
	has, _ := db.Has(accountKey(addr.Bytes()))
	return has
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

// WitnessCapsuleStateKey exposes the legacy witness key bytes for the native
// typed StateDB witness store. The key shape stays centralized in rawdb/schema.
func WitnessCapsuleStateKey(addr common.Address) []byte {
	return witnessKey(addr.Bytes())
}
