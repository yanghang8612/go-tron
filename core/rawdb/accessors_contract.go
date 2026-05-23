package rawdb

import (
	"bytes"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

func codeKey(addr []byte) []byte {
	return append(append([]byte{}, codePrefix...), addr...)
}

func contractKey(addr []byte) []byte {
	return append(append([]byte{}, contractPrefix...), addr...)
}

func storageKey(addr, key []byte) []byte {
	k := make([]byte, 0, len(storagePrefix)+len(addr)+len(key))
	k = append(k, storagePrefix...)
	k = append(k, addr...)
	k = append(k, key...)
	return k
}

func WriteCode(db ethdb.KeyValueWriter, addr common.Address, code []byte) {
	db.Put(codeKey(addr.Bytes()), code)
}

func ReadCode(db ethdb.KeyValueReader, addr common.Address) []byte {
	data, err := db.Get(codeKey(addr.Bytes()))
	if err != nil {
		return nil
	}
	return data
}

func WriteContract(db ethdb.KeyValueWriter, addr common.Address, data []byte) {
	db.Put(contractKey(addr.Bytes()), data)
}

func ReadContract(db ethdb.KeyValueReader, addr common.Address) []byte {
	data, err := db.Get(contractKey(addr.Bytes()))
	if err != nil {
		return nil
	}
	return data
}

func DeleteContract(db ethdb.KeyValueWriter, addr common.Address) {
	db.Delete(contractKey(addr.Bytes()))
}

func WriteStorage(db ethdb.KeyValueWriter, addr common.Address, key common.Hash, value []byte) {
	db.Put(storageKey(addr.Bytes(), key.Bytes()), value)
}

func DeleteStorage(db ethdb.KeyValueWriter, addr common.Address, key common.Hash) {
	db.Delete(storageKey(addr.Bytes(), key.Bytes()))
}

func ReadStorage(db ethdb.KeyValueReader, addr common.Address, key common.Hash) []byte {
	data, err := db.Get(storageKey(addr.Bytes(), key.Bytes()))
	if err != nil {
		return nil
	}
	return data
}

func DeleteCode(db ethdb.KeyValueWriter, addr common.Address) {
	db.Delete(codeKey(addr.Bytes()))
}

// ParseLegacyCodeKey decodes an old address-keyed CodeStore key.
func ParseLegacyCodeKey(key []byte) (common.Address, bool) {
	if len(key) != len(codePrefix)+common.AddressLength || !bytes.HasPrefix(key, codePrefix) {
		return common.Address{}, false
	}
	return common.BytesToAddress(key[len(codePrefix):]), true
}
