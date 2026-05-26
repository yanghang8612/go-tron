package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

const storageKeyPrefixBytes = 16

func javaStorageRowKey(addr tcommon.Address, key tcommon.Hash, meta *contractpb.SmartContract) tcommon.Hash {
	keyBytes := key.Bytes()
	if meta != nil && meta.GetVersion() == 1 {
		hashed := tcommon.Keccak256(keyBytes)
		keyBytes = hashed.Bytes()
	}

	addrSeed := addr.Bytes()
	if meta != nil && !isZeroBytes(meta.GetTrxHash()) {
		addrSeed = append(append([]byte{}, addrSeed...), meta.GetTrxHash()...)
	}
	addrHash := tcommon.Keccak256(addrSeed)

	var rowKey tcommon.Hash
	copy(rowKey[:storageKeyPrefixBytes], addrHash[:storageKeyPrefixBytes])
	copy(rowKey[storageKeyPrefixBytes:], keyBytes[storageKeyPrefixBytes:])
	return rowKey
}

func storageRowKeyFromFlatLatest(latest accountKVLatestGenerationReader, addr tcommon.Address, generation uint64, key tcommon.Hash) (tcommon.Hash, error) {
	var meta *contractpb.SmartContract
	if latest == nil {
		return javaStorageRowKey(addr, key, nil), nil
	}
	if data, ok, err := latest.KVLatest(addr, generation, kvdomains.ContractMetadata, contractMetaKVKey); err != nil {
		return tcommon.Hash{}, err
	} else if ok && len(data) > 0 {
		var sc contractpb.SmartContract
		if err := proto.Unmarshal(data, &sc); err == nil {
			meta = &sc
		}
	}
	return javaStorageRowKey(addr, key, meta), nil
}

func isZeroBytes(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
