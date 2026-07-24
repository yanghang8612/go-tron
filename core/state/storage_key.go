package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

const storageKeyPrefixBytes = 16

func javaStorageRowKey(addr tcommon.Address, key tcommon.Hash, meta *contractpb.SmartContract) tcommon.Hash {
	prefix, hashSlot := javaStorageKeyLayout(addr, meta)
	return storageRowKeyWithLayout(key, prefix, hashSlot)
}

func javaStorageKeyLayout(addr tcommon.Address, meta *contractpb.SmartContract) ([storageKeyPrefixBytes]byte, bool) {
	var seed [tcommon.AddressLength + tcommon.HashLength]byte
	copy(seed[:], addr[:])
	seedLen := len(addr)
	trxHash := meta.GetTrxHash()
	hasTrxHash := !isZeroBytes(trxHash)
	var addrSeed []byte
	if hasTrxHash && len(trxHash) > tcommon.HashLength {
		// Protocol hashes are 32 bytes, but preserve the historical behavior for
		// malformed/imported metadata instead of silently truncating its seed.
		addrSeed = make([]byte, len(addr)+len(trxHash))
		copy(addrSeed, addr[:])
		copy(addrSeed[len(addr):], trxHash)
	} else {
		if hasTrxHash {
			seedLen += copy(seed[seedLen:], trxHash)
		}
		addrSeed = seed[:seedLen]
	}
	addrHash := tcommon.Keccak256(addrSeed)
	var prefix [storageKeyPrefixBytes]byte
	copy(prefix[:], addrHash[:storageKeyPrefixBytes])
	return prefix, meta != nil && meta.GetVersion() == 1
}

func storageRowKeyWithLayout(key tcommon.Hash, prefix [storageKeyPrefixBytes]byte, hashSlot bool) tcommon.Hash {
	keyBytes := key.Bytes()
	if hashSlot {
		hashed := tcommon.Keccak256(keyBytes)
		keyBytes = hashed.Bytes()
	}

	var rowKey tcommon.Hash
	copy(rowKey[:storageKeyPrefixBytes], prefix[:])
	copy(rowKey[storageKeyPrefixBytes:], keyBytes[storageKeyPrefixBytes:])
	return rowKey
}

func (s *stateObject) storageRowKey(key tcommon.Hash, meta *contractpb.SmartContract) tcommon.Hash {
	if !s.storageKeyLayoutCached {
		s.storageKeyPrefix, s.storageKeyHashSlot = javaStorageKeyLayout(s.address, meta)
		s.storageKeyLayoutCached = true
	}
	return storageRowKeyWithLayout(key, s.storageKeyPrefix, s.storageKeyHashSlot)
}

func (s *stateObject) invalidateStorageKeyLayout() {
	s.storageKeyLayoutCached = false
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
