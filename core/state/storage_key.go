package state

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const storageKeyPrefixBytes = 16

func javaStorageRowKey(addr tcommon.Address, key tcommon.Hash, meta *contractpb.SmartContract) tcommon.Hash {
	prefix, hashSlot := javaStorageKeyLayout(addr, meta)
	return storageRowKeyWithLayout(key, prefix, hashSlot)
}

// LegacyStorageRowKey returns the historical 32-byte java-tron row key.
func LegacyStorageRowKey(addr tcommon.Address, key tcommon.Hash, meta *contractpb.SmartContract) tcommon.Hash {
	return javaStorageRowKey(addr, key, meta)
}

const OptimizedStorageRowKeyLength = storageKeyPrefixBytes + tcommon.HashLength

// OptimizedStorageRowKey returns the java-tron 48-byte storage migration key.
func OptimizedStorageRowKey(addr tcommon.Address, key tcommon.Hash, meta *contractpb.SmartContract) []byte {
	addrHash := javaStorageAddressHash(addr, meta.GetTrxHash())
	var seed [2 * tcommon.HashLength]byte
	copy(seed[:], addrHash[:])
	copy(seed[tcommon.HashLength:], key[:])
	slotHash := tcommon.Keccak256(seed[:])
	var row [OptimizedStorageRowKeyLength]byte
	copy(row[:storageKeyPrefixBytes], addrHash[:storageKeyPrefixBytes])
	copy(row[storageKeyPrefixBytes:], slotHash[:])
	return row[:]
}

func javaStorageKeyLayout(addr tcommon.Address, meta *contractpb.SmartContract) ([storageKeyPrefixBytes]byte, bool) {
	if meta == nil {
		return javaStorageKeyLayoutFields(addr, nil, 0)
	}
	return javaStorageKeyLayoutFields(addr, meta.GetTrxHash(), meta.GetVersion())
}

func javaStorageKeyLayoutFields(addr tcommon.Address, trxHash []byte, version int32) ([storageKeyPrefixBytes]byte, bool) {
	addrHash := javaStorageAddressHash(addr, trxHash)
	var prefix [storageKeyPrefixBytes]byte
	copy(prefix[:], addrHash[:storageKeyPrefixBytes])
	return prefix, version == 1
}

func javaStorageAddressHash(addr tcommon.Address, trxHash []byte) tcommon.Hash {
	var seed [tcommon.AddressLength + tcommon.HashLength]byte
	copy(seed[:], addr[:])
	seedLen := len(addr)
	// Java treats any nonempty trx_hash as an address salt.
	hasTrxHash := len(trxHash) != 0
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
	return tcommon.Keccak256(addrSeed)
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
		if err := statecodec.Unmarshal(data, &sc); err != nil {
			return tcommon.Hash{}, fmt.Errorf("decode contract metadata for storage key %s generation %d: %w", addr.Hex(), generation, err)
		}
		meta = &sc
	}
	return javaStorageRowKey(addr, key, meta), nil
}

func decodeStorageValueHash(context string, raw []byte) (tcommon.Hash, error) {
	if len(raw) > tcommon.HashLength {
		return tcommon.Hash{}, fmt.Errorf("%s: storage value length %d, want <= %d", context, len(raw), tcommon.HashLength)
	}
	var h tcommon.Hash
	copy(h[tcommon.HashLength-len(raw):], raw)
	return h, nil
}
