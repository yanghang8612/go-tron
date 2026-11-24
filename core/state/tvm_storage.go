package state

import (
	"bytes"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
)

// TVMStorage tracks a transaction's ownership and existence of storage rows.
// Its mutations use the StateDB journal so a failed child restores its parent view.
type TVMStorage struct {
	Optimize           bool
	ProtectLegacyReads bool
	owners             map[storageOwnerKey]tcommon.Hash
	slots              map[string]tvmStorageSlot
}

type storageOwnerKey struct {
	address tcommon.Address
	row     tcommon.Hash
}

type tvmStorageSlot struct {
	value          tcommon.Hash
	exists         bool
	preserveLegacy bool
	legacyFound    bool
}

type tvmStorageOwnerChange struct {
	storage *TVMStorage
	key     storageOwnerKey
}

func (c tvmStorageOwnerChange) revert(_ map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	delete(c.storage.owners, c.key)
}

type tvmStorageSlotChange struct {
	storage *TVMStorage
	key     string
	prev    tvmStorageSlot
	hadPrev bool
}

func (c tvmStorageSlotChange) revert(_ map[tcommon.Address]*stateObject, _ map[tcommon.Address]*types.Witness) {
	if c.hadPrev {
		c.storage.slots[c.key] = c.prev
	} else {
		delete(c.storage.slots, c.key)
	}
}

func (v *TVMStorage) own(s *StateDB, key storageOwnerKey, slot tcommon.Hash) {
	if _, ok := v.owners[key]; ok {
		return
	}
	if v.owners == nil {
		v.owners = make(map[storageOwnerKey]tcommon.Hash)
	}
	v.owners[key] = slot
	s.journal.append(tvmStorageOwnerChange{storage: v, key: key})
}

func (v *TVMStorage) cache(s *StateDB, key string, slot tvmStorageSlot) {
	if v.slots == nil {
		v.slots = make(map[string]tvmStorageSlot)
	}
	prev, hadPrev := v.slots[key]
	v.slots[key] = slot
	s.journal.append(tvmStorageSlotChange{storage: v, key: key, prev: prev, hadPrev: hadPrev})
}

// Read returns the slot, whether a storage row contributes to SSTORE energy,
// and whether java-tron's storage alias guard requires OUT_OF_TIME.
func (v *TVMStorage) Read(s *StateDB, addr tcommon.Address, slot tcommon.Hash) (tcommon.Hash, bool, bool, error) {
	legacy := s.storageRowKey(addr, slot)
	ownerKey := storageOwnerKey{address: addr, row: legacy}
	owner, owned := v.owners[ownerKey]
	taken := owned && owner != slot
	if !v.Optimize {
		if taken {
			return tcommon.Hash{}, false, true, nil
		}
		v.own(s, ownerKey, slot)
		value, exists := s.GetStateWithExist(addr, slot)
		return value, exists, false, nil
	}

	meta := s.GetContract(addr)
	newKey := OptimizedStorageRowKey(addr, slot, meta)
	cacheKey := string(newKey)
	if cached, ok := v.slots[cacheKey]; ok {
		return cached.value, cached.exists, false, nil
	}
	raw, found, err := s.getAccountKVForDecoding(addr, kvdomains.ContractStorage, newKey)
	if err != nil {
		return tcommon.Hash{}, false, false, err
	}
	if found {
		value, err := decodeStorageValueHash("read optimized contract storage", raw)
		if err != nil {
			return tcommon.Hash{}, false, false, s.recordStateError("read optimized contract storage", err)
		}
		exists := value != (tcommon.Hash{})
		if exists {
			v.own(s, ownerKey, slot)
		}
		v.cache(s, cacheKey, tvmStorageSlot{value: value, exists: exists, preserveLegacy: true})
		return value, exists, false, nil
	}
	if taken {
		if !v.ProtectLegacyReads {
			return tcommon.Hash{}, false, false, nil
		}
		// Java leaves the underlying legacy row intact until root repository
		// commit. Preserve that evidence after a journaled migration staged
		// earlier in this execution removed the row from our dirty overlay.
		ownerRow := OptimizedStorageRowKey(addr, owner, meta)
		if v.slots[string(ownerRow)].legacyFound {
			return tcommon.Hash{}, false, true, nil
		}
	}
	raw, found, err = s.getAccountKVForDecoding(addr, kvdomains.ContractStorage, legacy[:])
	if err != nil {
		return tcommon.Hash{}, false, false, err
	}
	if taken {
		return tcommon.Hash{}, false, found, nil
	}
	if !found {
		v.cache(s, cacheKey, tvmStorageSlot{preserveLegacy: true})
		return tcommon.Hash{}, false, false, nil
	}
	value, err := decodeStorageValueHash("read legacy contract storage", raw)
	if err != nil {
		return tcommon.Hash{}, false, false, s.recordStateError("read legacy contract storage", err)
	}
	v.own(s, ownerKey, slot)
	if !v.ProtectLegacyReads {
		if err := writeOptimizedStorage(s, addr, newKey, legacy, value, false); err != nil {
			return tcommon.Hash{}, false, false, err
		}
	}
	v.cache(s, cacheKey, tvmStorageSlot{value: value, exists: true, legacyFound: true})
	return value, true, false, nil
}

// Write is called after the corresponding Read used to price SSTORE.
func (v *TVMStorage) Write(s *StateDB, addr tcommon.Address, slot, value tcommon.Hash) error {
	if !v.Optimize {
		s.SetState(addr, slot, value)
		return nil
	}
	legacy := s.storageRowKey(addr, slot)
	newKey := OptimizedStorageRowKey(addr, slot, s.GetContract(addr))
	cached := v.slots[string(newKey)]
	v.own(s, storageOwnerKey{address: addr, row: legacy}, slot)
	v.cache(s, string(newKey), tvmStorageSlot{value: value, exists: true, preserveLegacy: cached.preserveLegacy, legacyFound: cached.legacyFound})
	return writeOptimizedStorage(s, addr, newKey, legacy, value, cached.preserveLegacy)
}

func writeOptimizedStorage(s *StateDB, addr tcommon.Address, key []byte, legacy, value tcommon.Hash, preserveLegacy bool) error {
	if err := s.SetAccountKV(addr, kvdomains.ContractStorage, key, value[:]); err != nil {
		return err
	}
	if !preserveLegacy {
		return s.DeleteAccountKV(addr, kvdomains.ContractStorage, legacy[:])
	}
	return nil
}

// LegacyTVMStorageRowKey resolves a logical slot against current contract metadata.
func (s *StateDB) LegacyTVMStorageRowKey(addr tcommon.Address, slot tcommon.Hash) tcommon.Hash {
	return s.storageRowKey(addr, slot)
}

// ReadLegacyTVMStorageRow reads the backing row before the current TVM frame's
// slot cache is published. Present zero values remain distinguishable from misses.
func (s *StateDB) ReadLegacyTVMStorageRow(addr tcommon.Address, slot, key tcommon.Hash) (tcommon.Hash, []byte, bool, error) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil || obj.deleted {
		return tcommon.Hash{}, nil, false, nil
	}
	if _, physical := lookupKVEntry(obj.kvDirty, kvdomains.ContractStorage, key[:]); !physical {
		if _, dirty := obj.dirtyStorage[slot]; dirty {
			s.recordStorageRead(addr, slot)
			row := obj.storage[slot]
			// The logical overlay is not the physical before-image.
			return row.value, nil, row.exists, nil
		}
	}
	raw, exists, err := s.getAccountKVForDecodingFromObject(obj, addr, kvdomains.ContractStorage, key[:])
	if err != nil || !exists {
		return tcommon.Hash{}, nil, exists, err
	}
	value, err := decodeStorageValueHash("read legacy TVM row", raw)
	if err != nil {
		return tcommon.Hash{}, nil, false, s.recordStateError("read legacy TVM row", err)
	}
	return value, bytes.Clone(raw), true, nil
}

// WriteLegacyTVMStorageRow publishes a physical row at the root VM commit.
// The optional before-image carries exact bytes captured by the normal SLOAD.
func (s *StateDB) WriteLegacyTVMStorageRow(addr tcommon.Address, key, value tcommon.Hash, before []byte) error {
	if err := s.Error(); err != nil {
		return err
	}
	if value == (tcommon.Hash{}) {
		return s.DeleteAccountKV(addr, kvdomains.ContractStorage, key[:])
	}
	if before != nil {
		err := s.setAccountKVWithPrev(addr, kvdomains.ContractStorage, key[:], value[:], true, before, true, true)
		if err != nil {
			return s.recordStateError("write legacy TVM row", err)
		}
		return nil
	}
	return s.SetAccountKV(addr, kvdomains.ContractStorage, key[:], value[:])
}
