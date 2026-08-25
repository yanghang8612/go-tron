package state

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// GetStateStrict is the archive/query counterpart of GetState. It preserves
// live overlay semantics while surfacing malformed contract metadata, corrupt
// storage values, and backend errors instead of degrading them to a zero slot.
func (s *StateDB) GetStateStrict(addr tcommon.Address, key tcommon.Hash) (tcommon.Hash, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return tcommon.Hash{}, nil
	}
	if slot, ok := obj.storage[key]; ok {
		return slot.value, nil
	}
	if obj.created {
		return tcommon.Hash{}, nil
	}

	var runtime ContractRuntimeMetadata
	if obj.contractMeta != nil || obj.contractMetaDirty {
		var ok bool
		runtime, ok = contractRuntimeMetadataFromProto(addr, obj.contractMeta)
		if !ok {
			runtime.storageKeyPrefix, runtime.storageKeyHashSlot = javaStorageKeyLayoutFields(addr, nil, 0)
		}
	} else {
		data, ok, err := s.getAccountKVForDecoding(addr, kvdomains.ContractMetadata, contractMetaKVKey)
		if err != nil {
			return tcommon.Hash{}, err
		}
		if ok && len(data) > 0 {
			runtime, err = decodeContractRuntimeMetadata(addr, data)
			if err != nil {
				return tcommon.Hash{}, fmt.Errorf("decode contract metadata for storage key %s generation %d: %w", addr.Hex(), obj.accountKVGeneration, err)
			}
		} else {
			runtime.storageKeyPrefix, runtime.storageKeyHashSlot = javaStorageKeyLayoutFields(addr, nil, 0)
		}
	}
	rowKey := storageRowKeyWithLayout(key, runtime.storageKeyPrefix, runtime.storageKeyHashSlot)
	raw, ok, err := s.getAccountKVForDecoding(addr, kvdomains.ContractStorage, rowKey[:])
	if err != nil {
		return tcommon.Hash{}, err
	}
	if !ok || len(raw) == 0 {
		obj.cacheStorageSlot(key, storageSlot{})
		return tcommon.Hash{}, nil
	}
	h, err := decodeStorageValueHash("live storage value", raw)
	if err != nil {
		return tcommon.Hash{}, err
	}
	obj.cacheStorageSlot(key, storageSlot{value: h, exists: h != (tcommon.Hash{})})
	return h, nil
}

// GetCodeStrict returns verified live bytecode. A database-owned positive cache
// may satisfy the immutable content hash directly; on a cache miss, durable and
// cold-history storage errors are preserved.
func (s *StateDB) GetCodeStrict(addr tcommon.Address) ([]byte, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil, nil
	}
	if obj.code != nil || obj.codeDirty || obj.codeHash == (tcommon.Hash{}) {
		return obj.code, nil
	}
	code, ok, err := s.readStateCodeStrict(obj.codeHash)
	if err != nil {
		return nil, err
	}
	if ok {
		obj.code = code
		return code, nil
	}
	if s.codeColdHistory != nil {
		if code, ok, err := s.codeColdHistory.GetCodeAtOrBefore(obj.codeHash, s.codeColdTxNum); err != nil {
			return nil, err
		} else if ok && len(code) > 0 {
			obj.code = append([]byte(nil), code...)
			if store := s.getStateCodeStore(); store != nil {
				s.admitStateCode(obj.codeHash, obj.code, store)
			}
			return obj.code, nil
		}
	}
	return nil, nil
}

func (s *StateDB) ReadContractStateStrict(addr tcommon.Address) (*types.ContractState, bool, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil, false, nil
	}
	data, ok, err := s.GetAccountKV(addr, kvdomains.ContractRuntimeState, contractStateKVKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	pb := new(contractpb.ContractState)
	if err := statecodec.Unmarshal(data, pb); err != nil {
		return nil, true, fmt.Errorf("decode contract state %s: %w", addr.Hex(), err)
	}
	return types.NewContractStateFromPB(pb), true, nil
}

func (s *StateDB) ReadContractABIStrict(addr tcommon.Address) (*contractpb.SmartContract_ABI, bool, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil, false, nil
	}
	data, ok, err := s.GetAccountKV(addr, kvdomains.ContractABI, contractABIKVKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	var abi contractpb.SmartContract_ABI
	if err := statecodec.Unmarshal(data, &abi); err != nil {
		return nil, true, fmt.Errorf("decode contract abi %s: %w", addr.Hex(), err)
	}
	return &abi, true, nil
}
