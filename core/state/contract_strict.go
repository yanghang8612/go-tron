package state

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

var (
	stateCodeStrictObjectHitCounter      = metrics.NewRegisteredCounter("state/code_strict/source/object", nil)
	stateCodeStrictObjectPromoteCounter  = metrics.NewRegisteredCounter("state/code_strict/object_promotions", nil)
	stateCodeStrictObjectVerifiedCounter = metrics.NewRegisteredCounter("state/code_strict/object_verified_matches", nil)
	stateCodeStrictObjectHashCounter     = metrics.NewRegisteredCounter("state/code_strict/object_hash_checks", nil)
	stateCodeStrictSharedCacheHitCounter = metrics.NewRegisteredCounter("state/code_strict/source/shared_cache", nil)
	stateCodeStrictHotHitCounter         = metrics.NewRegisteredCounter("state/code_strict/source/hot", nil)
	stateCodeStrictHotMissCounter        = metrics.NewRegisteredCounter("state/code_strict/hot_misses", nil)
	stateCodeStrictColdHitCounter        = metrics.NewRegisteredCounter("state/code_strict/source/cold", nil)
	stateCodeStrictFinalMissCounter      = metrics.NewRegisteredCounter("state/code_strict/final_misses", nil)
	stateCodeStrictErrorCounter          = metrics.NewRegisteredCounter("state/code_strict/errors", nil)
)

// GetStateStrict is the archive/query counterpart of GetState. It preserves
// live overlay semantics while surfacing malformed contract metadata, corrupt
// storage values, and backend errors instead of degrading them to a zero slot.
func (s *StateDB) GetStateStrict(addr tcommon.Address, key tcommon.Hash) (tcommon.Hash, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return tcommon.Hash{}, nil
	}
	context := fmt.Sprintf("live storage value generation %d", obj.accountKVGeneration)
	read := func(domain kvdomains.KVDomain, row []byte) ([]byte, bool, error) {
		if domain == kvdomains.ContractStorage && len(row) == tcommon.HashLength {
			if slot, ok := obj.storage[key]; ok {
				return bytes.Clone(slot.value[:]), true, nil
			}
		}
		return s.getAccountKVForDecoding(addr, domain, row)
	}
	if obj.contractMeta != nil || obj.contractMetaDirty {
		return readStorageRowsWithMetadata(addr, key, context, obj.contractMeta, read)
	}
	return readStorageRows(addr, key, context, read)
}

// GetCodeStrict returns verified live bytecode. A database-owned positive cache
// may satisfy the immutable content hash directly; on a cache miss, durable and
// cold-history storage errors are preserved.
func (s *StateDB) GetCodeStrict(addr tcommon.Address) ([]byte, error) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil, nil
	}
	if obj.code != nil {
		if s.matchesCachedStateCode(obj.codeHash, obj.code) {
			stateCodeStrictObjectVerifiedCounter.Inc(1)
			stateCodeStrictObjectHitCounter.Inc(1)
			return obj.code, nil
		}
		if obj.codeHash != (tcommon.Hash{}) {
			stateCodeStrictObjectHashCounter.Inc(1)
			if actual := tcommon.Keccak256(obj.code); actual != obj.codeHash {
				stateCodeCacheRejectCounter.Inc(1)
				stateCodeStrictErrorCounter.Inc(1)
				stateCodeDiagnosticContext{state: s, address: addr, addressKnown: true, source: "strict_object"}.report(obj.codeHash, actual, len(obj.code))
				return nil, fmt.Errorf("cached contract runtime code hash mismatch contract=%s codeHash=%s", addr.Hex(), obj.codeHash.Hex())
			}
		}
		stateCodeStrictObjectHitCounter.Inc(1)
		if store := s.getStateCodeStore(); store != nil && s.admitVerifiedStateCode(obj.codeHash, obj.code, store) {
			stateCodeStrictObjectPromoteCounter.Inc(1)
		}
		return obj.code, nil
	}
	if obj.codeDirty || obj.codeHash == (tcommon.Hash{}) {
		return nil, nil
	}
	code, ok, err := s.readStateCodeStrictWithContext(obj.codeHash, stateCodeDiagnosticContext{state: s, address: addr, addressKnown: true, source: "strict_hot"})
	if err != nil {
		stateCodeStrictErrorCounter.Inc(1)
		return nil, err
	}
	if ok {
		obj.code = code
		return code, nil
	}
	if s.codeColdHistory != nil {
		if code, ok, err := s.codeColdHistory.GetCodeAtOrBefore(obj.codeHash, s.codeColdTxNum); err != nil {
			stateCodeStrictErrorCounter.Inc(1)
			return nil, err
		} else if ok && len(code) > 0 {
			if actual := tcommon.Keccak256(code); actual != obj.codeHash {
				stateCodeCacheRejectCounter.Inc(1)
				stateCodeStrictErrorCounter.Inc(1)
				stateCodeDiagnosticContext{state: s, address: addr, addressKnown: true, source: "strict_cold"}.report(obj.codeHash, actual, len(code))
				return nil, fmt.Errorf("cold contract runtime code hash mismatch contract=%s codeHash=%s", addr.Hex(), obj.codeHash.Hex())
			}
			stateCodeStrictColdHitCounter.Inc(1)
			obj.code = append([]byte(nil), code...)
			if store := s.getStateCodeStore(); store != nil {
				s.admitVerifiedStateCode(obj.codeHash, obj.code, store)
			}
			return obj.code, nil
		}
	}
	stateCodeStrictFinalMissCounter.Inc(1)
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
