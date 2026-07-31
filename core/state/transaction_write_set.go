package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// TransactionWriteValue is the final value of one logical state path after a
// transaction. Exists distinguishes a deleted/missing cell from a present
// zero or empty value. Value is owned by the write set.
type TransactionWriteValue struct {
	Exists      bool
	Commutative bool
	Value       []byte
}

// TransactionWriteSet is the TRON counterpart of Erigon's typed WriteSet. It
// is extracted from the authoritative undo journal plus the inline access
// recorder after execution; it does not replace either source of truth.
type TransactionWriteSet map[TransactionAccessKey]TransactionWriteValue

func appendTransactionWriteKey(keys map[TransactionAccessKey]struct{}, key TransactionAccessKey) {
	if key.Kind != TransactionAccessUnknown {
		keys[key] = struct{}{}
	}
}

// CaptureTransactionWriteSet returns the final values of all logical paths
// written since journalMark. recorder supplies typed account and dynamic-
// property writes that the StateDB undo journal intentionally stores in a
// coarser shape. known is false if an unknown journal path is encountered.
func (s *StateDB) CaptureTransactionWriteSet(journalMark int, recorder *TransactionAccessRecorder, dynProps *DynamicProperties) (writes TransactionWriteSet, known bool, err error) {
	if s == nil {
		return nil, false, fmt.Errorf("capture transaction writes: nil state")
	}
	keys := make(map[TransactionAccessKey]struct{}, 16)
	modes := make(map[TransactionAccessKey]TransactionAccessMode, 16)
	if recorder != nil {
		recorder.Visit(func(key TransactionAccessKey, mode TransactionAccessMode) bool {
			if mode&(TransactionAccessWrite|TransactionAccessCommutativeWrite) != 0 {
				appendTransactionWriteKey(keys, key)
				modes[key] |= mode
			}
			return true
		})
	}
	known = s.VisitTransactionAccessWritesSince(journalMark, func(key TransactionAccessKey) bool {
		// accountScalarChange is deliberately coarse in the undo journal. When
		// the inline recorder identified every scalar write, retain only those
		// typed paths. Including the full Account post-image here would make an
		// otherwise independent worker depend on unrelated fields changed by an
		// earlier transaction in the block.
		if key.Kind == TransactionAccessAccount && recorder != nil {
			full, fields := recorder.AccountWriteCoverage(key.Address)
			if fields && !full {
				return true
			}
		}
		appendTransactionWriteKey(keys, key)
		return true
	})
	if !known {
		return nil, false, nil
	}
	writes = make(TransactionWriteSet, len(keys))
	for key := range keys {
		if key.Kind == TransactionAccessRawKV {
			value, ok := recorder.rawKVWrite(key)
			if !ok {
				return nil, false, fmt.Errorf("capture raw kv %x: missing post-image", key.LogicalKey)
			}
			writes[key] = value
			continue
		}
		mode := modes[key]
		if mode&TransactionAccessWrite == 0 && mode&TransactionAccessCommutativeWrite != 0 {
			if delta, ok := recorder.CommutativeDelta(key); ok {
				value := int64TransactionWriteValue(delta)
				value.Commutative = true
				writes[key] = value
				continue
			}
		}
		value, valueErr := s.transactionWriteValue(key, dynProps)
		if valueErr != nil {
			return nil, false, valueErr
		}
		writes[key] = value
	}
	return writes, true, nil
}

func ownedTransactionWriteValue(exists bool, value []byte) TransactionWriteValue {
	return TransactionWriteValue{Exists: exists, Value: append([]byte(nil), value...)}
}

func int64TransactionWriteValue(value int64) TransactionWriteValue {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	return ownedTransactionWriteValue(true, encoded[:])
}

func uint64TransactionWriteValue(value uint64) TransactionWriteValue {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return ownedTransactionWriteValue(true, encoded[:])
}

func boolTransactionWriteValue(value bool) TransactionWriteValue {
	if value {
		return ownedTransactionWriteValue(true, []byte{1})
	}
	return ownedTransactionWriteValue(true, []byte{0})
}

func (s *StateDB) transactionWriteValue(key TransactionAccessKey, dynProps *DynamicProperties) (TransactionWriteValue, error) {
	switch key.Kind {
	case TransactionAccessAccount:
		obj := s.getStateObject(key.Address)
		if obj == nil || obj.deleted {
			return TransactionWriteValue{}, nil
		}
		encoded, exists, err := encodeAccountLatestObject(obj, true)
		if err != nil {
			return TransactionWriteValue{}, fmt.Errorf("capture account %s: %w", key.Address.Hex(), err)
		}
		return ownedTransactionWriteValue(exists, encoded), nil
	case TransactionAccessAccountField:
		return s.accountFieldTransactionWriteValue(key.Address, key.AccountField)
	case TransactionAccessWitness:
		witness := s.GetWitness(key.Address)
		if witness == nil {
			return TransactionWriteValue{}, nil
		}
		encoded, err := witness.Marshal()
		if err != nil {
			return TransactionWriteValue{}, fmt.Errorf("capture witness %s: %w", key.Address.Hex(), err)
		}
		return ownedTransactionWriteValue(true, encoded), nil
	case TransactionAccessStorage:
		value, exists := s.GetStateWithExist(key.Address, key.StorageKey)
		return ownedTransactionWriteValue(exists, value[:]), nil
	case TransactionAccessCode:
		code := s.GetCode(key.Address)
		return ownedTransactionWriteValue(len(code) != 0, code), nil
	case TransactionAccessContractMetadata:
		encoded, exists, err := s.GetContractMetadataBytes(key.Address)
		if err != nil {
			return TransactionWriteValue{}, fmt.Errorf("capture contract metadata %s: %w", key.Address.Hex(), err)
		}
		return ownedTransactionWriteValue(exists, encoded), nil
	case TransactionAccessAccountKV:
		value, exists, err := s.GetAccountKV(key.Address, key.KVDomain, []byte(key.LogicalKey))
		if err != nil {
			return TransactionWriteValue{}, fmt.Errorf("capture account kv %s/%d/%x: %w", key.Address.Hex(), key.KVDomain, key.LogicalKey, err)
		}
		return ownedTransactionWriteValue(exists, value), nil
	case TransactionAccessAccountKVGeneration:
		obj := s.getStateObject(key.Address)
		if obj != nil && !obj.deleted {
			return uint64TransactionWriteValue(obj.accountKVGeneration), nil
		}
		generation, exists, err := s.readStateKVGeneration(key.Address)
		if err != nil {
			return TransactionWriteValue{}, fmt.Errorf("capture account kv generation %s: %w", key.Address.Hex(), err)
		}
		if !exists {
			return TransactionWriteValue{}, nil
		}
		return uint64TransactionWriteValue(generation), nil
	case TransactionAccessSelfDestruct:
		return boolTransactionWriteValue(s.HasSelfDestructed(key.Address)), nil
	case TransactionAccessTransientStorage:
		value := s.GetTransientState(key.Address, key.StorageKey)
		return ownedTransactionWriteValue(value != (tcommon.Hash{}), value[:]), nil
	case TransactionAccessDynamicInt:
		if dynProps == nil {
			return TransactionWriteValue{}, fmt.Errorf("capture dynamic int %q: nil properties", key.LogicalKey)
		}
		value, exists := dynProps.props[key.LogicalKey]
		if !exists {
			return TransactionWriteValue{}, nil
		}
		return int64TransactionWriteValue(value), nil
	case TransactionAccessDynamicString:
		if dynProps == nil {
			return TransactionWriteValue{}, fmt.Errorf("capture dynamic string %q: nil properties", key.LogicalKey)
		}
		value, exists := dynProps.stringProps[key.LogicalKey]
		return ownedTransactionWriteValue(exists, []byte(value)), nil
	case TransactionAccessDynamicHash:
		if dynProps == nil {
			return TransactionWriteValue{}, fmt.Errorf("capture dynamic hash: nil properties")
		}
		return ownedTransactionWriteValue(true, dynProps.latestBlockHeaderHash[:]), nil
	default:
		return TransactionWriteValue{}, fmt.Errorf("capture transaction writes: unsupported kind %d", key.Kind)
	}
}

func (s *StateDB) accountFieldTransactionWriteValue(address tcommon.Address, field TransactionAccountField) (TransactionWriteValue, error) {
	obj := s.getStateObject(address)
	if obj == nil || obj.deleted || obj.account == nil {
		return TransactionWriteValue{}, nil
	}
	account := obj.account
	switch field {
	case TransactionAccountFieldExistence:
		return ownedTransactionWriteValue(true, nil), nil
	case TransactionAccountFieldAccountType:
		return int64TransactionWriteValue(int64(account.Type())), nil
	case TransactionAccountFieldBalance:
		return int64TransactionWriteValue(account.Balance()), nil
	case TransactionAccountFieldAllowance:
		return int64TransactionWriteValue(account.Allowance()), nil
	case TransactionAccountFieldLatestWithdrawTime:
		return int64TransactionWriteValue(account.LatestWithdrawTime()), nil
	case TransactionAccountFieldNetUsage:
		return int64TransactionWriteValue(account.NetUsage()), nil
	case TransactionAccountFieldLatestOperationTime:
		return int64TransactionWriteValue(account.LatestOperationTime()), nil
	case TransactionAccountFieldLatestConsumeTime:
		return int64TransactionWriteValue(account.LatestConsumeTime()), nil
	case TransactionAccountFieldFreeNetUsage:
		return int64TransactionWriteValue(account.FreeNetUsage()), nil
	case TransactionAccountFieldLatestConsumeFreeTime:
		return int64TransactionWriteValue(account.LatestConsumeFreeTime()), nil
	case TransactionAccountFieldNetWindow:
		var encoded [9]byte
		binary.BigEndian.PutUint64(encoded[:8], uint64(account.RawNetWindowSize()))
		if account.NetWindowOptimized() {
			encoded[8] = 1
		}
		return ownedTransactionWriteValue(true, encoded[:]), nil
	case TransactionAccountFieldFrozenResource:
		// Resource state is currently read-only at this typed path; mutations
		// are represented by their split AccountKV/full-account paths. Refuse a
		// future unmodelled write instead of publishing an ambiguous value.
		return TransactionWriteValue{}, fmt.Errorf("capture account %s: unsupported writable resource field", address.Hex())
	default:
		return TransactionWriteValue{}, fmt.Errorf("capture account %s: unsupported field %d", address.Hex(), field)
	}
}

// EqualTransactionWriteSets compares logical path identity, presence and final
// bytes. It is allocation-free and deliberately independent of map order.
func EqualTransactionWriteSets(left, right TransactionWriteSet) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		rightValue, ok := right[key]
		if !ok || leftValue.Exists != rightValue.Exists || leftValue.Commutative != rightValue.Commutative || !bytes.Equal(leftValue.Value, rightValue.Value) {
			return false
		}
	}
	return true
}
