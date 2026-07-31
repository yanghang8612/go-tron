package state

import (
	"encoding/binary"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// TransactionRawKVWriter is the publication capability needed by raw Context.DB
// post-images. Blockbuffer.Buffer and ordinary ethdb writers satisfy it.
type TransactionRawKVWriter interface {
	Put(key, value []byte) error
	Delete(key []byte) error
}

// ValidateTransactionWriteSetApply rejects write families whose ordered
// publication semantics are not implemented yet. It validates the complete
// set before ApplyTransactionWriteSet mutates anything, so unsupported worker
// results can fall back to serial execution without a partial publication.
func ValidateTransactionWriteSetApply(writes TransactionWriteSet, dynProps *DynamicProperties, raw TransactionRawKVWriter) error {
	for key, value := range writes {
		if err := validateTransactionWriteApply(key, value, dynProps, raw); err != nil {
			return err
		}
	}
	for key, value := range writes {
		if key.Kind != TransactionAccessAccount {
			continue
		}
		envelope, _, err := decodeTransactionAccountCreate(key, value)
		if err != nil {
			return err
		}
		codeKey := TransactionAccessKey{Kind: TransactionAccessCode, Address: key.Address}
		code, hasCode := writes[codeKey]
		if hasCode && envelope.CodeHash != tcommon.Keccak256(code.Value) {
			return fmt.Errorf("apply transaction writes: account %s code hash does not match code post-image", key.Address.Hex())
		}
		if !hasCode && envelope.CodeHash != (tcommon.Hash{}) {
			return fmt.Errorf("apply transaction writes: account %s has code hash without code post-image", key.Address.Hex())
		}
	}
	return nil
}

func validateTransactionWriteApply(key TransactionAccessKey, value TransactionWriteValue, dynProps *DynamicProperties, raw TransactionRawKVWriter) error {
	if value.Commutative && key.Kind != TransactionAccessAccountField && key.Kind != TransactionAccessDynamicInt {
		return fmt.Errorf("apply transaction writes: commutative kind %d", key.Kind)
	}
	if value.Commutative && key.Kind == TransactionAccessAccountField && key.AccountField != TransactionAccountFieldBalance {
		return fmt.Errorf("apply transaction writes: commutative account field %d", key.AccountField)
	}
	switch key.Kind {
	case TransactionAccessAccount:
		_, _, err := decodeTransactionAccountCreate(key, value)
		return err
	case TransactionAccessAccountField:
		if !value.Exists {
			return fmt.Errorf("apply transaction writes: missing account field %d for %s", key.AccountField, key.Address.Hex())
		}
		switch key.AccountField {
		case TransactionAccountFieldAccountType,
			TransactionAccountFieldBalance,
			TransactionAccountFieldAllowance,
			TransactionAccountFieldLatestWithdrawTime,
			TransactionAccountFieldNetUsage,
			TransactionAccountFieldLatestOperationTime,
			TransactionAccountFieldLatestConsumeTime,
			TransactionAccountFieldFreeNetUsage,
			TransactionAccountFieldLatestConsumeFreeTime:
			if len(value.Value) != 8 {
				return fmt.Errorf("apply transaction writes: account field %d has %d bytes", key.AccountField, len(value.Value))
			}
			if key.AccountField == TransactionAccountFieldAccountType {
				encoded := transactionWriteInt64(value)
				if int64(int32(encoded)) != encoded {
					return fmt.Errorf("apply transaction writes: account type %d overflows int32", encoded)
				}
				if _, ok := corepb.AccountType_name[int32(encoded)]; !ok {
					return fmt.Errorf("apply transaction writes: account type %d is unknown", encoded)
				}
			}
		case TransactionAccountFieldNetWindow:
			if len(value.Value) != 9 || value.Value[8] > 1 {
				return fmt.Errorf("apply transaction writes: net window has invalid %d-byte value", len(value.Value))
			}
		default:
			return fmt.Errorf("apply transaction writes: account field %d is not supported", key.AccountField)
		}
	case TransactionAccessWitness:
		if !value.Exists {
			return fmt.Errorf("apply transaction writes: witness deletion is not supported")
		}
		witness, err := types.UnmarshalWitness(value.Value)
		if err != nil {
			return fmt.Errorf("apply transaction writes: witness %s: %w", key.Address.Hex(), err)
		}
		if witness.Address() != key.Address {
			return fmt.Errorf("apply transaction writes: witness key %s contains %s", key.Address.Hex(), witness.Address().Hex())
		}
	case TransactionAccessStorage, TransactionAccessTransientStorage:
		if len(value.Value) != tcommon.HashLength {
			return fmt.Errorf("apply transaction writes: hash value has %d bytes", len(value.Value))
		}
	case TransactionAccessCode:
		if !value.Exists && len(value.Value) != 0 {
			return fmt.Errorf("apply transaction writes: absent code has a value")
		}
	case TransactionAccessContractMetadata:
		if value.Exists {
			var contract contractpb.SmartContract
			if err := proto.Unmarshal(value.Value, &contract); err != nil {
				return fmt.Errorf("apply transaction writes: contract %s: %w", key.Address.Hex(), err)
			}
		}
	case TransactionAccessAccountKV:
		// Presence is represented directly by Set/DeleteAccountKV.
	case TransactionAccessAccountKVGeneration:
		return fmt.Errorf("apply transaction writes: account KV generation reset is not supported")
	case TransactionAccessSelfDestruct:
		return fmt.Errorf("apply transaction writes: self-destruct requires full account deletion support")
	case TransactionAccessDynamicInt:
		if dynProps == nil {
			return fmt.Errorf("apply transaction writes: nil dynamic properties")
		}
		if !value.Exists || len(value.Value) != 8 {
			return fmt.Errorf("apply transaction writes: dynamic int %q has invalid value", key.LogicalKey)
		}
	case TransactionAccessDynamicString:
		if dynProps == nil {
			return fmt.Errorf("apply transaction writes: nil dynamic properties")
		}
		if !value.Exists {
			return fmt.Errorf("apply transaction writes: dynamic string deletion is not supported")
		}
	case TransactionAccessDynamicHash:
		if dynProps == nil {
			return fmt.Errorf("apply transaction writes: nil dynamic properties")
		}
		if !value.Exists || len(value.Value) != tcommon.HashLength {
			return fmt.Errorf("apply transaction writes: dynamic hash has invalid value")
		}
	case TransactionAccessRawKV:
		if raw == nil {
			return fmt.Errorf("apply transaction writes: raw KV writer is nil")
		}
	default:
		return fmt.Errorf("apply transaction writes: kind %d is not supported", key.Kind)
	}
	return nil
}

func decodeTransactionAccountCreate(key TransactionAccessKey, value TransactionWriteValue) (*StateAccountV3, *types.Account, error) {
	if !value.Exists {
		return nil, nil, fmt.Errorf("apply transaction writes: account deletion %s is not supported", key.Address.Hex())
	}
	envelope, err := DecodeStateAccountV3(value.Value)
	if err != nil {
		return nil, nil, fmt.Errorf("apply transaction writes: account %s: %w", key.Address.Hex(), err)
	}
	if envelope.AccountKVRoot != EmptyKVRoot {
		return nil, nil, fmt.Errorf("apply transaction writes: account %s has non-flat KV root", key.Address.Hex())
	}
	if envelope.AccountKVGeneration != 0 {
		return nil, nil, fmt.Errorf("apply transaction writes: account %s generation %d is not a fresh creation", key.Address.Hex(), envelope.AccountKVGeneration)
	}
	account, err := types.UnmarshalAccount(envelope.AccountProto)
	if err != nil {
		return nil, nil, fmt.Errorf("apply transaction writes: account %s proto: %w", key.Address.Hex(), err)
	}
	if account.Address() != key.Address {
		return nil, nil, fmt.Errorf("apply transaction writes: account key %s contains %s", key.Address.Hex(), account.Address().Hex())
	}
	return envelope, account, nil
}

// ApplyTransactionWriteSet publishes validated typed post-images into target
// state. Commutative settlement values are signed deltas and are applied to the
// target's current ordered value; ordinary values replace the exact logical
// cell. The caller owns transaction ordering and calls FinalizeTransaction at
// the same boundary as normal execution.
func (s *StateDB) ApplyTransactionWriteSet(writes TransactionWriteSet, dynProps *DynamicProperties, raw TransactionRawKVWriter) error {
	return s.applyTransactionWriteSet(writes, dynProps, raw, nil)
}

// ApplyTransactionWriteSetRecorded is the discard-shadow verification form of
// ApplyTransactionWriteSet. It records the mutations produced by the applier
// so callers can extract and compare a second WriteSet. Canonical publication
// uses ApplyTransactionWriteSet and does not record itself as another task.
func (s *StateDB) ApplyTransactionWriteSetRecorded(writes TransactionWriteSet, dynProps *DynamicProperties, raw TransactionRawKVWriter, recorder *TransactionAccessRecorder) error {
	if recorder == nil {
		return fmt.Errorf("apply transaction writes: nil verification recorder")
	}
	return s.applyTransactionWriteSet(writes, dynProps, raw, recorder)
}

// ValidateTransactionWriteSetApply adds state-aware creation checks to the
// schema preflight. Full-account post-images are accepted only as fresh
// absent-to-present creations; replacement, deletion, and reincarnation stay
// on the serial path.
func (s *StateDB) ValidateTransactionWriteSetApply(writes TransactionWriteSet, dynProps *DynamicProperties, raw TransactionRawKVWriter) error {
	if s == nil {
		return fmt.Errorf("apply transaction writes: nil state")
	}
	if err := ValidateTransactionWriteSetApply(writes, dynProps, raw); err != nil {
		return err
	}
	var creates map[tcommon.Address]struct{}
	for key := range writes {
		if key.Kind != TransactionAccessAccount {
			continue
		}
		if obj := s.getStateObjectWithoutAccess(key.Address); obj != nil {
			return fmt.Errorf("apply transaction writes: full account replacement %s is not supported", key.Address.Hex())
		}
		if creates == nil {
			creates = make(map[tcommon.Address]struct{}, 1)
		}
		creates[key.Address] = struct{}{}
	}
	for key := range writes {
		switch key.Kind {
		case TransactionAccessAccountField,
			TransactionAccessWitness,
			TransactionAccessStorage,
			TransactionAccessCode,
			TransactionAccessContractMetadata,
			TransactionAccessAccountKV,
			TransactionAccessTransientStorage:
			if obj := s.getStateObjectWithoutAccess(key.Address); obj != nil && !obj.deleted {
				continue
			}
			if _, creating := creates[key.Address]; !creating {
				return fmt.Errorf("apply transaction writes: %s requires an existing or created account", key.Address.Hex())
			}
		}
	}
	return nil
}

func (s *StateDB) applyTransactionWriteSet(writes TransactionWriteSet, dynProps *DynamicProperties, raw TransactionRawKVWriter, recorder *TransactionAccessRecorder) error {
	if s == nil {
		return fmt.Errorf("apply transaction writes: nil state")
	}
	if err := s.ValidateTransactionWriteSetApply(writes, dynProps, raw); err != nil {
		return err
	}
	previousStateRecorder := s.transactionAccess
	var previousDynamicRecorder *TransactionAccessRecorder
	if dynProps != nil {
		previousDynamicRecorder = dynProps.transactionAccess
	}
	s.SetTransactionAccessRecorder(recorder)
	if dynProps != nil {
		dynProps.SetTransactionAccessRecorder(recorder)
	}
	defer func() {
		s.SetTransactionAccessRecorder(previousStateRecorder)
		if dynProps != nil {
			dynProps.SetTransactionAccessRecorder(previousDynamicRecorder)
		}
	}()

	for key, value := range writes {
		if key.Kind != TransactionAccessAccount {
			continue
		}
		recordTransactionWriteApply(recorder, key, value)
		if err := s.applyTransactionWrite(key, value, dynProps, raw); err != nil {
			return err
		}
	}
	for key, value := range writes {
		if key.Kind == TransactionAccessAccount {
			continue
		}
		recordTransactionWriteApply(recorder, key, value)
		if err := s.applyTransactionWrite(key, value, dynProps, raw); err != nil {
			return err
		}
	}
	return nil
}

// recordTransactionWriteApply retains the publication intent even when the
// final post-image equals the ordered baseline and the underlying setter
// correctly performs no mutation. CaptureTransactionWriteSet still reads the
// resulting logical value, so an incorrect no-op remains a value mismatch.
func recordTransactionWriteApply(recorder *TransactionAccessRecorder, key TransactionAccessKey, value TransactionWriteValue) {
	if recorder == nil || value.Commutative {
		return
	}
	if key.Kind == TransactionAccessRawKV {
		if value.Exists {
			recorder.RecordRawKVPut([]byte(key.LogicalKey), value.Value)
		} else {
			recorder.RecordRawKVDelete([]byte(key.LogicalKey))
		}
		return
	}
	recorder.record(key, TransactionAccessWrite)
}

func transactionWriteInt64(value TransactionWriteValue) int64 {
	return int64(binary.BigEndian.Uint64(value.Value))
}

func (s *StateDB) applyTransactionWrite(key TransactionAccessKey, value TransactionWriteValue, dynProps *DynamicProperties, raw TransactionRawKVWriter) error {
	switch key.Kind {
	case TransactionAccessAccount:
		return s.applyTransactionAccountCreate(key, value)
	case TransactionAccessAccountField:
		return s.applyTransactionAccountField(key, value)
	case TransactionAccessWitness:
		witness, _ := types.UnmarshalWitness(value.Value)
		return s.SetWitnessCapsule(witness)
	case TransactionAccessStorage:
		s.SetState(key.Address, key.StorageKey, tcommon.BytesToHash(value.Value))
	case TransactionAccessCode:
		s.SetCode(key.Address, value.Value)
	case TransactionAccessContractMetadata:
		if !value.Exists {
			s.SetContract(key.Address, nil)
			break
		}
		var contract contractpb.SmartContract
		_ = proto.Unmarshal(value.Value, &contract)
		s.SetContract(key.Address, &contract)
	case TransactionAccessAccountKV:
		logicalKey := []byte(key.LogicalKey)
		if value.Exists {
			return s.SetAccountKV(key.Address, key.KVDomain, logicalKey, value.Value)
		}
		return s.DeleteAccountKV(key.Address, key.KVDomain, logicalKey)
	case TransactionAccessTransientStorage:
		s.SetTransientState(key.Address, key.StorageKey, tcommon.BytesToHash(value.Value))
	case TransactionAccessDynamicInt:
		if dynProps == nil {
			return fmt.Errorf("apply transaction writes: nil dynamic properties")
		}
		amount := transactionWriteInt64(value)
		if value.Commutative {
			if dynProps.transactionAccess != nil {
				dynProps.addCommutativeInt(key.LogicalKey, amount)
				break
			}
			amount += dynProps.readInt(key.LogicalKey)
		}
		dynProps.Set(key.LogicalKey, amount)
	case TransactionAccessDynamicString:
		if dynProps == nil {
			return fmt.Errorf("apply transaction writes: nil dynamic properties")
		}
		dynProps.SetString(key.LogicalKey, string(value.Value))
	case TransactionAccessDynamicHash:
		if dynProps == nil {
			return fmt.Errorf("apply transaction writes: nil dynamic properties")
		}
		dynProps.SetLatestBlockHeaderHash(tcommon.BytesToHash(value.Value))
	case TransactionAccessRawKV:
		keyBytes := []byte(key.LogicalKey)
		if value.Exists {
			return raw.Put(keyBytes, value.Value)
		}
		return raw.Delete(keyBytes)
	default:
		return fmt.Errorf("apply transaction writes: validated kind %d has no applier", key.Kind)
	}
	return nil
}

func (s *StateDB) applyTransactionAccountCreate(key TransactionAccessKey, value TransactionWriteValue) error {
	envelope, account, err := decodeTransactionAccountCreate(key, value)
	if err != nil {
		return err
	}
	obj := s.getOrCreateAccountLoaded(key.Address, nil)
	obj.account = account
	obj.accountProto = envelope.AccountProto
	obj.accountProtoLoaded = true
	obj.accountKVRoot = envelope.AccountKVRoot
	obj.accountKVGeneration = envelope.AccountKVGeneration
	obj.accountKVGenerationDirty = false
	obj.codeHash = envelope.CodeHash
	return nil
}

func (s *StateDB) applyTransactionAccountField(key TransactionAccessKey, value TransactionWriteValue) error {
	amount := int64(0)
	if len(value.Value) >= 8 {
		amount = transactionWriteInt64(value)
	}
	switch key.AccountField {
	case TransactionAccountFieldAccountType:
		s.SetAccountType(key.Address, corepb.AccountType(amount))
	case TransactionAccountFieldBalance:
		if value.Commutative {
			if s.transactionAccess != nil {
				s.AddSettlementBalance(key.Address, amount)
			} else {
				s.AddBalance(key.Address, amount)
			}
		} else {
			s.AddBalance(key.Address, amount-s.GetBalance(key.Address))
		}
	case TransactionAccountFieldAllowance:
		s.SetAllowance(key.Address, amount)
	case TransactionAccountFieldLatestWithdrawTime:
		s.SetLatestWithdrawTime(key.Address, amount)
	case TransactionAccountFieldNetUsage:
		s.SetNetUsage(key.Address, amount)
	case TransactionAccountFieldLatestOperationTime:
		s.SetLatestOperationTime(key.Address, amount)
	case TransactionAccountFieldLatestConsumeTime:
		s.SetLatestConsumeTime(key.Address, amount)
	case TransactionAccountFieldFreeNetUsage:
		s.SetFreeNetUsage(key.Address, amount)
	case TransactionAccountFieldLatestConsumeFreeTime:
		s.SetLatestConsumeFreeTime(key.Address, amount)
	case TransactionAccountFieldNetWindow:
		s.SetNetWindow(key.Address, amount, value.Value[8] == 1)
	default:
		return fmt.Errorf("apply transaction writes: validated account field %d has no applier", key.AccountField)
	}
	return nil
}
