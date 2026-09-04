package state

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
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
	_, err := validateTransactionWriteSetApplySchema(writes, dynProps, raw)
	return err
}

func validateTransactionWriteSetApplySchema(writes TransactionWriteSet, dynProps *DynamicProperties, raw TransactionRawKVWriter) ([]TransactionAccessKey, error) {
	if err := validateTransactionWritePhysicalAliases(writes); err != nil {
		return nil, err
	}
	orderedKeys := sortedTransactionWriteKeys(writes)
	for _, key := range orderedKeys {
		value := writes[key]
		if err := validateTransactionWriteApply(key, value, dynProps, raw); err != nil {
			return nil, err
		}
	}
	for _, key := range orderedKeys {
		if key.Kind != TransactionAccessAccount {
			continue
		}
		value := writes[key]
		envelope, account, err := decodeTransactionAccountCreate(key, value)
		if err != nil {
			return nil, err
		}
		codeKey := TransactionAccessKey{Kind: TransactionAccessCode, Address: key.Address}
		code, hasCode := writes[codeKey]
		if hasCode && envelope.CodeHash != tcommon.Keccak256(code.Value) {
			return nil, fmt.Errorf("apply transaction writes: account %s code hash does not match code post-image", key.Address.Hex())
		}
		if !hasCode && envelope.CodeHash != (tcommon.Hash{}) {
			return nil, fmt.Errorf("apply transaction writes: account %s has code hash without code post-image", key.Address.Hex())
		}
		if err := validateTransactionAccountFieldPostImages(writes, key.Address, account); err != nil {
			return nil, err
		}
	}
	return orderedKeys, nil
}

// validateTransactionAccountFieldPostImages proves that a fresh account's
// coarse envelope and any exact scalar post-images describe one state. The
// deterministic applier installs the envelope first and the fields second;
// without this check, a contradictory set would mutate state before the
// recorded post-apply comparison rejected it. A commutative delta is never
// valid here because the full envelope already contains the final value.
func validateTransactionAccountFieldPostImages(writes TransactionWriteSet, address tcommon.Address, account *types.Account) error {
	fields := [...]TransactionAccountField{
		TransactionAccountFieldAccountType,
		TransactionAccountFieldBalance,
		TransactionAccountFieldAllowance,
		TransactionAccountFieldLatestWithdrawTime,
		TransactionAccountFieldNetUsage,
		TransactionAccountFieldLatestOperationTime,
		TransactionAccountFieldLatestConsumeTime,
		TransactionAccountFieldFreeNetUsage,
		TransactionAccountFieldLatestConsumeFreeTime,
		TransactionAccountFieldNetWindow,
	}
	for _, field := range fields {
		value, exists := writes[TransactionAccessKey{Kind: TransactionAccessAccountField, Address: address, AccountField: field}]
		if !exists {
			continue
		}
		if value.Commutative {
			return fmt.Errorf("apply transaction writes: account %s full post-image contains commutative field %d", address.Hex(), field)
		}
		got := transactionWriteInt64(value)
		var want int64
		switch field {
		case TransactionAccountFieldAccountType:
			want = int64(account.Type())
		case TransactionAccountFieldBalance:
			want = account.Balance()
		case TransactionAccountFieldAllowance:
			want = account.Allowance()
		case TransactionAccountFieldLatestWithdrawTime:
			want = account.LatestWithdrawTime()
		case TransactionAccountFieldNetUsage:
			want = account.NetUsage()
		case TransactionAccountFieldLatestOperationTime:
			want = account.LatestOperationTime()
		case TransactionAccountFieldLatestConsumeTime:
			want = account.LatestConsumeTime()
		case TransactionAccountFieldFreeNetUsage:
			want = account.FreeNetUsage()
		case TransactionAccountFieldLatestConsumeFreeTime:
			want = account.LatestConsumeFreeTime()
		case TransactionAccountFieldNetWindow:
			want = account.RawNetWindowSize()
		}
		if got != want {
			return fmt.Errorf("apply transaction writes: account %s field %d conflicts with full post-image", address.Hex(), field)
		}
		if field == TransactionAccountFieldNetWindow && (value.Value[8] == 1) != account.NetWindowOptimized() {
			return fmt.Errorf("apply transaction writes: account %s net-window mode conflicts with full post-image", address.Hex())
		}
	}
	return nil
}

// validateTransactionWritePhysicalAliases rejects logical sets whose mutation
// order or physical ownership cannot be reconstructed from a map. A serial
// transaction has an execution order; TransactionWriteSet intentionally does
// not. Publishing one of these combinations could therefore produce the same
// logical post-images while staging different rooted rows.
func validateTransactionWritePhysicalAliases(writes TransactionWriteSet) error {
	type addressFamilies struct {
		metadata bool
		storage  bool
	}
	var byAddress map[tcommon.Address]addressFamilies
	for key := range writes {
		if err := validateTransactionWriteKeyShape(key); err != nil {
			return err
		}
		switch key.Kind {
		case TransactionAccessContractMetadata:
			if byAddress == nil {
				byAddress = make(map[tcommon.Address]addressFamilies, 1)
			}
			families := byAddress[key.Address]
			families.metadata = true
			byAddress[key.Address] = families
		case TransactionAccessStorage:
			if byAddress == nil {
				byAddress = make(map[tcommon.Address]addressFamilies, 1)
			}
			families := byAddress[key.Address]
			families.storage = true
			byAddress[key.Address] = families
		case TransactionAccessAccountKV:
			switch key.KVDomain {
			case kvdomains.ContractMetadata:
				return fmt.Errorf("apply transaction writes: contract metadata account-KV is owned by the typed cache")
			case kvdomains.ContractStorage:
				return fmt.Errorf("apply transaction writes: contract storage account-KV is owned by the typed cache")
			case kvdomains.WitnessCapsule:
				return fmt.Errorf("apply transaction writes: witness capsule account-KV is owned by the typed cache")
			case kvdomains.SystemDynamicProperty:
				if tcommon.IsSystemAccount(key.Address) {
					return fmt.Errorf("apply transaction writes: dynamic-property account-KV is owned by the typed cache")
				}
			}
		case TransactionAccessDynamicInt, TransactionAccessDynamicString, TransactionAccessDynamicHash:
			for _, otherKind := range [...]TransactionAccessKind{
				TransactionAccessDynamicInt,
				TransactionAccessDynamicString,
				TransactionAccessDynamicHash,
			} {
				if otherKind == key.Kind {
					continue
				}
				if _, exists := writes[TransactionAccessKey{Kind: otherKind, LogicalKey: key.LogicalKey}]; exists {
					return fmt.Errorf("apply transaction writes: dynamic property %q has conflicting types %d and %d", key.LogicalKey, key.Kind, otherKind)
				}
			}
			switch key.Kind {
			case TransactionAccessDynamicInt:
				if _, stringTyped := defaultStringProps[key.LogicalKey]; stringTyped || key.LogicalKey == "latest_block_header_hash" {
					return fmt.Errorf("apply transaction writes: dynamic property %q is not int-typed", key.LogicalKey)
				}
			case TransactionAccessDynamicString:
				if _, intTyped := defaultProps[key.LogicalKey]; intTyped || key.LogicalKey == "latest_block_header_hash" {
					return fmt.Errorf("apply transaction writes: dynamic property %q is not string-typed", key.LogicalKey)
				}
			}
		case TransactionAccessRawKV:
			if rawdb.IsProtectedStateMutationKey([]byte(key.LogicalKey)) {
				return fmt.Errorf("apply transaction writes: protected state key cannot be published through raw KV")
			}
		}
	}
	for address, families := range byAddress {
		switch {
		case families.metadata && families.storage:
			return fmt.Errorf("apply transaction writes: contract %s metadata+storage order is not representable", address.Hex())
		}
	}
	return nil
}

// validateTransactionWriteKeyShape makes TransactionAccessKey a canonical
// tagged union. Mutators ignore fields that do not belong to their kind; if
// those fields were allowed to carry junk, a map could contain two distinct
// logical keys that both target one physical cell and the later post-apply
// capture could represent only one of them.
func validateTransactionWriteKeyShape(key TransactionAccessKey) error {
	zeroAddress := key.Address == (tcommon.Address{})
	zeroField := key.AccountField == TransactionAccountFieldUnknown
	zeroDomain := key.KVDomain == 0
	zeroStorage := key.StorageKey == (tcommon.Hash{})
	emptyLogical := key.LogicalKey == ""
	addressOnly := zeroField && zeroDomain && zeroStorage && emptyLogical
	addressScoped := func() error {
		// Rooted state strips the first byte into AccountID. Accepting an
		// internal WriteSet with a non-canonical prefix would let two distinct
		// StateDB cache keys collapse onto one physical owner.
		if !key.Address.ValidPrefix() {
			return fmt.Errorf("apply transaction writes: address %s has non-canonical prefix", key.Address.Hex())
		}
		return nil
	}
	invalid := func() error {
		return fmt.Errorf("apply transaction writes: kind %d has non-canonical key fields", key.Kind)
	}

	switch key.Kind {
	case TransactionAccessAccount,
		TransactionAccessWitness,
		TransactionAccessCode,
		TransactionAccessContractMetadata,
		TransactionAccessAccountKVGeneration,
		TransactionAccessSelfDestruct:
		if !addressOnly {
			return invalid()
		}
		return addressScoped()
	case TransactionAccessAccountField:
		if key.AccountField == TransactionAccountFieldUnknown || !zeroDomain || !zeroStorage || !emptyLogical {
			return invalid()
		}
		return addressScoped()
	case TransactionAccessStorage, TransactionAccessTransientStorage:
		if !zeroField || !zeroDomain || !emptyLogical {
			return invalid()
		}
		return addressScoped()
	case TransactionAccessAccountKV:
		if !zeroField || !zeroStorage {
			return invalid()
		}
		return addressScoped()
	case TransactionAccessDynamicInt, TransactionAccessDynamicString, TransactionAccessDynamicHash, TransactionAccessRawKV:
		if !zeroAddress || !zeroField || !zeroDomain || !zeroStorage || emptyLogical {
			return invalid()
		}
	default:
		return invalid()
	}
	return nil
}

func transactionWriteApplyRank(kind TransactionAccessKind) int {
	switch kind {
	case TransactionAccessAccount:
		return 0
	case TransactionAccessContractMetadata:
		return 1
	case TransactionAccessCode:
		return 2
	case TransactionAccessAccountField:
		return 3
	case TransactionAccessWitness:
		return 4
	case TransactionAccessAccountKV:
		return 5
	case TransactionAccessStorage:
		return 6
	case TransactionAccessTransientStorage:
		return 7
	case TransactionAccessDynamicInt, TransactionAccessDynamicString, TransactionAccessDynamicHash:
		return 8
	case TransactionAccessRawKV:
		return 9
	default:
		return 10
	}
}

func sortedTransactionWriteKeys(writes TransactionWriteSet) []TransactionAccessKey {
	keys := make([]TransactionAccessKey, 0, len(writes))
	for key := range writes {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(left, right TransactionAccessKey) int {
		if order := cmp.Compare(transactionWriteApplyRank(left.Kind), transactionWriteApplyRank(right.Kind)); order != 0 {
			return order
		}
		// Some kinds intentionally share an apply rank. Kind is still part of
		// the logical identity and must break ties before the remaining fields;
		// otherwise malformed same-name dynamic keys inherit random map order.
		if order := cmp.Compare(left.Kind, right.Kind); order != 0 {
			return order
		}
		if order := bytes.Compare(left.Address[:], right.Address[:]); order != 0 {
			return order
		}
		if order := cmp.Compare(left.AccountField, right.AccountField); order != 0 {
			return order
		}
		if order := cmp.Compare(left.KVDomain, right.KVDomain); order != 0 {
			return order
		}
		if order := bytes.Compare(left.StorageKey[:], right.StorageKey[:]); order != 0 {
			return order
		}
		return cmp.Compare(left.LogicalKey, right.LogicalKey)
	})
	return keys
}

func validateTransactionWriteApply(key TransactionAccessKey, value TransactionWriteValue, dynProps *DynamicProperties, raw TransactionRawKVWriter) error {
	if value.Commutative && key.Kind != TransactionAccessAccountField && key.Kind != TransactionAccessDynamicInt {
		return fmt.Errorf("apply transaction writes: commutative kind %d", key.Kind)
	}
	if value.Commutative && key.Kind == TransactionAccessAccountField && key.AccountField != TransactionAccountFieldBalance {
		return fmt.Errorf("apply transaction writes: commutative account field %d", key.AccountField)
	}
	if value.Commutative && key.Kind == TransactionAccessDynamicInt && !transactionDynamicIntIsCommutative(key.LogicalKey) {
		return fmt.Errorf("apply transaction writes: dynamic int %q is not a commutative settlement", key.LogicalKey)
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
		if value.Commutative && transactionWriteInt64(value) <= 0 {
			return fmt.Errorf("apply transaction writes: commutative balance delta is not positive")
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
	case TransactionAccessStorage:
		if len(value.Value) != tcommon.HashLength {
			return fmt.Errorf("apply transaction writes: hash value has %d bytes", len(value.Value))
		}
		// FinalizeTransaction removes zero-valued storage rows before a
		// publication WriteSet is captured. Exists must therefore be the exact
		// inverse of the zero post-image; the applier itself consumes only the
		// hash and cannot preserve a contradictory presence bit.
		zero := tcommon.BytesToHash(value.Value) == (tcommon.Hash{})
		if value.Exists == zero {
			return fmt.Errorf("apply transaction writes: storage presence does not match value")
		}
	case TransactionAccessTransientStorage:
		if len(value.Value) != tcommon.HashLength {
			return fmt.Errorf("apply transaction writes: transient hash value has %d bytes", len(value.Value))
		}
		// Transient storage is cleared by FinalizeTransaction before capture,
		// so the only canonical transaction-boundary post-image is absent zero.
		// Accepting anything else would be applied and then silently discarded.
		if value.Exists || tcommon.BytesToHash(value.Value) != (tcommon.Hash{}) {
			return fmt.Errorf("apply transaction writes: transient storage survives transaction boundary")
		}
	case TransactionAccessCode:
		if value.Exists != (len(value.Value) != 0) {
			return fmt.Errorf("apply transaction writes: code presence does not match value")
		}
	case TransactionAccessContractMetadata:
		if value.Exists {
			var contract contractpb.SmartContract
			if err := proto.Unmarshal(value.Value, &contract); err != nil {
				return fmt.Errorf("apply transaction writes: contract %s: %w", key.Address.Hex(), err)
			}
			if !bytes.Equal(contract.ContractAddress, key.Address.Bytes()) {
				return fmt.Errorf("apply transaction writes: contract key %s contains %x", key.Address.Hex(), contract.ContractAddress)
			}
		} else if len(value.Value) != 0 {
			return fmt.Errorf("apply transaction writes: absent contract metadata has a value")
		}
	case TransactionAccessAccountKV:
		if !kvdomains.IsRegistered(key.KVDomain) {
			return fmt.Errorf("apply transaction writes: account KV domain %#04x is not registered", uint16(key.KVDomain))
		}
		if !value.Exists && len(value.Value) != 0 {
			return fmt.Errorf("apply transaction writes: absent account KV has a value")
		}
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
		if value.Commutative && transactionWriteInt64(value) <= 0 {
			return fmt.Errorf("apply transaction writes: commutative dynamic int %q delta is not positive", key.LogicalKey)
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
		if key.LogicalKey != "latest_block_header_hash" {
			return fmt.Errorf("apply transaction writes: dynamic hash %q is not supported", key.LogicalKey)
		}
	case TransactionAccessRawKV:
		if raw == nil {
			return fmt.Errorf("apply transaction writes: raw KV writer is nil")
		}
		if !value.Exists && len(value.Value) != 0 {
			return fmt.Errorf("apply transaction writes: absent raw KV has a value")
		}
	default:
		return fmt.Errorf("apply transaction writes: kind %d is not supported", key.Kind)
	}
	return nil
}

func transactionDynamicIntIsCommutative(key string) bool {
	switch key {
	case "burn_trx_amount",
		"transaction_fee_pool",
		"total_transaction_cost",
		"total_create_account_cost",
		"total_create_witness_cost":
		return true
	default:
		return false
	}
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
	account, err := types.UnmarshalAccountStorageCoreV4(envelope.AccountProto)
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
// so callers can extract and compare a second WriteSet. Hardened canonical
// publication uses this form as its immediate post-apply proof; private worker
// prefix advancement may use the non-recording form.
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
	_, err := s.validateTransactionWriteSetApply(writes, dynProps, raw)
	return err
}

func (s *StateDB) validateTransactionWriteSetApply(writes TransactionWriteSet, dynProps *DynamicProperties, raw TransactionRawKVWriter) ([]TransactionAccessKey, error) {
	if s == nil {
		return nil, fmt.Errorf("apply transaction writes: nil state")
	}
	orderedKeys, err := validateTransactionWriteSetApplySchema(writes, dynProps, raw)
	if err != nil {
		return nil, err
	}
	var blackhole tcommon.Address
	blackholeResolved := false
	for _, key := range orderedKeys {
		if key.Kind == TransactionAccessAccountField && writes[key].Commutative {
			if !blackholeResolved {
				blackhole, err = s.transactionWriteBlackholeAddress()
				if err != nil {
					return nil, err
				}
				blackholeResolved = true
			}
			if key.Address != blackhole {
				return nil, fmt.Errorf("apply transaction writes: commutative balance target %s is not the blackhole account", key.Address.Hex())
			}
		}
	}
	var creates map[tcommon.Address]struct{}
	for _, key := range orderedKeys {
		if key.Kind != TransactionAccessAccount {
			continue
		}
		if obj := s.getStateObjectWithoutAccess(key.Address); obj != nil {
			return nil, fmt.Errorf("apply transaction writes: full account replacement %s is not supported", key.Address.Hex())
		}
		if creates == nil {
			creates = make(map[tcommon.Address]struct{}, 1)
		}
		creates[key.Address] = struct{}{}
	}
	for _, key := range orderedKeys {
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
				return nil, fmt.Errorf("apply transaction writes: %s requires an existing or created account", key.Address.Hex())
			}
		}
	}
	return orderedKeys, nil
}

// transactionWriteBlackholeAddress is the fail-closed publication equivalent
// of BlackholeAddress. A missing rooted index retains the legacy-mainnet
// compatibility fallback, but an unreadable or malformed row cannot authorize
// a commutative balance write.
func (s *StateDB) transactionWriteBlackholeAddress() (tcommon.Address, error) {
	raw, exists, err := s.systemKVGetForDecoding(kvdomains.SystemAccountIndex, blackholeAccountNameIndexKey[:])
	if err != nil {
		return tcommon.Address{}, fmt.Errorf("apply transaction writes: read blackhole account index: %w", err)
	}
	if !exists {
		return params.BlackholeAddress, nil
	}
	if len(raw) != tcommon.AddressLength {
		return tcommon.Address{}, fmt.Errorf("apply transaction writes: blackhole account index has %d bytes", len(raw))
	}
	address := tcommon.BytesToAddress(raw)
	if !address.ValidPrefix() {
		return tcommon.Address{}, fmt.Errorf("apply transaction writes: blackhole account index has non-canonical address %s", address.Hex())
	}
	return address, nil
}

func (s *StateDB) applyTransactionWriteSet(writes TransactionWriteSet, dynProps *DynamicProperties, raw TransactionRawKVWriter, recorder *TransactionAccessRecorder) error {
	if s == nil {
		return fmt.Errorf("apply transaction writes: nil state")
	}
	orderedKeys, err := s.validateTransactionWriteSetApply(writes, dynProps, raw)
	if err != nil {
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

	for _, key := range orderedKeys {
		if key.Kind != TransactionAccessAccount {
			continue
		}
		value := writes[key]
		recordTransactionWriteApply(recorder, key, value)
		if err := s.applyTransactionWrite(key, value, dynProps, raw); err != nil {
			return err
		}
	}
	for _, key := range orderedKeys {
		if key.Kind == TransactionAccessAccount {
			continue
		}
		value := writes[key]
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
