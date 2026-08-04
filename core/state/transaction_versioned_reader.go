package state

import (
	"encoding/binary"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type transactionVersionedStorageKey struct {
	address tcommon.Address
	key     tcommon.Hash
}

// SetTransactionVersionedValueReader binds a speculative StateDB to one
// immutable transaction boundary. Local writes made after binding still win:
// account objects and account-KV dirty rows are consulted before this reader.
func (s *StateDB) SetTransactionVersionedValueReader(reader TransactionVersionedValueReader, txIndex int) {
	if s == nil {
		return
	}
	s.transactionVersionedReader = reader
	s.transactionVersionedTxIndex = txIndex
	if reader == nil {
		s.transactionVersionedHydrated = nil
		s.transactionVersionedMissing = nil
		s.transactionVersionedStorageChecked = nil
		return
	}
	s.transactionVersionedHydrated = make(map[tcommon.Address]struct{}, 16)
	s.transactionVersionedMissing = make(map[tcommon.Address]struct{}, 4)
	s.transactionVersionedStorageChecked = make(map[transactionVersionedStorageKey]struct{}, 16)
	s.lastStateObject = nil
}

func (s *StateDB) readTransactionVersionedValue(key TransactionAccessKey) (TransactionWriteValue, int, bool) {
	if s == nil || s.transactionVersionedReader == nil {
		return TransactionWriteValue{}, 0, false
	}
	return s.transactionVersionedReader.ReadTransactionVersionedValue(key, s.transactionVersionedTxIndex)
}

// hydrateTransactionVersionedAccount overlays exact typed post-images on a
// block-start object. The returned object is execution-local and can therefore
// be journaled and mutated normally by the speculative task.
func (s *StateDB) hydrateTransactionVersionedAccount(addr tcommon.Address, obj *stateObject) *stateObject {
	if s == nil || s.transactionVersionedReader == nil {
		return obj
	}
	fullWriter := -1
	if value, writer, ok := s.readTransactionVersionedValue(TransactionAccessKey{
		Kind: TransactionAccessAccount, Address: addr,
	}); ok {
		fullWriter = writer
		if !value.Exists {
			return nil
		}
		var envelope StateAccountV3
		if decodeStateAccountV3Into(value.Value, &envelope) != nil {
			return nil
		}
		account, err := types.UnmarshalAccount(envelope.AccountProto)
		if err != nil || account.Address() != addr {
			return nil
		}
		obj = s.newStateObject(addr, account)
		obj.accountProto = envelope.AccountProto
		obj.accountProtoLoaded = true
		obj.accountKVRoot = envelope.AccountKVRoot
		obj.accountKVGeneration = envelope.AccountKVGeneration
		obj.codeHash = envelope.CodeHash
		obj.dirtySet = s.dirtyObjects
	}

	// A later existence tombstone dominates both the durable base and a prior
	// full-account version. Creation itself is represented by the full Account
	// post-image, so a bare existence=true never fabricates an incomplete object.
	if value, writer, ok := s.readTransactionVersionedValue(TransactionAccessKey{
		Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldExistence,
	}); ok && writer > fullWriter && !value.Exists {
		return nil
	}
	if obj == nil || obj.account == nil {
		return obj
	}

	overlaid := false
	for _, field := range [...]TransactionAccountField{
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
	} {
		value, writer, ok := s.readTransactionVersionedValue(TransactionAccessKey{
			Kind: TransactionAccessAccountField, Address: addr, AccountField: field,
		})
		if !ok || writer <= fullWriter || !value.Exists || len(value.Value) < 8 {
			continue
		}
		amount := int64(binary.BigEndian.Uint64(value.Value[:8]))
		switch field {
		case TransactionAccountFieldAccountType:
			obj.account.SetAccountType(corepb.AccountType(amount))
		case TransactionAccountFieldBalance:
			obj.account.SetBalance(amount)
		case TransactionAccountFieldAllowance:
			obj.account.SetAllowance(amount)
		case TransactionAccountFieldLatestWithdrawTime:
			obj.account.SetLatestWithdrawTime(amount)
		case TransactionAccountFieldNetUsage:
			obj.account.SetNetUsage(amount)
		case TransactionAccountFieldLatestOperationTime:
			obj.account.SetLatestOperationTime(amount)
		case TransactionAccountFieldLatestConsumeTime:
			obj.account.SetLatestConsumeTime(amount)
		case TransactionAccountFieldFreeNetUsage:
			obj.account.SetFreeNetUsage(amount)
		case TransactionAccountFieldLatestConsumeFreeTime:
			obj.account.SetLatestConsumeFreeTime(amount)
		case TransactionAccountFieldNetWindow:
			if len(value.Value) != 9 {
				continue
			}
			obj.account.SetNetWindow(amount, value.Value[8] == 1)
		}
		overlaid = true
	}
	if value, writer, ok := s.readTransactionVersionedValue(TransactionAccessKey{
		Kind: TransactionAccessAccountKVGeneration, Address: addr,
	}); ok && writer > fullWriter && value.Exists && len(value.Value) == 8 {
		obj.accountKVGeneration = binary.BigEndian.Uint64(value.Value)
		overlaid = true
	}
	if overlaid {
		obj.invalidateAccountProto()
	}
	return obj
}

func (s *StateDB) readTransactionVersionedAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, bool) {
	value, _, ok := s.readTransactionVersionedValue(TransactionAccessKey{
		Kind: TransactionAccessAccountKV, Address: owner, KVDomain: domain, LogicalKey: string(key),
	})
	if !ok {
		return nil, false, false
	}
	return value.Value, value.Exists, true
}

func (s *StateDB) readTransactionVersionedKVGeneration(owner tcommon.Address) (uint64, bool, bool) {
	value, _, ok := s.readTransactionVersionedValue(TransactionAccessKey{
		Kind: TransactionAccessAccountKVGeneration, Address: owner,
	})
	if !ok {
		return 0, false, false
	}
	if !value.Exists || len(value.Value) != 8 {
		return 0, false, true
	}
	return binary.BigEndian.Uint64(value.Value), true, true
}

func (s *StateDB) readTransactionVersionedStorage(owner tcommon.Address, key tcommon.Hash) (tcommon.Hash, bool, bool) {
	value, _, ok := s.readTransactionVersionedValue(TransactionAccessKey{
		Kind: TransactionAccessStorage, Address: owner, StorageKey: key,
	})
	if !ok {
		return tcommon.Hash{}, false, false
	}
	if !value.Exists {
		return tcommon.Hash{}, false, true
	}
	return tcommon.BytesToHash(value.Value), true, true
}
