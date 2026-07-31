package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// TransactionWriteKind identifies the logical state cell family touched by a
// journaled transaction write. It is intentionally value-free: the first P4
// shadow pass needs conflict identity, not a second copy of the post-state.
type TransactionWriteKind uint8

const (
	TransactionWriteUnknown TransactionWriteKind = iota
	TransactionWriteAccount
	TransactionWriteAccountCreate
	TransactionWriteWitness
	TransactionWriteStorage
	TransactionWriteCode
	TransactionWriteContractMetadata
	TransactionWriteAccountKV
	TransactionWriteAccountKVReset
	TransactionWriteSelfDestruct
	TransactionWriteTransientStorage
	TransactionWriteDynamicProperties
)

// TransactionWrite describes one logical write found in StateDB's undo
// journal. KVKey is the journal-owned composite key and is valid only during
// the VisitTransactionWritesSince callback. StorageKey is populated only for
// TransactionWriteStorage. Callers that retain either must copy it.
type TransactionWrite struct {
	Kind       TransactionWriteKind
	Address    tcommon.Address
	KVDomain   kvdomains.KVDomain
	KVKey      string
	StorageKey tcommon.Hash
}

// VisitTransactionWritesSince visits journaled writes at or after mark without
// allocating a second write set. It is the observation boundary for P4 shadow
// conflict analysis; canonical rollback and persistence continue to consume
// the original journal unchanged. Returning false stops the walk.
//
// Unknown journal entry types are reported conservatively as
// TransactionWriteUnknown so a future state mutation cannot accidentally be
// considered safe for speculative publication.
func (s *StateDB) VisitTransactionWritesSince(mark int, visit func(TransactionWrite) bool) {
	if s == nil || visit == nil {
		return
	}
	if mark < 0 {
		mark = 0
	}
	if mark > s.journal.length() {
		mark = s.journal.length()
	}
	for _, entry := range s.journal.entries[mark:] {
		write := transactionWriteFromJournal(entry)
		if !visit(write) {
			return
		}
	}
}

func transactionWriteFromJournal(entry journalChange) TransactionWrite {
	switch change := entry.(type) {
	case accountChange:
		kind := TransactionWriteAccount
		if change.prev == nil {
			kind = TransactionWriteAccountCreate
		}
		return TransactionWrite{Kind: kind, Address: change.address}
	case *accountScalarChange:
		return TransactionWrite{Kind: TransactionWriteAccount, Address: change.address}
	case witnessChange:
		return TransactionWrite{Kind: TransactionWriteWitness, Address: change.address}
	case *storageChange:
		return TransactionWrite{Kind: TransactionWriteStorage, Address: change.address, StorageKey: change.key}
	case codeChange:
		return TransactionWrite{Kind: TransactionWriteCode, Address: change.address}
	case contractMetaChange:
		return TransactionWrite{Kind: TransactionWriteContractMetadata, Address: change.address}
	case selfDestructChange:
		return TransactionWrite{Kind: TransactionWriteSelfDestruct, Address: change.address}
	case kvChange:
		return transactionKVWrite(change.address, change.mapKey)
	case *kvChange:
		if change == nil {
			return TransactionWrite{Kind: TransactionWriteUnknown}
		}
		return transactionKVWrite(change.address, change.mapKey)
	case kvResetChange:
		return TransactionWrite{Kind: TransactionWriteAccountKVReset, Address: change.address}
	case transientStorageChange:
		return TransactionWrite{Kind: TransactionWriteTransientStorage, Address: change.tk.addr, StorageKey: change.tk.key}
	case resourceWeightChange:
		return TransactionWrite{Kind: TransactionWriteDynamicProperties}
	default:
		return TransactionWrite{Kind: TransactionWriteUnknown}
	}
}

func transactionKVWrite(address tcommon.Address, mapKey string) TransactionWrite {
	domain, _, ok := splitKVCompositeKeyView([]byte(mapKey))
	if !ok {
		return TransactionWrite{Kind: TransactionWriteUnknown, Address: address, KVKey: mapKey}
	}
	return TransactionWrite{
		Kind:     TransactionWriteAccountKV,
		Address:  address,
		KVDomain: domain,
		KVKey:    mapKey,
	}
}
