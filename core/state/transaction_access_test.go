package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type unknownTransactionJournalChange struct{}

func (unknownTransactionJournalChange) revert(map[tcommon.Address]*stateObject, map[tcommon.Address]*types.Witness) {
}

func TestVisitTransactionWritesSinceClassifiesJournal(t *testing.T) {
	var account, created, witness, contract tcommon.Address
	account[0], created[0], witness[0], contract[0] = 0x41, 0x41, 0x41, 0x41
	account[20], created[20], witness[20], contract[20] = 1, 2, 3, 4
	var slot tcommon.Hash
	slot[31] = 9
	kvKey := kvCompositeKeyString(kvdomains.AccountPermissionAux, []byte("owner"))

	s := &StateDB{journal: newJournal()}
	s.journal.entries = []journalChange{
		accountChange{address: account, prev: []byte{1}},
		accountChange{address: created},
		&accountScalarChange{address: account},
		witnessChange{address: witness},
		&storageChange{address: contract, key: slot},
		codeChange{address: contract},
		contractMetaChange{address: contract},
		kvChange{address: account, mapKey: kvKey},
		kvResetChange{address: account},
		selfDestructChange{address: contract},
		transientStorageChange{tk: transientStorageKey{addr: contract, key: slot}},
		resourceWeightChange{resource: corepb.ResourceCode_BANDWIDTH},
		unknownTransactionJournalChange{},
	}

	var got []TransactionWrite
	s.VisitTransactionWritesSince(0, func(write TransactionWrite) bool {
		got = append(got, write)
		return true
	})
	wantKinds := []TransactionWriteKind{
		TransactionWriteAccount,
		TransactionWriteAccountCreate,
		TransactionWriteAccount,
		TransactionWriteWitness,
		TransactionWriteStorage,
		TransactionWriteCode,
		TransactionWriteContractMetadata,
		TransactionWriteAccountKV,
		TransactionWriteAccountKVReset,
		TransactionWriteSelfDestruct,
		TransactionWriteTransientStorage,
		TransactionWriteDynamicProperties,
		TransactionWriteUnknown,
	}
	if len(got) != len(wantKinds) {
		t.Fatalf("writes = %d, want %d", len(got), len(wantKinds))
	}
	for i, want := range wantKinds {
		if got[i].Kind != want {
			t.Fatalf("write %d kind = %d, want %d", i, got[i].Kind, want)
		}
	}
	if got[4].Address != contract || got[4].StorageKey != slot {
		t.Fatalf("storage write = %+v", got[4])
	}
	if got[7].Address != account || got[7].KVDomain != kvdomains.AccountPermissionAux || got[7].KVKey != kvKey {
		t.Fatalf("account KV write = %+v", got[7])
	}
}

func TestVisitTransactionWritesSinceFollowsRollback(t *testing.T) {
	var address tcommon.Address
	address[0], address[20] = 0x41, 1
	s := &StateDB{
		journal:      newJournal(),
		stateObjects: make(map[tcommon.Address]*stateObject),
		witnesses:    make(map[tcommon.Address]*types.Witness),
	}
	snapshot := s.Snapshot()
	mark := s.journal.length()
	s.journal.append(accountChange{address: address})

	count := 0
	s.VisitTransactionWritesSince(mark, func(TransactionWrite) bool {
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("writes before rollback = %d, want 1", count)
	}
	s.RevertToSnapshot(snapshot)
	count = 0
	s.VisitTransactionWritesSince(mark, func(TransactionWrite) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("writes after rollback = %d, want 0", count)
	}
}

func TestDynamicPropertiesSnapshotChangedBeforeNestedCommit(t *testing.T) {
	dp := NewDynamicProperties()
	blockSnapshot := dp.Snapshot()
	firstTx := dp.Snapshot()
	if dp.SnapshotChanged(firstTx) {
		t.Fatal("fresh transaction snapshot reported a change")
	}
	dp.SetPublicNetUsage(1)
	if !dp.SnapshotChanged(firstTx) {
		t.Fatal("transaction dynamic-property write was not observed")
	}
	dp.CommitSnapshot(firstTx)

	secondTx := dp.Snapshot()
	dp.SetPublicNetUsage(2)
	if !dp.SnapshotChanged(secondTx) {
		t.Fatal("repeated key write in a later transaction was not observed")
	}
	dp.CommitSnapshot(secondTx)
	dp.RevertToSnapshot(blockSnapshot)
	if got := dp.PublicNetUsage(); got != 0 {
		t.Fatalf("public net usage after outer rollback = %d, want 0", got)
	}
}
