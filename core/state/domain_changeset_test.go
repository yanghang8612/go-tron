package state

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestStateDBDomainChangeSetCapturesAccountKVAndStorage(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	systemKey := []byte("reward/key")
	systemValue := []byte("reward-v1")
	contract := testAddr(0x77)
	slot := tcommon.Hash{0x01}
	slotValue := tcommon.Hash{0x99}

	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, systemKey, systemValue); err != nil {
		t.Fatal(err)
	}
	sdb.CreateAccount(contract, corepb.AccountType_Contract)
	sdb.SetState(contract, slot, slotValue)

	changeDB := ethrawdb.NewMemoryDatabase()
	blockHash := tcommon.Hash{0x05}
	sdb.SetDomainChangeSetWriter(changeDB, 5, blockHash)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	txRange, ok, err := rawdb.ReadStateTxRange(changeDB, 5)
	if err != nil || !ok {
		t.Fatalf("tx range = ok:%v err:%v", ok, err)
	}
	if txRange.BeginTxNum != rawdb.BlockStateTxNum(5) || txRange.EndTxNum != rawdb.BlockStateTxNum(5) || txRange.BlockHash != blockHash {
		t.Fatalf("tx range = %+v", txRange)
	}

	changes := collectStateDomainChanges(t, changeDB, 5)
	if !hasDomainChange(changes, tcommon.SystemAccountAddress, kvdomains.SystemReward, systemKey, false, nil, true, systemValue) {
		t.Fatalf("system reward change not captured: %+v", changes)
	}
	rowKey := sdb.storageRowKey(contract, slot).Bytes()
	if !hasDomainChange(changes, contract, kvdomains.ContractStorage, rowKey, false, nil, true, slotValue.Bytes()) {
		t.Fatalf("contract storage change not captured: %+v", changes)
	}

	reopened, err := New(root, NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	nextValue := []byte("reward-v2")
	if err := reopened.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, systemKey, nextValue); err != nil {
		t.Fatal(err)
	}
	nextChangeDB := ethrawdb.NewMemoryDatabase()
	reopened.SetDomainChangeSetWriter(nextChangeDB, 6, tcommon.Hash{0x06})
	if _, err := reopened.Commit(); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	changes = collectStateDomainChanges(t, nextChangeDB, 6)
	if !hasDomainChange(changes, tcommon.SystemAccountAddress, kvdomains.SystemReward, systemKey, true, systemValue, true, nextValue) {
		t.Fatalf("system reward update pre-value not captured: %+v", changes)
	}
}

func collectStateDomainChanges(t *testing.T, db ethdb.Iteratee, blockNum uint64) []*rawdb.StateDomainChange {
	t.Helper()
	var changes []*rawdb.StateDomainChange
	if err := rawdb.IterateStateDomainChanges(db, blockNum, func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	return changes
}

func hasDomainChange(changes []*rawdb.StateDomainChange, owner tcommon.Address, domain kvdomains.KVDomain, key []byte, prevExists bool, prev []byte, nextExists bool, next []byte) bool {
	for _, change := range changes {
		if change.Owner != owner || change.Domain != domain || !bytes.Equal(change.Key, key) {
			continue
		}
		return change.PrevExists == prevExists &&
			bytes.Equal(change.Prev, prev) &&
			change.NextExists == nextExists &&
			bytes.Equal(change.Next, next)
	}
	return false
}
