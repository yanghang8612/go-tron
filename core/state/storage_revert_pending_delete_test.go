package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestRevertKeepsEarlierTransactionStorageDelete models mainnet block
// 5,794,792: tx 5 committed a storage delete, tx 9 rewrote the same slot and
// reverted, then tx 38 had to keep observing the delete rather than the
// durable value from the parent block.
func TestRevertKeepsEarlierTransactionStorageDelete(t *testing.T) {
	database := NewDatabase(ethrawdb.NewMemoryDatabase())
	statedb, err := New(tcommon.Hash{}, database)
	if err != nil {
		t.Fatal(err)
	}
	contract := tcommon.Address{0x41, 0x01}
	key := tcommon.BytesToHash([]byte{0x01})
	parentValue := tcommon.BytesToHash([]byte{0x11})
	statedb.CreateAccount(contract, corepb.AccountType_Contract)
	statedb.SetState(contract, key, parentValue)
	root, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	statedb, err = New(root, database)
	if err != nil {
		t.Fatal(err)
	}
	_ = statedb.Snapshot() // block rollback boundary

	_ = statedb.Snapshot() // successful tx 1
	statedb.SetState(contract, key, tcommon.Hash{})
	statedb.FinalizeTransaction()
	if got, exists := statedb.GetStateWithExist(contract, key); got != (tcommon.Hash{}) || exists {
		t.Fatalf("after committed delete: got (%x, %t), want (0, false)", got, exists)
	}

	_ = statedb.Snapshot() // tx 2
	frame := statedb.Snapshot()
	statedb.SetState(contract, key, tcommon.BytesToHash([]byte{0x22}))
	statedb.RevertToSnapshot(frame)
	statedb.FinalizeTransaction()

	if got, exists := statedb.GetStateWithExist(contract, key); got != (tcommon.Hash{}) || exists {
		t.Fatalf("after reverted rewrite: got (%x, %t), want pending delete (0, false)", got, exists)
	}
}
