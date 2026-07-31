package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestTransactionRecordingKVStoreForwardsAndCaptures(t *testing.T) {
	parent := ethrawdb.NewMemoryDatabase()
	if err := parent.Put([]byte("existing"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	var recorder state.TransactionAccessRecorder
	recorder.Reset(8)
	db := transactionRecordingKVStore{parent: parent, recorder: &recorder}

	if got, err := db.Get([]byte("existing")); err != nil || string(got) != "old" {
		t.Fatalf("get existing = %q, %v", got, err)
	}
	if err := db.Put([]byte("created"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("existing")); err != nil {
		t.Fatal(err)
	}
	if exists, err := parent.Has([]byte("created")); err != nil || !exists {
		t.Fatalf("canonical put not forwarded: exists=%v err=%v", exists, err)
	}
	if exists, err := parent.Has([]byte("existing")); err != nil || exists {
		t.Fatalf("canonical delete not forwarded: exists=%v err=%v", exists, err)
	}

	sdb := newTestState(t)
	writes, known, err := sdb.CaptureTransactionWriteSet(sdb.DomainChangeJournalMark(), &recorder, sdb.DynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture recorded DB: known=%v err=%v", known, err)
	}
	created := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "created"}
	if value, ok := writes[created]; !ok || !value.Exists || string(value.Value) != "new" {
		t.Fatalf("created write = %+v ok=%v", value, ok)
	}
	deleted := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "existing"}
	if value, ok := writes[deleted]; !ok || value.Exists {
		t.Fatalf("deleted write = %+v ok=%v", value, ok)
	}
}
