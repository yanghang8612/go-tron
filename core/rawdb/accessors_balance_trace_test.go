package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestBlockBalanceTrace_RoundTrip(t *testing.T) {
	db := memorydb.New()

	trace := &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   []byte("blockhash"),
			Number: 1000,
		},
		Timestamp: 1234567890,
	}

	if HasBlockBalanceTrace(db, 1000) {
		t.Fatal("expected absent")
	}

	WriteBlockBalanceTrace(db, 1000, trace)

	if !HasBlockBalanceTrace(db, 1000) {
		t.Fatal("expected present")
	}

	got := ReadBlockBalanceTrace(db, 1000)
	if got == nil {
		t.Fatal("ReadBlockBalanceTrace returned nil")
	}
	if got.Timestamp != trace.Timestamp {
		t.Errorf("Timestamp: got %d want %d", got.Timestamp, trace.Timestamp)
	}
	if got.BlockIdentifier == nil || got.BlockIdentifier.Number != 1000 {
		t.Errorf("BlockIdentifier mismatch")
	}
}

func TestBlockBalanceTrace_Absent(t *testing.T) {
	db := memorydb.New()
	if got := ReadBlockBalanceTrace(db, 999); got != nil {
		t.Fatalf("expected nil for absent key, got %v", got)
	}
}

func TestBlockBalanceTrace_Delete(t *testing.T) {
	db := memorydb.New()
	trace := &contractpb.BlockBalanceTrace{Timestamp: 42}
	WriteBlockBalanceTrace(db, 5, trace)
	if err := DeleteBlockBalanceTrace(db, 5); err != nil {
		t.Fatal(err)
	}
	if HasBlockBalanceTrace(db, 5) {
		t.Fatal("expected deleted")
	}
	if ReadBlockBalanceTrace(db, 5) != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestBlockBalanceTrace_MultiBlock(t *testing.T) {
	db := memorydb.New()
	for i := int64(1); i <= 5; i++ {
		WriteBlockBalanceTrace(db, i, &contractpb.BlockBalanceTrace{Timestamp: i})
	}
	for i := int64(1); i <= 5; i++ {
		got := ReadBlockBalanceTrace(db, i)
		if got == nil || got.Timestamp != i {
			t.Errorf("block %d: got %v", i, got)
		}
	}
}
