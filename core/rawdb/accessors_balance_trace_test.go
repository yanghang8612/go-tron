package rawdb

import (
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
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

	if err := WriteBlockBalanceTrace(db, 1000, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}

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

func TestBlockBalanceTrace_RejectsNilWrite(t *testing.T) {
	db := memorydb.New()
	if err := WriteBlockBalanceTrace(db, 1, nil); err == nil {
		t.Fatal("WriteBlockBalanceTrace accepted nil trace")
	}
	if HasBlockBalanceTrace(db, 1) {
		t.Fatal("nil trace write created a row")
	}
}

func TestBlockBalanceTrace_RejectsMismatchedWrite(t *testing.T) {
	db := memorydb.New()
	trace := &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{Number: 2},
		Timestamp:       42,
	}
	if err := WriteBlockBalanceTrace(db, 1, trace); err == nil {
		t.Fatal("WriteBlockBalanceTrace accepted mismatched block number")
	}
	if HasBlockBalanceTrace(db, 1) {
		t.Fatal("mismatched trace write created a readable row")
	}
}

func TestBlockBalanceTrace_RejectsMismatchedHotRow(t *testing.T) {
	db := memorydb.New()
	data, err := proto.Marshal(&contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{Number: 2},
		Timestamp:       42,
	})
	if err != nil {
		t.Fatalf("marshal trace: %v", err)
	}
	if err := db.Put(balanceTraceKey(1), data); err != nil {
		t.Fatalf("put mismatched trace: %v", err)
	}
	if HasBlockBalanceTrace(db, 1) {
		t.Fatal("HasBlockBalanceTrace accepted mismatched hot row")
	}
	if got := ReadBlockBalanceTrace(db, 1); got != nil {
		t.Fatalf("ReadBlockBalanceTrace mismatched hot row = %+v, want nil", got)
	}
	if trace, ok, err := ReadBlockBalanceTraceStrict(db, 1); err == nil || !ok || trace == nil {
		t.Fatalf("ReadBlockBalanceTraceStrict mismatched hot row = trace %+v ok %v err %v, want trace/ok/error", trace, ok, err)
	}
}

func TestBlockBalanceTraceStrictSurfacesHotReadError(t *testing.T) {
	wantErr := errors.New("hot balance trace read corrupt")
	db := failingGetStore{
		KeyValueStore: memorydb.New(),
		key:           balanceTraceKey(1),
		err:           wantErr,
	}

	if got := ReadBlockBalanceTrace(db, 1); got != nil {
		t.Fatalf("ReadBlockBalanceTrace hot read error = %+v, want nil compatibility miss", got)
	}
	trace, ok, err := ReadBlockBalanceTraceStrict(db, 1)
	if !errors.Is(err, wantErr) || ok || trace != nil {
		t.Fatalf("ReadBlockBalanceTraceStrict hot read error = trace %+v ok %v err %v, want hot read error", trace, ok, err)
	}
}

func TestBlockBalanceTrace_Delete(t *testing.T) {
	db := memorydb.New()
	trace := &contractpb.BlockBalanceTrace{Timestamp: 42}
	if err := WriteBlockBalanceTrace(db, 5, trace); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
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
		if err := WriteBlockBalanceTrace(db, i, &contractpb.BlockBalanceTrace{Timestamp: i}); err != nil {
			t.Fatalf("WriteBlockBalanceTrace %d: %v", i, err)
		}
	}
	for i := int64(1); i <= 5; i++ {
		got := ReadBlockBalanceTrace(db, i)
		if got == nil || got.Timestamp != i {
			t.Errorf("block %d: got %v", i, got)
		}
	}
}
