package rawdb

import (
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type executionSafetySyncStore struct {
	ethdb.KeyValueStore
	syncs   int
	syncErr error
}

func (s *executionSafetySyncStore) SyncKeyValue() error {
	s.syncs++
	return s.syncErr
}

func TestExecutionSafetyIncidentRoundTripAndSync(t *testing.T) {
	base := NewMemoryDatabase()
	store := &executionSafetySyncStore{KeyValueStore: base}
	if got, ok, err := ReadExecutionSafetyIncident(store); err != nil || ok || got != (ExecutionSafetyIncident{}) {
		t.Fatalf("missing marker = %+v,%t,%v", got, ok, err)
	}
	want := ExecutionSafetyIncident{
		Kind:      ExecutionSafetyIncidentParallelVMRepair,
		BlockNum:  18_414_381,
		BlockHash: common.Hash{0x12, 0x34},
	}
	if err := WriteExecutionSafetyIncident(store, want); err != nil {
		t.Fatal(err)
	}
	if store.syncs != 1 {
		t.Fatalf("durability syncs = %d, want 1", store.syncs)
	}
	got, ok, err := ReadExecutionSafetyIncident(store)
	if err != nil || !ok || got != want {
		t.Fatalf("marker = %+v,%t,%v, want %+v,true,nil", got, ok, err, want)
	}
}

func TestExecutionSafetyIncidentFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
		want  string
	}{
		{name: "empty", value: nil, want: "length"},
		{name: "version", value: append([]byte{2, byte(ExecutionSafetyIncidentSpeculativePublication)}, make([]byte, executionSafetyIncidentEncodedSize-2)...), want: "version"},
		{name: "kind", value: append([]byte{executionSafetyIncidentEncodingVersion, 0xff}, make([]byte, executionSafetyIncidentEncodedSize-2)...), want: "kind"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := NewMemoryDatabase()
			if err := db.Put(executionSafetyIncidentKey, tc.value); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := ReadExecutionSafetyIncident(db); err == nil || !ok || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("read malformed marker: ok=%t err=%v, want %q", ok, err, tc.want)
			}
		})
	}

	if err := WriteExecutionSafetyIncident(NewMemoryDatabase(), ExecutionSafetyIncident{}); err == nil || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("invalid kind write error = %v", err)
	}
	wantErr := errors.New("sync boom")
	store := &executionSafetySyncStore{KeyValueStore: NewMemoryDatabase(), syncErr: wantErr}
	if err := WriteExecutionSafetyIncident(store, ExecutionSafetyIncident{Kind: ExecutionSafetyIncidentSpeculativePublication}); !errors.Is(err, wantErr) {
		t.Fatalf("sync failure = %v, want %v", err, wantErr)
	}
}

func TestExecutionSafetyIncidentPersistsAcrossPebbleReopen(t *testing.T) {
	dir := t.TempDir()
	want := ExecutionSafetyIncident{
		Kind:      ExecutionSafetyIncidentSpeculativePublication,
		BlockNum:  22_123_859,
		BlockHash: common.Hash{0x22, 0x12, 0x38, 0x59},
	}
	db, err := NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteExecutionSafetyIncident(db, want); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := WriteExecutionSafetyQualification(db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, ok, err := ReadExecutionSafetyIncident(reopened)
	if err != nil || !ok || got != want {
		t.Fatalf("reopened marker = %+v,%t,%v, want %+v,true,nil", got, ok, err, want)
	}
	if qualified, err := ReadExecutionSafetyQualification(reopened); err != nil || !qualified {
		t.Fatalf("reopened qualification = %t,%v, want true,nil", qualified, err)
	}
}

func TestExecutionSafetyQualificationRoundTripAndFailsClosed(t *testing.T) {
	base := NewMemoryDatabase()
	store := &executionSafetySyncStore{KeyValueStore: base}
	if qualified, err := ReadExecutionSafetyQualification(store); err != nil || qualified {
		t.Fatalf("missing qualification = %t,%v, want false,nil", qualified, err)
	}
	if err := WriteExecutionSafetyQualification(store); err != nil {
		t.Fatal(err)
	}
	if store.syncs != 1 {
		t.Fatalf("qualification durability syncs = %d, want 1", store.syncs)
	}
	if qualified, err := ReadExecutionSafetyQualification(store); err != nil || !qualified {
		t.Fatalf("qualification = %t,%v, want true,nil", qualified, err)
	}

	for _, malformed := range [][]byte{nil, {0}, {2}, {1, 1}} {
		db := NewMemoryDatabase()
		if err := db.Put(executionSafetyQualifiedKey, malformed); err != nil {
			t.Fatal(err)
		}
		if qualified, err := ReadExecutionSafetyQualification(db); err == nil || qualified {
			t.Fatalf("malformed qualification %x = %t,%v, want false,error", malformed, qualified, err)
		}
	}
}
