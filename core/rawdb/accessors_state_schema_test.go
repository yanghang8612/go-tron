package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
)

func TestStateSchemaVersionRoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if _, ok, err := ReadStateSchemaVersion(db); err != nil || ok {
		t.Fatalf("empty schema: ok=%v err=%v", ok, err)
	}
	if err := WriteStateSchemaVersion(db, CurrentStateSchemaVersion); err != nil {
		t.Fatal(err)
	}
	version, ok, err := ReadStateSchemaVersion(db)
	if err != nil || !ok || version != CurrentStateSchemaVersion {
		t.Fatalf("schema: version=%d ok=%v err=%v", version, ok, err)
	}
}

func TestStateSchemaVersionRejectsMalformedValue(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if err := db.Put(stateSchemaVersionKey, []byte{3}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadStateSchemaVersion(db); err == nil {
		t.Fatal("expected malformed schema version to fail")
	}
}
