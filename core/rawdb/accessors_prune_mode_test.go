package rawdb

import (
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

func TestHistoryPruneModeAccessor(t *testing.T) {
	db := NewMemoryDatabase()

	if mode, ok, err := ReadHistoryPruneMode(db); err != nil || ok || mode != "" {
		t.Fatalf("missing prune mode: mode=%q ok=%v err=%v", mode, ok, err)
	}
	if err := WriteHistoryPruneMode(db, "archive"); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}
	if mode, ok, err := ReadHistoryPruneMode(db); err != nil || !ok || mode != "archive" {
		t.Fatalf("read prune mode: mode=%q ok=%v err=%v", mode, ok, err)
	}
}

func TestHistoryPruneModeAccessorRejectsEmptyValue(t *testing.T) {
	db := NewMemoryDatabase()
	if err := db.Put(historyPruneModeKey, nil); err != nil {
		t.Fatalf("write empty prune mode: %v", err)
	}
	if mode, ok, err := ReadHistoryPruneMode(db); err == nil || !ok || mode != "" {
		t.Fatalf("empty prune mode: mode=%q ok=%v err=%v", mode, ok, err)
	}
}

func TestHistoryPruneModeAccessorSurfacesStorageErrors(t *testing.T) {
	db := NewMemoryDatabase()

	if mode, ok, err := ReadHistoryPruneMode(failingPruneModeReader{reader: db, hasErr: errors.New("has boom")}); err == nil || ok || mode != "" || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("has error: mode=%q ok=%v err=%v", mode, ok, err)
	}

	if err := WriteHistoryPruneMode(db, "archive"); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}
	if mode, ok, err := ReadHistoryPruneMode(failingPruneModeReader{reader: db, getErr: errors.New("get boom")}); err == nil || ok || mode != "" || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("get error: mode=%q ok=%v err=%v", mode, ok, err)
	}
}

type failingPruneModeReader struct {
	reader ethdb.KeyValueReader
	hasErr error
	getErr error
}

func (r failingPruneModeReader) Has(key []byte) (bool, error) {
	if r.hasErr != nil {
		return false, r.hasErr
	}
	return r.reader.Has(key)
}

func (r failingPruneModeReader) Get(key []byte) ([]byte, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.reader.Get(key)
}
