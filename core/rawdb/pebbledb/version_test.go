package pebbledb

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	pebblev1 "github.com/cockroachdb/pebble"
	pebblev2 "github.com/cockroachdb/pebble/v2"
)

func writeLegacyPebbleV1(t *testing.T, dir string, key, value []byte) {
	t.Helper()
	db, err := pebblev1.Open(dir, &pebblev1.Options{Comparer: exactPointComparerV1()})
	if err != nil {
		t.Fatalf("open legacy Pebble v1: %v", err)
	}
	if got := db.FormatMajorVersion(); got != pebblev1.FormatMostCompatible {
		db.Close()
		t.Fatalf("legacy format = %d, want %d", got, pebblev1.FormatMostCompatible)
	}
	if err := db.Set(key, value, pebblev1.Sync); err != nil {
		db.Close()
		t.Fatalf("write legacy Pebble v1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy Pebble v1: %v", err)
	}
}

func TestNewUpgradesLegacyV1ToRevertibleV2Bridge(t *testing.T) {
	dir := t.TempDir()
	key, value := []byte("account-key"), []byte("account-value")
	writeLegacyPebbleV1(t, dir, key, value)

	db, err := New(dir, 16, 16, "test/v2bridge/upgrade/", false, DefaultOptions())
	if err != nil {
		t.Fatalf("New after legacy v1: %v", err)
	}
	got, err := db.Get(key)
	if err != nil || !bytes.Equal(got, value) {
		db.Close()
		t.Fatalf("v2 read after bridge = %q, %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close v2 bridge: %v", err)
	}

	exists, version, err := peekPebbleFormat(dir)
	if err != nil || !exists || pebblev2.FormatMajorVersion(version) != pebbleV2BridgeFormat {
		t.Fatalf("bridge format = exists:%t version:%d err:%v", exists, version, err)
	}
	// The selected bridge format must remain readable by the previous v1
	// runtime, so a failed production canary can roll its binary back without a
	// datadir restore.
	legacy, err := pebblev1.Open(dir, &pebblev1.Options{Comparer: exactPointComparerV1()})
	if err != nil {
		t.Fatalf("v1 rollback open: %v", err)
	}
	got, closer, err := legacy.Get(key)
	if err != nil || !bytes.Equal(got, value) {
		legacy.Close()
		t.Fatalf("v1 rollback read = %q, %v", got, err)
	}
	if err := closer.Close(); err != nil {
		legacy.Close()
		t.Fatalf("close v1 value: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close v1 rollback: %v", err)
	}
}

func TestNewReadOnlyDoesNotMutateLegacyV1(t *testing.T) {
	dir := t.TempDir()
	writeLegacyPebbleV1(t, dir, []byte("k"), []byte("v"))

	if _, err := New(dir, 16, 16, "test/v2bridge/readonly/", true, DefaultOptions()); err == nil ||
		!strings.Contains(err.Error(), "requires a writable v2 bridge upgrade") {
		t.Fatalf("read-only legacy error = %v", err)
	}
	_, version, err := peekPebbleFormat(dir)
	if err != nil {
		t.Fatalf("peek legacy after read-only rejection: %v", err)
	}
	if got := pebblev1.FormatMajorVersion(version); got != pebblev1.FormatMostCompatible {
		t.Fatalf("read-only open changed format to %d", got)
	}
}

func TestNewDatabaseStartsAtV2BridgeFormat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "chaindata")
	db, err := New(dir, 16, 16, "test/v2bridge/fresh/", false, DefaultOptions())
	if err != nil {
		t.Fatalf("New fresh v2 database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fresh v2 database: %v", err)
	}
	exists, version, err := peekPebbleFormat(dir)
	if err != nil || !exists || pebblev2.FormatMajorVersion(version) != pebbleV2BridgeFormat {
		t.Fatalf("fresh format = exists:%t version:%d err:%v", exists, version, err)
	}
}
