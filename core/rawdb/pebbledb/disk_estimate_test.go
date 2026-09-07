package pebbledb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestEstimateDiskUsageRangeAndClosed(t *testing.T) {
	db, err := New(t.TempDir(), 16, 32, "test/history-estimate/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, prefix := range []byte{'a', 'z'} {
		for i := 0; i < 10; i++ {
			if err := db.Put([]byte{prefix, byte(i)}, bytes.Repeat([]byte{byte(i)}, 4096)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.db.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := db.EstimateDiskUsage([]byte{'a'}, []byte{'b'})
	want, werr := db.db.EstimateDiskUsage([]byte{'a'}, []byte{'b'})
	if err != nil || werr != nil || got != want || got == 0 {
		t.Fatalf("estimate %d/%d: %v/%v", got, want, err, werr)
	}
	for _, bounds := range [][2][]byte{{nil, []byte{'b'}}, {[]byte{'a'}, nil}, {[]byte{'a'}, []byte{'a'}}, {[]byte{'z'}, []byte{'a'}}} {
		if _, err := db.EstimateDiskUsage(bounds[0], bounds[1]); err == nil {
			t.Fatal("invalid range accepted")
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.EstimateDiskUsage([]byte{'a'}, []byte{'b'}); !errors.Is(err, pebble.ErrClosed) {
		t.Fatal(err)
	}
}
