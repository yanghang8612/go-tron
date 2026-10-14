package pebbledb

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"testing"
)

// NewFaultGetCursorTestDB exposes only a test fixture to the external test
// package, so it can exercise real blockbuffer sessions without an import cycle.
func NewFaultGetCursorTestDB(t *testing.T, keys, values [][]byte, injected ...error) (*Database, func(bool)) {
	t.Helper()
	fs := &pointGetFailFS{FS: vfs.NewMem()}
	if len(injected) > 0 {
		fs.readErr = injected[0]
	}
	cache := pebble.NewCache(0)
	p, err := pebble.Open("session", &pebble.Options{FS: fs, Cache: cache, Comparer: exactPointComparer})
	cache.Unref()
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range keys {
		if err := p.Set(key, values[i], pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	// Pin reader/index metadata before injecting a data ReadAt failure. Cache0
	// ensures the following data block lookup still reaches the faulting VFS.
	for _, key := range keys {
		_, closer, err := p.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	d := &Database{db: p, boundedPointGet: true}
	t.Cleanup(func() {
		fs.fail.Store(false)
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, func(fail bool) { fs.fail.Store(fail) }
}
