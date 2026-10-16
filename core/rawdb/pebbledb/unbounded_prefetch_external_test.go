package pebbledb_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

func TestUnboundedPrefetchBatchReadErrorRetryAndLastClose(t *testing.T) {
	for _, last := range []bool{false, true} {
		name := "first-fails-then-success"
		if last {
			name = "last-fails-close-reports"
		}
		t.Run(name, func(t *testing.T) {
			ioErr := errors.New("prefetch transient read")
			keys := [][]byte{[]byte("a"), []byte("b")}
			values := [][]byte{bytes.Repeat([]byte("a"), 8192), bytes.Repeat([]byte("b"), 8192)}
			disk, fail := pebbledb.NewFaultGetCursorTestDB(t, keys, values, errors.Join(pebble.ErrNotFound, ioErr))
			buffer := blockbuffer.New(disk)
			buffer.SetBaseReadCacheSize(1 << 20)
			fail(!last)
			calls, readErrors := 0, 0
			err := buffer.PrefetchBatch(keys, func(id int, value []byte, present bool, err error) error {
				calls++
				if err != nil {
					readErrors++
					if present || len(value) != 0 || !errors.Is(err, ioErr) || !errors.Is(err, pebble.ErrNotFound) {
						t.Fatalf("error became miss/value: %t/%v", present, err)
					}
					fail(false)
				} else {
					if !present || !bytes.Equal(value, values[id]) {
						t.Fatal("successful callback changed")
					}
					if last {
						fail(true)
					}
				}
				// Ignoring this row's error permits the next exact key to retry.
				return nil
			})
			fail(false)
			if calls != 2 || readErrors != 1 {
				t.Fatalf("callbacks=%d errors=%d", calls, readErrors)
			}
			if last {
				if !errors.Is(err, ioErr) {
					t.Fatalf("last read failure lost at Close: %v", err)
				}
			} else if err != nil {
				t.Fatalf("successful retry retained old error: %v", err)
			}
			for id, key := range keys {
				value, err := buffer.GetNoCopyCached(key)
				if err != nil || !bytes.Equal(value, values[id]) {
					t.Fatalf("read failure installed a negative cache entry: key=%q err=%v", key, err)
				}
			}
		})
	}
}
