package pebbledb_test

import (
	"bytes"
	"errors"
	"github.com/cockroachdb/pebble"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

func TestGetCursorSpeculativeFailureDoesNotFailSessionClose(t *testing.T) {
	for _, unused := range []bool{false, true} {
		name := "same-key-retry"
		if unused {
			name = "unused-prediction"
		}
		t.Run(name, func(t *testing.T) {
			paths := [][]byte{{1, 2, 3, 4, 5, 6}, {2, 2, 3, 4, 5, 6}}
			keys := make([][]byte, len(paths))
			values := [][]byte{[]byte("prefetch-branch"), []byte("foreground-branch")}
			for i, path := range paths {
				keys[i] = append([]byte(rawdb.CommitmentBranchKeyPrefix), path...)
			}
			ioError := errors.New("joined speculative I/O failure")
			disk, fail := pebbledb.NewFaultGetCursorTestDB(t, keys, values, errors.Join(pebble.ErrNotFound, ioError))
			b := blockbuffer.New(disk)
			b.SetBaseReadCacheSize(1<<20, rawdb.CommitmentBranchKeyPrefix)
			b.BeginBlock(common.Hash{1}, 1)
			h, _ := b.NewestInflight()
			session, err := b.ViewLayer(h).NewCommitmentParentReadSession(2)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			fail(true)
			if _, err := session.(pointread.CommitmentParentPrefetchSession).PrefetchKeyParts(0, []byte(rawdb.CommitmentBranchKeyPrefix), paths[0]); !errors.Is(err, ioError) || !errors.Is(err, pebble.ErrNotFound) {
				t.Fatal("faulting prefetch unexpectedly succeeded")
			}
			fail(false)
			i := 0
			if unused {
				i = 1
			}
			called := false
			found, err := session.ViewKeyParts(1, []byte(rawdb.CommitmentBranchKeyPrefix), paths[i], func(v []byte, _ bool) error {
				called = true
				if !bytes.Equal(v, values[i]) {
					t.Errorf("foreground %q want %q", v, values[i])
				}
				return nil
			})
			if err != nil || !found || !called {
				t.Fatalf("foreground retry: found=%v called=%v err=%v", found, called, err)
			}
			if err := session.Close(); err != nil {
				t.Fatalf("already-reported speculative error reappeared at session Close: %v", err)
			}
		})
	}
}
