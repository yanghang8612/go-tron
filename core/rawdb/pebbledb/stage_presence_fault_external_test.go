package pebbledb_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

func TestStagePresenceRetainsJoinedStorageFailure(t *testing.T) {
	// Obtain a real typed key/value through the public writer, never duplicate
	// the schema prefix or bypass the actual resolver with a mock Get.
	seed := rawdb.NewMemoryDatabase()
	defer seed.Close()
	if err := rawdb.WriteStageProgressWithHash(seed, rawdb.StageSyncBodiesReady, 17, common.Hash{1}); err != nil {
		t.Fatal(err)
	}
	i := seed.NewIterator(nil, nil)
	if !i.Next() {
		t.Fatal("no stage fixture")
	}
	key, value := bytes.Clone(i.Key()), bytes.Clone(i.Value())
	i.Release()
	for _, kind := range []string{"Database.Has", "Database.GetWithPresence", "Snapshot.Has", "Stage.DirectHistory"} {
		t.Run(kind, func(t *testing.T) {
			ioErr := errors.New("SST ReadAt failed")
			db, fail := pebbledb.NewFaultGetCursorTestDB(t, [][]byte{key}, [][]byte{value}, errors.Join(pebble.ErrNotFound, ioErr))
			snap, err := db.NewKeyValueSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			fail(true)
			var present bool
			switch kind {
			case "Database.Has":
				present, err = db.Has(key)
			case "Database.GetWithPresence":
				_, present, err = db.GetWithPresence(key)
			case "Snapshot.Has":
				present, err = snap.Has(key)
			case "Stage.DirectHistory":
				history := rawdb.NewMemoryDatabase()
				defer history.Close()
				router := rawdb.NewDirectHistoryStore(db, history)
				_, present, err = rawdb.ReadStageProgressRow(router, rawdb.StageSyncBodiesReady)
			}
			fail(false)
			if present || !errors.Is(err, ioErr) || !errors.Is(err, pebble.ErrNotFound) {
				t.Fatalf("failure became missing: present=%t err=%v", present, err)
			}
			// The real row remains readable after the independent IO fault ends.
			if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady); err != nil || !ok || row.BlockNum != 17 {
				t.Fatal(row, ok, err)
			}
			if _, ok, err := db.GetWithPresence([]byte("absent")); err != nil || ok {
				t.Fatal(ok, err)
			}
		})
	}
}
