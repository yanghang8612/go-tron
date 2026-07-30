package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestEnsureSnapshotRestoreBootstrapDatadirSurfacesHeadReadErrors(t *testing.T) {
	store := gtronStrictReadFailingStore{
		KeyValueStore: rawdb.NewMemoryDatabase(),
		hasErr:        errors.New("head presence failed"),
	}

	err := ensureSnapshotRestoreBootstrapDatadir(store, common.Hash{0x01})
	if err == nil || !strings.Contains(err.Error(), "snapshot restore read head block hash") || !strings.Contains(err.Error(), "head presence failed") {
		t.Fatalf("ensureSnapshotRestoreBootstrapDatadir err = %v, want strict head read context", err)
	}
}

func TestDBRebuildToBlockSurfacesHeadReadErrors(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	rawdb.WriteHeadBlockHash(base, common.Hash{0x42})
	store := gtronStrictReadFailingStore{
		KeyValueStore: base,
		getErr:        errors.New("head get failed"),
	}
	chainDB := rawdb.NewChainDB(store, rawdb.NoopAncient{})
	ctx := makeDBTestContext(t, []string{"--db.from-block", "1"})

	_, err := dbRebuildToBlock(ctx, chainDB)
	if err == nil || !strings.Contains(err.Error(), "db rebuild read head block hash") || !strings.Contains(err.Error(), "head get failed") {
		t.Fatalf("dbRebuildToBlock err = %v, want strict head read context", err)
	}
}

type gtronStrictReadFailingStore struct {
	ethdb.KeyValueStore
	hasErr error
	getErr error
}

func (s gtronStrictReadFailingStore) Has(key []byte) (bool, error) {
	if s.hasErr != nil {
		return false, s.hasErr
	}
	return s.KeyValueStore.Has(key)
}

func (s gtronStrictReadFailingStore) Get(key []byte) ([]byte, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.KeyValueStore.Get(key)
}
