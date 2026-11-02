package core

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/params"
)

func TestCompactV4StartupRejectsV3WithoutMutation(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	defer disk.Close()
	genesis := &params.Genesis{Config: params.MainnetChainConfig}
	if _, _, err := SetupGenesisBlock(disk, genesis); err != nil {
		t.Fatal(err)
	}
	head, ok, err := rawdb.ReadStateCommitmentHead(disk)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	encoded, err := rawdb.EncodeStateCommitmentHead(head)
	if err != nil {
		t.Fatal(err)
	}
	encoded[7] = 3
	binary.BigEndian.PutUint16(encoded[8:10], 3)
	binary.BigEndian.PutUint16(encoded[10:12], 3)
	copy(encoded[14:], "state-flat-latest-muhash3072-compact-v3")
	if err := disk.Put(rawdb.StateCommitmentHeadKey(), encoded); err != nil {
		t.Fatal(err)
	}
	snapshot := func(store ethdb.Iteratee) map[string][]byte {
		it := store.NewIterator(nil, nil)
		defer it.Release()
		rows := make(map[string][]byte)
		for it.Next() {
			rows[string(it.Key())] = bytes.Clone(it.Value())
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	before := snapshot(disk)
	if _, _, err := SetupGenesisBlock(disk, genesis); err == nil {
		t.Fatal("startup accepted V3 source identity")
	}
	chain := rawdb.NewChainDB(disk, rawdb.NoopAncient{})
	block := rawdb.ReadBlock(chain, 0)
	if block == nil {
		t.Fatal("missing genesis fixture")
	}
	if _, err := loadStoredHeadBlock(chain, block); err == nil {
		t.Fatal("head loader accepted V3 source identity")
	}
	if !reflect.DeepEqual(before, snapshot(disk)) {
		t.Fatal("rejected old database was mutated")
	}
}
