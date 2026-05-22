package core

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// loadDPAtRoot loads dynamic properties the way the live node does after Phase
// 3b: derived head-pointer keys from db (flat dp-), rooted governance/economic
// keys from the system-account KV at the given state root. A zero root yields
// rooted defaults.
func loadDPAtRoot(tb testing.TB, db ethdb.KeyValueReader, sdb *state.Database, root tcommon.Hash) *state.DynamicProperties {
	tb.Helper()
	var sysKV *state.StateDB
	if root != (tcommon.Hash{}) {
		var err error
		sysKV, err = state.New(root, sdb)
		if err != nil {
			tb.Fatalf("open sysKV at %x: %v", root[:], err)
		}
	}
	return state.LoadDynamicProperties(db, sysKV)
}

// loadGenesisDP loads dynamic properties at the genesis state root straight
// from a raw disk store (no BlockChain). Used by genesis-bootstrap tests.
func loadGenesisDP(tb testing.TB, diskdb ethdb.KeyValueStore) *state.DynamicProperties {
	tb.Helper()
	sdb := state.NewDatabase(rawdb.WrapKeyValueStore(diskdb))
	return loadDPAtRoot(tb, diskdb, sdb, rawdb.ReadGenesisStateRoot(diskdb))
}
