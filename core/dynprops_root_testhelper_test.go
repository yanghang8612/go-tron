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
		if index, ok := db.(interface {
			ethdb.KeyValueReader
			ethdb.KeyValueWriter
			ethdb.Iteratee
		}); ok {
			sysKV.SetAccountKVIndexStore(index)
			sysKV.SetAccountKVIndexReads(true)
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

// seedRootedProposal stages proposal records + the matching index into the
// genesis state root and re-points both the genesis-root and genesis-block
// root pointers to the post-seed root, so a BlockChain opened afterward carries
// the proposals forward. Mirrors the rooted-seed pattern in block_builder_test.
// Proposals are runtime-created in production (genesis never seeds them), so
// this exists only to set up maintenance-boundary tests.
func seedRootedProposal(tb testing.TB, diskdb ethdb.KeyValueStore, sdb *state.Database, genesisHash tcommon.Hash, proposals []*rawdb.Proposal) {
	tb.Helper()
	root := rawdb.ReadGenesisStateRoot(diskdb)
	statedb, err := state.New(root, sdb)
	if err != nil {
		tb.Fatalf("open genesis state: %v", err)
	}
	ids := make([]int64, len(proposals))
	for i, p := range proposals {
		if err := statedb.WriteProposal(p.ID, p); err != nil {
			tb.Fatalf("seed proposal %d: %v", p.ID, err)
		}
		ids[i] = p.ID
	}
	if err := statedb.WriteProposalIndex(ids); err != nil {
		tb.Fatalf("seed proposal index: %v", err)
	}
	newRoot, err := statedb.Commit()
	if err != nil {
		tb.Fatalf("commit seeded proposals: %v", err)
	}
	rawdb.WriteGenesisStateRoot(diskdb, newRoot)
	rawdb.WriteBlockStateRoot(diskdb, genesisHash, newRoot)
}

// readRootedProposal resolves a proposal record from the rooted SystemProposal
// KV at the given state root.
func readRootedProposal(tb testing.TB, sdb *state.Database, root tcommon.Hash, id int64) *rawdb.Proposal {
	tb.Helper()
	statedb, err := state.New(root, sdb)
	if err != nil {
		tb.Fatalf("open state at %x: %v", root[:], err)
	}
	return statedb.ReadProposal(id)
}
