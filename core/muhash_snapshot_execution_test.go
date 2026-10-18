package core

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	"reflect"
	"testing"
)

func TestMuHashFreshSnapshotContinuesAcrossMaintenance(t *testing.T) {
	witness := testInsertAddr(1)
	cfg := *params.MainnetChainConfig
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{Config: &cfg, Accounts: []params.GenesisAccount{{Address: witness, Balance: 99_000_000_000_000_000}}, Witnesses: []params.GenesisWitness{{Address: witness, VoteCount: 1, URL: "test"}}, DynamicProperties: map[string]int64{"next_maintenance_time": 30_000, "maintenance_time_interval": 30_000, "change_delegation": 1, "current_cycle_number": 7, "witness_pay_per_block": 1000, "witness_127_pay_per_block": 0}}
	source, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	_, genHash, err := SetupGenesisBlock(source, genesis)
	if err != nil {
		t.Fatal(err)
	}
	sourceDisk := rawdb.WrapKeyValueStore(source)
	control, err := NewBlockChain(source, state.NewDatabase(sourceDisk), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	blocks := chainFrom(control.CurrentBlock(), witness, 2, 0)
	if err = control.InsertBlocks(blocks); err != nil {
		t.Fatal(err)
	}
	pin, head, err := control.AcquireStateLatestSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cycle, rewards, present, err := rawdb.ReadCycleRewardPending(pin)
	if err != nil || !present || len(rewards) == 0 {
		t.Fatalf("fixture lacks nonzero midcycle pending: %v", err)
	}
	dir := t.TempDir()
	built, err := snapshots.NewAggregator(dir).BuildSegments(pin, snapshots.AggregatorBuildOptions{FromTxNum: 1, ToTxNum: head.TxNum})
	if err != nil {
		t.Fatal(err)
	}
	if _, present, err := rawdb.ReadLatestStateCommitmentCheckpoint(source); err != nil || present {
		t.Fatal("live path unexpectedly wrote per-block checkpoint")
	}
	previous, present, err := rawdb.ReadStateTxRange(pin, blocks[0].Number())
	if err != nil || !present {
		t.Fatal(err)
	}
	// Keep the actual latest source cut ahead of the separately materialized
	// history range. The allocator's current cut comes from ExecutionCheckpoint.
	latestOnly := built[:0]
	for _, ref := range built {
		if ref.Dataset != snapshots.SegmentDatasetStateDomainChange {
			latestOnly = append(latestOnly, ref)
		}
	}
	historical, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(pin, dir, 1, previous.EndTxNum, "history/state-domain-change-older.seg")
	if err != nil {
		t.Fatal(err)
	}
	built = append(latestOnly, historical...)
	identity := snapshots.ChainIdentity{ChainID: 1, NetworkID: 11111, GenesisHash: fmt.Sprintf("%x", genHash[:]), ForkConfigHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	manifest := snapshots.NewManifestForChain(1, previous.EndTxNum, built, identity)
	if err = snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	if err = pin.Close(); err != nil {
		t.Fatal(err)
	}
	target, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	allBlocks := append([]*types.Block{control.genesisBlock}, blocks...)
	opts := snapshots.RestoreVerifiedSnapshotOptions{BeforePublish: func(staged ethdb.KeyValueStore, m *snapshots.Manifest) error {
		// Fixture wire payload represents the separate chain/freezer transport.
		for _, b := range allBlocks {
			if err := rawdb.WriteBlock(staged, b); err != nil {
				return err
			}
			if err := rawdb.WriteTaposRef(staged, b.Number(), b.Hash()); err != nil {
				return err
			}
		}
		if err := rawdb.WriteBlockStateRoot(staged, head.BlockHash, head.Root); err != nil {
			return err
		}
		rawdb.WriteGenesisStateRoot(staged, rawdb.ReadGenesisStateRoot(source))
		_, err := snapshots.VerifyCanonicalBoundaryFromRestoredSnapshot(staged, rawdb.NewChainDB(staged, rawdb.NoopAncient{}), m)
		return err
	}}
	if _, err = snapshots.RestoreSnapshotFromVerifiedManifestWithOptions(target, dir, identity, opts); err != nil {
		t.Fatal(err)
	}
	restoredCycle, restoredRewards, ok, err := rawdb.ReadCycleRewardPending(target)
	if err != nil || !ok || restoredCycle != cycle || !reflect.DeepEqual(restoredRewards, rewards) {
		t.Fatal("snapshot lost exact pending rewards")
	}
	restored, err := NewBlockChain(target, state.NewDatabase(rawdb.WrapKeyValueStore(target)), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var controlHooks, restoreHooks []string
	control.AddMaintenanceHook(func(*types.Block, []common.Address) { controlHooks = append(controlHooks, "maintenance") })
	control.AddBlockHook(func(*types.Block) { controlHooks = append(controlHooks, "block") })
	restored.AddMaintenanceHook(func(*types.Block, []common.Address) { restoreHooks = append(restoreHooks, "maintenance") })
	restored.AddBlockHook(func(*types.Block) { restoreHooks = append(restoreHooks, "block") })
	for _, timestamp := range []int64{30_000, 33_000} {
		b := buildTestBlock(control, witness, timestamp)
		if err = control.InsertBlock(b); err != nil {
			t.Fatal(err)
		}
		if err = restored.InsertBlock(b); err != nil {
			t.Fatal(err)
		}
		controlRange, ok, e := rawdb.ReadStateTxRange(control.buffer, b.Number())
		if e != nil || !ok {
			t.Fatal(e)
		}
		restoredRange, ok, e := rawdb.ReadStateTxRange(restored.buffer, b.Number())
		if e != nil || !ok || *restoredRange != *controlRange {
			t.Fatal("restored allocator transaction cut diverged")
		}
		if restored.HeadStateRoot() != control.HeadStateRoot() {
			t.Fatal("restored execution root diverged")
		}
	}
	if fmt.Sprint(controlHooks) != "[maintenance block block]" || !reflect.DeepEqual(restoreHooks, controlHooks) {
		t.Fatalf("hook order control=%v restored=%v", controlHooks, restoreHooks)
	}
	for _, bc := range []*BlockChain{control, restored} {
		s, err := bc.openCurrentState()
		if err != nil {
			t.Fatal(err)
		}
		if got := s.ReadCycleReward(cycle, witness.Bytes()); got <= 0 {
			t.Fatalf("maintenance failed to settle restored pending reward: %d", got)
		}
	}
}
