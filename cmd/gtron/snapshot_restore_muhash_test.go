package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepkg "github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/params"
)

func TestSnapshotRestoreMuHashFreshCLI(t *testing.T) {
	for _, mode := range []string{"success", "latest-ahead", "boundary-hash", "chain-root", "no-freezer", "java-trie"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "snapshot")
			dataDir := filepath.Join(root, "target")
			genesis := params.DefaultMainnetGenesis()
			g, err := corepkg.GenesisWireBlock(genesis)
			if err != nil {
				t.Fatal(err)
			}
			block := snapshotCmdBlock(1)
			if mode == "java-trie" {
				block.SetAccountStateRoot(common.Hash{9})
			}
			source := rawdb.NewMemoryDatabase()
			defer source.Close()
			owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, make([]byte, 20)...))
			if err := rawdb.WriteStateAccountLatest(source, owner, []byte("owned source")); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteCycleRewardPending(source, 7, map[common.Address]int64{owner: 123}); err != nil {
				t.Fatal(err)
			}
			latestT := uint64(1)
			if mode == "latest-ahead" {
				latestT = 2
			}
			head := snapshotCmdSeedMuHashHead(t, source, block, latestT)
			identityHash := block.Hash()
			if mode == "boundary-hash" {
				identityHash = common.Hash{99}
				head.BlockHash = identityHash
				if err := rawdb.WriteStateExecutionProperties(source, rawdb.StateExecutionProperties{BlockNum: 1, Timestamp: block.Timestamp(), BlockHash: identityHash}); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateCommitmentHead(source, head); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateCommitmentCheckpoint(source, &rawdb.StateCommitmentCheckpoint{BlockNum: 1, BlockHash: identityHash, Root: head.Root, Scheme: head.Scheme}); err != nil {
					t.Fatal(err)
				}
			}
			if err := rawdb.WriteStateTxRange(source, 1, identityHash, latestT, latestT); err != nil {
				t.Fatal(err)
			}
			// Production writes only HEAD. The cut checkpoint must be synthesized.
			if err := rawdb.DeleteStateCommitmentCheckpoint(source, head.BlockNum); err != nil {
				t.Fatal(err)
			}
			if _, present, err := rawdb.ReadLatestStateCommitmentCheckpoint(source); err != nil || present {
				t.Fatalf("source artificially retains checkpoint %v %v", present, err)
			}
			built, err := statesnapshots.NewAggregator(dir).BuildLatest(source, statesnapshots.AggregatorBuildOptions{FromTxNum: 1, ToTxNum: latestT})
			if err != nil {
				t.Fatal(err)
			}
			historySource := source
			historyBlock := uint64(1)
			historyHash := identityHash
			if mode == "latest-ahead" {
				historySource = rawdb.NewMemoryDatabase()
				defer historySource.Close()
				historyBlock = 0
				historyHash = g.Hash()
				if err := rawdb.WriteStateTxRange(historySource, 0, historyHash, 1, 1); err != nil {
					t.Fatal(err)
				}
			}
			if err := rawdb.WriteStateDomainChange(historySource, &rawdb.StateDomainChange{BlockNum: historyBlock, BlockHash: historyHash, TxNum: 1, Seq: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner, NextExists: true, Next: []byte("owned source")}); err != nil {
				t.Fatal(err)
			}
			history, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(historySource, dir, 1, 1, "history/state-domain-change-1-1.seg")
			if err != nil {
				t.Fatal(err)
			}
			refs := append(built.Segments, history...)
			if mode != "no-freezer" {
				src := openSnapshotCmdFreezer(t, filepath.Join(root, "freezer"))
				defer src.Close()
				chainRoot := head.Root
				if mode == "chain-root" {
					chainRoot = common.Hash{77}
				}
				appendSnapshotCmdFreezerRows(t, src, []snapshotCmdFreezerRow{{block: g, txInfosRaw: snapshotCmdTxInfoRawForBlock(t, g), stateRoot: rawdb.NewStateCommitmentHead().Root.Bytes()}, {block: block, stateRoot: chainRoot.Bytes()}})
				freezer, err := statesnapshots.BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(src), dir, "", 0, 1)
				if err != nil {
					t.Fatal(err)
				}
				index, err := statesnapshots.BuildChainIndexSegmentFromChainFreezerSegment(dir, freezer, "")
				if err != nil {
					t.Fatal(err)
				}
				refs = append(refs, freezer, index)
			}
			identity, err := snapshotExpectedChainIdentityFromGenesis(genesis, "")
			if err != nil {
				t.Fatal(err)
			}
			if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifestForChain(1, 1, refs, identity)); err != nil {
				t.Fatal(err)
			}
			pub, priv, err := ed25519.GenerateKey(nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := statesnapshots.PublishSignedSnapshotCatalog(dir, priv); err != nil {
				t.Fatal(err)
			}
			ctx := makeSnapshotRestoreTestContext(t, []string{"--datadir", dataDir, "--snapshot.dir", dir, "--snapshot.trusted-key", hex.EncodeToString(pub)})
			err = snapshotRestoreCmd(ctx)
			db, openErr := openPebbleDB(ctx, chainDataDir(dataDir))
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer db.Close()
			restored, present, readErr := rawdb.ReadStateCommitmentHead(db)
			if readErr != nil {
				t.Fatal(readErr)
			}
			marker, readErr := rawdb.StateCommitmentRestoreInProgress(db)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if mode != "success" && mode != "latest-ahead" {
				if err == nil || present || !marker || rawdb.ReadHeadBlockHash(db) != (common.Hash{}) {
					t.Fatalf("failure published: err=%v HEAD=%v marker=%v", err, present, marker)
				}
				if mode == "java-trie" && !strings.Contains(err.Error(), "Java account-state trie") {
					t.Fatal(err)
				}
				return
			}
			if err != nil || !present || marker || restored != head {
				t.Fatalf("restore err=%v head=%+v marker=%v", err, restored, marker)
			}
			allocator, ok, rangeErr := rawdb.ReadStateTxRange(db, head.BlockNum)
			if rangeErr != nil || !ok || allocator.EndTxNum != latestT {
				t.Fatalf("current allocator=%+v %v %v", allocator, ok, rangeErr)
			}
			cycle, rewards, present, readErr := rawdb.ReadCycleRewardPending(db)
			if readErr != nil || !present || cycle != 7 || rewards[owner] != 123 {
				t.Fatalf("pending lost cycle=%d rewards=%v present=%v err=%v", cycle, rewards, present, readErr)
			}
			for n, want := range []common.Hash{g.Hash(), block.Hash()} {
				got := rawdb.ReadTaposRef(db, []byte{0, byte(n)})
				if !bytes.Equal(got, want[8:16]) {
					t.Fatalf("TAPOS%d=%x, want%x", n, got, want[8:16])
				}
			}

		})
	}
}

func TestRebuildSnapshotTAPOSWrapExcludesFuture(t *testing.T) {
	source := rawdb.NewMemoryDatabase()
	defer source.Close()
	for n := uint64(0); n <= 65537; n++ {
		if err := rawdb.WriteBlock(source, snapshotCmdBlock(n)); err != nil {
			t.Fatal(err)
		}
	}
	target := rawdb.NewMemoryDatabase()
	defer target.Close()
	if err := rebuildSnapshotTAPOS(target, rawdb.NewChainDB(source, rawdb.NoopAncient{}), 65536); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		slot  uint16
		block uint64
	}{{0, 65536}, {1, 1}, {65535, 65535}} {
		want := snapshotCmdBlock(tc.block).Hash()
		got := rawdb.ReadTaposRef(target, []byte{byte(tc.slot >> 8), byte(tc.slot)})
		if !bytes.Equal(got, want[8:16]) {
			t.Fatalf("slot%d=%x, want block%d=%x", tc.slot, got, tc.block, want[8:16])
		}
	}
}
