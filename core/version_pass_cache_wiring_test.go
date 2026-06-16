package core

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/types"
)

// allUpgradeForkStats is a ForkStatsReader that reports every one of its N
// witness slots voting upgrade for any version — enough to clear the rate gate.
type allUpgradeForkStats int

func (n allUpgradeForkStats) ReadForkStats(int32) []byte {
	return bytes.Repeat([]byte{forks.VoteUpgrade}, int(n))
}

// warmVersionPassCache memoizes `version` as activated in bc's fork-pass cache.
// A block time well past the Aug-2020 hard-fork epoch plus an all-upgrade
// 27-slot bitmap clears both the time and rate gates, so Pass records it.
func warmVersionPassCache(t *testing.T, bc *BlockChain, version int32) {
	t.Helper()
	if !bc.versionPassCache.Pass(allUpgradeForkStats(27), version, 2_000_000_000_000, 21_600_000) {
		t.Fatalf("setup: version %d should pass with an all-upgrade bitmap", version)
	}
	if !bc.versionPassCache.IsPassed(version) {
		t.Fatalf("setup: version %d should be memoized after Pass", version)
	}
}

// TestVersionPassCache_SwitchForkResetsOnReorg is the reorg-safety wiring test:
// a version memoized as passed on the orphan branch must be dropped when
// switchFork rewinds below it, so the re-applied canonical branch re-evaluates
// each version from live fork-stats. Mirrors the proposalCache reset discipline.
func TestVersionPassCache_SwitchForkResetsOnReorg(t *testing.T) {
	bc, witnessAddr := newHistoryReorgChain(t)
	defer bc.Close()

	// Chain A: 5 transfer blocks (orphan side, amount n*999).
	chainA := make([]*types.Block, 6)
	chainA[0] = bc.genesisBlock
	for n := int64(1); n <= 5; n++ {
		b := buildTransferBlock(t, n, n*3000, chainA[n-1].Hash(), witnessAddr, n*999)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("insert A%d: %v", n, err)
		}
		chainA[n] = b
	}

	// Chain B: 6 transfer blocks branching from genesis (+1 ts offset → distinct
	// hashes, amount n*1000 → strictly longer once B6 lands).
	chainB := make([]*types.Block, 7)
	chainB[0] = bc.genesisBlock
	for n := int64(1); n <= 6; n++ {
		chainB[n] = buildTransferBlock(t, n, n*3000+1, chainB[n-1].Hash(), witnessAddr, n*1000)
	}
	// B1..B5 are a competing branch; chain A stays canonical (equal length).
	for n := 1; n <= 5; n++ {
		if err := bc.InsertBlock(chainB[n]); err != nil {
			t.Fatalf("insert B%d (pre-switch): %v", n, err)
		}
	}
	if bc.CurrentBlock().Hash() != chainA[5].Hash() {
		t.Fatalf("after inserting B1..B5 chain A should still be canonical")
	}

	// Warm the cache immediately before the reorg-triggering insert.
	warmVersionPassCache(t, bc, 35)

	// B6 makes chain B strictly longer → switchFork.
	if err := bc.InsertBlock(chainB[6]); err != nil {
		t.Fatalf("insert B6 (switch trigger): %v", err)
	}
	if got := bc.CurrentBlock().Number(); got != 6 {
		t.Fatalf("after switchFork head number = %d, want 6", got)
	}

	if bc.versionPassCache.IsPassed(35) {
		t.Fatal("switchFork must reset versionPassCache: a reorg can rewind below a version's activation block")
	}
}

// TestVersionPassCache_PersistsAcrossNormalApply is the complement: a clean
// head-extending block must NOT drop the memo, so the per-tx fork gate keeps
// paying off across blocks (the whole point of the cache).
func TestVersionPassCache_PersistsAcrossNormalApply(t *testing.T) {
	bc, witnessAddr := newHistoryReorgChain(t)
	defer bc.Close()

	b1 := buildTransferBlock(t, 1, 3000, bc.genesisBlock.Hash(), witnessAddr, 1000)
	if err := bc.InsertBlock(b1); err != nil {
		t.Fatalf("insert block 1: %v", err)
	}

	warmVersionPassCache(t, bc, 35)

	b2 := buildTransferBlock(t, 2, 6000, b1.Hash(), witnessAddr, 2000)
	if err := bc.InsertBlock(b2); err != nil {
		t.Fatalf("insert block 2: %v", err)
	}
	if bc.CurrentBlock().Number() != 2 {
		t.Fatalf("head should have advanced to 2, got %d", bc.CurrentBlock().Number())
	}

	if !bc.versionPassCache.IsPassed(35) {
		t.Fatal("a clean head-extending apply must NOT reset versionPassCache")
	}
}

// TestVersionPassCache_FailedApplyResetsCache covers the applyBlockWithPlan
// error path: a pass memoized while executing a block that then fails apply is
// dropped, matching the proposalCache failed-apply discipline.
func TestVersionPassCache_FailedApplyResetsCache(t *testing.T) {
	bc, witnessAddr := newHistoryReorgChain(t)
	defer bc.Close()

	good := buildTransferBlock(t, 1, 3000, bc.genesisBlock.Hash(), witnessAddr, 1000)
	if err := bc.InsertBlock(good); err != nil {
		t.Fatalf("insert good block: %v", err)
	}

	warmVersionPassCache(t, bc, 35)

	// A transfer far exceeding the sender's balance fails during apply.
	bad := buildTransferBlock(t, 2, 6000, good.Hash(), witnessAddr, 1_000_000_000_000_000_000)
	if err := bc.InsertBlock(bad); err == nil {
		t.Fatal("expected an over-balance transfer block to fail apply")
	}
	if bc.versionPassCache.IsPassed(35) {
		t.Fatal("a failed apply must reset versionPassCache")
	}
}
