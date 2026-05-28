package core

import (
	"errors"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

// Archive-query RPC surface over flat temporal state history.
//
// These tests cover the TronBackend.*At methods that wrap the slice-3
// PersistentHistoryReader: GetBalanceAt / GetCodeAt / GetStorageAtBlock.
// They are the "cross-impl parity" tests in self-consistency form (the
// brief and plan allow a deterministic fixture in place of a build-tagged
// java-tron run): build a chain, capture history, then assert the
// reconstructed as-of-N answer equals the value that was live at N.

// archiveBackend wraps a fresh history-enabled chain in a TronBackend so the
// archive-query methods can be exercised end-to-end. Reuses the slice-4
// fixture (three witnesses, only one produces → solidified pinned at 0 so
// every applyBlock layer stays in bc.buffer and the reader serves through
// the buffer overlay).
func archiveBackend(t *testing.T) (*TronBackend, tcommon.Address, tcommon.Address) {
	t.Helper()
	bc, witness := newHistoryReorgChain(t)
	t.Cleanup(func() { bc.Close() })
	b := &TronBackend{chain: bc}
	// recipient = addr(2): buildTransferBlock credits it `amount` per block.
	return b, witness, testInsertAddr(2)
}

// TestArchiveQuery_BalanceAtBlock builds a chain that bumps a recipient's
// balance by a known amount each block, then queries GetBalanceAt at every
// historical height and asserts the reconstructed value matches the value
// that was live at that height. Recipient end-of-N balance == running sum
// of per-block amounts {1*1000, 2*1000, ...}.
func TestArchiveQuery_BalanceAtBlock(t *testing.T) {
	b, witness, recipient := archiveBackend(t)
	bc := b.chain

	const numBlocks = 6
	want := make([]int64, numBlocks+1) // want[N] = recipient balance at end-of-N
	parent := bc.genesisBlock.Hash()
	var running int64
	for n := int64(1); n <= numBlocks; n++ {
		amount := n * 1000
		blk := buildTransferBlock(t, n, n*3000, parent, witness, amount)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		running += amount
		want[n] = running
	}
	if got := bc.CurrentBlock().Number(); got != numBlocks {
		t.Fatalf("head = %d, want %d", got, numBlocks)
	}

	// Historical queries: balance at the end of each past block must equal
	// the running sum captured above — proving the reader rolled back the
	// later blocks' deltas correctly.
	for n := uint64(1); n <= numBlocks; n++ {
		got, err := b.GetBalanceAt(recipient, n)
		if err != nil {
			t.Fatalf("GetBalanceAt(recipient, %d): %v", n, err)
		}
		if got != want[n] {
			t.Errorf("GetBalanceAt(recipient, %d) = %d, want %d", n, got, want[n])
		}
	}

	// Query at head must equal the live balance (and the final running sum).
	headGot, err := b.GetBalanceAt(recipient, numBlocks)
	if err != nil {
		t.Fatalf("GetBalanceAt(recipient, head): %v", err)
	}
	if live := b.GetBalance(recipient); headGot != live {
		t.Errorf("GetBalanceAt(recipient, head) = %d, live GetBalance = %d", headGot, live)
	}
	if headGot != want[numBlocks] {
		t.Errorf("head balance = %d, want %d", headGot, want[numBlocks])
	}

	// A block number past head has no committed state; it must not silently
	// resolve to the live value.
	if _, err := b.GetBalanceAt(recipient, numBlocks+100); err == nil {
		t.Fatal("GetBalanceAt(recipient, head+100) returned nil error")
	}

	// Independent oracle cross-check: the history reader (rollback over flat
	// temporal domain changes) must agree byte-for-byte with the account view
	// reconstructed from each block's committed state root — a completely
	// separate code path. This validates BOTH the credited recipient AND
	// the debited sender (whose balance also absorbs the one-time
	// account-creation fee for addr(2)), without the test having to model
	// fees itself. This is the slice-7 cross-impl parity assertion in
	// self-consistency form.
	for n := uint64(1); n <= numBlocks; n++ {
		for _, addr := range []tcommon.Address{recipient, witness} {
			oracle, err := b.GetAccountAt(addr, n)
			if err != nil {
				t.Fatalf("oracle GetAccountAt(%x, %d): %v", addr[:4], n, err)
			}
			got, err := b.GetBalanceAt(addr, n)
			if err != nil {
				t.Fatalf("GetBalanceAt(%x, %d): %v", addr[:4], n, err)
			}
			if got != oracle.Balance() {
				t.Errorf("GetBalanceAt(%x, %d) = %d, oracle (state-root view) = %d",
					addr[:4], n, got, oracle.Balance())
			}
		}
	}
}

// TestArchiveQuery_GetAccountAtFallsBackToHistory verifies the slice-7
// upgrade to TronBackend.GetAccountAt: when a block's committed state root
// has been pruned (StateRootAtBlock -> zero) the method reconstructs the
// account from flat temporal history instead of erroring. This is the
// TRON-flavored archive surface (/walletsolidity/getaccount over any past
// block). The fast path (root present) is unchanged and covered elsewhere.
func TestArchiveQuery_GetAccountAtFallsBackToHistory(t *testing.T) {
	b, witness, recipient := archiveBackend(t)
	bc := b.chain

	const numBlocks = 5
	parent := bc.genesisBlock.Hash()
	blocks := make([]*types.Block, numBlocks+1)
	blocks[0] = bc.genesisBlock
	for n := int64(1); n <= numBlocks; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		blocks[n] = blk
	}

	// Capture the ground-truth account at block 2 via the present-root fast
	// path BEFORE pruning.
	const prunedHeight = 2
	want, err := b.GetAccountAt(recipient, prunedHeight)
	if err != nil {
		t.Fatalf("GetAccountAt(recipient, %d) pre-prune: %v", prunedHeight, err)
	}
	wantBal := want.Balance()

	// Simulate full-mode pruning: drop the committed state root for block 2.
	// StateRootAtBlock now returns zero for it (the block proto carries no
	// account_state_root, so there's no fallback root either), forcing the
	// history-reader path.
	rawdb.DeleteBlockStateRoot(bc.db, blocks[prunedHeight].Hash())
	if root := bc.StateRootAtBlock(prunedHeight); root != (tcommon.Hash{}) {
		t.Fatalf("state root for block %d still present after delete: %x", prunedHeight, root)
	}

	// GetAccountAt must now reconstruct via history and return the same
	// balance the fast path returned before pruning.
	got, err := b.GetAccountAt(recipient, prunedHeight)
	if err != nil {
		t.Fatalf("GetAccountAt(recipient, %d) post-prune (archive fallback): %v", prunedHeight, err)
	}
	if got.Balance() != wantBal {
		t.Errorf("archive-fallback GetAccountAt(recipient, %d).Balance() = %d, want %d",
			prunedHeight, got.Balance(), wantBal)
	}

	// The genesis-funded sender reconstructs too (debit + creation-fee path).
	senderWant, err := b.GetBalanceAt(witness, prunedHeight)
	if err != nil {
		t.Fatalf("GetBalanceAt(sender, %d): %v", prunedHeight, err)
	}
	senderGot, err := b.GetAccountAt(witness, prunedHeight)
	if err != nil {
		t.Fatalf("GetAccountAt(sender, %d) post-prune: %v", prunedHeight, err)
	}
	if senderGot.Balance() != senderWant {
		t.Errorf("archive-fallback GetAccountAt(sender, %d).Balance() = %d, want %d",
			prunedHeight, senderGot.Balance(), senderWant)
	}
}

func TestArchiveQuery_RewardAtUsesHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain

	const numBlocks = 4
	parent := bc.genesisBlock.Hash()
	for n := int64(1); n <= numBlocks; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
	}

	wantAcc, err := b.GetAccountAt(witness, 1)
	if err != nil {
		t.Fatalf("GetAccountAt(witness, 1): %v", err)
	}
	headAcc, err := b.GetAccountAt(witness, numBlocks)
	if err != nil {
		t.Fatalf("GetAccountAt(witness, head): %v", err)
	}
	if wantAcc.Allowance() == headAcc.Allowance() {
		t.Fatalf("test setup did not change allowance: block1=%d head=%d", wantAcc.Allowance(), headAcc.Allowance())
	}
	got, err := b.GetRewardAt(witness, 1)
	if err != nil {
		t.Fatalf("GetRewardAt(witness, 1): %v", err)
	}
	if got.Reward != wantAcc.Allowance() {
		t.Fatalf("GetRewardAt(witness, 1) = %d, want %d", got.Reward, wantAcc.Allowance())
	}
}

func TestArchiveQuery_AccountResourceAtUsesHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	sender := testInsertAddr(1)

	const numBlocks = 4
	parent := bc.genesisBlock.Hash()
	for n := int64(1); n <= numBlocks; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
	}

	wantAcc, err := b.GetAccountAt(sender, 1)
	if err != nil {
		t.Fatalf("GetAccountAt(sender, 1): %v", err)
	}
	headAcc, err := b.GetAccountAt(sender, numBlocks)
	if err != nil {
		t.Fatalf("GetAccountAt(sender, head): %v", err)
	}
	if wantAcc.FreeNetUsage() == headAcc.FreeNetUsage() && wantAcc.NetUsage() == headAcc.NetUsage() {
		t.Fatalf("test setup did not change resource usage: block1 free=%d net=%d head free=%d net=%d",
			wantAcc.FreeNetUsage(), wantAcc.NetUsage(), headAcc.FreeNetUsage(), headAcc.NetUsage())
	}
	got, err := b.GetAccountResourceAt(sender, 1)
	if err != nil {
		t.Fatalf("GetAccountResourceAt(sender, 1): %v", err)
	}
	if got.FreeNetUsed != wantAcc.FreeNetUsage() || got.NetUsed != wantAcc.NetUsage() {
		t.Fatalf("GetAccountResourceAt(sender, 1) usage free=%d net=%d, want free=%d net=%d",
			got.FreeNetUsed, got.NetUsed, wantAcc.FreeNetUsage(), wantAcc.NetUsage())
	}
}

func TestArchiveQuery_HistoryReaderHoldsChainMutex(t *testing.T) {
	b, _, _ := archiveBackend(t)

	reader, _, releaseHistory, err := b.historyReaderAt()
	if err != nil {
		t.Fatalf("historyReaderAt: %v", err)
	}
	if reader == nil {
		t.Fatal("historyReaderAt returned nil reader")
	}

	locked := make(chan struct{})
	done := make(chan struct{})
	go func() {
		b.chain.chainmu.Lock()
		close(locked)
		b.chain.chainmu.Unlock()
		close(done)
	}()

	select {
	case <-locked:
		t.Fatal("historyReaderAt returned without holding chainmu")
	case <-time.After(20 * time.Millisecond):
	}

	releaseHistory()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("chainmu was not released")
	}
}

// TestArchiveQuery_GetAccountAtPrunedRootNoHistory verifies that on a
// non-archive node, GetAccountAt for a block whose state root was pruned
// returns ErrArchiveHistoryDisabled (actionable) rather than reconstructing
// or returning a generic error.
func TestArchiveQuery_GetAccountAtPrunedRootNoHistory(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = false
	witness := testInsertAddr(1)
	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts:  []params.GenesisAccount{{Address: witness, Balance: 99_000_000_000_000_000}},
		Witnesses: []params.GenesisWitness{
			{Address: witness, VoteCount: 1, URL: "test"},
			{Address: testInsertAddr(20), VoteCount: 1, URL: "sr2"},
			{Address: testInsertAddr(21), VoteCount: 1, URL: "sr3"},
		},
		DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), cfg)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}
	defer bc.Close()
	b := &TronBackend{chain: bc}

	parent := bc.genesisBlock.Hash()
	var b2 *types.Block
	for n := int64(1); n <= 3; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		if n == 2 {
			b2 = blk
		}
	}
	// Prune block 2's root. With history disabled the archive fallback must
	// refuse rather than silently degrade.
	rawdb.DeleteBlockStateRoot(bc.db, b2.Hash())
	if _, err := b.GetAccountAt(testInsertAddr(2), 2); !errors.Is(err, ErrArchiveHistoryDisabled) {
		t.Errorf("GetAccountAt with pruned root + history disabled: err = %v, want ErrArchiveHistoryDisabled", err)
	}
}

func TestArchiveQuery_PruneFloorRejectsUnavailableHistory(t *testing.T) {
	b, witness, recipient := archiveBackend(t)
	bc := b.chain

	const numBlocks = 6
	parent := bc.genesisBlock.Hash()
	var block2 *types.Block
	for n := int64(1); n <= numBlocks; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		if n == 2 {
			block2 = blk
		}
	}

	bc.buffer.BeginBlock(tcommon.Hash{0xEE})
	for n := uint64(1); n <= 3; n++ {
		if err := rawdb.DeleteStateDomainChanges(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateDomainChanges(%d): %v", n, err)
		}
		if err := rawdb.DeleteStateTxRange(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateTxRange(%d): %v", n, err)
		}
	}
	bc.buffer.CommitBlock()
	rawdb.DeleteBlockStateRoot(bc.db, block2.Hash())

	if _, err := b.GetBalanceAt(recipient, 2); !errors.Is(err, ErrArchiveHistoryPruned) {
		t.Fatalf("GetBalanceAt below prune floor: err = %v, want ErrArchiveHistoryPruned", err)
	}
	if _, err := b.GetCodeAt(recipient, 2); !errors.Is(err, ErrArchiveHistoryPruned) {
		t.Fatalf("GetCodeAt below prune floor: err = %v, want ErrArchiveHistoryPruned", err)
	}
	var slot tcommon.Hash
	if _, err := b.GetStorageAtBlock(recipient, slot, 2); !errors.Is(err, ErrArchiveHistoryPruned) {
		t.Fatalf("GetStorageAtBlock below prune floor: err = %v, want ErrArchiveHistoryPruned", err)
	}
	if _, err := b.GetAccountAt(recipient, 2); !errors.Is(err, ErrArchiveHistoryPruned) {
		t.Fatalf("GetAccountAt below prune floor with pruned root: err = %v, want ErrArchiveHistoryPruned", err)
	}
}

func TestArchiveQuery_UsesColdStateDomainChangeSnapshots(t *testing.T) {
	b, witness, recipient := archiveBackend(t)
	bc := b.chain
	bc.config.HistoryMode = params.HistoryModeSnap
	bc.config.HistoryPruneWindow = 1

	const numBlocks = 4
	parent := bc.genesisBlock.Hash()
	want := make([]int64, numBlocks+1)
	var running int64
	for n := int64(1); n <= numBlocks; n++ {
		amount := n * 1000
		blk := buildTransferBlock(t, n, n*3000, parent, witness, amount)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		running += amount
		want[n] = running
	}

	range2, ok, err := rawdb.ReadStateTxRange(bc.buffer, 2)
	if err != nil || !ok {
		t.Fatalf("read block 2 tx range: ok=%v err=%v", ok, err)
	}
	range3, ok, err := rawdb.ReadStateTxRange(bc.buffer, 3)
	if err != nil || !ok {
		t.Fatalf("read block 3 tx range: ok=%v err=%v", ok, err)
	}
	dir := t.TempDir()
	refs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(bc.buffer, dir, range2.BeginTxNum, range3.EndTxNum, "history/state-domain-change-2-3.seg")
	if err != nil {
		t.Fatalf("build cold state-domain-change segment: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(range2.BeginTxNum, range3.EndTxNum, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	b.SetStateColdHistory(mgr)

	bc.buffer.BeginBlock(tcommon.Hash{0xCF})
	for n := uint64(2); n <= 3; n++ {
		if err := rawdb.DeleteStateDomainChanges(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateDomainChanges(%d): %v", n, err)
		}
		if err := rawdb.DeleteStateTxRange(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateTxRange(%d): %v", n, err)
		}
	}
	bc.buffer.CommitBlock()

	for n := uint64(1); n <= numBlocks; n++ {
		got, err := b.GetBalanceAt(recipient, n)
		if err != nil {
			t.Fatalf("cold GetBalanceAt(recipient, %d): %v", n, err)
		}
		if got != want[n] {
			t.Errorf("cold GetBalanceAt(recipient, %d) = %d, want %d", n, got, want[n])
		}
	}
}

// TestArchiveQuery_GatedOnHistoryEnabled verifies the HistoryEnabled gate:
// on a node that did NOT capture history, an archive query for a block
// older than head returns ErrArchiveHistoryDisabled, while a query at head
// still succeeds from live state.
func TestArchiveQuery_GatedOnHistoryEnabled(t *testing.T) {
	// Build a chain with HistoryEnabled=false. Single producing witness so
	// blocks advance head; the absence of flat temporal rows is the point.
	diskdb := ethrawdb.NewMemoryDatabase()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = false
	witness := testInsertAddr(1)
	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witness, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witness, VoteCount: 1, URL: "test"},
			{Address: testInsertAddr(20), VoteCount: 1, URL: "sr2"},
			{Address: testInsertAddr(21), VoteCount: 1, URL: "sr3"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, cfg)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}
	defer bc.Close()
	b := &TronBackend{chain: bc}

	recipient := testInsertAddr(2)
	parent := bc.genesisBlock.Hash()
	for n := int64(1); n <= 3; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
	}
	head := bc.CurrentBlock().Number()
	if head != 3 {
		t.Fatalf("head = %d, want 3", head)
	}

	// Archive query for an OLD block must be gated.
	for _, n := range []uint64{1, 2} {
		_, err := b.GetBalanceAt(recipient, n)
		if !errors.Is(err, ErrArchiveHistoryDisabled) {
			t.Errorf("GetBalanceAt(recipient, %d) err = %v, want ErrArchiveHistoryDisabled", n, err)
		}
		if _, err := b.GetCodeAt(recipient, n); !errors.Is(err, ErrArchiveHistoryDisabled) {
			t.Errorf("GetCodeAt(recipient, %d) err = %v, want ErrArchiveHistoryDisabled", n, err)
		}
		var slot tcommon.Hash
		if _, err := b.GetStorageAtBlock(recipient, slot, n); !errors.Is(err, ErrArchiveHistoryDisabled) {
			t.Errorf("GetStorageAtBlock(recipient, _, %d) err = %v, want ErrArchiveHistoryDisabled", n, err)
		}
	}

	// Query AT head succeeds even with history disabled (served from live).
	if _, err := b.GetBalanceAt(recipient, head); err != nil {
		t.Errorf("GetBalanceAt(recipient, head) with history disabled: %v", err)
	}
	// A block past head must fail before the history-enabled gate; returning
	// live state here would make an explicit future block indistinguishable
	// from "latest".
	if _, err := b.GetBalanceAt(recipient, head+50); err == nil {
		t.Error("GetBalanceAt(recipient, head+50) returned nil error")
	}
}

func TestArchiveQuery_FutureBlockRejected(t *testing.T) {
	b, witness, recipient := archiveBackend(t)
	bc := b.chain

	parent := bc.genesisBlock.Hash()
	for n := int64(1); n <= 2; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
	}
	future := bc.CurrentBlock().Number() + 1

	if _, err := b.GetAccountAt(recipient, future); err == nil {
		t.Fatal("GetAccountAt future block returned nil error")
	}
	if _, err := b.GetBalanceAt(recipient, future); err == nil {
		t.Fatal("GetBalanceAt future block returned nil error")
	}
	if _, err := b.GetCodeAt(recipient, future); err == nil {
		t.Fatal("GetCodeAt future block returned nil error")
	}
	var slot tcommon.Hash
	if _, err := b.GetStorageAtBlock(recipient, slot, future); err == nil {
		t.Fatal("GetStorageAtBlock future block returned nil error")
	}
}
