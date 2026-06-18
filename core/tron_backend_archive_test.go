package core

import (
	"bytes"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
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
	gotNet, err := b.GetAccountNetAt(sender, 1)
	if err != nil {
		t.Fatalf("GetAccountNetAt(sender, 1): %v", err)
	}
	if gotNet.GetFreeNetUsed() != wantAcc.FreeNetUsage() || gotNet.GetNetUsed() != wantAcc.NetUsage() {
		t.Fatalf("GetAccountNetAt(sender, 1) usage free=%d net=%d, want free=%d net=%d",
			gotNet.GetFreeNetUsed(), gotNet.GetNetUsed(), wantAcc.FreeNetUsage(), wantAcc.NetUsage())
	}
}

func TestArchiveQuery_AccountByIdAtUsesSystemAccountIndexHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	accountID := []byte("User1234")
	addr1 := testInsertAddr(40)
	addr2 := testInsertAddr(41)

	parent := bc.genesisBlock.Hash()
	var block1, block2 *types.Block
	for n := int64(1); n <= 2; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		if n == 1 {
			block1 = blk
		} else {
			block2 = blk
		}
	}
	if block1 == nil || block2 == nil {
		t.Fatal("test setup did not build both blocks")
	}

	root := bc.StateRootAtBlock(0)
	commitAccountID := func(blk *types.Block, n int64, addr tcommon.Address, balance int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())
		statedb.CreateAccount(addr, corepb.AccountType_Normal)
		statedb.AddBalance(addr, balance)
		statedb.SetAccountId(addr, string(accountID))
		if err := statedb.WriteAccountIdIndex(accountID, addr); err != nil {
			t.Fatalf("write account id index block %d: %v", n, err)
		}
		root, err := statedb.Commit()
		if err != nil {
			t.Fatalf("commit account id block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitAccountID(block1, 1, addr1, 111)
	root = commitAccountID(block2, 2, addr2, 222)

	got1, err := b.GetAccountByIdAt([]byte("USER1234"), block1.Number())
	if err != nil {
		t.Fatalf("GetAccountByIdAt(block1): %v", err)
	}
	if got1.Address() != addr1 || got1.Balance() != 111 {
		t.Fatalf("block1 account = %x balance=%d, want %x balance=111", got1.Address(), got1.Balance(), addr1)
	}
	got2, err := b.GetAccountByIdAt([]byte("user1234"), block2.Number())
	if err != nil {
		t.Fatalf("GetAccountByIdAt(block2): %v", err)
	}
	if got2.Address() != addr2 || got2.Balance() != 222 {
		t.Fatalf("block2 account = %x balance=%d, want %x balance=222", got2.Address(), got2.Balance(), addr2)
	}
}

func TestArchiveQuery_DelegatedResourceV2AtUsesSystemDelegationHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	from := testInsertAddr(30)
	to1 := testInsertAddr(31)
	to2 := testInsertAddr(32)

	parent := bc.genesisBlock.Hash()
	var block1, block2 *types.Block
	for n := int64(1); n <= 2; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		switch n {
		case 1:
			block1 = blk
		case 2:
			block2 = blk
		}
	}
	if block1 == nil || block2 == nil {
		t.Fatal("test setup did not build both blocks")
	}

	root := bc.StateRootAtBlock(0)
	commitDelegation := func(blk *types.Block, n int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())
		if err := statedb.WriteDelegatedResourceV2(from, to1, false, &rawdb.DelegatedResource{
			From:                      from,
			To:                        to1,
			FrozenBalanceForBandwidth: n * 100,
			ExpireTimeForBandwidth:    n * 1000,
		}); err != nil {
			t.Fatalf("write unlocked delegation block %d: %v", n, err)
		}
		receivers := []tcommon.Address{to1}
		if n == 2 {
			if err := statedb.WriteDelegatedResourceV2(from, to1, true, &rawdb.DelegatedResource{
				From:                   from,
				To:                     to1,
				FrozenBalanceForEnergy: 222,
				ExpireTimeForEnergy:    333,
			}); err != nil {
				t.Fatalf("write locked delegation block %d: %v", n, err)
			}
			receivers = append(receivers, to2)
		}
		if err := statedb.WriteDelegationIndex(from, receivers); err != nil {
			t.Fatalf("write delegation index block %d: %v", n, err)
		}
		root, err := statedb.Commit()
		if err != nil {
			t.Fatalf("commit delegation block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitDelegation(block1, 1)
	root = commitDelegation(block2, 2)

	block1Resources, err := b.GetDelegatedResourceV2At(from, to1, block1.Number())
	if err != nil {
		t.Fatalf("GetDelegatedResourceV2At(block1): %v", err)
	}
	if len(block1Resources) != 1 || block1Resources[0].FrozenBalanceForBandwidth != 100 || block1Resources[0].FrozenBalanceForEnergy != 0 {
		t.Fatalf("block1 delegated resources = %+v, want one unlocked bandwidth record", block1Resources)
	}
	block2Resources, err := b.GetDelegatedResourceV2At(from, to1, block2.Number())
	if err != nil {
		t.Fatalf("GetDelegatedResourceV2At(block2): %v", err)
	}
	if len(block2Resources) != 2 || block2Resources[0].FrozenBalanceForBandwidth != 200 || block2Resources[1].FrozenBalanceForEnergy != 222 {
		t.Fatalf("block2 delegated resources = %+v, want updated unlocked plus locked record", block2Resources)
	}

	block1Index, err := b.GetDelegatedResourceAccountIndexV2At(from, block1.Number())
	if err != nil {
		t.Fatalf("GetDelegatedResourceAccountIndexV2At(block1): %v", err)
	}
	if len(block1Index.ToAddresses) != 1 || block1Index.ToAddresses[0] != hex.EncodeToString(to1[:]) {
		t.Fatalf("block1 delegation index = %+v, want only %x", block1Index, to1[:])
	}
	block2Index, err := b.GetDelegatedResourceAccountIndexV2At(from, block2.Number())
	if err != nil {
		t.Fatalf("GetDelegatedResourceAccountIndexV2At(block2): %v", err)
	}
	if len(block2Index.ToAddresses) != 2 ||
		block2Index.ToAddresses[0] != hex.EncodeToString(to1[:]) ||
		block2Index.ToAddresses[1] != hex.EncodeToString(to2[:]) {
		t.Fatalf("block2 delegation index = %+v, want %x,%x", block2Index, to1[:], to2[:])
	}
}

func TestArchiveQuery_MarketQueriesAtUseSystemMarketHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	owner := testInsertAddr(50)
	orderID := []byte("order-1")
	sellTokenID := []byte("sell")
	buyTokenID := []byte("buy")

	parent := bc.genesisBlock.Hash()
	var block1, block2 *types.Block
	for n := int64(1); n <= 2; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		switch n {
		case 1:
			block1 = blk
		case 2:
			block2 = blk
		}
	}
	if block1 == nil || block2 == nil {
		t.Fatal("test setup did not build both blocks")
	}

	root := bc.StateRootAtBlock(0)
	commitMarket := func(blk *types.Block, n int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())

		order := &corepb.MarketOrder{
			OrderId:                 orderID,
			OwnerAddress:            owner[:],
			SellTokenId:             sellTokenID,
			SellTokenQuantity:       n * 100,
			BuyTokenId:              buyTokenID,
			BuyTokenQuantity:        n * 1000,
			SellTokenQuantityRemain: n * 10,
			SellTokenQuantityReturn: n,
			State:                   corepb.MarketOrder_ACTIVE,
			CreateTime:              n * 3000,
		}
		if err := statedb.WriteMarketOrder(orderID, order); err != nil {
			t.Fatalf("write market order block %d: %v", n, err)
		}
		if err := statedb.WriteMarketAccountOrder(owner[:], &corepb.MarketAccountOrder{
			OwnerAddress: owner[:],
			Orders:       [][]byte{orderID},
			Count:        1,
			TotalCount:   n,
		}); err != nil {
			t.Fatalf("write market account order block %d: %v", n, err)
		}
		if err := statedb.WriteMarketPriceList(sellTokenID, buyTokenID, &corepb.MarketPriceList{
			SellTokenId: sellTokenID,
			BuyTokenId:  buyTokenID,
			Prices: []*corepb.MarketPrice{{
				SellTokenQuantity: n * 100,
				BuyTokenQuantity:  n * 1000,
			}},
		}); err != nil {
			t.Fatalf("write market price list block %d: %v", n, err)
		}
		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit market block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitMarket(block1, 1)
	root = commitMarket(block2, 2)

	order1, err := b.GetMarketOrderByIDAt(orderID, block1.Number())
	if err != nil {
		t.Fatalf("GetMarketOrderByIDAt(block1): %v", err)
	}
	if order1 == nil || order1.GetSellTokenQuantity() != 100 || order1.GetBuyTokenQuantity() != 1000 {
		t.Fatalf("block1 market order = %+v, want block1 quantities", order1)
	}
	order2, err := b.GetMarketOrderByIDAt(orderID, block2.Number())
	if err != nil {
		t.Fatalf("GetMarketOrderByIDAt(block2): %v", err)
	}
	if order2 == nil || order2.GetSellTokenQuantity() != 200 || order2.GetBuyTokenQuantity() != 2000 {
		t.Fatalf("block2 market order = %+v, want block2 quantities", order2)
	}

	orders1, err := b.GetMarketOrdersByAccountAt(owner, block1.Number())
	if err != nil {
		t.Fatalf("GetMarketOrdersByAccountAt(block1): %v", err)
	}
	if len(orders1) != 1 || orders1[0].GetSellTokenQuantity() != 100 {
		t.Fatalf("block1 market orders = %+v, want block1 order", orders1)
	}
	orders2, err := b.GetMarketOrdersByAccountAt(owner, block2.Number())
	if err != nil {
		t.Fatalf("GetMarketOrdersByAccountAt(block2): %v", err)
	}
	if len(orders2) != 1 || orders2[0].GetSellTokenQuantity() != 200 {
		t.Fatalf("block2 market orders = %+v, want block2 order", orders2)
	}

	prices1, err := b.GetMarketPriceByPairAt(sellTokenID, buyTokenID, block1.Number())
	if err != nil {
		t.Fatalf("GetMarketPriceByPairAt(block1): %v", err)
	}
	if len(prices1.GetPrices()) != 1 || prices1.GetPrices()[0].GetSellTokenQuantity() != 100 {
		t.Fatalf("block1 market prices = %+v, want block1 price", prices1.GetPrices())
	}
	prices2, err := b.GetMarketPriceByPairAt(sellTokenID, buyTokenID, block2.Number())
	if err != nil {
		t.Fatalf("GetMarketPriceByPairAt(block2): %v", err)
	}
	if len(prices2.GetPrices()) != 1 || prices2.GetPrices()[0].GetSellTokenQuantity() != 200 {
		t.Fatalf("block2 market prices = %+v, want block2 price", prices2.GetPrices())
	}
}

func TestArchiveQuery_ListExchangesAtUsesSystemExchangeHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	creator := testInsertAddr(60)

	parent := bc.genesisBlock.Hash()
	var block1, block2 *types.Block
	for n := int64(1); n <= 2; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		switch n {
		case 1:
			block1 = blk
		case 2:
			block2 = blk
		}
	}
	if block1 == nil || block2 == nil {
		t.Fatal("test setup did not build both blocks")
	}

	root := bc.StateRootAtBlock(0)
	commitExchanges := func(blk *types.Block, n int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())

		dynProps := state.NewDynamicProperties()
		dynProps.SetLatestExchangeNum(n)
		if n == 2 {
			dynProps.SetAllowSameTokenName(true)
		}
		if err := dynProps.FlushRooted(statedb); err != nil {
			t.Fatalf("flush dynamic properties block %d: %v", n, err)
		}

		if n == 1 {
			if err := statedb.WriteExchange(&corepb.Exchange{
				ExchangeId:         1,
				CreatorAddress:     creator[:],
				FirstTokenId:       []byte("TOKEN"),
				FirstTokenBalance:  100,
				SecondTokenId:      []byte("_"),
				SecondTokenBalance: 1000,
			}); err != nil {
				t.Fatalf("write v1 exchange block %d: %v", n, err)
			}
			if err := statedb.WriteExchangeV2(&corepb.Exchange{
				ExchangeId:         1,
				CreatorAddress:     creator[:],
				FirstTokenId:       []byte("1000001"),
				FirstTokenBalance:  200,
				SecondTokenId:      []byte("_"),
				SecondTokenBalance: 2000,
			}); err != nil {
				t.Fatalf("write v2 exchange block %d: %v", n, err)
			}
		} else {
			if err := statedb.WriteExchangeV2(&corepb.Exchange{
				ExchangeId:         1,
				CreatorAddress:     creator[:],
				FirstTokenId:       []byte("1000001"),
				FirstTokenBalance:  300,
				SecondTokenId:      []byte("_"),
				SecondTokenBalance: 3000,
			}); err != nil {
				t.Fatalf("write updated v2 exchange block %d: %v", n, err)
			}
			if err := statedb.WriteExchangeV2(&corepb.Exchange{
				ExchangeId:         2,
				CreatorAddress:     creator[:],
				FirstTokenId:       []byte("1000002"),
				FirstTokenBalance:  400,
				SecondTokenId:      []byte("_"),
				SecondTokenBalance: 4000,
			}); err != nil {
				t.Fatalf("write second v2 exchange block %d: %v", n, err)
			}
		}

		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit exchanges block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitExchanges(block1, 1)
	root = commitExchanges(block2, 2)

	block1Exchanges, err := b.ListExchangesAt(block1.Number())
	if err != nil {
		t.Fatalf("ListExchangesAt(block1): %v", err)
	}
	if len(block1Exchanges) != 1 ||
		block1Exchanges[0].GetExchangeId() != 1 ||
		string(block1Exchanges[0].GetFirstTokenId()) != "TOKEN" ||
		block1Exchanges[0].GetFirstTokenBalance() != 100 {
		t.Fatalf("block1 exchanges = %+v, want V1 pre-AllowSameTokenName exchange", block1Exchanges)
	}

	block2Exchanges, err := b.ListExchangesAt(block2.Number())
	if err != nil {
		t.Fatalf("ListExchangesAt(block2): %v", err)
	}
	if len(block2Exchanges) != 2 ||
		string(block2Exchanges[0].GetFirstTokenId()) != "1000001" ||
		block2Exchanges[0].GetFirstTokenBalance() != 300 ||
		block2Exchanges[1].GetExchangeId() != 2 ||
		block2Exchanges[1].GetFirstTokenBalance() != 400 {
		t.Fatalf("block2 exchanges = %+v, want V2 post-AllowSameTokenName exchanges", block2Exchanges)
	}
}

func TestArchiveQuery_AssetIssueAtUsesSystemAssetHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	issuer := testInsertAddr(70)

	parent := bc.genesisBlock.Hash()
	var block1, block2 *types.Block
	for n := int64(1); n <= 2; n++ {
		blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
		if err := bc.InsertBlock(blk); err != nil {
			t.Fatalf("insert block %d: %v", n, err)
		}
		parent = blk.Hash()
		switch n {
		case 1:
			block1 = blk
		case 2:
			block2 = blk
		}
	}
	if block1 == nil || block2 == nil {
		t.Fatal("test setup did not build both blocks")
	}

	asset := func(id int64, name string, supply int64) *contractpb.AssetIssueContract {
		return &contractpb.AssetIssueContract{
			Id:           strconv.FormatInt(id, 10),
			OwnerAddress: issuer[:],
			Name:         []byte(name),
			TotalSupply:  supply,
			TrxNum:       1,
			Num:          1,
		}
	}

	root := bc.StateRootAtBlock(0)
	commitAssets := func(blk *types.Block, n int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())

		dynProps := state.NewDynamicProperties()
		dynProps.SetTokenIdNum(firstAssetTokenID + n - 1)
		if n == 2 {
			dynProps.SetAllowSameTokenName(true)
		}
		if err := dynProps.FlushRooted(statedb); err != nil {
			t.Fatalf("flush dynamic properties block %d: %v", n, err)
		}

		if n == 1 {
			v2 := asset(firstAssetTokenID, "TOKEN", 101)
			legacy := asset(firstAssetTokenID, "TOKEN", 100)
			if err := statedb.WriteAssetIssue(firstAssetTokenID, v2); err != nil {
				t.Fatalf("write v2 asset block %d: %v", n, err)
			}
			if err := statedb.WriteAssetIssueByName([]byte("TOKEN"), legacy); err != nil {
				t.Fatalf("write legacy asset block %d: %v", n, err)
			}
			if err := statedb.WriteAssetNameIndex([]byte("TOKEN"), firstAssetTokenID); err != nil {
				t.Fatalf("write name index block %d: %v", n, err)
			}
		} else {
			if err := statedb.WriteAssetIssue(firstAssetTokenID, asset(firstAssetTokenID, "TOKEN", 201)); err != nil {
				t.Fatalf("write updated v2 asset block %d: %v", n, err)
			}
			if err := statedb.WriteAssetIssue(firstAssetTokenID+1, asset(firstAssetTokenID+1, "TOKEN2", 300)); err != nil {
				t.Fatalf("write second v2 asset block %d: %v", n, err)
			}
		}

		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit assets block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitAssets(block1, 1)
	root = commitAssets(block2, 2)

	block1ByID, err := b.GetAssetIssueByIDAt(firstAssetTokenID, block1.Number())
	if err != nil {
		t.Fatalf("GetAssetIssueByIDAt(block1): %v", err)
	}
	if block1ByID == nil || block1ByID.GetTotalSupply() != 101 {
		t.Fatalf("block1 by id = %+v, want V2 supply 101", block1ByID)
	}
	block1ByName, err := b.GetAssetIssueByNameAt([]byte("TOKEN"), block1.Number())
	if err != nil {
		t.Fatalf("GetAssetIssueByNameAt(block1): %v", err)
	}
	if block1ByName == nil || block1ByName.GetTotalSupply() != 100 {
		t.Fatalf("block1 by name = %+v, want legacy supply 100", block1ByName)
	}
	block1List, err := b.GetAssetIssueListAt(block1.Number())
	if err != nil {
		t.Fatalf("GetAssetIssueListAt(block1): %v", err)
	}
	if len(block1List) != 1 || block1List[0].GetTotalSupply() != 100 {
		t.Fatalf("block1 asset list = %+v, want legacy list", block1List)
	}

	block2ByName, err := b.GetAssetIssueByNameAt([]byte("TOKEN2"), block2.Number())
	if err != nil {
		t.Fatalf("GetAssetIssueByNameAt(block2): %v", err)
	}
	if block2ByName == nil || block2ByName.GetId() != strconv.FormatInt(firstAssetTokenID+1, 10) || block2ByName.GetTotalSupply() != 300 {
		t.Fatalf("block2 by name = %+v, want V2 TOKEN2", block2ByName)
	}
	block2List, err := b.GetAssetIssueListAt(block2.Number())
	if err != nil {
		t.Fatalf("GetAssetIssueListAt(block2): %v", err)
	}
	if len(block2List) != 2 ||
		block2List[0].GetTotalSupply() != 201 ||
		block2List[1].GetTotalSupply() != 300 {
		t.Fatalf("block2 asset list = %+v, want V2 list", block2List)
	}
	block2Page, err := b.GetAssetIssueListPaginatedAt(1, 1, block2.Number())
	if err != nil {
		t.Fatalf("GetAssetIssueListPaginatedAt(block2): %v", err)
	}
	if len(block2Page) != 1 || block2Page[0].GetTotalSupply() != 300 {
		t.Fatalf("block2 asset page = %+v, want second V2 asset", block2Page)
	}
}

func TestArchiveQuery_ArchiveStateSessionHoldsChainMutex(t *testing.T) {
	b, _, _ := archiveBackend(t)

	session, err := b.archiveStateAt(0)
	if err != nil {
		t.Fatalf("archiveStateAt: %v", err)
	}
	if session.reader == nil {
		t.Fatal("archiveStateAt returned nil reader")
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

	session.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("chainmu was not released")
	}
}

func TestArchiveQuery_ArchiveStateSessionReleasesChainMutexOnGateError(t *testing.T) {
	b, _, _ := archiveBackend(t)

	if _, err := b.archiveStateAt(1); err == nil {
		t.Fatal("archiveStateAt future block returned nil error")
	}

	locked := make(chan struct{})
	go func() {
		b.chain.chainmu.Lock()
		close(locked)
		b.chain.chainmu.Unlock()
	}()

	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("archiveStateAt gate error leaked chainmu")
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

	bc.buffer.BeginBlock(tcommon.Hash{0xEE}, 0) // sentinel; archive test layer
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

func TestArchiveQuery_BlocksAndMinimalModesUsePruneWindowGate(t *testing.T) {
	for _, mode := range []string{params.HistoryModeBlocks, params.HistoryModeMinimal} {
		t.Run(mode, func(t *testing.T) {
			b, witness, recipient := archiveBackend(t)
			bc := b.chain
			bc.config.HistoryMode = mode
			bc.config.HistoryPruneWindow = 2

			parent := bc.genesisBlock.Hash()
			for n := int64(1); n <= 5; n++ {
				blk := buildTransferBlock(t, n, n*3000, parent, witness, n*1000)
				if err := bc.InsertBlock(blk); err != nil {
					t.Fatalf("insert block %d: %v", n, err)
				}
				parent = blk.Hash()
			}
			if head := bc.CurrentBlock().Number(); head != 5 {
				t.Fatalf("head = %d, want 5", head)
			}

			if got, err := b.GetBalanceAt(recipient, 4); err != nil || got == 0 {
				t.Fatalf("GetBalanceAt inside %s prune window = %d/%v, want non-zero balance", mode, got, err)
			}
			_, err := b.GetBalanceAt(recipient, 3)
			if !errors.Is(err, ErrArchiveHistoryPruned) {
				t.Fatalf("GetBalanceAt below %s prune window err = %v, want ErrArchiveHistoryPruned", mode, err)
			}
			if !strings.Contains(err.Error(), "first_available=4") {
				t.Fatalf("GetBalanceAt below %s prune window err = %v, want first_available=4", mode, err)
			}
		})
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

	type resourceSnapshot struct {
		freeNetUsed      int64
		freeNetLimit     int64
		netUsed          int64
		totalNetLimit    int64
		totalEnergyLimit int64
	}
	resourceAt := func(blockNum uint64) resourceSnapshot {
		t.Helper()
		res, err := b.GetAccountResourceAt(witness, blockNum)
		if err != nil {
			t.Fatalf("GetAccountResourceAt(witness, %d): %v", blockNum, err)
		}
		return resourceSnapshot{
			freeNetUsed:      res.FreeNetUsed,
			freeNetLimit:     res.FreeNetLimit,
			netUsed:          res.NetUsed,
			totalNetLimit:    res.TotalNetLimit,
			totalEnergyLimit: res.TotalEnergyLimit,
		}
	}
	resourceWant := map[uint64]resourceSnapshot{
		2: resourceAt(2),
		3: resourceAt(3),
	}
	if resourceWant[2].freeNetUsed == resourceWant[3].freeNetUsed && resourceWant[2].netUsed == resourceWant[3].netUsed {
		t.Fatalf("test setup did not change resource usage: block2=%+v block3=%+v", resourceWant[2], resourceWant[3])
	}
	rewardAt := func(blockNum uint64) int64 {
		t.Helper()
		reward, err := b.GetRewardAt(witness, blockNum)
		if err != nil {
			t.Fatalf("GetRewardAt(witness, %d): %v", blockNum, err)
		}
		return reward.Reward
	}
	rewardWant := map[uint64]int64{
		2: rewardAt(2),
		3: rewardAt(3),
	}
	if rewardWant[2] == rewardWant[3] {
		t.Fatalf("test setup did not change reward: block2=%d block3=%d", rewardWant[2], rewardWant[3])
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

	bc.buffer.BeginBlock(tcommon.Hash{0xCF}, 0) // sentinel; archive test layer
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
	for n, wantResource := range resourceWant {
		gotResource := resourceAt(n)
		if gotResource != wantResource {
			t.Errorf("cold GetAccountResourceAt(witness, %d) = %+v, want %+v", n, gotResource, wantResource)
		}
	}
	for n, wantReward := range rewardWant {
		gotReward := rewardAt(n)
		if gotReward != wantReward {
			t.Errorf("cold GetRewardAt(witness, %d) = %d, want %d", n, gotReward, wantReward)
		}
	}
}

func TestArchiveQuery_CodeAndStorageUseColdStateDomainChangeSnapshots(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	bc.config.HistoryMode = params.HistoryModeSnap
	bc.config.HistoryPruneWindow = 1

	const numBlocks = 4
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

	range2, ok, err := rawdb.ReadStateTxRange(bc.buffer, 2)
	if err != nil || !ok {
		t.Fatalf("read block 2 tx range: ok=%v err=%v", ok, err)
	}
	range3, ok, err := rawdb.ReadStateTxRange(bc.buffer, 3)
	if err != nil || !ok {
		t.Fatalf("read block 3 tx range: ok=%v err=%v", ok, err)
	}

	contract := testInsertAddr(42)
	slot := tcommon.Hash{0xAA}
	code2 := []byte{0x60, 0x02, 0x60, 0x0A}
	code3 := []byte{0x60, 0x03, 0x60, 0x0B}
	codeHash2 := tcommon.Keccak256(code2)
	codeHash3 := tcommon.Keccak256(code3)
	storage2 := tcommon.Hash{0x02}
	storage3 := tcommon.Hash{0x03}

	root := bc.HeadStateRoot()
	bc.buffer.BeginBlock(tcommon.Hash{0xCD}, 0) // sentinel; archive test layer
	statedb, err := state.New(root, bc.StateDB())
	if err != nil {
		t.Fatalf("open manual domain state: %v", err)
	}
	if err := bc.prepareOpenState(statedb); err != nil {
		t.Fatalf("prepare manual domain state: %v", err)
	}
	applyDomainBlock := func(blockNum uint64, blockHash tcommon.Hash, txRange *rawdb.StateTxRange, mutate func(*state.StateDB)) {
		t.Helper()
		statedb.BeginDomainChangeJournalCapture(bc.buffer, blockNum, blockHash, txRange.BeginTxNum, txRange.EndTxNum)
		mark := statedb.DomainChangeJournalMark()
		mutate(statedb)
		if err := statedb.FlushDomainChangesSince(mark, txRange.EndTxNum); err != nil {
			t.Fatalf("flush domain changes block %d: %v", blockNum, err)
		}
		nextRoot, err := statedb.Commit()
		if err != nil {
			t.Fatalf("commit manual domain state block %d: %v", blockNum, err)
		}
		root = nextRoot
		statedb, err = state.New(root, bc.StateDB())
		if err != nil {
			t.Fatalf("reopen manual domain state block %d: %v", blockNum, err)
		}
		if err := bc.prepareOpenState(statedb); err != nil {
			t.Fatalf("prepare reopened manual domain state block %d: %v", blockNum, err)
		}
	}
	applyDomainBlock(2, blocks[2].Hash(), range2, func(s *state.StateDB) {
		s.SetCode(contract, code2)
		s.SetState(contract, slot, storage2)
	})
	applyDomainBlock(3, blocks[3].Hash(), range3, func(s *state.StateDB) {
		s.SetCode(contract, code3)
		s.SetState(contract, slot, storage3)
	})
	bc.buffer.CommitBlock()

	assertArchiveCodeStorage := func(label string, blockNum uint64, wantCode []byte, wantStorage tcommon.Hash) {
		t.Helper()
		gotCode, err := b.GetCodeAt(contract, blockNum)
		if err != nil {
			t.Fatalf("%s GetCodeAt(contract, %d): %v", label, blockNum, err)
		}
		if !bytes.Equal(gotCode, wantCode) {
			t.Fatalf("%s GetCodeAt(contract, %d) = %x, want %x", label, blockNum, gotCode, wantCode)
		}
		gotStorage, err := b.GetStorageAtBlock(contract, slot, blockNum)
		if err != nil {
			t.Fatalf("%s GetStorageAtBlock(contract, slot, %d): %v", label, blockNum, err)
		}
		if gotStorage != wantStorage {
			t.Fatalf("%s GetStorageAtBlock(contract, slot, %d) = %x, want %x", label, blockNum, gotStorage, wantStorage)
		}
	}
	assertArchiveCodeStorage("hot", 2, code2, storage2)
	assertArchiveCodeStorage("hot", 3, code3, storage3)

	dir := t.TempDir()
	historyRefs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(bc.buffer, dir, range2.BeginTxNum, range3.EndTxNum, "history/state-domain-change-code-storage-2-3.seg")
	if err != nil {
		t.Fatalf("build cold state-domain-change segment: %v", err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := statesnapshots.BuildCodeSegmentFilesFromDB(bc.buffer, dir, range2.BeginTxNum, range3.EndTxNum, "latest/code-2-3.seg")
	if err != nil {
		t.Fatalf("build cold code segment: %v", err)
	}
	refs := append(historyRefs, codeRef, codeAccessorRef, codeBTreeRef)
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(range2.BeginTxNum, range3.EndTxNum, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	b.SetStateColdHistory(mgr)

	bc.buffer.BeginBlock(tcommon.Hash{0xCE}, 0) // sentinel; archive prune layer
	for n := uint64(2); n <= 3; n++ {
		if err := rawdb.DeleteStateDomainChanges(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateDomainChanges(%d): %v", n, err)
		}
		if err := rawdb.DeleteStateTxRange(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateTxRange(%d): %v", n, err)
		}
	}
	if err := rawdb.DeleteStateCode(bc.buffer, codeHash2); err != nil {
		t.Fatalf("delete hot code2: %v", err)
	}
	if err := rawdb.DeleteStateCode(bc.buffer, codeHash3); err != nil {
		t.Fatalf("delete hot code3: %v", err)
	}
	bc.buffer.CommitBlock()

	assertArchiveCodeStorage("cold", 2, code2, storage2)
	assertArchiveCodeStorage("cold", 3, code3, storage3)
}

func TestArchiveQuery_ContractRecreateStorageGenerationUsesColdStateDomainChangeSnapshots(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	bc.config.HistoryMode = params.HistoryModeSnap
	bc.config.HistoryPruneWindow = 1

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

	range2, ok, err := rawdb.ReadStateTxRange(bc.buffer, 2)
	if err != nil || !ok {
		t.Fatalf("read block 2 tx range: ok=%v err=%v", ok, err)
	}
	range3, ok, err := rawdb.ReadStateTxRange(bc.buffer, 3)
	if err != nil || !ok {
		t.Fatalf("read block 3 tx range: ok=%v err=%v", ok, err)
	}
	range4, ok, err := rawdb.ReadStateTxRange(bc.buffer, 4)
	if err != nil || !ok {
		t.Fatalf("read block 4 tx range: ok=%v err=%v", ok, err)
	}

	contract := testInsertAddr(43)
	var slotA, slotB tcommon.Hash
	slotA[31] = 0xAA
	slotB[31] = 0xBB
	codeA := []byte{0x60, 0x0A, 0x60, 0x01}
	codeB := []byte{0x60, 0x0B, 0x60, 0x02}
	codeHashA := tcommon.Keccak256(codeA)
	codeHashB := tcommon.Keccak256(codeB)
	storageA0 := tcommon.HexToHash("a0")
	storageB0 := tcommon.HexToHash("b0")
	storageA1 := tcommon.HexToHash("a1")

	root := bc.HeadStateRoot()
	bc.buffer.BeginBlock(tcommon.Hash{0xDD}, 0) // sentinel; archive test layer
	statedb, err := state.New(root, bc.StateDB())
	if err != nil {
		t.Fatalf("open manual recreate state: %v", err)
	}
	if err := bc.prepareOpenState(statedb); err != nil {
		t.Fatalf("prepare manual recreate state: %v", err)
	}
	applyDomainBlock := func(blockNum uint64, blockHash tcommon.Hash, txRange *rawdb.StateTxRange, mutate func(*state.StateDB)) {
		t.Helper()
		statedb.BeginDomainChangeJournalCapture(bc.buffer, blockNum, blockHash, txRange.BeginTxNum, txRange.EndTxNum)
		mark := statedb.DomainChangeJournalMark()
		mutate(statedb)
		if err := statedb.FlushDomainChangesSince(mark, txRange.EndTxNum); err != nil {
			t.Fatalf("flush domain changes block %d: %v", blockNum, err)
		}
		nextRoot, err := statedb.Commit()
		if err != nil {
			t.Fatalf("commit manual domain state block %d: %v", blockNum, err)
		}
		root = nextRoot
		statedb, err = state.New(root, bc.StateDB())
		if err != nil {
			t.Fatalf("reopen manual domain state block %d: %v", blockNum, err)
		}
		if err := bc.prepareOpenState(statedb); err != nil {
			t.Fatalf("prepare reopened manual domain state block %d: %v", blockNum, err)
		}
	}
	applyDomainBlock(2, blocks[2].Hash(), range2, func(s *state.StateDB) {
		s.CreateAccount(contract, corepb.AccountType_Contract)
		s.SetCode(contract, codeA)
		s.SetState(contract, slotA, storageA0)
		s.SetState(contract, slotB, storageB0)
	})
	applyDomainBlock(3, blocks[3].Hash(), range3, func(s *state.StateDB) {
		s.SelfDestruct(contract)
		s.FinalizeTransaction()
	})
	applyDomainBlock(4, blocks[4].Hash(), range4, func(s *state.StateDB) {
		s.CreateAccount(contract, corepb.AccountType_Contract)
		s.SetCode(contract, codeB)
		s.SetState(contract, slotA, storageA1)
	})
	bc.buffer.CommitBlock()

	assertArchiveCodeStorage := func(label string, blockNum uint64, wantCode []byte, wantA, wantB tcommon.Hash) {
		t.Helper()
		gotCode, err := b.GetCodeAt(contract, blockNum)
		if err != nil {
			t.Fatalf("%s GetCodeAt(contract, %d): %v", label, blockNum, err)
		}
		if !bytes.Equal(gotCode, wantCode) {
			t.Fatalf("%s GetCodeAt(contract, %d) = %x, want %x", label, blockNum, gotCode, wantCode)
		}
		gotA, err := b.GetStorageAtBlock(contract, slotA, blockNum)
		if err != nil {
			t.Fatalf("%s GetStorageAtBlock(contract, slotA, %d): %v", label, blockNum, err)
		}
		if gotA != wantA {
			t.Fatalf("%s slotA @%d = %x, want %x", label, blockNum, gotA, wantA)
		}
		gotB, err := b.GetStorageAtBlock(contract, slotB, blockNum)
		if err != nil {
			t.Fatalf("%s GetStorageAtBlock(contract, slotB, %d): %v", label, blockNum, err)
		}
		if gotB != wantB {
			t.Fatalf("%s slotB @%d = %x, want %x", label, blockNum, gotB, wantB)
		}
	}
	assertArchiveCodeStorage("hot pre-destroy", 2, codeA, storageA0, storageB0)
	assertArchiveCodeStorage("hot destroyed", 3, nil, tcommon.Hash{}, tcommon.Hash{})
	assertArchiveCodeStorage("hot recreated", 4, codeB, storageA1, tcommon.Hash{})

	dir := t.TempDir()
	historyRefs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(bc.buffer, dir, range2.BeginTxNum, range4.EndTxNum, "history/state-domain-change-recreate-2-4.seg")
	if err != nil {
		t.Fatalf("build cold state-domain-change segment: %v", err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := statesnapshots.BuildCodeSegmentFilesFromDB(bc.buffer, dir, range2.BeginTxNum, range4.EndTxNum, "latest/code-recreate-2-4.seg")
	if err != nil {
		t.Fatalf("build cold code segment: %v", err)
	}
	refs := append(historyRefs, codeRef, codeAccessorRef, codeBTreeRef)
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(range2.BeginTxNum, range4.EndTxNum, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	b.SetStateColdHistory(mgr)

	bc.buffer.BeginBlock(tcommon.Hash{0xDE}, 0) // sentinel; archive prune layer
	for n := uint64(2); n <= 4; n++ {
		if err := rawdb.DeleteStateDomainChanges(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateDomainChanges(%d): %v", n, err)
		}
		if err := rawdb.DeleteStateTxRange(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateTxRange(%d): %v", n, err)
		}
	}
	if err := rawdb.DeleteStateCode(bc.buffer, codeHashA); err != nil {
		t.Fatalf("delete hot codeA: %v", err)
	}
	if err := rawdb.DeleteStateCode(bc.buffer, codeHashB); err != nil {
		t.Fatalf("delete hot codeB: %v", err)
	}
	bc.buffer.CommitBlock()

	assertArchiveCodeStorage("cold pre-destroy", 2, codeA, storageA0, storageB0)
	assertArchiveCodeStorage("cold destroyed", 3, nil, tcommon.Hash{}, tcommon.Hash{})
	assertArchiveCodeStorage("cold recreated", 4, codeB, storageA1, tcommon.Hash{})

	rpcServer := jsonrpc.NewServer(b, 0)
	defer func() { _ = rpcServer.Stop() }()
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	contractArg := "0x" + contract.Hex()
	slotAArg := "0x" + slotA.Hex()
	slotBArg := "0x" + slotB.Hex()
	assertRPCResult := func(method string, params any, want string) {
		t.Helper()
		resp := postCoreJSONRPC(t, httpServer.URL, method, params)
		if got := resp["result"]; got != want {
			t.Fatalf("%s(%v) result = %v, want %s", method, params, got, want)
		}
	}
	assertRPCResult("eth_getCode", []any{contractArg, "0x2"}, "0x"+hex.EncodeToString(codeA))
	assertRPCResult("eth_getStorageAt", []any{contractArg, slotAArg, "0x2"}, "0x"+storageA0.Hex())
	assertRPCResult("eth_getStorageAt", []any{contractArg, slotBArg, "0x2"}, "0x"+storageB0.Hex())
	assertRPCResult("eth_getCode", []any{contractArg, "0x3"}, "0x")
	assertRPCResult("eth_getStorageAt", []any{contractArg, slotAArg, "0x3"}, "0x"+(tcommon.Hash{}).Hex())
	assertRPCResult("eth_getCode", []any{contractArg, "0x4"}, "0x"+hex.EncodeToString(codeB))
	assertRPCResult("eth_getStorageAt", []any{contractArg, slotAArg, "0x4"}, "0x"+storageA1.Hex())
	assertRPCResult("eth_getStorageAt", []any{contractArg, slotBArg, "0x4"}, "0x"+(tcommon.Hash{}).Hex())
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
