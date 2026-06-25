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
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm/tracers"
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

func TestArchiveQuery_DynamicPropertiesAtUsesHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain

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
	commitDynamicProperties := func(blk *types.Block, n int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())
		dynProps := state.NewDynamicProperties()
		dynProps.SetNextMaintenanceTime(n * 1000)
		dynProps.SetTotalEnergyWeight(n * 10)
		dynProps.AddBurnTrx(n * 100)
		dynProps.SetBandwidthPriceHistory("0:10," + strconv.FormatInt(n, 10) + ":20")
		dynProps.SetEnergyPriceHistory("0:100," + strconv.FormatInt(n, 10) + ":200")
		if err := dynProps.FlushRooted(statedb); err != nil {
			t.Fatalf("flush dynamic properties block %d: %v", n, err)
		}
		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit dynamic properties block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitDynamicProperties(block1, 1)
	root = commitDynamicProperties(block2, 2)

	next1, err := b.NextMaintenanceTimeAt(block1.Number())
	if err != nil {
		t.Fatalf("NextMaintenanceTimeAt(block1): %v", err)
	}
	if next1 != 1000 {
		t.Fatalf("NextMaintenanceTimeAt(block1) = %d, want 1000", next1)
	}
	next2, err := b.NextMaintenanceTimeAt(block2.Number())
	if err != nil {
		t.Fatalf("NextMaintenanceTimeAt(block2): %v", err)
	}
	if next2 != 2000 {
		t.Fatalf("NextMaintenanceTimeAt(block2) = %d, want 2000", next2)
	}
	burn1, err := b.GetBurnTrxAt(block1.Number())
	if err != nil {
		t.Fatalf("GetBurnTrxAt(block1): %v", err)
	}
	if burn1 != 100 {
		t.Fatalf("GetBurnTrxAt(block1) = %d, want 100", burn1)
	}
	burn2, err := b.GetBurnTrxAt(block2.Number())
	if err != nil {
		t.Fatalf("GetBurnTrxAt(block2): %v", err)
	}
	if burn2 != 200 {
		t.Fatalf("GetBurnTrxAt(block2) = %d, want 200", burn2)
	}
	bandwidth1, err := b.GetBandwidthPricesAt(block1.Number())
	if err != nil {
		t.Fatalf("GetBandwidthPricesAt(block1): %v", err)
	}
	if bandwidth1 != "0:10,1:20" {
		t.Fatalf("GetBandwidthPricesAt(block1) = %q, want 0:10,1:20", bandwidth1)
	}
	bandwidth2, err := b.GetBandwidthPricesAt(block2.Number())
	if err != nil {
		t.Fatalf("GetBandwidthPricesAt(block2): %v", err)
	}
	if bandwidth2 != "0:10,2:20" {
		t.Fatalf("GetBandwidthPricesAt(block2) = %q, want 0:10,2:20", bandwidth2)
	}
	energy1, err := b.GetEnergyPricesAt(block1.Number())
	if err != nil {
		t.Fatalf("GetEnergyPricesAt(block1): %v", err)
	}
	if energy1 != "0:100,1:200" {
		t.Fatalf("GetEnergyPricesAt(block1) = %q, want 0:100,1:200", energy1)
	}
	energy2, err := b.GetEnergyPricesAt(block2.Number())
	if err != nil {
		t.Fatalf("GetEnergyPricesAt(block2): %v", err)
	}
	if energy2 != "0:100,2:200" {
		t.Fatalf("GetEnergyPricesAt(block2) = %q, want 0:100,2:200", energy2)
	}

	paramValue := func(blockNum uint64, key string) int64 {
		t.Helper()
		params, err := b.GetChainParametersAt(blockNum)
		if err != nil {
			t.Fatalf("GetChainParametersAt(%d): %v", blockNum, err)
		}
		for _, param := range params {
			if param.Key == key {
				return param.Value
			}
		}
		t.Fatalf("chain parameter %q missing at block %d", key, blockNum)
		return 0
	}
	if got := paramValue(block1.Number(), "next_maintenance_time"); got != 1000 {
		t.Fatalf("block1 next_maintenance_time = %d, want 1000", got)
	}
	if got := paramValue(block1.Number(), "total_energy_weight"); got != 10 {
		t.Fatalf("block1 total_energy_weight = %d, want 10", got)
	}
	if got := paramValue(block2.Number(), "next_maintenance_time"); got != 2000 {
		t.Fatalf("block2 next_maintenance_time = %d, want 2000", got)
	}
	if got := paramValue(block2.Number(), "total_energy_weight"); got != 20 {
		t.Fatalf("block2 total_energy_weight = %d, want 20", got)
	}
}

func TestArchiveQuery_BrokerageInfoAtUsesCycleHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain

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

	witnessAddr := testInsertAddr(41)
	root := bc.StateRootAtBlock(0)
	commitBrokerage := func(blk *types.Block, cycle int64, cycleRate, currentRate int) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", blk.Number(), err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, blk.Number(), blk.Hash())

		dynProps := state.NewDynamicProperties()
		dynProps.SetCurrentCycleNumber(cycle)
		if err := dynProps.FlushRooted(statedb); err != nil {
			t.Fatalf("flush current cycle block %d: %v", blk.Number(), err)
		}
		if err := statedb.WriteWitnessBrokerage(witnessAddr, int64(currentRate)); err != nil {
			t.Fatalf("write current brokerage block %d: %v", blk.Number(), err)
		}
		if err := statedb.WriteCycleBrokerage(cycle, witnessAddr.Bytes(), cycleRate); err != nil {
			t.Fatalf("write cycle brokerage block %d: %v", blk.Number(), err)
		}

		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit brokerage block %d: %v", blk.Number(), err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", blk.Number(), err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitBrokerage(block1, 7, 33, 91)
	root = commitBrokerage(block2, 8, 44, 92)

	got1, err := b.GetBrokerageInfoAt(witnessAddr, block1.Number())
	if err != nil {
		t.Fatalf("GetBrokerageInfoAt(block1): %v", err)
	}
	if got1 != 33 {
		t.Fatalf("GetBrokerageInfoAt(block1) = %d, want cycle brokerage 33", got1)
	}
	got2, err := b.GetBrokerageInfoAt(witnessAddr, block2.Number())
	if err != nil {
		t.Fatalf("GetBrokerageInfoAt(block2): %v", err)
	}
	if got2 != 44 {
		t.Fatalf("GetBrokerageInfoAt(block2) = %d, want cycle brokerage 44", got2)
	}
}

func TestArchiveQuery_GetContractAtUsesMetadataHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain

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

	contractAddr := testInsertAddr(51)
	root := bc.StateRootAtBlock(0)
	commitContract := func(blk *types.Block, name string, code []byte) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", blk.Number(), err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, blk.Number(), blk.Hash())
		statedb.SetContract(contractAddr, &contractpb.SmartContract{
			ContractAddress: contractAddr.Bytes(),
			Name:            name,
			Bytecode:        code,
		})
		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit contract metadata block %d: %v", blk.Number(), err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", blk.Number(), err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitContract(block1, "contract-one", []byte{0x01, 0x02})
	root = commitContract(block2, "contract-two", []byte{0x03, 0x04})

	got1, err := b.GetContractAt(contractAddr, block1.Number())
	if err != nil {
		t.Fatalf("GetContractAt(block1): %v", err)
	}
	if got1 == nil || got1.Name != "contract-one" || !bytes.Equal(got1.Bytecode, []byte{0x01, 0x02}) {
		t.Fatalf("GetContractAt(block1) = %+v, want contract-one", got1)
	}
	got2, err := b.GetContractAt(contractAddr, block2.Number())
	if err != nil {
		t.Fatalf("GetContractAt(block2): %v", err)
	}
	if got2 == nil || got2.Name != "contract-two" || !bytes.Equal(got2.Bytecode, []byte{0x03, 0x04}) {
		t.Fatalf("GetContractAt(block2) = %+v, want contract-two", got2)
	}
}

func TestArchiveQuery_TriggerConstantContractAtUsesBlockStateRoot(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain

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

	contractAddr := testInsertAddr(55)
	runtimeReturning := func(v byte) []byte {
		return []byte{0x60, v, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	}
	root := bc.StateRootAtBlock(0)
	commitRuntime := func(blk *types.Block, code []byte) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", blk.Number(), err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, blk.Number(), blk.Hash())
		statedb.CreateAccount(contractAddr, corepb.AccountType_Contract)
		statedb.SetCode(contractAddr, code)
		statedb.SetContract(contractAddr, &contractpb.SmartContract{
			ContractAddress: contractAddr.Bytes(),
			Name:            "runtime",
			Bytecode:        code,
		})
		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit contract runtime block %d: %v", blk.Number(), err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", blk.Number(), err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitRuntime(block1, runtimeReturning(0x11))
	root = commitRuntime(block2, runtimeReturning(0x22))

	got1, err := b.TriggerConstantContractAt(witness, contractAddr, nil, 1_000_000, block1.Number())
	if err != nil {
		t.Fatalf("TriggerConstantContractAt(block1): %v", err)
	}
	if len(got1.Result) != 32 || got1.Result[31] != 0x11 {
		t.Fatalf("TriggerConstantContractAt(block1) result = %x, want trailing 0x11", got1.Result)
	}
	got2, err := b.TriggerConstantContractAt(witness, contractAddr, nil, 1_000_000, block2.Number())
	if err != nil {
		t.Fatalf("TriggerConstantContractAt(block2): %v", err)
	}
	if len(got2.Result) != 32 || got2.Result[31] != 0x22 {
		t.Fatalf("TriggerConstantContractAt(block2) result = %x, want trailing 0x22", got2.Result)
	}
	call1, err := b.CallAt(&witness, &contractAddr, nil, 0, block1.Number())
	if err != nil {
		t.Fatalf("CallAt(block1): %v", err)
	}
	if len(call1) != 32 || call1[31] != 0x11 {
		t.Fatalf("CallAt(block1) result = %x, want trailing 0x11", call1)
	}
	call2, err := b.CallAt(&witness, &contractAddr, nil, 0, block2.Number())
	if err != nil {
		t.Fatalf("CallAt(block2): %v", err)
	}
	if len(call2) != 32 || call2[31] != 0x22 {
		t.Fatalf("CallAt(block2) result = %x, want trailing 0x22", call2)
	}
	energy, err := b.EstimateEnergyAt(witness, contractAddr, nil, block1.Number())
	if err != nil {
		t.Fatalf("EstimateEnergyAt(block1): %v", err)
	}
	if energy <= 0 {
		t.Fatalf("EstimateEnergyAt(block1) = %d, want positive energy", energy)
	}
	gas, err := b.EstimateGasAt(&witness, &contractAddr, []byte{0x01}, 0, block1.Number())
	if err != nil {
		t.Fatalf("EstimateGasAt(block1): %v", err)
	}
	if gas <= 0 {
		t.Fatalf("EstimateGasAt(block1) = %d, want positive energy", gas)
	}
}

func TestArchiveQuery_ProposalsAtUsesSystemProposalHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain

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

	proposer1 := testInsertAddr(61)
	proposer2 := testInsertAddr(62)
	approval := testInsertAddr(63)
	root := bc.StateRootAtBlock(0)
	commitProposals := func(blk *types.Block, n int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())

		switch n {
		case 1:
			if err := statedb.WriteProposal(1, &rawdb.Proposal{
				ID:             1,
				Proposer:       proposer1,
				Parameters:     map[int64]int64{3: 30},
				CreateTime:     100,
				ExpirationTime: 200,
				State:          rawdb.ProposalStatePending,
			}); err != nil {
				t.Fatalf("write proposal block %d: %v", n, err)
			}
			if err := statedb.WriteProposalIndex([]int64{1}); err != nil {
				t.Fatalf("write proposal index block %d: %v", n, err)
			}
		case 2:
			if err := statedb.WriteProposal(1, &rawdb.Proposal{
				ID:             1,
				Proposer:       proposer1,
				Parameters:     map[int64]int64{3: 31},
				CreateTime:     100,
				ExpirationTime: 250,
				Approvals:      []tcommon.Address{approval},
				State:          rawdb.ProposalStateApproved,
			}); err != nil {
				t.Fatalf("write proposal1 block %d: %v", n, err)
			}
			if err := statedb.WriteProposal(2, &rawdb.Proposal{
				ID:             2,
				Proposer:       proposer2,
				Parameters:     map[int64]int64{5: 50},
				CreateTime:     300,
				ExpirationTime: 400,
				State:          rawdb.ProposalStateCanceled,
			}); err != nil {
				t.Fatalf("write proposal2 block %d: %v", n, err)
			}
			if err := statedb.WriteProposalIndex([]int64{1, 2}); err != nil {
				t.Fatalf("write proposal index block %d: %v", n, err)
			}
		}

		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit proposal state block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitProposals(block1, 1)
	root = commitProposals(block2, 2)

	list1, err := b.ListProposalsAt(block1.Number())
	if err != nil {
		t.Fatalf("ListProposalsAt(block1): %v", err)
	}
	if len(list1) != 1 || list1[0].ProposalID != 1 || list1[0].State != "PENDING" ||
		len(list1[0].Parameters) != 1 || list1[0].Parameters[0].Key != 3 || list1[0].Parameters[0].Value != 30 {
		t.Fatalf("block1 proposals = %+v, want pending proposal 1", list1)
	}

	got1, err := b.GetProposalByIDAt(1, block1.Number())
	if err != nil {
		t.Fatalf("GetProposalByIDAt(1, block1): %v", err)
	}
	if got1.State != "PENDING" || got1.ExpirationTime != 200 || len(got1.Approvals) != 0 {
		t.Fatalf("proposal1 at block1 = %+v, want pre-approval state", got1)
	}

	list2, err := b.ListProposalsAt(block2.Number())
	if err != nil {
		t.Fatalf("ListProposalsAt(block2): %v", err)
	}
	if len(list2) != 2 {
		t.Fatalf("block2 proposal count = %d, want 2", len(list2))
	}
	if list2[0].ProposalID != 1 || list2[0].State != "APPROVED" ||
		len(list2[0].Approvals) != 1 || list2[0].Parameters[0].Value != 31 {
		t.Fatalf("proposal1 at block2 = %+v, want approved updated proposal", list2[0])
	}
	if list2[1].ProposalID != 2 || list2[1].State != "CANCELED" ||
		list2[1].ProposerAddress != hex.EncodeToString(proposer2.Bytes()) {
		t.Fatalf("proposal2 at block2 = %+v, want canceled second proposal", list2[1])
	}

	got2, err := b.GetProposalByIDAt(2, block2.Number())
	if err != nil {
		t.Fatalf("GetProposalByIDAt(2, block2): %v", err)
	}
	if got2.ProposalID != 2 || got2.State != "CANCELED" || got2.Parameters[0].Value != 50 {
		t.Fatalf("proposal2 at block2 = %+v, want canceled proposal 2", got2)
	}
}

func TestArchiveQuery_ListWitnessesAtUsesHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain

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

	w1 := testInsertAddr(31)
	w2 := testInsertAddr(32)
	voter := testInsertAddr(33)

	root := bc.StateRootAtBlock(0)
	commitWitnesses := func(blk *types.Block, n int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())

		switch n {
		case 1:
			w := types.NewWitness(w1, "witness-one")
			w.SetVoteCount(10)
			w.SetTotalProduced(1)
			w.SetLatestBlockNum(1)
			if err := statedb.SetWitnessCapsule(w); err != nil {
				t.Fatalf("set witness1 block %d: %v", n, err)
			}
			if err := statedb.WriteWitnessIndex([]tcommon.Address{w1}); err != nil {
				t.Fatalf("write witness index block %d: %v", n, err)
			}
			if err := statedb.WriteActiveWitnesses([]tcommon.Address{w1}); err != nil {
				t.Fatalf("write active witnesses block %d: %v", n, err)
			}
			if err := statedb.WriteVotes(voter, &corepb.Votes{
				Address:  voter.Bytes(),
				NewVotes: []*corepb.Vote{{VoteAddress: w1.Bytes(), VoteCount: 5}},
			}); err != nil {
				t.Fatalf("write votes block %d: %v", n, err)
			}
		case 2:
			wOne := types.NewWitness(w1, "witness-one-updated")
			wOne.SetVoteCount(20)
			wOne.SetTotalProduced(2)
			wOne.SetLatestBlockNum(2)
			if err := statedb.SetWitnessCapsule(wOne); err != nil {
				t.Fatalf("set witness1 block %d: %v", n, err)
			}
			wTwo := types.NewWitness(w2, "witness-two")
			wTwo.SetVoteCount(30)
			wTwo.SetTotalMissed(3)
			wTwo.SetLatestSlotNum(4)
			if err := statedb.SetWitnessCapsule(wTwo); err != nil {
				t.Fatalf("set witness2 block %d: %v", n, err)
			}
			if err := statedb.WriteWitnessIndex([]tcommon.Address{w1, w2}); err != nil {
				t.Fatalf("write witness index block %d: %v", n, err)
			}
			if err := statedb.WriteActiveWitnesses([]tcommon.Address{w2}); err != nil {
				t.Fatalf("write active witnesses block %d: %v", n, err)
			}
			if err := statedb.WriteVotes(voter, &corepb.Votes{
				Address:  voter.Bytes(),
				OldVotes: []*corepb.Vote{{VoteAddress: w1.Bytes(), VoteCount: 5}},
				NewVotes: []*corepb.Vote{{VoteAddress: w2.Bytes(), VoteCount: 7}},
			}); err != nil {
				t.Fatalf("write votes block %d: %v", n, err)
			}
		}

		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit witness state block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitWitnesses(block1, 1)
	root = commitWitnesses(block2, 2)

	list1, err := b.ListWitnessesAt(block1.Number())
	if err != nil {
		t.Fatalf("ListWitnessesAt(block1): %v", err)
	}
	if len(list1) != 1 {
		t.Fatalf("block1 witness count = %d, want 1", len(list1))
	}
	if list1[0].Address != hex.EncodeToString(w1.Bytes()) ||
		list1[0].URL != "witness-one" ||
		list1[0].VoteCount != 15 ||
		!list1[0].IsJobs ||
		list1[0].TotalProduced != 1 ||
		list1[0].LatestBlockNum != 1 {
		t.Fatalf("block1 witness = %+v, want witness1 with pending +5", list1[0])
	}

	list2, err := b.ListWitnessesAt(block2.Number())
	if err != nil {
		t.Fatalf("ListWitnessesAt(block2): %v", err)
	}
	if len(list2) != 2 {
		t.Fatalf("block2 witness count = %d, want 2", len(list2))
	}
	if list2[0].Address != hex.EncodeToString(w1.Bytes()) ||
		list2[0].URL != "witness-one-updated" ||
		list2[0].VoteCount != 15 ||
		list2[0].IsJobs {
		t.Fatalf("block2 witness1 = %+v, want updated inactive witness1 with pending -5", list2[0])
	}
	if list2[1].Address != hex.EncodeToString(w2.Bytes()) ||
		list2[1].URL != "witness-two" ||
		list2[1].VoteCount != 37 ||
		!list2[1].IsJobs ||
		list2[1].TotalMissed != 3 ||
		list2[1].LatestSlotNum != 4 {
		t.Fatalf("block2 witness2 = %+v, want active witness2 with pending +7", list2[1])
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

func TestArchiveQuery_DelegationIndexRejectsMalformedHistory(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	from := testInsertAddr(71)
	key := rawdb.DelegationIndexStateKey(from)
	if err := rawdb.WriteStateKVLatest(db, tcommon.SystemAccountAddress, 0, kvdomains.SystemDelegation, key, []byte("short")); err != nil {
		t.Fatalf("WriteStateKVLatest malformed delegation index: %v", err)
	}
	reader := state.NewPersistentHistoryReader(db, nil, 1)

	_, err := readDelegationIndexAt(reader, from, 1)
	if err == nil || !strings.Contains(err.Error(), "malformed length") {
		t.Fatalf("readDelegationIndexAt malformed error = %v, want malformed length", err)
	}
}

func TestArchiveQuery_StakeResourceAtUsesHistory(t *testing.T) {
	b, witness, _ := archiveBackend(t)
	bc := b.chain
	owner := testInsertAddr(33)
	receiver := testInsertAddr(34)

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
	commitStake := func(blk *types.Block, n int64) tcommon.Hash {
		bc.buffer.BeginBlock(blk.Hash(), blk.Number())
		statedb, err := bc.openState(root)
		if err != nil {
			t.Fatalf("open state block %d: %v", n, err)
		}
		statedb.SetDomainChangeSetWriter(bc.buffer, uint64(n), blk.Hash())
		if statedb.GetAccount(owner) == nil {
			statedb.CreateAccount(owner, corepb.AccountType_Normal)
		}
		switch n {
		case 1:
			statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 1000)
			statedb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100, 1000)
			statedb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 200, 5000)
			if err := statedb.WriteDelegatedResourceV2(owner, receiver, false, &rawdb.DelegatedResource{
				From:                      owner,
				To:                        receiver,
				FrozenBalanceForBandwidth: 200,
			}); err != nil {
				t.Fatalf("write delegation block %d: %v", n, err)
			}
		case 2:
			statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 2000)
			statedb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 300, 2500)
			statedb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 400, 7000)
			if err := statedb.WriteDelegatedResourceV2(owner, receiver, false, &rawdb.DelegatedResource{
				From:                      owner,
				To:                        receiver,
				FrozenBalanceForBandwidth: 700,
			}); err != nil {
				t.Fatalf("write delegation block %d: %v", n, err)
			}
		}
		if err := statedb.WriteDelegationIndex(owner, []tcommon.Address{receiver}); err != nil {
			t.Fatalf("write delegation index block %d: %v", n, err)
		}
		root, err = statedb.Commit()
		if err != nil {
			t.Fatalf("commit stake block %d: %v", n, err)
		}
		if err := rawdb.WriteBlockStateRoot(bc.buffer, blk.Hash(), root); err != nil {
			t.Fatalf("write block state root %d: %v", n, err)
		}
		bc.buffer.CommitBlock()
		return root
	}

	root = commitStake(block1, 1)
	root = commitStake(block2, 2)

	can1, err := b.CanDelegateResourceAt(owner, 123, corepb.ResourceCode_BANDWIDTH, block1.Number())
	if err != nil {
		t.Fatalf("CanDelegateResourceAt(block1): %v", err)
	}
	if can1.MaxSize != 1000 || can1.CanDelegateSize != 800 || can1.Balance != 123 {
		t.Fatalf("block1 can delegate = %+v, want max=1000 can=800 balance=123", can1)
	}
	can2, err := b.CanDelegateResourceAt(owner, 456, corepb.ResourceCode_BANDWIDTH, block2.Number())
	if err != nil {
		t.Fatalf("CanDelegateResourceAt(block2): %v", err)
	}
	if can2.MaxSize != 3000 || can2.CanDelegateSize != 2300 || can2.Balance != 456 {
		t.Fatalf("block2 can delegate = %+v, want max=3000 can=2300 balance=456", can2)
	}

	available1, err := b.GetAvailableUnfreezeCountAt(owner, block1.Number())
	if err != nil {
		t.Fatalf("GetAvailableUnfreezeCountAt(block1): %v", err)
	}
	if available1.Count != 30 {
		t.Fatalf("block1 available unfreeze count = %d, want 30", available1.Count)
	}
	available2, err := b.GetAvailableUnfreezeCountAt(owner, block2.Number())
	if err != nil {
		t.Fatalf("GetAvailableUnfreezeCountAt(block2): %v", err)
	}
	if available2.Count != 28 {
		t.Fatalf("block2 available unfreeze count = %d, want 28", available2.Count)
	}

	withdrawable1, err := b.GetCanWithdrawUnfreezeAmountAt(owner, 3000, block1.Number())
	if err != nil {
		t.Fatalf("GetCanWithdrawUnfreezeAmountAt(block1): %v", err)
	}
	if withdrawable1.Amount != 100 {
		t.Fatalf("block1 withdrawable = %d, want 100", withdrawable1.Amount)
	}
	withdrawable2, err := b.GetCanWithdrawUnfreezeAmountAt(owner, 3000, block2.Number())
	if err != nil {
		t.Fatalf("GetCanWithdrawUnfreezeAmountAt(block2): %v", err)
	}
	if withdrawable2.Amount != 400 {
		t.Fatalf("block2 withdrawable = %d, want 400", withdrawable2.Amount)
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

func TestArchiveExecutionRootSurfacesColdStateRootErrors(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	block := blockchainStartupBlock(2)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	chain := rawdb.NewChainDB(db, blockchainStartupFailingAncient{
		kind:   rawdb.AncientStateRootsTable,
		number: block.Number(),
		err:    errors.New("cold state root read failed"),
	})
	bc := &BlockChain{db: db, chaindb: chain}
	bc.currentBlock.Store(block)
	b := &TronBackend{chain: bc}

	_, err := b.archiveExecutionRoot(block.Number(), nil)
	if err == nil || !strings.Contains(err.Error(), "cold state root read failed") {
		t.Fatalf("archiveExecutionRoot err = %v, want cold state-root error", err)
	}
	if !strings.Contains(err.Error(), "state root for block 2") {
		t.Fatalf("archiveExecutionRoot err = %v, want state-root context", err)
	}
}

func TestHistoryReaderAtSurfacesColdStateRootErrorsAndUnlocks(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	block := blockchainStartupBlock(2)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	chain := rawdb.NewChainDB(db, blockchainStartupFailingAncient{
		kind:   rawdb.AncientStateRootsTable,
		number: block.Number(),
		err:    errors.New("cold state root read failed"),
	})
	bc := &BlockChain{db: db, chaindb: chain}
	bc.currentBlock.Store(block)
	b := &TronBackend{chain: bc}

	_, _, _, err := b.historyReaderAt()
	if err == nil || !strings.Contains(err.Error(), "cold state root read failed") {
		t.Fatalf("historyReaderAt err = %v, want cold state-root error", err)
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
		t.Fatal("historyReaderAt error path leaked chainmu")
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
	range4, ok, err := rawdb.ReadStateTxRange(bc.buffer, 4)
	if err != nil || !ok {
		t.Fatalf("read block 4 tx range: ok=%v err=%v", ok, err)
	}

	contract := testInsertAddr(42)
	slot := tcommon.Hash{0xAA}
	runtimeReturning := func(v byte) []byte {
		return []byte{0x60, v, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	}
	code2 := runtimeReturning(0x02)
	code3 := runtimeReturning(0x03)
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
		s.CreateAccount(contract, corepb.AccountType_Contract)
		s.SetCode(contract, code2)
		s.SetContract(contract, &contractpb.SmartContract{
			ContractAddress: contract.Bytes(),
			Name:            "cold-runtime",
			Bytecode:        code2,
		})
		s.SetState(contract, slot, storage2)
	})
	applyDomainBlock(3, blocks[3].Hash(), range3, func(s *state.StateDB) {
		s.CreateAccount(contract, corepb.AccountType_Contract)
		s.SetCode(contract, code3)
		s.SetContract(contract, &contractpb.SmartContract{
			ContractAddress: contract.Bytes(),
			Name:            "cold-runtime",
			Bytecode:        code3,
		})
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
	accountRef, accountAccessorRef, accountBTreeRef, err := statesnapshots.BuildAccountLatestSegmentFilesFromDB(bc.buffer, dir, range2.BeginTxNum, range4.EndTxNum, "latest/accounts-code-storage-2-4.seg")
	if err != nil {
		t.Fatalf("build cold account latest segment: %v", err)
	}
	kvGenerationRef, kvGenerationAccessorRef, kvGenerationBTreeRef, err := statesnapshots.BuildKVGenerationSegmentFilesFromDB(bc.buffer, dir, range2.BeginTxNum, range4.EndTxNum, "latest/kv-generation-code-storage-2-4.seg")
	if err != nil {
		t.Fatalf("build cold kv generation segment: %v", err)
	}
	metadataRef, metadataAccessorRef, metadataBTreeRef, err := statesnapshots.BuildLatestDomainSegmentFilesFromDB(bc.buffer, dir, kvdomains.ContractMetadata, range2.BeginTxNum, range4.EndTxNum, "latest/contract-metadata-2-4.seg")
	if err != nil {
		t.Fatalf("build cold contract metadata segment: %v", err)
	}
	storageRef, storageAccessorRef, storageBTreeRef, err := statesnapshots.BuildLatestDomainSegmentFilesFromDB(bc.buffer, dir, kvdomains.ContractStorage, range2.BeginTxNum, range4.EndTxNum, "latest/contract-storage-2-4.seg")
	if err != nil {
		t.Fatalf("build cold contract storage segment: %v", err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := statesnapshots.BuildCodeSegmentFilesFromDB(bc.buffer, dir, range2.BeginTxNum, range4.EndTxNum, "latest/code-2-4.seg")
	if err != nil {
		t.Fatalf("build cold code segment: %v", err)
	}
	refs := append(historyRefs,
		accountRef, accountAccessorRef, accountBTreeRef,
		kvGenerationRef, kvGenerationAccessorRef, kvGenerationBTreeRef,
		metadataRef, metadataAccessorRef, metadataBTreeRef,
		storageRef, storageAccessorRef, storageBTreeRef,
		codeRef, codeAccessorRef, codeBTreeRef,
	)
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(range2.BeginTxNum, range4.EndTxNum, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	b.SetStateColdHistory(mgr)

	assertArchiveExecution := func(label string, blockNum uint64, want byte) {
		t.Helper()
		triggered, err := b.TriggerConstantContractAt(witness, contract, nil, 1_000_000, blockNum)
		if err != nil {
			t.Fatalf("%s TriggerConstantContractAt(contract, %d): %v", label, blockNum, err)
		}
		if len(triggered.Result) != 32 || triggered.Result[31] != want {
			t.Fatalf("%s TriggerConstantContractAt(contract, %d) = %x, want trailing 0x%02x", label, blockNum, triggered.Result, want)
		}
		call, err := b.CallAt(&witness, &contract, nil, 0, blockNum)
		if err != nil {
			t.Fatalf("%s CallAt(contract, %d): %v", label, blockNum, err)
		}
		if len(call) != 32 || call[31] != want {
			t.Fatalf("%s CallAt(contract, %d) = %x, want trailing 0x%02x", label, blockNum, call, want)
		}
		gas, err := b.EstimateGasAt(&witness, &contract, []byte{0x01}, 0, blockNum)
		if err != nil {
			t.Fatalf("%s EstimateGasAt(contract, %d): %v", label, blockNum, err)
		}
		if gas == 0 {
			t.Fatalf("%s EstimateGasAt(contract, %d) = 0, want positive energy", label, blockNum)
		}
		trace, err := b.TraceCall(&witness, &contract, nil, 0, &blockNum, &tracers.TraceConfig{})
		if err != nil {
			t.Fatalf("%s TraceCall(contract, %d): %v", label, blockNum, err)
		}
		exec, ok := trace.(*tracers.ExecutionResult)
		if !ok {
			t.Fatalf("%s TraceCall(contract, %d) result type = %T, want *ExecutionResult", label, blockNum, trace)
		}
		if exec.Failed || !strings.HasSuffix(exec.ReturnValue, hex.EncodeToString([]byte{want})) {
			t.Fatalf("%s TraceCall(contract, %d) failed=%v return=%s, want trailing 0x%02x",
				label, blockNum, exec.Failed, exec.ReturnValue, want)
		}
	}

	bc.buffer.BeginBlock(tcommon.Hash{0xCE}, 0) // sentinel; archive prune layer
	for n := uint64(2); n <= 3; n++ {
		if err := rawdb.DeleteStateDomainChanges(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateDomainChanges(%d): %v", n, err)
		}
		if err := rawdb.DeleteStateTxRange(bc.buffer, n); err != nil {
			t.Fatalf("DeleteStateTxRange(%d): %v", n, err)
		}
	}
	generation, generationExists, err := rawdb.ReadStateKVGeneration(bc.buffer, contract)
	if err != nil {
		t.Fatalf("ReadStateKVGeneration(contract): %v", err)
	}
	if err := rawdb.DeleteStateAccountLatest(bc.buffer, contract); err != nil {
		t.Fatalf("delete hot account latest: %v", err)
	}
	if generationExists {
		if err := rawdb.DeleteStateKVGeneration(bc.buffer, contract); err != nil {
			t.Fatalf("delete hot kv generation: %v", err)
		}
	}
	if err := rawdb.DeleteStateKVLatest(bc.buffer, contract, generation, kvdomains.ContractMetadata, []byte("meta")); err != nil {
		t.Fatalf("delete hot contract metadata: %v", err)
	}
	if err := rawdb.DeleteStateKVLatestPrefix(bc.buffer, contract, generation, kvdomains.ContractStorage, nil); err != nil {
		t.Fatalf("delete hot contract storage latest: %v", err)
	}
	if err := rawdb.DeleteStateCode(bc.buffer, codeHash2); err != nil {
		t.Fatalf("delete hot code2: %v", err)
	}
	if err := rawdb.DeleteStateCode(bc.buffer, codeHash3); err != nil {
		t.Fatalf("delete hot code3: %v", err)
	}
	bc.buffer.CommitBlock()
	for n := uint64(2); n <= 3; n++ {
		rawdb.DeleteBlockStateRoot(bc.db, blocks[n].Hash())
		if root := bc.StateRootAtBlock(n); root != (tcommon.Hash{}) {
			t.Fatalf("state root for block %d still present after delete: %x", n, root)
		}
	}

	assertArchiveCodeStorage("cold", 2, code2, storage2)
	assertArchiveCodeStorage("cold", 3, code3, storage3)
	assertArchiveExecution("cold", 2, 0x02)
	assertArchiveExecution("cold", 3, 0x03)
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
		if _, err := b.CallAt(&witness, &recipient, nil, 0, n); !errors.Is(err, ErrArchiveHistoryDisabled) {
			t.Errorf("CallAt(recipient, %d) err = %v, want ErrArchiveHistoryDisabled", n, err)
		}
		if _, err := b.EstimateGasAt(&witness, &recipient, nil, 0, n); !errors.Is(err, ErrArchiveHistoryDisabled) {
			t.Errorf("EstimateGasAt(recipient, empty data, %d) err = %v, want ErrArchiveHistoryDisabled", n, err)
		}
	}

	// Query AT head succeeds even with history disabled (served from live).
	if _, err := b.GetBalanceAt(recipient, head); err != nil {
		t.Errorf("GetBalanceAt(recipient, head) with history disabled: %v", err)
	}
	if gas, err := b.EstimateGasAt(&witness, &recipient, nil, 0, head); err != nil || gas != 0 {
		t.Errorf("EstimateGasAt(recipient, empty data, head) = %d, err %v, want 0/nil", gas, err)
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
	if _, err := b.CallAt(&witness, &recipient, nil, 0, future); err == nil {
		t.Fatal("CallAt future block returned nil error")
	}
	if _, err := b.EstimateGasAt(&witness, &recipient, []byte{0x01}, 0, future); err == nil {
		t.Fatal("EstimateGasAt future block returned nil error")
	}
	if _, err := b.EstimateGasAt(&witness, &recipient, nil, 0, future); err == nil {
		t.Fatal("EstimateGasAt empty-data future block returned nil error")
	}
}
