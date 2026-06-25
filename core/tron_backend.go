package core

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/core/zksnark"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	"github.com/tronprotocol/go-tron/params"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"github.com/tronprotocol/go-tron/vm/tracers"
	"google.golang.org/protobuf/proto"
)

// TxBroadcaster announces new transactions to P2P peers.
// Implemented by net.BroadcastService; defined here to avoid an import cycle.
type TxBroadcaster interface {
	BroadcastTx(tx *types.Transaction)
}

// TronBackend implements tronapi.Backend.
type TronBackend struct {
	chain            *BlockChain
	pool             *txpool.TxPool
	txBroadcast      TxBroadcaster              // nil until wired from main
	peersFunc        func() []*tronapi.PeerInfo // nil until wired from main
	stateColdHistory state.StateDomainChangeColdHistory

	subsMu    sync.Mutex
	blockSubs []chan<- *types.Block
}

func NewTronBackend(chain *BlockChain, pool *txpool.TxPool) *TronBackend {
	b := &TronBackend{chain: chain, pool: pool}
	chain.AddBlockHook(b.notifyBlockSubs)
	return b
}

func (b *TronBackend) notifyBlockSubs(block *types.Block) {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	for _, ch := range b.blockSubs {
		select {
		case ch <- block:
		default: // drop if subscriber is slow
		}
	}
}

func (b *TronBackend) SubscribeBlocks(ch chan<- *types.Block) {
	b.subsMu.Lock()
	b.blockSubs = append(b.blockSubs, ch)
	b.subsMu.Unlock()
}

func (b *TronBackend) UnsubscribeBlocks(ch chan<- *types.Block) {
	b.subsMu.Lock()
	for i, s := range b.blockSubs {
		if s == ch {
			b.blockSubs = append(b.blockSubs[:i], b.blockSubs[i+1:]...)
			break
		}
	}
	b.subsMu.Unlock()
}

// SetTxBroadcaster wires in the P2P broadcaster so BroadcastTransaction
// announces the tx to peers after adding it to the local pool.
func (b *TronBackend) SetTxBroadcaster(bc TxBroadcaster) {
	b.txBroadcast = bc
}

// SetPeerLister wires in a function that returns connected P2P peers.
// Called from main.go to avoid a core→net import cycle.
func (b *TronBackend) SetPeerLister(fn func() []*tronapi.PeerInfo) {
	b.peersFunc = fn
}

// SetStateColdHistory wires snapshot-backed flat history into archive reads.
// Hot rows remain the primary source; this reader supplies pruned
// StateDomainChange rows and, when present, cold latest-domain segments after
// hot latest rows have been moved out of Pebble.
func (b *TronBackend) SetStateColdHistory(source state.StateDomainChangeColdHistory) {
	b.stateColdHistory = source
}

func (b *TronBackend) CurrentBlock() *types.Block {
	return b.chain.CurrentBlock()
}

func (b *TronBackend) SolidifiedBlockNum() uint64 {
	// Read through the buffer overlay so the answer reflects the latest
	// applied block, not just whatever the async flush worker has drained
	// to disk. Without this, a single-SR chain (solidified == head) would
	// return the previous block's solidified number after a successful
	// InsertBlock until the worker catches up.
	dp := b.chain.DynProps()
	n := dp.LatestSolidifiedBlockNum()
	if n < 0 {
		return 0
	}
	return uint64(n)
}

func (b *TronBackend) LatestPbftBlockNum() int64 {
	return rawdb.ReadLatestPbftBlockNum(b.chain.DB())
}

func (b *TronBackend) GetBlockByNumber(number uint64) (*types.Block, error) {
	if current := b.chain.CurrentBlock(); current != nil && number > current.Number() {
		return nil, fmt.Errorf("block %d not found", number)
	}
	block, ok, err := rawdb.ReadBlockStrict(b.chain.chaindb, number)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("block %d not found", number)
	}
	return block, nil
}

func (b *TronBackend) headStateRootStrict() (tcommon.Hash, error) {
	if b == nil || b.chain == nil {
		return tcommon.Hash{}, errors.New("head state root: nil backend")
	}
	current := b.chain.CurrentBlock()
	root, ok, err := b.chain.stateRootForKnownBlockStrict(current)
	if err != nil {
		return tcommon.Hash{}, err
	}
	if !ok {
		return tcommon.Hash{}, fmt.Errorf("state root for current block %d not available", current.Number())
	}
	return root, nil
}

func (b *TronBackend) GetAccount(addr tcommon.Address) (*types.Account, error) {
	statedb, err := b.chain.openCurrentState()
	if err != nil {
		return nil, fmt.Errorf("open head state: %w", err)
	}
	acc := statedb.GetAccount(addr)
	if acc == nil {
		return nil, fmt.Errorf("account not found")
	}
	return acc, nil
}

// GetAccountAt returns the account as of the post-apply state of blockNum.
// Flat latest roots are commitments, not historical MPT snapshots, so all
// non-head reads go through flat temporal domain history. The live head is
// served by the same history reader, which delegates to the current flat
// StateDB.
func (b *TronBackend) GetAccountAt(addr tcommon.Address, blockNum uint64) (*types.Account, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	acc, err := session.reader.AccountAt(addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct account at block %d: %w", blockNum, err)
	}
	if acc == nil {
		return nil, fmt.Errorf("account not found at block %d", blockNum)
	}
	return acc, nil
}

func (b *TronBackend) BroadcastTransaction(tx *types.Transaction) error {
	// Validate signature/permission against the head state before pool
	// admission so a malformed user-submitted tx never reaches gossip.
	// Mirrors java-tron Wallet.broadcastTransaction → pushTransaction's
	// validateSignature gate.
	if err := b.chain.ValidateTransaction(tx); err != nil {
		return err
	}
	if err := b.pool.Add(tx); err != nil {
		return err
	}
	if b.txBroadcast != nil {
		b.txBroadcast.BroadcastTx(tx)
	}
	return nil
}

func (b *TronBackend) GetNodeInfo() *tronapi.NodeInfo {
	current := b.chain.CurrentBlock()
	return &tronapi.NodeInfo{
		Version:      "0.2.0-dev",
		CurrentBlock: current.Number(),
	}
}

func (b *TronBackend) PendingTransactionCount() int {
	return b.pool.Count()
}

func (b *TronBackend) GetContract(addr tcommon.Address) (*contractpb.SmartContract, error) {
	statedb, err := b.chain.openCurrentState()
	if err != nil {
		return nil, fmt.Errorf("open head state: %w", err)
	}
	sc := statedb.GetContract(addr)
	if sc == nil {
		return nil, fmt.Errorf("contract not found")
	}
	return sc, nil
}

func (b *TronBackend) GetContractAt(addr tcommon.Address, blockNum uint64) (*contractpb.SmartContract, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	sc, err := session.reader.ContractAt(addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("read contract metadata at block %d: %w", blockNum, err)
	}
	return sc, nil
}

func (b *TronBackend) TriggerConstantContract(owner, contractAddr tcommon.Address, data []byte, energyLimit int64) (*tronapi.TriggerResult, error) {
	current := b.chain.CurrentBlock()
	root, err := b.headStateRootStrict()
	if err != nil {
		return nil, fmt.Errorf("read head state root: %w", err)
	}
	historyBlock := uint64(0)
	if current != nil {
		historyBlock = current.Number()
	}
	return b.triggerConstantContractAtRoot(owner, contractAddr, data, energyLimit, root, current, nil, historyBlock)
}

func (b *TronBackend) TriggerConstantContractAt(owner, contractAddr tcommon.Address, data []byte, energyLimit int64, blockNum uint64) (*tronapi.TriggerResult, error) {
	block, err := b.GetBlockByNumber(blockNum)
	if err != nil || block == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	root, err := b.archiveExecutionRoot(blockNum, session)
	if err != nil {
		return nil, err
	}
	return b.triggerConstantContractAtRoot(owner, contractAddr, data, energyLimit, root, block, session.reader, blockNum)
}

func (b *TronBackend) archiveExecutionRoot(blockNum uint64, session *archiveStateSession) (tcommon.Hash, error) {
	if root, ok, err := b.chain.stateRootAtBlockStrict(blockNum); err != nil {
		return tcommon.Hash{}, err
	} else if ok {
		return root, nil
	}
	if session != nil && blockNum < session.headNum {
		if root, ok, err := b.chain.stateRootAtBlockStrict(session.headNum); err != nil {
			return tcommon.Hash{}, err
		} else if ok {
			return root, nil
		}
		return tcommon.Hash{}, fmt.Errorf("state root for head block %d not available", session.headNum)
	}
	return tcommon.Hash{}, fmt.Errorf("state root for block %d not available", blockNum)
}

func (b *TronBackend) triggerConstantContractAtRoot(owner, contractAddr tcommon.Address, data []byte, energyLimit int64, root tcommon.Hash, block *types.Block, history *state.PersistentHistoryReader, historyBlock uint64) (*tronapi.TriggerResult, error) {
	if block == nil {
		return nil, fmt.Errorf("block context not available")
	}
	statedbCopy, err := b.archiveExecutionState(root, block.Number(), history, historyBlock)
	if err != nil {
		return nil, err
	}

	if energyLimit <= 0 {
		energyLimit = 30_000_000 // default max energy for constant calls
	}

	dp := state.LoadDynamicProperties(b.chain.buffer, statedbCopy)
	dp.SetLatestBlockHeaderNumber(int64(block.Number()))
	dp.SetLatestBlockHeaderTimestamp(block.Timestamp())
	dp.SetLatestBlockHeaderHash(block.Hash())
	cfg := vm.NewTVMConfig(block.Number(), dp)
	cfg.MultiSigCheckV2 = forks.PassVersionFromStore(statedbCopy, 27,
		dp.LatestBlockHeaderTimestamp(), dp.MaintenanceTimeInterval())
	cfg.CpuTimeGuard = forks.PassVersionFromStore(statedbCopy, 35,
		dp.LatestBlockHeaderTimestamp(), dp.MaintenanceTimeInterval())
	// Opt-in opcode trace for sync-stall diagnosis (GTRON_TVM_TRACE=<file>):
	// install a FileLogger tracer when the env var is set; nil/zero overhead
	// otherwise. Superseded by debug_traceCall for richer JSON traces.
	fileTracer := tracers.FileLoggerFromEnv()
	if fileTracer != nil {
		cfg.Tracer = fileTracer
	}
	evm := vm.NewTVM(statedbCopy, dp, owner, block.Number(), block.Timestamp(), tcommon.Address{}, 1, cfg)
	// BLOCKHASH/CHAINID need chain data; route through the ancient-aware
	// lookup so constant calls see the same hashes as block execution.
	evm.SetDB(b.chain.vmKV(b.chain.buffer))

	ret, energyLeft, vmErr := evm.Call(owner, contractAddr, data, uint64(energyLimit), 0)
	energyUsed := energyLimit - int64(energyLeft)

	if fileTracer != nil {
		_ = fileTracer.Flush(fmt.Sprintf("TriggerConstantContract %x energyUsed=%d err=%v", contractAddr[1:5], energyUsed, vmErr))
	}

	if vmErr != nil {
		return &tronapi.TriggerResult{
			Result:     ret,
			EnergyUsed: energyUsed,
		}, vmErr
	}

	return &tronapi.TriggerResult{
		Result:     ret,
		EnergyUsed: energyUsed,
	}, nil
}

func (b *TronBackend) archiveExecutionState(root tcommon.Hash, blockNum uint64, history *state.PersistentHistoryReader, historyBlock uint64) (*state.StateDB, error) {
	if root == (tcommon.Hash{}) {
		return nil, fmt.Errorf("state root for block %d not available", blockNum)
	}
	statedb, err := b.chain.openState(root)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}

	statedbCopy, err := statedb.Copy()
	if err != nil {
		return nil, fmt.Errorf("copy state: %w", err)
	}
	if history == nil {
		return statedbCopy, nil
	}

	statedbCopy.SetHistoricalLatestView(history, historyBlock)
	if codeColdHistory, ok := b.stateColdHistory.(state.StateCodeColdHistoryAtOrBefore); ok {
		txNum, err := b.archiveStateTxNumAtBlockEnd(historyBlock)
		if err != nil {
			return nil, fmt.Errorf("resolve archive code txnum for block %d: %w", historyBlock, err)
		}
		statedbCopy.SetCodeColdHistory(codeColdHistory, txNum)
	}
	return statedbCopy, nil
}

// traceEnergyCap is the generous energy ceiling for debug_trace* simulations.
// geth uses a high gas cap for traces so the call runs to completion regardless
// of the caller's fee limit; the struct logger's `limit` bounds the output.
const traceEnergyCap = 500_000_000

// TraceCall replays a read-only contract call with the configured tracer
// installed, returning the tracer's rendered result. It mirrors
// TriggerConstantContract's state setup: blockNumber==nil traces against a copy
// of head state, otherwise against archive state as of that block (requires
// --history.enabled). A revert is reported through the tracer result (failed/
// error), not as an RPC error, matching geth.
func (b *TronBackend) TraceCall(from, to *tcommon.Address, data []byte, value int64, blockNumber *uint64, cfg *tracers.TraceConfig) (interface{}, error) {
	if to == nil {
		return nil, fmt.Errorf("debug_traceCall: 'to' address is required")
	}
	owner := tcommon.Address{}
	if from != nil {
		owner = *from
	}

	tracer, err := tracers.New(cfg)
	if err != nil {
		return nil, err
	}

	statedbCopy, dp, block, release, err := b.traceStateContext(blockNumber)
	if err != nil {
		return nil, err
	}
	defer release()

	tvmCfg := vm.NewTVMConfig(block.Number(), dp)
	tvmCfg.MultiSigCheckV2 = forks.PassVersionFromStore(statedbCopy, 27,
		dp.LatestBlockHeaderTimestamp(), dp.MaintenanceTimeInterval())
	tvmCfg.CpuTimeGuard = forks.PassVersionFromStore(statedbCopy, 35,
		dp.LatestBlockHeaderTimestamp(), dp.MaintenanceTimeInterval())
	tvmCfg.Tracer = tracer

	evm := vm.NewTVM(statedbCopy, dp, owner, block.Number(), block.Timestamp(), tcommon.Address{}, 1, tvmCfg)
	evm.SetDB(b.chain.vmKV(b.chain.buffer))

	// The tracer captures the full execution; a revert/VM error is surfaced
	// through the tracer result (StructLogger.failed / callFrame.error), so the
	// vmErr is intentionally not propagated as an RPC error.
	_, _, _ = evm.Call(owner, *to, data, traceEnergyCap, value)
	return tracer.GetResult()
}

// traceStateContext resolves the state and block context for a trace: a nil
// block number yields a copy of live head state with the cached head dynprops;
// a concrete number yields a copy of archive state as of that block with the
// dynprops rooted at that block (so fork gates match the historical block).
func (b *TronBackend) traceStateContext(blockNumber *uint64) (*state.StateDB, *state.DynamicProperties, *types.Block, func(), error) {
	release := func() {}
	if blockNumber == nil {
		current := b.chain.CurrentBlock()
		if current == nil {
			return nil, nil, nil, release, fmt.Errorf("current block not available")
		}
		statedbCopy, err := b.archiveExecutionState(b.chain.HeadStateRoot(), current.Number(), nil, current.Number())
		if err != nil {
			return nil, nil, nil, release, err
		}
		dp := state.LoadDynamicProperties(b.chain.buffer, statedbCopy)
		dp.SetLatestBlockHeaderNumber(int64(current.Number()))
		dp.SetLatestBlockHeaderTimestamp(current.Timestamp())
		dp.SetLatestBlockHeaderHash(current.Hash())
		return statedbCopy, dp, current, release, nil
	}

	num := *blockNumber
	block, err := b.GetBlockByNumber(num)
	if err != nil {
		return nil, nil, nil, release, err
	}
	if block == nil {
		return nil, nil, nil, release, fmt.Errorf("block %d not found", num)
	}
	session, err := b.archiveStateAt(num)
	if err != nil {
		return nil, nil, nil, release, err
	}
	release = session.Close
	root, err := b.archiveExecutionRoot(num, session)
	if err != nil {
		release()
		return nil, nil, nil, func() {}, err
	}
	statedbCopy, err := b.archiveExecutionState(root, num, session.reader, num)
	if err != nil {
		release()
		return nil, nil, nil, func() {}, err
	}
	dp := state.LoadDynamicProperties(b.chain.buffer, statedbCopy)
	dp.SetLatestBlockHeaderNumber(int64(block.Number()))
	dp.SetLatestBlockHeaderTimestamp(block.Timestamp())
	dp.SetLatestBlockHeaderHash(block.Hash())
	return statedbCopy, dp, block, release, nil
}

// TraceTransaction re-executes a historical transaction with the configured
// tracer and returns the tracer's result. It reproduces the tx's pre-state by
// replaying its block from the PARENT post-state (archive — requires
// --history.enabled), installing the tracer only on the target tx. A revert or
// VM-result divergence is reflected in the tracer result, not surfaced as an RPC
// error, so the trace is available even when the replay's contract result
// disagrees with the recorded block (the exact case debug_traceTransaction is
// meant to diagnose).
func (b *TronBackend) TraceTransaction(hash tcommon.Hash, cfg *tracers.TraceConfig) (interface{}, error) {
	blockNum, ok, err := rawdb.ReadTransactionIndexStrict(b.chain.chaindb, hash[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("transaction %x not found", hash)
	}
	if blockNum == 0 {
		return nil, fmt.Errorf("cannot trace a genesis-block transaction")
	}
	block, hasBlock, err := rawdb.ReadBlockStrict(b.chain.chaindb, blockNum)
	if err != nil {
		return nil, err
	}
	if !hasBlock {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}
	txIndex := -1
	for i, tx := range block.Transactions() {
		if tx.Hash() == hash {
			txIndex = i
			break
		}
	}
	if txIndex < 0 {
		return nil, fmt.Errorf("transaction %x not found in block %d", hash, blockNum)
	}

	// Reproduce the tx's pre-state: open the parent block's post-state and replay
	// this block's transactions up to and including the target.
	parentNum := blockNum - 1
	session, err := b.archiveStateAt(parentNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	parentRoot, err := b.archiveExecutionRoot(parentNum, session)
	if err != nil {
		return nil, err
	}
	statedbCopy, err := b.archiveExecutionState(parentRoot, parentNum, session.reader, parentNum)
	if err != nil {
		return nil, err
	}
	dp := state.LoadDynamicProperties(b.chain.buffer, statedbCopy)

	tracer, err := tracers.New(cfg)
	if err != nil {
		return nil, err
	}

	_, perr := ProcessBlockTraced(statedbCopy, dp, block, b.chain.vmKV(b.chain.buffer),
		b.chain.ActiveWitnesses(), b.chain.GenesisTimestamp(), b.chain.Config().EnergyLimitForkBlockNum(),
		false, b.chain.versionPassCache, txIndex, tracer, b.chain.effectiveGenesisHash())

	res, gerr := tracer.GetResult()
	if gerr != nil {
		// GetResult only fails when the target tx never executed (e.g. a replay
		// divergence aborted an earlier tx); surface the replay error then.
		if perr != nil {
			return nil, fmt.Errorf("trace replay aborted before target tx: %w", perr)
		}
		return nil, gerr
	}
	return res, nil
}

func (b *TronBackend) GetTransactionByID(txHash tcommon.Hash) (*corepb.Transaction, error) {
	tx, _, ok, err := b.indexedTransactionByID(txHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("transaction not found")
	}
	return tx, nil
}

func (b *TronBackend) GetTransactionBlockNumByID(txHash tcommon.Hash) (uint64, bool, error) {
	return rawdb.ReadTransactionIndexStrict(b.chain.chaindb, txHash[:])
}

func (b *TronBackend) indexedTransactionByID(txHash tcommon.Hash) (*corepb.Transaction, uint64, bool, error) {
	blockNum, ok, err := rawdb.ReadTransactionIndexStrict(b.chain.chaindb, txHash[:])
	if err != nil || !ok {
		return nil, 0, ok, err
	}
	block, hasBlock, err := rawdb.ReadBlockStrict(b.chain.chaindb, blockNum)
	if err != nil {
		return nil, 0, false, err
	}
	if !hasBlock {
		return nil, 0, false, fmt.Errorf("block body missing for indexed transaction %x at block %d", txHash, blockNum)
	}
	for _, tx := range block.Transactions() {
		if tx.Hash() == txHash {
			return tx.Proto(), blockNum, true, nil
		}
	}
	return nil, 0, false, fmt.Errorf("transaction not found in block %d", blockNum)
}

func (b *TronBackend) GetTransactionInfoByID(txHash tcommon.Hash) (*corepb.TransactionInfo, error) {
	info, ok, err := rawdb.ReadTransactionInfoStrict(b.chain.chaindb, txHash[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("transaction info not found")
	}
	if head := b.chain.CurrentBlock(); head != nil && uint64(info.BlockNumber) > head.Number() {
		return nil, fmt.Errorf("transaction info not found")
	}
	return info, nil
}

func (b *TronBackend) GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error) {
	if head := b.chain.CurrentBlock(); head != nil && blockNum > head.Number() {
		return nil, nil
	}
	block, hasBlock, err := rawdb.ReadBlockStrict(b.chain.chaindb, blockNum)
	if err != nil {
		return nil, err
	}
	if !hasBlock {
		return nil, nil
	}
	infos, hasInfos, err := rawdb.ReadTransactionInfosByBlockStrict(b.chain.chaindb, blockNum)
	if err != nil {
		return nil, err
	}
	if hasInfos {
		if err := rawdb.ValidateTransactionInfosForBlock(blockNum, block.Transactions(), infos, "transaction info block query"); err != nil {
			return nil, err
		}
	}
	return infos, nil
}

func (b *TronBackend) GetBlockByHash(hash tcommon.Hash) (*types.Block, error) {
	// Try direct hash lookup first
	num, ok, err := rawdb.ReadBlockNumberStrict(b.chain.chaindb, hash)
	if err != nil {
		return nil, err
	}
	if ok {
		if head := b.chain.CurrentBlock(); head != nil && num > head.Number() {
			return nil, fmt.Errorf("block not found")
		}
		block, hasBlock, err := rawdb.ReadBlockStrict(b.chain.chaindb, num)
		if err != nil {
			return nil, err
		}
		if !hasBlock {
			return nil, fmt.Errorf("block body missing for indexed block %d hash %x", num, hash)
		}
		return block, nil
	}
	// The input may be a blockID (first 8 bytes = block number, rest = hash[8:]).
	// Extract the block number and look up by number, then verify the ID matches.
	blockIDNum := binary.BigEndian.Uint64(hash[:8])
	if blockIDNum > 0 {
		block, err := b.GetBlockByNumber(blockIDNum)
		if err != nil {
			if !strings.Contains(err.Error(), "not found") {
				return nil, err
			}
			return nil, fmt.Errorf("block not found")
		}
		if block.ID().Hash == hash {
			return block, nil
		}
	}
	return nil, fmt.Errorf("block not found")
}

func (b *TronBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	if end <= start {
		return nil, fmt.Errorf("invalid range")
	}
	if end-start > 100 {
		end = start + 100
	}
	var blocks []*types.Block
	for i := start; i < end; i++ {
		block, ok, err := rawdb.ReadBlockStrict(b.chain.chaindb, i)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func (b *TronBackend) BuildTransferTransaction(owner, to tcommon.Address, amount int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	tc := &contractpb.TransferContract{
		OwnerAddress: owner[:],
		ToAddress:    to[:],
		Amount:       amount,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_TransferContract, tc, 0)
}

func (b *TronBackend) BuildDeployContractTransaction(owner tcommon.Address, abi string, bytecode []byte,
	feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner[:],
		NewContract: &contractpb.SmartContract{
			OriginAddress:              owner[:],
			Abi:                        &contractpb.SmartContract_ABI{},
			Bytecode:                   bytecode,
			CallValue:                  callValue,
			Name:                       name,
			ConsumeUserResourcePercent: consumePercent,
		},
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_CreateSmartContract, csc, feeLimit)
}

func (b *TronBackend) BuildTriggerContractTransaction(owner, contract tcommon.Address, data []byte,
	feeLimit int64, callValue int64) (*corepb.Transaction, *tronapi.TriggerResult, error) {
	current := b.chain.CurrentBlock()
	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner[:],
		ContractAddress: contract[:],
		Data:            data,
		CallValue:       callValue,
	}
	tx, err := tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_TriggerSmartContract, tsc, feeLimit)
	if err != nil {
		return nil, nil, err
	}

	triggerResult, _ := b.TriggerConstantContract(owner, contract, data, 30_000_000)
	return tx, triggerResult, nil
}

func (b *TronBackend) EstimateEnergy(owner, contract tcommon.Address, data []byte) (int64, error) {
	result, err := b.TriggerConstantContract(owner, contract, data, 30_000_000)
	if err != nil {
		return 0, err
	}
	return result.EnergyUsed, nil
}

func (b *TronBackend) EstimateEnergyAt(owner, contract tcommon.Address, data []byte, blockNum uint64) (int64, error) {
	result, err := b.TriggerConstantContractAt(owner, contract, data, 30_000_000, blockNum)
	if err != nil {
		return 0, err
	}
	return result.EnergyUsed, nil
}

func (b *TronBackend) GetAccountResource(addr tcommon.Address) (*tronapi.AccountResource, error) {
	root, err := b.headStateRootStrict()
	if err != nil {
		return nil, fmt.Errorf("read head state root: %w", err)
	}
	return b.accountResourceAtRoot(addr, root)
}

// GetAccountResourceAt returns the resource view at the bound block (solid or
// PBFT-confirmed). Flat latest roots are commitments, not historical snapshots,
// so non-head reads use temporal domain history.
func (b *TronBackend) GetAccountResourceAt(addr tcommon.Address, blockNum uint64) (*tronapi.AccountResource, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	if blockNum == session.headNum {
		root := b.chain.StateRootAtBlock(session.headNum)
		if root == (tcommon.Hash{}) {
			return nil, fmt.Errorf("no state root for block %d", session.headNum)
		}
		return b.accountResourceAtRoot(addr, root)
	}
	acc, err := session.reader.AccountAt(addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct account resource at block %d: %w", blockNum, err)
	}
	dynProps, err := b.dynamicPropertiesAt(session.reader, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct dynamic properties at block %d: %w", blockNum, err)
	}
	return accountResourceFromAccount(acc, dynProps), nil
}

func (b *TronBackend) accountResourceAtRoot(addr tcommon.Address, root tcommon.Hash) (*tronapi.AccountResource, error) {
	statedb, err := b.chain.openState(root)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	// Read rooted dynprops through the same latest StateDB view so resource
	// limits stay consistent with the live account state. Use the buffer
	// overlay (not bc.db) as the derived dp- reader so those keys are as
	// fresh as the rooted reads even before the async flush settles —
	// matching cachedDynProps / block_builder. (Only rooted limit keys are
	// returned today, so this is defensive alignment, not a behaviour change.)
	dynProps := state.LoadDynamicProperties(b.chain.buffer, statedb)
	return accountResourceFromAccount(statedb.GetAccount(addr), dynProps), nil
}

// accountResourceFromAccount builds the getaccountresource view, mirroring
// java-tron Wallet.getAccountResource. It is the single source of truth shared
// by the live-head and archive paths. A nil account yields zero usages with the
// global limits still populated (the per-account share helpers return 0 for a
// nil account).
//
// Known parity gaps versus java (all out of scope for the empty-value fix):
//   - asset_net_used / asset_net_limit TRC10 maps are not ported (they need
//     asset-issue-store lookups).
//   - usages are the raw stored values; java runs Bandwidth/EnergyProcessor
//     updateUsage first, decaying them toward the head block time.
//   - a missing account returns the global limits here, whereas java returns
//     null (the servlet emits "{}"). The callers preserve that legacy behaviour.
func accountResourceFromAccount(acc *types.Account, dynProps *state.DynamicProperties) *tronapi.AccountResource {
	res := &tronapi.AccountResource{}
	if acc != nil {
		res.FreeNetUsed = acc.FreeNetUsage()
		res.NetUsed = acc.NetUsage()
		res.EnergyUsed = acc.EnergyUsage()
		res.TronPowerUsed = acc.TronPowerUsage()
		res.TronPowerLimit = acc.AllTronPower() / trxPrecision
		res.StorageUsed = acc.StorageUsage()
		res.StorageLimit = acc.StorageLimit()
	}
	if dynProps != nil {
		res.FreeNetLimit = dynProps.FreeNetLimit()
		res.TotalNetLimit = dynProps.TotalNetLimit()
		res.TotalNetWeight = dynProps.TotalNetWeight()
		res.TotalTronPowerWeight = dynProps.TotalTronPowerWeight()
		res.TotalEnergyLimit = dynProps.TotalEnergyCurrentLimit()
		res.TotalEnergyWeight = dynProps.TotalEnergyWeight()
		res.NetLimit = availableAccountNet(acc, dynProps)
		res.EnergyLimit = availableAccountEnergy(acc, dynProps)
	}
	return res
}

// accountResourceDynamicPropertyKeys are the dynamic-property keys the
// getaccountresource view reads. The archive path reconstructs only these from
// temporal history, so it must stay in sync with the dynProps getters consumed
// by accountResourceFromAccount — a missing key silently zeroes a limit/weight
// on archive reads while the live path (full dynProps) still works.
var accountResourceDynamicPropertyKeys = []string{
	"free_net_limit",
	"total_net_limit",
	"total_net_weight",
	"total_energy_current_limit",
	"total_energy_weight",
	"total_tron_power_weight",
}

var exchangeDynamicPropertyKeys = []string{
	"latest_exchange_num",
	"allow_same_token_name",
}

var assetDynamicPropertyKeys = []string{
	"token_id_num",
	"allow_same_token_name",
}

func (b *TronBackend) dynamicPropertiesAt(reader *state.PersistentHistoryReader, blockNum uint64) (*state.DynamicProperties, error) {
	return b.dynamicPropertiesAtKeys(reader, blockNum, accountResourceDynamicPropertyKeys)
}

func (b *TronBackend) dynamicPropertiesAtKeys(reader *state.PersistentHistoryReader, blockNum uint64, keys []string) (*state.DynamicProperties, error) {
	dp := state.NewDynamicProperties()
	for _, key := range keys {
		value, ok, err := reader.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemDynamicProperty, []byte(key), blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if len(value) != 8 {
			return nil, fmt.Errorf("dynamic property %s has length %d", key, len(value))
		}
		dp.Set(key, int64(binary.BigEndian.Uint64(value)))
	}
	return dp, nil
}

func (b *TronBackend) dynamicStringPropertiesAtKeys(reader *state.PersistentHistoryReader, blockNum uint64, keys []string) (*state.DynamicProperties, error) {
	dp := state.NewDynamicProperties()
	for _, key := range keys {
		value, ok, err := reader.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemDynamicProperty, []byte(key), blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		dp.SetString(key, string(value))
	}
	return dp, nil
}

func (b *TronBackend) GetChainParameters() []tronapi.ChainParameter {
	return chainParametersFromDynamicProperties(b.chain.DynProps())
}

func (b *TronBackend) GetChainParametersAt(blockNum uint64) ([]tronapi.ChainParameter, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	dynProps, err := b.dynamicPropertiesAtKeys(session.reader, blockNum, state.NewDynamicProperties().Keys())
	if err != nil {
		return nil, fmt.Errorf("reconstruct chain parameters at block %d: %w", blockNum, err)
	}
	return chainParametersFromDynamicProperties(dynProps), nil
}

func chainParametersFromDynamicProperties(dynProps *state.DynamicProperties) []tronapi.ChainParameter {
	if dynProps == nil {
		return nil
	}
	all := dynProps.All()
	params := make([]tronapi.ChainParameter, 0, len(all))
	for k, v := range all {
		params = append(params, tronapi.ChainParameter{Key: k, Value: v})
	}
	sort.Slice(params, func(i, j int) bool { return params[i].Key < params[j].Key })
	return params
}

func (b *TronBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) {
	statedb := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if statedb == nil {
		return nil, nil
	}
	witnessAddrs := statedb.ReadWitnessIndex()
	pendingDeltas, _ := pendingVoteDeltas(statedb)
	activeMap := activeWitnessMap(b.chain.ActiveWitnesses())

	var result []*tronapi.WitnessInfo
	for _, addr := range witnessAddrs {
		w := statedb.GetWitness(addr)
		if w == nil {
			continue
		}
		result = append(result, &tronapi.WitnessInfo{
			Address:        hex.EncodeToString(addr[:]),
			VoteCount:      w.VoteCount() + pendingDeltas[addr],
			URL:            w.URL(),
			IsJobs:         activeMap[addr],
			TotalProduced:  w.TotalProduced(),
			TotalMissed:    w.TotalMissed(),
			LatestBlockNum: w.LatestBlockNum(),
			LatestSlotNum:  w.LatestSlotNum(),
		})
	}
	return result, nil
}

func (b *TronBackend) ListWitnessesAt(blockNum uint64) ([]*tronapi.WitnessInfo, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	witnessAddrs, err := session.reader.WitnessIndexAt(blockNum)
	if err != nil {
		return nil, fmt.Errorf("read witness index at block %d: %w", blockNum, err)
	}
	pendingDeltas, _, err := session.reader.PendingVoteDeltasAt(blockNum)
	if err != nil {
		return nil, fmt.Errorf("read pending vote deltas at block %d: %w", blockNum, err)
	}
	activeSet, err := session.reader.ActiveWitnessesAt(blockNum)
	if err != nil {
		return nil, fmt.Errorf("read active witnesses at block %d: %w", blockNum, err)
	}
	activeMap := activeWitnessMap(activeSet)

	result := make([]*tronapi.WitnessInfo, 0, len(witnessAddrs))
	for _, addr := range witnessAddrs {
		w, err := session.reader.WitnessAt(addr, blockNum)
		if err != nil {
			return nil, fmt.Errorf("read witness %s at block %d: %w", addr.Hex(), blockNum, err)
		}
		if w == nil {
			continue
		}
		result = append(result, &tronapi.WitnessInfo{
			Address:        hex.EncodeToString(addr[:]),
			VoteCount:      w.VoteCount() + pendingDeltas[addr],
			URL:            w.URL(),
			IsJobs:         activeMap[addr],
			TotalProduced:  w.TotalProduced(),
			TotalMissed:    w.TotalMissed(),
			LatestBlockNum: w.LatestBlockNum(),
			LatestSlotNum:  w.LatestSlotNum(),
		})
	}
	return result, nil
}

func activeWitnessMap(activeSet []tcommon.Address) map[tcommon.Address]bool {
	activeMap := make(map[tcommon.Address]bool, len(activeSet))
	for _, a := range activeSet {
		activeMap[a] = true
	}
	return activeMap
}

func (b *TronBackend) NextMaintenanceTime() int64 {
	return b.chain.NextMaintenanceTime()
}

func (b *TronBackend) NextMaintenanceTimeAt(blockNum uint64) (int64, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return 0, err
	}
	defer session.Close()

	dynProps, err := b.dynamicPropertiesAtKeys(session.reader, blockNum, []string{"next_maintenance_time"})
	if err != nil {
		return 0, fmt.Errorf("reconstruct next maintenance time at block %d: %w", blockNum, err)
	}
	return dynProps.NextMaintenanceTime(), nil
}

func (b *TronBackend) BuildFreezeBalanceV2Transaction(owner tcommon.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.FreezeBalanceV2Contract{
		OwnerAddress:  owner[:],
		FrozenBalance: amount,
		Resource:      resource,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_FreezeBalanceV2Contract, c, 0)
}

func (b *TronBackend) BuildUnfreezeBalanceV2Transaction(owner tcommon.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.UnfreezeBalanceV2Contract{
		OwnerAddress:    owner[:],
		UnfreezeBalance: amount,
		Resource:        resource,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_UnfreezeBalanceV2Contract, c, 0)
}

func (b *TronBackend) BuildDelegateResourceTransaction(owner, receiver tcommon.Address, balance int64, resource corepb.ResourceCode, lock bool) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Balance:         balance,
		Resource:        resource,
		Lock:            lock,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_DelegateResourceContract, c, 0)
}

func (b *TronBackend) BuildUnDelegateResourceTransaction(owner, receiver tcommon.Address, balance int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Balance:         balance,
		Resource:        resource,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
}

func (b *TronBackend) BuildCancelAllUnfreezeV2Transaction(owner tcommon.Address) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.CancelAllUnfreezeV2Contract{OwnerAddress: owner[:]}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_CancelAllUnfreezeV2Contract, c, 0)
}

func (b *TronBackend) BuildWithdrawExpireUnfreezeTransaction(owner tcommon.Address) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.WithdrawExpireUnfreezeContract{OwnerAddress: owner[:]}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_WithdrawExpireUnfreezeContract, c, 0)
}

func (b *TronBackend) BuildVoteWitnessTransaction(owner tcommon.Address, votes map[tcommon.Address]int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	vs := make([]*contractpb.VoteWitnessContract_Vote, 0, len(votes))
	for addr, count := range votes {
		a := addr
		vs = append(vs, &contractpb.VoteWitnessContract_Vote{
			VoteAddress: a[:],
			VoteCount:   count,
		})
	}
	c := &contractpb.VoteWitnessContract{
		OwnerAddress: owner[:],
		Votes:        vs,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_VoteWitnessContract, c, 0)
}

func (b *TronBackend) BuildProposalCreateTransaction(owner tcommon.Address, params map[int64]int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   params,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_ProposalCreateContract, c, 0)
}

func (b *TronBackend) BuildProposalApproveTransaction(owner tcommon.Address, proposalID int64, approve bool) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    proposalID,
		IsAddApproval: approve,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_ProposalApproveContract, c, 0)
}

func (b *TronBackend) BuildProposalDeleteTransaction(owner tcommon.Address, proposalID int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.ProposalDeleteContract{
		OwnerAddress: owner[:],
		ProposalId:   proposalID,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_ProposalDeleteContract, c, 0)
}

func (b *TronBackend) ListProposals() ([]*tronapi.ProposalInfo, error) {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil, nil
	}
	ids := sysKV.ReadProposalIndex()
	result := make([]*tronapi.ProposalInfo, 0, len(ids))
	for _, id := range ids {
		p := sysKV.ReadProposal(id)
		if p == nil {
			continue
		}
		result = append(result, proposalInfoFromRaw(p))
	}
	return result, nil
}

func (b *TronBackend) ListProposalsAt(blockNum uint64) ([]*tronapi.ProposalInfo, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	ids, err := session.reader.ProposalIndexAt(blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct proposal index at block %d: %w", blockNum, err)
	}
	result := make([]*tronapi.ProposalInfo, 0, len(ids))
	for _, id := range ids {
		p, err := session.reader.ProposalAt(id, blockNum)
		if err != nil {
			return nil, fmt.Errorf("reconstruct proposal %d at block %d: %w", id, blockNum, err)
		}
		if p == nil {
			continue
		}
		result = append(result, proposalInfoFromRaw(p))
	}
	return result, nil
}

func proposalInfoFromRaw(p *rawdb.Proposal) *tronapi.ProposalInfo {
	params := proposalParametersToList(p.Parameters)
	approvals := make([]string, len(p.Approvals))
	for i, a := range p.Approvals {
		approvals[i] = hex.EncodeToString(a[:])
	}
	stateStr := "PENDING"
	switch p.State {
	case rawdb.ProposalStateApproved:
		stateStr = "APPROVED"
	case rawdb.ProposalStateCanceled:
		stateStr = "CANCELED"
	}
	return &tronapi.ProposalInfo{
		ProposalID:      p.ID,
		ProposerAddress: hex.EncodeToString(p.Proposer[:]),
		Parameters:      params,
		ExpirationTime:  p.ExpirationTime,
		CreateTime:      p.CreateTime,
		Approvals:       approvals,
		State:           stateStr,
	}
}

// proposalParametersToList converts a Proposal.parameters map to a sorted
// slice of {key, value} entries, matching java-tron's HTTP wire format
// (`[{"key":N,"value":V},...]`). Sorted by key for deterministic output.
func proposalParametersToList(m map[int64]int64) []tronapi.ProposalParameterEntry {
	if len(m) == 0 {
		return []tronapi.ProposalParameterEntry{}
	}
	out := make([]tronapi.ProposalParameterEntry, 0, len(m))
	for k, v := range m {
		out = append(out, tronapi.ProposalParameterEntry{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (b *TronBackend) GetDelegatedResourceV2(from, to tcommon.Address) ([]*tronapi.DelegatedResourceInfo, error) {
	statedb, err := b.chain.openCurrentState()
	if err != nil {
		return nil, fmt.Errorf("open head state: %w", err)
	}
	resources := make([]*tronapi.DelegatedResourceInfo, 0, 2)
	for _, locked := range []bool{false, true} {
		dr := statedb.ReadDelegatedResourceV2(from, to, locked)
		if !nonEmptyDelegatedResource(dr) {
			continue
		}
		resources = append(resources, delegatedResourceInfo(from, to, dr))
	}
	return resources, nil
}

func (b *TronBackend) GetDelegatedResourceV2At(from, to tcommon.Address, blockNum uint64) ([]*tronapi.DelegatedResourceInfo, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	resources := make([]*tronapi.DelegatedResourceInfo, 0, 2)
	for _, locked := range []bool{false, true} {
		dr, err := readDelegatedResourceV2At(session.reader, from, to, locked, blockNum)
		if err != nil {
			return nil, fmt.Errorf("read delegated resource v2 at block %d: %w", blockNum, err)
		}
		if !nonEmptyDelegatedResource(dr) {
			continue
		}
		resources = append(resources, delegatedResourceInfo(from, to, dr))
	}
	return resources, nil
}

func readDelegatedResourceV2At(reader *state.PersistentHistoryReader, from, to tcommon.Address, locked bool, blockNum uint64) (*rawdb.DelegatedResource, error) {
	key := rawdb.DelegatedResourceV2StateKey(from, to, locked)
	data, ok, err := reader.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, blockNum)
	if err != nil || !ok || len(data) == 0 {
		return nil, err
	}
	dr := &rawdb.DelegatedResource{}
	if err := json.Unmarshal(data, dr); err != nil {
		return nil, err
	}
	return dr, nil
}

func readDelegatedResourceAt(reader *state.PersistentHistoryReader, from, to tcommon.Address, blockNum uint64) (*rawdb.DelegatedResource, error) {
	var out *rawdb.DelegatedResource
	mergeKey := func(key []byte) error {
		data, ok, err := reader.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, blockNum)
		if err != nil || !ok || len(data) == 0 {
			return err
		}
		dr := &rawdb.DelegatedResource{}
		if err := json.Unmarshal(data, dr); err != nil {
			return err
		}
		if out == nil {
			out = &rawdb.DelegatedResource{From: from, To: to}
		}
		out.FrozenBalanceForBandwidth += dr.FrozenBalanceForBandwidth
		out.FrozenBalanceForEnergy += dr.FrozenBalanceForEnergy
		if dr.ExpireTimeForBandwidth > out.ExpireTimeForBandwidth {
			out.ExpireTimeForBandwidth = dr.ExpireTimeForBandwidth
		}
		if dr.ExpireTimeForEnergy > out.ExpireTimeForEnergy {
			out.ExpireTimeForEnergy = dr.ExpireTimeForEnergy
		}
		return nil
	}
	for _, key := range [][]byte{
		rawdb.DelegatedResourceStateKey(from, to),
		rawdb.DelegatedResourceV2StateKey(from, to, false),
		rawdb.DelegatedResourceV2StateKey(from, to, true),
	} {
		if err := mergeKey(key); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func nonEmptyDelegatedResource(dr *rawdb.DelegatedResource) bool {
	return dr != nil &&
		(dr.FrozenBalanceForBandwidth != 0 ||
			dr.FrozenBalanceForEnergy != 0 ||
			dr.ExpireTimeForBandwidth != 0 ||
			dr.ExpireTimeForEnergy != 0)
}

func delegatedResourceInfo(from, to tcommon.Address, dr *rawdb.DelegatedResource) *tronapi.DelegatedResourceInfo {
	return &tronapi.DelegatedResourceInfo{
		FromAddress:               hex.EncodeToString(from[:]),
		ToAddress:                 hex.EncodeToString(to[:]),
		FrozenBalanceForBandwidth: dr.FrozenBalanceForBandwidth,
		FrozenBalanceForEnergy:    dr.FrozenBalanceForEnergy,
		ExpireTimeForBandwidth:    dr.ExpireTimeForBandwidth,
		ExpireTimeForEnergy:       dr.ExpireTimeForEnergy,
	}
}

func (b *TronBackend) GetDelegatedResourceAccountIndexV2(addr tcommon.Address) (*tronapi.DelegationIndexInfo, error) {
	statedb, err := b.chain.openCurrentState()
	if err != nil {
		return nil, fmt.Errorf("open head state: %w", err)
	}
	receivers, err := statedb.ReadDelegationIndexStrict(addr)
	if err != nil {
		return nil, fmt.Errorf("read delegation index: %w", err)
	}
	toAddresses := make([]string, len(receivers))
	for i, r := range receivers {
		toAddresses[i] = hex.EncodeToString(r[:])
	}
	return &tronapi.DelegationIndexInfo{
		Account:     hex.EncodeToString(addr[:]),
		ToAddresses: toAddresses,
	}, nil
}

func (b *TronBackend) GetDelegatedResourceAccountIndexV2At(addr tcommon.Address, blockNum uint64) (*tronapi.DelegationIndexInfo, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	receivers, err := readDelegationIndexAt(session.reader, addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("read delegation index at block %d: %w", blockNum, err)
	}
	toAddresses := make([]string, len(receivers))
	for i, r := range receivers {
		toAddresses[i] = hex.EncodeToString(r[:])
	}
	return &tronapi.DelegationIndexInfo{
		Account:     hex.EncodeToString(addr[:]),
		ToAddresses: toAddresses,
	}, nil
}

func readDelegationIndexAt(reader *state.PersistentHistoryReader, addr tcommon.Address, blockNum uint64) ([]tcommon.Address, error) {
	key := rawdb.DelegationIndexStateKey(addr)
	data, ok, err := reader.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemDelegation, key, blockNum)
	if err != nil || !ok || len(data) == 0 {
		return nil, err
	}
	if len(data)%tcommon.AddressLength != 0 {
		return nil, fmt.Errorf("delegation index at block %d has malformed length %d, want multiple of %d", blockNum, len(data), tcommon.AddressLength)
	}
	count := len(data) / tcommon.AddressLength
	addrs := make([]tcommon.Address, count)
	for i := range addrs {
		start := i * tcommon.AddressLength
		copy(addrs[i][:], data[start:start+tcommon.AddressLength])
	}
	return addrs, nil
}

func (b *TronBackend) CanDelegateResource(addr tcommon.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	statedb, err := b.chain.openCurrentState()
	if err != nil {
		return nil, fmt.Errorf("open head state: %w", err)
	}
	acc := statedb.GetAccount(addr)

	// Compute already-delegated amount from the delegation index.
	var delegated int64
	receivers, err := statedb.ReadDelegationIndexStrict(addr)
	if err != nil {
		return nil, fmt.Errorf("read delegation index: %w", err)
	}
	for _, receiver := range receivers {
		dr := statedb.ReadDelegatedResource(addr, receiver)
		if dr == nil {
			continue
		}
		switch resource {
		case corepb.ResourceCode_BANDWIDTH:
			delegated += dr.FrozenBalanceForBandwidth
		case corepb.ResourceCode_ENERGY:
			delegated += dr.FrozenBalanceForEnergy
		}
	}
	return canDelegateResourceFromAccount(acc, amount, resource, delegated), nil
}

func (b *TronBackend) CanDelegateResourceAt(addr tcommon.Address, amount int64, resource corepb.ResourceCode, blockNum uint64) (*tronapi.CanDelegateInfo, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	acc, err := session.reader.AccountAt(addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct account at block %d: %w", blockNum, err)
	}
	receivers, err := readDelegationIndexAt(session.reader, addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("read delegation index at block %d: %w", blockNum, err)
	}
	var delegated int64
	for _, receiver := range receivers {
		dr, err := readDelegatedResourceAt(session.reader, addr, receiver, blockNum)
		if err != nil {
			return nil, fmt.Errorf("read delegated resource at block %d: %w", blockNum, err)
		}
		if dr == nil {
			continue
		}
		switch resource {
		case corepb.ResourceCode_BANDWIDTH:
			delegated += dr.FrozenBalanceForBandwidth
		case corepb.ResourceCode_ENERGY:
			delegated += dr.FrozenBalanceForEnergy
		}
	}
	return canDelegateResourceFromAccount(acc, amount, resource, delegated), nil
}

func canDelegateResourceFromAccount(acc *types.Account, amount int64, resource corepb.ResourceCode, delegated int64) *tronapi.CanDelegateInfo {
	var maxSize int64
	if acc != nil {
		maxSize = acc.GetFrozenV2Amount(resource)
	}
	canDelegate := maxSize - delegated
	if canDelegate < 0 {
		canDelegate = 0
	}
	return &tronapi.CanDelegateInfo{
		MaxSize:         maxSize,
		CanDelegateSize: canDelegate,
		Balance:         amount,
	}
}

func (b *TronBackend) GetCanWithdrawUnfreezeAmount(addr tcommon.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	statedb, err := b.chain.openCurrentState()
	if err != nil {
		return nil, fmt.Errorf("open head state: %w", err)
	}
	acc := statedb.GetAccount(addr)
	if acc == nil {
		return &tronapi.CanWithdrawUnfreezeInfo{Amount: 0}, nil
	}
	return canWithdrawUnfreezeAmountFromAccount(acc, timestamp), nil
}

func (b *TronBackend) GetCanWithdrawUnfreezeAmountAt(addr tcommon.Address, timestamp int64, blockNum uint64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	acc, err := b.accountAtOrNil(addr, blockNum)
	if err != nil {
		return nil, err
	}
	return canWithdrawUnfreezeAmountFromAccount(acc, timestamp), nil
}

func canWithdrawUnfreezeAmountFromAccount(acc *types.Account, timestamp int64) *tronapi.CanWithdrawUnfreezeInfo {
	if acc == nil {
		return &tronapi.CanWithdrawUnfreezeInfo{Amount: 0}
	}
	var total int64
	for _, u := range acc.UnfrozenV2() {
		if u.UnfreezeExpireTime <= timestamp {
			total += u.UnfreezeAmount
		}
	}
	return &tronapi.CanWithdrawUnfreezeInfo{Amount: total}
}

func (b *TronBackend) GetAvailableUnfreezeCount(addr tcommon.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	statedb, err := b.chain.openCurrentState()
	if err != nil {
		return nil, fmt.Errorf("open head state: %w", err)
	}
	return availableUnfreezeCountFromAccount(statedb.GetAccount(addr)), nil
}

func (b *TronBackend) GetAvailableUnfreezeCountAt(addr tcommon.Address, blockNum uint64) (*tronapi.AvailableUnfreezeCountInfo, error) {
	acc, err := b.accountAtOrNil(addr, blockNum)
	if err != nil {
		return nil, err
	}
	return availableUnfreezeCountFromAccount(acc), nil
}

func (b *TronBackend) accountAtOrNil(addr tcommon.Address, blockNum uint64) (*types.Account, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	acc, err := session.reader.AccountAt(addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct account at block %d: %w", blockNum, err)
	}
	return acc, nil
}

func availableUnfreezeCountFromAccount(acc *types.Account) *tronapi.AvailableUnfreezeCountInfo {
	const maxUnfreezeSlots = 32
	count := int64(maxUnfreezeSlots)
	if acc != nil {
		count = int64(maxUnfreezeSlots - len(acc.UnfrozenV2()))
	}
	if count < 0 {
		count = 0
	}
	return &tronapi.AvailableUnfreezeCountInfo{Count: count}
}

func (b *TronBackend) GetReward(addr tcommon.Address) (*tronapi.RewardInfo, error) {
	root, err := b.headStateRootStrict()
	if err != nil {
		return nil, fmt.Errorf("read head state root: %w", err)
	}
	return b.rewardAtRoot(addr, root)
}

// GetRewardAt returns the allowance at the bound block for the /walletsolidity/
// and /walletpbft/ variants.
func (b *TronBackend) GetRewardAt(addr tcommon.Address, blockNum uint64) (*tronapi.RewardInfo, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	acc, err := session.reader.AccountAt(addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct reward at block %d: %w", blockNum, err)
	}
	if acc == nil {
		return &tronapi.RewardInfo{}, nil
	}
	return &tronapi.RewardInfo{Reward: acc.Allowance()}, nil
}

func (b *TronBackend) rewardAtRoot(addr tcommon.Address, root tcommon.Hash) (*tronapi.RewardInfo, error) {
	statedb, err := b.chain.openState(root)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	return &tronapi.RewardInfo{Reward: statedb.GetAllowance(addr)}, nil
}

func (b *TronBackend) GetTransactionFromPending(txID string) (*corepb.Transaction, error) {
	hashBytes := tcommon.FromHex(txID)
	var hash tcommon.Hash
	copy(hash[:], hashBytes)
	tx := b.pool.Get(hash)
	if tx == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	return tx.Proto(), nil
}

func (b *TronBackend) GetTransactionListFromPending() ([]*corepb.Transaction, error) {
	txs := b.pool.Pending()
	result := make([]*corepb.Transaction, len(txs))
	for i, tx := range txs {
		result[i] = tx.Proto()
	}
	return result, nil
}

func (b *TronBackend) ListNodes() ([]*tronapi.PeerInfo, error) {
	if b.peersFunc == nil {
		return []*tronapi.PeerInfo{}, nil
	}
	return b.peersFunc(), nil
}

// firstAssetTokenID is the first TRC10 token id ever assignable: genesis
// token_id_num is 1_000_000 and AssetIssueActuator pre-increments before
// assigning, so ids start at 1_000_001. The rooted enumeration walks
// [firstAssetTokenID, token_id_num] because the KV trie cannot be prefix-scanned.
const firstAssetTokenID int64 = 1_000_001

func (b *TronBackend) GetAssetIssueByID(id int64) *contractpb.AssetIssueContract {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	return sysKV.ReadAssetIssue(id)
}

func (b *TronBackend) GetAssetIssueByIDAt(id int64, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	asset, err := session.reader.AssetIssueAt(id, blockNum)
	if err != nil {
		return nil, fmt.Errorf("read asset issue id %d at block %d: %w", id, blockNum, err)
	}
	return asset, nil
}

func (b *TronBackend) GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	dp := b.chain.DynProps()
	if !dp.AllowSameTokenName() {
		return sysKV.ReadAssetIssueByName(name)
	}
	var match *contractpb.AssetIssueContract
	for _, asset := range sysKV.ListAssetsV2(firstAssetTokenID, dp.TokenIdNum()) {
		if string(asset.Name) != string(name) {
			continue
		}
		if match != nil {
			return nil
		}
		match = asset
	}
	if id, err := strconv.ParseInt(string(name), 10, 64); err == nil {
		if asset := sysKV.ReadAssetIssue(id); asset != nil {
			if match != nil && match.Id != asset.Id {
				return nil
			}
			match = asset
		}
	}
	return match
}

func (b *TronBackend) GetAssetIssueByNameAt(name []byte, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	dynProps, err := b.dynamicPropertiesAtKeys(session.reader, blockNum, assetDynamicPropertyKeys)
	if err != nil {
		return nil, fmt.Errorf("reconstruct asset dynamic properties at block %d: %w", blockNum, err)
	}
	if !dynProps.AllowSameTokenName() {
		asset, err := session.reader.AssetIssueByNameAt(name, blockNum)
		if err != nil {
			return nil, fmt.Errorf("read legacy asset issue name %q at block %d: %w", string(name), blockNum, err)
		}
		return asset, nil
	}
	assets, err := session.reader.ListAssetsV2At(firstAssetTokenID, dynProps.TokenIdNum(), blockNum)
	if err != nil {
		return nil, fmt.Errorf("read asset issue list at block %d: %w", blockNum, err)
	}
	var match *contractpb.AssetIssueContract
	for _, asset := range assets {
		if string(asset.Name) != string(name) {
			continue
		}
		if match != nil {
			return nil, nil
		}
		match = asset
	}
	if id, err := strconv.ParseInt(string(name), 10, 64); err == nil {
		asset, err := session.reader.AssetIssueAt(id, blockNum)
		if err != nil {
			return nil, fmt.Errorf("read asset issue id %d at block %d: %w", id, blockNum, err)
		}
		if asset != nil {
			if match != nil && match.Id != asset.Id {
				return nil, nil
			}
			match = asset
		}
	}
	return match, nil
}

func (b *TronBackend) GetAssetIssueList() []*contractpb.AssetIssueContract {
	return b.listAssetsAtHead()
}

func (b *TronBackend) GetAssetIssueListAt(blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	return b.listAssetsAt(blockNum)
}

func (b *TronBackend) GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract {
	all := b.listAssetsAtHead()
	if offset >= len(all) {
		return nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end]
}

func (b *TronBackend) GetAssetIssueListPaginatedAt(offset, limit int, blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	all, err := b.listAssetsAt(blockNum)
	if err != nil {
		return nil, err
	}
	if offset >= len(all) {
		return nil, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

// listAssetsAtHead enumerates the rooted TRC10 asset set at the head state root,
// walking token ids firstAssetTokenID..token_id_num. Pre-AllowSameTokenName it
// returns the legacy (name-keyed) bucket; post-fork the V2 (id-keyed) bucket —
// matching the prior flat ListAllLegacyAssets/ListAllAssets selection.
//
// NOTE (java-parity, ordering change): the prior flat scan returned legacy
// records in name-lexicographic order (the astl- prefix sort); this walks the
// V2 id range and resolves each legacy twin, so the legacy leg now returns
// records in token-id-ascending order. The set is identical and the V2 leg is
// unaffected; post-fork the legacy bucket is frozen so the divergence is
// bounded. Flagged for stress-harness verification rather than fixed here, to
// keep the migration a pure storage move.
func (b *TronBackend) listAssetsAtHead() []*contractpb.AssetIssueContract {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	latest := b.chain.DynProps().TokenIdNum()
	if !b.chain.DynProps().AllowSameTokenName() {
		return sysKV.ListAssetsLegacy(firstAssetTokenID, latest)
	}
	return sysKV.ListAssetsV2(firstAssetTokenID, latest)
}

func (b *TronBackend) listAssetsAt(blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	dynProps, err := b.dynamicPropertiesAtKeys(session.reader, blockNum, assetDynamicPropertyKeys)
	if err != nil {
		return nil, fmt.Errorf("reconstruct asset dynamic properties at block %d: %w", blockNum, err)
	}
	if !dynProps.AllowSameTokenName() {
		assets, err := session.reader.ListAssetsLegacyAt(firstAssetTokenID, dynProps.TokenIdNum(), blockNum)
		if err != nil {
			return nil, fmt.Errorf("read legacy asset issue list at block %d: %w", blockNum, err)
		}
		return assets, nil
	}
	assets, err := session.reader.ListAssetsV2At(firstAssetTokenID, dynProps.TokenIdNum(), blockNum)
	if err != nil {
		return nil, fmt.Errorf("read asset issue v2 list at block %d: %w", blockNum, err)
	}
	return assets, nil
}

func (b *TronBackend) GetAssetIssueByAccount(addr tcommon.Address) *contractpb.AssetIssueContract {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	id, ok := sysKV.ReadAssetOwnerIndex(addr[:])
	if !ok {
		return nil
	}
	if !b.chain.DynProps().AllowSameTokenName() {
		if asset := sysKV.ReadAssetIssue(id); asset != nil {
			return sysKV.ReadAssetIssueByName(asset.Name)
		}
	}
	return sysKV.ReadAssetIssue(id)
}

func (b *TronBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	return sysKV.ReadMarketOrder(orderID)
}

func (b *TronBackend) GetMarketOrderByIDAt(orderID []byte, blockNum uint64) (*corepb.MarketOrder, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	order, err := session.reader.MarketOrderAt(orderID, blockNum)
	if err != nil {
		return nil, fmt.Errorf("read market order at block %d: %w", blockNum, err)
	}
	return order, nil
}

func (b *TronBackend) GetMarketOrdersByAccount(addr tcommon.Address) []*corepb.MarketOrder {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	mao := sysKV.ReadMarketAccountOrder(addr[:])
	var orders []*corepb.MarketOrder
	for _, id := range mao.Orders {
		if o := sysKV.ReadMarketOrder(id); o != nil {
			orders = append(orders, o)
		}
	}
	return orders
}

func (b *TronBackend) GetMarketOrdersByAccountAt(addr tcommon.Address, blockNum uint64) ([]*corepb.MarketOrder, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	mao, err := session.reader.MarketAccountOrderAt(addr[:], blockNum)
	if err != nil {
		return nil, fmt.Errorf("read market account order at block %d: %w", blockNum, err)
	}
	var orders []*corepb.MarketOrder
	for _, id := range mao.Orders {
		order, err := session.reader.MarketOrderAt(id, blockNum)
		if err != nil {
			return nil, fmt.Errorf("read market order %x at block %d: %w", id, blockNum, err)
		}
		if order != nil {
			orders = append(orders, order)
		}
	}
	return orders, nil
}

func (b *TronBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	return sysKV.ReadMarketPriceList(sellTokenID, buyTokenID)
}

func (b *TronBackend) GetMarketPriceByPairAt(sellTokenID, buyTokenID []byte, blockNum uint64) (*corepb.MarketPriceList, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	priceList, err := session.reader.MarketPriceListAt(sellTokenID, buyTokenID, blockNum)
	if err != nil {
		return nil, fmt.Errorf("read market price list at block %d: %w", blockNum, err)
	}
	return priceList, nil
}

// listExchangesAtHead enumerates the rooted exchange set at the head state root,
// walking ids 1..latest_exchange_num as RpcApiService.getExchangeList does. The
// V1/V2 bucket is selected through the same AllowSameTokenName final-store gate
// java-tron uses for exchange reads.
func (b *TronBackend) listExchangesAtHead() []*corepb.Exchange {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	// latest_exchange_num is read from the cached DynProps, which tracks the same
	// head this opens sysKV at; both are head-only, so they stay in sync.
	dynProps := b.chain.DynProps()
	if dynProps.AllowSameTokenName() {
		return sysKV.ListExchangesV2(dynProps.LatestExchangeNum())
	}
	return sysKV.ListExchanges(dynProps.LatestExchangeNum())
}

func (b *TronBackend) ListExchanges() ([]*corepb.Exchange, error) {
	return b.listExchangesAtHead(), nil
}

func (b *TronBackend) ListExchangesAt(blockNum uint64) ([]*corepb.Exchange, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	dynProps, err := b.dynamicPropertiesAtKeys(session.reader, blockNum, exchangeDynamicPropertyKeys)
	if err != nil {
		return nil, fmt.Errorf("reconstruct exchange dynamic properties at block %d: %w", blockNum, err)
	}
	if dynProps.AllowSameTokenName() {
		exchanges, err := session.reader.ListExchangesV2At(dynProps.LatestExchangeNum(), blockNum)
		if err != nil {
			return nil, fmt.Errorf("read exchange v2 list at block %d: %w", blockNum, err)
		}
		return exchanges, nil
	}
	exchanges, err := session.reader.ListExchangesAt(dynProps.LatestExchangeNum(), blockNum)
	if err != nil {
		return nil, fmt.Errorf("read exchange list at block %d: %w", blockNum, err)
	}
	return exchanges, nil
}

func (b *TronBackend) GetBrokerageInfo(addr tcommon.Address) int64 {
	// java-tron's RpcApiService.getBrokerageInfoCommon reads at
	// currentCycle, NOT at the base key (-1). Right after an UpdateBrokerage
	// tx the rate is only visible to readers who consult the snapshot at
	// the next maintenance — until then the cycle key holds the previous
	// rate. Mirror that semantic here so cross-impl byte-equal holds.
	dp := b.chain.DynProps()
	cycle := dp.CurrentCycleNumber()
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return int64(rawdb.DefaultBrokerage)
	}
	return int64(sysKV.ReadCycleBrokerage(cycle, addr.Bytes()))
}

func (b *TronBackend) GetBrokerageInfoAt(addr tcommon.Address, blockNum uint64) (int64, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return 0, err
	}
	defer session.Close()

	dynProps, err := b.dynamicPropertiesAtKeys(session.reader, blockNum, []string{"current_cycle_number"})
	if err != nil {
		return 0, fmt.Errorf("reconstruct current cycle at block %d: %w", blockNum, err)
	}
	rate, err := session.reader.CycleBrokerageAt(dynProps.CurrentCycleNumber(), addr.Bytes(), blockNum)
	if err != nil {
		return 0, fmt.Errorf("read cycle brokerage at block %d: %w", blockNum, err)
	}
	return rate, nil
}

func (b *TronBackend) TotalTransaction() int64 {
	// Read through the buffer overlay so the counter reflects the latest
	// applied block before the async flush worker has drained it to disk.
	return rawdb.ReadTotalTransactionCount(b.chain.BufferedDB())
}

func (b *TronBackend) GetBurnTrx() int64 {
	return b.chain.DynProps().BurnTrxAmount()
}

func (b *TronBackend) GetBurnTrxAt(blockNum uint64) (int64, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return 0, err
	}
	defer session.Close()

	dynProps, err := b.dynamicPropertiesAtKeys(session.reader, blockNum, []string{"burn_trx_amount"})
	if err != nil {
		return 0, fmt.Errorf("reconstruct burn TRX at block %d: %w", blockNum, err)
	}
	return dynProps.BurnTrxAmount(), nil
}

func (b *TronBackend) GetBandwidthPrices() string {
	return b.chain.DynProps().BandwidthPriceHistory()
}

func (b *TronBackend) GetBandwidthPricesAt(blockNum uint64) (string, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return "", err
	}
	defer session.Close()

	dynProps, err := b.dynamicStringPropertiesAtKeys(session.reader, blockNum, []string{"bandwidth_price_history"})
	if err != nil {
		return "", fmt.Errorf("reconstruct bandwidth prices at block %d: %w", blockNum, err)
	}
	return dynProps.BandwidthPriceHistory(), nil
}

func (b *TronBackend) GetEnergyPrices() string {
	return b.chain.DynProps().EnergyPriceHistory()
}

func (b *TronBackend) GetEnergyPricesAt(blockNum uint64) (string, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return "", err
	}
	defer session.Close()

	dynProps, err := b.dynamicStringPropertiesAtKeys(session.reader, blockNum, []string{"energy_price_history"})
	if err != nil {
		return "", fmt.Errorf("reconstruct energy prices at block %d: %w", blockNum, err)
	}
	return dynProps.EnergyPriceHistory(), nil
}

func (b *TronBackend) ListProposalsPaginated(offset, limit int) ([]*tronapi.ProposalInfo, error) {
	all, err := b.ListProposals()
	if err != nil || len(all) == 0 {
		return nil, err
	}
	if offset >= len(all) {
		return []*tronapi.ProposalInfo{}, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

func (b *TronBackend) ListProposalsPaginatedAt(offset, limit int, blockNum uint64) ([]*tronapi.ProposalInfo, error) {
	all, err := b.ListProposalsAt(blockNum)
	if err != nil || len(all) == 0 {
		return nil, err
	}
	if offset >= len(all) {
		return []*tronapi.ProposalInfo{}, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

func (b *TronBackend) ListExchangesPaginated(offset, limit int) ([]*corepb.Exchange, error) {
	all := b.listExchangesAtHead()
	if len(all) == 0 {
		return []*corepb.Exchange{}, nil
	}
	if offset >= len(all) {
		return []*corepb.Exchange{}, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

// ── M5.1 PR-1: Account / Permission ─────────────────────────────────────

func (b *TronBackend) BuildCreateAccountTransaction(owner, account tcommon.Address) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.AccountCreateContract{
		OwnerAddress:   owner[:],
		AccountAddress: account[:],
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_AccountCreateContract, c, 0)
}

func (b *TronBackend) BuildUpdateAccountTransaction(owner tcommon.Address, name []byte) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  name,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_AccountUpdateContract, c, 0)
}

func (b *TronBackend) BuildSetAccountIdTransaction(owner tcommon.Address, accountID []byte) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    accountID,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_SetAccountIdContract, c, 0)
}

func (b *TronBackend) BuildAccountPermissionUpdateTransaction(c *contractpb.AccountPermissionUpdateContract) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
}

func (b *TronBackend) GetAccountById(accountID []byte) (*types.Account, error) {
	// The account-id index is rooted (SystemAccountIndex): resolve it from the
	// system-KV at the head state root, mirroring ListWitnesses' rooted read.
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil, fmt.Errorf("account not found")
	}
	addrBytes, ok, err := sysKV.ReadAccountIdIndexStrict(accountID)
	if err != nil {
		return nil, fmt.Errorf("read account id index: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("account not found")
	}
	var addr tcommon.Address
	copy(addr[:], addrBytes)
	return b.GetAccount(addr)
}

func (b *TronBackend) GetAccountByIdAt(accountID []byte, blockNum uint64) (*types.Account, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	addrBytes, ok, err := session.reader.AccountIdIndexAt(accountID, blockNum)
	if err != nil {
		return nil, fmt.Errorf("read account id index at block %d: %w", blockNum, err)
	}
	if !ok || len(addrBytes) == 0 {
		return nil, fmt.Errorf("account not found")
	}
	var addr tcommon.Address
	copy(addr[:], addrBytes)
	acc, err := session.reader.AccountAt(addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct account id at block %d: %w", blockNum, err)
	}
	if acc == nil {
		return nil, fmt.Errorf("account not found at block %d", blockNum)
	}
	return acc, nil
}

func (b *TronBackend) GetAccountNet(addr tcommon.Address) (*apipb.AccountNetMessage, error) {
	root, err := b.headStateRootStrict()
	if err != nil {
		return nil, fmt.Errorf("read head state root: %w", err)
	}
	return b.accountNetAtRoot(addr, root)
}

func (b *TronBackend) GetAccountNetAt(addr tcommon.Address, blockNum uint64) (*apipb.AccountNetMessage, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	if blockNum == session.headNum {
		root := b.chain.StateRootAtBlock(session.headNum)
		if root == (tcommon.Hash{}) {
			return nil, fmt.Errorf("no state root for block %d", session.headNum)
		}
		return b.accountNetAtRoot(addr, root)
	}
	acc, err := session.reader.AccountAt(addr, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct account net at block %d: %w", blockNum, err)
	}
	dynProps, err := b.dynamicPropertiesAt(session.reader, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct dynamic properties at block %d: %w", blockNum, err)
	}
	return accountNetFromAccount(acc, dynProps), nil
}

func (b *TronBackend) accountNetAtRoot(addr tcommon.Address, root tcommon.Hash) (*apipb.AccountNetMessage, error) {
	statedb, err := b.chain.openState(root)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	acc := statedb.GetAccount(addr)
	if acc == nil {
		return nil, nil
	}
	dynProps := state.LoadDynamicProperties(b.chain.buffer, statedb)
	return accountNetFromAccount(acc, dynProps), nil
}

func accountNetFromAccount(acc *types.Account, dynProps *state.DynamicProperties) *apipb.AccountNetMessage {
	if acc == nil {
		return nil
	}
	frozenBW := acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH)
	var netLimit int64
	if dynProps != nil {
		if total := dynProps.TotalNetWeight(); total > 0 {
			netLimit = frozenBW * dynProps.TotalNetLimit() / total
		}
	}
	msg := &apipb.AccountNetMessage{
		FreeNetUsed: acc.FreeNetUsage(),
		NetUsed:     acc.NetUsage(),
		NetLimit:    netLimit,
	}
	if dynProps != nil {
		msg.FreeNetLimit = dynProps.FreeNetLimit()
		msg.TotalNetLimit = dynProps.TotalNetLimit()
		msg.TotalNetWeight = dynProps.TotalNetWeight()
	}
	return msg
}

// ── M5.1 PR-3+: Generic contract builder ────────────────────────────────

func (b *TronBackend) BuildContractTransaction(contractType corepb.Transaction_Contract_ContractType, contract proto.Message, feeLimit int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		contractType, contract, feeLimit)
}

func (b *TronBackend) GetProposalByID(id int64) (*tronapi.ProposalInfo, error) {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil, fmt.Errorf("proposal %d not found", id)
	}
	p := sysKV.ReadProposal(id)
	if p == nil {
		return nil, fmt.Errorf("proposal %d not found", id)
	}
	return proposalInfoFromRaw(p), nil
}

func (b *TronBackend) GetProposalByIDAt(id int64, blockNum uint64) (*tronapi.ProposalInfo, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	p, err := session.reader.ProposalAt(id, blockNum)
	if err != nil {
		return nil, fmt.Errorf("reconstruct proposal %d at block %d: %w", id, blockNum, err)
	}
	if p == nil {
		return nil, fmt.Errorf("proposal %d not found at block %d", id, blockNum)
	}
	return proposalInfoFromRaw(p), nil
}

func (b *TronBackend) ValidateAddress(addr string) (bool, string) {
	raw := tcommon.FromHex(addr)
	if len(raw) == 21 && raw[0] == 0x41 {
		return true, "Hex string format"
	}
	if len(raw) == 21 {
		return false, "Invalid address prefix"
	}
	return false, "Invalid address length"
}

// ── M5.1 PR-2: Transaction builders ─────────────────────────────────────

func (b *TronBackend) BuildTransferAssetTransaction(owner, to tcommon.Address, assetName []byte, amount int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.TransferAssetContract{
		AssetName:    assetName,
		OwnerAddress: owner[:],
		ToAddress:    to[:],
		Amount:       amount,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_TransferAssetContract, c, 0)
}

func (b *TronBackend) BuildParticipateAssetIssueTransaction(owner, to tcommon.Address, assetName []byte, amount int64) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.ParticipateAssetIssueContract{
		OwnerAddress: owner[:],
		ToAddress:    to[:],
		AssetName:    assetName,
		Amount:       amount,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_ParticipateAssetIssueContract, c, 0)
}

func (b *TronBackend) BuildCreateWitnessTransaction(owner tcommon.Address, url []byte) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.WitnessCreateContract{
		OwnerAddress: owner[:],
		Url:          url,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_WitnessCreateContract, c, 0)
}

func (b *TronBackend) BuildUpdateWitnessTransaction(owner tcommon.Address, url []byte) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.WitnessUpdateContract{
		OwnerAddress: owner[:],
		UpdateUrl:    url,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_WitnessUpdateContract, c, 0)
}

func (b *TronBackend) BuildWithdrawBalanceTransaction(owner tcommon.Address) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.WithdrawBalanceContract{OwnerAddress: owner[:]}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_WithdrawBalanceContract, c, 0)
}

func (b *TronBackend) BuildUpdateBrokerageTransaction(owner tcommon.Address, brokerage int32) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    brokerage,
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
}

func (b *TronBackend) BuildFreezeBalanceV1Transaction(owner tcommon.Address, amount, duration int64, resource corepb.ResourceCode, receiver tcommon.Address) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.FreezeBalanceContract{
		OwnerAddress:    owner[:],
		FrozenBalance:   amount,
		FrozenDuration:  duration,
		Resource:        resource,
		ReceiverAddress: receiver[:],
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_FreezeBalanceContract, c, 0)
}

func (b *TronBackend) BuildUnfreezeBalanceV1Transaction(owner tcommon.Address, resource corepb.ResourceCode, receiver tcommon.Address) (*corepb.Transaction, error) {
	current := b.chain.CurrentBlock()
	c := &contractpb.UnfreezeBalanceContract{
		OwnerAddress:    owner[:],
		Resource:        resource,
		ReceiverAddress: receiver[:],
	}
	return tronapi.BuildTransaction(current.Number(), current.Hash().Bytes(), current.Timestamp(),
		corepb.Transaction_Contract_UnfreezeBalanceContract, c, 0)
}

// ── JSON-RPC Backend implementation (Phase 11) ────────────────────────────

func (b *TronBackend) ChainID() int64 {
	return b.chain.Config().ChainID
}

// ── M5.2 PR-1: JSON-RPC node metadata ────────────────────────────────────────

func (b *TronBackend) GasPrice() int64 {
	return b.chain.DynProps().EnergyFee()
}

func (b *TronBackend) PeerCount() int {
	if b.peersFunc == nil {
		return 0
	}
	return len(b.peersFunc())
}

func (b *TronBackend) BlockNumber() uint64 {
	return b.chain.CurrentBlock().Number()
}

func (b *TronBackend) GetBalance(addr tcommon.Address) int64 {
	root := b.chain.HeadStateRoot()
	statedb, err := b.chain.openState(root)
	if err != nil {
		return 0
	}
	return statedb.GetBalance(addr)
}

func (b *TronBackend) GetAccountBalanceTrace(req *contractpb.AccountBalanceRequest) (*contractpb.AccountBalanceResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("account balance request required")
	}
	accountID := req.GetAccountIdentifier()
	if accountID == nil || len(accountID.GetAddress()) == 0 {
		return nil, fmt.Errorf("account_identifier address is null")
	}
	block, err := b.balanceTraceBlock(req.GetBlockIdentifier())
	if err != nil {
		return nil, err
	}
	requestedID := blockBalanceIdentifier(block)
	traceBlock, balance, ok, err := rawdb.ReadAccountTraceAtOrBefore(b.chain.chaindb, accountID.GetAddress(), int64(block.Number()))
	if err != nil {
		return nil, err
	}
	if !ok {
		return &contractpb.AccountBalanceResponse{
			Balance:         0,
			BlockIdentifier: requestedID,
		}, nil
	}
	responseID := requestedID
	if uint64(traceBlock) != block.Number() {
		traceBlockObj, err := b.GetBlockByNumber(uint64(traceBlock))
		if err != nil {
			return nil, fmt.Errorf("read account trace block %d: %w", traceBlock, err)
		}
		responseID = blockBalanceIdentifier(traceBlockObj)
	}
	return &contractpb.AccountBalanceResponse{
		Balance:         balance,
		BlockIdentifier: responseID,
	}, nil
}

func (b *TronBackend) GetBlockBalanceTrace(id *contractpb.BlockBalanceTrace_BlockIdentifier) (*contractpb.BlockBalanceTrace, error) {
	block, err := b.balanceTraceBlock(id)
	if err != nil {
		return nil, err
	}
	trace, ok, err := rawdb.ReadBlockBalanceTraceStrict(b.chain.chaindb, int64(block.Number()))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("block balance trace %d not found", block.Number())
	}
	return trace, nil
}

func (b *TronBackend) GetCode(addr tcommon.Address) []byte {
	root := b.chain.HeadStateRoot()
	statedb, err := b.chain.openState(root)
	if err != nil {
		return nil
	}
	return statedb.GetCode(addr)
}

func (b *TronBackend) GetStorageAt(addr tcommon.Address, slot tcommon.Hash) tcommon.Hash {
	root := b.chain.HeadStateRoot()
	statedb, err := b.chain.openState(root)
	if err != nil {
		return tcommon.Hash{}
	}
	return statedb.GetState(addr, slot)
}

func (b *TronBackend) balanceTraceBlock(id *contractpb.BlockBalanceTrace_BlockIdentifier) (*types.Block, error) {
	if id == nil {
		return nil, fmt.Errorf("block_identifier null")
	}
	if id.GetNumber() < 0 {
		return nil, fmt.Errorf("block_identifier number less than 0")
	}
	if len(id.GetHash()) != tcommon.HashLength {
		return nil, fmt.Errorf("block_identifier hash length not equals 32")
	}
	block, err := b.GetBlockByNumber(uint64(id.GetNumber()))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(block.Hash().Bytes(), id.GetHash()) {
		return nil, fmt.Errorf("number and hash do not match")
	}
	return block, nil
}

func blockBalanceIdentifier(block *types.Block) *contractpb.BlockBalanceTrace_BlockIdentifier {
	if block == nil {
		return nil
	}
	return &contractpb.BlockBalanceTrace_BlockIdentifier{
		Number: int64(block.Number()),
		Hash:   append([]byte(nil), block.Hash().Bytes()...),
	}
}

// ErrArchiveHistoryDisabled is returned by the *At archive-query methods
// when the caller asks for a historical block (blockNum < head) on a node
// that wasn't synced with --history.enabled. Such a node has no temporal
// domain changes on disk, so the historical answer is unrecoverable; rather
// than silently returning the live value (which would be wrong for any block
// < head) the backend surfaces this clear error. Queries AT head are still
// served from live state and never hit this path.
var ErrArchiveHistoryDisabled = fmt.Errorf("archive history not available: node not running with --history.enabled")

// ErrArchiveHistoryPruned is returned when a historical query asks for a block
// below the local flat-history retention floor. In full mode the domain pruner
// deletes StateTxRange/StateDomainChange rows below the retention window, so
// reconstructing those heights would silently skip required rollback deltas and return an
// unverifiable state.
var ErrArchiveHistoryPruned = fmt.Errorf("archive history pruned for requested block")

// historyReaderAt builds a single-use PersistentHistoryReader for one
// archive query and reports the chain head number it was constructed
// against. The reader walks temporal domain rows newest-first from head down
// to the requested block, so its `db` and `live` baseline must agree on "head":
//
//   - db = b.chain.buffer — the buffer overlay sees temporal rows for blocks
//     applied but not yet flushed to disk (head can lead the flushed/
//     solidified boundary by ~19 blocks on mainnet DPoS), matching the
//     fork-rewind-safe reader the reorg tests exercise.
//   - live = StateDB opened at the head's committed state root — the flat
//     account view the reader rolls domain changes back from, the same
//     baseline the live GetBalance/GetCode reads use.
//
// The chain mutex is held until the caller finishes the query. StateDB opens
// against a committed root marker, but latest flat rows are still resolved
// lazily through the buffer-backed latest domains. Releasing the mutex before
// AccountAt/CodeAt/StorageAt would let a concurrent InsertBlock advance those
// latest rows while headNum still points at the older threshold, leaving the
// new block's writes un-rolled-back in historical answers.
//
// Callers that serve archive/as-of API requests should use archiveStateAt so
// range, mode, and prune-window gates cannot be skipped. Direct callers remain
// responsible for the flat-history availability gate (see requireArchive) and
// must call the returned release function.
func (b *TronBackend) historyReaderAt() (*state.PersistentHistoryReader, uint64, func(), error) {
	b.chain.chainmu.Lock()
	headNum := b.chain.CurrentBlock().Number()
	root, ok, err := b.chain.stateRootAtBlockStrict(headNum)
	if err != nil {
		b.chain.chainmu.Unlock()
		return nil, 0, nil, err
	}
	if !ok {
		b.chain.chainmu.Unlock()
		return nil, 0, nil, fmt.Errorf("state root for head block %d not available", headNum)
	}
	live, err := b.chain.openState(root)
	if err != nil {
		b.chain.chainmu.Unlock()
		return nil, 0, nil, fmt.Errorf("open head state: %w", err)
	}
	return state.NewPersistentHistoryReaderWithColdHistory(b.chain.buffer, live, headNum, b.stateColdHistory), headNum, b.chain.chainmu.Unlock, nil
}

type archiveStateSession struct {
	reader  *state.PersistentHistoryReader
	headNum uint64
	release func()
}

func (s *archiveStateSession) Close() {
	if s == nil || s.release == nil {
		return
	}
	s.release()
	s.release = nil
}

func (b *TronBackend) archiveStateAt(blockNum uint64) (*archiveStateSession, error) {
	reader, headNum, releaseHistory, err := b.historyReaderAt()
	if err != nil {
		return nil, err
	}
	session := &archiveStateSession{
		reader:  reader,
		headNum: headNum,
		release: releaseHistory,
	}
	if err := b.requireArchive(blockNum, headNum); err != nil {
		session.Close()
		return nil, err
	}
	return session, nil
}

// requireArchive enforces the block range and flat-history gates for a query
// bound to blockNum. A query at head is served from live state. A query past
// head has no committed state and must fail instead of silently returning head.
// A query for a strictly-older block requires StateTxRange metadata at head and
// at the requested block (except genesis block 0).
func (b *TronBackend) requireArchive(blockNum, headNum uint64) error {
	if blockNum > headNum {
		return fmt.Errorf("block %d is beyond current head %d", blockNum, headNum)
	}
	if blockNum == headNum {
		return nil
	}
	cfg := b.chain.Config()
	if cfg == nil || !cfg.HistoryEnabled {
		return ErrArchiveHistoryDisabled
	}

	if archiveModeUsesLocalHistoryWindow(cfg.EffectiveHistoryMode()) {
		window := cfg.EffectiveHistoryPruneWindow()
		if window > 0 && headNum >= window {
			firstAvailable := headNum - window + 1
			if blockNum < firstAvailable {
				return fmt.Errorf("%w: requested=%d first_available=%d",
					ErrArchiveHistoryPruned, blockNum, firstAvailable)
			}
		}
	}

	if ok, err := b.archiveStateTxRangeAvailable(headNum); err != nil {
		return fmt.Errorf("read flat history head range: %w", err)
	} else if !ok {
		return ErrArchiveHistoryDisabled
	}
	if blockNum > 0 {
		if ok, err := b.archiveStateTxRangeAvailable(blockNum); err != nil {
			return fmt.Errorf("read flat history range: %w", err)
		} else if !ok {
			return fmt.Errorf("%w: requested=%d first_available=%d",
				ErrArchiveHistoryPruned, blockNum, blockNum+1)
		}
	}
	return nil
}

func archiveModeUsesLocalHistoryWindow(mode string) bool {
	switch mode {
	case params.HistoryModeFull, params.HistoryModeBlocks, params.HistoryModeMinimal:
		return true
	default:
		return false
	}
}

func (b *TronBackend) archiveStateTxRangeAvailable(blockNum uint64) (bool, error) {
	if _, ok, err := snapshots.StateDomainHistoryTxRangeForBlock(b.chain.buffer, blockNum); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}
	if cold, ok := b.stateColdHistory.(state.StateDomainChangeColdTxRange); ok {
		_, ok, err := cold.StateTxRangeForBlock(blockNum)
		return ok, err
	}
	return false, nil
}

func (b *TronBackend) archiveStateTxNumAtBlockEnd(blockNum uint64) (uint64, error) {
	if row, ok, err := snapshots.StateDomainHistoryTxRangeForBlock(b.chain.buffer, blockNum); err != nil {
		return 0, err
	} else if ok {
		return row.EndTxNum, nil
	}
	if cold, ok := b.stateColdHistory.(state.StateDomainChangeColdTxRange); ok {
		row, ok, err := cold.StateTxRangeForBlock(blockNum)
		if err != nil {
			return 0, err
		}
		if ok {
			return row.EndTxNum, nil
		}
	}
	return blockNum, nil
}

// GetBalanceAt returns addr's TRX balance (in SUN) as it stood at the end of
// blockNum, reconstructed via flat temporal domain history. blockNum >= head
// reads live state; an older block on a non-archive node returns
// ErrArchiveHistoryDisabled. A non-existent account at that height returns
// (0, nil) — matching the live GetBalance "no account ⇒ 0" convention.
func (b *TronBackend) GetBalanceAt(addr tcommon.Address, blockNum uint64) (int64, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return 0, err
	}
	defer session.Close()
	acc, err := session.reader.AccountAt(addr, blockNum)
	if err != nil {
		return 0, err
	}
	if acc == nil {
		return 0, nil
	}
	return acc.Balance(), nil
}

// GetCodeAt returns addr's contract bytecode as of the end of blockNum.
// Same gating as GetBalanceAt. Returns (nil, nil) for an account that had
// no code (or did not exist) at that height.
func (b *TronBackend) GetCodeAt(addr tcommon.Address, blockNum uint64) ([]byte, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	return session.reader.CodeAt(addr, blockNum)
}

// GetStorageAtBlock returns the value of (addr, slot) as of the end of
// blockNum. Same gating as GetBalanceAt. Returns the zero hash for an empty
// slot or a non-existent account at that height. Named GetStorageAtBlock
// (not GetStorageAt) so it doesn't collide with the live single-arg reader.
func (b *TronBackend) GetStorageAtBlock(addr tcommon.Address, slot tcommon.Hash, blockNum uint64) (tcommon.Hash, error) {
	session, err := b.archiveStateAt(blockNum)
	if err != nil {
		return tcommon.Hash{}, err
	}
	defer session.Close()
	return session.reader.StorageAt(addr, slot, blockNum)
}

func (b *TronBackend) GetTransactionByHash(hash tcommon.Hash) (*corepb.Transaction, *types.Block, int, error) {
	blockNum, ok, err := rawdb.ReadTransactionIndexStrict(b.chain.chaindb, hash[:])
	if err != nil {
		return nil, nil, 0, err
	}
	if !ok {
		return nil, nil, 0, nil // not found
	}
	block, hasBlock, err := rawdb.ReadBlockStrict(b.chain.chaindb, blockNum)
	if err != nil {
		return nil, nil, 0, err
	}
	if !hasBlock {
		return nil, nil, 0, fmt.Errorf("block body missing for indexed transaction %x at block %d", hash, blockNum)
	}
	for i, tx := range block.Transactions() {
		if tx.Hash() == hash {
			return tx.Proto(), block, i, nil
		}
	}
	return nil, nil, 0, fmt.Errorf("transaction not found in block %d", blockNum)
}

func (b *TronBackend) GetTransactionInfo(hash tcommon.Hash) (*corepb.TransactionInfo, error) {
	info, ok, err := rawdb.ReadTransactionInfoStrict(b.chain.chaindb, hash[:])
	if err != nil || !ok {
		return nil, err
	}
	if head := b.chain.CurrentBlock(); head != nil && uint64(info.BlockNumber) > head.Number() {
		return nil, nil
	}
	return info, nil // nil info = not found (not an error)
}

func (b *TronBackend) EstimateGas(from, to *tcommon.Address, data []byte, value int64) (uint64, error) {
	if to != nil && len(data) == 0 {
		return 0, nil // plain TRX transfer costs no energy
	}
	fromAddr := tcommon.Address{}
	if from != nil {
		fromAddr = *from
	}
	if to == nil {
		return 0, fmt.Errorf("eth_estimateGas: 'to' required for contract call")
	}
	result, err := b.TriggerConstantContract(fromAddr, *to, data, 30_000_000)
	if err != nil {
		return 0, err
	}
	return uint64(result.EnergyUsed), nil
}

func (b *TronBackend) EstimateGasAt(from, to *tcommon.Address, data []byte, value int64, blockNum uint64) (uint64, error) {
	fromAddr := tcommon.Address{}
	if from != nil {
		fromAddr = *from
	}
	if to == nil {
		return 0, fmt.Errorf("eth_estimateGas: 'to' required for contract call")
	}
	if len(data) == 0 {
		session, err := b.archiveStateAt(blockNum)
		if err != nil {
			return 0, err
		}
		session.Close()
		return 0, nil // plain TRX transfer costs no energy
	}
	result, err := b.TriggerConstantContractAt(fromAddr, *to, data, 30_000_000, blockNum)
	if err != nil {
		return 0, err
	}
	return uint64(result.EnergyUsed), nil
}

func (b *TronBackend) Call(from, to *tcommon.Address, data []byte, value int64) ([]byte, error) {
	fromAddr := tcommon.Address{}
	if from != nil {
		fromAddr = *from
	}
	if to == nil {
		return nil, fmt.Errorf("eth_call: 'to' address is required")
	}
	result, err := b.TriggerConstantContract(fromAddr, *to, data, 30_000_000)
	if err != nil {
		return nil, err
	}
	return result.Result, nil
}

func (b *TronBackend) CallAt(from, to *tcommon.Address, data []byte, value int64, blockNum uint64) ([]byte, error) {
	fromAddr := tcommon.Address{}
	if from != nil {
		fromAddr = *from
	}
	if to == nil {
		return nil, fmt.Errorf("eth_call: 'to' address is required")
	}
	result, err := b.TriggerConstantContractAt(fromAddr, *to, data, 30_000_000, blockNum)
	if err != nil {
		return nil, err
	}
	return result.Result, nil
}

func (b *TronBackend) GetLogs(filter jsonrpc.LogFilter) ([]*jsonrpc.RPCLog, error) {
	const maxBlockRange = 2000

	var fromBlock, toBlock uint64

	if filter.BlockHash != nil {
		// Single-block mode
		block, err := b.GetBlockByHash(*filter.BlockHash)
		if err != nil {
			if strings.Contains(err.Error(), "block not found") {
				return []*jsonrpc.RPCLog{}, nil
			}
			return nil, err
		}
		if block == nil {
			return []*jsonrpc.RPCLog{}, nil
		}
		fromBlock = block.Number()
		toBlock = block.Number()
	} else {
		current := b.chain.CurrentBlock().Number()
		fromBlock = 0
		if filter.FromBlock != nil {
			fromBlock = *filter.FromBlock
		}
		toBlock = current
		if filter.ToBlock != nil {
			toBlock = *filter.ToBlock
		}
		if toBlock > current {
			toBlock = current
		}
		if toBlock < fromBlock {
			return []*jsonrpc.RPCLog{}, nil
		}
		if toBlock-fromBlock+1 > maxBlockRange {
			return nil, fmt.Errorf("block range too large (max %d)", maxBlockRange)
		}
	}

	if logs, ok, err := b.getLogsFromColdEventLogs(fromBlock, toBlock, filter); err != nil {
		return nil, err
	} else if ok {
		return logs, nil
	}

	var logs []*jsonrpc.RPCLog
	bloomMatcher := newSectionBloomLogMatcher(b.chain.chaindb, filter)

	for num := fromBlock; num <= toBlock; num++ {
		if bloomMatcher != nil {
			mayContain, err := bloomMatcher.mayContain(num)
			if err != nil {
				return nil, err
			}
			if !mayContain {
				continue
			}
		}
		block, hasBlock, err := rawdb.ReadBlockStrict(b.chain.chaindb, num)
		if err != nil {
			return nil, err
		}
		if !hasBlock {
			continue
		}
		blockHash := block.Hash()
		infos, hasInfos, err := rawdb.ReadTransactionInfosByBlockStrict(b.chain.chaindb, num)
		if err != nil {
			return nil, err
		}
		if !hasInfos {
			continue
		}
		if err := rawdb.ValidateTransactionInfosForBlock(num, block.Transactions(), infos, "log query"); err != nil {
			if errors.Is(err, rawdb.ErrIncompleteTransactionInfoCoverage) {
				continue
			}
			return nil, err
		}

		logIndex := uint64(0)

		for txIdx, info := range infos {
			for _, l := range info.Log {
				thisIndex := logIndex
				logIndex++

				// Address filter
				if len(filter.Addresses) > 0 {
					addr := logAddressFromRaw(l.Address)
					match := false
					for _, fa := range filter.Addresses {
						if fa == addr {
							match = true
							break
						}
					}
					if !match {
						continue
					}
				}

				// Topics filter
				if !matchTopics(filter.Topics, l.Topics) {
					continue
				}

				topics := make([]string, len(l.Topics))
				for i, t := range l.Topics {
					topics[i] = fmt.Sprintf("0x%064x", t)
				}

				// Recover the txHash from block transactions at txIdx
				txHash := tcommon.Hash{}
				txs := block.Transactions()
				if txIdx < len(txs) {
					txHash = txs[txIdx].Hash()
				}

				addrStart := 0
				if len(l.Address) > 20 {
					addrStart = len(l.Address) - 20
				}
				address := fmt.Sprintf("0x%x", l.Address[addrStart:])

				logs = append(logs, &jsonrpc.RPCLog{
					Address:          address,
					Topics:           topics,
					Data:             fmt.Sprintf("0x%x", l.Data),
					BlockNumber:      fmt.Sprintf("0x%x", num),
					TransactionHash:  fmt.Sprintf("0x%x", txHash),
					TransactionIndex: fmt.Sprintf("0x%x", txIdx),
					BlockHash:        fmt.Sprintf("0x%x", blockHash),
					LogIndex:         fmt.Sprintf("0x%x", thisIndex),
					Removed:          false,
				})
			}
		}
	}

	if logs == nil {
		logs = []*jsonrpc.RPCLog{}
	}
	return logs, nil
}

func (b *TronBackend) getLogsFromColdEventLogs(fromBlock, toBlock uint64, filter jsonrpc.LogFilter) ([]*jsonrpc.RPCLog, bool, error) {
	db := b.chain.chaindb
	coldFilter := rawdb.EventLogFilter{
		Addresses: filter.Addresses,
		Topics:    filter.Topics,
	}
	logs := make([]*jsonrpc.RPCLog, 0)
	covered, err := db.IterateCoveredEventLogs(fromBlock, toBlock, coldFilter, func(row rawdb.EventLog) (bool, error) {
		if row.BlockNum < fromBlock || row.BlockNum > toBlock {
			return true, nil
		}
		if filter.BlockHash != nil && row.BlockHash != *filter.BlockHash {
			return true, nil
		}
		if row.Log == nil {
			return false, fmt.Errorf("cold event log row block=%d tx=%d log=%d is nil", row.BlockNum, row.TxIndex, row.LogIndex)
		}
		if !coldEventLogPayloadMatchesFilter(row.Log, filter) {
			return true, nil
		}
		logs = append(logs, rpcLogFromColdEventLog(row))
		return true, nil
	})
	if err != nil {
		return nil, true, err
	}
	if !covered {
		return nil, false, nil
	}
	return logs, true, nil
}

func coldEventLogPayloadMatchesFilter(log *corepb.TransactionInfo_Log, filter jsonrpc.LogFilter) bool {
	if len(filter.Addresses) > 0 {
		addr := logAddressFromRaw(log.GetAddress())
		matched := false
		for _, candidate := range filter.Addresses {
			if candidate == addr {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return matchTopics(filter.Topics, log.GetTopics())
}

func rpcLogFromColdEventLog(row rawdb.EventLog) *jsonrpc.RPCLog {
	log := row.Log
	return &jsonrpc.RPCLog{
		Address:          rpcLogAddress(log.GetAddress()),
		Topics:           rpcLogTopics(log.GetTopics()),
		Data:             fmt.Sprintf("0x%x", log.GetData()),
		BlockNumber:      fmt.Sprintf("0x%x", row.BlockNum),
		TransactionHash:  fmt.Sprintf("0x%x", row.TxHash),
		TransactionIndex: fmt.Sprintf("0x%x", row.TxIndex),
		BlockHash:        fmt.Sprintf("0x%x", row.BlockHash),
		LogIndex:         fmt.Sprintf("0x%x", row.LogIndex),
		Removed:          false,
	}
}

func rpcLogAddress(raw []byte) string {
	addrStart := 0
	if len(raw) > 20 {
		addrStart = len(raw) - 20
	}
	return fmt.Sprintf("0x%x", raw[addrStart:])
}

func logAddressFromRaw(raw []byte) tcommon.Address {
	if len(raw) > tcommon.AddressLength {
		raw = raw[len(raw)-tcommon.AddressLength:]
	}
	return tcommon.BytesToAddress(raw)
}

func rpcLogTopics(rawTopics [][]byte) []string {
	topics := make([]string, len(rawTopics))
	for i, topic := range rawTopics {
		topics[i] = fmt.Sprintf("0x%064x", topic)
	}
	return topics
}

// matchTopics returns true if the log topics match the filter topics.
// filter.Topics[i] == nil means any value is accepted at position i.
// filter.Topics[i] with multiple hashes means OR match.
func matchTopics(filterTopics [][]tcommon.Hash, logTopics [][]byte) bool {
	for i, required := range filterTopics {
		if len(required) == 0 {
			continue // nil / empty = any
		}
		if i >= len(logTopics) {
			return false
		}
		var logTopic tcommon.Hash
		copy(logTopic[:], logTopics[i])
		matched := false
		for _, h := range required {
			if h == logTopic {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

type sectionBloomLogMatcher struct {
	db     *rawdb.ChainDB
	groups [][][3]uint64
	cache  map[sectionBloomCacheKey]sectionBloomCacheRow
}

type sectionBloomCacheKey struct {
	section  uint64
	bitIndex uint64
}

type sectionBloomCacheRow struct {
	bitset []byte
	ok     bool
	err    error
}

func newSectionBloomLogMatcher(db *rawdb.ChainDB, filter jsonrpc.LogFilter) *sectionBloomLogMatcher {
	if db == nil {
		return nil
	}
	groups := make([][][3]uint64, 0, 1+len(filter.Topics))
	if len(filter.Addresses) != 0 {
		group := make([][3]uint64, 0, len(filter.Addresses))
		for _, addr := range filter.Addresses {
			addrBytes := addr.Bytes()
			if len(addrBytes) > 20 {
				addrBytes = addrBytes[len(addrBytes)-20:]
			}
			group = append(group, rawdb.SectionBloomBitIndexes(addrBytes))
		}
		groups = append(groups, group)
	}
	for _, requiredTopics := range filter.Topics {
		if len(requiredTopics) == 0 {
			continue
		}
		group := make([][3]uint64, 0, len(requiredTopics))
		for _, topic := range requiredTopics {
			group = append(group, rawdb.SectionBloomBitIndexes(topic[:]))
		}
		groups = append(groups, group)
	}
	if len(groups) == 0 {
		return nil
	}
	return &sectionBloomLogMatcher{
		db:     db,
		groups: groups,
		cache:  make(map[sectionBloomCacheKey]sectionBloomCacheRow),
	}
}

func (m *sectionBloomLogMatcher) mayContain(blockNum uint64) (bool, error) {
	section := blockNum / rawdb.SectionBloomBlockPerSection
	blockOffset := blockNum % rawdb.SectionBloomBlockPerSection
	for _, group := range m.groups {
		groupMayMatch := false
		groupUnknown := false
		for _, itemBits := range group {
			match, known, err := m.itemMayContain(section, blockOffset, itemBits)
			if err != nil {
				return false, err
			}
			if !known {
				groupUnknown = true
				continue
			}
			if match {
				groupMayMatch = true
				break
			}
		}
		if groupMayMatch || groupUnknown {
			continue
		}
		return false, nil
	}
	return true, nil
}

func (m *sectionBloomLogMatcher) itemMayContain(section, blockOffset uint64, bitIndexes [3]uint64) (match bool, known bool, err error) {
	for _, bitIndex := range bitIndexes {
		row := m.read(section, bitIndex)
		if row.err != nil {
			return false, false, row.err
		}
		if !row.ok {
			return true, false, nil
		}
		if !rawdb.SectionBloomBitSetHas(row.bitset, blockOffset) {
			return false, true, nil
		}
	}
	return true, true, nil
}

func (m *sectionBloomLogMatcher) read(section, bitIndex uint64) sectionBloomCacheRow {
	key := sectionBloomCacheKey{section: section, bitIndex: bitIndex}
	if row, ok := m.cache[key]; ok {
		return row
	}
	bitset, ok, err := rawdb.ReadSectionBloomBitSetStrict(m.db, section, bitIndex)
	row := sectionBloomCacheRow{bitset: bitset, ok: ok, err: err}
	m.cache[key] = row
	return row
}

// ValidateTransaction validates a transaction's contract logic against current state.
// Mirrors java-tron Wallet#broadcastTransaction's synchronous validation step.
// Returns nil if valid, nil for unsupported contract types (to allow broadcast),
// or a human-readable error describing the validation failure.
func (b *TronBackend) ValidateTransaction(tx *types.Transaction) error {
	act, err := actuator.CreateActuator(tx)
	if err != nil {
		// Unsupported contract type — skip validation, allow broadcast.
		return nil
	}
	if tx.ContractType() == corepb.Transaction_Contract_ShieldedTransferContract && !zksnark.Available() {
		return fmt.Errorf("shielded merkle tree backend unavailable: %w", zksnark.ErrPedersenUnimplemented)
	}

	head := b.chain.CurrentBlock()
	statedb, err := b.chain.openCurrentState()
	if err != nil {
		return fmt.Errorf("open head state: %w", err)
	}

	validationBuf := blockbuffer.New(b.chain.buffer)
	validationBuf.BeginBlock(tcommon.Hash{}, 0) // sentinel; validation layer, never committed
	defer validationBuf.DiscardActive()

	// statedb is opened at the head root; reuse it as the system-KV reader so
	// rooted dynprops match the state the tx is simulated against.
	dynProps := state.LoadDynamicProperties(b.chain.buffer, statedb)

	// Hydrate witnesses into statedb, matching InsertBlock's pre-processing
	// step. Witness index and capsules are rooted at the head state.
	witnessAddrs := statedb.ReadWitnessIndex()
	for _, addr := range witnessAddrs {
		_ = statedb.GetWitness(addr)
	}

	ctx := &actuator.Context{
		State:                      statedb,
		DynProps:                   dynProps,
		Tx:                         tx,
		BlockTime:                  head.Timestamp(),
		BlockNumber:                head.Number(),
		EnergyLimitForkBlockNum:    b.chain.Config().EnergyLimitForkBlockNum(),
		HasEnergyLimitForkBlockNum: true,
		DB:                         b.chain.vmKV(validationBuf),
		ActiveWitnesses:            b.chain.ActiveWitnesses(),
	}

	return act.Validate(ctx)
}
