package core

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/core/zksnark"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/proto"
)

// TxBroadcaster announces new transactions to P2P peers.
// Implemented by net.BroadcastService; defined here to avoid an import cycle.
type TxBroadcaster interface {
	BroadcastTx(tx *types.Transaction)
}

// TronBackend implements tronapi.Backend.
type TronBackend struct {
	chain       *BlockChain
	pool        *txpool.TxPool
	txBroadcast TxBroadcaster              // nil until wired from main
	peersFunc   func() []*tronapi.PeerInfo // nil until wired from main

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
	block := b.chain.GetBlockByNumber(number)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", number)
	}
	return block, nil
}

func (b *TronBackend) GetAccount(addr tcommon.Address) (*types.Account, error) {
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	acc := statedb.GetAccount(addr)
	if acc == nil {
		return nil, fmt.Errorf("account not found")
	}
	return acc, nil
}

// GetAccountAt returns the account as of the post-apply state of `blockNum`.
//
// Fast path (unchanged): when the block's committed state root is still on
// disk — always true for the recent solid / PBFT-confirmed heights the
// /walletsolidity/ and /walletpbft/ variants query — it opens a StateDB at
// that root. This keeps those responses isolated from live state.
//
// Archive fallback (slice 7): when the root is absent (pruned by full-mode
// gcmode, but the block is older than head) it reconstructs the account from
// the State History Index instead of erroring — this is the TRON-flavored
// equivalent of java-tron's archive /walletsolidity/getaccount serving any
// past block. The fallback requires the node to have captured history; on a
// non-archive node it returns ErrArchiveHistoryDisabled rather than a
// generic "no state root" error so operators get an actionable message.
//
// Both paths return a "not found" error for an address that didn't exist at
// that height, preserving the existing handler contract (which renders that
// as an empty `{}` body).
func (b *TronBackend) GetAccountAt(addr tcommon.Address, blockNum uint64) (*types.Account, error) {
	// Reject blocks past head up front. A future block has no committed state
	// root, so it would fall into the history-reader branch below — where
	// requireArchive and the reader both treat blockNum >= headNum as "serve
	// live" and would silently return the head account for a block that does
	// not exist yet. Pre-slice-7 this errored; preserve that contract.
	if head := b.chain.CurrentBlock(); head != nil && blockNum > head.Number() {
		return nil, fmt.Errorf("block %d is beyond current head %d", blockNum, head.Number())
	}
	root := b.chain.StateRootAtBlock(blockNum)
	if root == (tcommon.Hash{}) {
		// State root pruned (or never written). Reconstruct via history if
		// the node is an archive node; otherwise the answer is unrecoverable.
		reader, headNum, err := b.historyReaderAt()
		if err != nil {
			return nil, err
		}
		if err := b.requireArchive(blockNum, headNum); err != nil {
			return nil, err
		}
		acc, err := reader.AccountAt(addr, blockNum)
		if err != nil {
			return nil, fmt.Errorf("reconstruct account at block %d: %w", blockNum, err)
		}
		if acc == nil {
			return nil, fmt.Errorf("account not found at block %d", blockNum)
		}
		return acc, nil
	}
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state at block %d: %w", blockNum, err)
	}
	acc := statedb.GetAccount(addr)
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
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	sc := statedb.GetContract(addr)
	if sc == nil {
		return nil, fmt.Errorf("contract not found")
	}
	return sc, nil
}

func (b *TronBackend) TriggerConstantContract(owner, contractAddr tcommon.Address, data []byte, energyLimit int64) (*tronapi.TriggerResult, error) {
	current := b.chain.CurrentBlock()
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}

	// Use a copy of state so read-only calls don't pollute
	statedbCopy, err := statedb.Copy()
	if err != nil {
		return nil, fmt.Errorf("copy state: %w", err)
	}

	if energyLimit <= 0 {
		energyLimit = 30_000_000 // default max energy for constant calls
	}

	dp := b.chain.DynProps()
	cfg := vm.NewTVMConfig(current.Number(), dp)
	cfg.MultiSigCheckV2 = forks.PassVersionFromStore(statedbCopy, 27,
		dp.LatestBlockHeaderTimestamp(), dp.MaintenanceTimeInterval())
	evm := vm.NewTVM(statedbCopy, dp, owner, current.Number(), current.Timestamp(), tcommon.Address{}, 1, cfg)

	ret, energyLeft, vmErr := evm.Call(owner, contractAddr, data, uint64(energyLimit), 0)
	energyUsed := energyLimit - int64(energyLeft)

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

func (b *TronBackend) GetTransactionByID(txHash tcommon.Hash) (*corepb.Transaction, error) {
	blockNum := rawdb.ReadTransactionIndex(b.chain.chaindb, txHash[:])
	if blockNum == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	block := b.chain.GetBlockByNumber(*blockNum)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", *blockNum)
	}
	for _, tx := range block.Transactions() {
		if tx.Hash() == txHash {
			return tx.Proto(), nil
		}
	}
	return nil, fmt.Errorf("transaction not found in block %d", *blockNum)
}

func (b *TronBackend) GetTransactionInfoByID(txHash tcommon.Hash) (*corepb.TransactionInfo, error) {
	info := rawdb.ReadTransactionInfo(b.chain.chaindb, txHash[:])
	if info == nil {
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
	infos := rawdb.ReadTransactionInfosByBlock(b.chain.chaindb, blockNum)
	return infos, nil
}

func (b *TronBackend) GetBlockByHash(hash tcommon.Hash) (*types.Block, error) {
	// Try direct hash lookup first
	block := b.chain.GetBlockByHash(hash)
	if block != nil {
		return block, nil
	}
	// The input may be a blockID (first 8 bytes = block number, rest = hash[8:]).
	// Extract the block number and look up by number, then verify the ID matches.
	num := binary.BigEndian.Uint64(hash[:8])
	if num > 0 {
		block = b.chain.GetBlockByNumber(num)
		if block != nil && block.ID().Hash == hash {
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
		block := b.chain.GetBlockByNumber(i)
		if block == nil {
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

func (b *TronBackend) GetAccountResource(addr tcommon.Address) (*tronapi.AccountResource, error) {
	return b.accountResourceAtRoot(addr, b.chain.HeadStateRoot())
}

// GetAccountResourceAt opens state at the post-apply root of the bound
// block (solid or PBFT-confirmed) and returns the snapshot from there.
func (b *TronBackend) GetAccountResourceAt(addr tcommon.Address, blockNum uint64) (*tronapi.AccountResource, error) {
	root := b.chain.StateRootAtBlock(blockNum)
	if root == (tcommon.Hash{}) {
		return nil, fmt.Errorf("no state root for block %d", blockNum)
	}
	return b.accountResourceAtRoot(addr, root)
}

func (b *TronBackend) accountResourceAtRoot(addr tcommon.Address, root tcommon.Hash) (*tronapi.AccountResource, error) {
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	// Read rooted dynprops at the SAME root as the account state so resource
	// limits stay consistent with the queried (live or solid) head.
	dynProps := state.LoadDynamicProperties(b.chain.db, statedb)
	return &tronapi.AccountResource{
		FreeNetUsed:      statedb.GetFreeNetUsage(addr),
		FreeNetLimit:     dynProps.FreeNetLimit(),
		NetUsed:          statedb.GetNetUsage(addr),
		TotalNetLimit:    dynProps.TotalNetLimit(),
		TotalEnergyLimit: dynProps.TotalEnergyCurrentLimit(),
	}, nil
}

func (b *TronBackend) GetChainParameters() []tronapi.ChainParameter {
	dynProps := b.chain.DynProps()
	all := dynProps.All()
	params := make([]tronapi.ChainParameter, 0, len(all))
	for k, v := range all {
		params = append(params, tronapi.ChainParameter{Key: k, Value: v})
	}
	return params
}

func (b *TronBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) {
	statedb := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if statedb == nil {
		return nil, nil
	}
	witnessAddrs := statedb.ReadWitnessIndex()
	pendingDeltas, _ := pendingVoteDeltas(statedb)
	activeSet := b.chain.ActiveWitnesses()
	activeMap := make(map[tcommon.Address]bool, len(activeSet))
	for _, a := range activeSet {
		activeMap[a] = true
	}

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

func (b *TronBackend) NextMaintenanceTime() int64 {
	return b.chain.NextMaintenanceTime()
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
	var result []*tronapi.ProposalInfo
	for _, id := range ids {
		p := sysKV.ReadProposal(id)
		if p == nil {
			continue
		}
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
		result = append(result, &tronapi.ProposalInfo{
			ProposalID:      p.ID,
			ProposerAddress: hex.EncodeToString(p.Proposer[:]),
			Parameters:      params,
			ExpirationTime:  p.ExpirationTime,
			CreateTime:      p.CreateTime,
			Approvals:       approvals,
			State:           stateStr,
		})
	}
	return result, nil
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
	statedb, err := state.New(b.chain.HeadStateRoot(), b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
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
	statedb, err := state.New(b.chain.HeadStateRoot(), b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	receivers := statedb.ReadDelegationIndex(addr)
	toAddresses := make([]string, len(receivers))
	for i, r := range receivers {
		toAddresses[i] = hex.EncodeToString(r[:])
	}
	return &tronapi.DelegationIndexInfo{
		Account:     hex.EncodeToString(addr[:]),
		ToAddresses: toAddresses,
	}, nil
}

func (b *TronBackend) CanDelegateResource(addr tcommon.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	maxSize := statedb.GetFrozenV2Amount(addr, resource)

	// Compute already-delegated amount from the delegation index.
	var delegated int64
	for _, receiver := range statedb.ReadDelegationIndex(addr) {
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

	canDelegate := maxSize - delegated
	if canDelegate < 0 {
		canDelegate = 0
	}
	return &tronapi.CanDelegateInfo{
		MaxSize:         maxSize,
		CanDelegateSize: canDelegate,
		Balance:         amount,
	}, nil
}

func (b *TronBackend) GetCanWithdrawUnfreezeAmount(addr tcommon.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	acc := statedb.GetAccount(addr)
	if acc == nil {
		return &tronapi.CanWithdrawUnfreezeInfo{Amount: 0}, nil
	}
	var total int64
	for _, u := range acc.UnfrozenV2() {
		if u.UnfreezeExpireTime <= timestamp {
			total += u.UnfreezeAmount
		}
	}
	return &tronapi.CanWithdrawUnfreezeInfo{Amount: total}, nil
}

func (b *TronBackend) GetAvailableUnfreezeCount(addr tcommon.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	const maxUnfreezeSlots = 32
	count := int64(maxUnfreezeSlots)
	if acc := statedb.GetAccount(addr); acc != nil {
		count = int64(maxUnfreezeSlots - len(acc.UnfrozenV2()))
	}
	if count < 0 {
		count = 0
	}
	return &tronapi.AvailableUnfreezeCountInfo{Count: count}, nil
}

func (b *TronBackend) GetReward(addr tcommon.Address) (*tronapi.RewardInfo, error) {
	return b.rewardAtRoot(addr, b.chain.HeadStateRoot())
}

// GetRewardAt opens state at the bound block's root for the /walletsolidity/
// and /walletpbft/ variants.
func (b *TronBackend) GetRewardAt(addr tcommon.Address, blockNum uint64) (*tronapi.RewardInfo, error) {
	root := b.chain.StateRootAtBlock(blockNum)
	if root == (tcommon.Hash{}) {
		return nil, fmt.Errorf("no state root for block %d", blockNum)
	}
	return b.rewardAtRoot(addr, root)
}

func (b *TronBackend) rewardAtRoot(addr tcommon.Address, root tcommon.Hash) (*tronapi.RewardInfo, error) {
	statedb, err := state.New(root, b.chain.StateDB())
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

func (b *TronBackend) GetAssetIssueList() []*contractpb.AssetIssueContract {
	return b.listAssetsAtHead()
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

func (b *TronBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	return sysKV.ReadMarketPriceList(sellTokenID, buyTokenID)
}

// listExchangesAtHead enumerates the rooted exchange set (Phase 3d) at the head
// state root, walking ids 1..latest_exchange_num as RpcApiService.getExchangeList
// does off getLatestExchangeNum. This is a behavior-preserving swap of the prior
// flat ListAllExchanges(exchangePrefix): it returns the V1 bucket unconditionally.
//
// NOTE (java-parity, deferred): java-tron's getExchangeList selects the bucket
// via Commons.getExchangeStoreFinal — V1 pre-AllowSameTokenName, V2 after — so
// post-fork it returns the live V2 set, whereas this (like the code it replaces)
// always reads V1. That divergence predates this refactor and is left untouched
// here to keep the migration a pure storage move; it should be fixed separately.
func (b *TronBackend) listExchangesAtHead() []*corepb.Exchange {
	sysKV := b.chain.sysKVAt(b.chain.HeadStateRoot())
	if sysKV == nil {
		return nil
	}
	// latest_exchange_num is read from the cached DynProps, which tracks the same
	// head this opens sysKV at; both are head-only, so they stay in sync.
	return sysKV.ListExchanges(b.chain.DynProps().LatestExchangeNum())
}

func (b *TronBackend) ListExchanges() ([]*corepb.Exchange, error) {
	return b.listExchangesAtHead(), nil
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

func (b *TronBackend) TotalTransaction() int64 {
	// Read through the buffer overlay so the counter reflects the latest
	// applied block before the async flush worker has drained it to disk.
	return rawdb.ReadTotalTransactionCount(b.chain.BufferedDB())
}

func (b *TronBackend) GetBurnTrx() int64 {
	return b.chain.DynProps().BurnTrxAmount()
}

func (b *TronBackend) GetBandwidthPrices() string {
	return b.chain.DynProps().BandwidthPriceHistory()
}

func (b *TronBackend) GetEnergyPrices() string {
	return b.chain.DynProps().EnergyPriceHistory()
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
	addrBytes := sysKV.ReadAccountIdIndex(accountID)
	if addrBytes == nil {
		return nil, fmt.Errorf("account not found")
	}
	var addr tcommon.Address
	copy(addr[:], addrBytes)
	return b.GetAccount(addr)
}

func (b *TronBackend) GetAccountNet(addr tcommon.Address) (*apipb.AccountNetMessage, error) {
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	acc := statedb.GetAccount(addr)
	if acc == nil {
		return nil, nil
	}
	// Read rooted dynprops at the head root (same statedb) for a consistent
	// net-limit computation.
	dynProps := state.LoadDynamicProperties(b.chain.db, statedb)
	frozenBW := statedb.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH)
	var netLimit int64
	if total := dynProps.TotalNetWeight(); total > 0 {
		netLimit = frozenBW * dynProps.TotalNetLimit() / total
	}
	return &apipb.AccountNetMessage{
		FreeNetUsed:    statedb.GetFreeNetUsage(addr),
		FreeNetLimit:   dynProps.FreeNetLimit(),
		NetUsed:        statedb.GetNetUsage(addr),
		NetLimit:       netLimit,
		TotalNetLimit:  dynProps.TotalNetLimit(),
		TotalNetWeight: dynProps.TotalNetWeight(),
	}, nil
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
	}, nil
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
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return 0
	}
	return statedb.GetBalance(addr)
}

func (b *TronBackend) GetCode(addr tcommon.Address) []byte {
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil
	}
	return statedb.GetCode(addr)
}

func (b *TronBackend) GetStorageAt(addr tcommon.Address, slot tcommon.Hash) tcommon.Hash {
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return tcommon.Hash{}
	}
	return statedb.GetState(addr, slot)
}

// ErrArchiveHistoryDisabled is returned by the *At archive-query methods
// when the caller asks for a historical block (blockNum < head) on a node
// that wasn't synced with --history.enabled. Such a node has no sh-* rows
// on disk, so the historical answer is unrecoverable; rather than silently
// returning the live value (which would be wrong for any block < head) the
// backend surfaces this clear error. Queries AT head are still served from
// live state and never hit this path.
var ErrArchiveHistoryDisabled = fmt.Errorf("archive history not available: node not running with --history.enabled")

// ErrArchiveHistoryPruned is returned when a historical query asks for a block
// below the local State History Index retention floor. In full mode the pruner
// deletes sh-* rows below HistoryConfig.FirstBlock, so reconstructing those
// heights would silently skip required rollback deltas and return an
// unverifiable state.
var ErrArchiveHistoryPruned = fmt.Errorf("archive history pruned for requested block")

// historyReaderAt builds a single-use PersistentHistoryReader for one
// archive query and reports the chain head number it was constructed
// against. The reader walks history rows newest-first from head down to the
// requested block, so its `db` and `live` baseline must agree on "head":
//
//   - db = b.chain.buffer — the buffer overlay sees sh-* rows for blocks
//     applied but not yet flushed to disk (head can lead the flushed/
//     solidified boundary by ~19 blocks on mainnet DPoS), matching the
//     fork-rewind-safe reader the slice-4 tests exercise.
//   - live = StateDB opened at the head's committed state root — the MPT
//     account view the reader rolls deltas back from, the same baseline the
//     live GetBalance/GetCode reads use.
//
// headNum and the live root are tied to a single head number rather than
// read independently. Calling CurrentBlock().Number() and HeadStateRoot()
// separately lets a concurrent InsertBlock advance the head between the two
// loads, pairing headNum=N with the root of block N+1; because the reader
// only rolls deltas back to headNum, that newer base would leave block
// N+1's writes un-rolled-back and corrupt an older-block answer.
// StateRootAtBlock(headNum) resolves the root for that exact number, so the
// baseline and the threshold can no longer skew.
//
// The caller is responsible for the HistoryEnabled gate (see
// requireArchive); this helper only assembles the reader.
func (b *TronBackend) historyReaderAt() (*state.PersistentHistoryReader, uint64, error) {
	headNum := b.chain.CurrentBlock().Number()
	root := b.chain.StateRootAtBlock(headNum)
	live, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, 0, fmt.Errorf("open head state: %w", err)
	}
	return state.NewPersistentHistoryReader(b.chain.buffer, live, headNum), headNum, nil
}

// requireArchive enforces the block range and HistoryEnabled gates for a query
// bound to blockNum. A query at head is served from live state. A query past
// head has no committed state and must fail instead of silently returning head.
// A query for a strictly-older block requires the node to have been capturing
// history; otherwise it returns ErrArchiveHistoryDisabled.
func (b *TronBackend) requireArchive(blockNum, headNum uint64) error {
	if blockNum > headNum {
		return fmt.Errorf("block %d is beyond current head %d", blockNum, headNum)
	}
	if blockNum == headNum {
		return nil
	}
	if !b.chain.Config().HistoryEnabled {
		return ErrArchiveHistoryDisabled
	}
	cfg, err := rawdb.ReadHistoryConfig(b.chain.buffer)
	if err == nil {
		if cfg.FirstBlock > 0 && blockNum < cfg.FirstBlock {
			return fmt.Errorf("%w: requested=%d first_available=%d",
				ErrArchiveHistoryPruned, blockNum, cfg.FirstBlock)
		}
		return nil
	}
	if !errors.Is(err, rawdb.ErrHistoryConfigAbsent) {
		return fmt.Errorf("read history config: %w", err)
	}
	return nil
}

// GetBalanceAt returns addr's TRX balance (in SUN) as it stood at the end of
// blockNum, reconstructed via the State History Index. blockNum >= head
// reads live state; an older block on a non-archive node returns
// ErrArchiveHistoryDisabled. A non-existent account at that height returns
// (0, nil) — matching the live GetBalance "no account ⇒ 0" convention.
func (b *TronBackend) GetBalanceAt(addr tcommon.Address, blockNum uint64) (int64, error) {
	reader, headNum, err := b.historyReaderAt()
	if err != nil {
		return 0, err
	}
	if err := b.requireArchive(blockNum, headNum); err != nil {
		return 0, err
	}
	acc, err := reader.AccountAt(addr, blockNum)
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
	reader, headNum, err := b.historyReaderAt()
	if err != nil {
		return nil, err
	}
	if err := b.requireArchive(blockNum, headNum); err != nil {
		return nil, err
	}
	return reader.CodeAt(addr, blockNum)
}

// GetStorageAtBlock returns the value of (addr, slot) as of the end of
// blockNum. Same gating as GetBalanceAt. Returns the zero hash for an empty
// slot or a non-existent account at that height. Named GetStorageAtBlock
// (not GetStorageAt) so it doesn't collide with the live single-arg reader.
func (b *TronBackend) GetStorageAtBlock(addr tcommon.Address, slot tcommon.Hash, blockNum uint64) (tcommon.Hash, error) {
	reader, headNum, err := b.historyReaderAt()
	if err != nil {
		return tcommon.Hash{}, err
	}
	if err := b.requireArchive(blockNum, headNum); err != nil {
		return tcommon.Hash{}, err
	}
	return reader.StorageAt(addr, slot, blockNum)
}

func (b *TronBackend) GetTransactionByHash(hash tcommon.Hash) (*corepb.Transaction, *types.Block, int, error) {
	// Use TransactionInfo to locate the block, then find the tx within it.
	info := rawdb.ReadTransactionInfo(b.chain.chaindb, hash[:])
	if info == nil {
		return nil, nil, 0, nil // not found
	}
	block := b.chain.GetBlockByNumber(uint64(info.BlockNumber))
	if block == nil {
		return nil, nil, 0, nil
	}
	for i, tx := range block.Transactions() {
		if tx.Hash() == hash {
			return tx.Proto(), block, i, nil
		}
	}
	return nil, nil, 0, nil
}

func (b *TronBackend) GetTransactionInfo(hash tcommon.Hash) (*corepb.TransactionInfo, error) {
	info := rawdb.ReadTransactionInfo(b.chain.chaindb, hash[:])
	if info != nil {
		if head := b.chain.CurrentBlock(); head != nil && uint64(info.BlockNumber) > head.Number() {
			return nil, nil
		}
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

func (b *TronBackend) GetLogs(filter jsonrpc.LogFilter) ([]*jsonrpc.RPCLog, error) {
	const maxBlockRange = 2000

	var fromBlock, toBlock uint64

	if filter.BlockHash != nil {
		// Single-block mode
		block := b.chain.GetBlockByHash(*filter.BlockHash)
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

	var logs []*jsonrpc.RPCLog

	for num := fromBlock; num <= toBlock; num++ {
		block := b.chain.GetBlockByNumber(num)
		if block == nil {
			continue
		}
		blockHash := block.Hash()
		infos := rawdb.ReadTransactionInfosByBlock(b.chain.chaindb, num)

		logIndex := uint64(0)

		for txIdx, info := range infos {
			for _, l := range info.Log {
				thisIndex := logIndex
				logIndex++

				// Address filter
				if len(filter.Addresses) > 0 {
					addrStart := 0
					if len(l.Address) > 20 {
						addrStart = len(l.Address) - 20
					}
					addr := tcommon.BytesToAddress(l.Address[addrStart:])
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
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return fmt.Errorf("open state: %w", err)
	}

	validationBuf := blockbuffer.New(b.chain.buffer)
	validationBuf.BeginBlock(tcommon.Hash{})
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
		DB:                         validationBuf,
		ActiveWitnesses:            b.chain.ActiveWitnesses(),
	}

	return act.Validate(ctx)
}
