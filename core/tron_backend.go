package core

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/actuator"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
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
	txBroadcast TxBroadcaster             // nil until wired from main
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
	dp := state.LoadDynamicProperties(b.chain.DB())
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

// GetAccountAt opens state at the post-apply root of `blockNum` so the
// caller sees the account as of a specific block — used by the solid /
// PBFT HTTP variants to keep their responses isolated from live state.
func (b *TronBackend) GetAccountAt(addr tcommon.Address, blockNum uint64) (*types.Account, error) {
	root := b.chain.StateRootAtBlock(blockNum)
	if root == (tcommon.Hash{}) {
		return nil, fmt.Errorf("no state root for block %d", blockNum)
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

	cfg := vm.NewTVMConfig(current.Number(), b.chain.DynProps())
	evm := vm.NewTVM(statedbCopy, b.chain.DynProps(), owner, current.Number(), current.Timestamp(), tcommon.Address{}, 1, cfg)

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
	blockNum := rawdb.ReadTransactionIndex(b.chain.db, txHash[:])
	if blockNum == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	block := rawdb.ReadBlock(b.chain.db, *blockNum)
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
	info := rawdb.ReadTransactionInfo(b.chain.db, txHash[:])
	if info == nil {
		return nil, fmt.Errorf("transaction info not found")
	}
	return info, nil
}

func (b *TronBackend) GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error) {
	infos := rawdb.ReadTransactionInfosByBlock(b.chain.db, blockNum)
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
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}

	dynProps := state.LoadDynamicProperties(b.chain.db)

	return &tronapi.AccountResource{
		FreeNetUsed:      statedb.GetFreeNetUsage(addr),
		FreeNetLimit:     dynProps.FreeNetLimit(),
		NetUsed:          statedb.GetNetUsage(addr),
		TotalNetLimit:    dynProps.TotalNetLimit(),
		TotalEnergyLimit: dynProps.TotalEnergyCurrentLimit(),
	}, nil
}

func (b *TronBackend) GetChainParameters() []tronapi.ChainParameter {
	dynProps := state.LoadDynamicProperties(b.chain.db)
	all := dynProps.All()
	params := make([]tronapi.ChainParameter, 0, len(all))
	for k, v := range all {
		params = append(params, tronapi.ChainParameter{Key: k, Value: v})
	}
	return params
}

func (b *TronBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) {
	witnessAddrs := rawdb.ReadWitnessIndex(b.chain.db)
	activeSet := b.chain.ActiveWitnesses()
	activeMap := make(map[tcommon.Address]bool, len(activeSet))
	for _, a := range activeSet {
		activeMap[a] = true
	}

	var result []*tronapi.WitnessInfo
	for _, addr := range witnessAddrs {
		w := rawdb.ReadWitness(b.chain.db, addr)
		if w == nil {
			continue
		}
		result = append(result, &tronapi.WitnessInfo{
			Address:        hex.EncodeToString(addr[:]),
			VoteCount:      w.VoteCount(),
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
	ids := rawdb.ReadProposalIndex(b.chain.db)
	var result []*tronapi.ProposalInfo
	for _, id := range ids {
		p := rawdb.ReadProposal(b.chain.db, id)
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

func (b *TronBackend) GetDelegatedResourceV2(from, to tcommon.Address) (*tronapi.DelegatedResourceInfo, error) {
	dr := rawdb.ReadDelegatedResource(b.chain.db, from, to)
	if dr == nil {
		return nil, nil
	}
	return &tronapi.DelegatedResourceInfo{
		FromAddress:               hex.EncodeToString(from[:]),
		ToAddress:                 hex.EncodeToString(to[:]),
		FrozenBalanceForBandwidth: dr.FrozenBalanceForBandwidth,
		FrozenBalanceForEnergy:    dr.FrozenBalanceForEnergy,
		ExpireTimeForBandwidth:    dr.ExpireTimeForBandwidth,
		ExpireTimeForEnergy:       dr.ExpireTimeForEnergy,
	}, nil
}

func (b *TronBackend) GetDelegatedResourceAccountIndexV2(addr tcommon.Address) (*tronapi.DelegationIndexInfo, error) {
	receivers := rawdb.ReadDelegationIndex(b.chain.db, addr)
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
	for _, receiver := range rawdb.ReadDelegationIndex(b.chain.db, addr) {
		dr := rawdb.ReadDelegatedResource(b.chain.db, addr, receiver)
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
	root := b.chain.HeadStateRoot()
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

func (b *TronBackend) GetAssetIssueByID(id int64) *contractpb.AssetIssueContract {
	return rawdb.ReadAssetIssue(b.chain.db, id)
}

func (b *TronBackend) GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract {
	id, ok := rawdb.ReadAssetNameIndex(b.chain.db, name)
	if !ok {
		return nil
	}
	return rawdb.ReadAssetIssue(b.chain.db, id)
}

func (b *TronBackend) GetAssetIssueList() []*contractpb.AssetIssueContract {
	return rawdb.ListAllAssets(b.chain.db)
}

func (b *TronBackend) GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract {
	return rawdb.ListAssetsPaginated(b.chain.db, offset, limit)
}

func (b *TronBackend) GetAssetIssueByAccount(addr tcommon.Address) *contractpb.AssetIssueContract {
	id, ok := rawdb.ReadAssetOwnerIndex(b.chain.db, addr[:])
	if !ok {
		return nil
	}
	return rawdb.ReadAssetIssue(b.chain.db, id)
}

func (b *TronBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder {
	return rawdb.ReadMarketOrder(b.chain.db, orderID)
}

func (b *TronBackend) GetMarketOrdersByAccount(addr tcommon.Address) []*corepb.MarketOrder {
	mao := rawdb.ReadMarketAccountOrder(b.chain.db, addr[:])
	var orders []*corepb.MarketOrder
	for _, id := range mao.Orders {
		if o := rawdb.ReadMarketOrder(b.chain.db, id); o != nil {
			orders = append(orders, o)
		}
	}
	return orders
}

func (b *TronBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	return rawdb.ReadMarketPriceList(b.chain.db, sellTokenID, buyTokenID)
}

func (b *TronBackend) ListExchanges() ([]*corepb.Exchange, error) {
	return rawdb.ListAllExchanges(b.chain.db), nil
}

func (b *TronBackend) GetBrokerageInfo(addr tcommon.Address) int64 {
	// java-tron's RpcApiService.getBrokerageInfoCommon reads at
	// currentCycle, NOT at the base key (-1). Right after an UpdateBrokerage
	// tx the rate is only visible to readers who consult the snapshot at
	// the next maintenance — until then the cycle key holds the previous
	// rate. Mirror that semantic here so cross-impl byte-equal holds.
	dp := state.LoadDynamicProperties(b.chain.db)
	cycle := dp.CurrentCycleNumber()
	return int64(rawdb.ReadCycleBrokerage(b.chain.db, cycle, addr.Bytes()))
}

func (b *TronBackend) TotalTransaction() int64 {
	return rawdb.ReadTotalTransactionCount(b.chain.db)
}

func (b *TronBackend) GetBurnTrx() int64 {
	return state.LoadDynamicProperties(b.chain.db).BurnTrxAmount()
}

func (b *TronBackend) GetBandwidthPrices() string {
	return state.LoadDynamicProperties(b.chain.db).BandwidthPriceHistory()
}

func (b *TronBackend) GetEnergyPrices() string {
	return state.LoadDynamicProperties(b.chain.db).EnergyPriceHistory()
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
	all := rawdb.ListAllExchanges(b.chain.db)
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
	addrBytes := rawdb.ReadAccountIdIndex(b.chain.db, accountID)
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
	dynProps := state.LoadDynamicProperties(b.chain.db)
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
	p := rawdb.ReadProposal(b.chain.db, id)
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
	return state.LoadDynamicProperties(b.chain.db).EnergyFee()
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

func (b *TronBackend) GetTransactionByHash(hash tcommon.Hash) (*corepb.Transaction, *types.Block, int, error) {
	// Use TransactionInfo to locate the block, then find the tx within it.
	info := rawdb.ReadTransactionInfo(b.chain.db, hash[:])
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
	info := rawdb.ReadTransactionInfo(b.chain.db, hash[:])
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
		infos := rawdb.ReadTransactionInfosByBlock(b.chain.db, num)

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

	head := b.chain.CurrentBlock()
	root := b.chain.HeadStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return fmt.Errorf("open state: %w", err)
	}

	dynProps := state.LoadDynamicProperties(b.chain.DB())

	// Hydrate witnesses into statedb, matching InsertBlock's pre-processing step.
	// Actuators that check ctx.State.GetWitness (witness_update, vote, brokerage, etc.)
	// will fail "owner is not a witness" without this.
	witnessAddrs := rawdb.ReadWitnessIndex(b.chain.DB())
	for _, addr := range witnessAddrs {
		if statedb.GetWitness(addr) == nil {
			w := rawdb.ReadWitness(b.chain.DB(), addr)
			if w != nil {
				statedb.PutWitness(addr, w.URL())
				statedb.AddWitnessVoteCount(addr, w.VoteCount())
			}
		}
	}

	ctx := &actuator.Context{
		State:           statedb,
		DynProps:        dynProps,
		Tx:              tx,
		BlockTime:       head.Timestamp(),
		BlockNumber:     head.Number(),
		DB:              b.chain.DB(),
		ActiveWitnesses: b.chain.ActiveWitnesses(),
	}

	return act.Validate(ctx)
}
