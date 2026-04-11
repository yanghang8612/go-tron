package core

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
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
	txBroadcast TxBroadcaster          // nil until wired from main
	peersFunc   func() []*tronapi.PeerInfo // nil until wired from main
}

func NewTronBackend(chain *BlockChain, pool *txpool.TxPool) *TronBackend {
	return &TronBackend{chain: chain, pool: pool}
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

func (b *TronBackend) GetBlockByNumber(number uint64) (*types.Block, error) {
	block := b.chain.GetBlockByNumber(number)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", number)
	}
	return block, nil
}

func (b *TronBackend) GetAccount(addr tcommon.Address) (*types.Account, error) {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
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

func (b *TronBackend) BroadcastTransaction(tx *types.Transaction) error {
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
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
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
	root := current.AccountStateRoot()
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
	evm := vm.NewEVM(statedbCopy, owner, current.Number(), current.Timestamp(), tcommon.Address{}, 1, cfg)

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
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
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
			Address:   hex.EncodeToString(addr[:]),
			VoteCount: w.VoteCount(),
			URL:       w.URL(),
			IsJobs:    activeMap[addr],
		})
	}
	return result, nil
}

func (b *TronBackend) NextMaintenanceTime() int64 {
	return b.chain.NextMaintenanceTime()
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
		params := make(map[string]int64, len(p.Parameters))
		for k, v := range p.Parameters {
			params[fmt.Sprintf("%d", k)] = v
		}
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
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
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
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
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
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
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
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
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

// ── JSON-RPC Backend implementation (Phase 11) ────────────────────────────

func (b *TronBackend) ChainID() int64 {
	return b.chain.Config().ChainID
}

func (b *TronBackend) BlockNumber() uint64 {
	return b.chain.CurrentBlock().Number()
}

func (b *TronBackend) GetBalance(addr tcommon.Address) int64 {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return 0
	}
	return statedb.GetBalance(addr)
}

func (b *TronBackend) GetCode(addr tcommon.Address) []byte {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil
	}
	return statedb.GetCode(addr)
}

func (b *TronBackend) GetStorageAt(addr tcommon.Address, slot tcommon.Hash) tcommon.Hash {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
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
