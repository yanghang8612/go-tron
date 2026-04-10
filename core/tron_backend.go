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
	"github.com/tronprotocol/go-tron/internal/tronapi"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
)

// TronBackend implements tronapi.Backend.
type TronBackend struct {
	chain *BlockChain
	pool  *txpool.TxPool
}

func NewTronBackend(chain *BlockChain, pool *txpool.TxPool) *TronBackend {
	return &TronBackend{chain: chain, pool: pool}
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
	return b.pool.Add(tx)
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

	evm := vm.NewEVM(statedbCopy, owner, current.Number(), current.Timestamp(), tcommon.Address{}, 1)

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
