package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
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
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetTransactionInfoByID(txHash tcommon.Hash) (*corepb.TransactionInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetBlockByHash(hash tcommon.Hash) (*types.Block, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) BuildTransferTransaction(owner, to tcommon.Address, amount int64) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) BuildDeployContractTransaction(owner tcommon.Address, abi string, bytecode []byte,
	feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) BuildTriggerContractTransaction(owner, contract tcommon.Address, data []byte,
	feeLimit int64, callValue int64) (*corepb.Transaction, *tronapi.TriggerResult, error) {
	return nil, nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) EstimateEnergy(owner, contract tcommon.Address, data []byte) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetAccountResource(addr tcommon.Address) (*tronapi.AccountResource, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetChainParameters() []tronapi.ChainParameter {
	return nil
}

func (b *TronBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) NextMaintenanceTime() int64 {
	return 0
}
