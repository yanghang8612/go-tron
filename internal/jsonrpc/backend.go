package jsonrpc

import (
	"context"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/vm/tracers"
)

// LogFilter selects logs across a block range with optional address/topic constraints.
type LogFilter struct {
	FromBlock *uint64
	ToBlock   *uint64
	BlockHash *common.Hash
	Addresses []common.Address
	Topics    [][]common.Hash // Topics[i] = required hashes for position i; nil = any
}

// RPCLog is an Ethereum-format event log entry.
type RPCLog struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	BlockTimestamp   string   `json:"blockTimestamp"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	BlockHash        string   `json:"blockHash"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// BlockTraceResult is one entry in debug_traceBlockByNumber/Hash output.
// It follows the geth/Erigon shape: each transaction has its hash plus either
// a tracer result or a trace error.
type BlockTraceResult struct {
	TxHash string      `json:"txHash"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// FreezerStatus summarizes immutable chain data stored outside the hot KV.
type FreezerStatus struct {
	Available       bool                   `json:"available"`
	HasFrozen       bool                   `json:"hasFrozen"`
	FrozenMin       *uint64                `json:"frozenMin,omitempty"`
	FrozenMax       *uint64                `json:"frozenMax,omitempty"`
	TableCounts     map[string]uint64      `json:"tableCounts"`
	TableSizesBytes map[string]uint64      `json:"tableSizesBytes,omitempty"`
	Stage           *FreezerStageStatus    `json:"stage,omitempty"`
	Physical        *FreezerPhysicalStatus `json:"physical,omitempty"`
}

// FreezerStageStatus reports the persisted chain-freezer stage cursor.
type FreezerStageStatus struct {
	BlockNum  uint64 `json:"blockNum"`
	HashBound bool   `json:"hashBound"`
	BlockHash string `json:"blockHash,omitempty"`
}

// FreezerPhysicalStatus mirrors the local freezer file bounds when available.
type FreezerPhysicalStatus struct {
	Datadir          string               `json:"datadir,omitempty"`
	ReadOnly         bool                 `json:"readOnly"`
	Head             uint64               `json:"head"`
	Tail             uint64               `json:"tail"`
	Tables           []FreezerTableStatus `json:"tables,omitempty"`
	RepairApplied    bool                 `json:"repairApplied"`
	RepairTargetHead uint64               `json:"repairTargetHead,omitempty"`
	RepairTargetTail uint64               `json:"repairTargetTail,omitempty"`
	RepairRecordedAt string               `json:"repairRecordedAt,omitempty"`
}

// FreezerTableStatus is one freezer table's physical and visible bounds.
type FreezerTableStatus struct {
	Name         string `json:"name"`
	Head         uint64 `json:"head"`
	PhysicalTail uint64 `json:"physicalTail"`
	HiddenTail   uint64 `json:"hiddenTail"`
	Prunable     bool   `json:"prunable"`
	NoSnappy     bool   `json:"noSnappy"`
	VisibleSize  uint64 `json:"visibleSize"`
	HiddenSize   uint64 `json:"hiddenSize"`
}

// StageStatus reports canonical, sync, snapshot, prune, and freezer stage rows.
type StageStatus struct {
	Status        string              `json:"status"`
	Complete      bool                `json:"complete"`
	Pending       int                 `json:"pending"`
	Stages        []StageStatusRow    `json:"stages"`
	Pipeline      StageStatusPipeline `json:"pipeline"`
	Issues        []string            `json:"issues,omitempty"`
	IssueDetails  []StageStatusIssue  `json:"issueDetails,omitempty"`
	UnknownStages []string            `json:"unknownStages,omitempty"`
}

// StageStatusRow is one persisted stage-progress row plus verification data.
type StageStatusRow struct {
	Stage         string   `json:"stage"`
	Group         string   `json:"group"`
	Present       bool     `json:"present"`
	BlockNum      uint64   `json:"blockNum,omitempty"`
	HashBound     bool     `json:"hashBound"`
	BlockHash     string   `json:"blockHash,omitempty"`
	Verified      string   `json:"verified"`
	CanonicalHash string   `json:"canonicalHash,omitempty"`
	Details       []string `json:"details,omitempty"`
}

// StageStatusPipeline is the next schedulable stage-progress view.
type StageStatusPipeline struct {
	Complete bool                      `json:"complete"`
	Pending  int                       `json:"pending"`
	Issues   int                       `json:"issues"`
	Tasks    []StageStatusPipelineTask `json:"tasks,omitempty"`
}

// StageStatusPipelineTask is one stage edge that can advance or needs repair.
type StageStatusPipelineTask struct {
	Stage          string `json:"stage"`
	Upstream       string `json:"upstream"`
	Status         string `json:"status"`
	TargetValue    uint64 `json:"targetValue"`
	TargetHash     string `json:"targetHash,omitempty"`
	CurrentValue   uint64 `json:"currentValue,omitempty"`
	CurrentHash    string `json:"currentHash,omitempty"`
	CurrentPresent bool   `json:"currentPresent"`
}

// StageStatusIssue is one structured stage verification or ordering issue.
type StageStatusIssue struct {
	Kind            string `json:"kind"`
	Stage           string `json:"stage,omitempty"`
	Upstream        string `json:"upstream,omitempty"`
	Detail          string `json:"detail"`
	DownstreamValue uint64 `json:"downstreamValue,omitempty"`
	UpstreamValue   uint64 `json:"upstreamValue,omitempty"`
	HashMismatch    bool   `json:"hashMismatch,omitempty"`
	MissingUpstream bool   `json:"missingUpstream,omitempty"`
}

// Backend is the data-access interface for the JSON-RPC API.
// Implemented by core.TronBackend.
type Backend interface {
	// Chain metadata
	ChainID() int64
	BlockNumber() uint64

	// Block queries — same signatures as tronapi.Backend, already on TronBackend
	GetBlockByNumber(num uint64) (*types.Block, error)
	GetBlockByHash(hash common.Hash) (*types.Block, error)

	// Account state (always reads latest/current state)
	GetBalance(addr common.Address) (int64, error) // returns SUN; handler multiplies by 1e12
	GetCode(addr common.Address) ([]byte, error)
	GetStorageAt(addr common.Address, slot common.Hash) (common.Hash, error)

	// Archive state — the value AS OF the end of blockNum, reconstructed via
	// the State History Index. Callers pass the resolved block number (the
	// handler turns "latest"/"earliest"/"pending"/hex into a number first).
	// On a node not synced with --history.enabled, a query for a block older
	// than head returns an error; a query at head resolves from live state.
	GetBalanceAt(addr common.Address, blockNum uint64) (int64, error) // SUN; handler multiplies by 1e12
	GetBalanceAtContext(ctx context.Context, addr common.Address, blockNum uint64) (int64, error)
	GetCodeAt(addr common.Address, blockNum uint64) ([]byte, error)
	GetCodeAtContext(ctx context.Context, addr common.Address, blockNum uint64) ([]byte, error)
	GetStorageAtBlock(addr common.Address, slot common.Hash, blockNum uint64) (common.Hash, error)
	GetStorageAtBlockContext(ctx context.Context, addr common.Address, slot common.Hash, blockNum uint64) (common.Hash, error)

	// Transaction queries
	GetTransactionByHash(hash common.Hash) (*corepb.Transaction, *types.Block, int, error)
	GetTransactionInfo(hash common.Hash) (*corepb.TransactionInfo, error)
	GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error)

	// TVM execution (read-only simulation)
	Call(from, to *common.Address, data []byte, value int64) ([]byte, error)
	CallAt(from, to *common.Address, data []byte, value int64, blockNum uint64) ([]byte, error)
	CallAtContext(ctx context.Context, from, to *common.Address, data []byte, value int64, blockNum uint64) ([]byte, error)

	// Tracing (debug namespace). TraceCall replays a read-only call with the
	// configured tracer (blockNumber nil = head, else archive at that block);
	// TraceTransaction re-executes a historical tx from its parent state. Both
	// return the tracer's rendered result. TraceBlock re-executes a whole block
	// from its parent state, returning one result per transaction.
	TraceCall(from, to *common.Address, data []byte, value int64, blockNumber *uint64, cfg *tracers.TraceConfig) (interface{}, error)
	TraceCallContext(ctx context.Context, from, to *common.Address, data []byte, value int64, blockNumber *uint64, cfg *tracers.TraceConfig) (interface{}, error)
	TraceTransaction(hash common.Hash, cfg *tracers.TraceConfig) (interface{}, error)
	TraceBlock(block *types.Block, cfg *tracers.TraceConfig) ([]BlockTraceResult, error)

	// EstimateGas simulates execution and returns energy used.
	EstimateGas(from, to *common.Address, data []byte, value int64) (uint64, error)
	EstimateGasAt(from, to *common.Address, data []byte, value int64, blockNum uint64) (uint64, error)

	// Log queries
	GetLogs(filter LogFilter) ([]*RPCLog, error)

	// Node metadata
	GasPrice() int64 // energy fee in SUN per energy unit
	PeerCount() int
	SyncInfo() *tronapi.SyncInfo
	FreezerStatus() (*FreezerStatus, error)
	StageStatus() (*StageStatus, error)

	// Block subscriptions for the filter subsystem
	SubscribeBlocks(ch chan<- *types.Block)
	UnsubscribeBlocks(ch chan<- *types.Block)
}
