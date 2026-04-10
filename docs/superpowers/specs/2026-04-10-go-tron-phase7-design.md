# Phase 7: Transaction Lifecycle & Observability

## 1. Overview

Phase 7 closes the loop on transaction execution by making results observable. After Phases 1-6 built the full execution pipeline (state, consensus, resources, P2P, EVM), transactions execute correctly but their results vanish -- no receipts, no logs, no query APIs for fetching transaction results.

This phase adds:
- **TransactionInfo generation** -- after each tx executes, produce a `TransactionInfo` protobuf capturing fee, energy/net usage, logs, contract address, and result status
- **Event collection** -- LOG0-LOG4 opcodes currently consume gas and pop topics but discard the data; wire them to emit log entries that flow back to the caller
- **Persistent storage** -- store TransactionInfo indexed by txID and by block number
- **Transaction building APIs** -- server-side unsigned transaction construction (createtransaction, deploycontract, triggersmartcontract, estimateenergy)
- **Query APIs** -- transaction lookup, block lookup, resource queries, chain parameters

## 2. Scope

### In scope
- TransactionInfo generation and rawdb storage
- EVM log event collection (LOG0-LOG4 -> TransactionInfo.Log)
- 13 new HTTP API endpoints
- Backend interface extensions to support the new endpoints

### Out of scope
- gRPC API (HTTP only, matching java-tron format)
- Event subscription / WebSocket push
- Internal transaction tracking (CALL/CREATE traces)
- Transaction re-execution or historical state queries

## 3. Design

### 3.A TransactionInfo Generation & Storage

#### 3.A.1 Event Collection in the EVM

The `makeLog()` function in `vm/instructions.go` currently pops topics and reads log data from memory but discards them (line 637: `// Log data consumed but not stored`). We need to capture these events.

**Approach: Log slice on the EVM struct.** No channels, no callbacks -- just a `[]Log` that accumulates during execution.

**New type** in `vm/log.go`:

```go
package vm

import tcommon "github.com/tronprotocol/go-tron/common"

// Log represents a contract log event emitted by LOG0-LOG4.
type Log struct {
    Address tcommon.Address // contract address that emitted the log
    Topics  [][]byte        // 0-4 topic hashes (32 bytes each)
    Data    []byte          // arbitrary-length log data
}
```

**EVM struct change** in `vm/evm.go`:

```go
type EVM struct {
    // ... existing fields ...
    Logs []Log // accumulated log events from this execution
}
```

**makeLog() change** in `vm/instructions.go`:

Replace the current topic-discarding loop with event capture:

```go
func makeLog(topicCount int) executionFunc {
    return func(pc *uint64, interpreter *Interpreter, contract *Contract,
        memory *Memory, stack *Stack) ([]byte, error) {

        offset, size := stack.pop(), stack.pop()
        sz := size.Uint64()

        cost := EnergyLog + EnergyLogTopic*uint64(topicCount) + EnergyLogData*sz
        if mcost := memoryExpansionCost(memory, offset.Uint64(), sz); mcost > 0 {
            cost += mcost
        }
        if !contract.UseEnergy(cost) {
            return nil, ErrOutOfEnergy
        }
        memory.resize(offset.Uint64() + sz)

        // Collect topics
        topics := make([][]byte, topicCount)
        for i := 0; i < topicCount; i++ {
            t := stack.pop()
            b := t.Bytes32()
            topics[i] = make([]byte, 32)
            copy(topics[i], b[:])
        }

        // Collect log data
        data := memory.getCopy(int64(offset.Uint64()), int64(sz))

        // Append to EVM's log accumulator
        interpreter.evm.Logs = append(interpreter.evm.Logs, Log{
            Address: contract.Address,
            Topics:  topics,
            Data:    data,
        })

        return nil, nil
    }
}
```

**Static call protection:** Logs emitted during `StaticCall` must still be collected (they don't modify state; they're metadata). This matches Ethereum behavior. No changes needed to `StaticCall`.

**Revert handling:** When a CALL/CREATE reverts, its state changes are rolled back but logs emitted within that sub-call should also be discarded. This requires snapshot-based log tracking:

```go
// In EVM:
func (evm *EVM) LogSnapshot() int {
    return len(evm.Logs)
}

func (evm *EVM) RevertLogs(snapshot int) {
    evm.Logs = evm.Logs[:snapshot]
}
```

The `Call`, `Create`, `Create2` methods in `evm.go` must save a log snapshot before executing and revert logs on error (alongside the state revert). For example in `Call`:

```go
func (evm *EVM) Call(...) ([]byte, uint64, error) {
    // ... existing depth check, value transfer ...
    snap := evm.StateDB.Snapshot()
    logSnap := evm.LogSnapshot()          // <-- new

    // ... execute ...

    if err != nil {
        evm.StateDB.RevertToSnapshot(snap)
        evm.RevertLogs(logSnap)           // <-- new
        // ...
    }
    // ...
}
```

#### 3.A.2 Execution Result Type

Currently `actuator.Result` only has `Fee int64`. Extend it to carry all the data needed to build a TransactionInfo:

```go
// In actuator/actuator.go:
type Result struct {
    Fee             int64
    EnergyUsed      int64
    EnergyFee       int64     // energy_used * energy_price
    OriginEnergyUsage int64   // energy paid by contract deployer (0 for now)
    NetUsage        int64
    NetFee          int64
    ContractResult  []byte    // return data from VM execution
    ContractAddress []byte    // set only for CreateSmartContract
    Logs            []vm.Log  // log events from VM execution
    ContractRet     int32     // Transaction.Result.contractResult enum value
}
```

**VMActuator changes:** After EVM execution, populate the extended result fields:

```go
func (a *VMActuator) executeCreate(ctx *Context) (*Result, error) {
    // ... existing setup ...

    ret, contractAddr, energyLeft, vmErr := evm.Create(owner, bytecode, energyLimit, callValue)
    energyUsed := energyLimit - energyLeft
    fee := int64(energyUsed) * energyFee

    result := &Result{
        Fee:            fee,
        EnergyUsed:     int64(energyUsed),
        EnergyFee:      fee,
        ContractResult: ret,
        Logs:           evm.Logs,
    }

    if vmErr != nil {
        result.ContractRet = contractRetFromError(vmErr) // map vm errors to enum
        return result, nil
    }

    result.ContractRet = 1 // SUCCESS
    result.ContractAddress = contractAddr[:]

    // ... existing contract metadata storage ...
    return result, nil
}
```

**contractRetFromError mapping:**

```go
func contractRetFromError(err error) int32 {
    switch err {
    case vm.ErrExecutionReverted:     return 2  // REVERT
    case vm.ErrInvalidJump:           return 3  // BAD_JUMP_DESTINATION
    case vm.ErrOutOfEnergy:           return 10 // OUT_OF_ENERGY
    case vm.ErrStackUnderflow:        return 6  // STACK_TOO_SMALL
    case vm.ErrStackOverflow:         return 7  // STACK_TOO_LARGE
    case vm.ErrWriteProtection:       return 8  // ILLEGAL_OPERATION
    case vm.ErrDepthExceeded:         return 9  // STACK_OVERFLOW
    case vm.ErrContractCodeTooLarge:  return 15 // INVALID_CODE
    default:                          return 13 // UNKNOWN
    }
}
```

For non-VM actuators (TransferActuator, FreezeBalanceV2Actuator, etc.), result fields besides `Fee` remain zero-valued. Only `ContractRet` should be set to `1` (SUCCESS) on success, `0` (DEFAULT) otherwise.

#### 3.A.3 TransactionInfo Construction

After `ApplyTransaction` returns, the caller (`ProcessBlock`) builds a `TransactionInfo` protobuf. This happens in `state_processor.go`.

**New function** in `core/state_processor.go`:

```go
func buildTransactionInfo(
    tx *types.Transaction,
    result *actuator.Result,
    blockNum uint64,
    blockTime int64,
    netUsage int64,
    netFee int64,
) *corepb.TransactionInfo {
    txID := tx.Hash()

    info := &corepb.TransactionInfo{
        Id:             txID[:],
        Fee:            result.Fee + netFee,
        BlockNumber:    int64(blockNum),
        BlockTimeStamp: blockTime,
        Receipt: &corepb.ResourceReceipt{
            EnergyUsage:      result.EnergyUsed,
            EnergyFee:        result.EnergyFee,
            OriginEnergyUsage: result.OriginEnergyUsage,
            EnergyUsageTotal: result.EnergyUsed + result.OriginEnergyUsage,
            NetUsage:         netUsage,
            NetFee:           netFee,
            Result:           corepb.Transaction_Result_contractResult(result.ContractRet),
        },
    }

    if len(result.ContractResult) > 0 {
        info.ContractResult = [][]byte{result.ContractResult}
    }

    if len(result.ContractAddress) > 0 {
        info.ContractAddress = result.ContractAddress
    }

    // Convert VM logs to protobuf logs
    for _, l := range result.Logs {
        pbLog := &corepb.TransactionInfo_Log{
            Address: l.Address[:],
            Data:    l.Data,
        }
        for _, t := range l.Topics {
            pbLog.Topics = append(pbLog.Topics, t)
        }
        info.Log = append(info.Log, pbLog)
    }

    // Set result code: SUCESS(0) if contractRet is SUCCESS(1) or DEFAULT(0)
    if result.ContractRet > 1 {
        info.Result = corepb.TransactionInfo_FAILED
        // Encode VM revert message if available
        if result.ContractRet == 2 && len(result.ContractResult) > 0 {
            info.ResMessage = result.ContractResult
        }
    }

    return info
}
```

#### 3.A.4 ApplyTransaction Signature Change

`ApplyTransaction` must return the full `*actuator.Result` (not just `int64` fee) so the caller can build TransactionInfo:

```go
// Before:
func ApplyTransaction(...) (int64, error)

// After:
func ApplyTransaction(...) (*actuator.Result, error)
```

Bandwidth consumption results (net_usage vs net_fee) also need to be captured. Refactor `consumeBandwidth` to return what was consumed:

```go
type BandwidthResult struct {
    NetUsage int64 // bandwidth units consumed from frozen/free
    NetFee   int64 // TRX burned for bandwidth (in sun)
}

func consumeBandwidth(...) (*BandwidthResult, error)
```

The updated `ApplyTransaction` merges bandwidth results into the actuator result:

```go
func ApplyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties,
    tx *types.Transaction, blockTime int64, blockNum uint64) (*actuator.Result, error) {
    // ... create actuator, validate ...

    bwResult, err := consumeBandwidth(statedb, dynProps, tx, blockTime)
    if err != nil { return nil, ... }

    snap := statedb.Snapshot()
    result, err := act.Execute(ctx)
    if err != nil {
        statedb.RevertToSnapshot(snap)
        return nil, ...
    }

    result.NetUsage = bwResult.NetUsage
    result.NetFee = bwResult.NetFee
    return result, nil
}
```

#### 3.A.5 ProcessBlock: Collect and Return TransactionInfos

`ProcessBlock` must return the list of TransactionInfos so they can be persisted by the caller:

```go
// Before:
func ProcessBlock(...) error

// After:
func ProcessBlock(...) ([]*corepb.TransactionInfo, error)
```

```go
func ProcessBlock(statedb *state.StateDB, dynProps *state.DynamicProperties,
    block *types.Block) ([]*corepb.TransactionInfo, error) {

    var txInfos []*corepb.TransactionInfo

    for i, tx := range block.Transactions() {
        result, err := ApplyTransaction(statedb, dynProps, tx, block.Timestamp(), block.Number())
        if err != nil {
            return nil, fmt.Errorf("tx %d: %w", i, err)
        }
        info := buildTransactionInfo(tx, result, block.Number(), block.Timestamp(),
            result.NetUsage, result.NetFee)
        txInfos = append(txInfos, info)
    }

    // Pay block reward (unchanged)
    witnessAddr := block.WitnessAddress()
    if witnessAddr != (tcommon.Address{}) {
        reward := dynProps.WitnessPayPerBlock()
        if reward > 0 {
            statedb.AddAllowance(witnessAddr, reward)
        }
    }

    return txInfos, nil
}
```

#### 3.A.6 RawDB Storage

**New key prefixes** in `core/rawdb/schema.go`:

```go
var (
    // ... existing prefixes ...
    txInfoPrefix       = []byte("ti-")  // already declared but unused
    txInfoBlockPrefix  = []byte("tib-") // tib-<blockNum> -> TransactionRet (list of infos)
)

func txInfoKey(hash []byte) []byte {
    return append(append([]byte{}, txInfoPrefix...), hash...)
}

func txInfoBlockKey(number uint64) []byte {
    k := make([]byte, len(txInfoBlockPrefix)+8)
    copy(k, txInfoBlockPrefix)
    binary.BigEndian.PutUint64(k[len(txInfoBlockPrefix):], number)
    return k
}
```

**New accessor file** `core/rawdb/accessors_txinfo.go`:

```go
package rawdb

import (
    "encoding/binary"

    "github.com/ethereum/go-ethereum/ethdb"
    corepb "github.com/tronprotocol/go-tron/proto/core"
    "google.golang.org/protobuf/proto"
)

// WriteTransactionInfo stores a single TransactionInfo indexed by txID.
func WriteTransactionInfo(db ethdb.KeyValueWriter, txID []byte, info *corepb.TransactionInfo) {
    data, err := proto.Marshal(info)
    if err != nil {
        return
    }
    db.Put(txInfoKey(txID), data)
}

// ReadTransactionInfo retrieves a TransactionInfo by txID.
func ReadTransactionInfo(db ethdb.KeyValueReader, txID []byte) *corepb.TransactionInfo {
    data, err := db.Get(txInfoKey(txID))
    if err != nil {
        return nil
    }
    info := &corepb.TransactionInfo{}
    if err := proto.Unmarshal(data, info); err != nil {
        return nil
    }
    return info
}

// WriteTransactionInfosByBlock stores all TransactionInfos for a block as a TransactionRet.
func WriteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64,
    infos []*corepb.TransactionInfo) {
    ret := &corepb.TransactionRet{
        BlockNumber:     int64(blockNum),
        Transactioninfo: infos,
    }
    if len(infos) > 0 {
        ret.BlockTimeStamp = infos[0].BlockTimeStamp
    }
    data, err := proto.Marshal(ret)
    if err != nil {
        return
    }
    db.Put(txInfoBlockKey(blockNum), data)
}

// ReadTransactionInfosByBlock retrieves all TransactionInfos for a given block number.
func ReadTransactionInfosByBlock(db ethdb.KeyValueReader, blockNum uint64) []*corepb.TransactionInfo {
    data, err := db.Get(txInfoBlockKey(blockNum))
    if err != nil {
        return nil
    }
    ret := &corepb.TransactionRet{}
    if err := proto.Unmarshal(data, ret); err != nil {
        return nil
    }
    return ret.Transactioninfo
}
```

**Persistence in BlockChain.InsertBlock:** After `ProcessBlock` returns the info list, persist them:

```go
// In blockchain.go InsertBlock(), after ProcessBlock:
txInfos, err := ProcessBlock(statedb, dynProps, block)
if err != nil {
    return fmt.Errorf("process block: %w", err)
}

// ... existing maintenance, commit, dynProps update, WriteBlock ...

// Persist transaction infos
for _, info := range txInfos {
    rawdb.WriteTransactionInfo(bc.db, info.Id, info)
}
rawdb.WriteTransactionInfosByBlock(bc.db, block.Number(), txInfos)
```

### 3.B Transaction Building APIs

These endpoints construct unsigned transactions server-side. The client signs them locally and broadcasts via `/wallet/broadcasttransaction`.

All building APIs return a protobuf `Transaction` as JSON, with computed `txID` and `raw_data_hex` fields. The transaction has `ref_block_bytes`, `ref_block_hash`, `expiration`, and `timestamp` derived from the current head block.

#### 3.B.1 Common Transaction Builder

**New file** `internal/tronapi/txbuilder.go`:

```go
package tronapi

import (
    "crypto/sha256"
    "encoding/binary"
    "time"

    corepb "github.com/tronprotocol/go-tron/proto/core"
    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/types/known/anypb"
)

const txExpirationSeconds = 60

// buildTransaction creates an unsigned Transaction wrapping the given contract.
func buildTransaction(
    headBlockNum uint64,
    headBlockHash []byte,
    headBlockTimestamp int64,
    contractType corepb.Transaction_Contract_ContractType,
    contractMsg proto.Message,
    feeLimit int64,
) (*corepb.Transaction, error) {
    paramAny, err := anypb.New(contractMsg)
    if err != nil {
        return nil, err
    }

    // ref_block_bytes: bytes 6..7 of block number (big-endian)
    numBytes := make([]byte, 8)
    binary.BigEndian.PutUint64(numBytes, headBlockNum)
    refBlockBytes := numBytes[6:8]

    // ref_block_hash: bytes 8..15 of block hash
    var refBlockHash []byte
    if len(headBlockHash) >= 16 {
        refBlockHash = headBlockHash[8:16]
    }

    now := time.Now().UnixMilli()
    expiration := headBlockTimestamp + txExpirationSeconds*1000

    rawData := &corepb.Transaction_Raw{
        RefBlockBytes: refBlockBytes,
        RefBlockHash:  refBlockHash,
        Expiration:    expiration,
        Timestamp:     now,
        Contract: []*corepb.Transaction_Contract{{
            Type:      contractType,
            Parameter: paramAny,
        }},
    }

    if feeLimit > 0 {
        rawData.FeeLimit = feeLimit
    }

    return &corepb.Transaction{
        RawData: rawData,
    }, nil
}
```

#### 3.B.2 POST /wallet/createtransaction

Builds an unsigned `TransferContract` transaction.

**Request:**
```json
{
    "owner_address": "41...",
    "to_address": "41...",
    "amount": 1000000
}
```

**Response:** Unsigned Transaction JSON with `txID` and `raw_data_hex`.

**Backend method:**

```go
// In backend.go:
BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error)
```

**Implementation** builds a `TransferContract` protobuf, wraps it via `buildTransaction`, returns the proto.

#### 3.B.3 POST /wallet/deploycontract

Builds an unsigned `CreateSmartContract` transaction.

**Request:**
```json
{
    "owner_address": "41...",
    "abi": "...",
    "bytecode": "...",
    "fee_limit": 100000000,
    "call_value": 0,
    "name": "MyContract",
    "consume_user_resource_percent": 100
}
```

**Response:** Unsigned Transaction JSON.

**Backend method:**

```go
BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte,
    feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error)
```

#### 3.B.4 POST /wallet/triggersmartcontract

Builds an unsigned `TriggerSmartContract` transaction AND executes a constant simulation to return `energy_used` and `constant_result`.

**Request:**
```json
{
    "owner_address": "41...",
    "contract_address": "41...",
    "function_selector": "transfer(address,uint256)",
    "parameter": "0000...0001",
    "fee_limit": 30000000,
    "call_value": 0
}
```

**Response:**
```json
{
    "result": { "result": true },
    "transaction": { /* unsigned Transaction JSON with txID, raw_data_hex */ },
    "energy_used": 12345,
    "constant_result": ["0000...0001"]
}
```

**Backend method:**

```go
BuildTriggerContractTransaction(owner, contract common.Address, data []byte,
    feeLimit int64, callValue int64) (*corepb.Transaction, *TriggerResult, error)
```

The implementation builds the transaction AND runs `TriggerConstantContract` to provide the energy estimate and return data.

#### 3.B.5 POST /wallet/estimateenergy

Estimates energy cost of a contract call without constructing a transaction.

**Request:**
```json
{
    "owner_address": "41...",
    "contract_address": "41...",
    "function_selector": "transfer(address,uint256)",
    "parameter": "0000...0001",
    "data": ""
}
```

**Response:**
```json
{
    "result": { "result": true },
    "energy_required": 12345
}
```

**Backend method:**

```go
EstimateEnergy(owner, contract common.Address, data []byte) (int64, error)
```

This calls `TriggerConstantContract` with a high energy limit (30,000,000) and returns the `EnergyUsed` field. On VM error, returns the error.

### 3.C Transaction & Block Query APIs

#### 3.C.1 POST /wallet/gettransactionbyid

Lookup a raw transaction by its hash from the block that contains it.

**Request:**
```json
{
    "value": "abc123..."
}
```

**Response:** Transaction JSON (same format as in block responses).

**Implementation strategy:** We do not currently store raw transactions independently in rawdb -- they are embedded in blocks. We need a tx-to-block index.

**New rawdb accessor** `WriteTransactionIndex` / `ReadTransactionIndex`:

```go
// Key: tx-<txHash> -> 8 bytes blockNum
func WriteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte, blockNum uint64) {
    num := make([]byte, 8)
    binary.BigEndian.PutUint64(num, blockNum)
    db.Put(txKey(txHash), num)
}

func ReadTransactionIndex(db ethdb.KeyValueReader, txHash []byte) *uint64 {
    data, err := db.Get(txKey(txHash))
    if err != nil || len(data) != 8 {
        return nil
    }
    num := binary.BigEndian.Uint64(data)
    return &num
}
```

During `InsertBlock`, after writing the block, index each transaction:

```go
for _, tx := range block.Transactions() {
    h := tx.Hash()
    rawdb.WriteTransactionIndex(bc.db, h[:], block.Number())
}
```

**Backend method:**

```go
GetTransactionByID(txHash common.Hash) (*corepb.Transaction, error)
```

Looks up block number from tx index, reads block, scans transactions for matching hash, returns the proto.

#### 3.C.2 POST /wallet/gettransactioninfobyid

Lookup TransactionInfo by tx hash.

**Request:**
```json
{
    "value": "abc123..."
}
```

**Response:** TransactionInfo JSON (marshalTronJSON).

**Backend method:**

```go
GetTransactionInfoByID(txHash common.Hash) (*corepb.TransactionInfo, error)
```

Direct rawdb lookup via `ReadTransactionInfo(db, txHash[:])`.

#### 3.C.3 POST /wallet/gettransactioninfobyblocknum

All TransactionInfos for a given block number.

**Request:**
```json
{
    "num": 12345
}
```

**Response:**
```json
[
    { /* TransactionInfo */ },
    { /* TransactionInfo */ }
]
```

**Backend method:**

```go
GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error)
```

Direct rawdb lookup via `ReadTransactionInfosByBlock(db, blockNum)`.

#### 3.C.4 POST /wallet/getblockbyid

Lookup block by its hash.

**Request:**
```json
{
    "value": "abc123..."
}
```

**Response:** Block JSON (same as `/wallet/getblockbynum`).

**Backend method:**

```go
GetBlockByHash(hash common.Hash) (*types.Block, error)
```

Already exists on `BlockChain`. Expose via Backend interface.

#### 3.C.5 POST /wallet/getblockbylimitnext

Range of blocks `[startNum, endNum)`.

**Request:**
```json
{
    "startNum": 100,
    "endNum": 105
}
```

**Response:**
```json
{
    "block": [
        { /* Block 100 */ },
        { /* Block 101 */ },
        { /* Block 102 */ },
        { /* Block 103 */ },
        { /* Block 104 */ }
    ]
}
```

Limit the range to 100 blocks to prevent abuse.

**Backend method:**

```go
GetBlocksByRange(start, end uint64) ([]*types.Block, error)
```

### 3.D Resource & Chain Query APIs

#### 3.D.1 POST /wallet/getaccountresource

Returns the energy and bandwidth resource status for an account.

**Request:**
```json
{
    "address": "41...",
    "visible": false
}
```

**Response:**
```json
{
    "freeNetUsed": 150,
    "freeNetLimit": 1500,
    "NetUsed": 0,
    "NetLimit": 0,
    "TotalNetLimit": 43200000000,
    "TotalNetWeight": 100000000,
    "EnergyUsed": 500,
    "EnergyLimit": 0,
    "TotalEnergyLimit": 50000000000,
    "TotalEnergyWeight": 100000000
}
```

**Backend method:**

```go
GetAccountResource(addr common.Address) (*AccountResource, error)
```

**AccountResource struct** in `internal/tronapi/backend.go`:

```go
type AccountResource struct {
    FreeNetUsed       int64 `json:"freeNetUsed"`
    FreeNetLimit      int64 `json:"freeNetLimit"`
    NetUsed           int64 `json:"NetUsed"`
    NetLimit          int64 `json:"NetLimit"`
    TotalNetLimit     int64 `json:"TotalNetLimit"`
    TotalNetWeight    int64 `json:"TotalNetWeight"`
    EnergyUsed        int64 `json:"EnergyUsed"`
    EnergyLimit       int64 `json:"EnergyLimit"`
    TotalEnergyLimit  int64 `json:"TotalEnergyLimit"`
    TotalEnergyWeight int64 `json:"TotalEnergyWeight"`
}
```

Implementation reads the account's `FreeNetUsage`, `NetUsage`, `EnergyUsage` from StateDB, and the chain-wide limits/weights from DynamicProperties. Individual limits are derived from `(frozenAmount / totalWeight) * totalLimit`.

#### 3.D.2 POST /wallet/getchainparameters

Returns all dynamic properties as a list of key-value pairs.

**Request:** Empty body or `{}`.

**Response:**
```json
{
    "chainParameter": [
        { "key": "maintenance_time_interval", "value": 21600000 },
        { "key": "energy_fee", "value": 420 },
        ...
    ]
}
```

**Backend method:**

```go
GetChainParameters() []ChainParameter
```

**ChainParameter struct:**

```go
type ChainParameter struct {
    Key   string `json:"key"`
    Value int64  `json:"value"`
}
```

Implementation iterates `DynamicProperties.props` and returns each key-value pair. The DynamicProperties struct needs a new `All() map[string]int64` method exposing the full props map (read-only copy).

#### 3.D.3 POST /wallet/listwitnesses

Returns all witnesses in the witness index.

**Request:** Empty body or `{}`.

**Response:**
```json
{
    "witnesses": [
        {
            "address": "41...",
            "voteCount": 123456,
            "url": "https://example.com",
            "isJobs": true
        }
    ]
}
```

**Backend method:**

```go
ListWitnesses() ([]*WitnessInfo, error)
```

**WitnessInfo struct:**

```go
type WitnessInfo struct {
    Address   string `json:"address"`
    VoteCount int64  `json:"voteCount"`
    URL       string `json:"url"`
    IsJobs    bool   `json:"isJobs"`
}
```

`IsJobs` is true if the witness is in the active witness set. Implementation reads the witness index from rawdb, reads each witness's data, and cross-references with the active witness list.

#### 3.D.4 POST /wallet/getnextmaintenancetime

Returns the timestamp of the next maintenance period.

**Request:** Empty body or `{}`.

**Response:**
```json
{
    "num": 1712764800000
}
```

**Backend method:**

```go
NextMaintenanceTime() int64
```

Already exists on `BlockChain`. Expose via Backend interface.

## 4. Backend Interface Changes

The `Backend` interface in `internal/tronapi/backend.go` grows with these new methods:

```go
type Backend interface {
    // Existing methods (unchanged):
    CurrentBlock() *types.Block
    GetBlockByNumber(number uint64) (*types.Block, error)
    GetAccount(addr common.Address) (*types.Account, error)
    BroadcastTransaction(tx *types.Transaction) error
    GetNodeInfo() *NodeInfo
    PendingTransactionCount() int
    GetContract(addr common.Address) (*contractpb.SmartContract, error)
    TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*TriggerResult, error)

    // Phase 7 - Transaction queries:
    GetTransactionByID(txHash common.Hash) (*corepb.Transaction, error)
    GetTransactionInfoByID(txHash common.Hash) (*corepb.TransactionInfo, error)
    GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error)

    // Phase 7 - Block queries:
    GetBlockByHash(hash common.Hash) (*types.Block, error)
    GetBlocksByRange(start, end uint64) ([]*types.Block, error)

    // Phase 7 - Transaction building:
    BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error)
    BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte,
        feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error)
    BuildTriggerContractTransaction(owner, contract common.Address, data []byte,
        feeLimit int64, callValue int64) (*corepb.Transaction, *TriggerResult, error)
    EstimateEnergy(owner, contract common.Address, data []byte) (int64, error)

    // Phase 7 - Resource & chain queries:
    GetAccountResource(addr common.Address) (*AccountResource, error)
    GetChainParameters() []ChainParameter
    ListWitnesses() ([]*WitnessInfo, error)
    NextMaintenanceTime() int64
}
```

## 5. API Route Registration

All new routes are registered in `api.go`'s `RegisterRoutes`:

```go
func (api *API) RegisterRoutes(mux *http.ServeMux) {
    // Existing
    mux.HandleFunc("/wallet/getnowblock", api.getNowBlock)
    mux.HandleFunc("/wallet/getblockbynum", api.getBlockByNum)
    mux.HandleFunc("/wallet/getaccount", api.getAccount)
    mux.HandleFunc("/wallet/broadcasttransaction", api.broadcastTransaction)
    mux.HandleFunc("/wallet/getnodeinfo", api.getNodeInfo)
    mux.HandleFunc("/wallet/gettransactioncountinpool", api.getTxPoolCount)
    mux.HandleFunc("/wallet/getcontract", api.getContract)
    mux.HandleFunc("/wallet/triggerconstantcontract", api.triggerConstantContract)

    // Phase 7 - Transaction building
    mux.HandleFunc("/wallet/createtransaction", api.createTransaction)
    mux.HandleFunc("/wallet/deploycontract", api.deployContract)
    mux.HandleFunc("/wallet/triggersmartcontract", api.triggerSmartContract)
    mux.HandleFunc("/wallet/estimateenergy", api.estimateEnergy)

    // Phase 7 - Transaction queries
    mux.HandleFunc("/wallet/gettransactionbyid", api.getTransactionByID)
    mux.HandleFunc("/wallet/gettransactioninfobyid", api.getTransactionInfoByID)
    mux.HandleFunc("/wallet/gettransactioninfobyblocknum", api.getTransactionInfoByBlockNum)

    // Phase 7 - Block queries
    mux.HandleFunc("/wallet/getblockbyid", api.getBlockByID)
    mux.HandleFunc("/wallet/getblockbylimitnext", api.getBlockByLimitNext)

    // Phase 7 - Resource & chain queries
    mux.HandleFunc("/wallet/getaccountresource", api.getAccountResource)
    mux.HandleFunc("/wallet/getchainparameters", api.getChainParameters)
    mux.HandleFunc("/wallet/listwitnesses", api.listWitnesses)
    mux.HandleFunc("/wallet/getnextmaintenancetime", api.getNextMaintenanceTime)
}
```

## 6. Data Flow Summary

### Block insertion flow (with Phase 7 additions marked with **):

```
BlockChain.InsertBlock(block)
  -> state.New(parentRoot)
  -> ProcessBlock(statedb, dynProps, block)
       -> for each tx:
            ApplyTransaction(statedb, dynProps, tx, ...)
              -> actuator.Validate()
              -> consumeBandwidth() -> **returns BandwidthResult**
              -> actuator.Execute()
                   -> [for VM txs] evm.Create/Call
                        -> **EVM accumulates Logs**
                   -> **returns Result with EnergyUsed, Logs, ContractResult, etc.**
              -> **returns *actuator.Result (not just fee)**
            **buildTransactionInfo(tx, result, blockNum, blockTime, ...)**
       -> **returns []*TransactionInfo**
  -> maintenance (if needed)
  -> statedb.Commit()
  -> dynProps.Flush()
  -> rawdb.WriteBlock()
  -> **for each txInfo: rawdb.WriteTransactionInfo(txID, info)**
  -> **for each tx: rawdb.WriteTransactionIndex(txHash, blockNum)**
  -> **rawdb.WriteTransactionInfosByBlock(blockNum, txInfos)**
  -> WriteHeadBlockHash()
```

### API query flow:

```
HTTP POST /wallet/gettransactioninfobyid { "value": "abc..." }
  -> api.getTransactionInfoByID()
       -> backend.GetTransactionInfoByID(hash)
            -> rawdb.ReadTransactionInfo(db, hash[:])
       -> marshalTronJSON(info)
  -> JSON response
```

### Transaction building flow:

```
HTTP POST /wallet/triggersmartcontract { owner, contract, function_selector, parameter, fee_limit }
  -> api.triggerSmartContract()
       -> parse request, build calldata from function_selector + parameter
       -> backend.BuildTriggerContractTransaction(owner, contract, data, feeLimit, callValue)
            -> buildTransaction(headBlockNum, headHash, ..., TriggerSmartContract, ...)
            -> TriggerConstantContract(owner, contract, data, 30_000_000)
            -> return (tx proto, trigger result)
       -> JSON response with transaction + energy_used + constant_result
```

## 7. Storage Key Summary

| Key pattern | Value | Purpose |
|---|---|---|
| `ti-<32-byte txHash>` | proto TransactionInfo | Per-tx receipt/info lookup |
| `tib-<8-byte blockNum BE>` | proto TransactionRet | Per-block batch of all tx infos |
| `tx-<32-byte txHash>` | 8-byte blockNum BE | Tx hash to block number index |

## 8. Files Changed / Created

| File | Change type | Description |
|---|---|---|
| `vm/log.go` | **New** | `Log` struct definition |
| `vm/evm.go` | Modified | Add `Logs []Log` field, `LogSnapshot()`, `RevertLogs()` methods, call revert in Call/Create/Create2 |
| `vm/instructions.go` | Modified | `makeLog()` captures topics and data into `evm.Logs` |
| `actuator/actuator.go` | Modified | Extend `Result` struct with energy, net, logs, contractResult, contractAddress fields |
| `actuator/vm_actuator.go` | Modified | Populate extended `Result` fields after EVM execution, add `contractRetFromError()` |
| `core/state_processor.go` | Modified | Change `ApplyTransaction` return to `*actuator.Result`, change `ProcessBlock` return to `[]*TransactionInfo`, add `buildTransactionInfo()`, refactor `consumeBandwidth` return |
| `core/bandwidth.go` | Modified | `consumeBandwidth` returns `*BandwidthResult` |
| `core/blockchain.go` | Modified | `InsertBlock` persists TransactionInfos and tx indexes after ProcessBlock |
| `core/rawdb/schema.go` | Modified | Add `txInfoBlockPrefix`, `txInfoKey()`, `txInfoBlockKey()` functions |
| `core/rawdb/accessors_txinfo.go` | **New** | `WriteTransactionInfo`, `ReadTransactionInfo`, `WriteTransactionInfosByBlock`, `ReadTransactionInfosByBlock`, `WriteTransactionIndex`, `ReadTransactionIndex` |
| `internal/tronapi/backend.go` | Modified | Extend `Backend` interface with 13 new methods, add `AccountResource`, `ChainParameter`, `WitnessInfo` types |
| `internal/tronapi/txbuilder.go` | **New** | `buildTransaction()` common tx builder |
| `internal/tronapi/api.go` | Modified | Register 13 new route handlers |
| `core/tron_backend.go` | Modified | Implement all new Backend methods |
| `core/state/dynamic_properties.go` | Modified | Add `All() map[string]int64` method |

## 9. Testing Strategy

1. **Unit tests for rawdb accessors** (`core/rawdb/accessors_txinfo_test.go`): Write/read TransactionInfo by txID, write/read by block number, write/read tx index. Use `rawdb.NewMemoryDatabase()`.

2. **Unit tests for log collection** (`vm/instructions_test.go`): Execute LOG0-LOG4 opcodes and verify `evm.Logs` contains correct address, topics, and data. Verify logs are reverted on REVERT.

3. **Integration test for TransactionInfo generation** (`core/state_processor_test.go`): Build a block with a transfer tx, process it, verify the returned TransactionInfo has correct fee, net_usage, block metadata. Build a block with a contract creation, verify logs and contract_address in the info.

4. **API handler tests** (`internal/tronapi/api_test.go`): Test each endpoint with mock Backend. Verify request parsing, response format matches java-tron.

5. **Transaction builder tests** (`internal/tronapi/txbuilder_test.go`): Verify ref_block_bytes, ref_block_hash, expiration are computed correctly from head block. Verify the built transaction can be marshaled to valid JSON.

## 10. Migration Notes

- The `txInfoPrefix` key `"ti-"` is already declared in `schema.go` but unused. This phase activates it.
- The `txPrefix` key `"tx-"` is declared and used for block-number indexing. This phase repurposes it from "unused" to tx-hash-to-block-number mapping. Verify no existing code writes to `tx-<hash>` keys.
- `ProcessBlock` and `ApplyTransaction` signature changes affect callers: `InsertBlock` in `blockchain.go` and `BuildBlock` in the block producer. Both must be updated to handle the new return types.
- Non-VM actuators should set `ContractRet = 1` (SUCCESS) on success for consistent TransactionInfo generation.
