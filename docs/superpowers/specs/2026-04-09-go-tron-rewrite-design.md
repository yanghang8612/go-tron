# go-tron: Java-Tron Go Rewrite Design Spec

## 1. Overview

Rewrite the TRON blockchain client (java-tron) in Go, following go-ethereum's architectural patterns. The new client (`gtron`) must be **fully wire-compatible** with existing java-tron nodes on mainnet/testnet — same protobuf messages, same P2P protocol, same consensus rules.

Strategy: **core-first, incremental**. Build a minimal viable node that can sync with the TRON network, then expand features phase by phase.

## 2. Constraints

| Item | Decision |
|------|----------|
| Module path | `github.com/tronprotocol/go-tron` |
| Go version | 1.22+ |
| Network compat | Full — must join TRON mainnet/testnet, interop with java-tron |
| Protobuf | Reuse java-tron's `.proto` definitions verbatim |
| Initial API | Basic HTTP (chain/tx queries) + JSON-RPC (Ethereum-compat) |
| Database | Pebble (default) / LevelDB, single instance with prefix schema (go-ethereum style) |
| Config format | TOML (go-ethereum convention), with genesis JSON |

## 3. Directory Structure

```
go-tron/
├── cmd/
│   └── gtron/                  # Main binary entry point
│       ├── main.go             # CLI app (urfave/cli/v2)
│       ├── config.go           # Config loading and flag definitions
│       ├── consolecmd.go       # Console commands (later)
│       └── misccmd.go          # version, license, etc.
│
├── node/                       # Node container and lifecycle
│   ├── node.go                 # Node struct: manages lifecycles, RPC, DB
│   ├── lifecycle.go            # Lifecycle interface
│   ├── config.go               # Node-level config (data dir, P2P, RPC ports)
│   └── rpcstack.go             # HTTP/WS/IPC server wiring
│
├── tron/                       # TRON protocol backend (≈ eth/ in geth)
│   ├── backend.go              # Tron struct: blockchain, txpool, consensus, handler
│   ├── handler.go              # P2P message dispatcher
│   ├── api_backend.go          # Implements tronapi.Backend interface
│   ├── config.go               # Protocol-level config
│   ├── sync.go                 # Block sync orchestration
│   └── protocols/
│       └── tron/
│           ├── protocol.go     # Protocol version, message codes
│           ├── handler.go      # Per-peer message handling
│           └── peer.go         # Peer state tracking
│
├── core/
│   ├── types/                  # Core domain types
│   │   ├── block.go            # Block, BlockHeader (wrapping protobuf)
│   │   ├── transaction.go      # Transaction, TransactionResult
│   │   ├── account.go          # Account state
│   │   ├── receipt.go          # Transaction receipt / TransactionInfo
│   │   └── log.go              # Contract event logs
│   │
│   ├── state/                  # Account state management
│   │   ├── statedb.go          # StateDB: read/write account state
│   │   ├── state_object.go     # Single account in-memory representation
│   │   ├── database.go         # State database interface
│   │   └── snapshot.go         # State snapshots for revert
│   │
│   ├── rawdb/                  # Low-level DB schema and accessors
│   │   ├── schema.go           # Key prefixes for all data types
│   │   ├── accessors_block.go  # Block read/write
│   │   ├── accessors_state.go  # State read/write
│   │   ├── accessors_chain.go  # Chain metadata (head, latest solid block)
│   │   ├── accessors_tx.go     # Transaction indexing
│   │   └── database.go         # DB open/close helpers
│   │
│   ├── txpool/                 # Transaction pool
│   │   ├── txpool.go           # Pool interface and orchestrator
│   │   ├── legacypool.go       # Standard transaction pool
│   │   └── validation.go       # Transaction validation
│   │
│   ├── blockchain.go           # BlockChain: canonical chain, insertion, reorg
│   ├── chain_makers.go         # Test helpers for generating chains
│   ├── genesis.go              # Genesis block construction
│   ├── resource.go             # Bandwidth/Energy processor
│   │
│   └── vm/                     # TVM (TRON Virtual Machine)
│       ├── evm.go              # Main VM executor (forked from go-ethereum)
│       ├── interpreter.go      # Opcode interpreter
│       ├── instructions.go     # Opcode implementations
│       ├── jump_table.go       # Opcode dispatch table
│       ├── contracts.go        # Standard precompiled contracts (0x01-0x09)
│       ├── contracts_tron.go   # TRON-specific precompiled (0x100000x)
│       ├── gas_table.go        # Energy cost table
│       └── program.go          # Execution context
│
├── actuator/                   # Transaction executors (TRON-specific)
│   ├── actuator.go             # Actuator interface + registry/factory
│   ├── transfer.go             # TransferContract (TRX transfer)
│   ├── account.go              # CreateAccount, UpdateAccount, SetAccountId
│   ├── witness.go              # WitnessCreate, WitnessUpdate
│   ├── vote.go                 # VoteWitness
│   ├── freeze.go               # FreezeBalanceV2, UnfreezeBalanceV2
│   ├── resource.go             # DelegateResource, UnDelegateResource
│   ├── withdraw.go             # WithdrawBalance, WithdrawExpireUnfreeze
│   ├── proposal.go             # ProposalCreate, ProposalApprove, ProposalDelete
│   ├── asset.go                # TRC-10 actuators (Phase 3)
│   ├── market.go               # Market/Exchange actuators (Phase 4)
│   ├── vm.go                   # VMActuator — routes to core/vm (Phase 2)
│   └── shield.go               # ShieldedTransfer (Phase 4)
│
├── consensus/
│   ├── consensus.go            # Engine interface
│   ├── dpos/
│   │   ├── dpos.go             # DPoS engine implementation
│   │   ├── slot.go             # Slot calculation (3s intervals)
│   │   ├── schedule.go         # Witness scheduling (round-robin)
│   │   ├── maintenance.go      # Maintenance period (witness election, vote tally)
│   │   ├── reward.go           # Block rewards, standby allowance
│   │   └── state.go            # Consensus state machine
│   └── pbft/                   # PBFT overlay (Phase 3)
│       ├── pbft.go
│       └── message.go
│
├── p2p/                        # Peer-to-peer networking
│   ├── server.go               # P2P server (listen, accept, dial)
│   ├── peer.go                 # Peer connection
│   ├── message.go              # Message encoding/decoding (protobuf-based)
│   ├── discover/               # Node discovery
│   └── protocols/              # Sub-protocol definitions
│
├── trondb/                     # Database abstraction (≈ ethdb/)
│   ├── database.go             # Database, KeyValueStore interfaces
│   ├── pebble/                 # Pebble backend
│   ├── leveldb/                # LevelDB backend
│   └── memorydb/               # In-memory backend (testing)
│
├── params/                     # Chain parameters and configuration
│   ├── config.go               # ChainConfig, DPoS params, fork definitions
│   ├── genesis.go              # Genesis allocation and config
│   ├── protocol_params.go      # Protocol constants (block interval, max witnesses)
│   ├── network.go              # Network IDs, bootnodes
│   └── dynamic_properties.go   # Runtime-adjustable parameters (energy fee, etc.)
│
├── internal/
│   └── tronapi/                # Public API implementation
│       ├── api.go              # HTTP API handlers (chain queries, tx broadcast)
│       ├── backend.go          # Backend interface (what API depends on)
│       └── jsonrpc.go          # JSON-RPC adapter (eth_* compatible subset)
│
├── crypto/                     # Cryptography
│   ├── crypto.go               # Key generation, signing, recovery
│   ├── signature.go            # ECDSA signature handling
│   └── address.go              # TRON address encoding (Base58Check, hex)
│
├── common/                     # Shared utilities
│   ├── address.go              # Address type (21 bytes)
│   ├── hash.go                 # Hash types (SHA-256, Keccak-256)
│   ├── bytes.go                # Byte manipulation
│   └── math.go                 # StrictMath equivalent
│
├── accounts/                   # Account/wallet management (later phases)
│
├── log/                        # Structured logging (slog-based)
│
├── metrics/                    # Prometheus metrics
│
├── proto/                      # Protobuf definitions (copied from java-tron)
│   ├── core/
│   │   ├── Tron.proto
│   │   ├── Discover.proto
│   │   └── contract/
│   │       ├── common.proto
│   │       ├── transfer_contract.proto
│   │       ├── account_contract.proto
│   │       ├── witness_contract.proto
│   │       ├── balance_contract.proto
│   │       ├── smart_contract.proto
│   │       ├── asset_issue_contract.proto
│   │       ├── proposal_contract.proto
│   │       ├── exchange_contract.proto
│   │       ├── market_contract.proto
│   │       ├── shield_contract.proto
│   │       └── storage_contract.proto
│   └── api/
│       ├── api.proto
│       └── zksnark.proto
│
├── build/                      # Build tooling
│   ├── ci.go                   # CI build script
│   └── Dockerfile
│
├── Makefile                    # Build targets
├── go.mod
└── go.sum
```

## 4. Key Interfaces

### 4.1 Lifecycle (node/lifecycle.go)

Directly follows go-ethereum's pattern. Every service (Tron backend, P2P, miner) implements this.

```go
type Lifecycle interface {
    Start() error
    Stop() error
}
```

### 4.2 Consensus Engine (consensus/consensus.go)

Extends go-ethereum's Engine interface with TRON DPoS specifics.

```go
type Engine interface {
    // From go-ethereum
    Author(header *types.Header) (common.Address, error)
    VerifyHeader(chain ChainReader, header *types.Header) error
    VerifyHeaders(chain ChainReader, headers []*types.Header) (chan<- struct{}, <-chan error)
    Prepare(chain ChainReader, header *types.Header) error
    Finalize(chain ChainReader, header *types.Header, statedb *state.StateDB,
        txs []*types.Transaction) error
    Seal(chain ChainReader, block *types.Block,
        results chan<- *types.Block, stop <-chan struct{}) error

    // TRON DPoS specific
    GetScheduledWitness(slot int64) (common.Address, error)
    IsInMaintenance(timestamp int64) bool
    UpdateWitnessSchedule(statedb *state.StateDB) error
    CalcBlockReward(statedb *state.StateDB, witness common.Address) (*big.Int, error)
}

type ChainReader interface {
    CurrentBlock() *types.Block
    GetBlockByNumber(number uint64) *types.Block
    GetBlockByHash(hash common.Hash) *types.Block
    GetHeader(hash common.Hash, number uint64) *types.Header
    Config() *params.ChainConfig
    DynamicProperties() *params.DynamicProperties
}
```

### 4.3 Actuator (actuator/actuator.go)

TRON-specific pattern — no equivalent in go-ethereum. Each contract type maps to one actuator.

```go
type Actuator interface {
    Validate(ctx *Context) error
    Execute(ctx *Context) (*types.TransactionResult, error)
}

type Context struct {
    StateDB    *state.StateDB
    Block      *types.Block
    Tx         *types.Transaction
    ChainConfig *params.ChainConfig
    DynProps   *params.DynamicProperties
}

// Registry maps ContractType enum -> Actuator constructor
var registry = map[int32]func() Actuator{
    ContractType_TransferContract:       func() Actuator { return &TransferActuator{} },
    ContractType_AccountCreateContract:  func() Actuator { return &CreateAccountActuator{} },
    // ...
}

func CreateActuator(tx *types.Transaction) (Actuator, error)
```

### 4.4 Database (trondb/database.go)

Follows go-ethereum's ethdb interface.

```go
type KeyValueReader interface {
    Has(key []byte) (bool, error)
    Get(key []byte) ([]byte, error)
}

type KeyValueWriter interface {
    Put(key []byte, value []byte) error
    Delete(key []byte) error
}

type KeyValueStore interface {
    KeyValueReader
    KeyValueWriter
    NewBatch() Batch
    NewIterator(prefix []byte, start []byte) Iterator
    Stat() (string, error)
    Compact(start []byte, limit []byte) error
    io.Closer
}

type Database interface {
    KeyValueStore
}
```

### 4.5 API Backend (internal/tronapi/backend.go)

What the API layer depends on — implemented by `tron.Tron` backend.

```go
type Backend interface {
    ChainConfig() *params.ChainConfig
    CurrentBlock() *types.Block
    GetBlockByNumber(number uint64) (*types.Block, error)
    GetBlockByHash(hash common.Hash) (*types.Block, error)
    GetTransactionByHash(hash common.Hash) (*types.Transaction, error)
    GetAccount(addr common.Address) (*types.Account, error)
    SendTransaction(tx *types.Transaction) error
    SuggestEnergyPrice() (*big.Int, error)
    GetLatestSolidifiedBlockNumber() (uint64, error)
}
```

### 4.6 StateDB (core/state/statedb.go)

Manages in-memory account state with snapshot/revert support.

```go
type StateDB struct {
    db         Database
    stateRoot  common.Hash
    accounts   map[common.Address]*stateObject
    journal    *journal          // for revert
    // ...
}

func (s *StateDB) GetAccount(addr common.Address) *types.Account
func (s *StateDB) GetBalance(addr common.Address) *big.Int
func (s *StateDB) SubBalance(addr common.Address, amount *big.Int) error
func (s *StateDB) AddBalance(addr common.Address, amount *big.Int)
func (s *StateDB) GetFrozenBalance(addr common.Address, resourceType int) *big.Int
func (s *StateDB) GetBandwidth(addr common.Address) int64
func (s *StateDB) GetEnergy(addr common.Address) int64
func (s *StateDB) SetCode(addr common.Address, code []byte)
func (s *StateDB) GetCode(addr common.Address) []byte
func (s *StateDB) Commit() (common.Hash, error)
func (s *StateDB) Snapshot() int
func (s *StateDB) RevertToSnapshot(id int)
```

## 5. Core Type Mapping

java-tron uses protobuf "capsule" wrappers. go-tron wraps protobuf types in Go structs with helper methods, using composition instead of inheritance.

| java-tron | go-tron | Notes |
|-----------|---------|-------|
| `BlockCapsule` | `types.Block` | Wraps `protocol.Block` protobuf |
| `TransactionCapsule` | `types.Transaction` | Wraps `protocol.Transaction` protobuf |
| `AccountCapsule` | `types.Account` / `state.stateObject` | Split: wire format vs in-memory state |
| `WitnessCapsule` | `types.Witness` | Wraps `protocol.Witness` |
| `ProposalCapsule` | `types.Proposal` | Wraps `protocol.Proposal` |

Each type embeds its protobuf message and exposes typed accessors:

```go
type Block struct {
    pb *protocol.Block  // raw protobuf
    // cached derived fields
    hash     common.Hash
    size     int
}

func (b *Block) Number() uint64       { return b.pb.BlockHeader.RawData.Number }
func (b *Block) Timestamp() int64     { return b.pb.BlockHeader.RawData.Timestamp }
func (b *Block) ParentHash() common.Hash { ... }
func (b *Block) Transactions() []*Transaction { ... }
func (b *Block) Proto() *protocol.Block { return b.pb }
func (b *Block) Hash() common.Hash { ... }
```

## 6. Database Schema (core/rawdb/schema.go)

Single Pebble/LevelDB instance with prefix-based key design:

```go
var (
    // Chain metadata
    headBlockKey           = []byte("LastBlock")
    headSolidBlockKey      = []byte("LastSolidBlock")
    dynamicPropertiesPrefix = []byte("dp-")

    // Blocks
    blockPrefix       = []byte("b-")    // b-<number> -> Block protobuf
    blockHashPrefix   = []byte("bh-")   // bh-<hash> -> block number
    blockHeaderPrefix = []byte("bH-")   // bH-<number> -> BlockHeader

    // Transactions
    txPrefix      = []byte("tx-")       // tx-<hash> -> Transaction
    txInfoPrefix  = []byte("ti-")       // ti-<hash> -> TransactionInfo

    // Account state
    accountPrefix      = []byte("a-")   // a-<address> -> Account protobuf
    accountIndexPrefix = []byte("ai-")  // ai-<id> -> address

    // Witness
    witnessPrefix     = []byte("w-")    // w-<address> -> Witness
    witnessScheduleKey = []byte("ws")   // current schedule
    votesPrefix       = []byte("v-")    // v-<address> -> Votes

    // Smart contracts
    codePrefix        = []byte("c-")    // c-<address> -> bytecode
    abiPrefix         = []byte("ab-")   // ab-<address> -> ABI
    contractPrefix    = []byte("ct-")   // ct-<address> -> SmartContract
    storagePrefix     = []byte("s-")    // s-<address><key> -> value

    // Resources / Delegation
    delegationPrefix  = []byte("dl-")   // dl-<address> -> DelegatedResource
    delegationIndexPrefix = []byte("di-") // di-<address> -> index

    // Proposals
    proposalPrefix    = []byte("p-")    // p-<id> -> Proposal

    // Assets (Phase 3)
    assetPrefix       = []byte("as-")   // as-<id> -> AssetIssue
)
```

## 7. Bootstrap Sequence

```
cmd/gtron/main.go
├── app.Run(os.Args)                      // urfave/cli/v2
└── gtron(ctx *cli.Context)
    ├── loadConfig(ctx) *Config
    ├── node.New(nodeCfg) *Node
    │   ├── Open data directory
    │   ├── Init RPC server
    │   └── Init P2P server
    ├── tron.New(stack, tronCfg) *Tron
    │   ├── trondb.NewPebbleDB(path)          // Open chain database
    │   ├── params.LoadGenesis(cfg)           // Load genesis / chain config
    │   ├── core.NewBlockChain(db, config, engine)
    │   │   ├── Load head block from rawdb
    │   │   ├── Init state from head
    │   │   └── Validate chain integrity
    │   ├── dpos.New(config, blockchain)      // Create DPoS engine
    │   ├── txpool.New(blockchain, config)    // Create tx pool
    │   ├── handler.New(blockchain, txpool, p2p)  // P2P message handler
    │   └── Register APIs and Lifecycle
    ├── startNode(stack)
    │   ├── stack.Start()                     // Start all lifecycles
    │   │   ├── P2P server.Start()
    │   │   ├── Tron.Start()
    │   │   │   ├── blockchain.Start()
    │   │   │   ├── txpool.Start()
    │   │   │   ├── handler.Start()           // Begin syncing
    │   │   │   └── dpos.Start()              // Begin block production (if witness)
    │   │   └── RPC servers.Start()
    │   └── Setup signal handlers
    └── stack.Wait()                          // Block until SIGINT/SIGTERM
```

## 8. P2P Wire Compatibility

TRON P2P uses protobuf messages over TCP (via libp2p). go-tron must implement the same message types:

| Message | Code | Description |
|---------|------|-------------|
| HelloMessage | 0x01 | Handshake (node info, genesis, head block) |
| PingMessage | 0x02 | Keep-alive ping |
| PongMessage | 0x03 | Keep-alive pong |
| DisconnectMessage | 0x04 | Graceful disconnect |
| SyncBlockChainMessage | 0x05 | Request chain sync (send block IDs) |
| BlockInventoryMessage | 0x06 | Respond with block inventory |
| InventoryMessage | 0x07 | Announce available blocks/txs |
| FetchInvDataMessage | 0x08 | Request blocks/txs by hash |
| BlockMessage | 0x09 | Single block propagation |
| TransactionMessage | 0x0A | Single transaction |
| TransactionsMessage | 0x0B | Batch transactions |
| PbftCommitMessage | 0x0C | PBFT consensus (Phase 3) |

All messages are serialized as protobuf with a type-length-value envelope matching java-tron's wire format.

## 9. DPoS Consensus Rules

Must match java-tron exactly for network compatibility:

- **Block interval**: 3 seconds (`BLOCK_PRODUCED_INTERVAL = 3000ms`)
- **Slot calculation**: `slot = (timestamp - genesisTimestamp) / 3000`
- **Active witnesses**: Top N by votes (max 127, `MAX_ACTIVE_WITNESS_NUM`)
- **Scheduling**: Round-robin across active witness list, single repeat per round
- **Solidification**: Block is solid when confirmed by 2/3+ of active witnesses
- **Maintenance period**: Every 6 hours (adjustable via proposal)
  - Re-tally votes, update witness list, distribute standby allowance
  - Update dynamic properties
- **Block reward**: 16 TRX per block (default), witness top-127 gets 160 TRX
- **Standby allowance**: 115,200 TRX per maintenance period

## 10. Resource Model (Bandwidth & Energy)

### Bandwidth
- Each transaction consumes bandwidth proportional to its serialized size
- Free bandwidth: 600 per account per day (recovers linearly over 24h window)
- Frozen bandwidth: From FreezeBalanceV2 for BANDWIDTH
- If insufficient bandwidth, deduct TRX at `transactionFee` sun/byte

### Energy
- Smart contract execution consumes energy
- Frozen energy: From FreezeBalanceV2 for ENERGY
- If insufficient energy, deduct TRX at `energyFee` sun/energy-unit
- Dynamic energy: scales based on network-wide usage (when enabled)

### Implementation
```go
// core/resource.go
type ResourceProcessor struct {
    statedb *state.StateDB
    dynProps *params.DynamicProperties
}

func (r *ResourceProcessor) ConsumeBandwidth(tx *types.Transaction, account common.Address) error
func (r *ResourceProcessor) ConsumeEnergy(account common.Address, energyUsed int64) error
func (r *ResourceProcessor) RecoverBandwidth(account common.Address, now int64)
func (r *ResourceProcessor) RecoverEnergy(account common.Address, now int64)
```

## 11. Phased Implementation Plan

### Phase 1: Minimal Viable Node

Goal: A node that can sync blocks from TRON testnet, validate them, and maintain state.

1. **Project scaffold** — go.mod, Makefile, protobuf code generation, CI
2. **common/** — Address (21-byte), Hash (SHA-256), byte utilities
3. **crypto/** — secp256k1 signing/recovery, Base58Check address encoding
4. **proto/** — Copy .proto files from java-tron, generate Go code
5. **core/types/** — Block, Transaction, Account wrappers around protobuf
6. **trondb/** — Database interfaces + Pebble backend
7. **core/rawdb/** — Schema definitions, block/tx/account accessors
8. **params/** — ChainConfig, genesis, dynamic properties, mainnet/testnet/nile bootnodes
9. **core/state/** — StateDB with account read/write, snapshot/revert
10. **consensus/dpos/** — Slot calculation, witness scheduling, header verification, maintenance
11. **actuator/** — Core actuators: Transfer, CreateAccount, FreezeV2, UnfreezeV2, WitnessCreate, VoteWitness, WithdrawBalance
12. **core/resource.go** — Bandwidth/Energy consumption and recovery
13. **core/blockchain.go** — Block insertion, validation, chain management, reorg handling
14. **p2p/** — TCP transport, protobuf message codec, peer management
15. **tron/protocols/** — Handshake, block sync, transaction relay, inventory
16. **tron/backend.go** — Tron service: wire everything together
17. **node/** — Node container, lifecycle management
18. **cmd/gtron/** — CLI entry point, config loading
19. **internal/tronapi/** — Basic HTTP API: GetBlock, GetTransaction, GetAccount, BroadcastTransaction
20. **internal/tronapi/jsonrpc.go** — JSON-RPC: eth_blockNumber, eth_getBlockByNumber, eth_getBalance, eth_sendRawTransaction

### Phase 2: Smart Contracts (TVM)

Goal: Execute smart contracts, support TRC-20 tokens.

1. Fork go-ethereum's `core/vm/` as `core/vm/`
2. Replace gas model with TRON energy model
3. Implement TRON-specific precompiled contracts (0x09, 0x0a, 0x1000001-0x1000015)
4. Implement VMActuator (actuator/vm.go)
5. Add contract storage to rawdb (storage prefix)
6. Add contract-related API endpoints
7. JSON-RPC: eth_call, eth_estimateGas, eth_getCode, eth_getStorageAt

### Phase 3: Complete Features

Goal: Full transaction type support, governance.

1. Remaining actuators: Asset (TRC-10), Proposal, AccountPermissionUpdate, UpdateBrokerage, UpdateSetting, ClearABI
2. DelegateResource / UnDelegateResource actuators
3. CancelAllUnfreezeV2 actuator
4. Governance proposal system (create, approve, delete, auto-apply at maintenance)
5. PBFT consensus overlay
6. Full gRPC API (Wallet service)
7. Complete HTTP API parity

### Phase 4: Advanced Features

Goal: Full feature parity with java-tron.

1. Market/Exchange system (Bancor AMM + order book)
2. Shielded transactions (zkSNARK — requires Rust FFI for librustzcash)
3. Account permission / multi-sig
4. Full JSON-RPC Ethereum compatibility
5. Metrics, monitoring, profiling
6. Performance optimization (parallel tx execution, state caching)

## 12. Testing Strategy

- **Unit tests**: Co-located `*_test.go` files, table-driven, use `trondb/memorydb` for DB
- **Integration tests**: `core/blockchain_test.go` — build chains with `chain_makers.go`, verify state transitions
- **Consensus tests**: Verify slot calculation, witness scheduling, maintenance against known java-tron outputs
- **Wire compat tests**: Serialize/deserialize protobuf messages, compare byte-for-byte with java-tron output
- **Actuator tests**: For each actuator, test validate + execute with crafted transactions
- **Sync tests**: Connect to TRON testnet, sync N blocks, verify state root matches

## 13. Key Differences from go-ethereum

| Aspect | go-ethereum | go-tron |
|--------|-------------|---------|
| Consensus | PoW/PoS (Engine) | DPoS + PBFT (Engine + maintenance) |
| Tx execution | Direct in StateProcessor | Actuator pattern (registry-based dispatch) |
| Gas model | Gas (wei-denominated) | Bandwidth + Energy (sun-denominated) |
| Account model | Nonce + Balance + CodeHash + StorageRoot | Balance + Frozen + Votes + Assets + Permissions + Resources |
| Block interval | 12s (PoS) | 3s (DPoS) |
| Serialization | RLP | Protobuf |
| Address format | 20 bytes, hex | 21 bytes (0x41 prefix), Base58Check |
| Native tokens | ETH only | TRX + TRC-10 (native asset layer) |
| Governance | EIPs + hard forks | On-chain proposals (52 parameter types) |
| Finality | Probabilistic / Casper | 2/3 witness confirmation + optional PBFT |

## 14. Dependencies (Initial)

```
github.com/cockroachdb/pebble         # Database
github.com/syndtr/goleveldb           # LevelDB alternative
google.golang.org/protobuf            # Protobuf runtime
google.golang.org/grpc                # gRPC (Phase 3)
github.com/urfave/cli/v2              # CLI framework
github.com/ethereum/go-ethereum/crypto # secp256k1 (reuse, not full geth dep)
github.com/btcsuite/btcutil           # Base58Check encoding
github.com/prometheus/client_golang   # Metrics
golang.org/x/crypto                   # Additional crypto primitives
```

## 15. Build & Run

```bash
# Build
make gtron

# Run (sync with testnet)
./build/bin/gtron --testnet --datadir ~/.gtron

# Run (mainnet)
./build/bin/gtron --datadir ~/.gtron

# Run as witness
./build/bin/gtron --witness --witness.key <private_key> --datadir ~/.gtron

# Makefile targets
make gtron          # Build main binary
make all            # Build all binaries
make test           # Run all tests
make lint           # Run linters
make proto          # Regenerate protobuf Go code
make clean          # Clean build artifacts
```
