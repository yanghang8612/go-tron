# Phase 5: P2P Networking — Design Spec

## Goal

Enable multi-node go-tron networks: nodes discover each other via seed nodes, sync block history, and propagate new blocks and transactions in real time.

## Architecture

```
p2p/                          # Transport layer
  server.go                   # TCP listener + dial, peer lifecycle
  peer.go                     # Single peer: read/write goroutines, message I/O
  message.go                  # Message interface, type codes, encode/decode

net/                          # Protocol layer
  handler.go                  # TronHandler: routes messages to services
  sync.go                     # SyncService: chain summary → block fetch
  fetcher.go                  # FetchService: manages pending fetch requests
  broadcaster.go              # BroadcastService: inventory gossip for new blocks/txs
```

**Transport** is plain TCP with length-prefixed framing. **Protocol** handles TRON-specific logic (handshake, sync, gossip). This separation allows swapping transport later (e.g., libp2p for java-tron interop) without touching protocol logic.

## Wire Format

```
[4 bytes big-endian payload length][1 byte message type][protobuf payload]
```

- Max message size: 10 MB
- Payload length includes the type byte
- Empty payload (ping/pong): length=1, type byte only

## Message Types

Codes match java-tron for protocol-level compatibility:

| Code | Name | Protobuf | Purpose |
|------|------|----------|---------|
| 0x01 | TRX | `Transaction` | Single transaction data |
| 0x02 | BLOCK | `Block` | Single block data |
| 0x06 | INVENTORY | `Inventory` | Announce available hashes (TRX or BLOCK) |
| 0x07 | FETCH_INV_DATA | `Inventory` | Request data by hashes |
| 0x08 | SYNC_BLOCK_CHAIN | `BlockInventory` | Send chain summary for sync |
| 0x09 | CHAIN_INVENTORY | `ChainInventory` | Respond with missing block IDs |
| 0x20 | HELLO | `HelloMessage` | Handshake with chain state |
| 0x21 | DISCONNECT | `DisconnectMessage` | Graceful disconnect with reason |
| 0x22 | PING | (empty) | Keep-alive request |
| 0x23 | PONG | (empty) | Keep-alive response |

## Protobuf Additions

Add to `proto/core/Tron.proto`:

```protobuf
message HelloMessage {
  Endpoint from = 1;
  int32 version = 2;
  int64 timestamp = 3;

  message BlockId {
    bytes hash = 1;
    int64 number = 2;
  }
  BlockId genesisBlockId = 4;
  BlockId solidBlockId = 5;
  BlockId headBlockId = 6;
}

message DisconnectMessage {
  ReasonCode reason = 1;
}

message Endpoint {
  bytes address = 1;
  int32 port = 2;
  bytes nodeId = 3;
}

enum ReasonCode {
  REQUESTED = 0;
  BAD_PROTOCOL = 2;
  TOO_MANY_PEERS = 4;
  DUPLICATE_PEER = 5;
  INCOMPATIBLE_VERSION = 6;
  INCOMPATIBLE_CHAIN = 7;
  SYNC_FAIL = 8;
  PING_TIMEOUT = 9;
}
```

Existing `Inventory`, `BlockInventory`, `ChainInventory` messages from Tron.proto are reused directly.

## Peer Connection Lifecycle

```
Dial/Accept → TCP connected
    ↓
Send/Receive HelloMessage
    ↓
Validate: genesis match? version ok? not duplicate?
    ↓ (fail → send DisconnectMessage → close)
Active peer: sync + propagation enabled
    ↓
Ping every 30s, timeout at 90s of no response
    ↓
Disconnect: send DisconnectMessage → close → cleanup
```

### HelloMessage validation rules

1. `genesisBlockId.hash` must match our genesis — otherwise `INCOMPATIBLE_CHAIN`
2. `version` must match our P2P version — otherwise `INCOMPATIBLE_VERSION`
3. Peer address must not already be connected — otherwise `DUPLICATE_PEER`
4. Total peer count must not exceed max — otherwise `TOO_MANY_PEERS`

After successful handshake, compare `headBlockId.number` to decide who syncs from whom.

## Block Sync Protocol

Follows java-tron's chain-summary approach:

### Phase 1: Chain Summary Exchange

The syncing node builds a **chain summary** — a list of block IDs sampled from its chain at exponentially increasing intervals from the head:

```
head, head-1, head-2, head-4, head-8, head-16, ..., genesis
```

This is sent as `SYNC_BLOCK_CHAIN` (code 0x08).

The peer finds the highest common block, then responds with `CHAIN_INVENTORY` (code 0x09) containing:
- Up to 2000 sequential block IDs starting after the common block
- `remain_num`: total blocks still available after this batch

### Phase 2: Block Fetch

The syncing node sends `FETCH_INV_DATA` (code 0x07) with batches of block hashes from the chain inventory (up to 100 per request).

The peer responds with individual `BLOCK` messages (code 0x02).

Received blocks are inserted into the chain via `blockchain.InsertBlock()`.

### Phase 3: Repeat

If `remain_num > 0`, go back to Phase 1 with an updated chain summary. Repeat until fully synced.

### Sync state machine

```
IDLE → peer has higher head → SYNCING
SYNCING → remain_num == 0 → SYNCED
SYNCED → peer announces higher block → SYNCING
```

## Transaction and Block Propagation

After initial sync, live propagation uses an inventory-based gossip pattern:

### New block produced locally

1. Insert block into own chain
2. Send `INVENTORY(type=BLOCK, ids=[blockHash])` to all peers
3. Peers who don't have it reply with `FETCH_INV_DATA`
4. Respond with `BLOCK` message

### New transaction received via API

1. Add to local txpool
2. Send `INVENTORY(type=TRX, ids=[txHash])` to all peers
3. Peers reply with `FETCH_INV_DATA` if interested
4. Respond with `TRX` message

### Deduplication

Both blocks and transactions use a seen-cache (LRU, 10K entries for blocks, 50K for transactions) to prevent re-broadcasting.

## P2P Server

```go
type Server struct {
    config    ServerConfig
    listener  net.Listener
    peers     map[string]*Peer  // addr → peer
    handler   Handler           // protocol message handler
    mu        sync.RWMutex
    quit      chan struct{}
}

type ServerConfig struct {
    ListenAddr string
    MaxPeers   int
    SeedNodes  []string  // "host:port" list
    PrivateKey *ecdsa.PrivateKey  // node identity (optional, for future use)
}
```

- `Start()`: listen for inbound connections + dial seed nodes
- `Stop()`: disconnect all peers, close listener
- Implements `node.Lifecycle`

## Peer

```go
type Peer struct {
    conn       net.Conn
    id         string              // "host:port"
    inbound    bool                // accepted vs dialed
    hello      *HelloMessage       // received handshake
    handler    Handler
    writeCh    chan Message         // buffered write channel
    quit       chan struct{}
}
```

- Two goroutines per peer: `readLoop()` and `writeLoop()`
- `readLoop`: reads framed messages, decodes, dispatches to handler
- `writeLoop`: reads from `writeCh`, encodes, writes to connection
- Send via `peer.Send(msg)` which pushes to `writeCh`

## TronHandler (protocol layer)

```go
type TronHandler struct {
    chain       *core.BlockChain
    pool        *txpool.TxPool
    engine      *dpos.DPoS
    server      *p2p.Server
    syncService *SyncService
    broadcaster *BroadcastService
}
```

- Implements `p2p.Handler` interface
- `OnPeerConnected(peer)`: send HelloMessage, start handshake
- `OnPeerDisconnected(peer)`: cleanup sync state
- `OnMessage(peer, msg)`: route to appropriate service based on message type

## Config Additions

```go
// node/config.go
type Config struct {
    DataDir     string
    P2PPort     int
    HTTPPort    int
    JSONRPCPort int
    SeedNodes   []string  // new
    MaxPeers    int       // new, default 30
}
```

CLI flags:
- `--p2p.port` (existing, default 18888)
- `--seednode` (new, repeatable, "host:port")
- `--maxpeers` (new, default 30)

## Integration with Existing Code

### Producer → Broadcaster

When the producer creates a new block, it needs to notify the broadcaster:

```go
// In producer.produceBlock(), after successful InsertBlock:
if p.broadcaster != nil {
    p.broadcaster.BroadcastBlock(block)
}
```

The producer gets a reference to the broadcaster via its constructor.

### TxPool → Broadcaster

When a transaction is received via HTTP API and added to the pool, broadcast it:

```go
// In api.broadcastTransaction(), after successful pool.Add:
if api.broadcaster != nil {
    api.broadcaster.BroadcastTx(tx)
}
```

### Received blocks from peers

```go
// In TronHandler, on BLOCK message:
err := handler.chain.InsertBlock(block)
// On success, relay inventory to other peers (excluding sender)
```

### Received transactions from peers

```go
// In TronHandler, on TRX message:
err := handler.pool.Add(tx)
// On success, relay inventory to other peers (excluding sender)
```

## Testing Strategy

1. **Unit tests** — encode/decode messages, handshake validation, chain summary generation
2. **Pipe-based integration** — two peers over `net.Pipe()`, test handshake + sync flow
3. **Multi-node integration** — start 2-3 in-process nodes, one produces blocks, others sync
4. **Error cases** — wrong genesis, version mismatch, timeout, max peers exceeded

## Out of Scope

- Java-tron libp2p transport interop (future: add libp2p transport adapter)
- Kademlia DHT peer discovery
- PBFT consensus messages (0x14, 0x34)
- Peer scoring/reputation
- Light node sync
- DNS-based discovery
- Rate limiting per peer
