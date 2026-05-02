# TRON P2P Interop Status

Record of verified compatibility with the reference `io.github.tronprotocol/libp2p:2.2.7`.

## ✅ VERIFIED (2026-04-12 / updated 2026-05-02)

**go-tron successfully connects to a real java-tron node and completes the libp2p handshake.**

Against a local java-tron full node (networkID=0):
- `TestJavaTronHandshake` PASSES — libp2p handshake completes; java-tron logs
  "Add peer, total channels: 1" and proactively sends TRON Hello (0x20,
  code=32) within 3s. The bare `testHandler` accepts it but does not respond
  (no TRON protocol logic). Further protocol exchange (SyncBlockChain 0x08)
  does not happen since we never reply with our Hello. Real TronHandler path
  (net/handler.go) performs full Hello exchange; that path is exercised by
  `scripts/system_test.sh` 2-node dev chain.
- `TestJavaTronDiscoverPing` PASSES — UDP discovery ping/pong round-trip

To reproduce:
```bash
# Start java-tron locally (see java-tron-local.md)
# Note: if a prior test run disconnected from this node, wait 65s for the
# libp2p DEFAULT_BAN_TIME (60s) to expire before re-running.
cd /Users/asuka/Projects/asuka/go/go-tron
JAVA_TRON_ADDR=127.0.0.1:18888 JAVA_TRON_NETWORK=0 \
  go test -tags=integration ./p2p/ -run "TestJavaTron" -v
```

## Debug journey — notable fixes and discoveries

### Fix 1: NewServer overrode NetworkID=0 to 1
The first attempt failed with DIFFERENT_VERSION because our `NewServer`
treated `NetworkID==0` as "unset" and substituted the default value 1.
Java-tron's libp2p `Parameter.nodeP2pVersion` legitimately defaults to 0
when the HOCON config omits `p2p.version`. Many local and custom deployments
run with networkId=0. Fix: removed the default; callers must set NetworkID
explicitly. (Commit c089a4f.)

### Discovery: TRON network IDs
From java-tron `config.conf` line 197:
- **Mainnet: 11111**
- **Nile: 201910292**
- **Shasta: 1**

### Discovery: peer's external IP in Hello.From.Address
java-tron sends its external IP (not 127.0.0.1 for a local node — it discovers
its public IP via `NetUtil.getExternalIpV4`). Our validation should therefore
NOT assume `From.Address == the dial address`. Currently we ignore the peer's
Hello.From.Address for routing purposes (we only use it for protocol).

### Design confirmed: two nested enums for disconnect
- `DisconnectCode` (Java class, used in HelloMessage.code): NORMAL=0,
  TOO_MANY_PEERS=1, DIFFERENT_VERSION=2, TIME_BANNED=3, DUPLICATE_PEER=4,
  MAX_CONNECTION_WITH_SAME_IP=5, UNKNOWN=256
- `DisconnectReason` (proto enum, used in P2PDisconnectMessage.reason):
  PEER_QUITING=0, BAD_PROTOCOL=1, TOO_MANY_PEERS=2, ...

Our HelloMessage.code is int32 (DisconnectCode). Our DisconnectMessage.reason
is DisconnectReason. These are two separate spaces — intentional in libp2p.

## Cross-verified constants (don't change without re-validation)

- `MainnetNetworkID` = 11111
- `NileNetworkID` = 201910292
- `ShastaNetworkID` = 1
- libp2p Parameter default networkId = 0 (when not in config.conf)
- `KademliaOptions.BINS` = 17, `BUCKET_SIZE` = 16, `ALPHA` = 3
- `KademliaOptions.DISCOVER_CYCLE` = 7200 ms, `WAIT_TIME` = 100 ms
- `Parameter.MAX_MESSAGE_LENGTH` = 5 MB (TCP), UDP = 2048 bytes
- `KEEP_ALIVE_TIMEOUT` = 20 s, `NETWORK_TIME_DIFF` = 1 s
- `NODE_ID_LEN` = 64
- `Parameter.version` (HelloMessage.version) = 1

## ✅ Cross-impl block sync against live java-tron private chain (2026-05-02)

**End-to-end application-layer sync verified against a single-SR java-tron
private chain.** This is the first time gtron has finished a real sync handshake
against another implementation and stayed byte-identical through ongoing block
production.

Test setup: java-tron `FullNode.jar` running with the genesis at
`/Users/asuka/Works/Tests/TVM/run/config.conf` (networkId=0, chain_id=9999,
maintenance_time_interval=21600000ms, single SR). gtron started with
`--genesis test/fixtures/cross-impl/java-tron-private.json` and
`--seednode 127.0.0.1:18888` from a fresh datadir.

Verified byte-identical vs java-tron at H=78179:
- All 71 190 block hashes #1..#71 190 (spot-checked across the range; full
  agreement on block sync inventory)
- Active witness DPoS counters (`totalProduced`, `totalMissed`,
  `latestBlockNum`, `latestSlotNum`) on the lone SR
- Per-tx fee accounting on a TransferContract that creates a new account:
  `fee=100 000`, `net_fee=100 000`, sender SR delta identical on both nodes
- Bidirectional tx propagation: a TransferContract submitted to either node
  reaches the other node's pool via P2P; receiving node's mined block
  contains the tx; recipient balance and tx-info match on both sides

Driving fix commits (chronological):
- 1b323ed — `fix(net): SyncBlockChain summary + dedup + fetch throttle`
  (ascending summary order, drop already-have ids, 3 msg/s rate limit on
  outbound `FETCH_INV_DATA`)
- 7122d3f — `fix(cmd,api): genesis loader inits next_maintenance_time +
  expose witness counters` (mirror java-tron `Manager.initGenesis`;
  `listwitnesses` now exposes the four counters above)
- aa51ddf — `fix(net,core): cross-impl tx propagation works both directions`
  (wrap outbound TRX in TRXS 0x03; route reads through `HeadStateRoot`
  instead of empty `block.AccountStateRoot`; sync-finished trigger via
  `HandleChainInventory` single-id response)
- 906450a — `fix(core,actuator): create_account_fee parity with java-tron`
  (dedicated bandwidth path bypassing free-quota and tx-byte fee, mirroring
  `BandwidthProcessor.consumeForCreateNewAccount`; counter ownership moved
  from actuator-level to bandwidth path)
- 3d029f6 — `fix(core,state): totalMissed parity via state_flag DP key`
  (replace the broken `previousHeadTimestamp >= NextMaintenanceTime`
  heuristic with java-tron's explicit `state_flag` DP key, restoring
  `+MAINTENANCE_SKIP_SLOTS` adjustment in `SlotForTime`)

Reproduction:
```bash
# Start java-tron private chain (see java-tron-local.md Option 1, but with
# the genesis from config.conf and networkId=0, chain_id=9999).
JAVA_TRON_ADDR=127.0.0.1:18888 scripts/system_test_cross.sh
```

## What's still not validated

- **Real-network block sync (G1)** — requires a java-tron node on mainnet or
  Nile (networkID=11111 or 201910292). The cross-impl smoke above covers a
  private chain (networkID=0) but does NOT cover mainnet's longer history
  or its proposal-driven fork activations. G1 validation is deferred to
  M0″ Phase 2 (requires operator with mainnet-synced java-tron; see
  PLAN.md).
- **Testnet reachability** — live Nile/mainnet seeds appear unreachable from
  the test environment (TCP connects, first bytes received, but full sync
  not attempted yet).
- **PBFT SR signing cross-impl** — gtron's PBFT 3-phase machine is in place
  (M6b slice 2) but live byte-level cross-impl SR signing has not been
  validated. Needs mainnet SR private key + java-tron quorum on the other
  side.
