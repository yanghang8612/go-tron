# TRON P2P Interop Status

Record of verified compatibility with the reference `io.github.tronprotocol/libp2p:2.2.7`.

## ✅ VERIFIED (2026-04-12 / updated 2026-05-01)

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

## What's still not validated

- **Full application-layer sync** — TRON Hello (0x20) received from java-tron
  is confirmed; go-tron's `net/handler.go` implements the response and
  SyncBlockChain path. End-to-end block exchange is exercised by
  `scripts/system_test.sh` (2-node dev chain) but not yet against a real
  java-tron mainnet/Nile node.
- **Real-network block sync (G1)** — requires a java-tron node on mainnet or
  Nile (networkID=11111 or 201910292). The local private test chain
  (networkID=0) cannot be used: gtron defaults to networkID=11111 and the
  libp2p hello would reject with DIFFERENT_VERSION. Even with a matching
  network ID, genesis block hashes must match for TRON Hello validation to
  pass. G1 validation is deferred to M0″ Phase 2 (requires operator with
  mainnet-synced java-tron; see PLAN.md).
- **Testnet reachability** — live Nile/mainnet seeds appear unreachable from
  the test environment (TCP connects, first bytes received, but full sync
  not attempted yet).
