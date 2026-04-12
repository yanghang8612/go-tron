# TRON P2P Interop Status

Record of verified compatibility with the reference `io.github.tronprotocol/libp2p:2.2.7`
as of 2026-04-12.

## What works (unit + loopback verified)

- **Protobuf varint32 framing** — matches libp2p's `ProtobufVarint32LengthFieldPrepender`
  (Google protobuf varint). Verified with known vector 300 = `0xAC 0x02`. All p2p tests pass.
- **UDP discovery wire format** — `[type(1)][proto payload]`, no signature wrapper.
  Matches libp2p's `P2pPacketDecoder` + `Message.parse`.
- **Node ID** — 64 random bytes from `crypto/rand`. Matches libp2p `NetUtil.getNodeId()`.
- **Endpoint encoding** — IP addresses as ASCII string bytes (e.g., `[]byte("127.0.0.1")`).
  Matches libp2p `KadMessage.getEndpointFromNode` via `ByteArray.fromString(host)`.
- **Control message codes** — 0xFD HELLO, 0xFC STATUS, 0xFB DISCONNECT, 0xFF PING,
  0xFE PONG. Match libp2p `connection.message.MessageType`.
- **HelloMessage proto fields** — field numbers 1=from, 2=networkId, 3=code, 4=timestamp,
  5=version. Match libp2p `Connect.proto` via reverse-engineered field numbers from
  generated Java classes.
- **Two-phase handshake** — libp2p HELLO first, then peer registered. Keepalive +
  disconnect interception in `Peer.readLoop`. Self-compatible (tested loopback).

## What's NOT yet verified

- **Real java-tron handshake** — `TestJavaTronHandshake` against live Nile and mainnet
  seed nodes results in `read hello: EOF` — TCP connects, we send HELLO (96 bytes),
  peer closes without responding.
- **UDP discover/pong** — not yet tested against a real peer.

## EOF-on-send investigation

A `TestDumpHelloBytes` was run to confirm our wire bytes are structurally correct:

```
varint length field: 60             (96 in decimal; 1 type + 95 proto)
type byte:           fd             (HANDSHAKE_HELLO)
proto payload:       0a 51 ...      (starts with tag=1 length=81 — the From Endpoint)
total frame length:  97 bytes
```

The proto payload decodes correctly by `protoc --decode`:
```
from { address: "127.0.0.1" port: 18888 nodeId: <64 bytes> }
networkId: 11111 / 201910292 (match chain)
timestamp: <current ms>
version: 1
```

Structurally aligned with libp2p schema. The EOF suggests one of:
1. Nile seed is overloaded / rate-limiting / our IP is on a throttle list
2. Some subtle wire field we haven't noticed (e.g., the `Version` field's
   semantics differ, or there's a required `code` field value)
3. A Netty-level decoder-pipeline difference (e.g., fin before varint flush)

## Next steps for validation

1. **Build `FullNode.jar` for ARM64** following `docs/dev/java-tron-local.md`.
   Start java-tron locally with a custom networkId; point go-tron at it;
   capture `java-tron.log` for the exact rejection reason.
2. **Run tcpdump during a java-tron ↔ java-tron handshake** on a working
   deployment. Capture the first two HELLO frames. Diff byte-for-byte against
   what `TestDumpHelloBytes` produces.
3. **Review `HandshakeService.processMessage`** for any validation we
   might be failing: `processPeer` (capacity/ban/same-IP), `validNode`, and
   any side-effects like `ChannelManager.updateNodeId`.

## Cross-verified constants (don't change without re-validation)

- `MainnetNetworkID` = 11111 (from java-tron `config.conf` line 197)
- `NileNetworkID` = 201910292 (from same config comment)
- `ShastaNetworkID` = 1 (from same config comment)
- `KademliaOptions.BINS` = 17, `BUCKET_SIZE` = 16, `ALPHA` = 3
- `KademliaOptions.DISCOVER_CYCLE` = 7200 ms, `WAIT_TIME` = 100 ms
- `Parameter.MAX_MESSAGE_LENGTH` = 5 MB (TCP), UDP = 2048 bytes
- `KEEP_ALIVE_TIMEOUT` = 20 s, `NETWORK_TIME_DIFF` = 1 s
- `NODE_ID_LEN` = 64
- `Parameter.version` (HelloMessage.version) = 1 (hardcoded default)
