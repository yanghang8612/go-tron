# Running a local java-tron for P2P interop testing

This document explains how to stand up a local java-tron node that go-tron's
integration tests can target.

## Prerequisites

- JDK 8 (x86_64) or JDK 17 (ARM64 / Apple Silicon) — required by java-tron's build
- Gradle (or use the wrapper `./gradlew`)
- ~15 GB free disk space (for local chain data)
- The pre-built `FullNode.jar` at `/Users/asuka/Projects/tron/java-tron/build/libs/FullNode.jar`
  (build with `./gradlew build -x test` from the java-tron repo root if missing)

## Option 1 — Local private network (recommended for wire-format testing)

Use a custom config with a unique `networkId` so the local node doesn't try to
sync mainnet. This is the fastest way to verify the handshake wire format.

1. Create a minimal config file `/tmp/local-tron.conf`:

```hocon
net {
  type = mainnet
}

storage {
  db.directory = "/tmp/tron-local-db"
  index.directory = "/tmp/tron-local-idx"
}

node {
  # Listen on localhost only; no outgoing seed dials.
  listen.port = 18888

  # A non-conflicting network ID so peers on the real TRON network won't
  # accidentally connect.
  p2p {
    version = 999
  }

  maxConnections = 10
}

seed.node {
  ip.list = []
}

block {
  needSyncCheck = false
  maintenanceTimeInterval = 21600000
}

genesis.block {
  # Use the default mainnet genesis.
  timestamp = "0"
  parentHash = "0xe58f33f9baf9305dc6f82b9f1934ea8f0ade2defb951258d50167028c780351f"

  assets = [
    { accountName = "Zion", accountType = "AssetIssue", address = "TLLM21wteSPs4hKjbxgmH1L6poyMjeTbHm", balance = "99000000000000000" },
    { accountName = "Sun", accountType = "AssetIssue", address = "TXmVpin5vq5gdZsciyyjdZgKRUju4st1wM", balance = "0" },
    { accountName = "Blackhole", accountType = "AssetIssue", address = "TLsV52sRDL79HXGGm9yzwKibb6BeruhUzy", balance = "-9223372036854775808" },
  ]

  witnesses = [
    { address = "TN3zfjYUmMFK3ZsHSsrdJoNRtGkQmZLBLz", url = "http://GR1.com", voteCount = 100000026 }
  ]
}
```

2. Start the node:

```bash
cd /Users/asuka/Projects/tron/java-tron
rm -rf /tmp/tron-local-db /tmp/tron-local-idx
java -jar build/libs/FullNode.jar -c /tmp/local-tron.conf 2>&1 | tee /tmp/java-tron-local.log
```

Watch the log for `P2P version: 999` and `Net start success`.

3. Verify it's listening:

```bash
nc -zv 127.0.0.1 18888   # TCP
nc -zv -u 127.0.0.1 18888 # UDP
```

4. Run go-tron integration tests against it:

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
JAVA_TRON_ADDR=127.0.0.1:18888 JAVA_TRON_NETWORK=999 \
  go test -tags=integration ./p2p/ -run "JavaTron" -v
```

Expected outcomes:
- `TestJavaTronDiscoverPing` — UDP ping to java-tron, pong received
- `TestJavaTronHandshake` — TCP handshake completes, connection stays alive
  for 10 s with no disconnect

## Option 2 — Join an existing testnet

Point go-tron at a real Nile testnet seed node. This exercises real-world
network conditions but requires actual network connectivity and means failures
may be due to NAT / rate-limiting / out-of-date bootstrap lists.

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
JAVA_TRON_ADDR=47.252.19.181:18888 JAVA_TRON_NETWORK=201910292 \
  go test -tags=integration ./p2p/ -run "JavaTron" -v
```

## Debugging failed handshake

If `TestJavaTronHandshake` fails with `reject peer: DIFFERENT_VERSION` or
`TIMEOUT`:

1. Check java-tron logs for `Handshake failed` or similar.
2. Add temporary logging to go-tron's `performLibp2pHandshake` to print the
   hex of the exchanged frames — compare against a working java-tron ↔
   java-tron handshake captured with `tcpdump`.
3. Confirm that `JAVA_TRON_NETWORK` matches java-tron's `node.p2p.version`
   config — a mismatch silently drops us with a DIFFERENT_VERSION disconnect.

If the UDP ping test fails but TCP works:

- Check UDP port is actually open: `nc -zv -u 127.0.0.1 18888` on Mac requires
  `nc` with `-u` support; fallback to `sudo tcpdump -i lo0 udp port 18888`
  and manually trigger a ping.
- Java-tron may silently drop UDP messages with mismatched `networkId`.

## Rebuilding FullNode.jar for ARM64 (Apple Silicon)

If you get `UnsatisfiedLinkError: librocksdbjni... (mach-o file, but is an
incompatible architecture (have 'x86_64', need 'arm64'))` at startup, the
jar was built for x86_64. Rebuild from source:

```bash
cd /Users/asuka/Projects/tron/java-tron
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
./gradlew clean build -x test -x lint -x checkstyleMain -x checkstyleTest
ls -la build/libs/FullNode.jar
```

Build takes ~5–10 minutes on Apple Silicon. The `platform` module selects
the correct RocksDB native library by architecture automatically.

## Teardown

```bash
# Kill java-tron
pkill -f FullNode.jar

# Clean up
rm -rf /tmp/tron-local-db /tmp/tron-local-idx /tmp/local-tron.conf /tmp/java-tron-local.log
```
