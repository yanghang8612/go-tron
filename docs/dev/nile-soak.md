# Nile 7×24h soak

A long-running gtron Nile (testnet) sync used to demonstrate G1's
natural-language exit ("持续同步 7×24h 无 state root 分叉"). Independent of
the M0″ Phase 2 fixture replay, which is the formal G1 准入 check.

The soak runs against Nile rather than mainnet — `run-gtron.sh` passes
`--testnet` + Nile `--seednode`s, and `check-divergence.sh` compares against
`nile.trongrid.io`. Nile's shorter history and faster cold sync make it a
practical continuous-soak target; mainnet G1 validation still goes through
M0″ Phase 2.

## Layout

```
/Users/asuka/gtron-soak/
├── datadir/                                # gtron Pebble store
├── logs/
│   ├── gtron.err.log                       # gtron stderr (sync milestones, errors)
│   ├── gtron.out.log                       # gtron stdout (banner)
│   ├── monitor.{out,err}.log               # check-divergence.sh telemetry
│   └── soak-monitor.log                    # one line every 5 min: ts h=<height> peers=<n> gtron=<bid> oracle=<bid> {MATCH|DIVERGE|UNKNOWN|gtron-down}
└── scripts/
    ├── run-gtron.sh                        # gtron wrapper used by LaunchAgent
    └── check-divergence.sh                 # per-tick comparison vs nile.trongrid.io
```

LaunchAgents (~/Library/LaunchAgents):

| plist | StartInterval | KeepAlive |
| --- | --- | --- |
| `com.tronprotocol.gtron-soak.plist` | – | true |
| `com.tronprotocol.gtron-soak-monitor.plist` | 300s | – |

## Operations

```bash
# Start
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tronprotocol.gtron-soak.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tronprotocol.gtron-soak-monitor.plist

# Stop
launchctl bootout gui/$(id -u)/com.tronprotocol.gtron-soak-monitor
launchctl bootout gui/$(id -u)/com.tronprotocol.gtron-soak

# Restart cleanly (NEW datadir; do this rarely — repeat restarts can trip seed
# rate-limits. 30+ min cooldown afterward).
launchctl bootout gui/$(id -u)/com.tronprotocol.gtron-soak
rm -rf /Users/asuka/gtron-soak/datadir/* /Users/asuka/gtron-soak/logs/gtron.*.log
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tronprotocol.gtron-soak.plist

# Status
launchctl print gui/$(id -u)/com.tronprotocol.gtron-soak | grep -E "state|pid|last exit"
tail -F /Users/asuka/gtron-soak/logs/gtron.err.log
tail -F /Users/asuka/gtron-soak/logs/soak-monitor.log
curl -s http://127.0.0.1:8090/wallet/getnowblock | jq -r '.block_header.raw_data.number'
```

## Shielded TRC20 Replay Recovery

If a Nile node was already synced past the shielded TRC20 activation window
with an older binary, rebuilding the binary alone does not rewrite contract
storage that was materialized by earlier blocks. Rewind and replay from the
block immediately before proposal #39:

```bash
# One-shot recovery flag. Remove it after the node has replayed past the
# failing shielded TRC20 block range.
--sync.restart-from 6360100
```

This replays the historical proposal at block 6,360,101 and every subsequent
shielded TRC20 mint/transfer with the current Sapling-enabled precompile
implementation.

## Linux Sync Service Profile

For a dedicated Nile sync host with roughly 60 GiB RAM, the current profiling
baseline uses an 8 GiB Pebble block cache, 256 MiB memtables, and relaxed L0
thresholds:

```ini
[Service]
Environment=GOMEMLIMIT=32GiB
ExecStart=/bin/bash -lc 'exec /data/gtron/go-tron/build/bin/gtron \
    --datadir       /data/gtron/nile/datadir \
    --testnet \
    --p2p.port      18888 \
    --http.port     8090 \
    --jsonrpc.port  8545 \
    --grpc.port     50051 \
    --pprof.port    6060 \
    --pprof.addr    127.0.0.1 \
    --maxpeers      30 \
    --db.cache      8192 \
    --db.handles    8192 \
    --db.memtable   256 \
    --db.l0.compact 8 \
    --db.l0.stop    64 \
    --seednode      44.236.192.97:18888 \
    --seednode      44.236.125.107:18888 \
    --seednode      44.232.119.174:18888 \
    --seednode      52.39.105.180:18888 \
    --seednode      54.70.52.47:18888'
MemoryHigh=32G
MemoryMax=40G
LimitNOFILE=65536
```

If the host only has around 27 GiB available to gtron, keep the tighter
`GOMEMLIMIT=20GiB`, `MemoryHigh=20G`, `MemoryMax=23G` settings instead.

## Sync expectations

Nile's head is far lower than mainnet's, so cold sync from genesis is short.
The 2026-05-15 restart reached h≈59k within the first hours, MATCH'ing
`nile.trongrid.io` block IDs at every monitor tick. Plan for a brief
catch-up + 7d steady-state.

While catching up, `soak-monitor.log` will show
`oracle=<nile.trongrid.io blockID at gtron-head> gtron=<our blockID>`. MATCH
means historical block hashes are byte-identical (the G1 invariant).
DIVERGE at any height is the alert: capture the height, archive the
block, and add a divergence-allowlist entry or open a parity bug.

`gtron-down` lines indicate the HTTP endpoint isn't responding — common
during the first 15-30s after launchd restart while gtron initializes,
otherwise investigate (probably crashed, check `gtron.err.log`).

## Disk

Nile's datadir is small — observed ~472 MB at h≈59k on 2026-05-15. Disk
pressure is not a concern for the Nile soak (mainnet would be a different
story: java-tron mainnet datadir is 1-2 TB). Still worth a periodic
`du -sh /Users/asuka/gtron-soak/datadir` glance.

## Known constraints

- Seed-side rate limit per source IP: ~3-4 sync attempts in one session
  trip a session-wide ban; 30+ minute cooldown of *no reconnect attempts* lifts
  it. The per-addr dial throttle in `p2p.Server` (commit `bb52bb7`) prevents
  the maintainCh thundering herd that previously caused this within minutes.
- Discovery service routing table is seeded from `params.NileBootstrapNodes`
  (`--testnet` selects the Nile list) + the explicit `--seednode` flags;
  dead seeds in either list don't break sync as long as one accepts
  TRON-Hello.
