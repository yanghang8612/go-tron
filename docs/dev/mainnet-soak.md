# Mainnet 7×24h soak

A long-running gtron mainnet sync used to demonstrate G1's natural-language
exit ("持续同步 7×24h 无 state root 分叉"). Independent of the M0″ Phase 2
fixture replay, which is the formal G1 准入 check.

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
    └── check-divergence.sh                 # per-tick comparison vs api.trongrid.io
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

# Restart cleanly (NEW datadir; do this rarely — repeat restarts trip mainnet seed
# rate-limits, see `reference_tron_mainnet_seeds.md`. 30+ min cooldown afterward).
launchctl bootout gui/$(id -u)/com.tronprotocol.gtron-soak
rm -rf /Users/asuka/gtron-soak/datadir/* /Users/asuka/gtron-soak/logs/gtron.*.log
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tronprotocol.gtron-soak.plist

# Status
launchctl print gui/$(id -u)/com.tronprotocol.gtron-soak | grep -E "state|pid|last exit"
tail -F /Users/asuka/gtron-soak/logs/gtron.err.log
tail -F /Users/asuka/gtron-soak/logs/soak-monitor.log
curl -s http://127.0.0.1:8090/wallet/getnowblock | jq -r '.block_header.raw_data.number'
```

## Sync expectations

Catch-up rate observed during M3.5 sanity (2026-05-09): one full TRON-Hello
peer at ~200 blk/s. Mainnet head sits around #82.5M, so cold sync from
genesis takes ~5 days. Plan for ≥ 5d catch-up + 7d steady-state.

While catching up, `soak-monitor.log` will show
`oracle=<api.trongrid.io blockID at gtron-head> gtron=<our blockID>`. MATCH
means historical block hashes are byte-identical (the G1 invariant).
DIVERGE at any height is the alert: capture the height, archive the
block, and add a divergence-allowlist entry or open a parity bug.

`gtron-down` lines indicate the HTTP endpoint isn't responding — common
during the first 15-30s after launchd restart while gtron initializes,
otherwise investigate (probably crashed, check `gtron.err.log`).

## Disk

`/Users/asuka` has 315GB free as of 2026-05-09. java-tron mainnet datadir is
1-2 TB; gtron uses Pebble (no Merkle-trie overhead) so likely smaller, but
real numbers are unknown. Watch `du -sh /Users/asuka/gtron-soak/datadir`
during the first day; if growth rate × remaining catch-up > 250 GB, stop and
reconsider before filling the disk.

## Known constraints

- Mainnet seed-side rate limit per source IP: ~3-4 sync attempts in one session
  trip a session-wide ban; 30+ minute cooldown of *no reconnect attempts* lifts
  it. The per-addr dial throttle in `p2p.Server` (commit `bb52bb7`) prevents
  the maintainCh thundering herd that previously caused this within minutes.
- Discovery service routing table is seeded from
  `params.MainnetBootstrapNodes` (12 entries) + the explicit `--seednode`
  flags; dead seeds in either list don't break sync as long as one accepts
  TRON-Hello.
