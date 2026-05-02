# Cross-impl divergences — 2026-05-02

Findings from `make system-test-cross-flows` against the live java-tron
private chain at `/Users/asuka/Works/Tests/TVM/run/config.conf` (single-SR
Zion @ `TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC`, networkId=0, chain_id=9999).
Both nodes at `totalProduced=79956` when baseline assertions ran.

Source: `scripts/system_test_cross_flows.sh` commit `c884699`. See
`docs/dev/p2p-interop-status.md` for the underlying interop record.

## Repro

```bash
# java-tron must already be running with the config.conf genesis
# (see docs/dev/java-tron-local.md Option 1, with the private-chain
# adjustments documented in p2p-interop-status.md).
JAVA_TRON_HTTP=127.0.0.1:8090 make system-test-cross-flows
```

The script syncs gtron from genesis, then runs 7 P0 contract-type flows
and asserts post-tx state byte-equal on both sides.

## D-1 SR balance — **P0 consensus**

| node | balance (sun) |
|---|---|
| gtron | 98_999_998_972_389_000 |
| java-tron | 98_999_998_955_474_000 |
| Δ | **16,915,000 (gtron HIGHER)** |

Observed at baseline (pre-flow), so the divergence is accumulated during
historical replay of blocks 0..79956 — not from any test tx. gtron is
under-debiting or over-crediting on some path.

The chain is single-SR with mostly-empty blocks; only a small number of
blocks contain test txs from prior interop sessions. The 16.915M /
100,000 = 169.15 ratio doesn't cleanly match a per-tx 100k fee (no clean
integer factor). Most likely cause: a per-block or per-maintenance
small-amount accumulation that gtron skips.

Hooks to investigate:
- `core/state_processor.go::ApplyTransaction` — bandwidth fee, multisign
  fee, memo fee. Compare against java-tron `Manager.processTransaction`.
- `actuator/fees.go` — bandwidth/burn helpers.
- `core/reward.go::payBlockReward` — does brokerage split deduct from SR
  balance, or only from a non-balance reward pool?
- java-tron: `framework/.../db/Manager.java::processTransaction`,
  `MortgageService`, `BandwidthProcessor.consume*`.

Recommended probe: query both nodes' tx-info on a representative
non-empty block (e.g. earliest block with a Transfer); compare the
SR-side fee delta byte-for-byte.

## D-2 SR allowance — **P0 consensus**

| node | allowance (sun) |
|---|---|
| gtron | 6_820_992_000_000 |
| java-tron | 767_577_600_000 |
| ratio | **8.886× (gtron HIGHER)** |

Maintenance cycles elapsed: 79956 × 3s ≈ 66.6h ≈ 11 cycles
(`maintenance_time_interval = 21_600_000ms`). java-tron credit per
maintenance ≈ 7e10 sun; gtron ≈ 6.2e11.

The chain runs without `change_delegation` proposal active (default).
java-tron's no-change-delegation path uses a different formula from
the post-`change_delegation` brokerage split.

Hooks:
- `core/reward.go` — `payBlockReward`, `payStandbyWitness`,
  `applyRewardMaintenance`. Verify which fires when
  `change_delegation` is OFF.
- java-tron: `consensus/dpos/MaintenanceManager.payReward`,
  `MortgageService.payBlockReward`, `MortgageService.payStandbyWitness`.
- Witness pay constant: `dynProps.WitnessPayPerBlock()` — default mainnet
  16e6 sun/block; verify on this chain.

8.886× is suggestive of compound (e.g., paying both witness brokerage
AND standby pool to same allowance), or of paying per-block instead of
per-maintenance with wrong magnitude.

## D-3 proposal_id off-by-one — **P0 consensus**

First proposal created on each chain:

| node | proposal_id |
|---|---|
| gtron | 0 |
| java-tron | 1 |

java-tron `ProposalCreateActuator.execute` (master):
```java
long id = dynamicStore.getLatestProposalNum() + 1;
dynamicStore.saveLatestProposalNum(id);
proposal.setID(id);
```

go-tron `actuator/proposal_create.go` likely post-increments or starts
the counter at 0 instead of using the +1 offset. Trivial 1-line fix
once located. Watch for whether the issue is in the increment OR in
the default value of the `latest_proposal_num` DP key.

## Wire-format `listproposals.parameters` — **P1 SDK compat**

Same proposal, same chain:

| node | output |
|---|---|
| gtron | `"parameters": {"19": 259200000}` |
| java-tron | `"parameters": [{"key":19,"value":259200000}]` |

This is OUTPUT-side; M9.4 fixed INPUT parsing for the same array form on
`/wallet/proposalcreate`. The fix lives in
`internal/tronapi/`, wherever Proposal proto serialization happens.

Affected endpoints (likely all four):
- HTTP `/wallet/listproposals`
- HTTP `/wallet/getproposalbyid`
- gRPC `wallet.Wallet/ListProposals`
- gRPC `wallet.Wallet/GetProposalById`

## Status

All four open. None have a fix landed yet. Investigations dispatched
2026-05-02 in parallel; this doc is the shared briefing.
