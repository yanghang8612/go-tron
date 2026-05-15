# Counter / id DP keys parity sweep — 2026-05-15

Follow-up to [next-proposal-id-triage](next-proposal-id-triage-2026-05-15.md).
The proposal-counter bug had a recognizable shape:

- java-tron names the key `LATEST_*` (latest assigned id), defaults to 0/N,
  pre-increments in the create-actuator.
- gtron named the analogous key `next_*` (next id to assign), defaulted to
  0/N+1, post-incremented in the create-actuator.

Same first id, off-by-one in the stored counter — invisible to chain
behavior but a permanent +1 offset in the DP store, folded into DigestC.

Swept every counter-like DP key for the same pattern.

## Findings

| key | gtron (before) | java-tron | status |
|---|---|---|---|
| `next_proposal_id` | default 1, post-inc | `LATEST_PROPOSAL_NUM` default 0, pre-inc | **fixed `886bb0c`** — renamed to `latest_proposal_num` |
| `next_token_id` | default 1_000_001, post-inc | `TOKEN_ID_NUM` default 1_000_000, pre-inc | **fixed in this commit** — renamed to `token_id_num` |
| `next_exchange_id` | default 1, post-inc | `LATEST_EXCHANGE_NUM` default 0, pre-inc | **fixed in this commit** — renamed to `latest_exchange_num` |
| `current_cycle_number` | default 0, advances at maintenance | `CURRENT_CYCLE_NUMBER` default 0, same | OK (no pre/post-inc ambiguity) |
| `next_maintenance_time` | derived from genesis_ts + interval | `NEXT_MAINTENANCE_TIME` same | OK (not a counter) |
| `block_filled_slots_index` | rolling 128-byte ring index | same | OK (mod-128, not assignment) |

No more counter-shaped divergences in the DP key set.

## Fix shape for token/exchange (matches proposal counter)

- `token_id_num` default 1_000_000 (was `next_token_id` = 1_000_001).
  `AssetIssueActuator` now reads `TokenIdNum()`, increments to `+1`, stores
  the new value (java-tron `AssetIssueActuator.java:72-76` parity).
- `latest_exchange_num` default 0 (was `next_exchange_id` = 1).
  `ExchangeCreateActuator` pre-increments
  (java-tron `ExchangeCreateActuator.java:78` parity).
- Both create-actuators are the **only** consumers (no bounds-check sites
  like the proposal `>= NextProposalID()` we had to flip). Tests updated.
- Smoke oracle regenerated; `make conformance-replay` 1/1.

## Why the chain-behavior was unaffected

The first id assigned by either actuator was always correct (1_000_001 for
the first TRC10, 1 for the first exchange). The divergence was purely in
the *stored counter's serialized value*. Cross-flow tests covering only the
on-chain effects of the create txs never noticed; only DigestC, which folds
every DP value, would.
