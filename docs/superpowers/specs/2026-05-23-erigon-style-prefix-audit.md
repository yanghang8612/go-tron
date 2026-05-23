# Erigon-Style Prefix Audit

Date: 2026-05-23

Status: Phase 0 audit baseline

Scope: fresh database only. No migration path is required. This audit classifies
every hot-KV key prefix and singleton currently defined by `core/rawdb`, plus the
already-native typed stores in `core/state`.

## Classification

| Class | Meaning | Historical restart rule |
| --- | --- | --- |
| `rooted` | Canonical mutable state that must be covered by the internal full state root. | Rebuilt by genesis materialization plus canonical block replay; future Erigon phases move it to typed domains. |
| `derived` | Deterministic index/cache/statistic derived from canonical blocks or state. | Delete on restart and rebuild by replay or explicit backfill. |
| `immutable` | Canonical chain data, not mutable state. | Preserve up to target head; ancient tables are truncated by `AncientWriter`, not by hot-KV reset. |
| `history` | Archive/history rows outside canonical state. | Delete on restart and rebuild/backfill from retained blocks. Future history domains own pruning. |
| `runtime` | Local async metadata, finality data, recovery WAL, or operational cursor. | Delete on restart; if needed, recover through network sync or a dedicated backfill. |
| `legacy` | Compatibility mirror or old flat representation. | Keep only while callers still need the mirror; never use as canonical root input. |

## Chain, Head, And Transaction Data

| Key / prefix | Current owner | Class | Root policy | Restart / rewind rule |
| --- | --- | --- | --- | --- |
| `b-` | `rawdb.WriteBlock`, `ReadBlock` | `immutable` | Not rooted state. | Preserve canonical block bodies. Blocks above the target are hidden by head selection; ancient `bodies` uses `TruncateHead`. |
| `bh-` | `rawdb.ReadBlockNumber` | `immutable` | Not rooted state. | Preserve hot hash-to-number index for retained blocks. |
| ancient `bodies` | freezer | `immutable` | Not rooted state. | Managed only through `AncientWriter.TruncateHead(height+1)`. |
| `LastBlock` | head pointer | `runtime` | Not rooted. | Delete and rewrite after replay reaches target. |
| `LastSolidBlock` | solid head pointer | `runtime` | Not rooted. | Delete and recompute during replay. |
| `genesis-state-root` | genesis bootstrap | `runtime` | Not rooted. | Delete and reseed from genesis config. |
| `bsr-` | block hash -> internal state root | `derived` | Not rooted. | Delete and rewrite for replayed canonical blocks. Ancient `state_roots` is truncated separately. |
| `tx-` | tx hash -> block number | `derived` | Not rooted. | Delete and rebuild from replayed block transactions. |
| `ti-` | tx hash -> `TransactionInfo` | `derived` | Not rooted. | Delete and rebuild from transaction execution. |
| `tib-` | block number -> `TransactionRet` | `derived` | Not rooted. | Delete and rebuild from transaction execution. Ancient `tx_infos` is truncated separately. |
| `total-tx-count` | transaction counter | `derived` | Not rooted. | Delete and recompute during replay. It is a metric, not consensus state. |

## Rooted Consensus State

| Key / prefix | Logical owner / domain | Current access path | Phase action |
| --- | --- | --- | --- |
| `dp-` | `SystemAccountID / SystemDynamicProperty` | `DynamicProperties.FlushRooted`; flat `dp-` mirror for startup/head keys | Keep rooted typed store. Move remaining execution reads off rawdb compatibility paths. |
| `w-` | witness account / `WitnessCapsule` | `RootedStore` bridge; `StateDB` witness cache | Replace rawdb witness accessors in execution with typed witness store. |
| `wlb-` | witness account / `WitnessCapsule` | `RootedStore` bridge | Keep rooted; solidification depends on replay-deterministic witness latest-block state. |
| `wb-` | witness account / `WitnessCapsule` | `RootedStore` bridge | Keep rooted; migrate to typed witness/brokerage store. |
| `ws` | system / `SystemWitnessSchedule` | legacy sentinel, currently not written | Keep as compatibility only or remove when typed schedule fully owns state. |
| `fv-` | system / `SystemForkVote` | `RootedStore` bridge; fork controller still rawdb-shaped | Keep rooted; convert fork controller to typed store. |
| `dr-`, `dri-`, `drax-` | system / `SystemDelegation` | `RootedStore` bridge | Keep rooted; replace delegation rawdb accessors in actuators/reward code. |
| `dl-`, `rvi-` | system / `SystemReward` | `RootedStore` bridge | Keep rooted; reward withdrawal correctness depends on replayed reward state. |
| `aa-` | account / `AccountLocalIndex` | `RootedStore` bridge | Keep rooted for optimized account asset balances. |
| `abi-` | contract / `ContractABI` | `RootedStore` bridge | Keep rooted as contract metadata. |
| `cs-` | contract / `ContractRuntimeState` | `RootedStore` bridge | Keep rooted; dynamic energy state is block-deterministic. |
| `ct-` | contract / `ContractMetadata` | `StateDB` native KV plus mirror | Keep rooted; remove rawdb mirror from execution path. |
| `s-` | contract / `ContractStorage` | `StateDB` native KV plus mirror | Keep rooted; final physical key becomes prefix-iterable account-KV domain. |
| `c-` | contract now, future `CodeDomain` by `code_hash` | `StateDB` stores code under `ContractMetadata/code` | Move to content-addressed immutable code domain in Phase 4. |
| `nf-`, `nc-`, `nccount` | system / `SystemShielded` | `RootedStore` bridge | Keep rooted; spend/nullifier and note commitment state affects shielded validation. |
| `zkp-` | system / `SystemShielded` | `RootedStore` bridge | Keep rooted while java-tron-visible proof result cache participates in shielded execution. Revisit if proven pure cache. |
| `imt-`, `imt-LAST_TREE`, `imt-CURRENT_TREE` | system / `SystemShielded` | `RootedStore` bridge | Keep rooted; shielded anchor validation depends on deterministic tree state. |
| `mti-` | system / `SystemShielded` | `RootedStore` bridge | Keep rooted for now because the shielded container uses it during block execution to reuse previous roots; revisit when history domains are available. |

## Runtime, Derived, And History Rows

These rows must not enter the block state root. Current code removes them from
`LookupRootedStateKey`, so `RootedStore` writes fall through to the buffer/disk
compatibility path instead of mutating `StateDB`.

| Key / prefix | Class | Reason | Restart / rewind rule |
| --- | --- | --- | --- |
| `psd-` | `runtime` | PBFT quorum signatures arrive through async network paths, not deterministic block execution. | Delete; recover by PBFT data sync or explicit finality backfill. |
| `LATEST_PBFT_BLOCK_NUM` | `runtime` | Local finality cursor derived from PBFT messages. | Delete; recompute from PBFT sign data if a future service needs immediate RPC availability. |
| `tps-` | `derived` | 65536-slot TAPOS recent-block ring, deterministically rebuilt from block hash history. | Delete and rebuild during replay. |
| `at-` | `history` | Account balance audit trail gated by history lookup config. | Delete and rebuild through history backfill. |
| `btrace-` | `history` | Per-block balance trace for audit APIs. | Delete and rebuild through history backfill. |
| `sb-` | `derived` | Log-filter bloom accelerator; no consensus reads. | Delete and rebuild from receipts/logs. |
| `tbi-` | `derived` | Shielded proof acceleration index; no production writer today. | Delete and rebuild/backfill if the feature is enabled. |
| `cpv2-` | `runtime` | java-tron crash-recovery WAL placeholder; Pebble WAL is go-tron's recovery layer. | Delete if present; do not root. |
| `ws-shuffled`, `ws-prev-shuffled` | `runtime` until production scheduling uses them | PBFT handlers read these directly as finality membership helpers. Current block scheduling does not depend on them. | Delete/reseed from witness schedule or PBFT sync. If future block scheduling depends on them, promote through typed `SystemWitnessSchedule`. |

## State History Index

| Key / prefix | Class | Root policy | Restart / rewind rule |
| --- | --- | --- | --- |
| `sh-m-` | `history` | Not rooted. | Delete and rebuild from canonical replay/backfill. |
| `sh-a-` | `history` | Not rooted. | Delete and rebuild from canonical replay/backfill. |
| `sh-s-` | `history` | Not rooted. | Delete and rebuild from canonical replay/backfill. |
| `sh-i-a-` | `history` | Not rooted. | Delete and rebuild from canonical replay/backfill. |
| `sh-i-s-` | `history` | Not rooted. | Delete and rebuild from canonical replay/backfill. |
| `sh-cfg-` | `history` | Not rooted. | Delete/reseed according to node history mode. |
| `sh-bf-cursor-` | `runtime` | Not rooted. | Delete; backfill resumes from a new operator command. |

## Legacy And Already-Typed Stores

| Store | Current state | Final target |
| --- | --- | --- |
| `a-` legacy account rows | Compatibility flat account capsule accessor. `StateDB` account trie is canonical. | Remove from execution path; keep only test/compat reader until no caller needs it. |
| TRC10 asset store | Already typed under `SystemAsset`. | Keep; later backed by physical latest domain. |
| Exchange V1/V2 | Already typed under `SystemExchange`. | Keep; later backed by physical latest domain. |
| Market order book | Already typed under `SystemMarket`. | Keep; later backed by physical latest domain. |
| Proposal store | Already typed under `SystemProposal`. | Keep; remove rawdb proposal execution access. |
| Account name/id indexes | Already typed under `SystemAccountIndex`. | Keep. |
| Votes store | Already typed under `WitnessVoteState`. | Keep. |

## Phase 0 Decisions

1. PBFT/finality data is runtime metadata, not rooted state.
2. TAPOS and total transaction count are replay-derived, not rooted state.
3. Bloom, balance trace, account trace, tree-block index, and checkpoint-v2 are
   history/cache/recovery data, not rooted state.
4. `ResetMutableState` must continue deleting every non-immutable replay-derived
   hot prefix. It must preserve `b-` and `bh-`; ancient tables are handled by
   `AncientWriter.TruncateHead`.
5. `RootedStore` remains a transition bridge only for consensus state. Any newly
   rooted field must have deterministic block write timing before being added to
   `LookupRootedStateKey`.
