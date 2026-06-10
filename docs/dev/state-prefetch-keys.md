# State Prefetch Key Audit

Date: 2026-06-10

Status: first actuator-envelope extractor landed in
`actuator.PrefetchKeysFor(tx)`. The extractor is intentionally conservative:
it only emits raw latest-domain hints derivable from the transaction envelope.
Malformed payloads and invalid addresses produce fewer hints and never change
actuator validation behaviour.

## Implemented Key Kinds

| Key kind | Backing read |
| --- | --- |
| `state.AccountPrefetchKey(addr)` | `ReadStateAccountLatest(addr)` |
| `state.ContractMetadataPrefetchKey(addr)` | `ContractMetadata/meta` account-KV row |
| `state.AccountKVPrefetchKey(SystemAccount, SystemDelegation, key)` | delegation resource/index rows |

The driver warms Pebble or blockbuffer raw reads only. It does not mutate
`StateDB` object caches from worker goroutines.

## Covered Contract Families

| Contract family | Prefetch hints |
| --- | --- |
| TRX and TRC10 transfer | owner account, recipient account; transfer-style recipient contract metadata where a contract-address check may follow |
| TVM trigger | owner account, contract account, contract metadata |
| TVM create | owner account, declared origin account, deterministic new contract account, new contract metadata |
| Contract settings | owner account, target contract account, target contract metadata |
| Vote witness | voter account, each voted witness account |
| Stake 1.0 freeze/unfreeze | owner account, optional receiver account, legacy delegated-resource and account-index rows |
| Stake 2.0 delegate/undelegate | owner account, receiver account, locked/unlocked delegated-resource rows, owner delegation-index row |
| Shielded transfer | transparent from/to accounts when present and valid |
| Owner-only actuators | owner account for asset, witness, account, proposal, brokerage, freeze-v2, unfreeze-v2, withdraw, market, and exchange contracts |
| Account create and participate asset issue | owner account plus the explicitly referenced counterparty account |

## Not Yet Covered

- TRC10 asset issue rows and per-account TRC10 balances. The current key type
  set has account-KV support, but this needs a separate asset-domain audit so
  token-name fork behaviour is not guessed.
- Witness, proposal, exchange, market, and brokerage rawdb rows outside
  account latest. Some are still not state-domain rows.
- Contract origin account for update-setting/update-energy-limit/clear-ABI.
  It is read from contract metadata first, so it needs chained prefetch or a
  metadata-aware second pass.
- TVM runtime CALL/DELEGATECALL targets and storage slots. Those become known
  only during VM execution and need a VM hook, not tx-envelope extraction.
- Dynamic-property reads. They are normally hot and not represented by the
  first raw latest-domain prefetch key set.

## Acceptance Notes

- Over-prefetching is allowed because these are discarded read warmups.
- Under-prefetching only leaves performance on the table.
- The `ProcessBlock` integration must compare post-block state roots with
  prefetch enabled and disabled before the feature can be enabled by default.
