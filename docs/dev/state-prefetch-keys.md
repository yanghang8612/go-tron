# State Prefetch Key Audit

Date: 2026-06-10

Status: first actuator-envelope extractor landed in
`actuator.PrefetchKeysFor(tx)` and is wired into `ProcessBlock` behind the
opt-in `state.prefetch.enabled` config. The extractor is intentionally
conservative: it only emits raw latest-domain hints derivable from the
transaction envelope. Malformed payloads and invalid addresses produce fewer
hints and never change actuator validation behaviour.

## Implemented Key Kinds

| Key kind | Backing read |
| --- | --- |
| `state.AccountPrefetchKey(addr)` | `ReadStateAccountLatest(addr)` |
| `state.ContractMetadataPrefetchKey(addr)` | `ContractMetadata/meta` account-KV row |
| `state.ContractCodePrefetchKey(addr)` | `ReadStateAccountLatest(addr)` envelope, then `ReadStateCode(CodeHash)` when present |
| `state.ContractOriginAccountPrefetchKey(addr)` | `ContractMetadata/meta` account-KV row, then `ReadStateAccountLatest(origin_address)` when present |
| `state.AccountKVPrefetchKey(SystemAccount, SystemDelegation, key)` | delegation resource/index rows |
| `state.AccountKVPrefetchKey(SystemAccount, SystemAsset, key)` | TRC10 asset metadata/name/owner-index rows |
| `state.AccountKVPrefetchKey(SystemAccount, SystemMarket, key)` | market order/account/price-list/price-count/order-book rows derivable from the envelope |
| `state.AccountKVPrefetchKey(SystemAccount, SystemExchange, key)` | V1/V2 exchange rows keyed by exchange id |

The driver warms Pebble or blockbuffer raw reads only. It does not mutate
`StateDB` object caches from worker goroutines.

## Covered Contract Families

| Contract family | Prefetch hints |
| --- | --- |
| TRX and TRC10 transfer | owner account, recipient account; transfer-style recipient contract metadata where a contract-address check may follow; TRC10 legacy metadata, name index, and numeric-id V2 metadata |
| TVM trigger | owner account, contract account, contract metadata, contract code row, contract origin account from metadata |
| TVM create | owner account, declared origin account, deterministic new contract account, new contract metadata |
| Contract settings | owner account, target contract account, target contract metadata, target contract origin account from metadata |
| Vote witness | voter account, each voted witness account |
| Stake 1.0 freeze/unfreeze | owner account, optional receiver account, legacy delegated-resource and account-index rows |
| Stake 2.0 delegate/undelegate | owner account, receiver account, locked/unlocked delegated-resource rows, owner delegation-index row |
| Shielded transfer | transparent from/to accounts when present and valid |
| Asset issue | owner account, owner-index row, legacy name metadata, name index |
| Market sell/cancel | owner account, TRC10 token metadata/name-index hints derivable from the envelope, owner market-account row, cancel order row, current pair price-list/price-count/current price-level order-book, and reverse pair price-list; `_` TRX token legs are skipped for TRC10 metadata |
| Exchange token operations | owner account, TRC10 token metadata/name-index hints derivable from the envelope, and both V1/V2 exchange rows when an exchange id is present |
| Owner-only actuators | owner account for witness, account, proposal, brokerage, freeze-v2, unfreeze-v2, withdraw, update-asset, and unfreeze-asset contracts |
| Account create and participate asset issue | owner account plus the explicitly referenced counterparty account; participate also warms TRC10 metadata/name-index rows |

## Not Yet Covered

- Per-account TRC10 balances beyond the owner/recipient account rows already
  warmed above. Balances live inside the account proto, so there is no separate
  asset-balance KV hint to enqueue yet.
- Witness, proposal, and brokerage rawdb rows outside account latest. Some are
  still not state-domain rows.
- Second-stage market/exchange rows that require first reading an order,
  exchange, or price list, such as maker order ids behind a matched price level
  and the opposite exchange token id.
- Additional TVM runtime CALL/DELEGATECALL target code rows and storage slots.
  Those become known only during VM execution and need a VM hook, not
  tx-envelope extraction.
- Dynamic-property reads. They are normally hot and not represented by the
  first raw latest-domain prefetch key set.

## Acceptance Notes

- Over-prefetching is allowed because these are discarded read warmups.
- Under-prefetching only leaves performance on the table.
- The `ProcessBlock` integration has a 100-tx prefetch-on/off root equality
  regression test. Broader benchmarks and long replay soak are still required
  before enabling it by default.
