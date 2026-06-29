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
| `state.AccountKVPrefetchKey(addr, WitnessCapsule, key)` | witness capsule and current brokerage rows keyed by witness address |
| `state.AccountKVPrefetchKey(SystemAccount, SystemWitnessSchedule, key)` | witness index row |
| `state.AccountKVPrefetchKey(SystemAccount, SystemProposal, key)` | proposal record/index rows keyed by proposal id |
| `state.AccountKVPrefetchKey(SystemAccount, WitnessVoteState, key)` | pending VotesStore record/index rows keyed by voter |
| `state.AccountKVPrefetchKey(SystemAccount, SystemDelegation, key)` | delegation resource/index rows |
| `state.AccountKVPrefetchKey(SystemAccount, SystemAsset, key)` | TRC10 asset metadata/name/owner-index rows |
| `state.AccountKVPrefetchKey(SystemAccount, SystemAccountIndex, key)` | account name and account ID uniqueness index rows |
| `state.AccountKVPrefetchKey(SystemAccount, SystemShielded, key)` | shielded proof-cache, anchor, nullifier, and note-commitment counter rows |
| `state.OwnerIssuedAssetRowsPrefetchKey(owner)` | owner account latest row, then V2 asset metadata plus legacy/name-index rows from `asset_issued_id` and `asset_issued_name` |
| `state.AccountKVPrefetchKey(SystemAccount, SystemMarket, key)` | market order/account/price-list/price-count/order-book rows derivable from the envelope |
| `state.AccountKVPrefetchKey(SystemAccount, SystemExchange, key)` | V1/V2 exchange rows keyed by exchange id |
| `state.ExchangeTokenAssetsPrefetchKey(id)` | V1/V2 exchange rows keyed by exchange id, then TRC10 asset rows for both exchange token legs |
| `state.MarketMatchOrdersPrefetchKey(sellToken, buyToken, sellQty, buyQty)` | reverse pair price-list row, compatible price-level order-book rows, then maker market-order rows behind `Head`/`Next` up to bounded market match caps |

The driver warms Pebble or blockbuffer raw reads only. It does not mutate
`StateDB` object caches from worker goroutines.

## Covered Contract Families

| Contract family | Prefetch hints |
| --- | --- |
| TRX and TRC10 transfer | owner account, recipient account; transfer-style recipient contract metadata where a contract-address check may follow; TRC10 legacy metadata, name index, and numeric-id V2 metadata |
| TVM trigger | owner account, contract account, contract metadata, contract code row, contract origin account from metadata, Blackhole account-name index |
| TVM create | owner account, declared origin account, deterministic new contract account, new contract metadata, Blackhole account-name index |
| Contract settings | owner account, target contract account, target contract metadata, target contract origin account from metadata |
| Vote witness | voter account, each voted witness account, each voted witness capsule, pending-vote record, pending-vote index |
| Witness operations | owner account, owner witness capsule, witness index for creation, current brokerage row for brokerage updates |
| Proposal operations | owner account, owner witness capsule where validation requires it, proposal record by id, proposal index for creation |
| Stake 1.0 freeze/unfreeze | owner account, optional receiver account, legacy delegated-resource and account-index rows; unfreeze also warms pending-vote record/index rows |
| Stake 2.0 delegate/undelegate/unfreeze | owner account, receiver account for delegation, locked/unlocked delegated-resource rows, owner delegation-index row; unfreeze also warms pending-vote record/index rows |
| Shielded transfer | transparent from/to accounts when present and valid, proof-result cache row, spend anchor/nullifier rows, note-commitment count row for receiving transfers |
| Asset issue | owner account, owner-index row, legacy name metadata, name index |
| Market sell/cancel | owner account, TRC10 token metadata/name-index hints derivable from the envelope, owner market-account row, cancel order row, current pair price-list/price-count/current price-level order-book, reverse pair price-list, compatible reverse price-level order-book rows, and maker order rows reachable from those levels; `_` TRX token legs are skipped for TRC10 metadata |
| Exchange token operations | owner account, TRC10 token metadata/name-index hints derivable from the envelope, both V1/V2 exchange rows when an exchange id is present, and metadata-derived TRC10 asset rows for both exchange token legs |
| Asset update/unfreeze | owner account plus metadata-derived TRC10 asset rows for the asset recorded on the owner account |
| Account metadata | owner account plus account-name or account-ID uniqueness index rows |
| Owner-only actuators | owner account for witness, proposal, brokerage, freeze-v2, and withdraw contracts |
| Account create and participate asset issue | owner account plus the explicitly referenced counterparty account; participate also warms TRC10 metadata/name-index rows |

## Not Yet Covered

- Per-account TRC10 balances beyond the owner/recipient account rows already
  warmed above. Balances live inside the account proto, so there is no separate
  asset-balance KV hint to enqueue yet.
- Reward-cycle witness VI and cycle-brokerage rows. Those require maintenance
  cycle context and are not directly encoded in transaction envelopes.
- Deep market linked-list rows beyond the current match prefetch caps. The
  executor itself caps successful matching at 20 maker orders; the prefetch
  hint mirrors that order cap and also caps compatible price-level probes to
  keep background warmups bounded.
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
