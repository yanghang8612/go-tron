# go-tron → java-tron Compatibility TODO

Gap analysis of what remains before go-tron is fully interoperable with java-tron on TRON mainnet. Snapshot date: **2026-04-15**. Evidence-based; file:line references point at the current tree.

Severity legend:
- **P0** — blocks basic mainnet participation (state would diverge, sync would fail, or the node would be disconnected).
- **P1** — blocks common client use cases (wallets, explorers, dApps) even if the node itself stays in consensus.
- **P2** — correctness/completeness polish; latent until a feature activates or a proposal passes.

---

## 1. Consensus, State & Fork Machinery — **P0**

State-divergence risk is concentrated here. These gaps will cause go-tron's state root to drift from mainnet as soon as the corresponding feature is exercised.

### 1.1 Adaptive energy limit (TIP-341) — not implemented
- `DynamicPropertiesStore` keys `total_energy_current_limit`, `total_energy_target_limit`, `total_energy_average_usage`, `total_energy_average_time`, `adaptive_resource_limit_multiplier`, `adaptive_resource_limit_target_ratio` are absent from `core/state/dynamic_properties.go`.
- No recalculation pass on maintenance boundary.
- **Effect:** per-block energy limit wrong once mainnet is under load.

### 1.2 Fork version-bit tracking — missing
- java-tron advertises SR software version in block headers; `ForkController.statsByVersion()` gates activation on quorum.
- go-tron's `core/forks/forks.go` only keys on block number / timestamp. No version bitmap, no SR-readiness check.
- **Effect:** cannot safely vote in a TIP that uses version-bit activation; hardcoded fork heights are the only gate.

### 1.3 Actuator/VM fork-gate enforcement inconsistent
- `forks.IsActive()` exists but isn't called from every actuator. Spot-check: delegate/undelegate actuators, cancel-all-unfreeze v2, and VM staking opcodes need audit for `IsActive(<tip>)` guards.
- **Action:** grep every java-tron `forkController.pass(...)` call, mirror the check in the corresponding Go path.

### 1.4 Freeze V1 (legacy) — not supported
- `core/bandwidth.go:29` only looks at `GetFrozenV2Amount`. Pre-V2 frozen balances with delay unfreezes aren't consumable.
- **Effect:** historical accounts with legacy frozen balance will fail bandwidth checks.

### 1.5 Storage fee / rent model (`StorageTaxProcessor`) — not implemented
- Missing DP keys: `total_storage_pool`, `total_storage_tax`, `total_storage_reserved`, `storage_exchange_tax_rate`.
- No per-contract storage tax accounting in state processor.

### 1.6 Dynamic energy pricing (TIP-1327) — not implemented
- `allow_dynamic_energy` flag exists but no scaling logic when contracts go hot.

### 1.7 Reward algorithm v2 / vote rewards
- Missing DP keys: `new_reward_algorithm_effective_cycle`, `allow_new_reward`, `allow_old_reward_opt`, `current_cycle_number`.
- `consensus/dpos/reward.go` implements block reward + flat brokerage only; no `RewardViStore`-style per-cycle accumulated-reward bucket, no dynamic brokerage, no `WITNESS_127_PAY_PER_BLOCK` standby allowance path.

### 1.8 DynamicProperties keys missing (other clusters)
Grouped; each needs an entry in `core/state/dynamic_properties.go` plus a proposal-ID → key mapping in `core/forks/forks.go`:
- **Freeze-V2 windows:** `max_frozen_time`, `min_frozen_time`, `max_frozen_supply_time`, `min_frozen_supply_time`, `max_frozen_supply_number`, `witness_allowance_frozen_time`, `max_delegate_lock_period`.
- **Public-pool bandwidth:** `public_net_usage`, `public_net_limit`, `public_net_time`, `one_day_net_limit`.
- **Fee tracking:** `total_transaction_cost`, `total_create_account_cost`, `total_create_witness_cost`, `memo_fee`, `memo_fee_history`, `energy_price_history`, `bandwidth_price_history`, `transaction_fee_pool`, `allow_transaction_fee_pool`.
- **TVM flags w/o DP backing:** `allow_tvm_shanghai`, `allow_tvm_osaka`, `allow_higher_limit_for_max_cpu_time_of_one_tx`, `allow_strict_math`, `allow_cancel_all_unfreeze_v2`, `allow_optimize_return_value_of_chain_id`, `consensus_logic_optimization`, `allow_account_state_root`, `allow_account_asset_optimization`, `allow_asset_optimization`.
- **System bookkeeping:** `state_flag` (maintenance marker), `block_filled_slots` / `block_filled_slots_index` (slot presence bitmap), `version_number`, `latest_version`.

### 1.9 rawdb schema — missing store prefixes
`core/rawdb/schema.go` has ~20 prefixes vs java-tron's 37. Add prefixes + accessors for:
- **Indexing:** `account-asset`, `account-id-index`, `account-trace`, `delegated-resource-account-index`.
- **Consensus:** `witness-schedule` snapshot, `section`, `pbft-signdata`, `tree-block-index` (fork-tree tracking).
- **History / audit:** `transaction-history`, `transaction-retstore`, `balance-trace`, `receipt` (cached TX result), `check-point-v2`, `reward-vi`, `accumulated-reward`.
- **Market side:** `market-account`, `market-pair-to-price`, `market-pair-price-to-order` (only the order side is in `accessors_market.go` today).
- **Shielded:** `zkproof`, `note-commitment`, `incremental-merkle-tree`, `roots` (Sapling commitment roots).
- **Contract:** `abi` (ABI currently lives inline on ContractState).

### 1.10 Freeze-V2 delegated resource consumption
- Delegation records are written, but transaction processing doesn't debit the *delegatee's* consumable resource pool from the *delegator's* bucket. Audit `core/resource.go` + `core/bandwidth.go` against `DelegationService` in java-tron.

---

## 2. P2P / Net — **P0 for validator, P1 for full-node**

App-layer handshake and block/tx sync are verified working against a real java-tron (see `docs/dev/p2p-interop-status.md`). Gaps below are beyond that baseline.

### 2.1 PBFT message routing — entirely absent
- No handler for message codes in the PBFT range; no `PbftMsgHandler` / `PbftDataSyncHandler` equivalent.
- No signature verification, no dedup cache, no epoch validation.
- **Effect:** go-tron cannot participate as a validator and will be ignored by peers sending quorum blocks.

### 2.2 Peer-level advertisement & relay services
- No equivalent to java-tron's `AdvService` (mempool gossip batching, `MAX_TRX_FETCH_PER_PEER`, spread scheduling) or `RelayService` (max-5-peers-per-IP, backup reconnection).
- **Effect:** tx propagation is naive; node is fragile under peer churn.

### 2.3 Sync robustness
- `net/sync.go` opens a single syncPeer with no backoff / retry on disconnect.
- No `FetchBlockService`-style timeout-and-reschedule.
- No `PeerStatusCheck` / `TronState` (GOOD / BAD / SYNC_STARTED) tracking.
- No fork-recovery service akin to java-tron's `ResilienceService`.

### 2.4 Rate limiting / DOS protection
- No per-message-type rate limiter (java-tron has `P2pRateLimiter` with Guava `RateLimiter` cache).
- No per-IP connection cap (java-tron caps 5 peers per IP).
- No per-peer message statistics.

### 2.5 Keep-alive / status cadence
- libp2p-layer keepalive in `p2p/peer.go` is fine (uses `KeepAliveTimeout/2`).
- Application-layer keepalive in `net/handler.go:135` pings every 30s — verify this is what java-tron's `KeepAliveService` expects, or remove if the libp2p-layer ping already satisfies liveness.

### 2.6 Discovery polish
- Node eviction policy on 15-s ping timeout is not implemented in `p2p/discover/table.go`.
- Table sizing (17 buckets × 16 entries) vs current implementation — confirm `Node.id.Distance()` uses java-tron's Kademlia-XOR metric.

---

## 3. TVM / Precompiles — **P2** (not yet activated on mainnet)

### 3.1 Cancun/Dencun opcodes declared but not wired
- `vm/tvm_config.go:24` has a `Cancun` flag; `opcodes.go` + `jump_table.go` do **not** define or register:
  - `MCOPY` (0x5E)
  - `BLOBHASH` (0x49)
  - `BLOBBASEFEE` (0x4A)
- Add opcode bytes, handlers, energy costs, and gate entries on `Cancun` / blob flags in the jump table.

### 3.2 Energy model polish
- Verify `opSstore` applies `EnergySstoreRefund` correctly (refund bookkeeping needs spot audit).
- Missing explicit MCOPY memory-expansion cost once MCOPY lands.

### 3.3 Precompile parity
- No missing precompile addresses in the 0x01–0x0a or 0x01000001–0x01000015 ranges (precompile_std + precompile_tron match). No action.

---

## 4. API Surface — **P1**

Go-tron today is effectively a read-mostly HTTP API. Most wallet/dApp flows won't work.

### 4.1 gRPC Wallet service — **no server implementation**
- `proto/api/api.proto` defines the full Wallet service (200+ RPCs) but no `*_grpc.pb.go` and no server in `internal/`.
- This is the single biggest client-facing gap: wallets, SDKs, and most dApps use gRPC as the primary API.

### 4.2 HTTP servlet coverage (~30% of java-tron)
Missing servlet groups (each bullet = a cluster, not a single endpoint):
- **Account / permission / id:** `createaccount`, `updateaccount`, `setaccountid`, `accountpermissionupdate`, `getaccountbyid`, `getaccountbalance`, `getaccountnet`.
- **Transaction builders** (sign-on-server flow): `createcommontransaction`, `transferasset`, `participateassetissue`, `createwitness`, `votewitnessaccount`, `updatewitness`, `withdrawbalance`, `updatebrokerage`, `freezebalance` (v1), `unfreezebalance` (v1), `freezebalancev2`, `unfreezebalancev2`, `cancelallunfreezev2`, `delegateresource`, `undelegateresource`, `withdrawexpireunfreeze`.
- **TRC10:** `createassetissue`, `updateasset`, `getpaginatedassetissuelist`, pagination variants.
- **Smart contract:** `clearabi` (deploy already present).
- **Shielded / TRC20-shielded:** `createshieldedtransaction`, `createshieldedcontractparameters`, `scannotebyivk`, `scannotebyovk`, `getnewshieldedaddress`, `getzenpaymentaddress`, `getmerkletreevoucherinfo`, `getdiversifier`, `isshieldedtrc20contractnotespent`.
- **Exchange / market:** `exchangecreate`, `exchangeinject`, `exchangetransaction`, `exchangewithdraw`, `marketcancelorder`, `marketsellasset`.
- **Proposal:** `getproposalbyid`, `getpaginatedproposallist`.
- **Monitoring:** `metrics`, `getbandwidthprices`, `getenergyprices`.
- **Transaction meta:** `gettransactionreceiptbyid`, `gettransactionapprovedlist`, `gettransactionsignweight`, `validateaddress`.

### 4.3 JSON-RPC (eth-compat) — ~15 of ~69 methods implemented
Missing the write path and filter/subscription machinery. Highest-value adds:
- `eth_sendRawTransaction`, `eth_sendTransaction`
- `eth_gasPrice`, `eth_estimateGas` (verify present impl isn't a stub)
- `eth_newFilter`, `eth_newBlockFilter`, `eth_newPendingTransactionFilter`, `eth_getFilterChanges`, `eth_getFilterLogs`, `eth_uninstallFilter`
- `eth_accounts`, `eth_sign`, `eth_signTransaction`
- `web3_sha3`, `net_listening`, `net_peerCount`

### 4.4 Solidity / PBFT API variants
- java-tron exposes confirmed-state (`interfaceOnSolidity`) and PBFT-committed-state (`interfaceOnPBFT`) variants of HTTP and JSON-RPC APIs.
- go-tron has neither. Wallets that need finality guarantees must either poll a java-tron node or rely on block-depth heuristics.

### 4.5 Event subscription system
- java-tron has `services/event/` (EventService, RealtimeEventService, HistoryEventService, SolidEventService) with pluggable triggers.
- go-tron has no WebSocket/SSE/pub-sub layer. dApps cannot subscribe to contract logs or block events.

---

## 5. Actuators — essentially complete

Dispatch covers all 37 mainnet `ContractType` enums; `actuator/` has Go equivalents for every java-tron actuator under `actuator/src/main/java/.../actuator/`. Nativecontract-level operations (freeze v2, vote, delegate, withdraw-reward) are implemented as VM opcodes `0xD6–0xDF`. No action required at the dispatch level — but individual actuators still inherit the state-layer gaps in §1 (fork gating, DP keys, delegation consumption). That's where validation drift will show up in practice.

---

## 6. Cross-cutting / Meta

- **Integration coverage:** `scripts/system_test.sh` exercises 2-node gtron↔gtron. Add a parallel `system_test_cross.sh` that runs gtron ↔ java-tron end-to-end (both sides producing and relaying).
- **Conformance corpus:** replay a slice of mainnet blocks through go-tron's state processor and diff the resulting state trie against a java-tron node at the same height. This is the only way to catch the silent divergences in §1.
- **Genesis / params (`params/mainnet.go`):** 27 GRs + 3 genesis accounts match java-tron's `config.conf`. Still missing genesis-level defaults for `allowMultiSign`, `allowAccountStateRoot`, `allowCreationOfContracts`, shielded defaults — these are currently hardcoded in `dynamic_properties.go` rather than driven off the genesis file.
- **Phase plans not yet executed:** `docs/superpowers/plans/2026-04-12-p2p-discovery.md`, `2026-04-12-dex-actuators.md`, `2026-04-12-hard-fork-mechanism.md` — cross-reference against the items above before starting new work; some items here are already scoped there.

---

## Suggested sequencing

1. **State divergence killers (§1.1, §1.2, §1.3, §1.4, §1.8):** without these, any long-running sync against mainnet will eventually fail block verification.
2. **rawdb + DynamicProperties backfill (§1.8, §1.9):** prerequisite for 1 and for future proposals.
3. **Conformance harness (§6):** replay mainnet blocks, diff state. Makes subsequent work measurable.
4. **P2P hardening (§2.3, §2.4, §2.6):** tolerable peer loss and DoS resistance.
5. **gRPC Wallet server (§4.1):** unblocks the ecosystem.
6. **PBFT routing (§2.1):** required only for validator operation; can wait until 1–4 are solid.
7. **TVM Cancun opcodes (§3.1):** only needed if a TRON-Cancun equivalent is proposed; ship with that fork's plan.
