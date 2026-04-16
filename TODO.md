# go-tron → java-tron Compatibility TODO

Gap analysis of what remains before go-tron is fully interoperable with java-tron on TRON mainnet. **Last refreshed: 2026-04-15** (after M1.1/M1.2/M1.3). Evidence-based; all file:line references point at the current tree, and every claim about a java-tron DP key, proposal ID, or store name has been verified against `/Users/asuka/Projects/tron/java-tron` at the checked-out revision.

Severity legend:
- **P0** — blocks basic mainnet participation (state would diverge, sync would fail, or the node would be disconnected).
- **P1** — blocks common client use cases (wallets, explorers, dApps) even if the node itself stays in consensus.
- **P2** — correctness/completeness polish; latent until a feature activates or a proposal passes.

---

## 1. Consensus, State & Fork Machinery — **P0**

State-divergence risk is concentrated here. These gaps cause the state root to drift from mainnet once the feature is exercised.

### 1.1 Adaptive energy limit (TIP-341) — not implemented — *milestone M1.4*

All required DP keys exist in `core/state/dynamic_properties.go` as of M1.1 (`total_energy_current_limit`, `total_energy_target_limit`, `total_energy_average_usage`, `total_energy_average_time`, `adaptive_resource_limit_multiplier`, `adaptive_resource_limit_target_ratio`). What's missing is the behaviour:

- Per-block adjustment of `total_energy_current_limit` based on moving-average usage and the `allow_adaptive_energy` gate. Mirrors `DynamicEnergyProcessor.updateTotalEnergyAverageUsage` / `EnergyProcessor.updateAdaptiveTotalEnergyLimit` in java-tron.
- No maintenance-boundary recalculation.

**Effect:** per-block energy limit wrong once mainnet is under load.

### 1.2 Storage fee / rent model (`StorageTaxProcessor`) — not implemented — *milestone M1.6*

Missing DP keys: `total_storage_pool`, `total_storage_tax`, `total_storage_reserved`, `storage_exchange_tax_rate`. No per-contract storage tax accounting in the state processor.

### 1.3 Dynamic energy pricing (TIP-1327) — not implemented — *milestone M1.7*

DP key `allow_dynamic_energy` backfilled in M1.1 and per-contract `EnergyProperty` fields exist in `AccountCapsule`. Missing behaviour: the usage-driven scaling factor when a contract goes hot. Mirrors java-tron's `DynamicEnergyProcessor` cycle update.

### 1.4 Reward algorithm v2 / vote rewards — not implemented — *milestone M1.5*

DP keys `new_reward_algorithm_effective_cycle`, `allow_new_reward`, `allow_old_reward_opt`, `current_cycle_number` backfilled in M1.1. Missing behaviour:

- `consensus/dpos/reward.go` implements block reward + flat brokerage only.
- No `RewardViStore`-style per-cycle accumulated-reward bucket.
- No dynamic brokerage rate.
- No `witness_127_pay_per_block` standby allowance distribution.

### 1.5 Freeze-V2 delegated resource consumption — *milestone M1.8*

Delegation records are written and V1 acquired-in sums are counted (`availableAccountNet` / `availableAccountEnergy` since M1.2), but transaction processing does not yet debit the delegatee's consumable pool from the delegator's V2 bucket during actual resource use. Needs audit against java-tron's `DelegationService` and wiring into `core/resource.go` / `core/bandwidth.go`.

### 1.6 DynamicProperties keys still absent

M1.1 backfilled 76 DP keys. Verified still missing from go-tron (each was cross-checked against java-tron `DynamicPropertiesStore.java`):

- **Freeze-V2 windows** — `max_frozen_time`, `min_frozen_time`, `max_frozen_supply_time`, `min_frozen_supply_time`, `max_frozen_supply_number`, `witness_allowance_frozen_time`.
- **Public-pool bandwidth** — `public_net_usage`, `public_net_limit`, `public_net_time`, `one_day_net_limit`.
- **Fee tracking** — `total_transaction_cost`, `total_create_account_cost`, `memo_fee_history`, `energy_price_history`, `bandwidth_price_history`, `transaction_fee_pool`.
- **System bookkeeping** — `block_filled_slots`, `block_filled_slots_index`, `version_number`.

Add each to `core/state/dynamic_properties.go` defaults plus getters/setters; where a governance proposal writes to the key, add a row to `proposalParamKey` in `core/forks/forks.go`.

### 1.7 rawdb schema — missing store prefixes

`core/rawdb/schema.go` has ~20 prefixes vs java-tron's ~37. Verified-missing stores (each was confirmed to exist in `java-tron/chainbase/src/main/java/.../store/*Store.java`):

- **Indexing:** `account-asset`, `account-id-index`, `account-trace`, `delegated-resource-account-index`.
- **Consensus:** `witness-schedule` snapshot, `section-bloom`, `pbft-signdata`, `tree-block-index`.
- **History / audit:** `transaction-history`, `transaction-retstore`, `balance-trace`, `check-point-v2`, `reward-vi`.
- **Market side:** `market-account`, `market-pair-to-price`, `market-pair-price-to-order` (only the order side is in `accessors_market.go`).
- **Shielded:** `zkproof`, `incremental-merkle-tree`.
- **Contract:** `abi` (ABI currently lives inline on ContractState).

---

## 2. P2P / Net — **P0 for validator, P1 for full-node**

App-layer handshake and block/tx sync are verified working against a real java-tron (see `docs/dev/p2p-interop-status.md`). Gaps below are beyond that baseline.

### 2.1 PBFT message routing — entirely absent

No handler for PBFT-range message codes; no `PbftMsgHandler` / `PbftDataSyncHandler` equivalent, no signature verification, no dedup cache, no epoch validation. go-tron cannot participate as a validator; peers sending quorum blocks will ignore it.

### 2.2 Peer-level advertisement & relay services

No equivalent to `AdvService` (mempool gossip batching, `MAX_TRX_FETCH_PER_PEER`, spread scheduling) or `RelayService` (max-5-peers-per-IP, backup reconnection). Tx propagation is naive and the node is fragile under peer churn.

### 2.3 Sync robustness

- `net/sync.go` opens a single syncPeer with no backoff / retry on disconnect.
- No `FetchBlockService`-style timeout-and-reschedule.
- No `PeerStatusCheck` / `TronState` (GOOD / BAD / SYNC_STARTED) tracking.
- No fork-recovery service akin to `ResilienceService`.

### 2.4 Rate limiting / DoS protection

- No per-message-type rate limiter (java-tron has `P2pRateLimiter` backed by Guava `RateLimiter`).
- No per-IP connection cap (java-tron caps 5 peers per IP).
- No per-peer message statistics.

### 2.5 Keep-alive / status cadence

libp2p-layer keepalive in `p2p/peer.go` uses `KeepAliveTimeout/2`. Application-layer keepalive in `net/handler.go:135` pings every 30 s — confirm whether this is what java-tron's `KeepAliveService` expects, or drop if the libp2p-layer ping already satisfies liveness.

### 2.6 Discovery polish

- Node eviction on 15-s ping timeout not implemented in `p2p/discover/table.go`.
- Confirm `Node.id.Distance()` matches java-tron's Kademlia-XOR metric (17 buckets × 16 entries).

---

## 3. TVM / Precompiles — **P2** (not yet activated on mainnet)

### 3.1 Cancun/Dencun opcodes declared but not wired

`vm/tvm_config.go` has a `Cancun` flag; `opcodes.go` + `jump_table.go` do **not** define or register:
- `MCOPY` (0x5E)
- `BLOBHASH` (0x49)
- `BLOBBASEFEE` (0x4A)

Add opcode bytes, handlers, energy costs, and jump-table entries gated on `Cancun` / blob flags.

### 3.2 Energy model polish

- Spot-audit `opSstore` refund bookkeeping (`EnergySstoreRefund`).
- Add explicit MCOPY memory-expansion cost once MCOPY lands.

### 3.3 Precompile parity

No gaps in the `0x01–0x0a` and `0x01000001–0x01000015` ranges (precompile_std + precompile_tron match). No action.

---

## 4. API Surface — **P1**

go-tron today is effectively a read-mostly HTTP API. Most wallet/dApp flows won't work.

### 4.1 gRPC Wallet service — no server implementation

`proto/api/api.proto` defines the full Wallet service (200+ RPCs) but there is no `*_grpc.pb.go` and no server under `internal/`. Biggest client-facing gap: wallets, SDKs, and most dApps use gRPC as the primary API.

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

### 4.3 JSON-RPC (eth-compat) — ~15 of ~69 methods

Missing the write path and filter/subscription machinery. Highest-value adds: `eth_sendRawTransaction`, `eth_sendTransaction`, `eth_gasPrice`, `eth_estimateGas` (verify not a stub), filter RPCs, `eth_accounts`, `eth_sign`, `eth_signTransaction`, `web3_sha3`, `net_listening`, `net_peerCount`.

### 4.4 Solidity / PBFT API variants

java-tron exposes confirmed-state (`interfaceOnSolidity`) and PBFT-committed-state (`interfaceOnPBFT`) variants of HTTP and JSON-RPC APIs. go-tron has neither. Wallets needing finality guarantees must poll a java-tron node or rely on block-depth heuristics.

### 4.5 Event subscription system

java-tron has `services/event/` (EventService, RealtimeEventService, HistoryEventService, SolidEventService) with pluggable triggers. go-tron has no WebSocket/SSE/pub-sub layer — dApps cannot subscribe to contract logs or block events.

---

## 5. Actuators — essentially complete

Dispatch covers all 37 mainnet `ContractType` enums; `actuator/` has Go equivalents for every java-tron actuator under `actuator/src/main/java/.../actuator/`. Native-contract-level operations (freeze v2, vote, delegate, withdraw-reward) are implemented as VM opcodes `0xD6–0xDF`. No action required at the dispatch level — but individual actuators still inherit the state-layer gaps in §1 (fork gating, DP keys, delegation consumption).

---

## 6. Cross-cutting / Meta

- **Conformance corpus (M0″):** replay a slice of mainnet blocks through go-tron's state processor and diff the resulting state against a java-tron node at the same height. Deferred until M1.4–M1.8 land so the output isn't drowned in known-missing-feature noise.
- **Cross-impl integration coverage:** `scripts/system_test.sh` exercises 2-node gtron↔gtron. Add a parallel `system_test_cross.sh` running gtron ↔ java-tron end-to-end (both sides producing and relaying).
- **Genesis / params (`params/mainnet.go`):** 27 GRs + 3 genesis accounts match java-tron's `config.conf`. Still missing genesis-level defaults for `allowMultiSign`, `allowAccountStateRoot`, `allowCreationOfContracts`, and shielded defaults — these are currently hardcoded in `dynamic_properties.go` rather than driven off the genesis file.

---

## Completed since 2026-04-12

These items from earlier revisions of this file are now addressed and no longer appear above. Referenced here so reviewers can cross-check the decrease in scope:

- **DP-key backfill + rename + proposal-ID mapping rebuild** (M1.1) — `core/state/dp_key_mapping.go`, 76-key fixture match, `ProposalParamKey` rebuilt from `ProposalUtil.ProposalType`. Two residual routing bugs (proposals #19 and #49) corrected 2026-04-15.
- **Freeze V1 legacy support** (M1.2) — `availableAccountNet` / `availableAccountEnergy` sum V1+V2, freeze/unfreeze sync `total_{net,energy}_weight`, post-`allow_new_resource_model` gate rejects new V1 freezes.
- **Fork version-bit tracking + audit** (M1.3) — `ForkController` tallies SR votes from `BlockHeader.raw.version`, `fc.IsActive` combines DP soft flag with hardForkTime + rate quorum, `docs/dev/fork-audit-2026-04-15.md` snapshot catalogues execution-path vs proposal-validation gates. Two phantom AllowFlags (`AllowTvmBigInteger`, `AllowTvmSolidity058`) with no java-tron counterpart removed; two alias bugs (`AllowStakingV2`, `AllowTvmShieldedToken`) pointed at the real proposal keys.

---

## Suggested sequencing

1. **State divergence remaining (§1.1, §1.2, §1.4, §1.5):** the M1.4–M1.8 milestones. Each shipped independently behind its own fork gate.
2. **DP + rawdb backfill (§1.6, §1.7):** prerequisite for any store-layer work past M2.
3. **Conformance harness (M0″):** replay mainnet blocks, diff state. Makes subsequent work measurable; run only after M1.4/M1.5/M1.8 land.
4. **P2P hardening (§2.3, §2.4, §2.6):** tolerable peer loss and DoS resistance.
5. **gRPC Wallet server (§4.1):** unblocks the ecosystem.
6. **PBFT routing (§2.1):** required only for validator operation; wait until 1–4 are solid.
7. **TVM Cancun opcodes (§3.1):** ship with whatever TRON proposal activates them.
