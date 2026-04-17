# go-tron → java-tron Compatibility TODO

Gap analysis of what remains before go-tron is fully interoperable with java-tron on TRON mainnet. **Last refreshed: 2026-04-15** (after M1.1/M1.2/M1.3). Evidence-based; all file:line references point at the current tree, and every claim about a java-tron DP key, proposal ID, or store name has been verified against `/Users/asuka/Projects/tron/java-tron` at the checked-out revision.

Severity legend:
- **P0** — blocks basic mainnet participation (state would diverge, sync would fail, or the node would be disconnected).
- **P1** — blocks common client use cases (wallets, explorers, dApps) even if the node itself stays in consensus.
- **P2** — correctness/completeness polish; latent until a feature activates or a proposal passes.

---

## 1. Consensus, State & Fork Machinery — **P0**

State-divergence risk is concentrated here. These gaps cause the state root to drift from mainnet once the feature is exercised.

### 1.1 Adaptive energy limit (TIP-341) — **implemented** — *milestone M1.4*

Implemented in M1.4. `core/energy_adaptive.go` ports java-tron's `EnergyProcessor.updateAdaptiveTotalEnergyLimit` (contract 99/100, expand 1000/999, clamped to [base, base×multiplier]) and `updateTotalEnergyAverageUsage` (sliding-window average over 20 blocks). Both run per-block in `ProcessBlock` when `allow_adaptive_energy` is set. `SetTotalEnergyLimit` mirrors `saveTotalEnergyLimit2` side-effects. Proposal #21 activation auto-adjusts `targetRatio` (→2880) and `multiplier` (→50) when version ≥ 3.6.5. `availableAccountEnergy` now uses `TotalEnergyCurrentLimit` (dynamic) instead of `TotalEnergyLimit` (static). Missing DP keys `total_energy_average_time` and `block_energy_usage` added.

### 1.2 Storage fee / rent model (`StorageTaxProcessor`) — not implemented — *milestone M1.6*

Missing DP keys: `total_storage_pool`, `total_storage_tax`, `total_storage_reserved`, `storage_exchange_tax_rate`. No per-contract storage tax accounting in the state processor.

### 1.3 Dynamic energy pricing (TIP-1327) — **implemented** — *milestone M1.7*

Implemented in M1.7. New `core/types/contract_state.go` wraps the `ContractState` proto (energy_usage, energy_factor, update_cycle) with `CatchUpToCycle`: single-step increase when last-cycle usage exceeded the threshold (`factor' = min(maxFactor, (factor+decimal) × (1 + increaseFactor/decimal) − decimal)`) followed by compound decay across quiet cycles (`factor' = max(0, (factor+decimal) × (1 − increaseFactor/4/decimal)^cycleCount − decimal)`). Stored per contract via new rawdb prefix `cs-` (mirrors java-tron `ContractStateStore`). The VM interpreter fetches the factor once at contract entry (`updateContractEnergyFactor`), applies `cost × factor / decimal` per opcode when factor > 1.0×, and writes the accumulated **base** (pre-factor) energy back to `ContractState.energy_usage` at return/revert — that counter is the input the next cycle's catch-up tests against `DynamicEnergyThreshold`.

### 1.4 Reward algorithm v2 / vote rewards — **implemented** — *milestone M1.5*

Implemented in M1.5. `core/reward.go` + `core/reward/voter_reward.go` port java-tron's MortgageService + DelegationStore:
- Per-block `payBlockReward` splits by brokerage: witness commission → allowance, voter pool → DelegationStore (rawdb prefix `dl-`).
- Per-block `payStandbyWitness` distributes `witness_127_pay_per_block` pro-rata by votes among the top-127 (only when `change_delegation` is on).
- `applyRewardMaintenance` runs VI accumulation (`deltaVi = reward × 10^18 / voteCount`) + cycle rollover at each maintenance boundary.
- `ComputeVoterReward` handles old (pro-rata) and new (VI-based) algorithms with a hybrid split across `new_reward_algorithm_effective_cycle`.
- `withdrawReward` in the actuator settles pending voter rewards into allowance; called before vote changes in `VoteWitnessActuator.Execute` to preserve java-tron's invariant.
- Proposal #67 (ALLOW_NEW_REWARD) side-effect sets `new_reward_algorithm_effective_cycle = currentCycle + 1` on activation.
- `WithdrawBalanceActuator` no longer gates on `IsWitness` — voters can now withdraw pending rewards.

Known divergence from java-tron flagged for M0″ conformance: VI uses go-tron's immediate-vote-application counts vs java-tron's pre-countVote counts at the same maintenance boundary. Functionally equivalent when votes don't change during a cycle.

### 1.5 Freeze-V2 delegated resource consumption — **implemented** — *milestone M1.8*

Implemented in M1.8. `actuator/undelegate_resource.go` now mirrors java-tron's usage-transfer math:
- Before reducing balances, receiver's net/energy usage is recovered and the portion proportional to the undelegated balance (capped at `maxUsage = unDelegateBalance/TRX × totalLimit/totalWeight`) is peeled off and folded into the owner's counter.
- Both receiver and owner `latestConsumeTime` are advanced so the subsequent decay tracks from the right point.

Known parity gap flagged for M0″: go-tron uses a single global 24h recovery window (`params.WindowSizeMs`), while java-tron has per-account window sizes reshuffled via `getNewWindowSize` during undelegation. Functionally equivalent when windows haven't diverged. Separate lock/unlock delegation entries (java-tron `createDbKeyV2(owner, receiver, lock)`) are also not yet split — go-tron uses a single `DelegatedResource` record with per-resource expire time.

### 1.6 DynamicProperties keys still absent

M1.1 backfilled 76 DP keys. Verified still missing from go-tron (each was cross-checked against java-tron `DynamicPropertiesStore.java`):

- **Freeze-V2 windows** — `max_frozen_time`, `min_frozen_time`, `max_frozen_supply_time`, `min_frozen_supply_time`, `max_frozen_supply_number`, `witness_allowance_frozen_time`.
- **Public-pool bandwidth** — `public_net_usage`, `public_net_limit`, `public_net_time`, `one_day_net_limit`.
- **Fee tracking** — `total_transaction_cost`, `total_create_account_cost`, `memo_fee_history`, `energy_price_history`, `bandwidth_price_history`, `transaction_fee_pool`.
- **System bookkeeping** — `block_filled_slots`, `block_filled_slots_index`, `version_number`.

Add each to `core/state/dynamic_properties.go` defaults plus getters/setters; where a governance proposal writes to the key, add a row to `proposalParamKey` in `core/forks/forks.go`.

### 1.7 rawdb schema — missing store prefixes

`core/rawdb/schema.go` has ~20 prefixes vs java-tron's ~37. Verified-missing stores (each was confirmed to exist in `java-tron/chainbase/src/main/java/.../store/*Store.java`):

- **Indexing:** ✅ `account-asset` (`aa-`), `account-id-index` (`aid-`), `account-trace` (`at-`), `delegated-resource-account-index` (`drax-`) — M2 PR-1 (2026-04-17). Accessors are infrastructure-only; actuator wiring lands in follow-up PRs.
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

- **Conformance corpus (M0″):** **Phase 1 landed 2026-04-17** — replay engine `core/conformance/`, CLI `gtron-replay`, capture helpers `fixture-closure` / `fixture-digest`, smoke corpus, and full operator protocol in `docs/dev/conformance-harness.md`. `make conformance-replay` green on smoke. **Phase 2 blocked on java-tron operator access** — record 3 mainnet ranges (freeze-v2 activation, maintenance boundary, contract-dense), populate each `divergence-allowlist.json` with known M1.5 / M1.8 parity gaps, ship `scripts/system_test_cross.sh`. M0″ exits when `make conformance-replay-exit-gate` returns 0 (every allowlist empty).
- **Cross-impl integration coverage:** `scripts/system_test.sh` exercises 2-node gtron↔gtron. Add a parallel `system_test_cross.sh` running gtron ↔ java-tron end-to-end (both sides producing and relaying).
- **Genesis / params (`params/mainnet.go`):** 27 GRs + 3 genesis accounts match java-tron's `config.conf`. Still missing genesis-level defaults for `allowMultiSign`, `allowAccountStateRoot`, `allowCreationOfContracts`, and shielded defaults — these are currently hardcoded in `dynamic_properties.go` rather than driven off the genesis file.

---

## Completed since 2026-04-12

- **M0″ conformance replay harness — Phase 1** (2026-04-17) — `core/conformance/` pure-Go engine (seed loader with raw Account proto ingestion; DigestB/DigestC; allowlist with stale detection; Report; ReplayRange; Snapshot round-trip) with 26 unit tests. `cmd/gtron-replay` CLI + `scripts/conformance_replay.sh` + Makefile targets. `cmd/fixture-closure` (extracts touched-address closure from blocks.bin, merges optional `--standby-witnesses` CSV) and `cmd/fixture-digest` (java-tron capture snapshot → OracleEntry) for Phase 2 operator use. `test/fixtures/mainnet-blocks/smoke/` 5-block synthetic range regenerable via `go run ./scripts/fixtures/cmd/gen-smoke`. `docs/dev/conformance-harness.md` covers replay commands + the Phase 2 prune-and-replay-and-snapshot capture protocol + allowlist policy. Phase 2 (record 3 real mainnet ranges, populate allowlists with known M1.5/M1.8 parity gaps, ship `scripts/system_test_cross.sh`) is blocked on mainnet java-tron access; M0″ exits when every range's allowlist is empty.


These items from earlier revisions of this file are now addressed and no longer appear above. Referenced here so reviewers can cross-check the decrease in scope:

- **DP-key backfill + rename + proposal-ID mapping rebuild** (M1.1) — `core/state/dp_key_mapping.go`, 76-key fixture match, `ProposalParamKey` rebuilt from `ProposalUtil.ProposalType`. Two residual routing bugs (proposals #19 and #49) corrected 2026-04-15.
- **Freeze V1 legacy support** (M1.2) — `availableAccountNet` / `availableAccountEnergy` sum V1+V2, freeze/unfreeze sync `total_{net,energy}_weight`, post-`allow_new_resource_model` gate rejects new V1 freezes.
- **Fork version-bit tracking + audit** (M1.3) — `ForkController` tallies SR votes from `BlockHeader.raw.version`, `fc.IsActive` combines DP soft flag with hardForkTime + rate quorum, `docs/dev/fork-audit-2026-04-15.md` snapshot catalogues execution-path vs proposal-validation gates. Two phantom AllowFlags (`AllowTvmBigInteger`, `AllowTvmSolidity058`) with no java-tron counterpart removed; two alias bugs (`AllowStakingV2`, `AllowTvmShieldedToken`) pointed at the real proposal keys.
- **Adaptive energy limit** (M1.4) — `core/energy_adaptive.go` ports per-block `total_energy_current_limit` adjustment (contract 99/100 / expand 1000/999, sliding-window average, clamp to [base, base×multiplier]). `availableAccountEnergy` fixed to use `TotalEnergyCurrentLimit`. Proposal #21 side-effects (version ≥ 3.6.5: ratio→2880, multiplier→50). `ForkController` wired into `BlockChain`.
- **Reward algorithm v2 + voter rewards** (M1.5) — `core/reward.go` + `core/reward/voter_reward.go` + DelegationStore rawdb (prefix `dl-`). Brokerage-split per-block reward, per-block pro-rata standby, maintenance-time VI accumulation + cycle rollover, hybrid voter-reward computation (old pro-rata / new VI), `withdrawReward` settles into allowance. `VoteWitnessActuator` settles rewards before vote mutation. Proposal #67 sets effective cycle. DP keys `current_cycle_number` + `new_reward_algorithm_effective_cycle` added.
- **Freeze-V2 delegation usage transfer** (M1.8) — new `core/delegation` package shared between actuator (`delegate_resource.go`, `undelegate_resource.go`) and VM opcodes (`0xDE DELEGATERESOURCE`, `0xDF UNDELEGATERESOURCE`). Delegate path refreshes owner's usage before the frozen pool shifts; undelegate path transfers the proportional usage back from receiver to owner. `SetLatestConsumeTimeForEnergy` added to StateDB.
- **Dynamic energy pricing** (M1.7) — `core/types/contract_state.go` + rawdb `cs-` prefix for per-contract energy factor; `vm/dynamic_energy.go` hooks `Interpreter.Run` to fetch factor at entry, penalize opcodes with `cost × factor / decimal`, and accumulate base usage for the next cycle's threshold test. Proposals #72-#75 continue to flow through `proposalParamKey`.

---

## Suggested sequencing

1. **State divergence remaining (§1.2):** only M1.6 (storage rent) remains. Dormant on mainnet (never activated) — lowest-priority item on the M1 track.
2. **DP + rawdb backfill (§1.6, §1.7):** prerequisite for any store-layer work past M2.
3. **Conformance harness (M0″):** replay mainnet blocks, diff state. Makes subsequent work measurable; run only after M1.4/M1.5/M1.8 land.
4. **P2P hardening (§2.3, §2.4, §2.6):** tolerable peer loss and DoS resistance.
5. **gRPC Wallet server (§4.1):** unblocks the ecosystem.
6. **PBFT routing (§2.1):** required only for validator operation; wait until 1–4 are solid.
7. **TVM Cancun opcodes (§3.1):** ship with whatever TRON proposal activates them.
