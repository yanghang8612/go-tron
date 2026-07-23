# java-tron proposal parity review — 2026-07-24

This review was triggered by mainnet block 6,215,591, where java-tron's
proposal-aware `DelegatedResourceCapsule.getExpireTimeForEnergy(...)` used the
bandwidth expiry before `ALLOW_MULTI_SIGN`, while go-tron used the energy expiry
unconditionally. The execution fix is commit `55963043`.

## Sources and scope

- java-tron: `e89c0d66520231b0b8abb2baee776d1d570e5fca` (`develop`, 2026-07-15)
- go-tron: `55963043ec4792dfb03008b06616b72cefe22fc0`
- Full syntactic evidence: [`fork-audit-2026-07-23.md`](fork-audit-2026-07-23.md)
- Scanner: `scripts/dev/fork_audit.sh`

The evidence scan covers all production Java modules, not only actuators. It
found 77 active `ProposalType` entries, 77 validation cases, 77 apply cases,
and 425 direct production calls to methods beginning with `getAllow`, `allow`,
or `support` after excluding proposal/store/config plumbing. Those calls span
84 Java files and 85 method names. The raw report retains every file and line.

This is a static consensus/wire review. `OK` means the proposal-controlled
choice and its state transition were found on both sides; it does not replace
historical replay or adversarial wire fixtures.

## Governance lifecycle parity

| Surface | Result | Evidence / qualification |
|---|---|---|
| Active proposal IDs | **OK** | All 77 active Java IDs have a go-tron `ProposalParamKey`. |
| Extra go-tron IDs | **Expected** | #27 is the Nile historical shielded replay exception; #1000/#1001 are Nile PQ-branch parameters and are not active in the audited Java `develop` tree. |
| Value validation | **OK** | All 77 Java `ProposalUtil` cases have go-tron validation coverage, including dependency checks and one-shot checks. |
| Software-version admission | **OK** | Java's active `forkController.pass(...)` requirements match `proposalRequiredVersion`; #17 and #44 retain their two-boundary special handling. |
| Approved-value application | **OK** | All 77 Java `ProposalService` cases map to the same DP key. IDs #17/#19 retain their distinct energy-limit setter semantics. |
| Guarded re-application | **OK** | #10, #20, #21, #44 and #77 are guarded like Java; Nile-only #27 is guarded separately. |
| Apply side effects | **OK** | Price histories (#3/#11/#68), permissions (#26/#30/#44/#70/#77), reward-cycle locks (#59/#67), adaptive-energy initialization (#21/#33), and Prague history-contract deployment (#95) are mirrored. |
| Same-cycle ordering | **OK** | go-tron applies expiring proposals in Java's descending proposal-ID order. |

## Execution-gate review

The complete ID-by-ID proposal table is in the evidence report. The rows below
group only proposals whose values select execution, consensus, storage, or wire
behavior.

| Proposal(s) | Java-controlled behavior | go-tron result |
|---|---|---|
| #9 | VM/system-contract availability and contract-transaction bandwidth sizing | **OK** — `AllowCreationOfContracts` gates VM validation and bandwidth sizing. |
| #10 | one-time removal of genesis committee voting power | **OK** — maintenance path and one-shot state match. |
| #14 | account-name update/index uniqueness | **OK**. |
| #15 | legacy token-name vs token-ID addressing across accounts/assets/exchanges | **OK** — centralized TRC10 final-key helpers cover the Java capsule branches. |
| #16 | V1 delegation and Stake 2.0 delegate/undelegate availability | **OK**. |
| #18 | TRC10 transfer in TVM, CALLTOKEN/TOKENBALANCE behavior | **OK**. |
| #20 | permissions, witness signing, VM address words, account defaults, delegated ENERGY expiry | **OK after `55963043`** — the previously missed capsule compatibility getter is now proposal-gated. |
| #21 | adaptive-energy accounting and activation initialization | **OK**. |
| #24 | protobuf unknown-field discard on P2P message decoding | **GAP (wire)** — Java switches `CodedInputStream.shouldDiscardUnknownFields`; go-tron currently uses ordinary `proto.Unmarshal`, which preserves unknown fields. Normal historical messages are unaffected, but malformed/future-field payload behavior is not wire-identical. |
| #25 | account-state-root production/validation | **OK** — block builder and block application both gate the Java-compatible account-state root. |
| #26 | Constantinople VM behavior, missing-origin billing, receiver rules, ClearABI permission | **OK**. |
| #30 | delegation/reward/brokerage maintenance model | **OK**. |
| #32 | Solidity 0.5.9 VM/precompile and delegated-receiver behavior | **OK**. |
| #35 | forbid TRX/TRC10 system transfers to contracts | **OK**. |
| #39 | shielded-TRC20 precompiles | **OK**, including the exact-height Nile historical activation exception. |
| #40 | PBFT production, handling, inventory and data sync | **OK at feature-gate level**; transport timing was not replay-proven in this review. |
| #41 | Istanbul opcodes and precompile energy schedules | **OK**. |
| #44 | market contracts, permissions, and Nile post-v4.8.1 disable semantics | **OK**. |
| #48/#49 | transaction fee pool vs black-hole burn paths | **OK** across bandwidth, energy and fee-paying actuators. |
| #51/#52 | new resource model and TVM freeze contracts | **OK**. |
| #53/#66 | Java account-asset DB migration/optimized storage selection | **Architecture-equivalent, not byte-layout equivalent** — go-tron has one canonical rooted TRC10 representation rather than Java's migration between account fields and `AccountAssetStore`. No execution-value divergence was found; a Java DB-layout comparison is not meaningful. |
| #59 | TVM voting/reward precompiles and vote clearing | **OK**. |
| #60 | EVM-compatible address/call/precompile behavior | **OK**. |
| #63 | London code-prefix and BASEFEE behavior | **OK**. |
| #65 | higher CPU limit and memory-operation energy tier | **OK**. |
| #67/#79 | new reward algorithm and old-reward optimization | **OK**. |
| #69 | V1 delegate optimization path | **OK** in freeze and unfreeze actuators. |
| #70/#77/#78 | Stake 2.0, cancel-all-unfreeze, and max lock period | **OK**, including block-unit lock periods and the composite max-lock gate. |
| #71 | optimized CHAINID return word | **OK**. |
| #72–#75 | dynamic energy enable/threshold/increase/max-factor | **OK**, including strict-math cycle catch-up. |
| #76 | Shanghai/PUSH0 | **OK**. |
| #81 | adjusted TVM energy schedules and SELFDESTRUCT account handling | **OK**. |
| #83 | Cancun transient-storage/MCOPY behavior | **OK**. |
| #87 | StrictMath exchange/dynamic-energy calculations | **OK** — fdlibm-compatible strict `pow` is gated. |
| #88 | consensus checks, witness ordering, result-size validation and resource accounting hardening | **OK**. |
| #89 | blob opcodes and point-evaluation precompile | **OK at execution-gate level**. |
| #94 | SELFDESTRUCT restriction, signature-array parsing and energy schedule | **OK**. |
| #95 | Prague activation and TIP-2935 block-hash-history deployment/update | **OK**. |
| #96 | Osaka CLZ/P256VERIFY/ModExp behavior | **OK**. |
| #97 | hardened resource calculation | **OK** across bandwidth, energy, delegation usage and billing. |
| #98 | hardened exchange calculation | **OK** across create/inject/withdraw/transaction and safe arithmetic. |

All numeric proposals not repeated above — fees, limits, ratios, allowances,
time intervals and dynamic-energy coefficients — are present in the 77-row
mapping and have matching validation ranges and DP consumers.

## Proposal-aware helpers outside actuators

This was the blind spot that caused block 6,215,591. The full-tree scan found
the following helper families and checked their callers explicitly:

| Java helper family | Proposal | Result |
|---|---:|---|
| `Commons`, `AccountCapsule`, `AssetIssueCapsule`, `ExchangeCapsule` token-key helpers | #15 | **OK** via go-tron final TRC10-key helpers. |
| `BlockCapsule.validateSignature` | #20 | **OK** via witness-permission-aware DPoS header verification. |
| `DelegatedResourceCapsule.getExpireTimeForEnergy(DynamicPropertiesStore)` | #20 | **Fixed** by `55963043`. |
| `ContractStateCapsule.catchUpToCycle` | #87 | **OK** via strict-math-aware dynamic-energy state. |
| `ReceiptCapsule` billing/resource helpers | #21/#26/#48/#49/#52/#70 | **OK** via energy bill, bandwidth, receipt and resource paths. |
| `AssetUtil` optimized-store selection | #66 | **Architecture-equivalent** as described above. |
| `TransactionUtil` fee-pool selection | #48 | **OK**. |

## Remaining risks and next action

1. **Concrete mismatch:** proposal #24 is stored and validated by go-tron but
   has no P2P decoder consumer. Fixing it requires threading a head-state
   `DiscardUnknown` decision through handshake, discovery, sync, transaction,
   block, inventory and PBFT protobuf decoders. It should be implemented as a
   single decode policy with before/after proposal fixtures, not patched at one
   message type at a time.
2. **Non-consensus network difference:** Java's relay trust-node admission uses
   #20 to choose witness address vs witness-permission address. go-tron's peer
   admission does not implement Java's witness relay trust-node subsystem.
   Block signature verification itself is already proposal-correct.
3. **Proof boundary:** static parity cannot prove every numeric corner. The
   generated callsite inventory should be paired with proposal-boundary replay
   fixtures, especially immediately before/after #15, #20, #21, #26, #32 and
   #35 — the gates relevant to early mainnet history.

The previous audit script only scanned Java actuators and incorrectly described
proposal validation as deferred. It has been replaced with the full-tree
inventory used by this review so future capsule/store-level gates are visible.
