# Parallel execution adversarial audit — 2026-08-26

## Incident chain

Mainnet sync stopped at block 22,123,859 while validating transaction 27 with
`insufficient balance`. The balance check was correct. The first local state
divergence was block 22,097,772, transaction 58: an asynchronous Transfer
retry published a stale shared-recipient post-image and lost an incoming
4,455 SUN. The later transaction merely consumed the corrupted balance.

This failure belongs to the same safety family as the historical VM omissions
at 18,414,381 and 20,674,403: a speculative result reached canonical state
without a sufficiently independent publication proof.

| network/family | first divergence | sync stop | latent distance | mechanism |
| --- | ---: | ---: | ---: | --- |
| mainnet / VM | 18,402,304 | 18,414,381 | 12,077 | empty-code worker published zero-energy SUCCESS and omitted payment |
| mainnet / VM | 20,616,256 | 20,674,403 | 58,147 | same empty-code publication family omitted reward/storage/energy effects |
| mainnet / Transfer | 22,097,772 | 22,123,859 | 26,087 | async retry overwrote a shared-recipient post-image and lost 4,455 SUN |

Nile's 19,541,561 → 19,716,962 insufficient-balance incident is numerically
near the mainnet VM events but is not the same bug family: it came from a
one-energy origin/caller split difference and accumulated an 840-SUN
overcharge, not speculative publication.

The historical mainnet manifest was re-read from the public full-node API on
2026-08-26 rather than inferred from the local failed database:

| role | height | canonical block ID | transaction evidence |
| --- | ---: | --- | --- |
| first VM divergence | 18,402,304 | `000000000118cc0069cb79b229f9957fdc56abb0db25272fc524c7b95d4a6f38` | tx 3 `1f63de3942671be7824c8bd2f98106002253e641152aa8fcdfb240d5f291a748` |
| first VM stop | 18,414,381 | `000000000118fb2ded825760c49c8d2588e04dcc283a12cf7d957413f7defed8` | tx 5 `6186f5fe1b5ba706e5b17b608df422cb2143b369bfbba73fd0af4366cffe799e` |
| second VM divergence | 20,616,256 | `00000000013a9440662a5f6a2fb1a462743640a94192ceb4c21eed0e98228d38` | tx 42 `0def93583b6b879aa744d87a7a76c02285d97902fa2a050fc59d512c8641c677` |
| second VM stop | 20,674,403 | `00000000013b77633413e57c9a741dbcb6c6faf7ab1f261c1210a19d08d88b7d` | tx 0 `59feaaa85dc3b264cf2a5207f8ddb4d20ca5074a9a71aa3b98fd0938928d60aa`; WINK follow-up tx 55 `e6fc2876547d39db5d736e008166439ec4899104de33770035b5e5a4805c7c45` |
| Transfer divergence | 22,097,772 | `0000000001512f6c4d82bb26c6c7cb0e33dae8609e2a816c416fb1de74a97607` | 148 tx; shared-recipient overwrite is tx 58 `acb8d8b7ce87a2897157dea5b0561190c7119892ea40e97c99b8e50214599fb7` |
| Transfer stop | 22,123,859 | `00000000015195535afeb6a0da541fca9e571c8d001826247a7bcdb6a6aa7917` | tx 27 `393caebf9b11e90e21f149c146f8a1471e20df851b0c187d605d32f35cf6a2fe` spends 31,400,620,357 SUN from the corrupted shared recipient |

## Confirmed defects

1. An async retry sent a shallow `TransactionWriteSet` map to the canonical
   goroutine and then continued to inspect/forward that map. The receiver could
   also rebase public-bandwidth entries. The handoff had no exclusive owner.
2. Full post-apply capture covered retained block-start results but not every
   schedulable sender suffix. An initially unfunded sender can become valid
   after an earlier canonical credit and publish a result that block-start
   execution never retained.
3. Existing post-publication comparisons were observational and fail-open.
4. WriteSet application ranged over a Go map and accepted logical keys that
   alias the same physical state cell, so application order could change the
   result.
5. The public `GetAccountKV` path could return bytes borrowed from a shared
   version carrier.
6. A protected state key was rejected from the RawKV publisher only when the
   same WriteSet also contained a typed write. A raw-only result could bypass
   the StateDB cache and rooted logical ownership.
7. Account-KV domain and typed-key semantic validation was not complete in the
   preflight pass, so a late applier error could occur after earlier mutations.
8. Generic AccountKV and RawKV carriers could target typed-cache or rawdb-owned
   namespaces, ignored fields could make distinct tagged-union keys address the
   same physical cell, and two dynamic-property kinds could share one physical
   name. These malformed sets were deterministic only by accident.
9. Several post-images admitted contradictory `Exists` and value contents.
   Storage/transient appliers ignore the presence bit, while code, metadata,
   AccountKV and RawKV deletion paths ignore residual bytes; those conflicts
   were detected only by the post-apply audit, after mutation.
10. A fresh account's full envelope and its exact scalar field post-images
    could disagree. Because application creates the account first and applies
    scalar fields second, the contradiction was likewise rejected only by the
    post-apply comparison; a commutative balance could also be double-applied
    after the envelope already contained its final value.
11. The schema allowed the commutative flag on every dynamic integer and on
    any account balance. Version validation intentionally treats these writes
    as order-independent deltas, so accepting a mislabeled ordinary property
    or participant balance would weaken the exact dependency proof.
12. `StateDB.Copy` and `CopyBlockExecutionBase` cloned the primary account
    protobuf but omitted materialized split-account caches and flags, witness
    signer state, TRC10/FrozenV2 point caches, contract runtime and storage-key
    layout. A copied oracle could therefore rehydrate an older row from its
    shared reader instead of retaining the source's point-in-time effective
    state.
13. The same copy boundary omitted pending transaction-finalization addresses,
    transient storage and the transaction-versioned reader/transaction/view
    maps. A copy taken at an adversarial transaction boundary could skip
    zero-storage or SELFDESTRUCT finalization, or execute against a different
    effective transient/versioned view.
14. Unknown/future transaction-access kinds were not protected by exhaustive
    enum sentinels, and the unsupported-access classifier could hide an unknown
    kind when another known unsupported kind was also present. That weakened
    the fail-closed proof for future schema additions.
15. `transactionInfoSlot.build` deliberately discarded every VM
    `InternalTransaction` even though TVM execution had produced the exact
    records. A live comparison of all 349 receipts in the six historical
    incident blocks found 157 receipts that matched the reference in every
    other JSON field but omitted their internal-transaction list. This did not
    cause the rooted-state divergences, but it violated full-node
    TransactionInfo parity and made the receipt publication oracle blind to a
    consumed VM result.
16. Dynamic-energy surcharge units were charged correctly to VM execution but
    never accumulated into an actuator result, so every persisted
    `ResourceReceipt.energy_penalty_total` and every constant-call
    `energy_penalty` remained zero. java-tron accumulates only successfully
    accepted penalty charges across the complete call tree and persists that
    total. Mainnet transaction
    `aebfcad791c00033b72c2287535512cfa929ef483eec25a91d3fb9113b62a3dd`
    at block 66,747,568 has a reference value of 25,905, proving the field is
    active production data rather than a reserved schema slot.
17. After retaining ordinary CALL/CREATE/SUICIDE internal transactions, the
    native staking/voting/reward opcodes still emitted none. Their nonce,
    receiver shape (VOTEWITNESS has an empty receiver rather than a zero TRON
    address), note, pre-`setValue` identity hash, rejection bit, VOTE JSON and
    optional CANCELALL details are all externally observable TransactionInfo
    fields. The implementation now constructs the same complete execution
    records and applies java-tron's three node-local `vm.save*` filters only
    after both publication oracles have compared the unfiltered result.
18. WITHDRAWEXPIREUNFREEZE removed expired rows when their wrapping Java-long
    sum was zero, and did not reproduce java-tron's negative-total and checked
    balance-overflow validation. Zero-total execution must return immediately
    without clearing even zero-valued expired rows; a negative or overflowing
    result must reject without mutation.
19. The DELEGATERESOURCE and UNDELEGATERESOURCE opcodes were enabled whenever
    Stake V2 was active but did not enforce the independent
    `allow_delegate_resource` dynamic-property gate used by both Java native
    processors. They could therefore mutate delegation state before the
    proposal activated.
20. WITHDRAWREWARD converted a non-positive allowance directly to an unsigned
    stack word. Java returns zero and leaves the non-positive allowance intact;
    the local return and featured-internal value now follow that behavior.
21. FREEZE did not validate the owner's legacy bandwidth-frozen repeated-field
    cardinality. Java validates that the count is exactly zero or one before
    both BANDWIDTH and ENERGY freezes, so malformed/imported state with two
    entries was accepted and merged locally instead of being rejected.
22. A fully drained legacy delegated-resource capsule was deleted by the VM
    UNFREEZE path. Java always writes the capsule after zeroing the selected
    leg, including an all-zero capsule, so deletion changed delegation-store
    state and wallet-query results.
23. A failed FREEZEBALANCEV2 with insufficient balance initialized
    `old_tron_power` before returning zero. Java completes validation before
    entering execute and therefore leaves the resource-model marker unchanged
    on that failure path.
24. A fully drained VM-native UNDELEGATERESOURCE deleted its unlocked resource
    capsule and retained directional indexes when a separate locked capsule
    existed. Java does the inverse: it writes the all-zero unlocked capsule and
    commits empty index capsules as deletes based only on the unlocked record.
25. FREEZEEXPIRETIME returned an expiry for a zero-balance legacy slot and used
    the maximum bandwidth expiry across malformed extra rows. Java returns an
    expiry only for a non-zero balance and reads exactly bandwidth `Frozen[0]`.
26. CANCELALLUNFREEZEV2 converted an unexpired unknown/future resource enum into
    a new FrozenV2 row. Java records the unknown name only in its temporary
    result map, takes the resource switch's default branch, and clears the
    unfreeze queue without refreezing that entry.
27. SELFDESTRUCT's vote/reward settlement added the contract allowance to its
    balance with wrapping arithmetic. Java performs checked addition and raises
    `Suicide: balance and allowance out of long range.` before transferring
    assets or deleting the contract. The local overflow now aborts the frame
    with the same message and full rollback; the legacy path fails before its
    nonce/record, while the restricted path retains its already-created rejected
    record as described by Java's different ordering.
28. SELFDESTRUCT filtered explicit zero balances out of the AssetV2 map before
    building its internal transaction. Java passes the complete
    `getAssetMapV2()` into the record, and therefore exposes even zero-valued
    token entries in TransactionInfo.
29. Restricted SELFDESTRUCT (`Program.suicide2`) settled vote reward before
    constructing its internal transaction. Java hashes the record using the
    pre-reward balance, then settles allowance and changes only the published
    value with `setValue`; the cached identity hash deliberately does not
    change. The legacy `Program.suicide` uses the opposite order. Both paths are
    now represented separately.
30. V1 FREEZE debited the owner before validating a delegated receiver and then
    refunded when the receiver was a contract. Canonical state ended unchanged,
    but the transient debit/refund leaked into BlockBalanceTrace. Java completes
    the non-mutating balance check before missing-receiver creation, completes
    receiver validation next, and only then executes the debit. The local path
    now preserves that full ordering, including no account creation for an
    underfunded owner.
31. WITHDRAWREWARD checked only positive balance-plus-allowance overflow. Java
    uses `Math.addExact` before its non-positive allowance early return, so a
    malformed negative allowance can also underflow an `int64` balance and
    must reject without settling reward bookkeeping or mutating the account.
    The local opcode now checks both signs and marks its already-created
    featured internal transaction rejected on either range failure.
32. Both UNFREEZEBALANCEV2 execution paths removed expired rows but credited
    their wrapping sum only when it was positive. Java's embedded
    `unfreezeExpire` helper always assigns `balance + sum` and does not reuse the
    standalone withdrawal validator, so malformed negative expired amounts
    decrease the balance while the main unfreeze still succeeds. The VM opcode
    and transaction actuator now apply every non-zero signed sum before
    continuing.
33. V1 self-BANDWIDTH UNFREEZE treated a non-positive expired-row sum as
    execution failure. Java validates only that at least one bandwidth row is
    expired, then succeeds even when the wrapping sum is zero or negative: it
    clears the rows, applies the signed balance and resource-weight changes,
    backfills the internal-transaction value and returns true. The local opcode
    now separates validation success from the returned amount and preserves
    that behavior.
34. V1 ENERGY and TRON_POWER freeze helpers accumulated the balance but kept
    the maximum of old and new expiry timestamps. Both Java's transaction
    actuator and VM processor call the corresponding `setFrozenFor*` method,
    which accumulates the amount and unconditionally replaces expiry with this
    operation's calculated timestamp. The split-state helpers now overwrite
    expiry too, covering shortened-duration and imported-later-expiry states.
35. V2 undelegation clamped the owner's outgoing delegated aggregate to zero.
    Both Java's VM processor and transaction actuator perform raw signed-long
    subtraction instead. This is reachable when a per-pair record survives an
    owner contract's suicide/re-creation while the recreated account aggregate
    starts at zero; Java explicitly handles the analogous receiver shape. The
    shared state helper now preserves Java's negative/wrapping result while the
    receiver-side acquired balance keeps its separate zero clamp.
36. V2 delegation usage transfer used raw Go `float64`-to-`int64` conversions
    and forced a zero maximum transfer whenever total weight was non-positive.
    Java narrowing maps NaN to zero and saturates infinities/out-of-range values;
    for a positive resource limit and zero weight, its maximum is
    `Long.MAX_VALUE`, so the proportional receiver usage is transferred. A
    shared Java conversion helper now makes the result platform-independent,
    and the transfer/usage-to-frozen formulas no longer insert non-Java clamps.
37. The ResourceUsage/CheckUnDelegateResource query path always used wrapping
    long recovery and lossy double balance conversion. Current java-tron
    `RepositoryImpl` switches both calculations to BigInteger with exact-long
    output under `allow_harden_resource_calculation`. The precompile helpers now
    read that rooted property and reproduce both arithmetic modes; golden
    fixtures cover an intermediate long overflow and a four-unit double-vs-
    exact result difference.
38. SELFDESTRUCT called the Solidity059 beneficiary-creation helper only for a
    positive TRX balance and skipped MUtil's validation entirely for a negative
    balance. Java invokes account creation before its zero-value transfer early
    return, while a negative value enters validation and fails. The same gap
    also let the pre-Solidity059, zero-TRX, TRC10-enabled path succeed even
    though Java's subsequent `transferAllToken` dereferences a missing target,
    becomes `Unknown Exception`, spends all energy and rolls the frame back.
    Both legacy/restricted execution now preserve those account, error-class,
    energy and rollback transitions; the same-beneficiary branch additionally
    uses RepositoryImpl-compatible checked signed balance updates.
39. SELFDESTRUCT's V1 freeze-inheritance helper credited only a positive frozen
    sum. Java calls `RepositoryImpl.addBalance` unconditionally: it materializes
    a missing inheritor even for a zero delta, applies a negative signed sum when
    the inheritor can fund it, and throws on an insufficient debit or overflow.
    The local helper now has the same signed/checked behavior and propagates the
    failure through the VM snapshot rather than continuing to clear or delete.
40. Consensus/resource code used Go's implementation-dependent out-of-range
    `float64`-to-`int64` conversion and Go `math.Round` directly. Java narrowing
    instead maps NaN to zero, saturates infinities/out-of-range values, and
    rounds negative half ties toward positive infinity. The shared Java numeric
    helpers now cover exchange math, resource limits/recovery, Stake-V2 query
    and vote recomputation, dynamic energy, witness/voter reward distribution,
    and rollback reward reconstruction, so all execution modes use the same
    cross-platform endpoints.
41. The Java-HashMap vote-order emulator panicked when a treeified bucket held
    two distinct witness `ByteString`s with the same 32-bit spread hash. That
    shape is grindable within VOTEWITNESS's 30-entry limit and could crash a Go
    full node during block import. Java's own tie-break is
    `System.identityHashCode`, which is process-specific and has no deterministic
    wire-compatible value. The emulator now reports the ambiguity without a
    panic, and the VM propagates a dedicated fail-closed error through nested
    CALL and CREATE frames so block import stops instead of committing a guessed
    vote order. A fixed 11-witness treeification/collision fixture proves the
    node remains alive and publishes no vote state; this upstream Java
    nondeterminism remains an explicit interoperability boundary rather than a
    hidden crash or silent fork.

Defects 12–41 were found by this follow-up adversarial audit. They are
production-safety coverage defects, but there is no evidence that they caused
the three historical mainnet incidents above; those incidents were already
localized to the earlier VM and Transfer publication paths.

The featured-native parity pass was rechecked against official java-tron
`develop` commit `57b7b04f385bc5da1a417b0af75aaca200f41336`. This source audit
does not replace the exact-height database comparison required below: deployed
mainnet behavior is additionally controlled by proposal state and local
`vm.save*` configuration.

## Enforced publication contract

Every plain Transfer publication now requires all of the following:

1. exact typed read-version validation at its canonical transaction boundary;
2. non-commutative writes without an explicit read are treated as implicit
   read dependencies, preventing a cached stale post-image from overwriting an
   earlier writer;
3. an independent constant-time balance oracle derives the sender and existing
   recipient post-images from canonical balances, transfer amount, and the
   candidate receipt fee; an authoritative zero-copy serial execution at the
   same boundary independently proves that receipt fee, the complete receipt,
   WriteSet, and optional balance trace;
4. the independently executed serial result is retained as the private
   publication seal after admission; its WriteSet, read set, TransactionInfo
   and balance trace are the sole canonical payload. The speculative source's
   consumed fields (WriteSet, TransactionInfo and balance trace) must still
   match immediately before and after apply; its scheduler read carrier is
   diagnostic only and is never consumed;
5. deterministic schema-validated WriteSet application with physical-alias
   rejection; protected state keys may never publish through RawKV, every key
   is a canonical tagged union, typed-owned AccountKV domains are forbidden,
   and presence/value post-images are fully validated before the first
   mutation;
6. complete canonical post-apply WriteSet capture and exact comparison against
   the immutable seal.

Fresh-recipient transfers take the serial path. Transfers whose owner or
recipient is the chain Blackhole account also take the serial path: before
blackhole optimization, bandwidth/memo/multisig settlement can credit the fee
to that same participant balance, so the final post-image is not the ordinary
two-account `owner - amount - fee` / `recipient + amount` pair. The scheduler's
current version validation already rejects this physical alias, while the
balance oracle now enforces the fallback independently as defense in depth.
Every publishable Transfer —
both block-start results and async sender retries — is fully re-executed at the
exact canonical boundary before mutation. The Transfer oracle executes under
nested StateDB and dynamic-property snapshots and rolls them back, avoiding a
full StateDB copy while remaining independent of the speculative block-start
base. If a direct replay/audit caller requests balance-trace comparison without
installing a canonical block recorder, the oracle installs and removes a
private recorder for that execution only.

Every canonical-state oracle is additionally sealed across its rollback
boundary. This includes both the VM isolated/full-copy oracle and the direct
Transfer/VM oracle. An independent outer StateDB/DynamicProperties snapshot
encloses each oracle's own nested transaction and is always reverted before the
oracle returns; the isolated guard begins before `StateDB.Copy`, so copy-time
materialization leaks are also in scope. The StateDB domain-journal mark must be
identical before and after execution, while `DynamicProperties.SnapshotChanged`
detects a leaked property write even when that property is outside the
candidate carrier. Because
commutative publication carriers contain deltas rather than absolute
post-images, the oracle also snapshots the absolute canonical value of every
authorized commutative balance/property before execution and requires the same
value afterward. If a caller violates account ownership and changes the cached
Blackhole balance without journaling, a narrowly scoped inverse repair restores
the pre-oracle absolute value before the error is returned. A restoration
mismatch is still a typed publication-safety failure, not a per-transaction
fallback; the durable serial circuit opens before any speculative payload can
commit. BlockChain additionally discards the whole failed range StateDB.

Every VM publication remains sparse and now requires two canonical-boundary
serial re-executions before mutation. One executes on a full isolated StateDB
`Copy`; the other executes directly on the original canonical StateDB under
nested StateDB/dynamic-property snapshots while raw writes remain in a private
overlay. This prevents a shared `Copy`/`copyStateObjectInto` cache omission
from making both the speculative block-start base and the only oracle agree on
the same incomplete state. `StateDB.Copy` now also deep-retains every
materialized split-account cache/flag, permission point, witness signer,
TRC10/FrozenV2 point cache, contract runtime, storage-key layout, pending
transaction-finalization set, transient state and transaction-versioned view.
The account clone deliberately resets the canonical-pooled backing flag because
it owns independent protobuf backing. Reflective policy tests require every
future `StateDB` and `stateObject` field to be explicitly classified. TransactionInfo,
WriteSet and balance trace must match across both executions. Read-set
differences are diagnostic because the direct execution's complete read carrier
becomes the publication seal. Only that direct result may be published, and it
is followed by the same exact post-apply audit.

The independently executed serial result is retained as the sole publication
payload: its complete read set, WriteSet, receipt and balance trace are what
canonical state consumes. The speculative candidate is never cloned into or
consumed by canonical state. Candidate/serial read-set differences remain
observable through `serial_verify/read_set_differences`, but are diagnostic:
sender-chain forwarding and full-copy serial execution legitimately materialize
different cache/system-account reads. They cannot change publication because
the canonical serial carrier replaces the worker carrier in full. WriteSet,
receipt, and balance-trace differences still reject the candidate.
They are release-disqualifying safety incidents rather than ordinary
per-transaction fallbacks: the complete block attempt is rolled back and the
durable serial circuit is opened.

Two regression guards freeze this trust boundary. The live oracle ownership
test mutates every worker carrier after verification and proves that the
retained serial WriteSet, read set, receipt, and balance trace do not change.
AST architecture tests scope `processBlockWithOptions` and require all four
canonical Transfer/VM direct/retry apply sites to consume
`writeSeal.writes`; worker-owned WriteSets may only be applied inside discarded
verification state. A second guard requires the VM admission function to call
exactly one full-copy oracle, one direct canonical-state oracle and one
cross-comparison, in that order, and requires its final publication carrier to
be the direct result.

Transaction-access publication policy is likewise exhaustive. Count sentinels
force every future access kind and account field through an explicit policy;
unknown kinds fail key-shape validation and retain `Other` unsupported evidence
even when mixed with a known unsupported family. The policy test cross-checks
every publishable kind against recorder coverage, canonical key shape, schema
validation and its applier, while read-only/forced-serial fields remain explicit.

The safety-circuit test also stages a raw KV write in the failed block-buffer
layer. Both synchronous and asynchronous commit variants prove that
`DiscardActive` removes it before the serial retry and that it never reaches
the durable database. StateDB/DynamicProperties rollback is therefore not the
only rollback evidence; the non-rooted actuator/VM write surface is covered as
well.

Any balance/serial-oracle mismatch or error, publication-seal failure, or
post-publication invariant failure returns the typed speculative-safety error.
`processBlock` reverts the complete block attempt. `BlockChain` durably records
the incident, opens a sticky datadir-lifetime safety circuit, disables both
speculative publishers, and retries that block serially. Restarting cannot
re-enable publication; the canary must be rebuilt from a trusted
pre-divergence baseline.

## Operational metrics

All mismatch/error counters must remain zero:

- `core/mainnet_state_repair/create_transfer_failure`
- `core/mainnet_state_repair/parallel_vm_missed_payment`
- `core/mainnet_state_repair/cost_missed_reward`
- `core/mainnet_state_repair/wink_missing_runtime`
- `core/parallel_transfer/balance_oracle/*`
- `core/parallel_transfer/serial_verify/*`
- `core/parallel_transfer/serial_verify/restore_mismatches`
- `core/parallel_transfer/write_seal/*`
- `core/parallel_transfer/publish_audit/*`
- `core/parallel_vm/serial_verify/*`
- `core/parallel_vm/serial_verify/restore_mismatches`
- `core/parallel_vm/dual_oracle/*`
- `core/parallel_vm/write_seal/*`
- `core/parallel_vm/publish_audit/*`
- `core/speculative_execution/safety_fallbacks`
- `core/speculative_execution/safety_persisted`
- `core/speculative_execution/safety_qualified`
- `core/speculative_execution/safety_persist_errors`
- `core/parallel_transfer/errors`
- `core/parallel_transfer/sender_retry/errors`
- `core/parallel_vm/errors`
- `core/parallel_vm/retry/async_publish/errors`

`core/parallel_{transfer,vm}/serial_verify/read_set_differences` and
`core/parallel_vm/dual_oracle/read_set_differences` are not failure counters.
They measure differences in non-consumed read carriers; the release gate
requires the metrics to exist but does not require zero.

`core/speculative_execution/safety_disabled` must remain `0`. A value of `1`
means the process already rolled back a speculative attempt and is running the
serial fallback circuit.

`core/speculative_execution/safety_qualified` must be `1`. A mainnet datadir
opened by this code before the first known historical repair height receives a
forced-WAL local credential (`execution-safety-qualified-v1`). A datadir first
seen after that height without this credential cannot prove that it crossed the
legacy repair range cleanly: parallel enablement is refused and a
`historical-repair-status-unknown` incident is persisted. Serial sync remains
available; speculative testing requires a rebuild from genesis with a
safety-aware binary.

`core/speculative_execution/safety_persisted` must also remain `0`. Every
publication-safety failure and every legacy mainnet repair writes a forced-WAL
local marker (`execution-safety-incident-v1`) outside consensus state before
recovery continues. Startup restores the disabled circuit from this marker;
malformed markers abort startup, and marker write/sync failure aborts the block
instead of retrying without durable evidence. A marker persistence failure also
sets a sticky in-process insertion latch: every later block insertion returns
the original storage error without entering execution. The marker intentionally
survives mutable-state reset, so only a fresh immutable pre-divergence datadir
is canary eligible. `core/speculative_execution/safety_persist_errors` must
remain `0`; a non-zero value requires storage repair and a rebuild from the
trusted baseline.

Any non-zero `core/mainnet_state_repair/*` counter means the process activated
an exact legacy-state recovery patch. That database may be useful for forensic
recovery, but it is not a clean replay and is ineligible for a canary or
production promotion. Rebuild it from the immutable pre-divergence baseline.

Run the fail-closed metrics gate on the node after the replay crosses both
incident heights:

```bash
scripts/dev/parallel_execution_release_gate.py \
  --min-transfer-publications 10000
```

For the later VM canary, add `--min-vm-publications 100`. Missing metrics, a
disabled requested publisher, insufficient exposure, unclosed
candidate/outcome accounting, any Transfer or VM publication without exactly
one serial-oracle match, one immutable publication-seal match and one post-apply
audit, any Transfer publication without one balance-oracle match, any VM
publication without one isolated/direct dual-oracle match and one block-energy
settlement, a non-zero mismatch/error, or an open safety circuit all fail the
command.

## Verification evidence

- Exact 22,097,772 topology: all 148 transaction positions are retained with
  independent filler work; the six critical positions, addresses and amounts
  match mainnet, including transaction 58's exact 4,455 SUN credit. The
  serial/parallel receipts, balance traces, final balances, and state roots
  match for 32 iterations per test run.
- A protocol-special transfer to the Blackhole account is tested with its
  amount and legacy fee settlement merged into one recipient post-image. The
  balance oracle returns a counted serial fallback rather than misclassifying
  the legal alias as a persistent publication incident; the real parallel
  block path remains byte/root-equivalent to serial execution and publishes no
  speculative result.
- Existing-recipient Transfers with forced paid bandwidth, memo and multisig
  fees are exercised under all four settlement combinations: legacy
  Blackhole, burn, bandwidth fee pool plus Blackhole, and bandwidth fee pool
  plus burn. Each case actually publishes and verifies sender/recipient and
  Blackhole balances, `transaction_fee_pool`, `burn_trx_amount`, receipt,
  balance trace and state root against serial execution. Twenty repeated
  normal runs and five repeated race-detector runs pass.
- A deterministic 16-seed/1,536-transaction differential matrix combines hot
  shared recipients, sender suffixes that are invalid on the block-start view
  and become executable only after an earlier funding transaction, and global
  public-bandwidth exhaustion. It compares every receipt, the complete block
  balance trace, final balances, and the committed state root against serial
  execution. Coverage counters require real async sender retries, retained
  publications, public-net rebasing and limit fallbacks; 20 repeated normal
  runs and five repeated race-detector runs pass with one balance-oracle and
  canonical-serial-oracle match per publication and no mismatch/error.
- A forged worker result with both a wrong receipt fee and a matching wrong
  sender balance passes the deliberately narrow balance-post-image oracle, but
  is rejected by the independent canonical-boundary execution on both receipt
  and WriteSet. The same test proves, for both success and execution-error
  exits, identical commitment root, integer/string dynamic properties and
  domain-journal mark; restoration of the outer StateDB/dynamic access
  recorder; temporary balance-trace isolation; and preservation of an existing
  canonical trace recorder under 20 race-detector runs.
- A real paid-bandwidth Transfer result is fault-injected after the direct
  oracle returns: the cached Blackhole balance is mutated through the account
  object without adding a StateDB journal entry. The journal mark therefore
  remains unchanged, but the absolute commutative-value seal detects the leaked
  increment and the narrow inverse repair restores the original balance before
  returning the typed publication-safety/restoration error. A real paid-energy
  VM result receives a journaled `total_net_weight` mutation outside its
  carrier; `SnapshotChanged` detects it and the independent outer DP snapshot
  removes it. Both paths increment their dedicated `restore_mismatches`
  counter, and 20 normal/five race repetitions prove that the caller-visible
  StateDB/DP values and journal boundary remain unchanged. The release gate
  requires both counters to stay zero.
- Historical topology plus both async publisher cohorts: 100 repeated runs.
- The same adversarial cases under the Go race detector: 10 repeated runs.
- The sticky serial circuit was injected into block 2 of an `InsertBlocks`
  range while block 1's async commitment fold was deliberately held in flight;
  both sync/async paths and the in-flight-prefix race case pass 20 repeated
  race-detector runs.
- The same safety failure is injected while a longer competing branch is being
  replayed after a fork rewind. Both sync and async commit settle the successful
  fork prefix, open the sticky circuit, retry the failed fork block serially
  and finish the remaining branch serially instead of leaving the head at the
  LCA; 20 repeated runs pass.
- A real Transfer WriteSet is corrupted only after the generic applier has
  consumed it. The immediate canonical post-image audit detects the mismatch,
  reverts the whole block, opens the sticky circuit, and the serial retry lands
  the exact recipient balance under both sync and async commit; 20 repeated
  race-detector runs pass.
- A validly encoded recipient post-image is corrupted before the balance and
  serial oracles. The balance oracle now returns the typed safety error instead
  of a forgettable per-transaction fallback; synchronous and asynchronous
  commit both roll back the block, persist the incident and retry to the exact
  serial balance. Transfer/VM serial-oracle mismatches follow the same path.
- A second TOCTOU injection corrupts the Transfer WriteSet after preflight and
  serial-oracle admission but before apply. The applier's repeated schema check
  rejects it, the error is classified as speculative safety failure, and both
  sync/async commit paths roll back, open the circuit, and land the exact serial
  balance over 20 repeated race-detector runs.
- A well-formed TOCTOU injection changes an 8-byte Transfer balance post-image
  after serial-oracle admission. A matching VM injection changes a valid
  32-byte storage post-image. Schema checks alone accept both shapes; the
  immutable publication seal rejects both before mutation, and repeated race runs
  prove complete rollback. The seal is also rechecked after apply before the
  canonical post-image audit.
- Oracle-admitted TransactionInfo and balance-trace carriers are independently
  mutated before apply. The publication seal rejects each consumed-field
  mutation and rolls the VM block attempt back before receipt or trace
  publication. A separate ownership test mutates the discarded worker read
  carrier and proves that the serial oracle's canonical read carrier remains
  unchanged and is the one restored for publication.
- A retained public-net candidate whose usage/time post-image is removed,
  shortened, or changed to an invalid commutative shape is rejected at the
  publication boundary without mutation or panic; canonical execution falls
  back to the serial path.
- The equivalent real VM storage WriteSet corruption is detected on the
  canonical publication path; the block snapshot removes its storage, balance,
  and block-energy effects. Ten repeated race-detector runs pass before the
  unmodified VM cohort is allowed to publish.
- VM canonical admission is exercised through both full-copy and in-place
  serial paths. The tests require exact TransactionInfo, WriteSet and balance-
  trace agreement; prove read differences are diagnostic only; reject a
  mismatch in every consumed output; and prove the direct execution restores
  the outer access recorder, commitment root, domain-journal mark, all integer
  and string dynamic properties, storage, and balance-trace state. The
  architecture guard and real parallel VM publication cohorts pass 20 normal
  repetitions and five race-detector repetitions.
- Both the retained VM sender-chain cohort and the boundary-ready asynchronous
  retry cohort require exactly one isolated/direct dual-oracle candidate and
  match for every real publication, with zero receipt, WriteSet, balance-trace
  mismatch or oracle error. A real internal CREATE fixture also publishes
  through this path and proves exact child account, runtime code, contract
  metadata, LOG payload, TransactionInfo and committed-root parity with serial
  execution over 20 normal and five race-detector repetitions.
- Two additional real VM publications exercise split account state instead of
  only storage and scalar balances. CALLTOKEN moves an existing TRC10 asset
  between two AccountAssetV2 AccountKV rows; V1 FREEZE moves contract balance
  into an AccountResource row and increments total-energy-weight. Each requires
  exactly one dual-oracle match, immutable-seal match and post-apply-audit match,
  zero fallback/mismatch/error, exact receipt and affected-state equality, and
  an identical committed root over 20 normal and five race-detector repetitions.
  A two-contract FREEZE topology then makes both otherwise-independent calls
  update `total_energy_weight`: only the first result may publish, the second
  must take a version-conflict serial fallback before either oracle, and the
  final weight must be 2 rather than the stale post-image value 1. Receipts,
  both resource rows, balances and the committed root remain serial-equivalent
  over the same 20/5 repetitions.
- VM effects outside the deliberately narrow publication schema stay fail-safe.
  A WITHDRAWREWARD fixture writes reward-cycle/account-vote AccountKV rows and
  therefore takes the version-conflict serial fallback before either oracle.
  A distinct-beneficiary SELFDESTRUCT fixture deletes the contract and moves
  its balance but takes the unsupported-access serial fallback before either
  oracle. Both require zero publications and zero dual-oracle activity while
  proving exact receipt, affected state and committed-root parity with serial
  execution over the same 20/5 repetitions. The SELFDESTRUCT fixture also
  locks java-tron's legacy 20-of-21-byte beneficiary comparison so a last-byte-
  only address difference cannot accidentally exercise the blackhole path.
- Every exact legacy mainnet state-repair hook now emits an error log and
  increments its own `core/mainnet_state_repair/*` counter, persists the local
  safety marker and disables both publishers. Positive-path tests prove
  activation is visible and a second call on the repaired pre-image is inert.
- Restart adversarial tests prove the durable marker restores the disabled
  circuit; corrupt marker bytes fail startup, a failed marker write prevents
  serial retry and permanently rejects later insertions in that process,
  Pebble-capable stores receive a forced durability sync, a real Pebble
  close/reopen retains the exact incident, and mutable-state reset cannot erase
  it. A range-abort failure cannot hide the marker because the incident is
  persisted before abort is attempted. The 15-case fail-closed release-gate
  suite rejects either a live or persisted safety incident, a VM publication
  without a one-for-one dual-oracle proof, any dual-oracle mismatch/error, or a
  Transfer/VM canonical-oracle restoration mismatch.
- WriteSet schema adversarial tests reject, before a balance sentinel can be
  mutated, typed-cache domains routed through generic AccountKV, every ignored-
  field tagged-union alias, cross-kind dynamic-property aliases, non-canonical
  TRON address prefixes, rawdb infrastructure keys, and contradictory
  presence/value shapes for persistent storage, transient storage, code,
  contract metadata, AccountKV and RawKV. Every supported exact scalar field is
  also checked against a co-published fresh-account envelope, including the net
  window mode bit, and a commutative field cannot coexist with that envelope.
  Commutative dynamic writes are restricted to the five protocol accumulator
  properties that use `addCommutativeInt`; commutative balances are restricted
  to positive deltas targeting the chain-specific Blackhole address resolved
  from rooted state. That authorization uses a strict index read: missing rows
  retain the legacy-mainnet fallback, while read errors, malformed lengths and
  invalid address prefixes fail before mutation.
  Stable sorting has an explicit kind tie-breaker, and the valid absent-zero
  transient boundary post-image still round-trips through recorded publication.
  The focused suite passes 20 normal repetitions and five race-detector
  repetitions; the parallel differential cohorts pass the same 20/5 repetition
  counts afterward.
- Copy-boundary reproduction tests first failed on the old implementation: a
  copied materialized resource cache returned zero instead of 77 when the
  shared latest reader was intentionally empty, and the copy retained zero of
  two pending transaction-finalization addresses. The fixed full and
  block-execution copies retain all loaded split-account values without
  rehydrating shared older rows; deep-clone permission points; preserve
  zero-storage and SELFDESTRUCT finalization; and preserve independent
  transient and transaction-versioned reader/transaction/view state. Reflective
  field-policy tests fail whenever a new `StateDB` or `stateObject` field lacks
  an explicit copy decision. The focused copy/version suite passes 10 repeated
  race-detector runs.
- The VM oracle's second copy boundary, `DynamicProperties.Copy`, now has the
  same reflective field-policy guard. Its adversarial test starts from a live
  nested snapshot, copies integer, string, hash and dirty state, proves all
  maps are independently owned, and proves recorder ownership plus rollback
  history are deliberately reset. Source rollback and copy mutation remain
  isolated over 20 normal and 10 race-detector repetitions.
- Live TransactionInfo comparison covered all 349 transactions in the six
  historical incident blocks. Exactly 192 responses were already byte-for-byte
  JSON-equivalent to the reference. Each of the remaining 157 became exactly
  equal after removing only `internal_transactions`; there were zero other
  field differences. The per-height exact/missing-only counts were 16/12,
  10/20, 34/9, 34/31, 81/67 and 17/18 in manifest order. TransactionInfo
  construction now retains the arena-owned internal transaction slice with
  capacity sealed to its length; the block batch keeps that arena alive through
  asynchronous metadata serialization, while boundary-oracle retained results
  are deep cloned. Unit tests verify exact result inclusion, slot isolation,
  reuse clearing, dual-oracle mismatch rejection and compact database
  round-trip over 20 normal and 10 race-detector repetitions. Existing
  databases require replay/backfill to gain the omitted historical records.
- A second exhaustive receipt audit found that dynamic-energy penalty was not
  represented in `actuator.Result` at all. The implementation now adds each
  penalty only after `Contract.UseEnergy` accepts the complete charge, excludes
  the failing out-of-energy opcode just like java-tron, and accumulates the
  value on the transaction-wide TVM across nested frames. Create/trigger
  execution carries it into `ResourceReceipt.energy_penalty_total`; constant
  call HTTP and gRPC responses expose `energy_penalty`. Exact unit tests cover
  per-op rounding across split charges, rejected charges, pooled-TVM reset,
  VMActuator propagation, persisted receipt construction and a real hardened
  parallel VM publication with a non-zero penalty. Reflective Go-result and
  protobuf-descriptor policy tests now fail on any unclassified future field.
  Existing databases require replay/backfill to gain historical penalty
  values.
- Java-source differential fixtures now cover all featured VM internal-
  transaction families: V1 freeze/unfreeze, Stake-V2 freeze/unfreeze/withdraw/
  cancel/delegate/undelegate, VOTEWITNESS and WITHDRAWREWARD. They lock nonce
  advancement on accepted and rejected paths, exact identity hashes (including
  the empty VOTE receiver), note/value/rejected fields, exact VOTE JSON, nested
  expired-withdrawal records, and fixed-order CANCELALL detail JSON. Receipt
  filtering is independently tested for all four meaningful combinations of
  `saveInternalTx`, `saveFeaturedInternalTx` and
  `saveCancelAllUnfreezeV2Details`; execution and serial/speculative oracle
  comparisons always retain the complete record set before filtering. A
  SELFDESTRUCT receipt fixture additionally retains explicit zero-valued
  AssetV2 entries, matching java's complete token map, and distinguishes the
  legacy post-reward identity from restricted SELFDESTRUCT's pre-reward
  identity plus post-construction `setValue`.
- Focused Java-native-processor parity tests lock the newly found state edges:
  WITHDRAWEXPIREUNFREEZE wrapping-sum/checked-overflow rejection and zero-total
  row retention; the independent delegation fork gate; V1 FREEZE's zero-or-one
  frozen-list rule; persistence of a zeroed V1 delegated capsule; no
  `old_tron_power` initialization after a failed V2 balance check; VM-native V2
  undelegation's zero-capsule and index-delete behavior; non-positive reward
  return normalization; and FREEZEEXPIRETIME's non-zero/first-bandwidth-row
  semantics. CANCELALL also proves an unknown/future resource entry is cleared
  without manufacturing a FrozenV2 row. A SELFDESTRUCT fixture proves checked
  balance-plus-allowance overflow consumes the remaining energy while
  preserving nonce, account, allowance, beneficiary and deletion state. Each
  failure-path fixture asserts the relevant account, delegation row, index or
  resource-model marker remains unchanged. Contract-receiver FREEZE rejection
  additionally proves that no debit/refund operation leaks into the balance
  trace, while an underfunded delegated FREEZE cannot create its receiver.
  WITHDRAWREWARD additionally proves that checked negative
  balance-plus-allowance underflow rejects before reward settlement and leaves
  both scalar fields intact. VM and actuator UNFREEZEBALANCEV2 fixtures lock
  Java's distinct behavior for malformed negative expired amounts: the rows
  are cleared, the signed sum changes balance, and the requested unfreeze still
  completes. V1 self-
  BANDWIDTH fixtures independently prove that zero and negative expired sums
  still succeed, clear the rows, publish the non-rejected receipt, and apply
  the signed balance/weight result. V1 ENERGY and TRON_POWER fixtures prove a
  later stored expiry is replaced, rather than max-merged, while the balance
  still accumulates. A VM-native undelegation fixture builds the
  suicide/re-created-owner shape and proves the negative owner aggregate,
  returned frozen amount and per-pair record all match Java.
  Zero-total-weight usage transfer locks Java's positive-infinity saturation,
  while common numeric fixtures cover NaN, infinities and both signed endpoints.
  Hardened staking-query tests prove the rooted proposal flag selects exact
  BigInteger recovery and usage-balance conversion rather than the legacy
  wrapping/double branch. SELFDESTRUCT fork-matrix fixtures additionally pin
  Solidity059 zero-balance account creation, pre-Solidity059 zero-balance TRC10
  `Unknown Exception`, negative-balance transfer validation and rollback,
  unconditional zero-sum V1 inheritor creation, and signed negative frozen-sum
  migration. Shared numeric fixtures pin Java NaN/infinity saturation and the
  `Math.round(-1.5) == -1` tie rule; the exchange processor also locks both
  narrowing endpoints at its real consensus callsite.
- Transaction-access policy tests enumerate all 14 real access kinds and all
  12 real account fields. Every publishable kind must have recorder support, a
  canonical key shape, schema validation and an applier; forced-serial and
  read-only fields are explicit. Unknown/count/future kinds are rejected, and
  mixed known-unsupported plus unknown inputs preserve the unknown `Other`
  classification instead of being masked.
- Full `go test ./... -count=1 -timeout 300s`, `go test -race ./... -count=1
  -timeout 600s`, `go vet ./...`, and incremental golangci-lint pass after the
  final WriteSet schema, commutative-authorization, copy-policy and canonical-
  oracle restoration-seal changes. The restoration seal also has an AST
  architecture guard requiring the isolated guard before `StateDB.Copy`, plus
  two absolute-value captures, three journal marks, independent StateDB/DP
  snapshots and both normal/deferred reverts around the direct path. A
  repository scanner additionally rejects actuator/core/VM code that calls
  mutating Account methods or assigns through `Proto()` on a live
  `GetAccount`/reference result; the narrow inverse repair is the sole explicit
  exception.
- A repository-wide race run initially exposed two additional shared-state
  violations outside the speculative publisher itself. The mainnet
  `db-compare` delegation workers shared one goroutine-confined lazy `StateDB`;
  they now open an independent read view per worker. Sync import also read
  `targetHeadNum` off-lock while staged-body lookahead could update it under
  `ss.mu`; all target-height access now uses one atomic field. Both fixes pass
  20 repeated race runs, and `go test -race ./... -count=1` passes in full
  after the final production-path change.
- The two-node system suite passes 79/79 in its default serial mode. With
  `GTRON_SYSTEM_TEST_PARALLEL_EXECUTION=1`, it creates a recipient serially,
  publishes a second existing-recipient Transfer through the hardened path,
  runs the live metrics gate, and verifies P2P/account/receipt/contract parity
  across both nodes: 82/82 pass. The harness now always rebuilds `gtron` and
  `txsign`; it can no longer report a false green result from stale binaries.
- Production-shaped 150-block/12,000-transfer benchmark on Apple M1 Max:
  serial about 0.143 seconds; parallel with an authoritative zero-copy serial
  oracle and its independently owned result as the complete publication
  payload on every publication plus the restoration seal about 0.796 seconds
  and 1.157 GB allocated. The
  rejected per-publication full-copy design took about 4.54 seconds and
  allocated about 11.2 GB. The feature remains off by default; rollout requires
  the exposure and one-for-one verification gates above rather than assuming a
  speedup before production replay validates both safety and workload benefit.

The repository-wide golangci-lint v2.13.1 run still reports 158 pre-existing
findings; `golangci-lint run --new-from-rev=HEAD ./...` reports zero. The
checked-in conformance smoke corpus is stale at block 1,000,000 on unmodified
HEAD and must not be presented as a passing release signal until its
dynamic-property oracle is regenerated.

## Mainnet recovery and rollout gate

The database that crossed 22,097,772 is untrusted and must not be resumed.

The separate clean-resync process observed through the gateway at 2026-09-04
08:28 CST is healthy at `currentBlock=28,162,970`, `appliedTip=28,162,994`,
target 85,936,657, 29 peers / 8 sync peers, `active=true` and `paused=false`.
It has crossed every historical incident height, and its stored block IDs at
18,402,304, 18,414,381, 20,616,256, 20,674,403, 22,097,772 and 22,123,859
exactly match the audited mainnet manifest above.

This is serial-baseline evidence, not a parallel-execution canary. The live
process command line has neither `--exec.parallel-transfers` nor
`--exec.parallel-vm`; metrics report both publishers disabled with zero
publications. The fail-closed release gate rejects this binary with 52 missing
safety/oracle/seal/audit metrics, proving that the current hardened workspace
has not been deployed there. Block-ID parity proves canonical chain selection
only and cannot substitute for exact account, storage and dynamic-property
comparison.

After rebasing the audit on repository HEAD `501a7728`, full normal and race
tests pass again, as do `go vet`, incremental golangci-lint v2.13.1 and builds
of `gtron`, `txsign` and `db-compare`. The intervening committed changes are in
sync, snapshot and freezer performance paths; this rerun is required evidence
that they did not invalidate the uncommitted publication hardening.

1. Build the hardened binary and restore a signed/trusted snapshot whose
   boundary is at or before 18,402,303, or clean-resync from genesis. Preserve
   the snapshot read-only and create a separate copy for each canary; never
   reuse a datadir from a failed attempt.
2. Run a VM-only canary first (`--exec.parallel-transfers=false
   --exec.parallel-vm`) with `--sync.stop-at` checkpoints at 18,402,303,
   18,402,304, 18,414,381, 20,616,255, 20,616,256 and 20,674,403. Require at
   least 100 VM publications and a passing release gate at each post-incident
   boundary.
3. From the same immutable baseline, run a Transfer-only canary
   (`--exec.parallel-transfers --exec.parallel-vm=false`) through 22,097,771,
   22,097,772 and 22,123,859. Require at least 10,000 Transfer publications and
   a passing release gate.
4. Stop both clients cleanly at every comparison height and run the read-only
   full-state comparer against an exact-height Java LevelDB database:

   ```bash
   make db-compare
   build/bin/db-compare \
     --height <height> \
     --gtron <gtron-datadir> \
     --java <java-output-directory> \
     --json > db-compare-<height>.json
   ```

   Exit status must be 0, `state_coverage_complete` must be true, and every
   store mismatch total must be zero. Also compare affected balances,
   transaction receipts/balance traces, and block identity; a state-root-only
   comparison is insufficient for stores outside that commitment.
5. Only after both isolated canaries pass, replay a third copy with both
   publishers enabled through every listed historical boundary and then the
   live head. Any
   safety-circuit transition, mismatch/error, missing metric, DB difference or
   restart invalidates that canary; return to the immutable baseline.

The code is eligible for a controlled replay canary after these checks. A live
production promotion is not complete until the trusted-snapshot replay and
Java checkpoint comparison succeed.
