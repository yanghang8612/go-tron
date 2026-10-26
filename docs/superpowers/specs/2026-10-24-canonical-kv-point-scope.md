# Canonical account-KV point reads

Parent: 90bf5a99feef71f3a52494334a0a4a6fc7a22f6c. Keep VM and transfers
serial. No database format, cache size, compression or consensus changes.

The canonical processor already opens one StorageReadScope per nonempty
serial block and closes it before commit/publication. Its physical-key cursor
and captured layer topology support every account-KV domain. Currently only
ContractStorage decoding uses this scope; permission, reward and other KV
point reads rebuild ordinary DB.Get lookup state. Extend the existing scope
only to immediate-decoding point reads. Owning reads, preimages and journals
retain the original live route. Account envelopes, prefix iteration and
historical/worker views are not extended.

Preserve the versioned reader first. Higher-level dirty/fresh resolution is
unchanged; low-level preimage reads must not gain dirty/fresh shortcuts. Under
the canonical scope validate the bound reader/state/writer/index, consult the
writer pending overlay, then query the captured scope. The unchanged owning
route retains its ownership and empty-value shape. Only decoding
of ContractStorage counts pending SLOAD resolutions. Nil-scope paths stay
unchanged. No new value or missing-account cache is introduced.

AccountPermissionByID already resolves the owner and records exact existence
and row dependencies. Pass that object to the existing decoding helper rather
than resolving it a second time; retain the dependency set and cache fast path.

Validate differential traces against the unchanged live route: multiple domains,
generations, missing/empty/long keys, pending puts/deletes, flushing and pruning
older pending layers, mutation/revert, generation reset, recreation, owned
returns, sticky corruption/storage errors, reader replacement and scope close.
Compare roots, staged preimages and rows through a new StateDB over the same
open database (not a physical DB reopen). Existing
scope failure/interleaving and versioned-reader tests remain required.

Before timing, freeze a fixed fixture and command plan. Measure a multi-domain
point workload with present and missing rows and both resident-cache and durable
reads; separately verify object/permission hits and complete block lifecycle.
The selected path must improve the durable workload median by at least 5%,
with no more than 5% median regression in resident-cache/control and complete
lifecycle cost. Use four samples per arm in ABBA/BAAB order, fixed iterations,
no selective reruns. Count cursor construction and accesses. The wrapper View counter does not
cover GetWithPresence and is not a count of all ordinary base reads. This changes lookup construction, not the number of logical
keys or guaranteed physical disk reads. Do not claim a node-wide gain from it.

After full native Sapling tests and Astra's exact-candidate release audit,
perform an ordinary locked deployment preserving all flags and budgets. Use
one fixed five-minute post window for throughput and both cold backlogs, with
workload and background phase context. Existing dirty work remains untouched.

## Borrowed-only revision after rejected v2 verification

The broad candidate da939290 failed the frozen native lifecycle/control gates and remains NO-GO. It is not a release baseline. The revised candidate leaves the entire owning latest/preimage/journal route byte-identical to production. Only immediate decoding extends the existing ContractStorage scope to other KV domains; permission lookup still reuses the resolved object. This bounds exposure of extra scope validation to borrowed reads and preserves the existing warm generic GetAccountKV path. It does not establish that owning scope routing caused the rejected result; unchanged-path controls were also variable.

Existing synthetic traces are retained. Counter assertions now expect 24 borrowed cursor accesses; owning point reads stay on the production route and are separately checked for copy isolation. A new frozen validation plan must be reviewed before any revised-candidate timing. The previous results must not be pooled with new timing or relabelled as passing.
