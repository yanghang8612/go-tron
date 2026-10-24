# Pending latest-KV prefix enumeration

Reward account hydration can enumerate multiple auxiliary domains through a
long-lived latest writer. Every request currently scans all pending KV rows.
The value overlay, durable iteration, lexical order and copying contracts remain
unchanged. No persisted key or local/TRON commitment format changes.

An execution-confined writer may lazily index immutable logical keys by account
ID. Members retain (generation, domain, logical key) and queries filter all
three before reading authoritative pending. The overlay remains the sole authority for value,
tombstone and commit number. Inserts maintain an already active index only for
new identities; overwrites do not duplicate members. Actual KV pruning invalidates
the complete index. Reset/full flush release it. Deep-async durability failures
retain both pending entries and a valid existing index.

Small overlays (at most 256 rows) use the existing scan. The first four prefix
requests after a prune also use the scan; only a fifth request builds the index.
This avoids allocating/maintaining it for write-only and sparse-query lifetimes.
An owner bucket containing more than half the pending set also retains the original
map scan, avoiding an extra point lookup per row for dense owners. These
admissions are performance choices, not correctness bounds.

Stable StateDB copies can share a writer while reading. An independent mutex
protects scan admission and lazy index construction/publication for these readers.
Returned bucket slices are immutable while the parent pending view is stable.
This does not allow concurrent pending writes/prunes: that remains outside the
existing parent-view contract. Point reads acquire no new lock.

Durable iteration must succeed before any prefix-index side effects. Callback
rows retain ownership, stop/error ordering and pre-callback materialization.
Tests compare the old scan directly, including aliases, generations, empty rows,
tombstones, overlapping prefixes, live inserts/deletes and reset/prune. A StateDB
commit/revert/publish differential checks roots and visible rows. Concurrent
stable-reader tests cover lazy and warmed indexes under race detection.

The same deterministic trace and benchmark source must be compiled against both
revisions. Time construction, all writes and ownership, first build, incremental
maintenance, partial publication/prune/rebuild, final publication and reset.
Retain no-prefix, single/two/five-query and small-overlay counterexamples. Include
whole-process wall/CPU and closure costs. Fixed synthetic data is not a mainnet
replay. Only a passing predeclared Linux comparison and independent audit permit
ordinary master deployment. VM/transfer execution and resource budgets stay
serial/unchanged. Never delete or migrate the database for this change.

The separate first prototype used (owner, generation, domain) buckets and failed
its frozen native lifecycle gates. Its evidence is retained independently. This
candidate uses fewer owner map entries and larger compact arena members instead;
that tradeoff must pass the same seven-shape, eight-arm and cost bounds in a new
fixed experiment. Moving metadata after the old writer fields preserves their
offsets, but does not establish the cause of the rejected write-only result.
