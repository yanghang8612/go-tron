# 9M historical call: bounded cold-read work

Production remains on d11f4a57. Do not publish, restart, pause, reset or modify
the production databases. A frozen immutable cold catalog is a valid input for
cold-reader tests; it is not a chaindata/history consistency snapshot and cannot
substitute for a complete HTTP performance comparison or latest-state MuHash
recomputation. The previous 11.8M state copy has been deleted, and the user has
confirmed that no other state copy exists.

Retain all file encodings, canonical history rows, transaction ranges, MuHash
elements and restoration semantics. Add no new auxiliary format in this change.

Reuse a fully validated framed accessor header only for its current immutable
open descriptor. Honor both descriptor and outer context cancellation, reject
other sizes, discard the header at Close, and authenticate physical data again
on a later open. Transaction/owner tables must retain their existing checks.

For broad routing queries, authenticate the complete current physical file and
validate every encoded key, membership and source identity. Retain only requested
memberships instead of allocating the full large map. A selected view must never
enter the shared full-object cache. Unselected keys remain unknown and cannot
produce absence proofs. Keep full decoding for maintenance and strict import.

For small-system routing construction, hash the complete exact accessor against
its manifest checksum, retain the canonical history/dictionary binding, then
seek the small account and system-owner namespaces. Discover arbitrary keys and
generations, including maximum uint64 generation; apply existing bounded budgets.
Keep the independent full dictionary walk for strict import and exact membership
verification. Unsupported/uncovered sources retain authoritative fallbacks.

Compare production code, header-only code, the candidate before selected broad
decoding, and the final candidate on identical pinned canonical inputs. Verify
all returned rows, before-images and transaction ranges. Report process-first,
warm and verified private-file OS-cold measurements separately. Do not evict
production or hardlinked input pages; do not use global drop_caches. Record CPU,
allocation and logical/physical I/O separately. Attribute profiles only to their
observed sampling window, and exclude syscall tracing from performance samples.

Run local/native safety checks, historical full-row restoration, and relevant
MuHash/strict/signed snapshot tests. Report the missing full-state HTTP acceptance
condition explicitly rather than claiming an unmeasured production speedup.
