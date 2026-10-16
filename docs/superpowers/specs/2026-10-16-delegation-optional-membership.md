# Delegation cache optional membership accounting

The decoded legacy delegation cache remains private to each system stateObject,
with the existing 32 MiB charge limit and 1,024-row LRU limit. It is not a process
wide budget. Execution copies do not share it and transaction-versioned readers
continue to bypass it. No KV format, consensus, Java ordering, duplicate/remove-all,
unknown-field, journal, error, or historical-reader behavior changes.

A cache entry owns its decoded pages and independent aggregate headers.
Membership maps only accelerate contains checks. Previously, a row could fail
admission because its charge included hypothetical maps even when those maps
were never constructed. Charge the owned base first, then construct and charge
both optional direction maps only when their combined charge fits. A failed
optional-map attempt contributes no charge. A real oversized base still bypasses.
On append, a missing map is built only if its complete charge fits; a failed
attempt leaves the base charge unchanged. A constant-time minimum map-size check
rejects clearly oversized candidates before scanning all addresses.

Map-free membership uses the existing bytewise linear scan, including tail hits
and absent peers. This cost must be measured in the complete mutation pipeline.
Page/aggregate allocation accounting stays conservative: aggregate growth adds
the entire new header allocation without forgiving previously charged arenas;
removal does not reduce charges for retained ownership. Thus an initially fitting
row can legitimately leave the cache after append, and will reload on the next
access. This patch does not attempt to retain every large row.

Cache hits continue to replay all logical page dependencies for optimistic
execution. They avoid backing Get and page decoding, not dependency bookkeeping.
Public reads continue to return defensive owned data. Generic anchor/page writes,
generation changes, rollback and execution-copy boundaries retain invalidation.

Three fixed-cardinality counters explain admission decisions:

- `state/delegation_legacy/cache_base_oversize_loads`: loaded entries whose owned
  base already exceeds the per-cache charge bound.
- `state/delegation_legacy/optional_map_skipped`: construction/append attempts
  whose fitting base cannot accommodate the optional map(s).
- `state/delegation_legacy/cache_admitted_without_set`: admission events with a
  map-eligible direction and no map, including readmission after mutation. This
  is neither a unique-account count nor an instantaneous gauge.

Resident heap can increase because previously bypassed rows now remain owned.
The charge bound is unchanged, but is a conservative ledger rather than an exact
Go heap/RSS bound; separate StateDBs can each retain their own cache. Report
retained header capacities, sampled Go heap and process high-water RSS separately.
Do not describe the 32 MiB per-cache ledger as a process memory guarantee.

Validation uses malformed/fallback/error, generation/rollback/copy/versioned-reader
regressions plus large two-direction and actual oversized-base boundaries.
A sealed same-image dual-Pebble probe runs canonical transaction dependency
recording, mutation, commitment/latest publication, CDC history flush and cold
trio construction. Compare final state root, hot/cold logical digests and exact
cold file hashes; report backing Get, decoded page bytes and dependency replay
separately from VFS ReadAt and device I/O. The probe does not include the live
freezer lease, publication/pruning or gate scheduling. A faster foreground path
alone does not prove a higher cold service rate or a smaller cold backlog.
