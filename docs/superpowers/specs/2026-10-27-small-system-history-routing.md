# Small system history routing

Provide an independent auxiliary `small-system-history-routing` singleton for system/zero account and generation keys, and system-owner dynamic property and fork vote keys. Discover keys from authenticated dictionaries; 59 is an observation, never a schema limit. Contract storage and metadata retain the existing route. A small-family batch never opens the broad route, including when its own route is absent or incomplete.

Coverage binds the entire history/accessor identity. An uncovered pair always uses authoritative readers. Empty-family pairs remain covered. New sources, replacements and reorgs cannot inherit absence proofs. Import reconstructs exact membership from covered source dictionaries. No canonical rows, MuHash input, before-images or frontiers change.

Use an independent magic, decoded cache and conservative limits. Authenticate current physical bytes even on a decoded cache hit. Build incrementally in maintenance batches of 64, or all supported cold pairs in a single explicitly requested offline build. Bound source count, key count, source-key and membership memory; honor cancellation. Publish through the existing fsync/rename and manifest transaction. Retire only unpublished auxiliary files under existing read leases.

Acceptance: compare baseline and candidate complete HTTP eth_call against the same fixed canonical copy and independent expected values, across historical heights and warm/fresh/private-OS-cold cases. Recompute MuHash and exercise actual historical restoration. Measure full source coverage, physical/decoded size, build cost and remaining query bottlenecks. Production service and deployment remain untouched.

The existing broad route remains available to storage and metadata queries. The new small object is additional disk usage; a reduction in system-query read/decode work must never be reported as deleting the broad file. A manifest containing the new kind requires an aware reader; canonical state and existing history formats remain unchanged.

The 64 MiB builder charge bounds accumulated working structures, not peak RSS. Maintenance retains a process-local, single-directory attempt-order cursor so unsupported leading sources cannot permanently starve later supported ones. This hint never proves coverage, and reset/rotation only affects performance. Busy maintenance still admits bounded batches; uncovered tail pairs use authoritative fallback.

Measurements distinguish complete HTTP from an isolated cold-manager batch. Private OS-cold means verified independent inodes, page-cache eviction and mincore below 1% before a fresh diagnostic process. Whole-process syscall traces include startup; per-request I/O counters are the basis for request-volume claims. CPU cumulative categories may be nested and must not be added together.

## Bootstrap amendment (2026-10-28)

The first production rollout exposed a transition regression: a partial new
route replaced useful broad-route negatives with raw accessor scans. Until the
small route covers an exact current pair, retain the authenticated selected
broad fallback for that pair. Select all system-family batch keys at the first
broad load, including when metadata triggered it first. A complete small proof
still never needs the broad object. Neither route can prove absence for an
uncovered pair. This supersedes the original unconditional bypass rule for
incomplete or missing small routes. Canonical rows and formats do not change.
