# Pooled foreground base-read observation

## Purpose and scope

Measure residual logical absence before deciding whether to enable L6 Bloom filters for ordinary flat-latest reads. The isolated filter experiment reduced mixed-workload reads but added filter IO to a data-hot control and CPU to a present-only control. Its synthetic 25% absence rate is not evidence of the production foreground distribution. This change provides observation only; it makes no throughput or IO-reduction claim and does not enable that read policy.

`readBaseIntoCachePooledKey` serves Buffer and LayerView structured stack-key fallbacks after their overlay and base-cache checks. A schema-owned classifier selects only complete physical StateKVLatest keys with ContractStorage's 32-byte logical key or AccountAssetV2's canonical positive decimal int64 ID. Unknown, malformed and other domain keys are unmeasured. Classification does not establish existence, owner-generation currency, or decoded-value validity.

## Counters

The process-cumulative counters are:

- `blockbuffer/pooled_foreground_base/state_kv_latest/contract_storage/{calls,present,not_found,errors}`
- `blockbuffer/pooled_foreground_base/state_kv_latest/account_asset_v2/{calls,present,not_found,errors}`

`calls` counts completed pooled fallbacks. After the original synchronous read and scratch-key return, exactly one result is recorded: nil error is `present`, including empty or nil raw values; the helper's exact normalized `ErrNotFound` is `not_found`; other returned errors are `errors`. A panic is not a completed call. Separate atomic counters do not provide an atomic concurrent snapshot; at quiescence calls equal the three outcomes.

For an interval, report errors separately and compute successful-result logical absence as `delta(not_found) / (delta(present) + delta(not_found))`. A zero denominator is unavailable. Preserve reset and process identity boundaries when calculating deltas.

These counters exclude overlay hits/tombstones, positive and negative base-cache hits, cursor prefetch, generic non-pooled reads, and routes that return before reaching this helper. They are neither total SLOAD counts nor L6 probes, SST reads, device reads, or a direct estimate of avoidable block reads. A high absence ratio alone does not establish a filter benefit when data is already cached.

## Behavior and ownership

Classification obtains only a static metrics pointer before the read. Instrumentation retains no key or value. Recording happens after the existing pool return and does not read returned scratch memory. No additional backend operation, Has verification, timer, defer/recover, cache, configuration, writer policy, format, or error transformation is introduced. Existing read normalization, rawdb accessor error fallback, epoch admission, callback lifetime and panic behavior remain unchanged.

## Validation

`schema_pooled_foreground_test.go` checks complete physical key shapes, truncation, domain boundaries, canonical asset IDs and zero allocation. `pooled_foreground_base_metrics_test.go` checks actual nil-cache/cached helper results, empty values, raw versus normalized errors, transient-value ownership under epoch rejection, panic exclusion, both public structured routes, overlay/tombstone/cache/prefetch exclusions, and concurrent completed counts. Existing blockbuffer ownership and epoch tests also run in the application suite. No performance matrix is required or claimed for this observation change.
