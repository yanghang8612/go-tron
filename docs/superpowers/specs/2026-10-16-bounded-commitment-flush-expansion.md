# Bounded expansion for already coalescing canonical flushes

Based on 152fa7e96906e52946ce81f828c7000d31d19f03. Canonical point writes currently stop at a projected 32 MiB final value or encoded batch, even when a captured whole-layer prefix has extensive overwrites. Two separate batches can persist the intermediate branch versions to the WAL. A representative real ordered-commitment trace has 97.77 MB encoded source across 64 layers and 50.35 MB output in two groups; its whole-window final image is 42.29 MB. Repartitioning within the original bounds saves only 0.629%, and identical-value suppression finds no opportunity. Suppressing an earlier put while still committing its prefix would be incorrect.

## Admission and bounds

The base final value/encoded caps remain 32 MiB. Source value/encoded caps remain 128 MiB. Only the existing point-only probe-extension path may cross the base final cap. After the existing range and source checks and the existing size-delta scan, every such extension must satisfy all of:

- Existing accumulated input logical value bytes divided by existing output logical value bytes is at least two. This uses integer division without overflow-prone multiplication and is checked before adding the next layer.
- `next.encoded > encodedDelta`: the next layer itself eliminates a positive number of encoded record bytes. Repeated earlier layers followed by a wholly new next layer do not qualify.
- The conservative `len(merged.ops) + next.ops` upper bound is at most the existing 262,144 pooled write-key limit. No intersection scan is introduced.
- Both projected final value and encoded batch size are at most 64 MiB.

Later extensions over 32 MiB recheck these conditions. An extension below both base caps uses the original path, including its original row-count behavior even if the group previously expanded; the conservative entry bound applies only to above-base admission. No greedy search, regrouping, family classification, durable equality read, cache, timer or additional source scan is introduced. The source cap is checked first; range layers stay isolated; an originally accepted single oversized layer remains accepted. The batch buffer pool remains capped at 32 MiB, so an expanded batch can allocate an unpooled representation. This cost is measured rather than hidden by increasing the pool.

## Prefix, errors and ownership

A selected group is still an exact contiguous prefix of the captured eligible layers. Its canonical writes represent the final state of that prefix, in the same sorted order. Only successful groups promote the cache and drop their layers. If a backend partially applies a batch and returns an error, that entire group remains pending and retryable. Successful prefix boundaries can differ from the old grouping, which is intentional; no row from an uncommitted later group substitutes for an earlier successful checkpoint. The root, key/value encoding, history routing, source solid cutoff, reorg exclusion, and history Write → Sync → hot Write ordering remain unchanged.

Keys and values still borrow immutable selected layers during planning and are copied by the backend batch before selected layers are released. No arena is pooled or reused. No backend durability setting or sync call is removed.

## Observation

Fixed gauges under `blockbuffer/flush/limit/` report `base_value_bytes`, `base_encoded_bytes`, `max_value_bytes`, `max_encoded_bytes`, `source_value_bytes`, `source_encoded_bytes`, `expansion_min_input_output_ratio_ppm` and `expansion_max_entries`. They are registered and updated once at package initialization. Enabled node metrics expose configured policy; zero activity is distinct from a missing registration or disabled metrics.

`blockbuffer/flush/expanded/{planned,committed}/groups` counts groups admitted past the base cap. Planned counts attempted writes; committed counts only API Write success, not an extra fsync guarantee. Failed or partially applied groups do not increment committed. A group that crossed the base cap is marked expanded even if a later overwrite shrinks it. Accepted single-layer oversize remains separate. Existing stop/cap counters report actual rejected boundaries, retaining own-range → next-range → source → final priority and independent trigger bits. A final stop now includes rejection by the expansion guards; it does not prove increasing 64 MiB would help. Existing call histograms and sampled family bytes keep their documented semantics.

## Validation and tradeoffs

Tests cover exact ratio/row/size limits, positive marginal elimination and its zero-margin counterexample, repeated ratio checks, source precedence, range isolation, single oversize, successful expanded prefix followed by errors before/after partial application, retry and future-layer reorg. Full dual-Pebble experiments use fixed captured layers, per-block roots from a serial staged-store oracle, every successful batch's checkpoint root, complete final/reopened key/value digests, actual side-specific WAL/SST/read counters, large-flushable thresholds, stalls, memory and foreground latency. Controls include small windows, source-limited traces, append, history-dominant and small key/value streams. A paired snapshot control spans complete windows and compaction; public Compact does not force rewriting terminal L6, so residual pinned versions cannot be claimed fully elided merely because L0/debt are zero.

The measured benefit is fewer WAL bytes and batch writes on qualifying traces. SST/compaction bytes can remain unchanged because memtable overwrites already coalesce those versions. Larger batches increase allocation and synchronous batch/snapshot tail latency. Results from local ARM64 with a small seed image fitting the production-size cache are not an AMD64 mainnet throughput forecast or proof of improved freezer service rate. Unconditional 64 MiB was rejected because negative controls paid these costs without enough I/O reduction. The ignored evidence report preserves both rejected and final results.
