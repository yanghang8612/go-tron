# Canonical flush boundaries and per-call distributions

Based on0063f4af. This change observes existing decisions; final32MiB/source128MiB
limits, the1920ms collection window, eligible solid prefix, range ordering,
sorted point writes, reservations, backend failures, cache promotion, retry and
reorg behavior are unchanged. No new row scan, classification, hash, database
read, retained key/value, timer or logging is introduced. The existing sampled
physical-family counters retain their original1/32 coverage and names; they are
not full-stream byte totals. Exact per-row family classification is deferred.

## Group decisions

`blockbuffer/flush/plan_stop/<reason>/{groups,layers,input_bytes,output_bytes}`
counts each attempted selected group. The corresponding `committed_stop` series
publish only after `Batch.Write` returns success. Here committed means API write
success, not an additional fsync/durability guarantee. If a backend partially
applies a batch then returns an error, its entire planned group remains excluded
from committed counters, exactly as the retry/prefix-drop protocol treats it.
Earlier successful groups remain counted and are dropped before retry.

The fixed reasons are:

- `own_range`: the first layer contains a range tombstone and must stand alone,
  even when it also ends the selected slice.
- `next_range`: the next layer prevents point-only merging.
- `source_cap`: the next source layer exceeds value or encoded aggregation size.
- `final_cap`: the projected final merged value or encoded size exceeds the
  final-batch limit.
- `selected_end`: the selected eligible slice was exhausted.

These reflect existing branch priority: own range first; during extension next
range before source limits, and source limits before final-size projection.
The initial32MiB source window filling is provisional: successful overwrite
extensions can continue, so that initial boundary is never reported as the
final stop. A source rejection does not prove a larger source limit would help;
final projection was deliberately not evaluated after that rejection.

`blockbuffer/flush/cap_hit/{source_value,source_encoded,final_value,final_encoded}`
counts attempted-group trigger bits for the actually evaluated stop. Both value
and encoded bits may fire for one group, so their sum is not a group count.
A source stop never fabricates a final-cap bit. `accepted_oversize/groups` counts
planned groups whose accepted output exceeds a final limit: existing behavior
permits a single oversized layer, including an isolated range layer. No limit
is tightened to make this counter zero.

Group input/output bytes use the existing logical key+value (or tombstone-bound)
size, not SST/WAL physical bytes. Input includes overwritten source versions;
output is the final canonical stream. Range routing may split physical records.

## FlushUpTo calls

`blockbuffer/flush/call/attempts` increments after acquiring flushMu. Final
outcomes `empty`, `success`, `error` are mutually exclusive. Empty means no
committed layer was eligible, including when newer committed layers exist;
it does not mean the buffer is wholly empty. A live scrape can see an unfinished
attempt before its outcome. `eligible_layers` includes empty calls. Nonempty
calls publish `path/batched` or `path/unbatched` and one `end` counter:

- `all_committed`: every committed layer in the initial captured topology was
  eligible. Later concurrently appended committed layers do not change it.
- `solid_cutoff`: the captured topology had later committed layers above the
  requested cutoff. Inflight layers are never examined or counted.

Only batched nonempty calls publish `eligible_input_encoded`,
`committed_output_encoded`, and `committed_groups` distributions. Unbatched
writers do not have a group plan or measured output; missing bucket increments
on that path do not imply zero bytes. Existing `Flush` (full drain) and standalone
flush helpers do not publish these FlushUpTo call distributions.

Input is the full selected prefix, even if a later batch fails. Output/groups
are only successful batches before that failure. Encoded byte sizes exclude
per-batch12-byte headers and scratch slack; they include logical range records,
which may be split downstream. Each dimension has `sum` (cumulative counter),
`max` (process-lifetime gauge), and a fixed `bucket/le_<upper>` counter plus
`bucket/overflow`. Buckets are **non-cumulative**, first-matching inclusive upper
bound: for example le_96 receives values above64 and at most96. Sum the relevant
buckets for a cumulative threshold; do not interpret them as Prometheus histogram
cumulative buckets. Lifetime maxima cannot be differenced as window maxima.

Layer bounds:0,1,8,16,32,64,96,128,256. Group bounds:0,1,2,4,8,16. Encoded-byte bounds:
0,1,8,16,32,64,96,128,192,256MiB. No height, key, path, error string or request ID
appears in a metric name. Registry size is fixed at package initialization.

## Upstream time and queue provenance

`chain/flush/collect/{queued_snapshot,window_timeout,queue_closed}` records one
terminal observed collector branch. A closed queue observed during either phase
wins over a prior timeout; a closed channel with all buffered posts consumed may
not reveal EOF until the worker's next receive and is then not a collector-close
sample. `chain/flush/inline/{not_started_or_closed,queue_full}` records existing
inline fallback selection. No channel read, wait, timer reset or scheduling
condition changes.

Collector counters are independent aggregate provenance, not per-storage-call
labels: direct FlushUpTo calls and inline fallback need not pass through the
collector. `selected_end` alone proves neither timeout nor solid cutoff caused
the window size. Compare boundary and collector distributions without inventing
that causal pairing.

## Availability, cost and validation

Counters/gauges are registered through the existing metrics registry during
package initialization and appear through an enabled node metrics endpoint,
including zero values before use. Missing series indicate an older binary,
a disabled/unavailable endpoint or collector/export problem, not zero activity.
A nonempty batched call has one bucket increment per dimension; these bucket
counts can be cross-checked against path/batched. All static bounds fit in a
fixed registry, with no reservoir or retained per-call samples.

New group work consists of scalar reason/bit assignments and bounded counter
updates once before and once after a successful batch. New call work reuses the
existing layer-size pass, accumulates integer sums, selects four fixed buckets
and updates sum/max. No per-row work is added. Tests cover both cap dimensions
and simultaneous bits, provisional extension, range boundaries, accepted
oversized layers, empty calls, registration/bucket edges, and first-successful
then failed/partially-applied batch plus retry with later layers preserved.
Collector tests verify the original cutoff/pending/closed results and one
observed terminal reason. Targeted race and a short complete fixed-input
root-oracle pipeline control check safety and material overhead. Raw evidence
is saved under build/benchmarks/20261016-cold-write-gate/commitment.
