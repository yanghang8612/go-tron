# SST read execution-stack samples and compaction table bounds

## Purpose and limits

Current profiles show state point-read and compaction work, but logical misses do not establish SST data-block IO, and large commitment state does not prove large cross-namespace rewriting. Add two bounded observations using existing VFS reads and compaction callbacks. This is an intermediate diagnostic step, not a sync-speed optimization. Cache policy, filters, files, formats, writes, durability, snapshots and consensus are unchanged. Rejected CLOCK/index-reuse/default-L6-filter candidates are excluded.

## Completed SST ReadAt stack samples

The existing SST VFS observer reuses its one per-file atomic observation ordinal. Its original locality seed/selection and previous-offset rule are unchanged. A separate fixed salt plus successful-Open nonce selects approximately 1/256 completed reads, including first reads, errors and short reads. Reopening a path receives a new caller seed. Capture occurs after underlying ReadAt elapsed is frozen; actual n/error/buffer and closer ownership are unchanged. The sampled elapsed excludes the observation work, while full-function cost includes it.

Capture up to 96 physical PCs plus one truncation sentinel and expand inline frames with CallersFrames. A truncated physical or expanded stack is unknown_truncated; conflicting recognized roles are unknown_ambiguous; no marker is unknown. Function and closure-prefix markers are static and bounded. No PCs, GoIDs, keys, values, dynamic per-caller labels or unbounded map are retained.

Under `<namespace>disk/physical/read/sst/source_sample/fd/{random,sequential,other}/{role}/`, pre-register samples, returned_bytes, readat_nanos, errors and short_reads. The nine roles are commitment_demand, commitment_prefetch, state_prefetch, pooled_flat_stack, cold_builder_stack, pebble_compaction_path, unknown, unknown_truncated and unknown_ambiguous. This is 135 counters. Samples are mutually exclusive; live multi-counter snapshots are not atomic. FD class records the Open option, not a proven workload role or device behavior.

These are executing synchronous stack categories, not causal initiators or exact source totals. Unknown and truncation stay visible. `tableCacheValue.load` is synchronous under the current vendor's pprof.Do, so a visible caller marker is retained; work without one stays unknown. Pooled-flat requires both the pooled helper and a typed state latest read marker, not all SLOAD/flat access. `runCompaction` is a shared implementation path that may include flush/ingested work, not proof of a compaction Reason. No data/index/filter block-kind classification is claimed. VFS ReadAt can be satisfied by OS page cache, so none of these counters proves device IO or IO saved.

## Completed compaction event bounds

Initialize an immutable observer before pebble.Open from already validated, copied schema-owned DiskSpaceRanges, selecting only the range named commitment. Retain a private cloned start/end pair. The existing onCompactionEnd busy-time, activeComp decrement and underflow panic execute in their original order before observation. There is no DB reentry, file access, rescan, job-table retention or scheduling change.

Only Done, nil-error events with Reason default/elision-only/read/rewrite enter rewrite cohorts. Move, copy, delete-only, flush, ingested-flushable, unknown, failed and not_done events each have a separate status counter. not_done takes precedence over error, then failed over reason. Empty inputs/outputs are valid successful elisions.

For immutable commitment [start,end), classify each input/output table using its smallest/largest user keys:

- reversed extrema or no configured range: unknown;
- largest < start or smallest >= end: pure_outside_commitment;
- smallest >= start and largest < end: pure_commitment;
- otherwise: crosses_commitment_boundary.

Empty user keys are valid. End equality is never pure commitment. Crossing extrema are only candidates for mixed contents, not a claim that both families occur. A small crossing input can accompany large outside inputs in the same job. Three overlapping, input-only cohorts retain this distinction: all_successful_rewrite, has_cross_boundary_input, has_pure_inside_and_outside_inputs. Each stores the same job's input and output four-class table counts and table_size_bytes. Cohorts must not be summed; output layout does not determine membership.

Metrics live under `<namespace>compact/table_bounds/`: `events/{rewrite,move,copy,delete_only,flush,ingested_flushable,unknown,failed,not_done}`; `cohort/{cohort}/jobs`; `cohort/{cohort}/{input,output}/{class}/{tables,table_size_bytes}`; configured_instances; unconfigured_instances; size_overflow_events. Total 63 static counters; callbacks do not register metrics. The two instance counters mean observer-construction attempts before Open, including later Open failures, not successful/current DB instances or owners. Same-namespace helper/readonly instances aggregate counters without resetting configuration; job ownership cannot be recovered from that aggregation.

TableInfo.Size is manifest table scale, not actual physical IO, raw row bytes or avoidable rewrite. Virtual-table identity is unavailable in this event structure. Per-event class sums saturate locally at MaxInt64 and increment size_overflow_events once per overflowing event. Lifetime Counter sums do not saturate: any overflow event invalidates subsequent related byte ratios/upper bounds because later additions can wrap. Ordinary int64 counter lifetime limits still apply.

## Validation and measured cost

Functional tests in physical_read_caller_metrics_test.go and physical_read_source_route_test.go cover static role conflicts/unknown/inline/truncation, nonce, unchanged old locality, first/short/error reads, buffers and concurrent metrics. The real MemFS SST test explicitly verifies an ordinary unlabelled DB.Get unknown increment and a separate typed Buffer pooled-flat increment; Open alone need not issue unlabelled reads. Other role markers are synthetic tests, not six actual integrated routes. The test-only selecting FS is absent from production. compaction_bounds_metrics_test.go covers strict boundaries, ownership, input-only cohorts, all statuses, empty elision, callback lifecycle/panic, constructor coexistence, concurrency, uint64 size overflow and zero callback allocation.

Frozen ignored component results used Go1.25.5, CGO1, GOMAXPROCS2 and one ABBAAB group each. The ReadAt fake-VFS group used 1,048,576 mixed reads per process. Extra CPU was 19.67–26.52 ns/read and allocation 4.153 B/read; all three pairs met the predeclared diagnostic absolute limits (2.5 us/read and 16 B/read), with relative CPU +4.56–6.12%. Unsampled new allocations were zero. These are steady ReadAt-plus-Close costs, excluding separately measured constructor/Open setup (candidate about 37.9–38.5 KB allocation); they are not node throughput estimates. physicalReadFile grows 56 to 72 B and physicalReadMetrics 160 to 168 B, excluding counter-registry storage.

The callback group used 65,536 complete begin/end pairs, 21,848 successful rewrites with 128 input/16 output tables, plus excluded statuses. Extra CPU was 634–773 ns per callback pair over that fixed reason mix, steady allocation zero. Candidate setup allocated about 18.1–18.6 KB. The baseline windows were only 5.7–8.4 ms versus candidate 47.5–59.0 ms; preserve their large relative increases and do not divide the mix average by 144 to claim per-table cost or a fixed normal-job cost. No real compaction IO was measured. Both component groups preserve all failed setup artifacts and every measured arm; no sampling-rate or input tuning followed results. Combined integration checks and exact-source audit are required before release.

A subsequent fixed observation window must retain process/build identity, all unknown categories, exact existing VFS counters, stack samples and event classes. Source shares are sample-based, not exact/causal/device totals. Bounds aggregates can reject or motivate a later specific rewrite-reduction design; they do not by themselves justify namespace fences, forced rewrites or a speedup claim. This step must lead to a separately reviewed measurable optimization or an explicit evidence-based structural/hardware limitation.
