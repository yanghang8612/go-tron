# Prefix predecessor candidates (2026-10-22)

Frozen before production edits. Parent: `9030ce7a31016491ece014ac76e5656ec149417e`.
Scope: `core/blockbuffer/predecessor.go`, new independent B tests/benchmarks,
this specification, and `docs/dev/evidence/serial-state-reads-oct22-b/`.
No cache admission, shielded call order, consensus, persistence, view capture,
snapshot close, or iterator behavior changes.

## Algorithm

Keep the existing Buffer/LayerView/ReadSnapshot topology captures. Visit layers
newest first. Within each layer, choose its largest visible point write within
prefix and the exclusive absolute upper bound. A monotonically increasing best
write lets later layers skip keys at or below the best. Keep only point
tombstones above the best plus captured newer-layer range tombstones to mask
older candidates and the durable-base reverse seek. Point writes precede this
layer's ranges. Writes win over a coincident same-layer point tombstone.

Reuse an already-built generic sorted shard index by descending bounded search;
never create or sort that generic index for predecessor. Before any timing,
revision 1 extends this design to reuse the existing account owner bucket index:
rawdb.DecodeStateKVLatestKey must accept the complete physical prefix; derive
the schema bytes from that decoded header, and use the decoded account ID's
first byte. Build/read the existing unsorted bucket with its existing mutation
lifecycle, exact prefix/bound filtering, and a TryLock/map fallback behind flush.
No new persistent cache fields or lifetime are added. Revision 2, also before
any timing, narrows production scope to typed StateKVLatest prefixes. Generic
prefixes, partial headers and unknown domains use the original overlay walk and
base predecessor implementation unchanged. Their hot-stable behavior remains
part of the unchanged <=10% control gate. For typed prefixes without either
index (including a contended bucket build or schema mismatch), scan maps
under their existing shard read locks and select the bounded best candidate.
Returned bytes are owned. Keep base predecessor error/release behavior and
snapshot history-scope/closed errors. No persistent cache is added.

## Correctness gate

An independent materialized map is updated directly with base and layer
operations, oldest first. It is the complete expected-state oracle, never an
iterator or `overlayState.walk`. Enumerate prefix/bound pairs and compare
Buffer, LayerView, pinned and fresh ReadSnapshot. Require zero differences.
Cover nil upper separately from non-nil empty upper, empty/0xff prefixes,
upper <= prefix, conflicting cross-layer writes/deletes, ranges before/after
points, chains of highest tombstones, flush/discard boundaries, owned result
mutation/reuse, and base error/iterator release and snapshot close/scope errors.
Existing tests remain additional validation. Run focused race validation.

## Performance gate

Build a byte-identical common benchmark harness in an exported actual parent
and the candidate. Fixed five samples per revision, order ABBAABBAAB (A parent,
B candidate), no additional samples. Use shared exclusive benchmark lock
`/Users/asuka/Projects/asuka/go/go-tron/build/benchmarks/20261018-tvm-event-replay/local-benchmark.lock`;
coordinate with A and never overlap timed runs. Record source/harness hashes,
commands, toolchain, ns/op, allocs/op, B/op, base Get/iterator calls, and generic
sorted-index constructions. Include fixed 8 MiB and 128 MiB unrelated overlay
payload scales, one queried root with multilayer unrelated hot writes, cold
and churn primary workloads, and hot-stable, small-prefix, and many-tombstone
controls. One actual root query per operation, no independent sixteen-root work.
Primary median speedup >= 20%, generic all-key index builds reduce >= 50%,
primary (max-min)/median < 0.30; all controls regress <= 10%. Any failure is a
failed gate, including a pure predecessor hot-stable regression when the parent
has warmed its own generic index. Do not hide this with iterator prewarming,
resampling, or shape selection. Freeze harness before timing.


## Harness freeze (before first timing)

`core/blockbuffer/predecessor_candidates_bench_test.go` uses fixed `16x`, cpu=1,
four layers, 256-byte unrelated ContractStorage values, and rawdb's canonical
SystemAccountAddress/SystemShielded root-change physical prefix and key. Root
height 1 lives in the durable base; each operation queries predecessor below
height 1,000,000. Cold discards only transient index holders outside timing to
model fresh layers. Churn appends 64 unrelated physical keys per operation;
its mutations are timed. Hotstable warms only the predecessor itself. Generic
uses the same unrelated overlay with an untyped root prefix. Smallprefix is a
64-row control at both scale labels; tombstones delete the 96 highest base root
changes, requiring repeated durable fallback. Count actual generic-index slice
construction by comparing the retained pre-query backing pointers against the
post-query pointers (no production instrumentation). Record base Get, iterator,
and reverse-seek counts; wrapper base iterators contain only relevant roots.
Candidate export contains only this B production file over actual9030, so A's
concurrent implementation cannot contaminate the B timing result.
