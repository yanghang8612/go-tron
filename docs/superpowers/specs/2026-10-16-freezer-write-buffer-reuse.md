# Direct freezer transform borrowing and encoded destination reuse

Baseline: `0063f4af20e4fdcaa1695c60dfecab915fa2713c`. This reduces repeated
allocation inside the existing cold writer. It does not change the shared gate,
quantum, checkpoint window, recovery, worker count, dictionary policy, codec,
CRC, frame layout, segment publication unit, verification or hot-prune ordering.
The cooperative lease contract in `2026-10-15-freezer-cooperative-lease.md`
continues to apply.

## Observed costs

A natural job's roughly 240-second wall and 90.4-second writing hold describe
different scopes. Writing includes source reads, preparation/validation,
dictionary training, frame compression/CRC/write, fused ETL collection/spills
and table sync. Gate wait is excluded from held phase time. An acquired history
lease proves handoff, not publication. No checkpoint/gate policy is changed
based on that aggregate measurement.

The code already keeps compression workers/encoders across checkpoints, the
fused ETL collector across the segment, and a verification reader per table.
It rebuilds preparation queues every 2048-record window and unconditionally
drains frames there even without contention. Those costs remain; an API change
to skip drains was rejected because measured drain time includes necessary
compression and ordered writes and is not all removable idle time.

An ignored memory-source 8192-block/48-transaction full three-table direct run,
with fused transaction-index publication and externalized receipts, allocates
about 4.27 GB. A same-process `alloc_space` profile delta attributes about
644 MiB to zstd EncodeAll output growth, almost entirely `append(dst,
blk.output...)`, and about 304 MiB to copying decoded block byte fields.
Workspace allocation is separate; it is not rebuilt per cooperative yield.

## Ownership changes

Direct transforms use the existing `UnmarshalBlockBorrowed` decoder only while
consuming body bytes synchronously to calculate block hashes or validate
receipts. Parallel preparation retains its existing owned data/body clones
before another source read, including sources that reuse one backing buffer.
Serial dictionary sampling and verification complete the transform before the
next source read. No decoded graph escapes the transform. The full decoder,
malformed-input errors, pre-PQ historical fallback, header-number checks and
receipt identity rules are retained; this is not a weaker wire validator.

A queued compression job exclusively owns an optional encoded destination.
The worker calls EncodeAll(raw, dst[:0]) and transfers the resulting bytes to
the ordered writer. The write callback must consume compressed bytes
synchronously and may borrow them only until it returns. All production calls
write directly to the output file and retain only scalar frame metadata.
Only after a successful ordered write does the writer return that destination
to its private pool. A failed write does not recycle it; the existing stop and
worker join complete before the pipeline returns. Workers never recycle output
and cannot reuse a buffer while its write callback is consuming it.

The idle output pool counts capacity, has a fixed aggregate 32 MiB ceiling,
ignores empty destinations and drops any result that does not fit. Taking a
buffer removes it from the pool before assigning it to a job. There is no
shared/global pool, CLI option or cross-table lifetime. The first frame and an
input larger than the existing 64 MiB raw-input threshold keep their original
single-encoder path. A compressed result larger than 32 MiB remains valid and
is written normally but cannot enter the idle pool.

The 32 MiB cap covers **idle retained encoded destinations only**. Destinations
leased to pending jobs, newly grown encoded output, existing raw input/reuse
buffers, encoder workspaces, preparation/ETL buffers and lookahead remain
additional. This can retain up to 32 MiB more idle memory during a table than
the original path; it is neither a total pending-output cap nor an RSS bound.
Taking a destination removes its capacity from the idle counter, but that
capacity remains live while the job owns it. A large destination reused by a
small frame can have capacity far larger than that frame's raw bytes. If
EncodeAll grows it, the old job destination and new compressed allocation can
coexist until the ordered drain clears the job pointer. Pending destination
capacity is constrained by the number of queued jobs and their individual
capacities, not by the 32 MiB idle limit or the raw-input limit. There is **no
claim that the change's total extra memory is at most 32 MiB**. A size-mix
regression explicitly exercises large -> small -> larger output capacities.
The pool becomes unreachable when that table's pipeline returns, including on
error/cancel. Existing pressure admission and quiescent-yield behavior remain.

## Evidence and acceptance limits

Ignored sources, overlays, profile deltas and raw results are under
`build/benchmarks/20261016-cold-write-gate/`, isolated by a nested go.mod.
The complete paired sequence uses the same deterministic source corpus,
compression configuration, table/index publication and logical oracle. It
validates every body/receipt record and block hash outside timing, plus full
published table/index file SHA-256 values. All four .gtv2/.gtxi file SHA values
are equal across the four reference/combined ABBA processes.

Combined allocation falls from about 4272.84 MB to 3386.57 MB (20.74%). Body
reads remain 16646, index coverage 8192, and persisted bytes 159484799. Measured
wall times are reference 1.1784/1.0628 seconds and combined 1.0780/1.0164 seconds.
The mean is lower, but short process runs and overlapping warm results do not
establish stable wall improvement. Borrowing alone saves 7.18% allocation with
no established wall gain; its earlier small single run regresses and remains
recorded. The output-destination change explains the additional reduction.

The final frozen-source check uses actual production source without pipeline
instrumentation, with 0063 baseline source overlays. Fused ABBA has two complete
runs/process (four samples/arm): allocation 4272.79 -> 3386.51 MB (-20.74%),
wall mean 1.1100 -> 1.0668 seconds (-3.9%). Current production sync disables
fused-index building; a separate two-run/arm control for that shape reports
4146.03 -> 3259.75 MB (-21.38%), wall 0.9481 -> 0.9206 seconds (-2.9%). These
modes are not pooled into one mean. Every run retains equal source-read counts,
all-record checks and the mode's identical output SHA set (four files with
index, three without); persisted bytes are respectively 159484799/153094685.
Short local timing remains supporting evidence, not a native throughput claim.

This fixture uses memory sources and local files. It does not measure production
Pebble source I/O, importer interference, server memory peaks, history handoff
progress or a mainnet cold-throughput improvement. Native acceptance still
needs a complete natural job with actual phase holds separated from yielded
wait, unchanged output coverage, history publication, and memory/pressure.

Regression coverage includes serial/eight-worker default/raw/trained dictionary
records and frame boundaries/CRC, actual output larger than the retention cap,
aggregate capacity accounting, read/write/encoder-init errors, cancellation at
a quiescent checkpoint and joined pending work, and unchanged valid/unknown/
legacy/malformed block decoding. Existing cooperative cancel/ABA/history and
publication-before-prune tests remain required.
