# Bounded ordered page encoding for record columns

Follow the completed 0071ef14 allocation experiment. Its 45 real publication
samples preserve exact physical bytes but remain 53.52–68.17% slower than V2.
CPU profiles of the same three pairs show extra Zstd encoding work; common
column preparation is only 0.85% cumulative CPU. V5 ignores the factory worker
budget and always encodes synchronously, while V2 enters an ordered pipeline
at 1 MiB. Parallelism is a hypothesis to measure, not a promised speedup; the
extra work of evaluating all four candidates also remains.

Keep physical V5, its exact page bytes, all four candidate comparisons, strict
smaller selection, canonical offsets and independent 128 KiB decoding. Honor
the existing compression worker budget, capped by the existing maximum four;
do not raise CPU or memory configuration or alter CDC/ordinary paths.

Use bounded page jobs and ordered reduction, with at most workers*2 raw pages
in flight. Each worker owns an independent reusable column encoder. Every job
owns a copied header-position list; never queue the writer's mutable starts
slice or borrowed record/value buffers. Return annotation storage only after
the result is safely consumed. Bound annotations by floor(pageBytes/21).

The encoder's winning output is borrowed until its next call. A worker must
move that buffer to the result and detach it from the encoder; its remaining
candidate buffer stays private. Take a distinct output destination from the
bounded encoded pool for the next job. A reducer returns the winning buffer
only after synchronous body append. Never reuse a pending result's bytes.

After a page is queued, body.uncTotal belongs to the reducer and cannot locate
the writer's next annotation range. Derive that range from the writer-owned
logical position minus current chunk length. Preserve the existing first-page
split and retained prefix, including headers and values crossing a boundary.
Drop unused serial encoder scratch after switching to the pipeline. Reset,
abort, cancellation and errors must join both workers and reducer before
reusing/releasing storage or replacing a target.

Ordinary V2 retains its existing memory formula. Charge V5's extra per-worker
scratch and annotation buffers explicitly: raw/result pools, retained prefix,
each worker's private scratch and the main writer's annotation scratch. Verify
a conservative <=6 MiB buffer-capacity bound at four workers; this excludes
the pre-existing Zstd engine, ETL and block directory and is not a process RSS
claim. Exact-size global pools may reject differently sized scratch safely.

Tests must compare complete physical files and canonical reads for workers
1/2/4, varying Write partitions, page transitions, large cross-page values,
partial headers, plain fallback, Reset and backfilled prefix. Exercise queued
results during repeated worker encodes, encode/write failure, full-queue
cancellation and abort; prove no target publication or goroutine leaks and run
race checks. Then repeat the same real-pair publication matrix, and final
MuHash, full HTTP warm/fresh/private-cold and actual history/snapshot recovery
with the final candidate binary. No push, deploy, production restart or deletion.
