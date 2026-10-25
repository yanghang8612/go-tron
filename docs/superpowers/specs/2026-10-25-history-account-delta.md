# Bounded account before-image deltas (offline)

Authorized scope: local implementation and isolated fixed-capture validation.
No push, deployment, production service start/stop/restart, reset, deletion or
budget changes. Baseline is the completed V11 capture at head 11,818,192 and
its immutable `.seg/.idx/.kv` closure. Keep baseline and candidate separate.

Preserve the entire canonical V6 virtual stream byte for byte. A new physical
compressed-block version may encode account Prev bytes as reversible deltas;
logical offsets, key IDs, index/posting tuples, canonical values and MuHash
element bytes must remain unchanged. Non-account bytes are opaque literals. ADP3 column layout separates account
values from record headers and stably groups values by owner key ID within
one page; its explicit permutation reconstructs the original order. ADP1
patches copy only complete bytes from registered same-page bases, with full
checkpoints at the depth limit. A numeric patch may replace changed signed
varints inside a canonical V5/GTRC envelope with signed modular differences.
It requires identical presence bitmap, core version, all opaque byte fields
and envelope trailer. It regenerates canonical varints and RLP lengths and
compares/reconstructs all original bytes; unknown schemas fall back to byte
patches/literals. Addition/subtraction use explicit modulo-2^64 arithmetic,
including signed limits, and a changed-field bitmap is checked against the
base numeric schema before reconstruction. ADP2 and shared-envelope templates are trial
controls; no owner address, protobuf field or unknown byte is elided.

Dependencies are strictly within one independently decoded page, at most
1 MiB of canonical output (trial page sizes: 128 KiB, 512 KiB, 1 MiB). Account values larger than 4096 bytes remain
literals. Every checkpoint is complete; the candidate uses depth four, with a hard
maximum eight, and every base must be an earlier registered
account value of the same page. Never follow an external segment, hot DB,
arbitrary logical byte range or an unbounded historical chain.

Trial depth limits 1/4/8 on actual fixed and mature files. Compare prefix/suffix
patches and equal-width change masks after the existing Zstd codec, counting
all markers, headers, directories, checksums and companion/rebinding costs.
Compare an ordinary 1 MiB Zstd control separately: page size itself can improve
compression and must not be attributed to account deltas. A candidate page
selects the shortest of ordinary Zstd, owner-grouped columns and bounded
owner deltas, including its mode marker. Retain ordinary compression for unhelpful pages and retain the original file
when the complete converted file is not smaller. Report both raw delta
opportunity and actual compressed net bytes; do not extrapolate logical zeros
or repeated fields into physical savings.

The decoded page is bounded before allocation, canonical length and SHA256
must match, malformed references/depth/masks/lengths/truncation fail closed,
and cancellation and reader ownership remain intact. Old formats stay
explicit. Full import/compaction/inspect/restore paths must recognize the new
physical version before it can be emitted by a candidate builder.

Acceptance: real page/file byte equality and canonical digests; actual final
fsync/allocated/file-length accounting on the fixed capture; strict companion
and route closure; full MuHash recomputation; historical range/restore and
snapshot restore regressions; unchanged full HTTP host and oracle with warm,
fresh-process, private OS-cold ABBA comparisons. Do not run heavy census or
rewrites during timing. Preserve all samples, source identity, limitations and
non-improving outputs.

The new physical footer version is 4 (`gtcend04`); old reader paths remain
explicit. It is opt-in via compression format 4. Existing `auto` policy and
default page sizes remain unchanged. Conversion verifies the entire original
trio, compares every canonical virtual byte, copies/rebinds the immutable
companions and runs the strict semantic verifier before returning new refs.
Manifest publication and routing rebinding belong to the separate offline
lab driver; the conversion API never deletes a source or advances coverage.

Larger pages retain the old decoded-cache byte allowance per reader, with
at least one full page allowed. Sixteen 128 KiB entries become at most two
1 MiB entries. Cold/warm comparisons must use this bound, rather than silently
raising the per-reader cache eightfold.

This is an experimental forced format, not a replacement for `auto`: format 4
does not also select the large-value CDC container. The fixed-capture rewrite
retains original files when unhelpful; a from-genesis builder does not make
that whole-file comparison. Compaction under `auto` may later choose another
container. Therefore a successful offline rewrite alone does not establish
the best long-term production policy. Report any latency regression and the
increment over ordinary 1 MiB Zstd before recommending rollout.
