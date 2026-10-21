# Optional hot history owner row spans

Keep the production service, binary and database unchanged. Reuse the 11,818,192
capture and the final V2 cold-routing input from the prior round. Compare against
frozen 4d34e751 with the identical complete HTTP harness, seven heights and
warm/fresh/private OS-cold protocol. No push, deployment, reset or restart.

The remaining first hot scan spends most CPU decoding all fields of unrelated
rows. Add a derived per-block sidecar in a schema-owned history key family.
It describes consecutive same-owner byte spans in the original RLP row list,
with row counts/sequence positions, a sorted full 21-byte owner dictionary,
decoded size and SHA256 of the exact encoded canonical pack. A checksum covers
the entire bounded sidecar. Span lengths and row counts must partition the
complete row list, without holes, overlap, trailing bytes or sequence overflow.

Preserve canonical raw/Snappy/v2/v3 packs and cross-block chunks byte-for-byte.
The fast path must still materialize through the original pinned reader and
verify every required shared chunk/pack or compressed envelope before it can
use the sidecar. Only the RLP field decoding of unrelated rows is avoided;
this round does not claim random access inside a Snappy block. Selected spans
must validate full owner, sequence, row count, bounds and transaction ordering
before callbacks; a bad optional index falls back without partial callbacks.
Missing/stale/oversized/unsupported indexes also fall back. Never turn an
underlying canonical read/decode error into a successful absence.

Construct complete indexes from the writer's already validated complete rows
and exact RLP, publishing with its atomic block scope; bare writers do not gain
an assumed atomic capability. Offline rebuilding pins the source, validates
every original row, and only writes derived keys. Positive sequence repairs
remain outside the sidecar and override packed rows before owner filtering.
No cache crosses a pinned view. Keep explicit byte/count bounds and report
actual index growth and build cost, not only latency.

Route the key family to the direct history partition. Legacy staging copies
may omit this optional accelerator and retain the ordinary read fallback.
Delete indexes with the selected authoritative history blocks; an index cannot
advance any publishing/pruning watermark. Snapshot/history restore remains
authoritative on canonical rows and can rebuild or omit sidecars. Latest-state
keys and MuHash inputs remain untouched.

Owner spans serve the unindexed tail, not the entire coldization backlog.
Posting publication retires covered spans in the same history batch as its
watermark. Large advances use one coarse auxiliary-prefix range; small/no-work
passes sweep at most 512 keys and 4 MiB plus one whole value. Repeated sweeps
catch older buffered index puts flushed after a prior retirement; old MVCC
views retain their source and index. The fixed-copy build is restricted to
11,817,531..11,818,192 (662 blocks), with before/after hashes of that source
range and all index writes constrained to the derived key family. The broad
first experiment is preserved separately and is not an accepted performance run.

Admission is bounded to 4 MiB metadata, 65,536 owners/spans, at least 32 rows,
at least 16 KiB decoded data, at least two owners and at most 4 KiB average row
size. Encoded sources larger than half the decoded bytes are not indexed; stored
sidecars cannot exceed 4 KiB plus half the encoded pack. Reads use a sidecar only
when selected rows are at most one third of the pack; otherwise a single full
decode wins over two validation passes. Scratch buffers are pooled with retained
capacity caps; every current index and canonical source is still verified anew.

Validate encoding and the selected reader against generic full-row iteration,
including aliases, absent owners, repairs moving owners, legacy TxNum order,
raw/Snappy/shared packs, stale/replaced canonical packs, truncation/corruption,
budget/cancellation, MVCC and prune/reopen. Run package/race/vet/native builds.
On a derived fixed copy strictly rebuild/check all indexed packs, compare all
HTTP return values and cache conditions, recompute full latest-state MuHash and
exercise the same three real history restore windows. Attribute residual CPU
and I/O separately from the primary performance samples; seal reproducible
source, input, per-request, index-space and correctness evidence.
