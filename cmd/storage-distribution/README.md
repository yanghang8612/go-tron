# storage-distribution

Read-only exhaustive follow-up to `generation-census`. Requires an immutable
offline image and a complete, MuHash-verified baseline census of that image.
The tool checks HEAD and every source-family count and length-framed SHA-256;
it does not repeat MuHash arithmetic. All three physical families must match.

```sh
go build -o build/bin/storage-distribution ./cmd/storage-distribution
build/bin/storage-distribution --db-dir /private/offline/chaindata \
  --baseline /private/reports/census.json \
  --out-dir /private/new-storage-analysis --sst-experiments
```

The output directory must be new and outside the database. Pebble is opened
read-only with an exclusive lock. No service stop, production deletion,
compaction, schema change or deployment is performed by the tool.

Outputs:

- `distribution.json`: exact source hashes, domain/class metrics, Storage value
  lengths, mutually exclusive number-width classes, leading-zero counts, and
  Shielded subkeys/root frequencies/continuous height runs.
- `storage-owners.jsonl`: every Storage owner/incarnation, ordered by row count,
  with its generation class, slot prefixes, byte totals, zero values, small slot
  tails, contract name/version and metadata-derived prefix agreement.
- `storage-values.hex`: every 32-byte Storage payload as 64 hex digits + newline;
  external sort can compute exact global value frequencies with bounded RAM.
- `shielded-runs.jsonl`: lossless contiguous equal-root runs, preserving gaps,
  owners and generations. Root trees, sentinels, notes, nullifiers and proof
  cache rows remain distinct; unknown subkeys are reported explicitly.
- Optional `sst/`: isolated comparison SSTs of all Storage rows and the Shielded
  block-root index, including obsolete/future generations. These are experiments,
  not database tables that the node can open.

Each SST variant uses Pebble v4, 4 KiB blocks/indexes, restart interval 16,
10-bit Bloom filters, and files of at most 500,000 records. Both Snappy and no
block compression are measured; normal SST prefix encoding still applies in
both cases. Every emitted table's block checksums are verified.

Storage variants compare the original physical key/value; a key containing
owner20 + generation8 + slot-tail16; and that compact key with a leading-zero
trimmed value and no presence wrapper. A separate metadata SST retains each
incarnation's first slot-prefix16. Add its bytes to either compact variant.
The experiment aborts if an incarnation has multiple prefixes or a Storage key
or payload is not 32 bytes. SST empty values preserve zero-but-present rows.

The Shielded comparison measures the full physical block-root index against
explicit start/end/root runs. Run keys retain owner, generation and start;
values retain end and root. Other Shielded subkeys are unchanged and excluded
from both sides of that comparison. A production change-point representation
could avoid updating a growing run endpoint by using the canonical cut as its
upper bound; that runtime protocol is not implemented here.

These full-input controlled SST sizes isolate the encoding's effect. They do
not measure an existing mixed-family LSM, historical indexes, WAL, compaction
debt, foreground latency or end-to-end replay speed. Numeric value shapes are
not claims about contract business semantics. Logical key shortening alone
does not prove a corresponding physical saving after prefix encoding.
