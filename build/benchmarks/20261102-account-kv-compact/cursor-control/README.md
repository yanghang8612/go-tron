# Production cursor control for the frozen account experiments

This is an independent second-stage command. It does not modify or replace any
Go source from research commit `ed33556bd99b45067e7712bb2f935ec4177219b3`.
The old standalone `SeekGE` measurements remain labelled as that strategy.

The production baseline at `0096c0af` uses exact full-key lookups through
`Snapshot.NewGetCursorWithOptions({MaxRetainedTables:16, UseL6Filters:true})`.
Ordinary SST `SeekGE` does not establish that filter behavior. In particular,
packing original rows removes non-anchor keys from the SST filter: using the
unchanged exact Get on such a bundle returns false absence. A correct bundle
reader needs a separate locator/range path or an additional original-key
membership structure whose space and query costs must be counted.

## Run

```sh
go build -trimpath -o account-cursor-control ./build/benchmarks/20261102-account-kv-compact/cursor-control

# The original SST producer must have finished, with its results.json present.
GOMAXPROCS=1 ./account-cursor-control \
  -mode resource -rows /FIXED_EXPORT/AccountResourceAux.bin \
  -sst-dir /FIXED_RESULTS/resource-results -out /NEW/resource-cursor-control \
  -max-rows 500000 -point-ops 10000 -rounds 2

GOMAXPROCS=1 ./account-cursor-control \
  -mode bundle -rows /FIXED_EXPORT/AccountAssetV2.bin \
  -sst-dir /FIXED_RESULTS/asset-results -out /NEW/asset-cursor-control \
  -max-rows 500000 -point-ops 10000 -rounds 2
```

The command rereads only the exact consumed source prefix, verifies its row
count and SHA against the frozen producer receipt, and requires every original
SST SHA/length to match that receipt. It copies each source SST to its own new
ingest input. Ingestion must leave exactly one real L6 table, with a nonempty
full-key Bloom filter and the same inode as the independent copy. It refuses to
compact or rewrite the SST. Original files are retained unchanged.

Each arm is reopened read-only with a fresh **32 MiB application cache** for each
workload/scope/round. The first pass starts with an empty application cache; the
second reuses that cache. OS page cache is uncontrolled. The two rounds alternate
old/candidate ordering. Scopes are either a fresh MVCC snapshot/cursor for each
lookup or one scope shared by 32 lookups. These are Pebble snapshots, not TRON
executable snapshot export/import tests.

- Baseline: production exact GetCursor options above.
- Resource candidate: identical exact cursor/key/filter policy, followed by
  expansion of the experimental value to the exact old canonical native bytes.
- Asset bundle candidate: a snapshot iterator seeks the target and, when needed,
  its predecessor; the complete bounded bundle is validated and the matching
  original wrapped value is returned. It does not claim original-key Bloom
  rejection. The new read-only matcher avoids constructing a full row slice,
  but still validates every member before accepting a result.

Every positive and negative lookup is checked. The negative workloads are
synthetic keys absent from the fixed cohort: a key inside an existing owner and
a key under an owner absent from the cohort. They do not establish real request
frequencies or canonical wire-level call traces. Resource same-owner synthetic
keys are a raw point-reader control, not an assertion that a production resource
API issues that logical key.

Results report wall time, VFS SST ReadAt calls/bytes, allocations, actual Bloom
rejections/may-contain counts and GetCursor table reuse. In-sample exact negative
controls must exercise a real Bloom filter. Unit fixtures additionally require
at least 90% of same-owner exact misses to be rejected and demonstrate that an
unchanged exact Get cannot retrieve a packed non-anchor row.

This is a production-policy control on one L6 cohort table. It is **not full HTTP
eth_call**, a mixed production LSM, OS-cold latency, a workload frequency estimate
or an executable MuHash/snapshot compatibility proof. No production DB is opened.
