# generation-census

Exhaustive, read-only state generation accounting on an **immutable offline
chaindata copy**. The CLI requires Pebble's exclusive lock; do not point it at a
running service or copy mutable live database files without a consistent cut.
The tool has no reclamation path.

```sh
go build -o build/bin/generation-census ./cmd/generation-census
build/bin/generation-census --db-dir /private/offline/chaindata \
  --out /private/reports/census.json \
  --groups /private/reports/inactive-groups.jsonl
```

Outputs must be new files outside the database. Default timeout is two hours.
MuHash verification is enabled by default and rebuilds both normalized state
and digest from every account, generation and KV source row. The final CLI also
verifies all live account envelope generation pointers against the generation
table/default. An error produces an incomplete report and nonzero exit status.
Never use a partial report, or `--verify-muhash=false`, as reclamation evidence.

`obsolete` means stored generation < current generation; `current` means equal;
`future` means greater and requires investigation. Missing generation means zero.
Rows are visible logical records. Key + wrapped value bytes do not measure
compressed SST space, obsolete LSM versions, WAL, indexes or physical savings.
Hashes frame each key and value with their big-endian uint64 lengths.

For an image already scanned by the initial diagnostic build, a companion pass
can verify account pointers without repeating MuHash work:

```sh
build/bin/generation-census verify-account-pointers \
  --db-dir /private/offline/chaindata --out /private/reports/account-pointers.json
```

This companion is not a substitute for a full census. Match its complete
block/hash/transaction/root cut and account count to the authenticated census,
and independently preserve the immutability of the image between passes.

The reclamation protocol and its remaining implementation gates are described
in [the design](../../docs/superpowers/specs/2026-10-18-obsolete-generation-census-and-reclamation.md).
