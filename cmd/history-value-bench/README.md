# Read-only history value experiment

This command benchmarks a **copied, immutable V6 state-history trio**. It opens
only the three specified files, never opens chaindata or a production manifest,
and writes only stdout/stderr. There is no node, synchronization, publication,
pruning, or database-maintenance entry point.

```sh
CGO_ENABLED=0 go build -o /scratch/history-value-bench ./cmd/history-value-bench
/scratch/history-value-bench \
  --dir /scratch/copied-trio \
  --from-txnum 1734454193 --to-txnum 1734459458 \
  --max-raw-mib 128 > /scratch/value-benchmark.json
```

The default input is `history/state-domain-change-FROM-TO.seg`, plus its `.idx`
and `.kv` companions. Use `--history RELATIVE_PATH.seg` when the copied files
have another relative path. The input-size limit rejects a larger logical
stream rather than silently sampling a prefix. Memory can be several times
the logical stream size. SHA-256 and size are checked before and after reading.
The source copy should already be stable; these checks do not lock its writer.

The result includes value bytes and size buckets by flat/KV domain; maximum
value records without their contents; equal and prefix/suffix reuse between
successive records of each logical key; generic account-envelope versions and
legacy account protobuf field bytes. Account versions are measured rather
than silently converted to the current runtime codec.

Three experimental formats are actually encoded, compressed, decompressed,
and decoded:

* Same-key prefix/suffix copies plus literal middle.
* Same-key multiple copy/literal runs, using eight-byte anchors and a minimum
  sixteen-byte match. This can preserve several stable interior fields when
  distant counters change, including shifts caused by varint length changes.
* Segment-local exact-value deduplication, with full byte comparison after a
  hash match. Missing and present-empty retain separate existence flags.

Both deltas force a full value at least every 32 occurrences of that key by
default. An unprofitable patch falls back to a literal. Full stream equality,
including every original header and value, is the correctness oracle. The
container uses actual zstd `SpeedDefault` 128-KiB pages and counts the complete
48-byte header and 28-byte-per-page table. `current-v6` recompresses the original
stream under the same settings; original physical bytes are reported separately.

These are **not deployable snapshot formats**. Existing accessor offsets cannot
be reused. Deployment needs an explicit format version, rewritten offsets,
bounded anchor/value locators, and point/tx/prefix-query tests. A delta lookup
may need up to 31 predecessor versions, whereas the current history reader can
read one full previous value. The fixture demonstrates correctness and actual
compression, not production capacity savings or query latency.

## Layout findings and constraints

The current hot schema already stores only `TxNum, FlatDomain, Owner,
Generation, Domain, Key, PrevExists, Prev` in an RLP row, with per-block Snappy
packing. `Next` is transient; block number and sequence come from the physical
key, and block hash is stored once in `StateTxRange`.
See `core/rawdb/accessors_state_changeset.go`, especially
`persistedStateDomainChange` and `encodePersistedStateDomainChange`.

Current cold V6 records are a four-byte frame length followed by four-byte
key ID, eight-byte tx number, one-byte existence flag, four-byte value length,
and `Prev`. Keys are already dictionary encoded. The stream has one shared
tx-range table and dictionary commitment. Removing per-row owner/hash/Next
again is therefore not a new optimization.
See `core/state/snapshots/history_key_oriented_v6.go:829` and
`core/state/snapshots/history_binary.go:3043`.

The useful value opportunities are cross-version redundancy that independent
128-KiB pages may miss, and large stable subfields carried with small mutable
fields. Current account V4 already moves TRC10 maps, permissions, votes,
stake/resource and frozen-supply structures into KV domains, uses a presence
bitmap, omits defaults and uses signed varints. These are existing changes,
not additional projected savings. The account envelope still carries two
32-byte hashes; `EmptyKVRoot` is a candidate implicit constant only for a new
codec whose writer guarantees it. Address/code-hash sharing also needs checked
invariants; unknown fields and meaningful byte distinctions must survive.
See `core/state/state_account.go:11`, `core/types/account_storage_v4.go:47` and
`core/state/statecodec/codec.go:106`.

Full historic-state queries need the temporal key identity (including
generation), tx/block ordering and boundary mapping, existence, and exact old
values. `firstStateDomainChangeByKey` finds the first mutation after the target
tx and returns its previous value. Coalescing several transactions into one
block result would lose intermediate transaction states and is not acceptable.
Unchanged-value mutations are already filtered by the state journal; repeated
previous values at different times do not, by themselves, prove a redundant
logical history row. See `core/rawdb/accessors_state_changeset.go:2033` and
`core/state/domain_change_journal.go:242`.

Offsets, posting lists, tx indexes and compression page tables are derived and
can be rebuilt from complete immutable history plus its dictionary/range
metadata. They are still required by current readers: replacing or deleting
them without a validated replacement is not a space-recovery procedure.
The logical key dictionary and canonical block mapping are not disposable just
because they are metadata. All history versions remain logically retained.

## Verification

```sh
go test ./cmd/history-value-bench -count=1 -timeout=90s
```

Tests build a real compressed V6 trio through the current cold builder using a
temporary memory database, verify source immutability, and compare the entire
decoded stream for every codec. They also cover arbitrary binary values,
missing/present-empty distinctions, dedup references, checkpoint intervals,
decoded limits, invalid compression tables and invalid copy offsets.
