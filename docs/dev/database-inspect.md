# Database storage inspection

`gtron db inspect` explains an offline node's storage use without modifying
chain data. It reports:

- every live Pebble row in `chaindata`, grouped by the rawdb schema prefix;
- row count, key bytes, value bytes, logical bytes, and logical share;
- the total physical size of the Pebble directory;
- the physical bytes and row count of each ancient table (`bodies`,
  `tx_infos`, and `state_roots`).

## Run it

Pebble and the ancient freezer take database locks. Stop the node that owns the
datadir before inspecting it:

```bash
sudo systemctl stop gtron

/data/gtron/go-tron/build/bin/gtron db inspect \
  --datadir /data/gtron/main/datadir \
  --top 30

sudo systemctl start gtron
```

The scan reads every live Pebble key once. On a hundreds-of-gigabytes database
it can take a while and will populate the operating system page cache. Progress
is written every five seconds. Set `--progress 0s` to disable it.

For machine-readable output:

```bash
/data/gtron/go-tron/build/bin/gtron db inspect \
  --datadir /data/gtron/main/datadir \
  --progress 30s \
  --json > /tmp/gtron-db-inspect.json
```

`--top` affects text output only. JSON always contains every observed
keyspace.

## Interpret the numbers

The `chaindata` table shows **logical live bytes**: the uncompressed sum of key
and value lengths returned by Pebble. It is the right number for finding which
business keyspace owns the data, but it will not equal `du` because SST
compression, indexes, bloom filters, WAL files, obsolete files, and LSM
amplification are physical storage concerns shared by many prefixes.

The ancient table sizes are actual file sizes, so they are directly comparable
with `du`.

The most useful rows when diagnosing mainnet growth are:

| Output name | Key prefix | Meaning |
| --- | --- | --- |
| `transaction-info` | `ti-*` | Per-transaction `TransactionInfo` history in Pebble |
| `transaction-info-by-block` | `tib-*` | Per-block `TransactionRet` history in Pebble |
| `transaction-index` | `tx-*` | Transaction hash lookup index |
| `block-body` | `b-*` | Hot block bodies |
| `state-changeset` | `state-changeset-v2-*` | Temporal state pre-values |
| ancient `tx_infos` | freezer table | Frozen per-block transaction history |
| ancient `bodies` | freezer table | Frozen block bodies |

If `transaction-info` is large while ancient `tx_infos` also owns most of the
freezer, the report demonstrates hot/cold history duplication. New binaries no
longer write `ti-*`; individual lookups use `tx-*` to reach the canonical
block-level receipt instead.

## Prune legacy `ti-*` rows

Deploy the new binary before pruning. The node must be stopped, and an older
binary must not be used against the pruned datadir because it only understands
the legacy direct lookup.

The safe first step writes one durable range tombstone and lets normal Pebble
compaction reclaim physical bytes over time:

```bash
sudo systemctl stop gtron

/data/gtron/go-tron/build/bin/gtron db prune-tx-info \
  --datadir /data/gtron/main/datadir \
  --yes

sudo systemctl start gtron
```

Immediate offline reclamation is available with `--compact`, but on a large
mainnet database it may run for hours, generate heavy disk I/O, and need
substantial temporary free space:

```bash
/data/gtron/go-tron/build/bin/gtron db prune-tx-info \
  --datadir /data/gtron/main/datadir \
  --yes \
  --compact
```

The range is exactly `[ti-, ti.)`; it cannot delete adjacent `tib-*` or `tx-*`
rows. The command verifies that no live `ti-*` row remains before returning.
