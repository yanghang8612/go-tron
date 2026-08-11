# Event-log V3 snapshots

V3 targets the main `event-log` segment (about 95.79% of the measured active
event-log footprint). The external `event-log-index` companion remains V1 and
continues to map address/topic keys to candidate segment starts. A production
manifest may contain V1, V2, and V3 main segments at the same time.

## Physical format

V3 uses the distinct `gtevlg3\n` magic. Its fixed 176-byte header binds eight
contiguous sections:

1. block dictionary (`block number + block hash`);
2. transaction-hash dictionary;
3. 256-row frame directory;
4. delta-varint row frames;
5. payload-frame directory;
6. independent 32 KiB-target Zstd payload frames;
7. address dictionary plus framed delta-varint row postings;
8. topic-key dictionary plus framed delta-varint row postings.

Rows reference the block, transaction, and address dictionaries. The address is
removed from the stored `TransactionInfo_Log` protobuf and restored during a
read. Oversized single protobufs get a dedicated payload frame. Each row,
payload, and posting frame has a CRC32; the content-addressed file and manifest
retain the whole-file SHA-256 checksum.

One random row lookup reads at most one row frame and one payload frame plus
their directories and the three direct dictionary entries. Migration JSON
reports the measured physical maximum as `v3Physical.maxPointReadBytes` and
`v3Physical.maxPointDecompressBytes`. It separately reports the hottest single
address/topic lookup bounds; filesystem block amplification is not guessed.

## Fresh syncs

When cold event-log construction is enabled, new node runs write V3 main
event-log segments directly. The runtime default is
`--snapshot.event-log.version 3`; the equivalent environment variable is
`GTRON_SNAPSHOT_EVENT_LOG_VERSION=3`. The cold lifecycle reads canonical blocks
and `TransactionInfo` rows in two streaming passes, writes the V3 main segment,
derives its V1 external companion from the verified embedded dictionaries, and
only then atomically publishes both refs in the next manifest generation. It
does not create an intermediate V2 event-log segment.

The direct writer is used by both incremental state-snapshot catch-up in `snap`
mode and the chain-freezer snapshot lifecycle in `minimal` mode. Restarting a
node resumes at the first range not covered by the pinned production manifest;
already published V1/V2/V3 ranges remain readable and adjacent new ranges may
use V3.

For an explicit fresh-sync configuration:

```bash
./build/bin/gtron \
  --datadir /data/gtron/main/datadir \
  --prune.mode snap \
  --snapshot.event-log.version 3 \
  <the remaining normal mainnet arguments>
```

V3 is the default, but keeping the flag in the service command makes the chosen
format visible. To stop creating new V3 segments without changing existing
immutable refs, restart with `--snapshot.event-log.version 2`. This is a writer
selection only; the reader always supports mixed V1/V2/V3 manifests.

The manual builders use the same selection and default:

```bash
./build/bin/gtron snapshot build-event-logs \
  --snapshot.dir <snapshot-dir> \
  --snapshot.from-block <from> --snapshot.to-block <to> \
  --snapshot.event-log.version 3
```

On a measured 283,399,461-byte mainnet V2 segment, direct V3 construction
produced 60,514,272 physical bytes (78.647% smaller) and completed its exhaustive
verification in about 59 seconds on the test node. This is evidence for the
selected writer, not a promise that every segment will have the same ratio.

## Migration command

The command loads `manifest.json` once and opens a pinned snapshot manager. It
does not open chaindata or acquire the Pebble lock, so build-only runs are safe
while `gtron` is online.

List active boundaries:

```bash
jq -r '.segments[] | select(.dataset=="event-log" and .kind=="event-log") | [.fromTxNum,.toTxNum,.size,.path] | @tsv' \
  /data/gtron/main/datadir/gtron/state-snapshots/manifest.json | sort -n
```

Build and exhaustively verify eight consecutive segments without changing the
manifest:

```bash
./build/bin/gtron snapshot migrate-event-logs-v3 \
  --snapshot.dir /data/gtron/main/datadir/gtron/state-snapshots \
  --snapshot.from-block <exact-active-from-boundary> \
  --snapshot.event-log.merge 8 \
  > /tmp/event-log-v3-preview.json
```

An exact end boundary can replace the merge count:

```bash
./build/bin/gtron snapshot migrate-event-logs-v3 \
  --snapshot.dir /data/gtron/main/datadir/gtron/state-snapshots \
  --snapshot.from-block <from> --snapshot.to-block <to> \
  > /tmp/event-log-v3-preview.json
```

Inspect the output before publication:

```bash
jq '{sourceGeneration,fromBlock,toBlock,sourceSegments,sourceMainBytes,
     v3MainBytes,mainSavingsBytes,v3Physical,segments}' \
  /tmp/event-log-v3-preview.json
```

Publish by rerunning the same range with `--publish`:

```bash
./build/bin/gtron snapshot migrate-event-logs-v3 \
  --snapshot.dir /data/gtron/main/datadir/gtron/state-snapshots \
  --snapshot.from-block <from> --snapshot.to-block <to> --publish \
  > /tmp/event-log-v3-publish.json
```

Publication happens only after the V3 main file and V1 companion pass semantic,
frame, checksum, and cross-file verification. The command refuses publication
if the manifest generation or active refs changed during the build. Use a quiet
snapshot-maintenance window for the short publish step. Existing readers keep
using immutable old refs until they reload the atomically renamed manifest.

Old active refs move to `retired`; they are not deleted by this command. Do not
run retired-file pruning until V3 has passed production query and latency
validation. Re-running the same migration is safe because outputs are
content-addressed. V1/V2 readers remain available, and
`snapshot build-event-logs --from-cold` can rematerialize a selected range with
the V2 writer if rollback is needed before old retired files are pruned. Pass
`--snapshot.event-log.version 2` explicitly to select the legacy writer.

After publication, resample the active layout and exercise archive filters:

```bash
./build/bin/gtron snapshot event-log-space-benchmark \
  --snapshot.dir /data/gtron/main/datadir/gtron/state-snapshots \
  --snapshot.event-log.sample-segments 16 \
  > /tmp/event-log-space-after-v3.json

curl -sS -X POST http://127.0.0.1:8545 \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x1","toBlock":"latest","address":"<hex-address>"}]}'
```

`eth_getTransactionReceipt` is unaffected by the event-log segment format; cold
receipt logs continue to come from chain-freezer `TransactionRet` records.
