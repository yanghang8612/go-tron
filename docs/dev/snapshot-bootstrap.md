# Verified Snapshot Bootstrap

This runbook covers the operator flow for bootstrapping `gtron` from signed
state and chain-freezer snapshots. It is intentionally explicit: a node must
trust a catalog signer before it trusts any remote manifest path or segment
file.

## Inputs

- Snapshot URL: the HTTP(S) directory containing `snapshot-catalog.json`,
  `manifest.json`, and all referenced segment files.
- Trusted catalog keys: Ed25519 public keys for snapshot catalogs.
- Chain identity: selected by `--testnet`, `--genesis`, and optional
  `--snapshot.fork-config-hash`.

Official mainnet/testnet URLs and signer keys are release artifacts. Until they
are published, operators should pass their deployment-specific URL and key set
explicitly.

## Trusted Key File

Use `--snapshot.trusted-key-file` when more than one signer is trusted or during
rotation. The file accepts one or more keys per line, comma-separated entries,
blank lines, and `#` comments.

```text
# current production signer
ed25519:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

# rotation overlap: old signer remains accepted until the next catalog cutover
ed25519:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,
ed25519:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
```

Key rotation policy:

1. Add the next signer to the key file before publishing catalogs signed by it.
2. Publish catalogs signed by either old or new signer during the overlap.
3. After every node has had time to update the key file, remove the retired key.
4. Never reuse retired signer keys for future snapshot catalogs.

## Preflight Local Snapshot

After `snapshot fetch`, or before restoring a snapshot copied from another host:

```bash
gtron snapshot verify \
  --datadir /path/to/datadir \
  --snapshot.dir /path/to/datadir/gtron/state-snapshots \
  --snapshot.trusted-key-file /path/to/snapshot-trusted-keys.txt
```

This verifies the signed catalog, chain identity, manifest checksum, registered
segment families, file sizes, checksums, and format-aware segment checks. It
does not write chain state or freezer data.

## Fetch Then Restore

For a fresh datadir where you want to inspect the downloaded files first:

```bash
gtron snapshot fetch \
  --datadir /path/to/datadir \
  --snapshot.url https://snapshots.example.invalid/go-tron/mainnet/latest \
  --snapshot.reset \
  --snapshot.trusted-key-file /path/to/snapshot-trusted-keys.txt

gtron snapshot verify \
  --datadir /path/to/datadir \
  --snapshot.trusted-key-file /path/to/snapshot-trusted-keys.txt

gtron snapshot restore \
  --datadir /path/to/datadir \
  --snapshot.trusted-key-file /path/to/snapshot-trusted-keys.txt
```

`snapshot restore` refuses non-genesis datadirs. It restores state domains and
state-domain history, installs chain-freezer rows, verifies the canonical
boundary block, then advances canonical Headers/Bodies/Execution/Commitment/
Finish stages only after chain data proves the boundary hash.

## Optional Archive Trace Sidecars

Archive operators that enable balance-history capture can publish cold
account/balance trace sidecars with the same signed catalog:

```bash
gtron db audit-balance-traces \
  --datadir /path/to/datadir \
  --db.from-block 1 \
  --db.to-block 12345678

gtron db backfill-balance-traces \
  --datadir /path/to/datadir \
  --db.from-block 1 \
  --db.to-block 12345678 \
  --db.replay.dir /path/to/datadir/gtron/balance-trace-replay \
  --db.etl.tempdir /path/to/fast/tmp

# Optional tail-only acceleration when the replay DB can start from a verified
# snapshot boundary instead of genesis. Set --db.from-block to boundary+1.
gtron db backfill-balance-traces \
  --datadir /path/to/datadir \
  --db.from-block 12345679 \
  --db.to-block 13000000 \
  --db.replay.dir /path/to/datadir/gtron/balance-trace-replay \
  --db.etl.tempdir /path/to/fast/tmp \
  --snapshot.dir /path/to/datadir/gtron/state-snapshots \
  --snapshot.trusted-key-file /path/to/snapshot-trusted-keys.txt

gtron snapshot build-balance-traces \
  --datadir /path/to/datadir \
  --snapshot.dir /path/to/datadir/gtron/state-snapshots \
  --snapshot.from-block 1 \
  --snapshot.to-block 12345678

gtron snapshot build-section-blooms \
  --datadir /path/to/datadir \
  --snapshot.dir /path/to/datadir/gtron/state-snapshots \
  --snapshot.from-block 1 \
  --snapshot.to-block 12345678

# Equivalent one-pass manifest integration for both derived sidecars.
gtron snapshot build-derived-indexes \
  --datadir /path/to/datadir \
  --snapshot.dir /path/to/datadir/gtron/state-snapshots \
  --snapshot.from-block 1 \
  --snapshot.to-block 12345678

gtron snapshot publish-catalog \
  --datadir /path/to/datadir \
  --snapshot.dir /path/to/datadir/gtron/state-snapshots \
  --snapshot.signing-key <ed25519-seed-or-private-key-hex>

gtron snapshot prune-retired \
  --datadir /path/to/datadir \
  --snapshot.dir /path/to/datadir/gtron/state-snapshots
```

`snapshot build-balance-traces` repeats the coverage audit and refuses to build
if any canonical block in the requested range is missing a `BlockBalanceTrace`
row or if the trace payload identifies a different block hash/number. Generate
or repair the hot trace rows first; `gtron db backfill-balance-traces` does
this by replaying canonical blocks into an isolated replay database and then
copying only the generated trace rows back into the operator datadir through
the sorted ETL collector. The cold sidecar is only safe when the source range is
complete.

Use `--db.replay.dir` for long backfills so interrupted runs can resume from
the isolated replay database head. Use `--db.replay.tempdir` for one-shot runs
that should discard the replay database after the command exits. If a verified
state snapshot is available, pass `--snapshot.dir` plus the trusted catalog key
flags to seed an empty/genesis-only replay database from the signed snapshot
boundary before replay starts. Snapshot seeding can only backfill blocks after
that boundary; it does not reconstruct balance traces for the already-restored
prefix. The seed path restores latest state and state-domain history, verifies
the canonical boundary against local chain data, writes the boundary state
root/head, and copies the recent block/TAPOS window so the first post-boundary
block can validate without replaying from genesis.

`snapshot build-section-blooms` freezes existing java-tron-compatible `sb-`
rows for the source block range. It does not rebuild missing bloom rows; use
`gtron db rebuild-section-blooms` first when the hot bloom index is absent or
incomplete.

`snapshot build-derived-indexes` builds the balance-trace, section-bloom, and
event-log sidecars together and integrates them into a single manifest
generation. It uses the same balance-trace coverage audit as
`snapshot build-balance-traces`; run the specific single-dataset commands when
only one sidecar needs to be refreshed. Event-log builds advance the
`SnapshotEventLogBuild` stage to the highest covered source block.

`snapshot prune-retired` removes physical files listed in the manifest's
`retired` section after verifying that all active segments are still present.
It does not rewrite `manifest.json` or `snapshot-catalog.json`, so an already
signed catalog continues to authenticate the active snapshot view. Run it after
segment replacement or compaction when the retired files are no longer needed
for local audit. The runtime snapshot/prune lifecycle also runs the same
retired-file cleanup after cold/hot prune hooks in pruning modes that enable
snapshot-backed cold storage reclamation.

`snapshot fetch` and `snapshot verify` perform registered format checks for
`balance-trace` and `section-bloom` segments. At runtime, `ChainDB` falls
through to the snapshot manager for block/account balance trace reads and
section-bloom reads when hot rows are absent.

After a signed catalog has been fetched and verified, hot trace and bloom rows
covered by cold segments can be reclaimed:

```bash
gtron snapshot prune-balance-traces \
  --datadir /path/to/datadir \
  --snapshot.trusted-key-file /path/to/snapshot-trusted-keys.txt

gtron snapshot prune-section-blooms \
  --datadir /path/to/datadir \
  --snapshot.trusted-key-file /path/to/snapshot-trusted-keys.txt
```

The prune commands recheck the signed catalog and compare each covered hot row
against the cold segment before deleting anything. A missing or different cold
row aborts the prune. Runtime snapshot/prune lifecycle also runs balance-trace
and section-bloom pruning with persisted `SnapshotBalanceTracePrune` and
`SnapshotSectionBloomPrune` stages, so already processed cold segments are
skipped after restart. The manual prune commands update the same stages.

## One-Step Bootstrap

For the normal operator path:

```bash
gtron snapshot bootstrap \
  --datadir /path/to/datadir \
  --snapshot.url https://snapshots.example.invalid/go-tron/mainnet/latest \
  --snapshot.reset \
  --snapshot.trusted-key-file /path/to/snapshot-trusted-keys.txt
```

After bootstrap completes, start `gtron` normally. Sync resumes from the
verified snapshot/freezer boundary and imports the recent tail from peers.

## Safety Notes

- Use `--snapshot.reset` only for the snapshot directory, not for chain data.
  The command deletes the local snapshot directory before fetching the current
  remote catalog.
- Keep `archive` mode for RPC providers that need full historical state APIs.
  `full`, `snap`, `blocks`, and `minimal` may prune hot rows once verified cold
  coverage exists.
- If the catalog carries `forkConfigHash`, pass the matching
  `--snapshot.fork-config-hash sha256:<hex>` so a snapshot built for another
  fork configuration cannot install.
