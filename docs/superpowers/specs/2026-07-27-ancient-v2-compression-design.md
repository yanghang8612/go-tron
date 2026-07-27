# Ancient V2 compression

**Status:** Implemented
**Date:** 2026-07-27
**Related:** [Chain freezer](./2026-05-19-chain-freezer-design.md),
[TransactionInfo deduplication](./2026-07-26-transaction-info-dedup-design.md)

## Evidence

The mainnet inspection at height 13,761,515 measured 186.71 GiB of
`tx_infos` and 102.04 GiB of `bodies`. Both tables currently store one raw
protobuf per block and Snappy-compress every row independently. This preserves
cheap random reads but prevents the codec from seeing repeated protobuf fields,
addresses, topics, contract shapes, and receipt structure in adjacent blocks.

The first implementation step is an offline benchmark over the real freezer.
It compares the current row-Snappy layout with row-Zstd and fixed-count
Zstd frames. Results include existing physical table size and a sample-ratio
projection so the production frame size is selected from chain data rather
than synthetic fixtures.

The deployed mainnet benchmark at height approximately 18.97M selected a
64-block frame:

| Table | V1 physical | V2 projected | Saving |
|---|---:|---:|---:|
| `bodies` | 133.05 GiB | 90.98 GiB | 31.62% |
| `tx_infos` | 241.35 GiB | 169.04 GiB | 29.96% |
| combined | 374.40 GiB | 260.02 GiB | 30.55% |

Moving from 64 to 128 blocks would save only another 2.13 GiB while doubling
single-query read amplification. A 64-block frame retains 97.3% of the maximum
measured 256-block saving and is the compression/read-latency knee.

## Target V2 layout

V2 keeps exact marshalled block bytes. Transaction-info rows may omit the
redundant nested `TransactionInfo.id`; readers restore it from the 8-byte hot
transaction locator so Wallet API messages remain byte-equivalent. Consecutive
rows are encoded as length-delimited records and grouped into independently
compressed Zstd frames.
An immutable segment contains:

- a versioned header;
- a frame table mapping block ranges to compressed offsets;
- checksummed Zstd frames;
- enough record framing to recover any original row byte-for-byte.

The benchmark candidates were 32, 64, 128, and 256 blocks per frame. Production
uses 64. Readers share a 64 MiB decompressed LRU cache, cache parsed record
offsets for O(1) row extraction, and coalesce concurrent misses for the same
frame. Decoder concurrency is capped at four and decoded frames are bounded to
protect the node from corrupt or unexpectedly large frame metadata.

Each segment covers 65,536 blocks. The versioned header and frame table have
CRC32C checksums, and every compressed frame has its own CRC32C. Before publish,
the migration reopens the new segment, validates every frame and record boundary,
and byte-compares the first, middle, and last records against V1.

## Compatibility and migration

- No consensus, P2P, protobuf, or Wallet API format changes.
- The reader supports a V2 immutable prefix and an appendable V1 suffix.
- Migration operates offline, oldest complete range first.
- A V2 segment is written to a temporary file, fsynced, reopened, sampled and
  checksum-verified, then atomically published before any V1 bytes are removed.
- Interrupted migrations resume from the last published segment.
- Existing V1 data remains the rollback source until its matching V2 segment
  has been durably published.
- After the initial offline migration, the running freezer promotes at most one
  newly complete segment per pass. It installs the published V2 view under a
  read-lifetime lock before advancing the V1 tail, so concurrent historical
  reads always resolve through at least one tier.
- Online promotion is cancellable during node shutdown. A cancellation before
  publication removes the temporary file; a cancellation after publication
  leaves safe V1/V2 duplication for the next pass to reconcile.
- New freezing and online promotion omit redundant transaction IDs only after
  validating the block/info cardinality. Existing V2 segments use a separate
  offline manifest-replacement rewrite that also upgrades legacy transaction
  locators before publishing ID-less rows.

Publishing a manifest is the commit point. Only manifests are loaded at node
startup, so crash-leftover temporary or uncommitted segment files are ignored.
After publication, advancing the V1 virtual tail is restart-safe: a crash before
tail reclamation leaves duplicate data; a crash during multi-table reclamation
is reconciled to the greatest tail on reopen.

Operators may first migrate one segment with `--keep-v1`, restart and validate
historical APIs, then run without `--keep-v1` to reclaim V1. Once V1 space has
been reclaimed, rollback to a pre-V2 binary is not supported.

## Non-goals

- No `TransactionInfo` field other than the transaction ID is removed or
  semantically normalized.
- Remote object storage and history pruning are separate operating modes.
- V2 does not change the hot Pebble schema.

## Acceptance criteria

- Benchmark sampling is deterministic, bounded-memory, and spread across chain
  eras by default.
- Every V2 record round-trips byte-for-byte.
- Mixed V1/V2 reads pass block and transaction-info accessor tests.
- Crash tests never expose a range missing from both formats.
- Concurrent reads remain byte-identical while an online segment is published
  and its V1 source is reclaimed.
- Historical transaction and block API responses match pre-migration output.
