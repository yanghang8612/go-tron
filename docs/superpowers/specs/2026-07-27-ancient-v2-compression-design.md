# Ancient V2 compression

**Status:** In progress
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

## Target V2 layout

V2 keeps the exact marshalled protobuf bytes. Consecutive rows are encoded as
length-delimited records and grouped into independently compressed Zstd frames.
An immutable segment contains:

- a versioned header;
- a frame table mapping block ranges to compressed offsets;
- checksummed Zstd frames;
- enough record framing to recover any original row byte-for-byte.

The initial candidates are 32, 64, 128, and 256 blocks per frame. Readers use
a bounded decompressed-frame cache. The selected default must materially beat
row compression while keeping single-block read amplification bounded.

## Compatibility and migration

- No consensus, P2P, protobuf, or Wallet API format changes.
- The reader supports V1 freezer shards and V2 segments concurrently.
- Migration operates offline, oldest complete range first.
- A V2 segment is written to a temporary file, fsynced, reopened, sampled and
  checksum-verified, then atomically published before any V1 bytes are removed.
- Interrupted migrations resume from the last published segment.
- Existing V1 data remains the rollback source until its matching V2 segment
  has been durably published.

## Non-goals

- Removing or semantically normalizing `TransactionInfo` fields is a separate,
  higher-risk phase.
- Remote object storage and history pruning are separate operating modes.
- V2 does not change the hot Pebble schema.

## Acceptance criteria

- Benchmark sampling is deterministic, bounded-memory, and spread across chain
  eras by default.
- Every V2 record round-trips byte-for-byte.
- Mixed V1/V2 reads pass block and transaction-info accessor tests.
- Crash tests never expose a range missing from both formats.
- Historical transaction and block API responses match pre-migration output.
