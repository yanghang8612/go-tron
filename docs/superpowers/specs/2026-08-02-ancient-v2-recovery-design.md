# Ancient V2 recovery after Erigon storage alignment

**Status:** first recovery slice implemented
**Date:** 2026-08-02

## Problem

The Erigon-alignment merge replaced the old freezer/index implementation and
deleted three independent operator capabilities with it:

- logical rawdb inspection;
- seekable Zstd compression for the large `bodies` and `tx_infos` Ancient
  tables;
- the immutable transaction-hash lookup used before hot `tx-*` pruning.

The current chain-freezer remains correct and its virtual-tail, repair, snapshot
fallback, and chain-index work must be preserved. Reverting the merge or copying
the complete old freezer implementation would reintroduce competing index and
tail models.

## Recovery order

1. Keep finite hot history pruning active during long full/blocks/minimal syncs.
2. Restore inspection against the current rawdb schema.
3. Restore only the proven V2 compression tier and integrate it with the
   current freezer.
4. Rebuild transaction lookup optimization on the current chain-index and
   snapshot-manager boundary instead of restoring the superseded standalone
   index unchanged.

## V2 format and publication

The recovered format retains the mainnet-benchmarked layout:

- 65,536 blocks per immutable segment;
- 64 rows per independently decoded Zstd frame;
- checksummed header, frame table, and compressed frames;
- manifests as the only publication/commit point;
- mixed V2 prefix plus appendable V1 suffix reads;
- a bounded 64 MiB decoded-frame cache and capped decoder concurrency.

Fresh-genesis writers use a bodies-specific compression profile without
changing the stored block representation or the 64-row random-read boundary.
Up to 256 bodies are sampled at evenly spaced positions across each immutable
65,536-block segment, within fixed 16 MiB sample and 64 KiB history budgets.
They train a standard Zstd dictionary whose entropy tables and content are used
by every frame at the better-compression level. Degenerate corpora that cannot
train fall back to the previous raw-history dictionary. The reserved V2 header
codec field, dictionary ID, and checksum make both encodings self-describing
and fail closed; the dictionary bytes are covered by the same
file fsync/rename and manifest publication transaction as the frame payloads.
`tx_infos` and `state_roots` retain their existing codecs. Reads still return
the exact original `corepb.Block` wire bytes, so block hashes, transaction
index fusion, replay, and RPC behavior are unchanged.

Offline migration writes and verifies every table segment, publishes the
manifest atomically, installs the new reader, advances the V1 virtual tail, and
physically reclaims fully hidden V1 shards. An interruption before manifest
publication leaves an ignored temporary file; an interruption after publication
leaves readable duplicate data that the next pass reconciles.

## Current-freezer safety adaptation

The aligned freezer has one virtual tail shared by all tables. Therefore a V2
migration must include `bodies`, `tx_infos`, and `state_roots` together before
advancing that tail. Migrating only the two large tables would also hide the
unconverted state-root prefix. The state-root segment is small, so including it
has negligible storage cost and removes that unsafe partial-table state.

Local V2 currently preserves the complete prefix and is supported by archive,
full, blocks, and snap modes. Minimal mode intentionally advances a logical
history floor backed by registered cold snapshot segments; startup rejects a
minimal datadir that already contains local V2 coverage until V2 segment-tail
retirement is explicitly integrated with the signed cold-coverage proof.

## Online behavior

After an explicit offline migration establishes V2 coverage for a legacy
backlog, the live freezer promotes at most one newly complete segment per
interval. A fresh store may promote its first segment online. A legacy store
with multiple complete V1 segments and zero V2 coverage is not rewritten
implicitly; it logs an operator action instead.

The live promotion uses the same publish-before-reclaim order and is cancellable
at shutdown. Metrics expose V2 coverage and process-local promoted blocks.

## Acceptance gates

- V1, V2, and cross-tier range reads are byte-identical.
- Checksum corruption is rejected.
- Offline migration resumes from the last manifest.
- Concurrent reads remain available during online promotion.
- Every table remains readable before and after V1 shard reclamation.
- Full rawdb/freezer/cmd package tests pass with the aligned repair and
  virtual-tail implementation.
- Historical block, transaction, receipt, and state-root API probes pass on a
  canary before production V1 source reclamation.
