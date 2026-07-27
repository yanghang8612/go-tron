# Ancient V2 compression — implementation plan

**Spec:** [2026-07-27-ancient-v2-compression-design.md](../specs/2026-07-27-ancient-v2-compression-design.md)

- [x] Add an offline `gtron db benchmark-ancient` command.
- [x] Compare current row Snappy, row Zstd, and 32/64/128/256-block Zstd.
- [x] Report sampled bytes, savings, and projected whole-table sizes as text
  and JSON.
- [x] Run the benchmark against the deployed mainnet freezer and select the
  64-block production frame size.
- [x] Implement a bounded-memory seekable Zstd segment writer/reader.
- [x] Implement versioned V2 headers, frame tables, and CRC32C checksums.
- [x] Add mixed V1/V2 routing and a bounded byte-size frame cache.
- [x] Add an offline, resumable, per-segment migration command.
- [x] Add cancellable online promotion for newly complete V1 segments.
- [x] Install each online V2 read view before reclaiming its V1 source.
- [x] Coalesce concurrent frame misses and cache parsed record offsets.
- [x] Add frame-size and decoder-concurrency safety bounds.
- [x] Remove derivable TransactionInfo IDs from newly frozen rows.
- [x] Pack transaction ordinal into the existing 8-byte reverse index value.
- [x] Restore IDs for by-transaction and by-block historical API reads while
  retaining legacy-index and legacy-row compatibility.
- [x] Add an atomic, resumable rewrite for existing V2 tx_infos segments and
  upgrade their legacy reverse indexes before manifest publication.
- [ ] Verify historical block and transaction receipt APIs before deleting V1
  source shards.
