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
- [x] Add mixed V1/V2 routing and a bounded 16-frame cache.
- [x] Add an offline, resumable, per-segment migration command.
- [ ] Verify historical block and transaction receipt APIs before deleting V1
  source shards.
