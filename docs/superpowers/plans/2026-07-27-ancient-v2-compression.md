# Ancient V2 compression — implementation plan

**Spec:** [2026-07-27-ancient-v2-compression-design.md](../specs/2026-07-27-ancient-v2-compression-design.md)

- [x] Add an offline `gtron db benchmark-ancient` command.
- [x] Compare current row Snappy, row Zstd, and 32/64/128/256-block Zstd.
- [x] Report sampled bytes, savings, and projected whole-table sizes as text
  and JSON.
- [ ] Run the benchmark against the deployed mainnet freezer and select the
  production frame size.
- [ ] Extract the existing seekable Zstd-block primitive into a reusable cold
  segment package.
- [ ] Implement versioned V2 segment writer/reader with checksums.
- [ ] Add mixed V1/V2 routing and a bounded frame cache.
- [ ] Add an offline, resumable, per-segment migration command.
- [ ] Verify historical block and transaction receipt APIs before deleting V1
  source shards.
