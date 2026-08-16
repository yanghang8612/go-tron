# Fresh-genesis archive storage profile

This profile is the default for a new mainnet node running
`--prune.mode snap`. It deliberately optimizes the immutable archive as one
coherent generation instead of carrying an in-place upgrade path for older
snapshot layouts.

## Immutable formats

- Ancient V2 `bodies` use a 64 KiB per-segment raw Zstandard dictionary. The
  first frame is independently compressed and supplies the dictionary bytes;
  every later frame is independently checksummed and dictionary-compressed.
- Ancient V2 `tx_infos` retain the canonical receipt protobuf, but omit logs
  when doing so makes the row smaller and the complete block range is already
  covered by an authenticated event-log segment. Reads reconstruct and verify
  block hash, transaction hash, transaction ordinal, global log ordinal, log
  count, address and topic widths before returning the public protobuf.
- Event logs use the `gtevlg4` main segment and `gtevlx2` external lookup
  sidecar. Topics are dictionary IDs in the payload, and lookup keys/postings
  use front coding plus delta varints.
- State history V7 uses 128 KiB seekable Zstandard pages with key-oriented
  history/accessor/posting files.

Event-log V3/V1 data is intentionally rejected. Do not deploy this profile on
an existing snapshot directory. Stop the node, move or remove the exact data
directory, and sync from genesis.

## Retention and publication ordering

Snap/archive mode keeps 65,536 hot state-history blocks by default. Verified
cold history remains permanent; pruning never advances past published cold
coverage.

Receipt logs are externalized only after the event-log manager reports the
entire target Ancient V2 segment covered. Publication order is therefore:

1. publish and authenticate the event-log segment;
2. write and verify the Ancient V2 segment;
3. publish the Ancient V2 manifest;
4. remove the now-covered hot block/receipt rows.

Crashes before a step leave the older self-contained representation readable.
Readers fail closed if an external receipt cannot be matched exactly to its
immutable event-log rows.
