# History accessor shared offset pages

Offline only; do not deploy, push, restart services or mutate production data.
Compare with V8 on the same immutable accessor cohort and complete 11,818,192
capture. Preserve TxNum, ordinal, dictionary identities, singletons, `.seg`,
`.idx`, canonical state and MuHash. Separately examine the user's observed
25M–35M growth using comparable historical receipts and cold frontiers; the
11.8M HTTP capture does not validate later heights.

Accessor V9 keeps the 120-byte V8 header and key-page format. Multi posting
frames retain TxNum and ordinal varints, omit logical offset varints, and keep
their per-frame CRC and first-TxNum directory. Inline singletons retain their
direct tuple. A shared ordinal-to-offset table follows the posting section.
Each 128-row page stores a 48-bit absolute first offset and positive uvarint
deltas. A 12-byte page entry stores relative payload offset (48 bits), length
(16 bits) and CRC. CRC covers the page index, entry position/length and payload.
A 32-byte footer binds magic, record count, payload length, page size and CRC.
All lengths are checked before allocation/ReadAt; directory coverage and global
offset order are checked by the full verifier. No history-body scan is used to
recover offsets.

Point queries filter TxNum before resolving selected ordinals. A request-local
bounded page cache combines repeated page lookups; no cross-generation cache.
Builders collect offsets in ordinal order using bounded scratch files and
append the locator under the existing physical SHA, fsync and publication gate.
Joint verification compares locator pages against the sequential authoritative
history stream, and joins key/TxNum/ordinal through ETL without randomly
resolving every multi posting. Inline offsets are still independently checked.
Corrupt mappings with recomputed CRC/SHA must fail this semantic gate.

Acceptance includes V7/V8 reader fixtures, 127/128/129 page/frame boundaries,
overflow/truncation/cancellation, streaming/compaction, query result equality,
full accessor space accounting including every new locator byte, fixed HTTP
warm/fresh/private OS-cold A/B, full canonical MuHash and actual restore windows.
Do not call a static byte estimate a measured saving or hide query regressions.
