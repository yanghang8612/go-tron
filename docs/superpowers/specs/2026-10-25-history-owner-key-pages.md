# History owner key pages (offline V10 candidate)

User scope: compress repeated account and AccountAssetV2 keys; measure actual
whole-file savings on the same frozen 19,949-file V9 cohort, and full HTTP
eth_call warm/fresh/private OS-cold behavior on the complete 11,818,192 capture.
Verify canonical MuHash, history values, strict import, and actual restore.
Do not push, deploy, restart production, or alter production files/budgets.

Existing front coding already removes adjacent common prefixes. A 95-file
layout trial finds cross-flat-domain owner reuse worthwhile, but some 128-key
pages refer to dozens of owner pages. Therefore use an accessor-local immutable
owner dictionary (no new manifest dependencies), capped at 1,048,576 owners,
with 128 owners per page and a bounded directory. Preserve logical key bytes,
sorted key IDs, dictionary SHA, before-images, V9 posting/offset encoding, .seg,
.idx, and all canonical state/MuHash formats.

V10 retains V9's 120-byte header, logical first-key directories and framed
postings. Each key page begins with a tag: 0 = ordinary V8 canonical keys;
1 = the same prefix/metadata encoding with each 20-byte owner replaced by a
sorted global uint32 owner ID. Only use references when the key page requires
at most four owner pages and its compressed representation is smaller. All
canonical keys are reconstructed before returning them. Existing V7/V8/V9
readers remain explicit. Small/unsupported/oversized dictionaries fall back to
V9, and select V9 for an entire output if V10 including dictionary/footer is
not smaller; compare two bounded scratch streams, never count gross removal
as net savings.

Append the owner dictionary after the complete V9-style prefix (including its
offset footer), then a V10 footer. Footer binds prefix size, owner count, data
length, directory length, fixed page/entry sizes and CRC. Entries bind page
index, stored offset/length, decoded length and stored payload CRC; page
payload front-codes fixed 20-byte sorted owners with optional existing Zstd.
Validate all arithmetic and allocation limits before ReadAt/DecodeAll. Batch
selected directory entries and adjacent payload pages within 64 KiB; caches
are local to a key-page decode. Never trust data from another source/generation.

Full verification must cover owner dictionary ordering, page CRC/bounds,
coverage, key dictionary commitment and authoritative .seg tuple identity.
Forge an owner page while recomputing CRC/file SHA and require the existing
joint semantic verification to reject it. Test 127/128/129 owner boundaries,
page fallback, whole-file fallback, key/prefix/by-ID queries, truncation,
overflow, cancellation, streaming build/compaction and restore/export/import.

Keep original timing samples, regressions, and outliers. Native candidates use
the same compiler/harness/canonical state; no global cache eviction. Account
and AssetV2 compressed bytes cannot be attributed from logical bytes alone.
