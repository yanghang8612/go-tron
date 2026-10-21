# Pinned first-change tail summaries and scoped accessor pages

Keep production unchanged. Reuse the immutable mainnet 11,818,192 capture and
account-gc-input; compare against frozen 0b370448 with the same HTTP harness,
seven heights, warm/fresh/private OS-cold cache conditions and oracle.

For owners exceeding the existing full-row request tail cache, retain only the
first post-target change per full latest commitment key. Publish only after a
complete successful repaired-owner scan. The cache belongs to one pinned view,
has exact block/transaction bounds, shares the existing 8 MiB/16,384-item/16-owner
retention limits, and returns detached rows. Do not infer narrower transaction
windows from an out-of-window first row: legacy TxNum ordering can be nonmonotone.
Early stop, cancellation, corruption, unknown key identity or budget overflow
must preserve the original fallback and never publish absence. Generic history
iteration still returns every row, including positive sequence repairs.

The measured plain .kv accessor repeats 8/12/36/120-byte header reads on the same
open descriptor. Buffer at most eight 4 KiB physical pages for that descriptor,
coalescing small reads but sending larger reads directly to the source. Keep
all existing dictionary, header, frame and posting validation. No page crosses
an open-reader lifetime or file identity; close clears pool ownership and closes
the descriptor. Cover short/error reads, cancellation, bounds, cross-page reads,
concurrency, descriptor closure and physical corruption on a fresh open.

Run rawdb/state/snapshots correctness, race, vet and native Sapling builds;
recompute the fixed current MuHash, execute real cold history restore windows,
compare full HTTP values and delays, and retain source/inode/trace evidence.
The first frozen trial was neutral in CPU and regressed OS-cold 9M: aligned
physical pages split unaligned accessor pages and overread tiny headers. Replace
those with requested-offset windows and a 128-byte accessor prefix; apply 4KiB
prefix windows to segment physical headers. Retain the same per-open bound.

The real request scans the same 662-block hot tail separately for the system and
contract owners. Register those owners without I/O; a necessary full scan may
collect all planned owners within the shared budget. Repair rows override by
sequence before owner selection. Incomplete scans publish nothing. Unknown,
unplanned and over-budget cases keep their original authoritative paths.

The excluded cold-open diagnostic attributes 200 of 208 source opens to one
ContractStorage lookup. Extend the optional authenticated routing object with
owner/AccountID, generation and domain membership (31-byte canonical prefixes),
deduplicated across slots. This only excludes pairs having no changes for that
prefix. Positive slots still use the complete owner/key and existing codec/CRC
checks. V2 must not reuse a V1 source column without rechecking its dictionary;
V1 absences never exclude storage. Partial coverage, replacements and budget
limits fall back to ordinary reads. Strict verification reconstructs membership.

Build V2 in a derived view of the same frozen canonical input. Prove unchanged
canonical references, hashes/inodes, HEAD and progress. Compare the old reader
on V1 against the final reader on V2; report the combined code/index change
explicitly. No production mutation, canonical format, pruning watermark or
MuHash input changes. Recompute MuHash and restore using the final native build.
