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
If seconds remain at 9M, attribute the remaining I/O before accepting or expanding
this design. No new persistence, index format, pruning watermark or MuHash input.
