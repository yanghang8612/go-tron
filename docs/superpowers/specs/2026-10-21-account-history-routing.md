# Account history routing candidate

The first exact preflight index reduces the measured warm 3,144,020 call from
about 2.74 seconds to 0.78 seconds. The excluded per-query diagnostic confirms
that contract 41a4997124a328a4615eab437cede31da47a31b375 has no future envelope
change but visits 4,223 segments: 441 ms for the account batch, compared with
46 ms for the 141-key preflight batch and 1.2 ms for the demanded slot. Use a
general derived account-key upper-bound index rather than expanding the small
system-key dictionary or changing canonical state.
Production publication, restart and deletion remain unauthorized.

Index every account-envelope and generation dictionary key (fixed 21-byte
accessor identity) to the largest covered source's inclusive ToTxNum. A missing
key, or a bound below the request's lower TxNum, proves no matching change in
covered pairs. Uncovered or replaced immutable pairs must use the existing
reader. Bounds are conservative: a larger bound causes extra reads, never a
false absence. Keep full owner matching in the authoritative positive reader.

The bounded builder authenticates each newly indexed accessor and its history
header/dictionary pair, then walks unique dictionary keys. It never scans or
rewrites canonical posting rows. An external sort of (key, source upper bound)
produces each key's maximum independently of source/replacement ordering. Merge
that sorted delta with the already sorted old table in one stream; do not sort
the growing old table on every refresh. Removing sources may retain conservative
overestimates, but only exact current pair identities receive negative proofs.
Add at most 64 new pairs and eight million source dictionary keys per heavy-work
lease, with cancellation before atomic publication.

Use a separate first-class manifest family and a comparable string field for
the metadata authentication root. The file consists of a bounded metadata
header, complete covered pair IDs, a root directory of first-key/page-SHA256
entries, 4 KiB inner directory pages and 4 KiB data pages with fixed
(key, upper-bound) records. An inner directory covers 77 data pages; a data page
holds 141 records. The root binds the header, coverage and root directory.
Every query reads and hashes CURRENT root metadata before using a cached
decoded view, then authenticates each used inner directory and data page. Retain
no cross-request page bytes or descriptors. This keeps authentication cost
proportional to a small root and the used pages instead of the full account
table, while rejecting same-inode/size/mtime corruption in used pages. Physical
whole-file SHA256 remains mandatory for build and strict snapshot verification.

Validate duplicate identities, key ordering, count/size arithmetic, directory
first keys, record bounds, source coverage, short/empty streams, cancellation,
FD ownership, partial coverage, source replacement, generation/owner aliases,
and page corruption. Strict snapshot checking reconstructs every covered
account key from authoritative dictionaries and requires its stored bound to
cover that source. Compare full HTTP calls on the unchanged fixed head and
oracle, with warm, fresh-process and private OS-cold runs. MuHash and executable
snapshot restore checks remain required; auxiliary rows never enter MuHash or
advance hot-prune/cold-build frontiers.

The fixed-copy build retained 64 intermediate generations totaling
3,120,267,917 bytes while the newest table was 94,374,995 bytes. Add a separate
bounded retirement path for unpublished auxiliary routing objects. It must
never select canonical paths, before-images or recovery sources. Require exact
constructor paths, file class, current/retained-generation absence, a physical
retirement lease, publication serialization, stable regular-file/root/parent
identities and no migration journal. Keep missing retired metadata for the
existing metadata sweep. Inspect at most 256 refs and unlink at most sixteen
objects per pass; honor RetainObsoleteSegments. Protect immutable published
generations with at most 64 directory entries and 128 MiB of bounded metadata
reads; defer on legacy, malformed, oversized or incomplete catalog leases.
The signed catalog publisher must pin its source files for its complete
verification/publication interval, closing a publication-versus-retirement race.

Test reclamation on a separate copy with independent, exclusive old auxiliary
inodes. Preserve the measured corpus and every canonical input; verify exact
removed file sizes/allocated blocks, unchanged manifest bytes and canonical
identities, full final-binary MuHash, HTTP values and history restore. Primary
query timings remain labeled with the frozen measured reader revision.

The final excluded diagnostic isolates all remaining account cold visits to the
omitted eth_call caller (the empty address). Do not eagerly prefetch that optional
caller; keep non-empty callers and demand reads unchanged. This changes no state
commitment or absence semantics. Compare the final reader on the same reclaimed
fixed copy under all three cache conditions before accepting performance.
