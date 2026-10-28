# Contract metadata history capture and bounded negative routing

Production remains untouched. Work only in this checkout and the retained
immutable cold catalog. No full chaindata/history consistency copy exists;
report this limitation for complete HTTP comparison and current-state MuHash.

Fix SetContract through the ordinary generic KV mutation journal, before the
transaction's history flush. Use canonical encoded bytes as immutable preimages,
not the possibly caller-mutated GetContract pointer. Keep latest physical state
and MuHash elements identical. Creation, updates, no-ops, in-place mutation,
nil metadata/ABI deletion, transaction rollback and generation replacement must
retain their respective history semantics. This correction cannot reconstruct
missing old canonical changes: those require historical execution, never a claim
that rebuilding an auxiliary index repaired old history.

Add independent immutable metadata-filter pages, each containing at most 256
exact history/accessor pair identities. For each fully authenticated dictionary,
hash all full physical ContractMetadata keys (owner, generation and arbitrary
logical key) into a bounded Bloom filter. An empty domain has an empty filter;
positive filters can only request extra authoritative reads. Missing coverage,
unsupported formats and replaced sources remain unknown. No positive filter
result is an answer; only a negative may eliminate that exact source.

Use a fixed 1,024-byte/6-hash filter for each nonempty source (zero bytes for
an empty source). Hash SHA256 with the versioned full-key encoding. Saturation
is conservatively positive; it never requires truncating the scanned key set.
Bound filter bytes and page bytes, not global contract/key counts. Every page
is directly referenced by the manifest and authenticated in full against its
checksum. Validate counts, encodings, pair uniqueness and ranges. No indirect
files or shared mutable source table. Incremental publication retains completed
pages without rebuilding them; overlapping replaced pages are retired using
existing reader/publication protections. Strict import independently scans full
dictionaries and compares reconstructed filters exactly. Damaged referenced
pages fail closed. Partial or cancelled scans never publish absence proofs.

Prioritize these pages for metadata-only broad queries so a covered negative
does not read the old 67 MB singleton. Preserve fallback on uncovered pairs.
Canonical cold rows, Seq, refs, frontiers and history restoration remain unchanged
by the auxiliary optimization. Test both empty and nonempty metadata sources;
old production's missing history must not make the fast path appear sufficient.

Total I/O can grow with source count; paging bounds resident bytes and each read,
not all future negative-query I/O. Nonempty source density and Bloom saturation
must be measured after capture is corrected. A trusted old full route can migrate
an identical empty-family decision for exact covered pairIDs; independent strict
verification must still rescan their complete dictionaries before acceptance.

Measure the complete existing 141-query frozen cold batch and its metadata
scope, with exact returned-row parity. Distinguish warm, process-first and
verified OS-cold samples. Do not drop production pages or use drop_caches.
Run meaningful state capture/history, snapshot strict/signed restore, MuHash,
race, vet and build checks before reporting readiness.
