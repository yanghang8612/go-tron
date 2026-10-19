# Fresh-state v3 and compact cold indexes

User-authorized scope: incompatible local formats, fresh genesis allowed;
preserve Java consensus/wire behavior, MuHash, historical queries and executable
snapshot restore. No production reset/deployment is part of offline validation.

1. VotesIndex: rooted count, 128-address ordered pages, and rooted per-address
   membership. Append modifies only count/tail/member. Explicit replacement
   retains duplicate/order semantics; clear removes pages/membership and retains
   a present zero count. DeleteVotes does not remove enumeration membership.
   All writes use normal journal/history/commit paths.
2. Generic latest values: tag 3 for exact-eight-byte words with leading zeros,
   alongside existing tag 2 for exact-32-byte words. Preserve present empty,
   both zero widths, negative bit patterns, and reject noncanonical envelopes.
   Cold latest uses a separate flag for eight-byte words.
3. Account core: explicit codec 2/bitmap bit 28 means the address equals the
   enclosing row owner. Omit only an exact 21-byte match. Decode requires owner
   context when this flag is present; absent/different addresses and opaque
   unknown fields retain their bytes. Code-hash-only inspection needs no owner.
   Current, transaction-versioned and historical hydration pass the same owner.
4. Explicit v3 MuHash domain/HEAD and new snapshot version reject prior HEADs;
   no automatic deletion/reinitialization. Full source-fold accumulator and
   digest must agree after commit, unwind/replay and snapshot continuation.
5. Transaction-index v2 keeps 64-bit fingerprints, uses per-bucket relative
   locations for all-packed buckets and raw 64-bit fallback for mixed/plain
   buckets. Width derives from run span (not chain height); CRC covers encoding
   metadata and payload. Readers and merge return the original 64-bit location.
6. History inverted indexes: independent bounded compressed pages over the virtual
   index stream (Snappy default; compare Zstd as an alternative), checksummed header/directory/pages and raw-page fallback. A
   bounded cache serves ReaderAt without changing logical offsets/records.
   Keep the existing inner V7 reader and all historical validation semantics.
   Builders publish the new file atomically, manifests refer to physical bytes.

Acceptance: malformed/truncated/corrupt rejection; no presence conflation;
maintenance/duplicates/revert; full MuHash state and digest; hot and cold as-of;
snapshot import then execution; transaction candidates/collisions/merge; complete
index enumeration and range seeks. Compare actual written file bytes with equal
input/settings and benchmark fixed-budget point/range reads, cold/warm paths.
Use pinned server corpora on independent output paths when available. No claimed
whole-database saving from synthetic data or model-only calculations.
