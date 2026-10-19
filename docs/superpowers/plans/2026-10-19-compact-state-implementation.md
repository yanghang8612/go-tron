# Compact state implementation

User authorization: implement ContractStorage compact values/keys, then Shielded
change points; an incompatible local format and future fresh-genesis sync are
allowed. Production reset/deployment is not part of this implementation turn.

1. Introduce a canonical self-describing value envelope: ordinary byte strings
   retain tag 1; a 32-byte word with leading zeroes uses tag 2 and only its
   nonzero suffix. Logical readers restore the exact 32 bytes, including a
   present all-zero word. This preserves generic history and source capture.
2. Replace the long latest-KV schema prefix with a short versioned namespace.
   Keep the complete Java slot key: a measured common prefix is not authority
   to alias two different logical keys. Source enumeration excludes raw 32-byte
   trie-node keys that can share a short prefix. Version the commitment scheme,
   element domain and snapshot manifest so old data is rejected explicitly.
3. Update owned staging, before-image capture, iteration, caches, snapshots and
   restore to use the canonical encoding. Keep logical history per slot.
4. Add the Shielded change-point accessors and efficient predecessor queries
   over layered current/as-of state; use canonical execution height as bound.
   Preserve root-keyed tree, note, nullifier and proof records.
5. Test independent full-source MuHash, logical history, unwind/replay,
   publication failure and snapshot restore followed by continued execution.
   Run affected packages, race/vet, then required full build/test checks.
6. Commit reviewable implementation and report actual validation. Do not claim
   a live speedup or activate/reset production without a concrete release step.

Completed: canonical hot/cold word codecs, 66-byte full-identity Storage keys,
sparse Shielded roots, layered reverse seeks, explicit incompatible scheme and
manifest, MuHash/history/unwind/restore continuation tests and validation.
The full Go 1.25.5 suite passed 61 packages; an initial Go 1.27 async timing
failure and successful repetition are preserved with the validation evidence.
Production deployment remains a separate operation; no push or reset occurred.
