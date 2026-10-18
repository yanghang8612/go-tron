# MuHash local commitment implementation plan

Design: ../specs/2026-10-18-muhash-local-state-commitment.md.
Primary owns common/muhash3072, rawdb/schema/accessors, StateDB/domain capture,
commit engine/worker, genesis/blockchain/unwind and branch rotation cleanup.
Secondary owns snapshots, commands/config/tools and their tests; rawdb interfaces
are frozen together first. Original27WIP and ignored prototype freezes stay exact.

1. Obtain Root/Astra design approval. Freeze StateCommitmentHead/checkpoint API,
   canonical384LE complete state, owned physical delta and atomic durable cut.
2. Implement primitive/state codec and official/randomized reload vectors. Add
   schema/accessors and strict HEAD/checkpoint/element/delta validation; no new
   per-block accumulator or identity family. Communicate compilable API to cold
   implementer before consumer changes.
3. Replace after-only touch collection with write-before physical capture across
   account, generation, all KV fast/normal/delete/prefix paths. Preserve genuine
   before reuse, same-cut pending overlays, ownership and history independence.
4. Replace branch folding with serialized MuHash application in the existing
   worker's layer/publication lifetime. Bind block/parent/tx identity; put complete
   HEAD/latest root/current recovery metadata with source in the same canonical
   flush group. Separate early historical anchors from durable current HEAD.
5. Stage genesis atomically, validate/recover startup from constant HEAD cut,
   remove Java-root/missing-branch compatibility and full scans. Implement actual
   before-read inverse unwind, target-root verification and fork/reset lifecycle.
6. In parallel integrate new snapshot/checkpoint schema and tools via secondary.
   Remove obsolete branch runtime/config/rotation/repair dependencies and stale
   tests only where their behavior was explicitly retired; retain source/history
   and Java consensus-root tests.
7. Run focused normal/race and necessary repository checks. Independently compare
   small complete source-set oracle at every successful stage/reopen/fork; inject
   fold/flush/history/close failures. Verify27WIP hashes/index scope and unchanged
   prototype artifacts. Submit exact diff/test artifacts for Astra/Root review.
8. Stop before commit/push/service actions until Root release authorization.
   Then release approved master artifact and let Root coordinate fresh-genesis
   deployment and live verification. No old database compatibility is supplied.
