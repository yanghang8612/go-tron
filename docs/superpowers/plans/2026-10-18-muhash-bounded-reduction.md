# MuHash bounded reduction plan

1. Preserve2ca9 baseline/protected28 and old-profile dissection in ignored
   build/benchmarks/20261018-sync-signature-muhash-cold. Coordinate file ownership
   with the independent cold-maintenance implementation.
2. Record fixed old full-primitive and legal delta-chain benchmarks before
   modifying production. Keep signature-path memo/native behavior unchanged.
3. Implement fixed two-fold math/big product reduction with owned scratch and
   single-subtract mapping. Prove boundaries/random modulo equivalence,
   strictcodec, normalized-root and clone/Finalize ownership behavior.
4. Repeat identical benchmarks; retain real regressions and reject a change if
   scratch/setup overhead erases full-operation gain. No speculative goroutines.
5. Combine the explicit reviewed files with cold-maintenance changes, protect
   all28 WIP paths and freeze one exact candidate tree. Run required full
   Sapling/race/native checks once coordinated with Root. Commit/push ordinary
   in-place release only after independent Astra and Root/timer-lock gates.
