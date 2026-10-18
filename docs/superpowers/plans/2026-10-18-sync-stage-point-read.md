# Sync stage point-read implementation plan

1. Capture baseline identity and fixed growth benchmark evidence in ignored
   build/benchmarks/20261018-sync-stage-progress; do not modify older freezes.
2. Add the visible point-tombstone getIter termination and deterministic
   regression/real-Pebble correctness tests. Reuse existing GetCursor boundary,
   snapshot, merge, range-delete and error fixtures where sufficient.
3. Route stage-row reads through the established coupled-presence helper and
   prove single-read production behavior plus unchanged plain-reader semantics.
4. Run the identical growth benchmark and relevant normal/race checks; present
   exact diff/evidence to Root and independent Astra audit. No global cache,
   format migration, budget or concurrency adjustment is part of this change.
5. Freeze the explicit candidate files and protected WIP proof; validate the
   exact candidate tree/native dependencies. Commit and normally push only
   after Root release approval; deploy without wiping the current database.
