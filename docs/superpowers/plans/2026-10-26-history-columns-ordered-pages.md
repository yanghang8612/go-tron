# Continue publication optimization without reducing compression quality

1. Six CPU profiles and exact tops for frozen 0071ef14 are archived and locally
   recreated. Keep that native source and all real-pair outputs immutable.
2. Extend the existing ordered pipeline with an explicit record-column mode,
   copied/bounded annotations and per-worker encoders. Leave V2/CDC behavior
   unchanged. Keep a one-worker wrapper for existing converter/fixture callers.
3. Use writer logical offsets for queued pages, detach winning buffers before
   the next worker encode, and charge all retained capacity. Preserve page
   boundaries and byte selection exactly. Honor the existing factory workers.
4. Add complete-file oracle, ownership, error/cancel/Reset and bounded-capacity
   tests. Run focused, five affected packages, race, vet and Sapling once stable.
5. Freeze and build a new private native source. Reuse the three fixed real
   input pairs and alternate ordinary V2, frozen serial V5, and parallel V5 with
   the same process/resource budgets. Require the same physical/canonical and
   companion SHA proofs. No other heavy job during timing.
6. Accept or reject using actual space and publication cost, retaining failures.
   Finalize full fixed-copy MuHash, complete HTTP and history/snapshot recovery
   with the actual final binary. Keep the broader goal active until required
   work is complete; do not automatically deploy.
