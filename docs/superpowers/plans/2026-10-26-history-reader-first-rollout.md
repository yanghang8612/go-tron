# History master deployment

1. Merge current origin/master into the clean candidate and preserve all work.
2. Add and test an opt-in reader-first lifecycle hold; defaults do not change.
3. Validate merged Go source with Sapling, race/vet, nested cursor checks, build.
4. Preserve live release/config/DB identities, capture fixed RPC baseline, pause
   the deploy timer, publish the exact tested source and run the normal release.
5. Start with the lifecycle hold, verify readers and live sync, then clear it
   and restart the same capable binary. Recovery uses that capable binary with
   the hold set once new formats could exist. Do not delete/reset databases.
6. Verify cold progress, queries, errors and resources; restore timer; archive
   the concrete production receipts and report remaining verification limits.
