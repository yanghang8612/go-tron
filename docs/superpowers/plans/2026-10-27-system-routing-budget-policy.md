# Routing budget implementation and acceptance

1. Freeze the current small-routing worktree as the comparison baseline.
2. Separate authenticated source preparation and bounded route assembly; add
   adaptive batch selection without repeated dictionary reads.
3. Add Runner-owned scheduling hints, relevant-input invalidation, bounded
   budget backoff and observability; preserve query/import authentication.
4. Add meaningful correctness/race tests and run package, vet and build checks.
5. Build isolated native harnesses from both source states. Reuse the existing
   27,893-source catalog and 11.8M canonical copy, and compare controlled cold
   construction/maintenance CPU and full HTTP cache regimes. Clearly separate
   offline pipeline throughput from sustainable production throughput.
6. Recheck MuHash/history/restoration and seal/retrieve raw evidence. Leave
   production service, parameters, database and deployment untouched.
