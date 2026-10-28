# First historical query: account coverage and scheduling

The continuing objective is fast first cold historical eth_call, across relevant
historical heights, with correct values and unchanged canonical state semantics.
Production 52aea62e still takes 23 seconds at novel 10M/11M cuts. Do not treat a
warm repeat or an isolated metadata benchmark as completion. No further master
publication or production restart is authorized by this continuation.

Fresh read-only stacks show two separate phases: cold accessor/metadata reads
while prefetching account envelopes, then a several-thousand-block unindexed hot
tail while prefetching dynamic properties. The live manifest census authenticates
route source tables and computes the actual current history/accessor pair IDs:
34,542 sources, 1,521 missing account coverage, 53 missing small-system coverage,
8,943 missing broad coverage. Only one previously covered identity is stale;
accessor migration does not explain all these gaps.

First quantify the account gap on the same fixed H45,836,827 copy, comparing a
complete account route with a conservative derivative that removes coverage for
1,521 late sources. Keep account upper-bound rows unchanged (overestimates remain
safe), shrink/re-authenticate its source metadata and full object, and only edit
a private auxiliary object/manifest. Do not alter canonical history, hot indexes,
MuHash, or production caches. Compare complete HTTP values and fresh-process
latencies; these OS-cache-uncontrolled results are not controlled cold-page tests.

The four maintenance families must get independent opportunities. Current
small/metadata success returns before broad/account work, so busy small/metadata
backlogs can indefinitely starve account coverage. Use a persistent cursor across
small, metadata, account and broad, advancing after the actual success or error.
No-work families yield to the others in the same pass. Preserve each family's
existing work budget, publication proof, error propagation, recovery and gate.
Test real manifest publications while higher-priority families still have debt,
as well as continuous producer fairness and errors/no-work admission.

This scheduler fix is not the final query optimization. Existing account refresh
rewrites a 352 MB / 11.97 million-key base for at most 64 new source dictionaries.
Next evaluate bounded delta account tables with per-object, per-query proofs and
explicit atomic replacement on delta compaction. Do not collapse those proofs to
one max bound if that discards useful per-object absence. Limit total metadata,
objects, source IDs, dictionary bytes/time and active descriptors. An unsupported
early source must not starve later sources. Preserve exact pair identities;
unknown, stale, unqueried or budget-excluded objects never prove absence.

Prioritize fixed-copy performance and identical results. Do not repeat full-state
MuHash scans or whole-node restore loops for this read/scheduling iteration.
The unindexed hot-tail phase remains an independent required optimization.
