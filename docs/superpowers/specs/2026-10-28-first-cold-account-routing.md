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
rewrites a multi-gigabyte base for at most 64 new source dictionaries. The fixed
copy has 115,456,774 rows and a 3,399,129,767-byte base.
Next evaluate bounded delta account tables with per-object, per-query proofs and
explicit atomic replacement on delta compaction. Do not collapse those proofs to
one max bound if that discards useful per-object absence. Limit total metadata,
objects, source IDs, dictionary bytes/time and active descriptors. An unsupported
early source must not starve later sources. Preserve exact pair identities;
unknown, stale, unqueried or budget-excluded objects never prove absence.

Prioritize fixed-copy performance and identical results. Do not repeat full-state
MuHash scans or whole-node restore loops for this read/scheduling iteration.
The unindexed hot-tail phase remains an independent required optimization.

## Account design gate: measure density before selecting the representation

The 1,521-source account-gap ablation adds about 118.8 MB of actual query reads
for 1,490 extra unknown sources (about 80 KB/source). Source object sizes are
not equivalent to query read bytes. The existing small-system routing may
already eliminate the system account; inspect the actual contract envelope
separately. Do not assume that a per-source Bloom analogous to storage routing
will be smaller than the reads it replaces. A 65,536-key filter would occupy
128 KiB before metadata, and whole-object authentication reads that bitmap.

Authenticate all 1,521 unknown dictionaries on the fixed ablation input. Count
account/generation keys and exact membership for the two actual HTTP envelope
keys; generation keys are labeled controls. Record small/broad negative proofs
and exact source identities. Project 16-bit Bloom bytes with a bounded hash
array, identifying excluded dense/unsupported sources. Separately deduplicate
groups of 64 source dictionaries and project paged independent delta tables,
including full root metadata, duplicated initial header and the actual unique
4-KiB directory/data pages for this two-key request. A 2M-key union cap produces
a lower bound, not an exact result; unsupported groups are incomplete. These
are layout/I/O projections, not built-index or HTTP performance measurements.

Each delta must keep its own exact covered-pair set and key upper bound. Never
extend the old base's coverage set and authorize its stale bound for new sources.
New-account and later-than-old-bound positives require dedicated correctness
tests. Select the representation from measured density and remaining query
reads; if Bloom pages exceed the canonical reads avoided, use independently
paged deltas. The previous global-table fair-admission prototype remains
withdrawn; current small/metadata/storage scheduling does not admit that costly
account refresh as another fair family.

## Measured update: metadata replacement loses valid coverage

The 64-source account refresh on the private fixed copy took 58.53 seconds and
wrote 6,978,293,760 physical bytes, so do not deploy the initial fair-admission prototype with
the existing global-table builder. Its tests/review remain as experiment evidence;
the product scheduler is restored pending bounded delta work.

A new exact census found 10,387 metadata sources unknown out of 34,578, despite
229 active metadata objects. The fixed copy has 32,036 metadata proofs (500
64-source pages plus one 36-source page). A new failing test proves that repairing
one replacement pair currently retires an overlapping page without preserving
the other valid proofs. This mechanism can amplify a small number of replacements
into large gaps; the census alone does not attribute every historical gap to it.

Repair must authenticate every range-intersecting metadata page, retain filters
only for current exact pairs within the old page's bounds, and include both the
preserved pairs and new pair in the output range. Preserve proofs before startTx.
If one output cannot hold the union, defer it and advance the cursor; never
truncate the union to meet the 256-row cap. Finish any independent new page before
entering repair, and finish a successful repair immediately. Keep ordinary
retirement/read leases. Publication must reject a changed overlapping metadata
set under the publication lock before using a stale repair view.

Measure old/new actual maintenance with identical synthetic one-ID holes in
private metadata pages on the fixed copy, then full HTTP at 9M/10M/11M/15M/20M.
Use the real refresh cursor, since always restarting from the first missing
source would mask the repeated coverage-loss mechanism. Canonical input bytes,
HEAD, Tx and MuHash stay fixed. No production publication or restart.
