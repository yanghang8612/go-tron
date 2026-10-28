# Account history routing delta implementation

Evidence: `docs/dev/account-routing-distribution-20261029.md`. The active goal is
first historical HTTP eth_call latency, not just a smaller auxiliary file.
The next product change is independent paged account deltas. No master push,
deployment, production cache eviction or restart is authorized in this phase.

## Storage and proof boundaries

1. Add `account-history-routing-delta`; keep the old account base singleton.
   Reuse the GTACRT01 authenticated metadata/directory/data codec. Allow delta
   range envelopes to overlap; do not relax canonical overlap validation.
   Register the kind for strict manifest/import/checking, signed snapshots,
   auxiliary retirement and read leases. Account delta paths have a distinct
   content-addressed prefix.
2. Extract a writer accepting an explicit kind, exact selected pair IDs and an
   ETL stream. Delta building never opens or merges the base. Keep the old base
   builder only as an explicit offline operation. Refactor without changing the
   existing base byte format or authentication behavior.
3. Each object retains its own `covered` set and each request key's
   known/found/bound proof. Only a current exact pair ID in that object's set can
   use its absence/bound. Never union coverage and use an old base bound. Any
   valid independent negative proof may skip its own covered source; positive
   reads still use canonical Tx/Seq matching.

## Query behavior and budgets

4. Read delta proofs only when existing base/small/broad filters cannot eliminate
   an unresolved account query before a canonical source read. Do not scan all
   later objects while constructing the speculative candidate plan. Preserve
   pending-hit consumption when later sources are negative.
5. Open at most one extra delta FD, compute the current batch's proofs, then
   close it and retain request-local immutable results. Do not populate the
   base's single shared metadata cache with deltas. Limit metadata bytes,
   coverage-map/proof residency and attempted objects independently. Starting
   envelopes to validate are 2 MiB incremental metadata and 128 objects; record
   actual admission under the final build split rather than assuming the
   study's unconstrained 24 groups. Mark a rejected object attempted so each
   later source does not retry it.
6. Missing/corrupt metadata or pages, exhausted budgets and unloaded objects
   provide no absence proof and fall back to canonical. Propagate cancellation.
   Strict imports still authenticate the complete object and validate source
   coverage and upper bounds; query fallback must not weaken those checks.

## Builder, publication and scheduling

7. Bound attempted sources, complete source count, declared accessor bytes,
   dictionary keys, ETL memory/temp disk, output rows/bytes and between-source
   soft work time. Reserve a complete source before recording its coverage;
   avoid partial-source ETL entries authorizing absence. Include sorting,
   output copying and sync in heavy-work measurements; 2 seconds is not a hard
   pass deadline. Dense source counts exceed storage's 65,536-hash cap.
8. Publish through a dedicated append/explicit-input-replace operation, holding
   the build-to-publication read lease and publication lock. Revalidate selected
   current pairs and exact replaced delta refs against the latest manifest;
   preserve its frontiers and unrelated refs. Never auto-retire another delta
   because ranges intersect. Keep published-generation physical protections.
9. Introduce Runner-owned cursor/backoff/metrics and a fourth bounded fair
   family; remove the old account full-table refresh from normal scheduling.
   Unsupported/oversized sources advance scheduling-only progress, not proof
   coverage. Hard errors, cancellation and publication conflicts do not commit
   progress. Keep EOF-hole backoff, scoped replacement invalidation, and the
   existing canonical merge reservation. Actual no-output I/O consumes a heavy
   opportunity rather than stacking another expensive family.
10. Define bounded delta-only consolidation and backpressure before claiming
    sustained catch-up. Never fall back to merging the base. A union bound may
    weaken per-object absence selectivity even when it is safe; measure that
    tradeoff and explicitly replace only the selected input objects. Fixed
    object count must not silently stop all future proof coverage.

## Required verification

- New account absent from base but positive in a delta; a later change beyond
  the old base bound; AccountLatest and KVGeneration separation.
- A key absent in delta A but present in B: A independently skips its own
  sources, B still reads its positive canonical record.
- Same-range accessor replacement invalidates old exact coverage. Overlapping
  append and explicit consolidation retain all intended valid proofs.
- Bad/missing metadata and used pages, request budget exhaustion, cancellation,
  and publication conflict; no false absence or committed cursor on failure.
- Earlier canonical hit plus negative tail does not load all later delta pages.
- Unsupported first source cannot starve later sources; actual bounded family
  fairness and canonical merge reservation under successful/no-output/error work.
- Small ordinary/signed snapshot restore and MuHash regression, targeted race
  and affected package tests. Do not repeat the billion-row source audit.
- Build on the same immutable H45,836,827 input with the 1,521-ID account gap;
  reuse full metadata/storage proofs so the account comparison is isolated.
  Record actual table count/size, build/publish seconds, CPU, read/write bytes
  and cursor progress. Preserve canonical refs and HEAD/Tx/root.
- Interleaved full loopback HTTP old/new at 9M,10M,11M,15M,20M, fixed return
  values, process-first and immediate repeats. Report OS-cache-uncontrolled
  samples honestly; private newly generated cold delta pages can be tested
  separately, without evicting hard-linked production canonical file pages.
  Compare remaining canonical reads as well as added delta metadata/pages.

The live 10M first sample is still 23.65 s. A successful account experiment does
not close the unindexed hot-tail bottleneck or prove live latency. Keep the
overall goal active until those paths and actual production behavior are known.
