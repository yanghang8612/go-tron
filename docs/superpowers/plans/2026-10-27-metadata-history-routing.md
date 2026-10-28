# Implementation and verification

1. Reproduce SetContract journal-history omission with the real stage and a
   historical reader, then route metadata changes through owned generic KV rows.
2. Add bounded source-filter page encoding, authenticated dictionary building,
   exact independent strict verification and conservative read integration.
3. Integrate manifest publication, bounded maintenance and auxiliary retirement.
4. Cover positive metadata, arbitrary keys/generations, cancellation, corruption,
   replacement/partial coverage, rollback and restoration; run safety checks.
5. Build and strictly verify pages on the retained frozen cold catalog. Compare
   identical full-row query outputs and native CPU/I/O/performance; preserve raw
   evidence. State the missing full-state HTTP acceptance condition explicitly.

Implementation and local checks completed. The actual-stage history omission,
typed/generic writer ordering and missing read dependency were reproduced and
fixed. Direct Apply also rejects sticky before-image errors. Full core, state,
rawdb, JSON-RPC, actuator and VM packages; focused race; vet and build passed.
Snapshot product sources did not change after their full/race checks.

Native fixed-catalog verification completed: 29,658 sources independently fully
scanned, 116 pages verified; 464-page/default-64 layout has byte-identical source
filters. All 141 query rows match. Three 20-block restoration windows match full
row hashes including Seq (22,496 rows). Final native state/core checks match local
product hashes. Private-file cold samples are rerun after independent coverage
audit rejected the first incomplete 4-sample series; both accepted layouts have
complete actual-open/mincore coverage. The 777-member master archive is downloaded
and independently verified locally. Completed private timing catalogs were removed
only after acceptance, releasing 81,049,739,264 B allocated private-file space.
Production identity is unchanged; live sync/balance/transaction checks pass.

The complete HTTP comparison, full current-state MuHash and complete-state restore
remain blocked by the explicitly absent consistent full-state copy. The cold-query
measurement does not satisfy those acceptance conditions. No production changes.
