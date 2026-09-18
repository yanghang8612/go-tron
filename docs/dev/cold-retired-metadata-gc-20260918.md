# Bounded retired-manifest metadata GC candidate

Date: 2026-09-18. Base commit:
`cdbd49f865cc98b868675d78721a2c06d16c9dec`.

The production manifest observation motivating this candidate had generation
52,676, 124,088 active refs and 130,290 retired refs. All active files were
present; all retired files inspected by the operational evidence were missing.
The retired array therefore kept increasing manifest bytes and decoded cache
charge after physical cleanup had already finished.

This candidate makes the runtime cold-history Runner examine at most 1,024 old
retired rows immediately before its existing manifest integration. A ref is
forgotten only when fresh `Lstat` reports ENOENT for the leaf, the snapshot root
and every parent directory still exist as real directories, the path is absent
from the current active refs, and no migration/catalog/published-lease guard is
present. The root and all parents supporting a deletion candidate are checked
again by filesystem identity before integration. Regular files, dangling
symlinks, directories, active-path aliases, permission failures and all other
I/O failures remain in Retired. The code never unlinks an object.

The filtered view is folded into the ordinary cold publication. It does not
add a no-op publication. `integrateWithManifest` still assigns generation,
merges new active and retired refs, preserves chain identity and progress, and
runs the existing full validation/fsync/rename/cache-seed path. The old
caller-owned manifest is not mutated. Newly retired refs from the current
integration are first eligible on a later pass.

The process-local cursor belongs to Runner because Aggregator is recreated on
every pass. It stores a `sortSegments` key plus an offset within equal-key rows;
equal keys can have different metadata identities because the global manifest
sort ends at Path. A partial missing group resumes at offset zero after its
rows shift; a present group advances by count. Reaching the end wraps on the
next successful publication, so refs inserted before the cursor are delayed,
not permanently skipped. Cursor progress is committed only after the ordinary
manifest publication succeeds. A failed rename, canceled pass, no-output pass,
or restart retries from the old cursor or from the beginning.

Migration safety is fail closed. `.history-reference-migration.json` presence,
including a dangling symlink, or any error checking it disables the metadata
sweep. This preserves the journal's exact before/after manifest hashes and Old
retired refs. The guard is checked before scanning and again immediately before
publication. Cancellation is checked through the bounded leaf loop, active
membership scan, parent recheck and full filtered-slice construction.

Lease handling is deliberately conservative in this first version. Missing
`published/manifests` or an empty directory is a cheap path with no immutable
manifest decode. Any possible `manifest-*.json`, published-directory error, or
`snapshot-catalog.json` presence disables metadata GC for the pass. This keeps
every retained published active lease, covers legacy catalogs whose
`ManifestPath` is mutable `manifest.json`, and avoids turning per-pass lease
decode into the next fixed cost. Deployments that continuously retain signed
immutable generations will not reclaim Retired metadata with this version.

Runtime cold work is serialized by `Runner.passMu`; the existing reference
migration contract requires the node to be stopped. The double guard is not a
cross-process compare-and-swap. External writers that violate those ownership
rules remain outside the supported publication model.

`PassResult` and the existing publication log expose scanned, removed and
deferred-reason fields. `HistoryMetadataDuration` includes the sweep. No
read-only manifest helper or snapshot CLI gains a side effect, and retired-file
content verification, hot-prune coverage checks, immutable catalogs and
retired-file physical pruning are unchanged.

Tests cover confirmed missing removal; present file, dangling symlink,
directory, active alias and non-ENOENT retention; journal, catalog and published
lease fail-closed behavior; mixed valid/missing parent progress; caller-owned
manifest/progress/active preservation; real manifest rename failure and retry;
cancel during leaf `Lstat`; a journal appearing between scan and publish;
bounded cursor progression; equal-key groups larger than the bound with a
present-to-missing transition; and manifest insertion before the cursor.

Local verification:

- focused correctness tests passed;
- focused race tests passed;
- final full `core/state/snapshots` package passed in 80.508 seconds;
- `git diff --check` passed.

The full-operation benchmark uses 124,088 active and 130,290 retired synthetic
refs. Median publish-plus-load time was 218.669 ms for the original large
manifest, 223.345 ms for the first bounded deletion pass, and 122.947 ms after
the missing retired metadata had been cleared. The first pass has a small cost;
the stable result is the cumulative benefit after 128 successful publications.
Raw output and interpretation are under
`build/benchmarks/20260918-cold-optimization/metadata/`.
