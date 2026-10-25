# Opt-in automatic record columns with preserved CDC

The fixed-copy V5 experiment is still running against its frozen source at
`/Users/asuka/.codex/worktrees/cold-throughput-followup/go-tron`. Do not modify
that source or its remote acceptance inputs. This separate worktree prepares a
writer-policy option; its results do not replace pending V5 full-copy, MuHash,
restore, HTTP or publication acceptance. No push, deployment, service changes,
database reset/deletion or resource-budget changes are authorized here.

The synthetic large-Prev counterexample shows explicit `5` bypasses the existing
CDC choice. Add explicit `auto-columns`, leaving the default `auto`, empty/legacy
selection and numeric formats unchanged. The full bounded V6 builder observes
exactly the same effective records during its existing dictionary-key pass and
uses the existing bounded large-value policy. CDC-qualified inputs select `3`;
other inputs select `5`. Do not add another Prev scan, retain Prev values, change
thresholds or enlarge decoding/cache units. Ordered, borrowed, pipelined and ETL
fallback paths must use the resolved policy and preserve canonical bytes.

Automatic merges inspect the same small source headers as `auto`. If any input
is CDC, keep `3`; otherwise `auto-columns` chooses `5` while `auto` still chooses
`2`. Cover mixed 2/3/5 inputs and subsequent merges. No extra value scan or change
to source authentication, publication, pruning or reference-container admission.

An unresolved stream factory has no logical rows from which to infer CDC. Like
existing `auto`, it must conservatively retain V2 for legacy/unannotated callers;
the bounded V6 builder and merge resolve the new policy before invoking it.
This avoids representing unannotated legacy data as compressed record columns.
Migration compatibility accepts existing compatible 2/3/4/5 files as `auto` does;
choosing this emission policy must not itself trigger old-file rewrites.

The runtime CLI/environment and independent bounded diagnostic accept the new
name, with default and precedence unchanged. Reference-container diagnostics
continue to require their explicitly measured default-auto configuration.
No new physical format or reader change is required: this option selects the
already implemented 3 or 5, preserving MuHash elements, values, keys, logical
offsets, ordinals and snapshot authentication semantics.

Validation includes real hot/cold digest and canonical-byte oracles, the
previous large opaque-value counterexample, actual mixed-format compaction,
ETL/fallback and cancellation, environment precedence, and relevant migration
and snapshot restoration tests. Run package checks, race, vet and a build.
Archive local source identity and logs. Native tests and representative writer
costs remain an adoption gate; do not label this policy deployment-ready using
the older frozen binary's acceptance results.
