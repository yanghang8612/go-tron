# Retained V11 accessor rollout

The user now requests deployment and migration, superseding the earlier
offline-only publication hold. Retain all old files; do not import the other
chat's experimental routing/tail changes. Production remains on3e456e75 and all
64 sealed source trios are still active at preflight.

Deploy only the six frozen accessor reader files plus their focused tests and
a bounded adoption command. Reuse the existing guarded mainnet release manager
and preserve its source/binary pin, rollback path, service identity, resource
limits and ports. Build and test before any restart. Publish the tested release to master only after the isolated native build
and checks pass, because the existing deployment timer follows master.

Start with one already converted and fully compared V7 -> V11 sample. An
operator-provided adoption plan contains exact source trio refs and the sealed
candidate accessor ref. Default is read-only. Admit at most four trios,32MiB
per input file and4million postings per accessor. Verify full file SHA/size,
dictionary/key IDs/counts and every posting tuple again. Bind range, record
count and history dictionary commitment. No format change or history/index
rewrite is needed: install new immutable companion aliases and copy the V11
accessor under their common stem.

Apply/rollback require the normal datadir's exclusive Pebble lock with the
node stopped. Keep before/after manifest bytes and a durable journal outside
the snapshot directory. Check manifest identity at publication and retain all
outputs on uncertain rename/fsync outcomes. Resume must recognize exactly the
before or after manifest and refuse unrelated changes. Never replace a newer
manifest with the old whole manifest during rollback: reverse only the exact
adopted trio refs, preserving current HEAD/progress/range/other refs.

Old trios stay on disk and get hardlink backups outside lifecycle GC. They are
recorded in the adoption receipt rather than enrolled for automatic retirement;
reject selected sources already queued in Retired. New unused aliases are also
retained on rollback. No deletion or reclamation command is part of this tool.
Test retention after normal retired GC, crash/resume, conflicting manifests,
forged/corrupt candidates, path/symlink escape and rollback after unrelated
manifest progress. The migration is a small sealed-batch installer, not an
unbounded full-library transcoder.

The known format tradeoff remains explicit: expanded cold point reads are
slower. Do not claim the storage saving has already freed space. Capture live
API baselines, verify fixed historical calls, HEAD advancement and no new read
errors after the reader release and after the canary. Stop expansion and reverse
the canary on correctness errors, timeouts or a severe live regression (over
both2x baseline and baseline+2seconds); keep the reader rollback available.
No full MuHash or full database copy is required for this reader-only change.
