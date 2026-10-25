# Continue small-page record-column compression

The thread goal remains active: continue material database compression while
preserving MuHash, history and snapshot recovery. Production push/deployment,
start/stop/restart, deletion/reset and budget changes remain outside this work.
This plan is not a completion claim; unchecked items require new evidence.

## A. Reversible page screening

- Implement bounded 128 KiB HRC page columns and all three key-ID controls.
- Preserve prior unsuccessful account-delta and HRC1 SHA256 trials.
- Compare the same 14 immutable inputs, all page bytes and complete projected
  header/table/footer/mode costs; distinguish projected size from fsync/allocated
  measurements. Keep both fixed-nine and mature-five cohorts separate.
- Measure full Zstd plus reconstruction on eight identical pages, with reused
  buffers and alternating order. Profile extra CPU before integrating.
- Validate HRC2 page CRC32C plus unchanged whole-file/canonical-stream SHA256;
  do not remove the existing authenticated manifest/companion protocol.

## B. Candidate physical container (implemented; native acceptance pending)

Local implementation and five-package/race/vet/Sapling checks are recorded in
`docs/dev/history-record-columns-container-20261026.md`. Physical version 5 is
explicit-only. Whole fixed-capture and HTTP acceptance below remain pending.

- Allocate a new explicit physical version/footer (proposed 5 / gtcend05);
  recognize it in all reader, import, inspect and format validation paths.
  Retain explicit versions 1–4 and leave auto/default emission unchanged.
- Add stream annotations for every complete V6 record header, independent of
  domain. Include direct, borrowed and streamed writes, fallback ETL, compaction,
  prefix retention, final-page and Reset/cancellation paths. Cross-page headers
  and payloads remain literals. Do not retain borrowed owner/value buffers.
- Start with the measured best key-ID encoding; charge discriminator and
  directory costs when selecting plain fallback. Evaluate adaptive modes only
  against an actual compressed-size/cold-publication CPU control.
- Keep the old per-reader cache byte budget and 128 KiB independent read unit.
  Bound transformed Zstd output before allocation, validate every column and
  malformed varint/overflow/truncation/unused bits, and preserve exact canonical
  output with no dependencies on another page or file.
- Add a streaming offline trio converter with full source physical SHA checks,
  complete virtual byte comparison and strict semantic reconstruction. Retain
  original whole files when conversion is unhelpful. The API must not publish,
  delete, change frontier or modify source inodes.

## C. Whole fixed capture (pending)

- Use original V11 fixed capture head 11,818,192 / TxNum 601,386,177, not the
  failed 1 MiB account-delta candidate. New independent lab, retained source
  trios, copied/rebound byte-identical .idx/.kv and authenticated route membership.
- Preflight disk/memory and reserve headroom. Clone immutable files with hard
  links where safe; replace new candidate inodes rather than writing shared
  files. Never reclaim other tasks' or production data to fund the experiment.
- Convert all 4,609 files. Use ordinary 128 KiB same-boundary controls;
  the original immutable files qualify only after proving identical compressed
  frames/boundaries. Materialize controls when that equivalence does not hold.
  Rehash all baseline/candidate references and controls after fsync, reconcile
  file lengths, unique st_blocks, manifest bytes and directory du separately.
- Recompute full MuHash with before/after immutable chaindata fingerprints;
  prove candidate binary/source identity. Repeat actual historical restores at
  3,144,020 / 5M / 9M and relevant snapshot import/restore/commitment boundaries.
- Run package/race/vet/Sapling checks appropriate to the integrated reader,
  streaming writer, compaction and recovery changes.

## D. Full HTTP and decision (pending)

- Freeze old/new binaries on the same input and resource budgets. Reuse the
  previous formal HTTP host and seven height/oracle cases; warm/fresh/private
  OS-cold alternating comparisons. Check all values and per-host identities.
- Keep census/rewrite jobs out of timing. Use only independently SHA/inode
  verified private read sets for cold fadvise/mincore, never global drop_caches
  or production inodes. Retain all distributions and resource samples.
- Trace/profile separately to attribute remaining hot-history, route, LSM and
  column-decode costs. Page-only CPU results are not HTTP acceptance.
- Report actual complete net space and all regressions. Keep default disabled
  if space/performance is not acceptable. No push or rollout in this phase.
- Archive exact scripts, hashes, failures and nonwinning controls; provide a
  local verifier and Chinese findings. Keep the broader compression goal active
  while required integration and acceptance evidence remains incomplete.
