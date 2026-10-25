# V10 shared owner key pages: offline evidence

These are results from `/data/gtron/history-key-owner-20261025-01`; no production
push, restart, reset, deployment or database mutation was performed.
Implementation commits: `f42f53ae`, `6a96f60c` (local only).

Run `python3 docs/dev/evidence/20261025-history-key-owner/verify.py` with Python
3.9+ from this worktree. It verifies all compact members, recomputes space and
HTTP resource summaries, joins the prior immutable V9 SHA receipts, checks the
actual block ranges, MuHash/restore receipts, and the 1,077 production source
hashes against the current worktree. Later source changes intentionally break
that final source-identity assertion; use the audited implementation to repeat.

- `corpus-files.jsonl.gz`: all 19,949 immutable V9-to-V10 conversions. Every
  canonical expanded key/order/ID/metadata is compared; unchanged V9 postings
  and shared offsets are copied byte-for-byte. This does not claim a second
  decode of all 14.3 billion unchanged tuples.
- `end-output-checksums.json.gz`, `space-final.json`: all candidate files fsynced,
  then complete source/candidate SHA rereads and final allocated-block counts.
- `complete-input-files.jsonl.gz`, `complete-input-space-final.json`: all 4,609
  accessors of the full 11,818,192 fixed capture; separate file lengths and
  post-fsync allocations. Candidate files have independent source/candidate
  inodes. `complete-input-routing.json` binds the rebuilt routed memberships.
- `http-*`, `server-requests.jsonl.gz`, `cold-residency-proofs.json`: all 286
  responses (238 normal timed cases) from the unchanged full HTTP host. Seven
  heights, two arms, warm n10 / process-fresh OS uncontrolled n3 / private Linux
  OS-cold process-fresh n4. Startup excluded; retain every outlier. Private files
  are SHA verified and independent of source inodes; canonical private files can
  be reused across arms. No global page-cache eviction.
- `query-trace-summary.json`, `traces/`, `profiles/` and
  `nine-million-*-cpu-top.txt`: single-request file
  reads and separately timed CPU profile workloads, not mixed into normal
  latency samples. Cumulative CPU percentages overlap and must not be summed.
- `muhash-census.json`: final V2 full canonical recomputation on the complete
  capture, with unchanged root. Source-row counts include invalid generations.
- `history-restore.json`: three 20-block windows actually written via
  `RestoreStateDomainHistory` into an isolated memory DB and read back through
  rawdb; 22,496 rows and all 60 canonical block ranges match. This is not a full
  node export/import experiment. Full package tests also cover snapshot restore,
  commitment-boundary rejection and atomic fresh-restore publication.
- `native-status.json.gz`, `native2-*.log`, `local/`: final reader/build identity,
  full five-package tests, focused race, vet, build and local real-file review.
- `native-v1-status.json.gz` / `native-v1-pipeline-status.json`: previous loaded
  converter binary used the identical output encoding. Its isolated partial
  diagnostics were stopped for the read-only directory batching refinement.
  Final MuHash/restore/HTTP used V2; do not label the interrupted V1 diagnostic
  as a production integrity error. Both real-file receipts are byte-identical.
- `harness/` and `scripts/`: auditable opt-in harness sources and experiment
  orchestration, not automatically run against production. `compare-runtime-patch`
  only supplies Go pprof environment variables and preceded all timing.

The 19,949-file cohort ends at height 33,248,191. The 11,818,192 complete capture
is the HTTP dataset. Height-group byte distributions are current immutable
cohort distributions, not historical filesystem snapshots. Owner counts are
per-file entries, not chain-wide unique account counts. Mixed compressed bytes
cannot be attributed to AccountAssetV2 from logical key counts.

The full downloaded ZIP receipt and SHA are in `archive-receipt.json`; every
member checksum was verified locally before producing this compact subset.
