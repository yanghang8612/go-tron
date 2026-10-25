# V11 shared transaction runs: evidence

Implementation: local, unpushed commit `b6a6fdf508b281f6864f888563e92eaa656be594`.
No deployment, production database rewrite, deletion, or production service
start/stop/restart was performed by this experiment.

There are three distinct scopes:

1. **19,949 accessors**, V10/V9 → V11, maximum height 33,248,191:
   all posting TxNum/ordinal pairs, canonical keys/IDs and locator equivalence;
   final source/output hashes and fsync/allocated accounting. Includes every file; none grew in the measured cohort. This is not a complete restorable node.
2. **Complete fixed capture**, head 11,818,192 / TxNum 601,386,177:
   4,609 rewritten accessors, strict routing and `.seg` joint reconstruction,
   full MuHash recomputation, 60 actual historical restore ranges / 22,496 rows,
   and 238 timed / 286 correct ordinary full HTTP responses. The two storage
   scopes overlap and must not be added together.
3. **Complete [25M,35M] old-value census**, 8,651 immutable `.seg/.kv` pairs:
   5,610,054,750 exactly block-filtered records. Original scanner and full
   per-file receipts are under `census/`. Decoded domain bytes are not physical
   compressed domain shares.

Production was active at preflight. A systemd stop sequence began at
2026-10-25 11:12:51 UTC and completed cleanly at 11:13:43, status 0. The requester
is not identified by the journal. Full HTTP comparisons started later with
production consistently inactive/PID 0. Preserve this observation; do not claim
production remained running or compare this window with earlier loaded runs.
See `production-exit-observation.json` and the before/after identities.

`space-final.json` closes every encoded component, including transaction pages,
directories, footers and key/owner changes. `end-output-checksums.json.gz` records
the final fsync/SHA/st_blocks checks. The full fixed capture has a separate
`complete-input-space-final.json`; canonical manifest companions were also
fully rehashed after HTTP.

`http-requests.jsonl.gz` and `server-requests.jsonl.gz` retain every sample,
including excluded discovery/warmup/trace requests. Profiling requests use a
separate ID and are excluded from timing. `cold-residency-proofs.json` has 56
startup-before mincore receipts, each below 1%; only independently SHA/inode
verified private files were advised. No global drop_caches or production inode
fadvise. Diagnostic syscall traces use fresh processes with uncontrolled OS
cache and are not themselves cold latency samples.

The raw ZIP contains the full scripts, harnesses, original interruptions/resume
receipts and native logs. This directory stores a compact, lossless selection
plus recomputation scripts. Go harnesses have `.go.txt` suffixes here so evidence
does not become a separate Go package. See `local/final-download-receipt.json`
for the locally verified raw archive path, SHA256, CRC and membership counts.

```sh
python3 docs/dev/evidence/20261025-history-tx-runs/verify.py
python3 docs/dev/evidence/20261025-history-tx-runs/census/verify.py
```

Python 3.9+ and the unchanged implementation's 1,078 production Go source files
are required for the first command. These scripts recompute archived accounting
and associations; independently proving original data again requires running
the saved Go harnesses on the retained server inputs. Source inventory checks
are deliberate and should fail when a later implementation differs.

The complete raw archive remains in Downloads and on the server; no production
replacement or physical reclamation has been performed. The offline candidates
therefore consume additional storage pending a separately authorized rollout.
