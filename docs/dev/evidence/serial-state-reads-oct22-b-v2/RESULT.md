# B v2: performance gate passed

Formal Astra independent audit: B module GO (not overall release GO). See astra-audit.md. V1 remains sealed NO-GO in the separate B v1 evidence directory.

Actual parent: `9030ce7a31016491ece014ac76e5656ec149417e`. Fixed ABBAABBAAB, five samples per arm; no samples added or removed. All twelve workload gates pass. Complete materialized-map oracle, focused race, and full blockbuffer tests pass on both arms. Candidate-only sorted-bit mutation and borrowed-input ownership tests also pass.

This is a local overlay plus 512 actual unrelated-Put maintenance benchmark. It does not measure the complete Pebble or SaveCurrentAsBest stack.

| Workload | Parent median ns | Candidate median ns | Speedup | Parent spread | Candidate spread | Full sorts parent -> candidate |
|---|---:|---:|---:|---:|---:|---:|
| 8MiB/cold | 3610820 | 419591 | 88.38% | 0.101 | 0.229 | 16 -> 0 |
| 8MiB/churn | 1060523 | 87113 | 91.79% | 0.125 | 0.112 | 16 -> 0 |
| 8MiB/hotstable | 2478 | 2006 | 19.05% | 0.163 | 0.129 | 0 -> 0 |
| 8MiB/smallprefix | 2227 | 2008 | 9.83% | 0.178 | 0.123 | 0 -> 0 |
| 8MiB/tombstones | 1521174 | 1508096 | 0.86% | 0.046 | 0.036 | 0 -> 0 |
| 8MiB/generic | 2518 | 2506 | 0.48% | 0.196 | 0.083 | 0 -> 0 |
| 128MiB/cold | 76100823 | 12132353 | 84.06% | 0.065 | 0.059 | 32 -> 0 |
| 128MiB/churn | 19064668 | 140760 | 99.26% | 0.059 | 0.216 | 16 -> 0 |
| 128MiB/hotstable | 3764 | 2164 | 42.51% | 0.257 | 0.262 | 0 -> 0 |
| 128MiB/smallprefix | 2201 | 2024 | 8.04% | 0.270 | 0.165 | 0 -> 0 |
| 128MiB/tombstones | 1664437 | 1543501 | 7.27% | 0.103 | 0.098 | 0 -> 0 |
| 128MiB/generic | 4117 | 3627 | 11.90% | 0.036 | 0.153 | 0 -> 0 |

Production-only diff is `production.patch`; candidate source hashes and the identical common test/harness hashes are in `export-manifest.json`. `source-isolation.json` records every file hash and confirms an actual-parent reconstruction with the explicit patch matches the candidate tree byte for byte. Only three B production files and the candidate-only lifecycle test differ; A snapshot/cache changes are excluded. `post-timing-identity.json` confirms source, binary, workspace B hunks and evidence hashes after timing.

Base calls are unchanged: zero Get, one iterator/reverse-seek per ordinary query and 97 per tombstone-chain query. The full allocation metrics are retained in benchmark-summary.json. Cold tiny-allocation counts increase (45 -> 76 at 8 MiB; 77 -> 164 at 128 MiB), while cold allocation bytes fall 526376 -> 7992 and 8392232 -> 82040. Hot typed paths fall 13 -> 12 allocations. Tombstone-chain allocation count increases 19703 -> 19799; its median latency remains within the gate.

No commit, push, server access, or deployment was performed by B.
