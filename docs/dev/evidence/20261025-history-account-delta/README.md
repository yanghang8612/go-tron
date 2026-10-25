# Account previous-value bounded delta: evidence

Local implementation commit: `da44554cd6115512fa13f3da5e106ea3a70de9be`.
The format remains experimental and opt-in. It is **not recommended for deployment**:
the 4,609-segment fixed capture saves 7,892,188,970 file bytes, but materialized
ordinary 1 MiB Zstd pages already save 7,891,807,752 bytes. The entire account
column/delta format adds only **381,218 bytes** of net saving. The 9M full HTTP
warm median regresses 24.18%, and private OS-cold startup regresses 5.22%.

Fixed head: 11,818,192 / TxNum 601,386,177. This is not the current production
head or a complete 25M–35M cohort. Four-level base dependencies still require
full independent page decoding. The nine `.seg` files read by the 9M trace have
zero account-transformed pages; the CPU-only eight-page diagnostic attributes
the increase to the 1 MiB independent decode unit rather than decoder Lowmem.
Do not confuse its 8.084× per-page decode cost with full HTTP latency.

The experiment did not push, deploy, delete production data, or start/stop/restart
production gtron. All source/candidate/control files remain in the separate lab
`/data/gtron/history-account-delta-20261025-01`. Production had already stopped
in the preceding experiment; all 122 HTTP host before/after observations and
this final archive observe inactive/PID 0. No production reclamation occurred.

`raw/` stores an exact, lossless selection of 258 original files. Large text
receipts are gzip-compressed; Go code is saved as `.go.txt` to avoid introducing
packages. `http-host-members.jsonl.gz` preserves 612 additional original host
files as path/base64 records, including every request, identity and resource
sample. `original-file-map.json` associates saved names with archive names.
`archive-member-sha256.json` is the full original archive's 905-member SHA map;
all 870 selected originals are checked against it, not against newly invented
summaries. The 35 omitted originals are older intermediate payloads or redundant
artifacts; the complete ZIP retains them.

`derived/` contains independently recomputed space, performance and association
results. `scripts/` recomputes them. `local/` records local logs and the final
archive receipt. The complete ZIP is retained in Downloads and server `/tmp`:

- File: `gtron-account-delta-final-20261025-01.zip`
- Length: 14,539,613 bytes
- SHA256: `bceae43bca71bae1f75a402d11e3855869c7d8ab751e323ea624fe20ba9d94f3`
- ZIP CRC, safe paths, 906 unique members and all 905 content hashes verified.

```sh
python3 docs/dev/evidence/20261025-history-account-delta/verify.py
```

Python 3.9+ and the unchanged implementation's **1,089 Go/module files**
(1,085 Go files, four go.mod/go.sum files) are required. The script verifies the
local evidence inventory and original raw bytes, rebuilds the full HTTP/space
accounting in a temporary directory, checks both manifests and every control
receipt, and compares the source inventory. Source checks intentionally fail
when a later implementation differs; `--source-root` can select the original
checkout. Rechecking original database bytes requires rerunning the saved Go
harnesses against the retained server datasets; receipt recomputation alone
cannot independently reread those 192 GB.

Correctness evidence includes all 205,800,993,161 canonical virtual bytes across
4,609 segments, unchanged companion SHA/offsets, strict routing reconstruction,
full MuHash recomputation at the fixed head, 60 actual historical block restores
(22,496 rows), 286 correct ordinary full HTTP responses (238 timed), and 56
private cold-residency proofs. Related native packages/race/vet/Sapling builds
passed. This does not assert a whole-node 11.8M export/import test or unchanged
cold-publication throughput.

The first ordinary-control footer error and decoder diagnostic borrowed-buffer
error are retained in failed logs/retry receipts; their corrected harnesses and
regression tests passed. They did not alter the accepted HTTP/MuHash binary.
Interrupted serial controls were preserved and full-SHA checked before the
four-worker continuation. See the Chinese report for accounting caveats,
performance tables and proposed directions.
