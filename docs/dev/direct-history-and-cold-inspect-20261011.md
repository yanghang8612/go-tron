# Direct history and cold inspection — 2026-10-11

The overnight node was still running source `653f1fa35737399721230402b84bb99edcc55d1c`.
Between 18:41:44 and 08:01:41 CST, archive publication advanced 430,311 blocks
(8.97 blocks/s); archive eligibility advanced 513,300 (10.69 blocks/s). The
archive lag grew by 82,989 to 2,058,270 blocks. Physical hot storage grew about
40.32 GiB. The previous runtime mover did not meet the hot-storage objective.

At 08:01 CST, allocated sizes were hot 393.70 GiB, staging 7.54 GiB, cold
1,129.56 GiB and ancient 281.36 GiB. These are observed sizes, not forecasts
for the new direct store.

Read-only cold file/header census at 08:18 CST, manifest generation 83,196:

| Active family | GiB |
| --- | ---: |
| State history CDC outer V3 | 375.20 |
| State history R1 | 253.70 |
| State history outer V2 | 198.30 |
| State accessor V7 | 158.26 |
| State inverted index V7 | 11.73 |
| Event log | 95.06 |
| Event log index | 21.11 |

Retired manifest files accounted for another 17.21 GiB. Unreferenced directory
entries accounted for 0.45 GiB and include non-payload metadata; they must not be
treated as a disposable set. No file-read errors occurred. Publication continued
during sampling and the manifest changed, so this census is not an atomic view.

A second V7 header census (generation 83,223) covered 25,482 files and
169,992,036,678 bytes: dictionary key data 58,311,666,818 bytes, postings
110,183,173,124 bytes, directory 1,494,138,896 bytes. The writer stores dictionary
blocks raw although the reader supports zstd. The largest raw block must remain
raw, or the reader's aggregate keyDataLen bound must be corrected before enabling
compression. Postings already use delta varints; they are not raw fixed-width
records. Measure representative dictionary blocks before estimating savings.

Compression priorities: dictionary blocks first; sample old V2/R1 payloads
against CDC next; then investigate posting encoding with point-read latency
measurements. Current CDC bytes cannot be assigned savings from old V2 samples.
Retired-file reclamation is a separate lifecycle issue, not a compression ratio.

`scripts/inspect_cold_files.py` records referenced/retired/unreferenced allocation
and bounded format headers without opening Pebble or reading whole payloads.
Run with the node stopped when a stable manifest census is required.
