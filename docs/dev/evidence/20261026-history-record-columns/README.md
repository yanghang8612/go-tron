# Small-page record columns: screening evidence

This is **page screening, not whole-node acceptance or a deployment**. The private
codec is not called by production readers/writers; default output is unchanged.
The broader compression goal remains active. Integration, actual allocated
space, full HTTP, MuHash and recovery acceptance are still pending.

`archives/` retains complete, byte-exact small ZIPs from the isolated lab
`/data/gtron/history-record-columns-20261026-01`. Its source is the previous
immutable V11 fixed capture plus five mature samples, not production data.
The 14 files contain nine fixed-capture samples and five 25M–35M samples.
All original ZIP CRCs, safe paths and member SHA256s are checked. `probe` is the
HRC1 SHA256 trial, `round2` its nonwinning reconstruction-loop optimization,
`round3` the HRC2 CRC32C trial plus original CPU profile, and `adaptive` closes
all page choices using the same policy as the eight-page performance sample.

The ZIPs were copied through visible output selection in our dedicated
JumpServer terminal after SFTP controls failed to retrieve them. Clipboard
Base64 was decoded locally and compared with the server-printed length/SHA.
No alternate public file endpoint or production service change was introduced.

All reported file lengths are **projected complete encoded lengths**: actual
Zstd page bytes plus all projected existing container/table/footer and mode
bytes. Ordinary same-boundary recompression exactly matches every original
file length. New candidate files were not installed/fsynced and no allocation
savings have been measured. Whole-file plain fallback is charged. Old values
remain opaque and every transformed page has an exact inverse-byte oracle;
eight representative pages additionally test full Zstd decoding. These are
not full trio/routing/restore/MuHash/HTTP proofs.

The CPU comparison reuses resident compressed frames and output buffers; each
arm has 10 alternating batches of 256 full page decodes plus byte comparison.
The eight identical logical 128 KiB pages cover three fixed and five mature
files. Initial HRC1 per-page SHA256 dominated the combined CPU profile; HRC2
uses CRC32C for this additional page check. Whole-file authentication and full
canonical SHA256 acceptance remain required and unchanged. Performance samples
choose the smallest of the three key-column modes; the adaptive space archive
measures exactly that page-selection policy across all 14 files. Extra
compression attempts and production cold-publication throughput remain untested.

```sh
python3 docs/dev/evidence/20261026-history-record-columns/verify.py
```

Python 3.9+ and the unchanged prototype checkout are required. The verifier
checks archived bytes, both current codec/test files, the baseline's 1,089
Go/module source files, canonical sample identity, all accounting and all page
CPU medians. Source checks intentionally fail after subsequent implementation
changes; use this commit for reproduction. Rechecking original database bytes
requires rerunning the archived native harnesses against retained server files.
