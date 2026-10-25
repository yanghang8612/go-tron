# Small-page history record columns: isolated experiment

Continue data compression without production deployment, restart, deletion or
budget changes. The preceding account-delta experiment found that 1 MiB pages
explain almost all space gains but regress complete HTTP queries. Preserve the
existing 128 KiB independent decoding boundary and canonical V6 virtual bytes.

First measure three reversible per-page transforms of complete 21-byte V6
record headers: key-ID varints, signed key-ID differences, and key-ID byte planes.
All variants separate monotonic TxNum differences, previous-value lengths and
existence bits from opaque old values. Frame size is derived from the original
previous-value length, with overflow checked. Store the first complete header
position and count; subsequent positions follow original frame lengths. Prefix,
payloads, partial headers and all other bytes remain opaque literals. No field
of an account value or consensus element is discarded. Dependencies never leave
the independently decoded page. Bound count, output length and column sizes before allocation or copying;
verify the reconstructed canonical page CRC32C before returning bytes. The full
canonical stream and immutable physical files still receive SHA256 verification
in the offline acceptance and ordinary authenticated manifest protocol. HRC2
uses CRC32C only for the additional independently decoded page check, matching
the existing model of noncryptographic page checks plus cryptographic whole-file
authentication. The HRC1 trial used an extra per-page SHA256; CPU profiling found
that extra pass consumed 45.31% of the combined baseline/candidate workload.
Retain both trial results rather than attributing its cost to column decoding.

Compare ordinary Zstd on exactly the same pages, including all mode, directory,
footer and digest overhead. Keep plain pages/files when a transform is not
smaller. Start with the previous 14 immutable files (nine fixed-capture and five
25M–35M samples) and retain every result. Do not extrapolate sampled savings to
the production database. Only integrate a new opt-in physical container after
a material net improvement is measured. Default emission stays unchanged.

Subsequent acceptance requires complete fixed-capture byte equality, immutable
companion/routing authentication, physical allocated and file-length accounting,
full MuHash, historical and snapshot restoration, and identical full HTTP warm,
fresh-process and private OS-cold comparisons. A page-only test is not that
acceptance. No claim of deployment readiness until those gates pass.
