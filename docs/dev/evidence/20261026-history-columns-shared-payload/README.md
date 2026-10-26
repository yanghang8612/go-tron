# Shared payload page study

Run `python3 verify-study.py` and `python3 verify-local-proof.py`.

- `study-core.tar.xz`: **438,076 B**, 37 members, SHA256
  `1f1148ee9885c9efcc793ac3058826b5029fe77f7b9941b83c5f456864bd03c5`.
  Includes every one of the 1,817 raw rows, all four successful native logs,
  exact source inventories, before/after input proofs, helper/runner/overlay,
  effective Go environment, build identity, final unit status and both earlier
  failed native attempts. No measured row or size regression was dropped.
- `completed-local-proof.zip`: **6,087 B**, 9 members, SHA256
  `1560fb95575fbb63644b93a47c0195dfa4169584429a782b05bcc210f17ad68c`.
  Producer seal plus fresh UI transport, exact completed SFTP download receipt,
  independent raw-row recomputation, unchanged live product-source proof and
  the subsequent local scratch decoder with its two successful race checks.
  The browser rewrote the download filename; the bytes and whole SHA match.
- `local-and-server-summary-checkpoint.zip`: **178,091 B**, 40 members, SHA256
  `0c5ea91c27b58ba5a1c4251e2e2acb470dbef722dc851924a12c3034545945a8`.
  Earlier historical checkpoint before full raw transfer: all local prototype
  sources/logs, three dispatch versions, exact producer summaries and seals,
  local source inventory and retained environment/transport failures. Its
  `raw_archive_retrieved: false` is the historical checkpoint scope, superseded
  by the completed raw archive above.

The successful native helper SHA is
`86378ea65aa3685f1752be89847a3ac39affb42f31e36b956873463ccef7773c`;
its test binary SHA after build is
`484acbb5c0cb2e1f887cb9c01889578c93df1402c57389ffc4a40fd4430f348b`.
Both the complete 2,149 product inputs and 24 inherited helpers stayed unchanged.
No product Go edits, deployment, production service action, reset or deletion.

This proves the bounded page prototype and resident serial diagnostics. It
does not prove a registered physical file, compactor performance, HTTP latency,
MuHash or snapshot acceptance for HSP1. The 11 ordinary pages have no recovered
annotations and are explicitly counted. All remaining pages reproduce the old
stored control exactly and restore canonical bytes in the native test. Local
verification checks every raw row, cost selection and timing aggregation; it
does not claim to re-decode the three large server-only input files locally.
The later scratch decoder has local correctness/race evidence only.
