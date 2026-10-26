# Shared payload page study

Run `python3 verify-study.py` and `python3 verify-local-proof.py`.

For the completed bounded decoder follow-up, also run
`python3 verify-decoder-study.py` and `python3 verify-cpu-study.py`.
The CPU verifier needs Go 1.25.5, regenerates native top tables from the six
raw profiles, and checks the exact source-line attribution of key/common frame
work. With no arguments its reproduction files use an automatically removed
temporary directory; three explicit arguments retain them at the chosen path.

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
The completed native scratch-decoder follow-up is retained separately, without
replacing any earlier artifact:

- `decoder-study-core.tar.xz`: **712,128 B**, 30 members, SHA256
  `cc18b1830e0016afc364c56e74e8aff2fd81f1e2408c3147461863ac0e08ed27`.
  All 1,817 raw rows, six arms, five rounds, eight calls per interval, all four
  native pass logs, three physical input identities, before/after source
  inventories, frozen helpers, environment, dispatch and bounded unit receipt.
  Includes the first cold-build timeout; no failed trial was reused as a pass.
  The native test binary SHA is
  `478a5df1dad8a32dbe281bf5e80f2606ab4da34c30231e035b87d030c609e070`.
- `decoder-local-proof.zip`: **6,759 B**, 6 members, SHA256
  `487a870039a3e501e5739221e30f7db2066dc4b16e43033508a7f876e91475c7`.
  Producer seal/checkpoint, 15-frame standard WebCLI selection/copy receipt,
  whole-file SHA, independent raw-row totals, earlier savings distribution,
  and the read-only online head/balance/transaction responses. These responses
  are health probes, not HTTP performance or commitment acceptance.
- `cpu-study-core.tar.xz`: **500,088 B**, 36 members, SHA256
  `903c1ae670c5ca4de0833fd755a2b5f5a052f15bf0717c01d87fce1940cf0ac0`.
  Six complete 20+ second windows in V5/HSP1/HSP1/V5/V5/HSP1 order; full-cohort
  canonical checks before and after every window; raw profiles, CPU/pass
  counters, native flat/cumulative/call-context reports, exact harness/runner,
  unchanged source/input proofs and the same production process identity.
  Profiled loops exclude fixture construction, page encoding, data-file reads
  and ReadMemStats. Standard CPU profiler/output overhead remains in process.
- `cpu-local-proof.zip`: **38,427 B**, 17 members, SHA256
  `7b514f9e2822459f1c796af11823909902e3a9c6fbc14b9098115ad8e862a619`.
  Eleven-frame and whole-file transport verification, producer completion,
  independently regenerated top tables including zero-flat rows, unfiltered
  tables/raw callsites and recomputed CPU, size-budget and key-mode results.
- `decoder-final-health.zip`: **1,729 B**, 4 members, SHA256
  `aa824631e265909748cabb12afb2351ba119c7e8b0e9d7da6dbd267eaa37fb75`.
  Final read-only process/binary identity, both private units exited, HTTP head
  progression 40,406,330 to 40,510,739, and the 341/342 historical balances plus
  block-342 transaction location. This remains health verification of the
  existing production binary, not HSP1 HTTP/commitment/restore acceptance.

Both later units ran with 2 CPU, 4 GiB, Nice 15. Production stayed at active
PID 7788/start ticks 4872793077 with unchanged running binary SHA
`7e4e5dfd592903e7a8bf657df5b9d9dbe32ac412917e24d3e4e7bd018dacba0c`.
These are separate from the initial HSP1 study's inactive/PID 0 observations.

The completed comparison finds scratch HSP1 7.6895% slower than V5 when both
reuse buffers. The independent CPU windows find 8.5303% more CPU/pass.
HSP1 key-frame Huffman work is visible in exact sampled callsites. The shared
frame has no Huffman samples in this cohort. This is sampled CPU attribution,
not a claim that unsampled functions have zero actual cost.

HSP1 remains unregistered. The follow-up proves resident decoder behavior,
not whole-file publication, complete HTTP eth_call, MuHash or snapshot restore.
No product Go, deployment, production service, DB or production budget changed.
