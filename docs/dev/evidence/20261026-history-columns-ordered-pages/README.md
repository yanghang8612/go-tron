# Ordered record-column pages: local checks

`local-checks.zip`: 253,817 bytes, SHA256
`f9920a1cdf55b18149444dd997e578c237f28577f00f7a688f0ea7adb2186d3f`,
23 members. Includes all five changed Go sources, original/current inventories,
the six successful local checks, complete logs, the large publication fixture
and the exploratory failed test plus its corrected oracle.

Run `python3 verify-local.py`. This verifies frozen local source/checks only;
native publication and final full-copy MuHash/HTTP/recovery remain pending.
No push, deployment, production restart, deletion or budget change.

`native-dispatch.zip`: 265,972 bytes, SHA256
`522f74e11651df3f4e3119a89cdc005d6b12acc224d487046b30ce8791fcd967`,
40 members. Includes the 244,147-byte native payload, all 13 verified chunk
receipts, installer, preflight, start receipt and source verification. Run
`python3 verify-dispatch.py`. This proves dispatch only; the archived observation
is still in native-focused checks. Payload source commit is `6bb9b95e` (the Go
implementation is `b44cec7b`; the later commit corrects the evidence verifier).
The private unit has two CPUs, 4 GiB MemoryLimit and Nice=15. Production identity
was unchanged, inactive with MainPID=0; no production service operation occurred.

`native-checkpoint.zip`: 23,150 bytes, SHA256
`f4670750c6cb57d31e58f97785c6768f6679e2dfe58ab4efbd608a1d30a00bcf`,
20 members. Run `python3 verify-checkpoint.py`. Includes the fresh completed
10-check native status, the 8/60 publication checkpoint, and exact staged
follow-up scripts/receipts. Full native logs and completed publication raw rows
are not yet retrieved. Acceptance scripts are staged, unexecuted; no final
HTTP/MuHash/history recovery claim is made. Source Go remains frozen.

Completed raw evidence:

- `native-complete.zip`: 229,644 B, 27 members, SHA256
  `6a0cd0ad3137575e9264d12a1ca31c76ccd082f1fe39e2af35dbdfe0865f2853`.
  All 10 native logs, exact inventories/build identities and post-run proof.
- `publication-core.zip`: 55,048 B, 16 members, SHA256
  `dae16510d2c8e77c3483cd41970b2446183912f32ccf78bf68218c9249b64b91`.
  All 60 new/30 original raw rows, both complete process logs, selected plan,
  all 180 independently rehashed output refs and final source/binary identity.
  The unmeasured 8,688,213 B candidate enumeration remains on the server in
  the 1,727,578 B full archive (SHA256
  `6d922f4c5d5fd3acbbf13aab7f4858343eb23876a4ed2ca4e233c63e5cacbd41`).
  No measured row was omitted. Its full manifest/provenance is retained here.
- `completed-transfer-receipts.zip`: 33,816 B, 86 members, SHA256
  `83d0377e72e41a7c9971518bc486d98e68c995210d5e67477cb82a1b74d78fc7`.
  `verify-transfer.py` reconstructs all 71 exact packet JSON byte strings from
  the ZIP bytes and verifies their unique fresh UI receipt hashes/lengths.
- `acceptance-dispatch.zip`: 19,992 B, 18 members, SHA256
  `5e5617e86ee32296421b73d99a6b738e728129c8cf5eec1486c14aa575f8ebd3`.
  `verify-acceptance-dispatch.py` proves one bounded dispatch and the live
  parent 20092 / MuHash child 21000 at 01:24:50 UTC. It does not prove final
  HTTP, MuHash or recovery completion. Scripts and assessment are exact bytes.

Run `python3 verify-completed.py` to verify all native logs, source/binary
closure, all raw publication rows, all 180 output references and exactly
recompute 12 summary groups and three effects. This now requires the full
native log archive. The verifier explicitly reports that the unmeasured full
candidate enumeration was not retrieved and final fixed-copy acceptance is
incomplete. Earlier checkpoint ZIPs retain their historical observation scope.
