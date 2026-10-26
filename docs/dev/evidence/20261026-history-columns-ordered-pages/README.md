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
