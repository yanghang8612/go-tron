# Encoder reuse local evidence

`local-checks.zip`: 243,538 bytes, SHA256
`882826c94f3ec567e2a8c45b5ae8be30f35c6a80cd0feec37ea9e97df732cb89`,
21 members. Includes complete local checks, the exact four changed Go sources,
baseline/current inventories, logs, runners and the overlay publication fixture.
All ZIP member bytes have a separate SHA256 manifest.

Run `python3 verify-local.py` from any directory. This verifies a frozen local
result, not the current working tree or Linux fixed-copy acceptance. Native
publication, full MuHash, HTTP and recovery are still pending. No deployment.

`native-checks.zip`: 258,813 bytes, SHA256
`4095c2f85999f5ec912a783a4495219769da5fab7f9a76daf2c3b728a5d78575`,
49 members. `python3 verify-native.py` verifies the complete staged payload,
12 remotely acknowledged chunks, private source identity, resource limits and
ten successful native checks. It freezes an early publication observation at
eight samples; publication completion and new full acceptance are separate.

The first bulk terminal upload lacked completion acknowledgements. The final
transfer uses bounded commands and an individual SHA receipt per part; only
that verified 234,428-byte payload was dispatched. No production action.
