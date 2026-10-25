# Encoder reuse local evidence

`local-checks.zip`: 243,538 bytes, SHA256
`882826c94f3ec567e2a8c45b5ae8be30f35c6a80cd0feec37ea9e97df732cb89`,
21 members. Includes complete local checks, the exact four changed Go sources,
baseline/current inventories, logs, runners and the overlay publication fixture.
All ZIP member bytes have a separate SHA256 manifest.

Run `python3 verify-local.py` from any directory. This verifies a frozen local
result, not the current working tree or Linux fixed-copy acceptance. Native
publication, full MuHash, HTTP and recovery are still pending. No deployment.
