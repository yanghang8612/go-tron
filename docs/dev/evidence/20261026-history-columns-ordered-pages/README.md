# Ordered record-column pages: local checks

`local-checks.zip`: 253,817 bytes, SHA256
`f9920a1cdf55b18149444dd997e578c237f28577f00f7a688f0ea7adb2186d3f`,
23 members. Includes all five changed Go sources, original/current inventories,
the six successful local checks, complete logs, the large publication fixture
and the exploratory failed test plus its corrected oracle.

Run `python3 verify-local.py`. This verifies frozen local source/checks only;
native publication and final full-copy MuHash/HTTP/recovery remain pending.
No push, deployment, production restart, deletion or budget change.
