# eth_call request optimization evidence

Final comparison only: baseline `4e6978dd`, candidate
`b6db66fcbde2d17f4b8fca099e5fd642e4435308`, fixed HEAD 11818192 / TxNum
601386177. Server experiment root:
`/data/gtron/eth-call-request-20261020-04`.

Run from the repository, using Python 3:

```sh
python3 docs/dev/evidence/20261020-eth-call-request/verify.py
python3 docs/dev/evidence/20261020-eth-call-request/summarize.py
```

`server-evidence.zip` preserves the original downloaded bytes. `transport.json`
records its SHA256, size and extraction aliases. Experiment `.go` files become
`.go.txt` to keep offline test hosts/instrumentation out of repository builds.
`server/evidence-sha256.json` retains the original ZIP member names and hashes.
The verifier checks transport, exact Git source revisions, the common HTTP
host, before-image oracle, complete source-fold recheck, every HTTP result,
medians/sample counts, all private inode/residency proofs, and production identity.

`requests.jsonl` has 238 successful calls: 140 timed samples and 98 excluded
discovery/warmup/profile calls. Another 14 instrumented calls are in
`diagnostic-counts.json`. There are 78 ordinary readonly HTTP hosts and one
instrumented host. Resource deltas are joined by RPC ID in
`joined-request-metrics.json`; CPU profiles and counter overlays are separate
from timed samples. `census-final.json` is a new exhaustive fold on the same
private experiment database after the comparison. `oracle-rechecked.json`
comes from the prior independent owning before-image walker.

Earlier partial experiments in roots `...-01`, `...-02` and `...-03` were
superseded and excluded. They found an owner-prefix alias boundary and hot-only
prefetch regressions, both addressed before the final comparison. No partial
timings enter this evidence bundle. The evidence collector was adjusted to the
server's older Python before sealing; no product code changed during collection.

Local verification receipts are in `local/`, with a hash inventory. The initial
`packages.log` includes an earlier failing snapshot test fixture; authoritative
full-package snapshot receipts are `snapshots-final.log` and final-source
`snapshots-final-b6.log`. Final state/race/vet receipts are
`range-hint-state.log`, `range-hint-race.log`, and `range-hint-vet.log`.
`native-test-inventory.txt` is only a test-name inventory. Actual Linux Sapling
test/build success is recorded by `server/native-correctness.log`,
`server/native-build.log`, `server/http-build.log`, and `prepare-status.json`.

Snapshot acceptance includes fixture/CLI restore and continued execution. This
bundle does not claim a complete export/restore of the 11818192 mainnet copy.
No push, production restart, online parameter change, data reset or deletion.
