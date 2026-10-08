# History-staging startup receipt authentication — 2026-10-08

This candidate removes repeated offline semantic work from startup. It does not
change the on-disk schema, rerun historical migration, introduce a bypass flag,
or authorize deployment by itself.

## Observed bottleneck

At 03:27:58 UTC on October 8 (11:27:58 Asia/Shanghai), the existing production
reader was still in its pre-API startup audit at bucket 16,058 of 32,495. The
migration journal was already `DONE`. A separate 20-second resource sample at
03:19:22–03:19:42 UTC on the same day showed the 16-core host using about 1.13
CPU cores for startup, with approximately 76,000 small reads/second, disk
utilization 11.5%, device await 1.33 ms and process RSS 2.22 GiB. This resource
window does not establish sustained throughput or a completion estimate.

The startup receipt path called `HistoryStagingColdProver.authenticate`, which
ran `VerifyHistorySegmentWithCompanionsContext`: physical checksums followed by
record decoding and companion semantic coverage/ETL. This repeated the complete
trio audit already required before the durable cold certification was published.

## Exact trust boundary

The prerequisite is a **previously certified durable binding obtained from a
validated route**, not arbitrary supplied metadata. `CertifyColdRange` verifies
the semantic/canonical proof before atomically publishing route, binding and
reverse references and synchronizing the source WAL. `RebindCold` similarly
requires semantic equivalence for every affected span. Offline
`PrepareOfflineBinding` first runs the full `VerifyBinding` and only then permits
a fingerprint check for ordered publication under exclusive store/cold locks.

Startup retains the route/receipt/epoch checks, manifest validation and
ContentID membership, ordered in-bucket coverage, and nonzero semantic and
transaction-range commitments. It reauthenticates the exact certified
history/index/accessor bytes using `VerifyHistorySegmentCompanionChecksumsContext`
with SHA256 required for **all three files** and exact physical sizes. It checks
regular-file identity, inode, size, modification time and mode before/after each
audit, then rechecks the whole binding so an earlier trio cannot change while a
later one is verified. Shutdown cancellation interrupts checksum reads and
singleflight waits; failed, cancelled or changed-file audits cannot install a
successful cache entry.

This does not establish the truth of a newly forged nonzero semantic hash in the
database. Startup relies on the existing durable certification boundary for that
fact. A correctly checksummed but semantically malformed trio can satisfy only
physical authentication; full admission and RPC proof still reject it. Tests
deliberately construct this case to ensure physical success never becomes a
semantic proof.

The receipt checksum cache has a separate domain, map and inflight map from both
full-trio authentication and RPC per-range proofs. It retains at most 4,096 trio
fingerprints and permits at most eight concurrent physical audits. This patch
adds no startup workers and retains no additional history/index readers. Full
`Build`, offline admission/rebind and first-miss RPC semantic verification remain
unchanged. No successful receipt authentication populates their caches, and full
authentication does not populate the receipt cache.

Each unchanged referenced trio is normally scanned once with sequential SHA
reads while its success remains cache-resident, plus bounded route metadata and
fingerprint checks. Shared trios are reused across adjacent buckets. This is not
a persistent skip: eviction, process restart, file changes and failed audits
require reauthentication. The 4,096-entry bound preserves memory; its arbitrary
eviction can cause a revisited trio to be scanned again, so there is no promise
of exactly one pass per process.

## Local verification and limits

The snapshots package passed in 88.736 s, focused receipt/full-cache tests passed
in 2.131 s, and focused race tests covering the new receipt path plus existing
full-trio/pinned-binding paths passed in 10.688 s. Tests cover both cache and
inflight isolation directions, bad receipt metadata, required strong checksums,
all-companion tampering/replacement/missing files, real streaming-checksum
cancellation and retry, failed-flight retry, cross-bucket scan reuse, and a
mixed-trio binding changed during its second audit.

Three-iteration M1 Max Darwin/arm64 benchmarks used warm 1,024-block fixtures
with 8,192 history rows and no cached authentication success:

| Fixture | Receipt checksums | Full trio semantics |
| --- | ---: | ---: |
| V5 | 0.339 ms | 19.279 ms |
| V6 | 0.428 ms | 101.230 ms |
| Reference | 0.293 ms | 35.193 ms |

These measurements isolate the removed work on small warm fixtures. They do not
predict production startup duration, cold-cache disk throughput or remaining
time. Reproduce with:

```sh
go test ./core/state/snapshots -count=1 -timeout=300s
go test -race ./core/state/snapshots -run 'TestHistoryStaging(Receipt|FullProofDoesNotPopulate|TrioAuthentication|PinnedBinding)' -count=1 -timeout=180s
go test ./core/state/snapshots -run '^$' -bench '^BenchmarkHistoryStagingStartupTrioAuthentication$' -benchtime=3x -count=1 -timeout=180s
```

## Conditional afternoon rollout

On October 8 afternoon in Asia/Shanghai, first inspect the existing process,
startup bucket progress over a measured interval, API readiness and actual
listeners. If it has finished or is close to finishing, preserve that run and
retain the candidate for a later normal restart. Consider replacement only if
the current audit still has a long remaining interval and a reviewed, validated
candidate is ready. Compare the remaining measured old work with the cost of
starting the candidate's checksum audit from the beginning; do not infer an ETA
from the local benchmark ratios.

Before publication, the **exact final candidate revision** must pass native
Linux/Sapling validation and build, with captured successful exits:

```sh
make test-sapling
CGO_ENABLED=1 go vet -tags=sapling ./...
make gtron
```

Record source revision, executable SHA256, build tags/cgo identity and staging
reader capability for that same artifact, together with the focused startup
checks above. Local Darwin tests and a build-only success do not replace the
native suite. A failed or stale deployment test cycle is not release evidence.

Use the existing reviewed release/start-lock and capability protocol. Preserve
the contents and intended state of maintenance/stop holds, migration markers,
guards, rollback fences and paused deployment automation; the candidate must
not remove or bypass `/var/lib/gtron-offline-maintenance.hold` or
`/var/lib/gtron-mainnet-disk-stop.hold` to force activation. A newly blocking hold
keeps activation blocked. Do not restart the completed migration, clear its
route barrier, or substitute the old plan producer SHA for the new runtime
artifact SHA. Keep Nile and Java service configuration unchanged.

After any authorized replacement, observe startup audit completion and API
health through the intended mainnet gateway/listeners, and record actual
elapsed time, CPU, physical I/O, memory and bucket progress. Restore deployment
automation only through the existing health/hold policy. This document records
the candidate and acceptance gates; it does not claim native validation or a
production rollout has already succeeded.
