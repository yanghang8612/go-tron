# Implementation and verification

1. Add independent kind, strict codec/cache, bounded streaming membership builder and strict source verification.
2. Split query-family routing, register publication/import/retirement, and prioritize small maintenance before broad routes.
3. Test aliases, generations, empty sources, new keys, more than 64 sources, replacements, mixed batches, corruption, cancellation and retirement.
4. Build native baseline/candidate offline harnesses; verify fixed input identity, complete HTTP values and cache regimes, full MuHash and actual restoration; archive raw evidence.
5. Review limitations and remaining bottlenecks. Do not push, deploy or restart production.

Completed on 2026-10-27. The fixed large catalog has 27,893 authenticated
history/accessor pairs and 60 discovered keys (including allow_tvm_london,
which the older 59-key subset lacked). Full coverage and canonical identity
checks pass. Native system-batch warm median is 1.451038 s → 0.228314 s.

The unchanged 11.8M canonical copy passed 192 full HTTP eth_call oracles,
144,903,091-row MuHash recomputation and actual 60-block / 22,496-row history
restoration. Local strict/signed snapshot restoration, race, vet and build
checks pass. The 9M full HTTP private-OS-cold median remains unimproved
(additional n=4 per version: 791.420 ms → 824.675 ms). Traced and native
catalog latencies are kept separate.

All 614 sealed evidence members were independently hash-verified locally.
See docs/dev/cold-query-small-system-routing-20261027.md for the complete
results, reproduction scripts and remaining hot-tail/contract-route work.
Production PID, binary and process start identity stayed unchanged; no
push, deployment, service restart or canonical database mutation occurred.

Production bootstrap follow-up: preserve authenticated selected broad negatives
for exact pairs not yet covered by the lightweight route; full lightweight
coverage keeps the broad-free path. Validate missing/partial/full routes, mixed
selection order, V1/V2, positive members, uncovered tails and physical failures.
