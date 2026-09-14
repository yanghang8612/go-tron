# Cold history recovery-period pressure observation

2026-09-14. Narrow implementation and ten new focused tests completed; broader
package/race and native verification are tracked by the parent integration. No
deployment result is asserted by this specification.

## Problem and existing behavior

The deployed c329bf12 after window advanced cold history by 281 blocks while
importing 4,928. Its initial 474-block observation took 65.407 seconds of
density work; pressured targets then shrank batches through 14, 3 and 1 blocks.
Later complete work of 8.605 and 6.694 seconds earned 34.421 and 26.775 seconds
of recovery at 20% duty. These observations do not prove all small batches
are caused by the controller or establish a same-input version regression.

The existing [busy-resource observation plan](../plans/2026-09-10-busy-history-resource-observation.md)
arms a five-second check only for a resource-refused opportunity between soft
and busy watermarks. Ordinary completed forced builds do not arm it; their
next pressure/hysteresis observation follows the next full pass. Long recovery
can therefore delay both confirmation of improvement and detection of renewed
pressure. The existing metadata density subtraction is already in force and
must not be counted as a new fix.

## Narrow change

Reuse the existing observation timer, generation/CAS cancellation and passMu
TryLock serialization. Add a recovery-only mode after a successful full pass
with an existing future historyNotBefore deadline, during deep throughput
sync. Rate-limited rechecks may rearm while that deadline remains in force.
Do not arm this mode when the original busy-resource opportunity is armed.

The recovery branch only refreshes the unchanged load controller and always
returns wake=false. It does not open files, access application rows, acquire
or inspect a heavy-work lease, invoke CPU admission, repeat maintenance or
change any deadline. It stops when the current deadline expires, deep sync
ends, configuration disables it, context/runner stops, or its generation is
canceled. Normal retry/tick scheduling still starts the next complete pass.
Pending outer completion must prevent observation; outer failure and the next
preflight cancel the matching generation. Old completion cannot cancel newer
observations.

Use a separate optional Config.HistoryRecoveryLoadProbe. Production wires it
to runtimeHistoryResources.postingPressure: TryLock cached device state plus
current engine pressure metadata, with no proc/device refresh. The ordinary
HistoryLoadProbe/sampleLoad and original busy-resource branch remain unchanged.
This is bounded work, not a hard latency guarantee for engine metadata locks.
Nil callback disables only the new mode. Add distinct recovery_observation
checks, accepted_samples and last_duration metrics; do not reinterpret old
busy checks or wakeups.

## Invariants

Keep hard-pressure checks, acceptance freshness/spacing, hysteresis, targets,
duty, batch caps, byte budgets, failure recovery and full maintenance accounting
unchanged. An already installed deadline is never shortened, including after
pressure improves. Provisional/outer deadlines still take the maximum. The
next ordinary admission rechecks current conditions, space, coverage and lease.
No formats, checksums, canonical proofs, pruning windows or storage lifetimes
change. Support both integrated lifecycle and standalone Runner timers.

## Validation

Use controlled time and the real observer/loop to prove no work or early wake
while pressure samples are refreshed. Test continuous recovery and renewed
debt/stall pressure, repeated/unknown/stale/future samples, nil probe, stopped
sampler, locked runner, pending completion, later deadline extension, due retry
coalescing, concurrent cancellation, stale generations, preflight/outer errors,
shutdown and an actual next admission that sees new hard pressure. Preserve
the existing busy observation tests and device-cache-only probe tests.

Run Go 1.25.5 / GOMAXPROCS=2 focused tests and race checks, coordinating CPU
windows. A fixed producer trace must be consumed without inventing intervening
samples. Faster observation can detect pressure sooner as well as recovery;
no unconditional throughput improvement is promised.

The completed root trace contains 73 points at five-second intervals, from
10:00:38.162070 to 10:06:38.162119 UTC. Visible accepted sequence increased by
41; visible timestamp-gap median was 7.119s and maximum 24.144s. These gaps
can include accepted samples not visible between polls. Sampled levels were
1: 16, 2: 51, and 3: 6; these are poll counts, not exact time-at-level.
 HTTP budget fields
are the old controller's last observation, not a fresh complete StoragePressure
record; registry timestamps/fields cannot silently replace missing memtable,
stall or device state. Replay must label any reconstruction limitations and
must not present it as an exact counterfactual. Full online service-rate/lag
and disk validation remain a separate root-owned release step.

Local focused validation: snapshots 1.047s and pruning 1.882s, both PASS.
Evidence and reconstruction limits are recorded in
`build/benchmarks/20260914-cold-backlog/recovery/validation.md`. The synthetic
same-input trace proves earlier pressure recovery and renewed-stall detection
without changing the existing 40-second deadline; it is not a production
throughput estimate.
