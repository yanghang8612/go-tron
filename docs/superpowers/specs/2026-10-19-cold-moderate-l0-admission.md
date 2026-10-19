# Bounded serial cold work under moderate L0 pressure

Baseline `e45b39b09b0e7d0bb6ebe32cf9ec285d39d79a3b`. Root/Astra approved
one 60% serial policy, not a search over duty values. This is an ordinary
same-database upgrade. Formats, roots, source/proof/publication, hard admission,
resource budgets, global L0 thresholds and operator settings do not change.

## Independent qualified observations

The existing recovery and moderate-debt paths require L0 below the compaction
threshold T, although instantaneous L0 soft pressure starts at 2T. Persistent
T<=L0<2T can therefore retain level1 and block busy opportunities indefinitely.
The new lane independently requires two newly accepted qualifying observations
at least five seconds apart in one comparable observation epoch. It does not
reuse or promote the existing good/debt/cpuBurst state. Unknown/stale evidence,
reset, new stall, negative or L0>=2T, device pressure or severe debt revokes its
count immediately; restoring a rejected reused observation cannot reacquire it.

Healthy points with 0<=L0<T also count toward the independent observation pair;
only final lane selection requires L0>=T. Thus T-1/T oscillation cannot create a
new starvation boundary. Changed T/stop thresholds start a new pair.

Qualification requires fresh available engine/device timestamps; known positive
L0 compaction and stop thresholds; 0<=L0<2T using overflow-safe subtraction;
no instantaneous hard limit, active/new write stall, reset, existing device
pressure or severe recent debt growth; and device await in [0,2ms]. Existing
new-stall/debt definitions and hard memtable/L0 stop protections remain. No new
arbitrary queue veto is introduced. At admission, independently require the
node's current busy CPU/runtime/memory callback and recheck storage freshness
when it returns. Revalidate the already selected storage and runtime evidence
after potentially blocking range/source planning and before taking the lease,
then check cancellation again. If no longer valid, a soft-to-busy opportunity
defers and rearms observation; an above-busy/pressure fallback only clears the
new flag and continues under its original rules. Consuming expired evidence
revokes the independent pair. This is an opportunity, not a reservation.

## Per-pass selection and bounded work

Between soft and busy backlog marks, this lane may open the existing busy
resource opportunity and five-second observation wake. A normal full pass still
revalidates watermarks, storage, space, deadlines, sources and the shared lease.
Above the busy mark, first retain the existing forced-busy liveness decision;
then independently select this lane if qualified. Failed/unknown qualification
must never replace that fallback with deferral or resource observation waiting.
Every pass starts without an inherited selection. An already legal existing
cpuBurst opportunity takes precedence and keeps its original budget/topology;
the new lane only supplies an opportunity that old storage admission denied.

The selected PassResult latches 60% for that complete lifecycle. Use the existing
level1 density targets, 2 seconds/32MiB, with unchanged complete-block/txnum,
configuration, 25% growth and unsuccessful-attempt caps. Disable history/event
pairing and enhanced shared reads on this pass, irrespective of later storage
state. Preserve a requested reference format and the original serial codec
behavior. Keep ordinary lease cooldown, rather than the CPU-burst shortcut.
Do not elevate global cpuBurst/debtModerate/targets or optional merge admission.

Both provisional and final outer completion use the latched tier, charging the
complete nonnegative outer work, including any optional merge, catalog/prune
and metadata work. Recovery is at least ceil(2*work/3), computed without overflow
as work-floor(work/3), at least the original three-second floor and any longer
configured cooldown. Failures retain the longer existing one-minute/configured
backoff. Existing later deadlines only extend and are never shortened. A later
healthy/global level3 sample cannot lower this selected pass's charge.

Qualification/samples, selections/attempts, actual publications and successful
complete lifecycles are observed separately; denied lease or unattempted work
cannot be reported as completion. Keep existing reason bit assignments stable.

## Verification and interpretation

Freeze the protocol before implementation under
`build/benchmarks/20261019-cold-tiered-admission/`. Deterministic immutable
five-second traces include the >=408s L0 8–12 platform, qualification/revocation,
reused/reset/stale data, boundaries/overflow, exact above/below busy behavior,
resource callback aging, occupied lease/cooldown/space/cancellation and failures.
Real Pebble fixtures compare every cold file/query/history/event identity and
stage at fixed endpoints, retaining complete outer recovery and no pending
lease/maintenance. A full-clock fixture waits for real deadlines; no deadline
clearing is counted as scheduler throughput. Narrow race and relevant packages
precede exact Linux Sapling full-repository validation/build and final review.

At the observed reference 2190 blocks/4.5s paired work, 20% allows only about
97 blocks/s. At 60%, serial complete work must be <=7.38s for the same number
of blocks to match 178 blocks/s, and <=6.71s for a 10% margin. Smaller adaptive
ranges and minimum recovery can further limit service. These are conditional
capacity bounds, not a measured serial rate. No additional duty increase follows
a failed target. Local fixtures do not establish same-input mainnet contention.
A fixed unchanged-config baseline and 600-second post window must report cold
published versus eligible arrivals/lag, importer workload/rates, complete work
and recovery, L0/debt/stalls, CPU/IO/device and memory. Do not claim catch-up by
slowing the importer or call partial zero-to-positive relief a proven match.
Conditional starvation freedom does not override real hard pressure, missing
resource evidence, lease ownership, disk reserve or correctness failures.
