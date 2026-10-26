# Reader-first rollout of history lookup and accessor changes

The user explicitly authorized merging to master and production deployment on
2026-10-26. Keep the current database, MuHash scheme, compression policy `auto`,
ports, and memory budgets. HSP1 remains unregistered. Merge current master to
preserve the already deployed canonical point-read and pending-prefix work.

The prior production reader at 46e19f55 accepts accessor V6/V7 and does not
accept the new account/system routing manifest kinds. The candidate reads old
and new layouts but normally emits up to V11 and routing refs. An automatic
rollback to the old binary after these files publish would be unsafe.

Add an opt-in `GTRON_HISTORY_ROLLOUT_READERS_ONLY` environment control, default
false and validated before database opening. It holds the entire ordered
snapshot/prune lifecycle, including explicit OnePass and wakeups, without
turning off hot history capture, immutable readers, block execution, RPC, or
MuHash. It must not grant posting-prune permission. Stop must complete normally.
All ordinary behavior remains unchanged with the control unset.

Pause the deploy timer during the authorized rollout. Stage one uses the normal
root-owned release helper and new binary with the hold set. Verify its exact
identity, same database directories, advancing height, old-layout cold reads,
fixed historical responses and an unchanged cold manifest. Its startup rollback
may safely use the previous old-format binary because no new cold formats are
published. Stage two restarts the SAME reader-capable binary with the hold
cleared. If this fails, restore the hold and recover with the SAME capable
binary; never start 46e19f55 after new-format publication. Restore the existing
auto-deploy timer when the rollout is verified. Keep all prior releases/data.

Validation: meaningful lifecycle tests must reject every maintenance callback,
leave stored rows and prune permission unchanged, cover OnePass and wake/stop,
and retain ordinary lifecycle behavior by default. Run merged-source Sapling
package tests, relevant race/vet, nested Pebble cursor checks and native build
before publishing. Record exact source/binary hashes, applied flags/limits,
pre/post data identity, stage checks, RPC answers/latencies and journal errors.
No full-current-height MuHash recomputation or whole-node snapshot restoration
is implied by ordinary live probes; distinguish those from the prior fixed-copy
gates and all exact-head merged tests.
