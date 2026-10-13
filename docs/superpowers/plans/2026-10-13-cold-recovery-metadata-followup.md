# Implementation plan

1. Add moderate-debt recovery carryover with immediate protection revocation.
2. Add an authenticated private immutable manifest load and use it only in
   audited read-only builder/admission paths. Preserve public loader ownership.
3. Add event replay telemetry and use fixed-input tests/benchmarks to evaluate
   source replay improvements. Inspect history LSM evidence before tuning it.
4. Run targeted, race and full affected-package tests, review the final diff,
   and record measured gains and limits before production rollout.
