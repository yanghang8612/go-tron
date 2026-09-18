# Legacy voter reward arithmetic parity (2026-09-18)

## Problem

`core/reward.oldRewardSum` narrowed each witness's floating reward to `int64`
before adding it to a single accumulator shared by all cycles. Official
java-tron instead keeps a `long` accumulator for one cycle, applies Java's
floating compound-assignment conversion after every witness, then adds the
completed cycle result to an outer `long` accumulator.

This changes consensus results at floating-point boundaries. For example, in
one cycle an exact reward of 1 followed by `(1 / 49) * 49` produces 2 in Java
because the latter product is the double immediately below 1 and the floating
sum rounds to 2 before narrowing. The previous Go code produced 1 because it
narrowed that product to zero first.

## Java source evidence

The primary source is the official `tronprotocol/java-tron` repository. Tag
`GreatVoyage-v4.8.2` resolves to
`f8ff7c76f45ab41d9bb76922329657506819701b`. In
`MortgageService.java:171-187`, `computeReward(cycle, votes)` creates
`long reward = 0` and executes `reward += voteRate * totalReward` for each
witness. Its old-reward caller at lines 260-268 invokes that method once per
cycle and adds the returned longs.

This is also the historical behavior at the time of the affected withdrawal,
rather than a later optimization change. Official tag `GreatVoyage-v4.3.0`,
commit `0b8485f191bcda9a787e83af87849cee04c1c441` dated 2021-08-03, has the same
fresh per-cycle accumulator at `MortgageService.java:185-201`; lines 218-224
call it separately for every old-algorithm cycle.

Git blame at that historical source traces the inner accumulator and compound
assignment to commit `9dd36023cd1d44b29ef1f12eecfadc656a9d730a`
dated 2020-09-27, and the complete outer old-cycle loop to commit
`325bbdf6c5d4575afeec0e6a469deff01c4426bd` dated 2021-06-28.

Java bytecode for a faithful standalone copy of the expression is `l2d`,
`dmul`, `dadd`, `d2l`. The Go port must therefore round the multiplication to a
`float64`, convert the existing per-cycle integer to `float64`, add them, and
apply Java-compatible double-to-long narrowing to the sum. The outer cycle
addition remains ordinary wrapping `int64` arithmetic.

## Candidate correction and validation

The candidate gives every cycle a fresh `cycleReward`, evaluates each witness
with the Java compound-assignment order, and adds the completed cycle reward to
the outer total. It does not change the vote order, skip rules, reward feature
gates, VI path, or TVM behavior, and it adds no height-based fork gate.

Golden cases copied from an executed Java harness cover witness ordering,
cycle reset, repeated cycles, values above the exact-double integer range,
Java narrowing saturation, and outer `long` overflow. Before the correction,
four cases failed (1 instead of 2, 2 instead of 4, a different result above
2^53, and integer wrap instead of same-cycle saturation). All golden cases and
the full `core/reward`, `actuator`, `vm`, and `core` package tests pass after the
correction; the focused golden test also passes under the race detector.

Reproduction sources, raw Java/Go output, bytecode, source excerpts, and hashes
are under `build/benchmarks/20260918-cold-optimization/reward-oracle/` in the
local evidence workspace.

## Mainnet evidence and limits

For transaction
`22636ad43e891be19129b2a19fac9bde68047241f5b861cb78795c29e1d69734`
at block 34,740,159, transaction index 1, gtron recorded
`withdraw_amount=7704891` while TronGrid recorded `7704892`. Gtron's archived
pre-block account balance of 29,972,049 matches the reconstructed canonical
transaction ledger, and the post-transaction state differs by one SUN.

That receipt comparison is direct evidence of a one-SUN withdrawal divergence,
but the affected account's ordered per-cycle witness reward inputs have not yet
been reproduced through the Java/Go oracle. The general arithmetic mismatch is
proven; attribution of this specific withdrawal remains pending those inputs.
The candidate corrects future execution of the legacy formula. It does not
repair already divergent state or define a state recovery procedure.
