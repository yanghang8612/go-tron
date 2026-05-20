# TxPool subpool — plan

**Spec:** [2026-05-19-txpool-subpool-design.md](../specs/2026-05-19-txpool-subpool-design.md)

## Slice 1 — Interfaces + routing

- [ ] `core/txpool/subpool.go` — `SubPool` interface
- [ ] `core/txpool/routing.go` — `routeForContractType` mapping every
      `corepb.Transaction_Contract_ContractType` to a subpool name
- [ ] Unit test: every contract type routes to exactly one subpool
- [ ] Test: multi-contract tx with mixed routes → routing-error
      sentinel returned

## Slice 2 — Dispatcher + transparent subpool

- [ ] Rename current `core/txpool/pool.go` internals to `transparent/pool.go`
- [ ] `core/txpool/pool.go` becomes dispatcher with one subpool
      (transparent only); behaviour byte-identical to today
- [ ] Existing txpool tests retargeted to the new dispatcher path,
      green
- [ ] No new test regressions

## Slice 3 — Governance subpool

- [ ] `core/txpool/governance/pool.go` — admit governance contract types
- [ ] Eviction policy: never on size; only on `tx.expiration` past
- [ ] Slot cap `4096`, per-sender cap `4`
- [ ] Test: spam 100K transparent txs while submitting one vote tx;
      after 60s the vote tx still pending
- [ ] Add metric `txpool.governance.size`

## Slice 4 — Shielded subpool

- [ ] `core/txpool/shielded/pool.go` — admit ShieldedTransferContract
- [ ] Slot cap `2048`, per-sender cap `2`, admission rate-limit `1/s`
- [ ] Test: submit 100 shielded in 1s; assert exactly 1 admitted, 99 rejected
- [ ] Test: rate-limit reset after 1s passes
- [ ] Add metric `txpool.shielded.admit_rate_limited`

## Slice 5 — Reset / reorg semantics

- [ ] `Reset(oldHead, newHead, newState)` on each subpool
- [ ] Implementation: drop newHead.txs; re-validate remaining; collect
      orphan-chain txs on reorg, re-admit
- [ ] Test: linear next-block reset (5 txs included, those dropped from
      pool)
- [ ] Test: reorg reset (5-block reorg, 10 included-then-orphaned txs
      reappear in pool)
- [ ] Parallelize subpool resets via goroutine + WaitGroup if profile
      shows ≥ 10ms wall

## Slice 6 — Config + metrics + rollout

- [ ] `gtron.toml [txpool.*]` sections, per-subpool overrides
- [ ] CLI flags `--txpool.<name>.slots`, etc.
- [ ] Operator doc `docs/dev/txpool-subpools.md`
- [ ] Default rollout: enable all three subpools after one Nile soak

## Acceptance criteria

- [ ] Governance isolation test (slice 3) passes
- [ ] Shielded rate-limit test (slice 4) passes
- [ ] All existing txpool tests pass through the dispatcher
- [ ] Race detector clean
- [ ] No regression in producer's `Pending()` consumption
