# `next_proposal_id` triage — 2026-05-15

Secondary finding from the [DP defaults audit](dp-defaults-audit-2026-05-15.md).
Investigated and resolved in a single pass.

## Symptom

`core/state/dynamic_properties.go::defaultProps` seeds `next_proposal_id = 1`
at genesis (commit `42c597f`). java-tron's equivalent
`DynamicPropertiesStore::saveLatestProposalNum(0)` (constructor line 311)
seeds **0** at genesis. The values stay off-by-one for the lifetime of the
chain — every committed proposal increments both counters in lock-step, so
gtron's value is always `java-tron's value + 1`.

## Root cause

The semantic mismatch in `42c597f`:

- **java-tron** stores `LATEST_PROPOSAL_NUM` = "latest assigned id" (last id
  written by a `ProposalCreate`). At genesis = 0; first proposal pre-increments
  to id = 0+1 = 1 and stores 1.
- **gtron** stored `next_proposal_id` = "next id to assign". Author of
  `42c597f` reasoned that the first proposal should get id = 1 (correct vs
  java-tron) and so bumped the default from 0 to 1. This fixes the first-id
  but freezes a permanent +1 offset in the stored counter.

Both implementations produce identical proposal IDs on-chain and identical
chain behavior. The divergence is in the DP store's serialized counter, which
is folded into `core/conformance/digest.go::DigestC` for every block — so
post-`42c597f` the digest diverges from java-tron's at every block, masked
because:

- M0″ smoke oracle was last regenerated 2026-04-29 (before `42c597f`) and so
  on-disk shows `next_proposal_id = 0`, matching `42c597f`-era gtron until
  the rename. The replay never noticed because the oracle is regenerated
  from gtron itself.
- M0′ `00-genesis-dp-mainnet/fixture.json` captures only 76 java-tron
  getters and does not include `getLatestProposalNum`, so the M1.1 fixture
  test does not exercise this key.
- No live cross-impl run has exercised proposal create on the private chain
  cross-flows yet (no ProposalCreate in the 40-flow suite).

## Fix

Align gtron's semantics with java-tron:

1. Rename DP key `next_proposal_id` → `latest_proposal_num`, default `0`.
2. Rename getter/setter `NextProposalID/SetNextProposalID` →
   `LatestProposalNum/SetLatestProposalNum`.
3. `actuator/proposal_create.go`: pre-increment — `id = LatestProposalNum() + 1`
   then `SetLatestProposalNum(id)`.
4. `actuator/proposal_approve.go` / `proposal_delete.go`: bounds change
   `>= NextProposalID()` → `> LatestProposalNum()` (java-tron
   `ProposalApproveActuator.java:110`).
5. Tests: store first proposal at id=1 (not 0) — java-tron never assigns id 0.
6. Regenerate `test/fixtures/mainnet-blocks/smoke/` oracle via
   `go run ./scripts/fixtures/cmd/gen-smoke`.

## Validation

- `go test ./... -count=1` — full suite green.
- `make conformance-replay` — smoke range green after oracle regen.
- M0′ `00-genesis-dp-mainnet` fixture is unaffected (does not cover this key).
- Future cross-flows could add a ProposalCreate-bearing flow to catch
  regressions live.
