# M0″ — Conformance Replay Harness (Design)

**Status**: draft → awaiting user review
**Date**: 2026-04-17
**Milestone**: M0″ (PLAN.md §M0″, G1 exit gate)
**Prereq**: M0′, M1.1, M1.3, M1.4, M1.5, M1.7, M1.8 (all landed)

## 1. Motivation

go-tron's state layer has now landed the six heaviest divergence sources (adaptive
energy, reward v2, freeze V1 legacy, dynamic energy, delegation usage, DP backfill +
version-bit fork gates). At this point mainnet replay will produce meaningful signal
rather than noise — the failure modes left are specific, documented, and testable.

M0″ builds the harness that converts "state-layer parity" from a claim into an
automated exit gate for **G1 (mainnet follower)**: three representative mainnet
block ranges replay cleanly through go-tron's `ProcessBlock`, each block's
post-state matches java-tron's post-state, and no divergences remain on the
allowlist.

## 2. Non-goals

- Deep-history replay from genesis. We synthesize per-range seed state; we do
  not port java-tron's full chaindata into Pebble.
- Trie-root comparison. TRON is not Merkle-trie based; we compare account
  field sets + DP key/value sets, not a 32-byte state root.
- Performance benchmarking. Replay speed is bounded by `ProcessBlock`; we
  don't optimize the harness below "runs in minutes per range".
- Live mainnet follower. A standing always-on follower is a follow-on item
  under "Cross-cutting / Meta" in TODO.md; M0″ delivers the harness it would
  use, not the follower itself.

## 3. Architecture

Three disjoint pipelines sharing one fixture format. Only replay runs on CI.

```
┌─── Capture (manual, operator) ───────────────────────────┐
│ java-tron mainnet ──gRPC──► test/fixtures/mainnet-blocks/│
│                             <range>/                     │
│                               ├─ blocks.bin   (lfs)     │
│                               ├─ seed.json              │
│                               ├─ oracle.ndjson          │
│                               └─ divergence-allowlist   │
└──────────────────────────────────────────────────────────┘

┌─── Replay (CI, hermetic) ────────────────────────────────┐
│ seed.json → fresh Pebble/mem StateDB+DP                  │
│ blocks.bin → iterate BlockMessages                       │
│   for each block: ProcessBlock → DigestB → compare       │
│                     mismatch → allowlist? continue :     │
│                                 DigestC → exit nonzero   │
└──────────────────────────────────────────────────────────┘

┌─── Cross e2e (manual) ───────────────────────────────────┐
│ 1 gtron SR + 1 java-tron SR, P2P-connected               │
│ scripts/system_test_cross.sh drives txs, compares state  │
└──────────────────────────────────────────────────────────┘
```

The replay engine is a pure library (`core/conformance/`) taking seed + blocks
+ oracle and returning a report. A thin `cmd/gtron-replay` binary and
`scripts/conformance_replay.sh` wrap it. This keeps the engine unit-testable
with synthetic scenarios, and keeps capture (which needs a running java-tron)
distinct from replay (which must not).

## 4. Decisions

| # | Decision | Alternatives considered | Reason |
|---|---|---|---|
| D1 | **Per-range seed state synthesized from java-tron fixture**, not full chaindata import | Genesis replay (infeasible at 70M+ blocks); full snapshot port (milestone-sized by itself) | Reuses M0′ fixture infra; each of the three ranges has a bounded touched-account set |
| D2 | **Recorded corpus** checked in via git-lfs under `test/fixtures/mainnet-blocks/<range>/` | Live-fetch from a running java-tron on each run | Replay must be hermetic for CI; capture is a one-time operator step |
| D3 | **Per-block B-digest** (hash of touched-account + DP) as primary signal; fall back to **C-digest** (full JSON dump) on mismatch for diagnostics | End-of-range only (no divergence localization); always-C (bloated oracle) | B identifies the first diverging block cheaply; C is only emitted when we need it |
| D4 | **Allowlist-driven divergence handling** — per-field exceptions allowed during bring-up; **M0″ exit requires allowlist empty across all three ranges** | Strict from day one (would block M0″ on M1.5/M1.8 known gaps); permanent allowlist (meaningless parity claim) | Matches PLAN.md exit criterion ("state root 一致"); allowlist is a debt ledger, not a permanent carve-out |
| D5 | **Propose + confirm** concrete range heights rather than hard-coding now | Let user pick; defer entirely | Avoids bikeshed while letting user redirect on specific heights |

## 5. Components

### 5.1 Engine — `core/conformance/` (Go library)

| File | Responsibility |
|---|---|
| `seed.go` | `LoadSeed(path) → (*StateDB, *DynamicProperties)`. Parses `seed.json`, constructs fresh in-memory Pebble + StateDB + DP, seeds accounts/contracts/DP keys. No disk, no network. |
| `digest.go` | `DigestB(statedb, addrs, dp) → [32]byte` — canonical sha256 over: `sorted(addrs)` concatenated with each account's protobuf-canonical `Account`/`Contract`/`ContractState` bytes, followed by `sorted(dp-keys)` and their proto-encoded values. `DigestC(statedb, addrs, dp) → json.RawMessage` — same data emitted as structured JSON for human diffing. The `addrs` set is fixed per range (see §6); it does not depend on per-block dirty tracking. |
| `allowlist.go` | `LoadAllowlist(path) → Allowlist` with `IsWhitelisted(blockNum, field) bool`. |
| `replay.go` | `ReplayRange(seed, blocksReader, oracleReader, allowlist) → *Report`. Drives `ProcessBlock` per block, gathers touched set from StateDB journal, compares B, escalates to C on mismatch, consults allowlist, stops at first hard divergence. |
| `report.go` | `Report { Passed, BlockResults, FirstFailure, AllowlistHits, StaleAllowlistEntries }` + `String()` for human consumption. |

### 5.2 Fixture format — `test/fixtures/mainnet-blocks/<range>/`

| File | Content | Size |
|---|---|---|
| `fixture.json` | schema version + java-tron jar sha + capture timestamp (mirrors M0′ provenance) | <1 KB |
| `blocks.bin` | length-prefixed `BlockMessage` protos (`varint len ‖ bytes`) | 50–200 MB per range; git-lfs |
| `seed.json` | `{ schema, javaTronVersion, startHeight, dp: {...}, accounts: [...], contracts: [...] }` | 1–10 MB |
| `oracle.ndjson` | one JSON per line: `{ blockNum, digestB: hex32, diagC: optional }` | ~N × 100 B |
| `divergence-allowlist.json` | `[{ blockNum, field, reason, trackingIssue, expires }]`; empty for exit | <10 KB |

### 5.3 Capture — `scripts/fixtures/`

- `capture_range.sh <name> <start> <end>` — entry point. Starts temp java-tron against mainnet snapshot, drives capture via `GetBlockByNum` + `GetAccount` + DP dump.
- `lib/capture.sh` — helpers: touched-address closure computation (walks each block's contracts by type), per-height state pull, oracle writer.

### 5.4 Replay CLI — `cmd/gtron-replay/`

- `main.go` — flags: `--range=<dir>`, `--mode=fast|full` (fast = B only; full = B then C always), `--no-allowlist` (for CI's exit-gate invocation).
- Exit code: `0` = clean pass, `1` = divergence (possibly allowlisted), `2` = hard divergence (allowlist didn't cover), `3` = harness error (corpus missing, seed malformed).
- Prints `Report.String()`; on hard divergence dumps a unified-diff-style view of the C-digest.

### 5.5 Orchestration — `scripts/conformance_replay.sh`

Iterates three corpus dirs, runs `gtron-replay` on each, prints a summary table. When invoked as an exit-gate check (`--exit-gate`), requires every allowlist empty and every range returning `0`.

### 5.6 Cross e2e — `scripts/system_test_cross.sh`

Reuses `system_test.sh` structure but second node is java-tron:

- Env: `FULLNODE_JAR` (from M0′), `JAVA` (JDK 17).
- Scenarios: transfer, contract deploy+call, vote+reward, Freeze-V2 delegate+undelegate.
- Each scenario asserts both nodes agree on block hash + account state after confirmation.
- Not in `make test`; runs under `make system-test-cross`.

### 5.7 Docs — `docs/dev/conformance-harness.md`

Operator guide: capture a new range, run replay locally, interpret a failure, add/remove an allowlist entry, validate a parity-fix empties an entry.

## 6. Data flow

**Capture (operator, one-time per range)**:

```
capture_range.sh <name> <start> <end>
  │
  │ 1. boot temp java-tron at mainnet snapshot
  │ 2. GetBlockByNum(start..end) → blocks.bin
  │ 3. compute touched-closure = union of addresses referenced by any
  │    tx in any block across [start..end] (parsed from each
  │    ContractType: owner, receiver, contract callee, etc.)
  │    → this single address set is the digest input for every block
  │    in the range; both capture and replay digest the same addrs
  │ 4. GetAccount/GetContract at height start-1 for closure → seed.json (+ DP dump)
  │ 5. for each h in start..end:
  │      read java-tron state for `closure` addrs at height h
  │      B = DigestB(addrs=closure, dp=java-tron-dp-at-h)
  │      optionally C = DigestC(...)
  │      oracle.ndjson.append({ h, B, [C] })
  │ 6. fixture.json ← { schema, jarSha, capturedAt }
  │ 7. shutdown java-tron
  ▼
test/fixtures/mainnet-blocks/<name>/
```

The range-wide touched closure is fixed at capture time. Replay doesn't
need go-tron's StateDB to expose a per-block dirty set — it simply reads
the same `closure` addresses after each block. This keeps the engine
independent of StateDB internals.

**Replay (CI / dev, every run)**:

```
gtron-replay --range=<dir>
  │
  │ 1. LoadSeed → fresh mem Pebble + StateDB + DP
  │ 2. open blocks.bin, oracle.ndjson, allowlist.json
  │ 3. closure = addrs enumerated in seed.json (range-wide touched set)
  │    for each block in blocks.bin:
  │      ProcessBlock(statedb, dp, block)
  │      if DigestB(statedb, closure, dp) != oracle[i].digestB:
  │          C_got  = DigestC(statedb, closure, dp)
  │          C_want = oracle[i].diagC or re-captured from allowlist
  │          diff = json-diff(C_got, C_want)
  │          if all diff fields ∈ allowlist[blockNum]:
  │              report.AllowlistHits += diff; continue
  │          else:
  │              report.FirstFailure = { block, diff, got, want }; stop
  │ 4. emit Report.String(); exit with code per §5.4
```

## 7. Error handling

| Failure | Detection | Action |
|---|---|---|
| Corpus absent (git-lfs not pulled) | `blocks.bin` missing or size ≠ lfs pointer | Abort early with pointer to `docs/dev/conformance-harness.md` |
| Seed under-capture (account not in `seed.json` but referenced by block 0) | StateDB returns zero-account for known-needed addr | Abort with list of missing addresses; fix = rerun capture with extended closure |
| `ProcessBlock` panics mid-range | recover in replay loop | Treat as divergence at that block; last good block + stack trace in report |
| B mismatch, C diff not in allowlist | default path | Emit JSON diff, exit code 2 |
| B mismatch, C diff fully in allowlist | default path | Log, increment `AllowlistHits`, continue |
| Allowlist entry never matched across range | post-run check | Warn in report (`StaleAllowlistEntries`); does not fail the range by itself, but fails `--exit-gate` if present |

## 8. Testing

### 8.1 Unit (`make test`)

- `seed_test.go` — load a tiny hand-written `seed.json`, assert StateDB/DP mirrors it.
- `digest_test.go` — `DigestB` determinism (permutation invariance, byte-for-byte stability), `DigestC` round-trips.
- `allowlist_test.go` — lookup semantics, stale-entry detection.
- `replay_test.go` — synthetic 5-block range produced in-process (via `producer`), oracle computed by the engine itself on a reference StateDB, allowlist edge cases.

### 8.2 Smoke corpus (`make test`)

`test/fixtures/mainnet-blocks/smoke/` — 5-block synthetic range produced by gtron and dumped through the capture path (runs without git-lfs, without java-tron). Catches harness regressions.

### 8.3 Mainnet corpus (`make conformance-replay`)

Three ranges, git-lfs tracked:

- **Range-1 / Freeze V2 activation** — around proposal #62 activation (Oct 2022, mainnet block ≈ 45.3M). Window `[activation − 50, activation + 450]`. Exercises the V1/V2 consumption fork gate, freeze/unfreeze weight sync, and related DP rollovers.
- **Range-2 / Maintenance boundary** — a recent 6-hour cycle rollover. Window `[boundary − 20, boundary + 480]`. Exercises reward v2 (VI accumulation, brokerage split, cycle advance, standby distribution) — the M1.5 path most likely to surface known VI-timing parity gaps.
- **Range-3 / Contract-dense** — a recent USDT-heavy block window, `[N, N + 100]`. Exercises TVM, dynamic energy factor catch-up, and Freeze-V2 delegation through opcodes `0xDE/0xDF` — the M1.7/M1.8 surface.

Concrete heights proposed but not yet confirmed; finalized during PR-4.

### 8.4 Cross e2e (`make system-test-cross`)

`scripts/system_test_cross.sh` — 1 gtron SR + 1 java-tron SR (needs `FULLNODE_JAR`). Scenarios: transfer, contract deploy+call, vote+reward, freeze-v2 delegate+undelegate.

## 9. Milestone sequencing (5 PRs)

| PR | Scope | Exit |
|---|---|---|
| **PR-1** | `core/conformance/` engine + unit tests with synthetic scenarios | `make test` green; engine usable from Go code |
| **PR-2** | `cmd/gtron-replay` + `scripts/conformance_replay.sh` + `test/fixtures/mainnet-blocks/smoke/` | Harness replays smoke range end-to-end |
| **PR-3** | `scripts/fixtures/capture_range.sh` + format doc stub | Operator can produce a smoke range from java-tron |
| **PR-4** | Record 3 real mainnet ranges; populate allowlist with all observed divergences; commit analysis notes | Harness runs on 3 ranges; allowlist catalogs every remaining parity gap |
| **PR-5** | `scripts/system_test_cross.sh` + `docs/dev/conformance-harness.md` | Cross e2e runs; documentation complete |

**M0″ done** = PR-5 merged AND all three `divergence-allowlist.json` files empty (the allowlist emptying is driven by follow-up PRs patching the parity gaps, not by M0″ itself).

## 10. Known parity gaps the harness must surface

These are tracked as today-expected divergences; each gets an allowlist entry in PR-4 with a pointer to its follow-up fix:

- **Reward v2 VI timing** (M1.5 known gap): go-tron uses immediate-vote-application counts; java-tron uses pre-countVote counts at the maintenance boundary. Divergence visible at Range-2's maintenance tick only if votes change during the cycle.
- **Freeze-V2 per-account window sizes** (M1.8 known gap): go-tron uses global 24h window; java-tron has per-account window reshuffled via `getNewWindowSize`. Visible in Range-3 only for accounts with non-default windows.
- **Freeze-V2 lock/unlock delegation key split** (M1.8 known gap): go-tron uses single `DelegatedResource` record; java-tron splits into `createDbKeyV2(owner, receiver, lock)`. Visible in Range-3 only for locked delegations.
- **GR withdrawal check** (pre-M1.5 note): verify behavior aligns; if not, allowlist entry.

The allowlist is the audit trail for these. M0″ exit flips each back to "fixed" via subsequent PRs.

## 11. Open questions

None blocking design approval. Deferred to PR-4:

- Exact block-number windows for the three ranges.
- git-lfs vs out-of-tree storage if the 3-range corpus lands > 1 GB combined (current estimate: ~150–600 MB).
- Whether to include C-digests in every oracle entry (bloats `oracle.ndjson`) or reconstruct on-demand from the capture script's cached state (requires capture to be re-runnable; simpler default).

## 12. References

- PLAN.md §M0″
- TODO.md §6 (Cross-cutting / Meta)
- `docs/superpowers/specs/2026-04-15-fixture-extraction-design.md` (M0′ fixture tooling)
- `docs/dev/fixture-tooling.md`
- `docs/dev/java-tron-local.md`
- java-tron `ChainBaseManager`, `DynamicPropertiesStore`, `AccountStore`
