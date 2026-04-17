# Conformance replay harness

The harness replays pre-captured java-tron mainnet blocks through go-tron's
`core.ProcessBlock` and flags state divergences at field granularity. Design:
[`../superpowers/specs/2026-04-17-m0-double-prime-conformance-replay-design.md`](../superpowers/specs/2026-04-17-m0-double-prime-conformance-replay-design.md).

## Phases

M0″ splits naturally into two phases:

- **Phase 1 (delivered in-repo)** — replay engine (`core/conformance/`), replay
  CLI (`cmd/gtron-replay`), capture helpers (`cmd/fixture-closure`,
  `cmd/fixture-digest`), smoke corpus, and this document. Runs hermetically;
  no java-tron required.
- **Phase 2 (operator-driven, blocked on mainnet java-tron access)** — record
  three real mainnet ranges, populate divergence allowlists, run
  `scripts/system_test_cross.sh`. The allowlist is emptied by follow-up PRs
  against the known M1.5 / M1.8 parity gaps; M0″ exits cleanly only after
  that.

## Layout

```
test/fixtures/mainnet-blocks/
    smoke/                 # synthetic 5-block range (Phase 1)
    range-freeze-v2/       # Phase 2
    range-maintenance/     # Phase 2
    range-contract/        # Phase 2
```

Each range directory carries:

| File                        | What                                                          | Size             |
| ---                         | ---                                                           | ---              |
| `fixture.json`              | schema + java-tron jar sha + range metadata                   | <1 KB            |
| `seed.json`                 | state at StartBlock-1 for the range's touched-address closure | 1–10 MB          |
| `blocks.bin`                | varint-prefixed Block protos                                  | tens of MB (lfs) |
| `oracle.ndjson`             | per-block DigestB (+ optional DigestC diagnostic)             | small            |
| `divergence-allowlist.json` | known parity gaps whitelisted during bring-up                 | <10 KB           |

## Running replay

```bash
# Default: only the smoke range.
make conformance-replay

# All real mainnet ranges (requires `git lfs pull` first).
RANGES="range-freeze-v2 range-maintenance range-contract" make conformance-replay

# Enforce allowlist-empty + stale-free (the M0" exit gate).
make conformance-replay-exit-gate
```

Exit codes:

- `0` — every range passed; allowlist clean if `--exit-gate`.
- `1` — replay passed but allowlist has hits or stale entries (gate only).
- `2` — hard divergence (DigestB mismatch not covered by allowlist).
- `3` — harness error (missing fixture files, malformed seed/oracle).

## Regenerating the smoke range

Re-run only after intentional `ProcessBlock` semantics changes or fixture format bumps.

```bash
go run ./scripts/fixtures/cmd/gen-smoke
```

Output goes to `test/fixtures/mainnet-blocks/smoke/`; commit the resulting files.

## Capture protocol (Phase 2 — operator guide)

TRON's `wallet/getaccount` has no "at height" variant, so capture is
necessarily a "replay and snapshot" dance on the java-tron side. Phase 1
does not ship a bash script for this (it can't be meaningfully tested in
CI); operators with mainnet java-tron access are expected to write their
own driver following this protocol.

### Roles

- `cmd/fixture-closure` (this repo, Go) — derives the touched-address set
  from a pre-fetched `blocks.bin`. Output: JSON list of 41-hex addresses.
- `cmd/fixture-digest` (this repo, Go) — consumes a per-block capture
  snapshot JSON and emits one `OracleEntry` line. Used by the operator to
  build `oracle.ndjson` incrementally.
- Operator bash (NOT in this repo) — orchestrates java-tron: prune,
  restart, walk blocks, call HTTP endpoints, pipe into `fixture-digest`.

### One-time flow for a new range `<name>` covering blocks `[start..end]`

1. **Pre-fetch blocks** from any mainnet-synced java-tron via
   `wallet/getblockbynum` for every `h ∈ [start..end]`. Serialize each
   response `Block` proto via the protobuf library and write into
   `blocks.bin` as

   ```
   uvarint(len(proto))  ||  proto bytes  ||  next block...
   ```

   (same framing used by `core/conformance.openBlocksReader`). Store at
   `test/fixtures/mainnet-blocks/<name>/blocks.bin`.

2. **Compute closure** (touched address set for the range):

   ```bash
   go run ./cmd/fixture-closure --blocks=test/fixtures/mainnet-blocks/<name>/blocks.bin > closure.json
   ```

   Review the stderr warning list of unhandled ContractTypes; if any are
   present in your range, extend the switch in `core/conformance/closure.go`
   (TDD: add a test with a hand-built block, then the extraction) before
   continuing. Missing a closure address silently drops that account from
   the digest.

3. **Prune your mainnet java-tron to `start-1`**, restart it, wait for the
   chain head to settle at `start-1`. (java-tron's own rollback tool:
   `java -cp FullNode.jar org.tron.program.FullNode --db-rollback <start-1>`.
   Check current documentation — the exact invocation moves.)

4. **Dump the seed state** at `start-1` for the closure addresses. For
   each `addr` in `closure.json`:
   - HTTP: `POST wallet/getaccount {"address": "<addr>", "visible": false}`
     → base64-encode the full Account proto (or re-marshal from the JSON
     response using `google.protobuf`).
   - For any contract address, also HTTP `wallet/getcontract` for the
     runtime code bytes (hex) and fold into `code`.
   - HTTP: `wallet/getchainparameters` → map keyed by java-tron getters;
     translate each entry to the snake_case go-tron key (see
     `core/state/dp_key_mapping.go`'s `javaGetterToGoKeyMap`), or leave as
     getter keys — `LoadSeed` accepts both forms.

   Assemble into `seed.json` following the `core/conformance.Seed` schema
   (see `core/conformance/fixture_format.go`). Set `closureAddresses` to
   the closure list. The simplest path is to let each account go through
   the `raw` escape hatch with the full `anypb.Any` proto-JSON; that
   requires extending `applySeedAccount` to honor `raw`, a PR
   of its own — until then, limit `seed.json` to the fields the struct
   literally names (`balance`, `accountType`, `frozenV1Net`).

5. **Advance java-tron block-by-block**, dumping state after each block:

   ```
   for h in start..end:
       wait-for java-tron head == h          # check wallet/getnowblock
       build snapshot.json for this block:
           blockNum = h
           closure = <the 41-hex list>
           accounts/contractStates/code/dp = dump for every closure addr
       go run ./cmd/fixture-digest --mode=BC --input=snapshot.json \
           >> test/fixtures/mainnet-blocks/<name>/oracle.ndjson
   ```

   The snapshot JSON format is defined by `core/conformance.Snapshot`:

   ```json
   {
     "blockNum": 45000100,
     "accounts": [
       {"address": "41...", "accountProto": "<base64 of corepb.Account>"}
     ],
     "contractStates": [
       {"address": "41...", "contractStateProto": "<base64 of corepb.ContractState>"}
     ],
     "code": [
       {"address": "41...", "code": "<runtime bytecode hex>"}
     ],
     "dp": {
       "energy_fee": 420,
       "total_energy_current_limit": 50000000000
     },
     "closure": ["41...", "41..."]
   }
   ```

   `--mode=BC` is recommended during first-time capture — the embedded
   `diagC` makes it possible to diagnose divergences without going back
   to java-tron.

6. **Write `divergence-allowlist.json` as `[]`** (empty).

7. **Write `fixture.json`** with schema 1, scenario `<name>`,
   `javaTron.version` (read from `jar --version` or
   `wallet/getnowblockversion`), `javaTron.jarSha256` (from the running
   jar), `capturedAt`, `startBlock`, `endBlock`, `genesisTime`
   (mainnet genesis is `1529891469000`), and `activeWitnesses` (from
   the witness schedule at `start-1`).

8. **First replay pass:**

   ```bash
   RANGES=<name> make conformance-replay
   ```

   Expect divergences. For each, decide:
   - Real bug in go-tron → fix + re-replay.
   - Known parity gap (reward-v2 VI timing, freeze-v2 window size,
     lock/unlock key split) → add to
     `test/fixtures/mainnet-blocks/<name>/divergence-allowlist.json`:

     ```json
     [{
       "blockNum": 45000100,
       "field": "account:41...:balance",
       "reason": "known: reward v2 VI timing divergence at maintenance boundary",
       "trackingIssue": "internal:M1.5-vi-timing"
     }]
     ```

   - Under-captured closure (replay can't see some address the block
     actually touched) → extend closure + re-capture the range.

9. **Ship the range.** Commit `fixture.json`, `seed.json`,
   `divergence-allowlist.json`, `oracle.ndjson`. `blocks.bin` goes to
   git-lfs (see `.gitattributes`).

10. **Exit gate** (eventually):

    ```bash
    make conformance-replay-exit-gate
    ```

    Must return 0 — every range green AND every allowlist empty AND no
    stale entries. That's the M0″ closure condition and the green-light
    for G1.

## Interpreting a failure

When `gtron-replay` exits 2, the printed `Report` carries a
`first failure at block N` section listing `(field, got, want)` tuples.
Field paths follow:

- `account:<41-hex>:<proto-field-name>` — e.g. `account:41abc…:balance`
- `dp:<snake_case-key>` — e.g. `dp:total_energy_current_limit`
- `_digestB` — DigestB disagreed but DigestC couldn't explain; treat as
  a canonical-encoding issue (marshal ordering, proto field drift).
- `_processBlockError` — `core.ProcessBlock` panicked / errored before
  the digest got computed.

Use `--verbose` to dump the full got/want C-digest JSON for manual diffing.

## Allowlist policy

- Add entries only for divergences you've **explained and confirmed**
  are known upstream gaps. Don't blanket-allowlist a block.
- Every entry needs a `trackingIssue` (internal ticket or a note in
  PLAN.md/TODO.md that will drive the eventual fix).
- `expires` is optional but useful: an ISO date beyond which the entry
  is considered rot-candidate.
- **Empty allowlist is the goal.** A non-empty allowlist is a debt
  ledger; stale entries (never hit during replay) are a debt audit hit
  and should be removed.
