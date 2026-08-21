# Parallel/speculative VM adversarial audit — 2026-08-21

## Verdict

The canonical VM publisher is suitable for a fresh-genesis replay only after
the corrections in this audit. It remains a separate default-off opt-in. With
`--exec.parallel-vm` omitted, speculative VM observations may still run on
sampled blocks, but no VM worker post-image is admitted into canonical state.

When the flag is enabled, every block-start and asynchronous-retry VM candidate
must now pass an authoritative serial re-execution on a full copy of the exact
canonical transaction boundary. A failure is visible as a warning and falls
back before canonical mutation. This is intentionally a safety canary, not a
claim that VM publication currently provides a net execution speedup.

## Threat model and invariants

The review treated every speculative result as attacker-controlled until the
ordered canonical loop proved all of the following:

- the worker did not observe shared mutable canonical state or live raw KV;
- every typed, raw, code, storage, metadata, dynamic-property, range, and
  previous-sender dependency was either versioned exactly or failed closed;
- unknown reads, unsupported writes, stale incarnations, missing predecessors,
  and prefix/range uncertainty force serial execution;
- public bandwidth and block energy are recomputed at the real transaction
  boundary, not accepted from a block-start assumption;
- TransactionInfo, contract result, ordered WriteSet, BalanceTrace, raw writes,
  domain changes, and final state remain serial-equivalent;
- a publication error rolls back the block, and late workers cannot publish
  into a newer block or incarnation;
- the independent serial oracle does not share the optimized worker copy's
  cache-omission failure modes.

Java-tron remains the consensus source of truth. The oracle deliberately uses
go-tron's authoritative serial path, so full historical differential replay is
still the final release gate.

## Findings closed by this audit

| Severity | Finding | Correction | Regression evidence |
| --- | --- | --- | --- |
| High | Boundary-ready async VM retries could publish after version/preflight checks without passing the canonical serial oracle used by block-start VM candidates. An omitted worker dependency could therefore survive into canonical state. | Both canonical VM publishers now share `validateVMResultAtCanonicalBoundary`; mismatch/error restores resource overrides and falls back to serial before mutation. | Async-retry publication fixture requires exactly one oracle candidate/match and zero errors. |
| High | The serial oracle used `CopyBlockExecutionBase`, the same optimized copy family used for speculation. A cache omission could affect both executions and create a false match. | The oracle now uses full `StateDB.Copy()` at the exact canonical boundary. | A test removes the durable latest account row while the authoritative StateDB retains the correct cached account; the full-copy oracle still matches. |
| High | Both StateDB copy modes omitted the separate in-memory witness cache. An unflushed same-block witness mutation could be reloaded from stale durable state. | `newCopyBase` deep-copies witnesses and dirty-witness membership. | Full and optimized copies retain the unflushed witness view and cannot mutate the source. |
| High (operational) | `--exec.parallel-transfers` silently admitted VM publications, so operators could not enable Transfer optimization while keeping VM canonical execution serial. | Added independent default-off `--exec.parallel-vm` / `GTRON_EXEC_PARALLEL_VM`; Transfer-only mode is verified to publish zero VM results. | Flag tests plus serial-equivalent TransactionInfo, BalanceTrace, balances, storage/dynamic state, and final root in Transfer-only mode. |
| Medium (observability) | Serial-oracle rejection was visible only through counters. | Unexpected oracle error/mismatch emits a structured WARN with block, transaction index/hash, comparison classes, and `action=serial-fallback`. | Normal conflicts remain counter-only; the warning is reserved for a violated zero-mismatch gate. |

## Cross-check coverage

The adversarial pass traced and cross-checked:

- block-start optimized StateDB construction, cached accounts/code, storage
  generations, witness state, dirty state, and raw overlay isolation;
- typed account/storage/code/metadata/dynamic-property reads and writes,
  generic raw keys, immutable strict block-hash reads, and prefix/range access;
- exact latest-writer validation, sender-chain forwarding, predecessor
  publication, barriers, commutative deltas, retry incarnations, deadline-ready
  selection, stale/late draining, and worker reclamation;
- expected `contract_ret`, complete TransactionInfo, BalanceTrace, public-net
  reservation/rebase, block-energy settlement, WriteSet preflight/application,
  domain-change flush, post-publication audit, and whole-block rollback;
- all canonical VM publication call sites and their operational flag gates.

No additional fail-open canonical VM publication route was found after the
shared oracle gate was installed.

## Verification completed

- focused VM/state regression tests passed;
- the VM publication/oracle/retry tests passed 30 consecutive runs;
- focused `go test -race` passed three consecutive runs;
- `go vet ./cmd/gtron ./core/... ./vm/... ./actuator/...` passed;
- native and Linux/amd64 `cmd/gtron` builds passed;
- full repository tests passed with `go test ./... -count=1 -timeout 300s`;
- complete-copy benchmark: roughly 0.64–0.87 ms and 341 KiB per copy on the
  audit host; worker block-execution copy remained roughly 14–24 µs and 3.2
  KiB. Only publishable VM candidates pay the full-copy oracle cost.

The external java-tron integration-tag suite was not run locally because this
audit environment did not provide a dedicated java-tron integration endpoint.
The fresh mainnet replay is therefore part of the acceptance evidence, not a
post-acceptance formality.

## Fresh-genesis replay gate

The most conservative first replay is:

1. keep `--exec.parallel-transfers` if desired;
2. omit `--exec.parallel-vm`, proving the corrected serial canonical VM path
   through the historically divergent heights;
3. enable `--exec.parallel-vm` only for a separately observed canary replay or
   after serial checkpoint parity is established.

If VM publication is intentionally enabled, require:

- `core/parallel_vm/enabled = 1`;
- `core/parallel_vm/serial_verify/candidates = matches`;
- serial-oracle info, WriteSet, BalanceTrace mismatch counters and errors all
  remain zero;
- `core/parallel_vm/errors = 0`;
- async retry frozen-raw misses, `contract_ret/version_clean`,
  `contract_ret/invalid`, apply/audit mismatch, resource projection mismatch,
  and publication-error counters remain zero;
- no `Speculative VM result rejected by canonical serial oracle` warning;
- no historical guarded repair activates on a clean replay.

Compare database/receipt parity around at least 18,402,304, 18,414,381,
20,616,256, 20,674,403, and 20,674,405 before allowing the sync to proceed to
the next checkpoint. Any mismatch is a stop condition: preserve the datadir and
logs for differential analysis rather than applying another repair.

## Residual risk

- A full genesis-to-head mainnet replay is the only practical exercise of all
  historical fork combinations and contract bytecode. Unit/race tests cannot
  replace it.
- The serial oracle is independent of speculative copying and retained output,
  but it is still the same Go consensus implementation rather than java-tron.
  Java-tron checkpoint comparison remains mandatory.
- WriteSet completeness depends on the StateDB journal/recorder covering every
  future mutation family. New actuator, VM opcode, precompile, raw accessor, or
  fork behavior must fail closed or extend recorder and adversarial coverage
  before VM publication is widened.
- Because every publishable VM candidate is serially re-executed, the current
  design favors corruption detection over throughput. Removing or sampling the
  oracle requires a new production proof and must not be inferred from this
  audit.
