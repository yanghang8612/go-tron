# VM tracing hooks — plan

**Spec:** [2026-05-19-vm-tracing-hooks-design.md](../specs/2026-05-19-vm-tracing-hooks-design.md)

## Slice 1 — Hook surface + audit

- [ ] `core/tracing/hooks.go` — `Hooks` struct with all 18+ function
      fields per the spec
- [ ] `core/tracing/reasons.go` — `BalanceChangeReason`, `GasChangeReason`
      enums
- [ ] Audit every TVM opcode dispatch site in `vm/interpreter.go` +
      `vm/instructions*.go`; list them in `docs/dev/tracing-audit.md`
- [ ] Audit every `state.StateDB` mutation site (`SetBalance`,
      `AddTRC10Balance`, `SetState`, `SetCode`, etc.); same audit doc

## Slice 2 — Wire hooks into state.StateDB

- [ ] `state.StateDB.SetHooks(*tracing.Hooks)` setter (default nil)
- [ ] Hook firing in every mutation site identified in slice 1 audit
- [ ] Tests: a tracer that records every callback, exercise each
      mutation API once, assert callback fired
- [ ] Hot-path bench: confirm < 1% overhead with `hooks=nil`

## Slice 3 — Wire hooks into TVM interpreter

- [ ] Hook firing in `vm/interpreter.go::Run` (OnEnter/OnExit/OnOpcode/OnFault)
- [ ] Hook firing in `vm/precompile_*.go` (CALL into precompile triggers
      OnEnter/OnExit pair)
- [ ] Hook firing for system-call boundaries (rare; mostly for VM
      precompile vs user contract distinction)
- [ ] Tests: a recording tracer that drives a known multi-CALL contract,
      assert OnEnter/OnExit nesting matches expected depth pattern
- [ ] Bench: tracer disabled, confirm hot-path overhead remains < 1%

## Slice 4 — Wire hooks into state_processor + actuator

- [ ] `core/state_processor.go::applyTransaction` fires OnTxStart/OnTxEnd
- [ ] Each actuator (`actuator/*.go`) fires the relevant TRON-specific
      hook on its primary state mutation:
  - TransferContract → OnBalanceChange (already covered by StateDB hook
    via slice 2; just verify)
  - VoteWitnessContract → OnWitnessVote
  - FreezeBalance*Contract / Unfreeze* → OnFreeze
  - ShieldedTransferContract → OnShieldedReceive (per recv cm)
  - VMActuator → OnEnergyBill at PayEnergyBill time
- [ ] Tests: per-actuator recorder verifying the right hook fires

## Slice 5 — Tracer package + prestate tracer

- [ ] `core/tracers/tracer.go` — Tracer interface + registry
- [ ] `core/tracers/prestate/prestate.go` — accumulates touched account
      pre-state + (storage_addr, slot) pre-values
- [ ] Cross-impl test: pick 10 Nile txs known to diverge in past stage-C
      runs; assert prestate output matches java-tron archive

## Slice 6 — Additional tracers

- [ ] `core/tracers/struct_log/struct_log.go` — geth-format
      step-by-step trace (PC / OP / gas / stack / memory / storage)
- [ ] `core/tracers/call/call.go` — call-frame tree
- [ ] `core/tracers/energy/energy.go` — TRON-specific energy breakdown
      (caller-stake / origin-stake / balance-paid)
- [ ] Cross-impl: structLog output for a 100-tx sample matches
      java-tron vmTrace byte-for-byte (modulo whitespace)

## Slice 7 — RPC integration

- [ ] `internal/jsonrpc/debug.go` — handlers for
      `debug_traceTransaction`, `debug_traceBlockByNumber`,
      `debug_traceBlockByHash`, `debug_storageRangeAt`
- [ ] Re-execution at parent block depends on
      [state-history-index](./2026-05-19-state-history-index.md) — wire
      both together for archive support
- [ ] JSON config parsing: `{tracer: "..."}` selects tracer; `{tracerConfig:
      {...}}` passes options
- [ ] Tests against ethclient-compatible test fixtures

## Acceptance criteria

- [ ] Hot-path bench: < 1% overhead vs main, tracer disabled
- [ ] All four tracers work end-to-end on a fresh sync
- [ ] structLog output byte-identical to java-tron vmTrace on the
      cross-impl sample
- [ ] `make test` green; race detector clean
