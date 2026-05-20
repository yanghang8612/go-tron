# VM tracing hooks — design

**Status:** Proposed
**Author:** yanghang8612
**Date:** 2026-05-19
**Inspiration:** [go-ethereum/core/tracing/hooks.go](../../../../ethereum/go-ethereum/core/tracing/hooks.go), [eth/tracers/](../../../../ethereum/go-ethereum/eth/tracers/)
**Related plan:** [2026-05-19-vm-tracing-hooks.md](../plans/2026-05-19-vm-tracing-hooks.md)

## Background

[`core/blockchain.go`](../../../core/blockchain.go) today exposes only
**block-level** hooks:

- `AddBlockHook(fn func(*types.Block))` — fired post-`InsertBlock`
- `AddApplyStatsHook(fn func(*types.Block, ApplyStats))` — per-phase
  timing

Block-level hooks cover broadcast, metrics, sync stats. They do **not**
cover anything inside a tx — no opcode trace, no per-call frame, no
storage diff. As a result:

- `debug_traceTransaction` cannot be implemented; we'd need a re-execution
  path with a `Tracer` injected into the VM
- `debug_traceBlockByHash`, `debug_storageRangeAt`, prestate / callTracer —
  none feasible
- Cross-impl divergence triage (the dailyBuild work) ends up using
  java-tron's vmTrace and there's no gtron-side equivalent

go-ethereum's `core/tracing/hooks.go` ships ~15 callbacks the VM and state
transition fire as they execute:

```go
type Hooks struct {
    OnTxStart       func(env *VMContext, tx *types.Transaction, from common.Address)
    OnTxEnd         func(receipt *Receipt, err error)
    OnEnter         func(depth int, typ byte, from, to common.Address, ...)
    OnExit          func(depth int, output []byte, gasUsed uint64, err error, reverted bool)
    OnOpcode        func(pc uint64, op byte, gas, cost uint64, scope *OpContext, ...)
    OnFault         func(pc uint64, op byte, gas, cost uint64, scope *OpContext, depth int, err error)
    OnGasChange     func(old, new uint64, reason GasChangeReason)
    OnBalanceChange func(addr common.Address, prev, new *big.Int, reason BalanceChangeReason)
    OnNonceChange   func(addr common.Address, prev, new uint64)
    OnCodeChange    func(addr common.Address, prevCodeHash common.Hash, prev []byte, codeHash common.Hash, code []byte)
    OnStorageChange func(addr common.Address, slot common.Hash, prev, new common.Hash)
    OnLog           func(log *Log)
    OnBlockStart    func(event BlockEvent)
    OnBlockEnd      func(err error)
    OnSystemCallStart func()
    OnSystemCallEnd func()
    ...
}
```

`Hooks` is a struct of function pointers — nil means "not subscribed".
Hot paths in the EVM check `if hooks != nil && hooks.OnOpcode != nil`
before invoking. Zero overhead when no tracer is attached.

This spec ports the hook surface to gtron's TVM (forked EVM) + state
transition.

## Goals

- Functional `debug_traceTransaction`, `debug_traceBlock*`,
  `debug_storageRangeAt` (operates with state-history-index for past blocks)
- Functional prestateTracer, callTracer, structLog tracer parity with geth
- Cross-impl byte-identical VM trace output vs java-tron's vmTrace (when
  the same trace config is requested)
- Zero overhead on the hot path when no tracer attached
- Tracer code lives in `core/tracers/` (parallel to geth's `eth/tracers/`)
  so adding new tracers doesn't touch VM internals

## Non-goals

- Do NOT add tracers as a wire-protocol concern (these are RPC-only)
- Do NOT change TVM execution semantics. Hooks are observer-only.
- Do NOT slow the non-tracing hot path. Every hook check must compile to
  a nil-pointer compare + branch.
- Do NOT lift geth's entire `eth/tracers/` directory verbatim — TRON's
  state model differs (TRC10, energy, freezes). Port the **structure**;
  rewrite specific tracers against TRON semantics.

## Hook surface

`core/tracing/hooks.go`:

```go
type Hooks struct {
    // Block lifecycle
    OnBlockStart func(block *types.Block)
    OnBlockEnd   func(err error)

    // Tx lifecycle
    OnTxStart    func(tx *types.Transaction, from common.Address)
    OnTxEnd      func(receipt *corepb.TransactionInfo, err error)

    // VM call frames
    OnEnter      func(depth int, callType vm.CallType, from, to common.Address, input []byte, gas uint64, value int64)
    OnExit       func(depth int, output []byte, gasUsed uint64, err error, reverted bool)

    // VM execution
    OnOpcode     func(pc uint64, op byte, gas, cost uint64, scope vm.OpContext, depth int, err error)
    OnFault      func(pc uint64, op byte, gas, cost uint64, scope vm.OpContext, depth int, err error)
    OnGasChange  func(old, new uint64, reason GasChangeReason)

    // State mutations
    OnBalanceChange func(addr common.Address, prev, new int64, reason BalanceChangeReason)
    OnTRC10Change   func(addr common.Address, assetID int64, prev, new int64, reason BalanceChangeReason)
    OnCodeChange    func(addr common.Address, prevHash, newHash common.Hash, prevCode, newCode []byte)
    OnStorageChange func(addr common.Address, slot common.Hash, prev, new common.Hash)

    // Logs (event log emitted by the VM)
    OnLog func(log *types.Log)

    // TRON-specific
    OnWitnessVote   func(voter, witness common.Address, prevCount, newCount int64)
    OnFreeze        func(addr common.Address, resource vm.ResourceType, prevAmount, newAmount int64)
    OnShieldedReceive func(addr common.Address, cm []byte)
    OnEnergyBill    func(payer common.Address, fromStake, fromBalance int64)
}
```

### TRON-specific extensions

go-ethereum's hook set is EVM/account-centric. TRON adds:

- **TRC10**: separate from native balance — explicit hook
- **Resources**: freeze / unfreeze / delegate are common; track per-resource
- **DPoS**: votes mutate witness counters; tracers need visibility
- **Shielded**: cm append events for shielded-aware tracers
- **Energy billing**: `OnEnergyBill` fires once per tx with the
  caller/origin split (mirrors java-tron's receipt.callerEnergyLeft /
  originEnergyLeft surface)

## Wiring

### VM interpreter

`vm/interpreter.go` (today's `Run()` loop):

```go
func (in *Interpreter) Run(contract *Contract, input []byte, readOnly bool) (ret []byte, err error) {
    // ... existing setup ...
    hooks := in.evm.Config().Hooks      // *tracing.Hooks; may be nil
    if hooks != nil && hooks.OnEnter != nil {
        hooks.OnEnter(in.depth, ...)
    }
    defer func() {
        if hooks != nil && hooks.OnExit != nil {
            hooks.OnExit(in.depth, ret, gasUsed, err, reverted)
        }
    }()

    for {
        pc, op := pc, contract.GetOp(pc)
        gasCopy, costCopy := contract.Gas, cost
        // ... execute op ...
        if hooks != nil && hooks.OnOpcode != nil {
            hooks.OnOpcode(pc, op, gasCopy, costCopy, scope, in.depth, err)
        }
        if err != nil {
            if hooks != nil && hooks.OnFault != nil {
                hooks.OnFault(pc, op, gasCopy, costCopy, scope, in.depth, err)
            }
            break
        }
        pc++
    }
}
```

Every `if hooks != nil && hooks.OnX != nil` is a 2-pointer-compare + branch.
On the no-tracer path (the production hot path) this is one predicted
branch per opcode. Modern CPUs eat this in <1ns; the cost is negligible
against the opcode dispatch.

### state.StateDB

Hooks for balance / TRC10 / storage / code mutations live in `StateDB`:

```go
func (s *StateDB) SetBalance(addr common.Address, value int64, reason BalanceChangeReason) {
    obj := s.getOrNewStateObject(addr)
    prev := obj.Balance()
    obj.SetBalance(value)
    if s.hooks != nil && s.hooks.OnBalanceChange != nil {
        s.hooks.OnBalanceChange(addr, prev, value, reason)
    }
}
```

`BalanceChangeReason` enum mirrors geth: `TransferTo, TransferFrom,
ContractCreation, BlockReward, Refund, EnergyFeeBurn, ...`.

### state_processor wiring

`core/state_processor.go::applyTransaction`:

```go
hooks := ctx.Hooks                       // nil unless an RPC trace request set them
if hooks != nil && hooks.OnTxStart != nil {
    hooks.OnTxStart(tx, fromAddr)
}
defer func() {
    if hooks != nil && hooks.OnTxEnd != nil {
        hooks.OnTxEnd(receipt, err)
    }
}()
// ... existing actuator dispatch ...
```

## Tracer package

`core/tracers/`:

```
core/tracers/
  tracer.go               # Tracer interface + registry
  prestate/prestate.go    # prestateTracer
  call/call.go            # callTracer
  struct_log/struct_log.go # default structLog (geth's debug_traceTransaction default)
  energy/energy.go        # TRON-specific energy breakdown tracer
```

Each tracer implements:

```go
type Tracer interface {
    Hooks() *tracing.Hooks
    Result() (json.RawMessage, error)   // serialize collected trace
    Stop(err error)                     // cancellation
}
```

RPC method registration:

```go
// internal/jsonrpc registered:
//   debug_traceTransaction(txhash, config) -> {tracer: "structLog"|"prestate"|"call"|"energy", ...}
//   debug_traceBlockByNumber(blockNum, config)
//   debug_traceBlockByHash(blockHash, config)
//   debug_storageRangeAt(blockNum, txIdx, addr, startKey, limit)
```

Each method:

1. Parses config to pick tracer
2. Re-opens StateDB at parent block (via state-history-index → see
   [state-history-index spec](./2026-05-19-state-history-index-design.md))
3. Re-executes the tx with `ctx.Hooks = tracer.Hooks()`
4. Returns `tracer.Result()`

## java-tron parity

java-tron's vmTrace JSON has a specific shape (per-step `pc / op / gas /
gasCost / depth / stack / memory / storage`). The `structLog` tracer
produces the same fields in the same order. Cross-impl tests will replay
known divergence cases (the stage-C dailyBuild captures) and assert
byte-identical structLog JSON, modulo whitespace + key order.

The `prestate` tracer is the most useful for cross-impl triage: dumps
**every account/storage touched** by the tx with pre-execution values.
java-tron does not have a direct equivalent, but the same data is
derivable from their state-history; we make this gtron-side cheap.

## Acceptance criteria

- Hot path overhead measurable < 1% on `BenchmarkProcessBlock_HeavyTRX`
  (tracer disabled)
- `debug_traceTransaction(prestateTracer)` returns matching values vs
  java-tron archive for a set of known txs
- `debug_traceTransaction(structLog)` produces identical step-by-step
  trace vs java-tron's vmTrace (a 100-tx sample set verified
  byte-identical)
- All hook callbacks fire exactly once per event (no missing /
  duplicate)
- Race detector clean

## Risks

- TVM opcode coverage drift: every existing opcode (and every
  go-tron-specific one — `instructions_tron.go`) must fire `OnOpcode`.
  Easy to miss one in a refactor. Slice 1 audit lists every dispatch
  site.
- Hook ordering: `OnBalanceChange` for a TransferContract may fire
  before or after `OnTxStart` depending on where in actuator the
  mutation lands. Spec: hooks fire in **logical-order** matching java's
  trace order, not lexical-order of our Go code.
- Tracer mutation: a buggy tracer that holds onto VM scope pointers
  past `OnExit` would observe garbage stack/memory. Tracer interface
  doc must warn; defensive copy inside `OnOpcode` for stack/memory if
  the tracer requests it.

## Out of scope / future

- **WebSocket trace streaming** — `debug_subscribe(trace)`. Real-time
  trace push. Marginal value; defer.
- **Custom JS tracers** — geth's `tracer: "{step: function...}"` JSON
  surface accepts user-supplied JS that runs over the hook fan-out.
  Defer; security-sensitive (sandbox cost not worth it for now).
- **Performance regression CI gate** — automated benchmark that
  `BenchmarkProcessBlock` doesn't regress > 5% per release.
