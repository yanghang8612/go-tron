# Sync-stall debugging runbook

What to do when a Nile/mainnet re-sync stops with

```
Sync paused ... err="insert block range index K block N: process block:
  tx J: transaction vm result mismatch: tx <id> expected SUCCESS actual REVERT"
```

(or `validate: insufficient balance`, OOE flips, etc.). Distilled from the
stall series 6,498,505 → 7,799,482 → 8,825,873 → 9,220,578 → 11,359,658 →
14,151,095 → 16,745,722 → 18,112,819 → 18,278,266 → 10,552,292. Every step
below earned its place in at least one of those.

## 0. Triage first: regression or historical parity gap?

**Ask before anything else: did any earlier binary cross this block?**

- **YES → it's a regression.** Diff the commit set between the binary that
  passed and the one that fails (`git log <old>..<new>`), and suspect only
  consensus-relevant changes (vm/, actuator/, core/ execution paths, and
  anything that changes *when* data becomes readable — async/buffering
  changes count!). The answer is almost always in that set. This collapses
  the search space from "all of java-tron parity" to a handful of commits.
  (Block 10,552,292: five new VM fixes + async-commit-ON were the delta;
  the culprit was the interaction of two of them.)
- **NO → historical parity gap.** Date the block (genesis time + ~3s/block,
  minus maintenance skips), then ask which java-tron release and which
  Nile proposals were live in that era. Check `docs/dev/fork-gates.md` and
  the past-stall write-ups before re-deriving anything.

**Know what CAN drift silently.** TRON blocks commit only per-tx result
codes (txTrieRoot hashes tx bytes; there is no receipt root, no state root
java validates). Energy receipts, logs, internal transactions, and all of
state diverge *invisibly* until some contract's `require` finally flips a
result code. Corollaries:

- The first divergence is almost never at the stall block. Expect the real
  bug hundreds of thousands of blocks earlier.
- Any fix that changes execution of pre-stall blocks poisons the current
  datadir: **re-sync from genesis, do not resume** (resume re-fails at the
  same block from the already-divergent state).

## 1. Probe kit (no java archive node required)

- **tx0 trick**: if the failing tx is tx 0 of block N, the stalled node's
  head state (N−1) is the tx's *exact* pre-state. Replaying via
  `wallet/triggerconstantcontract` with the original (owner, contract,
  data) is a deterministic re-execution. A revert at rest ⇒ state or
  semantics, not an insertion race.
- **Interrogate divergent state with the contract's own view functions**
  via constant calls on the stalled node (e.g. a DEX's `getOrderList`/
  `getReserves` dumped the exact rows that differed from canonical),
  then reconstruct the canonical expectation from event history.
- **Public TronGrid is trustworthy for immutable data only**: tx content
  (`gettransactionbyid`), block headers/hashes (`getblockbynum`), event
  logs (`/v1/contracts/{b58}/events?event_name=...`), contract runtime
  code (`getcontractinfo`). It is **NOT** trustworthy for receipt fee
  fields or v1 tx lists on old blocks — for receipts, compare gtron node
  vs java node directly (see `docs/dev/java-tron-local.md`).
- hex→base58check: `0x41 || 20 bytes`, checksum `sha256(sha256(x))[:4]`.

## 2. Localize inside the VM without a tracer

- **Energy-budget bounding.** TVM keeps Frontier-era costs: simple ops
  2–3, BLOCKHASH 20, EXTCODESIZE 20, CALL family base 40, SLOAD 50,
  SSTORE 20000/5000. The `energy_used` of a reverting constant call
  brackets how far execution got — e.g. 1,145 energy ≈ proxy dispatch +
  one factory CALL + a few checks, i.e. "reverted at the first require
  after the logic lookup", before any SSTORE.
- **Disassemble and read.** `scripts/dev/evm_disasm.py runtime.hex`
  (TVM opcodes included). Map PUSH4 dispatch constants and receipt log
  topics back to names with `go run ./scripts/dev/abi_hash '<sig>'`.
- **Recognize the era's dapp shapes.** 2020-era TRON contracts love
  factory+proxy pairs: a tiny runtime with PUSH32-baked immutables that
  CALLs `factory.pairLogic()` then DELEGATECALLs it (logs appear from the
  proxy address; java internal-tx for the delegatecall shows self→self).
  And they love deriving ids/randomness from `blockhash`, `coinbase`,
  `origin` — every env-opcode divergence eventually surfaces through one
  of these.
- **Prove the formula arithmetically before touching code.** Once the
  disassembly suggests a derivation (e.g.
  `id = uint22(blockhash(number-1) ^ origin)`), verify it against public
  canonical data — parent hashes and event payloads — for *every*
  occurrence. 4/4 exact matches is a proof; one match is a coincidence.

## 3. Known divergence classes (check before re-deriving)

Past stalls, each with a network/block replay test pinned in-tree:
shielded proof layout (6,498,505), COINBASE=witness (7,799,482), energy
window refresh + recovery precision (8,825,873), over-depleted burn
skip-write (9,220,578), call depth 64 (11,359,658), per-frame returndata
buffer (14,151,095), BLOCKHASH vs freezer pruning (16,745,722), precompile
endowment (18,112,819), point-at-infinity recovery (18,278,266), BLOCKHASH
vs async commit (10,552,292), and Nile's allow_tvm_blob KZG point-evaluation
precompile at TRON address 0x02000a (55,611,077; first 50k-charged call — and
so first divergent fee — at 55,609,940, the tester contract deployed at
55,609,930, so resume/snapshot points must be ≤ 55,609,929). The 0x02000a
mapping is Nile-only; mainnet treats it as an ordinary account even when
allow_tvm_blob is active; CREATE/CREATE2 success words retaining the 0x41
TRON prefix (59,652,963); pre-Solidity059 internal TRX transfer to an
accountless recipient, which must spend all energy and record UNKNOWN rather
than implicitly create the recipient (mainnet 3,422,904); and the
pre-ALLOW_MULTI_SIGN empty-runtime-code cache NPE after an internal CREATE
(typically a constructor SELFDESTRUCT), which must spend all energy and record
UNKNOWN (mainnet 4,904,919, with repeats at 4,905,126 and 4,905,131); and
pre-ALLOW_TVM_CONSTANTINOPLE internal self-transfer validation, which is a
BytecodeExecutionException (UNKNOWN + spend-all) rather than the later
TransferException (TRANSFER_FAILED + refund), exposed by the prefixed ADDRESS
semantics at mainnet 4,997,510.

**Async-commit reader checklist.** Everything block N writes that block
N+1's *execution* reads must be visible through the buffer pipeline
(`bc.buffer` walks in-flight layers), never via a direct Pebble read that
the commit worker publishes later:

| data | mechanism |
|---|---|
| state / contract KV | buffer layers (by construction) |
| rooted dynamic properties | threaded via plan (decision-b) |
| TAPOS ring | dual-staged into buffer at apply |
| block body (BLOCKHASH) | dual-staged into buffer at apply |
| head / parent root for verify | range tip via plan |

When adding a new exec-time consumer of recently-written data, stage it
into the buffer at apply time (fork-rewindable for free) — the durable
copy in `writeBlockMetadataBatch` is not enough under
`GTRON_ASYNC_COMMIT=1`.

## 4. Fix + validation discipline

- Write the failing replay test first (`vm/*_replay_test.go` pattern with
  real bytecode/calldata, or the `core` async harness for pipeline bugs).
  For worker races, `SetCommitFoldHookForTest` with a number-gated sleep
  makes the foreground win deterministically — never rely on natural
  scheduling.
- Chain-test trap: pre-Constantinople create *ignores* the constructor's
  RETURN and pattern-scans the creation bytes for `RETURN;STOP` (else it
  stores 32 zero bytes — `legacyCreateContractCode`). Tests deploying
  eth-style creation code need genesis DP `allow_tvm_constantinople: 1`.
- A new fix must keep **all** prior replay tests green: the true java
  semantics satisfy every era simultaneously; if your fix trades one
  stall for another, it is not the java behavior.
- Accumulated-state fixes are only "chain-proven" by a from-genesis
  re-sync crossing the stall block (and the previous stall blocks).
