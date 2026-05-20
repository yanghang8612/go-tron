# TxPool subpool architecture — design

**Status:** Proposed
**Author:** yanghang8612
**Date:** 2026-05-19
**Inspiration:** [go-ethereum/core/txpool/subpool.go](../../../../ethereum/go-ethereum/core/txpool/subpool.go), [legacypool/](../../../../ethereum/go-ethereum/core/txpool/legacypool/), [blobpool/](../../../../ethereum/go-ethereum/core/txpool/blobpool/)
**Related plan:** [2026-05-19-txpool-subpool.md](../plans/2026-05-19-txpool-subpool.md)

## Background

[`core/txpool/`](../../../core/txpool/) today is a monolithic pool that
accepts every contract type via one acceptance/eviction path. TRON has
~30 contract types with very different characteristics:

- **Transparent value transfers** (TransferContract, TransferAssetContract)
  — small, deterministic fee, high churn
- **TVM calls** (TriggerSmartContract, CreateSmartContract) — variable
  energy cost, can be expensive; dapps spam during DEX rebalances
- **Resource ops** (Freeze/Unfreeze V1/V2, DelegateResource) — low volume,
  state-heavy
- **Governance** (VoteWitness, WitnessCreate, ProposalCreate, ProposalApprove) —
  very low volume, must not be lost during normal churn
- **Shielded** (ShieldedTransferContract) — heavy: 580-byte ciphertexts,
  Pedersen tree appends, expensive validation

Today's pool applies one set of admission policies (size cap, per-sender
nonce, fee floor) across all types. This has two pain points:

1. **Governance txs get evicted under TVM spam**. The Nile h=860k stuck
   proposal was found behind a flood of TVM calls that pushed older
   governance txs out before they could be included in a block.
2. **Shielded txs are expensive to validate** (Pedersen hash + ZK proof
   shape check) and run on the same hot path as cheap transparent
   transfers. A shielded burst slows transparent admission for everyone.

go-ethereum hit a similar problem with EIP-4844 blob transactions — blobs
are oversized and need their own admission policy. The geth team's answer:
**SubPool architecture**.

> [`core/txpool/txpool.go`](../../../../ethereum/go-ethereum/core/txpool/txpool.go) is a dispatcher.
> Each `SubPool` (legacy / blob) manages its own admission, eviction,
> reorg behaviour. The dispatcher routes by tx type and coordinates
> chain-head events.

This spec proposes a port for gtron, split into:

- `transparent` subpool — TransferContract / TransferAssetContract /
  TVM calls (the bulk of mainnet txs)
- `governance` subpool — VoteWitness / WitnessCreate / WitnessUpdate /
  Proposal* / ExchangeCreate / ExchangeInject / AccountPermissionUpdate /
  AccountCreate / WithdrawBalance (low-volume, never-evict)
- `shielded` subpool — ShieldedTransferContract (separate budget,
  rate-limit independently of transparent)

## Goals

- Governance txs never evicted during transparent traffic bursts
- Shielded admission rate-limited separately from transparent
- Per-subpool admission policy is independently tunable in config
- Clean extension surface: future tx types (e.g. AA-style) add new
  subpools without touching dispatcher logic
- Existing `core/txpool.Pool` public API preserved (txpool consumers in
  `core/producer/*`, RPC, broadcaster — all unchanged)

## Non-goals

- Do NOT change wire format for tx broadcast
- Do NOT change tx admission semantics (Validate functions still gate
  what's a legal tx)
- Do NOT add tx fee market / priority gas auction. TRON's fee model is
  fixed energy/bandwidth pricing; pool ordering stays FIFO-per-sender
- Do NOT split actuator code — actuators are unchanged

## Target structure

```
core/txpool/
  pool.go                # Dispatcher (renamed from current monolithic pool)
  subpool.go             # SubPool interface
  transparent/
    pool.go              # TransferContract + Asset + TVM txs
    pool_test.go
  governance/
    pool.go              # vote/witness/proposal/exchange/permission
    pool_test.go
  shielded/
    pool.go              # ShieldedTransferContract
    pool_test.go
  routing.go             # txTypeRoute(tx) → SubPool
```

### SubPool interface

```go
// SubPool is the contract every per-type pool implements.
// Mirrors go-ethereum's core/txpool.SubPool.
type SubPool interface {
    // Filter reports whether this subpool wants to handle this tx.
    Filter(tx *types.Transaction) bool

    // Add admits (or rejects with reason) a tx. Must be safe to call
    // from multiple goroutines (the dispatcher fan-in).
    Add(tx *types.Transaction) error

    // Pending returns pending txs visible to block production, in
    // include-order. Producer drains the dispatcher; the dispatcher
    // merges Pending() from every subpool.
    Pending() []*types.Transaction

    // Has reports whether the tx hash is currently held.
    Has(hash tcommon.Hash) bool

    // Get returns the tx for the hash, or nil.
    Get(hash tcommon.Hash) *types.Transaction

    // Reset is called when a new block lands. The subpool may evict
    // included txs, re-validate against the new head, etc.
    Reset(oldHead, newHead *types.Block, newState *state.StateDB)

    // Status reports subpool-level metrics (size, oldest, evictions).
    Status() SubPoolStatus
}
```

### Dispatcher

`core/txpool/pool.go`:

```go
type Pool struct {
    subpools []SubPool
    chain    *core.BlockChain
    mu       sync.RWMutex
}

func New(...) *Pool {
    return &Pool{
        subpools: []SubPool{
            transparent.New(...),
            governance.New(...),
            shielded.New(...),
        },
    }
}

func (p *Pool) Add(tx *types.Transaction) error {
    for _, sp := range p.subpools {
        if sp.Filter(tx) {
            return sp.Add(tx)
        }
    }
    return errors.New("tx type rejected: no subpool accepts it")
}

func (p *Pool) Pending() []*types.Transaction {
    var out []*types.Transaction
    for _, sp := range p.subpools {
        out = append(out, sp.Pending()...)
    }
    return out
}

func (p *Pool) Reset(oldHead, newHead *types.Block, newState *state.StateDB) {
    // Sequential reset; subpools could go parallel later if profile shows it.
    for _, sp := range p.subpools {
        sp.Reset(oldHead, newHead, newState)
    }
}
```

### Per-subpool sizing

Default budgets (tunable via `gtron.toml`):

| Subpool | Slot count | Per-sender cap | Eviction policy |
|---|---|---|---|
| transparent | 32768 | 16 | oldest-first FIFO within sender, then global LRU |
| governance | 4096 | 4 | **never evict on size** — these matter; cap by `tx.expiration` only |
| shielded | 2048 | 2 | oldest-first; admission rate-limited to 1/s |

### Routing

`core/txpool/routing.go`:

```go
func routeForContractType(t corepb.Transaction_Contract_ContractType) string {
    switch t {
    case Transaction_Contract_TransferContract,
         Transaction_Contract_TransferAssetContract,
         Transaction_Contract_TriggerSmartContract,
         Transaction_Contract_CreateSmartContract:
        return "transparent"
    case Transaction_Contract_VoteWitnessContract,
         Transaction_Contract_WitnessCreateContract,
         Transaction_Contract_WitnessUpdateContract,
         Transaction_Contract_ProposalCreateContract,
         Transaction_Contract_ProposalApproveContract,
         Transaction_Contract_ProposalDeleteContract,
         Transaction_Contract_ExchangeCreateContract,
         Transaction_Contract_ExchangeInjectContract,
         Transaction_Contract_ExchangeWithdrawContract,
         Transaction_Contract_AccountCreateContract,
         Transaction_Contract_AccountPermissionUpdateContract,
         Transaction_Contract_WithdrawBalanceContract,
         Transaction_Contract_FreezeBalanceContract,
         Transaction_Contract_UnfreezeBalanceContract,
         Transaction_Contract_FreezeBalanceV2Contract,
         Transaction_Contract_UnfreezeBalanceV2Contract,
         Transaction_Contract_DelegateResourceContract,
         Transaction_Contract_UnDelegateResourceContract,
         Transaction_Contract_CancelAllUnfreezeV2Contract:
        return "governance"
    case Transaction_Contract_ShieldedTransferContract:
        return "shielded"
    default:
        return "transparent"  // safe default for new types
    }
}
```

### Reset / reorg semantics

On `ChainHeadEvent` (new block applied), the dispatcher calls `Reset` on
every subpool with the old head, new head, and a state snapshot. Each
subpool:

1. Drops txs whose hash was included in `newHead.Transactions()`
2. Re-validates remaining txs against `newState` (insufficient balance,
   nonce gaps, expiration past)
3. Notifies block-broadcast subscribers if anything changed

For reorgs (new head ancestor != old head): drop txs that were included
in the orphaned chain but not in the new chain — they need re-admission.
Geth's pattern: walk back to LCA, collect tx hashes, re-add them as
if newly-arrived.

### Configuration

`gtron.toml`:

```toml
[txpool]
[txpool.transparent]
slots         = 32768
per_sender    = 16
[txpool.governance]
slots         = 4096
per_sender    = 4
[txpool.shielded]
slots         = 2048
per_sender    = 2
admission_rate_per_sec = 1
```

### Metrics per subpool

- `txpool.<name>.size` (gauge)
- `txpool.<name>.add_total` (counter)
- `txpool.<name>.evict_total` (counter, with reason label)
- `txpool.<name>.reset_duration` (histogram)

Operators can graph governance subpool size separately from transparent;
spike-on-transparent that doesn't spike governance proves the isolation
works.

## Migration

Existing single-pool code in `core/txpool/pool.go` gets refactored into
`transparent` subpool first (most of its logic carries over). Governance
and shielded subpools are added as **new code** initially with minimal
admission logic (drop-in routing then expand).

Block producer in `core/producer/*` consumes `Pool.Pending()` which
returns concatenated subpools — no change to the producer.

## Acceptance criteria

- All existing txpool tests pass through the dispatcher unchanged
- New test: spam 100K transparent txs while submitting 1 vote tx;
  vote tx still in pool after 60 s (impossible today)
- Shielded admission rate-limit works (submit 100 shielded in 1s;
  only first accepted, rest 429)
- Reorg-replay test: 5-block reorg with 10 txs per block; non-included
  txs reappear in pool after the reset
- Metrics exposed per subpool
- Race detector clean
- No regression on Nile soak

## Risks

- Reset latency: with 3 subpools each running sequential validation
  loops, reset wall time may double. Mitigation: parallelize across
  subpools (each subpool's Reset runs in its own goroutine, dispatcher
  waits for all).
- Routing edge cases: a tx with multiple contracts (rare in TRON but
  proto allows it). Decision: route by `tx.Contract()[0].Type`; reject
  multi-contract txs with mixed routes (legal contracts always have one
  type per tx; multi-contract is a TRON anti-pattern).

## Out of scope / future

- **Adaptive admission throttling** — under sustained pressure, drop
  admission rate dynamically. Marginal value; defer.
- **Cross-subpool MEV considerations** — TRON doesn't have MEV today; if
  it ever does, ordering logic moves into the producer, not the pool
