# M10 Fee Conformance — Design Spec

**Date:** 2026-04-28  
**Status:** Active  
**Milestone:** M10 — Protocol fee conformance  
**Slices:** Slice 1 (multi-sign fee + memo fee + burn_trx_amount), Slice 2 (total transaction counter)

---

## 1. Problem

Three protocol-level gaps prevent go-tron from exactly matching java-tron's transaction processing:

1. **`consumeMultiSignFee`** — java-tron charges `multi_sign_fee` (default 1 TRX) per transaction when `signature_count > 1` and `allow_multi_sign` is active. go-tron silently skips this.
2. **`consumeMemoFee`** — java-tron charges `memo_fee` (default 0, set by governance proposal #60) per transaction when `rawData.Data` is non-empty. go-tron silently skips this.
3. **`burn_trx_amount`** — java-tron tracks the cumulative TRX burned (via fees when `AllowBlackholeOptimization` is active) in a DP key. go-tron's `GetBurnTrx()` always returns 0.
4. **`TotalTransaction`** — go-tron's `TotalTransaction()` always returns 0 (non-consensus rawdb counter, used by node info API).

---

## 2. Java-tron Reference

### consumeMultiSignFee (TransactionTrace.java)
```java
void consumeMultiSignFee(...) {
    if (transaction.getSignatureCount() > 1) {
        if (!dynamicPropertiesStore.allowMultiSign()) return;
        long fee = dynamicPropertiesStore.getMultiSignFee();
        Commons.adjustBalance(accountStore, owner, -fee);
        if (dynamicPropertiesStore.supportBlackHoleOptimization()) {
            dynamicPropertiesStore.burnTrx(fee);
        } else {
            Commons.adjustBalance(accountStore, blackhole, +fee);
        }
    }
}
```

### consumeMemoFee (TransactionTrace.java)
```java
void consumeMemoFee(...) {
    if (!transaction.getRawData().getData().isEmpty()) {
        long fee = dynamicPropertiesStore.getMemoFee();
        if (fee <= 0) return;
        Commons.adjustBalance(accountStore, owner, -fee);
        if (dynamicPropertiesStore.supportBlackHoleOptimization()) {
            dynamicPropertiesStore.burnTrx(fee);
        } else {
            Commons.adjustBalance(accountStore, blackhole, +fee);
        }
    }
}
```

### burnTrx (DynamicPropertiesStore.java)
```java
void burnTrx(long amount) {
    long current = burnTrxAmount();
    saveLong(BURN_TRX_AMOUNT, current + amount);
}
```

### Ordering in java-tron Manager.processTransaction
```
consumeBandwidth → consumeMultiSignFee → consumeMemoFee → actuator.execute
```

---

## 3. Design

### Slice 1: Multi-sign fee + Memo fee + burn_trx_amount

#### 3.1 `core/state/dynamic_properties.go`
- Add `"burn_trx_amount": 0` to `defaultProps`.
- Add `BurnTrxAmount() int64` getter.
- Add `AddBurnTrx(amount int64)` method (adds delta, marks dirty; no-op on 0).

#### 3.2 `actuator/fees.go`
- In `burnFee()`: when `AllowBlackholeOptimization` is active, call `ctx.DynProps.AddBurnTrx(fee)` (fee is already subtracted from owner; this records the burn amount).
- Add `extractOwner(tx *types.Transaction) common.Address` — same logic as `core/bandwidth.go:extractSender`.
- Add `ConsumeMultiSignFee(ctx *Context) error` (exported, called from `core`).
- Add `ConsumeMemoFee(ctx *Context) error` (exported, called from `core`).

#### 3.3 `core/state_processor.go`
After `consumeBandwidth`, before `act.Execute`:
```go
if err := actuator.ConsumeMultiSignFee(ctx); err != nil {
    return nil, fmt.Errorf("multi-sign fee: %w", err)
}
if err := actuator.ConsumeMemoFee(ctx); err != nil {
    return nil, fmt.Errorf("memo fee: %w", err)
}
```

#### 3.4 `core/tron_backend.go`
```go
func (b *TronBackend) GetBurnTrx() int64 {
    return state.LoadDynamicProperties(b.chain.db).BurnTrxAmount()
}
```

### Slice 2: Total Transaction Counter

Non-consensus rawdb counter, not in any state root.

#### 3.5 `core/rawdb/schema.go`
```go
totalTransactionCountKey = []byte("total-tx-count")
```

#### 3.6 `core/rawdb/accessors_chain.go`
`ReadTotalTransactionCount(db) int64` / `WriteTotalTransactionCount(db, count int64)`

#### 3.7 `core/blockchain.go` (in `InsertBlock` after persist)
```go
if n := len(block.Transactions()); n > 0 {
    count := rawdb.ReadTotalTransactionCount(bc.db)
    rawdb.WriteTotalTransactionCount(bc.db, count+int64(n))
}
```

#### 3.8 `core/tron_backend.go`
```go
func (b *TronBackend) TotalTransaction() int64 {
    return rawdb.ReadTotalTransactionCount(b.chain.db)
}
```

---

## 4. Fee and Balance Correctness

- `consumeMultiSignFee`: subtracts `MultiSignFee()` from owner. If `AllowBlackholeOptimization` is active, fee is burned (not credited anywhere). Otherwise, credited to blackhole genesis account.
- `consumeMemoFee`: same routing. Only charged when `MemoFee() > 0` (governance-controlled; default 0).
- Both fees are charged per-contract, per tx (java-tron iterates contracts but TRON txs have exactly one contract in practice).
- No new proto fields added — fees affect only balance, not `ResourceReceipt`.

---

## 5. Exit Gate

| Test | Assertion |
|------|-----------|
| `TestConsumeMultiSignFee_charged` | Multi-sig tx with allow_multi_sign=1: owner balance decreases by multi_sign_fee |
| `TestConsumeMultiSignFee_skipped_single_sig` | Single-sig tx: no fee charged |
| `TestConsumeMultiSignFee_skipped_flag_off` | allow_multi_sign=0: no fee charged |
| `TestConsumeMemoFee_charged` | Tx with non-empty data, memo_fee=500: owner balance decreases by 500 |
| `TestConsumeMemoFee_skipped_empty` | Tx with empty data: no fee charged |
| `TestConsumeMemoFee_skipped_zero_fee` | memo_fee=0: no fee charged |
| `TestBurnTrxAmount_accumulates` | Two fees burned: BurnTrxAmount() = sum of both |
| `TestTotalTransaction_counter` | N txs across 2 blocks: TotalTransaction() = N |
| `make test` | Full suite green |
