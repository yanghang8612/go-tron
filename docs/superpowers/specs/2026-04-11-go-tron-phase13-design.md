# Phase 13: Stake 1.0 + Market Orders — Design Spec

**Date:** 2026-04-11

## Goal

Implement the four remaining practical TRON contract types:
- `FreezeBalanceContract` (11) and `UnfreezeBalanceContract` (12) — Stake 1.0 legacy staking
- `MarketSellAssetContract` (52) and `MarketCancelOrderContract` (53) — on-chain TRC10 order book DEX

After Phase 13, all practical contract types in go-tron's proto are implemented (exchange contracts 41–44 are disabled in java-tron 4.8.0; ShieldedTransfer 51 requires ZK cryptography).

## Context

Phases 1–12 built a working node with Stake 2.0 (types 54–59), TRC10 tokens (types 6, 9, 14, 15), governance, smart contracts, P2P networking, and full HTTP + JSON-RPC APIs. Stake 1.0 is the legacy staking mechanism still present in the proto and needed for historical mainnet TX compatibility. The market contracts implement TRON's on-chain DEX for TRC10 token exchange.

## Architecture

**Two independent subsystems:**

**Stake 1.0** — V1 frozen fields already exist in the `Account` protobuf (`pb.Frozen` list for bandwidth, `pb.AccountResource.FrozenBalanceForEnergy` single entry for energy, delegation fields for both). `core/types/account.go` needs new V1 accessors; `core/state/statedb.go` needs thin wrapper methods. Two actuators follow the exact same pattern as `freeze_v2.go` / `unfreeze_v2.go`.

**Market** — New rawdb layer (`mo-`, `mao-`, `mop-`, `mpl-` prefixes), a matching engine embedded in `market_sell_asset.go`, and two actuators. The order book uses a linked list per price point. Matching is price-time priority with maker price winning.

**No new external dependencies.** No new packages outside existing ones.

## File Map

| File | Action | Responsibility |
|---|---|---|
| `core/types/account.go` | Modify | Add V1 frozen balance accessors |
| `core/state/statedb.go` | Modify | Add V1 StateDB wrapper methods |
| `core/state/statedb_v1_test.go` | Create | Unit tests for V1 state methods |
| `actuator/freeze_balance.go` | Create | `FreezeBalanceActuator` (type 11) |
| `actuator/freeze_balance_test.go` | Create | Tests for FreezeBalanceActuator |
| `actuator/unfreeze_balance.go` | Create | `UnfreezeBalanceActuator` (type 12) |
| `actuator/unfreeze_balance_test.go` | Create | Tests for UnfreezeBalanceActuator |
| `core/rawdb/schema.go` | Modify | 4 new market prefixes + key functions |
| `core/rawdb/accessors_market.go` | Create | Market order read/write functions |
| `core/rawdb/accessors_market_test.go` | Create | Unit tests for market rawdb |
| `actuator/market_sell_asset.go` | Create | `MarketSellAssetActuator` (type 52) + matching engine |
| `actuator/market_sell_asset_test.go` | Create | Tests for MarketSellAssetActuator |
| `actuator/market_cancel_order.go` | Create | `MarketCancelOrderActuator` (type 53) |
| `actuator/market_cancel_order_test.go` | Create | Tests for MarketCancelOrderActuator |
| `actuator/actuator.go` | Modify | Register types 11, 12, 52, 53 |
| `internal/tronapi/backend.go` | Modify | Add 3 market query Backend methods |
| `internal/tronapi/api.go` | Modify | Add 3 market query routes + handlers |
| `core/tron_backend.go` | Modify | Implement 3 market query Backend methods |
| `scripts/system_test.sh` | Modify | Add Section 12: market query checks |

---

## Part 1: Stake 1.0 State Model

### V1 Frozen Fields in Account Proto

The `Account` proto (already generated) contains these V1 fields:

```
pb.Frozen              []Account_Frozen  // bandwidth freeze list (repeated)
pb.AccountResource.FrozenBalanceForEnergy  *Account_Frozen  // single energy freeze entry
pb.DelegatedFrozenBalanceForBandwidth      int64
pb.AcquiredDelegatedFrozenBalanceForBandwidth int64
pb.AccountResource.DelegatedFrozenBalanceForEnergy  int64
pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy int64
```

`Account_Frozen` has two fields: `FrozenBalance int64` and `ExpireTime int64` (milliseconds).

### New `core/types/account.go` Methods

Add after the existing V2 methods (before the Votes accessor block):

```go
// --- V1 Stake (Stake 1.0) frozen balance accessors ---

// FrozenBandwidthList returns all V1 bandwidth frozen entries.
func (a *Account) FrozenBandwidthList() []*corepb.Account_Frozen {
    return a.pb.Frozen
}

// AddFrozenBandwidth appends a new bandwidth freeze entry.
func (a *Account) AddFrozenBandwidth(amount, expireTimeMs int64) {
    a.pb.Frozen = append(a.pb.Frozen, &corepb.Account_Frozen{
        FrozenBalance: amount,
        ExpireTime:    expireTimeMs,
    })
}

// TotalFrozenBandwidth returns the sum of all V1 frozen bandwidth amounts.
func (a *Account) TotalFrozenBandwidth() int64 {
    var total int64
    for _, f := range a.pb.Frozen {
        total += f.FrozenBalance
    }
    return total
}

// RemoveExpiredFrozenBandwidth removes all frozen bandwidth entries whose
// ExpireTime <= blockTimeMs. Returns the total refunded amount.
func (a *Account) RemoveExpiredFrozenBandwidth(blockTimeMs int64) int64 {
    var refunded int64
    remaining := a.pb.Frozen[:0]
    for _, f := range a.pb.Frozen {
        if f.ExpireTime <= blockTimeMs {
            refunded += f.FrozenBalance
        } else {
            remaining = append(remaining, f)
        }
    }
    a.pb.Frozen = remaining
    return refunded
}

// FrozenEnergyAmount returns the V1 frozen energy amount (0 if none).
func (a *Account) FrozenEnergyAmount() int64 {
    if a.pb.AccountResource == nil || a.pb.AccountResource.FrozenBalanceForEnergy == nil {
        return 0
    }
    return a.pb.AccountResource.FrozenBalanceForEnergy.FrozenBalance
}

// FrozenEnergyExpireTime returns the V1 frozen energy expiry in ms (0 if none).
func (a *Account) FrozenEnergyExpireTime() int64 {
    if a.pb.AccountResource == nil || a.pb.AccountResource.FrozenBalanceForEnergy == nil {
        return 0
    }
    return a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime
}

// AddFrozenEnergy adds to the V1 energy freeze entry (extends expiry if later).
func (a *Account) AddFrozenEnergy(amount, expireTimeMs int64) {
    a.ensureAccountResource()
    if a.pb.AccountResource.FrozenBalanceForEnergy == nil {
        a.pb.AccountResource.FrozenBalanceForEnergy = &corepb.Account_Frozen{
            FrozenBalance: amount,
            ExpireTime:    expireTimeMs,
        }
    } else {
        a.pb.AccountResource.FrozenBalanceForEnergy.FrozenBalance += amount
        if expireTimeMs > a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime {
            a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime = expireTimeMs
        }
    }
}

// ClearFrozenEnergy removes the V1 energy freeze entry.
func (a *Account) ClearFrozenEnergy() {
    if a.pb.AccountResource != nil {
        a.pb.AccountResource.FrozenBalanceForEnergy = nil
    }
}

// V1 delegation: bandwidth
func (a *Account) DelegatedFrozenBandwidth() int64 { return a.pb.DelegatedFrozenBalanceForBandwidth }
func (a *Account) SetDelegatedFrozenBandwidth(v int64) { a.pb.DelegatedFrozenBalanceForBandwidth = v }
func (a *Account) AcquiredDelegatedFrozenBandwidth() int64 { return a.pb.AcquiredDelegatedFrozenBalanceForBandwidth }
func (a *Account) SetAcquiredDelegatedFrozenBandwidth(v int64) { a.pb.AcquiredDelegatedFrozenBalanceForBandwidth = v }

// V1 delegation: energy
func (a *Account) DelegatedFrozenEnergy() int64 {
    if a.pb.AccountResource == nil { return 0 }
    return a.pb.AccountResource.DelegatedFrozenBalanceForEnergy
}
func (a *Account) SetDelegatedFrozenEnergy(v int64) {
    a.ensureAccountResource()
    a.pb.AccountResource.DelegatedFrozenBalanceForEnergy = v
}
func (a *Account) AcquiredDelegatedFrozenEnergy() int64 {
    if a.pb.AccountResource == nil { return 0 }
    return a.pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy
}
func (a *Account) SetAcquiredDelegatedFrozenEnergy(v int64) {
    a.ensureAccountResource()
    a.pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy = v
}
```

### New `core/state/statedb.go` Methods

Pattern (from `AddFreezeV2` and `AddBalance`): `getStateObject` → `journalAccount` → modify → `obj.markDirty()`. For delegation both parties must exist (validated before Execute), so `getStateObject` is used for both.

Add after the V2 frozen methods:

```go
// --- V1 Stake (Stake 1.0) StateDB methods ---

// FreezeV1Bandwidth appends a bandwidth freeze entry to the account.
func (s *StateDB) FreezeV1Bandwidth(addr tcommon.Address, amount, expireTimeMs int64) {
    obj := s.getStateObject(addr)
    if obj == nil { return }
    s.journalAccount(addr, obj)
    obj.account.AddFrozenBandwidth(amount, expireTimeMs)
    obj.markDirty()
}

// UnfreezeV1Bandwidth removes expired bandwidth freeze entries and returns the refunded amount.
func (s *StateDB) UnfreezeV1Bandwidth(addr tcommon.Address, blockTimeMs int64) int64 {
    obj := s.getStateObject(addr)
    if obj == nil { return 0 }
    s.journalAccount(addr, obj)
    refunded := obj.account.RemoveExpiredFrozenBandwidth(blockTimeMs)
    obj.markDirty()
    return refunded
}

// FreezeV1Energy adds to the energy freeze entry (creates it if absent).
func (s *StateDB) FreezeV1Energy(addr tcommon.Address, amount, expireTimeMs int64) {
    obj := s.getStateObject(addr)
    if obj == nil { return }
    s.journalAccount(addr, obj)
    obj.account.AddFrozenEnergy(amount, expireTimeMs)
    obj.markDirty()
}

// UnfreezeV1Energy clears the energy freeze if it has expired. Returns the refunded amount (0 if not expired).
func (s *StateDB) UnfreezeV1Energy(addr tcommon.Address, blockTimeMs int64) int64 {
    obj := s.getStateObject(addr)
    if obj == nil { return 0 }
    if obj.account.FrozenEnergyExpireTime() > blockTimeMs { return 0 }
    amount := obj.account.FrozenEnergyAmount()
    if amount == 0 { return 0 }
    s.journalAccount(addr, obj)
    obj.account.ClearFrozenEnergy()
    obj.markDirty()
    return amount
}

// GetDelegatedFrozenV1Bandwidth returns the V1 delegated bandwidth amount for the owner.
func (s *StateDB) GetDelegatedFrozenV1Bandwidth(addr tcommon.Address) int64 {
    obj := s.getStateObject(addr)
    if obj == nil { return 0 }
    return obj.account.DelegatedFrozenBandwidth()
}

// GetDelegatedFrozenV1Energy returns the V1 delegated energy amount for the owner.
func (s *StateDB) GetDelegatedFrozenV1Energy(addr tcommon.Address) int64 {
    obj := s.getStateObject(addr)
    if obj == nil { return 0 }
    return obj.account.DelegatedFrozenEnergy()
}

// FreezeV1DelegatedBandwidth increases delegated bandwidth from owner to receiver.
func (s *StateDB) FreezeV1DelegatedBandwidth(owner, receiver tcommon.Address, amount int64) {
    ownerObj := s.getStateObject(owner)
    if ownerObj == nil { return }
    s.journalAccount(owner, ownerObj)
    ownerObj.account.SetDelegatedFrozenBandwidth(ownerObj.account.DelegatedFrozenBandwidth() + amount)
    ownerObj.markDirty()
    recvObj := s.getStateObject(receiver)
    if recvObj == nil { return }
    s.journalAccount(receiver, recvObj)
    recvObj.account.SetAcquiredDelegatedFrozenBandwidth(recvObj.account.AcquiredDelegatedFrozenBandwidth() + amount)
    recvObj.markDirty()
}

// UnfreezeV1DelegatedBandwidth decreases delegated bandwidth between owner and receiver.
func (s *StateDB) UnfreezeV1DelegatedBandwidth(owner, receiver tcommon.Address, amount int64) {
    ownerObj := s.getStateObject(owner)
    if ownerObj == nil { return }
    s.journalAccount(owner, ownerObj)
    ownerObj.account.SetDelegatedFrozenBandwidth(ownerObj.account.DelegatedFrozenBandwidth() - amount)
    ownerObj.markDirty()
    recvObj := s.getStateObject(receiver)
    if recvObj == nil { return }
    s.journalAccount(receiver, recvObj)
    v := recvObj.account.AcquiredDelegatedFrozenBandwidth() - amount
    if v < 0 { v = 0 }
    recvObj.account.SetAcquiredDelegatedFrozenBandwidth(v)
    recvObj.markDirty()
}

// FreezeV1DelegatedEnergy increases delegated energy from owner to receiver.
func (s *StateDB) FreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
    ownerObj := s.getStateObject(owner)
    if ownerObj == nil { return }
    s.journalAccount(owner, ownerObj)
    ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() + amount)
    ownerObj.markDirty()
    recvObj := s.getStateObject(receiver)
    if recvObj == nil { return }
    s.journalAccount(receiver, recvObj)
    recvObj.account.SetAcquiredDelegatedFrozenEnergy(recvObj.account.AcquiredDelegatedFrozenEnergy() + amount)
    recvObj.markDirty()
}

// UnfreezeV1DelegatedEnergy decreases delegated energy between owner and receiver.
func (s *StateDB) UnfreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
    ownerObj := s.getStateObject(owner)
    if ownerObj == nil { return }
    s.journalAccount(owner, ownerObj)
    ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() - amount)
    ownerObj.markDirty()
    recvObj := s.getStateObject(receiver)
    if recvObj == nil { return }
    s.journalAccount(receiver, recvObj)
    v := recvObj.account.AcquiredDelegatedFrozenEnergy() - amount
    if v < 0 { v = 0 }
    recvObj.account.SetAcquiredDelegatedFrozenEnergy(v)
    recvObj.markDirty()
}
```

### FreezeBalanceActuator (type 11)

**File:** `actuator/freeze_balance.go`

```go
type FreezeBalanceActuator struct{}
```

**Validate:**
1. Unmarshal `FreezeBalanceContract`
2. `ownerAddr = common.BytesToAddress(c.OwnerAddress)`
3. Owner must exist
4. `c.FrozenBalance < 1_000_000` → error "frozen balance must be at least 1 TRX"
5. `c.FrozenDuration < 3` → error "frozen duration must be at least 3 days"
6. `ctx.State.GetBalance(ownerAddr) < c.FrozenBalance` → error "insufficient balance"
7. `c.Resource` must be BANDWIDTH (0), ENERGY (1), or TRON_POWER (2); anything else → error "invalid resource"
8. If `c.ReceiverAddress` is set and non-empty: receiver must exist

**Execute:**
```go
expireTimeMs := ctx.BlockTime + c.FrozenDuration*86_400_000
ownerAddr := common.BytesToAddress(c.OwnerAddress)
ctx.State.SubBalance(ownerAddr, c.FrozenBalance)  // returns error — check it

delegated := len(c.ReceiverAddress) > 0
receiverAddr := common.BytesToAddress(c.ReceiverAddress)

if !delegated {
    switch c.Resource {
    case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
        ctx.State.FreezeV1Bandwidth(ownerAddr, c.FrozenBalance, expireTimeMs)
    case corepb.ResourceCode_ENERGY:
        ctx.State.FreezeV1Energy(ownerAddr, c.FrozenBalance, expireTimeMs)
    }
} else {
    switch c.Resource {
    case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
        ctx.State.FreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, c.FrozenBalance)
    case corepb.ResourceCode_ENERGY:
        ctx.State.FreezeV1DelegatedEnergy(ownerAddr, receiverAddr, c.FrozenBalance)
    }
}
return &Result{Fee: 0, ContractRet: 1}, nil
```

### UnfreezeBalanceActuator (type 12)

**File:** `actuator/unfreeze_balance.go`

**Validate:**
1. Unmarshal `UnfreezeBalanceContract`
2. Owner must exist
3. `delegated = len(c.ReceiverAddress) > 0`
4. If not delegated:
   - BANDWIDTH/TRON_POWER: check at least one expired entry in `FrozenBandwidthList` (ExpireTime <= blockTime)
   - ENERGY: check `FrozenEnergyAmount() > 0` and `FrozenEnergyExpireTime() <= blockTime`
5. If delegated:
   - BANDWIDTH/TRON_POWER: `DelegatedFrozenBandwidth() > 0`
   - ENERGY: `DelegatedFrozenEnergy() > 0`

**Execute:**
```go
ownerAddr := common.BytesToAddress(c.OwnerAddress)
delegated := len(c.ReceiverAddress) > 0
receiverAddr := common.BytesToAddress(c.ReceiverAddress)

if !delegated {
    var refunded int64
    switch c.Resource {
    case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
        refunded = ctx.State.UnfreezeV1Bandwidth(ownerAddr, ctx.BlockTime)
    case corepb.ResourceCode_ENERGY:
        refunded = ctx.State.UnfreezeV1Energy(ownerAddr, ctx.BlockTime)
    }
    ctx.State.AddBalance(ownerAddr, refunded)
} else {
    var delegatedAmt int64
    switch c.Resource {
    case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
        delegatedAmt = ctx.State.GetDelegatedFrozenV1Bandwidth(ownerAddr)
        ctx.State.UnfreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, delegatedAmt)
    case corepb.ResourceCode_ENERGY:
        delegatedAmt = ctx.State.GetDelegatedFrozenV1Energy(ownerAddr)
        ctx.State.UnfreezeV1DelegatedEnergy(ownerAddr, receiverAddr, delegatedAmt)
    }
    ctx.State.AddBalance(ownerAddr, delegatedAmt)
}
return &Result{Fee: 0, ContractRet: 1}, nil
```

**Note:** Add `GetDelegatedFrozenV1Bandwidth` and `GetDelegatedFrozenV1Energy` to StateDB as simple readers of the account delegation fields.

---

## Part 2: Market Orders

### rawdb Schema (`core/rawdb/schema.go`)

Add after the existing asset prefixes:

```go
marketOrderPrefix        = []byte("mo-")
marketAccountOrderPrefix = []byte("mao-")
marketOrderBookPrefix    = []byte("mop-")
marketPriceListPrefix    = []byte("mpl-")
```

Key functions:

```go
func marketOrderKey(orderID []byte) []byte {
    return append(append([]byte{}, marketOrderPrefix...), orderID...)
}

func marketAccountOrderKey(ownerAddr []byte) []byte {
    return append(append([]byte{}, marketAccountOrderPrefix...), ownerAddr...)
}

// priceKey normalizes a {sellQty, buyQty} pair by GCD and encodes as 16 bytes.
func priceKey(sellQty, buyQty int64) [16]byte {
    g := gcdInt64(sellQty, buyQty)
    var k [16]byte
    binary.BigEndian.PutUint64(k[:8], uint64(sellQty/g))
    binary.BigEndian.PutUint64(k[8:], uint64(buyQty/g))
    return k
}

// gcdInt64 computes the GCD of two positive int64s.
func gcdInt64(a, b int64) int64 {
    for b != 0 { a, b = b, a%b }
    return a
}

func marketOrderBookKey(sellTokenID, buyTokenID []byte, pk [16]byte) []byte {
    k := append(append([]byte{}, marketOrderBookPrefix...), sellTokenID...)
    k = append(k, '|')
    k = append(k, buyTokenID...)
    k = append(k, '|')
    return append(k, pk[:]...)
}

func marketPriceListKey(sellTokenID, buyTokenID []byte) []byte {
    k := append(append([]byte{}, marketPriceListPrefix...), sellTokenID...)
    k = append(k, '|')
    return append(k, buyTokenID...)
}
```

### rawdb Accessors (`core/rawdb/accessors_market.go`)

```go
package rawdb

import (
    "encoding/binary"
    "github.com/ethereum/go-ethereum/ethdb"
    corepb "github.com/tronprotocol/go-tron/proto/core"
    "google.golang.org/protobuf/proto"
)

func WriteMarketOrder(db ethdb.KeyValueWriter, orderID []byte, order *corepb.MarketOrder) error {
    data, err := proto.Marshal(order)
    if err != nil { return err }
    return db.Put(marketOrderKey(orderID), data)
}

func ReadMarketOrder(db ethdb.KeyValueReader, orderID []byte) *corepb.MarketOrder {
    data, err := db.Get(marketOrderKey(orderID))
    if err != nil || len(data) == 0 { return nil }
    var o corepb.MarketOrder
    if err := proto.Unmarshal(data, &o); err != nil { return nil }
    return &o
}

func WriteMarketAccountOrder(db ethdb.KeyValueWriter, ownerAddr []byte, mao *corepb.MarketAccountOrder) error {
    data, err := proto.Marshal(mao)
    if err != nil { return err }
    return db.Put(marketAccountOrderKey(ownerAddr), data)
}

func ReadMarketAccountOrder(db ethdb.KeyValueReader, ownerAddr []byte) *corepb.MarketAccountOrder {
    data, err := db.Get(marketAccountOrderKey(ownerAddr))
    if err != nil || len(data) == 0 {
        return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
    }
    var mao corepb.MarketAccountOrder
    if err := proto.Unmarshal(data, &mao); err != nil {
        return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
    }
    return &mao
}

func WriteMarketOrderBook(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pk [16]byte, list *corepb.MarketOrderIdList) error {
    data, err := proto.Marshal(list)
    if err != nil { return err }
    return db.Put(marketOrderBookKey(sellTokenID, buyTokenID, pk), data)
}

func ReadMarketOrderBook(db ethdb.KeyValueReader, sellTokenID, buyTokenID []byte, pk [16]byte) *corepb.MarketOrderIdList {
    data, err := db.Get(marketOrderBookKey(sellTokenID, buyTokenID, pk))
    if err != nil || len(data) == 0 { return nil }
    var list corepb.MarketOrderIdList
    if err := proto.Unmarshal(data, &list); err != nil { return nil }
    return &list
}

func DeleteMarketOrderBook(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pk [16]byte) error {
    return db.Delete(marketOrderBookKey(sellTokenID, buyTokenID, pk))
}

func WriteMarketPriceList(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pl *corepb.MarketPriceList) error {
    data, err := proto.Marshal(pl)
    if err != nil { return err }
    return db.Put(marketPriceListKey(sellTokenID, buyTokenID), data)
}

func ReadMarketPriceList(db ethdb.KeyValueReader, sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
    data, err := db.Get(marketPriceListKey(sellTokenID, buyTokenID))
    if err != nil || len(data) == 0 {
        return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
    }
    var pl corepb.MarketPriceList
    if err := proto.Unmarshal(data, &pl); err != nil {
        return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
    }
    return &pl
}
```

### Order ID Generation

```go
// In market_sell_asset.go
func generateOrderID(ownerAddr tcommon.Address, txHash tcommon.Hash) []byte {
    input := append(ownerAddr[:], txHash[:]...)
    h := tcommon.Keccak256(input)
    return h
}
```

`ctx.Tx.Hash()` returns `common.Hash` — this is the transaction hash, deterministic and unique per tx.

### Token Transfer Helper

```go
// transferToken credits amount of tokenID to address addr.
// TRX is identified by tokenID == []byte("_").
func transferToken(state *state.StateDB, to tcommon.Address, tokenID []byte, amount int64) {
    if bytes.Equal(tokenID, []byte("_")) {
        state.AddBalance(to, amount)
    } else {
        id, _ := strconv.ParseInt(string(tokenID), 10, 64)
        state.AddTRC10Balance(to, id, amount)
    }
}

// deductToken deducts amount of tokenID from address addr. Returns error if insufficient.
func deductToken(state *state.StateDB, from tcommon.Address, tokenID []byte, amount int64) error {
    if bytes.Equal(tokenID, []byte("_")) {
        return state.SubBalance(from, amount)
    }
    id, _ := strconv.ParseInt(string(tokenID), 10, 64)
    return state.SubTRC10Balance(from, id, amount)
}
```

### Matching Engine (in `actuator/market_sell_asset.go`)

The matching engine is a package-level function called by `MarketSellAssetActuator.Execute`.

```go
// matchOrder tries to match the incoming order against the order book.
// Modifies state (token transfers) and rawdb (updates existing orders, price list) in-place.
// Returns the remaining sell quantity not yet filled.
func matchOrder(db ethdb.KeyValueStore, state *state.StateDB, incoming *corepb.MarketOrder) (int64, error)
```

**Algorithm:**

```
remaining = incoming.SellTokenQuantity

// 1. Get existing price list for the OPPOSITE direction: buyToken selling for sellToken
oppPriceList = ReadMarketPriceList(db, incoming.BuyTokenId, incoming.SellTokenId)
if oppPriceList has no prices → return remaining (no match)

// 2. Filter compatible prices and sort best-for-incoming first
// Match condition (cross-multiply, no floats):
//   opposite sells Q buyTokens for P sellTokens
//   incoming sells S sellTokens for B buyTokens
//   compatible if: Q * S >= P * B  (i.e., opposite gives enough sellTokens per buyToken)
//   Using big.Int to avoid int64 overflow:
//     new(big.Int).Mul(big.NewInt(oppPrice.SellQty), big.NewInt(incoming.BuyQty)) >=
//     new(big.Int).Mul(big.NewInt(oppPrice.BuyQty), big.NewInt(incoming.SellQty))
compatible = filter+sort(oppPriceList.Prices)
// Sort: best for incoming = highest sellQty/buyQty ratio among opposites
// i.e., opposite gives most sellTokens per buyToken → sort descending by oppSell/oppBuy

// 3. For each compatible price point:
for each price in compatible:
    pk = priceKey(price.SellQty, price.BuyQty)
    orderIdList = ReadMarketOrderBook(db, incoming.BuyTokenId, incoming.SellTokenId, pk)
    // Walk the linked list head → tail
    currentID = orderIdList.Head
    for currentID != nil and remaining > 0:
        existingOrder = ReadMarketOrder(db, currentID)
        
        // Calculate fill amount (at existing order's price)
        // existingOrder sells existingRemain buyTokens for minSellWanted sellTokens
        // incoming receives buyTokens, gives sellTokens
        // Fill: give min(remaining, existingOrder.SellTokenQuantityRemain) sellTokens
        //       existing gets filled in sellTokens, incoming gets buyTokens
        
        // At maker price (existingOrder's price):
        //   ratio = existingOrder.SellTokenQuantity / existingOrder.BuyTokenQuantity
        //   incoming gives fillSell sellTokens → gets fillBuy = fillSell * sellQty / buyQty buyTokens
        //   But cap at existingOrder.SellTokenQuantityRemain

        existingRemain = existingOrder.SellTokenQuantityRemain
        
        if remaining >= existingRemain * incoming.BuyTokenQuantity / incoming.SellTokenQuantity:
            // Full fill of existing order
            fillBuy = existingRemain  // existing gives all its remaining sellTokens (= buyTokens for incoming)
            fillSell = fillBuy * existingOrder.BuyTokenQuantity / existingOrder.SellTokenQuantity
            
            transferToken(state, existing.OwnerAddress, incoming.SellTokenId, fillSell)
            // incoming.BuyTokenId == existing.SellTokenId
            incoming.SellTokenQuantityRemain -= fillSell
            remaining -= fillSell
            
            // existingOrder fully filled
            existingOrder.SellTokenQuantityRemain = 0
            existingOrder.State = INACTIVE
            WriteMarketOrder(db, existingID, existingOrder)
            
            // Remove from linked list, update head
            currentID = existingOrder.Next
        else:
            // Partial fill of existing order — incoming is fully satisfied
            fillSell = remaining
            fillBuy = fillSell * existingOrder.SellTokenQuantity / existingOrder.BuyTokenQuantity
            
            transferToken(state, existingOrder.OwnerAddress, incoming.SellTokenId, fillSell)
            existingOrder.SellTokenQuantityRemain -= fillBuy
            WriteMarketOrder(db, currentID, existingOrder)
            
            incoming.SellTokenQuantityRemain = 0
            remaining = 0
            break
    
    // Credit incoming with buyTokens received so far (deducted at start, credited here)
    // Update orderIdList head if orders were consumed
    if all orders in this price exhausted: remove price from oppPriceList

// 4. Credit incoming seller with accumulated buyTokens
transferToken(state, incoming.OwnerAddress, incoming.BuyTokenId, totalBuyReceived)
```

**Important:** The incoming order has its tokens deducted at the START of Execute (before matching). Tokens received from matching are credited immediately. Any remaining sell tokens stay locked in the order book (not returned to owner until cancel or fill).

The math uses `big.Int` for compatibility checks but `int64` for amounts (TRON token quantities fit in int64).

### MarketSellAssetActuator (type 52)

**File:** `actuator/market_sell_asset.go`

**Validate:**
1. Unmarshal `MarketSellAssetContract`
2. Owner exists
3. `c.SellTokenQuantity <= 0` → error
4. `c.BuyTokenQuantity <= 0` → error
5. `bytes.Equal(c.SellTokenId, c.BuyTokenId)` → error "cannot sell token for itself"
6. Owner has sufficient `c.SellTokenQuantity` of `c.SellTokenId`
7. If `c.SellTokenId == "_"`: check `GetBalance >= c.SellTokenQuantity`
8. Else: parse tokenID, check `GetTRC10Balance >= c.SellTokenQuantity`

**Execute:**
```go
ownerAddr := common.BytesToAddress(c.OwnerAddress)

// 1. Deduct sell tokens from owner
deductToken(ctx.State, ownerAddr, c.SellTokenId, c.SellTokenQuantity)

// 2. Generate order ID
txHash := ctx.Tx.Hash()
orderID := generateOrderID(ownerAddr, txHash)

// 3. Create order
order := &corepb.MarketOrder{
    OrderId:                  orderID,
    OwnerAddress:             c.OwnerAddress,
    CreateTime:               ctx.BlockTime,
    SellTokenId:              c.SellTokenId,
    SellTokenQuantity:        c.SellTokenQuantity,
    BuyTokenId:               c.BuyTokenId,
    BuyTokenQuantity:         c.BuyTokenQuantity,
    SellTokenQuantityRemain:  c.SellTokenQuantity,
    State:                    corepb.MarketOrder_ACTIVE,
}

// 4. Match
remaining, err := matchOrder(ctx.DB, ctx.State, order)
if err != nil { return nil, err }

// 5. If remaining > 0: add to order book
if remaining > 0 {
    order.SellTokenQuantityRemain = remaining
    if err := addOrderToBook(ctx.DB, order); err != nil { return nil, err }
} else {
    order.State = corepb.MarketOrder_INACTIVE
}

// 6. Persist order
if err := rawdb.WriteMarketOrder(ctx.DB, orderID, order); err != nil { return nil, err }

// 7. Update account order list
mao := rawdb.ReadMarketAccountOrder(ctx.DB, c.OwnerAddress)
mao.Orders = append(mao.Orders, orderID)
mao.TotalCount++
if order.State == corepb.MarketOrder_ACTIVE { mao.Count++ }
if err := rawdb.WriteMarketAccountOrder(ctx.DB, c.OwnerAddress, mao); err != nil { return nil, err }

return &Result{Fee: 0, ContractRet: 1}, nil
```

**`addOrderToBook`:**
```go
func addOrderToBook(db ethdb.KeyValueStore, order *corepb.MarketOrder) error {
    pk := priceKey(order.SellTokenQuantity, order.BuyTokenQuantity)
    
    // Append to price list
    pl := rawdb.ReadMarketPriceList(db, order.SellTokenId, order.BuyTokenId)
    pkExists := false
    for _, p := range pl.Prices {
        if p.SellTokenQuantity == order.SellTokenQuantity/gcdInt64(order.SellTokenQuantity, order.BuyTokenQuantity) &&
           p.BuyTokenQuantity == order.BuyTokenQuantity/gcdInt64(order.SellTokenQuantity, order.BuyTokenQuantity) {
            pkExists = true; break
        }
    }
    if !pkExists {
        g := gcdInt64(order.SellTokenQuantity, order.BuyTokenQuantity)
        pl.Prices = append(pl.Prices, &corepb.MarketPrice{
            SellTokenQuantity: order.SellTokenQuantity/g,
            BuyTokenQuantity:  order.BuyTokenQuantity/g,
        })
        rawdb.WriteMarketPriceList(db, order.SellTokenId, order.BuyTokenId, pl)
    }
    
    // Append to linked list (as tail)
    list := rawdb.ReadMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk)
    if list == nil {
        list = &corepb.MarketOrderIdList{Head: order.OrderId, Tail: order.OrderId}
    } else {
        // Update previous tail's Next pointer
        if len(list.Tail) > 0 {
            prevTail := rawdb.ReadMarketOrder(db, list.Tail)
            if prevTail != nil {
                prevTail.Next = order.OrderId
                rawdb.WriteMarketOrder(db, list.Tail, prevTail)
            }
        }
        order.Prev = list.Tail
        list.Tail = order.OrderId
    }
    return rawdb.WriteMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk, list)
}
```

### MarketCancelOrderActuator (type 53)

**File:** `actuator/market_cancel_order.go`

**Validate:**
1. Unmarshal `MarketCancelOrderContract`
2. Owner exists
3. `len(c.OrderId) == 0` → error "order ID required"
4. Order = `ReadMarketOrder(ctx.DB, c.OrderId)` — must not be nil
5. `!bytes.Equal(order.OwnerAddress, c.OwnerAddress)` → error "not order owner"
6. `order.State != ACTIVE` → error "order not active"

**Execute:**
```go
order := rawdb.ReadMarketOrder(ctx.DB, c.OrderId)
ownerAddr := common.BytesToAddress(c.OwnerAddress)

// Return remaining sell tokens to owner
if order.SellTokenQuantityRemain > 0 {
    transferToken(ctx.State, ownerAddr, order.SellTokenId, order.SellTokenQuantityRemain)
}

// Remove from order book
pk := priceKey(order.SellTokenQuantity, order.BuyTokenQuantity)
removeOrderFromBook(ctx.DB, order, pk)

// Mark as canceled
order.State = corepb.MarketOrder_CANCELED
order.SellTokenQuantityReturn = order.SellTokenQuantityRemain
order.SellTokenQuantityRemain = 0
rawdb.WriteMarketOrder(ctx.DB, c.OrderId, order)

// Update account order list
mao := rawdb.ReadMarketAccountOrder(ctx.DB, c.OwnerAddress)
if mao.Count > 0 { mao.Count-- }
rawdb.WriteMarketAccountOrder(ctx.DB, c.OwnerAddress, mao)

return &Result{Fee: 0, ContractRet: 1}, nil
```

**`removeOrderFromBook`** — unlinks an order from the doubly-linked list at the given price key. If the list becomes empty, removes the price from the price list.

---

## actuator.go Registration

Add to `CreateActuator` switch (in the existing cases):

```go
case corepb.Transaction_Contract_FreezeBalanceContract:
    return &FreezeBalanceActuator{}, nil
case corepb.Transaction_Contract_UnfreezeBalanceContract:
    return &UnfreezeBalanceActuator{}, nil
case corepb.Transaction_Contract_MarketSellAssetContract:
    return &MarketSellAssetActuator{}, nil
case corepb.Transaction_Contract_MarketCancelOrderContract:
    return &MarketCancelOrderActuator{}, nil
```

---

## HTTP API

### Backend Interface Additions (`internal/tronapi/backend.go`)

```go
// Market queries (Phase 13)
GetMarketOrderByID(orderID []byte) *corepb.MarketOrder
GetMarketOrdersByAccount(addr common.Address) []*corepb.MarketOrder
GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList
```

### Routes (`internal/tronapi/api.go`)

In `RegisterRoutes`, add after the TRC10 routes:

```go
// Phase 13: Market order queries
mux.HandleFunc("/wallet/getmarketorderbyid", api.getMarketOrderByID)
mux.HandleFunc("/wallet/getmarketordersfromaccount", api.getMarketOrdersFromAccount)
mux.HandleFunc("/wallet/getmarketpricebypair", api.getMarketPriceByPair)
```

### Handler Implementations

```go
func (api *API) getMarketOrderByID(w http.ResponseWriter, r *http.Request) {
    var body struct{ Value string `json:"value"` }
    json.NewDecoder(r.Body).Decode(&body)
    if body.Value == "" { body.Value = r.URL.Query().Get("value") }
    if body.Value == "" { http.Error(w, "value required", http.StatusBadRequest); return }
    orderID := common.FromHex(body.Value)
    order := api.backend.GetMarketOrderByID(orderID)
    if order == nil { w.Header().Set("Content-Type","application/json"); w.Write([]byte("{}")); return }
    writeTronJSON(w, order)
}

func (api *API) getMarketOrdersFromAccount(w http.ResponseWriter, r *http.Request) {
    var body struct{ Address string `json:"address"` }
    json.NewDecoder(r.Body).Decode(&body)
    if body.Address == "" { body.Address = r.URL.Query().Get("address") }
    if body.Address == "" { http.Error(w, "address required", http.StatusBadRequest); return }
    addr := common.BytesToAddress(common.FromHex(body.Address))
    orders := api.backend.GetMarketOrdersByAccount(addr)
    var list []map[string]any
    for _, o := range orders { list = append(list, marshalMessage(o.ProtoReflect())) }
    if list == nil { list = []map[string]any{} }
    data, _ := json.Marshal(map[string]any{"orders": list})
    w.Header().Set("Content-Type", "application/json")
    w.Write(data)
}

func (api *API) getMarketPriceByPair(w http.ResponseWriter, r *http.Request) {
    var body struct {
        SellTokenId string `json:"sell_token_id"`
        BuyTokenId  string `json:"buy_token_id"`
    }
    json.NewDecoder(r.Body).Decode(&body)
    if body.SellTokenId == "" || body.BuyTokenId == "" {
        http.Error(w, "sell_token_id and buy_token_id required", http.StatusBadRequest); return
    }
    pl := api.backend.GetMarketPriceByPair([]byte(body.SellTokenId), []byte(body.BuyTokenId))
    if pl == nil { w.Header().Set("Content-Type","application/json"); w.Write([]byte("{}")); return }
    writeTronJSON(w, pl)
}
```

### TronBackend Implementations (`core/tron_backend.go`)

```go
func (b *TronBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder {
    return rawdb.ReadMarketOrder(b.chain.db, orderID)
}

func (b *TronBackend) GetMarketOrdersByAccount(addr tcommon.Address) []*corepb.MarketOrder {
    mao := rawdb.ReadMarketAccountOrder(b.chain.db, addr[:])
    var orders []*corepb.MarketOrder
    for _, id := range mao.Orders {
        if o := rawdb.ReadMarketOrder(b.chain.db, id); o != nil {
            orders = append(orders, o)
        }
    }
    return orders
}

func (b *TronBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
    return rawdb.ReadMarketPriceList(b.chain.db, sellTokenID, buyTokenID)
}
```

---

## Testing

### `core/state/statedb_v1_test.go`
- `TestFreezeV1Bandwidth` — freeze, check TotalFrozenBandwidth, check balance deducted
- `TestUnfreezeV1Bandwidth` — freeze, advance blockTime, unfreeze, check refund
- `TestFreezeV1Energy` — freeze, add more (accumulates), check
- `TestUnfreezeV1Energy_NotExpired` — unfreeze before expire → no refund
- `TestFreezeV1DelegatedBandwidth` — freeze delegated, check both accounts
- `TestUnfreezeV1DelegatedBandwidth` — unfreeze delegated, check both accounts

### `actuator/freeze_balance_test.go`
- `TestFreezeBalanceValidate_Success`
- `TestFreezeBalanceValidate_InsufficientBalance`
- `TestFreezeBalanceValidate_DurationTooShort`
- `TestFreezeBalanceExecute_Bandwidth` — check bandwidth frozen list
- `TestFreezeBalanceExecute_Energy` — check energy frozen entry
- `TestFreezeBalanceExecute_Delegated` — check delegation fields on both accounts

### `actuator/unfreeze_balance_test.go`
- `TestUnfreezeBalanceValidate_NotExpired` — should fail
- `TestUnfreezeBalanceValidate_NoFrozen` — should fail
- `TestUnfreezeBalanceExecute_Bandwidth` — unfreeze after expire, balance restored
- `TestUnfreezeBalanceExecute_Energy` — same for energy
- `TestUnfreezeBalanceExecute_Delegated` — restore delegated bandwidth

### `core/rawdb/accessors_market_test.go`
- `TestWriteReadMarketOrder`
- `TestReadMarketOrder_NotFound`
- `TestWriteReadMarketAccountOrder`
- `TestWriteReadMarketOrderBook`
- `TestWriteReadMarketPriceList`

### `actuator/market_sell_asset_test.go`
- `TestMarketSellAssetValidate_Success`
- `TestMarketSellAssetValidate_InsufficientBalance`
- `TestMarketSellAssetValidate_SameToken`
- `TestMarketSellAssetExecute_NoMatch` — order goes into book
- `TestMarketSellAssetExecute_FullMatch` — two orders that fully match
- `TestMarketSellAssetExecute_PartialMatch` — incoming partially fills existing

### `actuator/market_cancel_order_test.go`
- `TestMarketCancelOrderValidate_NotOwner`
- `TestMarketCancelOrderValidate_AlreadyInactive`
- `TestMarketCancelOrderExecute_ReturnsTokens`
- `TestMarketCancelOrderExecute_RemovesFromBook`

### `scripts/system_test.sh` — Section 12

```bash
# ─────────────────────────────────────────────────────────────────
# SECTION 12: Phase 13 — Market Order Query Endpoints
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 12: Market Order Query Endpoints ==="

# 12.1 getmarketorderbyid — unknown returns {}
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getmarketorderbyid" \
    -H "Content-Type: application/json" \
    -d '{"value":"0000000000000000000000000000000000000000000000000000000000000000"}' \
    2>/dev/null || echo "CURL_ERROR")
check "getmarketorderbyid unknown returns {}" "$RESULT" '{}'

# 12.2 getmarketordersfromaccount — unknown account returns orders array
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getmarketordersfromaccount" \
    -H "Content-Type: application/json" \
    -d "{\"address\":\"$WITNESS_ADDR\"}" 2>/dev/null || echo "CURL_ERROR")
check "getmarketordersfromaccount returns orders key" "$RESULT" '"orders"'

# 12.3 getmarketpricebypair — unknown pair returns {}
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getmarketpricebypair" \
    -H "Content-Type: application/json" \
    -d '{"sell_token_id":"1000001","buy_token_id":"_"}' 2>/dev/null || echo "CURL_ERROR")
check "getmarketpricebypair unknown returns {}" "$RESULT" '{}'
```

---

## No New External Dependencies

Standard library only: `encoding/binary`, `math/big`, `bytes`, `strconv`. No third-party packages.

## Error Handling

- All rawdb Write calls return `error` — propagate with `fmt.Errorf("...: %w", err)`
- Matching engine returns `error` — propagate from Execute
- `null` result for missing order/price is a success (`{}` response), not an error

## Task Decomposition

| Task | Files | Content |
|---|---|---|
| 1 | `core/types/account.go`, `core/state/statedb.go`, `core/state/statedb_v1_test.go` | V1 frozen accessors + StateDB methods |
| 2 | `actuator/freeze_balance.go`, `actuator/freeze_balance_test.go` | FreezeBalanceActuator (type 11) |
| 3 | `actuator/unfreeze_balance.go`, `actuator/unfreeze_balance_test.go` | UnfreezeBalanceActuator (type 12) |
| 4 | `core/rawdb/schema.go`, `core/rawdb/accessors_market.go`, `core/rawdb/accessors_market_test.go` | Market rawdb schema + accessors |
| 5 | `actuator/market_sell_asset.go`, `actuator/market_sell_asset_test.go` | MarketSellAssetActuator + matching engine |
| 6 | `actuator/market_cancel_order.go`, `actuator/market_cancel_order_test.go` | MarketCancelOrderActuator |
| 7 | `actuator/actuator.go` | Register types 11, 12, 52, 53 |
| 8 | `internal/tronapi/backend.go`, `internal/tronapi/api.go`, `core/tron_backend.go` | HTTP market query endpoints |
| 9 | `scripts/system_test.sh` | Section 12 + final verification |
