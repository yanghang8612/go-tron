# M1.2 Freeze V1 Legacy 支持 — 设计说明

**日期：** 2026-04-15
**状态：** Draft
**目标：** 让 go-tron 在消费带宽/能量时正确识别并累计 V1 冻结余额（`frozen` list、`account_resource.frozen_balance_for_energy`），使历史账户的 V1 冻结资源可被交易使用；同时在冻结/解冻路径上同步全局权重计数器 `total_net_weight` / `total_energy_weight`，并在 `AllowNewResourceModel`（proposal #62 / key `allow_new_resource_model`）激活后按 java-tron 行为拒绝新的 V1 freeze。

对应 [PLAN.md M1.2](../../../PLAN.md#m12-freeze-v1-legacy-路径)。依赖：M1.1（DP 已对齐）。

---

## 1. 背景

> 原始数据来源：java-tron `BandwidthProcessor.java` 432-449、`EnergyProcessor.java` 149-160、`FreezeBalanceActuator.java` 88-150/271-273、`UnfreezeBalanceActuator.java` 195-260、`AccountCapsule.java` 929-1092。

Freeze V2（proposal `ALLOW_NEW_RESOURCE_MODEL`，ID 62，激活高度 42908900）上线后 **V1 与 V2 在消费侧同时存在**：

- BandwidthProcessor 用 `getAllFrozenBalanceForBandwidth() = V1_frozen + V1_delegated + V2_frozen + V2_delegated` 计算可用带宽。
- EnergyProcessor 同理取 `frozen_balance_for_energy (V1)` 与 V2 之和。
- 单位差异：V1 权重是"TRX"（值 `= frozen_balance / TRX_PRECISION`，即除以 1_000_000），V2 权重是"SUN"（直接使用 `frozen_balance`）。`TotalNetWeight` / `TotalEnergyWeight` 累积的是这两种值的**混合合计**。
- `FreezeBalanceActuator` 在 V2 激活后会拒绝新的 V1 freeze（`FreezeBalanceActuator.java` 271-273: "freeze v2 is open, old freeze is closed"），但**已有的 V1 冻结余额继续正常消费**，直到 owner 主动 unfreeze。
- `UnfreezeBalanceActuator` 保留 V1 解冻路径：遍历 `frozen` list，弹出 `expireTime <= now` 的条目，按负权重调整 `TotalNetWeight` / `TotalEnergyWeight`。
- `UnfreezeDelayDays`（M1.1 校正为 default 0，V2 激活后由 proposal #70 调到 14）只作用于 V2；V1 unfreeze 使用 `expireTime`（freeze 时刻 + `FROZEN_PERIOD`），**不读 `UnfreezeDelayDays`**。

### go-tron 现状（core/bandwidth.go、core/resource.go、actuator/freeze_balance.go、actuator/unfreeze_balance.go、core/state/statedb.go）

| 层面 | 已有 | 缺失 |
|---|---|---|
| Proto accessors | `core/types/account.go:223-323` 完整 V1/V1-delegated getter/setter/remove 已在 M0 补齐 | — |
| StateDB 冻结写入 | `FreezeV1Bandwidth`、`FreezeV1Energy`、`FreezeV1DelegatedBandwidth/Energy`（`core/state/statedb.go:172-220`） | — |
| StateDB 解冻 | `UnfreezeV1Bandwidth`、`UnfreezeV1Energy` | — |
| **权重同步** | — | 冻结/解冻都**未**更新 `total_net_weight` / `total_energy_weight` |
| **BandwidthProcessor V1 消费** | `core/bandwidth.go:29` 仅取 `GetFrozenV2Amount(…, BANDWIDTH)` | 缺 V1 sum 与 delegated V1 sum |
| **EnergyProcessor V1 消费** | `core/resource.go` 只有 usage recovery，无权重侧逻辑 | 同上 |
| Actuator | `FreezeBalanceActuator`、`UnfreezeBalanceActuator` 实现 V1 但不更新权重，也未按 `allow_new_resource_model` 拒绝新 V1 freeze | 权重更新 + fork gate |

**影响**：任何持有 V1 冻结余额的账户（≈ 2020 年前活跃的大部分主网地址）在 gtron 上执行转账时会被误判为带宽/能量不足——直接触发链上状态分叉。

## 2. 范围

**In scope**

1. `core/bandwidth.go` `consumeBandwidth`：用 `V1_frozen + V1_delegated + V2_frozen + V2_delegated` 计算可用带宽；按 java-tron 同一公式 `(frozen / TRX_PRECISION) * (totalNetLimit / totalNetWeight)`。
2. `core/resource.go` 等价的能量消费路径（目前仅有 usage recovery，需新增 `calculateGlobalEnergyLimit` + 消费支路）。
3. Actuator 侧 `TotalNetWeight` / `TotalEnergyWeight` 更新：
   - `FreezeBalanceActuator`：根据 `allow_new_reward`（proposal #67，已在 M1.1 落到 DP）分支，V1 按 `amount / TRX_PRECISION` 增量；
   - `UnfreezeBalanceActuator`：过期条目做负向增量。
4. `FreezeBalanceActuator` 入口加 fork gate：`allow_new_resource_model == 1` 时拒绝新的 V1 freeze（错误信息对齐 java-tron 文案）。
5. `UnfreezeBalanceActuator` 路径**不加** gate（即使 V2 激活后，仍允许对历史 V1 冻结做 unfreeze）。
6. 单元测试覆盖三种账户形态：纯 V1、纯 V2、V1+V2 混合（带 delegated）；覆盖 freeze 前/后、unfreeze 前/后的 `total_*_weight` 值。
7. fixture 扩展：在 `test/fixtures/` 新增一个 **合成 scenario**（不需 java-tron 重放），对"带 V1 frozen 的账户"预置 accounts json 条目，供 bandwidth/energy 消费公式做 golden-value 断言。
8. `PLAN.md` M1.2 行更新为完成。

**Out of scope**（另行里程碑）

- Freeze V2 委托资源从 delegator→delegatee 的实时账务（M1.8）。
- 版本位投票 / fork audit（M1.3）。
- 从真实 java-tron 重放 V1/V2 混合账户的 state root（M0″）。
- 奖励侧 V1/V2 视图（M1.5，与 `allow_new_reward` 强耦合）。

## 3. 验收路径

**A. 单元测试（必过）**

- `core/bandwidth_v1_test.go`
  - 给账户预置 `frozen: [{ balance: 1_000_000, expire: past }]`、`total_net_weight: 1`、`total_net_limit: 43_200_000_000`；断言 `availableNet == 43_200_000_000`（整个池子都给这唯一持仓）。
  - 纯 V2：预置 `frozenV2: [{ type: BANDWIDTH, amount: 1_000_000_000 }]`、`total_net_weight: 1_000`；断言对应 `availableNet`。
  - 混合：两者都在，断言 V1 + V2 权重相加后的分配一致。
- `core/resource_v1_test.go` 能量镜像。
- `actuator/freeze_v1_weight_test.go`：
  - freeze V1 后 `total_net_weight += amount/TRX_PRECISION`；
  - unfreeze V1 过期条目后同量减去；
  - `allow_new_resource_model=1` 时 FreezeBalance V1 返回 `ContractValidateException`，文案对齐 java。
- `actuator/unfreeze_v1_postfork_test.go`：`allow_new_resource_model=1` 仍可 unfreeze 已有 V1 条目。

**B. Fixture 一致性（软约束）**

- 新 fixture scenario `02-v1-frozen-synthetic`：手工构造 accounts + DP，在 M1.2 里只作为 getter 断言；真正的 "跑一条交易 → state diff" 留给 M0″。

**C. 回归**

- `go test ./... -count=1` 全绿；M1.1 的 `TestDynamicProperties_MatchMainnetFixture` 继续通过。
- `actuator/freeze_balance_test.go` 既有覆盖更新为两种 fork 状态（未激活 / 激活）各一组。

## 4. 设计要点

### 4.1 公式镜像

java-tron `BandwidthProcessor.calculateGlobalNetLimit`：

```
frozenBalance = V1_sum_bandwidth + V2_amount(BANDWIDTH) + delegated_V1 + delegated_V2
if frozenBalance < TRX_PRECISION: return 0
weight = frozenBalance / TRX_PRECISION       // 注意：V1 list 的 balance 本就是 SUN，这里的除法是把 SUN → TRX
return (weight * totalNetLimit) / totalNetWeight
```

Go 这边在 `core/bandwidth.go` 新建 `availableAccountNet(acct, dp)`：

```go
frozen := acct.FrozenBandwidthTotal()          // sum of `frozen` list .balance
frozen += acct.DelegatedFrozenBandwidth()      // V1 delegated out → still counts as owner's weight
frozen += dp_v2_bandwidth(acct)                // V2 stake
frozen += dp_v2_delegated_bandwidth(acct)      // V2 delegated out
if frozen < TrxPrecision { return 0 }
weight := frozen / TrxPrecision
return weight * dp.GetTotalNetLimit() / dp.GetTotalNetWeight()
```

能量侧镜像，换 `FrozenEnergyAmount` + `DelegatedFrozenEnergy` + V2 energy。

### 4.2 权重累加的 fork 依赖

`FreezeBalanceActuator` 当前（java-tron 134-150）：

```
if (allow_new_reward) {
    newWeight  = newFrozenBalance / TRX_PRECISION
    oldWeight  = oldFrozenBalance / TRX_PRECISION
    delta      = newWeight - oldWeight
} else {
    delta = frozenBalance / TRX_PRECISION     // legacy：直接加本次冻结的 weight
}
dynamicStore.addTotalNetWeight(delta)
```

`allow_new_reward` = proposal #67（M1.1 已有 key `allow_new_reward`，default 0；mainnet 从块 35_291_108 起=1）。

Go 这边新增 `state.StateDB.AddTotalNetWeight(delta int64)` / `AddTotalEnergyWeight(delta int64)`（透传到 DP），在 actuator 分支调用。

### 4.3 post-fork 拒绝 V1 freeze

```go
if forks.IsActive(forks.AllowNewResourceModel, blockNum, ctx.DP) {
    return errors.New("freeze v2 is open, old freeze is closed")
}
```

插入在 `actuator/freeze_balance.go` Validate 入口，resource==BANDWIDTH/ENERGY 两分支共用。

### 4.4 delegated V1 的"不迁移"

java-tron 在 V2 激活后 **不会** 自动把 V1 delegated 迁到 V2——委托对两方都保持 V1 记录直到主动 undelegate。go-tron 同样保持："Freeze V1→Delegate" 的历史记录消费路径照常，不额外数据迁移。

## 5. Task 大纲（详见同日 plan）

1. Fixture：`02-v1-frozen-synthetic` scenario JSON + loader 断言。
2. StateDB：`AddTotalNetWeight` / `AddTotalEnergyWeight` 与对应 DP setter。
3. BandwidthProcessor：V1 消费路径 + 混合公式。
4. EnergyProcessor：镜像 + `calculateGlobalEnergyLimit`。
5. Actuator：freeze V1 加权重更新 + post-fork gate；unfreeze V1 加负权重更新。
6. 单元测试：五项（A.1~A.4 + 回归）。
7. PLAN.md + CLAUDE.md pointer 更新。

## 6. 风险

| 风险 | 缓解 |
|---|---|
| V1 权重公式里的"除以 TRX_PRECISION"被误整数除 → 小额账户权重归 0 与 java 一致 | 断言 `frozen < TRX_PRECISION → 0`，单测覆盖临界值 999_999、1_000_000、1_999_999 |
| delegated V1 的"双重计数"：当 A delegate 给 B 时，A 的 V1 delegated 与 B 的 V1 acquired 是否都纳入各自权重？ | 严格对照 java `AccountCapsule.getAllFrozenBalanceForBandwidth()`（含 `delegated_frozenBalance_for_bandwidth`），B 侧的 acquired 不计入 B 的权重（java-tron 逻辑：委托给出方保留权重用于计算，委托持有方只获得"额度"消耗） |
| 回归：改 BandwidthProcessor 可能让 M0 快照的 smoke 账户计算出错 | Task 6 单测在老 scenario（纯 V2）上也跑一次，断言数值不变 |
| allow_new_reward 未激活期间的账户冻结权重 delta 与当前 go-tron 实现假设不同 | M1.1 已验 `allow_new_reward` default 0 与 fixture 一致；分支条件显式写 `if !forks.IsActive(AllowNewReward) → delta = amount/1e6`（legacy 路径） |
