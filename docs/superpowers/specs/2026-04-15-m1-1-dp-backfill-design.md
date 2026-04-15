# M1.1 DynamicProperties Backfill — 设计说明

**日期：** 2026-04-15
**状态：** Draft
**目标：** 让 go-tron 新建链的 `DynamicProperties` 产出 76 个与 java-tron mainnet 默认值逐一相同的 (key, value) 对；同时修正 `core/forks/forks.go` 的 proposal ID → key 映射，使其与 java-tron `ProposalUtil.ProposalType` 枚举一致。

对应 [PLAN.md M1.1](../../../PLAN.md#m11-dynamicproperties-全量-backfill)。Golden 数据来源：`test/fixtures/00-genesis-dp-mainnet/fixture.json`（M0′ 产出）。

---

## 1. 背景与问题

从 fixture vs 当前 `core/state/dynamic_properties.go` 的比对结果（2026-04-15 master）：

- **44 个 key 缺失**：fixture 有、go-tron 无。覆盖自适应能量、Freeze V2、奖励 v2、动态能量、TVM 标志、市场、费率、系统簿记等簇。
- **6 处命名错位**：go-tron 现有 key 名与 java-tron getter 反向推导的 snake_case 不一致——governance 提案应用会把值写到错误的 key，等价于静默分叉：
  - `account_permission_update_fee` → `update_account_permission_fee`（java `getUpdateAccountPermissionFee`）
  - `allow_adaptive_energy_limit` → `allow_adaptive_energy`（java `getAllowAdaptiveEnergy`）
  - `allow_change_delegation` → `change_delegation`（java `getChangeDelegation`，无 Allow 前缀）
  - `allow_tvm_compatibility` → `allow_tvm_compatible_evm`（java `getAllowTvmCompatibleEvm`）
  - `allow_tvm_big_integer`、`allow_tvm_shielded_token` —— java-tron 当前版本 `getchainparameters` 未返回；保留 getter 但在 fixture 比对中显式跳过，后续验证是否仍有链上语义。
- **`core/forks/forks.go` proposal ID 映射多条错误**：抽样对照 `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` 的 `ProposalType` 枚举：
  - go-tron `14: allow_same_token_name` ↔ java `15`
  - go-tron `17: allow_adaptive_energy_limit` ↔ java `21 ALLOW_ADAPTIVE_ENERGY`
  - go-tron `33: allow_market_transaction` ↔ java `44`
  - ……全量核对后全表重建。
- **22 个 "额外" key** 在 go-tron 有、fixture 无：其中一部分是链上内部簿记（`latest_block_header_number`、`next_token_id`、`next_exchange_id`、`next_proposal_id` 等计数器），另一部分是冗余/废弃（`allow_tvm_compatibility` 等需删或改名）。

以上问题中，**任一项单独发生都会导致 state 分叉或治理票投错位置**，属 P0 分叉风险。

## 2. 范围

**In scope**
- 在 `core/state/dynamic_properties.go` 补齐 fixture 中的 76 个 key；默认值以 fixture 为准，并二次核对 `ProposalUtil.java` 注释与 `DynamicPropertiesStore.java` 的初始化常量。
- 为每个新加 key 提供 getter/setter（bool 类 + int64 数值类）。
- 重命名 6 处命名错位的 key；全仓库引用同步修正。
- 重建 `core/forks/forks.go` 的 `ProposalParamKey` 映射，覆盖 `ProposalType` 枚举全部未注释掉的条目。
- 新增 fixture 驱动的一致性测试：迭代 fixture 的每个 key，断言 go-tron 初始化后 DP 同 key 同值。
- 对 22 个 "额外" key 分类：(a) 内部簿记保留；(b) 冗余删除；(c) 改名归并。

**Out of scope**（迁至其他 M1.x）
- 自适应能量 **计算逻辑** —— 仅加 key，逻辑在 M1.4。
- 奖励 v2 **计算逻辑** —— M1.5。
- 存储租金 **逻辑** —— M1.6。
- 动态能量 **逻辑** —— M1.7。
- Freeze V2 委托消费 **账务** —— M1.8。
- 提案应用 **行为** —— 既有 `ProcessProposals` 保留不动；本里程碑只保证它读到正确的 key。
- 版本位分叉投票 —— M1.3。
- Nile 链参数对齐 —— 等 Nile fixture（Task 6 延后项）。

## 3. 验收路径

一致性测试 = 本里程碑的"完成"判据：

```go
func TestDynamicProperties_MatchMainnetFixture(t *testing.T) {
    fix := fixture.Load(t, "00-genesis-dp-mainnet")
    db := rawdb.NewMemoryDatabase()
    dp, err := state.NewDynamicProperties(db, params.MainnetConfig())
    require.NoError(t, err)

    for javaKey, want := range fix.DynamicProperties {
        goKey := javaGetterToGoKey(javaKey)
        if goKey == "" {
            t.Errorf("no go-tron key mapped for java %q", javaKey)
            continue
        }
        got := dp.GetRaw(goKey)
        if got != want {
            t.Errorf("DP[%s/%s]: got %d, want %d", javaKey, goKey, got, want)
        }
    }
}
```

退出条件：此测试 0 errors、0 skips（skips 仅限本文档 §1 中明确列出的 "非 fixture" 极少数例外）。

## 4. 设计

### 4.1 映射表

新建 `core/state/dp_key_mapping.go`：

```go
// javaGetterToGoKey maps /wallet/getchainparameters key names (java-tron
// getter conventions) to go-tron snake_case DP keys. Authoritative source
// for the 76 parameters exposed via governance + internal flags.
//
// Extend only when java-tron adds a new chain parameter (bump fixture,
// add the mapping, add the DP key + default).
func javaGetterToGoKey(javaGetter string) string
```

全显式列举，不做自动 snake 化（避免 PBFT/TRC20/CPU 这类三连大写误判）。映射表 + 一致性测试放在同一文件便于 diff review。

### 4.2 默认值来源

- 数值：fixture.json 的 `dynamicProperties.<key>`。
- 复核：对照 `java-tron/chainbase/src/main/java/org/tron/core/store/DynamicPropertiesStore.java` 的常量与 `init()`；冲突时以 fixture 为准（因为 fixture 是运行时观测值）。
- 默认值一次性在 `defaultProps` map 写死；不依赖 `params/mainnet.go`（后者只提供链级覆盖，不覆盖就走默认）。

### 4.3 重命名处理

6 处老 key 名用 Edit 批量替换。清单锁定在 plan 的 Task 2。**不保留任何 alias / backward-compat**——此仓库尚未 release，重命名即改即成。

### 4.4 额外 key 分类

| key | 分类 | 处置 |
|---|---|---|
| `latest_block_header_number` | 内部簿记 | 保留 |
| `latest_block_header_timestamp` | 内部簿记 | 保留 |
| `latest_solidified_block_num` | 内部簿记 | 保留 |
| `next_maintenance_time` | 内部簿记 | 保留 |
| `next_proposal_id` | 计数器 | 保留 |
| `next_token_id` | 计数器 | 保留 |
| `next_exchange_id` | 计数器 | 保留 |
| `total_sign_num` | 冗余（fixture 无） | 归入 `getTotalSignNum`——留意 java-tron 有同名 getter 但不通过 chainparameter 暴露；**保留** |
| `exchange_balance_limit` | 历史遗留 | **删除**（java 无对应，且 `ProposalUtil` 未出现） |
| `allow_account_history` | 实为 java `getAllowAccountStateRoot`？ | Task 4 核对；若是 rename 则归 4.3，否则删 |
| `allow_tvm_big_integer` | java getter 存在，不在 chainparameters | 保留 getter，一致性测试 skip |
| `allow_tvm_shielded_token` | 同上 | 保留 getter，一致性测试 skip |
| `allow_shielded_transaction` | shielded 池开关（v1，legacy） | 保留，一致性测试 skip（已提前为 M1.x 的 shielded 工作加入） |
| `shielded_transaction_fee` / `shielded_transaction_create_account_fee` / `zen_token_id` / `total_shielded_pool_value` | shielded v1 相关 | 保留，测试 skip |
| `allow_tvm_compatibility` | rename 到 `allow_tvm_compatible_evm` | 见 4.3 |
| `account_permission_update_fee` | rename 到 `update_account_permission_fee` | 见 4.3 |
| `allow_adaptive_energy_limit` | rename 到 `allow_adaptive_energy` | 见 4.3 |
| `allow_change_delegation` | rename 到 `change_delegation` | 见 4.3 |
| `allow_pbft` | 拼法已对（java `getAllowPBFT` → `allow_pbft`） | 保留，更新一致性测试 skip 列表为空 |

### 4.5 forks.go 重建

`ProposalParamKey` 完全按 `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java` 的 `ProposalType` 枚举重写。注释掉的历史 ID（27, 28, 34, 42, 43, 58）保留为注释，便于后续对照但不映射。引入一个表驱动单测，循环检查所有活跃 ID 映射到的 go-tron key 存在于 `DynamicProperties.defaultProps`。

## 5. 风险

| 风险 | 缓解 |
|---|---|
| 默认值 fixture 与 DynamicPropertiesStore.java 冲突 | plan Task 3 要求逐 key 附注 java 源文件行号；冲突在 PR 里显式讨论 |
| rename 漏掉某处调用点 | 先跑 `grep` 全量定位，再批量 Edit；最后 `go build ./... && go test ./... ` 兜底 |
| forks.go 映射表老代码被其他包直接复用 | `ProposalParamKey` 目前是唯一出口，grep 确认无旁路 |
| shielded / 部分非 chainparameter 暴露的 key 今后确实分叉 | 一致性测试把它们显式列入 skip set，评审时必须手工确认每条 skip |

## 6. 未决问题

- `allow_account_history` 是 go-tron 内部的什么？需 git log 考古，或等 Task 4 挖出。
- `getRemoveThePowerOfTheGr`（fixture 有）默认是 1；java 枚举标签 10 对应此 key；需确认这是主网已激活的"移除 GR 特权"开关，还是仍未启用。

## 7. 完成定义

1. `go test ./core/state -run TestDynamicProperties_MatchMainnetFixture -v` 0 失败。
2. `go test ./core/forks -run TestProposalParamKey_AllMapped -v` 0 失败。
3. `go build ./... && go test ./... -count=1` 全绿。
4. PLAN.md M1.1 行标"完成"，记录提交哈希。
