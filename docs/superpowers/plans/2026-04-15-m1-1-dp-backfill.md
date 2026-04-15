# M1.1 DynamicProperties Backfill 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 [m1-1-dp-backfill-design](../specs/2026-04-15-m1-1-dp-backfill-design.md) spec：补全 `core/state/dynamic_properties.go` 缺失的 44 个 key、修正 6 处命名错位、重建 `core/forks/forks.go` 的提案 ID 映射、并由 fixture 驱动的一致性测试作为验收标志。

**Architecture:** 新增一个显式的 `javaGetter → goKey` 映射表；`defaultProps` 按 fixture 值补齐；`ProposalParamKey` 全量重建。所有改动由 `test/fixtures/00-genesis-dp-mainnet/fixture.json` 作为 golden oracle。

**Tech Stack:** 既有 Go 1.25 工具链；无新依赖。

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `core/state/dp_key_mapping.go` | `javaGetterToGoKey` 显式映射 + 文档 |
| Create | `core/state/dp_key_mapping_test.go` | 映射表自洽性测试（无空目标、无重复） |
| Create | `core/state/dynamic_properties_fixture_test.go` | fixture-driven 一致性测试（主验收） |
| Modify | `core/state/dynamic_properties.go` | +44 key default、+getter/setter；6 处 rename |
| Modify | `core/state/*_test.go` 及其他引用旧名的测试 | rename 同步 |
| Modify | `core/forks/forks.go` | 按 ProposalType 重建 `ProposalParamKey` |
| Create | `core/forks/forks_proposal_mapping_test.go` | 表驱动单测：每个活跃 ProposalType → 存在的 DP key |
| Modify | `actuator/proposal_approve.go`（若用旧 key） | rename 同步 |
| Modify | `actuator/fork_gates_test.go` | rename 同步 |
| Modify | `core/proposal.go` / 任何读 DP 的消费者 | rename 同步 |

---

## Task 1：映射表 + 一致性测试（先失败）

先把测试框架立起来、故意让它爆红；这样后续每个 Task 的进展都可量化。

**Files:**
- Create: `core/state/dp_key_mapping.go`
- Create: `core/state/dp_key_mapping_test.go`
- Create: `core/state/dynamic_properties_fixture_test.go`

**Steps:**

- [ ] **Step 1**：写 `javaGetterToGoKey`，先只覆盖已存在于 go-tron 的 key（大约 20 条）；其余 56 条映射到 `""`（测试会跑出 56 个错误）。

```go
package state

var javaGetterToGoKeyMap = map[string]string{
    "getMaintenanceTimeInterval": "maintenance_time_interval",
    "getAccountUpgradeCost":      "account_upgrade_cost",
    // ... 先只填已存在的 ~20 条
}

// skippedJavaGetters 明示不参与 fixture 比对的 key，理由见 spec §4.4。
var skippedJavaGetters = map[string]string{
    // v1 shielded 池：fixture 没有，go-tron 有遗留 key。
    // 等 shielded v1 正式废弃或 java-tron 重新暴露时再入比对。
    "<none yet>": "",
}

func javaGetterToGoKey(javaGetter string) string { return javaGetterToGoKeyMap[javaGetter] }
```

- [ ] **Step 2**：写 `dp_key_mapping_test.go`：断言无重复 go-key、无空映射（空 = TODO，留错误日志）。

- [ ] **Step 3**：写 `dynamic_properties_fixture_test.go` 的 `TestDynamicProperties_MatchMainnetFixture`：
  - 从 `fixture.Load` 拿 76 条。
  - 对每条查映射表；missing / mismatch 计入错误。
  - 在测试末尾 `t.Logf` 输出汇总（N missing, M mismatches）便于迭代。

- [ ] **Step 4**：`go test ./core/state -run TestDynamicProperties_MatchMainnetFixture -v 2>&1 | tail -20` 确认测试可跑（预期大量 Fail）。

**Acceptance:**
- 测试运行，输出预期数量级的错误（>40），证明框架就绪。
- 无编译错误。

---

## Task 2：Rename 6 处命名错位

修 spec §4.3 的 6 处。每处都先 `grep` 定位所有引用，再批量 Edit。

**Files to touch:** `core/state/dynamic_properties.go` 及跨 actuator/forks/proposal/test 的所有使用方。

**Rename 表**：

| 旧 key (go) | 新 key | 对应 java getter |
|---|---|---|
| `account_permission_update_fee` | `update_account_permission_fee` | `getUpdateAccountPermissionFee` |
| `allow_adaptive_energy_limit` | `allow_adaptive_energy` | `getAllowAdaptiveEnergy` |
| `allow_change_delegation` | `change_delegation` | `getChangeDelegation` |
| `allow_tvm_compatibility` | `allow_tvm_compatible_evm` | `getAllowTvmCompatibleEvm` |

外加 2 处"看起来像 rename 但需要核实"的：

- `allow_account_history`：git log 查源头。若无 java 对应，删除；否则列出正确 getter。
- `exchange_balance_limit`：java 无对应；删除（含 getter、初始化、测试引用）。

**Steps:**

- [ ] **Step 1**：`grep -rn "allow_adaptive_energy_limit" --include='*.go'` 列全部命中点。

- [ ] **Step 2**：`Edit --replace_all` 在每个文件分别重命名。禁止跨文件一次替换（避免误伤同名字符串）。

- [ ] **Step 3**：同步把对应的 Go getter/setter 方法名从 `AllowAdaptiveEnergyLimit()` 改为 `AllowAdaptiveEnergy()`（一致性 & 语义清晰）。其他 rename 同理。

- [ ] **Step 4**：`go build ./...` + `go test ./core/state ./core/forks ./actuator -count=1` 全绿。

**Acceptance:**
- 所有 `grep` 命中点已处理。
- 编译与单测通过。
- 一致性测试的 "no go key mapped" 错误数减少 4（因为映射表现在能找到重命名后的 key）。

---

## Task 3：补齐 44 个缺失 DP key

按功能簇分 PR。每个簇先在映射表填入条目，再在 `defaultProps` 填默认值，再加 getter（setter 视需要）。默认值必须等于 fixture 中该 key 的值；如果与 `DynamicPropertiesStore.java` 常量有出入，在 PR 描述中注明并以 fixture 为准。

**簇划分**（每簇一个独立 git commit，便于 review / cherry-pick）：

### Task 3.1 自适应 & 能量限制（6 key）
`getAdaptiveResourceLimitMultiplier` / `getAdaptiveResourceLimitTargetRatio` / `getTotalEnergyAverageUsage` / `getTotalEnergyLimit` / `getTotalEnergyTargetLimit` / `getAllowAdaptiveEnergy`（若 Task 2 已 rename 则已到位）。

- [ ] 添加 key + 默认值（来自 fixture）
- [ ] 添加 getter/setter
- [ ] 一致性测试减 6 个 mismatch

### Task 3.2 动态能量（3 key）
`getDynamicEnergyIncreaseFactor` / `getDynamicEnergyMaxFactor` / `getDynamicEnergyThreshold`。

### Task 3.3 奖励 v2（2 key）
`getAllowNewReward` / `getAllowOldRewardOpt`。

### Task 3.4 TVM 标志簇（10 key）
`getAllowTvmCancun`（已存在则确认默认值）/ `getAllowTvmOsaka` / `getAllowTvmShangHai` / `getAllowTvmSolidity059` / `getAllowTvmTransferTrc10` / `getAllowTvmCompatibleEvm`（Task 2 已改名）/ `getAllowTvmSelfdestructRestriction` / `getAllowTvmBlob`（已存在则确认）/ `getAllowStrictMath` / `getAllowHigherLimitForMaxCpuTimeOfOneTx`。

### Task 3.5 市场 & 费率（5 key）
`getMarketCancelFee` / `getMarketSellFee` / `getMemoFee` / `getMaxFeeLimit` / `getMultiSignFee`。

### Task 3.6 账户 / 创建相关（6 key）
`getAllowAccountAssetOptimization` / `getAllowAccountStateRoot` / `getAllowAssetOptimization` / `getAllowOptimizeBlackHole` / `getAllowUpdateAccountName` / `getMaxCreateAccountTxSize`。

### Task 3.7 治理 / 协议开关（7 key）
`getAllowCreationOfContracts` / `getAllowProtoFilterNum` / `getAllowDelegateOptimization` / `getAllowOptimizedReturnValueOfChainId` / `getAllowCancelAllUnfreezeV2` / `getAllowTransactionFeePool` / `getAllowShieldedTRC20Transaction` / `getForbidTransferToContract` / `getConsensusLogicOptimization` / `getRemoveThePowerOfTheGr` / `getChangeDelegation`（Task 2 rename）。

### Task 3.8 Freeze V2 / 委托 / 其他（3 key）
`getMaxDelegateLockPeriod` / `getWitness127PayPerBlock` / `getFreeNetLimit`（核实默认与 fixture）。

**每簇的通用步骤：**

- [ ] 在 `core/state/dynamic_properties.go` 的 `defaultProps` 按簇插入 key=default
- [ ] 在 `javaGetterToGoKeyMap` 插入 javaGetter → goKey
- [ ] 添加 getter 方法（bool / int64 依语义）
- [ ] 按需添加 setter（仅当 actuator/proposal 流程会修改时）
- [ ] 重跑 fixture 一致性测试，错误数下降
- [ ] `go build ./... && go test ./core/state -count=1` 通过
- [ ] commit

**Acceptance（Task 3 整体）：**
- fixture 一致性测试 0 失败、0 "no mapping"（剩余仅来自 spec §4.4 明示的 skip 条目）。
- 新增 getter 全部跟着包内既有测试风格写至少一条单测（default 值断言即可）。

---

## Task 4：归并 / 删除 "额外" key

按 spec §4.4 的分类表逐条处理：

- [ ] **Step 1**：`exchange_balance_limit` —— grep 全部引用，确认无 actuator/VM 实际读取后，删除 default、getter、测试。

- [ ] **Step 2**：`allow_account_history` —— `git log -S "allow_account_history" --oneline` 找引入 commit；
  - 若是 go-tron 自己的发明：确认无生产读取路径后删除；
  - 若对应 java 某 getter：按 Task 2 rename 流程归并。

- [ ] **Step 3**：shielded 与 `next_*`、`latest_*`、`total_sign_num` 等保留项，在 `skippedJavaGetters` 文档注释里明确"为何 skip"；确保一致性测试不会因它们失败。

**Acceptance:**
- go-tron `defaultProps` key 集合 = (76 fixture key) ∪ (§4.4 保留的内部簿记项)，无多余。

---

## Task 5：重建 `core/forks/forks.go` 的 `ProposalParamKey`

**Files:**
- Modify: `core/forks/forks.go`
- Create: `core/forks/forks_proposal_mapping_test.go`

**Steps:**

- [ ] **Step 1**：把 `ProposalUtil.java` 的 `ProposalType` 枚举（spec §4.5 引用的行）翻译成 Go map：每个枚举 ID → 对应 `dp key`（依 Task 1–4 后的命名）。注释掉的历史 ID（27, 28, 34, 42, 43, 58）**不**入 map。

- [ ] **Step 2**：对照当前 `ProposalParamKey` 的错误映射，列 diff 到 PR 描述里（便于 reviewer 审计每一条改动）。

- [ ] **Step 3**：写 `TestProposalParamKey_AllActiveTypesMapped`：对 `ProposalUtil.java` 列出的每个活跃 ID，断言 `ProposalParamKey(id)` 返回非空字符串，且该字符串在 `defaultProps` 中存在。

- [ ] **Step 4**：写 `TestProposalParamKey_UnknownReturnsEmpty`：对几个枚举外的 ID（如 1000）断言返回 `""`。

**Acceptance:**
- `go test ./core/forks -count=1 -v` 全绿。
- PR 描述列出全部改动的 (id, old_key, new_key) 三列。

---

## Task 6：全仓回归 & PLAN.md 收尾

- [ ] `go test ./... -count=1 -timeout 300s` 全绿。
- [ ] `go build ./...` 全绿。
- [ ] 更新 PLAN.md 进度表：M1.1 行 "完成"、记录 commit hash。
- [ ] 更新 `TODO.md` §1.8、§1.9 相关"DP key / proposal 映射"子项勾掉（如果当时列入则打勾）。

---

## Risks

| 风险 | 应对 |
|---|---|
| rename 漏一处导致生产路径读到旧 key，静默返回 0 | Task 2 用 grep 全量 + `go build` 兜底 + 一致性测试最后验证 |
| 默认值 fixture 与 `DynamicPropertiesStore.java` 不一致（可能 fixture 来自特定 java 版本） | 以 fixture 为准，但在 PR 注释里写明 java 源行号 / 版本，便于升级 java-tron 时追溯 |
| 新增 getter 太多，PR 变大不利于审阅 | Task 3 明确按簇拆 8 个 commit，每个自洽 |
| `ProposalUtil.java` 注释掉的历史 ID 对应 key 已被某老版本链上启用，主网 state 里仍有痕迹 | 保留为注释；未来若遇历史区块回放失败再单独修 |

## Out of scope (明确不做)

- 真正的自适应能量 / 奖励 / 存储租金 / 动态能量 / Freeze V2 消费 **逻辑**（M1.4–M1.8）。
- Nile 一致性比对（等 01 Nile fixture）。
- 版本位分叉投票（M1.3）。
- 在提案应用里调用 `forks.IsActive` 门控 —— 由 M1.3 统一 audit。
