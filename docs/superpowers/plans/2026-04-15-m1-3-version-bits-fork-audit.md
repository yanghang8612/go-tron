# M1.3 版本位 + Fork Gate Audit — 实施计划

**日期：** 2026-04-15
**对应 spec：** [2026-04-15-m1-3-version-bits-fork-audit-design.md](../specs/2026-04-15-m1-3-version-bits-fork-audit-design.md)
**依赖：** M1.1 完成（DP 已对齐）；**M1.2 先合并**（Task 5 audit 会修 freeze actuator）。

---

## 执行原则

1. 每 Task 独立 commit：`feat(forks): M1.3 Task N — …`。
2. 本里程碑 diff 较大——必须逐 Task 全量 `go test ./... -count=1` 绿。
3. 第 5 Task（audit 补齐）内部再细分，每个 actuator 子 commit；出问题回溯粒度小。
4. Task 1-4 可单线推进；Task 5-6 可以并行拆两条线。

---

## Task 1 · 块头 version 读写闭环

**动作**
- 确认 `proto/core/Tron.proto` 的 `BlockHeader.raw.version` 已与 java-tron 一致（tag 10, int32）。
- `params/mainnet.go`：`const BlockVersion = 35  // VERSION_4_8_2`；注释注明"更新此值 = 声明 SR 软件升级到新 fork 版本"。
- `params/nile.go`：同上，值可暂同 35。
- `core/block_builder.go` / `consensus/dpos/producer.go`：打块写 `block.GetBlockHeader().GetRawData().Version = params.BlockVersion`。
- `core/blockchain.go` InsertBlock：在 state commit 后调 `ctx.ForkController.Update(block)`（Controller 未实装前先 stub）。

**验收**：本地双 witness 出块十余块，脚本 dump 最新块 header 的 version=35；`go test ./consensus/... ./core/...` 绿。

---

## Task 2 · DynamicProperties bytes bucket + fork_version 持久化

**动作**
- `core/state/dynamic_properties.go` 扩 bytes 类型：新加 `bytesDefaults map[string][]byte`、`dp.GetBytes(key) []byte` / `SetBytes(key, val)`；rawdb 以现有 DP prefix 存储（value 带 proto `bytes` 编码或直接 raw bytes + length prefix）。
- 新增 key 形态 `fork_version_<N>`（动态生成，不在 defaults 表；访问时若 nil 返回空 slice）。
- `core/state/statedb.go`：pass-through `ForkStats(v int) []byte` / `SetForkStats(v int, b []byte)`。

**验收**：`core/state/dynamic_properties_bytes_test.go` 覆盖 set/get/持久化 roundtrip；M1.1 既有 76-key fixture 测试不受影响。

---

## Task 3 · ForkController 实装

**动作**
- 新文件 `core/forks/controller.go`：
  - `type ForkController struct { dp DP; witnessCount int; versions map[int]VersionParams }`
  - `type VersionParams struct { HardForkTime int64; HardForkRate int }`
  - `Update(block *types.Block)`：写对应 slot 的 0x01。
  - `Reset()`：对每个未激活 version 把 bytes 清成全 0x00。
  - `Pass(version int) bool`：按 spec §4.1 老/新两分支。
  - `RequiredVersion(flag AllowFlag) (int, bool)`：查表。
- 新文件 `core/forks/versions.go`：`ForkBlockVersionEnum` 的 Go 镜像（硬编 + 注释链接到 `Parameter.java` 行号）。
- `consensus/dpos/maintenance.go`：切 witness schedule 时调 `ForkController.Reset()`。

**验收**：`core/forks/controller_test.go` spec §3.A 的 10 组 table test 全绿；集成 e2e `block_processor_version_test.go`。

---

## Task 4 · IsActive 双层 + RequiredVersion 表

**动作**
- `core/forks/forks.go`：重写 `IsActive`：读 DP 软开关 → 读 `RequiredVersion`（若有则要求 `Pass`）。
- 新 `core/forks/required_versions.go`：从 java-tron `ProposalUtil.java` 逐 case 提取 `ProposalType → ForkBlockVersionConsts` 映射（手写，spec 预期 ~40 条）。
- 所有已有 `forks.IsActive` 调用点改签名（若新签名增加 `fc *ForkController` 参数，需要在调用上下文注入，或把 ForkController 挂到 package-level）。**推荐**：`forks.IsActive` 签名不变，ForkController 注入为 `core/state.DynamicProperties` 里的一个依赖——即 `IsActive` 内部 `dp.ForkController()`。

**验收**：
- 所有 actuator / VM 编译通过。
- `go test ./core/forks/... -count=1` 绿（含新建的 required_versions 覆盖测试）。
- `TestDynamicProperties_MatchMainnetFixture` 仍绿。

---

## Task 5 · Fork Gate Audit 脚本 + 补齐调用点（仅执行路径）

**动作**
- `scripts/dev/fork_audit.sh`：对 java-tron grep `forkController\.pass\(\s*[^)]+\)`，对 go-tron grep `forks\.(IsActive|Pass)`，输出 CSV 到 `docs/dev/fork-audit-2026-04-15.md`。
- **分类每一行为 (a) 执行路径门 或 (b) 提案合法性门**。(b) 归入 "Deferred to M4" section，本里程碑不补。
- 从 (a) CSV 逐条核对，把缺失调用点按 actuator 分子 commit 补齐。预计至少以下子集：
  - `asset_issue.go`（pass(V_4_8_1)）
  - `update_asset.go`
  - `exchange_create.go` / `exchange_inject.go` / `exchange_withdraw.go` / `exchange_transaction.go`
  - `account_create.go`（pass(ENERGY_LIMIT)）
  - `transfer.go` / `transfer_asset.go`（pass(V_3_6_5) 影响费率）
  - `shielded_transfer.go`
  - `vote_witness.go`
- `core/receipt.go`（或等价文件）的 pass(V_3_6_5) 能量 receipt 逻辑。
- VM 层：`vm/interpreter.go`、`vm/precompile_tron.go`，按 audit 指引补。

**验收**：
- `docs/dev/fork-audit-2026-04-15.md` 列出的 61 个 java callsite 在 go-tron 有对应（或说明不需要——比如 "该路径 go-tron 无此 actuator"）。
- 全仓 test 绿。

---

## Task 6 · 三个独有 flag 处置

**前置事实**（已在 spec 写稿期 grep 确认，无需再 Step A）：
- `AllowTvmShieldedToken` 在 `vm/precompiles.go:65` + `vm/tvm_config.go:38` 使用。
- `AllowTvmBigInteger` 在 `vm/tvm_config.go:44` 使用。
- `AllowTvmSolidity058` 仅在 `vm/tvm_config.go` 加载（interpreter 实际是否用到待查）+ `forks_test.go` / `dynamic_properties_fork_test.go` 测试。

**动作**
- **ShieldedToken → Rename**：把 AllowFlag 改名 `AllowShieldedTrc20Transaction`，DP key 改 `allow_shielded_trc20_transaction`；`vm/tvm_config.go` 的 struct 字段沿用 `ShieldedToken`（VM 内部命名延续），但 `isActive(...)` 调用点改成新 flag。`proposalParamKey[39]` 已映射此 key（M1.1 已就位），无需新增。
- **BigInteger → 保留，登记**：keep as-is；`docs/dev/fork-gates.md` "go-tron specific" 段登记；`requiredVersion` 表里留空或指向一个合理的版本门（查 java-tron 的 BIGINT 精度处理是否与某 fork 版本绑定；若无，置 no-version-gate）。
- **Solidity058 → 深查 VM**：在 `vm/` 全文搜 `Solidity058` / `cfg.Solidity058`——
  - 若 interpreter 真用：登记为 go-tron private，保留。
  - 若仅 tvm_config 读不用：标 "dead, pending M7 cleanup"，但本里程碑不删（避免误伤）。
- **测试同步**：`core/forks/forks_test.go` + `core/state/dynamic_properties_fork_test.go` 适配 rename。
- **Fixture 一致性测试**：`TestDynamicProperties_MatchMainnetFixture` 若涉及这些 key，需加 skip 列表（java-tron `getchainparameters` 不返回它们）——M1.1 已处理，但 rename 后 key 名变了要同步。

**验收**：
- `go test ./... -count=1` 绿。
- `docs/dev/fork-gates.md` 在"go-tron specific"段显式列出任何保留项，或在"Deleted (2026-04-15)"段列出删除项。

---

## Task 7 · `audit_parity_test.go` + `docs/dev/fork-gates.md`

**动作**
- `core/forks/audit_parity_test.go`：把 java-tron ProposalUtil 里用正则解析出的 `(ProposalType, ForkBlockVersion)` 对（从 Task 5 的 audit 数据衍生；采用 snapshot 文件 `testdata/java_fork_callsites.json`）断言 go-tron `requiredVersion` 表覆盖所有 flag。
- `docs/dev/fork-gates.md`：
  - 列表：AllowFlag | DP key | Proposal ID | Required version | go-tron usage count | java-tron usage count | Notes。
  - Section "Retired" / "go-tron specific"。

**验收**：测试绿 + 文档 readable。

---

## Task 8 · PLAN.md + CLAUDE.md

**动作**
- PLAN.md：M1.3 行标完成 + 日期 + 备注（"版本位闭环 + audit 完成；3 个独有 flag 处置决策写入 docs/dev/fork-gates.md"）。
- CLAUDE.md：`Architecture / Params & forks` 段加"SR 版本位在块头 tag 10；ForkController 见 core/forks/controller.go"。

**验收**：`git log --oneline` 有完整 Task 1-8 commit；`go test ./...` 绿；`make lint` 无新增 warning。

---

## 合并入口与退出

- **入口**：M1.1 完成（已）；M1.2 若已开始也不会冲突（本 plan 不改 core/bandwidth.go / core/resource.go / actuator/freeze_balance.go——除非 Task 5 audit 指向它们）。
- **退出**：Task 1-8 全绿；双 witness 本地 e2e dump 块头 version=35 且几个周期后 `fork_version_35` byte 数组多个 slot 填 0x01。

---

## 风险对应（摘自 spec §6）

| 风险 | 触发 Task |
|---|---|
| 块头 version 写入破坏与旧 gtron peer 兼容 | Task 1 后端到端 smoke（双 gtron 节点） |
| hardForkTime 参数漂移 | Task 3 `core/forks/versions.go` 集中，后续维护点唯一 |
| audit 大 diff 回归面广 | Task 5 按 actuator 子 commit |
| 独有 flag 语义无 java 对应 | Task 6 docs 公开，不强修 |
| witness count != 27 | Task 3 `witnessCount` 从 DP 读，Test 包含 2-witness |
