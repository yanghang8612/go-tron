# M1.3 版本位 + Fork Gate Audit — 设计说明

**日期：** 2026-04-15
**状态：** Draft
**目标：** 把 go-tron 的 fork 激活机制从"仅按 DP 标志 `allow_*` 读"升级为 java-tron 兼容的"SR 版本位 + 时间窗 + 比率门槛"三件套；同时完成一次 actuator/VM 的 fork-gate 调用点 audit，把 java-tron 每一个 `forkController.pass(...)` 调用点镜像到对应的 go-tron 路径，并处置 go-tron 独有的 3 个 TVM flag。

对应 [PLAN.md M1.3](../../../PLAN.md#m13-分叉版本位与激活门)。依赖：M1.1（DP 已对齐）；**不**依赖 M1.2。

---

## 1. 背景

> 原始数据来源：java-tron `ForkController.java:41-205`、`Parameter.ForkBlockVersionEnum`、`BlockHeader.raw.version` (proto/core/Tron.proto:503-519)、`ProposalService.java`、`ProposalUtil.java`（61 处 `forkController.pass(...)` 调用）。

### 1.1 java-tron 激活机制

- **块头版本位**：`BlockHeader.raw.version`（proto tag 10，int32）。生产者在打块时写入 `ChainConstant.BLOCK_VERSION`（当前 mainnet = 35 / `VERSION_4_8_2`）。
- **投票统计**：`ForkController.update(block)` 在每块处理后，将 `block.version` 写入 DP key `FORK_VERSION_<v>` 对应的 byte 数组，索引为该块的 witness slot（一个字节一票）；`reset()` 在维护周期切换时清表。
- **激活判定**：`pass(version)` 对 `v > V4.0` 走：
  1. `System.currentTimeMillis() >= hardForkTime(v)` 必须成立；
  2. `count(statsByVersion(v) == VERSION_UPGRADE) >= ceil(hardForkRate(v) * 27 / 100)`；
  - `hardForkTime` / `hardForkRate` 硬编码在 `ForkBlockVersionEnum`（e.g. VERSION_4_8_2: `1596780000000ms, 80%`）。
  - 对 `v <= V4.0` 的老版本走不同分支（按高度或 ENERGY_LIMIT 特判）。
- **与 proposal 的关系**：提案通过 → `ProposalService.process` 写 DP `allow_X=1`；但 **`forkController.pass(V)` 检查的是"版本 V 是否激活"**，而不是"提案 X 是否通过"。很多 actuator 同时要 `(allow_X=1) AND pass(V_GATE)`——等同"软件版本够新 + 提案通过"才放行。

### 1.2 go-tron 现状

- `core/forks/forks.go:65-75` 的 `IsActive(flag, blockNum, dp)` 只读 DP 标志，不看版本位，也不看 `hardForkTime`。
- 块头 proto 里的 `version` 字段：生产侧（`consensus/dpos` + `core/block_builder.go`）是否写入？（待 audit 中确认；若未写，属于一个"静默分叉"——生产的块与 java-tron 不一致。）
- `ProposalParamKey` 在 M1.1 已重建（74 条），但 `FORK_VERSION_*` 的 DP bucket **完全未建模**（`state.DynamicProperties` 不持这些 key，rawdb 也无对应 column）。
- actuator 中只有 11 处调 `forks.IsActive`；而 java-tron `ProposalUtil` 一处就有 61 次 `forkController.pass`，外加 `ReceiptCapsule`、`AssetIssueActuator`、TVM 内部各类判定——覆盖率远未到位。

### 1.3 TVM 独有 flag

go-tron `AllowTvmSolidity058` / `AllowTvmShieldedToken` / `AllowTvmBigInteger` 三者在 java-tron ProposalType 中**均不存在**（java 只有 `ALLOW_TVM_SOLIDITY_059` / `ALLOW_SHIELDED_TRC20_TRANSACTION` / 无 big_integer 对应）。这些 flag 若未被 VM 真正使用则属死代码；若被使用则该用法与 java-tron 行为不一致，属分叉风险。

## 2. 范围

**In scope**

1. **块头 version 字段端到端**：
   - Proto：`BlockHeader.raw.version` 在 go-tron `proto/core/Tron.proto` 已有（M0 期间整表 copy），确认字段 tag 与 int32 类型一致。
   - 生产侧：`core/block_builder.go` / `consensus/dpos/producer.go` 写入 `params.BlockVersion`（新常量，初始 = 35）。
   - 消费侧：`BlockChain.InsertBlock` 在 accept 前把 `block.GetBlockHeader().GetRawData().GetVersion()` 喂给 ForkController。
2. **ForkController 本体**：
   - 新 package / 结构 `core/forks/controller.go`，字段包含 `dp`、`hardForkTime[version]int64`、`hardForkRate[version]int`、`witnessCount int`（= 27 from DP `active_witnesses` or 常量）。
   - `Update(block)` / `Reset()` / `Pass(version int) bool`。
   - 版本位 byte 数组持久化：新增 DP keys `fork_version_<N>` (byte slice)；在 `core/state/dynamic_properties.go` 加 bytes-type 支持（目前 DP 只有 int64 + bool，需要扩一个 bytes bucket 或新建 `core/state/fork_store.go` 专门存）。
   - `Pass()` 同时兼容 `<= V4.0` 的老分支（mainnet 当前运行版本已过 V4.0，但回放老数据要用）。
3. **IsActive 重写**：
   - `IsActive(flag, blockNum, dp)` 的语义改为：先看 `allow_<flag>` DP key（软开关），再看 `Pass(requiredVersion[flag])`（版本位门）。
   - 新 `RequiredVersion` 映射：`AllowFlag → ForkBlockVersion`，数据来自 java-tron `ProposalUtil` 每个 case 的 `pass(VERSION_X)` 调用。
4. **Fork gate audit**：
   - 在 java-tron 全仓 grep `forkController.pass(`，生成 CSV（文件/行/检查的 version/关联的 proposal）。
   - 逐条在 go-tron 找到对应 actuator/VM 路径；缺的补 `forks.IsActive(...)` 或 `forks.Pass(...)` 调用。
5. **3 个 TVM flag 的处置**：
   - `AllowTvmSolidity058`：java 无对应 proposal；若 VM 当前用它区分 "pre-0.5.9 vs 0.5.9" 语义，则**重命名** flag 名保留，但把 proposal ID 映射去掉（这个 flag 只能由 "版本位 ≥ V3_6_5" 隐式触发）；若 VM 没实际用，**删除**。
   - `AllowTvmShieldedToken`：java 无此 ID；若 go-tron 的 ShieldedTRC20 precompile / opcode 用它，改成读 `allow_shielded_trc20_transaction`（proposal #39，已在 M1.1 ProposalParamKey 里）。
   - `AllowTvmBigInteger`：java 无此 ID；若 VM BIGINTEGER opcode 有依赖，找到 java-tron 对应的 `ForkBlockVersionEnum` 值替代；若没有依赖，删除。
   - Task 里会先做"是否被引用"的 grep 审查再定处置方案。
6. **单元测试**
   - `core/forks/controller_test.go`：构造 mock dp，填 fork_version byte 数组不同组合，断言 `Pass()` 在 `hardForkTime` 前后、rate 到达前后的表现。
   - `core/forks/audit_parity_test.go`：从 java-tron `ProposalUtil.java` 解析出 `(ProposalType, RequiredVersion)` 对，断言 go-tron `RequiredVersion` 表全覆盖。（解析用纯正则；非必须完美，够用即可；偏差列表 snapshot 落 golden）
   - 块生产 + 插入 e2e：2 个 witness 互相出块，几个周期后断言 `fork_version_<v>` byte 数组的填充。

**Out of scope**

- 真正新增或激活一个未激活的 fork（那属于 M2+ 升级路径）。
- PBFT / consensus 层的版本协议（M6）。
- 历史维护周期的 reset 语义（mainnet 每 6 小时 reset；本里程碑验 reset 功能到位即可，不对齐历史 reset 点）。
- Freeze V1（M1.2 并行做）。

## 3. 验收路径

**A. 单元 + 集成（必过）**

1. `controller_test.go`：10 组 table-driven 覆盖：(a) 时间未到；(b) rate 未达；(c) 两者都达；(d) 老版本路径；(e) 参数未知；(f) reset 后计数清零；(g) bytes 持久化 roundtrip。
2. `audit_parity_test.go`：对 java-tron `ProposalUtil` 的 61 调用点解析，确保 go-tron 对相同 AllowFlag 有等价判断（不要求行对行一致，但要求判断覆盖：同 flag 在 go-tron 至少有一个 callsite）。
3. `core/block_processor_version_test.go`：插入一块 version=35 → `ForkController` 状态更新；第 `rate*27` 块后 `Pass(35)` 返回 true（假设 `hardForkTime` 已过）。
4. 回归：`go test ./... -count=1` 全绿；M1.1 的 fixture 测试仍绿。

**B. 文档**

- `docs/dev/fork-gates.md`：新文件，列出每个 AllowFlag 的 (RequiredVersion, proposal ID, java-tron callsite count, go-tron callsite count)——将来加 flag 时必须更新本表。

**C. 手工（本里程碑最低要求）**

- 启动本地 gtron 单节点，打 10 块，读 `fork_version_35` DP bytes，确认首字节 = `VERSION_UPGRADE`（0x01）。
- 同一 java-tron 节点同条件对比首字节一致。

## 4. 设计要点

### 4.1 块头 version 读写

```go
// params/mainnet.go
const BlockVersion = 35 // VERSION_4_8_2; update when advancing fork

// core/block_builder.go
header.RawData.Version = params.BlockVersion

// core/blockchain.go InsertBlock
ctx.ForkController.Update(block)
```

生产端的 version 字段不是 per-fork 投票——它是"我作为 SR 告诉大家我跑的是 35 版软件"。SR 若不升级则停在老版本号，rate 永远到不了。

### 4.2 `fork_version_<v>` 的存储

byte 数组 = len(activeWitnesses) = 27。`ForkController.Update`：

```go
slot := block.WitnessSlot()              // 0..26
arr := dp.ForkStats(version)
if arr == nil { arr = make([]byte, witnessCount) }
arr[slot] = VERSION_UPGRADE               // 0x01
dp.SetForkStats(version, arr)
```

`Reset()` 在每次维护周期（同 java-tron，在 maintenance.go 切换 witness schedule 时调）清空**所有**未激活 version 的数组。已激活的保留。

为容纳 byte[]，在 `core/state/dynamic_properties.go` 引入 `bytesKey` bucket（与现有 int64/bool bucket 并列；持久化用 rawdb 的 `DPPrefix + key`，value 原样）。

### 4.3 IsActive 双层

```go
func IsActive(flag AllowFlag, dp DP, fc *ForkController) bool {
    if dp.Get(dynKey[flag]) == 0 { return false }     // 软开关
    req, ok := requiredVersion[flag]
    if !ok { return true }                             // 无版本门：老 flag
    return fc.Pass(req)
}
```

这样 proposal 把 `allow_*=1` 写入后，还需 SR 版本位到位才真正启用，与 java-tron `pass()` 语义一致。

### 4.4 audit 生成脚本

写个一次性 `scripts/dev/fork_audit.sh`（bash + sed）：

- 对 `/Users/asuka/Projects/tron/java-tron` 下 `*.java` grep `forkController\.pass\(\s*[^)]+\)` 产生 `(file, line, version)` 三元组。
- 对 go-tron `actuator/**/*.go` + `vm/**/*.go` grep `forks\.IsActive`、`forks\.Pass`，对照。
- 输出 `docs/dev/fork-audit-2026-04-15.md`（snapshot）。

这个脚本不是 CI 的一部分；只在本里程碑执行一次，结果冻结为文档。

### 4.5 三个独有 flag 处置决策树

```
for flag in [Solidity058, ShieldedToken, BigInteger]:
  ref_count = grep "flag" under vm/**/*.go actuator/**/*.go core/**/*.go
  if ref_count == 0:
    delete from AllowFlag enum + dynKey + remove DP key
  else:
    inspect each reference:
      if semantics == java-tron's Solidity059/ShieldedTrc20/<nothing>:
        rename to the java-tron AllowFlag and repoint
      else:
        keep but mark in docs/dev/fork-gates.md with "go-tron specific; no java equivalent" + freeze (cannot be toggled by proposal)
```

## 5. Task 大纲（详见同日 plan）

1. Proto + params + BlockBuilder：块头 version 读写闭环。
2. DP bytes bucket 支持 + `fork_version_<v>` key 持久化。
3. `ForkController` 实装（Update / Reset / Pass / Stats）+ 单测。
4. `IsActive` 改写 + `requiredVersion` 表（从 java-tron ProposalUtil 提取）。
5. Fork gate audit 脚本 + `docs/dev/fork-audit-2026-04-15.md` + 补齐缺失 callsite。
6. 三个独有 flag 处置（决策 + 代码 + 测试更新）。
7. `audit_parity_test.go` + `docs/dev/fork-gates.md`。
8. PLAN.md + CLAUDE.md 指针更新。

## 6. 风险

| 风险 | 缓解 |
|---|---|
| 改块头 version 写入会让既有 gtron 节点产的块与旧 gtron peer 不兼容 | PR-1：只写不校；PR-2：校验开关默认 off；双节点互测过后再打开 |
| `hardForkTime` 参数在 `ForkBlockVersionEnum` 多条不同——硬编到 go-tron 后若 java-tron 改动我们漏改 | 参数表放一个单独的 `core/forks/versions.go` 文件 + 注释指向 `Parameter.java`；每次升级必改这一个文件 |
| `fork_version_<v>` byte 数组长度随 `active_witnesses` 变化；重组时内存/ rawdb 数据不一致 | Reset() 在每个 maintenance 周期显式重建；DP 存储带长度前缀（用 proto `bytes` 类型） |
| audit 补齐大量 actuator 判断 → 大 diff → 回归面广 | Task 5 内部按 actuator 分文件提交，每提交一个跑全量 test；任一测试红就停下对原 java-tron 再核对 |
| 三个独有 flag 真被 VM 用到，且语义无 java 对应 | docs/dev/fork-gates.md 单独 section "go-tron specific"——暴露给未来 M7 TVM 对齐 milestone 处理，不在 M1.3 强行解决 |
| Witness count 非 27（比如 Nile 或 devnet） | `witnessCount` 从 DP `active_witnesses` 读，不写死；测试覆盖 2-witness devnet 场景 |
