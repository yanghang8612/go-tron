# Fixture 抽取工具 — 设计说明

**日期：** 2026-04-15
**状态：** Draft
**目标：** 构建最小化工具，从本地 java-tron 节点按脚本化场景抽取 post-state 快照（JSON），作为 go-tron M1 子里程碑的单测 oracle。

本文档对应 [PLAN.md M0′](../../../PLAN.md#m0-fixture-抽取工具--轻前置)。

---

## 1. 背景

go-tron 当前状态层离 java-tron 有大量未对齐特性（见 [TODO.md §1](../../../TODO.md#1-consensus-state--fork-machinery--p0)）。直接对主网做完整回放只会在第 1 个块就分叉，无信号。

更合理的做法是：每个 M1 子里程碑实现某个特性时，用 java-tron 对同一场景跑出的 state 快照作为 golden 对照，做细粒度单测。本工具提供这层抽取能力，不解决回放。

## 2. 范围

**In scope**
- 从本地 java-tron 节点跑脚本化场景、抓取 post-state、落成 JSON fixture。
- 覆盖三类 state：`DynamicProperties`、`Account`、`TransactionReceipt`。
- go-tron 侧的 fixture 载入/断言帮助函数。

**Out of scope**
- 完整 state trie / 区块回放一致性（M0″）。
- 活 mainnet 数据抓取（M0″）。
- 跨节点端到端双向同步测试（M0″ 的 `system_test_cross.sh`）。
- 性能基准。

## 3. 架构

### 3.1 抽取流程

```
清空数据目录
 → 以场景指定的 config 初始化 java-tron
 → 启动节点，等待就绪
 → （可选）通过 wallet API 广播场景脚本化的交易
 → 等待目标块高确认
 → 调用 wallet/* HTTP 端点 dump state
 → 归一化 + 合并为 fixture.json
 → 关停并清理 java-tron
```

全部通过 bash + curl 实现，不引入新 Go 依赖。复用既有 `docs/dev/java-tron-local.md` 的本地节点启动方式。

### 3.2 目录结构

```
scripts/fixtures/
├── run.sh                           # 入口：./run.sh <scenario> | ./run.sh all
├── lib/
│   ├── java-tron-ctl.sh             # start/stop/init 本地 java-tron
│   ├── api.sh                       # curl 封装 wallet/* API
│   └── dump.sh                      # state → JSON 合成
└── scenarios/
    ├── 00-genesis-dp-mainnet/
    │   ├── config.conf              # java-tron 启动 config
    │   ├── setup.sh                 # 构造场景前置（空亦可）
    │   ├── run.sh                   # 场景执行（本场景仅等待 block 0）
    │   ├── dump.sh                  # 调 lib/dump.sh 的具体 dump
    │   └── README.md                # 场景说明
    └── 01-genesis-dp-nile/
        └── ...

test/fixtures/
├── 00-genesis-dp-mainnet/
│   └── fixture.json                 # golden 数据（入库）
└── 01-genesis-dp-nile/
    └── fixture.json

internal/testutil/fixture/
├── fixture.go                       # Load() 帮助函数 + 断言
└── fixture_test.go                  # 自身单测

docs/dev/
└── fixture-tooling.md               # 使用说明
```

设计要点：
- **场景自带 config**：每个 `scenarios/<name>/config.conf` 自描述，避免依赖全局环境。
- **fixture 分离**：extraction 脚本在 `scripts/fixtures/`（脚本），输出数据在 `test/fixtures/`（数据），go 侧 loader 在 `internal/testutil/fixture/`（代码），三者解耦。
- **schema 稳定**：fixture.json 顶层 `schema` 字段做版本管理，后续只增字段不变语义。

### 3.3 Fixture JSON 格式（schema v1）

```json
{
  "schema": 1,
  "scenario": "00-genesis-dp-mainnet",
  "javaTron": {
    "version": "4.7.5",
    "gitCommit": "abc1234",
    "configSha256": "deadbeef..."
  },
  "extractedAt": "2026-04-15T12:00:00Z",
  "blockNum": 0,
  "blockHash": "0x...",
  "dynamicProperties": {
    "MAINTENANCE_TIME_INTERVAL": 21600000,
    "ACCOUNT_UPGRADE_COST": 9999000000,
    "CREATE_ACCOUNT_FEE": 100000,
    "...": "..."
  },
  "accounts": {
    "TLLM21wteSPs4hKjbxgmH1L6poyMjeTbHm": {
      "balance": 99000000000000000,
      "type": "AssetIssue",
      "...": "..."
    }
  },
  "receipts": {}
}
```

规则：
- 任何一个 section（`accounts`/`receipts`/`dynamicProperties`）都可为 `null` 或省略，表示该场景不涉及。
- 数值字段保留 java-tron 原始 int64 精度，不转字符串。
- 地址统一 Base58 编码（与 java-tron HTTP 返回一致）。
- `javaTron.configSha256` 锁定本场景使用的 config 文件哈希，任何 config 改动都必须重跑并重提 fixture。

### 3.4 Go 侧 loader

```go
package fixture

type Fixture struct {
    Schema            int
    Scenario          string
    JavaTron          JavaTronVersion
    BlockNum          uint64
    BlockHash         string
    DynamicProperties map[string]int64
    Accounts          map[string]*Account
    Receipts          map[string]*Receipt
}

// Load 读取 test/fixtures/<name>/fixture.json。若 schema 不匹配或文件缺失，t.Fatal。
func Load(t *testing.T, name string) *Fixture
```

调用方在 M1 的测试里：

```go
func TestDynamicProperties_MainnetDefaults(t *testing.T) {
    fix := fixture.Load(t, "00-genesis-dp-mainnet")
    dp := state.NewDynamicProperties(...)  // 按 params/mainnet.go 初始化
    for key, want := range fix.DynamicProperties {
        got := dp.Get(key)
        require.Equal(t, want, got, "dp key %s mismatch", key)
    }
}
```

## 4. 种子场景

两个，仅此足以支撑 M1.1 启动：

| 场景 | config | 交易 | dump section | 用途 |
|---|---|---|---|---|
| `00-genesis-dp-mainnet` | mainnet 默认 | 无 | `dynamicProperties` | M1.1 断言 `params/mainnet.go` 初始 DP |
| `01-genesis-dp-nile` | nile 默认 | 无 | `dynamicProperties` | M1.1 断言 `params/nile.go` 初始 DP |

后续 M1 子里程碑按需新增场景：
- M1.2 Freeze V1: `freeze-v1-then-unfreeze`、`freeze-v1-delegate` 等。
- M1.4 自适应能量：`adaptive-energy-high-load` 跑 maintenance 边界。
- M1.5 奖励 v2: `one-full-cycle-reward`。

每个场景 PR 单独走 review，不纳入本计划范围。

## 5. 依赖 & 假设

- 开发机已按 `docs/dev/java-tron-local.md` 可本地跑 java-tron（JDK、jar、端口）。
- fixture 由开发者本地生成后手工提交；**不**在 CI 跑抽取（java-tron jar 重，CI 上跑不实际）。CI 只跑 go 侧的 `internal/testutil/fixture` 与消费它们的测试。
- fixture 视为对 java-tron 特定版本的"冻结观测值"。java-tron 升级时需重跑全部场景并在 commit message 中说明版本跳变。

## 6. 验收标准

- `./scripts/fixtures/run.sh 00-genesis-dp-mainnet` 能从零跑到 `test/fixtures/00-genesis-dp-mainnet/fixture.json` 落盘。
- `go test ./internal/testutil/fixture/...` 通过（包含一个针对预置 fixture 的 round-trip 载入测试）。
- `docs/dev/fixture-tooling.md` 能让没做过的同事独立跑出两个场景的 fixture。
- schema v1 下，第三个被加入的场景无需改 loader 代码。

## 7. 风险

| 风险 | 缓解 |
|---|---|
| java-tron HTTP API 在不同版本返回字段变化 | fixture.json 携带 `javaTron.version`；loader 只读声明过的字段，未知字段忽略 |
| 本地节点启停不稳定导致 fixture 不可复现 | `java-tron-ctl.sh` 用固定 datadir + 每次先清空；退出前必 kill |
| JSON 数值精度丢失（JS 风险） | Go 侧用 `json.Number` 解析成 int64；禁止 float |

## 8. 未决问题

- 是否把 fixture 产出自动化为一个 CI "每周跑"的 job？—— 暂不，先走手工；如果 M1 进度快，再做。
- fixture 体积变大后是否需要压缩存储？—— v1 不处理；超过 100KB 的单 fixture 再议。
