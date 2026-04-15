# M0′ Fixture 抽取工具 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 [fixture-extraction-design](../specs/2026-04-15-fixture-extraction-design.md) spec 中描述的 fixture 抽取工具与两个种子场景（`00-genesis-dp-mainnet`、`01-genesis-dp-nile`），为 M1 各子里程碑提供 golden 数据来源。

**Architecture:** bash + curl 驱动本地 java-tron 节点启停和 API 调用；产出 JSON fixture 入库；Go 侧 `internal/testutil/fixture` 提供 loader。设计全文见 spec。

**Tech Stack:** bash 4+、curl、jq、既有 go-tron Go 1.25 工具链。无新第三方依赖。

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `scripts/fixtures/run.sh` | 入口：调度到具体 scenario |
| Create | `scripts/fixtures/lib/java-tron-ctl.sh` | start/stop/init 本地 java-tron |
| Create | `scripts/fixtures/lib/api.sh` | curl 封装 wallet/* API |
| Create | `scripts/fixtures/lib/dump.sh` | wallet API 输出 → fixture JSON 合成 |
| Create | `scripts/fixtures/scenarios/00-genesis-dp-mainnet/config.conf` | 场景 0 的 java-tron config |
| Create | `scripts/fixtures/scenarios/00-genesis-dp-mainnet/setup.sh` | 前置（空） |
| Create | `scripts/fixtures/scenarios/00-genesis-dp-mainnet/run.sh` | 等待 block 0 |
| Create | `scripts/fixtures/scenarios/00-genesis-dp-mainnet/dump.sh` | 调 lib/dump.sh 抓 DP |
| Create | `scripts/fixtures/scenarios/00-genesis-dp-mainnet/README.md` | 场景说明 |
| Create | `scripts/fixtures/scenarios/01-genesis-dp-nile/` | 同上（nile config） |
| Create | `test/fixtures/00-genesis-dp-mainnet/fixture.json` | 入库 golden 数据 |
| Create | `test/fixtures/01-genesis-dp-nile/fixture.json` | 入库 golden 数据 |
| Create | `internal/testutil/fixture/fixture.go` | `Load(t, name) *Fixture` + 类型 |
| Create | `internal/testutil/fixture/fixture_test.go` | round-trip 载入测试 |
| Create | `docs/dev/fixture-tooling.md` | 使用文档 |
| Modify | `.gitignore` | 忽略 `/tmp/fixture-tron-*` 本地工作目录 |
| Modify | `Makefile` | 增加 `fixtures` target（可选） |

---

## Task 1：Go 侧 fixture loader 骨架

为什么先做 loader：schema 是整个工具链的契约。先定类型，再让 shell 端按这个契约产出，避免来回改。

**Files:**
- Create: `internal/testutil/fixture/fixture.go`
- Create: `internal/testutil/fixture/fixture_test.go`
- Create: `test/fixtures/.gitkeep`（占位，scenarios 落盘前目录存在）

**Steps:**

- [ ] **Step 1**：在 `internal/testutil/fixture/fixture.go` 定义类型与 `Load()`

按 spec §3.3 的 schema：

```go
package fixture

type Fixture struct {
    Schema            int               `json:"schema"`
    Scenario          string            `json:"scenario"`
    JavaTron          JavaTronVersion   `json:"javaTron"`
    ExtractedAt       string            `json:"extractedAt"`
    BlockNum          uint64            `json:"blockNum"`
    BlockHash         string            `json:"blockHash,omitempty"`
    DynamicProperties map[string]int64  `json:"dynamicProperties,omitempty"`
    Accounts          map[string]*Account `json:"accounts,omitempty"`
    Receipts          map[string]*Receipt `json:"receipts,omitempty"`
}

type JavaTronVersion struct {
    Version      string `json:"version"`
    GitCommit    string `json:"gitCommit"`
    ConfigSha256 string `json:"configSha256"`
}

type Account struct {
    Balance   int64  `json:"balance"`
    Type      string `json:"type"`
    // 后续按需扩展
}

type Receipt struct {
    // 后续按需扩展
}

const SchemaVersion = 1

// Load 从 test/fixtures/<name>/fixture.json 读取并校验。
// 工作目录不固定时，按 repo root 的 test/fixtures 解析（基于 runtime.Caller 定位）。
func Load(t testing.TB, name string) *Fixture
```

实现要点：
- 用 `json.Decoder` + `UseNumber()`，再手工把 `json.Number` 转 `int64`，防止精度丢失。
- `SchemaVersion` 不匹配则 `t.Fatalf`。
- 文件缺失或场景名非法 → `t.Fatalf`。

- [ ] **Step 2**：给 `fixture_test.go` 写一个"假 fixture"的 round-trip 测试

在测试文件里临时写一个 fixture JSON 到 `t.TempDir()`，然后 `Load()` 回来，断言关键字段一致。

注意：此刻 `test/fixtures/` 下尚无真实 fixture，因此**不要**依赖 `Load()` 的默认路径，改用显式路径函数；另外提供一个包内 `loadFrom(path string)` 便于测试。

- [ ] **Step 3**：`go test ./internal/testutil/fixture/...` 通过

**Acceptance:**
- `Fixture` 结构体字段与 spec §3.3 一致。
- round-trip 测试通过，且 int64 字段保留精度（测试中用 `math.MaxInt64` 或足够大的值验证）。

---

## Task 2：java-tron 控制脚本 (`lib/java-tron-ctl.sh`)

**Files:**
- Create: `scripts/fixtures/lib/java-tron-ctl.sh`

**Steps:**

- [ ] **Step 1**：实现 5 个 shell 函数

```bash
# 对外函数
jt_init <workdir> <config_path>   # 清空 workdir，复制 config，准备 datadir
jt_start <workdir> <config_path>  # 后台启动 FullNode.jar，重定向日志到 workdir/java-tron.log
jt_wait_ready <http_port>         # 轮询 wallet/getnowblock 直到 200 响应或超时 60s
jt_stop                            # kill 已启动的 java-tron，等 pid 退出
jt_cleanup <workdir>              # 全清，含 /tmp 下的 index/db
```

硬约束：
- `FULLNODE_JAR` 环境变量或默认值 `/Users/asuka/Projects/tron/java-tron/build/libs/FullNode.jar`（与 `docs/dev/java-tron-local.md` 一致）。
- workdir 固定模式：`/tmp/fixture-tron-$$`（带 pid 避免并发冲突）。
- `jt_stop` 必须幂等且对未启动态安全。
- trap：调用方 `set -euo pipefail; trap jt_stop EXIT` 就能保证异常退出不留僵尸。

- [ ] **Step 2**：`scripts/fixtures/lib/java-tron-ctl.sh` 顶部加 `# shellcheck shell=bash` 并保证 `bash -n` 通过

- [ ] **Step 3**：写一个最小烟雾测试 `scripts/fixtures/lib/java-tron-ctl.test.sh`（可选但推荐），能启停一次

**Acceptance:**
- 手动 `source lib/java-tron-ctl.sh && jt_init /tmp/test1 path/to/config && jt_start ... && jt_wait_ready 8090 && jt_stop && jt_cleanup /tmp/test1` 全链路跑通。

---

## Task 3：Wallet API 封装 (`lib/api.sh`)

**Files:**
- Create: `scripts/fixtures/lib/api.sh`

**Steps:**

- [ ] **Step 1**：封装 4 个 curl 调用

```bash
api_get_now_block <http_port>              → 打印 JSON
api_get_block_by_num <http_port> <num>     → 打印 JSON
api_get_chain_parameters <http_port>       → 打印 JSON
api_get_account <http_port> <base58_addr>  → 打印 JSON
```

统一行为：
- 失败（非 2xx 或 JSON 包含 `"Error"`）→ stderr 打印、`return 1`。
- 超时 5s。
- 不加任何解析，原样透传 JSON，由 `dump.sh` 负责归一化。

- [ ] **Step 2**：`bash -n lib/api.sh` 通过

**Acceptance:**
- 对着一个手工启动的 java-tron，`api_get_chain_parameters 8090 | jq '.chainParameter | length'` 输出 ≥ 60。

---

## Task 4：Fixture 合成 (`lib/dump.sh`)

**Files:**
- Create: `scripts/fixtures/lib/dump.sh`

**Steps:**

- [ ] **Step 1**：实现核心合成函数

```bash
dump_fixture <output_path> <scenario_name> <config_path> <http_port> \
             [--section dp] [--section accounts <addr>,<addr>,...] \
             [--section receipts <txid>,...]
```

行为：
- 从 java-tron 拉取对应 section 数据。
- 拉取本地 java-tron 版本：执行 `java -jar $FULLNODE_JAR --version 2>&1 | head -1`。git commit 未知时填空字符串；`configSha256` = `sha256sum <config_path> | awk '{print $1}'`。
- 用 `jq` 合成最终 JSON，按 schema v1 顶层字段顺序固定。
- `extractedAt` 用 `date -u +%Y-%m-%dT%H:%M:%SZ`。
- 写文件前创建父目录；写完后 `jq empty` 校验产出合法。

- [ ] **Step 2**：DP section 的字段归一化

java-tron `/wallet/getchainparameters` 返回形如 `{"chainParameter": [{"key": "MAINTENANCE_TIME_INTERVAL", "value": 21600000}, ...]}`。归一化成 `{"MAINTENANCE_TIME_INTERVAL": 21600000, ...}`。没有 value 字段的条目（默认 0 的布尔）补 `0`。

- [ ] **Step 3**：Account section 空跑桩

本里程碑两个种子场景不需要 accounts，但预留接口，实现为：传入空列表时输出 `null`；非空列表实现推迟到使用方（M1.2）。在 dump.sh 里加 `TODO(M1.2)` 注释。

**Acceptance:**
- 手动 `dump_fixture /tmp/fix.json sanity /tmp/sanity.conf 8090 --section dp` 产出合法 JSON，jq 解析无错，DP map 非空。

---

## Task 5：种子场景 00 (`00-genesis-dp-mainnet`)

**Files:**
- Create: `scripts/fixtures/scenarios/00-genesis-dp-mainnet/config.conf`
- Create: `scripts/fixtures/scenarios/00-genesis-dp-mainnet/setup.sh`
- Create: `scripts/fixtures/scenarios/00-genesis-dp-mainnet/run.sh`
- Create: `scripts/fixtures/scenarios/00-genesis-dp-mainnet/dump.sh`
- Create: `scripts/fixtures/scenarios/00-genesis-dp-mainnet/README.md`
- Create: `test/fixtures/00-genesis-dp-mainnet/fixture.json`（抽取产物，入库）

**Steps:**

- [ ] **Step 1**：编写 `config.conf`

基于 java-tron `framework/src/main/resources/config.conf` 的 mainnet 默认值，裁剪成本地单节点私链（不接任何 seed、`needSyncCheck=false`）。关键字段：
- `net.type = mainnet`
- `node.listen.port = 未使用的端口`（避免与 HTTP 冲突，用 19888）
- HTTP API 监听 18090
- `seed.node.ip.list = []`
- **不**改动任何 chain parameter 默认值

文件末尾 `# sha256: <填入>` 作为注释便于追溯（实际 sha 由 dump.sh 计算）。

- [ ] **Step 2**：编写 `setup.sh`

本场景无前置，仅 `exit 0`。

- [ ] **Step 3**：编写 `run.sh`

仅等待链高 ≥ 0（也就是 genesis）。java-tron 启动后 block 0 立即可读，所以实际就是 `api_get_now_block` 成功。

- [ ] **Step 4**：编写 `dump.sh`

调用 `dump_fixture <fixture_path> 00-genesis-dp-mainnet <config> 18090 --section dp`。

- [ ] **Step 5**：编写 `README.md`

说明：本场景用途（M1.1 golden）、如何重跑、为何不含 accounts/receipts。

- [ ] **Step 6**：跑一次，`test/fixtures/00-genesis-dp-mainnet/fixture.json` 生成入库

产出必须：schema=1，dynamicProperties 字段数 ≥ 60，javaTron.configSha256 非空。

**Acceptance:**
- `./scripts/fixtures/run.sh 00-genesis-dp-mainnet` 从零到落盘一键跑通。
- 生成的 fixture 能被 Task 1 的 `fixture.Load` 读取不报错。

---

## Task 6：种子场景 01 (`01-genesis-dp-nile`) — **DEFERRED**

**状态**：暂缓。`tronprotocol/tron-deployment` 公开仓库当前只提供 `main_net`、`test_net`、`private_net` 三个 config，**没有** Nile 专用 config；java-tron 源码树也不含。为避免猜造一份不可信 config 污染 fixture，Task 6 延后到能确认可信 Nile config 来源后再补。

不影响 M1.1 主验证目标：M1.1 的核心价值是对齐 go-tron 默认 DP 与 java-tron mainnet 默认 DP，场景 00 已够。Nile 场景后续作为独立 PR 补入。

---

## Task 7：`run.sh` 入口 + `bash -n` 全量检查

**Files:**
- Create: `scripts/fixtures/run.sh`
- Modify: `.gitignore`（新增 `/tmp/fixture-tron-*`）

**Steps:**

- [ ] **Step 1**：`run.sh` 支持三种用法

```bash
./run.sh <scenario>        # 跑单个
./run.sh all               # 跑 scenarios/ 下所有
./run.sh list              # 列场景
```

实现：
- 解析参数。
- `source lib/java-tron-ctl.sh lib/api.sh lib/dump.sh`。
- 对每个场景：`jt_init → jt_start → jt_wait_ready → setup.sh → run.sh → dump.sh → jt_stop → jt_cleanup`。
- trap EXIT 确保异常路径下 `jt_stop`、`jt_cleanup`。
- 任一场景失败：累计错误、继续下一个；最终退出码 = 失败数。

- [ ] **Step 2**：`bash -n scripts/fixtures/run.sh scripts/fixtures/lib/*.sh scripts/fixtures/scenarios/*/*.sh` 全部无语法错。

- [ ] **Step 3**：`./run.sh all` 能把两个种子场景按序跑完并落盘两个 fixture。

**Acceptance:**
- `./run.sh list` 列出 `00-genesis-dp-mainnet`、`01-genesis-dp-nile`。
- `./run.sh all` 成功后 `git status` 里只有两个 fixture.json 新增。
- 异常退出（手动 Ctrl-C）后 `ps aux | grep FullNode.jar` 无残留。

---

## Task 8：使用文档 + Makefile target

**Files:**
- Create: `docs/dev/fixture-tooling.md`
- Modify: `Makefile`

**Steps:**

- [ ] **Step 1**：`docs/dev/fixture-tooling.md` 覆盖三件事

1. 前置依赖（复用 `java-tron-local.md`）。
2. 如何跑单个场景、如何跑全部。
3. 如何新增一个场景（复制 scenarios 下某个目录 → 改 config → 改 dump 的 section 参数）。
4. fixture.json 字段含义（简表，指向 spec）。

- [ ] **Step 2**：Makefile 增加 target

```makefile
.PHONY: fixtures fixtures-list

fixtures-list:
	@scripts/fixtures/run.sh list

fixtures:
	@scripts/fixtures/run.sh all
```

提示：不要把 `fixtures` 挂到 `all` 或 `test`，它需要本地 java-tron，在 CI 不跑。

**Acceptance:**
- `make fixtures-list` 列出两个场景。
- 文档能让一个没跑过的同事按步骤独立复现。

---

## Task 9：收尾 — PLAN.md 进度表

**Files:**
- Modify: `PLAN.md`

**Steps:**

- [ ] **Step 1**：`PLAN.md` 进度表的"M0′ Fixture 抽取工具"行标 `完成`，写入合并 commit hash

- [ ] **Step 2**：`TODO.md` 暂无对应项需勾（M0′ 不是 TODO 直列项），无需改

**Acceptance:**
- PLAN.md 进度表 M0′ 行填完。

---

## Risks

| 风险 | 应对 |
|---|---|
| 本地 java-tron 启动失败（端口占用、JDK 版本错） | `jt_wait_ready` 60s 超时 + 清晰报错；docs 指向 `java-tron-local.md` |
| 不同开发者 java-tron 版本不同导致 fixture drift | fixture.json 里记录版本，review 时看到版本变就要过问；规则写入 `docs/dev/fixture-tooling.md` |
| JSON 大整数精度 | Go 侧用 `json.Number`；fixture 生成端用 `jq --argjson`（非 `--arg`）保留数值类型 |
| Task 5/6 跑出来的 DP 字段集合在未来 java-tron 升级时扩大 | loader 只断言测试关心的 key（调用方显式遍历），不强校验 superset，见 spec §3.4 示例代码 |

## Out of scope (明确不做)

- accounts / receipts section 的实际 dump（推迟到 M1.2 使用时再实现）。
- 网络场景（多节点、端到端同步，归 M0″）。
- fixture 自动生成 CI job。
