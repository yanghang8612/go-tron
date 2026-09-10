# 线上采样驱动的状态读取优化（2026-09-10）

第一批改动完成：native 合约运行时读取不再构造 ABI 对象图；奖励查询和无需完整快照的结算分支不再物化无关账户字段。另修复了投票缓存的交易读取依赖，以及 recorder 重复记录时临时 key 的生命周期问题。本文记录本地实现与验证；后续已通过 GitHub 提交部署主网，Linux 版本、线上采样及回退信息见[发布报告](sync-state-hotpaths-deployment-20260910.md)。

依据：[线上同步采样](sync-performance-sampling-20260910.md)。该窗口前台导入持续忙碌，ContractRuntime、GetAccount 引发的辅助字段扫描出现在 CPU 栈中。本轮修改集中于这些已有证据的路径。

## 实现和兼容性边界

- `core/state/statecodec/contract_runtime.go`：验证完整 native SmartContract → ABI → Entry → Param，保留字段顺序、类型、shape、默认值规范、UTF-8、int32 范围及 opaque unknown trailer 检查。只返回运行时所需的五项字段；异常输入回退原通用 decoder，保留错误诊断。schema 测试对照生成描述符。
- `core/state/contract_runtime_metadata.go`：借用字段在调用内立即转成已有值类型，沿用缓存失效和存储 key 计算逻辑。native 正常解码为零分配；未修改编码格式或 protobuf wire 解码逻辑。
- `actuator/withdraw_reward.go`：查询使用存在性、allowance、votes；结算先判断未来周期、当期已有快照等条件，无当前票时也能跳过全量账户。需要记录当前票快照时，仍在任何 `AddAllowance` 之前完整序列化账户。Java 依据为本地 java-tron `MortgageService.java:89–184`，该仓库 HEAD 为 `705ab290861e65c1bb24a572a0a65ea8e2c3816c`。
- `core/state/account_votes.go`：缓存命中时补齐 30 个允许槽（包括不存在的槽）和账户 generation 的读取依赖。缓存跨交易、Copy、Revert 和 generation 切换后仍可检测前序投票写入。
- `core/state/transaction_access.go`：重复相同访问模式只查找 map；升级访问模式时先复制 key 再写入。避免 borrowed scratch 替换 map 中原本拥有独立字节的 key，导致多个槽的依赖丢失。新回归在原 HEAD overlay 上三种组合均失败，修复后三种均通过。

没有改动 state root 算法、持久化格式、奖励算法、同步并发或服务配置。commitment 和冷化的后续优化仍需分别处理；本轮没有复用尚被异步 layer 引用的 arena。

## 基准方法

仓库基线 `b44113a8af8e0a92644d68a1c4e699b4a30ec549`，Apple M1 Max、darwin/arm64、Go 1.25.5、CGO=1、`-tags=sapling`、默认 10 个逻辑 CPU。测试和其他本任务的 CPU 工作结束后，各基准顺序运行，每项 5 次，以下为中位数。decode/首次读取使用 250ms，奖励每次 500 个独立状态视图，vote 缓存使用 1s。运行时间与完整参数在 `benchmark-runs.json`。

这是单机合成微基准，没有统计全链各分支命中频率，也没有重放同一段主网区块；不能把这里的百分比直接当作同步吞吐提升。

### native 合约解码

旧路调用通用 native decoder 后投影到 runtime 字段；新路调用专用 reader，两个基准都产出同一值。空样本为空合约；其余样本有 4 KiB bytecode，每个 ABI 项含两个 input 和一个 output。protobuf 对照也有测量，但其旧生产路径本轮未修改，不作为新增收益。

| 场景 | 旧 → 新（μs/op） | 耗时变化 | 旧 → 新（B/op） | 旧 → 新（allocs/op） |
| --- | ---: | ---: | ---: | ---: |
| native，ABI 0 项 | 0.491 → 0.422 | -14.1% | 224 → 0 | 1 → 0 |
| native，ABI 8 项 | 16.526 → 2.157 | -86.9% | 11,800 → 0 | 307 → 0 |
| native，ABI 64 项 | 114.593 → 13.251 | -88.4% | 61,976 → 0 | 2326 → 0 |

### StateDB 真正首次读取

每次新建 StateDB，再读取已 Commit 的 native 行，计入账户和 KV 首读。底层是内存 DB，包含已有 DB cache 的影响，不代表磁盘冷读。旧版通过 Go overlay 恢复 HEAD 的 `contract_runtime_metadata.go`，运行完全相同的 `ContractRuntime` benchmark；表中不使用 `GetContract` 替代旧实现。

| 场景 | 旧 → 新（μs/op） | 耗时变化 | 旧 → 新（B/op） | 旧 → 新（allocs/op） |
| --- | ---: | ---: | ---: | ---: |
| 首次读取，ABI 0 项 | 7.267 → 7.342 | +1.0% | 11,189 → 10,965 | 42 → 41 |
| 首次读取，ABI 8 项 | 26.311 → 10.829 | -58.8% | 28,158 → 16,344 | 348 → 41 |
| 首次读取，ABI 64 项 | 128.937 → 23.241 | -82.0% | 85,340 → 23,270 | 2367 → 41 |

空合约的首次读取没有收益（约 +1.0%）；8/64 项 ABI 分别减少约 59%/82% 耗时。新版仍须遍历 ABI 做完整校验，但消除了其对象分配和 bytecode 复制。

### 奖励查询和结算

同一 fixture 中，账户包含 6 类辅助 map × 24 条记录、权限和 Stake V1 资源，底层包含 8 层 blockbuffer × 64 个账户的写入。旧函数为冻结的原 reward 实现，新旧共用最终 StateDB。每次建立独立状态视图，初始化和预热不计时；“冷/热”指账户是否已完整物化，系统 reward 数据仍由该状态视图读取，不指操作系统 page cache。

| 场景 | 旧 → 新（μs/op） | 耗时变化 | 旧 → 新（B/op） | 旧 → 新（allocs/op） |
| --- | ---: | ---: | ---: | ---: |
| 有票奖励查询（冷） | 221.531 → 21.938 | -90.1% | 199,180 → 14,818 | 1941 → 308 |
| 有票奖励查询（热） | 6.072 → 6.530 | +7.5% | 3,168 → 3,168 | 50 → 50 |
| 有票结算（冷） | 331.444 → 329.131 | -0.7% | 276,481 → 276,468 | 3444 → 3444 |
| 有票结算（热） | 118.617 → 119.130 | +0.4% | 80,200 → 80,226 | 1553 → 1553 |
| 无票结算（冷） | 233.477 → 21.082 | -91.0% | 214,236 → 19,637 | 1941 → 299 |
| 无票结算（热） | 23.039 → 6.903 | -70.0% | 18,252 → 8,348 | 53 → 44 |
| beginCycle 在未来，早退（冷） | 216.995 → 4.305 | -98.0% | 198,397 → 2,466 | 1921 → 31 |
| beginCycle 在未来，早退（热） | 5.108 → 4.982 | -2.5% | 2,365 → 2,363 | 30 → 30 |
| 当期快照已存在，早退（冷） | 220.005 → 7.794 | -96.5% | 199,795 → 5,479 | 1941 → 65 |
| 当期快照已存在，早退（热） | 8.134 → 8.118 | -0.2% | 3,767 → 3,767 | 50 → 50 |

收益集中在冷态查询、无票结算及早退分支。需要写完整快照的有票结算基本持平，不能宣称这部分物化已消除。热态查询此次中位数增加 0.458 μs（7.5%），分配不变；保留这一结果，不宣称所有分支均加速。无 recorder 的已缓存 GetVotes 从 8.449 ns 变为 8.803 ns，均为零分配。

## 验证结果

- Go 1.25.5 全仓 `CGO_ENABLED=0 go test ./... -count=1 -timeout=300s`：通过，54 个有测试的包。
- `core/state`、`core/state/statecodec`、`actuator` 全包 race：通过；`core` 中 Parallel/Speculative/TransactionAccess 相关 race：通过。
- native reader 差分覆盖合法值、随机值、截断、逐位变异、四层 malformed 矩阵与 schema 检查；15 秒 fuzz 共 44,639 次，无差异。
- reward 对照覆盖 10 种周期/投票/快照组合，以及冷/热、dirty→revert→copy、新 generation；完整结算后账户、cursor 和实际 SystemReward native 快照行逐字节一致。损坏 vote 不更新 allowance/cursor。
- 快照比较读取实际原始 native 行。`ReadCycleAccountVote` 的 API 会通过非确定性 protobuf 重编码 map，直接比较两次 API 返回的字节顺序会误报，这不代表存储行发生变化。
- native 与 protobuf 的 generic metadata 写入、runtime 缓存失效和回滚均有覆盖；最终定向 Sapling race 日志为 `runtime-cache-final-race.txt`。
- `make gtron`：Sapling/CGO 生产目标构建通过，产物为本机 darwin/arm64，不是 Linux 服务器发布包。
- `CGO_ENABLED=1 GOFLAGS=-tags=sapling scripts/system_test.sh`：79 passed、0 failed、0 skipped，两个临时节点已由脚本清理。
- 初次全仓 lint 输出 158 项，但工具默认每个 linter 最多输出 50 项、相同文本最多 3 项，因此不是全量计数。后续取消这两项输出上限，当前扫描报告 1,893 条：errcheck 1,623、govet 4、ineffassign 5、staticcheck 126、unused 135；其中 1,014 条位于测试文件。它们是检查条目，不等于已证实的独立缺陷。包含所有未跟踪新 Go 文件的补丁增量 lint 为 0 issues；该结果也不能代替对全仓清单的逐条基线比对。全仓 lint 未通过。

本轮未执行真实主网区间的同输入 A/B replay。部署后的版本、回退信息及 CPU、blocks/s、alloc rate、state/history debt 复采结果已记录在[发布报告](sync-state-hotpaths-deployment-20260910.md)，仍不将局部微基准收益直接当作端到端提升。

## 可复核证据

原始输出位于 `build/benchmarks/20260910-sync-hotpaths/`（git 忽略目录）：

- `benchmark-runs.json`、`benchmark-summary.json`、`run_benchmarks.py`、`summarize_benchmarks.py`：命令、时段、原始数据汇总和可重复运行脚本。
- `runtime-baseline-first-read-go125.txt`、`runtime-bench-go125.txt`、`reward-bench-go125.txt`、`votes-cache-bench-go125.txt`：基准原始输出。
- `validation-results.json` 与各测试/lint/build 日志；`recorder-baseline-test.txt` 和 `recorder-fixed-test.txt` 是旧版失败、新版通过的直接对照。
- `baseline-*.go` 与 `*-baseline-overlay.json`：冻结的原始 production 代码及 Go overlay。
- `changes.patch` 包含 tracked/untracked Go 修改；`evidence-sha256.json` 保存本轮最终源码补丁、产物和日志的摘要。
