# 热历史范围清理排队调度部署记录（2026-09-13）

本轮排队调度已上线，完整十分钟后测确认 range 实际执行：新增 124 个范围、删除
11,679 行、覆盖 3,072,141,187 字节原始 key/value（2.861 GiB）。锁忙回退旧 point
的选中块比例由前测 100% 降至 31.447%；guard 错误无增量，排队持链锁总计 0.1248 秒。
这证明范围清理获得了执行机会，逻辑删除量不等于立即释放磁盘。同步速度观察为
23.3635→27.2542 块/秒，但负载与后台工作不同，不能把这项差异当作因果加速。

01:26:27 UTC 的 native prepare 因旧 binary SHA 人工转录错一字符在 preflight 拒绝，
尚未 archive、build 或停止服务；01:28:25 的现场核对确认运行 binary 与旧 prepared
原始记录完全一致，纠正新 helper 并重跑离线审计后，01:33:49–01:36:54 的重试成功。

## 改动与修正的前提

[上一轮完整后测](sync-reclaim-rollout-20260913.md)中，17,856 个热历史选中块全部锁忙
回退旧 point，实际 range runs 为 0。本轮前测仍未出现 range，说明需要解决执行机会。
已有指标没有区分 index 和 chain 锁，不能仅凭 fallback 数量确定主导锁。

调用链复核确认：冷构建函数返回时释放 heavy-work lease，之后才执行 beforeMerge
中的热清理；live 热清理没有持有该 lease，仍同步处于 `Runner.passMu` 序列。
本轮保留该序列与原热清理资源策略，不新增 gate 或 posting 的 32 GiB 准入门限。
验证缓存的 `retain` 不是冷文件 lease，不能把选块和 coverage 直接交给独立任务。

原 `TryWithStateDomainChangePruneGuard` 继续两锁非阻塞。新增显式排队入口先尝试
index 锁，再公平等待 chain 锁；获锁立即检查取消，随后验证原 canonical、
Finish/index、solidified、commit/flush 错误和 `HistoryPrefixSettled`，两锁保持到
同步 scan 与全部 batch flush 完成。仅锁忙可以回退 point，proof、取消或写入错误
不能降级；失败不提前推进整 pass 水位，部分写入仍沿用原幂等重试。

`GTRON_HISTORY_RANGE_QUEUE=1` 要求原 range 开关也为 1。每 pass 合计最多尝试
256 个 selected blocks 的排队额度，每次 64 块、最多四次，尝试前扣额度，index busy 也消耗额度。
剩余块及不足 64 块的片段继续原 256 块 Try/point；额度在同一 pass 的所有回调间共享。
这限定数量，不限定等待或 I/O 时长：`sync.Mutex` 不能即时取消，Stop 仍需等当前
持锁者释放后检查取消。不改变共识、wire、数据库键、保留规则或 Pebble 压实配置。
设计与计划：[spec](../superpowers/specs/2026-09-13-history-range-scheduling.md)、
[plan](../superpowers/plans/2026-09-13-history-range-scheduling.md)。

## 固定版本、回滚与本地验证

| 项目 | 固定值 |
|---|---|
| 新源码 | `fdfe853236445ca77c2d7a9553c1d4e172ff9a43` |
| 最终 Ops 提交 | `024a3b1bbc08c186e0232517f56598e2a413e57c`（首版 `8880d92971acc28e9fa4f9634afa48045e65f5be`） |
| 纠正后 Helper SHA256 | `a0fcf33a7ccf09ad45dab5d8bc5f101955d36e8193d022854bf2f0568c6ff70b` |
| 新 release | `/data/gtron/releases/20260913-range-scheduling` |
| 新 binary SHA256 | `f6a61e0d0f3c1426ab2d7849991878ff9639168791fbdb940d08e628432460f1` |
| 旧版/回滚源码 | `4940853af2424d04b86236e4ec682dcfc32e75e2` |
| 旧版/回滚二进制 | `/data/gtron/releases/20260912-reclaim-optimization/gtron` |
| 旧版/回滚 SHA256 | `b47a0920272cc54fdd0c355663e2c9d477bd5c912d99bc689df7ee99a3e4d868` |
| 旧 PID / start ticks | `20101` / `4499354749` |
| 旧精确 metrics 进程身份 | `1789259053976239426` |

01:12:10.560636 UTC native 现场核验旧身份及四个开关为 1、queue 不存在、40 GiB
MemoryLimit、guard/holds、原配置一致。现场记录为截图人工转录，不是原始 stdout。
首次 prepare 将 SHA 零起算第 56 位的 `a` 错录为 `c`；现场逐字符核对及旧 helper
使用原始 prepared SHA 的完整 preflight/config 均通过，PID/ticks 未变，candidate
source 目录尚不存在。新 helper 仅纠正该字符，源码及 14 项 manifest 未变，17/16 项
离线审计再次通过。证据：[SHA 纠错](../../build/benchmarks/20260913-range-scheduling/observed-old-sha-correction.json)。
隔离 release 使用固定源码的 14 个非 docs 文件 SHA256（13 个 Go 文件及上一部署脚本），
现有 checkout 保持 `19eda11f44424f673a40b06051edcbf629b50846`。激活仅替换 exe 并新增
第五开关，回滚恢复旧 unit 完整字节和元数据及旧版二进制。

第一次完整 affected 测试中，仅既有
`TestProcessBlockPublishesAsyncSenderRetryOnOrdinaryBlock` 出现 `candidates=0, want 1`
的偶发失败；其他受影响包通过。该项定向重跑 10 次通过（1.076 秒），随后完整 core
复查通过（156.061 秒），不能把第一次执行记为全绿。新 guard 诊断、排队证明/取消、
批量写入锁生命周期、Worker 额度与 adapter 测试均通过定向和 race；最终增量 lint 为
`0 issues`。证据：[affected 日志](../../build/benchmarks/20260913-range-scheduling/affected-tests.log)、
[重跑](../../build/benchmarks/20260913-range-scheduling/async-retry-recheck.log)、
[core 复查](../../build/benchmarks/20260913-range-scheduling/core-final-recheck.log)、
[queue race](../../build/benchmarks/20260913-range-scheduling/gtron-range-queue-race.log)、
[wiring race](../../build/benchmarks/20260913-range-scheduling/wiring-final-race.log)、
[lint](../../build/benchmarks/20260913-range-scheduling/lint-final.log)。

离线部署静态审计 17 项、source/身份/pin 审计 16 项全部通过。原生测试筛选已补齐
`StateDomainChangePruneQueue` 和 `DomainPruner.*GuardAdapter`，已提交的 13 项新增测试
均落入 native targeted 或完整包测试范围；实际原生执行结果另见下段。
证据：[静态审计](../../build/benchmarks/20260913-range-scheduling/deployment-static-audit.json)、
[pin 审计](../../build/benchmarks/20260913-range-scheduling/deployment-final-pin-audit.json)、
[原生筛选覆盖](../../build/benchmarks/20260913-range-scheduling/deployment-native-selection.json)、
[现场身份](../../build/benchmarks/20260913-range-scheduling/observed-preflight.json)、
[rollout pins](../../build/benchmarks/20260913-range-scheduling/rollout-pins.json)。

## 原生构建与激活

纠正后的 prepare 于 01:33:49–01:36:54 UTC 成功：固定源码与 14 项文件清单复核通过，
native affected 10 个包、五开关全启用 6 个包、targeted 4 个包全部通过；Sapling probe
及 Go 1.25.5、linux/amd64、`CGO_ENABLED=1`、`tags=sapling` 构建通过。构建前后旧
PID/ticks、配置/argv、guard/holds 均保持，源码从隔离目录构建。

01:40:53.684503 UTC 的 postflight 已通过：exe 为新 release 下的 `gtron`，PID `4345`、
start ticks `4499806875`，精确 metrics 身份 `1789263575235585425`；激活健康检查链头
29,900,319→29,900,332。五个开关均为 1，MemoryLimit 为 42,949,672,960 字节（40 GiB），
guard/holds、受保护服务与配置均核验通过，配置差异仅为批准的 exe 和第五开关；原 31
项及新增 15 项指标合同通过启动检查。后测首个 HTTP 进程身份与该进程一致。

新 binary 由服务器 `prepared.json` 与 helper 的 `verify_prepared` 校验作为原始依据；
01:52:20.235769 UTC 已将上述本地 SHA 字符串与服务器原始记录作 native equality
确认，完整相等。PID/ticks、五开关、40 GiB、批准的配置差异及 guard/holds 末检通过。
PID `4345` 对应 UDP/TCP 18890、TCP 8090/8545/50051、loopback TCP 6062/6071，均符合布局。
证据：[原生准备复核](../../build/benchmarks/20260913-range-scheduling/observed-prepared-review.json)、
[激活后核验](../../build/benchmarks/20260913-range-scheduling/observed-postflight.json)。

## 本轮完整前测

21 个 metrics 与 21 个 Wallet 请求全部有效且关联同一个精确进程。原 31 字段完整，
新增 15 字段整组未暴露，记为 N/A；不能补零。43 个采集源文件分析前后未变。
metrics 窗口为 01:13:26.864667–01:23:26.804132 UTC、599.940738 秒；
Wallet 独立窗口 599.909252 秒。后测应使用本目录 before，不能复用上一轮基线。

| 观察 | 全窗口 |
|---|---:|
| Session 同步速度 / 交易速度 | 23.3635 块/秒；2,393.6387 tx/秒 |
| 交易密度 / 全进程平均 CPU | 102.4519 tx/session块；3.0108 核 |
| State 发布/热清理头距 | 327,674→329,185（+1,511） |
| Body 冷覆盖头距 | 375,471→389,518（+14,047） |
| 交易索引冷覆盖头距 | 375,471→389,518（+14,047） |
| 成功热清理 pass / blocks | 17 / 12,536 |
| 实际 range runs / fallback_blocks 增量 | 0 / 12,536 |
| Posting callbacks / chunks / pressure 增量 | 0 / 0 / 599 |
| Debt 首→末；范围 | 43.6746→41.4091 GiB；41.4091–45.0248 GiB |
| Compaction 输入 / 输出增量 | 50.7161 / 45.4204 GiB |

State 覆盖/清理推进 12,536 块，body 与交易索引覆盖均停在 29,491,200，三类头距
仍为原数量级但本窗均扩大。交易索引来自 `chain/freezer/txindex/coverage`，
不是 guard 验证的 `StageStateHistoryIndex`；三类头距不可相加。
buffer 块数 1,799→1,911，观察范围 1,406–1,973，无暂停或背压。
原 point fallback 已成功清理，不应把新 `point/rows=0` 解释为没有 point 删除。

后半 metrics/Wallet 均为 011–020 的 10 点，分别 269.986233/270.005636 秒；
session 28.9179 块/秒、2,417.7236 tx/秒、83.6064 tx/块。块速随密度变化，不能将
不同负载窗口直接当版本因果收益。所选 canonical/storage/non-shadow、prune/posting、
写 stall、SST read/prefetch 错误均无增量；shadow sender-chain +1、VM sender-chain +41
单列，后者 readiness +38、unsupported +3 为子项，不相加。
证据：[before-summary](../../build/benchmarks/20260913-range-scheduling/before-summary.md)、
[完整分析](../../build/benchmarks/20260913-range-scheduling/before-analysis.json)。

## 完整后测与前后比较

本轮 after 为 21 个 metrics、21 个 Wallet 成功请求，全部关联精确进程
`1789263575235585425`，原 31 项及新增 15 项字段完整有效，43 个采集源文件分析前后
未变。metrics 为 01:41:05.997811–01:51:05.920298 UTC、599.922858 秒；Wallet 为
599.980183 秒。比较明确引用本目录 `before-analysis.json`，
`before_after_observation.available=true`；各进程内分别求差，没有跨重启减累计值。

| 全窗口观察 | Before | After |
|---|---:|---:|
| Session 块/秒 | 23.3635 | 27.2542（观察 +16.65%） |
| tx/秒 | 2,393.6387 | 3,088.0753（观察 +29.01%） |
| tx/session块 | 102.4519 | 113.3063 |
| 全进程平均 CPU 核数 | 3.0108 | 3.2589 |
| State 发布/热清理头距 | 327,674→329,185（+1,511） | 328,428→327,334（−1,094） |
| Body 冷覆盖头距 | 375,471→389,518（+14,047） | 344,621→360,985（+16,364） |
| 交易索引冷覆盖头距 | 375,471→389,518（+14,047） | 369,197→360,985（−8,212） |
| 成功热清理 pass / blocks | 17 / 12,536 | 26 / 17,458 |
| 锁忙 fallback blocks / 本窗成功清理块 | 12,536 / 12,536（100%） | 5,490 / 17,458（31.447%） |
| Range runs / rows 增量 | 0 / 0 | 124 / 11,679 |
| Posting callbacks / chunks / pressure 增量 | 0 / 0 / 599 | 0 / 0 / 599 |
| Debt 首→末 | 43.675→41.409 GiB | 49.572→54.704 GiB |
| Compaction 输入 / 输出增量 | 50.716 / 45.420 GiB | 54.663 / 47.325 GiB |

后测 state 发布/清理推进 17,458 块，body 覆盖未推进，交易索引冷覆盖按批次推进
24,576 块。三类积压仍在原数量级，其中 state 与交易索引头距收窄，body 头距扩大；
不能合称“积压完全没扩大”。buffer 为 1,488→1,154 块，观察范围 635–1,890，21 点均
active，无暂停或背压。

负载差异不能忽略：各自 19 个去重完整 import 子窗口内，VM 交易占比由 33.06% 降至
30.36%，每 VM 原始能量 23,281.60→21,612.80；每 apply 块 commitment updates
378.51→387.52。这些子窗口与 Wallet 分母不同，执行/commit/persist 阶段重叠，不能
相加。后半 after 的 metrics 010–020 为 11 点、299.852357 秒，Wallet 011–020 为
10 点、269.993534 秒；session 24.5339 块/秒、2,928.9627 tx/秒、119.3842 tx/块。
相比 before 后半，块速低 15.16%、每块交易高 42.79%、TPS 高 21.15%，再次说明不能
单看块速判断版本性能。后半 metrics 起点 state 发布/热删除水位相差 1,008，分别
推进 12,457/11,449 块；不将异步水位强行对齐。

Guard 共尝试 156 次、准入 122 次，index busy 5 次、旧 Try 路径 chain busy 29 次；
prefix_unsettled、proof/other/work errors 均为 0。排队 API 进入 99 次，其中 44 次遇到
chain busy 后等待；累计 chain wait 4.921916279 秒、chain held 0.124787783 秒。
按 API 进入摊销为 49.716/1.260 ms，包含未进入阶段的零值，不是实际获锁均值。
held 进程峰值为 10.215855 ms；wait 峰值 5.775208 秒已在窗口首点出现，不能将其
称为本后测窗口新增的最长等待。

Range work（scan+flush）累计增加 76.134369 ms，原 key/value 逻辑字节增加
3,072,141,187；范围路径内另有 point rows +289、logical bytes +189,776,531。
锁忙后的旧 key-only point 清理不在上述 point 行/字节统计内；31.447% 是 fallback
选中块比例，其余块不保证都形成 range。一次 guard 可形成多个范围，guard 即时计数
与成功整 pass 才发布的 range 统计也不是相同分母，不能将 99 次排队等同于 124 ranges。

所选 canonical/storage/non-shadow、prune/posting、写 stall、SST read/prefetch 错误
均无增量。Shadow VM sender-chain +31 单列，其 readiness +28、unsupported +3 为
子项；它们不等同 canonical 错误，也不叠加求总数。
证据：[完整后测](../../build/benchmarks/20260913-range-scheduling/after-analysis.json)、
[后测摘要](../../build/benchmarks/20260913-range-scheduling/after-summary.md)。

## 物理占用与后验

01:12:10–11 UTC 同路径执行 `du -x -B1 --max-depth=1` 成功，统计 allocated 字节：

| 目录 | Before allocated 字节 / GiB | After allocated 字节 / GiB | 变化 GiB |
|---|---:|---:|---:|
| chaindata | 230,372,638,720 / 214.5512 | 227,306,745,856 / 211.6959 | −2.8553 |
| state-snapshots | 577,933,074,432 / 538.2421 | 580,743,327,744 / 540.8594 | +2.6173 |
| ancient | 217,504,231,424 / 202.5666 | 218,492,358,656 / 203.4869 | +0.9203 |
| 合计 | 1,025,809,948,672 / 955.3600 | 1,026,542,436,352 / 956.0421 | +0.6822 |

后验 du 在 01:54:11.473971–01:54:11.882935 UTC 同路径执行成功，exit 0、stderr 空；
首次执行因运行中的 snapshotlog 临时文件消失返回 1，未用于比较。数据盘可用
1,964,293,005,312 字节。约 42 分钟的两次 du 间包含旧进程、构建、重启和新进程，
chaindata 下降 2.8553 GiB 不能归因于后测 2.861 GiB 的 range 逻辑删除，两者数值
接近也不构成证据；全目录仍净增 0.6822 GiB。

Before du 早于前测、after du 晚于后测，两次均非原子，不能与 gauge 逐字节对账。
12 个 namespace 均有
有效估算：before 末次 changeset/commitment/state_change_index 为
86.731/55.585/42.511 GiB，after 末次为 80.666/56.373/42.728 GiB。
这些估算各自时间不同、可能包含旧版本且边界重叠，不求和当全库，也不据其差额分摊 du。
Before 14/21、after 9/21 样本仍有已知 obsolete 负值，同源 disk gauge 不用于计算回收量。
After 12 项估算均已知且 errors 增量为 0，成功样本年龄 30.770–360.770 秒。
证据：[du](../../build/benchmarks/20260913-range-scheduling/observed-disk-before.json)、
[前测分布](../../build/benchmarks/20260913-range-scheduling/before-distribution.md)、
[后测分布](../../build/benchmarks/20260913-range-scheduling/after-distribution.md)。
后验物理证据：[after du](../../build/benchmarks/20260913-range-scheduling/observed-disk-after.json)。

Canonical canary 的 29,901,300 / 29,910,200 / 29,917,700 三块，block ID、parent hash、
有序 transaction IDs 均与 TronGrid 一致，交易数分别 86/142/120。第三块首次 TronGrid
请求 curl 56，重试成功，共 8 请求；结论限于这三块，不扩大为全部区块或 state roots
验证。证据：[canary](../../build/benchmarks/20260913-range-scheduling/canonical-final-summary.json)。
01:56:05 UTC 检查日志尾部 5,000 行，`ERROR|EROR|CRIT|FATAL|panic` 匹配 0；近期冷快照
发布、冷快照 compaction 完成、热 prune 均持续，末日志链头 29,923,671。结论仅覆盖
该有限尾部和所选正则，不代表穷尽错误或逐块验证。01:58:06 已退出专用 SSH 并观察到
logout，关闭本任务专用浏览器标签，保留原 overview 和用户 Web 终端。
证据：[日志末检](../../build/benchmarks/20260913-range-scheduling/observed-log-review.json)、
[会话清理](../../build/benchmarks/20260913-range-scheduling/observed-session-cleanup.json)。

后测新增 15 项含 queue enabled、guard 八个尝试计数和六个 queued 计时/计数。
计数在尝试期间即时发布，与成功 pass 才发布的 range 工作量分母不同；
`work_errors` 是 `admitted` 子集，`queued/chain_busy` 也是子集，不重复相加。
进行中的尝试和非原子 scrape 可有分类差额，不能强制修平；max 是进程运行期峰值，
不是独立窗口最大值。旧版回滚只要求原 31 项，不要求新增 15 项。
逻辑删除、compaction 流量、debt、SST 估算与实际 du 分开解释；只有真实 range 工作
发生后才能评估此次排队是否有效，不能以开关或排队次数代替结果。

## 下一步

本轮已解决“range 始终无法执行”，先保留当前 queue 参数。剩余 31.447% 的 selected
blocks 回退旧 point，按代码定义仅由 guard 锁忙触发，应继续用锁诊断解释。另一个问题
是已准入 guard 内的 point 行能否形成范围；连续段对齐只针对后者，尚无证据将其归因于
缺口/repair 行。若后续证据支持，应在同一 selected 列表、相同 proof、256 块总额度内
用固定输入和真实 Pebble 验证，不能混作减少锁忙 fallback 的方案。

Posting 仍在压力前置检查退让：after 每点 debt 都高于 32 GiB 门限，callbacks 为 0，
不能再归因于 chain/gate 或靠扩大扫描预算解决。需保持门限，先补压力原因分项，
并对代表性写入/删除和查询轨迹做 L5 Snappy 的离线 I/O/CPU 对照，评估是否降低持续
压实成本；这只是候选，不是已验证应上线的改动。Changeset 已有压缩，不能按其估算
大小推算二次压缩收益，也不通过强制 Compact 或格式升级追求立即缩容。

`state_change_index` 的 42.728 GiB 是含 posting 与 keys 目录的范围估算，不是纯
posting 或可删除量。当前 posting-only 只删最后高度不超过冷覆盖许可 H 的整帧，
不删目录或跨 H 混合帧。依据与后续实验边界见
[下一轮只读判断](../../build/benchmarks/20260913-range-scheduling/next-optimization-review.md)。
