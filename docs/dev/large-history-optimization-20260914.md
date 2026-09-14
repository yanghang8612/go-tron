# 大旧值 CPU 优化与部署核验

2026-09-14。状态：业务与运维已推送 master，业务版本 cda09766 已通过 GitHub master 原生部署，运行采样与健康核验完成。

## 修改范围

本轮处理委托名单 key 的逐项小分配，以及共享历史 writer 对相同片段的重复 SHA。
设计边界见 [方案](../superpowers/specs/2026-09-14-large-history-cpu.md)。
不改变存储表示、已有数据、查询校验、预算或回收条件。

委托 membership 仍为字符串 map；初始键字节复制到私有不可变字符串，再按各项
原长度切片。任意字节、空项、重复项均保留原语义。计费不减少，超预算路径仍使用
原实现，缓存失效和回滚机制不变。

共享历史 writer 每次仍读取并完整解码已存在片段。字节等于本次已计算 SHA 的
候选片段时，复用该证明；字节不同时仍按原顺序检查 hash mismatch / collision。
普通 reader 继续验证每片段和整包 SHA。没有跨操作的可信 chunk 缓存。

## 部署前基线

现版本源码 `64ed654f582a58f8c03578ba38cd78f7ba4d385e`。UTC 06:20:09.488151
原生 pinned adapter 的 `check(candidate)` 与 `healthy_mode(candidate)` 通过。
PID 18282、start ticks 4509893088、process/start/unix_nano
1789364437368890714；仍为上一轮进程。历史 balance canary 返回
`0xb6283b0374b0e000`，与上一轮一致。

独立 `du -s -B1` 读取每个目录，单位为实际分配字节：

| UTC | chaindata | state-snapshots | ancient |
|---|---:|---:|---:|
| 05:47:51（上一轮末） | 245,970,284,544 | 677,963,202,560 | 234,350,960,640 |
| 06:20:09（本轮前） | 240,214,130,688 | 679,399,301,120 | 235,687,653,376 |
| 06:53:21（切换前最后核验） | 244,826,324,992 | 681,079,767,040 | 235,687,653,376 |

06:20 可用磁盘 1,826,331,561,984 字节。05:47→06:20 仍运行旧版本：hot 下降约 5.76 GB，
冷快照和 ancient 同时增长，三者合计下降约 2.98 GB；不能归因于尚未部署的本轮改动。
06:20→06:53 仍是同一旧进程，hot 又增加约 4.61 GB，冷快照增加约 1.68 GB；
可用空间变为 1,819,714,625,536 字节。再次通过原生健康核验和历史查询 canary。
这些目录同时受新增数据、迁移、文件回收和压实影响，短区间不能推断长期增长率。

CPU 基线从 UTC 06:19:05 开始，45.10 秒墙钟、140.42 CPU 秒，build ID
`dfc757ca3c10373e4f6faa6a2a595f83a7a0cacc`。SHA256 AVX2 flat 为 12.78 CPU 秒，
约 9.10%；相比上一轮的 24.03% 已明显变化，说明热点份额受输入和后台任务影响。
固定输入 oracle 基准用于评估本次代码收益，线上不同高度样本用于观测运行状态。

原始采样保存在本机 `build/benchmarks/20260914-large-history/`，13 点 metrics/Wallet
基线位于 `build/benchmarks/20260913-history-prev-0952/before-large-history-20260914/`。
完整原生健康和磁盘 JSON 位于服务器 `/tmp/gtron-large-history-before-native.json`。

## 验证和发布记录

本地固定输入：Apple M1 Max、Go 1.25.5、GOMAXPROCS=2；各组 100ms × 5，
两类基准串行运行。控制函数逐字冻结自 `5915d3cb`，仅机械重命名；业务函数与
控制函数位于同一测试二进制。下表为中位数，完整范围与原始记录保存在本机基准目录。

| 固定工作量 | 旧实现 | 新实现 | 耗时下降 |
|---|---:|---:|---:|
| 10 万个 21 字节地址，完整 decode + entry | 5.781 ms | 4.443 ms | 23.14% |
| 10 万项混合长度/空/重复，完整 decode + entry | 5.279 ms | 4.312 ms | 18.32% |
| 3 个重复大值，共约 6 MiB RLP，已有 chunks，baseline codec + planner + pack staging | 11.283 ms | 10.135 ms | 10.18% |
| 单大值、已有 chunks、未触发 v2，同上 | 4.204 ms | 3.124 ms | 25.71% |

首行分配次数 100,283→269，B/op 10,410,472→10,115,784。这不包含 StateDB 读、
实际修改、历史或 commitment，不是整块执行收益；字符串 arena 可能保留已删除键的
字节至最后引用释放，原始非递减 charge 已保留。预算约束仍针对保守计费，不能将
B/op 或 charge 当作整进程 RSS 硬上限。

共享规划本身的重复大值场景为 4.434→3.220 ms，但完整编码链的下降较小。
上述共享基准不包含 RLP 创建、磁盘 I/O、batch commit 或重开；分配次数不变。
seed/disabled 控制组约 ±2% 且范围重叠，不据此认定这些未修改路径提速或回退。

测试包括 32 个完整 planner 操作（fresh/v2 proof 两条路径）、读写 trace、所有
故障前缀与字节/计数对比；公共 reader 的独立旧 decoder oracle 覆盖错误 digest、
raw/Snappy、长度边界、损坏和 1000 个确定性随机包。委托测试增加超过建表阈值的
任意字节名单，进入原 generic 字节、history/as-of/root 和嵌套回滚对照。
另一代理对共享证明链和冻结 oracle 完成只读交叉审查，无实质问题。

冻结后的 98 项部署事务/恢复及验收解析测试通过。原生准备脚本的固定包列表包含 rawdb 全包，但不包含
core/state 全包；激活前另行执行带 Sapling 的 core/state、actuator 全包测试，再运行
两类原生基准，不能以新增 test pattern 冒充已覆盖这些包。

本地 rawdb/...、state/...、blockbuffer、actuator 全包通过；rawdb/state/actuator 的
Shared/History/LegacyDelegation/Delegated 定向 race 通过，rawdb/state vet 通过。
macOS race 链接器报告已有 LC_DYSYMTAB 警告，测试退出码为 0。

首次组合运行的 core 全包在 154.218 秒结束，唯一失败为
`TestProcessBlockPublishesAsyncSenderRetryOnOrdinaryBlock`：publication candidates=0，
预期 1。没有删除或改弱测试。干净旧版 `5915d3cb` 与候选版在 GOMAXPROCS=2 下
各运行 Cohort 和 Ordinary 两项用例、每项 50 次（合计各 100 次通过）；
GOMAXPROCS=1 下每项 20 次（合计各 40 次同样失败）。旧版单核诊断记录 executed=2、
ready=0、late=2、stale=0、errors=0：异步结果错过边界后按设计串行回退。
目标用例对本次 4 个修改函数的 134 条 statement 覆盖全部为 0。
这证明既有测试的时序前提在受控调度下不成立，保留全部原始记录于 `async-retry-*`，
不以重复通过覆盖首次失败。候选串行 core 全包复查在 156.878 秒结束，仍有该异步
断言失败，另有 `TestStateChangePostingPruneGuardObservedFailureReleasesLease/write`
断言失败：注入写错误正确返回、未推进删除进度且 lease 已释放，但极短操作的
WriteDuration 为 0s，测试要求严格大于 0。

进一步使用干净旧版、相同 Go1.25.5/GOMAXPROCS=2 和完整 core 顺序，复现了相同
异步断言失败；该次代理测试随后因工具环境禁止本地 loopback 监听而中断，不能称为
完成整个旧版测试集。旧版写入子测试单次执行 `-count=200`，176 次通过、24 次均因
同样的 0s 计时失败。两个候选断言问题都在旧版直接复现，未改弱原测试或改动生产
调度。没有发现前序测试遗留修改 GOMAXPROCS 或异步预算，但不据此排除全部调度/GC
影响。临时工作树和 clone 已检查干净后移除。core 全包不是全绿，此限制保留在审计中；
发布依据为本轮业务路径等价性、相关包/race、原生兼容验收及后续运行核验。

业务源码 `cda09766dfc6cce85253484430f4297d8751ea92`，运维
`7b5d21b2bd56d60cf5113b47fdfe340e863993b8`。
部署脚本 SHA256 `9d07ba16924d57b28bb64d0e765e036cfddfedac78f5a29b583051884c4a6bbb`，
额外验收脚本 SHA256 `bb3977430c3dcc1507298ca5e44b1f7bc9f0b83163199460dd0060c54c1ad619`。
即时回滚保留 `64ed654f` 的 writer ON/OFF 和全部五代永久 reader 标记。

原生 prepare 从 UTC 06:41:15.376052 至 06:43:27.696668，退出码 0，Sapling probe
通过。rawdb 全包 11.658s、pebbledb 1.795s；兼容性定向 core 36.813s，pruning
5.925s、snapshots 11.939s，通过。此处 core 为脚本列明的模式过滤，不能冒充全包。
二进制 SHA256 `60d25e94aef665b14564f86e550df69b1a1e9d7e42f403d8f18ff178aba8ec6f`；
upgrade-prepared SHA256 `ee91e9170ab97a37a3cd935c20496ec6de687ab05f80ec6114ead0ffddc79a5d`。
完整准备日志在服务器 `/tmp/gtron-large-history-prepare-20260914.{out,err,result.json}`
及 release 目录；额外原生状态验收和基准由固定 SHA 的 verify 脚本运行。

原生额外验收 UTC 06:45:28→06:46:48 完成，Go 1.25.5 linux/amd64，
GOMAXPROCS=2，CGO/Sapling 开启。state 与 actuator 全包通过；24 个 state 和
16 个 rawdb 基准各运行 200ms × 5，解析器分别核对 120/80 条记录；四条命令
（版本、全包测试、两组基准）均退出 0。
完整记录 `/tmp/gtron-large-history-acceptance-20260914/20260914T064528432678Z-32717/`，
summary SHA256 `5d431bed6dabc6b8785980564eee536992155e5599859d9635afa78a059fad51`。

| 服务器固定工作量 | 旧实现中位数 | 新实现中位数 | 耗时变化 |
|---|---:|---:|---:|
| 10 万地址完整 decode + entry | 18.550 ms | 12.533 ms | −32.44% |
| 重复大值，已有 chunks，完整 codec + planner + pack staging | 62.576 ms | 56.650 ms | −9.47% |
| 重复大值，仅 planner + staging | 28.306 ms | 19.901 ms | −29.69% |
| 单大值、已有 chunks、无 v2，完整 codec + planner + staging | 23.539 ms | 18.954 ms | −19.48% |

首行分配 100,283→269；完整编码路径分配次数不变。服务器基准与本地同样不包含
磁盘写入/commit，也不代表整个同步链路。重复大值完整链路的 min/max 为
旧 54.606–69.351 ms、新 51.883–62.922 ms，有明显重叠；未修改 seed/disabled
完整链路的中位数也分别下降约 5.30% / 8.18%，范围重叠。因后台负载和执行顺序噪声，
不能把 9.47% 视为已严格证明的因果收益。委托名单和 planner 的固定输入范围分离，
与减少分配/重复 SHA 的预期一致。原生控制组 min/median/max 已转录保存在本机
`native-acceptance-observed.json`，完整五次原始输出以上述服务器记录为准。
seed/disabled 的 planner-only 中位数分别上浮约 3.28% / 4.93%，范围同样重叠，
控制组双向波动进一步限制线上基准的精度。

## 原生激活

UTC 06:58:35.843829→06:59:33.827428 激活作业退出 0，事务 phase=active，
active=true，stderr 为空。新进程 PID 3454，start ticks 4510362787，精确
process/start/unix_nano 为 `1789369134357715998`。UTC 07:00:36 再次通过
pinned adapter 的 candidate check/healthy_mode，二进制 SHA 与准备记录一致；
五代永久 reader 标记、observer/GC 队列和配置守卫保持有效。历史余额 canary
与切换前完全相同。原生证据 `/tmp/gtron-large-history-after-native.json`。

UTC 07:01:55 独立 du：chaindata 244,054,654,976 B，冷快照 681,439,526,912 B，
ancient 235,687,653,376 B，可用空间 1,819,943,727,104 B。

部署后完成 13×30 秒运行窗口与 45 秒 CPU 采样，结果如下。


## 部署后的 CPU 构成

新版本内部 pprof 时间 UTC 07:02:26.197187，持续 45.13 秒；HTTP 请求耗时
46.994 秒包含传输，不能替换 profile 时长。新进程年龄约 212 秒，旧样本约 2308 秒。
样本 CPU 总量 140.42→131.63 秒，前后不是同一高度/输入/后台阶段，不据此宣称
整体 CPU 节省 6.26% 或同步提速。

| CPU 路径 | 旧样本 CPU 秒 | 新样本 CPU 秒 |
|---|---:|---:|
| 全部 SHA256 AVX2 flat | 12.78 | 13.20 |
| 共享 writer 的 SHA（互斥归属） | 1.36 | 1.31 |
| 共享 reader 整包及 chunks 的 SHA（互斥归属） | 8.29 | 8.58 |
| 共享 reader 解码整包 cumulative | 9.93 | 10.42 |
| 委托 membership cumulative | 0.59 | 0.59 |
| commitment pipeline union | 40.33 | 35.42 |
| Pebble compaction | 15.74 | 12.81 |

writer 旧 authenticated-decoder 路径采样 0.44→0 秒，但新 planner 直接 SHA 为
0.92→1.31 秒；只能说明调用路径已经变更，不能只看消失的旧函数就宣称所有 SHA
消除。普通 reader 严格认证保持，后样本 reader SHA 占总 CPU 约 6.52%。该窗口
委托名单热点较轻，固定输入的大名单收益未转化为可分离的整体 CPU 下降证据。

commitment 仍占约 26.91%，其持久化读取 cumulative 20.89 秒嵌套在 pipeline 内，
不能重复相加。Pebble compaction 约 9.73%；runHandCold 此样本未采到，不能证明
不再存在缓存淘汰。前后 background 工作不同，短时重启后的样本不足以外推稳态。
CPU、6 分钟吞吐和 du 各有独立时段，不混用分母。


## 完整运行窗口与积压

旧窗口 UTC 06:19:04→06:25:04，新窗口 07:01:11→07:07:11，各 13 点。
每个窗口单独固定精确进程身份、核对 26 个 HTTP 请求完整性和计数单调性，
不跨重启相减。新采样 capture SHA256
`bdabe9fbf5ca805a08dbd554473184c252cfe73e95b0c3e2c8876369c31b0992`。

| 项目 | 旧窗口 | 新窗口 |
|---|---:|---:|
| 同步块/秒 | 15.024 | 13.939 |
| 交易/秒 | 2,983.90 | 2,758.11 |
| 交易/块 | 198.61 | 197.87 |
| stateCommit rolling gauge 中位数 ms/块 | 31.691 | 29.849 |
| metadata density 秒中位数 | 1.234661 | 1.223219 |
| eligible 状态积压，首→末 | 440,101→441,499 | 468,972→471,195 |
| 委托地址解码事件，窗口增量 | 14,600,303 | 30,952,218 |

全窗口吞吐约下降 7.22%，stateCommit 采样中位数约下降 5.81%；不同高度、
地址解码工作量、后台工作和进程年龄不同，不能将其中任一变化直接归因于本轮。
新窗口的 13 点 apply_sample_blocks 都大于 0，没有剔除未初始化点；第二点仅 1 个
apply 样本。前三点 buffer_wait 分别约 78.4%、99.77%、7.96%，从第 003 点起为 0，
表明开头有供块等待。rolling gauge 重叠且可能滞后，不是独立块延迟的统计分位数。
作为事后辅助核算，第 003→012 点（约 270 秒、供块等待 gauge 为 0）为
15.289 块/秒、3,099.17 交易/秒，eligible lag 469,283→471,195（+1,912）。
这说明开头等待影响全窗口观测；该辅助区间不能覆盖 13.939 的完整主结果，
也不能把不同交易构成的 15.289 与旧窗口直接比较来证明提速。


新窗口状态 cold coverage 与 pruned-through 均从 30,697,284 推进到 30,700,070，
推进 2,786 块（约 7.74 块/秒），head-gap 从 534,526 增至 536,761。
body/transaction-index coverage 均为 30,670,848，窗口内未变化；其 head-gap
从 560,962 增至 565,983。eligible 状态积压另一个口径增长 2,223 块（0.47%），
仍处于约 47 万块的同一数量级，但冷迁移尚未追上新增速度，不能称作积压已经稳定。
最后 Wallet currentBlock=31,236,853，targetHead=86,232,483，剩余约 5,499.6 万块，
17 个 peer、8 个 sync peer，bufferedBlocks=953，fetchBackpressured=false；
网络追头缺口不能与约 47 万块冷迁移积压混称。

body 的 30,670,848 是 468×65,536 的完整段边界。源码默认 V2 段长 65,536，
directV2 仅发布完整段；receipt 日志外置时还要求该整段 ReceiptLogRangeCovered，
transaction index 不超过 V2 coverage。末点 V2 backlog 为 524,288 块/8 段，
index debt=0，相关错误为 0。现有指标没有逐段 receipt/event-log 认证覆盖结果，
尚不能定位 body 具体停在哪个门槛；不能用 state cold coverage 替代该覆盖证明，
也不能把完整段发布间隔误判为死锁。需要后续区分正常分段发布与实际迁移受阻。


共享 GC 本进程 retired 累计 8→27，本窗口完成 19 次逻辑退休；覆盖门槛 defer +119，
busy +6。队列 attempts/admitted 均 +7，queue busy/errors 均无新增（+0；
queued_busy 累计 1→1、queued_errors 0→0），工作总墙钟
22,733,809 ns；生命周期最大值维持 65,196,356 ns 不代表窗口没有工作。
退休是逻辑状态，不是已经释放的磁盘容量。

本窗口历史写入 logical staged 289,111,819 B，同输入旧编码估算 1,793,073,932 B；
这是已存在共享编码的写入成本观测，不是本轮 bytes-preserving CPU 修改新增的节省，
也不能直接转换为 SST/WAL 物理净增长。

检查的 70 项非 shadow 错误指标均为 0，历史余额 canary 切换前、切换后和最终
07:08:18 均返回相同值。但不能说所有 error 指标为 0：VM sender-chain shadow
errors 为 2→64，其中 readiness 2→63（+61）、apply_unsupported 0→1；
旧窗口 errors +13（readiness +10、unsupported +2、result +1）。
`preexecutedResultReady` 在六项结果/write/apply 条件都通过后检查 contractRetMismatch；
这里的 readiness 分类对应影子预执行合约返回状态不匹配，不能称为调度超时。
该采样统计在 state_processor 尾部执行，返回 stats 被丢弃；它不是 canonical 入块
或历史写入失败，但增加值得后续按交易类型和高度定位，不能以总同步健康掩盖。

## 最终磁盘与结论

UTC 07:07:36.591404 原生再次通过 candidate check/healthy_mode，仍为 PID 3454、
同一 start ticks/start nano，未再重启。独立 du 的实际分配字节：

| 目录 | 切换前 06:53:21 | 最终 07:07:36 | 变化 |
|---|---:|---:|---:|
| chaindata | 244,826,324,992 | 243,286,437,888 | −1,539,887,104 |
| state-snapshots | 681,079,767,040 | 681,737,900,032 | +658,132,992 |
| ancient | 235,687,653,376 | 235,687,653,376 | 0 |
| 三者合计 | 1,161,593,745,408 | 1,160,711,991,296 | −881,754,112 |

最终可用磁盘 1,820,469,157,888 B。该区间包含切换前旧版运行和重启，且新版本
持久化字节完全等价；不能把热库下降约 1.54 GB 或总量下降约 0.882 GB 归因于
CPU 优化，也不能外推日增长率。obsolete SST gauge 仍出现负值
（最低 −163,412,650 B / −9 files），保留原值并拒绝用受污染的 engine disk gauge
替代真实 du。完整原生记录 `/tmp/gtron-large-history-final-native.json`，SHA256
`79e584a683759028907a0b3ba26e3396eeadea63fdf615355f5b1f8e293b113b`。

本轮达到两项局部目标：固定输入下，大名单的逐项分配显著减少，writer 重复 chunk
认证 SHA 减少；输出、历史查询和保护条件保持。上线成功，短窗口内未发现历史/GC
错误，热库未继续净膨胀。整体同步提速、长期积压稳定及长期磁盘增长率尚未证明。

下一步优先针对 reader 的 Snappy 临时解码/复制及同包重复引用认证建立独立 oracle
与固定输入基准，再考虑优化；不得跳过每次存储读取或削弱完整性校验。
commitment 持久化读取仍是更大的 CPU 项；冷迁移吞吐与新增速度的差额、影子合约
返回状态不匹配应继续按输入分类。上述后续 reader 改动本轮没有实施。
