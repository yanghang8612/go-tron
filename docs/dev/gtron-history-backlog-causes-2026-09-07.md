# gtron 热历史积压成因核对

2026-09-07。只读审查本地代码及已归档生产观察、离线维护结果；没有访问服务器、启动节点或修改运行时代码。本文件只讨论 gtron，未独立核验 Erigon 实现。

目前最有依据的判断是：**持续导入产生历史的入口，与构建、合并、验证、裁剪、物理回收的出口没有形成按历史债务约束导入的闭环。** 深度同步时主动降低后台工作的优先级；达到水位后只保证仍有进展，不保证追平。事故前已测窗口实际上正在缓慢清债，并非始终入口快于出口，但尚余约 132 万块欠账。历史大 value 的整行原像和反复处理放大了工作量，磁盘满又阻止需要临时空间的后台操作。不能把整个积压归因于某个尚未测量的 CPU、压缩算法或单次慢查询。

## 水位说明：主要落后在冷覆盖，不只是最后删除

维护后的只读重测在 2026-09-07 07:43:04 UTC 完成：head 仍为 30,512,882；hot-prune 已至 29,127,572，第一条 live changeset 为 29,127,573。热历史跨度为 **1,385,310 块**，是生产 snap 热窗口 65,536 块的 **21.14 倍**；这是区块跨度，不是程序已经运行了多少天。changeset SST 范围估算 **1,353,738,143,222 B＝1,260.77 GiB**，占 chaindata 约 87.59%。估算含旧版本和 tombstone，不等于唯一 live value，也不等于可删除字节。

证据：[维护后分布](../../build/benchmarks/20260907-history-format/distribution-transcribed.json)、[最终水位](../../build/benchmarks/20260907-offline-recovery/cold-results-transcribed.json)、[维护前水位](../../build/benchmarks/20260906-disk-audit/pebble-observed.json)。这些 JSON 是远端终端结果转录，原始输出保留在服务器。

维护前 cold history/accessor 已到 txNum 1,734,454,192、对应 block 29,127,540，hot-prune 到 txNum 1,734,063,547、block 29,125,223。两者只差 **2,317 块 / 390,645 txNums**，约一个普通冷构建批次。离线已逐条验证并删除这个已覆盖范围，随后仅新建了 32 块。与百万块的热跨度相比，现存冷覆盖后“尚未删热副本”的这一个批次不是主体；主要存量位于尚未冷覆盖的更后区间。

默认窗口不能当删除命令。snap 要保留全部历史，未有正确冷副本就必须留热原像。在线 builder 的 cutoff 是 `min(solidified - HistoryWindow, verified Finish)`；维护已知 solidified=30,512,864，据此在线 cutoff 为 30,447,328，距当前 hot-prune 尚有 1,319,756 块。此计算只说明候选跨度，不能按比例推算候选字节或绕过逐段覆盖证明。[cold_builder.go:1102](../../core/state/snapshots/cold_builder.go:1102)、[worker.go:223](../../core/state/pruning/worker.go:223)；snap 默认来自 [params/config.go:72](../../params/config.go:72) 和 [:124](../../params/config.go:124)，不是 full 模式的 262,144 块默认。

## 已部署调度如何允许债务持续存在

9 月 6 日审计确认实际 release 为 `/data/gtron/releases/20260906-p2f/gtron`，`--prune.mode snap` 且没有覆盖热窗口。发布二进制 SHA 为 `6428c224f80ba592e9a4bdc66c2776195a63be20ef57d3d57f67bc6f41320c2c`，配置保留 history format=2、双源 merge 验证 workers=2。[P2f 部署记录](28m-history-stream-p2f-2026-09-06.md:101)

本审查形成时、后续本地实现开始之前，重新计算的 `cold_builder.go`、`lifecycle.go`、`history_config.go` SHA 与 9 月 6 日现场逐文件证明相同：[证明](../../build/benchmarks/20260906-disk-audit/split-observed.json)。因此下列准入逻辑对应旧部署，而非新维护工具。`cmd/gtron/main.go` 接线亦与归档生产基线副本相符，但它未列入当时五个现场 SHA 文件，证据层次应区分。随后这些文件已因[本地调度实现](targeted-cold-scheduling-2026-09-07.md)发生变化；本节是旧部署分析，不再声称当前工作区 SHA 仍匹配。旧行号也应结合归档基线阅读。

| 条件 | 行为 | 对清债的影响 |
|---|---|---|
| 深度同步，eligible backlog ≤ 4×窗口＝262,144 块 | 延期 cold history | 明确允许先积累一段历史 |
| 超过软水位，但 importer 忙；backlog ≤ 8×窗口＝524,288 块 | 继续延期 | “超过软水位”不表示马上全速构建 |
| 超过 busy 水位，importer 仍忙 | 强制较小批次，构建后恢复 30–60 秒 | 保证尝试推进，不能保证出口速率高于入口 |
| 软水位以上且 importer 持续有余力 | accelerated 路径可绕过普通 catch-up 间隔 | 并非所有构建都固定每分钟一次 |
| 同步不活跃且有待处理数据 | 不受同步期间限速；lifecycle 请求下一次 pass | 停机本身不会运行此后台组件，需另行维护入口 |

代码：[水位倍数](../../cmd/gtron/history_config.go:170)、[准入判断](../../core/state/snapshots/cold_builder.go:1173)、[重试间隔](../../core/state/snapshots/cold_builder.go:1661)、[恢复窗口](../../core/state/snapshots/cold_builder.go:1721)、[连续下一批](../../core/state/pruning/lifecycle.go:489)。实际 wiring 为 30 秒 quiet period、1 分钟 catch-up interval、3 秒特殊重工作 cooldown：[main.go:53](../../cmd/gtron/main.go:53)、[:1145](../../cmd/gtron/main.go:1145)、[:1212](../../cmd/gtron/main.go:1212)。

“有余力”判断要求下载队列没有 buffered/inflight，且 target/applied/session progress 在观察间保持不变；不是单次看到空队列就放行。[history_config.go:208](../../cmd/gtron/history_config.go:208)。P2g 归档现场在 head 30,472,894 时 `active=true, paused=false`、bufferedBlocks=4,182，说明当时仍在忙碌追赶；它只能证明该时点，不能代替全时段准入比例。[node-current.json](../../build/benchmarks/20260906-p2g/node-current.json)

源码更精确的注释明确 busy watermark **不是严格 backlog 上限，因为 importer 可能比强制后台批次更快地产生债务**：[cold_builder.go:145](../../core/state/snapshots/cold_builder.go:145)。`history_config.go:153` 等较早的“bounded/hard cap”注释不能压过实际控制流。

这里不是说整个节点没有背压：下载器有按 bufferedBytes 的 fetch 背压，Pebble 也有自身写入控制。缺少的是以冷历史积压字节、已证明覆盖/回收进度及可用空间为反馈，限制前台持续导入的闭环。下载队列背压只限制下载缓冲，不限制积存在持久库中的历史。[net/sync.go:1753](../../net/sync.go:1753)

后台还有共享 `HeavyWorkGate`：重任务竞争同一非阻塞名额，失败者延期；需要支持 freezer 的 event-log 缺口可先获准入，history build 后还可能执行 merge，再进入 ordered prune。慢构建/合并或失败可以拖延这一轮后续裁剪，但现证据不能量化每种等待的全天占比。[heavy_work.go:11](../../core/maintenance/heavy_work.go:11)、[cold_builder.go:925](../../core/state/snapshots/cold_builder.go:925)、[:946](../../core/state/snapshots/cold_builder.go:946)、[lifecycle.go:231](../../core/state/pruning/lifecycle.go:231)。

## 事故前实际窗口：有清债，但已有存量很大

根任务后续只读提取生产 `gtron.log`，在北京时间 2026-09-06 17:00–18:30 的 publication 时间窗口获得 98 条发布日志，全部 `forcedBusy=true`。首尾可比状态如下；这是远端结果转录，本子任务未再次访问生产。[原始字段转录](../../build/benchmarks/20260907-history-types/cold-log-window-transcribed.json)

| publication 时刻 | 已发布 block | eligible cutoff | 未覆盖跨度 |
|---|---:|---:|---:|
| 17:00:29.816 | 29,056,713 | 30,407,305 | 1,350,592 |
| 18:29:24.783 | 29,125,223 | 30,446,936 | 1,321,713 |

首尾间隔 **5,334.967 秒**，cold 覆盖前进 68,510 块，eligible 边界前进 39,631 块，因此 backlog 减少 28,879 块；相应净边界速率是 **12.8417 / 7.4285 / 5.4132 blocks/s**。这里 cold 速率使用两次已发布边界之差，未把首条日志那一批也重复计入。eligible 速率是该候选边界的前进速度，不等于网络 TPS、history 字节写入速度或前台 head 的逐条导入计数。

这证明按区块边界计，该窗口冷覆盖推进快于 eligible 边界，**未覆盖跨度正在缩小**，所以不能写成“事故前一直越积越多”。它同时证明 busy 缩批和恢复窗口确实在运行：首条 elapsed=13.749 秒、恢复约 54.997 秒；末条 elapsed=59.879 秒、恢复 60 秒。既有存量仍大，冷覆盖进展不等于同量物理字节已释放；不能凭此一个窗口外推全部历史的完成 ETA，也不能反推更早债务的形成速率。缺少按历史字节/磁盘余量约束导入的闭环判断仍成立。

## 批大小：32 块不是线上默认

- 在线正常批次的 block 上限为 **5,000**，txNum 目标 **390,625**，以先触及者附近的完整 block 为边界。
- 生产 busy 配置将两者除以 4，得到 **1,250 blocks / 97,656 txNums 目标**。txNum 目标不是严格硬上限：代码纳入跨过目标 txNum 的完整末块，所以可略超；也不能将 txNum 与交易数量、区块数量互换。
- 本次 **32 blocks** 是人工选择的离线安全 canary，与生产默认无关。已有覆盖试验先做 32，再一次处理余下 **2,285**；新建试验另取 32 块，没有将全链限制为每批 32。

依据：[默认批次](../../core/state/snapshots/cold_builder.go:26)、[busy 缩批](../../core/state/snapshots/cold_builder.go:1703)、[完整末块选择](../../core/state/snapshots/cold_builder.go:1246)、[离线批次转录](../../build/benchmarks/20260907-offline-recovery/cold-results-transcribed.json)。9 月 5 日实际在线日志曾记录一批 846 blocks / 97,784 txNums，输出 256,385,507 B、12.595 秒，与“busy 目标、完整块收尾”相容；这是一次观察，不是稳态平均值。[P2b 记录](28m-history-footer-p2b-2026-09-05.md:13)

## 已测性能事实与不能推出的结论

1. **历史大记录反复处理是实际负担，但全库业务占比仍未知。** 当前新 32 块的 21,747 records 中，旧式 `drax-0` 委托 aggregate 的 Prev 共 48,242,830 B；最大单条 4,731,955 B，同一个 key 10 个原像合计 47,318,560 B。证明该样本存在大列表反复写入，而不是证明全热库 1,260.77 GiB 都是委托数据。[分类转录](../../build/benchmarks/20260907-history-format/canary-transcribed.json)
2. **过去测到过具体转换热点，且已有局部修复。** P2d1 的 112,758,934 条历史记录 merge 中，build-accessor 阶段约 261 秒；P2e 已改逐 key scratch 为有界内存缓冲。P2f 又在固定 428,548,548 B 生产 fixture 上将 merge 中位数 8.049651→6.659756 秒，改善约 17.27%。这些是特定阶段/样本，不能重新把已修的旧瓶颈当当前全链唯一根因。[P2e](28m-history-posting-p2e-2026-09-06.md:7)、[P2f](28m-history-stream-p2f-2026-09-06.md:52)
3. **Pebble 压实竞争有观测依据，但无法等同冷历史债务。** P2f 一个 25.17 秒 CPU 样本中 Pebble compaction 占 14.33 CPU-s / 24.76%；对应线上有效窗口的 Pebble compaction debt 54.32→72.04 GiB。该窗口未执行目标大型 history merge，后台任务和负载不同；不能把单次 profile 推成全天 CPU 比例，也不能把 Pebble debt GiB 与冷覆盖落后块数相加。[P2f 观察](28m-history-stream-p2f-2026-09-06.md:75)
4. **离线耗时不是原在线调度吞吐。** 2,285 块 prune-only 的 92.681 秒包括内部计划、完整覆盖验证、冷热比对、删除与游标持久化，不是“解码 1.3 GB 用 92 秒”。32 块新建的 10.244 秒也是多阶段，之前 CLI RO plan/RW open 和之后独立压实不全部计入；线上还有 trusted verification cache 等区别。不能按这两个数字线性预测百万块完成时间。[效率口径](history-conversion-efficiency-2026-09-07.md:11)
5. **冷化不是等量节省磁盘。** 新 32 块完整冷化加物理压实后，chaindata 减少 49,815,552 B，snapshot 增加 49,733,632 B，净省仅 81,920 B。加速冷构建可以收紧热尾，但未证明能把完整历史装进固定磁盘。已有压缩及 V7 索引，不能把“开启压缩”当缺失开关。[恢复结果](mainnet-offline-recovery-2026-09-07.md:142)

## 已清除与尚未证明的原因

维护前旧逻辑删除 changeset 残留约 90.87 GiB，已通过受限压实处理；前三次物理维护分别前后 df 增量合计约 92.88 GiB，含正常 WAL/过期文件回收，不能全部精确归因同一前缀。当前主要剩余热历史不是这批已回收垃圾。

retired manifest 的 876.34 GiB 是已经不存在的文件记账，本轮集合可回收物理空间为 0；不能说“只因 retired prune 延期而囤了 876 GiB”。posting/key-directory 现约 45.46 GiB，确有“冷构建追到 cutoff 且网络追赶空闲才扫”的控制，可能保留失效索引，但不能把全部索引认定为垃圾，更不足以解释 1.26 TiB changeset。[盘点](mainnet-disk-audit-2026-09-06.md:53)、[索引调度](../../core/state/pruning/lifecycle.go:271)

事故日志北京时间 2026-09-06 18:32:21.370 明确出现 `ancient/tx-index/etl/...: no space left on device`，证明临时工作空间实际耗尽；该条直接失败点是交易冷索引 ETL，不是 state-history 压缩器。因此它能证明共享磁盘压力阻断维护，不能独自证明状态冷构建此前每次失败的原因。停机后持久 hold 阻止节点再启动，剩余积压不会靠时间自然变少。[事故审计](mainnet-disk-audit-2026-09-06.md:18)

仍缺少同一长时段的输入变化字节、cold 发布/热删/物理回收速率，以及各准入拒绝和阶段耗时序列。故最稳妥的成因排序是：**已证明存在巨大未冷覆盖热历史；已部署调度允许持续导入优先且不能强制清债；具体记录形态、重复 I/O、merge/LSM 竞争增加成本；磁盘耗尽是已经发生的终端约束。** 各项贡献百分比、精确故障起点和完成 ETA 尚未被测出。

后续实现应同时验证历史字节债务上限与磁盘余量反馈、后台净清债率、历史大列表无损增量布局和临时峰值；不要只调大批次、取消校验或恢复同步来试。用户保留全部历史查询的前提不变，所有新格式和控制回路仍须独立实现验证后才具备生产能力。
