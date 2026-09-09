# 冷化流水线提速：实施与验证记录（2026-09-09）

本文记录本轮四项冷化优化及其证据边界。部署身份、最终验收和新进程连续观测结果记录在文末；本地组件提速不能单独证明线上积压已解决。

本轮继续保留全部已同步历史状态查询，不缩短历史保留范围，不重置现有数据库。目标是减少重复扫描和转换、利用富余 CPU 缩短冷化工作，并让独立交易索引获得可持续的工作机会。已有 Zstd frame 并行、manifest 解码缓存和此前状态冷化加速不重复计为本轮新增功能。

## 旧进程连续基线

证据：[baseline-full-window/summary.json](../../build/benchmarks/20260909-cold-pipeline/baseline-full-window/summary.json)。北京时间 **2026-09-09 10:30:51.968—10:35:51.974（UTC+8）**，21 个样本，客户端单调时钟跨度 300.050 秒；进程身份保持一致，采样无错误、各覆盖前沿无回退。这是旧进程基线，不能与新代码本地基准混作部署收益。

| 前沿或积压，单位：块 | 窗口开始 | 窗口结束 | 增量 |
|---|---:|---:|---:|
| 同步 head | 13,697,683 | 13,719,894 | +22,211 |
| 状态历史 published | 13,079,078 | 13,099,532 | +20,454 |
| 状态历史 pruned | 13,079,078 | 13,099,532 | +20,454 |
| `head − state published` | 618,605 | 620,362 | **+1,757** |
| 正文 V2 coverage | 12,976,128 | 13,041,664 | +65,536 |
| 交易索引 coverage / pruned | 2,613,248 | 2,621,440 | +8,192 |
| `正文 V2 coverage − index pruned` | 10,362,880 | 10,420,224 | **+57,344** |

窗口内同步约 **74.02 块/秒**，状态发布及裁剪约 **68.17 块/秒**；状态相对 head 的积压仍增加。正文完成一个 65,536 块整段，索引只完成一个 8,192 块小批：这是离散整段发布造成的窗口平均值，不能把正文的 218.42 块/秒当作长期稳定速率。旧索引每五分钟一个 8,192 块批次的配置，本身把理想吞吐限制在约 27.31 块/秒。

`eligible − published` 从 547,226 增至 554,634（+7,408），但 eligible 是上一次 runner cutoff，可能滞后当前 head，不能用它替代上表的 head 差值解释同步追赶。各 gauge 也不是一个原子数据库快照。窗口内 cold/prune/index/body 错误计数及 lifecycle failure/recovery 计数均未增加；这不代表对全库完成了完整性审计。

独立资源窗口来自服务器 `/var/tmp/gtron-cold-pipeline-20260909/baseline-resources-summary.json`：北京时间 **10:31:06.558—10:33:06.573**，9 样本、120.015 秒，无采样错误。进程平均使用 **3.86 / 16 核**，最大 RSS **22.84 GiB**；共享磁盘 busy **73.50%**、await **0.955 ms**、平均队列深度 **3.01**，读 **140.48 MiB/s**、写 **66.95 MiB/s**。文件系统 available 为 **2,714,625,208,320 字节（约 2.47 TiB）**。资源窗口较短且与前沿窗口不完全重合，磁盘数值还包含其他进程的 I/O；CPU 有余量不等于可以无限并发磁盘写入。

## 四项实施

### 1. 交易索引动态预算与完整哈希复用

实现：[transaction_index_budget.go](../../core/freezer/transaction_index_budget.go)、[transaction_index_reuse.go](../../core/freezer/transaction_index_reuse.go)、[heavy_work_reservation.go](../../core/maintenance/heavy_work_reservation.go)。

同步中仍按接近 8,192 块的可恢复小批构建或裁剪。engine 与 device 观测均新鲜且健康时，依据完整工作耗时安排下一次机会，积压较大时提高允许占用时间。**10% / 20% 是该索引维护任务的工作时间占空比目标，不是进程或整机 CPU 百分比，也不是给磁盘划出的固定带宽。** 当前公式对工作耗时 `T` 分别留出至少 `9T` / `4T` 恢复时间，且至少等待 3 秒；大积压阈值为八个默认 V2 段，即 524,288 块。实际准入还受共享 gate 和压力约束，可能低于目标。

未知或过期观测回退原有保守间隔；新鲜硬压力拒绝新任务。两类观测的新鲜度上限为 15 秒，另检查写停顿、L0、持续 compaction debt 增长及设备延迟。启动延迟和错误退避保留。独立 timer 只重试索引小批；最长 10 秒的一次性 gate 预约避免每次重试又被正文发布抢先，预约不打断活跃任务、不跳过恢复期和压力检查，失去资格即取消。`txIndexMu` 串行化显式维护与生命周期维护，预算内部 mutex 保护 timer/准入/完成状态；nil gate 下同样保持单次索引工作互斥。

构建时已经得到且验证过的完整交易哈希，在索引发布并安装 live reader 后直接复用于热行删除，省去第二次读取正文和计算哈希。replay 最多保留 **8 MiB 的 buffer 容量**；超出后使用 collector 临时目录内带 CRC 的顺序块，结束时清理。**8 MiB 仅是 replay buffer 上限，不是整个索引任务或进程 RSS 上限**，还存在 ETL、索引构建器、冷读缓存和数据库工作内存。replay 不作为崩溃恢复依据：重启或恢复已有 run 时仍可从正文重新计算，范围或校验不符会报错。

健康同步的每批发布和删除明细降为 Debug，各维护入口共享约 30 秒的 INFO 进度汇总；首次、最终追平和空闲维护保留 INFO，错误不节流。

### 2. 正文 V2 准备阶段分批并行

实现：[freezer_prepare_v2.go](../../core/rawdb/freezer/freezer_prepare_v2.go)、[direct_v2_prepare.go](../../core/freezer/direct_v2_prepare.go)。

Source 读取保持串行；调用方显式选择并行安全的 Transform 后，独立记录的解码、验证和转换分批并行。在线默认最多四个准备 worker，并受 `GOMAXPROCS / 2` 约束。receipt 验证和紧凑转换共用本次取得的 body，移除原 receipt Source 路径的重复正文读取。

每批至多 **128 条、16 MiB 输入 allocation**，允许多读一条 lookahead；单条超过预算时独立处理。借用源数据在下一次 Source 读取前复制，避免跨表共用缓冲导致内容被覆盖。**16 MiB 不包含转换结果、protobuf 临时对象、lookahead、frame 编码队列和压缩 workspace，不是 RSS 上限。** 所有准备 worker 在返回前 join；取消和错误不会遗留后台转换。字典采样、有序 frame 写入、整表先后顺序及 hashes 的使用边界保留。

### 3. manifest 派生索引与紧凑 JSON

实现：[manifest_lookup.go](../../core/state/snapshots/manifest_lookup.go)、[manifest.go](../../core/state/snapshots/manifest.go)、[compactor.go](../../core/state/snapshots/compactor.go)。

在 Manager 私有的不可变 manifest 视图上建立路径查找及已排序 freezer 行号索引，避免每次读取重新扫描和排序；compactor 的一次候选选择建立局部索引，替代每个 history segment 两次全目录 companion 扫描。仅存 `uint32` 行号，不重复复制路径和 SegmentRef；超过 1,048,576 个 active refs 时退回扫描，每份索引最多约 8 MiB，40,960 active refs 且没有 freezer 的本地样例约 160 KiB。

公开 loader/clone 返回可修改且不携带索引的 manifest；外部可变对象只在同步调用内建立借用视图，不挂长期缓存。已有解码缓存的完整字节一致性检查、Manager 文件身份判断和 manifest 校验不变，没有使用 mtime/generation 绕过验证，也没有提高 64 MiB 解码缓存上限。

发布改为紧凑 JSON，省去格式空白，减少编码和每次重新读取的输入。字段、校验与签名语义、原子替换、文件及目录 fsync 保留。JSON 变小只证明 manifest 空白开销下降，不能据此推算整库压缩率。

### 4. ETL 按独立 radix 桶并行排序

实现：[collector_radix_parallel.go](../../core/rawdb/etl/collector_radix_parallel.go)、[collector.go](../../core/rawdb/etl/collector.go)。

串行 radix pass 先确定互不重叠的桶区间，只在至少 128K 条可排序记录、且有足够独立任务时使用最多四个 worker，每组至少 32K 条。短输入、全等键和缺少独立工作的严重倾斜桶保留串行路径。worker 复用已有 order/scratch 的不相交区间，不增加一份全量排序数组，也不并行文件或数据库写入。

保留 key/sequence 排序、前缀键、最后 Put/Delete 生效及跨 spill 合并语义。中断回调只在调用方执行，worker 通过原子 stop flag 响应；取消或回调 panic 后先 join，再允许复用池内数组。spill 写入取消时清理临时文件且保留可重试输入。

## 持久化与历史查询约束

本轮不改变共识、公开 API、历史保留策略或已有冷文件读取格式。热数据删除仍以已验证、持久化发布且可读的冷数据覆盖为前提；交易索引发布失败不使用 replay 删除，删除未完成不提交该小批的完成游标。正文维持全记录输入验证、全帧读取检查及抽样字节对照，event-log 外置依赖仍须先有已认证的覆盖。没有通过跳过验证、直接推进游标或丢弃旧历史换取速度。

## 本地基准

均为 Apple M1 Max / Darwin arm64 的同输入组件比较；正文准备基准 `GOMAXPROCS=8`，以下其他基准为 10。取三次测量的中位数。ETL 固定五次排序/测量；正文及 replay 固定三次完整操作/测量；manifest 为 300ms 自动迭代窗口。

| 测量范围 | 对照 | 优化路径 | 可确认结果 |
|---|---:|---:|---|
| 1,024 块、每块 48 笔交易，完整三表 V2 migration；准备 worker 1→4 | 347.98 ms | 277.14 ms | 耗时下降 **20.36%**；V2 输出均为 19,133,768 B |
| 8,192 块、131,072 笔交易，索引构建/发布/裁剪；双扫→replay | 206.65 ms | 150.91 ms | 耗时下降 **26.97%**；正文读取 16,384→8,192；索引逐次 SHA-256 一致、均为 2,195,592 B |
| 40,960 active refs，全部 companion 查询，含索引构建 | 1,422.95 ms | 7.61 ms | 消除该查询工作量的平方级扫描；已建索引查询为 2.71 ms |
| 60,000 总 refs，JSON 编码 | 38.76 ms | 20.16 ms | 编码耗时约减半；17,312,352→13,352,279 B（**−22.9%**） |
| 262,144 条随机键，ETL 排序旧串行→4 workers | 9.582 ms | 3.910 ms | 该排序阶段 **2.45×** |
| 相同规模重复键 / 共享前缀 / 顺序键 | 14.707 / 14.259 / 5.847 ms | 7.181 / 7.383 / 4.415 ms | 该排序阶段分别 **2.05× / 1.93× / 1.32×** |

正文准备对照组已包含本轮移除重复正文读取的串行路径，因此这个 20.36% 衡量额外准备并行的收益，并不是相对旧二进制的完整改动收益。其 `B/op` 中位从约 852.01 MB 增至 892.26 MB（+4.72%）；这是一次操作累计分配，不能当作驻留或峰值内存。replay 的累计分配从 432.87 MB 降至 292.20 MB（−32.50%），含义相同。

manifest companion 对照是同一数据的线性扫描路径，含索引构建约 187 倍的差异只适用于该目录查找组件。JSON 近 2 倍、ETL 约 2 倍及其他组件改善，**都不能称为全节点、全冷化流水线或同步速度的相同倍数收益，更不能相乘。** manifest 输入为合成目录，其最初计时不能排除其他本地编译/测试干扰，只用于数量级和规模趋势判断。

ETL 基准不含收集、spill I/O、merge、Pebble 写入；新单 worker 随机键样例比旧串行中位约慢 3.4%，短样本不足以据此认定普遍回退。正文基准包含字典、验证、fsync 和 manifest，但 Source 为内存构造的 protobuf 语料，不模拟服务器 Pebble 读负载。replay 基准使用真实 V2 读取及 run 构建/验证/发布，热删除为 memorydb，不代表 Pebble WAL/compaction；每轮重开冷 reader，未清 OS 文件缓存。这些本地结果均未覆盖主网 20M 以上高度的交易密度和磁盘争用。

原始证据：

- [preparation-bench.txt](../../build/benchmarks/20260909-cold-pipeline/preparation-bench.txt)
- [hash-reuse-results.json](../../build/benchmarks/20260909-txindex-budget/hash-reuse-results.json) 与 [hash-reuse-bench.txt](../../build/benchmarks/20260909-txindex-budget/hash-reuse-bench.txt)
- [manifest-index/bench.txt](../../build/benchmarks/20260909-manifest-index/bench.txt)
- [parallel-etl/report.md](../../build/benchmarks/20260909-parallel-etl/report.md)，含旧实现固定基线、独立排序 oracle、命令和局限

`build/benchmarks` 为本地采样/基准工件路径；若未随仓库分发，复验时应保留或另行归档这些文件。

## 已完成的局部验证与待验收项

已完成 manifest / Manager / Catalog 定向测试及相关 race；覆盖公开副本修改、同 generation 原子替换、companion 完整身份、freezer 顺序和并发副本隔离。ETL 包测试及新增排序/取消相关 race 通过，包含二进制前缀键、重复键、严重倾斜、长公共前缀、取消、回调 panic、spill 清理和重试。

正文相关定向 race 在 `core/rawdb/freezer` 与 `core/freezer` 通过，见 [preparation-race.txt](../../build/benchmarks/20260909-cold-pipeline/preparation-race.txt)。新增验证涵盖借用缓冲、并行结果乱序、超大行单独运行、实际阻塞 worker 的取消 join、非法 body/receipt 不发布，以及全部测试记录字节一致。

交易索引预算/replay 定向 race 已覆盖 nil/非 nil gate、显式维护与 timer 并发、压力/过期观测、发布失败、取消删除和无 replay 恢复；末次记录为 `core/freezer` 6.485s、`core/maintenance` 2.286s。日志收尾及其行为测试已被后续全仓执行覆盖。独立只读审查发现的预算数据竞争已通过 `txIndexMu` 与预算 mutex 修正。

全仓执行 `go test ./... -count=1 -timeout 300s`：53 个有测试包通过、13 个包无测试，仅 snapshots 包的两项字典取消测试失败。原因是测试硬编码第六次 `ctx.Err()` 取消，新增排序中断检查令取消提前发生。测试改为实际处理达到 4,096 条时取消，同时要求少于 8,192 条、collector 尚存、错误为 `context.Canceled` 且关闭后无文件残留；没有削弱生产取消或文件清理。字典/访问器与内存/落盘四种组合的定向 race 已通过（2.565s），完整 snapshots 包复验通过（56.158s）。最终共有 54 个有测试包通过，另有 13 个包无测试。热数据读取路径审计已通过。

原始全仓结果保留在 [full-test.txt](../../build/benchmarks/20260909-cold-pipeline/full-test.txt)，完整 snapshots 复验见 [snapshots-final-package.log](../../build/benchmarks/20260909-cold-pipeline/snapshots-final-package.log)，取消验证见 [etl-cancellation-race.log](../../build/benchmarks/20260909-cold-pipeline/etl-cancellation-race.log)。这里记录的是首轮通过的 53 包加失败包修正后的完整复验，未把首轮末尾的 `FAIL` 改写成单次全仓命令通过。

服务器使用 sapling 构建标签完成七个相关包原生测试：`core/freezer`（9.612s）、`core/rawdb/freezer`（8.291s）、`core/rawdb/etl`（2.468s）、`core/maintenance`（0.004s）、`core/state/snapshots`（38.404s）、`core/state/pruning`（9.058s）、`cmd/gtron`（21.011s），全部通过；原生构建退出码为 0。这些验证仍不能证明固定磁盘最终可容纳全部主网历史。

## 部署身份与已完成的在线查询

状态：**已上线 `3c2ac2e387c5f8b84c18bc4889b18a0e0f839fa2`**。部署与原生验证证据见 [deployment-verification.json](../../build/benchmarks/20260909-cold-pipeline/deployment-verification.json)。服务器命令结果经 JumpServer 转录保存，精确进程启动标识另由 HTTP metrics 读取；本文未把转录的原生测试摘要描述成原始 stdout。

| 部署项 | 已核对结果 |
|---|---|
| 切换开始，北京时间 UTC+8 | 2026-09-09 10:57:51.350 |
| 旧进程停止 / 新进程启动 | 10:58:02.057 / 10:58:02.285 |
| PID | 旧 2332 → 新 **23970** |
| `process/start` 纳秒标识 | **1788922682298445556** |
| 运行文件 | `/data/gtron/releases/20260909-cold-pipeline-3c2ac2e3/gtron` |
| 二进制 SHA-256 | `3c23a6b4870b156dd765581e949172b4ade6189d9ebbdf375ff03c11a20f4850` |
| 部署退出码 | 0 |

北京时间 **10:59:59.827** 的早期历史查询 canary 通过，见 [genesis-canary.log](../../build/benchmarks/20260909-cold-pipeline/genesis-canary.log)：在同一新进程标识下，对预先选定账户的 2,043、2,044、2,045 高度区块 hash 与余额序列完成裁剪后核验；当时状态 published/pruned 均已达到 13,150,035。该证据只覆盖**一个账户的标量余额个例**，不能代替全部历史域或全库完整性验证。

## 首版 3c2ac2e3 的部分在线窗口

首版观测保留了 **38 / 41 个预定样本**，北京时间 **11:00:23.164—11:09:38.184**，客户端单调时钟跨度 **555.606 秒**。读取下一个样本（index 38）时 curl 超时，采集终止；随后检查发现本机 `127.0.0.1:1088` SOCKS 代理停止监听，curl 连接被拒绝、`lsof` 无 listener，浏览器显示 `ERR_PROXY_CONNECTION_FAILED`。这证明后续远程访问路径受阻，**不能据此认定服务器或 gtron 故障**。

原始 [summary.json](../../build/benchmarks/20260909-cold-pipeline/post-window/summary.json) 为 `capture_failed`、`accepted_window=false`，保留这些状态；这不是完成并验收的十分钟窗口。另由 [independent-analysis.json](../../build/benchmarks/20260909-cold-pipeline/post-window/independent-analysis.json) 对已有点核算，且独立逐份读取 raw JSON 复核：38 个样本均属于进程 `1788922682298445556`，与 observations 中的前沿一致，所采前沿均无回退。

| 前沿或积压，单位：块 | 首个成功样本 | 最后成功样本 | 部分窗口增量 |
|---|---:|---:|---:|
| head | 13,827,820 | 13,859,928 | +32,108 |
| state published / pruned | 13,152,765 | 13,183,443 | +30,678 |
| `head − state published` | 675,055 | 676,485 | **+1,430** |
| 正文 V2 coverage | 13,107,200 | 13,107,200 | **0** |
| index coverage / pruned | 2,777,088 | 3,555,328 | +778,240 |
| `正文 V2 coverage − index pruned` | 10,330,112 | 9,551,872 | **−778,240** |

已有点覆盖的窗口中，head 约 **57.79 块/秒**，state published/pruned 约 **55.22 块/秒**，状态相对 head 的净追赶为 **−2.57 块/秒**：索引债务确实减少，但状态积压并未在该部分窗口整体收敛。索引完成 95 个 8,192 块小批，发布及裁剪约 **1,400.70 块/秒**。正文覆盖没有推进，因此这段数据**不能验证正文准备并行的线上吞吐收益**，也不能据此把未发布整段等同于后台完全没有工作。

首尾 state published/pruned 一致；中间 37 个点相等，一个点（11:06:08）显示 pruned 比 published 高 793 块，随后再次相等。指标读取和更新并非一个原子快照，保留此差异，不将其截为零，也不单凭该点认定裁剪越过持久化覆盖。

在这 38 个点中，cold、prune、正文 V2、交易索引 errors，以及 prune checksum failed、lifecycle pass failures/recoveries 的增量均为 **0**。范围仅限所采相关子系统；全量独立快照中仍有 shadow sender-chain 的既有诊断计数，不能写成全进程所有错误计数为零，也不能跨 PID 相减。此部分窗口未形成可对齐的完整 CPU/RSS/磁盘资源验收结果，不将旧资源窗口数值套用到新进程。

前几个短子窗口的状态速率曾先低于、随后高于 head；最终应以上述完整可用范围为准，不能挑选局部区间证明持续回退或持续追平。旧进程与首版处于不同高度、缓存和 I/O 条件，此处是实际观察，仍不是严格同输入主网 A/B，更不是全部优化的最终线上验收。

## 第二补丁：分离索引自身等待与共享 gate 恢复期

**状态：本地实现与相关测试完成，尚未部署，尚未线上验证。** 上述部署身份和 38 点数据仍只归属于首版 `3c2ac2e3`；第二补丁部署后须以新的二进制、进程身份和独立窗口追加验证，不能把两版混为一次验收。

首版在健康索引小批完成后，为共享 gate 安装至少 3 秒恢复期。约 0.3 秒的短批也会让其他需要 gate 的状态/正文工作共同等待。这是源码可确认的额外门控等待，值得消除；首版窗口还受到区块负载、合并和资源争用等条件影响，**不能把这一点认定为状态速率波动的唯一原因**。

第二补丁在 lease 释放时，用 gate 实际测得的持有时间决定共享恢复期：仅当准入及完成时均健康、资格有效且本批没有错误时，采用 `min(3 秒, 实际持有时间)`。完成时观测未知、失去健康条件或本批出错，则回到 gate 的默认恢复策略（生产默认 15 秒）；gate 原有最小工作时长阈值继续生效。预约获得的 lease 与普通 lease 使用相同逻辑，已有活跃 lease、既有 cooldown 和准入压力检查不能被绕过；释放策略异常时也会归还 token 并保留默认恢复，避免锁住全部维护。

**索引自身的等待没有缩短**：10% / 20% 工作占空比仍分别按至少 `9T` / `4T`，且至少等待 3 秒。改变的是其他维护任务何时能够竞争空闲 gate，未放大索引小批、取消共享互斥或削弱发布后才能删除的约束。

第二补丁相关包复验通过：`core/freezer` 28.515s、`core/maintenance` 1.041s、`core/state/snapshots` 69.208s、`core/state/pruning` 25.565s，命令为 `go test ./core/freezer ./core/maintenance ./core/state/snapshots ./core/state/pruning -count=1 -timeout=300s`，输出见 [measured-recovery-packages.txt](../../build/benchmarks/20260909-txindex-budget/measured-recovery-packages.txt)。最终定向 race 命令 `go test -race ./core/maintenance ./core/freezer -run 'TestHeavyWork|TestTransactionIndex(SmallBatch|ReleaseRechecks|FailedBatch|Budget|ProgressLog)' -count=1 -timeout=120s` 通过（maintenance 2.029s、freezer 6.385s），覆盖真实构建错误回退、释放时压力复查、短批恢复、预约、并发与日志行为。该补丁的服务器原生验证及线上新窗口仍待访问恢复后完成。
