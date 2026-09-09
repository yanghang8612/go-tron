# 冷化压力恢复与准备流水线：第二轮验证

## 当前结论

本轮减少 CPU 重复工作，并消除索引软压力触发后的固定五分钟空等。保留全部历史查询、现有数据格式、65,536 块正文段、8,192 块索引叶和发布后删除顺序。没有调整压缩级别，因此不声称提高数据库压缩率。

代码、全仓测试、针对性 race 和本地对比基准已完成。服务器已拉取并部署 `22972031`，隔离源码的八个相关包原生 sapling 测试与构建全部通过。旧版正常退出，新进程二进制哈希一致，历史查询与新版本热转冷查询复验通过。完整十分钟窗口确认索引积压减少 188,416 块，但状态积压增加 50,213 块，整体积压仍未解决。

## 改动

### 索引软压力恢复

实际工作恢复截止、压力复查时刻、保守恢复截止和错误退避分别保存。软压力每 15 秒提供复查机会；压力消失后，需要两个不同的新鲜健康观测，engine 时间至少相隔 5 秒，device 样本也必须更新。重复读取缓存样本不算恢复。复查只是观察机会，实际工作仍受 `max(3s, 4T)` 或 `max(3s, 9T)` 恢复截止、共享维护 gate 和已有错误退避约束。

硬压力每 30 秒复查且不准入。未知或硬压力下完成的批次保留保守间隔；真实失败继续按原有错误退避处理，健康观测不能清除它。取消或异常完成也必须归还 gate，整批工作时长包含完成时探针成本。

新增 `chain/freezer/txindex/budget/{admission,completion}/reason` 与观测时间；`wait/reason`、绝对截止时间和采样时剩余等待用于区分压力、工作恢复、gate 和错误。旧 `healthy` gauge 表示最后一次准入检查，不代表当前磁盘状态。`remaining_at_sample` 也不是实时倒计时。

Health reason：0 未观测，1 健康，2 engine 不可用，3 engine 时间无效，4 device 不可用，5 device 时间无效，6 write stall，7 memtable 硬阈值，8 L0 硬阈值，9 新 stall，10 L0 软阈值，11 debt 连续增长，12 await 超过 2ms，13 等待第二次健康证据，14 批次失败/取消，15 工作候选范围未知。

Wait reason：0 无计划等待，1 工作恢复，2 软压力复查，3 保守恢复，4 硬压力复查，5 实际错误退避，6 启动延迟，7 共享 gate。

### 回执转换复用

紧凑 ID 转换和外置日志身份检查共用一次 wire 交易哈希计算；外置日志复用已解析的紧凑 TransactionRet。direct 入口的完整 typed 验证继续执行，避免将 typed 重编码哈希与原始 wire 哈希混用。

先执行原有 wire strip，再解析实际输出，保留异常 protobuf 的回退语义：重复 ID、未知字段、非最短 varint、异常 block number、创世空行、普通空行、已有 envelope、无日志和外置负收益。包含 protobuf map 的成功外置结果原本就可能以不同字段顺序 marshal，测试比较 envelope 和解码语义；其余确定性用例逐字节及错误结果比较。

### 有界准备流水线

一个 producer 顺序读源，固定 CPU workers 并行准备，按序交付给现有 frame 编码流水线。默认准备 4 workers、编码最多 8 workers。128 个可复用槽位，已准入输入 allocation capacity 最多 16MiB，另允许一个大小事前未知的已读输入；超大记录独占且不再前读。这个预算不是进程 RSS 上限，已交付结果、解码图和 frame 编码仍占额外内存。

字典训练先完成才启动异步读源。成功、读取/转换/编码/ETL/fsync 失败及取消路径都在验证或进入下一表之前 Close/join，不能留下后台 Source 访问。磁盘源读取保持串行，保留验证、fsync、manifest 发布和热删除边界。

## 性能证据

Apple M1 Max / darwin arm64，本地基准均串行运行；以下为三次测量中位数。累计分配不等于峰值 RSS，各项收益不能相加或作为节点整体倍数。

| 对比 | 原实现 | 新实现 | 结果 |
| --- | ---: | ---: | --- |
| 128 笔交易回执转换 | 160.275 µs/row | 106.049 µs/row | 耗时下降 33.83%，约 1.51 倍 |
| 同回执累计分配 | 423,955 B/row | 286,538 B/row | 下降 32.41% |
| 同回执 allocation 次数 | 1,431/row | 781/row | 输出同为 800 B，逐字节一致 |
| 三表准备 barrier → pipeline | 247.612 ms | 237.709 ms | 耗时下降 4.00%，约 1.04 倍 |
| 三表累计分配 | 944,976,760 B/op | 944,774,818 B/op | 下降 0.021%，基本持平 |

三表基准使用同一内存 Source、1,024 块 × 48 笔交易、相同 CPU 转换、4 准备 workers 和 8 编码 workers，包含写三表、验证全部 frame、fsync、manifest 和 reopen。冻结旧 barrier reader 与新 reader 共用当前 writer，包括字典先训练顺序。它不模拟 Pebble 读取或热删除。输入 30,874,437 B，输出均为 27,202,988 B；每次查询全部 3,072 行，SHA-256 均为 `28852445482986cb9c53aa2d270d749f7cfe9592b8b8502a95c0317ca8c1aabf`。

旧三表三次耗时范围 242.478–277.159ms，新三表 233.685–240.559ms。仅三组短测，流水线改善温和；不能据此宣称原来约 74 秒整段持有 gate、约 90 秒状态发布停顿已解决。

进一步使用现有 `BenchmarkDirectV2PreparedMigration` 检验真实 direct 转换流程：相同 1,024 块 × 48 笔交易 fixture，启用实际回执紧凑转换和日志外置，包含三表压缩、验证、fsync 与 manifest 发布。当前版本仅把基准中的准备 worker 列表扩为 1/4/8，**生产默认仍为 4**。另通过只读 `git archive` 将旧版 `849a336b` 解包到新的 `/tmp` 目录，运行其原有 workers=4 基准；已经逐字核对，旧版和当前整个 fixture/test 文件除新增 worker 档位及对应注释外完全一致。两版均使用 Go 1.27.1、`GOMAXPROCS=10`，分别运行 `-benchtime=3x -count=3`，期间没有其他本地重测试。

| 真实 direct 三表流程 | 中位耗时 | 累计分配 B/op | allocation 次数/op |
| --- | ---: | ---: | ---: |
| 旧版 `849a336b`，4 workers | 246.398 ms | 927,021,640 | 1,237,311 |
| 当前 `f6f8a3c4`，1 worker | 287.429 ms | 848,966,821 | 878,724 |
| 当前 `f6f8a3c4`，4 workers | 227.432 ms | 889,004,930 | 881,332 |
| 当前 `f6f8a3c4`，8 workers | 221.410 ms | 889,008,840 | 881,351 |

保持 4 workers 比较两版，本轮流水线与回执复用组合的中位耗时下降 **7.70%**，累计分配下降 **4.10%**，allocation 次数下降 **28.77%**。这是同一真实 direct 路径的探索性组合比较，不能把前述组件的 33.83% 与 4.00% 相加。旧版三个样本为 235.791–271.928ms，当前 4 workers 为 221.290–237.182ms；样本少且顺序测量，仍不足以确定生产提升幅度。

当前 4→8 workers 的中位改善只有 **2.65%**，8 workers 的范围为 219.936–232.053ms，与 4 workers 重叠；不据此提高默认值。Mac 的 10 个逻辑 CPU 也不能代替服务器 16 核上的扩展性与同步争用验证。四种配置每轮三表输出都为 **19,133,768 B**，均通过 `MigrateV2` 自身的所有 frame 校验和首/中/末记录字节验证；该基准没有额外逐条查询全表，输出尺寸相同本身也不等于完成全库一致性审计。这组使用真实 compact/external 编码，不能与上一组保留 fixture 原文的 27,202,988 B 输出直接作压缩率对比。

Source 仍是内存 fixture，不包含 canonical Pebble 读取、同步导入竞争或热行删除/压实。原始日志：[当前 1/4/8 workers](../../build/benchmarks/20260909-cold-pipeline/direct-preparation-workers-1-4-8.txt)、[旧版 4 workers](../../build/benchmarks/20260909-cold-pipeline/direct-preparation-849a336b-workers-4.txt)；[解析汇总](../../build/benchmarks/20260909-cold-pipeline/direct-preparation-comparison-summary.json)与[archive/fixture 核对记录](../../build/benchmarks/20260909-cold-pipeline/direct-preparation-comparison-context.json)保留全部样本和范围限制。此处均为本地结果，线上情况另行核验。

部署前一次 30.11 秒 CPU profile 捕获正文冷化，共 202.54 CPU 秒（约 6.73 核）：V2 编码 49.56 CPU 秒，准备 34.09 CPU 秒，其中回执紧凑/外置转换 17.83 CPU 秒。回执属于准备的子集，不能重复相加；这一活跃窗口也不是同步整体平均 CPU。

另一个部署前五分钟连续基线为北京时间 12:23:09–12:28:09，21/21 点属于同一进程，实际跨度 300.009654 秒：head 增加 21,938 块（73.12/s），状态发布及删除增加 18,283 块（60.94/s），状态积压 631,851→635,506，增加 3,655 块；正文增加 65,536 块，索引仅增加 8,192 块，索引债务 8,060,928→8,118,272。正文最近一段耗时 51.38 秒，窗口中存在约 90 秒状态发布不变。

该基线继承了一次工作与恢复合计 300 秒的等待，窗内又出现一次新完成：工作 3.712881 秒、恢复 296.287119 秒。旧指标没有 completion reason，不能仅凭 admission healthy=1 判定完成时的具体压力原因。这一窗口与上一轮十分钟状态净追赶的结果不同，说明必须按持续净追赶评估，不能用单段提速断言积压已解决。逐 raw 的独立复核保存在 `cold-followup/baseline-window/independent-baseline.json`。

## 已执行验证

```sh
go test ./... -count=1 -timeout=300s
go test -race ./core/rawdb -run 'TestCompactExternalLogs' -count=1
go test ./core/rawdb -run '^$' -bench '^BenchmarkCompactExternalLogsReuse$' -benchmem -benchtime=1s -count=3
go test ./core/rawdb/freezer -run '^$' -bench '^BenchmarkV2PreparationThreeTablePipeline$' -benchmem -benchtime=3x -count=3
```

全仓通过。另有索引压力/失败/取消/release 的定向 race，以及两个 freezer 包 preparation/direct preparation 定向 race，通过。流水线覆盖真实 Source/Transform 重叠、顺序错误、借用缓冲、128 槽与超大行、字典互斥、writer 错误后 join、取消和并发 Close/Read。独立代码复核未发现阻断问题。

原始本地证据保存在忽略目录 `build/benchmarks/20260909-cold-followup/`；流水线基准与最后定向 race 保存在 `build/benchmarks/20260909-cold-pipeline/preparation-pipeline-*`。基准实现与正确性测试随代码提交，可重新执行。

## 服务器原生验证与部署

服务器仓库已拉取 `22972031e29abbba11bb97db029af93c026c52b6`。在 `/var/tmp/gtron-cold-followup-20260909/src-22972031` 中通过 git archive 隔离源码，复制现有 Sapling 静态库并记录其 SHA-256；没有改动仓库原有的 native library 和 solc 内容。测试使用服务器 Go、`GOMAXPROCS=4`、`CGO_ENABLED=1` 及已有 Sapling 参数。

```sh
/data/go/bin/go test -p 2 -tags sapling ./core/freezer ./core/rawdb ./core/rawdb/freezer ./core/rawdb/etl ./core/maintenance ./core/state/snapshots ./core/state/pruning ./cmd/gtron -count=1 -timeout=300s
/data/go/bin/go build -p 4 -tags sapling -o /var/tmp/gtron-cold-followup-20260909/gtron-22972031 ./cmd/gtron
```

八个包全部通过，测试和构建退出码均为 0。二进制 SHA-256 为 `457f01bfa484b3a72e0bf08d76b034e0788b2e582fce1830cde6ddaf5dd6e92b`。原始源码身份、日志、退出码和构建哈希均保存在服务器 `/var/tmp/gtron-cold-followup-20260909/`；本地状态记录明确标注服务器终端转录来源。

已保存六个区块及六个回执的切换前 RPC 内容，采样时正文前沿 13,959,168、索引前沿 7,626,752、solid 14,755,112。用户对本次正常停机、更新和启动作出明确批准后完成切换：旧 PID 2048 的 `Result=success`、`ExecMainStatus=0`，新 PID 2395 运行 `/data/gtron/releases/20260909-cold-followup-22972031/gtron`，进程开始时间标识 `1788935080960436932`（北京时间 14:24:40.960436932）。构建文件、发布文件与实际进程文件的 SHA-256 一致；`deploy.exit=0`。磁盘守卫通过，既有全局维护 hold 保留，部署前监听及 unit 备份保存于服务器部署目录。

新进程启动后，2043/2044/2045 高度的既有账户余额和区块哈希复验通过（一个账户标量余额样本，不代表全库）。跨部署的六个区块、六个回执，以及新版本同进程热转冷的另一组六个区块、六个回执，在正文及索引裁剪前沿越过样本范围后逐项复验一致；两组有重叠区块，不能合称十二个互不相同的区块。

## 完整线上窗口

北京时间 14:26:46.305–14:36:46.350，41/41 点均属于新进程，采样单调时钟跨度 599.970727 秒。独立逐 raw 核算与 observations、summary 一致，没有前沿/累计计数回退或压力指标组合异常。

| 指标 | 窗口增量 | 速率/结果 |
| --- | ---: | --- |
| 同步 head | +60,246 块 | 100.415 块/秒，末高度 14,849,087 |
| 状态发布、热层裁剪 | +10,033 块 | 16.722 块/秒，末高度 14,026,330 |
| 状态真实积压 head−published | +50,213 块 | 772,544→822,757，仍在扩大 |
| 正文冷化 | +65,536 块 | 完成一整段，耗时 82.775 秒 |
| 交易索引发布、裁剪 | +253,952 块 | 31 叶，423.274 块/秒；裁剪交易行 +21,675,420 |
| 索引积压 body−index_pruned | −188,416 块 | 6,283,264→6,094,848，正在净追赶 |

索引累计完整批次工作为 79.284 秒，占窗口墙钟 13.215%，不能当作 CPU 或共享 gate 占比。可见健康完成恢复为实际工作时间的 4 倍，约 6.410–21.542 秒；保守五分钟截止及错误截止全窗为 0。

准入软压力恢复得到线上证据：debt growth 在 14:30:27.234 被观察到，14:30:46.580 为 recovering，14:31:00.234 转为 healthy；两个引擎样本相隔 13.654 秒，设备样本也更新。下一批尝试按完成时间减完整工作时长推算在软准入约 33 秒后开始，并完成发布和裁剪。另一条准入恢复约 20.915 秒。本窗可见完成记录全部 healthy，**没有捕获完成阶段 soft pressure**，因此不能宣称该分支也已在线验证。

正文最初未推进有发布前提：下一段需要事件日志连续、验证覆盖至 14,024,703；状态/事件覆盖到达整段要求后才申请正文 gate。样本25状态 14,024,522 尚未满足，样本26才跨过该界限，随后正文成功发布。本窗正文资源拒绝为 0，没有正文 gate 饥饿证据。正文执行期间状态发布出现约 105 秒可见停顿；此外状态只完成 44 个强制批次，平均约 228 块/批，并有三次冷段合并。不能将全部状态低吞吐唯一归因于索引或正文。

相关 cold/prune/body/index errors、lifecycle failures/recoveries、checksum failed 全窗无新增。部署后从日志 offset 12,410,714 到 12,473,031 的 62,317 字节扫描未截断，共 154 行，没有 ERROR/CRIT/panic/fatal 行；这不是对所有其他诊断计数的零错误声明。

独立资源窗口为 14:26:29.883–14:36:29.895，41点，600.012442秒，同 PID/start_ticks，错误及警告为空。平均 CPU **4.49/16 核**，采样 RSS 最大 **13.85 GiB**；整个共享盘 busy **81.75%**、await **1.123ms**、平均队列 **3.071**，读/写约 **121.20/102.33 MiB/s**。这些磁盘数据包含 MySQL 等负载。共享文件系统空闲减少 535,236,608 字节，不能直接作为 gtron 净空间变化。

14:37:55 在线非原子 `du`：

| 目录 | 字节 | GiB | 三目录占比 |
| --- | ---: | ---: | ---: |
| chaindata | 89,324,605,440 | 83.19 | 26.31% |
| state-snapshots | 162,813,313,024 | 151.63 | 47.96% |
| ancient | 87,315,009,536 | 81.32 | 25.72% |
| 合计 | 339,452,928,000 | 316.14 | 100% |

资源与大小由服务器脚本校验后，经终端 UI 转录到本地；完整资源 raw JSONL 和原始 du/日志保存在服务器，本地独立复核只核对转录算术、单位与证据边界，没有假称重新计算远端原始 CPU/I/O 计数。指标和查询证据则有完整本地原始响应。

## 下一处已确认的成本

本窗 manifest cache hits 和 resident bytes 始终为 0，misses 与 bypasses 各增加 768。源码中禁用缓存或原始文件预拦只增加 bypass，而完整解码后的容量拒绝同时增加 miss/bypass；这些计数和调用链指向 64MiB 缓存预算拒绝，并不表示 manifest 文件本身超过 64MiB。

进一步对运行清单的独立副本，用服务器原生 Go 和当前代码的完整计费公式核算：generation 12,570，JSON 16,877,031 字节，active 25,888 条、retired 35,532 条；解码切片 capacity 分别 27,569 和 43,559，SegmentRef 为 104 字节。计费为 **67,919,852 字节（约 64.77MiB）**，超过 **67,108,864 字节（64MiB）** 预算 **810,988 字节**。这直接确认了缓存容量拒绝，原始文件及运行数据库均未修改。64.77MiB 是保守计费值，包含倍率、切片剩余容量和重复计入的不可变字符串，不能等同实际进程常驻内存。原始诊断保存在服务器部署目录的 `manifest-charge-result.log`，本地有明确标注 UI 转录来源的 `manifest-cache-diagnosis.json`。

在完整窗口结束、查询复验完成后另采集 30.12 秒 CPU profile（14:39:21 开始，共 130.97 CPU 秒）。限定 `SnapshotLifecycle.OnePass` 调用链后，生命周期累计 7.68 CPU 秒，其中 `loadManifestFile` 为 6.24 秒，约占该生命周期 CPU 样本的 81%；其内 decode 及 JSON 子调用互相包含，不能相加，也不是整个节点 81% 的 CPU。这确认清单反复解析是当前状态生命周期的重要成本，缓存预算及大清单处理应作为后续优化重点。

本轮保持完整历史与现有数据格式，没有通过删历史减少容量。不同窗口处在不同高度、交易密度及维护阶段，不能把线上速率比宣传为全节点加速倍数。固定盘能否容纳完整主网、20M 以上的实际竞争及所有积压能否持续清零，仍未完成验证。

本地完整核验产物：`build/benchmarks/20260909-cold-followup/final-window/independent-final-review.{json,md}`、`independent-pressure.{json,md}`、`final-resources-summary.json`、`final-db-size.json`、两组 `fresh-cold-verification.json` 和 `genesis-canary-verification.log`。本轮部署状态见 `deployment-verification.json`。
