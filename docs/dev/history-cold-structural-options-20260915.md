# 冷历史吞吐的结构改造候选

2026-09-15，结构设计评估。本文生产调度/新格式候选均未开启；随后获批实现了下面所述
离线并行诊断，并通过本地正确性和 race 验证，尚无该原型的原生性能结论。
现有 owned-Get 修复和原生固定物理范围 replay 由本轮主任务独立验收；本文不提前引用其
线上收益。约束沿用 [根因测量方案](../superpowers/specs/2026-09-15-cold-throughput-root-cause.md)、
[shared 热历史格式](../superpowers/specs/2026-09-13-shared-hot-history.md) 和
[历史完整性设计](../superpowers/specs/2026-09-07-fresh-history-format.md) 的历史查询契约。
最后一份旧设计有其当时“新 datadir”范围，不能直接当作本文允许现网混用新格式的依据。

离线原型 `benchmark-history-parallel` 只执行同一份 16 块输入的多个完整 trio 构建，
支持 (workers,segments)=(1,1)/(1,2)/(2,2)/(1,4)/(2,4)/(4,4)。源物理验证、每段 hot/cold
全文与 companion 验证均在计时外；**没有实现本文的合并 manifest 发布或生产并行调度**。
共同 pinned view 只由私有只读 Pebble opener 创建，所有 worker join 后释放。ETL 合计
配置阈值为 64 MiB，其他内存另计；详细协议已补入根因 spec。
本地 Go1.25.5/GOMAXPROCS=2：新 parallel 与旧 cold/range 定向回归 PASS 6.192 秒，
新 parallel focused race PASS 6.128 秒；原始命令/退出码/输出在
`build/benchmarks/20260915-root-cause/parallel-local/`。这些测试不提供线上吞吐收益证据。

最值得先做完整回放验证的是**一个维护任务内的有界多段构建与一次 manifest 发布**。
它保留现有文件格式，直接检验 CPU 并行度和每小批全清单工作的影响。更大的算法改造是
**自包含的引用型冷容器**，让已认证的大 Prev 不再经过第二遍热读取、完整冷记录复制和
CDC 重分块。后者有更大的减少工作空间，也有明确的 reader、恢复和回滚成本。
单遍展开值 spool 是可比较的中间实验，但不能预设它会胜过第二次热读取。

## 已有证据和目标差距

证据目录为 `build/benchmarks/20260915-cold-backlog/` 与
`build/benchmarks/20260915-root-cause/`；以下是已有观测，不是这些候选的后测。

- 第二个完整 13 点窗口，同一进程、359.963638 秒：导入 20.442587 blocks/s，冷发布覆盖
  2.661380 blocks/s；eligible lag 995,083→1,000,941，12 个相邻间隔均增加。
  head−cold gap 是另一口径，增量 6,359，不能替代 eligible lag 的 5,858。
- 当时实际 batch 的 rolling gauge 中位数 25 块、范围 13–46；block limit 中位数 27、
  范围 16–37。metadata 中位数 1.596198 秒，非 metadata density work 3.446407 秒。
  这些 gauge 可能来自不同 pass，不能将各中位数相加、相除后声称得到实测性能分解。
- duty 80%–90%，hard=0 的 13 个观测点；rate-limit defer +40，resource/sync defer
  无增量。恢复观察继续接受资源样本，不能把增长再次归因于已经修复的观察饥饿。
  同窗口还有派生 state history index 两次完成、累计阶段墙钟 73.934527 秒，冷压实
  三次；不是只有 base builder 在串行维护链上运行。
- 此窗口没有新的 CPU profile。之前 02:00:18 UTC 的 45.17 秒 profile 共 157.14 CPU 秒，
  cold builder 根 17.41，shared decoder 13.40，reader SHA 10.67（pack 5.45、chunk 5.22）。
  那次 profile 的 shared 子树归于 cold；其他时段不能照搬归属或用样本零推断阶段不存在。
- 原生硬件观测：16 个逻辑 CPU、8 核/2 线程，40 GiB cgroup 上限、无 CPU quota。
  03:08:39 UTC 的独立约 10 秒资源窗口：系统 idle 53.15%、iowait 10.11%，gtron
  约 3.547 核；共享数据设备读 158.219 MB/s、写 146.675 MB/s、busy 86.43%、平均队列
  5.259。它证明当时有 CPU 空闲，也表明 I/O 已很活跃；不证明 EBS 的持续限额或线性扩展。
  gtron、Java、MySQL 等共享主机，不能将 16 个逻辑 CPU 都视为冷构建可用额度。

从 2.661 到 20/25 blocks/s 分别需要约 7.5/9.4 倍的**完整服务速率**。
现有合成 owned-Get 实验只确定减少分配；它以及某个 reader 微基准都没有证明这一差距已消失。
绝不能把 B/op 当成 RSS，把冷输出计数当成 chaindata 物理回收，或把减少 CPU 工作当成
同步一定追平。更快冷覆盖有助于热裁剪，但 shared 整桶证明、guard、flush 和 LSM 压实仍须完成。

## 现有流程究竟重复了什么

`history_stream_build.go` 的 `buildStateDomainChangeHistoryBinarySegmentsFromDBRangeContext`
及显式格式入口持有一个 `AcquireStateHistoryReadView`，执行以下步骤：

1. 第一遍 `iterateStateDomainChangeHistoryChanges` 读取并认证完整热 history，只把 key
   交给 `stateDomainChangeV6Build.CollectKey`。
2. `FinishDictionaryContext` 排序、去重并完成 key 字典；建立 StateTxRange 表。
3. 第二遍相同固定视图，`writeChangeWithV6KeyID` 查询 keyID、收集 posting，并用
   `appendStateDomainChangeBinaryRecordFrameV6` / `putStateDomainChangeRecordV6` 物化完整 Prev。
4. `cdcStreamWriter.Write/flushChunk` 对冷 V6 逻辑字节重新切块、摘要、逐字节去重和压缩，
   完成 history/index/accessor，随后 `validateBuiltStateDomainChangeBinaryFiles` 自检。
5. 外层 `Runner.onePassWithPressureContext` 调用 `Aggregator.integrateWithManifest`，
   构建全 manifest、验证、JSON、原子文件发布；维护生命周期随后做证明、热裁剪及其他工作。

V6 字典仅去重 key，完整记录头为 21 B 加 Prev；热 shared chunk 引用没有传递到冷层。
逻辑记录的 RLP 头和冷 V6 头不同，不能把一个热 chunk 摘要直接用作原有冷 CDC chunk 摘要。
已有完整 trio 基准覆盖步骤 1–4，**不含步骤 5 与整个维护链**。

`decodeStateHistorySharedPack` 验证 envelope、块身份、引用表、每 chunk 和整 pack。
现有 v3 整包承诺是 `SHA256(完整原始 RLP)`，不能由若干 chunk SHA 合成。因此，即使新冷
表示只存一次 unique chunks，处理已有积压仍至少要按每个源 pack 的逻辑字节完成一次 SHA。
引用保存可去掉重复工作，不能把已有 v3 的认证复杂度变成仅与引用数和 unique bytes 成正比。
未来支持组合认证的热格式属于另一个 writer/reader 升级，不能倒推免验现有数据。

## 路径一：有界多段构建、合并发布

### 小批量的来源

`cold_builder.go` 默认 `BatchBlocks=5000`、`BatchTxNums=390625`，后者是 state tx sequencing
positions，不等于 Wallet 交易数。当前 16/25 块不是这两个静态值直接设成了几十块。

`history_load.go:historyLoadState.batchLimit` 使用上次成功批次：

`ratio = min(targetWork / previousNonMetadataWork, targetOutputBytes / previousRefBytes, 1.25)`。

level 2 的目标为 4 秒/64 MiB，level 3 为 8 秒/128 MiB；同一比例分别缩放上次 block 和
txnum 数量，然后在完整 block 边界应用两个上限。未知或 hard 样本不得增长，失败批次另有
减半上限；小整数恢复有明确的一单位例外。已有 metadata 剔除修复仍在，不能再次声称
“首次把固定清单成本从 density 去掉”。完整维护成本仍被 `applyThroughputRecovery` 计费。

这些输出字节目标**不是**解码内存或 CPU 资源的硬上限。一个合法大块可以超出通常 txnum
目标，不能截断或跳过。现有采样没有每次计算用的 `historyWorkSample.bytes`，也没有完整
逐次决策配对；故目前只能确定自适应机制把范围缩小，不能确定每次究竟是时间、输出 bytes、
txnum、最近失败还是前一批增长限制绑定。batch gauge 与当前 limit 偶尔大小不一致可由
更新时间和完整块边界解释，不应据此认定算法越界。

全 Go 源中未发现名为 `canPublish` 或 `stepBudget` 的 base-builder 开关。
`compactor_budget.go:selectBudgetedHistoryCompactionLeaves` 的 `MaxSteps/MaxSources` 是
已经发布叶段的压实选择预算，不是几十块 base batch 的来源；不得通过修改它“扩大构建批次”。
当前发布门槛实际散布在 solidified−HistoryWindow、verified Finish、连续 tx range、
资源/空间/恢复/lease 准入、builder 认证、取消检查、manifest 验证和 hash-bound stage 写入中。

### 可实施的最小形态

先增加**离线 coordinator 原型**，比较固定相同总范围的 1/2/3/4 workers。每个 worker
仍产出原有完整 trio；不启动 N 个 Runner，也不并行更新 manifest 或 pruner。

1. 一次准入和一个共同 pinned view，在该视图上预先确定有限数量、互不重叠、按块/txnum
   连续的子范围；完整检查范围/块 hash/tx table，计划不能越过同一 eligible/Finish 边界。
   读视图 pinned 标记本身不承诺任意实现并发安全：第一版只允许已审计的 Pebble snapshot，
   各 worker 自己的 iterator；借来的未知 view 走串行。最后一个 worker 退出后才能 Close。
2. 每个 worker 的 ETL、字典、scratch、writer 和临时路径独立，仍做所有 chunk/pack SHA、
   行顺序检查、companion binding 与文件自检。相同热桶可以并发读，但不新增跨 worker
   的“见过 hash 所以可信”缓存，也不修改全进程压缩环境来调 worker。
3. coordinator 等待所有已启动工作退出。取消/任一错误不发布这一组，错误按计划范围顺序
   汇总而非按完成快慢选择；现有单段错误保留。合法异常/修复行、unordered fallback 的
   能力边界必须与原入口一致，不另选弱校验 reader。
4. 全部成功后，核对边界和输入归属，按 tx 顺序合并 refs，仍用一次完整
   `integrateWithManifest` → `PublishManifest`。所有候选文件先完成耐久化，随后才原子
   替换清单，最后按整个连续前缀写 stage。不能先把最大完成 block 当作已覆盖。
   计划使用的生产 manifest 必须保持单一发布者串行所有权；若将来允许其他发布者，须在
   发布前重读/合并或拒绝过期 generation，不能仅依赖一个旧副本覆盖别人的新 refs。
5. `PassResult` 的范围、batch 数和 Segments 代表整组；`RecordTrustedSnapshotSegments`
   接收全部完成 refs。现有生命周期本来遍历 `result.Segments`，但仍须检查单一 `Segment`
   字段的所有调用方。随后只执行一次正常 pruner/维护收尾，保留 coverage、canonical、
   Finish、settled-prefix、guard 和 committed flush；新“批量发布”不是新的删除证明。
6. 失败时只删除本任务独占且未公布的临时文件。现有 builder 可能已生成 content-addressed
   final 文件，它也可能被旧 manifest 引用；不能见到任务失败就随手 unlink。沿既有 orphan
   清理政策处理。crash 在清单后、stage 前的情况继续用 `reconcileSnapshotBuildStageBlock`
   从连续清单修复，必须补整组故障测试。

这可以复用 `history_event_parallel.go:buildHistoryEventFiles` 的“输出文件先完成、父任务
统一发布、取消时 join”模式，但现有模式仅明确允许 history+event 两个 builder。
它不自动授权新增四个 history 加四个 event，更不能把别人的 lease 当成自己的并行额度。

### 必须同时设计的资源计费

同样总范围拆成多个子段，有机会并行但**不会减少这一组清单发布次数**；要摊薄原来每 25 块
一次清单，需要把多个旧批次组成一个更大的总范围，这就是显式调度策略变化，不能藏在
worker 参数里绕开目前 work/byte 目标。先验证固定总范围的安全并行，再比较受限总量的分组。

每组必须有统一 CPU tokens、逻辑解码 bytes、输出/temp bytes、最大并行请求和内存预算。
现有资源组成至少包括：每次 shared pack 最多 128 MiB 输出，活跃 chunk/旧值 frame；
V6 key/posting 两个 ETL（默认各 64 MiB buffer，另有条目/排序元数据、spill reader）；
最高 512 MiB key table；最多 16 MiB key 去重 payload 加 map；每 writer 最多 64 MiB CDC
去重 payload 加索引；压缩 in-flight buffers 与稀疏表。它们不是同时必然达到上限，
也不能把这些常量简单相加称为已经证明的严格 RSS 上界。实际 capacity、allocator、Go pools、
解码器共享资源及 snapshot 保留的旧 SST 都要计入。

单 builder 自身已有最多四个压缩 workers；盲目四组外层并行可能产生十六个内部 worker。
应显式传每组压缩并行度并共用 CPU 额度，支持先 2 组、每组较少压缩 worker 的试验。
pressure hard/debt/device freshness/空间判断保持，硬压力禁止启动下一组；已入场调用的
非抢占部分诚实等待退出，不能承诺取消马上停止 I/O。

`CompleteHistoryMaintenance` 仍对一个 maintenanceID 完成一次、deadline 只延长。
并行任务只按墙钟计算 recovery 会改变单位时间内的 CPU/I/O 投入；须用整组资源上限约束
它，不能让每 worker 独享 80%–90% duty。调度的 total-work 与单段 density 也须分开记录，
否则把 N 段和 `max(worker time)` 当成单段密度会反馈出意外的下一轮增量。

### 收益判断与风险

可减少多个旧批次重复执行全 manifest validate/sort/JSON/fsync、prune 入口固定 metadata，
并利用当时空闲 CPU。`UpdateHotPruneProgress` 和其他维护发布还会重写全清单；合并 builder
发布不等于自动省掉所有外层成本。更多小段还可能增加 active ref 数和跨段重复 chunk，
使今后的清单/compaction 成本变大；因此要与“同总范围一个大段、一个 worker”同时比较。

理论模型可写成 `总块数 / (并行构建墙钟 + 发布 + 维护 + recovery)`，不能将单段 bps
直接乘 worker 数。即使某个真实单段 replay 测得约 5 bps，三四组也只有忽略全部共享争用和
额外工作时的 15–20 bps 算术值，尚未达到 25，更不是线上保证。这个方向是**最容易先用原格式
证伪或证实的吞吐方案**，而不是已经证明足够的方案。

## 路径二：单遍认证，展开值 spool，仍写旧 V6

第一次源扫描保留正常读序和完整认证，同时把规范化行写到私有、可校验、有界 spool，收集
key 字典和 tx 表。完成字典后从 spool 重放，交给现有 V6 record writer；源不再扫描第二遍。
`stateDomainChangeHistoryRecordETLSortKey` 的 txNum/Seq/块与 key 身份、稳定 ordinal 必须保留，
重复记录不能被 ETL 的相同 key 合并。PrevExists 与存在空值、generation、同 tx 的顺序不可丢。

需要新增独占文件格式的长度/记录数/总 bytes/摘要、EOF 校验、取消和所有失败 cleanup。
普通小值可先用硬限额内存 spool，超限明确落盘或原路径回退；已开始消费后不能在不重新
证明输入的情况下混合两条输出。合法大块须明确资源不足，不能偷偷截断。
spool 是本次 builder 私有对象，不跨 view、重启或 manifest 复用，不成为新公共缓存契约。

主要代价是把重复大 Prev 展开后写盘再读盘，可能远多于原 shared 物理输入。Snappy/shared
复用高时尤其不利。重新压缩 spool 又引入一次编码/解码。它仍保留最终完整 V6 展开、CDC
扫描/摘要/压缩、companions 和外围发布。只删除两次等价源扫描之一的理想上限也不超过这部分
成本的一半；没有理由凭此承诺从 2.66 变成 20–25 bps。应纳入完整流程，直接比较总 I/O 和
实际耗时，不用源扫描的 SHA 下降替代总收益。

## 路径三：自包含的引用型冷容器

这是新格式设计，不是复用现有 CDC magic 后改变内容。第一版可以保持**虚拟的 V6 逻辑流**：
header、字典绑定、tx range、21B record header 仍可按原逻辑 offset 读取；物理文件保存小行
元数据、Prev 的 span 描述和一份 unique encoded chunks。小值/legacy 源可 inline。

构建的一次完整源认证同时收集 key、行描述和完整已认证的 chunk bytes，unique chunk 在
本段 private arena 只写一次。完成字典后生成 keyID、posting 和逻辑 offsets，不再展开 Prev
写进旧 CDC。解析器必须提供经验证的 RLP Prev byte spans；不能用猜测字段长度、unsafe
地址差或“同 hash”来推导有效偏移。一个 Prev 可跨 chunk，一个 chunk 也可含多个字段和
其他记录。复用整个 chunk 可能保留不需要的邻接 bytes，需测真实输出大小。

必需的验证边界：

- 仍验证每个源 pack 的完整逻辑 SHA、全部字段和块绑定；候选去重先确认字节身份。
  chunk 的 raw/encoded 长度、codec、digest 与引用范围须一致；不允许引用链和越界相加。
- 新文件独立含所有依赖，不能存 hot Pebble key 当成长期可用的地址；严格限制逻辑总长、
  最大 record、span 数、目录规模、解码输出、缓存及随机读放大。
- 文件完整 SHA 覆盖物理目录、行描述和 chunks，三件套依旧原子加入清单。新 reader
  dispatch、验证器和 compactor 都识别新容器；不能只让正常 query 打开、而 prune verifier
  把新容器当成原始 V6 或跳过它。
- `openHistorySegmentForReadWithCacheLimit` 的新 adapter 可以暴露 `ReadAt` 和逻辑长度，
  让 V6 key/V7 posting 的逻辑 offsets 继续适用；这只是可行接口方向，尚未证明当前所有
  快路径只依赖该接口。顺序 reader、record-specific 快路径、compaction logical-size
  预算和 whole-file/export validator 均须审计。小范围读取只解码必要 chunks，不能退化成
  从段头重建所有大值；全量读取必须有单独有界复用策略。

历史语义由 `core/state/history.go` 和 `history_segment.go` 的 point/as-of/prefix/range/
restore 保持：取 T 之后第一次变更的完整 Prev，无后续时用同视图 latest；保留 generation、
presence、块/tx 绑定和重复顺序。回调所有权不能别名随后复用的 chunk buffer。
当前冷格式已经移除 transient Next、重建稳定 Seq，等价性应比较规范化 archival rows，
不能借机再删逻辑历史。新旧格式混合边界、unwind、restore 都要逐行 oracle。

### 生命周期、旧文件和回滚

现有 `Manifest.SegmentRef` 只有路径/范围/大小/摘要等，没有依赖 DAG。
`inspectRetiredSegmentFiles` 保护 active 与已发布 immutable manifest 中直接出现的路径。
**将 chunks 内嵌在新 `.seg`** 可以继续以单文件生命周期处理；把它们另存为隐式 side 文件
会造成 GC 漏掉依赖，不能这样发布。

hot GC 的 `history_shared_chunk_gc.go` / `state_changeset_shared_gc.go` 在 cold proof 完整、
guard 内 flush 后新视图确认整个桶无 hot rows 时，会删除该桶 chunk 范围并写永久 retired
marker。新 cold 必须完全不依赖它，才能保留该证明与回收逻辑。关闭一个旧 pinned view 前
Pebble 可能保留旧版本，不能把 MVCC 保留误认为 cold 永久拥有 chunks。

新 reader 先作为 bridge 部署，默认只写旧格式；再启用新 writer。旧已有 V6/CDCV1–V3
仍可读，混合段 compaction 明确读取双方、输出选定格式并完整认证。旧二进制不认识新容器，
新清单一旦启用并允许删除原 hot 历史，就不能直接回滚旧 binary。可回滚到保留新 reader 的
bridge；彻底退回旧格式需要先把所有依赖新容器的历史重写成旧 V6，留足双份空间、验证、
原子改清单再解除新格式使用。运维兼容 marker 和启动检查须随 writer 开启设计，不能声称
旧程序天然会安全拒启。新格式发布失败不得推进任何 hot prune 权威水位。

此方案减少的工作比普通 spool 多，且唯一 chunks 可保持现有 Snappy/raw 编码而免去对
重复展开值重新 zstd。但“独特 chunks 占比”“完整 pack SHA 的剩余下限”“小行/索引/全清单
成本”决定实际收益；仍需真实范围全构建和查询测量。它是**有机会显著降低算法工作量**的
方向，尚无至少 20–25 bps 的证据。

## 不宜作为第一版：跨冷段共享 chunk store

跨段只保存一次 chunks，比段内 arena 多一层空间收益，但它改变全部依赖生命周期：
manifest 必须列出 chunk pack 身份及引用闭包，catalog 签名/下载/恢复集包含所有依赖，
GC 遍历所有 active 和 retained/published generations，不能只看当前段或可丢失 refcount。
禁止引用环和引用链；依赖索引须可从权威 immutable 文件重建。

已发布且有 SHA 的 chunk pack 不能继续 append。按 1024-block 桶等封存会延迟 cold frontier；
更小 immutable chunk packs 能提前发布，但增加目录、共享边界和转存/重复选择复杂度。
compaction 还要避免把一个 small live span 拖住整大 pack，且不能清理仍由旧 manifest/reader
使用的依赖。它不是简单新增一个 hash→offset map 的改动。先用自包含容器验证减少工作和
空间分布，再考虑此层；目前复杂度和故障面高于有证据支持的近期收益。

## 同输入验收与继续实施顺序

1. 使用主任务导出的真实物理范围与确定 manifest SHA，保留原高度、raw/Snappy 编码、
   refs、所有 repair 行和原 unique/逻辑大小分布。验证源完整，基线与候选同 binary 或冻结
   实现，交替顺序多轮，明确 page-cache 条件，避免递减热身曲线被当成优化。
2. 对多段 coordinator 同时测：固定总范围串行多个原段、并行相同分段、串行一个大段；
   然后测多个旧批次合成一组。记录完整 trio、publication、maintenance 的各自墙钟和完整
   端到端耗时。固定真实清单副本放在私有目录并复制其合法段/验证输入，不能伪造 refs 让
   verification 不工作，亦不能使用会被 live GC 删除的生产文件作为离线基准依赖。
3. 总量包括 block/tx positions/records、source logical/physical/unique bytes、所有 SHA
   和解码工作、CDC input/stored、临时和最终读写 bytes、实际 CPU、B/op/peak heap/RSS、
   设备 latency/queue。并行 1/2/3/4 只在全局资源额度允许时比较；每 worker/总组单独计数。
4. 逐行比对所有 archival 字段与重复数量，再测 point/as-of/prefix/restore。损坏、缺 chunk、
   正确大小错误 bytes、overflow、重复/乱序、取消、临时失败、worker 乱序结束、发布各故障
   点和 manifest/stage crash 恢复必须有确定性测试；pinned 生命周期和共享 writer 跑 race。
5. 离线达到有意义改善后才能进入默认关闭的 online 试验，连续完整窗口比较 cold service
   rate 与 eligible arrival、GC 与热裁剪、设备/内存压力和执行延迟。20–25 bps 是待验证
   目标；若只到 10–15，也应如实保留剩余差距并决定继续格式改造，不能称为根治。

推荐先落地步骤 1–3 中**原格式并行与分组发布原型**；它最直接区分剩余串行调度/固定清单
成本和硬件限制。并行若受 I/O 限制或收益不足，优先原型化自包含引用冷容器；展开值 spool
作为同输入的低复杂度对照，只有全流程确实赢才保留。上述候选均不靠缩短历史保留、削弱
认证、隐藏积压或无限提高准入预算来成立。
