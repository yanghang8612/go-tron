# 历史转换效率审计与新数据目录方案

2026-09-07。范围：只读审计本地源码、既有设计和已归档维护结果；本报告不启动同步，不修改生产数据。用户要求保留全部历史状态查询，并允许为新格式从创世重新构建，因此下文的新数据目录方案不要求旧格式兼容。现有数据库的离线维护仍沿用已经验证的发布、比对、删除和持久化顺序。

当前值得优化的是历史大 value 进入 LSM 后再读出、排序、重编码、验证及删除产生的处理成本。不能把“转换更快”当成“占盘更少”：本轮新建 32 块 cold 后，热库减少 49,815,552 B，快照增加 49,733,632 B，合计只省 81,920 B。索引、value 的新压缩实验必须另报完整 trio 的净大小、临时峰值和查询代价。

## 1. 实测能说明什么

数据源为 [离线恢复记录](mainnet-offline-recovery-2026-09-07.md:97)。这些数字是已经完成的维护记录，非本次重新运行的基准。

| 操作 | 输入计数 | 已报告用时 | 可支持的结论 |
|---|---|---:|---|
| 已冷覆盖范围的剩余 2,285 块 prune-only | 1,599,310 records；InputBytes 1,306,811,975 B | 92.681 s | 包含内部重新规划、完整覆盖验证、冷热逐条比对、删除和水位发布；不能称为纯解码吞吐 |
| 新建 32 块 cold 并验证、比对、逻辑删除 | 21,747 records；InputBytes 52,718,292 B | 10.244 s | 此区间完整流程成功；不能分解出 Zstd、ETL、SHA 各自比例 |
| 前一项之后另行物理压实 | 新 trio txNum 1,734,454,193–1,734,459,458 | 独立步骤 | 热冷实际分配合计净省 80 KiB，不能用 52.7 MB 输入当作节省量 |

`InputBytes` 是完整 V1 change 编码大小的保守计数，包含现代 hot/cold 已省略的字段；不是 SST 物理读取量，也不是当前 V6 解压后的精确长度。维护 `ElapsedSeconds` 从内部 pass 开始计时；CLI 之前的只读计划、RW 重开和之后的检查，以及独立执行的物理 compaction，不都包含在里面。[计数定义](../../core/state/pruning/offline_history_input.go:17)、[计时入口](../../core/state/pruning/offline_history.go:331)、[CLI 顺序](../../cmd/gtron/db_offline_history.go:162)。

这两个范围的平均每条输入大小差异很大，且一个复用约 1.1 GB 的完整旧 trio，一个新建约 49.7 MB 的小 trio。不能横向比较 92.681 s 和 10.244 s 得出规模线性关系，也不能据此预测全链完成时间。下一轮需要同一固定输入的阶段计时与实际读写字节。

## 2. 当前离线路径的实际 passes

下表按现代、有序 block pack、V6 history、V7 accessor/index 说明。一个逻辑 pass 不等于一次设备读：页缓存可能命中，压缩 reader 的缓存互相驱逐也可能重复读/解压。

| 阶段 | hot / cold / scratch 上的实际工作 | 依据 |
|---|---|---|
| CLI RO 计划 | 遍历 StateTxRange；对每个完整 block 解码 history，计 N、key/value 字节、顺序与预算；复用 case 打开完整覆盖 trio 的 header 计算验证预算 | `cmd/gtron/db_offline_history.go:162`；`pruning/offline_history.go:235,267,300` |
| RW 后重新计划 | 再执行同样的 hot 完整扫描，并重查 manifest、canonical 边界，防止 RO→RW 间输入变化 | `pruning/offline_history.go:340` |
| build key pass，仅新建 | 再读 hot records，向 key ETL 写 logical key；排序、去重后写临时字典，再读字典生成摘要及 keyID 查询表 | `snapshots/history_stream_build.go:85`；`history_key_oriented_v6_build.go:215,254` |
| build record pass，仅新建 | 再读 hot records；按 tx/seq 顺序直接写 history 和 tx index，同时收集 keyID/ordinal 排序的 posting ETL；不把大 Prev 再放进 record ETL | `history_stream_build.go:108,132` |
| sidecar 收尾，仅新建 | 临时固定宽 tx index 读回并改写 V7；posting ETL 排序后生成 posting/meta，读取字典和 meta 生成 keyData，再把 keyData/posting 复制组装到最终 accessor | `history_stream_build.go:206,228`；`history_key_oriented_v6_build.go:633,646,663,771` |
| builder 自检，仅新建 | history 使用 writer 事实绑定的 trusted reopen 和抽样 payload 检查；当前 V7 accessor 仍完整遍历结构 | `history_stream_build.go:233,247,285` |
| 独立删除验证门 | 完整 trio 物理 SHA；校验范围表；按 index 顺序解码每个 history record；收集并外排 posting tuple，再与完整 accessor 逐 tuple 比对 | `history_binary.go:1495,1642,1709,1739` |
| 冷热比对 | 重新读取计划内 hot；按 tx index 读取 cold 对应范围，逐条精确比较 owner/domain/key/PrevExists/Prev 等，允许重试时 cold 包含已经删掉的旧 hot 块 | `pruning/offline_history_compare.go:23,85,103` |
| 删除和水位 | 一次连续 changeset key 扫描；已被 StateHistoryIndex 水位覆盖时不再解码大 value，直接删 block pack；flush→WAL Sync→manifest hot-prune cursor→stage batch→Sync | `rawdb/accessors_state_changeset.go:1146,1229`；`pruning/offline_history.go:413` |
| 物理 compaction | 独立维护命令证明选定逻辑范围为空，再在空间 guard 下请求 Pebble compaction；可能重写相邻存活数据，不能预先保证其输出或释放量 | `core/rawdb/pebbledb/maintenance.go` |

因此，CLI `--yes` 的**新建有序 case 至少有 5 次完整 hot record 解码**：RO plan、RW plan、key collect、record write、cold/hot compare；**prune-only 至少 3 次**：RO plan、RW plan、compare。内部 pass 的计时分别包含其中 4 次/2 次。独立人工先执行的 plan 命令再另加一次。budget 达到上限时，计划还可能读取下一个不能纳入的完整块。

此计数不包括低于索引水位之外的 fallback 删除解码、legacy 无序输入的 record ETL 重试，也不包括 Pebble WAL 恢复和 LSM 内部 compaction。当前维护 scanner 拒绝无序 legacy pack，不应为了性能将其改为静默回退。

复用现存 trio 时，即便每批只删其中 32 块，独立验证仍针对**完整 trio**。重复调用 32 块维护自然重复付出完整 SHA/semantic sort；在已知空间和输入上界内，把同一覆盖 trio 的剩余范围一次纳入可减少这一固定成本。本轮从 32 块 canary 扩大到剩余 2,285 块正是这种摊销。不能为摊薄固定成本取消 `max-input`、work budget，或改成整个历史前沿无上限一批。

## 3. 已有优化，不能重复算作新方案

- **P5.24/P5.25** 已有按 chunk 有界并行压缩、按原顺序输出，以及直接写压缩流；小于 1 MiB 保留串行快路。当前 history chunk 为 128 KiB，worker 为 `min(GOMAXPROCS,4)`，不是尚待实现的“多核压缩”。见 [既有性能设计](../superpowers/specs/2026-07-31-erigon-full-performance.md:3906) 和 `snapshots/compressed_block.go:1063,1088`。
- **P5.26** 已移除新建 history 的第二次全 payload 自检，但这个信任限定为同一 builder 事务；外来文件和本次离线删除仍走完整独立验证。**P5.29/P5.30** 已有线上按完整 trio 身份的持久语义证明缓存，以及 manifest verifier 直接委托联合 trio gate，避免先做另一遍普通 history checker。见同一设计 `:3967,4109,4175`，`pruning/lifecycle.go:244`、`pruning/verification_cache.go`、`snapshots/manifest_verify.go:340`。不能把线上的 trusted cache 当作当前离线工具已经复用的能力。
- **P2b** 已有外层 format=2 的 footer 格式，压缩 body 顺序写一次、rename 同一 inode，少一次 body 读回和输出复制。代码默认仍为 format=1；本轮离线新 trio 实测也是 1。P2b 同输入服务器文件基准的 64 MiB 总耗时 489.734→398.664 ms、收尾 203.506→50.302 ms，不能等同整链速度或稳态容量改善。[P2b 记录](28m-history-footer-p2b-2026-09-05.md:1)。
- **P2f** 已为 V6 **merge** 分开 range/record reader，借用块内完整 frame、跨块才复制；双源 SHA/验证最多 2 worker，失败必须 cancel+join。固定 428,548,548 B 生产 fixture 三轮中位数 8.049651→6.659756 s，只有此组件约 17.27% 耗时改善；不能称为整个转换/同步提速。[P2f 记录](28m-history-stream-p2f-2026-09-06.md:1)。
- 已有 radix ETL、tournament spill merge、keyID 字典、只存 Prev 的 V6 history，以及 V7 的差分索引。当前 217 个 accessor/217 个 index 已是 V7，不能提出一次“迁移到 V7”并把它算成容量收益。检查实际 128 KiB chunk 布局与索引版本是不同事项。

## 4. 已定位的效率机会与安全边界

### 4.1 小范围候选：重复 accessor 验证

审计时 `history_binary.go:1558` 只排除 V4/V5，V7 仍走 `CheckStateDomainChangeAccessorSegmentContext`，该函数又计算 accessor 完整 SHA，并走 `checkStateDomainChangeBinaryAccessorV7`。随后 V7 sequential gate 再消费全部 posting。上游 `:1511` 已先计算过 accessor SHA，因此同一次联合验证有一个确定冗余的 accessor SHA pass。

已按根任务授权实现最小修改：**仅 V7** 在这里的独立检查改为已有 `checkStateDomainChangeBinaryAccessorLayoutContext`，保留完整结构遍历，少做重复 SHA。上游仍先以同一个 `dir/accessorRef` 校验完整物理 size/SHA；后面的完整 tuple 比对保持不变，其它版本仍走原分支。公共 standalone 检查入口未改成 layout-only。这是效率修改，不改变文件格式、空间预算或发布/删除顺序。

新增 `history_verification_sha_test.go`，联合 gate 的合法输入、坏 SHA、坏 size、重算 SHA 后的坏 header/额外 posting 尾部、有效结构但 posting 绑定不同、取消测试通过；独立 instrumented Reader/ReaderAt 证明省掉的是恰好一个 accessor 文件长度的 SHA 读取，结构读取量不变。这是调用层读取量，不是设备 I/O。首次测试夹具误用不符合 binary reader 命名约定的文件名，走到了旧非 binary builder；已修正夹具命名后复测通过，未将首次失败算成通过。

测试专用 `BenchmarkHistoryV7JointVerificationSHAProduction` 可比较私有单 trio fixture，环境变量为 `GTRON_HISTORY_VERIFY_FIXTURE`；临时目录须与 fixture 同文件系统以便硬链接，ETL 在独立临时目录。子项 `baseline_extra_sha` 在优化后的完整 gate 前补一次同 accessor SHA，`single_sha` 使用优化 gate。此 baseline 只还原合法输入的工作量，额外 SHA 的执行位置与旧代码不同；严谨的最终性能结论仍应交替运行固定前后版本 binary。没有添加可关闭生产验证的开关，也未执行服务器基准。

本地 512 条记录、accessor 仅 2,364 B 的微基准已验证 helper 可运行，3×3 次中未出现稳定加速，且当时相关包回归并行运行；不据此给出提速百分比。首次集成曾在实验 `TestExperimentalPostingsAgainstProductionV7Oracle` 的夹具上失败（source companions mismatch），该夹具随后修正。

最终整体验证执行 `go test ./core/state/snapshots ./core/state/pruning ./cmd/history-value-bench -count=1 -timeout=180s`，退出码 0：snapshots **60.849 s**、pruning **18.564 s**、history-value-bench **2.496 s** 均通过，`git diff --check` 通过。这是修复后的本地完整相关包结果，不是服务器性能或容量收益证明。

本次 SHA 优化的上述定向测试普通运行 1.618 s、race 运行 3.499 s 通过，`git diff --check` 通过。另一个代理只读复核确认同 ref 的前置完整 SHA、V7 完整 layout 和后置 tuple gate 均保留；不可变文件及持续维护锁前提不变。

更进一步不能只在条件中排除 V7。虽然 `:1599` 注释声称 sequential cursor 已覆盖全部结构检查，但 `history_key_oriented_v7.go:531` 的 cursor 在本次审计时缺少 standalone `:826–863` 的跨 key postingOffset 连续、每 key 实际长度和最终 postingLen 精确覆盖。先将这些拒绝条件融合进 cursor，并用重算 SHA 后的 gap/trailing/错 count/错序损坏文件验证，再移除独立 structural pass；否则会削弱删除门槛。

### 4.2 把 P2f 的顺序读取能力接到验证和比较

V7 verifier 的 `txRanges` 与 record 使用同一个 segment ReaderAt（`history_binary.go:1651,1688`）；frame helper 仍先读 4 B prefix，再读并复制 payload（`:4274`）。冷热比较调用 tx-index reader，后者逐条使用通用 `readStateDomainChangeBinaryRecordAtBoundedIndex`（`:2571,2610`），也没有直接采用 P2f 的独立 range cursor/borrowed 顺序流。

这是代码层面的重复拷贝和潜在解压缓存驱逐，**尚无本轮 profile 证明其占 92.681 s 的比例**。候选实现应复用独立 range reader，并使 frame 借用持续到所有当前消费者完成。比较端现有无缓冲 channel+ack 仅借用一条，所有退出 cancel+join（`offline_history_compare.go:21–61`）；简单增大 channel 会让借用 Prev 在消费前被覆盖，不能这样“并行化”。若按块批量传递，必须使用有界自有 arena、消费确认和按字节计费。

### 4.3 减少重复 hot 规划，不能跨重开盲目信任

RO→RW 重新规划同时承担安全检查，不适合直接删掉。适合新数据目录的改进是：在 block history 写入时，持久化与 pack 内容绑定的 N、logical length、largest row、排序/范围摘要；规划通过这些**认证元数据**计算预算，后续真正转换仍验证实际计数和硬字节限额。仅 head 或 manifest SHA 相同不能证明所有 hot bytes 未变。

当前维护二进制继续完整扫描；新布局元数据须和 block commit 同一恢复边界发布，损坏/丢失一律失败或全扫描，不可用可能过期的 sidecar 低估工作预算。

### 4.4 sidecar 一次写成

即使 P2b format=2 已消除 history body 复制，V7 tx index 仍有 fixed-width→V7 的 staging 改写，accessor 仍有 posting/keyData 临时文件→最终文件复制。新格式可将 tx-index restart frame 直接从 record writer 输出，使用 footer 记录目录、总数和校验和；accessor 也可在排序流中直接写 posting/key blocks，尾部存目录和 keyID 映射，从而减少组装复制。

这是新 sidecar writer/reader 设计，尚未实现；posting 按 key 顺序仍需要排序或等价分区。不能仅把目录移到尾部便宣称消除了 key/posting ETL，也不能把只省数 MB 的 sidecar 复制当作省掉 GB 级 value 搬运。并行索引实验由独立实验组件负责，生产 registry 不自动接入。

## 5. 适合新 datadir 的一次写入路线

“一次写入”应明确目标为**历史 value 不再先经 Pebble WAL/SST 多层重写，再转写 cold**。无法承诺每个字节一生只落盘一次：恢复日志、压缩、验证读取和后续 merge 仍可能产生额外 I/O。

推荐分两阶段验证，而非立刻把 history 从 commit 中抽走：

1. **独立、顺序追加的 block history journal**：完整 block 的 txNum 范围、block hash、每 tx 前像和 block-final ordinal 构成封口单元，采用可截断的 frame/CRC/长度。latest 状态提交记录引用该 journal 的精确 durable offset/hash；journal fsync 先于任何声称可恢复到该块的持久 head。崩溃时丢弃未封口尾部，按同一恢复边界读取或重放；不能出现 latest 已持久化而对应 Prev 不可恢复的块。
2. **journal 直接形成不可变 cold 分区**：优先让有序输入一遍产生压缩记录、tx index 和供 key index 使用的轻量 tuple。局部 dictionary 可按首次出现分配 ID，另有按 key 排序的查找目录，或使用有界微分区，以免先扫描所有大 value 收集全段字典。具体格式需要测量字典收益、查找读取量与边界开销；不能用全链常驻 key→ID map 实现“单遍”。

查询必须同时覆盖已发布 cold、未转换 journal 和当前 mutable tail，且多层切换不出现空洞或两份不同版本。每 tx 历史、空值与不存在、account generation、block-final changes、reorg/unwind 仍是硬约束；仅保留块末状态不符合当前语义。派生 postings 可重建，原始前像不得在新文件、完整验证及 manifest 发布以前回收。

若记录始终保持 tx 顺序，通常还要一个 key-index 排序阶段；若改为按 key 存放以获得 same-key value delta，tx 范围迭代将需要第二种索引或有界 merge。必须报告它对 tx-range/unwind 与 as-of 两类查询的读取放大，不能只报压缩率。value delta 要设置完整 checkpoint 和最大重构链，按原始 Prev 字节逐条恢复验证，避免让深历史查询成本无界。

第一阶段可先采用顺序 journal→现有 cold writer 的适配实验，隔离“绕开 LSM history 大 value”的收益；第二阶段再改变 dictionary/index/value 格式。小 sidecar/footer 修改与完整 journal 原子提交协议是不同风险和工作量，不合成一个无法定位回归来源的版本。

## 6. 批大小、并行与背压

建议作为**隔离实验起点**：单个 builder、单个 publisher；输入按完整 block 封口，默认目标 64/256 MiB 解码数据，两档对照，最大 records 与单条 value 长度独立限制；workers 1/2/4 对照，队列按 owned bytes 限流而不是只数块。生产配置不因本报告自动改变。

批量大小不能只写 5,000 blocks 或 400,000 txNums：同一区间每 tx 的历史条数和 Prev 长度可能差几个数量级。过大的段摊薄固定 SHA/fsync，却增加失败重做、ETL spill 数和历史查询目录大小。已有 cold 复用批尽量对齐完整 trio，但仍必须满足实际 `MaxInputBytes` 和全段 verification scratch 的预算。

当前内存边界尤其需要准确表达：

- V6 keyID 加速表自身最多 **512 MiB**，超限回退字典搜索（`history_key_oriented_v6_build.go:25,254`）；另有字典 blocks、cache、hot 单块解码和 reader buffers。
- ETL `BufferLimit` 按 key+value+17 B 计费，不包含 Go slice/entry/order/arena 的全部容量（`rawdb/etl/collector.go:313`）。其 merge 当前一次打开所有 spill runs，每个 reader 使用 1 MiB 缓冲（`:24,459,899`）。缩小 BufferLimit 同时增大批量会提高 FD 和 heap，不能宣称总内存严格限制在 32/64 MiB。
- 128 KiB chunk × `2*workers` 只是压缩流水线未压缩 in-flight buffers 的局部上界；编码 buffers、encoder、页目录和其他阶段都另外计费。V7 验证 ETL 默认 32 MiB，同样不是验证进程 RSS 上限。

新实现应加入 ETL fan-in 上限（例如 32/64 路实验），超过时分级归并，明确增加的临时读写次数；分别记录 `spillRuns/maxOpenRuns/peakScratch/peakRSS`。不要靠无限提高 fd limit 掩盖无界 reader 数。

背压的触发应包括：未来一个完整批次的 output+scratch 保守预算不足、未转换 journal 累计字节超过阈值、服务率持续低于导入产生率、compaction debt 增长及实际 I/O 延迟。达到阈值先暂停历史工作准入或导入生产者，不能丢历史数据，也不能让多个冷构建、验证和 Pebble compaction 各自以为拥有全部剩余空间。失败后尚可见的新文件保留，下一次先审计恢复状态，不抢跑第二个 writer。

## 7. 可执行的实验矩阵与验收

所有输入固定到私有 fixture 目录，输出独立目录；不打开生产数据库 RW，不连接 P2P，不启动 gtron node。生产文件只读基准与本地合成基准分别报告。先为原路径保留基准 binary、源码 hash、输入六文件/trio 的 SHA 和环境；固定相同输入交替 A/B 至少 3 轮，避免先跑一边后跑另一边的缓存偏差。

| 实验 | 独立变量 | 必须记录 | 通过条件 |
|---|---|---|---|
| E0 原路径分解 | prune-only / new build；已有两个真实 trio，加不同年代、不同大 value 占比样本 | plan/key pass/write/finish/SHA/semantic sort/compare/prune 分阶段 wall 与 CPU，实际 read/write 调用字节，allocated/RSS，scratch 峰值 | 形成同输入可重放基线；计时不把目录生成或最终 oracle 混入主阶段 |
| E1 去重复 SHA | 原联合 gate / 仅一次 accessor SHA | accessor 被完整 hash 次数、总 gate wall | 全部原损坏检查仍拒绝，合法文件结果相同；不先移除结构扫描 |
| E2 verifier / compare 顺序 reader | 原 reader / 独立 range+borrowed frame | 解压 chunk 次数、borrowed/cross-chunk copied bytes、allocs、wall | 高频换块、跨 chunk prefix/payload、70 KiB+大 value、取消和错误下字节一致；无借用越期或泄漏 goroutine |
| E3 sidecar 直接写出 | staging rewrite / footer 直接写；固定 index 编码策略 | 全 trio 大小、sidecar 逻辑与实际写量、finish wall、scratch | 所有 tx/offset/ordinal/count 和 as-of 结果一致；损坏 header/footer、溢出、gap、trailing 拒绝 |
| E4 有界资源 | 64/256 MiB batch；workers 1/2/4；ETL fan-in 32/64；高 unique key/大 Prev 两极 | spill、FD、RSS、GC、queue wait、IO 与失败重做字节 | 峰值不随总链长增长；强制低资源时失败可重开，不推进未完成 cursor |
| E5 新 journal writer | 原 hot→cold / journal→cold，先保持逻辑 V6 数据一致 | 同一执行 effects 输入下总落盘写量、恢复时间、history 查询延迟、steady bytes | 每个 fsync/rename/manifest/head 边界 kill/fault 后恢复同一合法前缀；逐 tx 前像与旧路径 oracle 相同 |
| E6 value/index 新格式组合 | 原 trio / 每个单项 / 最终组合 | net trio bytes、完整构建和查询 CPU/IO、随机 p50/p95、最差 delta 重放长度 | 不把各单项百分比相加；全部 records exact reconstruction，存在/缺失/跨 checkpoint/reorg 查询一致 |

现成组件基准可复用 `BenchmarkHistoryV6StreamProduction`、`BenchmarkHistoryV6StreamMerge`、`BenchmarkHistoryAccessorAssembly`、`BenchmarkBuildStateDomainChangeHistoryTxRanges`，以及 V7/chunk/取消故障测试；它们没有覆盖整个离线 pass，不应仅重跑这些就宣称端到端通过。新增改动需要有阶段埋点和确切原始结果后，才填写改进比例。

收益门槛由同输入结果决定：至少证明目标读写 pass 确实消失或需要保存的 bytes 减少，同时完整验证、可恢复性和历史查询不退化到无界成本。若仅加快转换而冷热净空间几乎不变，应作为效率改进交付，不将其称为固定磁盘容量的最终解决方案。全历史增长在有限磁盘上仍须持续预算；从创世重构不会取消这个边界。
