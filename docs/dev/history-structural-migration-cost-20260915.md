# 历史结构重构：迁移与创世重同步的成本

2026-09-15，独立成本评估。用户明确选择**不复制数据库，直接原库就地迁移**，并接受
迁移失败、原仓不可恢复时清空后从创世重同步。完整备份或双仓空间不是本方案前置条件。
本文自身不执行删除、停机、迁移或重启。现有线上修复已经减少重复工作并出现净追赶，
但没有改变大旧值的长期物理表示。

当前可以给出有效历史存量和逐批空间预算，**不能给出可信的全仓迁移或创世重同步 ETA**。
新格式双读、有界 writer、逐 trio 迁移事务及恢复测试已实现，但首轮原生构建对照未通过
性能门槛。尚未执行任何生产迁移；先验证修正后的候选及分层冷输入转换，再以唯一发布者
逐 trio 替换原库 manifest。
新文件完整认证后才替换对应旧引用；不要求保存整份旧仓用于回滚。
从创世重同步作为用户接受的失败恢复路线，仍必须重新执行整条链。
若仍用原物理表示，清空重同步会重新产生同一类大旧值成本，并不构成结构修复。

## 证据版本与适用范围

主依据是[最新根因和上线报告](cold-backlog-root-cause-20260915.md)、
[原结构候选](history-cold-structural-options-20260915.md)及
[真实输入规范](../superpowers/specs/2026-09-15-cold-throughput-root-cause.md)。
原结构候选开头的“尚无原生结果”“优先多段并发”属于早期状态，不能当作最新选择：

- 后续同一 16 块原生实验中，四段输出从 12,421,391 B 增至 27,350,568 B，增加约
  120.19%；直接上线分段并发已被否决。该实验不是新引用容器或迁移工具的结果。
- 单 trio 内部流水线、CDC 完整相等后复用摘要、构建内分块缓存已经验证并上线，仍保持
  原输出格式。原生 R4/CDC1/cache 的 25.477 块/秒只代表那份私有热输入的完整 trio 构建，
  不含生产发布、裁剪、合并、恢复等待，更不是旧冷文件迁移或创世重放速度。
- 最新生产源码为 `197b73a920a826f21b460b3a85cc0e7ba1727d99`。06:33:33–06:45:33 UTC
  完整 25 点窗口 cold published 为 21.086 块/秒，eligible 新增 8.390 块/秒，积压减少
  9,141 块；期末 head 32,219,932、cold 31,100,592、eligible 32,154,374，eligible lag
  1,053,782。短窗证明当前净追赶，不证明以后恒速，更不能用 `32M / 21.086` 估算重同步。

本地数字来自 `build/benchmarks/20260915-root-cause/` 的 `native-rollout.json`、
`native-distribution-observed.json`、`native-abba-observed.json`、
`native-shared-read-observed.json`、`gc-accounting-full-window-summary.json` 和
`gc-accounting-final-native-observations.json`。这些 native 摘要含终端转录事实，不冒充
已下载的完整服务器报告。原始采集在 `/data/gtron/releases/20260915-history-range/result/`，
最终目录观测在 `/data/gtron/releases/20260915-history-gc-accounting/activation/`。
下列 07:38/07:50 库存和首轮 R1 ABBA 数字来自根任务执行的原生只读命令/实验转录，
本评估未另行上服务器，也未取得完整生产 manifest 副本；不同时间的库存不拼成单一快照。

## 已知物理存量

06:46:10.906–06:46:13.455 UTC，运行中顺序执行 `du -s -B1`，三项退出码均为 0、无 stderr。
以下是文件系统分配字节，既不是同一时刻停机快照，也不是每种历史数据的逻辑字节数。
单位 GB/TB 均为十进制。

| 范围 | 分配字节 | 约 GB | 能说明什么 |
| --- | ---: | ---: | --- |
| `chaindata` | 345,149,509,632 | 345.150 | 整个 Pebble 目录；包含当前状态、热历史及其他表，不能全算待迁移历史 |
| `state-snapshots` | 760,584,224,768 | 760.584 | 当时整个冷目录；有效历史后来另行盘点，不能直接与后一时间的文件长度相减作其他 dataset 存量 |
| `ancient` | 243,473,563,648 | 243.474 | freezer 目录；不是可直接用于新状态初始化的 state checkpoint |
| 三目录算术合计 | 1,349,207,298,048 | 1,349.207 | 当前已测范围的总量，不代表数据卷所有服务占用 |
| 数据卷可用 | 2,076,890,697,728 | 2,076.891 | 当时 `df` 可用值，需扣未来增长和操作余量 |
| 数据卷总容量 | 7,609,457,909,760 | 7,609.458 | 同卷还承载其他服务，不能把未分配给 gtron 的部分都算可迁移容量 |

同一采样期 Pebble 指标增加 1,031,741,981 B，而 LSM compaction debt 增加
2,316,119,765 B；后者是压实估算工作量，不是额外占用文件，也不是历史积压块数。
不能将 debt 再加到 `du` 作为“已经使用”的空间，但要为真正的压实新文件预留空间。
不能将 12 分钟的目录增量线性外推数天。

后续 **07:38:44 UTC、manifest generation 43,851** 的原生库存给出了待转换范围：

| 有效 state-history 文件 | 数量 | 文件长度合计 | 约 GB |
| --- | ---: | ---: | ---: |
| history | 3,476 | 518,879,853,185 B | 518.880 |
| accessor | 3,476 | 128,370,122,392 B | 128.370 |
| inverted / index | 3,476 | 10,260,841,736 B | 10.261 |
| 完整三件套合计 | 3,476 trios | **657,510,817,313 B** | **657.511** |

index 是 **10.261 GB**，不是早期终端转录误记的 102.608 GB。库存中 40,375 组旧
retired trio 引用所对应的现存文件为 **0 个 / 0 B**；没有 published manifests 租约或
snapshot catalog。这不赋予未验证新输出删除权，只说明当次盘点未发现租约保留成本。
本轮新测可用空间为 **2,081,587,335,168 B，约 2.082 TB**，是容量基点，不是空间预留。

最大单个 history 文件为 7,308,378,929 B，aggregation steps 为 256，txNum 范围
29,695,367–97,969,993；随后两个分别为 1,529,026,847 B、977,347,846 B。最大 history
文件不等于最大 trio 或最大逻辑展开量，companions、临时目录和校验 ETL 须另计。

另一次 **07:50 UTC** 的容器版本盘点为 V2 固定块 1,407 个 / 212,599,045,434 B，
V3 CDC 2,081 个 / 306,705,960,143 B，共 3,488 个 history / 519,305,005,577 B。
节点期间继续生成历史，因此不能把这次 3,488 个的版本分布和前次 3,476 个的 companions
拼接求精确三件套总量。此处 V2/V3 是外层压缩容器版本，不是逻辑 V6/V7 数据版本。
最大 V2 文件的逻辑大小必须按 footer/table 读取，不能将头部保留字段的零值当成空历史。

这些库存是文件身份/尺寸与目录存在性观测，还没有逐文件完成全仓内容认证。`du` 分配量
和 manifest `SegmentRef.Size`/`stat.Size` 文件长度须分别记录；不能在未排除稀疏文件、
硬链接及不同采样时刻前，将二者相减解释成精确的可释放量。

## 必须做多少认证与逻辑扫描

应分开三个量：源物理读取 `P`、必须处理的逻辑字节 `L`、新物理输出 `N`。
重复引用使它们显著不同；CPU 工作、内存峰值和磁盘读写量也不能互相替代。

唯一已完成严格验证的固定热范围为 30,975,118–30,975,133，16 个完整块；capture manifest
文件 SHA-256 为 `897936be40658c1b8a770970ca15f9a16037e6f8eea822aad26d390c8a826678`。

| 该范围的量 | 字节或数量 | 限定 |
| --- | ---: | --- |
| 物理键和值 | 13,192,384 B / 283 行 | 私有导出的 KV 字节，不是私有 Pebble 的最终 SST 分配量 |
| pack/repair 声明展开量 | 101,902,854 B | 每个逻辑 pack/repair 计一次；不再加 unique chunk 展开量而重复计数 |
| 全部 Prev | 101,515,932 B / 9,163 条记录 | 完整逐行内容认证已完成 |
| legacy 委托聚合 Prev | 100,940,178 B / 98 条 | 占 Prev 99.43284%；不是整个仓库的占比 |
| shared 引用 / unique chunks | 2,064 / 234 | 单次捕获的引用闭包，不是全仓去重率 |
| 最大 Prev | 4,361,457 B | 49 条至少 128 KiB 的大旧值只涉及 2 个不同逻辑键 |
| 原单 trio 输出 | 12,421,391 B | 旧格式、此范围、完整三个文件合计 |

该样本 `L/P=7.72437`。它证明逻辑认证放大确实存在，不提供全仓压缩比。
少数超大聚合列表反复出现，按“每块均值 × 全部块数”会过度依赖抽样高度和键分布。

**待归档热历史。** 既有 shared v3 的整包承诺是原始完整 RLP 的 SHA-256。新构建器可以
只物化一次、缓存 unique chunks、保存合法 Prev spans，但仍必须对每个源 pack 的完整
逻辑字节做至少一次认证，不能把 chunk SHA 合成为 pack SHA。对上面已认证范围，一遍
历史展开处理规模就是约 101.903 MB；现已部署原路径的两遍源扫描对应约 203.806 MB 的逻辑处理，
缓存不取消这两次整 pack 认证。此处是工作字节，不代表同等大小的实际设备读取或同时驻留内存。
全体热积压的下界应由固定视图中所有选中 pack/repair 原始长度求和，现有数据尚无该总量。
不能把这 16 块的 101.903 MB 乘以约百万块积压。

**既有冷文件转换。** 新的 `ReencodeHistoryReferenceTrioContext` 已实现有界转换：CDC
按已有 anchor/reference 边界转存 unique chunks，固定块按页转存；companions 保持原字节
和逻辑偏移，目标容器自包含，不引用热 Pebble 或会被退休的旧文件。转换前验证源完整
物理 checksum 和 companion 覆盖；转换后对新旧虚拟 V6 全字节作比较、验证新 trio，
并重验源 checksum，才返回可发布的 refs。

**转换阶段保存引用，当前完整验证仍有逻辑扫描成本。** 对选中范围定义
`L_history = Σ(history logical_size)` 和 `L_prev = Σ(len(Prev))`。单次新旧虚拟字节
比较就读取源 `L_history` 和目标 `L_history`，即至少 `2 × L_history` 的逻辑读取；
源/新 companion 覆盖校验还需遍历完整记录。管理器在发布后删除旧文件前又验证新 trio，
并对实际存在的旧路径重验 size/SHA。不能将这些逻辑读取当成等量设备读取，因为页缓存、
chunk cache 和重复引用会改变实际 I/O；也不能把 Prev 字节再加到包含它的 logical_size
中当成唯一数据量。

全仓选中源的第一轮完整物理认证至少须读上述三件套文件长度约 **657.511 GB**；这是
那个库存时点、选择全部三件套的源读取下界，不含后续源复核、新输出认证及虚拟比较。
后续 manifest generation 43,961 的头部/目录盘点得到 V2 逻辑量 734,793,371,385 B，
V3 逻辑量 3,765,813,027,176 B，合计 **4,500,606,398,561 B，约 4.501 TB**。
这给出了当次 `L_history`，仍没有单独的全仓 `L_prev`，不能将其再加进逻辑量。
最大 V2 的逻辑量为 31,860,013,965 B、243,073 个固定页；最大 CDC 文件逻辑量为
3,799,433,419 B、84,038 个目录项。不同时间的文件数不合并为同一精确库存。
不能拿 657.511 GB 或冷目录 `du` 代替逻辑校验量。实现已具备，
但没有完成生产冷文件转换测量，不能把完整迁移成本写成 `O(unique bytes)`；旧格式
逐行迁移的性能数字也不能替代新转换的总成本。

**展开 spool。** 对本样本，若把所有 Prev 展开写入临时盘再读回，仅 Prev 的一次写加一次读
就至少 203,031,864 B，尚不含行头和最终输出；这可能抵消节省第二次热读取的收益。
只保存 span/unique chunks 的 spool 是另一种实现，不能借用此数字当其成本。

## 峰值空间模型

默认是就地逐 trio 转换，源数据库复制量 `C=0`。以当时可用空间
`F=2,081,587,335,168 B` 为本轮更新的基点，下一批能否入场应满足：

`max(0, C + N_live_peak + T_peak + V_peak + G + H - R_proven) ≤ F`。

各项都是**相对当前已有文件的新增分配量**：`C` 在本方案为零，`N_live_peak`
是尚未可回收的新输出峰值，`T_peak` 是 ETL/spool/压缩临时文件，`V_peak` 是长读视图等
保留的额外旧版本，`G` 是生产和其他服务在操作期间的增长，`H` 是既有空间保护与操作
余量。`R_proven` 只能计已满足完整证明、lease 和文件句柄条件并实际释放的字节，不能先减
“预计会被 retired 的源”。`N_live_peak` 已包含未发布但准备保留的 final 文件时，不再把它
同样计入 `T_peak`。

通常下一批的新增需求是“一个新 trio + 该批 scratch + 尚未退休旧引用造成的累计新增量”。
源段本来已在 `du` 中，不应为了保留到验证结束再加算一份源副本。若上一批旧文件可以
合法退休并确实释放，再给后续批次抵扣；若 published lease 或读者仍保留它，峰值会累计。
就地迁移不需要另申请 1.349 TB 来复制三目录，也不以复制后的剩余空间决定能否开始。
少量固定范围实验文件只按实际新增量计入，不等同于复制数据库。

新的逐 trio 工作估计器按额外空间而非整仓复制计费：固定块用声明逻辑大小，CDC 用
anchor 解码字节合计 `U`，估计为 `2U + 2C_companion + 2×32 MiB + 1 GiB + E`；
其中两份覆盖 scratch/final 共存，V7 校验 ETL 的 `E=128×recordCount B`，V6 无此 ETL
项。不支持的旧 accessor 或超目录/codec 上限输入直接失败。此数是保守入场估计，不是
测得峰值，也不预留磁盘；每段仍按同卷最新 available 与 reserve 检查。

实际管理器在节点停止且持有对应 Pebble 排他锁时逐段执行：完整验证输出，写小型耐久
journal，原子发布，再删没有 active/published lease 引用的精确旧文件并 fsync 目录。
已有租约会阻止该源转换，管理器不会主动过期租约。此次盘点没有租约，因此成功路径可
及时回收每批旧 trio，**不按整份新仓同时保留来预算**。发布/删除中断时保留 journal
和必要文件，恢复仅接受精确旧/新 manifest；新 manifest 已 active 时允许旧文件部分缺失。
失败遗留的孤立新输出和其他任务 scratch 仍占空间，不能当作已回收。

这里只要求新 active view 完整认证及正常删除条件，不要求旧二进制可回滚。
“接受失败后重同步”不等于跳过正常删除证明，也不改变同卷其他服务的空间竞争。

## 迁移与从创世重同步的工作差异

| 路径 | 保留什么 | 必须重新做什么 | 主要成本与缺口 |
| --- | --- | --- | --- |
| **默认：原库就地逐段迁移** | 当前未处理旧段和正常发布/删除保护 | 每批认证、转换、companions、自检、原子改 manifest，再按条件回收旧段 | 不复制源库；新增空间以单批和不可即时退休的累计引用为准，失败可走创世重同步 |
| 新 writer + 旧 reader，旧冷段按需迁移 | 现有当前状态、已同步块及旧历史 | 新热范围写新格式；选中旧段做完整转换/验证 | 最少重复链执行；需新旧双读、混合 compaction 和归档读取验证 |
| 全部旧历史离线迁移 | 当前状态和区块执行结果 | 被选 history 的认证、转码、索引、自检、清单发布 | active 三件套约 657.511 GB，后来盘点 history 逻辑量约 4.501 TB；新输出比例、临时峰值和停机时间仍待测 |
| 清空原仓后创世重同步 | 仅另行保留的配置、身份和证据 | 从创世重新执行至固定验证高度，再追最新 head；状态/历史重新生成 | 不保留双仓，但失去原仓回退；新结构必须先就绪，否则重复旧成本 |

当时若三目录完全独占、无其他链接/打开文件/快照保留，且全部被删除并实际回收，理论
最多可回收已测 1,349,207,298,048 B，使当时可用空间增至约 3,426,097,995,776 B。
该算术使用 06:46 的旧 df 基点，不与后次库存混算；它不是可立即执行的删除清单或
保证释放量。只清 state-snapshots
不等于从创世重同步，可能直接破坏现有历史和读取版本契约。

创世重放要执行早期至目标高度的交易、TVM、维护和历史写入。最新窗口的目标观察高度
为 32,219,932，完成时网络目标仍会继续前进。各高度的交易密度、委托列表规模、fork、
合约负载、缓存、压实和网络供应不同；32M 不是 32M 个成本相同的任务。同期 freezer v2
coverage 指标为 31,064,064，这只是已有覆盖水位，不证明已经有可独立完成创世重放的
完整源集合。应另核 canonical bodies、交易和缺口，且新仓从创世构造状态的 replay 入口
需要单独实现/验收，不能把 ancient 迁移 CLI 当作现成状态重放器。

时间模型应采用分层输入的端到端测量。对固定目标，`T_replay` 由各历史区间的交易执行、
历史构建与维护服务成本、输入供给以及追赶尾部共同决定；不能相加会重叠的流水线阶段
计时，也不能用当前 12 分钟块速率作常数。迁移时间同理应实测每层转换、发布及回收，
CPU/I/O 下界可写为 `max(CPU_work/available_cores, read_bytes/read_rate,
write_bytes/write_rate)`，另加无法重叠的发布与耐久化部分。这只是条件模型：共享设备的
实际 rate、写放大和与生产竞争尚未测清。

## 现有工具能复用到什么程度

源码审计范围为当前仓库；新迁移事务已在临时目录完成本地 normal/race 测试，未对生产执行迁移：

- 新的 `db migrate-history-reference` → `MigrateHistoryReferenceContext`：默认 DryRun，
  `BeforeTrio` 对选中范围做只读空间估算；`--yes` 才转换。CLI 限定对应 datadir 的默认
  snapshot 目录，整个操作持 Pebble 排他只读锁；另有迁移 flock 防止同工具并发。
  每 trio journal 绑定旧/新 refs 与完整 manifest 字节摘要，保持 Chain/Progress，发布
  后验证新文件再回收旧文件，支持部分删除后的恢复。它不反复全仓文件 Verify/prune，
  但仍有整 manifest 序列化与摘要读取的固定成本。源码的本地正确性通过不等于生产
  7 GB 级旧冷段的转换时间、空间和性能已通过。

- `snapshot migrate-history-v7` → `MigrateHistoryV7`：转换现有 compact V7 布局，按
  trio 发布并跳过已 current 的段；它没有实现新引用型容器。`ActiveBytesBefore/After`
  是 active refs 的文件长度，`RetiredBytesAdded` 不是实际释放字节。函数保留旧文件，
  每个 trio 都调用 `NewAggregator(...).Integrate`，仍有全清单遍历/序列化等固定成本。
- 该 CLI 的 `--yes` 和说明要求节点停止，但该入口本身未核验 systemd/PID 或取得生产
  发布互斥。`opts.Context` 在 trio 之间检查，实际 `cfg.CompactHistory` 仍走 Background
  wrapper；不能拿它承诺当前大 trio 可立即取消。新迁移任务须有明确唯一发布者、进度
  原子性、错误保留、取消/join 和资源上限，不能直接把该 CLI 指向运行目录。
- `InspectRetiredSegmentFiles` / `PruneRetiredSegmentFilesContext` 会保护 active 与 published
  manifests 的路径；存在候选时，默认还要先完整验证 active 文件。首次清理可能增加
  全 active-view 验证成本。现有 lifecycle 的内容身份验证缓存不能被计为“每 trio 都必然
  全重验”，也不能省去新/变化对象的验证。
- `snapshot benchmark-history-space` 的 `InspectHistorySpace` 能汇总旧格式 active trios
  和做有限样本；选择策略为 largest-half-plus-even-range，不是随机总体样本。
  `inspectHistorySpaceHeaders` 明确拒绝 CDC v3 的 fixed-block 投影。因此当前 `auto`
  仓可能包含 CDC 时，不能靠调大 sample 参数就得到完整库存或可信新格式空间预测。
- `ExportStateHistoryRange` + `db benchmark-history-cold` 已能取得保留原编码的有界热
  输入和完整认证回放。硬上限为 256 块、1 GiB 物理、4 GiB 声明展开、262,144 keys；
  物理采集报告的 `ContentVerified=false` 是正确语义，恢复服务后的私有解码验证另做。
  canonical hot body 不存在时要安全失败；它不应被冒充全仓冷段迁移或 ancient adapter。

相关代码：[`history_reference_migrate.go`](../../core/state/snapshots/history_reference_migrate.go)、
[`history_reference_transcode.go`](../../core/state/snapshots/history_reference_transcode.go)、
[`db_history_reference_migrate.go`](../../cmd/gtron/db_history_reference_migrate.go)、
[`history_migrate_v7.go`](../../core/state/snapshots/history_migrate_v7.go)、
[`history_binary.go`](../../core/state/snapshots/history_binary.go)、
[`retired_prune.go`](../../core/state/snapshots/retired_prune.go)、
[`history_space_inspect.go`](../../core/state/snapshots/history_space_inspect.go)、
[`history_range_export.go`](../../core/rawdb/history_range_export.go)。

## 首版 R1 原生对照：尚未通过性能门槛

同一份已认证的 16 块私有热输入，首轮原生 ABBA 汇总为：

| 完整构建指标 | 原路径 | 首版 R1 | 变化 |
| --- | ---: | ---: | ---: |
| wall seconds | 0.6556656585 | 0.853176329 | **增加 30.124%** |
| 完整 trio 文件长度 | 12,421,391 B | 13,290,144 B | **增加 6.994%** |

双方完整构建与逻辑 digest 均通过，输入前后不变；这是同输入的热范围构建对照，
不是旧冷文件转换或线上整周期维护对照。它支持“首版未通过性能/空间门槛”，不支持
“新结构已更快”，也不能用这个 7% 预测 657.511 GB 全仓的新输出量。

首版遗漏了将临时 V2 tx index 重写为原 V7 的步骤，根任务已补齐；writer 的 128 KiB
目录页缓存及单遍 R4 源流水线也在后续候选中修正。修正代码和本地正确性测试不替代
同固定输入的原生复测；当前尚不能报告修正后的收益。**没有执行生产迁移。**

## 下一步需要实测的最小集合

1. **补完库存的工作量字段。** 已有物理组成和 lease 存在性盘点；继续固定 manifest
   generation/SHA、chain identity、published leases 和
   文件 closure；按 dataset、active/retired/其他、V6/V7、固定块/CDC、block/tx 范围列出
   `st_size` 与实际分配字节。CDC-aware reader 只读目录可汇总声明 logical_size、记录数、
   unique encoded bytes 和引用数，但未完整认证时必须标注 preflight，不充当删除证明。
2. **补足热积压分层。** 固定视图按旧值规模、codec、shared 复用和区块/交易密度选择多个
   有界范围，至少包含已知大 delegation、普通低复用、小值、修复/异常大块；复用原物理
   导出并在私有库完整认证。不要把前台 32M 的数据形态直接归因到冷端 31M 的处理负载。
3. **冷迁移测完整 trio。** 原库选择一个通过工作估算的有界 trio，完整记录转换前后
   文件身份；必要的诊断输出只占该批额外空间，不复制数据库。同输入交替测旧逐行转码
   和候选转换，正式切换前完成验收；记录完整逻辑/物理 digest、所有 archival rows、point/as-of/prefix/
   restore、混合 compaction 和每个新文件。采集实际 read/write bytes、CPU、wall、峰值
   RSS、临时与新文件分配峰值、冷/热 page-cache 条件；分配 B/op 不当作 RSS。
4. **把发布与清理单独纳入端到端。** 用认证的私有 manifest 与文件 closure，测连续多段
   发布、首次校验、旧 lease 保留、retired 回收和失败恢复。以实际阶段记录配对成本，
   不相加独立 rolling gauges 的中位数；检查新旧份累计峰值而不只看单次输出。
5. **只有需要选择重同步时才测执行成本。** 固定目标和可重放 block source，从创世
   checkpoint 建立正确状态；多个后续区间必须从已验证的对应状态运行，不能只抽取块体
   跳过前置状态。记录交易/TVM/维护分布、区间执行时间、历史输出及设备预算，再给区间化
   ETA 和上下界。重放与同卷其他在线服务会共享设备，独占重放与共用设备的测量须分开。

工具已落实；先完成修正后原生性能门槛及 1–4 的有界端到端验证，再在原库开始迁移；
无需先清空，也不要求整仓备份。源码与输入/结果 identity、每批 manifest 提交和错误记录仍须可复核，便于定位问题。
如果迁移失败使原仓不可用，或已经证明重放更合适，可走用户接受的清空重同步路线。
目前明确缺的是全仓逻辑量、各层新格式输出率、临时峰值和分层执行速度；
没有证据支持“迁移只需几小时”或“从创世几天一定完成”。
