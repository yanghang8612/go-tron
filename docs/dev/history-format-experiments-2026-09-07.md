# 全历史存储格式与转换效率实验

日期：2026-09-07。约束：磁盘固定、MySQL 留在原机、保留全部历史状态查询；允许新数据目录从创世构建，不要求旧存储格式兼容。本次没有启动 gtron 同步，也没有替换生产快照或删除历史。

## 范围和证据

本次交付包括可执行的值编码和 posting 编码原型，以及一处保持验证强度的转换效率改动。原型没有接入默认 writer/reader。不能把原型的编码字节数说成服务器已经回收的空间。

生产 manifest 在实验开始时为 generation 10716，SHA-256 为 `695401a98ee27fbf7c8985d82dd4839424629a39fe9b69cac8e3cbbd980aced0`，217 个 state history trio。此前统计的逻辑文件大小如下；它们不是新格式的容量估算。

| 文件族 | 文件字节 | 新实验针对的问题 |
| --- | ---: | --- |
| 历史 `.seg` | 863,819,535,021 | 同 key 不同版本的大 value 是否有跨页冗余 |
| keyed accessor `.kv` | 94,947,648,975 | posting 中的 txNum、offset、ordinal 及 restart 目录编码 |
| tx index `.idx` | 8,945,638,728 | 时间范围定位及派生位置字段；不能只优化这个较小的索引后声称解决全部容量 |

此前只读结构统计给出约 82.57 GB posting、12.38 GB accessor 其余部分。后文的 posting 百分比只针对实际测过的 posting section，不能直接乘这些生产总量。

最小真实样本取维护过程中已完整验证并发布的 32 块：block 29,127,541–29,127,572，txNum 1,734,454,193–1,734,459,458。复制到 `/var/tmp/gtron-format-lab-20260907/canary`，按 manifest 校验全部三个文件的 SHA 和大小后运行只读 helper。源码在独立 `src` 克隆内编译，未覆盖生产 release。

| 样本文件 | 字节 |
| --- | ---: |
| `history/state-domain-change-1734454193-1734459458-ffa7340839b2bddf.seg` | 49,198,752 |
| 同名 `.kv` | 498,095 |
| 同名 `.idx` | 27,183 |
| 合计 | 49,724,030 |

样本不是随机主网样本，只覆盖一个近期小段。它能证明这段数据的真实编码效果，不能证明从创世重建后的总容量。

## 现有格式已经做过的压缩

hot 持久记录已只存 Prev，省去 Next、每行块哈希及块号；cold V6 使用 keyID 字典。V6 每条非 value 固定成本为 21 B（包含 frame length），本样本 21,747 条只有 456,687 B，不能靠再次“去掉冗余字段”解释几十 MB 的容量收益。

当前 fresh-genesis 代码已有账户 V4 的字段拆分、presence bitmap、signed varint，资源/权限/投票/TRC10 等单独放在 KV domain。已有压缩、外置 receipt logs、主题字典及 V7 差分索引也不算本次新增。历史中旧账户编码的比例和大 value 所属 domain 必须由样本统计确定。

## 两组无损编码原型

`cmd/history-value-bench` 实际编码、Zstd 压缩、解压、解码完整历史流，与原始 V6 流逐字节比较。对照使用同一 Zstd 设置、128 KiB pages，并计算压缩容器 header/table。实验包括 same-key 前后缀 patch、多片段 copy/literal patch、完整 value 去重。两个 same-key 差分方案每 key 最迟每 32 次出现写一个完整 anchor；整值去重引用段内先前完整值，若用于生产需要另计 value locator。不存在与 present-empty 保持独立，时间顺序及全部原记录不变。

其完整流编码用于测可压缩性，尚无生产随机查询目录。新 history 的 offset 已变化，原 `.kv/.idx` 不能直接复用。因此“新 history 字节 + 原 companion 字节”只能是固定索引成本的会计对照，不能当作一个可读的新 trio。任何新布局最终还要计入重写索引、anchor locator 和查询 I/O。

`core/state/snapshots/experimental_history_postings*.go` 对同一批实际 V7 postings 构造两种可查询表示：dense 保留 txNum/offset/ordinal 的自适应常量、bitpack 或 varint 编码；indirect 保留 txNum/ordinal，并用整个 segment 的实际序列化 sparse locator 恢复 offset。locator 每 64 条记录设置 restart，不能省略其全段成本。选中 key 的 posting 全量扫描与 lower-bound 查询均对照 V7 decoder；该 oracle 不是新的全链内容认证。

完整设计和尚未接入的层次见 [新数据目录设计](../superpowers/specs/2026-09-07-fresh-history-format.md)。

## 真实样本结果：优先处理大型委托历史值

完整样本有 21,747 条记录、10,556 个 key，V6 解压流 50,778,680 B，其中 Prev 为 50,320,125 B。SystemDelegation 的 136 条记录中有 111 条 present，合计 **48,259,650 B，占全部 Prev 95.91%**；最大单值 4,731,955 B。账户 present 值全部已经是 envelope V4，账户 domain 总值只有 1,565,965 B。本样本的大头不是旧账户 protobuf。

进一步按真实逻辑 key 分类，确认大头是 **`drax-0` 委托聚合列表**：39 个 key、68 条记录，其中 56 条 present，累计 48,242,830 B，占全部 Prev **95.87%**。其余委托记录全部为 `dr-` 资源对，只有 16,820 B；本段没有 `dri-` 或 directional key，不能据此推断全库没有。

其中仅一个聚合列表（段内 keyID 10407）的 10 个历史值便占 **47,318,560 B，即全部 Prev 的 94.04%**；第二个聚合列表的 5 个历史值占 918,495 B。这解释了为何跨版本差分在本段收益特别大，也进一步限制了其代表性。现有写入路径在列表增删时完整编码、写回 aggregate；结构拆分应优先针对这个已确认的来源。这里统计的是解码后的旧值字节，不是将压缩文件空间精确归属到某个 key。

SystemDelegation 是混合编码域，`dr-` JSON、`dri-` 地址数组都属于正常数据，不能把“不带 native marker”一律当成旧格式。分类器对完整样本执行公共 reader 与原始流逐条比对、输入前后指纹检查，均通过；原始分类结果为服务器 `/var/tmp/gtron-format-lab-20260907/classify-canary.json`。

同输入运行三轮，所有编码都完整还原原始流，`ByteExact / ReaderMatchesRaw / InputUnchanged` 均通过。压缩后大小三轮完全相同；基准重新压缩出的 V6 容器也恰好等于原始文件大小。

| 值编码 | 完整 history 容器字节 | 对 V6 大小变化 | 编码＋压缩中位耗时 | 解压＋解码中位耗时 |
| --- | ---: | ---: | ---: | ---: |
| 原 V6，同设置重新压缩 | 49,198,752 | 基准 | 233 ms | 70 ms |
| 同 key 前后缀 patch | 48,565,914 | −1.29% | 305 ms | 94 ms |
| 同 key 多片段 copy/literal patch | **5,385,948** | **−89.05%** | **834 ms** | 49 ms |
| 完整值去重 | 49,215,679 | +0.034% | 402 ms | 69 ms |

多片段差分确实利用了前后缀之间的稳定内容；只比较前后缀或做整值去重会错过这段数据的主要冗余。然而其编码＋压缩阶段约为基准的 3.58 倍，不能宣传为转换加速。解压＋解码是完整流重构成本，不是随机历史查询延迟；随机读取所需的 checkpoint/locator 尚未接入。

这是单小段、固定执行顺序、热缓存下的三轮组件实验，未做独立进程交替 codec 顺序、冷缓存或 RPC p95/p99。独立 helper 每轮完整四方案实验耗时 3.62–3.77 s，最大观测子进程 RSS 505,588 KiB（约 494 MiB，`GOMEMLIMIT=512MiB` 是软限，不是硬 RSS 上限）。它会将完整有界样本放入内存，不能拿来转换几十 GB 大段。

结论是优先验证大型委托列表的**有序页/关系变化历史＋有界完整 checkpoint**。保持原逻辑 aggregate 可物化，避免每次少量关系增删都将几 MB 整值送入历史和压缩流水线；全部顺序、双向关系、存在性、generation、unknown 字段及每 tx 状态仍需恢复。它有机会同时减少空间与编码工作，比无条件对所有值套通用 patch 更值得优先做。此结构尚未实现，不能用 89.05% 推算其全链收益。

## 真实索引结果：短列表收益有限，间接定位有明确代价

本样本全量 key/posting 都被测到，没有跳过大列表；三轮 `all_keys_measured`、`oracle_equal` 均为 true。

| 表示 | Posting 字节，含新增必要 locator | 对原 posting 变化 | 84,448 次定位中位总耗时 | 全量 posting 扫描中位耗时 |
| --- | ---: | ---: | ---: | ---: |
| 原 V7 | 203,165 | 基准 | 34.54 ms | 未单独测 V7 扫描 |
| dense，保留直接 offset | 197,363 | −2.856% | 20.65 ms | 2.84 ms |
| indirect，含全段 2,768 B locator | 131,556 | −35.247% | 157.32 ms | 832.28 ms |

dense 只节省 5,802 B，占本样本完整 `.kv` 约 1.165%、整个 trio 约 **0.0117%**。本样本平均每 key 仅 2.06 条；实测 8,785 个 key 只有一条（83.22%），仅 8 个 key 超过 128 条。V7 的短列表本来就是差分 varint＋小开销，不能套用本地长列表测试的 40.44% 收益。其 accessor 非 posting 部分占 59.21%，也不同于全库约 13% 的构成；需要再测合并大段才能评价全库索引。

indirect 的查询约为 dense 的 7.62 倍，三轮扫描为 dense 的约 293 倍（首轮约 315 倍）。定位计时已预载两方 posting，排除字典查找及 value hydration；不能将 dense 较低的微基准耗时称为完整 RPC 提速。稀疏扫描多读 frame header、可能重解压较远的页；当前没有足够计数将其直接转为设备读放大倍数。

因此不把当前 indirect 原型作为默认方向。下一项索引实验应是**短列表内嵌 key 元数据页、共用校验开销，继续保留直接 offset**，再测有界跨 key 列压缩。必须计整个 `.kv` 的目录、定位和检查成本，不能只报告某一列减了多少。

原始服务器结果位于 `/var/tmp/gtron-format-lab-20260907/{posting,value}-canary{,-r2,-r3}.json`；本地 [核心数值转录](../../build/benchmarks/20260907-history-format/canary-transcribed.json) 明确标记为人工转录，未冒充原始下载文件。输入三文件与副本均重新匹配 manifest SHA；manifest 未变，四个同步/部署 unit 均 inactive，停机 hold 仍在。

## 转换效率：已改动与后续顺序

联合 V7 trio 删除验证原先重复计算一次 accessor SHA。已改为复用同一调用中刚完成的完整物理 SHA，继续执行完整 layout 检查和后续每个 posting tuple 的语义比对。独立 checker 不变，其他格式分支不变。测试证明少一个 accessor 文件长度的 hash 读取；这不是设备 I/O 数量或端到端提速百分比。

现有离线新建路径至少解码 hot records 五次；复用已有 cold 的路径至少三次。逐项证据、已有 P2b/P2f 优化、ETL 的真实内存边界及 E0–E6 验收矩阵见 [转换效率审计](history-conversion-efficiency-2026-09-07.md)。

对可从创世构建的新目录，优先隔离验证“顺序持久 history journal → cold”，减少大 Prev 在 Pebble WAL/SST 中的反复写入、再读出与压实。journal 必须与 latest/head 共享可证明的恢复前缀，先完成落盘再允许对应状态提交；它不是简单将 history 写入异步队列。先保持现有 cold 布局，单独测总写量、恢复和吞吐，再接新 value/index 格式，便于定位收益与回归。

后续候选包括有界 ETL fan-in、按字节准入、顺序 borrowed reader 接入验证/冷热比较、sidecar footer 直接写出，以及同一完整覆盖 trio 的验证摊销。已有完整 SHA、字节比对、fsync、原子发布及回收顺序必须保留。只提高压缩等级或扩大并行度，可能增加 CPU、临时占盘与积压，不能作为固定容量的独立解决方案。

## 验证状态

本地 `go test ./core/state/snapshots ./core/state/pruning ./cmd/history-value-bench -count=1 -timeout=180s` 全部通过，分别为 60.849 s、18.564 s、2.496 s；`git diff --check` 通过。posting 实验另通过 race、损坏/溢出边界测试、语义 fuzz 和旧生产源码基线构建；value helper 也在旧基线独立构建、测试通过。

固定容量是否能覆盖重新追到主网目标高度，仍需不同年代、不同 domain 和大 value 占比的样本，以及完整组合格式的净字节、热尾、所有其他数据库、临时峰值和增长斜率。压缩只能降低增长速度，不能让持续增加的全部历史在有限磁盘内无限保留。
