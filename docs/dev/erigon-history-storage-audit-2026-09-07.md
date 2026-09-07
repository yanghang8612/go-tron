# Erigon 热历史、冷文件与积压控制的主源核验

日期：2026-09-07。只读审计；没有访问生产、启动同步、修改运行代码或迁移数据。
本文只负责 Erigon 对照，不替代 gtron 的真实业务字节统计或积压诊断。

## 结论

**Erigon 3 仍把近期状态变更历史写入 `chaindata` 的 MDBX 表。**它不是把所有
历史直接写冷文件、从不经过数据库。区别在于：按 domain 分表、按 txNum 保存
旧值，持续把完整 step 转成可查询文件，再裁剪已被文件覆盖的热表；稳定 latest
也大量保存在 domain 文件，MDBX 主要留下文件边界之后的增量。

对 gtron 最值得单独验证的借鉴是：**先快速冻结成可查询文件并安全裁剪热副本，
再在后续 merge 中优化压缩**；配合小型逻辑状态单元、按 key 聚簇、裁剪调度和
提交/文件读取边界。我们的“独立持久 history journal 绕过 LSM”是另一项设计
候选，不能说成 Erigon 现成路径。

## 版本和证据身份

本次浏览官方最新 release，确认当前稳定版为 **v3.6.0**，2026-08-24 发布，
commit `ecc2ad992b86b75f5aa26368918b3f428cc329e8`。本文关于当前实现的行号
均指该版本，而非滚动 `main`。[官方 release](https://github.com/erigontech/erigon/releases/tag/v3.6.0)

本地已有两个参考 checkout：

| 参考 | 本地位置与身份 | 使用限制 |
| --- | --- | --- |
| E2 时代实现 | `/Users/asuka/Projects/ledgerwatch/erigon`，`d3c3be9c914f1fcd71b8b243855b3362179de285`，2023-06-15 | 用于确认旧 changeset/history-index 路径；不能代表每一个 E2 配置 |
| E3 开发 checkout | `/Users/asuka/Projects/erigontech/erigon`，`5bf2fe2835981a479e41f9950cdda97217b7517b`，2026-08-04 | 早于当前 release，只用于导航，再以 v3.6.0 核对关键源码 |

v3.6.0 的相关源码只下载为文本到
`build/benchmarks/20260907-erigon-reference/v3.6.0`；
[来源清单](../../build/benchmarks/20260907-erigon-reference/v3.6.0/source-manifest.json)
记录 URL、大小与 SHA-256，并保留没有找到的候选路径及 HTTP 404，避免把缺失
文件当作已核验内容。没有编译或执行 Erigon。官方数据库文档更新于 2026-09-01；
其体积和性能介绍是上游测量/陈述，不是本机 A/B。

## Erigon 2 与 Erigon 3 不能混用的概念

E2 的传统路径使用 `PlainState` 保存 latest，`AccountChangeSet` 和
`StorageChangeSet` 保存每块修改前的值；writer 实际通过 `AppendDup` 写 MDBX。
`AccountHistory/StorageHistory` 保存分片 Roaring bitmap，用块号定位变化，再
回 changeset 取旧值。记录是 block-oriented，不是 E3 的逐交易 domain history。
[E2 writer](https://github.com/erigontech/erigon/blob/d3c3be9c914f1fcd71b8b243855b3362179de285/core/state/change_set_writer.go#L127)，
[当前保留的旧 schema 说明](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/kv/tables.go#L901)

同一个 2023 E2 checkout 已有 `historyV3` 条件分支：启用时调用 aggregator
裁剪，否则按传统 changeset 表裁剪。因而“E2 全部如此”或“historyV3 这个名称
必定表示 E3 二进制”都不准确。[E2 裁剪分支](https://github.com/erigontech/erigon/blob/d3c3be9c914f1fcd71b8b243855b3362179de285/eth/stagedsync/stage_execute.go#L835)

E3 还保留 `ChangeSets3` 这类服务近期 reorg 的数据，其裁剪受 MaxReorgDepth
和显式调试选项控制；它与保留全部历史查询的 `AccountHistoryVals` 等表是两项
职责。不能看到 `ChangeSets3` 只留很短尾部，就推断 archive 没有旧状态。
[v3.6.0 stage 裁剪](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/execution/stagedsync/stage_execute.go#L524)

## E3 的正常热写入路径

1. `DomainBufferedWriter.PutWithPrev` 先把旧值交给 history writer，再记录
   latest 更新；删除路径同样捕获旧值。
2. history writer 用 ETL collector 收集有序写入。小值热表逻辑为
   `key → (txNum, Prev)` 的 DupSort；大值使用 `(key, txNum) → Prev`。
   另一张表保存 `txNum → key`，支持转冷和裁剪遍历。
3. `Flush(ctx, rwTx)` 通过 ETL `Load` 把历史值及索引写入传入的 MDBX RW
   transaction；domain latest 随后也 flush 到该 transaction。这里的
   collector/内存暂存不等于已持久化的独立 history journal。
4. temporal transaction 包装器固定文件视图，最终提交底层 MDBX transaction。
   存在 nosync 入口和异步提交模式，不能把每次内存修改或 collector flush
   都称作一次独立 fsync。

对应源码：
[domain Put/Flush](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/domain.go#L417)，
[history AddPrevValue/Flush](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/history.go#L387)，
[temporal 提交](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/kv/temporal/kv_temporal.go#L527)。

因此，官方文档说“较老历史不在 MDBX”应理解为**转冷后的归属**，不是否定
上述近期历史热表。[官方数据库布局](https://docs.erigon.tech/fundamentals/database)

## 冷文件布局与聚合顺序

| 层 | 主要内容 | 作用 |
| --- | --- | --- |
| `snapshots/domain` | 每个 domain 的 key/value 文件 | 保存文件覆盖范围的稳定 latest；多个文件与热增量组合读取 |
| `snapshots/history` | 各 domain 的旧值历史，当前源码使用 `.v` | 精确时间点取值 |
| `snapshots/idx` | key 到变化 txNum 序列，当前源码使用 `.ef` | 查找/交并历史交易集合 |
| `snapshots/accessor` | value、inverted-index 的随机定位结构 | 避免每次顺序扫描文件 |

官方概览有时把不可变文件统一称为 `.seg`；不能据此把 E3 history 的具体扩展名
写成 gtron 的同一种 `.seg` 格式。[官方目录说明](https://docs.erigon.tech/fundamentals/database)

当前默认一个 step 为 **390,625 txNums**，完全冻结文件的标准上限为
256 steps；旧配置是 1,562,500 txNums × 64。step 不是区块数，也不是字节
上限；domain 的上限还可另行配置。
[v3.6.0 config3](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/config3/config3.go#L22)

`History.collate` 扫描热表的 `(txNum, key)`，用 ETL 重排为 `(key, txNum)`，
同 key 的历史值顺序输出；同时生成该 key 的 txNum 序列。它不是“没有排序”，
而是把排序与读取单位设计在 temporal schema 上。
[collate](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/history.go#L573)

首次 collation **关闭页级压缩**，目的是降低冻结延迟。account/storage history
本身采用 CompressNone；code history 可以使用另外的 word-pattern 压缩，不能
笼统说所有文件完全不压缩。后续 merge 使用完整 domain 压缩配置；account
history 的默认配置包含 64 values/page。是否适合 gtron 的几 MB aggregate，
必须重新测量，不能直接搬用页内值个数。
[初次配置](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/history.go#L554)，
[domain schema](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/statecfg/state_schema.go#L210)，
[history merge](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/merge.go#L791)

builder 先产生完整文件和 accessor，再集成到文件集合。后台循环优先构建
多个小 step 文件，使热数据尽早可裁剪，然后启动 merge。merge 按对齐区间
合并，而非每新增一个 step 就重写全历史。
[builder 顺序](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/aggregator.go#L2340)，
[merge range](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/merge.go#L99)

## 发布、裁剪与空间复用

以下是所读路径中可直接确认的门槛，不是对 Erigon 全部崩溃恢复行为的重新证明：

* 正常 collation 只处理完整 step，并检查配置的 reorg 深度；后台还拒绝在
  已有冷文件与 MDBX 首条数据之间的明显空洞上生成新文件。
* 普通 archive 历史的热裁剪上界取 **history 文件末端、相应 inverted-index
  文件末端、调用者上界的最小值**。只有 history 文件没有索引，或索引先到而
  value 尚未到，都不能按较快一侧推进。SnapshotsDisabled 是另外的保留策略
  分支，不能拿它作为 archive 的依据。
* compressor 默认先完成临时文件、文件 Sync，再 rename；builder 打开产物、
  构建 accessor。可见文件集合以跨 domain 的一致 generation 发布。
* 旧文件被标为 retired 后，最后一个固定该 generation 的 reader 释放时才
  删除；文件数刚减少或引用刚更新，都不等于磁盘已经释放。

源码：
[reorg 门](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/aggregator.go#L1128)，
[hot history 裁剪上界](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/history.go#L1028)，
[Sync 与 rename](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/seg/compress.go#L383)，
[可见 generation 与 reader 回收](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/aggregator.go#L1833)。

MDBX 没有 LSM 的后台 SST compaction，但也不是零写放大。旧 MVCC reader 可
阻止页面复用，E3 的 commitGate 会在特定写边界协调后台只读事务。删除热行后
可以复用数据库页，**不等于同量字节立即归还文件系统**；MDBX 的文件缩小取决于
geometry、可回收页和打开方式等条件。
[E3 commitGate](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/aggregator.go#L1746)，
[MDBX size geometry](https://libmdbx.dqdkfa.ru/doxygen/group__c__settings.html)。

## 积压、调度和背压的实际边界

E3 在初始同步与链尖路径都调用维护，而不是只在初始同步结束时做一次。
`runForkchoicePrune` 的源码直接解释：链尖若不再触发文件构建，文件边界无法
推进，新的 MDBX 数据无法裁剪，热库仍会不断长大。
[链尖维护路径](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/execution/execmodule/forkchoice.go#L903)

具体控制包括：

* 单个后台 build 的 CAS 状态、构建 semaphore、受限 worker 数，避免同一
  builder 重复并发；不代表总磁盘占用有硬上限。
* 链尖按 slot 分配 prune 时间，并根据可裁剪的 domain steps 积压增加预算，
  仍有上限。初始同步给予更激进预算。
* PruneSmallBatches 分批推进，记录进度，并按耗时调整批量；未裁完会在后续
  维护继续。步数与时间预算不等于按字节的峰值空间保证。
* `--db.size.limit` 限制 MDBX 文件，不能覆盖 snapshots、ETL、其他服务和
  新旧文件重叠。所审路径没有提供“一个限额保证全部 datadir 永远不满”的依据。

源码：
[后台构建限制](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/aggregator.go#L2212)，
[批次裁剪](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/db/state/aggregator.go#L1428)，
[stage 预算](https://github.com/erigontech/erigon/blob/ecc2ad992b86b75f5aa26368918b3f428cc329e8/execution/stagedsync/stage_execute.go#L492)，
[官方 CLI](https://docs.erigon.tech/fundamentals/configuring-erigon)。

还需区分两种 prune：**把已安全转冷的热副本删掉**可以保留全部历史；**删除
保留窗口之前的冷历史文件**会缩短历史能力。用户要求对应 archive；不能拿
full/minimal/blocks 的较小磁盘数字当作同等历史能力的优化。v3.6 才落地的
旧 snapshot 窗口回收主要针对 pruned 模式，不应移植为 gtron 的全历史删除。
[官方 pruning modes](https://docs.erigon.tech/fundamentals/pruning-modes)

## 性能证据与能作出的判断

官方 v3.6 release 报告：特定 state snapshot merge 的压缩阶段约提升
2–3 倍、字典构建 merge 约提升 1.5 倍。该 release 同时改成更直接的 commitment
存储，以文件变大换读取和 merge 成本。这说明上游也在做体积/查询/维护的取舍，
不是永远压得越小越快。它们不是 TRON 输入、不是我们的硬件，也不是 gtron
对 Erigon 的对照结果。
[官方 release 的性能范围](https://github.com/erigontech/erigon/releases/tag/v3.6.0)

官方数据库页列出一台配置明确的 Ethereum archive 节点：chaindata 22.75 GB、
总量约 2.17 TB，且排除一些可选历史/cache。这里只用来说明冷热归属的稳态
形状；不据此估算 TRON 完整 archive 大小，也不引用其概括性倍数作为我们的
性能承诺。[官方容量样本](https://docs.erigon.tech/fundamentals/database)

对 gtron 可建立以下独立 A/B，不能直接宣称已经更快：

| 候选 | 希望减少的工作 | 必须计量的代价与验收 |
| --- | --- | --- |
| 快速可查冻结，再 merge 压缩 | 首次冷构建延迟、热副本等待时间 | 临时未压缩冷文件峰值、完整索引/校验、prune 完成水位、后台 merge 能否追上 |
| 大 aggregate 拆有序边/页、每 tx 历史变化 | 每次少量增删重复写几 MB 整值 | 完整历史逻辑列表/顺序/unknown/commitment、metadata 和 anchor 成本 |
| key-major history 与小值/大值专用路径 | 重复解包、同 key 版本跨页读取 | ETL 外排与随机取值、tx-range 逆索引、冷热查询端到端延迟 |
| 裁剪与构建持续调度 | 已可裁剪热数据长时间滞留 | 写入/冻结/索引/裁剪的四个 bytes/tx 水位、失败重试、磁盘预算不足时在提交边界停止 |
| MDBX 或独立 journal | 避免现有 LSM 的部分维护写放大 | 实际全设备写量、事务恢复、旧 reader、查询、峰值和总成本；不能只比较引擎名称 |

最后两列是基于源码的设计推论与验收建议，不是已测结论。Erigon 的 storage
主要按合约 slot 记录小值；gtron 的巨大 legacy 委托 aggregate 是另一种业务
粒度。改变引擎不能自动消除这些整行历史复制。

相关 gtron 证据及设计：
[真实格式实验](history-format-experiments-2026-09-07.md)，
[新 datadir 设计](../superpowers/specs/2026-09-07-fresh-history-format.md)。
