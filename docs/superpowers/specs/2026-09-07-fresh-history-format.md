# 新数据目录的历史写入与冷格式设计

日期：2026-09-07。状态：设计与独立原型阶段；未选择最终布局，未默认接入节点。

本设计仅用于从创世构建的**新 datadir**，或恢复由同一新格式生成并验证的完整
快照；恢复集必须包含全部 canonical 历史，只有 latest 的快照不满足要求。
不提供现有 datadir 的就地升级、旧新 datadir 格式混用或旧二进制回滚承诺。当前
生产 gtron 继续停止；文中的启动、重放和 A/B 步骤是后续验收设计，不授权本轮
开启生产同步。当前库的停机维护仍遵循
[离线空间回收协议](2026-09-07-offline-history-space-recovery.md)。

用户要求保留全部历史状态查询。优化对象是历史的物理表示、重复副本和处理
成本，不能缩短历史窗口、丢弃旧值，或把历史查询改成从创世重新执行。TRON
protobuf、P2P、共识、执行结果和外部 API 语义保持一致。

## 1. 目标与已知边界

目标分别计量，不能互相替代：

1. 减少大 history value 先进入 Pebble WAL/SST、再读出转冷和删除所产生的
   写放大、读放大、排序及临时峰值。
2. 降低稳定保留的完整历史、字典和索引的合计物理大小。
3. 保持 point-in-time、交易边界、区块范围、逻辑前缀和 unwind 查询的正确性，
   并为解码、I/O 和内存提供可执行的上限。
4. 在崩溃、空间预算拒绝、取消、重试和 reorg 后，始终能找到完整且一致的
   canonical 历史与最新状态，不允许进度领先其依赖数据。

“journal 绕过 LSM”主要改善第 1 项，并不自动改善第 2 项。换索引可能只影响
较小的 sidecar；差分压缩也可能增加随机读取成本。只缩短 cold 转换时间的改动
应作为效率改进验收，不称为固定磁盘容量问题的最终解决方案。

有限磁盘无法无条件永久保存无限增长的全历史。新格式必须记录真实每块/每
交易净增长、可用空间和处理积压，据此给出可运行区间及停止接收新工作的
条件；不能承诺重同步一次后永不再满。此次不以扩盘、迁走 MySQL 或删除历史
作为设计成立的前提。

## 2. 当前数据的所有权与已完成优化

| 内容 | 当前表示及作用 | 本设计允许的变化 |
| --- | --- | --- |
| 历史旧值 | `StateDomainChange.PrevExists/Prev` | 无损编码、分段去重、带完整锚点的差分；不能删逻辑版本 |
| 时间与 key 身份 | txNum、记录顺序、owner/domain/key/generation、StateTxRange | 可编码、字典化和移动位置；必须精确恢复关联关系 |
| 最新状态 | Pebble flat latest account/KV/generation、代码等 | 第一阶段维持现有职责；不能用缺失历史替代 |
| posting、tx index、offset locator | 从完整记录及字典/范围信息派生 | 可以重建和换布局，但新查询入口可用且验证完成前不得移除 |
| 压缩页表、计数和摘要 | 定位、校验和预算元数据 | 可以移至 footer 或一次写成，不能丢失认证与边界检查 |

当前 hot `persistedStateDomainChange` 已不保存 `Next`、逐行 BlockHash、
BlockNum 和 Seq；块信息与记录位置承担共享上下文，block pack 使用 RLP 和
Snappy。冷 V6 的记录已是
`frameLen + keyID + txNum + PrevExists + valueLen + Prev`，完整固定记录头为
21 B，key dictionary 与 StateTxRange 表共享。
因此，不能再次把“删除 Next/重复 owner/hash、引入 keyID”算作新增收益。

当前 checkout 的账户内层 StorageCoreV4 已把 TRC10 maps、权限、投票、
stake/resource 等移到 KV，使用 presence bitmap、默认值省略和 signed varint。
本轮已将账户 envelope V5 接入正常最新状态提交与读取：默认 `EmptyKVRoot`
编码为空串；零代码哈希、`Keccak256(empty)` 和引用内层同一个 32B code hash
使用彼此不同的标记。非默认 root 与不能证明重复的 hash 仍显式保存 32B。
引用必须验证完整 canonical StorageCoreV4 且内外 hash 完全相同；内层 Address、
未知字段和其余 core 字节原样保留，避免让没有 owner 上下文的历史读取失去信息。
V4 envelope 仍可显式读取，旧行不自动改写；V5 的正常 writer 是已实现路径，
不代表生产 datadir 已部署或转换，也不等同于下文尚未实现的 journal 方案。
默认 root 加可省略 hash 的 envelope 每行至少减少 64B；这不是整库或冷压缩后
的节省比例。更改内部 envelope 会改变其参与计算的内部 commitment 字节，
不改变 protobuf/API/共识消息；测试须覆盖历史旧值、代码哈希扫描及 unwind 后
原始 envelope 和 commitment root 精确恢复。

依据：[hot schema](../../../core/rawdb/accessors_state_changeset.go)、
[V6 record](../../../core/state/snapshots/history_key_oriented_v6.go)、
[账户 envelope](../../../core/state/state_account.go)、
[账户 V4](../../../core/types/account_storage_v4.go)。

## 3. 历史查询契约

`GetAsOf(key, T)` 的核心规则是寻找该 key **第一条 txNum 大于 T 的变更**并返回
它的旧值；没有后续变更时回到该视图对应的 latest。不存在和“存在但值为空”
必须区分。generation 是 key 身份的一部分，不能把重建合约或账户 KV 的新旧
世代合并。

还必须保持：

* 一个区块内各交易之间的状态，以及 block-final mutation 的时间位置。
* 相同 txNum 的多条记录的确定顺序；不能假设每 key 每 tx 只有一个 posting。
* 按交易/区块遍历的完整记录集合，以及 unwind 所需的逆序行为。
* 按完整逻辑 key 和逻辑前缀枚举；哈希只能作候选定位，原始身份必须可校验。
* 任意保留高度的账户、存储、代码、resource/reward，以及调用/trace 的历史
  状态视图，遵守现有 hot/cold 组合读取的一致性边界。

历史 code hash 引用的代码内容也必须可读取；不能因为 latest 已换为另一个
hash 就回收旧代码。区块、交易、receipt/log 等关联查询资料不在本设计的删除
范围内。

不能仅保留每块最终状态，也不能看到两个相同 Prev 就直接删除其中一个时间
位置。现有 journal 已过滤无实际变化的写入；重复历史值本身不是无效变更的
证明。新表示应先做到全部原始记录可重构，再考虑单独证明过的逻辑等价变换。

## 4. 先改写入路径，再选择冷布局

两阶段分别设置开关、计量和验收，避免把多个变量同时改变后无法归因。

### 阶段 A：持久 history journal 绕过 LSM

目标是让大 `Prev` 不再先成为 Pebble SST。按 canonical 执行顺序写入有界、
只追加的持久 journal；latest 和必要的小型提交/定位元数据仍由 Pebble 管理。
冷构建先继续产出现有 V6 history/V7 companion，保持值编码与查询 oracle
不变，单独验证省掉的 LSM 写入、读取和物理回收成本。

这个 **durable journal 尚未实现**。它不同于当前
`core/state/domain_change_journal.go` 的内存变更捕获机制。后者捕获 tx 前值，
不提供独立于 Pebble 的持久提交协议。

近期 journal 必须可查询，不能等转冷完成才开放历史。可以保留有界的派生
key/posting 定位层，指向已提交 journal 的 segment/offset；索引水位落后时
从经过提交认证的 tail 读取。tail 过大时施加背压，不能让 fallback 退化为
无限扫描。派生索引可以留在 LSM，第一阶段的重点是大 value，而非声称完全
不再使用数据库。

journal 切段以完整 block commit 为单位，同时限制实际字节。选择 chunk、
并行数和转冷阈值时同时计入 journal + 新 cold + ETL + 尚未回收旧文件的峰值。
单个合法块超过通常预算时应停止纳入批次并明确报告所需资源，不能截断块或
跳过其历史。不能通过不断增大待转冷窗口来掩盖处理能力不足。

### 阶段 B：在同一 journal 输入上比较新布局

阶段 A 恢复与查询验收通过后，再对相同不可变输入比较 key-major、tx-major、
full-value、delta、dedup 及 sidecar 方案。新 writer 可直接输出压缩 frame 与
footer，减少 staging 文件组装复制；key/posting 的重排成本仍须计量。

**不预定 delta 胜过 full-value，也不预定 dense 胜过 indirect。**按完整组合
实测选型；若收益不足，保留现有表示。不同 domain/段可以在明确格式标记和
reader 支持下选择不同编码，但不能产生无法计费、恢复或验证的隐式混合。

## 5. 跨 journal/Pebble 的写入与恢复协议

以下是阶段 A 必须满足的协议草案，尚非现有实现。前置条件是一个 canonical
block 的 latest 更新、head/Execution/Finish 以及 history commit 绑定可在
**同一原子 Pebble batch** 内提交，并完成 WAL Sync。必须先审计真实提交路径；
如果现有路径把这些内容拆成可能单独持久化的多个 batch，应先改造原子边界，
或另行设计经过故障验证的 redo/事务协议。不能仅增加一个末尾 marker 就声称
跨文件原子性成立。

1. **准备**：执行产生完整 block 的历史和 latest 更新，但不发布新 head 给
   reader。journal frame 记录格式/链身份、epoch、block/parent hash、tx
   范围、记录数、长度、最大记录及 payload 摘要；frame 有长度和完整性校验，
   可识别截断尾部。格式身份包含 genesis、fork/domain schema 身份，不能仅凭
   文件名或数值 tx 范围混用不同链。
2. **先持久化历史**：完整 frame 追加到 journal，fsync 成功后取得准确的
   segment/endOffset/digest。新建/切换日志时同步必要的目录项。失败不得
   发布 latest/head；记录原错误，不让后台无限重试掩盖停滞。
3. **提交 latest 与绑定**：把 latest 更新、canonical head/stages 与
   `HistoryCommit(epoch, blockHash, txRange, endOffset, digest, parentCommit)`
   放在同一原子 batch，随后 WAL Sync。Sync 的返回若不确定，必须重开并从
   已持久化绑定判断结果，不盲目重新应用状态更新。
4. **开放视图**：只有第 3 步成功后，新 head 才可被读取/服务。reader 同时
   固定 latest snapshot、canonical/journal commit 和 cold manifest generation；
   不把新 latest 与旧 journal 或不相干的 cold 世代拼接。

`HistoryCommit` 是拟议的逻辑元数据名，不是已经分配的 rawdb schema key。
实际接入必须通过 `core/rawdb/schema.go` 与 accessor 定义，不散写前缀。

| 崩溃位置 | 恢复行为 |
| --- | --- |
| journal frame 未完整或 fsync 未成功，DB 未提交 | 保持旧 head；尾部不对查询可见，验证后回收未提交尾部 |
| journal 已 fsync，DB 未提交 | 完整但未提交的 frame 作为 orphan，不推进 canonical；重试不重复应用 latest |
| DB batch 是否持久化未知 | 读取原子提交绑定；存在则验证其 journal 或下面定义的 cold 替代绑定，缺失则保持旧提交边界 |
| DB 已提交，journal 丢失/摘要不符且无有效 cold 替代绑定 | 失败关闭并报告损坏；不能猜测旧值或继续同步 |
| cold 文件完成但未发布 | journal 仍权威；只有经引用审计确认的 scratch/orphan 才可回收 |
| cold 已发布而 journal 尚未回收 | 暂时重复，查询一致；恢复后幂等继续回收 |

重启必须检查提交链、父块/epoch 及 frame 边界。正常追加日志不得在已提交区间
就地重写。批量 group sync 只有在“所有被声明已提交的块依赖历史均已 fsync”
和原子提交前沿可证明时才允许，不能把 fsync 批处理变成 history 落后 latest。

### Reorg 与转冷

未稳定尾部可以发生 reorg。先用已提交历史计算 rewind，所有需要回退的 latest、
commitment、stage、canonical head 与 journal 可见提交链也必须放在同一原子
恢复边界内提交；不能先持久化 latest 回退再单独切 head。若无法使用单 batch，
须先设计并验证完整 durable redo 协议，中途不开放不一致视图。新分支使用
独立 epoch/分支绑定，不能
仅按复用的 txNum 混入旧分支。旧 reader 的固定视图完成前保留其依赖文件。
只清理不再引用的 orphan 分支，不删除 canonical 全历史。

转冷只选择已提交、canonical 的连续完整区块，满足
`block <= min(solidifiedBlock, headBlock - historyWindow)`；head 不足窗口时
没有候选。仍需允许 reorg 的尾部不能纳入。候选输出必须具备全记录重构与
companion 一致性证明。顺序为：

1. 按真实 bytes 预算构建完整输出，fsync 文件及必要目录项。
2. 核对源 journal commit、链/范围及全部记录与候选读取的等价性，验证全部
   字典、locator、postings、offset 和校验和；发布不能只依赖端点或抽样。
3. 原子发布带明确新格式身份的 cold manifest，并同步目录。
4. 将 covered 前沿及**替代绑定**一起原子持久化并 Sync：源 journal 的
   commit/range 和规范逻辑记录摘要，对应哪些已完整验证的 cold 精确 refs、
   checksum、chain/epoch、manifest 身份/digest。这样 HistoryCommit 原先引用
   的 journal offset 不会在合法回收后变成无法解释的悬空引用。
5. 查询与恢复确认该替代绑定可解析并覆盖全部源提交，等待旧 generation/journal
   reader 引用释放后，才删除完整且全部被覆盖的已关闭 journal segment，
   并同步目录。

恢复先解析已持久化的 cold 替代绑定；未覆盖的提交仍必须验证原 journal，
无 journal 且无有效替代绑定才判为损坏。后续 cold merge 也须验证等价内容、
发布新对象并持久化更新后的替代绑定，之后才能回收旧 cold。证明链必须可
解析、可合并，不能依赖已经删除的中间文件或清单，也不能无限增加启动成本。

跨越 covered 边界的 journal 文件保留，不能按时间猜测删一半。manifest 发布
失败只留下重复数据；删除失败不推进“已释放空间”的报告。历史完整性与物理
回收分别报告。外来文件、修复文件和未绑定的缓存不能冒充同一 writer 事务的
可信证明。

## 6. tx-major 与 key-major 的权衡

| 维度 | tx-major：按交易/记录顺序存 values | key-major：同 key 版本相邻 |
| --- | --- | --- |
| 写入与 cold 构建 | 贴近执行和 journal，values 可顺序流出；postings 仍需重排 | 需要按 key 重排 values 或索引后间接读源，可能引入外排、随机读和额外复制 |
| block/tx range、unwind | 连续范围读取，恢复顺序直接 | 依赖逆向 tx index/locator 聚合多 key，可能增加页读取与排序 |
| 单 key 历史、GetAsOf | 先查 posting，再跳到 value；相邻版本可能分散在多页 | 同 key 局部性更好，锚点与后续版本可放近；不能假设都在一页 |
| 压缩 | 不同 key 混排，独立小页可能看不到远处重复 | zstd 可直接利用同 key 稳定字段，可能减少显式 delta 的收益空间 |
| key/prefix 枚举 | 需要完整逻辑字典与 posting 层 | 仍需要逻辑字典与时间筛选；物理连续不等于可省全部索引 |
| 内存和临时空间 | 可做到页/块流式，key 索引仍要有界 ETL | 禁止把全链所有 key/values 放内存；需字节限额分区、外排及受控 merge |

key-major 的压缩收益必须连同 tx 逆索引、locator、字典、重排临时文件和查询
代价一起计算。tx-major 的“顺序写一次”也不包含后续冷 merge 的重写。二者
都应限制 fan-in、页 cache 和超大 key 的单次版本物化；热点 key 不能击穿预算。

## 7. 已实现的独立原型与尚未实现的部分

以下原型均有自己的入口/格式标记；生产 registry、正常 reader、builder 和
pruner 没有默认接受这些格式。它们是实验工具，不是可直接替换生产文件的包。

| 原型 | 已实现 | 尚需证明/接入 |
| --- | --- | --- |
| [tx index](../../../core/state/snapshots/experimental_history_index.go) | 对连续 ordinal 做省略；restart frame；常量/varint/bit-pack 列；dense offset 与 sparse offset；真实序列化和查询 | 真实完整索引/组合大小与 I/O、分页读取、writer 和 manifest 版本接入 |
| [posting 与共享 locator](../../../core/state/snapshots/experimental_history_postings.go) | 保留重复 tx；128-posting frame；dense 或 indirect；共享 sparse record locator；边界/CRC/查询检查 | 完整 locator 成本、稀疏定位读取放大及生产 reader 的有界内存 |
| [posting 只读实验](../../../core/state/snapshots/experimental_history_postings_bench.go) | 与 V7 oracle 比较实际样本；报告 locator 全量成本和被跳过的 oversized key | 预载 posting 的计时不包含字典 lookup/value hydration；需端到端查询验收 |
| [value 实验](../../../cmd/history-value-bench/README.md) | 整 V6 流的前后缀差分、多段 copy/literal、整值去重；相同 zstd/128 KiB 下实际 encode/decode，完整字节 equality | anchor/value locator、新 offset companion、实际随机访问、生产 writer；不能直接替换 `.seg` |
| durable journal | 本文的恢复协议草案 | 尚未实现、没有故障或吞吐验收结果 |
| key-major value 布局 | 本文列出评估维度 | 尚未实现完整 writer/reader，不声明优于 tx-major |

原型的 in-memory 序列化或打开时全量验证不能直接当作生产容量上界。生产化
要改为受控 ReaderAt、分页元数据和逐帧校验，并保持取消与错误路径有界。

### 真实样本证据与后续采样

真实 canary 源范围为 txNum `1734454193..1734459458`，不可变源 history 名为
`history/state-domain-change-1734454193-1734459458-ffa7340839b2bddf.seg`，须与
对应 companion 一起固定身份。首个只读 canary 已完成，具体压缩比、延迟、
峰值内存及完整原始输出由独立实验报告记录，本文不替代该报告。此前维护的
InputBytes 是保守计数，不等于
V6 解压流长度，也不等于可回收的物理空间。

| 证据项 | 所需证据 |
| --- | --- |
| value 体积与 domain 分布 | 原 trio SHA/bytes，V6 raw bytes，各 codec 实际容器 bytes，全部解码 SHA 一致 |
| posting/tx index 选择 | 同一完整或明确抽样集合，dense/indirect 全成本，locator bytes，oracle equality |
| 查询收益/回退 | 相同请求集的冷/暖 cache 延迟、实际页/bytes、CPU/RSS、最坏有界扫描 |
| 新数据目录容量 | 完整组合的 steady bytes、构建/merge/recovery 峰值，跨多个历史时期的增长率 |

结果只在明确测试分布内有效。canary 不代表全链；大段后续可采用分层 tx
窗口和同 key 连续版本样本。抽散 records 会破坏 delta 邻接关系；key 均匀
抽样也不等于 byte 加权样本。不得把抽样后的局部验证声称为整段完整 SHA/
语义认证，或把样本最大值称作全库最大值。

### SystemDelegation 的有序关系分解候选

首个 canary 已在固定 SHA 的 trio 副本中完成全部 21,747 条记录、10,556 个
key 的只读分类，`ReaderMatchesRaw/InputUnchanged` 均通过。
`SystemDelegation` 的 `drax-0-aggregate` 类包含 39 个 key、68 条记录，其中
56 条 present 且全部 native，Prev 合计 48,242,830 B；`dr-v1-resource` 类有
68 个 key、68 条记录，其中 55 条 present，Prev 合计 16,820 B。这个完整
32 块样本没有 `dri-` 或 directional 类记录。

最大的 KeyID 10407 已确认为 `kv-latest/SystemDelegation/drax-0-aggregate`，
27 B key、generation 0；txNum `1734457660..1734458067` 内的 10 条记录均
present/native，Prev 累计 47,318,560 B，单值最大 4,731,955 B。第二大的
同类 KeyID 10405 有 5 条记录、Prev 918,495 B，单值最大 183,721 B。
这些是该样本内的 keyID，不作为跨段全局身份，也不输出完整账户大值。

**这证明了当前 32 块样本由少数巨大 legacy aggregate 历史值主导，不证明
全链具有相同分布。**“legacy aggregate”是 proposal 前的逻辑类别，不表示
其 native 编码过时。具体 From/To 列表项数和 unknown 字节占比尚未逐字段
统计；同 key 多段 copy/literal 的实际收益支持继续验证有序关系分解，不能
代替该结构的完整实现与容量验收。详细实验证据见
[实验报告](../../dev/history-format-experiments-2026-09-07.md)。

当前同域内有多种布局，`IsNative=false` 本身不能判定为旧版或异常：

| 逻辑 key | 当前 value | 候选适用性 |
| --- | --- | --- |
| `dr- || from || to`，及带锁状态字节的 V2 key | 单对资源余额、到期时间等的 JSON | 已按关系拆行；先实测，不当成巨大 peer 列表 |
| `dri- || account` | 顺序拼接的 21 B receiver 地址 | 若真实样本存在巨大行，可研究有序页；当前非测试代码检索未发现该 writer 的调用者，不能假设它正与另一列表双写 |
| `drax- || 0x00 || account` | native `DelegatedResourceAccountIndex`，包含 Account、FromAccounts、ToAccounts、Timestamp、未知字段 | 已确认是本样本的大值类别；真正增删时仍编码并写回整行，是有序页/关系增量候选 |
| `drax- || 0x01..0x04 || anchor || peer` | Account=peer、Timestamp 的小 native 行 | 已是逐关系索引；不能把再次拆分计作收益 |

schema 定义见 [rawdb schema](../../../core/rawdb/schema.go)，rooted 映射见
[rooted key](../../../core/rawdb/rooted_key.go)。原生 aggregate 的 repeated bytes
编码按原顺序写入每个地址长度和内容，保留重复项与未知字段；
[专用 codec](../../../core/state/statecodec/delegation_index.go) 只减少分配和反射。
[委托缓存](../../../core/state/delegation_cache.go) 已跳过已存在关系的无变化写入，
但真实增删仍把整个 aggregate 交给 SetAccountKV。不能把已完成的缓存优化再
算作历史体积优化。

这个候选延续既有
[P0b 有序页与事务增量设计](2026-09-05-28m-sync-performance.md)，尚未实现。
在**新 datadir 的物理适配层**保存 header、两个有序关系集合及 membership
定位信息：可比较“一 occurrence 一 key”与有界页/COW 两种表示。普通唯一关系
增删只改变少量边或页和必要 metadata，历史按每个 txNum 记录这些变化；原
逻辑 key 的完整 native value 仍由适配层精确物化。物理页不能混入逻辑前缀
枚举或作为额外 commitment 叶子。每 tx 的双向关系、存在性及顺序变化必须
属于同一原子状态/历史恢复边界。

以下语义不能因为从创世重建而省略：

* 新关系追加到尾部；重复添加不增加条目；删除保留其他元素的相对顺序，
  删除后重加排尾。内部 slot/序号可有洞，但 proposal 转换必须按**当时存活
  列表**重新从 1 编号；不能把内部 sequence 直接当作 Java timestamp。
* header 保留 Account、Timestamp、unknown、缺失/存在空 aggregate 的区别。
  generic codec 接受重复及任意 bytes，不能暗中转成 set。可以证明合法创世
  及交易序列保持唯一性后走唯一关系快路；超出该证明的输入需 occurrence ID
  或保真回退，不能静默丢数据。
* 正常 freeze/unfreeze 禁止 self-edge，但 generic accessor 的异常输入语义
  不能凭此假定为相同。当前 Go 与 Java 对人工重复列表的移除行为不同，不能
  把现有 Go 异常行为当作 Java oracle；此类差异单列验收，不夹带修改共识。
* 保持原 proposal/fork 边界、交易余额和到期时间更新、错误与 receipt/费用
  结果。不能提前调用现有 `ConvertDrAccountIndexLegacy`，把旧 aggregate
  逻辑替换成 `drax-1..4`。当前 Go V1 API/历史 API 直接读取 aggregate，而
  Java 在 aggregate 缺失时会回退 directional 并按 timestamp 排序；新物理
  层必须仍支持原逻辑查询，API 对照另外覆盖此既有差异。
* `dri-` 当前被 V2 查询和可委托金额计算读取；未证明它的写入来源、完整性、
  列表顺序及重复语义前，不认定为可删除的 derived 副本，也不与 `drax-`
  强行合并。

Go 转换、读写和历史 API 见
[delegation store](../../../core/state/delegation_store.go) 与
[TronBackend](../../../core/tron_backend.go)。本地 Java 对照版本为
`75c0304681cd32804c2f3c030468a88bdf3d8e32`：
`DelegatedResourceAccountIndexStore.convert/getIndex` 与
`FreezeBalanceActuator/UnfreezeBalanceActuator`；正式实现须把这些顺序、fork
和查询行为纳入固定输入的 Java/Go 对照测试。

历史仍须在任意保留 tx/block 边界恢复完整双向列表。边/页 checkpoint 和
delta 链必须有明确最大重放记录数、bytes 与随机 I/O 上限；逐边存储的 key、
双向索引、排序 locator、页 metadata 与 anchor 全量计费。完整列表 API 仍有
O(列表长度) 的输出下界；如 commitment 保持原完整逻辑叶子，则变化块还需
处理其完整行字节，不能承诺所有写入或 root 计算都为 O(1)。最终只在真实
大行样本的完整字节/语义重构、全部历史查询、回滚及容量 A/B 通过后选择此
路径；它与通用 copy/literal 压缩分别计量，不相加外推节省比例。

## 8. delta、dedup 与 locator 的完整成本

若真实实验支持 delta，首版只允许**段内、有界、可独立恢复的链**。当前两个 same-key delta
原型按每 key 最多 32 次出现设置 full anchor；这是实验参数，不是最终格式
承诺。实际值还应根据查询上限和热 key 分布比较其它间隔。

需要纳入正式格式和 bytes 统计的内容包括：

* 每 key 或每版本组的 anchor ordinal/offset/长度与编码标记。
* 找到 anchor 的目录或 locator，以及 key/时间到版本组的索引。
* 存在性、变更顺序、generation、校验与不同 codec 间的明确边界。
* 超大 value 的分块/长度检查；不能因只需一条记录就分配任意大小的内存。

最多 31 个前驱版本不等于最多读 31 个廉价字节；每次定位可能再经过 sparse
locator、压缩页解码和远距离 I/O。必须测组合最坏成本。tx index 的“最多 256
个交易”也不保证最多 256 条记录，需另设 record/byte 预算。不得用一个 bounded
frame 的说法掩盖无界 value 或目录加载。

整值 dedup 应计算 unique-value table、ID、locator 与校验的合计成本。哈希
可以作候选，命中仍校验实际 bytes；缺失和空值状态不能仅由一个 value ID
推断。首版不引入跨段无限依赖图：跨段共享若有价值，必须另设依赖 pin、
rebuild/merge 和回收协议，不能让清理一个旧段损坏后续全部历史。

field delta 也不是“解码 protobuf 后重编码大致相同”即可通过。第一阶段实验
以原 bytes 为 oracle；若新 genesis codec 有不同但等价的规范化表示，必须
单独验证完整账户/KV 语义、未知字段、默认值和外部 API，不与无损 byte delta
的证据混在一起。

## 9. 空间预算、背压和格式选择

预算须同时覆盖持久数据与临时重叠：

`live latest + committed journal + retained cold + derived indexes + new output
+ ETL/merge scratch + pinned old generations + recovery/control reserve`。

区间估计不是实际输出峰值保证，压缩比不能在写入前盲目信任。writer 应按
实际 bytes 和文件预分配检查预算；不能因拒绝某次输出而破坏 commit/manifest
的持久化尾部。空间不足时在明确的提交边界停工，保留已提交历史和控制文件
余量。外部 MySQL 等同卷服务仍会改变空闲量，本工具的门槛不是全卷配额保证。

新 datadir 也不意味着能在当前磁盘同时放下旧库和完整重建的新库。若切换需要
两者并存，旧库占用必须另外计入重建峰值；不满足时，这份格式设计不能冒充
已经解决了切换空间。重建与服务切换需有单独的容量/恢复计划，实验阶段不删除
现有数据库。

选择方案时比较完整保存成本和必要资源，不直接相加各原型的百分比。同 key
聚簇可能已经让 zstd 吸收重复，显式 delta 可能不再合算；indirect 省下的
posting offset 也可能被 locator 和查询放大抵消。可以保留 full-value 或 dense，
也可以只选部分优化，但每项结论须来自同输入对照。

## 10. A/B 验收与接入门

### 固定输入和实验隔离

实验固定源 manifest/trio 或 journal commit 的 SHA、范围和代码版本。只在
独立 scratch/new datadir 运行；本轮不启动生产 gtron，不修改生产 manifest，
不为模拟冷 cache 清空生产机器的系统页缓存。标明编译器、并行数、内存预算、
介质和 cache 条件，交替运行固定 A/B 二进制并保留完整原始日志。

| 实验轴 | 控制项 | 验收证据 |
| --- | --- | --- |
| LSM history vs durable journal | 同执行输入、相同冷格式与查询语义 | 真实设备/调用层 I/O 分列，处理吞吐、CPU/RSS、峰值占盘；恢复矩阵通过 |
| tx-major vs key-major | 同全量记录和 compression 参数 | 完整 history+双向索引+字典+locator bytes，构建/merge 资源和混合查询代价 |
| full vs delta/dedup | 同布局、同原记录及页大小 | 所有 bytes 精确重构，显式 locator/anchor 成本；最坏读取与内存限额不被突破 |
| V7 vs dense/indirect index | 同 posting/tx 集合与查询 | 全量 locator 计费，查询结果/order 一致；不只比较预载解码时间 |
| 最终组合 vs 基线 | 包括热尾部、cold、derived 和恢复文件 | 稳态净增长和转换峰值独立统计；不以局部百分比推算组合收益 |

查询集覆盖历史最早/最近边界、存在/删除/重建 generation、空值、单次与高频
key、同 tx 重复记录、跨 anchor/frame/page/segment、逻辑前缀、范围与 unwind，
以及账户/合约/resource/reward 的真实历史调用和 trace。性能同时报告 p50、
p95、尾部/最大工作量、读取页/bytes 和 RSS，不能只报告均值。

故障注入至少覆盖每次 append、file/dir sync、DB batch/Sync、manifest rename、
stage publication 和源回收前后；覆盖截断、坏长度、重算外层 SHA 后的语义
损坏、错误字典/key绑定、overflow、空间拒绝、取消及 reorg。每个位置重开
后必须满足：latest/head 与 history 同一提交边界，所有已提交 canonical
历史仍可查询，重试幂等，错误会有界返回。

接入必须同时满足：

1. 正确性与上述恢复测试全部通过；没有以抽样验证代替删除前全量验证。
2. 在已声明输入/查询分布上，目标资源收益可重复，且各项退化均有明确上限
   并符合事先确定的服务预算；实际阈值在实验前记录，不事后挑选有利指标。
3. 对多个历史时期及大小/热点分布复测，报告外推范围；若不足以覆盖目标
   新 datadir 的容量预算，继续停留在实验状态。
4. 新 datadir 写入明确格式/链身份，旧/未知格式拒绝打开。未经过上述门槛
   的 codec 不加入默认 registry，也不产生隐式自动迁移。

现有相关测试及独立原型已用于验证局部实现；它们不是 durable journal、
key-major 完整格式或长期磁盘容量的验收结果。实施顺序与 passes 的补充分析见
[历史转换效率审计](../../dev/history-conversion-efficiency-2026-09-07.md)。
