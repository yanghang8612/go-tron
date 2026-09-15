# 自包含引用型历史容器

2026-09-15。用户已要求开始结构重构、评估旧数据迁移，并明确可以接受清空后从创世同步。
本设计接续已上线的认证缓存与独立 GC 密度计费修复，优先改变冷历史的物理表示。
旧设计中的“只限新 datadir”及当时的停机状态不代表当前事实；本方案通过独立 magic 和
旧格式 reader 共存评估迁移。生产切换前必须完成新格式验证，不因接受重同步而立即删库。

## 目标与边界

当前源 v3 已按块桶共享 chunk，但冷构建两遍展开原 pack，并把相同 Prev 再次复制、CDC
切块、摘要和压缩。本方案一次完整认证源 pack，记录 Prev 对认证分块的片段映射，冷端
只保存一份本文件拥有的分块。完整 V6 逻辑流、现有 V7 索引和 accessor 语义保持不变。

范围包括新容器、只读认证 span 导出、单遍源构建、统一 reader 接线、完整语义/故障
验证及固定输入性能测量。第一阶段不改变共识、proposal 生效高度、状态 latest 逻辑键、
commitment，不实现跨文件 chunk store。离线迁移使用逐 trio 的小型耐久 journal；
它只记录发布与回收状态，不复制数据库或保存整份旧仓。

既有热 v3 的承诺是 SHA256(完整原始 RLP)，因此至少一次全文认证仍然必要，不能由
chunk SHA 组合替代。新格式消除重复源扫描和重复冷物化，不承诺所有历史操作为 O(1)。
完整列表 API 仍有输出长度下界；从创世重同步也不自动消除旧协议的大列表语义。

## 源读取与所有权

`IterateStateHistorySpanBlocks` 只接受调用者持有的同一个 pinned `StateHistoryReadView`。
每次回调最多暴露一个完整认证块。字段、RLP、chunk 和整包摘要、块身份、交易范围、
顺序全部通过后才交付元数据和 Prev spans；使用 RLP 编码边界定位，不使用 unsafe 地址差。
对旧 self-contained 编码及 repair 保持现有解析/合并语义，必要时走 owning fallback。

块 API 只提供分块 digest/长度、复制方法和规范行迭代；不暴露可修改的内部 raw。
行保留存在性、generation、逻辑 key、txNum、Seq 与 block 绑定，Prev/Next 不重复物化。
借用行字段仅在行回调内有效，块对象在外层回调结束后失效。取消及所有失败释放私有资源，
不得关闭调用者仍在使用的 view。

共享 v3 连续范围可使用 2/4/8 个 worker 并行认证，回调仍按块有序交付。预检遇到旧格式、
repair 或不连续范围时，在交付任何回调前回退串行；最多 5,000 块，已完成结果与消费中的
块共同受 256 MiB 预算约束，取消或失败必须等待 worker 退出后再释放 view。

## 物理容器

新 magic `GTHREF01`，独立于 `gtcblk01`。虚拟地址空间逐字节等价于原 V6：
header、字典 commitment、StateTxRange、记录固定头和 Prev 的逻辑 offset 均不变。
物理文件包含完整 chunk 数据、定长 chunk 目录及虚拟 span 目录。span 只指向本文件的
直接 chunk 区间，不允许 hot DB key、外部文件、引用链或依赖 DAG。

Writer 提供先 `StoreChunk`、后顺序 `WriteLiteral/WriteSpan`；小元数据 literal 有界合并。
只允许修改已写入的 literal 前缀，供 V6 count 与 tx-range count 回填；不能改引用数据。
chunk 编码使用 raw/Snappy，SHA 后还须在去重命中时核对完整字节。去重元数据、单 chunk、
unique bytes、span 数、逻辑/物理总长均设明确上限，超过预算返回错误，不截断合法历史。

Reader 提供有界缓存的 `ReaderAt`、Close、逻辑长度和完整布局校验。目录落盘并分页查询，
不能启动即加载整段所有元数据。读取 chunk 完整检查长度与 SHA，再把结果复制给 caller；
查一条记录无需回放它之前的全部版本。所有长度、offset、count、乘加、索引与 EOF 必须
检查，损坏文件有界报错。完整布局校验覆盖目录连续性及引用闭包，不能只抽样头尾。

## 构建流程

1. 一次取得 view。按完整块范围完成源认证，将涉及 Prev 的分块复制进本段私有 arena；
   写入有明确 byte/count 上限的行元数据与 span-ID spool，同时收集原逻辑 key 字典。
2. 完成字典、读取相同 view 的 StateTxRange 表；按源规范顺序重放小型 spool，写 V6
   记录头和对应 spans，生成原有 posting 与 tx index。不得将展开 Prev 写进 spool。
3. 所有私有输出完成、文件/目录同步和内容摘要生成；通过完整 reader/companion 绑定
   验证后才交付 refs。失败不发布 manifest/stage，不推进 hot prune 水位。
4. 既有 manifest 原子发布、可信来源绑定、连续覆盖、GC proof、writer guard 和完整
   维护预算保持；冷文件拥有全部依赖后，热桶才按原协议回收。

异常/repair 顺序必须与旧 oracle 一致。无序输入不能通过忽略 Prev 排序消歧；若无法在
metadata 路径保持同一顺序，使用明确 fallback 或返回待支持错误，不能发布不等价历史。

## 验收与切换

新 reader 默认可识别新 magic，生产 writer 通过 `--history.reference-container` 显式启用。
CPU/cache 压力回退可以降低读取并发，但必须继续写新格式。生产启用前安装绑定新 binary
摘要的 reader-required guard；首次新文件发布后不得回滚到不能识别新 magic 的旧 binary。
compactor、prune verifier、export/inspect、restore 和逻辑大小预算必须一起审计。
不得只接 query 后就允许 hot prune，也不得 merge 回旧格式却声称消除了长期重复工作。
含 R1 的合并保留引用，混合旧输入转换为私有 R1 后合并。目录预算通过低成本头部探测
预先检查；超过上限的组划为边界并继续查找后续可合并组，避免反复尝试失败的同一组。

固定原生 16 块样本优先比较旧 R4/cache/CDC1 与新单遍引用构建：完整 trio/逻辑行/全部
字段一致，比较实际墙钟、CPU、临时写入、最终三文件总量以及 point/range/restore 成本。
既有约 12.421 MB 输出是该样本基线，不作为全仓比例；输出明显膨胀时继续调整设计。
测试包含 shared/legacy/repair、空值/不存在、重复 tx 顺序、跨 chunk/段、字节损坏、
错长度/引用、取消、临时失败和生命周期；本地与服务器原生分别验证。

旧数据就地逐 trio 迁移：节点停止并持对应 Pebble 排他锁，完整验证新输出后原子发布，
再删除精确旧文件。源数据库复制量为零，每批预算只包含新输出和 scratch 等新增空间。
旧数据实际 active/retained/logical bytes、每批峰值、源认证/转换时间、重同步各历史时期
执行速度分别测量。不能用某一高度
12 分钟速度推算全链 ETA，不能把可接受清空解释为可以跳过新格式正确性验收。
