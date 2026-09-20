# 2026-09-20 同冷覆盖高度的同步存储对照

本次只读检查确认，当前从创世重同步节点在几乎相同的冷发布高度下，状态冷历史的有效文件占用和活跃 trio 数都显著高于 9 月 9 日旧节点。当前 `published=12,785,708`，旧样本为 `12,776,464`，只差 9,244 块；当前 active state-domain history 三件套为 **477,563,658,582 B、16,624 个 trio**，旧样本为 **110,206,523,156 B、602 个 trio**。当前分别是旧样本的 **4.333 倍**和 **27.615 倍**。

这证明有效冷文件空间效率已经回退，并存在大量小段。它不能由当前较大的未冷化积压、Pebble obsolete SST 或热库空间解释。现有证据尚不能把 4.333 倍增量精确拆分为编码格式膨胀、段碎片、合并不足或它们的组合。

## 数据来源与口径

当前原始采样位于 `build/benchmarks/20260920-current-storage/20260920T030139Z/`，包含五次 Wallet 和 metrics 响应。节点使用 9 月 18 日清库后从创世同步的新库，当前 binary source 为 `16ea25075c380dd610d0200e9e6048fa46a2d84c`。本报告后续取得的 `2026-09-20T03:20:33Z` 目录 `du` 与 suffix 文件 `stat` 是另一个非原子时点，不能覆盖或改写这组 03:01:39Z metrics 数字。

旧对照来自：

- `build/benchmarks/20260909-status-0931/metrics-initial.json`
- `build/benchmarks/20260909-status-0931/disk-summary.json`
- `build/benchmarks/20260909-cold-followup/after-deploy-metrics.json`
- `build/benchmarks/20260909-cold-pipeline/final-end-metrics.json`

`state/prune/verification/active_bytes` 是当前 manifest 中 active state-domain history trio 的三个实际文件大小之和；每个 history ref 连同其 index 和 accessor 计入。`active_segments` 是 trio 数，不是三个文件分别计数。这是有效冷历史存量，不是进程累计写入量。`state/snapshot/cold/bytes/built` 和 `segments/built` 则是进程累计计数，不能作为存量对照。

旧提交 `22972031` 与当前源码的同名指标语义已经逐项核对：两版 `setActiveManifest` 都按 active history trio 对三个文件执行 stat 并求和，Pebble `disk/size` 都来自 `stats.DiskSpaceUsage()`。因此下表可比较同名指标，不依赖仅凭名称猜测口径。

Pebble `disk/size`（当前响应同时暴露为 `storage/engine/disk/bytes`）只覆盖 chaindata 引擎。它不包含 `state-snapshots`、ancient、retired snapshot 文件或整个 datadir，因此不能与三目录 `du` 相加或互换。

## 同冷覆盖高度对照

| 指标 | 旧节点，2026-09-09 | 当前节点，2026-09-20 | 当前 / 旧 |
| --- | ---: | ---: | ---: |
| head | 13,435,049 | 14,543,032 | 仅作位置说明 |
| cold published | 12,776,464 | 12,785,708 | 差 9,244 块 |
| eligible cutoff | 13,369,220 | 14,477,148 | 不作为存量分母 |
| active trio bytes | 110,206,523,156 B（102.638 GiB） | 477,563,658,582 B（444.766 GiB） | **4.333 倍** |
| active trios | 602 | 16,624 | **27.615 倍** |
| 平均 bytes / active trio | 183,067,314 B | 28,727,362 B | 旧样本的 15.692% |
| Pebble `disk/size` | 86,167,339,027 B | 106,228,229,356 B | 1.233 倍 |

旧节点 published 比当前少 9,244 块，冷覆盖范围几乎相同，且旧覆盖还略短。当前却多出 **367,357,135,426 B** active trio 文件，并产生约 27.6 倍的 trio。平均 trio 明显变小，支持“分段过碎或合并没有跟上”；总 active bytes 同时增加 4.333 倍，说明问题不只是把相同字节拆成更多文件。

旧样本附近的在线、非原子 allocated `du` 为 chaindata 86,057,496,576 B、state-snapshots 142,245,666,816 B、ancient 71,621,287,936 B，总计 299,924,455,424 B。当前 active trio 指标的 477,563,658,582 B 已是旧整个 state-snapshots 目录 allocated 值的 3.358 倍。两者口径不同，不能据此推导当前目录的 `du` 或逐字节差额；它只进一步表明量级已经明显偏离旧库。

旧进程同点累计 `segments/built=6,849`、`segments/compacted=6,847`，说明当时该进程的构建和压实累计次数接近；这两个累计值不代表当时只有两个待合并段，也不能直接与当前跨重启累计值比较。

## 当前五点窗口

按 `storage/engine/sampled_at`，窗口跨度 354 秒。Wallet head 从 14,543,039 增至 14,577,153；metrics 中相邻但非原子的 head 为 14,543,032→14,577,145。以下速率使用本轮既定端点和 354 秒分母：

| 指标 | 开始 | 结束 | 增量 / 速率 |
| --- | ---: | ---: | ---: |
| Wallet head | 14,543,039 | 14,577,153 | +34,114，约 96.37 块/秒 |
| cold published | 12,785,708 | 12,807,360 | +21,652，约 61.16 块/秒 |
| eligible cutoff | 14,477,148 | 14,511,685 | +34,537，约 97.56 块/秒 |
| eligible − published | 1,691,440 | 1,704,325 | +12,885，约 36.40 块/秒 |
| Pebble `disk/size` | 106,228,229,356 B | 107,085,293,203 B | +857,063,847 B |
| active trio bytes | 477,563,658,582 B | 478,505,094,887 B | +941,436,305 B |
| active trios | 16,624 | 16,664 | +40 |

窗口显示同步和冷发布都在前进，但 cold published 低于 eligible 增速，积压继续扩大。`state/snapshot/cold/compaction/merges` 保持 99，`segments/compacted` 保持 394，同时 active trios 增加 40；因此该窗口没有完成合并，新增小段没有被窗口内的已完成合并抵消。运行中的旧 binary 没有导出 compaction `Deferred/DeferReason`，所以这 99 次 merge 不动不能进一步归因为 `import-load`、恢复窗口、heavy-work gate、leaf/input budget 或 reference 上限；缺失原因字段本身不能证明其中任一种。

期末 engine 为 107,085,293,203 B（99.731 GiB），active trio 为 478,505,094,887 B（445.643 GiB），两项指标数值合计 545.374 GiB。这个合计混合 Pebble bookkeeping 估计与 active 文件 stat 大小，未包含 ancient、retired 文件和其他目录，既不是全目录大小，也不是严格的物理 `du` 下界。采集该五点窗口时，经 SOCKS 的新 SSH 连接曾在 15 秒内未收到 banner 而超时，所以当时没有把未复核的 JumpServer 画面写入窗口数据；后续 03:20:33Z 已取得带时间戳的可靠 `du` 与 suffix `stat`，如下单列。

## 03:20:33Z 目录与文件证据

`2026-09-20T03:20:33Z` 的在线、非原子 allocated `du` 为：chaindata
108,675,964,928 B（101.212 GiB）、ancient 82,983,505,920 B（77.284 GiB）、snapshots
513,237,966,848 B（477.990 GiB），三目录合计 704,897,437,696 B（656.487 GiB）。snapshots
内同时采到 history 480,593,793,024 B（447.588 GiB）、log 32,645,697,536 B（30.404 GiB）和
manifest 23,088,056 B（0.022 GiB）。父目录包含这些子项，不能把父、子结果再次相加；各命令也
不是同一个文件系统快照，文件可在采集间变化，因此不能要求子项和逐字节等于父目录。

同一轮对 16,750 个 trio 按 suffix 汇总普通文件 `stat` 长度：`.seg` 442,185,711,995 B
（411.818 GiB）、`.idx` 3,532,518,490 B（3.290 GiB）、`.kv` 34,968,654,132 B
（32.567 GiB），合计 480,686,946,617 B（447.675 GiB）。这个值是文件逻辑长度之和；上述
history `du` 是 allocated blocks。再加上采集非原子，两者约 88.8 MiB 的差值不能解释为 retired、
稀疏空间或采样增长中的任意单一项，也不能直接与 03:01:39Z 的 active manifest stat 指标相减。

本轮证据来自 JumpServer 终端实时只读采样，未单独重定向或下载成本地原始文件。命令口径为
`du -sx -B1` 分别读取 `/data/gtron/main/datadir/gtron/{chaindata,ancient,state-snapshots}` 和
`state-snapshots/{history,log}`，`stat -c '%s %n'` 读取 `manifest.json`；suffix 汇总由 `find`
限定 history 目录第一层的 `*.seg`、`*.idx`、`*.kv` 普通文件后按扩展累计。服务器上的私有证据包
为 `/data/gtron/releases/20260920-space-fix-evidence/space-fix-real-inputs.tgz`，另有副本
复制目标 `/home/java-tron/space-fix-real-inputs.tgz`，但本轮 UI 失效后未重新 `stat` 确认该目标；
证据包保存真实 immutable trio 输入，不是 03:20:33Z
终端 `du` 的原始转录。已识别、计划用于 codec 复核的 trio 为
`/data/gtron/main/datadir/gtron/state-snapshots/history/state-domain-change-689728138-689759206-c2e89d983cf9417a.{seg,idx,kv}`；header hex 尚未取得可核验原始输出。

取证结束时 JumpServer terminal iframe 已变成白屏，无法再取得新命令输出；filemanager 刷新后也失效，
因此该真实 trio 没有成功下载，新的 codec A/B 没有在服务器运行。包含 bounded codec 3 reader/writer、
手动真实样本诊断和有限 defer-reason gauge 的提交
`fea7c859710198c40e90b8e096f9ae8bf4557127` 已推送到 `origin/master`，但尚未部署；不能把本地合成
测试或已推送代码描述成线上空间收益证据。

busy leaf 流式 fallback 修复也已作为
`1e6f5c2565cedb581d1c3401da07c8d37520117e` 推送：当 R1 输出估计超限而通用输入预算仍允许时，
它使用 256 KiB 固定缓冲重编码，并保留完整发布验证。该提交已通过 93.296 秒全包测试和 3.721 秒
定向 race；它同样尚未部署，也没有运行真实线上 A/B，不能据此保证线上收益。

这组证据推翻的只是“当前实际目录占用尚未取得”这一采集状态，不改变前述同覆盖高度对照：
03:20:33Z 整个三目录实际 allocated 量级为 656.49 GiB，其中 snapshots 约 477.99 GiB，history
约 447.59 GiB；suffix stat 同时确认 history trio 的主要字节来自 `.seg`，但仍需按 codec 和合并层级
分层，才能归因格式与碎片各自占比。

## 已证实原因与边界

当前 active 冷覆盖比旧样本只多 9,244 块，active bytes 却为 4.333 倍，且没有 Pebble obsolete SST：当前首点 `storage/engine/sst/obsolete/bytes=0`、zombie bytes 也为 0。因此，当前异常不是“热库积压更大”或 obsolete SST 造成的这 367 GB active 冷文件差额。

当前线上 `16ea25075c380dd610d0200e9e6048fa46a2d84c` 版本的 R1 history reference writer 在 `history_reference_container_writer.go` 中把 chunk 写为 Snappy，压缩不划算时保存 raw。9 月 15 日首个旧 trio 转换从 222,407,358 B 增至 376,210,416 B，净增 69.1538%；其中 history 从 148,229,628 B 增至 302,032,686 B，两个 companion 字节不变。该固定段证明特定旧 Zstd→R1 Snappy/raw 转换会膨胀，但它只是单个早期大记录量段，不能把 69.1538% 外推到整个当前库，也不足以解释或量化本次 4.333 倍差异。格式效率与 27.6 倍 trio 碎片各自贡献多少，仍需对当前 manifest 做按格式、按三件套和按高度的有界库存。

当前首点 history budget 为 level 1、duty 20%；后续点为 level 2、duty 80%。`shared_read/last/fallback_reason=7`，且运行时 memory available 只有约 557 MiB，说明共享读取路径受内存余量约束并发生回退。这可以解释部分归档吞吐不足，但不能据此宣称硬件已到极限。当前 manifest cache budget 为 512 MiB，resident/candidate 约 85.4 MiB，headroom 约 426.4 MiB，五点均没有 budget rejection；本轮异常不是此前 manifest 缓存准入失败重现。

## 建议顺序

1. 优先修复冷 history 格式的空间效率，并让新写入和混合格式合并采用一致、可验证的编码策略；以固定真实 trio 做字节级 A/B，不能只比较逻辑字节或单个首段。
2. 对当前 manifest 做只读、有界的 active/retired 库存，按 history 格式、history/index/accessor 文件大小、块范围和 trio 大小分层，量化格式膨胀与碎片各自占比。
3. 恢复有界合并调度，使构建与合并长期收敛；同时保留同步吞吐、内存余量和共享 I/O 保护，避免用无限并发掩盖空间回退。
4. 校准 shared-read 的内存资源探测和 fallback，重新验证 level/duty 选择。该项用于恢复冷发布吞吐，不能替代格式和合并修复。
5. 后续验收必须在相同 published 覆盖附近同时比较 active bytes、active trios、三件套分项、retired bytes、实际 allocated `du` 和查询正确性。

本轮生产侧仅完成只读采样和历史证据对照，没有修改生产配置、服务、数据库或端口；两项修复代码
已经推送但尚未部署，真实 A/B 未运行，当前没有线上收益保证。
