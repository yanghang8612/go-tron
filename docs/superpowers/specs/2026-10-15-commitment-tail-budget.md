# Commitment 深层缓存 tail 容量验收

2026-10-15。延续 [2026-10-14 容量设计](2026-10-14-commitment-tail-cache.md)，基于 `ff6f0820dde5bed63d78f525f51a92e915b959f9` 的线上诊断及固定输入实验；原始证据在 `build/benchmarks/20261015-sync-cold/deep-cache-candidate/`。本规格只选择已有 tail 的配置候选，不改变缓存算法或共识状态。

## 决策与容量边界

候选将 `--state.commitment.cache.tail`（或 `GTRON_STATE_COMMITMENT_CACHE_TAIL`）从 512 MiB 提到 1024 MiB，base 仍为 512 MiB，retained-charge 合计上限从 1024 提到 1536 MiB。增加的 512 MiB 在原 entry map/CLOCK 中与现有类共享，不是深 6/7 独占分区。trunk 64 MiB、window 8 MiB、other 128 MiB 的 base-derived reservation、probation、namespace 软借用和现有预取不变；空闲私有 value backing 上限仍按 `(limit-additionalTail)/8` 算，不随 tail 同比例增大。

线上现有 1 GiB retained-charge 容量近满，tail 887,448,437 B、other 134,217,176 B、trunk 43,687,127 B、window 8,383,819 B。94.5% 前台 durable branch reads 在深度 6/7；单次 Get 约 314 µs、随机 4 KiB SST ReadAt 约 294 µs，但 VFS 小读还有其他来源。保留 charge 不是 heap/RSS 上界：map、队列、entry slab、allocator 和 GC 需要单独计量。当前约 32 GiB memcg 已有压力，候选必须与另行审定的 40 GiB hard limit 一起验收；本规格不编辑 service，也不将 hard limit 当成实际可用 Go heap。

固定 8-block Pebble pipeline 用 base32 KiB + tail32→64 KiB 模拟容量比例，三次/模式。durable reads/op 在 hot、cold_scan、scan、delayed_read 分别下降 50.5%、26.3%、42.5%、42.5%；B/op 分别下降 3.1%、6.0%、10.8%、10.3%。完整 pipeline 的 `cold_scan` 时间却从 16.50 增至 17.91 ms（**+8.6%**）。这只是小输入比例实验，不是生产吞吐预测；时间退化必须列入线上验收。

## 被拒绝的准入变更

纯 flush 深 6/7 window 不给读信用的原型，以及仅给它一次 FIFO window 宽限的原型，都已撤回。两者在各 126 个同输入 replay 中返回值和 trace SHA 没有差异，但 paired/short read-after-write 压力场景的 durable Get 分别回退约 40–42% 与 41–44%，晚读也回退约 14%。线上 cohort 有高度截尾；已完成 cohort 的低复用比例不能外推所有仍活跃的 entry。现有 foreground/prefetch credit、flush 准入和 CLOCK 策略保持原样。

不池化已经被 layer 持有的 immutable branch arena；不靠新增底层读取判断已全部 changed 的 fold 是否可跳写。两者既没有本轮同输入获益证据，也会改变所有权或 I/O 边界。

## 正确性与生产验收

容量变更不触及持久格式、root、共识、keyspace、数据库内容或 branch 读写语义。overlay 优先、per-key epoch、snapshot-version fill guard、旧 snapshot 隔离、flush、reorg/rewind/drop、tombstone、missing/empty/error、borrowed bytes 生命周期与 prefetch 均继续执行现有代码。root oracle、legacy/delta、zero/missing、读后写、迟读、scan、容量不足混合 other，以及 flush 失败/reorg/discard 的既有回归是 native Sapling 候选的门槛。

上线需先核对精确源码、binary、配置与 hard limit；按相同链段和相同长度窗口保存变更前后：深 6/7 及总体 durable Get 次数/延迟、随机 VFS ReadAt、tail 命中/淘汰/驻留、prefetch useful、完整 fold/执行时间、Go heap/GC、RSS 与 memcg anon/file、failcnt/under_oom/major faults、同步速率和冷历史积压。持续检查 root/head 与错误、stall；在 `cold_scan` 或混合负载真实回退、内存压力/写入停顿恶化时恢复 tail512 配置并按正常服务流程重启。回退只改 tail 配置，不删库，不变更缓存记录格式。固定输入读数不能替代这次生产观测，也不保证热库最终仅剩当前状态。
