# 用富余 CPU 减少历史存储 I/O

2026-09-07，继 [上一轮冷热历史实现](targeted-history-storage-2026-09-07.md)。本轮仅本地实现、故障测试与基准；没有启动或部署 gtron，没有操作生产 datadir、维护停机标记或 MySQL。继续保留全部历史状态、查询与回滚语义。

## 资源取舍

CPU 核心富余时，应比较并行后关键路径的额外时间与实际减少的读写时间。不能因为累计 CPU 秒增加就拒绝压缩，也不能用文件缩小倍数直接声称同步加速。

对于大量重复的委托账户关系历史，额外花 CPU 去掉重复内容，优先于统一提高 zstd 等级。账户继续使用上一轮的 V5 可逆字段表示；本轮不改变字段、逻辑历史或文件格式，而是降低切块和冷压缩的串行耗时。

## 已接入正常代码的改动

### 大输入并行切块

`internal/historychunk/bulk.go` 的 `Splitter.Cuts` 把大输入的 Gear 候选扫描分给最多 8 个 worker，再顺序应用原最小/最大块规则。64 次左移后 uint64 旧状态完全消失，因此每个 stripe 只需 64 B warmup；自然合法边界距 reset 至少 8 KiB，算法与原逐字节 Next 等价。返回前所有 worker 已退出，输入仅在调用期间借用。

小于 1 MiB 继续串行。候选列表每 worker 最多 16,384 项，异常密集候选触发有界回退，最终边界、pending 和 hash 不变。切分结果的数量另受最小 8 KiB 块长约束。

热 block pack 编码接入这条路径，默认最多使用 4 个 CPU worker；只在原热去重开关及大小/重复身份筛选通过时承担工作。冷 V3 writer 的大 Write 同样接入，每次窗口最多 8 MiB，以限制临时列表和取消延迟。小记录写入继续 Next，避免每个账户记录都建立任务。

### 单个冷文件并行压缩、有序写入

此前 factory 的 workers 参数没有作用到 V3 单文件，锚块逐个调用 EncodeAll。本轮 V3 使用有界压缩流水线：

1. producer 顺序识别内容、校验 SHA 候选的完整字节相等性、维护 LRU 和引用。
2. 只把唯一锚块交给多个 CPU worker 做 zstd，重复片段不重复压缩。
3. 单个 writer 按原顺序收取结果并写入；先完成锚块，才发布引用的物理位置。任务完成顺序不改变文件字节。

自动并行度沿用每阶段最多 4，内部支持并测试 1/2/4/8；共享 EncodeAll 池上限 8。切块扫描和前一个窗口的压缩可重叠，所以“4”不是整个进程最多占 4 个核心的承诺。队列最多 `2 × workers` 项，另有 8 MiB raw+encoded 预留硬线。即使使用 8 workers，当前 16 项配置的最坏预留约 4.02 MiB；这**不是总进程内存上限**，64 MiB 去重字典、编码器内部工作区、prefix、普通文件缓冲和目录空间另计。

已接受的锚块拥有输入副本，调用者可在 Write 返回后复用原始输入。LRU 淘汰不会破坏排队引用。写入失败、worker 错误、取消、Reset、Abort 都会收回工作线程；Finish 在队列已满且仍有尾块时也响应其独立 context。worker 不直接写文件或发布 manifest，全部完成后才最终化文件。

### 保持针对性选择

冷格式 auto 仍利用现有 CollectKey 遍历判断重复大值，再选择 V3；账户、小记录和无重复大 key 的内容使用 V2。没有增加全历史扫描。热去重仍有开关、同一 block pack 身份条件和至少额外节省 12.5% 的收益门；跨 block 引用协议没有被引入。

保留单个 heavy-work 准入、空间底线、完整覆盖校验、先回收热历史再重合并的顺序。多开 CPU worker 不意味着同时开多个磁盘写入任务。当前成功 cooldown 仍混合 CPU/I/O 墙钟，不能把它称为真实磁盘预算；后续需要逐轮读取/写入/ETL 字节和设备等待指标才适合替换，详见 [I/O 模型](history-io-model-2026-09-07.md)。

## 测量口径

本地 Apple M1 Max、Go 1.27.1、darwin/arm64。下面的比例来自固定 seed 合成输入，不能外推为主网整库压缩率。冷 factory 测量包括建 writer、压缩、文件写入、Sync 和 Finish，但不包含 Pebble 读取、ETL、索引生成、整 trio 验证、manifest 发布和 SST 物理回收。不同 worker 数的输出逐字节相同，因此本轮并行化本身不额外改变空间比例。

每组重复 3 次，切块/热编码每次 20 次，冷 factory/scaling 每次 10 次；表中使用中位数。基准按顺序运行，未与其他 CPU 基准或全仓测试重叠。

| 同一输入、同一格式 | 1 worker | 2 workers | 4 workers | 8 workers |
| --- | ---: | ---: | ---: | ---: |
| 8 MiB Gear 切块 | 6.606 ms | 2.832 ms | 1.506 ms | 1.174 ms |
| 热 CDC 编码，4×2 MiB 大列表版本 | 12.703 ms | 8.895 ms | 7.553 ms | 7.267 ms |
| 冷 V3 factory，约 16 MiB 重复大列表 | 32.968 ms | 25.500 ms | 23.868 ms | 23.123 ms |

4 worker 热 CDC 编码比 1 worker 耗时下降约 40.5%；冷 V3 大列表下降约 27.6%。8 worker 相比 4 worker 对这两项只再快约 3.8% / 3.1%，因此没有放大默认并行度。

热表只测 CDC 函数。真实生产择优门还执行原 Snappy 尝试：完整旧路径中位 0.567 ms，完整新路径 8.158 ms；输出 8,388,960→2,353,719 B，少 5.756 MiB（−71.94%）。增加的 7.591 ms 不能忽略，但也不代表 I/O 受限同步一定变慢：仅算一次同量写入，100 MiB/s 下少写这些字节约节省 57.56 ms；简化 break-even 约 758 MiB/s。这里是合成样本的编码测量加带宽假设，不是服务器实测；解压、缓存、WAL/SST 表示及实际设备放大仍影响全流程。读回整个 pack 的额外解压成本没有包含在这个写侧公式内。

针对该服务器后续新目录同步，建议选用 `--history.block-dedup=true` 与 `--history.compression-format=auto`，从热写入源头减少重复字节；这些参数本轮没有在服务器执行。通用 CLI 的热去重默认仍为 false，不因单台机器的资源前提改变其他部署的默认值。自动冷格式仍可被已有 `GTRON_HISTORY_COMPRESSION_FORMAT=2` 覆盖，后续实际配置应明确选 auto。

同一窗口另测正常 factory（默认 4 workers）：

| 合成输入 | V2 文件 B | V3 文件 B | V2 factory | V3 factory | 取舍 |
| --- | ---: | ---: | ---: | ---: | --- |
| 重复大列表 | 16,783,058 | 2,362,146 | 20.897 ms | 22.302 ms | 文件少 85.93%，编码增加 6.72%，使用 V3 |
| 小账户形状 | 2,219,141 | 2,219,679 | 20.887 ms | 23.809 ms | V2 |
| 低重复随机 | 8,391,520 | 8,396,782 | 13.923 ms | 22.609 ms | V2 |

独立 scaling 与 factory 两组的 4 worker 结果有测量波动，不能混用最优值拼成加速倍数。相较上一轮 V3 串行 33.13 ms 的记录，本轮减少串行开销后，在大列表相同空间收益下已经接近 V2 的编码时间。

原始日志：[热与切块](../../build/benchmarks/20260907-cpu-history/medians.json)、[冷 factory 与 scaling](../../build/benchmarks/20260907-offline-recovery/cdc-worker-scaling-final.txt)。

## 更高压缩等级与共享 I/O

[等级对照](history-compression-levels-2026-09-07.md) 同时核验固定依赖 klauspost/compress v1.17.11 的等级映射和实际生产 encoder 选项。CPU 富余不等于更高等级必然更省：账户、32 B storage 和随机地址各有不同结果；生产 V2 同参数的 32,768 条账户与计数器两组实测中，better 相比 default 分别多 514 B、185 B，编码 CPU 增至约 1.90 倍、2.18 倍，因此本轮保留 default，没有增加第二遍整段重压。

[实际字节与模型](history-io-model-2026-09-07.md) 单独记录 factory 墙钟和进程 user/system CPU。大列表 V3 除最终 2,362,146 B，还产生临时目录写/读各 12,712 B；相对 V2 的净应用层 I/O 减少 14,395,488 B。若共同源读取按 2×16 MiB 假设，则 I/O 比约 48/34≈1.41，而非仅看最终文件得到的约 7 倍。WAL、flush、compaction 以明确的参数假设列入敏感性模型，没有冒充现场放大测量。

## 验证与交付状态

本轮最终代码验证全部通过：

- `go test ./... -count=1 -timeout=300s`：54 个有测试的 package，13 个无测试 package，0 失败。
- `go test -race` 对共享 chunker、热 pack、CDC pipeline/reader/fault、I/O 模型和等级边界的关键测试全部通过。
- 显式 `GTRON_HISTORY_COMPRESSION_FORMAT=3` 的历史账户/CodeHash、commitment 回滚和全部 OfflineHistory 测试通过，包含发布不确定、损坏覆盖、取消、分批删除和重试边界。
- 新切块器跨输入分片/pending/hash、真实过密候选回退及热/冷串行并行文件逐字节一致测试通过。另一代理只读复核数学等价及锚点/取消/内存协议，无遗留阻断。
- `git diff --check` 通过。Linux amd64、CGO=0 构建通过；没有运行该节点二进制。

验证日志位于 [本轮验证目录](../../build/benchmarks/20260907-cpu-history/validation-summary.json)。产物为 `build/bin/gtron-history-linux-amd64`，SHA-256 `e0b96e2d427f30d0dc75487175553e471b6138e894a9c25605e837de5ab5eaab`。此哈希替代上一轮产物哈希；生产运行状态没有改变。

固定容量能否最终容纳全主网历史，仍需实际全链增长、冷化峰值和同期 I/O 测量。当前证据证明本轮代码的字节一致性、故障边界及所列合成样本的压缩阶段收益，没有把局部压缩比例当作整库容量担保。
