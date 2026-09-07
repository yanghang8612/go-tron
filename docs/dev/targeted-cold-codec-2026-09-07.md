# 针对重复大历史值的 CDC 容器

日期：2026-09-07。状态：已接入本地生产 builder/reader/merge/migration 路径并通过定向测试；尚未部署或转换生产文件，本轮没有启动生产 gtron 同步。

结论：对有少量插入变化的重复大列表，V3 实际生成的文件比 V2 小 **85.93%**。最初串行 V3 的编码耗时是 33.13 ms；本轮进一步实现有界单文件压缩流水线和并行分块后，同窗口默认 4 路工厂对照为 **V2 20.897 ms、V3 22.303 ms（V3 +6.73%）**。独立 V3 scaling 对照中，4 路比 1 路缩短 **27.6%**，8 路比 4 路仅再缩短 **3.1%**。因此默认保留 4 路，且 auto 继续仅针对重复大值；小账户和低重复随机数据没有空间收益。各轮采用同输入、完整输出 equality，但不同基准子项的短计时不当作精确生产 CPU 比率。

## 实际实现与选择条件

V3 是 `gtcblk01` 的新压缩容器版本，里面仍是逐字节相同的 V6 history 逻辑流；不是新的业务 value schema。任意 Prev bytes、存在/空值状态、完整 key、generation、txNum、记录顺序和 StateTxRange 均不改变。V7 `.idx/.kv` 继续使用原来的逻辑 offset，point/range/merge/prune 通过统一压缩 reader 接入，不需要间接 posting 或从创世重放。

`GTRON_HISTORY_COMPRESSION_FORMAT` 支持 `1`、`2`、`3`、`auto`。库调用空环境变量保留原有 V1 行为；节点入口的 `--history.compression-format` 默认 `auto`，只在启动前设置一次进程配置，单个 build/merge 不修改环境变量。

自动构建在已有 CollectKey 第一遍读取中计数，不增加一次完整 value 扫描。只有同时满足下列条件才选 V3：

- 单个 Prev 至少 128 KiB 的记录占本批 Prev 字节至少 50%。
- 至少两个这样的大值具有完全相同的逻辑身份（flat domain、owner、generation、domain、key）。
- 计数没有溢出，也没有发现无效输入。

精确身份只保留最多 4,096 个、合计 4 MiB 的 key，触及上限后不插入新身份，仍可发现已有身份重复。不会保存完整 Prev 以作选择。该策略是重复的候选检测，**没有证明两次大值实际相同**；同键的大随机替换仍可能错误选中 V3。此时记录完整性不受影响，但不能保证空间或 CPU 收益。

自动 merge 若任何已验证输入是 V3，则继续 V3；其余输入产出 V2。merge 的字典读取不提供 Prev 长度证据，故只额外读输入 16 B 格式头，不偷偷扫描旧值判定。已有纯 V1/V2 大段不会因 auto 就全量转成 CDC；主动转换可显式选择 3。migration 能识别当前 V3，连续再次运行时不重新改写；auto 也接受现有 V3。显式 1/2 按各自目标识别，空配置维持原先 V1/V2 的“当前”判定。

## 文件与读取上限

`internal/historychunk` 的固定 Gear 分块规则按字节确定边界，最小 8 KiB、目标掩码 32 KiB、最大 128 KiB；“目标”不是每个输入都会达到的平均大小。原固定首段最多 128 KiB 留在内存，用于 V6 头部计数回填，最终作为独立锚点写入。

每个后续 chunk 先用 SHA-256 查候选，再比较完整 bytes。命中引用本文件内更早的完整 zstd 锚点，否则写一个完整 zstd frame。只保留 64 MiB 原始候选 chunk、最多 8,192 项；没有跨文件引用、引用链或无限前驱重放。单文件新锚点现在通过有界流水线并行编码；共享八槽 EncodeAll encoder 支持最多 8 路任务，正常 factory 当前取最多 4 路。字典判定和最终落盘仍由一个 writer 按原顺序完成，重复 chunk 不重复压缩。下节记录其资源、故障边界与实测。

每条目录 entry 为 28 B（逻辑起点、物理起点、压缩长度、锚点 ordinal），每 1,024 条一页并带 CRC32；CRC 稀疏目录每页一个逻辑起点，最后是 48 B CRC footer。目录 spool 是单独临时文件；最终 body 写一次并流式计算全文件 SHA，不复制一遍压缩 body。统计计入真实最终目录和 footer 大小。

打开仅加载固定 header/footer 与稀疏目录，不遍历全部元数据页或解压全文件。编码稀疏目录上限 16 MiB；打开时编码缓冲与解码数组短暂并存，约 **32 MiB**，不是 16 MiB 进程内存保证。reader 缓存最多两页元数据和两个 128 KiB 解码锚点，另有最多约 129 KiB 的压缩 frame 缓冲；zstd 池和调用方完整记录缓冲需另外计入。

读取引用先验证其指向更早的完整锚点，raw/stored/physical 均与锚点一致；不接受 ref→ref。独立 zstd decoder 在解码前限制 window、输出以及目标 slice capacity，已覆盖已知和未知 FCS 的压缩 bomb。每次最多恢复一个 128 KiB 锚点；读取一个巨大 Prev 仍需处理其全部逻辑字节，现有完整记录 API 的分配上限继续生效，不能把 chunk 上限当成整个查询的总字节上限。

元数据页 CRC 和 frame 校验按访问加载，完整 manifest SHA/语义校验继续由原验证流程负责，不能把快速打开当成全文件认证。独立测试用 128 GiB 逻辑、约 28 MiB 页表的构造文件证明：打开仅两次元数据读取、少于 9 KiB；随后读取最后一个字节总读取少于 80 KiB。

## 第一轮同输入基准（单文件并行优化前）

环境：Apple M1 Max，macOS 26.6.2，Go 1.27.1 darwin/arm64，GOMAXPROCS=10，生产 factory 自动取得 4 workers。固定 seed，每项 `350ms × 3`，以下为三次的中位数。这一轮 V2 使用原正常有序并行压缩流水线；V3 当时仍使用串行单文件锚点路径。计时包含 factory、写入、file Sync、Finish rename；不包含从 Pebble 读取、ETL、整 trio 验证、manifest 发布或旧 SST 回收，因此不是整个冷热转换的吞吐。

三个输入均为**确定性合成数据**：2 MiB 随机基底上八个有微小插入的列表版本（16,777,289 B）；65,536 条账户形状的小记录（8,388,608 B，非真实主网账户分布）；8,388,608 B 低重复随机字节。三类均在计时前完整解码并逐字节对照。同一主机前一轮大列表 V2/V3 为 24.58/32.81 ms，最终为 20.98/33.13 ms；该运行差异也说明不能把单机短基准当作稳定生产 CPU 比率。

| 输入 | V2 文件 B | V3 文件 B | V2 编码 ms | V3 编码 ms | V3 编码耗时倍数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 重复大列表 | 16,783,058 | 2,362,146 | 20.978 | 33.126 | 1.579 |
| 小账户形状记录 | 2,219,141 | 2,219,679 | 19.258 | 53.486 | 2.777 |
| 低重复随机字节 | 8,391,520 | 8,396,782 | 14.660 | 29.366 | 2.003 |

| 输入/格式 | Open μs | 完整顺序 Decode ms | 4 KiB 随机读 μs |
| --- | ---: | ---: | ---: |
| 重复大列表 V2 | 19.326 | 3.055 | 21.349 |
| 重复大列表 V3 | 15.407 | 2.688 | 7.039 |
| 小账户形状 V2 | 17.944 | 4.819 | 74.290 |
| 小账户形状 V3 | 15.243 | 4.914 | 66.185 |
| 随机字节 V2 | 18.085 | 1.386 | 17.968 |
| 随机字节 V3 | 15.453 | 1.426 | 10.026 |

Open/Read 使用已热的 OS 页缓存；随机读取固定伪随机 offset，复用一个 reader，包含 reader 的有限内部缓存。以上是基准均摊耗时的中位数，不是请求延迟 p95、冷盘 I/O、端到端 GetAsOf 或服务器性能。V3 更小 chunk 在这些读取中减少了解码工作，不能据此保证所有冷盘随机查询更快。

解码缓存保持原 128 KiB backing capacity，同时每次仍在解码前把可见容量限制到声明 rawLength；成功后才恢复可复用容量。修复前随机数据的 V3 随机读约 50 KiB 分配/操作，最终约 9 B/操作、0 allocs/操作（启动缓存分摊），没有放宽解压门槛。V3 writer 的字典复制仍有明显分配成本：大列表约 3.25 MB/次、小账户约 9.00 MB/次、随机约 9.62 MB/次，不能称作零分配转换。

第一轮原始数据：[串行阶段最终日志](../../build/benchmarks/20260907-offline-recovery/cdc-factory-bench-final.txt)、[中位数 JSON](../../build/benchmarks/20260907-offline-recovery/cdc-factory-bench-final.json)、[前一轮日志](../../build/benchmarks/20260907-offline-recovery/cdc-factory-bench.txt)。命令：

```sh
go test ./core/state/snapshots -run '^$' -bench '^BenchmarkHistoryCompressionFactory$' -benchtime=350ms -count=3 -timeout 300s
```

## 第二轮：有界单文件并行流水线

CPU 的使用有明确范围：生产 factory 传入的 workers 为 1/2/4/8 时确实控制该文件的外部压缩任务数量；超过 8 拒绝。单文件逻辑大小不足 1 MiB 时维持串行路径。顺序 Gear 分块、SHA-256/完整 bytes 去重判定和 LRU 更新不随任务完成顺序变化，只对新锚点提交 zstd；完成结果按 ordinal 顺序写入。后续 chunk 可以命中尚在压缩中的字典锚点，但其目录引用必须等更早锚点实际完成 body 写入后才获得 physical/stored 值。锚点 handle 独立于 LRU 项，淘汰不会让已经排队的引用指向另一个任务。

同时在途最多 `2 × workers` 个 chunk，最多 16 个；每个 slot 在复制 raw/分配输出前预留 128 KiB raw + 129 KiB encoded，另设 **8 MiB** 硬线。连同已完成任务归还并保留的 encoded buffer，16 slot 的保守上界约 **4.02 MiB**。这不包含 64 MiB 字典、当前 producer chunk、保留首段和全局 zstd 工作空间；也不是进程 RSS 上限。实测最大在途预留出现在小账户 8 路：**3,836,880 B**。任务只读自己拥有的 immutable raw，不借用调用方在 Write 返回后可能复用的输入。

对一次 Write 中至少 1 MiB 的剩余输入，workers>1 使用 `Splitter.Cuts` 并行计算 Gear 候选，再顺序应用相同的 min/max 边界。每次最多扫描 **8 MiB**，限制切点列表、候选 scratch 与取消检查等待的工作量；不是把任意大 Prev 一次交给不可取消的全值扫描。每个 cut 独立增加 logical、追加当前 chunk 并 flush，未完成尾巴保留；小 Write 和 workers=1 继续 Next。Cuts 自身在返回前等待全部 stripe 任务结束，不保留调用方输入。

取消和错误流程先 cancel/join 所有压缩 worker，再释放文件和 buffer；Reset 不接收上一段的结果。Finish 在全部结果按顺序 drain 并 join 后，才压缩保留首段、写 footer、Sync 与 rename。特别回归覆盖了 **队列已满 + 未 flush 尾巴 + Finish 调用方取消**：等待空 slot 也使用该调用方的 context，不能只在最开始检查一次。错误参数的 WriteAt 保留原可恢复契约，但先 drain/join 已接受的任务再返回错误，不让工作线程留在错误返回之后。

以下新一轮环境仍为 M1 Max、Go 1.27.1、GOMAXPROCS=10，`10x × 3` 中位数。每个 worker 配置正式计时前都校验了完整序列化文件与 workers=1 **逐字节相同**，并完整解码与输入比较；包括相同 chunk 边界、anchor 选择、物理偏移、目录 CRC 和全文件 SHA。计时包含 factory、file Sync、Finish，口径限制与第一轮相同。

| 输入 | workers=1 ms | workers=2 ms | workers=4 ms | workers=8 ms | 8 路在途预留峰值 B |
| --- | ---: | ---: | ---: | ---: | ---: |
| 重复大列表 | 32.968 | 25.500 | 23.868 | 23.123 | 2,919,229 |
| 小账户形状 | 59.770 | 34.348 | 25.600 | 22.450 | 3,836,880 |
| 低重复随机 | 29.623 | 23.709 | 22.486 | 21.716 | 3,146,162 |

同一个隔离测量窗口另运行当前 factory（4 workers）的 V2/V3 Encode-only 基准：

| 输入 | V2 ms | V3 ms | V2 / V3 文件 B |
| --- | ---: | ---: | ---: |
| 重复大列表 | 20.897 | 22.303 | 16,783,058 / 2,362,146 |
| 小账户形状 | 20.887 | 23.809 | 2,219,141 / 2,219,679 |
| 低重复随机 | 13.923 | 22.609 | 8,391,520 / 8,396,782 |

相同 4 路在两组子项中仍有普通运行差异，因此不得将 22.303 与 23.868 ms 拼成两个不同模式的收益。可靠结论是串行瓶颈得到实测改善，4→8 在目标大列表上的增益较小，输出 bytes 不变，仍应按输入选择 V2/V3。没有因此提升同时写文件数量，也没有把结果外推为磁盘限速条件下的服务器吞吐。

[第二轮完整日志](../../build/benchmarks/20260907-offline-recovery/cdc-worker-scaling-final.txt)与[逐次样本和中位数 JSON](../../build/benchmarks/20260907-offline-recovery/cdc-worker-scaling-final.json)：

```sh
go test ./core/state/snapshots -run '^$' -bench '^BenchmarkCDCWorkerScaling$|^BenchmarkHistoryCompressionFactory/.*/.*/Encode$' -benchtime=10x -count=3 -timeout 300s
```

## 正确性、持久化与范围

第二轮 `go test -race ./core/state/snapshots -run '^TestCDC' -count=1 -timeout 180s` 已通过（6.098 s）。新增覆盖 1/2/4/8 路与不同 Write 分区的完全相同文件、乱序完成、pending 锚点 LRU 淘汰、输入立即复用、worker/写盘故障、Write/Finish 取消、Reset/Abort join 和满队列尾巴取消。

第一轮定向测试也已通过：完整任意字节往返、插入后 chunk 重对齐、跨 chunk frame、存在/空值、负 offset/EOF、并发随机读取、坏 CRC/截断/错误长度/前向与链式引用、已知和未知 FCS bomb、取消/Reset/Abort、超大稀疏目录预分配拒绝、生产 builder + V3/旧 raw V6 混合 merge + 完整 range/point oracle、auto 两种输出、V3 migration 二次幂等及 active SHA 保持。

最终 `.seg/.idx/.kv` 临时文件重命名后，新增同步最终父目录，成功才把引用交回调用方。注入目录同步失败测试证明错误时最终文件仍保留；这项门同样修复原 V2 路径。manifest 发布报错可能发生在 rename 已完成之后，migration 不再删除这时的不可变输出；compactor 的失败清理也不会删除同名输入文件。这里的故障注入和重开测试不是物理断电验收，目录同步仍遵循项目现有平台/文件系统支持契约。

`GTRON_HISTORY_COMPRESSION_FORMAT=3 go test ./core/state/pruning -run '^TestOfflineHistory' -count=1 -timeout 120s` 已通过，覆盖 offline build → 完整 trio 与 hot preimage 对照 → 发布 → 删除 → durable barrier，以及取消、错误 preimage、旧无 identity 精确 pin、manifest 发布不确定、删除 Sync 失败后的重试。没有省略原删除前的内容验证。根任务随后完成 FORMAT=3 的历史 AccountAt/CodeAt 查询测试、相关包 race 测试，以及 Linux amd64 静态二进制构建；全仓最终测试另由根任务统一记录。

成功 Finish 后记录 `state/snapshot/cold/compression_cdc/{files,input_bytes,reused_bytes,stored_bytes}` 计数。这表示完成文件，不代表 manifest 已发布、旧文件已回收或全卷净收益。旧只读业务 sampler 仍明确拒绝 V3；固定块比例空间预测工具对 V3 明确报 unsupported，避免静默零样本被误称 100% 节省。

该格式解决段内重复字节的物理表示，未实现关系页/COW、durable history journal 或全库一次转换；不改变早 prune/空间背压各自的职责。不能把合成数据的 85.93% 乘到全库、不能保证有限磁盘无限保留历史，也不把编码 CPU 代价隐藏到“冷化提速”中。生产迁移前仍应在独立 scratch 上用固定 SHA 的真实大段比较完整组合 bytes、构建/验证时间、读取与空间峰值。
