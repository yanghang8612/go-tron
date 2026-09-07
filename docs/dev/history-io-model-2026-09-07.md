# 历史压缩的实际字节与共享 I/O 模型

2026-09-07。本项只新增本地测试和模型，不修改生产 writer、调度或节点配置，不访问服务器或启动同步。重点是在 CPU 有余量而磁盘 I/O 受限时，比较更小输出与额外压缩工作的取舍。

## 测量与模型分开

[测试观察器](../../core/state/snapshots/history_io_cost_test.go) 调用实际 `newHistoryCompressedStreamFormat`，分别选择 V2/V3，不更改进程环境变量。三类大样本复用已有 factory benchmark 的确定性输入，包含重复大列表、小账户形状记录、低重复随机字节。每个产物检查输入 SHA、最终文件 size/流式 SHA，并完整解码后逐字节比较；这些 oracle 检查在 factory 耗时测量之外。

实际观察字段：

| 字段 | 口径 |
|---|---|
| `FactoryWall` | factory 初始化、Write、压缩、临时文件写入、file Sync、Finish rename 的本机墙钟时间；不等于纯 CPU |
| `ProcessUserCPU` / `ProcessSystemCPU` | Linux/macOS `Getrusage(RUSAGE_SELF)` 的分别增量，计入本测试进程的工作线程、运行时和系统 CPU；不是主机全部进程 |
| `CPUAvailable` | 无法取得 CPU 时间的平台为 false，不能将未取得解释成 0 |
| `StoredBytes` | 最终文件实际字节，包含压缩 body、最终目录和 footer |
| `SpoolWriteBytes` / `SpoolReadBytes` | CDC 临时目录每个非首段 chunk 的 28 B entry，分别写一次、读一次；V2 为 0 |

最终字节已经包含 CDC 目录复制到最终文件的那一份；额外加临时 spool 的写、读，不把它当成免费。此计数是成功 writer 的应用层字节，不包含文件系统元数据、设备扇区放大、失败重试或 fsync 固定时延。V3 `compression_cdc/{files,input_bytes,reused_bytes,stored_bytes}` 现有计数仅在成功 Finish 后增加，不代表 manifest 已发布、旧 SST 已回收或整个卷的净减少。

实际 writer 内部使用 `*os.File`，Finish 会重设 metadata writer 的目标。只在外层包一层限速 Writer 会漏掉首段、目录/footer 或 spool。因此本项不把不完整的限速拦截当作真实限速 factory，也不拿复制最终文件加 sleep 冒充冷热转换端到端实测。

## 离散事件共享磁盘模型

[模型](../../core/state/snapshots/history_io_cost_model_test.go) 使用上述实际字节和 factory 墙钟观察值，不执行按带宽睡眠。每组 8 个相同产物，producer 为 1 或 2 个，等待队列容量固定为 2，共用一个串行磁盘服务。producer 完成一个产物的 work 后进入队列；队列满时仍占用 producer，不能无限生成压缩输出。阻塞 producer 按固定 worker 下标优先准入，不模拟不同完成时间之间的严格 FIFO 公平性。报告队列峰值、占用 producer 峰值、阻塞 producer 时间总和、磁盘服务时间和模型完成墙钟时间。

模型允许不同产物的 compute/I/O 重叠；没有模拟一个产物内部的压缩/写入重叠。所有读写成本聚合在 producer work 后的磁盘服务中，因此也未模拟源读取必须先于解码的阶段依赖。输入 work 使用本机观察的 `FactoryWall`，其中已混有本机文件操作等待，再叠加假设磁盘服务会保守重复计入部分本机 I/O。模型时长不是主网预测、真实限速设备墙钟，也不是严格上下界。生产单文件 V2/V3 内部并行方式仍以当前 factory 为准；模型的两个产物 producer 不表示已经给在线生命周期新增两个 builder。

磁盘服务按有效聚合带宽 **50 / 100 / 250 MiB/s** 计算。读和写共享此预算；没有为每个 worker 分配一份带宽，也没有假设随机 IOPS 与顺序带宽等价。该带宽指扣除其他业务后可分配的有效速率，不是对现有服务器的测量。

每个产物的成本为：

```text
physical_IO = source_reads + final_container_write
            + CDC_spool_write + CDC_spool_read
            + parameterized_WAL_write + parameterized_flush_write
            + parameterized_compaction_read + parameterized_compaction_write
service_time = ceil(physical_IO / effective_shared_bandwidth)
```

时间乘除采用 128 位中间乘积并检查溢出；字节累加与假设倍率也检查溢出。不用浮点相乘掩盖极值或将溢出变成零耗时。

## 共同成本不能消失

hot build 原有 CollectKey 和实际记录写入需要两次逻辑历史遍历。OS/Pebble 缓存命中、pack 压缩、修复覆盖等使物理读取量不能简单等于两倍 Prev。因此源读取采用 **0 / 1× / 2× logical bytes** 的敏感性假设：它们不是现场读字节，不是精确热盘布局，也不是保证覆盖所有最坏情况的上下界。两个格式使用相同假设。

LSM 成本独立参数化。报告给出三个明确标为假设的示例端点：

| 示例 | WAL write | flush write | compaction read | compaction write |
|---|---:|---:|---:|---:|
| 容器单独比较 | 0 | 0 | 0 | 0 |
| 示例总计 4× logical I/O | 1× | 1× | 1× | 1× |
| 示例总计 10× logical I/O | 1× | 1× | 4× | 4× |

这些倍率不是测得的 Pebble 放大，也不表示热 history 的表示字节等于冷逻辑流；只是演示未改动的共同成本如何稀释全流程收益。模型的 `historyIOCost` 允许分别输入四项绝对字节，应以未来同范围读写计数替换示例。不能凭当前结果宣称主网放大处于 4–10 倍。

key/posting ETL、accessor/index 生成、整段验证、冷查询读取、物理回收和 MySQL 等其他服务没有自动模拟。若没有可靠字节数据，就明确列为遗漏，不能把输出字节比当整个链路的吞吐比。

## break-even 与 CPU 富余

对同一工作量，简化的串行服务取舍为：

```text
Δcritical_compute_wall < saved_physical_IO / effective_IO_bandwidth
```

`saved_physical_IO` 必须扣除额外 spool 或其他新增读写；两个方案都要发生的共同源读取不能被算成节省。`Δcritical_compute_wall` 是并行之后位于关键路径上的额外 compute 墙钟，**不能直接用 CPU 秒差，也不能未经测量按 CPU 秒除核心数**。已有 factory 墙钟包括 fsync 等等待，若仅有它，则计算是一个粗略敏感性判断，应同时给 CPU user/system 和原始 wall，而非冒充纯 compute。

即使共同读取不改变绝对 saved bytes，它会降低总流程加速倍数。例如输出由 16 MiB 降为 2 MiB，但双方都再读 32 MiB 源数据时，忽略其他成本的 I/O 比是 48/34≈1.41，而不是 8。真实 CPU 关键路径、spool 和其他共同 I/O 会继续改变结果。

## 现有 cooldown 的不足及最小后续建议

当前 [cold pressure recovery](../../core/state/snapshots/cold_pressure.go) 在压力成功路径使用 `clamp(work, 3s, 30s)`；work 来自 build + early-prune + merge 的墙钟。原 forced-busy 是 `clamp(4×work, 30s, 60s)`。该计时同时混合 CPU、读、写、校验和等待；多用富余 CPU 使字节明显下降时，仍可能因为 elapsed 增加而多罚恢复时间。因此它是经验式资源让步，不是磁盘负载控制器。

最小可行改进应先新增**每轮直接输出及 spool 读写字节**，在未取得指标时保持现回退策略，再用配置的共享 I/O 预算或实测磁盘服务债务决定成功后的 admission 延迟。保留单 heavy-work lease、最小公平间隔、空间底线、失败后的完整恢复、无进展退让以及所有原覆盖验证。完整业务字节应由 PassResult 逐阶段传递，不能用全局 Finish counter 差值推断某一轮，因为其他产物/并行任务也会增加计数，失败/未发布文件也有成本。

有长期同卷队列/延迟和 import I/O 的测量后，再将成功恢复改成“预算债务需归还多久”，避免把 CPU 富余当作必需磁盘恢复。暂时没有这种证据时，不应直接删除 cooldown 或以 `stored_bytes` 单一指标取代全部读取/ETL负载。本项没有改调度。

## 运行与结果状态

小样本测试已通过：

```sh
go test ./core/state/snapshots -run '^TestHistoryIO' -count=1 -timeout=120s
```

其中实际 codec canary 使用约 1 MiB 重复列表与 256 KiB 随机输入，验证两格式完整 oracle、spool 计数以及所有情景队列上限。确定性模型测试证明多个 producer 不能分别占一份带宽，共同读取会降低相对收益，并覆盖时间/字节溢出。

较大三类实际 factory 观察为显式 opt-in，普通全仓测试会跳过：

```sh
GTRON_HISTORY_IO_REPORT=1 go test ./core/state/snapshots \
  -run '^TestHistoryIOCostReport$' -count=1 -timeout=180s -v
```

每个格式先 warm 一次共享 encoder，再测一次，得到 6 个观察值及每观察 54 个模型情景。输出固定在 `build/benchmarks/20260907-history-io/observations-<UTC>-<pid>.json`，含输入 SHA、环境、全部成本假设、CPU/wall 和字节。JSON 时长为整数纳秒，字节为 B，MiB=1,048,576 B。单次观察不提供统计置信区间；可在协调好的机器空闲窗口重复命令比较分布。

根任务协调最终窗口后，于 **12:11:57 UTC** 重测 bulk/pipeline 接入后的源码，测试体 0.47s、包 1.443s。以下采用这次最终观察：[observations-20260907T121157.333723000Z-2159.json](../../build/benchmarks/20260907-history-io/observations-20260907T121157.333723000Z-2159.json)，[完整源码指纹与运行上下文](../../build/benchmarks/20260907-history-io/run-context-20260907T121157.json)。环境为 Go 1.27.1、Darwin/ARM64、GOMAXPROCS=10；两格式 factory 传入 4 workers。V2 走 `newCompressedBlockStreamWriterWithFooter`，V3 走 `newCDCStreamWriterWorkers` 的 bulk/Cuts 与有界编码流水线。

compressed block、CDC writer/reader、footer、CDC pipeline、`internal/historychunk/bulk.go` 和 splitter 共七个文件运行前后 SHA 均相同。它们不是完整 release/binary 身份。以下每项仍是一轮 warm 后的一次观察，不能称为中位数或稳定性能倍率。较早 [12:03:49 JSON](../../build/benchmarks/20260907-history-io/observations-20260907T120349.316570000Z-97236.json) 保留为中间基线，不再用它作为本轮最终结论；[旧上下文](../../build/benchmarks/20260907-history-io/run-context-20260907T120349.json) 已标明早于 bulk 完整接入。

| 输入 | V2 / V3 最终文件 B | V3 额外 spool 写 B / 读 B | V2 / V3 factory wall ms | V2 / V3 CPU user+system ms |
|---|---:|---:|---:|---:|
| 重复大列表 | 16,783,058 / 2,362,146 | 12,712 / 12,712 | 21.716 / 24.006 | 18.514 / 24.423 |
| 小账户形状 | 2,219,141 / 2,219,679 | 2,492 / 2,492 | 21.777 / 24.632 | 40.777 / 50.399 |
| 低重复随机 | 8,391,520 / 8,396,782 | 5,292 / 5,292 | 15.213 / 21.211 | 10.034 / 22.233 |

CPU 总和可以大于墙钟，因为包含多个并行线程；也可以小于墙钟，因为有等待。重复大列表在扣除额外 spool 写读后每个产物少 **14,395,488 B≈13.729 MiB** I/O，观察 factory wall 增加约 2.291 ms。代入简化公式得到约 5,994 MiB/s 的粗略 crossover；小差分很容易受本机噪声影响，而且 factory wall 混有本机 I/O，不能当真实设备分界或主网选型阈值。

以下是重复列表、8 个产物、1 个 producer、等待队列 2、无 LSM 额外计账时的**模型总时间**。每个模型同时允许 producer 与上一产物的磁盘服务重叠，不能直接以八倍单产物 serial 时间替代。

| 共享有效带宽 | 共同源读假设 | V2 / V3 模型总时间 ms | V2/V3 时间比 |
|---|---:|---:|---:|
| 50 MiB/s | 0× logical | 2,582.607 / 388.320 | 6.651 |
| 50 MiB/s | 2× logical | 7,702.629 / 5,508.343 | 1.398 |
| 100 MiB/s | 0× logical | 1,302.161 / 214.818 | 6.062 |
| 100 MiB/s | 1× logical | 2,582.167 / 1,486.169 | 1.737 |
| 100 MiB/s | 2× logical | 3,862.172 / 2,766.174 | 1.396 |
| 250 MiB/s | 0× logical | 533.894 / 201.156 | 2.654 |
| 250 MiB/s | 2× logical | 1,557.898 / 1,120.873 | 1.390 |

同为 100 MiB/s、2×源读时，再加入双方相同的 4×或10× logical LSM 示例成本，时间比分别降到 **1.139 / 1.070**。这些只是成本敏感性结果：它们证明不能把最终文件约七倍的大小差直接当成全流程七倍；没有测出生产 LSM 成本，也没有据此给主网吞吐或完成 ETA。
