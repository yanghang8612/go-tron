# 状态历史与事件日志的双分支构建

本轮接续 `a869cfb1` 的缓存及批次成本修复。该版本完整十分钟状态84.370块/秒、同步87.665块/秒，积压仍净增1,977块；因此继续缩短同一状态批次的关键路径。保留当前数据库和全部历史，不改变文件格式或查询语义。

## 实现边界

同一已固化范围的状态历史和V4事件日志读取不同的数据，没有新文件依赖。本轮在原共享维护lease内并行两个原有builder，各自的压缩worker上限保持不变。两边退出、全部构建成功且父任务尚未观察到取消后，才统一组装manifest发布，后续严格校验和热裁剪顺序保持。

只在吞吐同步模式、必需事件侧车、新鲜健康的存储观测、至少8个Go调度线程，以及额外CPU/内存探针全部允许时启用。其他情况保持串行；串行构建成功后重新观察同步状态，保留原来的balance/section侧车延期行为。既有内部扫描不能即时抢占，取消时等待当前有界工作全部退出，不提前释放lease。未发布内容地址文件沿用原孤儿安全处理；不能盲删可能被旧manifest引用的同名文件。manifest已发布后的阶段写入失败仍保留文件，不能将所有错误概括为未发布。

使用现有生产并发安全的Pebble和完整ChainDB读接口，保留ancient回退和external-log还原。同步暂存cleanup使用不同键前缀；正文转换与对应热删除受同一lease保护，历史裁剪依赖Runner的有序发布与验证覆盖。没有新增MVCC或伪装的可写快照适配器。此结论限于当前生产接线和固定已固化范围。

## 资源准入

探针懒采样，不启动后台任务或扫描数据库。每5秒读取有界proc/cgroup文件，CPU取间隔5–30秒的两点平均。只统计亲和性允许的CPU，guest不重复计数，iowait与steal不作为空闲；从min(GOMAXPROCS、NumCPU、允许CPU数)中扣除观察到的忙核，避免GOMAX较小时高估余量。动态调低GOMAX立即收紧；有限CPU quota保守串行，不从宿主闲核推断cgroup额度尚有剩余。

启用门槛为实际idle至少25%、剩余执行能力至少2核，以及至少2GiB内存余量。内存取MemAvailable和各可见cgroup父层硬/软限制减用量的最小值。首次、陈旧、时间/计数回退、范围变化、慢读取、缺失或截断均拒绝并行。资源提示仅用于准入，不能保证整个批次的负载或峰值内存。

服务器已核为16核、普通cgroup v1、gtron.service硬上限40GiB、CPU quota无限；本轮按该环境原生验证。私有cgroup namespace可能隐藏宿主祖先，不能从可见挂载根推导任意容器的全部限制。

实际运行指标为 `state/snapshot/cold/history/event_parallel/` 下的 attempts、builds、last/paired、last/build_wall、last/history、last/event。last仅在真正获准历史构建时更新，延期轮次不覆盖；分支耗时可以重叠，批量密度和完整恢复继续使用实际总墙钟。探针指标位于 `history/parallel/runtime/`，ready不能替代实际并行计数。

## 本地验证

真实Pebble定向测试通过2.418秒，对应race通过3.806秒；资源探针定向通过1.152秒，race通过2.087秒。覆盖实际双源重叠、失败/取消时等待另一分支且lease保持、manifest/stage/prune不提前推进、失败重试、预存输出不被误删、发布后阶段失败保留文件、串行idle→sync时序与错误优先级。串并行全部文件引用/checksum、完整状态变化和事件查询相等，包括protobuf未知字段；并发同步暂存写删通过race。

受控同输入基准：Apple M1 Max，Darwin arm64，实际10核，显式GOMAXPROCS=16、event worker8。256块×32交易，共8,192交易、16,384状态变化，prev/log数据各2KiB；包括真实读取、两遍扫描、压缩、校验、fsync、统一发布及阶段写入。每次新清单，没有模拟生产的大存量清单和同步/正文转换争用。

| 三次中位数 | 串行 | 双分支 |
|---|---:|---:|
| 完整OnePass | 265.817ms | 215.878ms（耗时减少18.8%） |
| 内层BuildDuration | 260.108ms | 208.607ms（耗时减少19.8%） |
| 输出大小 | 51,287,680B | 51,287,680B |

所有输出引用和checksum相等。各分支因争用略变慢，总耗时因重叠而减少；不能承诺理想35%减时，也不能将累计分配B/op当作峰值RSS。

命令：`GOMAXPROCS=16 go test ./core/state/snapshots -run '^$' -bench '^BenchmarkParallelHistoryEventFullBuild$' -benchtime=1x -count=3 -timeout=180s`。正式本地原始证据在 `build/benchmarks/20260909-parallel-history-event/{targeted,race,benchmark-gomax16}.txt`；资源探针证据在上一轮 `20260909-state-cost/parallel-runtime-probe-*`。两位独立审查完成，已修正串行规划时序和CPU有效余量的计算。

全仓与服务器原生验证正在单独进行，尚未依据本地基准宣称线上积压已解决。
