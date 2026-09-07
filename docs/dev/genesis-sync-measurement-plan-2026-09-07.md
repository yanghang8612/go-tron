# 从创世重新同步的采集、验收与止损

2026-09-07。本文为用户授权重建后的执行方案，不记录尚未发生的生产成绩。编写与采集器测试仅在本地进行，没有连接服务器、删除数据库、启动节点或创建后台自动任务。部署、启动和必要时正常停机由根任务执行。

目标是在固定共享磁盘、MySQL 保持原位置、保留全部历史状态查询的条件下，测出实际同步进度、冷热处理进度和全流程 I/O。**早期同步通过不代表整条主网能装入当前磁盘**；历史随链继续增长，必须保留容量止损和后续阶段复验。

## 启动前固定实验身份

记录实际执行的 binary SHA256、build info、源文件/发布包身份、脱敏启动参数、配置与覆盖环境变量；仓库 commit 不能代表本轮有未提交改动的 binary。保存启动时间、网络/创世身份、实际 datadir、运行用户和 PID/starttime。旧实验与新创世运行分别建目录，不跨重启拼接计数。

运行路径以启动参数和实际进程为准：既有主网实例的节点 datadir 为 `/data/gtron/main/datadir`，Pebble 位于 `/data/gtron/main/datadir/gtron/chaindata`。不能漏掉中间的 `gtron` 层。启动前一次确认 `/data` 的挂载设备、容量、可用空间、inode、MySQL 所在卷及服务状态；不要因为文档曾记 `/dev/nvme1n1` 就跳过现场核对。

保留 `snap` 的全历史语义及既定热窗口 65,536；不要误把名为 `full` 的另一保留模式当成“所有历史状态查询”。确认启动日志中的 history enabled、mode、window、cold lifecycle、实际压力阈值及 `history.compression-format=auto`。本次不通过缩短历史保留、跳验证或改为无限 cold 并行来制造成绩。

指标需要在启动时显式启用：`--metrics --metrics.addr=127.0.0.1 --metrics.port=6071`。6071 是本方案建议端口，须先确认未占用；当前 CLI 的默认 metrics 端口仍是 6061，不应依赖默认值与 Nile pprof 共存。采集前用实际 listener 核实该 metrics 端口属于所选 gtron PID，避免把另一实例的 head 与本实例的 I/O 混合；脚本不自动认证 HTTP listener 的 PID。采集只请求本机 `/metrics`，不把它代理到公网。来源：[启动接线](../../cmd/gtron/main.go)、[历史模式](../../cmd/gtron/history_config.go)、[观测说明](observability.md)、[指标 HTTP 实现](../../internal/metricsapi/server.go)。

## 最小采集

新增 [genesis_sync_sample.py](../../scripts/dev/genesis_sync_sample.py) 为 Python 标准库、Linux 只读观察器。它不打开 Pebble，不枚举数据库文件，不调用服务管理命令、不发送进程信号、不连接远端。JSONL 写到 stdout；由操作者重定向到 root 卷唯一实验目录，避免放在紧张的 `/data` 卷。

第一轮建议 15 秒一次、121 点，约 30 分钟。在根任务已经启动节点并确认实际 PID、设备和 metrics 端口之后执行，例如：

```sh
python3 scripts/dev/genesis_sync_sample.py \
  --pid ACTUAL_GTRON_PID --device nvme1n1 --metrics-port 6071 \
  --data-path /data --interval 15 --samples 121 \
  > /var/tmp/UNIQUE_GENESIS_RUN/samples-0001.jsonl \
  2> /var/tmp/UNIQUE_GENESIS_RUN/samples-0001.stderr
```

上面 PID 和目录是待替换占位，目录由根任务预先创建且文件应不存在。不要把该示例理解为已执行。后续仍以每 30 分钟至 2 小时一个采集段为宜；程序最多约 6 小时、1,441 点、128 MiB 输出，每次读取和 metrics 响应最多 4 MiB，HTTP socket inactivity timeout 为 2 秒。它没有严格的 HTTP 整体墙钟 deadline，正常本机 exporter 不应持续慢发；操作者应为整个采集命令设置略大于计划时长的外层 timeout。采集结束、超限或中断**不会停止节点**，不得用它替代值守或服务停止流程。

每点保存以下原始数值，只有有有效相邻观测时才输出速率；缺失明确记为 missing/error，不能当零。固定 PID 的 `/proc/PID/stat` starttime 在每点前后核对；进程消失或 PID 复用时结束采集并报错，下一进程必须新建记录。

| 数据 | 频率与口径 |
|---|---|
| head、solidified、sync remaining、peers、import window | 每 15 秒，现有 Prometheus gauges；附采样 UTC、monotonic 和采集耗时 |
| cold eligible/published/visible tx、batch/recovery/deferred、prune frontier、verification/lifecycle errors | 每 15 秒，保留对应 metric 名及原值；这些 gauge 非原子快照且可能在 pass 完成后才更新 |
| gtron CPU 与 RSS/线程 | `/proc/PID/stat` 的 user/system ticks、starttime、RSS pages；`/proc/PID/status` 的 VmRSS/VmHWM/VmSwap/Threads；记录 HZ 与 page size |
| gtron I/O | `/proc/PID/io` 的 read_bytes/write_bytes、rchar/wchar、cancelled_write_bytes 等；保留物理记账与应用调用字节的区别 |
| 共享卷 I/O | `/proc/diskstats` 指定整设备的读写完成次数、512 B sectors、读写时间、inflight、io_ms、weighted_io_ms；不把整盘和分区再相加 |
| 主机资源 | `/proc/stat` CPU ticks、`/proc/meminfo`，以及 root 与 data 的 `statvfs` 可用 B/inode；不执行递归 `du` |
| 日志 | 保留原有轮转配置；启动、每个验收节点及异常时截取有字节上限的日志尾部/增量，不每点重新读完整日志 |

采集器包含 `state_`、`chain_`、`sync_`、`compact_`、`level_`、`disk_` 等现有指标。默认 Pebble namespace 为空，实际指标是 `compact_input`、`level_6_compact_write` 等，不能只筛 `pebble_` 前缀。geth exporter 将 `/` 转为 `_`；meter 的导出值为累计 Count，不是瞬时每秒速率。首次出现的 per-level 指标可能延迟注册，缺失不等于零。源码：[Pebble 指标](../../core/rawdb/pebbledb/pebble.go)、[节点 gauges](../../internal/metricsapi/node_collector.go)、[导入 window](../../net/sync_observability.go)。

采集器不高频读取日志，默认 Info 中的 `Imported chain segment` 和 `Sync progress` 足以补充区块、交易和阶段耗时。`History cold snapshot published` 的 Info 可能限频/汇总，不能把可见 Info 行数当作全部批次数。必要时只在一个有界诊断窗口增加指定模块日志。不要持续开启 CPU profile、heap dump、mutex/block profile 或全库审计。

## 需要计算与保留的指标

以同一 PID 内的 5/30/60 分钟窗口计算差分，展示分子、分母、块高区间与持续时间。15 秒速率用于发现突变，不用于外推主网完成 ETA。

| 指标 | 计算与限制 |
|---|---|
| 导入速度 | `Δhead / Δt`；并列 transactions/s 与每块交易量。负差分先查 reorg/重启，不静默改成零 |
| 未冷化跨度 | `max(0, eligible - published)`；eligible 来自 runner 本轮安全 cutoff，受 solidified、热窗口及 verified Finish gate 约束 |
| 已冷覆盖待热删跨度 | `max(0, published - hot_pruned)`；短暂跨 pass 的 gauge 不一致需要复核，不能据单点宣称越界或数据损坏 |
| 冷化/回收速度 | 分别计算 `Δpublished / Δt` 与 `Δhot_pruned / Δt`；都只是区块边界速度，不能解释成字节吞吐或实际释放速度 |
| CPU | `Δ(user_ticks+system_ticks)/(HZ×Δt)` 得到使用核心数；乘 100 为“单核心=100%”口径。不能直接称整机 CPU 百分比 |
| I/O | gtron 与整盘分别计算 `Δread_bytes/Δt`、`Δwrite_bytes/Δt`。`rchar/wchar` 包含缓存与其他 I/O，不能拿它们当设备字节；取消写计数单列，不随意减出负“物理写量” |
| 队列和延迟 | `Δweighted_io_ms/(1000×Δt)` 为平均队列深度近似；读/写毫秒除完成次数为延迟代理。NVMe 并行和设备记账使 `busy_fraction≈1` 不等于证明已达到最大带宽 |
| 字节/块 | gtron 设备记账读写增量、SST compaction input/output、全卷净使用增量分别除同窗口 `Δhead`，不要混成一个放大率。无导入进度时仍记录后台 I/O |
| 物理容量 | `available_bytes` 变化包含 MySQL、日志、冷输出、Pebble 压实和其他服务；hot prune 计数增加不保证当点可用空间增加 |
| CDC | 比较 `Δcompression_cdc_stored_bytes / Δcompression_cdc_input_bytes`，另列 reused/files。只统计成功 Finish 的 V3 文件，非所有历史、非 manifest 成功数、非热库净节省率 |

没有单独采样 MySQL 时，不能从全盘 I/O 减一个假想固定背景值得到“纯同步 I/O”。需要归因时可在同一有限窗口对实际 MySQL PID 的 `/proc/PID/io` 做平行原始采样，仍考虑内核 writeback/元数据记账的差异。不要暂停其他业务来制造不可持续成绩。

## 分阶段验收

1. **启动与前 30 分钟**：核对确为新的创世运行、网络/创世/binary/config 相符，metrics 可读，head 和 solidified 前进，进程 starttime 稳定，没有错误/重启循环、WAL/磁盘错误或 OOM。保存第一轮完整原始 JSONL。此阶段只能说明新编码和同步路径能开始工作。
2. **跨热保留窗口并观察真正冷热闭环**：至少跨过 65,536 窗口，进一步等到实际 cold publication 与原门控 hot prune 前沿都推进；最好观察连续三轮成功完成。深同步默认在未冷化 ready blocks 不超过 `4×window` 时可以延期，importer busy 时又可延期到 `8×window`，压力模式可以较早准入。因此“head 到 65,537”不等于必然触发 cold；busy 常态下可能需约 `9×window=589,824` 加 solidified/Finish 差额才自然越过强制边界，此数是调度条件量级，不是承诺具体启动高度。不能通过缩短保留窗口或跳覆盖门验收。
3. **历史查询闭环**：在第一次热裁剪前，保存少量已发生非空状态变更的账户/合约/槽位在固定已 solidified 块 B 的查询结果与该块 hash；等 B 确实落在已经裁剪的区间，再查询同一块/键并逐项比较。至少一个样本还须证明该键在 B 之后的块 C 再次变化，且 C 也落入已裁剪范围，否则始终未再变化的键可能只从 latest 返回，不能证明 cold 历史已参与重建。可复用 [archive fixture capture](../../scripts/dev/archive_state_fixture_capture.py) 对 `eth_getBalance`、`eth_getCode`、`eth_getStorageAt` 的方式；不能只用不存在地址返回零的样本。API canary 是小样本端到端证明，不是全库审计；完整历史安全仍依靠生产原覆盖/校验/持久化门。若某种账户/合约尚不存在，记录该类别未覆盖，后续补验。
4. **容量与持续进度**：初次 cold/prune 后再取至少一个 30 分钟完整窗口，观察未冷化跨度、已覆盖待删跨度、可用空间、compaction debt、write stall 与 importer 进度。可以在阶段边界做一次目录大小统计，或正常停机后做原有只读存储审计；不要在线另开 Pebble 诊断进程与节点抢锁，也不要高频全盘扫描。
5. **后期业务重新验收**：创世早期小账户与短列表不能代表 28M–30M 的巨大委托反向列表。auto 早期选择 V2、CDC 计数为零可以完全正常。只有大 Prev/重复大 key 的候选实际出现，且 V3 成功输出、冷发布、裁剪与旧状态查询都完成后，才能评估本次大头优化。后期在相同块高范围记录累计输入/输出、全流程 I/O、CPU 与容量增长，不拿早期 bytes/block 线性推到主网末端。

这里的历史查询前后比较需要保留同一 canonical block hash，避免正常 reorg 换链造成无效比较。正在增长的 gauge 不是磁盘 manifest 的完整证明；若要持久化审计，应由根任务正常停止节点后执行现有只读工具，而非从在线目录直接拼接一个“稳定快照”。

## 止损与人工执行边界

新构建的运行时 2% free admission 是启动新工作之前的信号，不是逐次写入的硬空间 guard；MySQL 也可以同时消耗空间。因此实验停止线必须更早，不能等到 builder 自己拒绝工作才处理。

| 条件 | 本轮执行建议 |
|---|---|
| `/data` available ≤10% | 立即复核最近 30 分钟下降速度、两个积压跨度和其他业务用量；缩短人工检查周期 |
| `/data` available ≤5%，或按最近 30 分钟持续净下降速度距 5% 线少于 2 小时 | 根任务结束本轮同步实验并正常停机，保存采集与错误日志，不继续盲跑等待磁盘耗尽 |
| root available ≤5 GiB 或 inode 可用比例持续低于 5% | 同样处理；报告落在 root 卷也需要余量。采集器记录这些原值，只有 data 百分比告警为内置，其余由根任务判断 |
| ENOSPC/EIO、校验/覆盖错误、历史查询固定 hash 不一致、OOM、反复重启 | 立即停止扩大实验，正常停机并保留现场；不自动切旧 binary 或删更多数据掩盖问题 |
| 有有效冷候选且已过调度延期条件，但 published 连续 15 分钟不进；或已发布覆盖而 hot_pruned 连续 15 分钟不进 | 先查阶段进度、verification、门控、recovery 和错误；有持续 I/O/校验进展的长批不能仅凭边界未更新判死锁。确认无工作进展或有重复错误后结束实验诊断 |
| head 连续 5 分钟不进 | 检查 peers、target、buffer/retry、commitment rebuild、write stall、磁盘队列及阶段进度；追到 tip 或 rebuild 正常推进不算故障 |

以上 10%/5% 为本次实验的审慎操作阈值，不是磁盘厂商保证或新的 runtime 默认；应按现场 `statvfs total` 算绝对 B。若总量恰为 7 TiB，则分别约 716.8/358.4 GiB，早于约 143.36 GiB 的 2% 构建底线。时间到线只是按最近窗口的警报预测，冷临时输出、LSM compaction 和 MySQL 突发可能使它失准；不要当作保证还能运行两小时。

采集器仅输出告警，**不会帮根任务执行停止动作**。长时间继续同步必须有实际检查与明确停止执行者，不能只让采集器到时退出而无人检查。正常停机后确认进程退出、端口与服务状态、记录最后 free/frontier；若退出缓慢，先查收尾阶段，避免习惯性强杀打断持久化。本文没有实施任何定时服务、自动重启或自动停机任务。

## 结果报告最低内容

每轮给出：binary/config/网络身份、UTC 与块高区间、head/eligible/published/pruned 起止值、各速率及窗口、进程 CPU 核心数与 RSS 峰值、gtron/整盘读写 B、平均队列代理、Pebble compaction/stall、可用空间起止值、错误与重启、所覆盖的历史查询 canary、尚未达到的阶段。标明完整窗口还是部分窗口、是否发生冷构建/裁剪、是否真的出现大列表 V3 输入。

本地验证：`python3 -m unittest discover -s scripts/dev -p 'genesis_sync_sample_test.py'` 覆盖 Linux stat 的含空格/括号进程名、diskstats 512 B sector 单位、整数 metric 精度、缺失/非有限值、重启身份不合并、字节读取上限；不启动网络或节点。真实服务器采样与端口/权限验证仍由根任务执行，不能把本地 parser 测试说成生产运行通过。
