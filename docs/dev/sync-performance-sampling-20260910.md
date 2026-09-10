# 主网同步性能采样：2026-09-10

采样指向的首要优化对象是**前台逐块交易执行与状态访问**；异步 commitment 是显著的 CPU、读取和分配成本，后台冷化/压实则带来资源竞争与吞吐波动。下载供给充足，现有数据不支持优先增加 peers、签名 worker 或提交队列深度。10 分钟平均同步 **109.17 块/秒**，状态冷化 **111.95 块/秒**，状态距离净减 **1,670 块**，但持续追赶余量很小。

本轮只做线上采样与本地分析，没有部署、重启、改配置、启用 block/mutex/trace 或强制 GC。所有时间为北京时间（UTC+8），日期为 2026-09-10。

## 采样范围与身份

- HTTP metrics：**06:32:36.309–06:42:36.110**，41 点、请求单调时钟中点跨度 **599.798 秒**，全部成功；1,456 个指标完整，进程精确标识始终为 `1788948199153245090`，前沿及 count 型累计计数无回退。主代理另从原始响应独立重算端点和吞吐。
- Wallet getnodeinfo：**06:33:43.931–06:41:43.938**，33 点、480.005 秒；由同一进程的 metrics 窗口完整包围，端点不与主窗口混用。
- 两组 CPU profile：06:33:39、06:35:17 开始，各约 30.17 秒；两份 heap、两份 goroutine，全部保留原始文件。
- 服务器资源：**06:35:09.542–06:40:09.552**，31 点、300.010 秒。服务器执行只读 `/proc` 与 metrics 采样，PID/start_ticks 连续、采集错误和告警为空。完整 JSONL 留在服务器，本地资源摘要为终端截图转录，并非本地下载后重算。

服务器核验 `gtron.service` active/running，PID **25232**，启动时间为 9 月 9 日 18:03:19。实际文件为 `/data/gtron/releases/20260909-parallel-history-event-9059eb7f/gtron`；SHA-256 为 `2d3f9707c176c3e15b30d492d676e8594b483200a9569912b649ef7407bf4991`，两份 profile Build ID 均为 `d8e6da771cef1a9df3a65fc362135973421779ca`。已读取实际监听，gtron 使用 18890、8090、8545、50051，pprof/metrics 为 loopback 6062/6071。

## 实际同步与冷化

| 前沿 | 起点 | 终点 | 10 分钟增量 | 块/秒 |
|---|---:|---:|---:|---:|
| 主链 head | 20,685,671 | 20,751,149 | 65,478 | **109.167** |
| 状态 published | 20,094,102 | 20,161,250 | 67,148 | **111.951** |
| 状态热裁剪 | 20,094,102 | 20,161,250 | 67,148 | 111.951 |
| 冷正文 coverage | 20,054,016 | 20,119,552 | 65,536 | 109.263 |
| 交易索引 coverage/pruned | 20,054,016 | 20,119,552 | 65,536 | 109.263 |

状态距离采用当前 `head − published`，从 **591,569 降到 589,899 块**；包含正常保留范围，不能全称可立即清除的数据。`last_eligible_cutoff` 会陈旧，没有用它代替当前 head。起末 published/pruned 相等，正文与索引起末也相等。

**前 9 分钟状态距离增加 6,757 块，最后一分钟减少 8,427 块，才使全窗净减 1,670 块。** 06:36:36–06:37:36 附近，状态发布整分钟未前进，head 增加 6,861，正文推进一个 65,536 块段；该正文任务记录工作时长仅 **13.309 秒**，不能将整分钟都算作正文执行。窗口同时发生两次冷合并、资源 gate 让行与恢复，现有指标无法精确分摊各 owner 的等待墙时。一次窗口的小幅净追赶不证明长期稳定清积压。

Wallet 独立 8 分钟窗口推进 52,384 个 session blocks、1,699,112 笔交易，即 **109.13 块/秒、3,539.78 交易/秒**。33 点全部 active、未 paused；30 个连接 peers、8 个同步 peers，缓存 **62,502–63,630 块 / 845–858 MB**，fetchBackpressured 全部为 true、retryBlocks 全部为 0。这是下游导入消化速度限制下载的证据。

## 导入阶段与 CPU 关键路径

最近导入指标是滚动窗口 gauge，按 `updated_unix` 去重。20 个可见报告中，18 个完整位于采样内的报告用于加权，覆盖 543.276 秒、59,872 个 apply 样本块。每块阶段按 apply blocks 加权，忙碌率按报告时长加权。4 个报告的 apply 覆盖数各有 ±1 块偏差，覆盖率为 99.9527%–100.0319%；汇总后 apply/import 都为 59,872 块，阶段统计采用实际 apply 样本数。

| 顶层阶段 | 平均 ms/块 | 顶层工作计时占比 |
|---|---:|---:|
| Execute | **6.423** | **49.40%** |
| StateCommit | **4.845** | **37.26%** |
| Persist | **1.659** | **12.76%** |
| Validate、Maintenance、DPUpdate、Hooks 合计 | 0.075 | 0.58% |

导入调用忙碌率 **99.361%**，缺连续 raw block 的 bufferWait 为 **0**；这不是 CPU 使用率。异步提交 enabled=1、depth=4，排队背压累计仅 **3.142 秒，占主窗口 0.524%**。该计数只覆盖提交队列发送等待，并非所有同步依赖。Apply 平均工作计时 13.003 ms/块，实际 import 9.016 ms/块：前后台跨块重叠，因此不能将表内占比当作串行关键路径或 CPU 占比，交易/VM 等内层也不能重复相加。

| CPU 根调用路径 | 第一组 CPU-s | 第二组 CPU-s |
|---|---:|---:|
| 前台 applyBlockWithPlan | **24.21** | **24.16** |
| commitment runPartition | **24.97** | **25.01** |
| commitment prefetch lanes | 6.00 | 6.53 |
| commit worker 协调/发布 | 3.05 | 3.21 |
| 签名预热 worker | 13.54 | 11.61 |
| Pebble compact 根栈 | 11.69 | 4.82 |
| 冷化/history/event 根栈合计 | 0.00 | **25.91** |
| GC 后台标记 | 2.24 | **8.91** |
| 整个进程 CPU 样本 | 100.12 | 126.39 |

两组均约 30.17 秒。前台自身约占 **0.80 核**，导入调用又几乎持续忙碌，说明优化前台单块路径有直接价值。第二组 CPU 总量上升主要伴随后台冷化与 GC，前台绝对 CPU 几乎不变；两组输入不同，不能据此作优化 A/B 或精确因果归因。

前台过滤后的具体热点：交易执行 **16.26/15.40 CPU-s**，其中 TVM **7.27/6.76**；`ReadStateKVLatestNoCopy` **3.63/4.25**、`IterateAccountKV` **2.79/2.26**、`ContractRuntime` **2.40/2.57**。这些有嵌套关系。状态访问包括 cache/layer 查找、prefix 合并、元数据解码、数据库内存结构与物理读取，不能统一称为磁盘耗时。相关入口：`core/state/account_kv.go:1378`、`core/blockbuffer/buffer.go:2241`、`core/state/contract_runtime_metadata.go:116`。

commitment partition 中 `nodeHashWithStats` **10.47/10.48 CPU-s**，父分支 `GetBranchInto` **6.41/6.94**，分支 flush **5.22/4.59**；prefetch 在另一个 goroutine。已存在 64 partitions、父分支缓存与预读，当前排队背压很低，不能仅因 commitment 总 CPU 大就宣称扩大流水线一定提速。入口：`core/state/domains/commitment_partitioned.go:162`、`commitment_tree.go:716`、`commitment_staged.go:765`。

签名两组包络 ready_at_import 分别增加 264/220，pending 与导入后 overlap 均无增加。签名成本已经被提前执行覆盖，增加签名 worker 不属于当前优先项。两份 goroutine 快照的前台导入均 runnable，commit worker 为 select；瞬时快照不能当作等待比例，但也没有支持 commitment 长期堵满。

## 资源、分配与后台竞争

服务器独立五分钟资源窗口：

| 指标 | 结果 |
|---|---:|
| gtron 平均 CPU | **3.979 / 16 核** |
| gtron RSS 采样峰值 | **22.320 GiB** |
| 共享 nvme1n1 busy | **77.72%** |
| 共享盘 await | **1.099 ms** |
| 共享盘平均队列 | **4.306** |
| 共享盘读 / 写 | **127.71 / 86.75 MiB/s** |

全十分钟 metrics 的进程 CPU 累计差也约 **4.014 核**。CPU 未整机饱和，磁盘忙碌但没有足够证据判定持续硬饱和。共享盘还承载 Java、MySQL 和其他进程，终端也观察到 tracker 进程；上述设备数值不能全归因 gtron。CPU profile 不包含 off-CPU I/O 等待，前台 ReadAt CPU 较低也不能排除读取延迟。

资源子窗口 head +32,212、state +25,022，状态距离反而增加 7,190，说明取窗差异会改变结论，不能混用其端点与十分钟 HTTP 端点。共享文件系统 available 增加 798,474,240 B，不能当作 gtron 净回收量。

两份 heap 相隔 **96.463 秒**，alloc_space 差分 **48.11 GiB，即约 510.70 MiB/s**；存活 heap 仅增加 238.65 MiB。这是带 GC 周期滞后的采样估计，不是内存泄漏量或 RSS 增长；全十分钟 allocation counter 约 **463.69 MiB/s**，口径不同但都显示高分配。

分配热点：`rawdbBranchStore.putBranches` 独占 **6.72 GiB**；`loadManifestFile` 累计 **8.35 GiB**，其中包括 ReadFile/clone/decode；`cloneDecodedManifest` 独占 **1.59 GiB**，`Manifest.Validate` 独占 **1.54 GiB**。累计路径不能与其子项重复相加。branch arena 当前随不可变 layer 存活，不能提前回收到池中。

清单缓存命中 **715/(715+109)=86.77%**，预算 256 MiB，budget rejection/bypass 无增加，已实际有效。`manifest_cache.go:78` 仍每次读当前文件，命中时复制对象，变化时解码；简单扩缓存无法消除这些成本。53 次成功 history 构建中 41 次并行（**77.36%**）；12/41 个观测点处于软压力预算，硬压力为零。优化应降低完整生命周期成本，维持当前内容验证、统一发布与资源恢复语义。

## 优化优先级与验证目标

1. **前台状态访问和元数据解码。** 先针对实际交易路径的 point read、prefix 迭代、ContractRuntime 重复解码、domain-change capture 降低每块 CPU/分配；验证相同区块输入下 import ms/块、执行 ms/交易及状态/回执结果一致。
2. **commitment 分支读取、哈希和编码分配。** 核查重复父分支查找、同一分支重复 hash/encode、arena 保留量。保留相同根与编码，分别衡量 partition CPU、物理/缓存读取、背压及真实同步吞吐；不能只报告总 CPU 下降。
3. **冷化清单与完整生命周期分配。** 合并同一 pass 内可安全共享的不可变清单视图、读取/克隆/校验，减少 GC 与压实竞争；验证多个正文/合并周期内 `head−published` 持续下降，并保留热裁剪与索引追赶。

本轮未实施候选改动，不能提供预计提速百分比。增加 peers、扩大 depth、单纯增加签名 worker、扩大已有效的清单缓存，都缺少当前数据支持。

## 错误口径与复现证据

采样所观测 cold/prune/freezer/commitment/SST 读取错误无新增，主链持续前进。`sender_chain/errors +67`、`vm_sender_chain/errors +62`（readiness 51、unsupported 5、result 6）仍增长；它们是每 64 块运行的 shadow 预执行拒绝，窗口内 parallel_transfer/parallel_vm 均关闭，不表示 canonical 导入失败。没有完整 canonical failure 总计数，不能宣称所有错误为零。日志检查仅为当时尾部 5,000 行的 WARN/ERROR/CRIT/panic 筛查，未命中，不代表全历史日志审计。

原始与分析目录：`build/benchmarks/20260910-sync-sampling/`。

- `capture.jsonl`、`metrics-000…040.json`：完整 metrics 原始响应及请求时间。
- `wallet-capture.jsonl`、`wallet-000…032.json`：真实 Wallet 状态。`node-*.json` 是不可用的 `gtron_nodeInfo` RPC 探测响应，已全部排除分析，不作节点故障证据。
- `analysis-independent.py/.json/.md`：可复跑的指标检查、逐分钟趋势、阶段加权、错误明细；`root-crosscheck.json` 为第二次独立端点核算。
- `profiles/analysis.md`、原始 CPU/heap/goroutine、top/cum/tree/raw 和调用栈过滤脚本：详细热点证据。
- `server-identity-ui-transcript.json`、`server-resource-summary-ui-transcript.json`：明确标记来源的服务器终端转录；完整服务器资源原始文件为 `/var/tmp/gtron-sync-sampling-20260910/resources.jsonl`。

复跑本地分析：`python3 build/benchmarks/20260910-sync-sampling/analysis-independent.py`。本次是移动历史高度的生产观察，两个 CPU 窗口、五分钟资源窗口、八分钟 Wallet 窗口、十分钟主窗口各自使用各自时长，不能替代固定输入性能实验。
