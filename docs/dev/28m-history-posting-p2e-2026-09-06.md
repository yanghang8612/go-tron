# P2e：历史索引装配的有界内存缓冲

2026-09-06。用户已明确取消“同一 28M 区间重放验收”，本轮沿用主网直接部署、连续采样与运行检查。以下局部固定输入基准用于验证实现，不要求主网回到历史高度。

## 定位与实现

P2d1 的 03:53 UTC 主网 CPU 样本长 25.18 秒，共 95.94 CPU-s；`BuildAccessorContext` 为 24.43 CPU-s，`V7PostingWriter.flushKey` 为 21.89 CPU-s，`os.File.Truncate` 为 9.86 CPU-s。完整阶段日志中，112,758,934 条历史记录的压实在 03:51:20 进入 build-accessor，03:55:41 完成，该阶段约 261 秒。SHA 校验是此前 validate-sources 阶段的热点，本轮先消除最新定位的逐 key 临时文件操作。

旧流程每个 key 都写 scratch、Seek、回读、Truncate、再 Seek，即使 key 只有一条 posting。现在每个 writer 对编码 payload 保留至多 64 KiB；超过后才创建复用临时文件，并以 64 KiB 缓冲顺序写入。普通 key 不访问 scratch 文件，大 key 完整输出后截断复用。frame offset 改成内存计数；metadata 用顺序缓冲读取，key blocks 和最终目录用缓冲顺序写入。

V7 字节布局、每 frame 128 postings、CRC、完整 SHA、Sync/Close/rename、manifest 发布和历史 as-of 语义不变。payload 新增预算最多 128 KiB（内存区和 spill writer）；另有 metadata 64 KiB、两个 1 MiB 输出缓冲。既有 frame directory 随当前 key 的 frame 数增长，不把 64 KiB 声称为整个 accessor/进程内存上限。

指标 `state_snapshot_cold_posting_assembly_{memory_keys,spill_keys,spill_bytes}`统计已装配的 key stream；后续整个 build 仍可能取消，因此它们不等同于已发布文件数。

## 固定输入基准

完整 `BuildAccessor` 包含 ETL load、posting 编码、metadata/key 装配、checksum、Sync 和 rename；输入构建不计时。旧源码通过 Go overlay，GOMAXPROCS=2，各三次，中位数：

| 形状 | M1 旧/新 | Linux EBS 旧/新 |
|---|---:|---:|
| 32,768 keys × 1 posting | 7,170.8 / 23.1 ms | 511.9 / 39.9 ms |
| 4,096 keys × 32 postings | 192.2 / 22.9 ms | 106.0 / 49.2 ms |
| 8 keys × 32,768 postings | 73.9 / 30.4 ms | 71.5 / 72.3 ms |

Linux 为 Xeon Platinum 8175M，Go 1.25.5，`/data` ext4/EBS，主网同步同时运行；极热 key 场景基本持平。Mac 的 truncate 成本尤其高，不能把其数百倍局部比值用于 Linux 或整链。缓冲增加固定内存，ETL 池的跨轮复用也会影响 alloc bytes，原始三轮均保留。

## 验证

- posting 与独立旧 V7 编码器逐字节比较，同时比较 metadata offsets/counts，覆盖 1/127/128/129、多 frame、3万/4万 postings 和大小 key 交替。
- 64 KiB 前一字节、恰好、后一字节，多次 spill/reuse，边界内容完整与临时文件清理。
- 创建 spill 失败、文件 I/O 失败、目标写入失败、内存/磁盘路径取消；完整压实取消不改变 manifest 或输入。
- 本地 snapshots/pruning 全包、相关 race、最终新增指标后的 posting race、core/actuator/VM 全部回归及增量 lint（0 issues）通过。
- Linux/Sapling snapshots/pruning 全包与相关 race、主网用二进制构建通过；服务器基线和五个源文件 SHA 与本地匹配。

## 部署与证据

构建目录 `/data/gtron/posting-20260906/src` 从当前 P2d1 的隔离源码复制；共享 `/data/gtron/go-tron` 未修改。release `/data/gtron/releases/20260906-p2e/gtron`；部署覆盖 `/etc/systemd/system/gtron.service.d/99-posting-p2e-20260906.conf`，保留 Get、64/32、history format=2、原端口和资源配置。

部署脚本 `switch-version.sh p2e <tag>` 使用现有部署锁并临时暂停 timer；必须正常停止、继续导入并核对运行二进制与环境。失败恢复旧覆盖配置。回退使用同脚本 `p2d1 <unique-tag>`，它认识已写入的 history format=2。canary 保持停止。

本地证据：`build/benchmarks/20260906-p2e/`；服务器证据：`/data/gtron/posting-20260906/`，原始 30 秒采样在 `/data/gtron/commitment-20260905/p2e-*.{json,prom,txt}`。

主网于 04:15:51 UTC 启动 P2e，04:16:20 完成健康确认，PID 15688。SHA-256 为 `2202bad8e43164c4f5111239526fb29630eddd7249903bc294ab9af11fcd54e5`，CPU Build ID 为 `152f4a89ca88a21303c424ccff97348b4a1b420a`。部署前后各 9 分钟、每 30 秒采样。两组各剔除前 90 秒后，保留 450.3 秒、16 个有效点；RSS 使用包含预热的整组峰值。

| 指标 | P2d1 部署前 | P2e 部署后 |
|---|---:|---:|
| blocks/s | 18.475 | 25.937 |
| tx/s | 1,919.487 | 2,384.737 |
| tx/block | 103.895 | 91.943 |
| energy/tx | 7,595.691 | 7,624.551 |
| VM 交易占比 | 35.6% | 33.6% |
| commit ms/block | 19.353 | 15.048 |
| commit µs/update | 53.494 | 42.542 |
| 随机 VFS KiB/block | 2,143.19 | 1,580.46 |
| 顺序 VFS MiB/s | 113.81 | 98.81 |
| RSS 峰值 GiB | 20.434 | 19.374 |
| compaction debt GiB 起/止 | 91.39 / 92.61 | 90.50 / 94.81 |
| Get 错误增量 | 0 | 0 |

P2e 有效窗口新增 1,660,346 个 memory key、4 个 spill key，spill payload 共 377,949 B。新指标说明真实路径使用，文件发布与正确性另行验证。吞吐观测 +24.24%，同时每块交易数下降 11.5%，后台大型 merge 阶段也不同；不将这个比值写成严格因果加速。

04:20 和 04:27 UTC 的两份后续样本为 25.15/25.16 秒、76.98/68.00 CPU-s，BuildAccessorContext 各 0.40 CPU-s，flushKey 为 0.07/0.06 CPU-s。它们含例行 cold build，部署前样本含大型 merge，不能把 24.43→0.40 CPU-s 当作同量工作的 98% 降幅。真实文件装配的直接收益由上面的相同输入 Linux 基准支撑。


上线后生产 manifest 中选取一组三件套，txNum 1,694,954,032–1,695,051,710，history/accessor/inverted 分别为 173,079,144 / 7,988,088 / 503,041 B，总计 181,570,273 B。先硬链接固定输入，再调用 `VerifyLoadedManifestFiles`（RequireRegistered/RequireChecksums，默认完整语义校验），三文件全部通过，2.243 秒。校验在性能窗口和最后 CPU 采样结束后执行；未打开运行中的 Pebble 或修改生产 manifest。

本轮保留 P2e。主网观测期间 compaction debt 上升约 4.31 GiB，当前剩余热点仍有 Pebble 压实和阶段性历史整文件校验；这次消除了 posting 临时文件瓶颈，没有据此宣布长期写入债务已解决。04:28 UTC `/data` 约 90% 使用、可用 762 GiB，后续压实/退休回收的服务率仍需要继续改善。


04:43:45 UTC 最终检查：P2e 已连续运行约 28 分钟，head **30,245,536**，30 peers，active、未暂停；PID/SHA/环境一致。两组共 **38/38 原始点 active**，32/32 有效点 active。进程累计已装配 5,747,748 个 memory key、7 个 spill key，spill payload 649,486 B。唯一 gtron 进程为正式主网，canary 未启动，部署 timer active。只读校验硬链接已释放，生产文件不受影响。


最终 compaction debt 已从窗口末的 94.81 GiB 回落到 **91.60 GiB**，commitment/snapshot 错误指标均为 0，部署锁空闲；因此本轮未观察到持续单向发散。仍不以短期回落代替长期后台吞吐结论。服务器 171 项原始样本/源码/构建/校验/部署证据归档为 `/data/gtron/posting-20260906/server-evidence.tar.gz`（716,810 B），SHA-256 `688b9aa746ce626aa16c4b1752bbc0191e24637bde19a282cc45c84219f9bb94`。
