# P2f：V6 历史合并顺序流与双源校验

日期：2026-09-06。状态：第二候选已通过本地、Linux/Sapling 检查及固定生产文件对照，07:34:58 UTC 已直接部署正式主网，前后各九分钟观察完成，保留 P2f。确认固定生产文件组件收益；未确认整链吞吐提升。

本轮沿用用户明确授权的主网直接部署和连续观测，不要求同一 28M 区间重放，不启动 canary。

## 问题与改动

压缩 V6 合并原来让 record 与 StateTxRange 共享一个单块解压缓存。每跨区块读取范围表，会驱逐正在顺序消费的记录块；每条记录另有 frame 分段读取和 payload 复制。V5 已采用独立范围流，V6 尚未接入。

本轮让 V6 使用独立范围 reader/cursor，并通过 `ReadRecordFrameAt` 借用当前解压块中的完整 frame；跨块 frame 复制到复用 scratch。借用的 Prev 在下一次记录读取前同步编码，writer 只保留顺序标量。keyID remap、范围覆盖、记录次序及 canonical 输出保持不变。

第一候选的合成输入收益很大，但同一组生产文件的整段合并中位数仅改善 1.8%。没有据此宣布稳定生产收益。对真实文件继续采样：合并主调用链 3.77 CPU-s，其中源 SHA 1.42 CPU-s，约 38%；该分母不包含异步压缩 worker，也不是整个进程 CPU 或合并墙钟。

第二候选增加 `GTRON_HISTORY_COMPACTION_SOURCE_WORKERS=1|2`，默认 1。两个独立源可以同时做完整校验，按输入顺序收集结果和首个错误，全部通过后才构造输出。取消或失败时必须取消并 join 所有源任务，防止任务超出输入 lease 生命周期。保留完整 SHA、sidecar 结构检查、逐记录验证、输出 CRC/SHA、同步及原子发布。复制阶段仍逐源进行；额外内存包括第二个校验流和独立范围 reader，不把单块缓存大小当成整进程内存上限。

新增 `compaction/v6_stream/borrowed_records` 与 `copied_records` 按源批量更新，统计已消费记录，包含后来取消而未发布的合并。`compaction/source_workers` 反映最近使用的源 worker 数量。

## 验证

- 压缩格式 1/2，257/4,096/16,384 字节 chunk；频繁区块范围切换，prefix/payload 跨块，70 KiB/35 KiB Prev 后接小记录，空值与不存在值。
- 压缩输入新路径与 plain V6 原路径的 history/accessor/inverted 三件套字节一致，完整语义校验通过。
- 非法 keyID、range、marker、length、order、尾部和 SHA；活跃复制过程的取消、目标写错，不发布输出、不修改源。
- 双源有界并发、结果顺序、后一个错误先完成仍返回前一个源错误、取消后无遗留读任务、配置校验。
- 第一候选本地 snapshots/pruning、core/state/actuator/VM 与相关 race 通过；第二候选 snapshots/pruning 53.311/15.835 秒、相关 snapshots race 8.207 秒，增量 lint 为 0 issues。未将 pruning 的“no tests to run”记作 race 覆盖。
- 第一候选 Linux/Sapling 回归和相关 race 通过；服务器源码逐文件 SHA 与本地一致。第一候选仅用于基准，未部署。
- 第二候选 Linux/Sapling snapshots/pruning 54.038/7.810 秒，相关 snapshots race 9.173 秒。07:22:13 UTC 构建完成。

## 固定输入组件证据

服务器：Linux amd64，Xeon Platinum 8175M，Go 1.25.5，`GOMAXPROCS=2`，`GOMEMLIMIT=2GiB`，Sapling，`/data` 上运行；正式主网同步同时运行。fixture 构造、清理以及最终完整验证不在计时内。

第一候选的合成输入整段合并三次中位数：

| 输入形状 | P2e | V6 顺序流 | 比值 |
|---|---:|---:|---:|
| 每区块 1 tx、64 B Prev | 6,605.70 ms | 159.72 ms | 41.36× |
| 每区块 100 tx、256 B Prev | 182.43 ms | 132.03 ms | 1.38× |
| 每区块 100 tx、16 KiB Prev | 486.52 ms | 445.24 ms | 1.09× |

这些是特定输入形状的组件结果，不能代替主网吞吐。

实际生产 fixture：两个相邻 aggregationSteps=1 的历史三件套，txNum 1,706,990,947–1,707,186,423，共 6 个文件、428,548,548 字节。只用硬链接固定不可变文件，独立 manifest 和输出目录，不打开链 DB，不修改生产 manifest。第一候选交替 A/B/A/B/A/B 的合并耗时：

| 版本 | 三轮秒数 | 中位数 |
|---|---|---:|
| P2e | 8.035601 / 8.249847 / 7.827480 | 8.035601 s |
| V6 顺序流 | 8.095966 / 7.892765 / 7.337735 | 7.892765 s |

六次完整输出验证通过，三种输出的 SHA 逐次一致。第一候选仅 1.8% 的中位数差异不足以确认稳定整段收益，因此继续了第二候选。

第二候选包含 V6 顺序流及双源校验。使用相同 fixture，重新交替运行三轮：

| 版本 | 三轮秒数 | 中位数 |
|---|---|---:|
| P2e | 8.049651 / 8.171574 / 7.982239 | 8.049651 s |
| P2f，source workers=2 | 6.659756 / 6.885408 / 6.635265 | 6.659756 s |

耗时中位数减少 **17.27%**，处理速率比 **1.209×**，三轮新版均快于三轮旧版。六次完整输出验证通过，三件套 SHA 一致。07:23:35 UTC 测试完成。该结果只覆盖这组不可变文件，不等于整链 tx/s 提升 20.9%，也不能直接外推至不同尺寸的大型 merge。

## 下一轮定位

2026-09-06 P2g 静态核查补充：合并默认已删除未被 published manifest 保护的旧文件，但 retired 引用仍留在清单。因此下面 799.89 GiB 仅为记账，不证明同等物理积压；`DeferRetiredPruneWhileSyncing` 也不能单独作此推断。是否需要重构回收，应先做实物盘点。见 `docs/dev/28m-manifest-cache-p2g-2026-09-06.md`。

07:27:16 UTC 主网 P2e 新样本：25.17 秒、80.63 CPU-s，Pebble `runCompaction` 21.17 CPU-s（26.26%），SnapshotLifecycle 8.46 CPU-s；其中 `LoadProductionManifest` 4.54 CPU-s、`decodeManifest` 4.34 CPU-s。父子调用不可相加。本次仍未采到大型历史 merge，P2f 主要优化周期性后台工作。

同次空间检查 `/data` 已用 94%，可用约 477 GiB；清单记账的 retired 为 30,720 个引用、799.89 GiB（不是已核验可立即删除的物理空间）。下一轮优先处理持续同步期间的历史回收与写放大，以及生命周期多次读取、解码、验证同一 manifest 的重复工作。`cmd/gtron/main.go` 当前明确配置 `DeferRetiredPruneWhileSyncing=true`，会推迟额外的 retired 清理，但实际积压大小仍需物理盘点确认。下一轮考虑将回收单位缩小到有完整替代覆盖证明的一组历史文件，在受控 I/O 预算下验证、重新检查发布引用和 lease，再回收；旧清单缺少这种证明时保留现有严格路径。实现时需要保留文件 lease、活动清单身份、完整覆盖证明和错误失效，不直接删除 retired 文件或跳过验证。

## 主网观察

P2e 基线 07:25:30–07:34:30 UTC，共 19 个原始点；跳过前 90 秒后 16 个有效点、450.4 秒，高度 30,406,310→30,412,101，12.861 blocks/s、1,281.107 tx/s，99.615 tx/block，10,135 energy/tx、VM 占比 44.4%。有效点均 active，最少 30 peers，点读和相关错误计数增量 0。Pebble 债务 80.26→50.65 GiB，RSS 全部原始点范围 21.936–22.703 GiB。该阶段自身的后台债务变化明显，不能只用前后吞吐比归因于版本。

07:34:33 UTC 开始切换，旧 P2e `ExecMainStatus=0`，07:34:58 UTC 新进程健康检查与二进制/环境检查完成；新 PID 29303，实际运行 SHA 与发布文件一致，部署 timer 恢复 active。

07:37:49 UTC 的 P2f CPU 样本为 25.17 秒、57.88 CPU-s，Build ID `04486d3e8041675e52c7ce5cd8dc87a0888d8f49`。Pebble 压实 14.33 CPU-s（24.76%），该样本未采到大型历史合并。新旧样本后台阶段不同，不能把 CPU 总量变化当作 P2f 的节省。

P2f 观察 07:34:58–07:43:58 UTC，19 个原始点；跳过前 90 秒后 16 个有效点、450.4 秒。前后全部 38 个原始点均 active 且未 paused，每组一个 PID，点读及相关错误增量均为 0。

| 指标 | P2e 有效窗口 | P2f 有效窗口 |
|---|---:|---:|
| 高度 | 30,406,310→30,412,101 | 30,413,019→30,417,646 |
| blocks/s | 12.861 | 10.232 |
| tx/s | 1,281.107 | 1,091.108 |
| tx/block | 99.615 | 106.640 |
| energy/tx | 10,135 | 8,859 |
| VM tx 占比 | 44.4% | 40.6% |
| commit ms/block，滚动值中位数 | 30.133 | 31.352 |
| commit µs/update，滚动值中位数 | 79.211 | 72.636 |
| Get 调用 | 1,868,980 | 1,449,235 |
| Get 错误 | 0 | 0 |
| 最少 peers | 30 | 11 |
| Pebble 债务，GiB | 80.26→50.65 | 54.32→72.04 |
| RSS，全部原始点范围 GiB | 21.936–22.703 | 4.998–19.707 |

线上吞吐在该窗口下降约 14.8%，不能报告整链提速。P2f 的 borrowed/copied 记录增量均为 0，目标历史合并未在此窗口执行；两窗口还有启动预热、peer 数量、负载及后台债务差异。RSS 下限包含刚启动时的状态，不能解释为稳定内存节省。当前保留 P2f 的依据是相同生产文件的合并收益与完整正确性检查，以及主网无新增相关错误；长期大型合并和稳态整链收益尚未确认。

最后 API 核验高度 30,419,335，active=true、paused=false、25 peers，P2f PID 29303，部署 timer active，只有一个 gtron 进程。最后磁盘检查可用约 430 GiB、已用 94%；待回收历史及持续写放大应作为下一轮首要事项。

## 发布与回退入口

第二候选发布文件：`/data/gtron/releases/20260906-p2f/gtron`，SHA-256 `6428c224f80ba592e9a4bdc66c2776195a63be20ef57d3d57f67bc6f41320c2c`。构建目录中的 `gtron-p2f` 是未部署的第一候选，最终发布来自 `gtron-p2f2`。

配置由 `99-v6-stream-p2f-20260906.conf` 覆盖 P2e executable，并设置 `GTRON_HISTORY_COMPACTION_SOURCE_WORKERS=2`。保留 Get、Commitment 64 分区/32 读取名额、history format=2、关闭 overlap 和原端口。

统一部署/回退脚本 `/data/gtron/v6-stream-20260906/switch-version.sh` 使用部署锁，暂时停止部署 timer，要求旧进程正常退出，核验新进程实际二进制及环境，检查继续导入；失败恢复之前覆盖文件，结束恢复 timer。脚本及内嵌 Python 语法已检查，服务器与本地 SHA 一致。回退使用：

```bash
sudo -n bash /data/gtron/v6-stream-20260906/switch-version.sh p2e <unique-tag>
```

不要只运行旧轮次的部署脚本：新的 systemd 覆盖文件排序在 P2e 后，旧脚本可能无法改变最终有效 executable。P2e 已支持 format=2，可作为此次回退基线。

## 证据位置

本地：`build/benchmarks/20260906-p2f/`，保留补丁、基线 overlay、源码 SHA、测试、基准和主网 CPU profile。Mac 合成基准以 `*-final.txt` 为准，早期记录包含后来修正的 fixture 计时。

服务器：`/data/gtron/v6-stream-20260906/`，保留 Linux 原始输出、真实 fixture 的源引用及 checksum、交替测试、profile、构建和部署记录。第一候选原始基准与第二候选 `dual-*` 文件分开保存。

早期 `p2f-before` 观察在改进第二候选时主动终止，不能作为完整部署基线；最终主网窗口使用独立标签。

最终服务器归档于 07:47:23 UTC 完成：`p2f-final-source.tar.gz` 5,052,180 字节（源码，排除构建/缓存目录），`p2f-evidence.tar.gz` 727,611 字节（原始测试、profile、部署和主网采样）。两者 SHA-256 保存在同目录 `final-evidence-index.json`。固定生产文件 fixture 单独保留以支持后续比较，不混入证据压缩包。
