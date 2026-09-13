# 热历史回收优化部署记录（2026-09-13）

本轮已通过 master 推送、原生部署、配置与主链抽样验收，前后各十分钟采样完整。
新进程同步正常，但新增回收路径尚未实际命中：17,856 个热历史清理块全部在 guard
锁忙后退回原 point 路径，range runs 为 0；posting 仍因压力退让，没有进入回调。
因此本轮完成了部署，尚未验证两项新路径的线上收益。

文中 `build/benchmarks/` 下的原始采样和分析文件保存在本地 ignored 证据目录，
不随 Git 文档提交；现场终端读数明确标注为截图人工转录。

后测 27.3598 块/秒比前测高 29.53%，同时每块交易数下降 21.05%，TPS 仅高 2.26%，
不能归因优化提速。State 头距缩小 1,458，body 扩大 16,398，交易索引近持平（+14），
三类仍为原数量级。同口径 du 的约 35 分钟区间内 chaindata 净减 7.248 GiB，
包含旧版运行、构建、重启和新版，不能算作 range 的回收收益。

## 版本和执行范围

| 项目 | 固定值或当前状态 |
|---|---|
| 实现源码 | `4940853af2424d04b86236e4ec682dcfc32e75e2` |
| 部署 ops | `bef33d58ece0ab0ecc29dc67a092a412f2ea814e` |
| Helper SHA256 | `12db7f2f9fa4fa3e61e5c76e90a4529dd8f85e60e1fd7402219f8e338cbe3c2d` |
| 新 release | `/data/gtron/releases/20260912-reclaim-optimization` |
| 旧 release | `/data/gtron/releases/20260912-posting-prune-fair-lock/gtron` |
| 旧 PID / start ticks | `18236` / `4492264350` |
| 旧精确 metrics 进程身份 | `1789188149990537883` |
| 新 PID / start ticks | `20101` / `4499354749` |
| 新精确 metrics 进程身份 | `1789259053976239426` |
| 新 binary SHA256 | `b47a0920272cc54fdd0c355663e2c9d477bd5c912d99bc689df7ee99a3e4d868` |

09/13 01:28 UTC 后续部署核验纠正此处 SHA 的截图转录错误：零起始位置 56 的
`a` 曾误写为 `c`。现场 `/proc/20101/exe` 哈希与本 release 原始 `prepared.json`
完全一致，二进制没有变化；错误清单被下一轮 prepare 拒绝，未发生服务切换。
证据见 [核验记录](../../build/benchmarks/20260913-range-scheduling/observed-old-sha-correction.json)。

源码和 ops 经 master 推送，服务器隔离 release 使用 Go 1.25.5 Linux amd64、
CGO/Sapling 原生构建，prepare 在本次前测全部结束后约 00:15 启动。
服务器原 checkout 保持原位；固定的 29 个非文档变更文件不由现场构建临时改写。
证据：[rollout-pins.json](../../build/benchmarks/20260913-reclaim-rollout/rollout-pins.json)、
[离线 static 审计](../../build/benchmarks/20260913-reclaim-rollout/deployment/deployment-static-audit.json)、
[pin 审计](../../build/benchmarks/20260913-reclaim-rollout/deployment/deployment-final-pin-audit.json)。
两项离线审计各 16 项通过。prepare 于 00:18:09 UTC 完成，29 文件 manifest、
原配置及旧 ticks 复核通过；native affected、四开关 enabled、targeted 与 Sapling probe
均通过。00:24:33.439037 UTC 激活健康检查通过，head `29787183→29787196`。
证据：[prepared 核验](../../build/benchmarks/20260913-reclaim-rollout/observed-prepared-review.json)、
[postflight 核验](../../build/benchmarks/20260913-reclaim-rollout/observed-postflight.json)。
本地最终完整测试、race、增量 lint 和 A/B 的历史记录见
[9/12 验证报告](sync-reclaim-optimization-20260912.md)；A/B 证明范围删除机制可工作，
也含物理占用未优于 point 的反例，不是线上收益承诺。

00:02:30 UTC 现场旧服务 active，40 GiB MemoryLimit、原三个环境开关均为 1，
guard/holds 与此前 activation 后配置一致。Nile 和自动部署服务/定时器保持 inactive。
本轮 helper 激活仅替换 exe 并新增 `GTRON_HISTORY_RANGE_PRUNE=1`；失败恢复旧 unit 的完整
字节、元数据和公平锁二进制，旧健康检查不依赖新指标。
现场证据：[observed-preflight.json](../../build/benchmarks/20260913-reclaim-rollout/observed-preflight.json)。
该文件和磁盘读数均为 Native JumpServer 截图的人工转录，不是原始 stdout。
00:36:23 UTC 末检确认新 PID/ticks/二进制一致，四个开关均为 1、40 GiB 限制、
guard/holds、受保护服务及 checkout 均符合预期；unit 与 activation 后配置相同，
相对 before 只有获批的 exe 与第四开关变更。00:38 监听归属 PID 20101：
TCP/UDP 18890、TCP 8090/8545/50051、loopback 6062/6071。
三个 canary 高度 `29787100 / 29796961 / 29804305` 的块哈希、父哈希与交易顺序
均与 TronGrid 一致，交易数 `68 / 96 / 133`，共六个请求；这不代表全链或 state root 验证。
证据：[canonical 摘要](../../build/benchmarks/20260913-reclaim-rollout/canonical-after-trongrid/summary.json)。

## 本次完整前测

21 个 metrics 与 21 个 Wallet 请求全部成功有效且完成进程关联，新 31 项指标整组
未暴露，标记 `legacy_not_exposed` / N/A。旧进程身份以精确整数/字符串核对，不转 float。
metrics 中点窗口为 00:04:36.822693–00:14:36.694190 UTC，599.874585 秒；
Wallet 独立窗口为 599.926219 秒。正式后测明确使用同日 `/before` 作为比较对象。

| 同步与归档观察 | 全窗结果 |
|---|---:|
| Session 处理速度 / 交易速度 | 21.1226 块/秒；2,641.8932 tx/秒 |
| 交易密度 | 125.0743 tx/session块 |
| State 已发布/热清理头距 | 333,991→333,294，−697 |
| Body 冷覆盖头距 | 338,134→350,823，+12,689 |
| 交易索引冷覆盖头距 | 395,478→391,783，−3,695 |
| 热历史清理 | 23 次成功 pass，13,386 个 domain-change blocks |
| Posting 新增 chunk / pressure deferral | 0 / 598 |
| Debt 首→末；观察范围 | 40.5248→40.7602 GiB；40.1137–41.3303 GiB |
| Compaction 输入 / 输出增量 | 48.9420 / 37.1622 GiB |

State 和交易索引冷覆盖推进，body 本窗没有完成新的覆盖批次；不能把三类头距混为
同一水位或统称没有扩大。交易索引来自 `chain/freezer/txindex/coverage`，
不是 range guard 的 `StageStateHistoryIndex`，也不是 posting sweep 完成水位。
这些头距互相重叠，不能相加。下载 buffer 块数 1,541→880，21 点均未 paused/backpressured。

所有采样点 debt 均高于原 posting 32 GiB 准入门限，足以阻止这些点的准入；
自动压实仍在运行，不能将 posting 无新增分片描述为压实停止。
prune、posting、所选 canonical/storage/non-shadow 错误及写 stall 无新增。
shadow sender_chain/errors +1、vm_sender_chain/errors +25 单列，其中 readiness 23、
result 2 是其子项，不叠加；所选计数不代表全部失败路径。

全进程平均 CPU 为 2.8734 核，包含后台工作。VM 占比 35.8563%、每 VM 交易 raw energy
21,338.68 来自 18 个去重完整 import 窗口（557.026438 秒），与全窗 session 分母不同。
后段 session 为 17.7751 块/秒，交易密度升至 154.9446、TPS 升至 2,754.1480，
因此仅块速变化不能作为版本性能归因。后段 metrics/Wallet 都是 011–020 的 10 点，
分别 269.954854/270.041405 秒。
详见 [before-summary.md](../../build/benchmarks/20260913-reclaim-rollout/before-summary.md)
及 [完整 before-analysis.json](../../build/benchmarks/20260913-reclaim-rollout/before-analysis.json)。

## 完整后测与新路径命中情况

后测 21 个 metrics 与 21 个 Wallet 请求全部完整有效，精确进程均为
`1789259053976239426`，31 字段合同完整，没有可见累计量或运行期 max 回退。
`before_after_observation.available=true`，比较路径已另行断言为今日 before，
两进程累计计数只在各自窗口内求差。metrics 为 00:25:23.612974–00:35:23.773100 UTC、
600.164478 秒；Wallet 为 600.005456 秒，进程约启动 70 秒后进入后测。

| 同口径观察 | Before | After |
|---|---:|---:|
| Session 块/秒 | 21.1226 | 27.3598（+29.53%） |
| Session tx/秒 | 2,641.8932 | 2,701.6121（+2.26%） |
| tx/session块 | 125.0743 | 98.7440（−21.05%） |
| State 已发布/热清理头距 | 333,991→333,294 | 329,427→327,969 |
| Body 冷覆盖头距 | 338,134→350,823 | 362,187→378,585 |
| 交易索引冷覆盖头距 | 395,478→391,783 | 378,571→378,585 |
| 成功热清理 pass / blocks | 23 / 13,386 | 23 / 17,856 |
| Posting chunk / pressure deferral 增量 | 0 / 598 | 0 / 599 |
| Debt 首→末 GiB | 40.5248→40.7602 | 42.1180→41.1358 |
| Compaction 输入 / 输出 GiB | 48.9420 / 37.1622 | 51.4201 / 41.3895 |

后测 state 发布和热清理推进 17,856 块，交易索引推进 16,384 块，body 没有新增覆盖批次。
buffer 块数 1,649→1,670，bytes 36,714,358→58,465,962，21 点均 active 且未暂停/背压。
全进程平均 CPU 2.8734→2.9675 核；19 个完整 import 窗口内 VM 占比为 35.6056%，
与前测 35.8563% 接近，raw energy/VM 交易则由 21,338.68 升至 22,932.48。
重启预热、交易密度、每交易负载及后台工作不同，不能将更高块速当成新回收收益。

range `runs/rows/logical_bytes/work` 全为 0，`fallback_blocks` 670→18,526，
增量恰为 17,856；所有本窗成功发布的清理块均保留原 key-only point 行为。
新 `point/rows` 与字节为 0 不代表旧 point 没有执行，它们不计 fallback 路径。
代码中只有 index 或 chain `TryLock` 忙会返回正常 fallback；目前没有区分两把锁的指标。
pending-prefix 未 settled、canonical/Finish/index proof 错误会报错，不能拿它们解释
这次无错误的 fallback。参见 [guard](../../core/state_domain_change_prune_guard.go)
和 [worker](../../core/state/pruning/worker.go)。

Posting callbacks 和七个 phase 均为 0，均值为 N/A；本窗没有验证新的获锁后 gate lease。
Debt 全窗为 40.5187–42.4340 GiB，所有观察点均高于原 32 GiB 门限，
足以阻止该点准入。压实输入 55,211,899,142 B、输出 44,441,607,209 B，仍持续工作。
prune/posting、写 stall、所选 canonical/storage/non-shadow 错误无新增。
shadow sender_chain +1、vm_sender_chain +38 单列；后者 readiness +35、
apply_unsupported +3 为子项，不相加，也不等于 canonical 失败。

后段 after metrics 为 010–020 的 11 点、299.999810 秒，Wallet 为 011–020 的
10 点、269.982902 秒。后段 session 24.8905 块/秒、2,907.0952 tx/秒、116.7955 tx/块；
相对 before 后段块速 +40.03%、TPS +5.55%、密度 −24.62%，仍不构成因果比较。
后段 state published gap 缩小 2,364，pruned gap 缩小 1,664；首点两个 gauge 差 700，
007 点也观察 pruned 领先 published 639，不能把不同步 gauge 当超前删除的证据。
后段 body 与交易索引 gap 各增 7,345。
证据：[after-summary.md](../../build/benchmarks/20260913-reclaim-rollout/after-summary.md)、
[完整 after-analysis.json](../../build/benchmarks/20260913-reclaim-rollout/after-analysis.json)。

## 实际占用前后观察

00:03:25–26 UTC 对 `/data/gtron/main/datadir/gtron` 执行
`du -x -B1 --max-depth=1` 成功，统计 allocated 字节，不是 apparent size 或 Pebble gauge。

00:38:16–17 UTC 同命令再次成功，退出 0、无 stderr，前后分别为：

| 目录 | Before allocated 字节 | After allocated 字节 | GiB 变化 |
|---|---:|---:|---:|
| chaindata | 240,948,744,192 | 233,166,127,104 | 224.401→217.153（−7.248） |
| state-snapshots | 571,075,780,608 | 574,321,930,240 | 531.856→534.879（+3.023） |
| ancient | 216,326,062,080 | 216,448,212,992 | 201.469→201.583（+0.114） |
| 根目录合计 | 1,028,350,590,976 | 1,023,936,274,432 | 957.726→953.615（−4.111） |

实际 allocated 占用出现净下降，与自动压实一直运行相容；约 35 分钟区间包括
旧版、构建、重启和新版，不是十分钟后测，也不能归因 range（本窗实际 runs 为 0）。
数据盘末次可用 1,966,432,665,600 B，约 1.788 TiB。
证据：[observed-disk-after.json](../../build/benchmarks/20260913-reclaim-rollout/observed-disk-after.json)。

本次 before 相对 9/12 04:56:44 UTC 的同口径测量，chaindata 净增 53.953 GiB，
冷快照净增 88.638 GiB，ancient 净增 22.230 GiB，根目录净增 164.822 GiB。
该约 19 小时区间不能充当前测十分钟的空间增量，也不能全部归因为热库积压。
运行中 du 非原子，缺少严格同刻块数分母，不计算每块净增长。
详见 [physical-baseline.md](../../build/benchmarks/20260913-reclaim-rollout/physical-baseline.md)
及 [observed-disk-before.json](../../build/benchmarks/20260913-reclaim-rollout/observed-disk-before.json)。
`format_named_files=[]` 只表示该次列表为空，不证明数据库 format version。

## 下一步与解释边界

优先区分 range guard 的 index/chain 锁忙来源和机会窗口，再调整有界调度，
保留 canonical、Finish/index、pending-prefix 与异步错误证明。不能仅扩大扫描预算，
或让持有重维护 gate 的任务长时间排队。Posting 则应在原压力门限允许进入后核对
phase 和真实删除；本窗无法判断该新路径效果。12 个 namespace 均完成有效观测，
末次异步估算以 changeset 87.427 GiB、commitment 55.171 GiB、state_change_index
42.176 GiB 为主，后续仍需结合冷覆盖推进和物理变化分析，不能把估算总和当全库。
各项末次成功采样相对响应已过去约 9–339 秒，详见
[分布摘录](../../build/benchmarks/20260913-reclaim-rollout/distribution-after.md)。

00:41:03 UTC 有限日志审核的 tail 5,000 行中，所选
`ERROR|EROR|CRIT|FATAL|panic` 正则无匹配，近期同步、热 prune、cold publish 仍推进，
最近日志 head 为 29,812,286。末行 peer `BAD_MESSAGE` 断连单列，不等于 canonical
执行失败；有限 tail 与正则不覆盖全部失败路径。专用 SSH 和本任务标签已关闭，
原用户标签保留。证据：[日志审核](../../build/benchmarks/20260913-reclaim-rollout/observed-log-review.json)、
[会话清理](../../build/benchmarks/20260913-reclaim-rollout/observed-session-cleanup.json)。

本轮 range 在原冷覆盖选块后，每最多 256 块尝试 guard，scan 与 batch flush 同锁内完成，
验证 durable canonical/Finish/index 和 pending-prefix/异步错误。仅锁忙保留原 point，
proof 错误不能降级。rawdb 64 MiB 是单 run 合作预算，不是 guard 硬时延。
posting 取得链锁后才申请真实 gate lease，压力门限不变，等待不占 gate。

后测已逐点核对 31 新字段和新进程；累计项/max 若回退须撤销相应差分，before 未暴露不能补零。
range work 仅 callback、整 pass 成功后发布；busy point 只记 fallback_blocks，
不能与新 point 行/字节直接合并。phase 父子重叠、last/max 不是分位数或独立窗口最大值。
逻辑删量、compaction 工作量、debt、异步且可能重叠的 namespace SST 估算、
snapshot pinned 累计量和 du 分开解释；负 obsolete 及同源 disk 污染不能当物理回收。
重启预热、交易密度、VM 能量和后台任务不同，前后观察差异不直接等于因果提速。
