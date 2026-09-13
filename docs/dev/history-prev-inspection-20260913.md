# 热历史 Prev 有界检查与编码对比

2026-09-13。记录 chaindata 增长的有界诊断，以及基于实盘证据调整小历史包去重门限的过程。
诊断命令只读；写入策略修复保持现有历史格式和保留范围。
增长基线见 [当日采样](sync-chaindata-growth-20260913.md)。

`gtron db inspect-history-prev` 必须在停止节点、释放 Pebble 目录锁后运行。它以固定 seed 在显式高度范围内
分层选点，读取现代 `state-changeset-v2` 的 seq=0 物理 pack；支持共享 v3 的诊断版本还会读取该包引用的 chunk。
全部读取固定在同一个快照。每个分层只选一个高度，空值保留、不补选。
这不是包含 repair 的完整有效历史视图，也不是 SST 物理字节或全库业务占比。

默认检查 256 个高度，物理 pack 编码值累计上限 256 MiB、chunk 读取累计上限 256 MiB、导出累计上限 1 GiB、
解码累计上限 1 GiB、单包解码上限 128 MiB、200 万行、
60 秒工作时间。时间检查在读取、解码和各行之间协作执行；单次 Get 必须先取得值才能知道编码大小，
所以报告分别计量读取字节与预算接受字节。进程外层超时须同时覆盖数据库 Open/Close。
不完整、损坏、取消或超预算均输出明确状态并返回非零，不能把已处理前缀当成完整样本。

输出包括 FlatDomain/KVDomain 旧值字节与直方图、最大的 20 行、固定容量的大 key 聚合，以及现有块内 CDC
大小/重复 key 门限。大 key 统计只覆盖 Prev 至少 16 KiB 的行；溢出显式报告。CDC 门限通过不能证明运行开关开启，
也不能证明它比 Snappy 额外节省超过生产要求的 12.5%。

```sh
gtron db inspect-history-prev --datadir /path/to/datadir \
  --from-block 30000000 --to-block 30300000 --samples 128 \
  --db.cache 64 --db.handles 128 --max-duration 60s \
  --export-packs /private/diagnostics/new-packs > inspection.json
```

高度和路径是示例，运行时应从当次水位选择。可选导出目录必须是 chaindata 以外的新目录，父目录预先存在。
目录权限 0700、文件 0600，拒绝覆盖；仅完整解码验证通过的 pack 才导出。manifest 包含高度、编码/解码长度和
文件 SHA256。它不包含 canonical block hash、repair 或 ancient，不能当作备份。

恢复服务后可直接在导出文件上运行：

```sh
gtron db benchmark-history-codecs --export-packs /private/diagnostics/new-packs \
  --max-duration 2m > codecs.json
```

这个命令不打开数据库。它验证 manifest、固定文件名、长度和 SHA256，逐包比较现有表示、生产 Snappy 基线、
忽略前置门限的生产 CDC、独立 zstd frame。每项都逐字节验证恢复结果；zstd 只用于实验，没有接入生产格式。
统计同时包含墙钟与进程 CPU，不能当作线上导入吞吐。汇总仅包含所有候选都完成的样本，保留取消/失败信息。

## 共享 v3 的诊断与导出合同

后续共享格式实现配套扩展诊断命令；以下描述新代码能力，不改变下方已经完成的旧版本实盘采样结论。
新 pack 先验证物理块号、完整引用表和解码长度预算，再从同一固定读视图解析 chunk。
缺失/损坏 chunk、无法取得固定视图、取消或预算不足都返回明确 partial；不能回退为 legacy 或 missing。
不具备快照的旧 reader 仍可读取自包含 raw/Snappy/v2 数据。

`--max-chunk-read-bytes` 与 `--max-export-bytes` 分别限制 chunk 返回字节和导出候选字节，最大值为
1 GiB 与 4 GiB；新增选项的零值采用默认值。chunk 每次读取前后检查时间/取消/字节预算。
单个存储值的大小须在 Get 返回后才能确定，因此最后一个被拒绝值仍计入读取量；不会继续读取下一片。

| 计量 | 含义 |
| --- | --- |
| `encoded_bytes_read` / `encoded_bytes_accepted` | 物理 seq=0 pack 值的读取量 / 预算接受量，不含 chunk |
| `chunk_read_bytes` / `chunk_reads` | 实际返回的 chunk 编码值字节 / 引用解析请求数；重复引用重复计量，非唯一存量或磁盘 I/O 次数 |
| `decoded_bytes_reserved` | 完整原始 RLP 的预算预约，含失败尝试 |
| `export_bytes_attempted` / `export_bytes_accepted` | 调用导出回调的候选字节 / 回调成功字节；失败回调不证明文件完全未写 |

新导出 manifest 为 **version 2**。每条 `codec`、`encoded_bytes`、`decoded_bytes`、`sha256` 描述实际文件；
`source_codec`、`source_pack_bytes`、`source_chunk_read_bytes`、`source_chunk_reads` 单独保留来源口径。
原 source 为 `shared3` 时，`materialized=true`，导出文件为 **raw RLP**；它不再依赖 chunk 数据库。
旧格式仍复制原自包含表示。仅完整解码的 pack 可以进入 manifest，未完成采样不会伪装成完整导出。

`benchmark-history-codecs` 同时接受旧 version 1 与新 version 2，校验实际文件的编码类型、大小和 SHA256。
其 `existing` 候选始终表示**导出文件中的表示**：对物化的 v3 样本，它是 raw RLP，不是原共享存储占用。
不能用它推算共享格式节省；来源 chunk 读取字节也不能当作唯一 chunk 存量。

运维脚本 `scripts/inspect_history_prev_20260913.py` 分 prepare 和 inspect。prepare 从 GitHub 已取回的完整提交
归档到隔离目录，执行原生构建和相关测试。inspect 持有 start.lock，校验原进程、二进制、配置与磁盘保护，
停止原服务后运行 128 个高度的检查，finally 恢复同一服务、同一二进制与参数。脚本不移除真实磁盘保护锁，
也不调用会切回旧版本的部署 rollback。停机恢复结果以运行记录为准。

## 实盘结果与恢复记录

诊断代码由 GitHub 提交 `3055eded341f310bb080164cf96e4d2f3447160e` 在服务器独立构建。
原生 rawdb/pebbledb、诊断命令测试与 Sapling 校验通过后，读取高度
**30,104,143–30,449,127** 的 128 个分层样本。128 个 pack 全部完成，0 缺包、0 预算截断，
检查循环耗时 1.352 秒；这里不包含数据库 Open/Close 和服务停启。

原始证据保留于服务器 `/data/gtron/releases/20260913-history-prev/`：
`inspection-stdout.json`、`result/packs/manifest.json`、`codec-benchmark.json`、
`proposed-gate-policy.json` 和 `verified-recovery.json`。
本地 `build/benchmarks/20260913-history-prev-0952/observed-*.json` 是对原生终端截图的人工转录，
不是下载的原始 stdout。原始诊断 JSON 和编码样本没有对外公开。

| 数据 | 样本行数 | Prev 字节 | Prev 字节占比 |
| --- | ---: | ---: | ---: |
| SystemDelegation 委托关系 | 254 | 165,791,170 | 97.2864% |
| 账户主记录 | 37,971 | 2,767,649 | 1.6241% |
| WitnessVoteState | 112 | 1,314,331 | 0.7713% |
| ContractStorage | 11,479 | 308,992 | 0.1813% |
| 全部类型 | 65,609 | 170,415,536 | 100% |

最大的两个大 key 为 `SystemDelegation/drax-0` 委托关系列表，合计贡献样本 Prev 字节的
**96.57%**；最大单条为 **6,999,253 B**。这明确了样本中主要的业务来源，不能把这些比例乘以
chaindata 的物理大小当作全库精确分布。不同区块中的列表版本具有不同历史 key，仍然是需要保留的
有效历史，普通 LSM 压实不会把这些版本自动合并为一个。

检查时实际运行的旧版本已经启用 `--history.block-dedup=true` 和
`--history.compression-format=auto`。128 个 pack 中：Snappy v1 103 个、raw 16 个、CDC v2 9 个。
旧 2 MiB 大小门限排除了 113 个包，另 5 个大包没有重复的大值完整身份，10 个通过旧前置门限。
其中通过前置门限但未选 CDC 的包仍可能因额外收益不足而保留基线，这是正常回退。

恢复脚本最初把 systemd `ExecStart` 整串当作静态配置，退出后 `stop_time/code/status` 变化导致拒绝恢复。
复核原 unit 文件、实际 argv、五个开关和磁盘保护均未变后，比较静态命令字段并恢复了原服务。
旧进程退出时间为 **10:26:42 UTC**，原二进制新进程启动时间为 **10:30:22 UTC**；
随后区块推进与运行配置核验通过。`ExecStartPre` 的同类运行状态比较问题也已修复，28 项运维测试通过。
这次额外停机来自恢复校验缺陷，不能把 1.352 秒检查耗时描述为总停机时间。

本次 inspect 恢复后运行原 `fdfe8532` 版本，进程 PID **14862**、start ticks **4502991603**，
保持全部五个原开关。10:42:35 UTC 实测 chaindata 分配空间 **447,502,618,624 B / 416.769 GiB**，
`/data` 可用空间约 **1,586.4 GiB**。上述磁盘值是该时间点的读数。

## 同一实盘样本的无损编码对比

恢复服务后只读取已导出文件，128 个 pack 的全部候选都完成逐字节恢复验证。
对比进程使用 `GOMAXPROCS=2`，整个实验约 3.794 秒。

| 编码方案 | 128 包字节 | MiB |
| --- | ---: | ---: |
| 现有存储表示 | 95,611,787 | 91.182506 |
| 生产 Snappy 基线（含 raw 回退） | 170,785,929 | 162.874154 |
| 对所有包尝试现有 CDC（无益时 raw） | 93,340,935 | 89.016852 |
| 独立 zstd frame | 109,781,171 | 104.695483 |

不能把现有存储与纯 Snappy 的差额当成本次新增收益：大部分收益来自早已启用的 CDC。
强制 CDC 的总量也不是可上线策略，因为它仍需逐包与 Snappy 比较并保留回退。
普通 zstd 比现有表示大约 14.82%，没有采用；这个实验也没有定义新的热历史格式。

本轮实际策略保留同完整 key 至少两条 `Prev>=128KiB`、比 Snappy 额外节省超过 12.5%、
128 MiB 解码上限和现有开关，只把前置 raw 大小门限从 2 MiB 降到 **256 KiB**。
256 KiB 等于两条合格大 Prev 的理论最小总量，普通小值和不重复的大列表仍不会进入 CDC 尝试。

| 新增受益样本块 | 原字节 | 新字节 | 减少字节 |
| --- | ---: | ---: | ---: |
| 30,173,167 | 1,180,164 | 513,898 | 666,266 |
| 30,267,288 | 1,231,439 | 694,729 | 536,710 |
| 30,271,629 | 2,019,802 | 869,702 | 1,150,100 |

按完整新策略逐包选择后，总量 **93,258,711 B**，比现有表示少 **2,353,076 B / 2.4611%**。
所有新增 eligible 包恰好是上述三个；本样本没有额外尝试但不胜出的包。
三次额外 CDC 编码墙钟合计 **23.604 ms**、进程 CPU 合计 **25.079 ms**。
这些数字不包含新门限扫描及线上调度，不是线上吞吐提升结论；抽样频率也不等于线上固定频率。

这是有限的写入增量优化，已有 SST 不会因门限改变立即缩小。更大的问题仍是热窗口中跨块重复的
大列表版本。冷格式 V3 已支持单段内跨记录、跨块锚点去重；它不消除进入冷层以前的 WAL/SST 写入。
后续大幅优化需要在保持逐交易历史查询、原子提交、回滚和锚点回收正确性的前提下，减少这种热历史复制。

## 发布与运行验证

修复已直接提交并推送 master：`b1305f553b5639d90eb2f58c6a5a1b23b9b025b5`。
服务器使用 GitHub 同一提交在 `/data/gtron/releases/20260913-history-pack-gate/` 隔离构建，
原 `/data/gtron/go-tron` checkout 和 Rust 子模块工作区未修改。

原生 Sapling 构建与测试通过：完整 rawdb/pebbledb、诊断命令、历史读取/AsOf/Unwind 和快照相关检查。
候选二进制再次读取同一 128 包导出，全部候选逐字节恢复验证通过；新生产策略总量仍为
93,258,711 B，符合首次分析。相关原始记录为 `gate-prepared.json`、`native-*-tests.log`、
`candidate-codecs.json` 和 `candidate-codecs.stderr`。

新增四项 `state/history/changeset/block_pack/small_chunk/` 计数：`attempts`、`selected`、
`candidate_saved_bytes`、`work_nanos`。只计原 2 MiB 门限以下新接纳的编码尝试；它们在 Put 之前更新，
可包含失败写入/重试，不能当作持久落盘量或已回收磁盘空间。耗时只覆盖新增 CDC 工作，
不含 Snappy 基线编码和门限扫描。回退、不重复、关闭开关、旧路径以及失败 Put 的统计语义已测试。

切换事务 `activation-state.json` 为 `active=true`，从 **11:02:23 UTC** 开始，
**11:03:44 UTC** 完成区块推进及完整健康核验；这是停启和等待恢复同步的事务耗时，
不是 API 不可用时间。新进程 PID **22687**，进程指标 start unix nano **1789297356925847442**。
只替换有效 ExecStart 的二进制路径，保留原五个开关、所有参数和磁盘保护。
部署记录为 `activation-before.json`、`activation-after.json`、`activation-state.json`、
`rollout-snapshot.json`。本次切换成功，没有走候选失败回滚。

进一步代码核对：legacy delegate/undelegate 会修改关系并将整个 From/ToAccounts 列表写入
SystemDelegation（`core/state/delegation_cache.go`）。拆分存储的业务路径已存在，但受链上
AllowDelegateOptimization 控制，不能为了空间提前打开。委托缓存和 domain change journal 已过滤
重复添加、无效删除和字节相同的 Prev/Next；不能将已有 no-op 过滤当作新优化。
跨块去重必须保证持久锚点与引用原子有效、重组/repair/prune 不提前回收锚点、完整身份与逐交易
Prev 可精确恢复，并保持有界随机读取。当前热 CDC 不跨包、冷 V3 不跨文件，均不具备直接扩大
引用范围所需的全部回收与提交约束。

## 上线后六分钟观察（北京时间 19:03–19:09）

`after-gate` 13 个点、间隔 30 秒，26 个 HTTP 请求全部成功且进程身份一致。首点尚在重启后
等待 peer，完整窗口为 11:03:32.605655–11:09:32.633221 UTC。另取 11:04:02 起的
12 点稳定子窗，330 秒内持续 active、没有 paused 或 fetch backpressure。
本地原始 HTTP 证据和分析在 `build/benchmarks/20260913-history-prev-0952/` 的
`before-gate/`、`after-gate/`、`live-gate-analysis.json`、`live-gate-analysis.md`。

| 稳定子窗指标 | 开始 | 结束 / 增量 |
| --- | ---: | ---: |
| 同步高度 | 30,472,330 | 30,474,276（+1,946） |
| 状态历史已裁剪水位 | 30,143,384 | 30,146,654 |
| 状态历史头距 | 328,946 | 327,622（−1,324） |
| 区块体 / 交易索引冷覆盖水位 | 30,081,024 | 30,081,024 |
| 区块体 / 交易索引头距 | 391,306 | 393,252（+1,946） |
| 压实欠账估计 | 85.774 GiB | 82.928 GiB |
| 小包 CDC 尝试 / 选中 | 0 / 0 | 0 / 0 |

稳定子窗为 **5.8906 blocks/s、1,071.99 TPS、181.98 tx/block**。较上线前 60 秒短基线
7.4656 blocks/s、853.13 TPS、114.27 tx/block，块速较低、交易处理量较高，交易密度也高
约 59%。不同高度、负载、后台工作与重启预热，不构成同负载版本性能对照。
所列 prune/freezer/storage 错误和 write-stall 计数增量为零。状态头距在收敛，
区块体/交易索引覆盖在本窗未推进，不能笼统描述为所有积压都在下降。

压实欠账在全窗波动于 80.794–90.789 GiB，末值仍高于上线前短基线的 61.737 GiB。
子窗内回落不代表整体存储压力已经消除。压实输入约 32.75 GB、输出约 29.23 GB，
说明压实正在进行；两者差额不等于物理空间回收。

**新增四个小包 CDC 指标在全部 13 点均为零。** 当前高度段尚未触发新增路径，不能把历史样本
2.4611% 的改善说成线上已节省同样比例。稳定窗仍新增约 5.369 GB 编码 pack，
原始量约 12.986 GB；平均约 2.622 MiB/pack，依然承载很大的有效历史写入。

原生最终读数为 **11:09:02 UTC：chaindata 430.027 GiB，/data 可用 1,571.345 GiB**，
服务 active、进程与发布一致。磁盘数据来自服务器 `rollout-final.json`，本地
`observed-gate-rollout.json` 明确标为终端截图人工转录。已有 SST 未因门限调整而立即缩小；
本轮定位了主要业务来源、上线了有限门限修复，但尚未根治当前高度段跨块大列表历史复制。
