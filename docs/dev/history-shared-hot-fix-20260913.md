# 热历史跨块去重修复与部署审计

2026-09-13。**热历史跨块共享已部署并启用**，业务源码、原生构建、连续实盘回放及线上健康检查均已确认。本文区分新增历史逻辑写入的减少与旧存量磁盘空间的释放。

当前线上业务源码为 `8058e99d4e080c3329a587a2feef16133e676e84`，兼容升级运维版本为 `6d970e0b256182f1517d7e957180c781f19c45c4`。首轮跨块共享源码为 `d656be3c43eb237fac4d46a8847e5fe551bc6a78`，旧 systemd 解析修复为 `593c7de3dd1b324c08fe6a2085691c5a71e7b86d`；首轮运维修复未重编业务，随后 8058 消除了重复编码工作。

## 根因与此前优化的边界

此前停机检查已将主要样本来源定位到 `SystemDelegation` 的 `drax- || 0x00 || account` 聚合记录：关系变化会保存整个 FromAccounts/ToAccounts 列表的历史 Prev。128 个分层样本中，SystemDelegation 占 Prev 字节的 97.2864%，两个最大的聚合 key 合计占 96.57%，最大单条 Prev 为 6,999,253 B。这是样本归因，不能将比例乘以 chaindata 大小作为全库物理分布。

这些是不同区块、不同交易时刻需要保留的有效历史，普通压实不会自动把它们合并。已有 no-op 过滤和块内 CDC 不能消除跨块重复；此前降低块内 CDC 大小门限，在同一批 128 包离线回放中额外节省了 2.4611%，但没有解决主要的跨块写放大。冷层压缩在发布后生效，也不能减少进入冷层之前的热历史重复。不能为了空间提前开启由链上 `AllowDelegateOptimization` 决定的业务存储规则。

来源与此前诊断限制见 [Prev 检查报告](history-prev-inspection-20260913.md)；聚合 key 定义与实际业务写入分别位于 `core/rawdb/schema.go`、`core/state/delegation_cache.go`。

## 新格式、读写与回收边界

本轮采用同一 Pebble 内的固定桶共享片段，不增加跨文件 journal。格式和安全约束见 [设计说明](../superpowers/specs/2026-09-13-shared-hot-history.md)。

- 热 pack v3 按真实高度每 1024 块划桶；包内保存物理块号、原始 RLP 长度与 SHA256、片段长度及 SHA256。片段只能引用同桶内容，不允许引用链。完整交易顺序、存在性、generation 和任意 Prev 字节保持不变。
- 新片段、桶 metadata 与 pack 通过同一原子写入范围发布；pack 最后写入，失败沿原有 blockbuffer 丢弃路径处理。祖先提交失败时，依赖它的后继引用不能单独落盘。继承原有 Pebble 持久化语义，没有新增每块 fsync 保证。
- writer 默认关闭；仅在存在大 Prev 且 writer 同时具备原子 batch、一致读能力时尝试共享。比较基线与 pack、新增唯一 chunk KV、metadata 成本，允许至多 12.5% 的初始种子开销；不满足条件则保持原自包含格式。不是每个输入都保证节省。
- 读取 pack、repair 和片段时固定同一快照及 buffer 拓扑；冷构建多遍扫描也复用同一读视图。缺片段、错桶、损坏或校验失败返回错误，不能当作 missing 或退回旧格式掩盖问题。旧 raw、Snappy、块内 v2 与 repair 行继续可读。
- GC 先等待热 pack 删除 flush，再在严格 chain/index guard、canonical/Finish/solidified 与 settled-prefix 证明、完整冷覆盖及 retention 校验下，用新视图确认整桶无任何 changeset 物理行。repair 或异常残留也会阻止回收。同一 batch 删除 chunk 范围并写永久 retired metadata；retired 桶的旧高度重放只能写自包含格式。

GC 每轮最多检查 64 条桶 metadata、处理 4 个候选桶，最多验证 4096 个块的 tx-range。锁忙、缺覆盖或非空均保守延后并重扫；错误记录后保留数据，不把正常 prune 水位当作已回收字节。桶 0 若缺 genesis 的 tx-range，当前会持续延后，未加入特殊放行规则。完整 mutable state reset 则在同一 batch 删除 pack、chunk 与桶 metadata，避免只留下半个引用关系。

一旦启用新引用，不能回滚到不识别 v3 的 b130/fdfe。运维先部署 reader、显式保持 `--history.cross-block-dedup=false`，再独立启用 writer。启用前持久写最低 reader marker，并安装独立 systemd 预启动 guard，校验固定 reader 二进制和命令。后续恢复只能使用同一新 reader 并关闭 writer；marker 不删除。该保护针对受控 systemd 启动，不能宣称旧程序本身认识 marker 或任意直接执行旧 binary 都会被阻止。

## 实盘连续样本回放

原生停机采样取得真实连续高度 **30,514,144–30,514,207** 的 64 个 seq0 pack，跨越 **30,514,176** 桶边界，两侧各 32 块；没有重编号来制造跨块局部性。采样完整、恢复成功，导出编码正文 190,650,540 B、解码正文 471,717,129 B，共 28,899 行。1.54839397 秒是 inspect 循环时间，不含整个停启事务。

随后在新的私有 Pebble 中，使用同一批原始 RLP、真实高度和生产编码逻辑回放。候选关闭并重开数据库后，逐包恢复结果与全部原 RLP 完全一致。

| 比较项 | 字节 |
| --- | ---: |
| 同一输入的原自包含编码基线 KV | 190,652,780 |
| 新格式重开后实际遍历所得逻辑 KV | 29,941,506 |
| 减少 | 160,711,274 |
| 相对基线减少 | **84.2953%** |

新格式统计包含所有 pack、唯一新增 chunks、key 和桶 metadata；不是只算引用包大小。导出正文与基线 KV 是不同口径。**这里的实际逻辑 KV 节省不是 SST/WAL、`du` 或线上 chaindata 净回收，也不是同步提速比例。** 回放计时不能直接与线上块处理速度相比；RLP 等价也不能代替独立的主链 canonical canary。

本地采样转录见 `build/benchmarks/20260913-history-shared-chunks/observed-boundary-inspection.json`；服务器保留原始导出与原生回放输出。回放实现的 `Seal` 在重开数据库后实测逻辑 KV 并核对发布成本，而非仅累加编码预测值（`core/rawdb/history_sharing_benchmark.go`）。

## 验证与桥接恢复

本地测试覆盖原子发布失败与重试、祖先依赖、重组、桶边界、retired 后旧高度重写、损坏传播、一致快照、并发 GC、as-of/unwind/repair/cold build 及 reset 的引用完整性。首次全仓运行含沙箱绑定端口失败和既有 async sender retry 用例失败；允许环境下完整重跑通过，最终受影响包复查通过。GC/reset/worker race 检查通过。日志保留在本轮 `full-tests.log`、`full-tests-network.log`、`final-affected-tests.log` 和 `gc-race-tests.log`。

服务器已通过隔离源码、原生 Sapling/CGO 构建及相关测试。原始源码、二进制、依赖和运维脚本的 pin 保存在 prepared 记录中；升级保留原五个环境开关、argv、40 GiB 内存限制、端口、其他服务、holds 与真实磁盘保护。

首次 reader 桥接在替换为 writer=false 配置后、启动候选前被 `ExecStartPre` 验收拒绝。现场确认旧 systemd 将 space guard 和新 reader guard 分别输出为两条同名属性行，继承的 dict 解析只保留最后一条；自动恢复也被相同的比较问题阻断，服务停在 inactive。ARMED 与数据目录 reader marker 均不存在，writer 尚未启用。

运维事务起点 **12:49:21 UTC（1789303761）** 至恢复健康 **12:57:35 UTC（1789304255）** 共 **494 秒，约 8 分 14 秒**。这是覆盖该故障与恢复的观测区间上限；没有测得精确进程停启与 API 不可用起止，不能称为短暂停机，也不能把 494 秒冒充精确停机时长。

恢复时只在原 pinned d656 运维模块内按原顺序聚合重复启动属性，完整 `compare('off')` 通过后，启动已经安装的新 reader 并通过健康检查，恢复 PID **15143**。没有跳过静态配置比较，没有改写 prepared/source/binary，也没有开启 writer。原失败 state 保留，恢复另记 `bridge-recovery.json`。

永久修复包括原 helper 的属性聚合和一个独立 `enable/disable` wrapper。后者校验自身精确 ops blob，同时继续加载原 d656 脚本及其原生证据；只修正主服务属性解析，不重编业务二进制。55 项 Python 测试及独立审阅通过。首次执行该 wrapper 又在变更前因原 archive 解压脚本为 root:root 0664 被严格权限检查拒绝；现场内容 SHA 与 pin 一致，reader 仍健康关闭 writer、marker 未触。对已经核验内容的文件描述符收紧权限为 0644 并 fsync 后，原脚本与 prepared 内容保持不变。第二次启用退出 0，状态为 active。

## 线上验收

启用事务为 13:05:43–13:06:16 UTC（1789304743.117919–1789304776.4933794），包括停启及健康验证，不等于精确 API 不可用时长。实际 argv 为 `--history.cross-block-dedup=true`；PID **17289**，start ticks **4503924631**，进程启动纳秒 **1789304752801423589**。原五个环境开关、服务端口、内存约束、holds、space guard 均通过完整配置校验。reader marker 与新 guard 已保留；恢复只允许同一新 reader 关闭 writer。

业务二进制 SHA256 为 `4c18760699e34a72ec23200cb0351b9ab59d8ef8a3a97c0f1a53c5a81d9e4829`，原 prepared 文件 SHA256 为 `ef7f4c0ff67e323b56df3a7868a34a4bc096e6c7f08330047ec366016c627fad`。原生 `writer-state.json`、`shared-health-on.json`、`writer-systemd-wrapper-last.json` 均保存在 `/data/gtron/releases/20260913-history-sharing`。

固定区块 **30,514,176** 的 `eth_getBalance` 部署前后响应完全一致，为 `0xb6283b0374b0e000`。启用后 Solidity 大委托账户索引历史请求成功，返回 **9,876,866 B**，`fromAccounts` 0 项、`toAccounts` **219,471** 项，耗时 **4.991 秒**；请求前观测 solid 为 **30,525,904**，高于 writer 健康起点 30,525,550。响应原文和摘要保留在原生 release 的 `shared-delegation-canary*.json`。该请求没有逐次 codec 追踪，不能单独证明实际命中 v3 片段；明确的逐包 v3 完整性证据来自前述原生回放、重开和相关测试。
### 第一轮线上连续采样（d656）

13:06:37–13:12:37 UTC，13 点、26 次成功请求，窗口内固定 PID 对应启动纳秒与 owner=1；采集索引 SHA256 为 `7fb5798a91ddcda3d6076e410c333a287fc77ad173ce78f7f09b3785bc18bfad`。原始 HTTP 响应保留在 `build/benchmarks/20260913-history-prev-0952/after-cross-block`；逐样本校验及汇总在本轮 `live-shared-analysis.*`、`live-shared-summary.*`。

| 指标 | 结果 |
| --- | ---: |
| 写入 pack 次数 | 1,141 |
| 共享尝试 / 选中 / fallback | 683 / 683 / 0 |
| 同批输入按旧编码的全部逻辑成本估计 | 3,855,604,672 B |
| 实际暂存发布的全部逻辑 KV | 732,991,896 B |
| 减少 | **3,122,612,776 B，80.9889%** |
| 共享命中包自身成本降幅 | 81.2726% |
| 新 chunks | 9,286 |
| 原始片段复用累计 | 14,067,611,165 B |
| GC 退休 / 因覆盖不足延后 / 错误 | 0 / 1 / 0 |

旧编码对照来自同一请求预先计算的 baseline，非共享包沿用实际成本；不是拿不同高度的两段流量相减。完整 pack key 为 35 B；新版 encoded 已含新 chunk values，logical 再含 keys 与新 metadata，不重复加 chunks。全 13 点字节恒等式残差均为 0；7 个中间点有 1 次共享尝试尚未分类，首尾守恒，按在途/非原子指标处理。所列错误、失败及 stall 指标未见新增，额外检查的 69 个非 shadow 错误/失败相关指标也全部为 0。这是成功暂存的逻辑成本统计，不是 fsync 或物理回收承诺。

空间改善同时伴随显著吞吐下降：前测 12:27:18–12:28:48 UTC 为 **5.6894 blocks/s、751.41 TPS**；后测为 **3.1500 blocks/s、443.11 TPS**，块速下降 **44.63%**。后半窗仍只有 3.3782 blocks/s，不能归为单纯启动预热。交易密度从 132.07 到 140.67 tx/block（+6.51%），CPU 均值从 2.383 到 2.498 cores。后测执行能量更高、冷后台推进也更快；不能将全部速度差都归因于共享编码，但必须处理新路径的额外开销。

| 历史水位与 head 的距离 | 窗口起点 | 窗口终点 |
| --- | ---: | ---: |
| state prune | 316,202 | 308,865 |
| body cold coverage | 379,038 | 314,642 |
| transaction index cold coverage | 379,038 | 371,986 |

三类历史积压都缩小。网络剩余高度下降 1,122；下载 buffer 从 100 到 970，中途最高 1,983，后半窗从 1,578 降到 970，不能用重启初值夸大持续积压。全部点均 active、无 pause 或 fetch backpressure。

### 物理空间与新增 CPU 开销

同窗 engine disk gauge 从 572,397,267,150 B 到 570,311,482,270 B，减少约 1.943 GiB；current SST gauge 减少约 2.583 GiB。compaction debt 从约 46.975 GiB 到 44.711 GiB；压实输入 11,662,630,014 B、输出 10,256,306,964 B。这些指标表明后台仍在工作，不能将 debt 当作一定可回收空间。

13:11:40 UTC（1789305100.721148）的独立 `du -s -B1` 为 **571,992,543,232 B**，它与上述 gauge 的时点和计量都不同，不拿它们相减。该时点 native service active、PID 17289，最近 2 MiB 日志中未见指定 panic/FATAL/corrupt/missing-chunk/shared-error 模式。记录在原生 `shared-live-du.json`；这是有界检查，不代替全日志审计。

既有热包不会因开关自动重编码；旧存量仍须经过冷发布、prune 与后续压实才可能释放。新 chunks 也须等整个桶具备安全回收条件后才退休。首轮 GC 尚未实际退休共享桶，不能宣称线上回收生命周期已经完整走过。

13:13:08 起补采 30.11 秒 CPU profile，采样 CPU 69.30 秒。SHA256 AVX2 占 25.34% flat CPU；共享规划 9.74 秒累计 CPU，其中整包 SHA 4.43 秒、每片段 SHA 4.20 秒，已有片段 decode/校验 0.74 秒、查询 0.20 秒。此前块内 v2 编码又消耗 4.89 秒，并独立分块/计算相同的片段哈希。热点与源码核对后，继续优化单次调用内的分块与 hash 复用；保留完整输出格式、整包校验、已存片段内容校验和发布边界，不通过跳过数据校验换取速度。

### CPU 后续优化（8058）

源码 `8058e99d4e080c3329a587a2feef16133e676e84` 已提交并推送：只修改 3 个生产 Go 文件，新增对照测试与基准。块内编码将本次 RLP 的切分位置和片段 SHA256 借给共享编码；绑定原缓冲区起始地址、长度和合法切分布局，生命周期止于本次发布，归池前清空。仅共享开启、含大 Prev 且具备原子写能力时请求元数据，v2 没执行则保持原来的计算路径。无全局缓存，也不缓存数据库片段存在性；格式、whole-pack SHA、既存片段校验、成本门限与原子发布保持。

本地 M1 Max / GOMAXPROCS=2，以相同 6 MiB RLP、相同内存基线及 batch staging，400ms×3 中位数：重复大值 seed 场景 **16.281→11.287 ms/op（-30.67%）**，existing 场景 **17.185→11.963 ms/op（-30.39%）**；单大值不运行 v2、关闭共享路径无明显变化。该基准不含实际磁盘或整链同步，不能把 30% 直接当作线上块速变化。24 条原始观测全部保留在 `local-codec-reuse-benchmark.json`。

对照测试覆盖旧新字节完全一致、同长度不同缓冲区、布局和开关、seed/existing/桶边界/退休/fallback/损坏/写失败等情形；相关 race 通过。完整受影响包 rawdb、blockbuffer、cmd/gtron 分别 8.292、3.980、37.646 秒通过。独立源码复审无阻断。服务器原生 Sapling/CGO 构建、存储、检查与兼容历史相关测试全部通过。

同一批连续 64 个实盘区块再次用 D656 和 8058 分别回放，每条样本的 block、原 RLP SHA、行数、完整性、片段/包成本及重开结果全部一致，最终仍为 **29,941,506 B** 逻辑 KV。写入阶段 wall **8.1563→6.1288 秒（-24.858%）**，进程 CPU **9.7814→7.3827 秒（-24.523%）**；包含读回校验的整个命令 wall **35.5124→33.0728 秒**。未改的 baseline/read/reopen 阶段变化较小。原生原始 JSON 与比较记录保存于两个 release 的 `before-cpu-sharing-replay.json`、`continuous-sharing.json`、`cpu-replay-comparison.json`，本地明确标注转录来源的摘要为 `observed-native-codec-reuse.json`。

8058 原生二进制 SHA256 为 `c7f35a84471ad2277c28096017ede9e0511ae943a95a63ebaf08b0380521c505`，`upgrade-prepared.json` SHA256 为 `8b00e50505ef3c91e4f71c6366285c9a9c352b7b57579eb040c086295d7089cc`；均在服务器执行了字面量 pin 比较。release 为 `/data/gtron/releases/20260913-history-sharing-cpu`。

兼容升级事务 **13:41:22–13:42:48 UTC**（1789306882.59356–1789306968.4699717）退出 0、phase=active，期间包含停启与健康校验，不等于精确 API 不可用时长。实际 PID **25805**、start ticks **4504138768**、启动纳秒 **1789306894172214942**，writer=true；健康头高从 **30,533,297** 到 **30,533,311**。原 D656 release 的永久 marker 原样保留，数据目录及新 release marker 指向 8058；guard 代码不变。当前失败恢复通过同一兼容升级脚本回 D656 reader，禁止回到不支持 v3 的 b130。

最终版本再次查询固定区块 30,514,176 的余额，与两次之前的响应一致。大委托索引历史请求返回 **9,171,611 B**、203,812 项 ToAccounts，耗时 0.3144 秒；其 solid 高度已经改变，不能把列表长度或耗时与此前请求直接做正确性或性能差分，亦没有逐请求 codec 追踪。

### 最终线上连续采样（8058）

13:44:40.775025–13:50:40.769945 UTC，13 点、26 次请求全部成功，进程启动纳秒始终为 `1789306894172214942`、owner=1。所有差分仅在该进程内计算；原始响应位于 `build/benchmarks/20260913-history-prev-0952/after-cross-block-cpu`，独立分析为本轮 `live-cpu-shared-analysis.*` 与 `live-cpu-shared-summary.*`，首轮结果未覆盖。

| 最终窗口指标 | 结果 |
| --- | ---: |
| 全部 pack 写入 / 共享选中 | 7,776 / 882 |
| 同批输入按原自包含编码的全部逻辑成本估计 | 3,982,313,316 B |
| 实际暂存发布的全部逻辑 KV | 602,947,724 B |
| 全部历史编码逻辑成本减少 | **3,379,365,592 B，84.8594%** |
| 选中共享包自身成本减少 | 89.1312% |
| 同步块速 / 交易速率 | **21.5998 blocks/s / 1,708.70 TPS** |
| 网络缓冲块数 | 1,251 → 1,482 |
| GC 候选 / 覆盖不足延后 / 退休 / 错误 | 19 / 19 / 0 / 0 |

所有 pack、唯一新 chunk、key 和 metadata 均已计入，不能再次叠加 chunk values。共享 attempts 增加 881 而 selected 增加 882，原因是首点在途残差 1、末点为 0，不能把差值当作失败或算出超过 100% 的命中概率。13 点两条字节恒等式残差均为 0；fallback 为 0，检查的 69 项非 shadow 错误相关指标各点均为 0，写 stall 无增长。同步始终 active，无 pause 或 fetch backpressure。

**本窗口更快，但负载也明显变轻，不能把 21.6/3.15 倍数当作补丁收益。** 相对第一轮，tx/block 从 140.67 降到 79.11，未压缩 bytes/pack 从 12.979 MB 降到 1.269 MB，共享包占比从 59.86% 降到 11.34%。进程平均 CPU 从 2.498 增至 2.814 cores，并未下降。可归因的 CPU 改善证据是前述同 64 包、同字节结果的原生写入回放 CPU 减少 24.523%；线上窗口仅作为运行状态与成本核验。

| 同点 metrics 的 head 距离 | 首点 | 末点 |
| --- | ---: | ---: |
| state cold published | 267,492 | 275,252 |
| body cold coverage | 322,967 | 330,727 |
| transaction index cold coverage | 322,967 | 330,727 |

本窗口 head 从 30,535,063 到 30,542,823，三类冷覆盖边界未推进，差距均增加 7,760 块，仍在数十万块数量级。**实际 state 热历史删除水位不可用（N/A）**：本进程 last-pass 水位与对应删除计数均为 0，不能用 head−0 得出三千万块积压；表中的 state cold published 是独立冷边界，不冒充删除进度。共享桶全部因冷覆盖不足延后，尚未在线观察到实际退休，仍保留该验收限制。

压实债务从 37.964 降至 9.490 GiB，压实累计读入约 17.436 GiB、输出约 14.563 GiB；这不是可回收空间承诺。current SST 从 486.025 到 486.282 GiB，zombie bytes 从 2,629,510,474 到 0。第 005 点出现 obsolete files=-7、obsolete bytes=-58,947,896 的指标异常，原值保留并标无效；受其影响的同点 disk gauge 不作为精确磁盘证据。

### 最终目录占用与剩余边界

同一路径、同一 `du -s -B1` 口径，13:47:38–13:50:47 UTC 的约 188.195 秒子窗口内，chaindata 从 **525,398,650,880 B** 到 **523,226,918,912 B**，减少 **2,171,731,968 B（2.023 GiB）**。13:50:47 整个 datadir 为 1,374,944,759,808 B，缺少同区间整个目录起值，不能宣称总数据目录释放了多少空间。原生记录为 `du-sampling-first.json` 与 `final-live-state.json`；检查时服务 active、PID 25805，最近 2 MiB 日志未见指定严重错误模式。

14:00:01 UTC 再以 `du -B1 --max-depth=2` 核对目录分布，退出 0、stderr 为空，原始记录为新 release 的 `final-directory-distribution.json`：

| 目录 | 分配字节 | 十进制约值 |
| --- | ---: | ---: |
| `gtron/chaindata` | 523,504,345,088 | 523.50 GB |
| `gtron/state-snapshots` | 623,109,976,064 | 623.11 GB |
| `gtron/ancient` | 228,607,848,448 | 228.61 GB |
| 整个 datadir（含目录等开销） | 1,375,222,185,984 | 1.375 TB |

热库较第一轮 13:11:40 的 571.99 GB 已缩小，但冷迁移也会增加其他目录；整个目录的两个末段观测反而增加约 277 MB。上述运行中 `du` 并非原子快照，不能按目录增减反推精确对象迁移量，也不能把热库下降全部归因于新编码。该修复减少新写入的重复历史，不承诺数据目录停止增长或立刻改写全部旧包。

13:51:10 起的最终 30.11 秒 CPU profile 采样 CPU 65.48 秒，SHA256 flat 占 12.02%；共享规划累计 5.31 秒，其中整包 SHA 3.21 秒、仍需独立计算片段 SHA 的分支 0.58 秒、已有片段 decode/校验 1.22 秒。它与消除重复 hash 的实现一致，但由于区块负载不同，不用两个线上 profile 的比例证明同负载 CPU 收益。原始 profile、top 和按行归因保存在 `after-codec-reuse-cpu*`，归因源码固定为 8058。

当前保留的边界是：旧热历史继续等待正常冷发布、裁剪与压实；共享桶线上实际退休尚待完整冷覆盖；state last-pass 为零时不能报告真实删除水位；obsolete 指标出现过负值，应以原生目录测量核对空间。完整历史内容、失败恢复、重开和并发回收的测试证据均已保留，当前服务正常运行，无新增错误证据。
