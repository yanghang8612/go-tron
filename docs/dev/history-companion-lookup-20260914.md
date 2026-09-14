# 历史覆盖检查 companion lookup 优化记录

2026-09-14。业务提交 `8a0681d3`，运维适配提交 `9d968e95`。已完成原生构建部署。本文中的“baseline”始终指旧业务 `af989753`、PID `20814`、process start `1789356509868677122`；新版部署与观测另行记录。

本轮消除 pruner 构造覆盖检查时重复扫描整个 active manifest 的成本。该改动有明确 CPU 栈和组件基准支持，但没有直接改变历史数据编码、冷存发布水位、保留窗口或 Pebble 压实策略，不能视为已经解决全部 chaindata 增长。

## 上线前同步、积压和数据库状态

证据目录为 `build/benchmarks/20260914-metadata-commit/`。`before-analysis.json` 和 `before-analysis-summary.json` 记录 13×30 秒完整同进程样本；metrics 中点时间为 UTC 04:08:08.355376–04:14:08.275025，约 359.917 秒。Wallet 使用独立请求时间。同期为北京时间 12:08–12:14。

| 指标 | baseline 结果 |
|---|---:|
| Wallet session 导入块增量 | 5,504 |
| Wallet session 交易增量 | 997,856 |
| 导入速度 | 15.292 blocks/s |
| 交易速度 | 2,772.43 TPS |
| 平均交易密度 | 181.30 tx/block |
| 进程平均 CPU | 3.230 cores |
| buffered blocks 首末 | 1,878 → 1,886 |
| buffered blocks 采样范围 | 1,094–1,940 |
| buffered bytes 首末 | 87,892,308 → 115,763,086 B |
| 同步状态 | 全部 active；未观察到 pause 或 fetch backpressure |

Wallet current block 增加 5,512，metrics head 增加 5,514；这些读数来自不同时间与口径，不能强行与 session blocks 的 5,504 对齐。以下头距统一用同一个 metrics 样本相减。

| 冷覆盖/裁剪指标 | 水位首末 | 头距首末 | 头距变化 |
|---|---:|---:|---:|
| state cold published | 30,607,697 → 30,610,199 | 448,611 → 451,623 | +3,012 |
| state domain-change pruned through | 30,607,697 → 30,610,199 | 448,611 → 451,623 | +3,012 |
| body coverage | 30,605,312 → 30,605,312 | 450,996 → 456,510 | +5,514 |
| transaction-index coverage | 30,588,928 → 30,605,312 | 467,380 → 456,510 | −10,870 |

state eligible-cutoff 到 published 的差距为 382,090 → 385,975，增加 3,885；它与 head 到 published 的差距不是同一个指标。六分钟内 state 和 body 头距有所扩大，仍在约 45 万块量级；transaction index 窗口内累计推进 16,384 块（两次 8,192）。短窗口不能证明全天积压稳定，也不能把 body 暂未发布直接认定为 worker 卡死。

共享历史 GC 在此窗口逻辑退休 +3，coverage defer +71，busy defer +6，错误 +0。专属 queue attempts/admitted 均 +3，queue busy/errors 均 +0；admitted 闭包工作累计 3.567403 ms。defer/candidate 是可重复扫描计数，不是不同桶数；retired 也不等于磁盘释放。共享编码对同批输入的全 pack 逻辑成本估算从旧编码 4,795,668,790 B 降到实际暂存 605,665,878 B，少 87.37%；该比较不包含 WAL、SST、索引、memtable 或压实，不能当作 chaindata 净回收。

原生 `/proc` 与物理 `du` 摘录在 `native-runtime-before-observed.json`，Unix 时间 `1789359528.0369174`：

| 目录 | 物理字节数 | GiB |
|---|---:|---:|
| chaindata | 241,106,391,040 | 224.55 |
| state snapshots | 673,527,320,576 | 627.27 |
| ancient | 234,350,960,640 | 218.26 |
| datadir 总计 | 1,148,984,688,640 | 1,070.08 |

这是上线前独立物理单点，不能用于计算本窗口增长率。metrics 窗口内压实输入 +23,157,938,583 B、输出 +19,994,903,135 B，说明压实正在工作；debt 从 1,352,565,914 B 到 2,388,536,918 B，采样峰值 4,523,599,647 B。未观察到 write stall，所检查的 70 个错误指标均无非零观察。obsolete bytes/files 曾出现负值，故拒绝用受影响的 engine disk gauge 推导物理净增或回收。压实输入减输出与 debt 均不等于可回收空间。

## 真实 manifest 的规模和分布

原生固定元数据文件：`/tmp/gtron-metadata-baseline-20260914/manifest.json`。

- generation：33,683。
- JSON 大小：45,128,916 B。
- SHA256：`d752baf77b3ef3503b38d6e89400b82c5cc2a4ea091506a100e2a81f3f46821e`。
- active：70,198 refs；retired：92,667 refs。

| Active dataset / kind | refs |
|---|---:|
| state-domain-change / history | 2,794 |
| state-domain-change / inverted | 2,794 |
| state-domain-change / accessor | 2,794 |
| event-log / event-log | 30,908 |
| event-log / event-log-index | 30,908 |
| 合计 | 70,198 |

事件文件与事件索引占 active 条目约 88.06%。pruner 需要识别的 history 仅 2,794 个，但原来为每个 history 找 index/accessor 时都在线性扫描包含这些事件条目的整个 active catalog。条目数量分布描述的是 metadata 查找规模，不能替代各类段文件的物理字节分布；retired 条目数量也不等于仍存在的文件数或等待回收的字节数。

该文件只固定 JSON 元数据，没有 pin 住其源段文件。后台 GC 可继续回收源段；原生 metadata replay 不读取源段，也不能作为内容完整性验证。统计来源为 `native-manifest-observed.json`。

## 定位与实施范围

`cpu-before-45s.pb.gz` 从 UTC 04:08:12 起采样 45.17 秒，记录 146.93 CPU 秒，Build ID `a9f8ee5288f99097bf4a98cc78fe9c48f278329e`。CPU 秒是各线程的采样总和，不是串行耗时。

`findManifestRef` cumulative 为 7.14 CPU 秒，占总 CPU 4.86%；其中 7.11 秒来自 `Worker.newSnapshotStateDomainChangeCoverageGate` 构造。完整 gate 构造栈为 7.50 CPU 秒。原 lookup 包含 2.14 秒自身 flat、4.66 秒 `runtime.duffcopy` 等成本，和重复复制/比较扫描行的实现相符。函数调用链嵌套，以上数字不能相加。可复现命令：

```bash
GOTOOLCHAIN=local go tool pprof -top -cum \
  -focus='newSnapshotStateDomainChangeCoverageGate' -nodecount=16 \
  build/benchmarks/20260914-metadata-commit/cpu-before-45s.pb.gz
```

9/10 已实现的 manifest 发布后 decode cache 预填充、完整 bytes 对照及公共可变副本仍然保留。本轮没有重复实现 decode cache；本次瓶颈是公共副本不带私有 lookup 时，bulk gate 逐 history 的 companion 线性查询，约 O(H×N)。

生产修改为：

1. `snapshots.HistoryCompanionView` 只复制 active `SegmentRef` 容器，用既有有界 `manifestLookupView` 建索引；字符串作为不可变值共享。它只提供 companion 查询并返回值，不公开私有 Manifest 或切片，也不保留 retired、chain、progress。
2. 仅在单次 gate 构造的局部作用域使用 view。构造约 O(N log N)，随后每次路径查找约 O(log N)，相同路径的候选仍按原顺序检查。输入超过 1,048,576 refs 时直接返回 nil，零额外复制/索引，调用方沿用原扫描语义。
3. gate 返回后不持有 view，其额外容器和索引可被 GC 回收。后续按需 `verifyHistory` 仍使用原 companion 查询路径，因此没有宣称清除了全程序的全部扫描。

公共 loader 仍返回可变且不带 lookup 的副本。原 Manifest 或 Segments 后续修改不会污染 view；按 path、dataset、kind、range、effective aggregation steps 匹配的规则及重复路径的首个有效匹配均保持一致。

完整 manifest ReadFile 与 bytes 对照、每个 history/index/accessor 的 fresh Stat、size/mtime 身份核对、SHA 和语义认证全部保留。view 只查 metadata，不替代认证。Finish、canonical、settled-prefix、覆盖证明和原子退休的安全条件均未修改；没有改变磁盘格式、数据编码、跨轮缓存、缓存预算或运行并发。

## 组件基准与内存成本

本地 Apple M1 Max / darwin-arm64，Go 1.25.5，GOMAXPROCS=2，`-benchtime=1x -count=3`。结果保存于 `companion-local-bench.txt`。Synthetic 使用上述精确 active kind 数量与 retired 数量；文件名、范围、size、checksum 是代表性元数据，不是原生 JSON 的逐条复制。

| 基准范围 | 原扫描 | 新 view | 原扫描 B/op | 新 view B/op |
|---|---:|---:|---:|---:|
| 完整 gate 构造 | 1,144.87–1,181.15 ms | 47.32–49.68 ms | 63,361,432–63,361,448 | 70,955,720 |
| metadata companion replay | 1,108.54–1,114.36 ms | 9.39–9.86 ms | 268,224 | 7,862,512 |

完整 gate 基准对比实际新版构造器与测试内冻结的原扫描构造器，计入当前 JSON 读取/bytes 对照、公共 loader 副本、view 构造、每个 history 的三次 Stat（总计 8,382 次）、active verification cache retain、gate 排序。生产 manifest cache 开启，预算 256 MiB，发布在计时外预填充。文件为 size 匹配的一字节占位文件；此阶段构造只收集 metadata 身份，内容 SHA/语义认证在后续按需验证阶段，未计入该基准。

metadata replay 在计时外完成 JSON 读取、解码、Validate/ValidateProduction 和 history 列表准备；计时内每轮包含 active 副本、索引构建以及全部 2,794 个 history 的 index/accessor 查询。它不做 JSON 逐轮读取、Stat、cache retain 或内容认证。两种基准都不是整条同步流水线重放，其提升比例不能等同线上 blocks/s 的提升比例。

当前架构 `sizeof(SegmentRef)=104 B`，70,198 条 active 副本为 7,300,592 B，path 索引为 280,792 B。该 catalog 没有 freezer refs，所以无 freezer 索引 backing 数组。加上小对象与分配器对齐，实测新 view 每轮增加约 7,594,288 B（7.24 MiB）、7 次分配；不额外复制 92,667 条 retired，也不增加字符串内容副本。

这只是本轮额外分配，不是进程峰值 RSS 或常驻缓存大小；gate 原本持有的公共 manifest 仍有自己的生命周期。1M 上限内，active 容器本身最多约 104 MiB，path 索引 4 MiB，若全为 freezer 再有至多 4 MiB，合计约 112 MiB，另有小对象/对齐。不能把整个 view 误称为最多 8 MiB。超过上限则直接退回扫描而不分配上述容器。

本地复现：

```bash
GOTOOLCHAIN=go1.25.5 GOMAXPROCS=2 go test ./core/state/pruning \
  -run '^$' -bench 'BenchmarkHistoryCompanion' -benchtime=1x -count=3
```

在原生主机使用固定真实 JSON，仅重放 metadata：

```bash
GTRON_HISTORY_COMPANION_MANIFEST=/tmp/gtron-metadata-baseline-20260914/manifest.json \
GTRON_HISTORY_COMPANION_MANIFEST_SHA256=d752baf77b3ef3503b38d6e89400b82c5cc2a4ea091506a100e2a81f3f46821e \
GOTOOLCHAIN=go1.25.5 GOMAXPROCS=2 go test ./core/state/pruning \
  -run '^$' -bench BenchmarkHistoryCompanionMetadataReplay -benchtime=1x -count=3
```

指定原生文件时 SHA256 必须同时提供且匹配；该基准不依赖线上源段的存活时间。

## stateCommit 与下一步优化判断

详细调用栈及互斥分组见 `statecommit-review.md`、`statecommit-review-summary.json` 和 `statecommit-review-hand-cold-stacks.json`。同期滚动 stateCommit gauge 采样为 17.25–42.69 ms/block，中位数 38.41；execute busy 中位数约 99.48%。滚动指标可能跨越窗口、互相重叠，不能作为独立的全窗口均值，更不能直接等同 CPU 函数耗时。

| Profile 路径 | cumulative CPU 秒 | 总 CPU 占比 |
|---|---:|---:|
| 前台 CommitStateCapture / commitWithStatsOptions | 1.15 | 0.78% |
| latestUpdateBatchFromTouches | 0.18 | 0.12% |
| accountCommitPlanGenerationResolver | 0.01 | 0.007% |
| buildOpsWithHasher | 2.58 | 1.76% |
| 其中 resolveOpWithHasher 必要哈希 | 2.48 | 1.69% |
| commitment runPartition | 28.18 | 19.18% |
| 其中 GetBranchInto | 19.20 | 13.07% |
| 其中 readDurable | 16.08 | 10.94% |
| 分区内 nodeHashWithStats | 4.97 | 3.38% |

这些路径嵌套，禁止求和。现有证据不支持同时改 PreparedOps 或 generation resolver：重复排序/校验路径很小，必要哈希不能省略，而跨 cut 数据所有权与生命周期改变有额外验证成本。stateCommit 墙钟还可能包含 deferred fold 等待和归账。

Pebble `runHandCold` 为 15.62 flat CPU 秒。按直接承担该 CPU 的调用栈互斥分组，commitment 分区点读 5.96 秒、critical prefetch 4.44 秒、lookahead prefetch 1.87 秒，合计 12.27 秒（78.55%）；再加 StateReadAhead 2.02 秒，共 91.49%。cold builder event-log/state-history 仅合计 0.09 秒。这能说明哪个调用者支付淘汰 CPU，但不能识别被淘汰块此前是谁读入的，也不能排除冷扫描的间接污染；不能把全部淘汰成本归咎冷 builder。

原生 PID 20814 实际 `--db.cache 8192`（8 GiB）、`--state.trie.cache 128`、`GOMEMLIMIT=10GiB`，commitment partitions/read concurrency/prefetch overlap 为 64/32/0。不能用源码默认的 256 MiB 解释线上缓存热点。环境没有 GOMAXPROCS 变量，不代表已经确认有效 GOMAXPROCS 或 cache shard 分布。

当前锁定 Pebble v1.1.5，公开 DB/Snapshot iterator 没有现成 no-fill 接口；`OnlyReadGuaranteedDurable` 会改变可见性，不能用作缓存控制。后续需要在同一不可变数据库和相同 commitment 更新序列上，联合测量逻辑 parent-cache miss、block-cache miss/fill/eviction、物理读与 fold 时间，并核对 root 和 snapshot 隔离。已有缓存准入和 prefetch 实验也未提供可直接复用的正向结论。本轮不随 metadata 修复增加缓存预算或修改缓存策略。

## 已完成验证

- 新增 view 的原 Manifest/Segment 切片及返回值修改隔离测试；path/dataset/kind/range/steps 错配与重复 path 的首个有效匹配对照；nil 和边界内准入、超限零分配退回扫描。
- 新旧 gate 的 key/顺序一致；history/index/accessor 缺文件、size 错配、同 size mtime 变化均维持原失败或新身份行为；复用同一个 view 仍 fresh Stat；旧 gate 在实际 verify 时拒绝已变化的身份。
- 同 size、同 mtime 的 manifest 替换仍重新读取 bytes，generation 更新可见，非法 JSON 被拒绝；构造与后续 verify 取消测试通过。
- `go test ./core/state/pruning -count=1 -timeout 180s` 全包通过，28.31 秒。
- `go test ./core/state/snapshots -count=1 -timeout 180s` 全包通过，58.47 秒。首次因沙箱禁止既有 httptest 绑定 localhost 而中断；允许本地临时监听后完整通过。
- `go test -race ./core/state/snapshots ./core/state/pruning -run 'TestHistoryCompanion|TestManifestLookup' -count=1 -timeout 120s` 通过，分别 2.01/3.58 秒。
- 完整 gate / metadata component 基准各三次通过，`git diff --check` 通过。测试使用 Go 1.25.5；pprof 分析使用本地可用工具链。

## 原生回放与发布

原生机器为 Intel Xeon Platinum 8175M，Go 1.25.5 linux/amd64、Sapling、GOMAXPROCS=2。
固定真实 manifest 回放三次：scan 为 1.7210/1.7503/1.6836 秒，view（含构造）为
29.810/26.481/26.576 毫秒，中位数比值 64.76。该基准只测元数据查找，不含 JSON
解码及段文件 I/O，不能等同完整 gate 或同步吞吐改善。额外分配为 7,594,288 B/op、
7 allocs/op，与本地相同。原始输出保留在服务器
`/tmp/gtron-companion-native-replay-20260914.out`，本地转录为
`build/benchmarks/20260914-metadata-commit/native-replay-observed.json`。

原生 rawdb/pebbledb 全包、cmd/gtron 和 rawdb 专项、blockbuffer/pruning/snapshots/core
历史兼容性测试全部通过，Sapling probe 确认可用。部署恢复测试新增23项，与当天GC
适配联合运行44项通过，包括源码冻结、部分事务重建、marker保留、所有六项GC排队
指标和 ON/OFF 兼容恢复。

| 固定发布项 | 值 |
|---|---|
| 业务提交 | `8a0681d3b62be2375dbcb0b449c4efe7b8ad5279` |
| 运维提交 | `9d968e953b47c87230ced2326c7b718749675268` |
| Release | `/data/gtron/releases/20260914-history-companion-lookup` |
| 二进制 SHA256 | `a2ce66026ce4357e55e189eeda90365aac9c6b5c9f8f5636fc94fd51594f15d4` |
| upgrade-prepared SHA256 | `3cd5417ec7dd09474597cd4d34c4a5b0c5ad2ae09b9c063fc623e78950360fcb` |
| 部署脚本 SHA256 | `fbb73333bdfa5230827fda8ef0ceff05f5d91b2fa54837565a5f7a0e572a75d4` |

服务器只更新远端引用、归档固定源码构建，保留原工作树。部署事务沿用 start.lock、
优雅停止和健康失败恢复；仅允许回到 af989 兼容 reader，保留 D656、8058、af989 的
永久 reader-required marker。配置、共享写入、预算及格式不变。

## 线上验收

北京时间 12:33:03，事务完成且健康检查通过。新进程 PID 2830，start ticks
4509485376，精确 process/start 为 `1789360360249684220`；共享写入及五个 observer/prune
环境保持开启，服务 active/running。固定历史区块 30,514,176 的 eth_getBalance 与部署前
完全一致，结果 `0xb6283b0374b0e000`。完整13点窗口、CPU profile 及独立 du 均已完成。

跨重启不相减累计 counters；新旧窗口只在各自精确进程内计算增量。观察 lookup CPU
下降时还需检查交易密度、后台工作与预热差异。即使 metadata 成本下降，也不能据此
宣布冷覆盖永远追上、chaindata 不再增长或压实回收已经完成。

### 上线后 CPU 栈

同为约45秒采样，旧 profile 为45.17墙钟秒/146.93 CPU秒，新 profile 为45.14墙钟秒/
131.77 CPU秒。新 profile UTC 04:35:09 开始，属于上面固定新进程。

| 路径 | 旧 CPU 秒 | 新 CPU 秒 |
|---|---:|---:|
| 完整 coverage gate 构造 | 7.50 | 0.29 |
| 其中 companion 查询 | 7.11 | 0.01 |
| 其中新增 view 构造 | 0 | 0.08 |
| 其余 gate 工作 | 0.39 | 0.20 |

新查询+构造合计0.09秒，完整 gate CPU使用速率下降约96.1%。控制基准与线上栈结果
一致，说明本轮查找热点已显著降低；不能把组件比例外推为同步同比例加速。新进程
重启后缓存尚在预热，runHandCold 本次未采到样本，不表示其成本已被此次修复消除。
总CPU差异也不能全归因于该patch；没有与45秒profile严格匹配的交易计数，不报每块/
每交易CPU。原始profile、SHA和分组在 `cpu-before-after-comparison.{json,md}`。


### 完整同步窗口

新窗口 metrics 中点为北京时间12:34:51.691–12:40:51.717，26个 HTTP 请求全部成功，
13点均为精确新进程。分析 `before-after-analysis.json` 的 valid=true；同一工具保留
每个样本SHA和完整性检查。前后分别在各自进程内计算counter差，不跨重启相减。

| 指标 | 旧窗口 | 新窗口 |
|---|---:|---:|
| blocks/s | 15.292 | 17.158 |
| TPS | 2,772.43 | 3,426.37 |
| tx/block | 181.30 | 199.69 |
| 平均 CPU cores | 3.230 | 3.025 |
| state cold / prune 水位推进 | 2,502块 | 3,882块 |
| state 头距变化 | +3,012 | +2,304 |
| state 末点头距 | 451,623 | 464,101 |
| body / txindex 末点头距 | 456,510 / 456,510 | 482,921 / 482,921 |
| GC 逻辑退休增量 | 3 | 11 |
| 非零错误指标 | 0 / 所检70项 | 0 / 所检70项 |

新窗口 metrics head 为31,082,047→31,088,233；state cold/prune为30,620,250→
30,624,132，积压仍在46万量级。buffered blocks为1,467→1,750、范围1,214–1,893，
全部 active，没有pause或fetch backpressure。body/txindex水位仍为30,605,312，该六分钟
没有完成新的整段发布，头距增加6,186块；这不等于全天不推进，也不能单凭此断定卡死。

17.158相对15.292高12.20%，TPS高23.59%，但交易密度、区块/后台输入及缓存预热不同。
这是观测窗口变化，不能称为本patch的精确吞吐提升。state积压增长有所减缓，仍未完全
追平实时导入，不能宣称积压已经消除或全天稳定。

GC retired累计2→13，专属排队attempts/admitted/busy/errors增量14/11/3/0；工作累计
45.624ms，进程生命周期峰值274.687ms且窗口内未刷新。coverage/busy defer分别+85/+44，
均为可重复扫描次数。共享历史同批输入逻辑成本为旧编码估算1,496,682,680B→实际暂存
303,752,031B，节省79.70%；它受输入组合影响，不是物理空间节省或chaindata净流入。

输入组合也在变化：raw/pack较旧窗口下降约72.2%，同样的交易数不代表相同历史写入量。
冷阶段滚动lastpass指标的样本中位数：density metadata 3.235→1.313秒，build 5.337→
1.817秒，maintenance 9.487→4.202秒；窗口最后一次history batch为93块/23,081txnums。
新旧batch blocks的样本中位数为82/139；这些gauge可能重复同一批次且对应不同数据，
不是独立批次平均，也不能把全部变化归因
于新索引。原值保留在 `cold-stage-observations.json`。

### 实际磁盘大小

以下使用独立native du、十进制GB/TB；文件遍历期间服务仍在运行，不是原子快照。

| 北京时间 | chaindata GB | state snapshots GB | ancient GB | datadir TB |
|---|---:|---:|---:|---:|
| 12:18:48（旧版） | 241.106 | 673.527 | 234.351 | 1.148985 |
| 12:36:53（新版） | 245.312 | 674.214 | 234.351 | 1.153877 |
| 12:40:41（新版） | 241.598 | 674.368 | 234.351 | 1.150317 |

最后约3分47秒chaindata实测下降3.714GB；相对12:18旧版单点仍增加0.492GB，整个datadir
增加约1.332GB。该跨度包含旧版本运行、构建及重启，不是同一新进程的增长率或因果A/B。

新六分钟内压实输入26.756GB、输出22.673GB，说明压实持续进行；debt末点5.970GB。
同期目录下降与压实回收并不矛盾，但不能把全部物理下降精确归给GC或以压实输入减输出
代替du。obsolete仍偶有负数，只保留诊断值。/data文件系统尚有约1.832TB可用。
物理数据分别保留在 native-runtime-before-observed、native-runtime-after-observed 和
native-final-observed JSON；这些本地文件注明为原生终端转录，原始JSON留在服务器/tmp。

### 本轮结论及后续重点

coverage gate重复线性查找已完成优化、部署并通过真实数据回放及线上栈验证；历史
查询、保护标记、共享写入和GC能力正常。数据库确实出现了物理回落，但总体仍可能随
同步和数据密度增长。下一步优先继续减少冷历史密度统计的元数据开销，并以固定数据库/
更新序列分析commitment点读的缓存命中和淘汰成本；需要同时验证root和snapshot隔离，
不凭一次重启后profile调整缓存预算。现有证据不足以把剩余问题归给排序或安全增加并发。
