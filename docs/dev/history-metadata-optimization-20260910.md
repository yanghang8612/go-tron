# 归档元数据优化与代码副本修复（2026-09-10）

## 范围与证据

延续 `19eda11f` 的资源探针修复，本轮减少重复 manifest 解码，补齐归档预算降级及深度 6/7 缓存来源诊断，并修复本轮真实日志揭示的代码副本问题。保留全部历史、现有缓存预算、资源压力阈值、duty/恢复计费、快照版本与代码哈希校验；不改变链格式、共识规则、端口或数据库目录。

本轮初始采样为北京时间 15:18:37.732–15:23:38.554，21/21 组 metrics/Wallet 成功，跨度 300.812 秒，仍为 PID 6479、进程标识 `1789023218388384212`。head 22,582,200→22,589,953（25.774 块/秒），状态发布/裁剪 21,991,375→21,999,314（26.392 块/秒），差距 590,825→590,639，五分钟仅净消化 186 块。代码哈希拒绝与 strict error 均由 0 增至 1。

临近部署的第二个五分钟窗口为 15:39:41.631–15:44:41.874，21/21 组成功、同一进程，head +10,096（33.628 块/秒），状态发布/裁剪 +7,381（24.585 块/秒），head-state 差距 593,971→596,686（增加 2,715）。body 暂未跨段，index 推进 49,152 块；各层差距不能相加。状态距离扣除 65,536 常规保留窗口后为 531,150 块。这说明原版本尚未稳定净追赶，不能用初始五分钟的微小净减少宣称积压已解决。

## 已复现的代码副本缺陷

15:20:05.660 的真实诊断为 `strict_object`、`codeBytes=0`、非空预期代码哈希、实际 `Keccak(empty)`、`codeDirty=false`。调用链经过 `discardShadowBlock.preexecuteSenderChainsWithRetryState`、`discardShadowWorker.execute`、`VMActuator.Validate` 和 `StateDB.GetCodeStrict`，明确来自 sender-chain worker。该日志不能单独证明持久化代码文件损坏，也不能把此前所有拒绝都归为同一原因。

`copyStateObjectInto` 使用 `append([]byte{}, obj.code...)`，把尚未加载的 nil 代码变为非 nil 空切片，同时保留已有的非空 codeHash。严格读取因此误入“已有代码”分支，对空切片做哈希；普通读取则直接返回空代码。完整 Copy 与 block execution 的 dirty-object Copy 共用该路径，worker 从 block base 创建额外副本时可触发此形态。

修复改为 `bytes.Clone`，保留 nil/空/已加载三种状态及独立字节所有权。八项热库/冷历史 × 完整/执行副本 × 严格/普通读取回归在旧实现全部失败，修复后通过；同时覆盖显式 dirty 清空、空代码、已加载代码与别名隔离，既有副本及严格校验回归也通过。没有把空代码设为校验例外。该缺陷与现场形态一致，尚未重放该笔线上交易，不宣称解释所有历史拒绝。

## 成功发布后复用 manifest 校验结果

前一十分钟样本的 manifest decode cache 为 hits +430、misses +64、预算拒绝 0。发布者已经规范化、校验、排序并生成 JSON；新代发布后，首个 loader 原来还会重复 JSON 解码和元数据校验。profile 中该解码链占 4.17 CPU 秒。

现在仅在文件同步、原子 rename、目录同步全部成功后，把实际发布 bytes 与已验证对象放入既有单条有界缓存。每次 loader 仍读取当前完整文件并逐字节核对；外部替换、同 stat 改写、损坏或删除仍走原错误/解码路径。相同 bytes 的幂等发布保留已有独立条目。

缓存复制容器与字符串 backing，按原预算计费，保持生产格式校验错误和段文件认证的独立性。Retired 的 omitempty、Segments 的 nil/empty 与 JSON round trip 一致；非法 UTF-8 跳过预填充，由原 decoder 处理，包括替换字符产生重复路径的情况。无效缓存配置不能使已经完成的发布新增失败，loader 继续返回原配置错误。

新增 `state/snapshot/cold/manifest_cache/publication_seeds`、`publication_hits`、`publication_bypasses`。seed 不记作读取 hit；publication_hits 是所有命中发布来源条目的读取，不能直接当作“省掉的解码次数”，同一代原来也只有第一次 miss 需要解码。

同输入组件基准使用 15,534 个 active 与 35,532 个 retired 引用，实际执行校验、JSON、文件/目录 fsync、rename 及 ReadFile；不读取段数据。Go 1.25.5 / Darwin arm64 / M1 Max，3 组 × 5 次：legacy 196.105 ms/op → seeded 72.363 ms/op（−63.1%），151.20 → 100.67 MB/op，389,935 → 317,813 allocs/op，每次 decode 1 → 0。这是发布加首次读取链路的组件结果，不能当作节点同步提速比例。

最近一次部署前 CPU profile（15:46:52 起，30.19 秒，91.22 CPU 秒）中，LoadProductionManifest 为 1.95 CPU 秒，decodeOwned 1.68 秒、decodeManifest 1.46 秒、PublishManifest 0.69 秒；这些累计调用链相互嵌套，不能相加。原始第一次 profile 因网关传输超时保留为失败记录；重试仍只采样 30 秒，延长下载超时后完整成功，前后进程标识相同。

## 降级原因指标

`state/snapshot/cold/history/budget/` 新增 reason_bits、L0 值和阈值、debt_rises/good、sample_accepted、accepted_sequence 与 accepted_sample_unix_nano。原因 bit 固定如下：

| 位值 | 原因 |
| ---: | --- |
| 1 | 硬资源限制 |
| 2 | 新 write stall |
| 4 | 设备繁忙、队列与延迟同时达到压力条件 |
| 8 | L0 sublevels 压力 |
| 16 | 压缩债务连续上升 |
| 32 / 64 | 引擎不可用 / 观测过期或时间异常 |
| 128 | 良好恢复样本尚不足 |
| 256 | 原压力级别的恢复滞后 |
| 512 | 设备观测缺失，无法进入最高级别 |

`reason/<name>/entered` 只累计原因从不活跃到活跃的次数，不是刷新次数或持续时间。位可同时出现；采样点分布不等于连续时间占比。原 5 秒样本接受、压力和恢复条件保持原样。

## 深度 6/7 缓存来源

沿既有分支细分 probation、epoch、window sampling/capacity 以及容量淘汰，区分 foreground、prefetch、flush 实际授予的引用来源。计数不重建唯一 key 的访问轨迹；probation 为指纹观测，epoch 拒绝可含保守槽冲突。初次 window admission 不算 foreground 再读，引用来源组合也不是独立命中数。

本轮不新增淘汰历史键表、不放宽快照版本条件、不调整固定容量。新增计数用于判断下一步是否值得做有界 exact-key 再读距离抽样，不能据 no_resident 总量直接判定缓存太小。

复用既有 atomic 字段的空闲高位记录来源与深度，entry 大小仍为 80 字节。owner 累计 Gauge 在 clear 后保留，owner 重建可归零；发布端在每 64 次 Close 的既有节奏串行采样，忙时跳过。

Go 1.25.5、GOMAXPROCS=1、交错计时中，普通 Get 14.29 → 15.61 ns（+1.32 ns），scoped snapshot view 21.41 → 21.13 ns；prefetch/flush 变化小于 1%。这些成功路径均为 0 B/op、0 allocs/op。真实 Close 基准包含每 64 次发布，1 reader 178.7 → 200.4 ns，65 readers 3584 → 3445 ns，分配不变。该短微基准显示有小幅局部成本，不宣称绝对零开销或线上等比例变化。

## 验证与发布

候选源码固定为 `500b61273cbf0d175624cb1ff114b39ba409a37a`。全仓运行除一项依赖后台就绪时机的异步 VM 测试外全部通过；该测试现改用 testing/synctest 与既有 trace 回调确定等待，删除 256 笔无关延时交易，保留全部发布、oracle、receipt 与 root 断言。GOMAXPROCS=1/2 各 100 次和 race 20 次通过；最终 core 全包已通过（150.890 秒）。状态副本 race、manifest race、blockbuffer race 已通过，新增 lint 为 0。服务器已于 15:57:39 完成原生准备：Go 1.25.5 / Linux amd64 / CGO_ENABLED=1 / tags=sapling，六包定向测试均通过，Sapling 可用性及 Pedersen probe 通过。源码固定 `500b61273cbf0d175624cb1ff114b39ba409a37a`，脚本提交 `71bcf9d34cc9b6f4d4ce7f06223c12f400e53dd1`，脚本 SHA-256 `9707c0c1394edf5ee0f86b81cb1618b1718e246c2ed28269c97a884402ab0560`；候选 binary SHA-256 `134ea2b22c83a0f0ea5d2d77e1784ead27c556a0fa9f97e2c2c9880c9a19ba87`。GitHub [PR #3](https://github.com/yanghang8612/go-tron/pull/3) 待审查，本轮不绕过主分支审查规则。服务于 15:59:57 启动新进程 PID 22527，start_ticks `4476169123`，metrics identity `1789027197720109146`；运行 binary 与候选 SHA-256 完全一致，激活记录为 active，初始健康检测 head 22,651,576→22,651,589。运行路径为 `/data/gtron/releases/20260910-history-metadata/gtron`。保护状态和除既有 ExecStart 路径外的配置对比通过。

切换窗口保留 25 组请求证据：两类请求各 5 次 502，最后旧成功请求起点为 15:59:43.531，首个新成功请求起点为 16:00:07.737；这些请求起点界限不能当作精确服务停机时长。metrics 在 16:01:48.967 另有一次下载超时（已收到 133,816 bytes），Wallet 同期成功，未将其并入 502 窗口。

固定账户在区块 2043/2044/2045 的哈希与余额于部署前 15:41:15 和部署后 16:02:23 两次校验均一致，每次 12 次只读请求。该探针只覆盖一个账户的历史余额，不代表全库或所有历史域。后续十分钟稳态采样单独保存为 after，不与重启 Counter 混算。

原始初始采样、哈希事件字段转录、修复前失败回归与修复后结果保存在 `build/benchmarks/20260910-history-metadata/`。manifest 同输入基准在 `build/benchmarks/20260910-manifest-publication-cache/`。服务器身份已确认：原 binary 为 `/data/gtron/releases/20260910-history-readiness/gtron`，SHA-256 `c84fee73d08425cdf189467129e9382f76fddb0f733fa788d1dc44dd23ac1915`，监听端口与空间保护正常。


## 上线后十分钟结果

稳定窗口为北京时间 16:02:09.571–16:12:07.252，实际跨度 597.674 秒，38/41 份 metrics 有效、41/41 份 Wallet 成功；metrics-023、038、039 因 15 秒传输超时排除，原始部分响应与错误保留。有效点最大间隔 43.064 秒，全窗精确进程 identity 一致，已检查的累计计数无回退；起终点均有效，因此区块增量仍可计算。Gauge 的期间峰值和原因点数不能覆盖缺失时段。

| 指标 | 部署前五分钟 | 部署后十分钟 | 部署后后半段 |
| --- | ---: | ---: | ---: |
| head 块/秒 | 33.628 | 28.055 | 27.515 |
| 状态发布/裁剪 块/秒 | 24.585 | 8.600 | 17.218 |
| head-state 差距变化 | +2,715 | +11,628 | +3,074 |

结束时 head=22,669,786，状态发布与裁剪均为 22,078,751，差距 591,035 块，其中超过 65,536 常规窗口的距离为 525,499 块。body/index 均为 22,020,096，本窗口未跨归档段，距 head 649,690 块；这些层次有重叠，不能相加为总积压。末次 Wallet 仍处于深度同步，29 peers、8 sync peers、bufferedBlocks=2,466、fetchBackpressured=false，距 peer head 约 6,345 万块。

不能宣称已经实现净追赶，也不能把块速下降直接归因为本轮修改：Wallet 同进程窗口的交易密度由 52.736 升至 66.193 笔/块（+25.5%），TPS 1,778.34→1,850.69（+4.1%）。平均 CPU 3.332→2.931 核，每块 CPU 99.083→104.467 ms，每块分配 8.690→6.657 MB；负载、归档活跃阶段和重启后的缓存状态均不同，这些不是受控 A/B 效果。

代码哈希拒绝、strict error、存储错误计数在稳定窗口均无新增，进程末值的 hash rejection/strict error 仍为 0。预执行 shadow 错误仍有增长：VM sender-chain 总错误 +26，其 readiness/result/apply_unsupported 子项分别 +18/+5/+3；这些为嵌套预执行结果，不能与 sender-chain +11 再相加，也不能当作 canonical 区块执行失败总数。

## 归档暂停的实际原因

前半段并非死锁：唯一冷归档生命周期 goroutine 位于 `SnapshotLifecycle.loop` 的 select 等待；预算接受序列约每分钟更新，随后恢复发布也与原有门槛精确吻合。

1. `maxDeferredColdHistoryBlocks` 与 `maxBusyDeferredColdHistoryBlocks` 将 busy 水位设为 65,536×4×2=524,288 块。`SyncBuildReady=false` 且可归档 readyBlocks 不超过此值时，runner 返回 HistoryDeferred，由生命周期分钟 ticker 再检查。`parallel/runtime/ready=1` 只代表资源就绪，不能替代 importer 的 admission ready；本窗口 importer admission 持续 busy。
2. 初始归档已在同步繁忙门槛接管前推进至 22,073,611。after0 的可归档 lag 为 512,554，低于 busy 水位，因此前几分钟同步继续、归档暂缓是原策略。
3. 首次恢复点 after25：当时 eligible cutoff=22,597,990，减去旧发布水位 22,073,611，得到 524,379，刚超过 524,288；forced-busy 随即发布 2,853 块至 22,076,464。after33 再发布 2,287 块，窗口总计 5,140。
4. 恢复还受动态预算限制。after26 的实际预算为 level1、20% duty、reason_bits=16（压缩债务连续增长），recovery_cost=6.987318069 秒，恢复时间 27.949272276 秒，精确等于 cost×(1−0.2)/0.2。这里是 throughput 动态预算公式，不能误称额外固定 20% 下限；后续观测即使恢复为 80%，也不会追溯缩短已经安装的 deadline。

`forced_busy/last/debt_blocks` 是那批发布后的 lag，不能拿它与水位比较而断言“未达门槛却启动”。完整请求边界、原始文件哈希、源码位置与算术见 `build/benchmarks/20260910-history-metadata/scheduling-analysis.{json,md}`。

## CPU profile 的解释边界

上线后 profile 为 16:05:19 起 30.14 秒、76.58 CPU 秒，前后 identity 一致。Commitment pipeline 去重范围 32.40 CPU 秒（42.31%）、Pebble 去重范围 30.82 秒（40.25%）、前台 apply 20.96 秒（27.37%），它们相互嵌套，不能相加。durable commitment read 为 16.88 秒（22.04%），点读取仍是重要成本。

该 profile 正好处于冷归档暂缓期，SnapshotLifecycle、manifest load/publish/decode/seed 均没有 CPU 样本；前台写热 history rows 仍有 0.60 秒。故“decode=0”及总 CPU 下降不能作为 manifest 优化的线上收益证明。可确认的是同输入组件基准改善，以及新版本真实发布后的 seed 命中；要量化生产节省，应对相同归档阶段和处理量再做对照。


## 新增诊断结果与下一步

预算原因 entered 增量：debt_growth +2、recovery_samples +2、pressure_hysteresis +1；硬限制、write stall、device/L0 压力、engine unknown/stale 均无新增。38 个有效点中 reason16 为6点、reason128为27点、reason384为4点、无原因1点，这是观测点分布，不是时间占比。L0 sublevels 为3–5，低于8的compaction阈值；观测的压缩债务约1.84–4.99 GiB。accepted age 中位30.86秒、最大58.18秒，接受序列 +12，主要按分钟刷新；不能把保留值解释成实时设备压力。

Manifest 在十分钟内 seeds +4、publication_hits +45、decode misses +0、预算拒绝 +0，末值 resident 102.48 MiB / 256 MiB。末值 miss=1 来自启动期。真实四次预填充后读路径继续命中，但45次命中不能当作省掉45次解码。

深度6/7的62个owner累计Gauge逐点均无回退，事件摘要如下。来源集合记录驻留期间的引用，不是命中率或唯一key数。

| 事件 | 深度6 | 深度7 |
| --- | ---: | ---: |
| window 晋升次数 | 1,156,313 | 772,866 |
| 晋升中含 foreground reference | 0.6103% | 0.8633% |
| tail 容量淘汰次数 | 3,868,788 | 2,386,381 |
| tail 淘汰中含 foreground reference | 37.0494% | 5.6295% |
| foreground no_resident 次数 | 2,907,163 | 2,442,145 |

深度7晋升的99.1367%为flush-only，预取相关计数为0；深度6晋升中flush-only 652,123、prefetch-only 329,363、prefetch+flush 167,770。两深度实际 probation、epoch、window-capacity 准入拒绝均为0，window sampling拒绝分别869/724。准入拒绝为0与驻留后容量淘汰很多并不矛盾；含foreground历史的淘汰也不能证明未来必然重读。

后续优先顺序：

1. 对 importer-busy 准入、水位与动态恢复安排做同输入回放，评估在持续同步中允许有界后台工作能否减少周期性积压；保留存储/内存保护，以前台TPS、净归档速度和压缩债务共同验收。
2. 在固定缓存预算与版本规则下，针对深度7的flush主导晋升、深度6曾被前台读取的tail淘汰做有界key再读距离抽样及回放，再决定调整晋升保护，现有计数不足以直接支持扩容。
3. 在有真实冷归档发布的阶段量化manifest节省，按发布量与元数据规模归一化；当前CPU profile不承担这项因果结论。

最终现场核验保持PID22527及同一binary，空间保护通过，/data可用约2.50 TB，MemoryLimit仍40 GiB，P2P/API/pprof/metrics监听符合既有布局。原始采样、失败响应、所有分析及验证日志保存在 `build/benchmarks/20260910-history-metadata/`，服务器原生与激活证据保存在本轮release目录。
