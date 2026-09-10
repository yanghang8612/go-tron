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

候选源码固定为 `500b61273cbf0d175624cb1ff114b39ba409a37a`。全仓运行除一项依赖后台就绪时机的异步 VM 测试外全部通过；该测试现改用 testing/synctest 与既有 trace 回调确定等待，删除 256 笔无关延时交易，保留全部发布、oracle、receipt 与 root 断言。GOMAXPROCS=1/2 各 100 次和 race 20 次通过；最终 core 全包正在复验。状态副本 race、manifest race、blockbuffer race 已通过，新增 lint 为 0。服务器原生构建及同进程复采结果将在后续补录，线上当前仍为 `19eda11f`。

原始初始采样、哈希事件字段转录、修复前失败回归与修复后结果保存在 `build/benchmarks/20260910-history-metadata/`。manifest 同输入基准在 `build/benchmarks/20260910-manifest-publication-cache/`。服务器身份已确认：原 binary 为 `/data/gtron/releases/20260910-history-readiness/gtron`，SHA-256 `c84fee73d08425cdf189467129e9382f76fddb0f733fa788d1dc44dd23ac1915`，监听端口与空间保护正常。
