# Commitment 深层分支缓存容量

2026-10-14。基于 `ebe93b33e31b21cb0feb66922d3b7b602aeaa2b3`。

## 目标与预算

线上同为 512 MiB base cache，18M 至 23M 的每更新 parent durable get
从约 0.116 增至 0.517，而单次 get 约 227→214µs、每更新 node hash 和
fold rounds 基本不变。深度 6/7 miss 较高，首先增加有界驻留容量，减少重复
持久读取和填充的分配；本轮不增加 owner-local decoded cache。

新增 tail 预算与 base 共用原 entry map、CLOCK、namespace 软借用和生命周期。
**新增字节不是深层分支独占的硬分区**：原有 other 类仍可借用未使用的容量，
拥挤时按原 namespace reservation 回收。trunk 放不下的 shallow branch 也仍可
按原规则进入普通 CLOCK。此名称表示增加普通 commitment tail 的可用空间，
不表示新增一种 entry、队列或物理 keyspace。

所有 reservation、probation 和 invalidation metadata 按原 base 预算计算。
base512 MiB 加 tail512 MiB 时，resident retained-charge 总上限为 1024 MiB，
other128 MiB、trunk64 MiB、window8 MiB 不变。每 shard 记录 additionalTail，
idle 私有 value backing 仍限制在 `(limit-additionalTail)/8`；扩容不会把其
上限变成总 resident budget 的八分之一。ownership 测试使用同一严格上限。

retained charge 是缓存预算，并非精确 Go heap/RSS。新增每 shard 一个 int；
更多 resident entries 会增加 map、queue、entry slab 和 GC 成本。其额外
metadata、allocator/native retention 和 RSS 必须独立观测，不能宣称只增加
tail payload，或将预算直接等同于真实进程内存。

## 配置与兼容

`--state.commitment.cache.tail` 和标准 urfave/cli `EnvVars`
`GTRON_STATE_COMMITMENT_CACHE_TAIL` 均以 MiB 为单位，默认 0。
有效环境值可被显式 CLI 覆盖，包括 CLI 0；格式错误由 CLI 框架拒绝。
负值、base 关闭时配置正 tail、合计字节溢出均拒绝。旧配置零 tail 继续执行
原预算策略。Buffer 配置接口保持 disabled base 语义，忽略非法 tail 字节预算。

## 不可改变的边界

不改持久编码、keyspace、root、共识或数据库内容格式，不需要删库。所有读写
继续使用现有实现：overlay 优先、per-key epoch、全局 snapshot-version fill
guard、旧 snapshot 隔离、canonical flush 准入、完整 physical generation key、
rewind/clear、range-delete invalidation、tombstone、missing/空值/error 语义均保留。
旧 snapshot 的首次 probe 即使发生在新 flush 后，也不能回填旧值至新 cache。

保留直接 Get 已外泄字节、callback 锁内借用、cursor 与 shared flight 的字节
生命周期；exposed backing 不能原位覆盖或回收到 private pool。不池化 live arena，
不吞掉存储或 callback 错误，不因 flush-only source 比例较高删除 flush 准入，
不因容量调整关闭已有高 useful 比例的预取。cache hit 仍执行原 BranchData decode。

## 固定输入验证与限制

记录位于 `build/benchmarks/20261014-deep-cache/`。baseline 是 isolated
`git archive ebe93b33`，仅复制相同 trace/bench 与零 tail 编译 forwarding shim，
不包含本轮 production changes；仅运行 baseline tail0。

真实 Pebble session 语料覆盖深度6/7 legacy/delta、hot/cold、scan、delayed read
及保留预取的读→写→commit→canonical flush 八块操作。完整 pipeline 语料为
1536 个合法固定聚簇键，每 operation 从相同 durable rows 开始，执行八块
ordered fold→commit→flush→close，每块 root 与独立顺序内存 oracle 相等。
它使用64-partition constructor、默认32 read permits、prefetch depth5、overlap0；
聚簇键只有一个活跃 partition，按块等待并 flush，不能代表生产64-owner并发吞吐。

每 case 五次重复，记录真实 durable calls、深6/7 calls、B/op、allocs/op、完整
operation 时间及范围。flush map 遍历可轻微改变 CLOCK 入队次序，读数也保留范围。
本地小语料主要驻留 RAM，没有复现生产约214µs的 durable get 成本。
容量增加降低了真实读取和部分分配，但完整 pipeline 时间没有单调改善；
cold scan 控制还可能多付 metadata allocation。不能只用 helper 微基准，或将
本地读数比例、RSS峰值及时间差外推为整节点提速或生产内存比例。

扩容 snapshot guards 与 legacy/delta/generic prefetch/direct borrowed bytes/flush
oracle、blockbuffer/domains 完整包、CLI 解析及 targeted race 通过后才进入部署验收。
原生 Sapling、独立最终审计及线上资源验收仍由部署流程完成。

## 首次生产验收

先使用 base512 MiB 加 tail512 MiB，保留并发、GC、内存限制及其他生产参数。
环境变量可随现有 memory drop-in 设置，旧 binary 忽略它，避免回滚 executable
收到不认识的新 CLI flag。验证实际运行环境、startup 日志及 cache capacity/
reservation，采集深6/7及总 durable gets/更新、prefetch useful、完整 fold/执行时间、
Go heap/GC、native allocator 与 OS RSS，再决定是否提高到 tail1024 MiB。
保持原 executable 与精确服务配置可回滚；本地固定输入结论与线上结果分开报告。
