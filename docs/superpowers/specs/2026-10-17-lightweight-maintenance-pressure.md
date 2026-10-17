# 冷化维护的轻量 Pebble 压力采样

2026-10-17，基于 `4967120062fb5b8762d9cfec0c7229b75a306d32`。本地实现及针对性测试完成，独立审计与服务器原生全套验收属于后续流程。证据在 `build/benchmarks/20261017-lightweight-maintenance-pressure/`。

## 已观察到的依赖

线上 `20261017-cold-selector/long-tail-before/tail-2-goroutine.raw` 的第二次抓栈中，冷化 goroutine531 的调用链为 `RLock → cache.Cache.Metrics → DB.Metrics → pebbledb.MaintenancePressure → DirectHistoryStore → runtimeHistoryLoadProbe.sample → historyLoadPermitsMerge → compactHistory`；同一快照的 commitment prefetch goroutine2515 处于 `runHandCold → evict → Set`。这是确切的瞬时缓存锁等待依赖，不能把完整约49秒离散发布平段全归于此调用链，也不能由此估算消除后的整节点吞吐。

维护决策只消费 L0 sublevels、memtable count 和 estimated compaction debt。原路径却为这些字段收集完整 Metrics，包括 cache、table cache、log、table metadata 和 object-provider 统计。缓存填充可因此挡住冷化读取当前压力。

## 实现与安全边界

新增公开值类型 `pebble.MaintenanceMetrics` 及 `(*DB).MaintenanceMetrics()`，只在 `d.mu` 内读取三项，与完整 Metrics 的来源一致：

| 输出 | 来源 |
|---|---|
| `L0Sublevels int32` | `d.mu.versions.metrics.Levels[0].Sublevels` |
| `MemTableCount int64` | `int64(len(d.mu.mem.queue))` |
| `CompactionDebt uint64` | `d.mu.versions.picker.estimatedCompactionDebt(0)` |

债务估计沿现有 picker 计算，遍历固定的 levels；不读取 SST 数据或遍历额外 table properties。方法不访问 opts/cache/table cache/log/object provider。完整 `DB.Metrics()` 的公共行为继续由原实现提供。

`pebbledb.MaintenancePressure` 仅替换字段来源。它继续在 `quitLock.RLock` 生命周期租约内检查 nil/closed，采样前记录 `SampledAt`，在引擎采样前后观察 `writeStalled`，随后读取累计 stall count/duration，并返回实际打开时的阈值。Close 仍与采样互斥；引擎字段和 atomic stall 观察仍不是跨子系统事务快照。

本轮没有缓存旧 pressure、改采样时间戳或削弱 fresh/stall/debt/device 保护。DirectHistoryStore 合并及维护门控、阈值和择源策略继续消费同一三字段。没有新增长期热点 atomic、cache metrics 计数器或生产测试 hook。新方法仍可能等待 DB.mu；其价值是去除这里已抓到且不必需的 cache 锁依赖，不能保证所有维护长尾消失。

## 正确性验证

全部本地命令使用 Go1.25.5、darwin/arm64、CGO_ENABLED=1。这里没有进行 Linux AMD64 或 Sapling native full-suite 验收。

- Pebble 真实内存文件系统测试经过 pending memtable、flush、compact、四次 overlapping flush、非零 compaction debt 与再次 compact，在静止阶段独立调用完整 Metrics 比较全部三项。并发 writes/flush 与轻量采样通过针对性 race。
- 一个只提供 versions 中的 L0 字段、flushableList 长度和最小 picker 的未打开 receiver，没有 opts/cache/table cache/manifest/object provider/WAL 或实际 memtable 对象，仍能调用轻量方法，且 `AllocsPerRun` 为零。该测试证明其依赖边界，不操纵 live mutex，不模拟49秒等待。
- wrapper 既有 nil/closed、opened thresholds、stall 生命周期、并发 Close 测试，加上新的 flush/compaction 后实时字段与时间边界测试通过；Pebble 与 wrapper 两包的针对性 race 均通过。

具体命令与结果在 `commands.json`、`wrapper-tests.txt`、`race.txt`。未将定向测试冒充全包或原生验收。

## 成本实验与必须保留的负面

基准用64个实际写入/flush形成的互不重叠 SST、4 MiB逻辑值、128 KiB block cache、4个 Get worker 和内存文件系统。固定 key/value 生成器及每 worker PRNG 配方；`b.RunParallel` 的 worker 消费长度与交错是动态的。每独立进程的 `cache.robin_hood.hashSeed` 仍来自 `time.Now`，依赖包的测试 init 不参与当前 Pebble 测试 binary，因而冻结 source 不等于冻结 hash seed 或桶布局。不能称这些并发 case 具有逐操作完全相同输入流。

首轮在同一 Go benchmark 进程中依次运行所有模式，未显式等待初始异步 table stats，也未排除其他本机测试活动。结果显示采样 API 局部成本下降，但 foreground control 的 light median1709 ns/read，full1425，none1359，light 对 full 约 **+19.93%**。该负面完整保留在 `benchmark.txt`；不能断言它由异步统计、热顺序或其他进程造成。

| 首轮单次采样成本，ns/call | full min/median/max | light min/median/max |
|---|---:|---:|
| 无并发 Get | 913.3 / 949.5 / 968.5 | 13.58 / 13.69 / 13.74 |
| 四个并发 read-fill worker | 1937 / 2074 / 2327 | 16.03 / 16.33 / 16.41 |

首轮无并发 Get 时，完整采样3528 B、9 allocs/call，轻量0 B、0 alloc/call。read-fill 模式的 B/op 和 allocs/op 包含后台 Get 分配，不能全部归于采样 API。饱和 sampler 的后台 reads/s 不代表生产前台吞吐。

因首轮负面，唯一复核为5轮串行、交替顺序、独立进程的 foreground control：每 case 总计300000 reads，`-cpu=4`；等待当时 `tableStats.loading` 为真时的工作结束，并做4096次固定生成器 warmup，均在 timer 外。cache 实现 source 用 HEAD overlay 冻结；其运行时 hash seed、worker 交错及消费前缀仍未固定。root 协调该复核期间无其他本地 CPU benchmark。原始命令、时刻与日志在 `foreground-runs.json`。

| 复核 foreground，ns/read | min | median | max |
|---|---:|---:|---:|
| none | 1314 | 1355 | 1378 |
| full | 1334 | 1382 | 1534 |
| light | 1373 | 1433 | 1602 |

light median 对 full 仍 **+3.69%**，对 none **+5.76%**，原因未证实。该控制将采样同步插入每一个 foreground worker 的每128次读取中，频率高于生产，且实际冷化 sampler 位于独立 goroutine。不能据此宣称实际前台回退已消除，也不能用局部13–16 ns/call掩盖这个负面。没有追加重复来筛出更好的结果。

选择此候选的依据是三个压力字段保持同源、生命周期安全边界不变，并明确去掉不必要且已有瞬时抓栈证据的 cache 锁依赖；本地数据只支持采样 API 局部成本下降，未证明前台或整节点提速。后续线上验收需要分别观察维护长尾、导入/冷化速率、memory/stall/debt 和相同运行身份；不承诺49秒平段归零。

## 冻结

`summary.json` 保存全部首轮、复核样本和 min/median/max，`frozen-sha256.json` 包含实现、测试、spec、脚本、日志与 overlays 的 SHA。首轮测试源码按仅移除后续 warmup/失败清理修补的配方保留为 reconstructed source，精确复核时的 bench source 单独保存；这两项在 metadata 中区别标记。未提交、推送或部署。
