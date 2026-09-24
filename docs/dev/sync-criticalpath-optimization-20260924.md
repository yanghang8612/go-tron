# 同步导入关键路径优化（2026-09-24，本地候选）

本轮从同一主网进程 `gtron.service` PID 32506、源码 `3994faa58e16` 的只读采样出发。六分钟内导入 8,270 块、753,603 笔交易，前台 `exec_busy_ratio` 中位数为 99.53%，但进程平均仅用 2.88 个 CPU 核。三秒运行时 trace 中，主导入 goroutine 有 17.88% 墙时等候 `bufferBatch.writeFiltered` 的 `flushMu`，持锁 flush worker 在对应两段时间的 99.90% 处于 Go `Running` 状态，栈落在层合并、排序、Pebble memtable 插入和 base-read cache promotion。另有 20.39% 主导入墙时在 SST `pread` 系统调用。这说明前台主要受状态执行及 flush 锁串行化限制，同步读延迟也是显著次因；不等于整机 CPU 或 NVMe 已经耗尽。

本地候选有两处：

1. `core/blockbuffer/buffer.go` 的 `FlushUpTo` 在 `b.mu.Lock` 下标记本次选定的 committed prefix，随后仍持有 `flushMu` 完成原有 Pebble handoff。层合并、排序、提交和缓存 promotion 在旧版就已经位于 `b.mu` 临界区之外；本次解除的是 `writeFiltered` 对全程 `flushMu` 的依赖。它改为在 `b.mu.RLock` 下预检实际要写的目标；命中被标记的层时才在锁外等待，并在唤醒后沿原有 strict/drop-stale 规则重新判定。读快照的 base sequence 与层拓扑仍由 `flushMu` 保护，没有增加无界队列或新的常驻缓存。
2. `core/rawdb/accessors_state_kv.go` 的结构化 `ReadStateKVLatestNoCopy` 只在无法分类的错误路径构造物理 key 和诊断字符串；命中及原生已分类缺失直接返回。`verifyStateReadMiss` 的 Has 复核及错误文本保持，非结构化 reader 的路径不变，借用值只供立即解码。生产 CPU profile 将该函数无条件格式化行计为 0.67 CPU 秒／30 秒，约为主导入 CPU 样本的 3.8%；改动不试图消除 `pread` 本身。

固定输入、Go 1.25.5、GOMAXPROCS=2、CGO=1 的本机对照中，真实 Pebble 并发 flush 基准的前台 `WriteUpTo(4)` 延迟五轮中位数为 **62.383 → 1.307 ms**；完整 flush 完成时间约 **65.33 → 62.67 ms**。这说明前台等待从持锁重活中解耦，不能表述为整链快约 48 倍。无 flush 的 80,000 操作基准为 2.587 → 2.513 ms，在本地波动范围内。结构化状态读的 mock accessor 基准命中为 **258.1 → 34.39 ns/op**、258 B/6 alloc → 48 B/1 alloc；已分类缺失为 **298.1 → 39.61 ns/op**、354 B/7 alloc → 48 B/1 alloc。它只度量 accessor 固定开销，不能代替 Pebble/SLOAD 或在线吞吐对照。

正确性边界集中在 flush 已选前缀、后续新层、旧 pinned 快照、部分写入失败及错误后的缓存失效；blockbuffer 的真实 Pebble 测试和 race 定向测试覆盖这些情况。状态读测试覆盖 borrowed hit、classified miss、Get/Has 错误、错误文本及损坏 envelope。Go 1.25.5、GOMAXPROCS=2、CGO=1 的集成验证中，`go vet ./core/blockbuffer ./core/rawdb ./core/state/... ./core`、生产 `make gtron`（`sapling`、CGO=1）和 41 个跨层定向 race 测试均通过；`-v` 日志确认四个新 `TestFlush*` 回归实际运行。`go test ./... -count=1 -timeout=300s` **exit 1**，唯一失败为 `core/TestProcessBlockPublishesAsyncSenderRetryOnOrdinaryBlock`（`async retry publication candidates = 0, want 1`）；其它完成包通过。独立审计中，当前代码与 `3994faa` 双文件基线 overlay 在隔离的 2P 测试里各通过 100 次及 1,000 次；诊断性 1P 测试中两版各 100 次均因同一断言失败。覆盖率确认该测试不执行本轮 blockbuffer 变更路径，rawdb accessor 只走未改的 fallback。因此它属于已有的调度敏感测试限制；原集成负载下的 2P 基线失败未复现，不能写成已复现。全仓测试仍记录为失败，不修改生产等待策略以迁就该断言。原始输出、每条退出码、时间、构建信息及失败归因均在 `build/benchmarks/20260924-sync-criticalpath-integrated/`。本轮没有部署，也没有做相同输入的整链重放或上线 A/B；上线表现尚未验证。

没有扩大缓存或改冷数据并行读取的内存准入。现有 20 GiB 服务 cgroup 在独立主机观察时余量仅 0.24–0.33 GiB、`failcnt` 两分钟增加 170,770；HTTP 窗口的并行读取探针余量也只有 0–950 MB。虽然 SST ReadAt 量大，但其中可能命中内核 page cache，当前证据不能证明增加缓存会消除重复物理读。扩大常驻内存还会直接挤占服务 cgroup 余量。

原始与分析证据：`build/benchmarks/20260924-resource-bottleneck/README.md`、`trace-analysis.md`、`server/README.md`；两处优化的本机对照及测试：`build/benchmarks/20260924-flush-lock-optimization/README.md`、`build/benchmarks/20260924-state-read-optimization/README.md`。这些本地 `build/` 目录是 ignored 证据，不包含在源码提交中。
