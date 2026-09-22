# 同步期间两处存储分配优化（2026-09-22）

本轮针对主网同步后部署样本中两条可定位的分配栈做局部优化：transaction index 每次发布都重新打开全部旧 run；snapshot manifest 命中解码缓存时仍由 `os.ReadFile` 分配整份 JSON。现场 30 秒分配 profile 分别记录 `OpenTransactionIndexRun` 770.95 MiB flat、`snapshots.loadManifestFile` 321.68 MiB（包含于 `os.readFileContents` 322.43 MiB flat）；这里沿用 `go tool pprof` 显示的二进制单位。这些是采样窗口中的分配归因，不是本轮预计省下的字节数；原始 profile 与同步、GC、Pebble 背景在 `build/benchmarks/20260922-postdeploy-bottleneck/`。

## 最终行为与数据边界

`Freezer.PublishTransactionIndexRun` 先读取并校验完整持久 manifest 声明与当前已选中 run。匹配时沿用旧的不可变 run 句柄和已解析目录，只对新 run 保留完整验证、原子 manifest 发布和一次打开，并在 `v2Mu` 写锁下同时发布新 run 与 coverage。旧读者的查找持有该锁的读侧，发布前后只会看见完整版本。若 live view 与 manifest 不符（包括前次 manifest 已持久化但进程尚未安装新 run），回到完整重开与校验；恢复出的 coverage 不得超过 V2 coverage。已有 run 的桶校验、全哈希核验及 immutable-run 约束没有放宽。该复用路径假定已安装 run 不会被进程外原位改写；发布时校验完整 manifest 声明，但不会重新读取每个已验证 run 的内容。

`LoadProductionManifest` 对缓存中至少 64 KiB 的 JSON 用至多 64 KiB 的临时块重新读取当前文件直至 EOF，并逐字节比较。只有完整相等且比较后仍指向同一缓存对象时才返回独立克隆；字节不同、增尾、截短、同长度原位/原子替换及读取错误沿用当前文件的解码/验证或报错路径。流式 miss 将本次实际观察到的字节组装后走原有校验；`Stat` 仅给分配容量提供提示，不参与命中判断。较小文件仍走原来的 `ReadFile` 路径。缓存预算、resident charge、返回对象的独立所有权及 segment 文件验证均保持原有规则；比较缓冲区只活到本次 load 结束，不进入常驻池。

## 可复现局部证据

本机 Apple M1 Max、Darwin arm64，固定 `GOTOOLCHAIN=go1.25.5 GOMAXPROCS=2 CGO_ENABLED=1`。transaction index 配对基准由同一测试文件提供旧发布流程的 `full_reopen_reference` 和新 `incremental`，各在四个已发布 run 后发布第五个；每个 run 用 20-bit prefix，目录为 8,388,616 字节。fixture 构造及每轮回滚 manifest/live view 均在计时外，两个变体都包含新 run 的完整验证和持久发布。命令：

```sh
GOTOOLCHAIN=go1.25.5 GOMAXPROCS=2 CGO_ENABLED=1 go test ./core/rawdb/freezer -run '^$' -bench '^BenchmarkFreezerPublishTransactionIndexRunWithExistingRuns$' -benchtime=5x -count=3 -benchmem -timeout=600s
```

三轮中位数为旧流程 **167,961,672 B/op、49.61 ms/op**，增量流程 **33,599,366 B/op、20.99 ms/op**；本地 publish fixture 的分配减少约 80.0%。第一次计时器设置有误的 `paired-bench.log` 已作废，只采用修正后 `build/benchmarks/20260922-txindex-incremental/paired-bench-corrected.log`。固定 20-bit 目录不代表生产中的自适应 run 尺寸，这也不是钱包查询或整链导入基准。

manifest 配对基准调用完整公开的 `LoadProductionManifest`，旧流程是测试文件内冻结的 `ReadFile` loader。每种大小与工作负载各用独立 fixture；`-benchtime=3x -count=3 -benchmem`，报告三轮中位数。复现命令：

```sh
GOTOOLCHAIN=go1.25.5 GOMAXPROCS=2 CGO_ENABLED=1 go test ./core/state/snapshots -run '^$' -bench '^BenchmarkManifestStream(Lifecycle|Growth)$' -benchtime=3x -count=3 -benchmem -timeout=300s
```

14,089,025 字节的生产数量级 fixture 在**暖读**时从 **20,489,016 降至 6,464,104 B/op**（约 −68.5%）；11,120,279 字节 fixture 从 16,335,640 降至 5,276,232 B/op（约 −67.7%）。17,228 字节小文件仍为 28,952 B/op。大文件初次 miss 两侧均为 76,458,072 B/op；同长度变更与从 200 增至 8,000 个引用的增长循环基本持平，流式 miss 约多一次 64 KiB 临时缓冲区。14.09 MB 暖读的 resident charge 与 JSON 容量在两侧同为 57,519,104 与 14,089,026 字节，未测进程 RSS。

manifest 暖读三轮中位耗时在 14.09 MB fixture 为 3.19→2.25 ms/op，11.12 MB 为 3.24→1.36 ms/op；首次 miss 约 122.27→122.72 ms/op，增长循环约 162.016→162.023 ms/op。每轮仅 3 次操作，时间只描述这个本机 fixture，不能外推同步吞吐。17,228 字节小文件另用 `-benchtime=500ms -count=5` 补测，每轮约 29–30 千次操作：旧流程 19,747 ns/op，新流程 20,151 ns/op（约 +2.0%），两侧均为 28,952 B/op、11 allocs/op。小文件保留 `ReadFile` 路径；其公开调用仍有缓存指针读取与阈值判断。较长补测未复现原 3x 基准的大差异；中位耗时约 +2.0%，分配相同。完整 fixture、环境、分组（暖读、首次 miss、同长度跨目录、同 inode 覆写、发布后读取和增长）及原始输出在 `build/benchmarks/20260922-manifest-read-allocation/`，小文件补测为 `small-warm-long.txt`。

commitment 分支编码也做了有界原型：layout 复用 path mask、按精确大小直接写入，但完整 ordered pipeline 的四块变化、均匀/偏斜 64 分区基准未显示可归因收益，局部 codec 测试还退化；原型已撤销。`apply` 当前按共享前缀的已排序组一次下沉，变化父分支在组处理后哈希一次。主要编码 arena 是 blockbuffer immutable layer 与旧读者持有的最终字节，不能在单次 flush 后归池；跨块延迟编码必须先证明 `ValueAt(solidifiedCutoff)`、删除/unwind 与并行 lane 的版本语义。负收益数据在 `build/benchmarks/20260922-commitment-branch-followup/`。

## 正确性与验证

新增 freezer 测试覆盖旧 run 句柄复用、坏新 run 拒绝、持久 manifest 先于 live 安装的恢复、并发读者、V2 上界及 manifest 元数据变更。新增 manifest 测试覆盖完整字节比较、尾部增删、同长度替换、EOF/读错误、并发发布跨越 64 KiB 阈值、缓存预算和独立克隆；现有 compact/recovery 与 manifest 验证测试保持覆盖。组合验证使用同一 `GOTOOLCHAIN=go1.25.5 GOMAXPROCS=2 CGO_ENABLED=1` 配置，日志和各命令退出码记录在 `build/benchmarks/20260922-storage-allocation-integrated/`。

| 本地验证 | 结果 | 证据 |
| --- | --- | --- |
| `go test ./... -count=1 -timeout 300s` | PASS，退出码 0 | `fulltest.log`、`fulltest.exit` |
| `go test -race ./core/rawdb/freezer ./core/freezer ./core/state/snapshots -run '^(TestFreezerPublishTransactionIndexRun\|TestCompactTransactionIndexTail\|TestTransactionIndexOrphanCleanupRecoversPublishedMergeCrash\|TestFreezerDirectV2RecoversManifestPublishedBeforeLiveInstall\|TestFreezerDirectV2RecoveryReportsPublishedTransactionIndex\|TestTransactionIndexReplayPublicationFailureAndCanceledPruneRecover\|TestManifestStreaming\|TestManifestCacheAuthenticatesCurrentBytes\|TestManifestCacheBudgetAndDisable\|TestManifestCacheConcurrentPublication\|TestManifestPublication)' -count=1 -timeout 300s -v` | PASS，退出码 0 | `race.log`、`race.exit` |
| `go vet ./core/rawdb/freezer ./core/freezer ./core/state/snapshots ./core/state/pruning` | PASS，退出码 0 | `vet.log`、`vet.exit` |
| `make gtron`（默认 `gtron-sapling`，CGO 开启） | PASS，退出码 0 | `build.log`、`build.exit` |

`race.log` 的 `=== RUN` 记录确认了 freezer 新 publish 五项、compact 与 crash recovery、core/freezer 发布失败/取消恢复，以及 manifest 流式读取、预算、publication 与并发发布的 `refs-12` 和 `refs-64` 子例；64 refs 用实际 JSON 大小断言进入流式路径。全量测试启动后曾补一条 manifest missing-file 断言；后续 race 重新编译并运行了包含该断言的 `TestManifestStreamingReadMatchesWholeFile`，没有仅为它重复整个全量测试。`source-sha256.txt` 记录了定向验证阶段两项变更及测试文件的字节摘要。

本轮未部署到主网，尚无可比的线上分配率、CPU-s/block、同步 blocks/s、冷数据推进或 RSS 复测。局部 publish 与 manifest 暖读的改善只证明固定输入下的路径成本下降。
