# 2026-09-22 主网同步优化评估

本轮以 05:33–05:38 UTC 的六组公开指标、两次主机只读观测和现有 pprof 为依据；采样时间短，结论用于确定验证方向，不代表线上调参已获收益。以下 `build/benchmarks/` 原始文件是本地 ignored 证据，不随 Git 提交；主要数据、结果和复现命令已写在本文。原始数据和详细口径见 [实时采样](../../build/benchmarks/20260922-live-sampling/README.md)、[主机观测](../../build/benchmarks/20260922-live-sampling/server/relayed-host-observations.md)、[内存预算审计](../../build/benchmarks/20260922-memory-budget/README.md) 及 [前一日资源审计](../../build/benchmarks/20260921-postdeploy-followup/server/OPTIMIZATION-AUDIT.md)。

## 内存预算与 Pebble 缓存

主网服务的 20 GiB cgroup 已使用 18.2–18.9 GiB，进程 RSS 为 10.4–11.2 GiB，cgroup 文件缓存约 7.8 GiB；355 秒内 `memory.failcnt` 增加 26,414，表示内存 charge/reclaim 压力，不能解释为相同次数的 OOM。并行历史读取的可用内存探针在六点仅有 0.323–1.292 GiB，低于配对路径的 2 GiB 和增强读取的 2.3125 GiB 门槛，`fallback_reason=7` 与之吻合。约 524,288 块的 cold backlog 是 65,536 块窗口下配置的 busy 模式水位（普通延迟 4 个窗口，busy 上限再乘 2）；单看这个水位不能判定构建器落后。

当前 `--db.cache 4096` MiB，Pebble `memory/manualalloc` 为 3.839–4.000 GiB：其中 live 和 zombie memtable 各 256 MiB，推得 block cache 3.339–3.500 GiB。Pebble 为 memtable 调用 `Cache.Reserve`，所以 4 GiB 配额在这组 512 MiB memtable 预留下大约只剩 3.5 GiB 的 block 目标。单个 256 MiB zombie memtable 符合 Pebble v1.1.5 保留至多一个旧 arena 供下次轮转复用的设计；现有 iterator 和 snapshot 计数也没有提供泄漏证据。新增 `storage/engine/block_cache/bytes` 和 `storage/engine/block_cache/configured_capacity_bytes`，复用原有 3 秒 Pebble 指标采样，直接区分实际 block 占用与配置容量；namespace 所有权测试覆盖辅助 DB 不覆盖该容量。

把 cache 从 4 GiB 调到 3 GiB、且仍预留 512 MiB memtable 时，六点当前占用对应的**模型节省上界**约 0.839–1.000 GiB；这不是 RSS 或 cgroup 可用内存的保证值。即使假设这部分立即全部变成探针可用空间，六点中仍没有一点达到 2.3125 GiB 增强门槛，仅两点理论上超过 2 GiB 配对门槛。五分钟 block cache 命中率为 86.62%（约 3020 万 hit、466 万 miss）；减容可能增加 SSD 读取，无法只凭当前命中率预测净同步吞吐。维持现有生产配置与准入门槛；如评估 3 GiB，应以相近链高和负载做可回退对照，联合看 cache hit/miss 增量、Pebble 物理读、主链块速、cold 水位、cgroup current/RSS/reclaim/refault 和并行准入。不能把 `inactive_file` 全部计为即时可用，也不能把 Go `system/memory/held`、Pebble manualalloc 与 RSS 相加；`GOMEMLIMIT` 是 Go runtime 软限制，并不封顶 Pebble 原生缓存或 cgroup 文件缓存。

内存分项的 Pebble 包完整测试已通过。`GOTOOLCHAIN=go1.25.5` 可用；尚未部署或更改线上配置。

## 事件日志路径

V4 完整构建的第一遍扫描已统计 address/topic posting 数量，第二遍只在实际遇到 key 时按已知数量预留切片；每 256 行的 topic ID 改用可复用、按需增长的 arena。仍保留两遍 source 检查、protobuf 编码、文件校验和查询格式。固定 270 行回归样本跨 row-frame 边界，HEAD 与候选生成的主文件和索引文件 SHA-256 完全一致；第二遍缺行、字典缺失、key 消失等路径也有回归测试。

| V4 完整构建样本 | 基线 B/op | 候选 B/op | 分配变化 | 基线 allocs/op | 候选 allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4096 logs、重复地址/topic、512 B payload | 21,925,568 | 21,554,660 | −1.69% | 82,222 | 73,888（−10.14%） |
| 4096 logs、高基数地址/topic、128 B payload | 26,126,728 | 24,435,596 | −6.47% | 138,524 | 134,350（−3.01%） |

本地五轮完整构建中位耗时分别从 32.685 降到 29.996 ms/op、39.725 降到 39.550 ms/op；较长的高基数三轮也更快。但这是固定内存 reader 和本机 darwin/arm64 的两组顺序测试，未包含生产 source 解码、并行历史构建和主链导入，**不能宣称线上同步吞吐提升**。小样本和空样本未显示稳定收益。实验命令为 `GOTOOLCHAIN=go1.25.5 GOMAXPROCS=2 go test ./core/state/snapshots -run '^$' -bench '^BenchmarkEventLogV4CompleteBuild$' -benchtime=200ms -count=5 -benchmem -timeout=300s`；原始结果见 [事件日志实验](../../build/benchmarks/20260922-event-build/README.md)。

## Commitment 分配路径

`rawdbBranchStore.putBranches` 在 30 秒线上分配 profile 中占 2,608.66 MiB（25.27%），但其不可变 arena 由 blockbuffer 层持有，直至最后一个读者结束才可释放。本轮不复用或改变 arena。实际调整的是写入层 map 的容量提示：ordered lane、64 分区与 parallel root fold 根据当前 sibling 的 resolved ops 占比估计容量，避免第一个完成的巨大分区把所有后续 map 都过度预留；编码、root、分支字节与层生命周期不变。

| 真实 ordered pipeline，四个变化块/次 | 基线 B/op | 候选 B/op | 变化 |
| --- | ---: | ---: | ---: |
| 1 分区 × 256 ops | 439,067 | 442,592 | +0.80% |
| 2 分区 × 128 ops | 446,517 | 447,495 | +0.22% |
| 64 分区 × 8 ops，均匀 | 1,172,810 | 1,165,149 | −0.65% |
| 分区 0 为 256 ops，其余 63 个为 8 ops | 1,435,295 | 1,386,935 | **−3.37%** |
| 分区 63 为 256 ops，其余 63 个为 8 ops | 1,391,564 | 1,389,353 | −0.16% |

五轮固定 corpus 中，只有偏斜且大分区编号靠前的真实 pipeline 样本显示清楚的分配改善；其它样本基本持平，本地耗时差异不足以证明吞吐收益。单独 batch-writer 合成基准在“大 sibling 先写”时分配从 2.29 降到 1.07 MB/op，只说明预留机制与完成顺序的上界，不能替代真实导入。测试覆盖 commit/discard/flush 后旧读者持有的分支字节、ordered 与 partition 并发 root 一致性。实验原始命令和结果见 [Commitment 分配实验](../../build/benchmarks/20260922-commitment-allocation/README.md)。

## 本地组合验证

环境均为 `GOTOOLCHAIN=go1.25.5 GOMAXPROCS=2 CGO_ENABLED=1`；以下命令全部退出码 0，日志及退出码保存于 `build/benchmarks/20260922-integrated/`。

| 验证 | 命令与结果 |
| --- | --- |
| 四包完整测试 | `go test ./core/state/domains ./core/blockbuffer ./core/state/snapshots ./core/rawdb/pebbledb -count=1 -timeout 600s`；四包均 `ok` |
| 定向 race | `go test -race` 运行下方正则选中的 domains、snapshots、pebbledb 测试；所有目标均实际运行且通过 |
| 静态检查 | `go vet ./core/state/domains ./core/blockbuffer ./core/state/snapshots ./core/rawdb/pebbledb`；通过 |
| Sapling 生产目标 | `make gtron`；通过，产物为本机 darwin/arm64 Mach-O、Go 1.25.5，**不是 Linux 部署产物** |
| Diff | `git diff --check`；通过 |

定向 race 完整命令：

```sh
go test -race ./core/state/domains ./core/state/snapshots ./core/rawdb/pebbledb \
  -run '^Test(EventLogV4BuildPostingAndTopicStorageMatchesBaseline|EventLogV4BuildRetainsSecondPassSourceChecks|EventLogV4BuildSecondPassKeyOmissionMatchesBaseline|OrderedChainEventLogBuildMatchesETL|CommitmentBranchArenaAcrossCommitDiscardAndFlush|OrderedCommitmentPipelineMatchesSequentialAcrossInflightBlocks|ParallelFoldOverBlockbuffer_RaceAndRootMatch|PartitionedCommitmentCompatibility|ApplyRootParallelFlushesOptedInStoreConcurrently|DatabaseSpaceMetricsNamespaceOwnership)$' \
  -count=1 -timeout 300s -v
```

这些本地正确性与分配测试尚未检验 Linux 主网真实负载下的 RSS、I/O、cold 准入或同步块速；本轮没有推送、部署或变更线上参数。
