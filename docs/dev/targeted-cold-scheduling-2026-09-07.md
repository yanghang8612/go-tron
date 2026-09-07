# 冷历史压力调度与先验证回收

2026-09-07。本次是本地生产路径实现与测试，没有访问服务器、部署或启动 gtron 同步。保留完整历史状态查询，不改变历史覆盖证明、冷热内容校验、事件日志覆盖条件或历史保留范围。

## 实际改变

原 ordered lifecycle：history / 必要 event-log 构建并发布 → cold merge → latest → chain freezer → 原热历史 pruner。因此已存在完整冷副本时，后续 merge 的耗时、失败或取消仍可能延迟这一轮热删除。

新顺序：history / 必要 event-log 构建并发布 → **原 pruner 验证覆盖并回收热历史** → 有界 cold merge → latest → chain freezer → 其他原有维护。每轮最多调用一次 pruner。新构建 refs 通过原 `RecordTrustedSnapshotSegments` 接入相同的窄验证缓存；已有未认证 refs 仍需完整验证。event-log、freezer、代码引用等原覆盖门没有跳过。

实现入口：[Runner.OnePassWithMaintenanceContext](../../core/state/snapshots/cold_builder.go)、[SnapshotLifecycle.OnePass](../../core/state/pruning/lifecycle.go)。callback 在 Runner 的 pass mutex 内执行，不允许重入 Runner。Standalone `OnePassContext` 没有 callback，保留独立构建行为。生命周期默认每轮至多一次 cold merge；一个 merge 自身仍按原 `CompactMaxSteps` 选段和完成全部校验。独立 Runner 的 `MaxCompactionPasses=0` 仍表示原 drain 行为。

此顺序减少已证明冷覆盖后等待重合并的延迟，**不承诺每轮总耗时下降**。初次遇到未缓存的旧 refs，需要在合并前先验证原文件，可能增加本轮工作；已有验证记录可以复用。prune 是逻辑删除，物理空间仍由 Pebble 压实回收。

## 压力输入和准入

[runtime probe](../../cmd/gtron/history_pressure.go) 读取 chaindata、state-snapshots 和配置 ETL 目录的可用空间；未创建目录使用最近存在的父目录，不创建目录。可用空间每轮重读，hot history SST 范围估算最多缓存一分钟。[EstimateDiskUsage](../../core/rawdb/pebbledb/disk_estimate.go) 使用 Pebble 元数据及块偏移，不遍历历史 value，并持有与 Close 配对的读锁。估算不含 WAL/memtable，可以包含 obsolete versions，不能解释成唯一 live 历史或可释放字节。

默认阈值使用所检查卷中**最小总容量**换算为绝对 bytes；读数采用所有路径中**最小可用 bytes**。这不是分别保证各卷各自的百分比。

| 参数 | 默认 | 作用 |
|---|---:|---|
| `history.pressure-hot-mib` | 最小总容量 5% | hot SST 估算达到该值，允许较早准入有界 cold history |
| `history.pressure-free-mib` | 最小总容量 10% | 任意相关路径可用空间低至该绝对值，进入压力模式 |
| `history.build-min-free-mib` | 最小总容量 2% | 可用空间严格低于该值，延期新冷输出 |

参数为 MiB，0 表示自动比例；明确值检查位移溢出，并要求 minimum ≤ pressure-free < 最小卷总容量。availability flags 区分没有测量与实际 0 bytes。hot 阈值可以明确高于卷容量，此时主要依靠 free-space 分支。

空间不足时不构建 history、derived event sidecar、latest、merge 或本生命周期 chain freezer，仍可运行原覆盖验证和热回收。probe 报错时同样禁止新构建；若父 context 未取消，先尝试原 pruner，再返回 probe/prune 合并错误。父 context 已取消则不开始 callback。错误与硬余量延期设置正常 Interval 重试，无进展不会触发立即 catch-up。

**这是构建准入信号，不是磁盘峰值硬限额，也不是前台导入背压。** 单次大区块、ETL、压实、MySQL 或其他同卷写入仍可消耗空间；本次没有构建中途的全卷预留机制，没有承诺固定磁盘永久容纳无限全历史。严格约束峰值和总增长仍需写入预算/前台准入及长期净增量测量。

## busy 恢复窗口与资源边界

[pressure policy](../../core/state/snapshots/cold_pressure.go) 仅在有可用测量且达到阈值时绕过普通 soft/busy 区块欠账延期。仍获取同一 `HeavyWorkGate` 名额，保留 cooldown；没有新增并行 builder。压力批次至多为配置 BatchBlocks/BatchTxNums 的四分之一，最小一单位。生产默认最多 1,250 blocks、97,656 txNums 目标；txNum 目标以完整 block 收尾，可能略超，**不是字节上限**。

原 forced-busy 成功恢复为 `clamp(4 × work, interval/2, interval)`。生产 interval=60s，即 30–60s。新测量压力下 successful forced-busy 恢复为 `clamp(work, heavy-cooldown, max(heavy-cooldown, interval/2))`，生产为 3–30s。work 统计 history 构建 + 前移 prune + merge；其他阶段仍实际占用本轮墙钟时间。publication 日志先报告构建阶段估计，最终 PassResult/metrics 纳入后续已完成阶段。

失败 forced-busy 构建或其后维护保留完整 60s 恢复，deadline 从失败完成时开始。压力不绕过重试窗口；只有完成发布且仍有候选 work 才立即请求下一轮，紧接轮次仍检查恢复 deadline。无候选、无进展、缺失 hot 行等状态不会通过 `NeedsCatchup` 自旋。

## 验证

本地完整包：`go test ./core/state/snapshots ./core/state/pruning -count=1 -timeout=300s` 通过（snapshots 62.997s，pruning 17.240s）。定向 race 通过（2.374s / 2.816s）：

```sh
go test -race ./core/state/snapshots ./core/state/pruning \
  -run 'TestColdPressure|TestPressureHistory|TestColdCompactionPassLimit|TestSnapshotLifecyclePressure|TestSnapshotLifecycleEarly|TestSnapshotLifecycleStopCancels' \
  -count=1 -timeout=180s
```

[压力测试](../../core/state/snapshots/cold_pressure_test.go)：unavailable 测量、minimum 边界、共享 lease 不可绕过、成功恢复上限、失败恢复与取消、一次 merge 上限且全历史可读。[生命周期故障测试](../../core/state/pruning/lifecycle_pressure_test.go) 使用真实 writer 构建 cold trio：probe 错误后仍安全删除已覆盖 hot；坏 accessor 保留 hot 并停止 merge；后续 freezer 失败不撤销已完成 prune。[Stop 测试](../../core/state/pruning/lifecycle_compaction_stop_test.go) 在真实 merge copy 阶段取消，验证 hot-prune 已持久化，旧 cold refs 保留且全部历史记录仍可读。cmd probe 的实际零值缓存及参数边界另见 [root 测试](../../cmd/gtron/history_pressure_test.go)。

现有 build API 部分阶段只在调用边界观察 context；本次没有宣称所有压缩/文件系统调用均可立即抢占。已验证的 merge/验证/prune 取消路径协作式退出，后续失败不回滚已安全完成的冷发布/热回收进度。

`git diff --check` 通过。包测试是在共享工作区当时快照上运行，并非对已部署 release 的确认。

## 可复现排序基准与限制

```sh
go test ./core/state/pruning -run '^$' \
  -bench '^BenchmarkCoveredHistoryPruneOrder$' -benchtime=5x -count=3 -timeout=180s
```

fixture 为 32 blocks、每条 8 KiB Prev，分成两个真实 cold trios。建 fixture 不计时；计时包括真实校验、合并、manifest 更新和逻辑删除，最后重新读取 cold 检查全部历史。`prune-ready` 从本轮开始算到原 prune 完成。旧序与新序均按生命周期实际行为登记 compactor refs，没有用 sleep 模拟 merge。

| 原 refs 状态 | 顺序 | 本轮总耗时范围，ms | prune-ready 范围，ms |
|---|---|---:|---:|
| 已存在、未在验证缓存中 | merge → prune | 63.45–65.39 | 63.45–65.38 |
| 已存在、未在验证缓存中 | prune → merge | 83.94–120.52 | 33.61–47.03 |
| 同进程真实 builder refs，沿用原窄信任门 | merge → prune | 73.35–104.33 | 73.35–104.33 |
| 同进程真实 builder refs，沿用原窄信任门 | prune → merge | 64.07–71.36 | 10.13–10.64 |

以上为 Apple M1 Max / Darwin ARM64、本地共享开发机三组各五次的均值范围；不是磁盘隔离实验、生产吞吐或全库 ETA。最早诊断版未模拟 lifecycle 的 compactor refs 登记且与完整测试并行，其数字不用于该表。实验支持“验证缓存状态影响成本，热回收不必等重合并”的机制；未缓存路径明确呈现总时延增加，不能只摘取新序最快值宣称整体加速。
