# Erigon 3 风格 Archive 冷覆盖裁剪 MVP

日期：2026-08-05

状态：MVP 已实现，等待长期 mainnet/Nile 容量与故障注入验证

## 目标与非目标

`archive` 的承诺是“任意历史高度的状态永久可查询”，不是“同一份历史必须永久同时留在 Pebble 和冷文件”。本阶段把这两个概念明确拆开：

- **逻辑保留**：完整历史必须始终可由 hot/cold 组合视图读取；archive RPC 不设本地历史下限。
- **物理保留**：最近 `prune_window` 个块的状态变更保留在 Pebble；更老的重复 `StateDomainChange` 只有在不可变冷层已发布并通过格式、范围和伴随索引校验后才能删除。

本阶段不改变 protobuf、P2P、DPoS、执行、fork gate 或 java-tron 可见的共识行为。它也不裁剪区块、交易查找、receipt/log、balance trace、section bloom、最新状态、代码域、commitment checkpoint 或 `StateTxRange`。

## 官方 Erigon 基线

本设计只用 Erigon 官方资料作外部基线：

1. [Hardware Requirements](https://docs.erigon.tech/get-started/hardware-requirements) 在 2026-07-19 的 Ethereum mainnet 样本中列出 Archive 约 2.03 TB、Full 约 419.04 GB。这说明 archive 的“全历史可查”不等于无限增大的热数据库，但该容量不能直接套用到 TRON。
2. [Database](https://docs.erigon.tech/fundamentals/database) 把数据目录描述为较小且热的 `chaindata` 与不可变 snapshots 的分层布局，并列出 `domain`、`history`、`idx`、`accessor` 等目录。该页的 archive 示例中 `chaindata` 约 15 GB，而主要容量位于 snapshots。
3. [Pruning Modes](https://docs.erigon.tech/fundamentals/pruning-modes) 定义 archive 为保留完整历史状态，并说明 prune mode 是 datadir 级选择；切换通常需要删除 `chaindata` 后重同步。这支持 go-tron 继续持久化并锁定模式，而不是在线偷偷改变语义。
4. [Snapshots Management](https://docs.erigon.tech/fundamentals/snapshots-management) 说明不可变历史由多类 snapshot 文件及其索引共同组成，而非单个数据库表的永久副本。
5. [Why Erigon](https://docs.erigon.tech/v3.3/get-started/why-using-erigon) 说明 flat KV、不可变数据和 staged sync 是同一套存储/同步架构的组成部分。

可借鉴的是“热 KV + 不可变历史 + accessor/index + 分阶段进度 + 覆盖后裁剪”。不能照搬 Ethereum 的 Merkle trie、账户/存储域编码、MDBX 事务模型或共识 state root。go-tron 当前是无 Merkle-trie 的 Pebble flat-state 模型，正确的适配轴是现有 TRON `StateDomainChange`、`StateTxRange` 和 domain registry。

## 现状审计

### 已具备的基础

- `StateDomainChange` 记录 previous image，`StateTxRange` 提供 block 到 txNum 的时间轴，inverse rows 支持按 owner/domain/key 查询。
- 冷历史以 `history/state-domain-change-*.seg` 发布，并带 `.idx` 顺序索引与 `.kv` keyed accessor。当前 builder/compactor 会生成压缩 segment，读取器按 key/prefix/tx range 访问，不需要扫描整个历史。
- `manifest.json` 记录 generation、visible tx range、segment checksum/size 和 progress；生产 manifest 校验会验证注册的数据集和二进制 companion 的范围、记录与 accessor 一致性。
- `SnapshotLifecycle` 已按 build/compact -> prune -> retired reclaim 的顺序串行执行；builder 与 pruner 不并发修改同一代视图。
- `PersistentHistoryReader` 和 `TronBackend.archiveStateAt` 已组成 hot/cold 读取链路；balance/code/storage、`eth_call`/constant call、resource/reward 等历史入口共享归档 session。
- prune mode 已写入 `history-prune-mode-v1`，启动拒绝 datadir 的隐式模式切换，并对模式不允许的 stage progress 做冲突检查。

### 原先的 archive 阻塞点

- `archive` 没有注册 `DomainStatePruner`/`SnapshotLifecycle`。
- cold history builder 只在 `snap` 启用。
- worker/pruner/checker 对 `ModeArchive` 直接 no-op，导致逻辑全历史退化为 Pebble 物理全保留。
- 启动守卫把 `SnapshotHotPrune` 与 `SnapshotPrune` 也视作 archive 冲突，无法表达“只裁剪可恢复的重复状态变更”。
- catalog signing 只允许 snap，archive 无法给自己的冷历史代签名。

## MVP 数据所有权

| 数据族 | archive MVP 热层 | 冷层 | 本阶段是否裁剪 |
| --- | --- | --- | --- |
| 最新账户/KV/generation | 权威可变值 | latest snapshot 可作读取/恢复基线 | 否 |
| `StateDomainChange` + inverse rows | 最近 hot window | verified `.seg/.idx/.kv` 永久历史 | **是，仅覆盖后的旧重复行** |
| `StateTxRange` | 完整保留 | history segment 内也带 tx-range table | 否，后续单独审计 |
| code content rows | 完整保留 | code latest snapshot | 否 |
| commitment checkpoint/branch | 完整保留 | commitment snapshot | 否 |
| block/freezer 与 tx lookup | 完整保留 | 现有 freezer/sidecar | 否 |
| receipt/log/trace/bloom | 完整保留 | 已有独立 cold family | 否 |

保留 `StateTxRange` 是刻意的安全余量：它很小，却是 block/txNum 边界、启动诊断和历史可用性判断的共同依赖。只有在所有调用点都证明能从冷 tx-range table 恢复、并完成故障注入后，下一阶段才考虑删除。

## 安全协议

### 覆盖与删除门

对每个候选 block：

1. 必须位于 solidified head 的 `prune_window` 之外；因此同时位于 reorg window 之外。
2. manifest 中的 state-domain history segment 必须连续覆盖该 block 的 `[BeginTxNum, EndTxNum]`。
3. 每个参与覆盖的 `.seg`、`.idx`、`.kv` companion 必须通过现有格式和交叉一致性校验。缺文件、checksum/metadata/record/accessor 损坏都会在删除前返回错误。
4. 删除只包含该 block 的 `StateDomainChange` 和 inverse rows；`StateTxRange` 留在热层。
5. 删除批次成功后才推进 manifest hot-prune progress 和 hash-bound `SnapshotHotPrune`/`SnapshotPrune` stage。

覆盖集合按 txNum 排序并要求无缺口。manifest 声称覆盖但 companion 损坏时，不降级为“相信元数据”，也不推进进度。

### 崩溃一致性与启动恢复

- builder 先写临时文件、`fsync` 文件，再 rename 成不可变文件；manifest 是唯一可见性切换点。
- manifest 临时文件 `fsync` 后原子 rename，本阶段增加 manifest 所在目录的 `fsync`，保证进程返回成功前 publication directory entry 已持久化。
- 生命周期严格先发布、再重新校验、再删 hot duplicate。崩溃在 publication 前只留下未引用文件；崩溃在 publication 后、删除前只造成暂时重复；崩溃在删除后、进度前会在启动后幂等重跑。三种情况都不丢失逻辑历史。
- `SnapshotPrune` 由当前 verified `Finish` block/hash 绑定；进度校验会拒绝与 canonical boundary 不一致的行。
- 启动继续校验持久化 mode。archive 允许 `SnapshotHotPrune` 和 `SnapshotPrune`，但仍拒绝 chain lookup、section bloom、balance trace 和 freezer-tail prune progress。
- manager 对查询固定一个 manifest generation；retired segment 受 active/published lease 保护，避免查询与 compaction/reclaim 竞态。

### 重组边界

裁剪目标以 latest solidified block 为上界，`HistoryWindow >= ReorgWindow` 是 policy validation 的硬条件。archive 的 `RetainHistory` 仍然为永久，只有 `RetainHotHistory` 使用有限窗口。最新状态、checkpoint、code 和 tx range 不在 MVP 删除集合内，因此正常 fork switch 不需要从冷层回写这些结构。

### 同步干扰控制

当同步仍 active 且 remaining blocks 大于 history window 时，history build/compaction 延后，并累计 `state/snapshot/cold/history/deferred/sync`。进入近 tip 窗口或 sync-complete hook 唤醒后才补建冷层并裁剪。pruner 原有 sync-lag gate 继续生效。这避免全量追块期间争抢 Pebble 顺序扫描、压缩和 accessor ETL I/O。

## 模式迁移、兼容与回滚

- **已有 archive datadir 升级**：模式值不变。首次启动不会立即删除历史；先逐批构建/验证冷 coverage，随后才裁剪已覆盖的旧 change rows。默认 hot window 为 262,144 blocks，可用现有 `prune_window` 覆盖。
- **full/snap/archive 互换**：仍然拒绝原地切换，提示使用已持久化模式或新 datadir。不同模式对历史完整性的承诺不同，不能靠改 flag 推断缺失数据可恢复。
- **回滚到旧 binary**：旧 binary 会把 archive 的 `SnapshotHotPrune` 视作冲突，且不了解“冷层是唯一历史副本”。安全回滚需要升级前 datadir 备份，或先用当前 binary 的 verified restore 把 state history 恢复进新 datadir；不能直接用旧 binary 打开已裁剪 datadir。
- **禁用新物理裁剪**：`ArchivePolicy()` 的零 window 仍保留为内部 legacy/no-prune policy；生产 CLI 的零配置使用安全默认 hot window。需要永久停用时应在运行前使用足够大的显式 window，而不是删除 stage/manifest。
- **远程发布**：archive 允许 catalog signing；remote restore 仍要求 chain identity、registered datasets 和 checksum。签名不是本地删除的唯一依据，本地删除还会做 format-aware companion 校验。

## 验证与观测

MVP 的自动化门包括：

- policy：逻辑永久保留与有限 hot retention 分开验证。
- worker：verified coverage 删除旧 duplicate、保留 recent row 和 tx range；损坏 accessor 时完全保留 hot rows 且不推进 stage。
- lifecycle：一次 pass 必须先构建 cold history，再删除 hot duplicate，随后 cold reader 仍返回原变更。
- 并发/race：manager 持续读取 immutable history 时执行 hot prune，结果不丢、不重、不报竞态。
- backend：archive 模式下删除 hot history 后，balance/resource/reward、code/storage、constant call/`eth_call` 等结果与删除前一致。
- sync：远离 tip 时 history build 和 compaction 均 deferred；进入 window 后正常构建。

现有 metrics 已覆盖：

- `state/snapshot/cold/*` 的 build/compaction bytes、segment count、lag、duration；
- `state/snapshot/cold/history/deferred/sync` 的同步延期次数；
- `state/pruning/deleted/domain_change_blocks`、pass/error/catch-up 与 last-pruned head；
- `gtron_storage_state_hot_prune_txnum` 将已由 verified cold coverage 接管的
  hot duplicate 上界暴露给 storage-alerts/Prometheus；
- storage benchmark 的 Pebble、ancient、snapshot 总量/增量、每 block 增量和 cold/hot growth ratio。

本地快速验证脚本为 `scripts/dev/archive_cold_prune_verify.sh`。容量实验继续使用 `scripts/dev/storage_benchmark.sh` 与 `scripts/dev/storage_benchmark_acceptance.py`，比较 archive 升级前后 `chaindataBytes`、`snapshotBytes`、`coldArchiveBytes` 和增长斜率，而不是把 Erigon 的绝对 TB 数字当作 TRON 验收阈值。

## 后续阶段与剩余风险

1. 在 Nile/mainnet 执行长时 soak、kill -9/磁盘满/损坏 companion 故障注入，并记录恢复时间与 archive RPC P95/P99。
2. 增加显式 `archiveStateHotPruneToBlock/TxNum` 到 storage benchmark JSONL/Prometheus，使容量下降能与 state prune boundary 直接关联。
3. 审计所有 `StateTxRange` 调用点和 cold table 的无 manifest/缺口行为，再决定是否裁剪旧 hot tx ranges。
4. 只有在 code hash 引用闭包与 latest recovery 完整证明后，才允许 archive 裁剪 hot code content rows。
5. 派生索引和 chain lookup 必须各自定义 archive 可用性承诺、连续 coverage 与回滚路径；本 MVP 明确不借 state-history coverage 越权删除它们。
6. 目录 `fsync` 能保证 publication entry 的持久性，但无法消除硬件/文件系统谎报 flush 或底层介质损坏；生产仍需快照备份、checksum scrub 和远端副本。
