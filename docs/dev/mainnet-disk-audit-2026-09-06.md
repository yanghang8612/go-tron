**主网数据库磁盘审计与处理方案 — 2026-09-06**

后续约束与处理：用户已明确不能扩盘、不能迁移 MySQL、必须保留全部历史状态查询。
本文件保留 9 月 6 日审计时点的原始结论与选项；扩容、迁移和有限历史模式不属于当前执行方案。
9 月 7 日已经实施停机保护、受控物理回收与离线历史维护，最新结果见
[离线空间回收执行记录](mainnet-offline-recovery-2026-09-07.md)。

审计对象：`ip-172-12-16-193.us-east-2.compute.internal` / `3.12.206.71`。通过独立 Chrome JumpServer 终端检查，HTTP 通过本机 SOCKS5 `127.0.0.1:1088`。本次未启动任何 gtron 节点、未恢复同步、未删除业务数据、未运行压实或修改生产服务配置。仅在服务器 `/dev/shm/gtron-disk-audit-20260906` 放置审计源码、编译缓存和小型输出；独立工具以 Pebble ReadOnly 模式读取现有数据库。

结论：当前问题是共享数据盘容量不足，叠加主网热状态历史积压。应先扩容或提供独立数据卷，再做受控离线回收和冷历史补建。单纯删除 retired 快照没有收益，单纯压实不能消除主要存量。

**现场状态与事故证据**

- `/data`：7 TiB EBS，直接在 `/dev/nvme1n1` 上建立 ext4，无分区；设备序列号 `vol05fdb6d079be96ccf`，对应卷 ID `vol-05fdb6d079be96ccf`。实际操作前仍须重新核对卷 ID 与挂载映射。
- 14:48 前后精确 `df -B1`：文件系统容量 7,669,457,909,760 B，已用 7,485,023,751,168 B，普通用户可用 124,029,157,376 B（115.51 GiB），显示使用率 99%。`df` 可用空间与“容量减已用”不同，预留块不应作为可放心消耗的预算。
- 根文件系统是另一块 80 GiB XFS，剩约 31 GiB。`/data` inode 使用率不足 1%，不是 inode 耗尽。没有启用 swap。
- `gtron.service`、`gtron-nile.service`、`gtron-deploy.timer` 观测时均 inactive/dead，未发现 gtron 进程。三者 unit 仍 enabled，未来重启主机/重新启动部署 timer 有自动恢复风险。
- 主网有效配置使用 `/data/gtron/releases/20260906-p2f/gtron`、`--prune.mode snap`，未显式覆盖 history window；相应生产源码默认是 65,536 blocks。当前审计不涉及端口改动。
- 应用日志北京时间 **18:32:21.370** 明确记录交易冷索引 ETL 写入失败：`ancient/tx-index/etl/.../.run-465065128.tmp: no space left on device`。这证明磁盘不足真实发生过。
- 最近一次停机日志北京时间 **22:26:04.220** 为 Clean shutdown，head **30,512,882**；systemd 记录退出状态 0。本次检查时 Nginx、Java 主网及部分其他服务有监听，不能据此声称所有服务当前均停止，也不能仅凭一条 gtron 日志确定每个服务此前的退出原因。

**物理分布**

下表以 `du -x -B1` 已分配字节换算，1 GiB = 2³⁰ B、1 TiB = 2⁴⁰ B。同盘其他服务继续运行，连续两次统计存在小幅增长；主网目录与清单保持稳定。

| 数据范围 | 实际分配空间 | 含义 |
|---|---:|---|
| `/data` 全盘文件 | 6.807 TiB | 与 df 量级一致 |
| `/data/mysql` | 3.160 TiB | 约占全盘文件空间 46.4% |
| `/data/gtron` 全部 | 2.716 TiB | 约占 39.9% |
| 其中正式主网 `/data/gtron/main` | 2.654 TiB | 2,918,336,303,104 B |
| `/data/fastnode` | 323.7 GiB | 独立 Java 数据等，未判断可删除 |
| `/data/bench` | 140.9 GiB | 名称不能作为删除依据 |
| `/data/precompiled-sync` | 118.7 GiB | 同上 |
| `/data/market_pairs` | 99.2 GiB | 其他业务数据 |
| `/data/event` | 91.8 GiB | 其他节点/业务数据 |
| `/data/mock` | 78.7 GiB | 需核对用途和保留要求 |
| `/data/nile` | 55.0 GiB | 不是 gtron 主网目录 |

主网内部没有把父目录和子目录重复相加：

| 主网组件 | 物理空间 | 占主网约 |
|---|---:|---:|
| `chaindata` Pebble 热库 | 1,532.22 GiB | 56.4% |
| `state-snapshots` | 988.95 GiB | 36.4% |
| `ancient` 区块/回执/交易索引冷库 | 196.70 GiB | 7.2% |
| 应用日志等其余 | 约 0.05 GiB | 很小 |

`ancient/v2` 约 171.01 GiB，`ancient/tx-index` 约 25.69 GiB。应用 `gtron.log` 约 39.64 MiB，不是容量主因。

两个旧 canary 目录合计约 **58.90 GiB**（`/data/gtron/canary`、`/data/gtron/canary-c00d1a98`）。整个 gtron 非主网部分合计约 63.57 GiB。它们可作为确认用途后的应急清理候选，但不能未检查备份价值、引用及硬链接便删除；也不能靠这几十 GiB 解决 TB 级增长。

**快照：876.34 GiB retired 记账不等于可释放空间**

对整个 `state-snapshots` 目录逐文件 Lstat，将实体与 active、retired、published 引用集合比对，并对 `(device,inode)` 去重统计分配块。清单读取前后内容一致。

- manifest generation **10712**，18,582,422 B。
- SHA256：`233489bc80e920fa317ce91ff5c66b2b35d2f851d1bc4c2a1c0d40625a434039`。
- active **21,990** 个引用，缺失 **0**；retired **31,488** 个引用，实体缺失 **31,488**，存在 **0**。
- published manifests 为 **0**；活动段没有发现额外硬链接。
- retired 账面大小 940,961,761,893 B（876.34 GiB），本次这个集合的物理可回收量为 **0**。
- 未引用文件只有 manifest 本身和两份小型 verification cache，没有发现大体积孤儿段或临时文件。不能把这些元数据误列为垃圾。

| 活动快照族 | 文件数 | 文件长度合计 GiB |
|---|---:|---:|
| history 主段 | 216 | 804.45 |
| accessor | 216 | 88.43 |
| inverted index | 216 | 8.33 |
| event-log | 10,671 | 75.99 |
| event-log-index | 10,671 | 11.68 |

文件长度与分配块有小幅差异，上表不能与 du 每字节混用。存在性检查也不等于全部文件 SHA/语义校验；本次没有为了只读空间分析扫描全部约 1 TB 冷数据。

源码能够解释 retired 账面长期增大：合并完成后默认删除不再受保护的旧文件，retired 引用仍留在清单。参见 [compactor.go](/Users/asuka/Projects/asuka/go/go-tron/core/state/snapshots/compactor.go:304)。再次运行 `snapshot prune-retired` 不会凭空释放这 876 GiB。

**热库：状态变更历史占 88.3%，其中确有约 90.9 GiB 已删除区间的物理残留**

独立 Go 工具只依赖 Pebble，明确设置 `ReadOnly:true`、`ErrorIfNotExists:true`、`DisableAutomaticCompactions:true`，使用兼容的 comparer/Bloom。工具没有导入节点、网络或同步代码；两个检查均 exit 0。Pebble 只读打开会把 WAL 重放到内存，不会恢复节点同步或写出压实结果。

统计采用固定版本 Pebble 的 `EstimateDiskUsage`。它按 SST 元数据和边界索引估算物理范围，包含旧版本、被遮蔽的数据和 tombstone，不是未压缩 live KV 大小，也不是可保证释放的容量。[Pebble v1.1.5 实现](https://github.com/cockroachdb/pebble/blob/v1.1.5/db.go)

| 物理数据族 | SST 范围估算 GiB |
|---|---:|
| `state-changeset-v2-*` 状态变更历史 | **1,352.67** |
| commitment branch | 62.42 |
| state-change posting | 37.98 |
| block body | 35.53 |
| 最新 KV | 14.20 |
| 热交易索引 | 9.06 |
| state-change key directory | 7.51 |
| 按块交易回执 | 5.96 |
| 最新账户 | 2.88 |
| StateTxRange | 1.32 |
| code | 0.16 |
| legacy balance/account traces | 不到 0.001 |

因此，压缩最新账户、删除 trace、清应用日志不是主要优化方向。大约 85.8% 的热库字节已在 L6；当前代码 L0–L5 不压缩、L6 默认 Snappy，修改中间层压缩不能直接消除保留的历史。参见 [pebble.go](/Users/asuka/Projects/asuka/go/go-tron/core/rawdb/pebbledb/pebble.go:234)。

关键边界通过数据库读取交叉验证：

- Execution = Finish = head = **30,512,882**。
- StateHistoryIndex = **30,510,432**。
- manifest hotPruneBlockNum = **29,125,223**；第一条 live changeset 的 block = **29,125,224**，seq = 0。
- state history 涵盖的热高度跨度为 **1,387,659 块**，约为默认 65,536 块热窗口的 **21.17 倍**。这是跨度比较，不意味着这些块都立即可删。
- historyBuildTxNum = accessorBuildTxNum = **1,734,454,192**，hotPruneTxNum = **1,734,063,547**，相差 390,645 txNums，约一个普通构建批次。txNum 与 blockNum 不能混用。
- 数据库读取 block 29,125,223 的 EndTxNum 正是 **1,734,063,547**，与 manifest 对应，下一块从 **1,734,063,548** 开始。

再以 `prefix || BE64(29,125,224)` 为边界拆开物理范围：

| 状态变更物理范围 | 字节估算 | GiB | 判断 |
|---|---:|---:|---|
| 边界之前，已无 live changeset | 97,573,155,409 | **90.87** | 已逻辑删除，但 SST 尚有残留；可作为受控压实对象 |
| 边界之后，仍保留的热区间 | 1,354,849,412,758 | **1,261.80** | 不能因文件旧而直接删，需要合法保留策略与冷覆盖 |

两次边界估算之和比全族估算高 102,210 B（约 100 KiB），属于边界估算重叠，不影响结论。第二个范围仍可能含旧版本；没有逐条全扫描，不能称其中每个字节都是唯一 live 值。

这排除了“1.45 TB 全是压实垃圾”的判断，也排除了“完全没有压实收益”的判断。posting 与目录族合计 45.49 GiB，其中只有已失效部分能删除。不能把这 45.49 GiB 全部加成保证收益，更不能承诺净释放整个 1.32 TiB changeset。

**形成积压的机制与修复方向**

本项目 snap 模式保留完整冷历史，65,536 blocks 只是期望保留的热尾，不是磁盘硬上限。冷覆盖尚未完成时必须保留旧热历史，不能为了窗口数字直接丢数据。[params/config.go](/Users/asuka/Projects/asuka/go/go-tron/params/config.go:72)

同步调度偏向导入：深度同步期间冷构建可能延期；超过 busy watermark 后仅缩小批次并增加恢复间隔，没有对导入形成强制背压。代码明确说明 watermark 不保证 backlog 有界。正常一批上限为 5,000 blocks / 390,625 txNums，繁忙强制构建可缩为四分之一，随后等待 30–60 秒。源码见 [cold_builder.go](/Users/asuka/Projects/asuka/go/go-tron/core/state/snapshots/cold_builder.go:145)、[cold_builder.go](/Users/asuka/Projects/asuka/go/go-tron/core/state/snapshots/cold_builder.go:1173)、[history_config.go](/Users/asuka/Projects/asuka/go/go-tron/cmd/gtron/history_config.go:170)。

另有 posting/key-directory 清理明确等网络追赶空闲，并可能被同步重新活跃取消，[lifecycle.go](/Users/asuka/Projects/asuka/go/go-tron/core/state/pruning/lifecycle.go:271)。这解释了索引积累的设计原因，但其体量只有约 45.5 GiB，不能当作本次主因。现场的磁盘错误、持续压实提示和水位落后都支持冷化/回收跟不上；现有证据没有把具体每个延期原因或耗时精确归因。

**建议按以下顺序处理，所有阶段均可保持节点同步关闭**

1. **先得到可靠空间余量，并隔离共享盘风险。** 最确定的路径是扩大已确认的 EBS 卷，或把 gtron/MySQL 分别迁到独立卷。按当前约 6.807 TiB 文件占用，静态降到 80% 就需约 8.51 TiB，尚未计文件系统开销、未来增长与维护峰值。因此 9–10 TiB 只是当前缓冲方案；10 TiB 扩容相较现有卷增加约 3 TiB 原始空间，不是保证主网最终可容纳的预测。

   在暂停相关写入并取得可恢复备份/卷快照后修改 EBS 容量；卷进入 optimizing/completed 且设备映射复核无误，再对这个无分区 ext4 执行 `sudo resize2fs /dev/nvme1n1`，用 df 验证。它不需要启动 gtron。不要使用针对 XFS 的命令、重新格式化或修改分区表。[AWS 官方扩容流程](https://docs.aws.amazon.com/ebs/latest/userguide/recognize-expanded-volume-linux.html)

   如果选择新卷搬迁，完整保留 chaindata、ancient、snapshots、manifest、伴随索引与元数据；停止对应数据库写入后复制，核验文件与数据库，再切挂载。迁移不减少总数据量。未通过验证前保留源副本，不在同一已近满卷上复制出另一份 TB 级数据库。

2. **回收已经逻辑删除的物理残留及失效索引。** 约 90.87 GiB changeset 旧区间是实测的压实目标；posting/key-directory 可以依据已验证的删除边界做 stale sweep。已有 `db compact-state-history` 不启动同步，但会写数据库并压实整个 changeset/posting/directory 范围；不能在只有 115.5 GiB 余量时默认它安全。

   后续维护前固定并核验 manifest、第一条 live changeset、canonical/Finish、真实 solidified 与冷文件完整性，备份并预算重写峰值。CLI 快速路径本身仅信任 manifest hot-prune watermark，没有独立完成所有上述校验；现场 first-live 恰好等于 watermark+1 是额外证据，执行前仍需重验。[db_compact_state_history.go](/Users/asuka/Projects/asuka/go/go-tron/cmd/gtron/db_compact_state_history.go:98)

   扩容后可用现成离线命令；若要缩小临时峰值，则先实现并验证仅压实已删除旧区间的工具，而不能把完整 CLI 当成已有的分段接口。实际收益以维护前后 df/allocated blocks 为准。此阶段能缓解压力，不能根治 1,261.8 GiB 保留热区间。

3. **实现专用离线 history 构建与裁剪流程，处理主要热积压。** 当前 CLI 没有一条现成的“离线补建 state history + verified hot prune”命令。`build-derived-indexes` 只处理其他派生族；`migrate-history-v7` 只重写已存在冷段；`compact-state-history` 不删除尚需保留的 live changeset。不能用任何一条冒充完整方案。[snapshot_cmd.go](/Users/asuka/Projects/asuka/go/go-tron/cmd/gtron/snapshot_cmd.go:193)

   可复用已有历史构建/覆盖验证/删除组件，实现不构造 Node、P2P、SyncService 的 history-only 维护入口：固定合法 cutoff → 小批读取 hot → 写出并验证 `.seg/.idx/.kv` → fsync/原子发布 → 依据同一覆盖删除 hot duplicates → WAL 同步屏障 → 持久化游标 → 有预算的物理回收 → 校验并记录净空间变化。每批可取消、可续跑，余额不够下一批就停止。

   cutoff 必须受真实 solidified、verified Finish、history window 约束；不能直接用 head 替代。若仅把 head 当宽松上界，65,536 窗口外最多约 1,322,123 块属于候选，但这不是本次批准的删除清单。冷数据新增量与 scratch 峰值需要小批实测，不能承诺净释放 1.26 TiB 或固定完成时间。

   不能直接运行完整 lifecycle：它还会触发 latest 全扫描、derived 构建、code/checkpoint 裁剪和大型 cold merge。专用入口应明确排除这些工作，并给 ETL、字典、输出和 Pebble 重写分别做预算。现有 builder 中部分 scratch 仍位于 snapshot 目录，不能只设置某个 tempdir 就假设全部换盘。

   上线前补齐跨 WAL/manifest 的删除持久化顺序；故障注入覆盖磁盘不足、fsync/rename 失败、各发布边界强制退出、损坏/缺失 companion、重跑与边界查询。验收要求最新状态/head 不变、冷热历史查询等价、未跨保留窗口、实际净占用下降。已有组件测试通过不等同这个新工具已可直接用于生产。

4. **修复持续增长的控制机制。** 引入剩余字节/inode、历史积压字节和预计维护峰值门槛；冷化＋删除＋物理回收持续跟不上导入时，必须降低或停止导入，而不能只等待偶尔空闲。外部监控应先写持久化停机标记，再优雅停止；服务启动守卫与 `/data/gtron/start.sh` 都检查标记及空间，防止“异常恢复”再次启动。仅加硬配额会把 ENOSPC 换成 EDQUOT，不替代提前停止。容量预留需覆盖最大在途输出、WAL/停机刷盘、监控延迟期间增长和运维余量。

**其他方案的适用范围**

- MySQL 是同盘最大占用者，不能忽略。目录里主要是大量 `transactions_日期.ibd` 等表文件，最大抽样文件约 7.1 GiB，不是已证明单张表占数 TB。应按表核对行数、索引、保留期限和碎片；普通 DELETE 不保证马上归还磁盘，OPTIMIZE/重建需额外排序/中间表空间，改 tmpdir 也不移动所有中间数据。确认不用的日表可通过数据库正式流程归档/删除，但本次没有授权删除这些业务数据，也没有执行。不要直接 rm `.ibd`。[MySQL 官方空间要求](https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-space-requirements.html)、[独立表空间说明](https://dev.mysql.com/doc/refman/8.0/en/innodb-file-per-table-tablespaces.html)
- 需要永久历史查询：冷历史会随链增长，必须给长期存储预算；压缩、合并、降低写放大只能降低增长系数，不能把总量变成固定上限。
- 仅需当前状态和有限近期查询：可评估 full/blocks 等有限历史模式，但当前 datadir 的 snap 模式已持久化，直接改 flag 会被拒绝；需要另行设计验证转换，或在未来明确授权后使用新 datadir。不能通过删模式键/手工删快照绕过检查。[history_config.go](/Users/asuka/Projects/asuka/go/go-tron/cmd/gtron/history_config.go:302)
- V7 全量迁移不是满盘急救。它先写新三件套，旧文件只记 retired，阶段性占用增加；当前发布失败后的清理还需处理“rename 已成功、目录 fsync 失败”的不确定状态。应先修复并做故障验证，再在独立固定样本上验证净收益。[history_migrate_v7.go](/Users/asuka/Projects/asuka/go/go-tron/core/state/snapshots/history_migrate_v7.go:116)、[manifest.go](/Users/asuka/Projects/asuka/go/go-tron/core/state/snapshots/manifest.go:220)
- 不得按大小/日期手工删除 `.sst`、编号 `.log` WAL、CURRENT、MANIFEST 或 active 快照三件套。应用 `gtron.log` 与数据库 WAL 是不同文件。

**证据、验证及未完成的边界**

本地证据目录：[20260906-disk-audit](/Users/asuka/Projects/asuka/go/go-tron/build/benchmarks/20260906-disk-audit)。`observed-physical.json`、`pebble-observed.json` 是从可见终端结果转录的结构化记录，明确标注来源，不伪装成原始机器导出。服务器原始 helper 输出位于 `/dev/shm/gtron-disk-audit-20260906`，属于内存文件系统，重启后不保留。

实际使用的只读工具：[pebble_readonly.deployed.go](/Users/asuka/Projects/asuka/go/go-tron/build/benchmarks/20260906-disk-audit/pebble_readonly.deployed.go)，SHA256 `09ad201fed851823b997c4e765258dbbdb12a719c2f7e812776967e69c2e8a6a`；[pebble_split_readonly.go](/Users/asuka/Projects/asuka/go/go-tron/build/benchmarks/20260906-disk-audit/pebble_split_readonly.go)，SHA256 `748e4c9a2591e30e1017673a091a6e8485e9b151f41e73ef65aadfc8e9511bad`。两者本地编译通过，服务器源码指纹通过，生产只读执行 exit 0。快照实体盘点脚本也保留在证据目录。

本次完成的是现场量化、源码机制核对和分阶段处理方案；未执行扩容、数据清理、迁移或新维护工具开发。没有逐条扫描整个热库，也没有完整认证所有冷段内容，因而不宣称已完成整库逻辑完整性检查。新离线维护工具和任何实际删除动作都应按上述验证门落地，节点无需启动同步。

最终现场复核为 **14:56:38 UTC / 北京时间 22:56:38**：主网、Nile、部署 timer 仍 inactive/dead，未发现 gtron PID；可用空间 123,966,521,344 B（约 115.45 GiB）。manifest SHA 与最初盘点完全一致。生产 P2f 构建目录的 `core/state/pruning/lifecycle.go`、`core/state/snapshots/cold_builder.go`、`cmd/gtron/history_config.go`、`cmd/gtron/db_compact_state_history.go`、`core/rawdb/pebbledb/pebble.go` 五个文件 SHA256 均与本次本地审计源码一致，避免把未部署机制误当成线上实现。
