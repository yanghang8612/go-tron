# 独立待归档历史暂存层

2026-10-01。设计稿；本轮只定义协议及一次性离线迁移接口，不改代码或线上配置。目标是在 canonical block、最新状态、stage 与可回滚历史仍由原 `chaindata` 的同一原子写入链提交的前提下，把**已稳定、尚未归档的历史 payload** 按完整 1024 高度共享桶转交给另一 Pebble。现有线上数据推荐在停机窗口由独立脚本一次性迁移；正常节点启动不懒迁旧库。迁移成功后的 source 路由模型与从创世使用新版节点同步的模型相同：原库只保留当前未成熟的近期尾部，完整且稳定的桶随后按同一 handoff 协议交给暂存库。近期尾部和 genesis 例外不能也不应假装已经移出。冷文件、历史查询与回滚看到的值必须与迁移前逐字节一致。首版仍保留现有 v3 shared pack/chunk 和旧 raw/Snappy/v2、正序号 repair 格式，不展开共享片段或改变共识。

## 已核对的边界

- `core/rawdb/state_changeset_shared.go` 的 v3 pack 引用桶内 chunk；`state_history_read_view.go` 要求 pack、repair、chunk 属于同一 pinned view。`state_changeset_shared_gc.go` 仅在 fresh committed store 确认整桶 changeset 物理范围为空后，才以同库 batch 删 chunk 并写永久 retired marker。跨库路由不能复用这个 GC 的授权。
- `core/state/snapshots/history_stream_build.go` 用同一 view 完成 dictionary、records 和 tx-range 三遍；`cold_builder.go` 还在构建前以热 tx-range 定位边界。`core/tron_backend.go` 的 archive 会话把 head、flat latest、buffer 历史绑定于一个快照。跨库后都需要显式复合读视图，不能把 `db` 指针改为新库。
- `core/state/pruning/worker.go` 当前先基于已发布冷覆盖裁剪热 changeset，并保留 snap/archive 的热 tx-range；`StageSnapshotHotPrune` 只表达该路径的已验证冷清理，不是移交水位。全局 posting/directory 按 key/tx 跨块组织，`StateChangeIndexPruner` 在冷覆盖后另行清理；不能按 1024 高度直接搬迁或删除。
- `core/blockchain_rewind.go:canIncrementalUnwind` 用 `height+1` 的热 tx-range 存在作为整段 changeset 覆盖的代理。移交后它失效；增量 unwind、fork switch、reset/replay 与 repair 必须先知道每段唯一来源。现有 guard 不覆盖并发离线修复/restore。
- Pebble wrapper 提供 `SyncKeyValue()`，以同步 WAL 保证此前写入持久。首版必须在两库各自使用并检查错误；**不存在跨两个 DB 的原子 batch**。文件系统冷 manifest 的发布/lease 又是第三种持久边界。

## 数据范围与不变量

原库继续持有 canonical chain、flat latest、stage、全局 posting/directory、热 tx-range 和现有冷文件 manifest。暂存库首版仅保存移交桶中仍需热源的完整原始 changeset 键值（包括 seq0 与所有正序 repair、异常但合法的历史行）、其引用的桶内 chunk、桶 metadata，以及每个高度的完整 tx-range **副本**与认证 receipt。已经按现有规则由冷文件覆盖并从热库清除的高度，在 receipt 中记录对应冷覆盖证明；没有 seq0 不能直接判损坏，因为合法块可以没有 changeset。不得复制或删除 canonical block、state、stage、posting/directory，也不得因移交提前删除原库 tx-range。因而目标是控制原库历史 payload 的增长，**不是承诺整个 `chaindata` 有界**。默认目录建议为 `datadir/gtron/history-staging`，可配置其他物理磁盘；它与原库及冷目录路径必须互不包含。新 Pebble 使用独立 metrics namespace，不以 `rawdb.NewPebbleDB` 的默认空 namespace 重复注册原库指标；最终路径/namespace 由实现阶段固定并测试。

桶 `b` 对应 `[1024b, 1024b+1023]`，只接纳完整桶；当前尾部不足 1024 不接纳。**首版跳过 bucket 0**：它是 `[0,1023]`，常规历史 tx-range 从块 1 开始，不能伪造 genesis block 0 的 tx-range，也不能把 `[1,1024]` 当作一个共享桶。后续若要支持它，须单独证明 genesis 语义并测试。迁移前后原始 key/value、块 hash、tx-range、顺序、存在性和 Prev 字节不变。每个桶在一个 source epoch 中只有一个 hot payload owner：`source` 或 `staging(receipt ID)`；冷层可为已覆盖的子区间提供最终来源。任何缺 receipt、错 epoch、缺 chunk、损坏 checksum、重叠且不等值、缺少可证明的空高度都返回错误，不能退回到另一个库碰运气或把缺失当空值。物理键值复制的长度/SHA/行数清单不是压缩内容的逻辑认证；既有共享 pack、chunk、RLP、cold trio 的完整解码和 SHA 校验继续执行。

## 现有数据的一次性离线迁移

推荐将现有热历史积压在节点停止、自动部署暂停且三处目录独占锁到位后，用专用新版本 `gtron db history-staging` 子命令就地逐桶移交。外层 shell/Python 脚本只编排 systemd、锁、binary 身份、JSON 结果及恢复；**不直接解析或写 Pebble**。现有库**首次正式启用** staging reader 前必须完成此流程和成功出口；可以预先构建 capable 候选，但自动部署不得在迁移前发布它并启动旧布局。正常节点启动只做 route/receipt/cold manifest reconciliation 并按唯一来源读，不能扫描旧热库作 lazy backfill，不能在 route 缺失时 source-first/staging-fallback 双读。新创世节点仍先在原库完成 canonical 原子 writer；稳定桶的近尾 handoff 是正常、必要的短窗口，并非旧库兼容模式。旧库存量在启用统一运行模型前完成可迁部分；合法 cold-only、bucket 0、retention 内或不完整尾桶可以零搬迁。

CLI 设计为 `gtron db history-staging inspect|migrate|apply|resume`，四者共用运行时的 bucket receipt/epoch/claim 状态机而非另一套离线复制器：

- `inspect` 只读锁定原库、暂存库和冷 manifest，输出 machine-readable 库存与一致性：每桶 `source-hot`、`adopted`、`source-cleared`、`cold-only`、`mixed-cold-proof`、`tail-ineligible`、`genesis-skipped`、`invalid` 分类，首末高度/hash、确切 payload/chunk/tx-range 行数与字节、冷覆盖 generation、claim/receipt/route、空间与错误。它不把已冷裁剪的空源判为缺历史。
- `migrate` 生成只读计划及 plan ID：冻结 source HEAD/epoch、solid/Finish/index、retention/flush、manifest generation 与当前 eligible 完整桶集合，同时枚举已冷裁剪桶、bucket 0 与近期尾桶的确定路由。按桶给出最大单行/总值、目标 WAL/SST、双库压实及最低剩余空间的保守估计。`--max-buckets`/`--max-work-bytes`/`--min-free-bytes`/deadline 有上限，溢出或无法准确盘点即拒绝。默认不修改 DB；计划不等于执行授权，计划过期必须重算。
- `apply --plan-id` 执行已核对计划中的 eligible 桶，遇版本/证明变化重新规划或 fail closed；按 `TARGET_READY→ADOPTED→SOURCE_CLEARED` 逐桶 checkpoint，JSONL 进度和最终 JSON 只记录已持久的状态。命令本身无需交互式 `--yes`；一次性脚本可以在明确启用的迁移任务中直接调用。`--max-buckets=0` 可表达处理所有当时 eligible 桶，但每批/每桶及运行时磁盘/内存额度仍有硬上限。
- `resume --job-id` 先重开两库并逐个 reconcile source route、claim、target receipt、cold proof，恢复中断桶的 copy/adopt/delete；不可在未核对唯一副本前跳到下一桶。已完成的桶幂等 no-op，orphan 不作为可读历史；不同计划的身份冲突拒绝。`apply/resume` 还须持久化统一 route 初始化屏障：eligible 且实际搬迁的桶指向 staging receipt，已冷裁剪桶按完整冷证明指向 cold，明确保留的 bucket 0/近期尾桶指向 source；不存在“无 route 就试读旧库”的兼容分支。`inspect --verify-complete --plan-id` 作为成功出口，必须证明冻结时所有 eligible source-hot 桶均 `ADOPTED+SOURCE_CLEARED` 或按原 cold proof 为 cold-only，剩余仅明确的 genesis/recent/partial/不可接纳项，并验证初始化屏障、每类路由的唯一读源以及查询、索引 ETL、冷构建的同一 `HistorySource` 能力；发现未知缺口、长期占用 claim、未清源、损坏 target 或源路由矛盾即非零退出。零 eligible/零实际复制可成功，但仍完成全库存和读者一致性检查。

离线脚本先固定经原生 Sapling 测试的新 capable Linux binary SHA/source/feature marker，且**预先安装并验证** systemd 启动前、`start.sh`、root-owned release helper、自动 rollback 和 space guard 的持久 latch/capability 检查；旧 binary 本身不认识新 marker，不能指望它主动拒启。记录当时 service/timer 的 active/enabled 状态，暂停 `gtron-deploy.timer`，停/等待已运行的 `gtron-deploy.service`，取得与 `start.sh` **相同的** `/data/gtron/start.lock` 排他 `flock`。在持锁且确认无 deploy 作业后，原子落盘跨重启生效的 `MIGRATION_IN_PROGRESS` latch（含 chain、source、target、job、候选 SHA 和原服务状态），阻断普通启动及旧 binary；再停止 `gtron.service` 并确认无进程，取得原库、暂存库、冷目录的排他锁。服务停止至迁移结束不释放维护锁；不能只依赖脚本 PID、内存锁或 `flock`。现有 `start.sh` 的“已配置源但不健康即重启”分支不能在此窗口运行，timer 停止不代表已有 deploy.service 已结束。脚本调用上述 `inspect→migrate→apply/resume→inspect --verify-complete`，不复制整库、不删除目录；可用按桶 checkpoint 在断电、SIGTERM、磁盘满后续跑。每次 CLI 重开都核对 latch、链身份、source/target 路径与候选 SHA，拒绝仅凭旧 plan ID 跳过准入。任何阶段失败都保持节点停机、自动部署暂停和持久 latch 生效，输出失败桶/持久阶段/下一条 resume 命令，不用 shell `trap` 自动恢复，不自动回滚旧 binary 或清理唯一副本。仅成功后安装/验证新 release，将 latch 转为 `VERIFIED_PENDING_ACTIVATION`；它只允许经 SHA 固定的 capable binary 按原 service active 意图启动，其他普通启动/旧版 rollback 继续拒绝。原 service active 才进行新进程 startup reconciliation 与健康检查；原 service inactive 则用离线 verify 证明一致而不强行启动。验收完成后原子写完成标记、解除迁移 latch，并按原先 active/enabled 意图恢复 timer/service；原 timer active 而 service inactive 时，timer 必须尊重持久 stop-intent 并继续 no-op。原 disabled/inactive 的 timer 不擅自开启；不改变 space guard、Nile 或 Java。脚本/CLI 规格在本设计中定义，本轮不放置可误运行的占位脚本。

离线 CLI 只打开持锁的存储与只读校验依赖，不启动 P2P、同步器、producer、archive worker 或 node 生命周期。停机、排空所有历史 writer、独占原库/目标库/冷目录并确认原库已 flush 的证明，替代在线 `HistoryPrefixSettled` 对内存 overlay 的证明；**不能在无 live buffer 的 CLI 中伪造该返回值**。canonical/solid、hash-bound durable Finish 与 StateHistoryIndex、retention、冷 manifest/裁剪证明仍须逐桶验证；缺任何一项即不准移交。任务重开时重新验证独占与持久身份，不能把“进程已停”推断成所有 DB 写入都已安全落盘。

## 准入与三条持久边界

移交 eligibility 是每次重新计算的证明，不是水位：完整桶末高度须在已验证 canonical/solidified、hash-bound durable Finish、必要的 StateHistoryIndex、已 flush 的 source 前缀及配置的 hot retention 外。持现有 index→chain guard，检查取消、commit/flush error、`HistoryPrefixSettled(last)`，对照每个 tx-range 的 canonical hash 与连续 tx 区间；读取原库的单一 pinned snapshot。若前半桶已由冷清理移除，只在当时已认证的完整冷覆盖和既有热裁剪证明下接受该缺口，并登记下述可重绑定的持久冷依赖；不能把当时的 manifest generation 当永久地址。不可把 256 块诊断导出 API 当作 1024 桶移交工具或归档证明。

记录三种**相互独立**、每桶有 receipt 的持久边界，派生连续前缀仅用于进度展示：

1. `adopted`：原库 route 指向已持久并核验的暂存 receipt，热 payload 可从原库删除；不是冷发布。
2. `cold-certified`：对应的完整冷文件 trio 已按现有 manifest/StageSnapshotBuild 协议发布、可读并与桶 tx-range/块 hash 绑定；仅满足它不能删暂存。
3. `target-reclaimed`：route 已持久转为已验证冷来源，所有 source/target/manifest 依赖和读 pin 已关闭，暂存物理键被逻辑删除且经复核；不代表 SST 空间已经释放。

冷发布可先于移交，三数值不能简单要求 `adopted ≥ cold-certified ≥ target-reclaimed`。资格必须按 bucket receipt、canonical epoch 和覆盖区间判断，不能凭一条全局高度或 `StageSnapshotHotPrune` 越过洞。reorg/rewind 换 epoch 后旧水位只作历史记录，不能授权新分支读取或回收。

`TARGET_READY` receipt 不可变，保存复制时的历史认证事实及当时冷缺口证明；**当前可读冷文件的依赖**另存原库 source route 的版本化 cold-binding。binding 精确列出所覆盖子区间、canonical 首末 hash、tx-range 摘要、冷 trio 的语义内容身份、当前 manifest generation/文件内容身份与 route epoch；mixed-cold 和 cold-only 均登记，即使没有 RPC pin 也构成持久 GC 依赖。按文件内容身份→bindings 的有界反向索引记录受影响路由；正常新增 publication 未替代其引用文件时，原 binding 不变，无需全库扫描或重验。替代 merge 发布新 manifest 前保留旧文件；发布后若 binding 尚未持久同步，旧文件和对应 retired metadata 仍受 pin 保护。以经审计且包含内容/tx-range/行数契约的 merge transition 认证证据，或在该证据缺失时完整逻辑解码/内容校验，证明新 trio 在相同区间、canonical/tx-range 与语义内容上等价后，才按反向索引有界批次在原库原子更新 route binding/索引并 `SyncKeyValue()`；随后等待旧 view lease 退出，才允许旧 trio 及 metadata GC。同高度覆盖、文件名、物理 SHA 或 generation 自增都不足以证明等价。若 G+1 已发布但 binding sync 前崩溃，启动先用保留的旧依赖与当前 manifest 重新认证，幂等 rebind；认证失败继续保留旧文件、旧 binding 并阻止相关回收，不能直接把绑定改指 G+1。未完成 rebind 和必要的完整逻辑核验会占空间/I/O；依赖积压或空间高水位须暂停新的冷替换/移交接纳并报告，不能无限保留每代文件或绕过认证。

## 有序移交和恢复状态机

状态为 `SOURCE → COPYING → TARGET_READY → ADOPTED → SOURCE_CLEARED → COLD_RELEASED → TARGET_CLEARED`；所有状态带 bucket、source epoch、首末 canonical hash、tx-range 起止/摘要、物理键集摘要、目标 receipt ID、复制时冷证明身份。当前冷文件依赖由单独可重绑定的 route binding 承载，不能把 receipt 里的原 generation 用作永久读取地址。重试须按同一 identity 幂等；冲突/未知版本 fail closed。

1. 在短 guard 内取得 sealed source/route epoch 的**桶级 transfer claim**（持久 ID、写版本、route epoch）与 pinned source view；现有热裁剪、共享 chunk GC、repair 和 rewind writer 都须协调 claim，延期或取消冲突作业，不能持 chain 锁等待复制结束。claim 有启动恢复与取消释放规则，不依赖进程内布尔值来证明重启后的源稳定。源快照下的完整键集摘要在锁外计算并写入目标 receipt；claim 的不可变写版本保证该源快照可用于随后采纳。然后在锁外按严格 key prefix/高度边界复制到暂存私有键空间。以有界 batch 写入，不改原库。对原库已冷清理的子区间携带已核实的冷覆盖证明；其余高度必须有真实 tx-range，允许零个 changeset。复制 exact key/value 清单、每行 SHA、整体有序摘要和完整桶边界。暂存写入后 `SyncKeyValue()`；在目标 pinned view 复读所有复制键并比对清单，继续现有逻辑解码认证。然后写 `TARGET_READY` receipt 并再次 `SyncKeyValue()`，任何错误只留下可清扫 orphan，绝不删源。
2. 再持 index→chain guard 与短 route publication lock 重验 claim ID/不可变写版本/route epoch、canonical、Finish/index、solid/retention、settled/flush 与目标 receipt 的摘要/identity；这只有有界 metadata/proof 读取，**不在 chain 锁下重扫或 hash 整桶 payload**。冻结该桶旧高度新写。原库**单一原子 batch** 写 route=`ADOPTED(receipt,epoch)`、源桶永久 retired marker 和 source deletion intent，并结清 claim；在同一 routeMu 边界向本进程读者发表新 generation，随后立即释放 routeMu，再调用原库 `SyncKeyValue()`。可在有限 guard 阶段完成 sync，但不持 routeMu 等 fsync；同步失败禁止任何源删除，目标 receipt/任务 lease 保留到重启或重新同步确认。route 可能在 sync 前对并发读可见，但目标已经 durable；崩溃时源 batch 要么恢复为旧 route 且完整源仍在，要么恢复为新 route 且目标已在。绝不能先删除源再持久化 route。
3. 在 route 已 durable 后，用有界批次删除原库该桶 changeset/repair 与 chunk；留原库 tx-range、route、retired marker。每次失败都按 receipt 重试。fresh source iterator 核对整个 changeset 范围及 chunk 前缀确实为空，再持久化 `SOURCE_CLEARED`。若原库 route 缺失或 receipt 不匹配，不继续删除。DeleteRange/tombstone 只说明逻辑删除；旧 MVCC 快照和压实会推迟物理回收。
4. 冷 builder 可从暂存复合视图构建现有 trio。完整验证、fsync/manifest 原子发布、目录 fsync 和原库 StageSnapshotBuild hash-bound 进度遵循原协议；阶段之间崩溃由现有 reconciliation 修复，不能因暂存 receipt 自行推进 cold stage。只有冷覆盖、语义内容和持久 cold-binding/source dependencies/读 pin 再核实，才在原库持久切 route 到 `COLD_RELEASED`。等同 epoch 的旧读者/构建器/repair lease 全部退出，再删除目标键；fresh target iterator、source route 和 manifest lease 复核后写 `TARGET_CLEARED` receipt。目标删除失败保留可重试数据，不影响冷来源。普通新增冷发布登记其新 binding；替代 merge 和 retired metadata GC 对受影响的旧文件执行上述等价 rebind/保留规则。

启动恢复先打开并锁定两库及冷目录，再服务查询/维护。对 `COPYING/TARGET_READY` 无源 route 者保留源、校验后重试或安全清 orphan；`ADOPTED/SOURCE_CLEARED` 者要求目标 receipt 与全部所需行存在，继续删源或冷构建；`COLD_RELEASED` 者先核对完整冷覆盖与 lease，再重试目标 GC；`TARGET_CLEARED` 者仍保留源 route/epoch 墓碑。任何路由指向不存在/损坏的唯一副本、canonical epoch 分叉、部分冷 trio 或两个源相互矛盾时暂停相应读和维护并要求显式修复，不能自动选择“看起来还有数据”的库。

## 读视图、冷发布与旧高度写入

引入统一 `HistorySource`，所有块/tx-range/按 key/按 prefix/批量第一变更、posting 候选复核、冷 builder、历史 RPC、unwind、诊断与 repair 经它访问。一次调用先取得 route generation lease；在固定锁序下捕获原库 head/flat/buffer pinned snapshot、目标 pinned snapshot 和当前冷 manifest generation lease，确认 route epoch/receipt 后释放短锁。锁序写为已有 `index → chain → route`，读为 `chain → route`；route/target GC 不反向等待 chain。后续迭代全程使用这组固定视图，按 bucket 路由且保持 deterministic tx/seq 次序；不能拿新目标快照配旧 source route，不能在 pack 解码时回到 live `Get`。route 切换、冷 manifest 安装和 GC release 与 view lease 注册共享同一短协调边界，build/完整校验/compaction 在边界外执行。GC 须等待所有可能读旧 owner 的 lease 退出；异步调用退出时必须 join/release，不得遗弃 pin。

冷 merge 的新读者先在任何 index/chain/route 锁外取得可取消的 publication Read admission，只持到 route、热/暂存与冷 manifest/file lease 的一致捕获完成；已捕获的长读只靠 file lease 继续。merge 的昂贵构建不占 Write admission，但 `Integrate G+1 → 受影响 binding 的语义认证和有界批次 Sync → 切换可服务代` 整段占 Write admission。若发布后同步失败，同进程后续后台 pass 或重启恢复必须在 admission 重新开放前完成认证；失败期间拒新捕获并保留旧文件。统计新读捕获等待时间，不能把整次冷构建放在门内。

全局 posting/directory 留原库。其候选可指向 source、staging 或已冷覆盖的高度，仍需到唯一 authoritative changeset/冷文件复核；按高度/tx 窗口合并、去重并保持次序，不能因原库点查 miss 跳过 stage 或因 stale posting 当成历史。现有 posting ETL 与 sweep 水位只有在所有查询/构建读者改用复合源后才允许原库 payload 删除。旧单库诊断在暂存启用后应 fail closed，不输出虚假的“缺失历史”。

`canIncrementalUnwind` 的 tx-range proxy 必须替换为整个回滚区间逐块/逐桶的显式热源覆盖证明；**首版只在 source-owned 且未裁剪的完整区间允许增量 unwind**。跨已移交桶的深 rewind/fork switch 才进入下述 reset+replay；普通 fork 仍受现有 solid/共识边界约束，运维 reset 不能变成接受非法深分叉的途径。

现有 `ResetMutableState` 会清理**整个**可重放历史及 stage/latest，不只是 rewind 尾部，且跨多个 batch；因此首版 reset 在首次破坏性写入前必须逐块取得并校验 `[1,replayTarget]` 的全部必需 block/body 与预定 canonical hash，缺块先失败且不修改库。随后在不被 `ResetMutablePrefixes` 清理的持久命名空间写入并 sync `RESETTING` intent，固定旧/新 epoch、目标高度/hash、重放终点、完整 block/hash 来源、冷可见性失效范围及“从头安全重做”策略。intent 生效即禁止普通启动、查询、冷构建、移交、归档和维护；只允许持锁的 reset/resume 路径。旧 `[1,replayTarget]` source/staging/cold owner 及旧 receipt 全部隔离到旧 epoch，旧冷文件及 binding 在重放期间不可见但保留到旧 view pin 退出和新 epoch 完成认证，不能只隔离回滚尾部。重放从 genesis 开始在原库构建新 epoch 的明确 `SOURCE` route 和 history/tx-range/stage/latest；该 epoch 的已就绪范围仅到已持久重放进度，未就绪高度不读旧 owner。首版不做复杂 checkpoint：任何中断都凭同一 intent 和保留的 canonical block 来源，安全重做完整 mutable reset、genesis 与 `[1,replayTarget]` 全量重放；intent 在重做期间不能被 reset 清掉，旧 owner 不得被提前 GC。重放期间禁止正常 handoff 与冷归档，完成后才可重新按稳定桶规则运行。最后校验全段 block hash、hash-bound Finish/Index 与其他 stage/root、history/tx-range 和新 epoch route 覆盖、冷可见性一致性，sync 原库与必要的持久边界后才写并 sync `RESET_COMPLETE`，放行普通启动/查询/维护；任何未完成状态都保持 `RESETTING` 并明确 resume，不靠人工猜缺口。在线 repair 遇已移交高度先拒绝；独占停机 repair 若需重写该高度，须验证并完整 rehydrate 受影响桶到原库（包括 chunk、repair、tx-range），持久化后原子切 route 回 `SOURCE` 的新 epoch、处理冷尾，再允许 canonical writer。原桶 retired marker 不能简单清 flag；恢复 v3 写资格须证明新桶元数据/引用完整，否则继续 self-contained fallback。不得把新的正序 repair 行悄悄叠在热库、同时继续从暂存读旧 seq0。

`RESET_COMPLETE` 后先将旧 active state-domain 冷 trio 以新 manifest generation 持久隔离到 Retired，新 epoch 的冷 builder 和 merge 输出使用 epoch 私有物理路径，避免覆盖旧目录或已 pinned 文件；再恢复普通读/归档。旧 epoch target payload、cold refs 与热库 per-block receipt/binding/claim/high route 分别通过独立持久游标有界回收，均需无旧 view 和冷尾隔离证明。当前 SOURCE 完整性 receipt 保留到源移交或冷认证后的源清理；已清源桶的 receipt 可有界删除。在线 reset 请求须明确拒绝，不能以运行中自动 replay 绕过离线维护边界。

## 资源与运维门禁

每桶是额外的 source read + target WAL/SST write + source tombstone/后续 compaction read/write + 可能的目标 GC；目标与原库在同设备共享真实带宽，移交不能宣称天然加速 cold build。就地搬运**不是零冗余**：至少同时容纳当前桶的目标副本/校验与 receipt WAL，以及原库和目标库压实的临时空间；copy 前按最坏有界桶大小、WAL/SST/compaction allowance 预留并检查文件系统低水位。空间不足时幂等暂停而不删除源；若长期接近满盘，须扩容或先做经验证的离线回收，不能假定搬运可自行腾出空间。移交 worker 与当前 cold Runner `passMu` 生命周期独立，copy/verify 在 index/chain 锁外；捕获 sealed source/route epoch proof 后，adopt 前重验有界 claim/guard 证明，只有 capture/adopt/分批 delete 的短闭包进入现有 guard。不能让一次 1024 桶 copy 或冷 merge 长时间阻塞对方；移交独立不代表有第二份无限 heavy-work 额度。引入同设备公平的总 I/O token/队列和两引擎各自 WAL、L0、compaction debt、stall/错误硬保护；热导入优先，暂存只在独立压力及全局余量均满足时接纳。32 GiB 是两库、冷构建、OS cache 与其余进程的总预算，不给新库复制一套 4 GiB cache/默认全核 compaction。初始测量候选可把现有 4 GiB Pebble cache 总额拆为热 3.5 GiB、暂存 0.5 GiB，限制暂存 memtable/压实并发；数值须用相同输入及 host/cgroup 峰值、WAL/SST、write stall/设备物理 I/O 压测确认。未取得线上字节/带宽实测前按 `copied bytes / measured effective bandwidth` 估算搬运窗口，不猜块数工时。停止/取消/IO错误保留当前安全状态，不绕过既有 hard admission 追求吞吐。

在线移交与后续 retirement 使用各自持久有界 cursor：每轮最多扫描 256 桶，不能让早期未完成桶永久阻断后续纯热晚发布桶，也不能漏掉 mixed partial 桶后来变成 full-cold 的目标释放。Target→Cold 认证比较真实逻辑行、修复覆盖优先级和 Prev 字节语义；仅相同高度、tx-range 或压缩物理 SHA 都不足以放行。缺行或 Prev 改动保持原 claim 和旧冷 ContentID 依赖，重试仍须拒绝，不能通过丢弃旧证明把唯一正确旧 trio 回收。压力探针未知即不准入；上线后分别观测 source/target/cold 字节与搬运、归档吞吐，隔离 LSM 不增加共享设备物理带宽。

新 route 首次可读且尤其首次删源前，必须有独立 staging-reader format marker 与 deployment helper 的运行二进制 capability 检查；阻止自动 rollback 至 `889e96b7` 或其他不认识暂存的 reader。停机 `db inspect`、repair、export、backup/restore、bootstrap、reset 必须同时识别和锁两库及冷目录；旧单库工具默认 fail closed，不能报告缺数据，也不能把单库备份当完整恢复点。允许只读单库 `du` 作容量观测。若要回滚旧 binary，必须先完成受影响全桶完整 rehydrate、逐行认证、恢复热覆盖与 route/marker 撤销的受控协议，不能只关功能开关或删除暂存目录。现有积压采用就地逐桶搬运；无需整库备份，也不假定删库。可安排受控停机窗口，但重启恢复必须验证上述状态机。

首版**不**分离 cold build 与 merge 调度、不迁 posting/directory、不重写历史格式、不引入双库原子提交。任何净收益以相同输入的真实 cold publish 进度、hot/stage 各自 allocated bytes、总设备物理 I/O、CPU/内存和重启恢复为证；逻辑 delete 数、cache hit 或单独桶搬运次数都不是释放空间或吞吐收益。
