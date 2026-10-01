# History staging 设计独立审计

2026-10-01，Astra。**最终复审结论：可开始分阶段实施。** 首轮结论为“需修订后实施”，原 P1/P2 已由下述修订关闭；首轮发现和依据保留在本文，复审详情见末节。此次仅审计本地设计及相关代码，没有运行生产采样、迁移、部署或故障测试；没有修改实现或被审计文档。设计通过不代表实现或生产验证通过。

审计对象：

- `docs/superpowers/specs/2026-10-01-history-staging-store.md`，SHA256 `88c38d6935c5b120e93329127b6882f2a46e502d7da15752e92126ef80aebbef`。
- `docs/superpowers/plans/2026-10-01-history-staging-store.md`，SHA256 `cc358095e2855582d7d41bc653139543e41d37caf18eebced391e79727291f60`。

## P1：reset+replay 缺少持久恢复屏障及整个重建范围的路由规则

位置：规格第 63 行；计划第 38 行。

代码证据：`core/blockchain_rewind.go:279` 调用 `ResetMutableState`，第 298 行从块 1 开始重放。`core/rawdb/reset.go:16` 清理的是**所有**可重放状态，而非 `(目标高度, 原 head]`：先删除全部 changeset/chunk/bucket metadata，随后分多次写删除 tx-range、posting、stage、latest 等命名空间；这不是一个跨库、跨 manifest 的原子重置。现有代码不提供新 staging epoch/route 的 reset journal。

可复现执行序列：已有低高度桶 A 为 staging-owned、近期桶 B 为 source-owned；一次跨已移交桶的 rewind 进入设计规定的 reset+replay。只按文档增加 epoch/quarantine、失效受影响冷尾，然后调用现有 reset；在删除热 history、但 tx-range/head 等还未全部清完时 kill/reopen。旧 source route 已没有 payload，新 epoch 的 source 覆盖也尚未建立；普通启动的 route reconciliation 最多能拒启，无法由现有移交状态机判定应继续哪一次 reset。即便不中断，重放必经 A 的历史高度；若只重置 rewind 尾部路由，原 staging-owned writer fence 会拒绝这段合法重放，或仍把新热写屏蔽在旧 owner 后。笼统要求“扩展 reset 协议”还没有确定这些持久状态的合法组合。

最小修订：在首次破坏性写入前持久化 `RESETTING` intent，固定旧/新 epoch、目标 canonical hash、重放终点及冷失效策略；它必须阻止普通启动、查询和维护。明确整个 `[1, replayTarget]` 的路由重建规则，而非仅 rewind 尾部。首版可选择把全部旧历史 owner quarantine，让重放按新 epoch 的 SOURCE 路由构建，再按正常规则 handoff；旧冷文件是否保留须有明确可见性和重新认证规则。reset/replay 具有可幂等恢复阶段或可安全重做的检查点；只有重放、hash-bound stages、路由覆盖和冷尾一致性验证后才持久发布完成并放行。所需不可再取得的 block 数据应在破坏前验证，或由 intent 保留足以恢复的来源。

验收：在 intent sync、epoch 切换、history references 删除、tx-range/stage/latest 删除、genesis 写入、每个 replay checkpoint、cold 失效及完成屏障前后分别 kill/reopen；含多个保留前缀 staging 桶、cold-only 桶与 SOURCE 尾桶。每次均恢复到同一目标，或保持有明确 resume 路径的 RESETTING，不能靠人工猜测缺失数据。普通 fork-switch 仍必须遵守既有 solid/共识边界；运维 reset 不能成为接受非法深分叉的捷径。

## P2：长期 cold 路由/混合桶证明缺少 manifest 换代重绑定规则

位置：规格第 28、48、53、55、59 行；计划第 19、25、37 行。

代码证据：`core/state/snapshots/aggregator.go:637` 的 `integrateWithManifestMode` 在常规 publication 中增加 generation，并把被替代 segment 放入 Retired；`retired_metadata_gc.go` 允许回收后的 retired 元数据被进一步清理。generation 不是永久不变的历史地址。

可复现执行序列：迁移一个前半已热裁剪、后半仍热的完整桶，receipt 的前半缺口依赖冷 generation G；或初始化 cold-only 路由。结束迁移且所有查询 lease 已释放后，正常 build/merge 发布 G+1 并替换、回收 G 的 trio，再重启。持久 receipt 仍依赖 G，但启动只取得当前 manifest lease。严格按 generation/旧文件检查将把正常压实判成证明丢失并拒绝读/恢复；若实现自行只看 G+1 的覆盖高度，则没有指定的内容、canonical/tx-range 等价认证。当前“所有依赖与 pin”原则允许正确实现，但未说明持久 receipt 本身如何登记依赖及如何安全退出，查询 lease 不能替代它。

最小修订：选定一种规则写入格式契约：例如 immutable receipt 保存历史认证事实，source route 另存版本化 cold-dependency binding；当前 manifest 的替代 trio 经内容/范围/canonical/tx-range 等价认证后，将 binding durable 更新，之后才允许删除旧依赖。持久依赖须在重启时重建，包含 mixed-cold 和 cold-only，不能只统计活跃 RPC pin。另一可行选择是显式固定旧文件直到依赖释放，但必须计入空间预算，不能无限保留每代文件。定义 manifest 先发布、binding 尚未 sync 时的恢复及保留旧文件规则。

验收：mixed-cold 与 cold-only 分别经过一次普通新增 publication、一次替代 merge、旧文件 GC 和重启；在每个 rebind 持久边界注入故障。旧 generation 消失不应使正常历史永久不可读，新 generation 仅有同高度但内容/hash 不符必须拒绝。

## 不阻断的文档对齐

规格第 30 行在暂停/排空 deploy 并拿 `/data/gtron/start.lock` 后落迁移 latch；计划第 44 行先落 latch 再暂停/排空。请统一为同一协议，并明确拿锁前的失败仍属未进入迁移，不承诺已停节点。计划第 46 行也应直接引用规格已有的 `VERIFIED_PENDING_ACTIVATION`：原 active 服务在此状态用固定 SHA 启动验收；原 inactive 服务离线验收。否则只看计划会在 latch 阻止启动时无法完成 startup reconciliation。这些已在规格中有可用方案，不另列为新增架构问题。

## 已通过的设计检查及实施边界

- `SyncKeyValue` 的实际实现是向 WAL 写同步 LogData。目标 payload sync、复核、receipt sync，再 source route batch/sync，最后 delete 的正常顺序没有发现唯一副本丢失窗口。source sync 失败禁止删源及保留 target lease 是必要条件，文档已包含；目标 GC 的 cold proof + durable source release + pin 退出条件同样正确。以上是协议审计结论，不代替真实 Pebble 掉电测试。
- 复合 view 固定 head/latest/buffer、两库 snapshot、route generation 与冷 lease；锁序 index→chain→route、route 锁外 fsync；claim 协调 prune/shared-GC/repair/rewind，锁外 copy/hash，短 adopt 校验，均覆盖当前单库读者的主要风险。posting authoritative 复核、ETL fallback 对 repair precedence 的迁移已纳入计划。
- 首版保留 hot tx-range，因此必须替换增量 unwind 的存在性代理；整段 source-owned/未裁剪证明及跨 stage reset 的选择明确。bucket 0、合法零 changeset、mixed-cold、durable Finish/index/solid/retention、无 live buffer 的离线资格都没有被误简化。
- 既有库必须离线移交、统一 route 初始化，fresh writer 仍同库原子提交；不 lazy backfill、不双读碰运气，满足统一运行模型意图。旧 binary 不认识新 marker，规格已正确要求外部启动/release/rollback fence，而非依赖旧 binary 自觉拒绝。
- 同设备 I/O 总额、双库 cache 与 32 GiB 总预算、临时空间 reservation、逻辑 delete 不立即回收、无整库复制、成本按实际 bytes/bandwidth 测量均已明确。不能从本设计推出吞吐一定提高。

修订上述两项恢复契约并同步计划后，可进入分阶段实现；上线仍需按原计划完成原生 Sapling、真实双 Pebble 重开/故障、读写竞态、系统编排和容量验证。

## 定向复审与最终结论

2026-10-01，Astra 对原 P1、P2 以及 latch/activation 文档一致性进行定向复审。实际读取文件 SHA256 与本轮冻结版本一致：

- 规格：`cbe3ef81e5394b4bbcb9882f46ea8b272ffacaf8f8f7dabada08bcbae541104d`。
- 计划：`a741581b919af2379cf7739284f32189e3b4eda52f46ccefa9331d0b22b3c141`。

**P1 已关闭（设计层）。** 规格第 67 行与计划第 38 行现在明确：破坏前逐块验证并保留 replay 来源；在 reset 不会删除的命名空间 sync `RESETTING`；禁止普通服务和维护；隔离整个重建前缀的旧 owner；用新 epoch 的明确 SOURCE 路由从 genesis 重放；中断后依据同一 intent 从头安全重做；完整路由、历史、stages/root 与冷可见性认证并 sync 后，才持久发布 `RESET_COMPLETE`。这消除了首轮指出的“只改 rewind 尾部”和“半 reset 后无恢复状态”的缺口，且保留 solid/共识约束。

**P2 已关闭（设计层）。** 规格第 46、50、55 行与计划第 19、37 行已把 immutable receipt 的历史事实和可重绑定的当前 cold-binding 分离。cold-only/mixed-cold 都登记持久文件依赖及反向索引；不替代引用文件的 publication 无需重绑。替代 merge 必须认证语义内容/canonical/tx-range 等价、有界原子更新 binding/索引并 sync，再等旧 view pin 退出后 GC；G+1 发布而 rebind 未持久的中断有保留旧依赖和幂等恢复路径。额外保留空间及验证 I/O 有接纳限制，无需冻结所有 GC 或无限保留旧代。

**文档对齐已关闭。** 计划第 44 行现在与规格第 30 行一致：预装 guard、暂停 timer、排空 deploy job、取得同一 `start.lock`、持久 latch、停止节点并取得存储锁。计划区分了尚未进入迁移的前置失败。计划第 11、46 行补齐 `VERIFIED_PENDING_ACTIVATION`：仅固定 SHA capable binary 可按原 active 意图启动验收；原 inactive 服务离线验收；成功后才解除 latch 和恢复原运行意图。

最终结论：**可开始分阶段实施；本次定向复审没有剩余的阻断设计缺口。** 后续真实 Pebble sync/kill/reopen、pin/GC/claim 并发、reset 重做、binding 换代、systemd/release 编排及 32 GiB/磁盘容量压测仍是必须执行的实现验收门禁，尚未取得任何通过结果。剩余这些风险属于执行与验证工作，不是本轮继续冻结设计的理由。本复审不授权部署、迁移或宣称吞吐收益。
