# 28M 同步性能实施计划

日期：2026-09-05。依据：`../specs/2026-09-05-28m-sync-performance.md`。

当前进度：采样摘要、P0a-1 专用 codec、P0a-2 有界派生缓存及本地旧新 StateDB 基准已完成。2026-09-06 用户取消同一 28M 重放验收，继续主网直接部署测试；P2a 的清单校验优化、预取所有权修复及主网开关试验已完成；P0b/P1/P2 更深表示优化/P3 仍为后续工作。

在线更新：用户明确授权直接在主网部署，P0a 已于 2026-09-05 04:23 UTC 上线并持续导入。约 20 秒样本中 legacy delegate 累计 CPU 占比 17.23% → 0.54%，但整体吞吐目标尚未达到；不同区间窗口从约 10.86 降至首轮 7.76 blocks/s，较后约 8–10 blocks/s，Commitment wall 与压实积压上升。下一步优先量化 Commitment/共享 I/O，不盲目继续扩大委托缓存。详细口径、回退入口和限制见 `docs/dev/28m-server-ab-2026-09-05.md`；后续以主网部署、连续观测和按需切换作为验收方式。

## P2g 当前执行记录（2026-09-06）

- 已实现完整内容身份的清单解码缓存，保持每次 ReadFile、独立返回视图、64 MiB 驻留计费和生产验证边界；文件 SHA/删除门槛不变。
- 本地 snapshots/pruning、state/CLI、相关 race 和增量 lint 通过；60,000 引用固定输入 warm 中位数 75.320→3.043 ms，尚不代表主网收益。
- 已准备 Linux/Sapling 检查、固定生产清单基准、主网部署/回退和 retired 实物盘点脚本。Chrome 专用终端未附着，以上生产步骤尚未执行；线上仍为 P2f。
- 静态核查确认合并默认删除未被租约保护的旧文件，retired 记账不能当作物理积压。先盘点再决定回收重构。
- 详细实现、原始证据和后续执行入口：`docs/dev/28m-manifest-cache-p2g-2026-09-06.md`。

## 1. 固化主网基线与局部测试输入

- 固化昨天 28.24M profile 摘要、Build ID、原始文件指纹。
- 记录主网实际版本、环境、连续指标及 profile；不再安排 28M checkpoint 副本或同区间整链重放。
- 按三种交易形状记录 FreezeBalance 次数、各 anchor degree、native bytes、重复边/实际变化比例。degree 分布与重复率是目前缺失的数据，不从 CPU profile 猜测。
- 补充真实 `StateDB.WriteDrAccountIndexLegacyDelegate` 基准，包括 history 开启/关闭、同块多次/跨块、冷/热、不同 degree。已有 protobuf decoder 微基准不能替代。

完成条件：实际版本、profile、metrics 口径和后台债务起点可复核；局部实现用固定输入差分及基准验证。

## 2. P0a-1：专用 native codec

涉及 `core/state/statecodec`、`core/state/delegation_store.go`，不改 protobuf generated 文件。

- 专用 framed native parser/encoder，生成有序地址段及原始/未知字段，不经过反射列表。
- 专用输出与旧 native codec 完全一致；未知/异常输入保留同等接受与拒绝行为。
- 覆盖字段顺序、默认值、空列表、未知字段、损坏长度、重复项及非标准地址；采用旧 codec differential 和 fuzz。
- 保持其他 rooted 消息 codec 不变，仅接入已测量热点。

完成条件：native bytes、错误语义一致，真实路径度数基准的编解码与分配下降；尚不宣称已解决 O(D)。

## 3. P0a-2：有序邻接 resident 与 no-op

涉及 StateDB 生命周期、delegation store、SystemDelegation generic write/rollback。

- 分页/连续 arena 保存有序序列，membership 只加速存在性判定，不决定遍历顺序。
- 缓存 canonical 当前行的私有派生表示，新增、删除和删除后重加维护顺序；native KV/journal/history 继续权威。
- 精确 no-op 跳过编码；两侧分别判定。codec differential 验证原 decoder 接受的 native 行已规范化，无需遗漏原 writer 的规范化步骤。
- 沿用原 journal 撤销 encoded KV，不额外引入 cache undo/COW；generic 写入、conversion、rollback、reset、reader/generation 切换精确失效；copy 不共享缓存，read-set 命中仍记录。
- 32 MiB / 1,024 行上限覆盖所有缓存 backing 与索引；LRU 淘汰派生数据，不影响原 dirty/历史版本可恢复性。
- 超大账户不建 membership、不准入 resident；版本 reader 与 self-edge 使用原双记录路径。

完成条件：逐 tx/块旧新对照通过；高 degree 重复边复杂度平坦。正确性及局部基准通过后，按用户授权独立部署主网并继续测量实际吞吐。

实现与本地结果：`core/state/statecodec/delegation_index.go`、`core/state/delegation_cache.go`；完整原始 benchmark、环境与局限见 `docs/dev/28m-delegation-benchmark-2026-09-05.md`。六类独立 generic oracle 对照覆盖 native bytes、history、as-of、root、嵌套 rollback；补充缓存生命周期、错误、所有权和 codec fuzz/race。主网上线与后续观测按用户明确授权推进。

本地验证已通过全仓测试、相关包 race、codec fuzz、go vet、Sapling 构建及双节点系统测试（79/79）。补装历史同版 `golangci-lint v2.13.1` 后，完整修改补丁的增量 lint 为 0 issues；全仓 lint 仍报告 158 项，与历史审计数量相同。后续直接采样主网 degree/重复率/缓存命中与新热点，按证据决定是否进入 P0b；取消固定 28M checkpoint 和隔离重放要求。

## 4. 复采样后选择下一主线

- 若实际增删大行仍占主要串行时间：进入 P0b，先实现版本化逻辑行/历史接口，再切物理存储。
- 若资源/history bookkeeping 接管热点：进入 P1 的 Resource cell 与 effect log。
- 若 commitment backpressure 接管：优先 P2 PreparedOps 和 compact branch。

这里的观测只决定后续工程顺序；不改变已经选定的 P0a 第一实施项。

## 5. P0b：逻辑行之下的分块存储

- 先定义有序页、membership、header、tx delta、checkpoint 和原逻辑 row projection。
- 在 `core/rawdb/schema.go` 注册物理家族；逻辑 key/原 native value 与 root 不变。
- 接通 point/range/as-of 读、history index、snapshot build/restore、dbcompare/reset；测试隐藏的物理页不会成为额外 commitment leaves。
- per-tx 历史保留，块末生成原 logical row 的 commitment 输入；明确完整 leaf hash 仍 O(D)。
- 迁移 marker 与基准 root/txNum 绑定，验证完成后原子切换；中断后可继续或回退，不允许部分迁移当作完成。

完成条件：真实 append/delete 工作量的执行+history总成本下降，所有逻辑投影、逐块 root、重启与回滚一致。不接受只把编码移到计时器外。

## 6. P1：统一热点状态变更来源

- Resource original/current cell，消除 materialize 后首次写的重复 preimage read。
- first-write undo 和顺序 dirty IDs 替换逐 tx 扫 journal/建四张 map；保留旧 collector 作迁移对照。
- 以 cell effect log 驱动 history、版本和 block commitment touches；保留嵌套 snapshot 与每 tx 边界。
- 扩展真实 owner resource/permission point read-ahead，独立衡量 useful hits 与 foreground I/O，维持 stale-fill 防护。

完成条件：history 开启的真实重放也获得收益；功能、历史和事务依赖对照一致。

## 7. P2：Commitment 分阶段交付

- 独立 bounded prepare，生产 unique touches 仅进行最终 path sort；不阻塞 publisher，不增加总 inflight。
- lane compact arena，先逐块输出旧 branch row；独立跑 root/row identity 和分配基准。
- 前两项有收益后才引入跨块延迟编码，设计 ValueAt(cutoff)、undo、checkpoint 和恢复边界。
- group metadata batch 另一个独立提交；NoSync 语义不冒充 fsync 优化。

完成条件：真实关键路径下降；已有 decoded-trunk 失败原型不能换名称重上。

## 8. P3 与最终上线门槛

- P0/P1 稳定后再做 MVCC 正式执行；从完整依赖模型和冲突重试证明开始，不删除 oracle 换性能。
- 先跑相关包和必要 race/fuzz、原历史修复高度与 proposal 转换重放，再执行全仓测试、lint、构建和系统测试。
- 保留生产 parallel safety 资格门禁；新表示不自动使旧 datadir 获得并行资格。
- 直接在主网部署测试，源码构建保持隔离；保留兼容当前数据格式的旧 binary 与配置回退入口。物理格式切换后回退必须按迁移协议恢复。
- 导入速度提高后同步验证 cold-history/freezer/pruning 的长期服务率、RSS、I/O 和 compaction debt。

不安排无证据的 P2P 扩容、GPU/hash 替换、提前 proposal 转换、跳过历史/验证，或直接换整个数据库引擎。


## P2a 现场执行（2026-09-05）

- [x] 新采样区分维护与前台读：确认清单重复扫描、预取高利用率和首层整批等待。
- [x] 预取改用独立的池化路径数组，修复删除整理与 lookahead 的共享数组竞态。
- [x] 按需等待调度保留开关；真实 Pebble 的 24 块根/全部分支字节对照，落盘切换与 session lifetime 检查。
- [x] 清单 companion 索引和 event coverage 单向游标；身份错误、缺口、随机差分和 uint64 边界回归。
- [x] 本地 core 回归及 async/reorg/rotation race 检查（rawdb 的 offline replay 审计边界已补齐并单包复验）。
- [x] Linux Sapling 编译、包回归与 race 复验。
- [x] 主网部署、连续窗口采样和模式开关复验；识别并剔除 canary 退出的环境干扰，保留 P0a 回退入口。最终关闭 overlap，保留清单优化及竞态修复。详见 `docs/dev/28m-commitment-p2a-2026-09-05.md`。

## P1a 当前轮次

- [x] canary 停止后重新采样，定位 GetCodeStrict 重复 hash/分配。
- [x] 使用已有私有正缓存的逐字节等价验证，缓存 miss 保留原 hash 校验。
- [x] 消除重复 admission 的代码副本分配。
- [x] 加入返回值逐字节变异、长度变化、copy/revert、缓存关闭/超限/并发淘汰检查。
- [x] 同一 StateDB 输入、旧源码 overlay 的 256 B / 4 KiB / 32 KiB 微基准。
- [x] Linux Sapling 构建与 race、主网部署及预热后 profile。
- [x] 完整窗口汇总与最终健康核验；本轮保留，后续继续主网观测。


## P1b 进展（2026-09-05）

已完成状态两阶段读取计划 + blockbuffer 批量缓存准入 + Pebble 排序 exact-key cursor，并直接部署主网 release `20260905-p1b`。固定 SST 基准 Linux 约 1.6–2.5×；本地、Linux/Sapling、race 及 79 项 system test 均通过。三份线上 CPU 样本前台 KV 读取粗归一降幅 41–53%；连续 9 分钟同步健康、预读错误/丢块 0。最后窗口 1646 tx/s 对比部署前 1425 tx/s，但交易/能量构成不同，不能宣称严格整链收益或 +50% 已达成。commitment 读取与提交排队是当前更突出的剩余瓶颈。详见 `docs/dev/28m-state-readplan-p1b-2026-09-05.md`。

## P2c 进展（2026-09-05）

- [x] 新采样确认 commitment 读路径成本和既有预取高利用率。
- [x] 将 16 首层 owner 重构为 64 第二层有序分区，保留原 branch/root 编码与逐块 FIFO 发布。
- [x] 明确分区持有父节点、互斥写前缀、独立 reader/arena、空块 completion 和失败传播的生命周期。
- [x] 完成根/完整 branch bytes 对照、collapse/重启、immutable/frozen base、真实 Pebble inflight/prefetch、读取故障与 race 验证。
- [x] 第一版 Linux Sapling 构建及主网 64→16 顺序试验；没有稳定线上收益，退出无限额 64 模式。
- [x] 第二版实现共享 32 个 durable cursor 名额，缓存/overlay/singleflight follower 绕过；错误及 panic 归还名额。
- [x] 池化固定分区工作区，匹配 162-reader 的有界 scratch pool，减少额外分配。
- [x] 第二版本地 core/actuator/VM、两轮相关 race、增量 lint、Linux Sapling 回归与 race、79/79 系统测试通过。
- [x] 两种版本分别完成固定 SST 基准；延迟模型加速，常驻数据回退，默认保持 16。
- [x] 第二版主网 64/32→16 两组各九分钟完整观察，二进制/模式/错误指标与部署互斥检查通过。
- [x] 再次切回 64/32 做六分钟重复验证，51 个总采样点持续 active；观察到更接近负载下的提交耗时与吞吐改善，主网保留该模式，代码默认 16。
- [x] 完成源码/二进制 hash、原始 profile/指标、实际配置与回退入口归档；部署 timer 恢复，canary 保持停止。

详细指标、最终主网配置和回退入口统一记录在 `docs/dev/28m-commitment-partitions-p2c-2026-09-05.md`。同一 28M 输入验收已按用户要求取消；合成延迟倍数和不同高度的 tx/s 比值仍须注明各自测量条件。


## P2b 进展（2026-09-05）

- [x] 新采样区分前台 commitment 随机读与周期性冷历史成本，确认大型压缩 body 二次复制仍存在。
- [x] 实现 version=2 固定 header + 旋转首 chunk + 尾索引/trailer，写入同步累计原整文件 SHA-256，同 inode 发布。
- [x] 新旧文件/内存 reader、历史空间统计、stream Reset/Abort、blob 便捷写入入口按版本兼容。
- [x] 完成逻辑字节/随机读/整文件 checksum、格式损坏/溢出、取消/写错不发布、混合压实与生产读取路径测试。
- [x] 本地完整 core/actuator/VM、新格式完整 snapshots、两轮相关 race、增量 lint 及 79/79 系统测试通过。
- [x] 服务器 Linux Sapling 快照/裁剪/freezer 回归、race 与构建通过；源码与本地 SHA-256 一致。
- [x] 读取器先行部署并验证，随后开启 format=2，新 reader 固定为最低回退基线。
- [x] 对主网发布的新旧历史三件套进行完整校验，完成 reader=2 / writer=1 回退重启、最终六分钟 13/13 active 观察、实际配置和回退归档。
- [x] 真实 Linux 跨文件系统测试复现 blob 临时文件 EXDEV，修复为目标盘临时文件，相关回归与 race 通过并部署修复版 P2b2。
- [x] 如实报告主网吞吐与长期债务；文件基准约 19%/75% 收益仅代表对应文件操作。

详细结果见 `docs/dev/28m-history-footer-p2b-2026-09-05.md`。

## P2d 进展（2026-09-06）

- [x] 新主网 CPU 样本与 Seek/Get 静态路径分析，识别多层旧版本定位成本。
- [x] 有开关的 bounded Snapshot.Get，保留快照、prefix、回调所有权及资源限额。
- [x] 多层 SST 全量/稀疏更新固定输入对照，保留单层和常驻缓存回退结果，默认仍为 iterator。
- [x] 值/存在性、MVCC/压实/删除/Merge、故障/回调/生命周期验证；全仓测试、race、lint、79/79 system test、Darwin Sapling 构建。
- [x] 增量源码包、SHA 清单和服务器部署/回退脚本准备。
- [x] 恢复 SOCKS5 1088 与 Chrome 跳板机；完成 Linux/Sapling 构建、相关回归、race 和 /data 固定 SST 基准。
- [x] 首次部署停机触发旧 P2P 重连竞态，自动回退；修复连接准入和回调等待，旧实现确定性失败/修复通过，Linux 与本地 P2P、net race 通过。
- [x] P2d1 iterator 模式部署，核验实际运行 SHA、启动环境、继续同步和 timer 恢复。
- [x] 完成同一 P2d1 的 iterator→Get→iterator→Get 四组 9/9/6/6 分钟对照，64 原始点/52 有效点均 active。
- [x] 根据反向复现的提交成本和重复吞吐改善保留主网 Get；两组 463 万 Get 无错误，运行 SHA、端口、timer、回退脚本及证据归档已核验。

P2d1 已部署；模式对照和完整结果见 `docs/dev/28m-point-get-p2d-2026-09-06.md`。

## P2e 进展（2026-09-06）

- [x] 按用户要求取消同一 28M 区间重放门槛，更新活动方案、计划和 P2d 报告。
- [x] 新主网样本定位 accessor 装配的逐 key Write/Seek/Read/Truncate 系统调用，优先处理该热点。
- [x] 64 KiB 内存 payload + 64 KiB 顺序 spill 缓冲，普通 key 不创建 scratch；metadata、key blocks、最终目录顺序缓冲装配。
- [x] V7 编码器逐字节对照、frame/64 KiB 边界、大小 key 交替、文件/目标故障、取消、清理和完整压实不发布验证。
- [x] 本地 snapshots/pruning、core/actuator/VM、相关 race、增量 lint 和 Linux/Sapling 回归、race、构建通过。
- [x] 同量完整 BuildAccessor 基准三轮；Linux 单次 key 512→40 ms、混合 106→49 ms、极热 key 约 72 ms 持平。
- [x] 04:16 UTC 正式主网 P2e 部署，Get/64/32/history format=2 保持，健康和二进制确认通过，timer 恢复。
- [x] 主网前后各九分钟完整观察；P2e 有效窗口 166 万 memory keys、4 个 spill keys，点读错误 0；新发布约 182 MB 历史三件套完整验证通过，最终保留 P2e。证据与最后状态归档。

## P2f 进展（2026-09-06）

- [x] 两次主网复采样确认 P2e 运行，当前没有大型历史 merge；辨认持续成本与阶段成本。
- [x] 找到 V6 合并的 record/range 单块缓存互相驱逐，接入独立范围 cursor 和同步借用 frame。
- [x] 格式 1/2、多块 payload、频繁跨区块、空/不存在原像、非法 keyID/marker/length/range/order、校验和/尾部错误与活跃复制取消/写入错误验证。
- [x] 压缩输入与 plain V6 原路径的输出三件套逐字节一致，完整语义校验通过；本地 snapshots/pruning、core/state/actuator/VM、相关 race 与增量 lint 通过。
- [x] Linux/Sapling 全包回归、相关 race 和隔离构建通过；同输入合并基准完成，固定约 429 MB 生产历史文件。
- [x] 第一候选交替运行固定生产文件旧/新合并，六次完整验证及 SHA 一致；整段仅改善 1.8%，继续真实文件 profile 定位。
- [x] 第二候选将完整源校验改为有界双 worker，按源顺序返回和报错，取消/失败 join；默认 1，主网启用 2。
- [x] 第二候选本地及 Linux/Sapling 快照/裁剪、相关 race、增量 lint 和构建通过；相同生产文件三轮中位数 8.050→6.660 秒（耗时 -17.27%），输出完整验证与 SHA 一致。
- [x] 07:34:58 UTC 直接主网部署 P2f，旧进程正常退出、新 SHA/环境/timer 核验通过。前后各九分钟、38 个原始点持续 active，相关错误 0；保留 P2f。
- [x] 如实记录线上 1,281→1,091 tx/s、后台债务和预热差异；新合并计数为 0，未宣称整链提速。回收积压与写放大列为下一轮优先事项。

详细证据见 `docs/dev/28m-history-stream-p2f-2026-09-06.md`。下一轮优先分析历史回收、持续写放大与 manifest 重复解码。
