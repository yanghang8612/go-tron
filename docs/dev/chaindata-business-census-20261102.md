# Chaindata 业务与热尾精确统计（2026-11-02）

本报告分析原线上提交 `0096c0af878ea37fc784e0abade8b55454899a0f` 的一致副本。切点高度 53,016,002，solid 53,015,984。除特别注明，GB 为十进制 10^9 B。物理 SST、活跃行逻辑字节与范围估算分开报告，不能相加或互相代替。

## 采集与服务恢复

2026-11-02 04:18:36.565723 UTC 请求停止，04:18:49.458890 完成停止，04:18:54.689690 完成 chaindata/history 同切点采集；04:19:14 原版本已运行。闭库复制仅硬链接不可变 SST，WAL/MANIFEST 等可变文件独立复制，未复制 LOCK。9,037 个 SST 共 178,183,836,126 B；16 个独立复制文件共 1,614,060,317 B。chaindata 文件长度 73,045,794,719 B，history 106,752,101,724 B。副本会保留原 inode，所以副本压实减少目录长度不代表服务器实际释放同样字节。

采集脚本错误地将包含 PID/时间戳的 ExecStart 展示字符串用于恢复守卫，原 capture-receipt 保留该错误。独立 original-restore-verification 已核实原二进制、SHA256、服务活跃及高度前进；未二次重启。后续已修正工具为稳定配置比对。

原二进制 `/data/gtron/releases/auto-0096c0af878e-6611fc4273c457b4/gtron`，SHA256 `6611fc4273c457b47cad2ea47742a2eea0653b50e59fe0b1d74d10105bb41745`。本次没有发布产品代码、改变生产格式、删除生产数据或重同步。

闭库副本点检查通过：Execution与Finish高度及块hash相同，MuHash HEAD与Finish相同，保存的MuHash state/digest自洽；scheme=`state-flat-latest-muhash3072-compact-v3`，root=`3e613b6be003aa2f2715b8472dce27575e685ec99194b4efaa643fd417dcf823`。这是元数据点检查，不是全source重新折叠。

## 非主状态数据的组成

此前“约 28 GB”是目录占用减去几个状态范围估算的粗差额，不能当作严格互斥的业务分类。本次全量活跃行扫描如下：

| 类型 | 活跃行数 | key 字节 | value 字节 | 逻辑合计 GB | 高度范围 |
|---|---:|---:|---:|---:|---|
| 区块体 b- | 390,595 | 3,905,950 | 18,600,540,446 | 18.604446 | 52,625,408–53,016,002 |
| 按块收据 tib- | 390,595 | 4,687,140 | 8,783,473,760 | 8.788161 | 52,625,408–53,016,002 |
| 交易哈希反查 tx- | 72,228,384 | 2,527,993,440 | 577,827,072 | 3.105821 | 52,625,408–53,014,690 |
| 全链区块哈希反查 bh- | 53,016,003 | 1,855,560,105 | 424,128,024 | 2.279688 | 非高度主键 |
| 热区块状态根 bsr- | 390,595 | 14,061,420 | 12,499,040 | 0.026560 | 非高度主键 |
| 奖励 VI 辅助索引 rvi- | 1,115,797 | 37,937,069 | 10,751,531 | 0.048689 | 非高度主键 |

这几类是活跃行键值长度，未经 SST 压缩；区块体和收据各含 72,514,316 笔交易/收据，逐高度窗口计数和字节闭合。交易索引最大高度53,014,690与持久TxLookup阶段完全一致；比Finish低1,312块，少285,932个tx定位是该独立阶段尚未追到head，不能称为丢索引。旧逐笔 ti- 收据为 0；归档/交易索引 prune 水位之前没有活跃 b-/tib-/tx- 遗留行。停止时暂存 staged body 为 0，但 SyncService.Stop 会主动清理预取暂存行，不能推断运行时也为 0。


作为“约28GB非状态差额”的物理侧佐证，闭库副本的SST范围估算为（包含共享边界块，不是精确互斥分摊）：

| 范围 | SST估算GB |
|---|---:|
| block_body | 13.349832 |
| transaction_receipts | 4.619883 |
| transaction_index | 3.705093 |
| staged_body | 2.812837 |

上述四类约24.488GB；此外还有全链bh-、WAL/元数据及其他辅助索引。由于同一压缩块可能跨范围、旧版本/墓碑仍在SST，不能把范围估算与活跃逻辑字节直接相减、加总或按比例分配回收量。

区块体、收据进入 ancient；状态旧值、变化点/posting 从 history 进入 state-snapshots。两条冷化管线相互配合，但不会把热区块体和收据搬进 history。

## 为什么热区块尾部这么大

ChainFreezer / TxIndexPrune 的 exclusive 前沿均为 52,625,408；按 solid-128 算的可归档 exclusive 截止为 53,015,857；V2 要完整 65,536 块段，完整段截止为 52,953,088。

| 热尾分类 | 区块数 | 区块体 value B | 收据 value B | 说明 |
|---|---:|---:|---:|---|
| 5 个已满段积压 | 327,680 | 16,063,311,186 | 7,650,057,129 | 52,625,408–52,953,087 |
| 不足一完整段的正常尾部 | 62,769 | 2,528,779,507 | 1,129,674,636 | 52,953,088–53,015,856 |
| 128 块安全边界 + 18 块 solid 差 | 146 | 8,449,753 | 3,741,995 | 53,015,857–53,016,002 |

5 个完整段的区块体、收据、交易索引总逻辑键值为 **26,399,489,659 B**。这是真正的可缩短热尾范围；不是承诺释放 26.4 GB 压缩磁盘空间。

直接阻塞条件：下一 ancient 段终点为 52,690,943，事件日志只覆盖到 52,686,022，缺 4,921 块。`freezer/runner.go` 在收据 log 外置模式下要求完整段 event-log coverage 后才发布/清理。`derivedSidecarCatchupPlan` 又将事件补建上限限制为 SnapshotBuild；状态历史在同步时允许延期 4×65,536 块（此外保留 65,536 块窗口）。因此状态历史延期政策也把区块归档卡住，形成 5 个完整段级别的热尾。不是仅靠减小 128 块 safety margin 能解决。

优化方向：将事件日志的可发布上界与状态旧值快照进度解耦，采用独立、hash-bound 的最终执行/solid cut，优先补到下一完整 ancient 段；保留当前资源预算、manifest-first/stage-second 崩溃恢复和原 hot source，确认 event coverage 后再删除热区块/收据。不得直接把 publishedBlock 换成 head 绕过验证。当前只完成原因定位，尚未实施产品改动。

53,016,003 条 bh- 是全链块哈希→高度索引，逻辑 2.279688129 GB。snap 模式刻意不再生成第二份 chain-freezer/chain-index snapshot；其通用 lookup 清理依赖该冷覆盖，而 ancient txindex 并不替代 bh-。后续可研究利用 TRON BlockID 内嵌高度并验证 canonical block hash，消除这张索引；需评估冷读 I/O 和非 canonical 查询语义，不能直接删除。

## 已有优化为何没有消除这些数据

当前按块热收据已省略可从区块恢复的 id/高度/时间字段；ancient 的收据日志外置以及交易相对定位只在冷发布后体现。热区块全量交易体仍需供同步/归档读取，热 tx- 仍为 32 字节哈希主键与 8 字节定位值。因此“格式已经压缩”不会自动解决“5 个完整段迟迟未发布”的保留量。

## 暂存区间的物理清理对照

在第二份可丢弃副本上重放/Flush后闭库建立基线，再只对 schema 定义的 staged-body 范围执行 Compact，无 Delete，前后活跃 staged 行均为0。文件长度 71,928,553,092 → 69,014,138,866 B，净减少 **2,914,414,226 B**；目录实际分配（含目录项）71,936,565,248 → 69,021,429,760 B，减少 **2,915,135,488 B**；Compact耗时21.5996秒。范围SST估算2,812,857,085 →7,178 B。压实可能重写相邻边界块，所以全目录差额与范围估算差额不完全相等。

初次工具在RW数据库异步删除已过期WAL时遍历文件，触发ENOENT，尚未执行手动Compact即退出；修正为闭库后取中间基线，第二次完成，首轮失败证据保留。打开/Flush本身另减少1,117,241,627 B，主要涉及副本WAL回放/过期文件清理，不能混入staged区间压实收益。原副本和生产SST均保留，所以这些数值是副本目录收敛收益，并非本次主机实际已回收空间；线上尚未运行手动压实。停机前metrics记录1,953个buffered blocks和2,653,329,248 B staged范围SST估算；Stop会删掉当前预取行，因此本实验不能将全部2.914GB都判定为停机前已经无效的垃圾，也不能保证在线运行时全部可回收。

## 全量当前KV业务分布

全扫描完成：1,266,853,461条KV、172,936,016个account_latest、970,592条generation。account envelope与generation watermark无矛盾，future generation=0。每个业务域在all/current/obsolete/missing四类上，行数、物理key/封装value和解包logical字节均独立算术闭合。全部domain hist也闭合；4.814GB的owner-fanout压缩逐行流保留服务器，本地未再重读该全流；不能声称完成第二次全流重汇。

以下为当前Pebble可见物理key＋wrapped value逻辑长度，包含语义上失效generation，**不是压缩磁盘GB**。KV合计103,080,782,571B，对应SST范围估算34.850GB。

| 业务域 | 行数 | key B | wrapped value B | 逻辑GB | KV逻辑占比 |
|---|---:|---:|---:|---:|---:|
| AccountPermissionAux | 338,943,975 | 12,540,936,909 | 20,852,985,751 | 33.393923 | 32.396% |
| ContractStorage | 422,867,214 | 27,909,236,124 | 4,647,503,281 | 32.556739 | 31.584% |
| AccountAssetV2 | 360,435,453 | 14,777,853,573 | 1,344,734,251 | 16.122588 | 15.641% |
| SystemReward | 19,724,230 | 1,494,538,075 | 8,880,048,482 | 10.374587 | 10.065% |
| AccountResourceAux | 111,382,602 | 3,898,391,070 | 2,732,590,783 | 6.630982 | 6.433% |
| ContractMetadata | 1,605,763 | 61,018,994 | 2,478,486,466 | 2.539505 | 2.464% |
| ContractABI | 644,324 | 23,839,988 | 639,789,039 | 0.663629 | 0.644% |
| SystemDelegation | 3,731,255 | 230,195,701 | 210,398,377 | 0.440594 | 0.427% |
| AccountAsset | 6,221,995 | 261,690,312 | 12,671,005 | 0.274361 | 0.266% |
| AccountVotesAux | 463,679 | 17,619,802 | 14,631,241 | 0.032251 | 0.031% |
| AccountFrozenBandwidthAux | 359,545 | 13,662,710 | 6,559,382 | 0.020222 | 0.020% |
| ContractRuntimeState | 223,555 | 8,718,645 | 6,930,227 | 0.015649 | 0.015% |
| AccountFrozenV2Aux | 147,503 | 5,605,114 | 1,917,539 | 0.007523 | 0.007% |
| SystemAsset | 25,549 | 1,166,897 | 1,929,126 | 0.003096 | 0.003% |
| SystemAccountIndex | 44,479 | 2,045,380 | 978,538 | 0.003024 | 0.003% |
| AccountUnfrozenV2Aux | 24,781 | 1,040,802 | 478,856 | 0.001520 | 0.001% |
| WitnessVoteState | 698 | 42,530 | 61,596 | 0.000104 | 0.000% |
| SystemProposal | 89 | 3,831 | 92,824 | 0.000097 | 0.000% |
| AccountFrozenSupplyAux | 1,247 | 47,386 | 23,243 | 0.000071 | 0.000% |
| AccountAssetOperationTimeV2 | 1,524 | 62,484 | 7,620 | 0.000070 | 0.000% |
| WitnessCapsule | 629 | 36,082 | 33,148 | 0.000069 | 0.000% |
| AccountFreeAssetNetUsageV2 | 1,524 | 62,484 | 4,091 | 0.000067 | 0.000% |
| SystemExchange | 331 | 14,233 | 23,914 | 0.000038 | 0.000% |
| AccountAssetOperationTime | 716 | 27,422 | 3,580 | 0.000031 | 0.000% |
| AccountFreeAssetNetUsage | 716 | 27,422 | 1,667 | 0.000029 | 0.000% |
| SystemWitnessSchedule | 2 | 95 | 8,977 | 0.000009 | 0.000% |
| SystemDynamicProperty | 61 | 3,394 | 585 | 0.000004 | 0.000% |
| SystemForkVote | 19 | 779 | 532 | 0.000001 | 0.000% |
| SystemShielded | 3 | 163 | 49 | 0.000000 | 0.000% |

前五域（权限、Storage、AssetV2、Reward、Resource）占KV逻辑字节96.12%左右；优化应围绕这些域。SystemShielded仅3行/212B，不是当前高度大头。此百分比不代表它们的独立压缩磁盘占比。

当前有效1,263,943,272行/102,840,496,700B；旧generation1,525,126行/112,639,592B；缺失owner1,385,063行/127,646,279B。旧generation约0.113GB逻辑量，不是数十GB膨胀来源；missing owner不能直接算安全删除，MuHash source也包含其物理字节。964,014条generation watermark没有当前account，并不表示可删：它们防止重建账户复用旧generation。

SystemReward的priority导出与全域最终导出，行数、字节、SHA256和首末key完全相同，因此19,724,230行对照已覆盖整个该域。

权限owner169,462,448个，其中169,455,802个owner恰有2行（99.9961%），适合研究保留原key的严格模板/SELF省略。ResourceAux111,382,601个owner，除1个owner有2行外全部1行，因此owner分页对此域没有价值。AssetV2有96,282,833个owner、40,422,319个owner仅1行，分页全域收益不能按前500k多行样本放大。Storage样本前200万行中71.206%来自同一owner，并非全域均匀样本。


## 三条并行压缩试验

所有原型在研究目录实现；未改生产读取格式。每次对照使用相同活跃行、相同输入分段、相同块/过滤器参数，空间以重建 baseline 与 candidate 的实际 SST 文件长度比较。

### SystemReward（已完成）

当前系统 owner 19,724,230 行、输入 10,532,380,397 B，SHA256 `512ccfce04f593357cf8423031de0dd53c1a2709d47ca8e409c6d516dc708cc3`。其中 account-vote 12,726,979 行，native value 8,841,164,502 B，占本样本绝大部分；不能把历史 cycle 当前键当作可删除旧值，它们参与待提现奖励计算。

独立单值紧凑格式保持原字段存在性、未知字段、次序和 native 字节可逆；没有还原链。Snappy 同输入重建 SST：4,512,508,220 → 3,766,082,494 B，节省 **746,425,726 B / 16.5413%**。raw SST：9,985,789,292 → 6,787,728,667 B。不是全 chaindata 的磁盘回收量。

全部 account-vote 均经 fast/reference 两套解码逐字节核对，mismatch=0。内存中完整 Account 解码并生成 API protobuf（非 HTTP/DB I/O）28,672 次交错 AB/BA：平均 46.239 → 54.957 µs；p95 124.543 → 144.808 µs。空间收益明确，需判断额外解码与写入路径成本是否可接受。该4096项微测使用stride251、达到4096即停止，仅覆盖前1,027,846个account-vote（cycle-major前8.08%），并非全域均匀采样；不能将均值外推为全部周期平均。

### AccountResourceAux / AccountAssetV2（已独立复核）

资源采用 sparse bitmap/varint 单值编码；资产试验按 owner 做最大 8 行、16 KiB 有界 bundle，减少重复完整物理 key。原型保留原始逐行字节展开能力。资产分页新增更新放大和范围读取成本；不能仅凭 raw bytes 节省决定上线。

### ContractStorage（已独立复核）

SPG2 页按 owner/generation/domain/slot 前缀共享、最多 128 行且单页 value ≤4,000 B；小组保持普通行，完整 slot 身份可还原。生产兼容的 mutable overlay/merge 与 MuHash canonical source 展开尚未接入，暂不能替换 SLOAD/SSTORE 持久层。

### Reward Votes-only新状态表示（全域实测，尚未产品集成）

继续核对Go和本地Java调用点后，奖励结算及待提现查询的旧快照只读Votes与presence。完整Account中的AssetV2/owner+active permissions合计占native内容64.24%，不进入奖励算式；原dbcompare和reward-trace完整Account/allowance诊断仍依赖它们，所以只保留Votes是**新语义表示**。

用完全相同全域源SHA、19,724,230行、10,532,380,397输入B、64MiB原输入切段、157个SST、4KiB块/索引、Bloom10/PebbleV4进行投影，仅改变account-vote值，其他subtype封装字节不变。Snappy SST **4,512,508,220→823,022,838B，净少3,689,485,382B /81.761300%**，比保全Account的无损候选再少2,943,059,656B。全部12,726,979个快照中的19,363,802个Vote数量、顺序、地址、票数、unknown核对一致。

改用全流Algorithm-R reservoir4096项，内存拒绝0；typed Votes-only语义结果内存读取均值19.342→2.930µs（降低84.85%），没有DB/HTTP，不可与完整Account API返回性能混比。实际数据empty/unknown快照为0，空presence与边界依靠本地fixture覆盖，未执行Java节点二进制重放。

当前保留了全部cycle键，未做单槽或删除旧键。若后续改owner→{cycle,Votes,presence}单槽，正常共识读点只要求当前beginCycle，但须核对owner/endCycle集合及回退生命周期；997,145只是依赖join成立的条件上界，不是安全删除数。单槽覆盖会新增history旧Votes before-image，不能把热库行数减少全算总盘收益。

**该候选不能重建原完整Account、旧MuHash及原完整Account历史原值；需要新scheme、typed presence、历史/恢复/诊断工具的正式设计和验证。未写入线上库。**普通history前像也不能凭空恢复已舍弃Account字段，且mid-TVM快照不一定能从块级Account历史重建。

### 权限SELF/严格默认模板补充（已独立复核）

追加只读短前缀探针，扫描1,367,777条KV取得500,000条PermissionAux即停止，没有重复全库扫描。原physical key不变；Owner/Witness严格模板、Active精确ops（不可变常量ID或原始32B）、自定义SELF字节补丁，其余raw fallback，候选逐行还原原native字节。

同输入Snappy SST 11,990,650 →8,107,628 B，节省3,883,022B（32.383749%）。两臂真实L6 full-key Bloom均625,093B，所以收益不依赖缩减逐行negative filter。样本499,954次SELF address、249,860个严格默认Owner、248,257个带原样ops的默认Active、1,611个不可变ops模板，unknown字段0、raw fallback46；这是地址排序前缀样本，不是全域默认比例证明。保留存在行，不能用删除默认权限模拟implicit default。生产GetCursor(16,L6=true)、单cursor整批10,000次、32MiB app cache、交错两轮：正查初次66.069→60.445ms，暖28.286→30.217ms（+6.82%，约每次增加0.193µs）；负查首次11.591→10.005ms、暖7.718→6.705ms，短窗口存在离散。两臂负查均9891/10000 Bloom拒绝。不是request32/HTTP/OS冷；正式接入还应降低暖查展开分配。

若进一步优化为默认Owner+Active显式内嵌account flags/确切ops ID，custom仍KV，可能省掉默认权限的两条独立物理行。必须区分absent/inline-present/external-custom，重新设计MuHash source和历史投影，并防余额更新将不变权限反复写入account before-image导致history增长；未实现、未计入已证实收益。

### 首轮真实空间及生产读取策略复核

| 候选 | cohort行数 | Snappy baseline B | candidate B | 节省 | 建议 |
|---|---:|---:|---:|---:|---|
| Resource单值sparse | 500,000 | 17,606,413 | 15,831,034 | 10.0837% | key/Bloom不变，空间收益有限；暖group32正查平均增加约6.8%，无提速证据 |
| AssetV2 owner bundle | 500,000 | 8,308,251 | 7,613,591 | 8.3611% | 暂缓当前设计，同owner负查退化6.7–10.7倍 |
| ContractStorage SPG2 | 2,000,000 | 56,409,387 | 54,001,367 | 4.2688% | 暂缓当前设计，暖group32总CPU增加14.45%，负查4.71倍慢 |

Asset bundle在生产exact基线的同owner负查（每组10,000次，两轮平均）应用暖缓存6.968→46.716ms、应用空缓存8.941→95.617ms，VFS冷读0.977→7.247MB；原full-key Bloom拒绝9883/10000，bundle没有此拒绝。正查group32暖26.76→45.24ms。其Bloom从625,093→191,685B，433,408B的过滤器缩水解释了694,660B总节省的62.39%；若恢复同等原行membership过滤，剩余净节省仅261,252B（约baseline3.14%，还未计额外定位开销）。

Storage真实inode去重分配56,438,784→54,030,336B，减少2,408,448B。暖group32总scope wall+14.42%、CPU+14.45%、SST读取+73.91%；负查均值1.957→9.211µs。冷group32后续lookup合并+35.23%，负查0.532→1.395ms。冷fresh1首查虽快42.5%，但其新scope的旧reader读取整张L6 Bloom，而候选页Bloom更小，这种单点优势不能代表多次SLOAD的HTTP请求。已仅读取现有6个SST的footer/metaindex/properties共5,495B，data区域读取0。full-row Bloom 2,500,175→page Bloom36,175B，少2,464,000B，解释总节省的102.325%；非filter部分53,909,212→53,965,192B反增55,980B；物理data块本身53,399,635→53,670,379B增加270,744B。若补回相同逐行filter字节，候选56,465,367B，已经大于baseline。IndexSize属性为writer估算，未拿它拼物理闭合表。**本SPG2没有保全过滤能力后的真实空间收益，否决生产集成。**

上述均为前缀cohort、真实L6的底层对照，不是全库HTTP eth_call。全部返回存在性/原值核对通过；没有全量MuHash/产品恢复接入的证明。资源Zstd匹配SST仅省0.1124%，不宜将Snappy结果推广到其他压缩级别。现阶段不将查询退化的分页原型接入生产。

### 查询基线限制与补正

原型普通 Get/Seek 并非当前线上请求级 GetCursor(16, L6 Bloom=true) 策略，会低估旧格式负查能力。原空间对照仍有效，另冻结独立 cursor-control：真实 L6、原数据 cohort、fresh1/group32、正负查、交错 AB/BA；单独记录生命周期、CPU、SST 读量，ContractStorage 冷缓存只驱逐自身实验 SST。结果不能标为完整 HTTP 性能。

当前 MuHash scheme 按物理 source 键值折叠。格式变化必须在 source fold/快照导入导出使用 canonical 旧行展开，或显式引入新版本并从创世建立；仅 codec 可逆不代表原 production root 自动不变。本轮不做无依据的全库 MuHash 重算，也不声称原型已通过完整 HTTP/生产快照验收。

## 实施优先级与本轮状态

1. 先解耦状态历史延期与事件日志/ancient发布，缩短5个完整段级热尾；这是非状态占用最直接的大头。需独立hash-bound最终执行/solid cut、事件coverage与manifest恢复验证，不能绕过覆盖校验或直接删热行。冷化会新增ancient/事件文件，热库减少不等于同量总盘回收。
2. 账户侧优先保留原key的权限SELF/严格模板，兼顾暖正查展开分配；Resource单值为次级候选。现原型只证明同cohort可逆/空间与底层点读，MuHash source fold、历史序列化和恢复尚未正式接入。
3. Reward优先设计Votes+presence新模型。全域实测省3.689GB，收益明显高于保全完整Account的0.746GB，但要显式接受新内部状态表示并完成新scheme/历史/快照接入。

资产bundle和SPG2均暂缓，保留原始精确点查及逐行Bloom；不把以负查能力换来的文件缩小当合格优化。本轮所有改动是研究工具/原型，未变更产品代码或生产格式；master未推送，未部署新版本，未删库或重同步。三条首轮任务及补充权限/奖励/元数据任务已完成，后台无持续性能负载。

## 原服务收尾核验

2026-11-02 05:45:16 UTC，服务active、PID21838，原二进制SHA匹配，高度53,159,783（高于采集点143,781块），RSS24,302,364kB、swap0，主机MemAvailable11,361,452kB，/data可用2,013,948,821,504B。近期最后8MiB日志未匹配MuHash错误；这不是全天日志的全量错误保证。副本已核对保存的MuHash digest及Finish身份，未重复全源MuHash折叠。

历史RPCoracle：接收者341高度余额0、342高度余额33,251,807,524,000,000 SUN（JSON-RPC乘10^12），两点正确，耗时0.3275s/0.11195s；342块指定交易定位正确，耗时4.2548s。交易定位仍有秒级读延迟，本轮原型点查指标不等于HTTP改善。

## 原始证据

- 本地热库证据：`build/benchmarks/20261102-chaindata-census/hot-evidence`，源 ZIP SHA256 `bee3ec6fc9006b5b47c18acb7d91f2e835eac20a954636786097778fcfa2e455`。
- 奖励证据：同目录 `reward-evidence`，源 ZIP SHA256 `ffedff95867ef66435c670e0ff83ed595117b8e345e581d4509d6c2b6ffe8382`。
- 服务器全部原始输入/结果：`/data/gtron/maintenance/chaindata-business-20261102-01`。
- 全量/首轮对照ZIP SHA256 `1bfefda22af7dc08f3201dedc11f69a4be97950f0161b048b6947b0e4c215e76`（282,766B，75文件）。
- 最终ZIP SHA256 `5bf4904d42a2cedff86686e4568dbff68a4667a1b03e1acbbc4fb1653271508f`（298,144B，92文件），本地逐文件长度/SHA/ZIP CRC验证；`final-evidence`包含权限、Votes投影、六SST属性及生产收尾回执。
- 冻结初版 21 源：`ed33556bd99b45067e7712bb2f935ec4177219b3`；副本压实修正/独立 cursor-control：`8dcd5a44d54dc84531169a3cf2b391f52cb5df99`。均只在研究分支 `codex/chaindata-business-20261102`，没有触碰 master。
