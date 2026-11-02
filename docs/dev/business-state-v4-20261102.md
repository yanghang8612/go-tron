# Business state V4：实现及固定副本验收（2026-11-02）

最终产品源码 `5e51a44c0d405863f3766fa6a55fee6d9dfc293c`；固定样本性能测量产品源码 `f6af09b1309c9c6ed9a137532ac2b81bbf1073f3`；最终窄查询测试源码 `3856d7237eb27be34e9cdf9858f463e309d6bd08`，窄查询测试提交仅调整测试范围；最后的5e51a44c修复历史代际候选，不改物理codec或事件读取器，因此不重做相同空间实验。研究分支 `codex/chaindata-business-v4-20261102`，未合入 master、未部署、未启停生产 gtron、未删除数据库。基线及完整一致副本仍为 0096c0af，固定高度 **53,016,002**。

## 已接入的三个方向

1. **独立事件归档前沿**：direct Ancient V2 的事件准备不再等待 state SnapshotBuild；仍受下一 ancient 段需求、solid、带哈希 Finish、源文件完整性及发布前后 canonical anchor 限制。正常 pair 原子发布、短 state batch 不覆盖较宽 event 后缀；从冷库重写/迁移保留 exact-end anchor，无日志末块也有证明。修复 crossing main/index 退休丢覆盖、恢复后提前信任未来文件、缺 anchor 却写 install stage的问题。无法在当前需求范围内安全修复的损坏宽 companion 返回明确离线修复错误，见 [维护说明](event-archive-frontier-v4.md)。
2. **权限紧凑值**：原 key、存在性、完整 Bloom 均保留；默认 Owner/Witness/Active 模板及 SELF 省略，operations 只引用不可变精确字典或保存原始 32 字节；未知字段用原 native raw fallback。模板直接构造 Permission，避免先展开旧大值。修复 gen0 未建水位行、当前账户已删除或重生时的冷历史候选遗漏：调用方先确认目标历史账户存在，枚举严格使用其目标 generation；不依赖当前账户是否存在，不把另一代新键混入。
3. **奖励最小状态**：保留所有周期键，只存有序 Votes＋独立 presence；保留重复/顺序/有符号票数及 Vote unknown，空快照不等于缺失。删除完整 Account 快照 API，actuator/TVM 改用 typed Votes；generic KV、physical writer、MuHash source fold 都拒绝旧完整 Account。诊断明确标记 Votes projection。

新 scheme 为 `state-flat-latest-muhash3072-compact-v4`，HEAD/element=4，snapshot manifest=5。MuHash承诺实际物理源的完整 key/value/presence，格式变化导致新根。旧 HEAD/manifest 必须先拒绝，不能自动清库、降级或重封旧根。本轮没有迁移线上库；以后上线需要新格式数据目录。

## 权限：同一 50 万行，实际完整 SST

[原始结果](business-state-v4-20261102-evidence/permission-results.json)。原输入 53,261,305 B、SHA256 `77f5db2d25223ea7d5bb52a1fcca72b87471993a37db947b7386b4088aae8b3b`；50 万行全部能逐字节还原旧 canonical native，输入长度与 key/value/每行8B framing 独立闭合。

|项目|旧值|V2 值|
|---|---:|---:|
|完整 Snappy SST|11,990,669 B|8,120,088 B|
|原物理 key 字节|18,500,012 B|相同|
|wrapped value 字节|30,761,293 B|11,966,802 B|
|Bloom|625,093 B|625,093 B|

实际减少 **3,870,581 B（32.2799%）**，完整 Bloom 大小保持一致。各自 ingest 为一张 L6 table，确认 private SST inode/内容未重写；32MiB cache，exact cursor，正负查询，每个结果验证，4轮交错AB/BA。此为同一当前点查栈中旧/新编码对比，不是新旧二进制完整 HTTP。

下表是每个点查的进程 CPU 中位数（µs，4轮）；scope=32 表示32次查询共用一次 cursor，缓存是 application cache，OS cache未清空。每轮一万次；表中正值表示变慢，不能只挑加速项。

|查询|scope|缓存|旧 CPU µs|新 CPU µs|变化|
|---|---:|---|---:|---:|---:|
|命中|1|空|18.7968|10.3535|-44.92%|
|命中|1|热|7.4281|7.1689|-3.49%|
|命中|32|空|9.7384|8.5349|-12.36%|
|命中|32|热|5.2363|5.4980|+5.00%|
|缺失|1|空|3.4350|1.8813|-45.23%|
|缺失|1|热|1.9833|2.7563|+38.98%|
|缺失|32|空|0.8332|0.9226|+10.73%|
|缺失|32|热|0.6689|0.8151|+21.85%|

空缓存命中改善明显；热缓存并非全面加速，缺失控制也出现回退。缺失路径不解码权限值，尚需 A/A 控制及真实 RPC 才能归因；本轮不作“零回退”或9M HTTP时延承诺。样本是固定 owner 前缀 cohort，不能把32.28%直接乘到整库。

## 奖励：正式实现与此前全量空间实验的关系

此前[同输入完整对照](chaindata-business-census-20261102-evidence/reward-votes-projection.json)覆盖 **19,724,230 行 SystemReward**，其中12,726,979条周期快照、19,363,802个 Votes；157个等原始输入边界的 Snappy SST，完整 keys/Bloom 保留：**4,512,508,220 → 823,022,838 B，减少3,689,485,382 B（81.7613%）**。这是整个该业务域独立 SST 对照，不是当前线上 du 已回收。

本轮正式 `MarshalRewardVotes` 使用与该实验相同的 native Account{Votes} 值模型，新增 strict 零分配 validator、typed API和生命周期验证。没有再次生成全量奖励 SST，也不把原实验的解码微基准当作新版完整 HTTP 指标。丢弃完整 Account 的非 Votes 字段是明确的新语义模型；旧完整值/旧 MuHash 不保留，不提供静默兼容适配。

## 事件：真实下一完整 ancient 段

[原始整段结果](business-state-v4-20261102-evidence/event-full-segment.json)：**52,625,408–52,690,943**，65,536块。旧已验证 SnapshotBuild=52,686,022，比下一段末端少4,921块；独立 verified Finish/solid cap=53,015,984，可完整放行该段。

- 产出 503,123,247 B（主文件438,734,269 B、index64,388,978 B）。
- **6,116,163 条日志**全部与 raw source 的顺序/位置/值 digest 相同，所需 range 的 indexed coverage 为 true。
- 1核 CPU quota、2GiB limit、I/O idle优先级下，构建 868.527s wall、734.374s process CPU；原始全段扫描 154.966s，冷侧全段扫描 381.154s。是六百多万条遍历，不是一次 eth_call。解耦消除了前沿等待，不能据此宣布实际发布吞吐已经追平同步；事件构建/遍历的 CPU和小读仍值得单独profile。
- 实验未捕获此前全部冷段/ancient，`whole_prefix_covered=false`；没有伪造前缀覆盖或宣称完整主网 freezer 追赶已实测。

最终产品读取器复用相同不可变文件，另外做[64个跨全段分布的窄查询](business-state-v4-20261102-evidence/event-narrow-queries.json)，每个查询均有 raw-source oracle，两个pass全部返回值一致：首轮 P50 **18.831ms**、最大 **97.229ms**；热缓存 P50 **6.695ms**、最大 **22.312ms**。是 block event 查询，仍不是完整 HTTP eth_call。

## 正确性、测试和构建边界

联合 fixture 覆盖权限＋奖励的 present/absent/empty/unknown、热冷历史、内存 revert、增量/全源 MuHash比较、unwind/replay、verified snapshot 恢复后继续执行；旧格式拒绝测试检查失败前后源不变。VM奖励新增不同caller对同合约连续提现，第一笔350、第二笔0并正确冲突回退，检查精确读依赖、双串行oracle、write seal、回执、历史前像和根。

产品 `make test`（900s）发现一项因新 typed 路径已能合法并行发布而失效的旧回退预期，修复并用11项针对性测试复核。随后完整 snapshots 包发现两项新增字段/版本 fixture 遗漏，修复并通过 schema/cache 定向测试及 race。修复后仅复测受影响范围，未再次运行完整全仓套件；原始失败和修复日志均保留。最终相关回归、race、`go vet ./...`、pure Go构建通过；没有反复重算线上1.27B源行。最后补查的删除/重生案例在仅cold history时确实复现了旧权限及AssetV2遗漏；已修复为target-generation候选，默认回归及联合MuHash/unwind/restore定向race通过，Astra只读复核无明确阻塞。服务器最终源码又独立复验通过（见final-history-status.json）。未变化的旧代行仍可能是历史基线，不能因当前账户不存在便视为可删除。

服务器现有 GCC11 的 CGO=1/Sapling构建通过；最后代际修复后的构建为 27.474s，[最终构建回执](business-state-v4-20261102-evidence/final-native-build.json)的源码为 `5e51a44c0d405863f3766fa6a55fee6d9dfc293c`、SHA256 `6f9523067bbf189e91486f840869520bc1aba9cd5c31cb1c25254277c0c028b6`，未运行/部署该二进制。Go overlay覆盖全部4个Go文件差异，四个源SHA均已在本机独立对照工作树；其余源码来自固定f6af09b1基线。旧 GCC4.8 构建过慢已终止，改用已安装工具链，没有安装软件或改变生产配置。

尚未在完整新V4主网数据上运行9M/10M/15M/20M完整 HTTP 对照，V3一致副本与V4格式不兼容，需要真实V4数据才能完成这项对照。上线前应使用真实V4数据补这项，并对热缓存控制的回退做有针对性的复核。

## 原始证据和线上身份

ZIP 34,941 B、36个成员，SHA256 `4870a648845154d3ccc6b546dbd110a4317a3a8994d24d991f9b50fcdb4f7e00`。本机已独立验证ZIP CRC及35个成员的长度/SHA；最终历史修复增补包16,315 B、13个成员，SHA256 `ce360223dfdcbd8f562d35c2b76e36c46c65e98fa36d8fe98b928a82f3b8e233`，其CRC/12个成员SHA也已独立验证；证据[校验与测试摘要](business-state-v4-20261102-evidence/verification-summary.json)。归档同时保留失败/取消的试验调度记录及后续替代验收关系。

最终 2026-11-02T08:19:29.957345Z，生产仍 active、PID21838、原0096c0af release，binary SHA `6611fc4273c457b47cad2ea47742a2eea0653b50e59fe0b1d74d10105bb41745`；高度 **53,426,413**。此前HTTP采样高度53,328,981，341余额0，342余额33,251,807,524,000,000 SUN（RPC按10^12缩放）准确。该采样验证原服务健康，不是新版主网性能结果。
