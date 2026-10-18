# 线上失效 generation 全量统计与安全回收结论（2026-10-18）

当前重同步实例在 **5,715,086** 高度只有 **849 条失效 KV 行、128,101 B 逻辑数据**（125.0986328125 KiB），来自 4 个仍存在的账户。占全部 KV 行数 **0.001871409%**，占 KV 逻辑字节 **0.002864413%**。没有 future generation 行。**目前不启用 generation GC**；它不是这个切面的容量大头。不能把这一高度的结果外推到先前 24M/35M 的另一实例。

## 一致切面与恢复服务

用户批准的一次短暂停机只用于采集 chaindata，一共 **8.894085 秒**完成采集与恢复。采集开始 `2026-10-18T13:02:26.990846+00:00`，结束 `2026-10-18T13:02:36.104313+00:00`；立即从现有高度用原二进制恢复，未删生产数据、未改配置、未发布新版本、未启动创世重同步。

- 运行源码：`bc939af29656839de71c1716b440100a5df7cab0`。
- 原二进制 SHA-256：`3eaf3405eb877f48b548e36d337d434215bd10b7b8061916b45f5c599e276b19`；恢复后相同。
- 冻结块：`5715086`；块哈希：`000000000057348e8ed54d676bea369a0a413af09e92e00a4ef630091218e6ad`。
- 冻结 TxNum：`174,114,684`；完整 MuHash 根：`3840ed6f0fb414aae75d26169149f638fb82aa4335c13defbf2cab85a811a9e3`。
- SST 硬链接保留 **15,329,235,658 B**；WAL/元数据独立复制 **794,414,579 B**。采集跳过生产 LOCK；副本只读打开并单独持锁。
- 全量扫描：`2026-10-18T13:05:31.62498534Z` 至 `2026-10-18T13:11:47.885044362Z`，未采样，后台限制 1 核、2 GiB、低 I/O 优先级。
- 证据收尾 `2026-10-18T13:22:55.920012+00:00`：599 个捕获文件检查通过；可变文件 SHA 未变，SST 大小及设备/inode 未变；原二进制 PID 23101 为 active。
- 后续公开 API 采样高度 5,816,739，sync active=true、paused=false，sessionBlocks=101,643。与冻结切面分别记录，不混作一个高度的统计。

副本保留在服务器 `/data/gtron/generation-census-20261018-1259/chaindata`，供复核使用。它保留捕获 SST 的硬链接，会阻止这些 SST 在源端后续 compaction 后释放对应磁盘块；这些保留占用不属于生产逻辑数据增长。尚未清理副本。

## 完整扫描与校验

| 物理源族 | 可见行数 | key + value 逻辑字节 |
|---|---:|---:|
| Account latest | 1,349,700 | 131,254,411 |
| Generation 表 | 89 | 4,539 |
| KV latest | 45,366,888 | 4,472,155,490 |
| 合计 | 46,716,677 | 4,603,414,440 |

全部源行重新编码后复算 **MuHash 完整规范化状态和 digest**，二者均精确等于 HEAD。所有可见 KV 包装与 key 通过严格解码；没有 pending restore/unwind，HEAD 与内部 latest root 一致，扫描结束 HEAD 未变。

补充遍历所有 **1,349,700** 个账户封套：4 个使用显式 generation，1,349,696 个使用默认 0；封套与 generation 表/default 的差异 **0**。补充结果的块号、块哈希、TxNum、root 与主扫描全部一致，文件不可变证明也通过。89 条 generation 记录中只有 4 条对应现存账户；其余计数器不能因账户暂不存在而删除。

该结果证明本次冻结状态内部一致；本次没有实现 GC，也没有运行“新 GC 后快照恢复”的生产实验，不能将 MuHash 复算说成此类实验已通过。未来 GC 必须通过独立恢复、重启、重放、reorg 验收后才可启用。

## 失效数据的精确分布

定义：物理 stored generation < 同一视图的 current generation；缺失 generation 表行取 0。stored > current 单列为异常并保留，不计可回收。本次全部失效行都是 **stored=0、current=1**，共 14 个 owner/generation/domain 分组；4 个 owner 均有账户封套，且封套 generation=1。没有失效 owner 缺账户。

| 分类 | 行数 | key B | 包装 value B | payload B | 逻辑 B |
|---|---:|---:|---:|---:|---:|
| current | 45,366,039 | 3,248,335,201 | 1,223,692,188 | 1,178,326,149 | 4,472,027,389 |
| obsolete | 849 | 68,438 | 59,663 | 58,814 | 128,101 |
| future | 0 | 0 | 0 | 0 | 0 |
| 合计 | 45,366,888 | 3,248,403,639 | 1,223,751,851 | 1,178,384,963 | 4,472,155,490 |

| 失效 domain | 行数 | key B | 包装 value B | payload B | 逻辑 B | 占失效逻辑字节 |
|---|---:|---:|---:|---:|---:|---:|
| ContractStorage | 837 | 67,797 | 27,621 | 26,784 | 95,418 | 74.486538% |
| ContractMetadata | 4 | 212 | 27,067 | 27,063 | 27,279 | 21.294916% |
| ContractABI | 4 | 208 | 4,939 | 4,935 | 5,147 | 4.017923% |
| AccountAssetV2 | 2 | 112 | 18 | 16 | 130 | 0.101482% |
| AccountAsset | 2 | 109 | 18 | 16 | 127 | 0.099141% |

| 账户（21 B hex） | 失效行数 | 失效逻辑 B |
|---|---:|---:|
| `4126997d9f8b5599cb316c27a7c764eaa96aac9629` | 513 | 62,331 |
| `41ff42bdac9f34bb61063e8157839b105ced3454ea` | 247 | 33,301 |
| `4138ec9ae573d6f87c1822a20273772caddfcfb1de` | 85 | 30,769 |
| `4115a53304d9d412171396b765336cf10ffed947fa` | 4 | 1,700 |

按 payload 大小：1–32 B 共 841 行/95,675 逻辑 B；129–1,024 B 共 1 行/370 B；1,025–4,096 B 共 6 行/12,898 B；4,097–65,536 B 共 1 行/19,158 B。最大失效 payload 为 **19,104 B**。逐组原始数据见 [inactive-groups.jsonl](evidence/20261018-generation-census/inactive-groups.jsonl)；原始 domain ID 256/257/258/514/515 对应 Storage/Metadata/ABI/Asset/AssetV2。

**128,101 B 是可见 key + 包装 value 的精确逻辑体积，不是承诺磁盘可释放量。** SST 内压缩块混存多种记录，还含索引、旧版本、tombstone 与 WAL；未来安全删除还必须新增历史 before-image、posting/accessor 等。没有做对照 compaction 实验，因此不宣称物理净回收量。当前收益过小，不值得为了测出少量物理字节而放大 I/O 或部署新 GC。

## 安全回收设计与当前决策

完整协议见 [设计文档](../superpowers/specs/2026-10-18-obsolete-generation-census-and-reclamation.md)。关键约束：

1. 失效行仍参与现有 MuHash，**不能直接 Delete、仅过滤快照，或原地改写当前 HEAD**。也不能删除 generation 计数器。
2. 若以后确有容量收益，应作为明确归属某个后继块的物理状态变更：重验已固化 generation、捕获旧物理 generation 的 before-image，通过正常 history 生命周期发布删除，然后将源行删除与 MuHash successor/HEAD/root/块身份一同提交。
3. GC 选择顺序、预算、激活边界和进度必须可确定性重放；所需游标/计划随同一提交切面及 checkpoint 保存。旧快照须有明确兼容策略，不能通过删库解决。
4. reorg 使用 before-image 同时恢复源行和 accumulator；快照始终输出剩余全部物理源，包括尚未回收的旧 generation。必须验证恢复后继续普通块与 GC 块、跨维护周期重放及故障恢复。
5. 仅在 disposable 副本对照实验确认**包含新增历史/索引在内的总物理净收益为正**，且上述完整验证通过后，才考虑单独评审、上线。此次只交付统计与设计，不启用回收。

## 此切面的真正大头

以下都是 KV latest 的可见逻辑数据，**不能直接当作整个 chaindata/history/冷库的磁盘占比**。

| domain | 行数 | 逻辑 B | 占 KV 逻辑字节 |
|---|---:|---:|---:|
| ContractStorage | 26,940,778 | 3,071,248,692 | 68.674909% |
| SystemShielded | 5,715,088 | 537,218,249 | 12.012513% |
| AccountAsset | 6,221,995 | 411,018,192 | 9.190606% |
| AccountAssetV2 | 6,253,495 | 406,477,175 | 9.089066% |
| ContractMetadata | 3,671 | 22,837,661 | 0.510663% |
| AccountResourceAux | 117,110 | 8,711,261 | 0.194789% |
| ContractABI | 3,430 | 4,780,786 | 0.106901% |
| AccountVotesAux | 46,277 | 3,935,315 | 0.087996% |
| AccountFrozenBandwidthAux | 30,111 | 2,164,934 | 0.048409% |
| SystemAsset | 13,269 | 1,898,265 | 0.042446% |
| SystemAccountIndex | 16,407 | 1,351,382 | 0.030218% |
| WitnessVoteState | 492 | 139,264 | 0.003114% |
| AccountFrozenSupplyAux | 950 | 67,786 | 0.001516% |
| AccountAssetOperationTimeV2 | 931 | 60,515 | 0.001353% |
| AccountFreeAssetNetUsageV2 | 931 | 60,515 | 0.001353% |
| AccountAssetOperationTime | 716 | 44,606 | 0.000997% |
| AccountFreeAssetNetUsage | 716 | 44,606 | 0.000997% |
| SystemExchange | 295 | 38,432 | 0.000859% |
| WitnessCapsule | 182 | 28,828 | 0.000645% |
| SystemProposal | 16 | 22,306 | 0.000499% |
| SystemWitnessSchedule | 2 | 4,524 | 0.000101% |
| SystemDynamicProperty | 23 | 1,933 | 0.000043% |
| SystemForkVote | 2 | 168 | 0.000004% |
| AccountPermissionAux | 1 | 95 | 0.000002% |

优先分析当前有效 ContractStorage 的 key/值编码和历史重复，再拆分 SystemShielded 的子键类别、保留需求及 key 开销；资产两表涉及 java-tron 的兼容语义，不能仅因看似重复而合并删除。本次未对这些当前有效行做内容合并或可删除判断。所有 KV 中 key 占 3,248,403,639 B，是后续编码评估的重要量，但压缩后的真实收益仍需实测。

## 工具、验证与证据

新增 `cmd/generation-census` 只读 CLI 和 `core/rawdb/inspect_generations.go`；未连接到生产启动/提交/快照/恢复路径。使用方法见 [CLI README](../../cmd/generation-census/README.md)。服务器初版主扫描工具 SHA-256 `15ba208628a3433ce26d2c43c51421342bcc44d3270930e37d4d1316b9014dee`，追加账户校验工具 SHA-256 `84c02e7392069a953e395d05c7418ad21362a4ec6f27f0cae252668a17237528`。最终 CLI 已将账户校验纳入一次完整运行。

本地 rawdb 与 CLI 完整包测试通过；相关 race、vet 和构建通过。覆盖随机独立 oracle、分组合计、缺省/未来 generation、异常包装/marker/root、取消、输出失败、独占锁与只读源保护。服务器 Go 1.25.5 原生构建及相关测试通过；捕获脚本的复制失败恢复/错误版本拒绝等模拟检查通过。

原始回执和报告保存在 [证据目录](evidence/20261018-generation-census/sha256.json)：每个文件均有 SHA-256，主报告保存全量分类分布与长度分帧源哈希，补充报告保存完整 HEAD。完整下载包 `/tmp/gtron-generation-census-evidence-20261018.tar.gz`，14,521 B，SHA-256 `f82633a334ace6bd781d180b5e2d5531c2bf29aa24fd9aa4127df859461fbb67`；本地下载校验一致。数据库未随证据包传输。
