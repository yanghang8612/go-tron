# 冷热历史的针对性实现与验证

后续更新：用户明确 CPU 核心富余后，又加入热/冷大输入并行切块与冷文件有界并行压缩，见 [CPU 与 I/O 优化报告](cpu-history-io-2026-09-07.md)。下文保留上一轮基线测量和验证时点；“V3 单文件串行”、编码耗时与二进制哈希不代表后续版本。

2026-09-07。本地实现；未部署、未启动服务器 gtron，没有操作 MySQL、生产 datadir 或维护停机标记。全部历史版本、交易边界、存在/不存在、generation、未知字段和查询含义必须保留。

## 选择依据

[业务抽样](history-business-types-2026-09-07.md) 中，130 个完整热区块的 Prev 字节约 89.87% 来自 `drax-0` 委托账户关系索引，7.45% 来自账户。最大委托值含 318,233 个地址条目、约 7 MB；地址本身是必要历史信息。问题在于小增删反复保存几乎相同的大列表，并非地址都能删掉。这里的比例是样本逻辑 Prev 字节，不能等同于整库物理分布。

选择可逆的重复表示压缩：账户消除确定可恢复的 envelope 字段；大列表在物理块层复用相同内容。保留全部逻辑旧值，避免把未知字段、列表顺序、重复条目或时间位置错误地当作冗余删除。

## 账户 V5：同时减少 latest 与新产生的热历史

实现见 `core/state/state_account_v5.go`、`core/state/state_account.go` 和正常提交路径 `core/state/statedb.go`。

- 默认 `EmptyKVRoot` 用空字段表示，非默认根仍完整保存。
- 区分零 CodeHash、空代码的 Keccak 哈希、引用内层完全相同的 32 B CodeHash、独立完整哈希；只有能够严格恢复的情况才缩短表示。
- 地址、内部 StorageCoreV4、未知字段和 generation 原样保留。无需 owner 的通用历史读取仍自包含。
- 新写入采用显式 V5；V4 明确可读且解码后重新编码保持 V4，不偷偷转换旧历史。

本地合成账户：151→87 B（42.38%）、184→120 B（34.78%），两类均减少 64 B。正常提交直接写入 arena 的中位时间分别 149.9→144.7 ns、151.1→159.2 ns，均一次分配；引用型通用解码多约 0.1 μs 校验。详见 `build/benchmarks/20260907-account-v5/summary.json`。这不是账户全量分布或磁盘 SST/冷压缩后的实际降幅。

V5 改变内部存储 commitment 的字节，适合用户已允许的新目录/从创世构建。外部 protobuf、API、共识执行结果不变；不能把新二进制当作旧格式无条件回滚承诺。

## 热区块内去重：完整实现，默认关闭

`core/rawdb/state_changeset_chunks.go` 使用共享的内容定义切块器，把同一 block pack 内相同字节复用为完整锚块引用。新压缩 envelope 版本为 2；原 RLP、序号、时间和修复行覆盖语义不变。正常与 borrowed 读取都先还原完整原始 RLP。

仅显式开启 `--history.block-dedup` 后，且 block pack ≥2 MiB、同一完整身份有至少两个 ≥128 KiB 的 Prev，才尝试该路径。候选逐字节验证；引用不能跨区块/DB value，也不能引用其他引用。decoded pack 上限 128 MiB，块大小 8–128 KiB（最后一块可更短），CRC 和长度/尾部检查拒绝损坏输入。只有相对原 Snappy 路径至少再省 12.5% 才保留新结果。

关闭时普通块保留现有 Snappy 快速路径，reader 仍支持新旧压缩 envelope。这样不会因为部署代码就让每个大块承担新增前台 CDC、哈希和复制成本。4×2 MiB、插入导致地址对齐变化的合成 fixture：8,388,960→2,353,719 B，减少 71.94%。最终三轮各 10 次基准中位编码约 0.616→11.717 ms，CDC 还原约 1.600 ms；原路径因该输入不适合 Snappy 而保存 raw，读取这个容器是直接返回 raw，不应解释其接近零的容器解码时间为实际历史查询耗时。因此本轮不默认开启，也不宣称提高前台同步吞吐。基准见 `build/benchmarks/20260907-targeted-history/hot-chunk-final.txt`；开启后的择优逻辑还会执行一次原 Snappy 尝试。

跨区块热历史依旧不做共享 blob 或差分链：这类格式需要新原子提交、重组恢复和引用回收协议。账户 V5 与更早冷化会作用于热数据，但不能声称已经消除了所有跨区块大列表的热写放大。

## 冷历史与积压

冷压缩采用透明 V3 容器，保持 V6 逻辑 record 和现有索引 offset。8–128 KiB 的内容块在每文件 64 MiB 有界字典中去重；相同片段指向更早的完整锚块，禁止引用链。元数据按页校验、稀疏目录定位，读取只缓存两个元数据页和两个解码页。每块解压受 128 KiB 上限保护，**读取一个数 MB 的完整旧值仍需访问其全部内容块**，并不承诺完整账户关系查询只读一页。

节点增加 `--history.compression-format=auto`，现有 `GTRON_HISTORY_COMPRESSION_FORMAT` 可覆盖为 1/2/3。auto 复用已有的 CollectKey 遍历：≥128 KiB 的 Prev 占总 Prev 字节至少一半，且完整 logical key 有两个大值，才选 V3；否则选原有 V2 流式 footer 格式。统计最多保留 4096 个完整身份/4 MiB key，不保存 Prev，也不增加全历史扫描。合并继承 V3 来源；没有此信息的旧 V2 段不在 auto 下强制迁成 CDC，可通过明确格式选择进行离线转换。

该条件是针对大值重复的候选筛选，不能证明任意输入都有相同片段；特别构造的同 key 大随机值仍可能没有收益。没有启用所有历史通用 CDC：小账户和低重复数据在对比基准中几乎不省空间且更慢。冷热改动是生产读写路径及明确开关，不是仅存在于实验命令中。

最终生产 factory 对比（同输入、V2 正常四 worker 配置、文件写入与 Sync，三轮中位；V3 单文件锚点串行）：

| 合成输入 | V2→V3 文件大小 | V2→V3 编码 | auto 决策 |
|---|---:|---:|---|
| 重复大列表，约 16 MiB | 16,783,058→2,362,146 B（−85.93%） | 20.98→33.13 ms（+57.9%） | 已有遍历确认重复大值占比后 V3 |
| 小账户记录，约 8 MiB | 2,219,141→2,219,679 B（略增） | 19.26→53.49 ms | V2 |
| 低重复随机内容，8 MiB | 8,391,520→8,396,782 B（略增） | 14.66→29.37 ms | 无重复大 key 条件时 V2 |

大列表顺序解码 3.055→2.688 ms，4 KiB 随机读取 21.3→7.0 μs，打开约 19.3→15.4 μs。最终缓存修复保留最大页 storage 容量，同时每次解码仍严格限制 cap，不反复为不同大小的页分配。完整数据见 [冷容器专项报告](targeted-cold-codec-2026-09-07.md) 和 `build/benchmarks/20260907-offline-recovery/cdc-factory-bench-final.txt`。所有数字均为本地合成输入，不是主网整库压缩率或在线 TPS 测量；也不声称新编码本身更快。

同时补齐迁移幂等识别、最终 history 目录 fsync 与解码上限测试。固定块压缩投影工具遇 V3 现在明确拒绝不支持的分析，不会把空样本错误投影成 100% 空间节省。

[冷化调度报告](targeted-cold-scheduling-2026-09-07.md) 记录实际实现、故障测试与排序基准：完整基础冷文件发布后先走原验证和热回收，再做重合并；压力输入由热历史 SST 估算与相关卷剩余容量组成。默认热量/低余量/构建最低余量为最小卷容量的 5%/10%/2%，可用 MiB 参数覆盖。

借鉴 Erigon 的部分是基础历史先成型、尽早从数据库回收、重合并可延后且分步推进。Erigon 本身也先把热历史写入数据库，本轮没有实现“journal 完全绕过 LSM”。来源及固定版本见 [Erigon 审计](erigon-history-storage-audit-2026-09-07.md)。

这些水位是调度与新构建准入控制，尚不是前台导入的硬背压或全卷写入预算；热删也不等于文件系统立即释放空间。固定磁盘能否容纳目标主网高度，需要真实全链净增长与转换峰值测量，不能用局部样本直接担保。

## 验证记录

- 账户：实际 Commit→热 Prev→冷文件→删除热 history 后的历史账户/代码查询；Snapshot revert 和 internal commitment unwind 后原 envelope/root 精确恢复；unknown/default/hash差异及定向 race。
- 热压缩：生产 writer、owning/borrowed/point reader、真实 unwind、身份/generation隔离、前向引用/引用链/损坏/长度上限；读写与已存在 repair 行路径的原测试。
- 调度：压力门、低空间、probe 失败、坏 accessor、后续 freezer 失败、merge 取消、重试恢复和单次合并上限。
- 第一遍 `go test ./...` 中所有实际源码包通过；失败项仅为 `build/benchmarks` 中此前下载的 Erigon/离线安装器源代码残片。已给这个被 git 忽略的审计产物目录加独立 `go.mod` 边界，避免主模块递归编译非项目源码；没有删掉生产测试。

最终版本验证完成：

- `go test ./... -count=1 -timeout 300s` 全通过：54 个有测试的 package、13 个无测试 package，0 失败。日志 `build/benchmarks/20260907-targeted-history/all-tests-final.txt`。
- rawdb、Pebble、cmd、state、snapshots、pruning 的新增关键路径定向 race 全通过；共享 chunker 单独全包 race 通过。日志同目录 `race-tests-final.txt`、`race-chunker-final.txt`。
- 显式 `GTRON_HISTORY_COMPRESSION_FORMAT=3` 的历史账户/代码查询测试通过（`cdc-state-query-tests.txt`）；冷容器专项报告另记录 FORMAT=3 全部 OfflineHistory 故障测试通过。
- `git diff --check` 通过。仅文档在以上最终代码验证后补充结果，未再改变生产代码。
- Linux amd64 / CGO=0 静态构建通过，产物 `build/bin/gtron-history-linux-amd64`，构建工具 Go 1.27.1；SHA-256 `c8742cc1bd98443f793caeb0f5472ea066609c0b7b0926f2c7437a5372878e6e`。未执行该节点二进制。

这次交付的是已接入正常路径并经过上述本地验证的代码与可构建产物；尚未部署或对主网数据作全量转换。当前生产占用不会因本地代码完成而自动下降。下一次真实回放/离线转换应分别记录组合文件体积、转换峰值、前台区块吞吐和历史查询性能，再据此决定热块去重是否开启，不能用新格式的局部比例替代固定磁盘容量验收。
