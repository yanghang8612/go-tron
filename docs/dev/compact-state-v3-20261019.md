# compact-v3 状态与冷索引优化验证（2026-10-19）

五项改造已实现，已合并线上 `eaae18c8` 的 MuHash 缓冲所有权优化和中等 L0 压力下的冷化调度修复。本报告对应独立分支的离线验证；没有推送、部署、停止服务或删库。本轮未执行完整主网创世重放。

## 实际空间收益

所有数值为实际写出的文件字节，包含相应头部、目录、校验和。GB/MB 均按十进制；不是预估线上 `du` 会立即减少的数量。

| 项目及数据边界 | 原文件 B | 新文件 B | 净节省 |
|---|---:|---:|---:|
| 8 字节值，5.715M 固定副本全部 12,478,784 行，Snappy SST | 146,875,958 | 134,432,259 | 12,443,699 B，8.47% |
| 账户，固定副本全部 1,349,700 行，Snappy SST | 67,897,044 | 61,593,097 | 6,303,947 B，9.28% |
| 交易索引，完整 1,307 run / 491,120,809 行 | 8,146,612,152 | 6,182,123,469 | 1,964,488,683 B，24.11% |
| history `.idx`，16 份完整固定样本，最终 4 KiB Snappy | 9,278,583 | 5,890,791 | 3,387,792 B，36.51% |
| VotesIndex，2,048 次独立追加/提交，真实历史 trio | 59,865,515 | 6,493,559 | 53,371,956 B，89.15% |

账户和数值的 SST 是相同键、相同 block/restart/filter/分文件设置的完整 corpus 对照，包含失效 generation。没有将它们当作运行中 LSM 的磁盘归因，也没有把不同范围的节省相加成全库预测。对应无压缩 SST，账户减少 25.56%，8 字节值减少 22.14%；可见 Snappy 原先已压掉不少重复字节。

VotesIndex 合成数据保留每块一个追加造成的历史放大；旧基线执行前身完整算法：读取/解码整表、线性去重、追加、整值写回。名单前值逻辑字节由 44,026,876 降至 2,755,512，减少 93.74%。代价是该名单的 latest 文件从 45,468 增至 189,096 B，多 143,628 B，用于分页和成员点查。此比例不能外推为整个 history 库减少 89%。

## 查询与更新性能

服务器 Intel Xeon Platinum 8175M，Go 1.25.5，一个 CPU、2 GiB 限额。索引基准使用预热的 OS 缓存；每个 reader 的解压页缓存限 64 KiB。下表索引耗时是“每文件均值的中位数”，不是 API 查询 p50/p95；8 字节/账户微基准为三次测量的中位数。

| 操作 | 原值 | 新值 | 含义 |
|---|---:|---:|---|
| 交易索引候选定位 | 1.627 µs | 1.522 µs | 成对耗时比中位数 0.940，略有改善 |
| history `.idx` 随机条目读取，4 KiB Snappy | 11.653 µs | 21.554 µs | 成对比中位数 1.860，额外约 10 µs |
| 8 字节值，MemoryDB 点读 | 836.5 ns | 838.9 ns | 基本持平；296→280 B/op，8→9 allocs/op |
| 8 字节值，单独解码 | 34.00 ns | 32.28 ns | 8 B/op、1 alloc/op 均相同 |
| 账户 core 解码 | 644.4 ns | 600.9 ns | 720 B/op、5 allocs/op 均相同 |
| VotesIndex 追加，2,048 次均值 | 121.247 µs | 21.032 µs | 约 5.76 倍吞吐差，未包含提交/MuHash 时间 |
| VotesIndex 全表枚举，100 次均值 | 58.398 µs | 233.855 µs | 约慢 4 倍，周期维护需要枚举 |

压缩并非所有操作都更快。最终选择的 4 KiB Snappy `.idx` 用约 10 µs 的索引读取开销换取 36.51% 样本空间；没有测量端到端历史 API 或冷设备 IO，所以不能宣称历史查询性能不变。完整枚举的代价也应纳入维护周期观察。

实际尝试的其他页策略：4 KiB Zstd 节省 56.94%，条目查询 11.873→45.956 µs；16 KiB Zstd 节省 62.52%，12.130→92.276 µs；16 KiB Snappy 节省 42.96%，12.267→49.193 µs。它们均未被选为默认。Reader 复用压缩读取缓冲和淘汰页的解压缓冲，避免每次 cache miss 新增页缓冲分配。

## 格式与正确性约束

- VotesIndex 存入同一 WitnessVoteState 域：`vi3-count` 精确 8 字节、每页最多 128 个有序地址、逐地址 membership。追加只改尾页/计数/成员；显式替换保留重复和顺序，清空删除所有页与成员但保留 present-zero 计数。DeleteVotes 保持前身“不删除枚举成员”的行为。热状态和 `PersistentHistoryReader.VotesIndexAt` 共用校验/分页逻辑，历史 Witness API 同样适配。
- 通用 latest 值新增 tag 3，仅对长度恰好 8 且有前导零的值使用最短大端尾部，读时恢复精确 8 字节。tag 2 仍表示精确 32 字节；空值、8 字节零、32 字节零各自独立。负数位模式不变；拒绝非规范外壳。cold latest 使用独立 bit29；这是 latest 编码优化，history `.seg` 的逻辑旧值仍保持其现有编码。
- 账户内部 core codec 2 / bitmap bit28 表示地址等于行 owner；仅在 21 字节完全匹配且 owner 前缀为 0x41 时省略。缺失/不同地址和未知 protobuf 字段保留；读取必须提供 owner。热读取、交易版本视图、历史读取和回滚均传递同一 owner。账户 wire protobuf 和共识规则不变。
- 交易索引 V2 保留 64 位 fingerprint，每 bucket 使用最小相对定位宽度；plain/mixed bucket 回退完整 8 字节。定位仍恢复为原先的 block/ordinal，碰撞仍返回全部候选，由原调用方用完整交易 hash 复核。合并、区间和 CRC 语义保留。
- history `.idx` 外层 V8 对原 V7 流分独立页压缩，逻辑 offset 不变。96 B 头、每页 24 B 目录、header/directory/page CRC、不可压缩页 raw fallback；页尺寸字段高位声明 Snappy。保留 Zstd reader/实验路径，生产 writer 默认 4 KiB Snappy。解压长度和目录边界均受限。

HEAD / element version 均为 3，MuHash domain 为 `state-flat-latest-muhash3072-compact-v3`，snapshot manifest version 为 4，latest 二进制 version 为 3。旧 HEAD/manifest 被明确拒绝；没有自动删除或重新初始化旧状态的路径。未来部署要使用独立新数据目录从创世同步，旧数据应保留；不能直接让新二进制继续旧数据目录。

## 验证与证据

固定热源三个源族的全部行数/分帧 SHA 与此前已完整验证 MuHash 的 5,715,086 高度 census 一致。账户/数值逐行逻辑 round trip，所有 SST 校验和通过。交易索引全部原 SHA 与此前 census 相同，所有 bucket 的 fingerprint/location 完全相等；16 份 history 原 SHA 与固定 manifest 相同，新旧全部索引条目相等。

集成测试在每次提交、回滚、重放、快照恢复后比较完整 MuHash accumulator 与 digest；覆盖分页跨界/重复/清空、8 字节空/零/负数、账户地址上下文、Storage 与 Shielded、冷热 as-of、历史 Witness 查询、恢复快照后继续执行。格式损坏、截断、页 CRC、旧版本拒绝、候选碰撞/合并和边界测试均保留。

Go 1.25.5 的完整仓库测试、相关 race/vet、Sapling 定向测试与生产 Sapling 构建的最终结果见证据目录；Linux 隔离版本正确性和构建亦通过。没有以这些测试代替整条主网创世重放。

完整原始 JSON、每 run/hash、基准输出、源码归档/补丁 SHA 和独立验证器位于 [证据目录](evidence/20261019-compact-v3/README.md)。复核命令：

```sh
python3 docs/dev/evidence/20261019-compact-v3/verify.py
```

服务器实际新旧文件保留在 `/data/gtron/compact-v3-validation-20261019-01`。交付时服务仍 active；此任务没有修改线上二进制、服务、参数或数据。
