# 热历史覆盖检查的局部 companion 索引

2026-09-14，实施中。沿用 master → GitHub → 原生构建部署流程。当前线上业务
`af989753`，共享写入与 GC 排队已启用；本轮只优化 coverage gate 元数据查找。

## 定位

UTC 04:08:12 起 45.17 秒 CPU profile，146.93 CPU 秒。`findManifestRef` 累计
7.14 秒（4.86%），其中 7.11 秒来自 `newSnapshotStateDomainChangeCoverageGate`。
公共 manifest loader 返回不带 lookup 的可变副本，gate 遍历所有 history 时为每个
index/accessor 反复线性扫描整个 active catalog，约 O(H × N)。9/10 的发布后
decode cache 预填充已经生效，本轮不重复实现它。

原生固定目录样本位于 `/tmp/gtron-metadata-baseline-20260914/manifest.json`：
45,128,916 B，SHA256 `d752baf77b3ef3503b38d6e89400b82c5cc2a4ea091506a100e2a81f3f46821e`，
generation 33,683，active 70,198、retired 92,667。active 包含 history/inverted/accessor
各 2,794 个，event-log/event-log-index 各 30,908 个。此样本只固定元数据，不为源段
文件建立 pin，不允许离线基准假设其段文件会在生产回收期间永久存在。

## 设计及约束

新增 snapshots 窄只读 history companion view，私有持有 active SegmentRef 容器副本，
通过既有 `manifestLookupView` 建路径索引。只提供原 DomainCfg companion 查找，
保留 path、dataset、kind、range、aggregationSteps 等匹配规则及重复路径首个匹配
语义，不公开可变 Manifest 或其切片。共享不可变 string 值；原输入切片后续修改不
影响 view。超过既有 1,048,576 refs 上限直接返回 nil，调用方沿用原扫描，不额外复制。

pruner 仅在单次 gate 构造内使用该 view。返回 gate 后不持有额外索引/容器；延迟认证
仍走原路径。本轮不增加跨轮缓存，不改公共 loader 的可变副本契约，不改磁盘格式。

每轮读取完整 manifest bytes、缓存逐字节核对、每个 history/index/accessor 的 Stat、
size/mtime 身份检查、SHA 与语义认证、获锁后的 canonical/settled-prefix 证明全部保留。
完整覆盖和原子退休保持不变。新增成本为单轮 active refs 副本、排序索引及查询；测量
实际分配，不能把整个 view 内存误称为仅索引数组大小。

## 对照验证

1. 纯查询 oracle：无序与重复路径、所有身份字段不匹配、原 Manifest 修改、nil 和
   超限回退，候选与原扫描结果完全一致。
2. 真实文件 gate：缺 companion、尺寸或 mtime 改变、取消、旧 view 与新发布隔离，
   保留原完整认证与错误路径。
3. 混合规模完整 gate 基准计入索引构建、manifest load、全部 Stat；另用固定原生 JSON
   做仅元数据 companion replay。两者分别声明范围，不外推为同步同比例提速。
4. 本地受影响测试及 race、原生 Sapling 构建与兼容性测试、固定历史查询；上线后
   完整同进程采样与 CPU profile，结合批次/交易密度、冷覆盖和物理 du 评价。

## stateCommit 分支的结论

同期 profile 中前台 CommitStateCapture/commitWithStatsOptions 仅 1.15 CPU 秒；
PreparedOps 重复排序/校验占比很小，buildOps 大部分为必需哈希，不实施流水线改动。
commitment parent durable read 累计 16.08 秒，底层 block-cache 淘汰值得后续分析。
`runHandCold` 15.62 秒平 CPU 主要来自 commitment 预取/前台点读；冷 builder 顺序扫读
仅占 0.09 秒，不能据此归咎冷存扫描，也不盲目提高缓存或并发预算。调用链嵌套不相加。
